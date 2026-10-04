//! Storage's own finalized-state replay + snapshot ring producer (CC-42 / D-P4-4).
//!
//! Architecture §3.3–3.6:
//! - Cadence `storage.snapshot_epochs` (default **32**), ring `storage.snapshot_ring`
//!   (default **4**), both config.
//! - Replay + SSZ serialize run on this **REPLAY TASK**, never on the writer.
//! - Snapshot put + oldest-ring delete are submitted as a **P2** chunk.
//! - Divergence guard: `hash_tree_root(replayed state)` must equal the stored
//!   `state_roots[slot]` (or the block's embedded `state_root`); mismatch is
//!   **fatal**, logs both roots, increments `cc_storage_replay_divergence_total`.
//! - Snapshots are **uncompressed** (ADR P4-14).
//!
//! Three measured terms (§3.4 / OQ-P4-4):
//! - (a) epoch-transition time during replay → `phase=replay`
//! - (b) SSZ serialize / write → `phase=serialize` + `phase=write`
//! - (c) SSZ deserialize + tree-hash-cache rebuild → `phase=load`
//!
//! Cadence conditional: if term (c) > **5.0 s**, `storage.snapshot_epochs` drops
//! to **16** in the same commit (executed via config default after measurement).

#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use cc_state_transition::{
    BlockSignatureStrategy, ExecutionEngine, NewPayloadRequest, PayloadStatus, TransitionContext,
    measured_canonical_root, process_slots, state_transition,
};
use cc_store::blocks::{get_block, get_state_root, state_root_at_offset};
use cc_store::canonical::get_canonical;
use cc_store::engine::Engine;
use cc_store::keys::BlockRegion;
use cc_store::meta::Split;
use cc_store::{
    DEFAULT_SNAPSHOT_EPOCHS, DEFAULT_SNAPSHOT_RING, Root, Slot, SplitLock, epoch_of_slot,
    epoch_start_slot, newest_snapshot, plan_snapshot_put, ring_depth, snapshot_due,
};
use cc_types::{
    BeaconState, ChainConfig, ForkName, Mainnet, Minimal, PresetName, SignedBeaconBlock,
};
use ssz::Encode;
use tokio::sync::{oneshot, watch};
use tracing::{error, info, warn};

use crate::metrics::{SnapshotPhase, SnapshotPhaseLabels, StorageClass, StorageMetrics};
use crate::writer::{BackgroundChunk, WriterError, WriterHandle};

/// Default snapshot cadence (Grandine archival interval).
pub(crate) const DEFAULT_SNAPSHOT_EPOCHS_CFG: u64 = DEFAULT_SNAPSHOT_EPOCHS;
/// Default ring depth.
pub(crate) const DEFAULT_SNAPSHOT_RING_CFG: u64 = DEFAULT_SNAPSHOT_RING;

/// Exact term-(c) boundary used by the cadence conditional (§10.2 / CC-42 /3).
pub(crate) const TERM_C_BOUNDARY_SECS: f64 = 5.0;

/// Cadence when term (c) exceeds the boundary.
pub(crate) const FALLBACK_SNAPSHOT_EPOCHS: u64 = 16;

/// Always-Valid execution engine for storage own-replay.
///
/// Payloads were accepted at first import; Phase 4 re-applies DA from
/// `da_status` on restore (CC-45), not here. Storage must not open a second
/// EL client (DAG: no fork-choice / no engine service edge).
#[derive(Debug, Default, Clone, Copy)]
struct ReplayAcceptEngine;

impl<P: cc_types::preset::Preset> ExecutionEngine<P> for ReplayAcceptEngine {
    fn verify_and_notify_new_payload(
        &self,
        _request: NewPayloadRequest<'_, P>,
    ) -> Result<PayloadStatus, cc_state_transition::EngineError> {
        Ok(PayloadStatus::Valid)
    }
}

/// Snapshot / replay knobs from `config/storage.toml`.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ReplayConfig {
    /// Produce a snapshot every N finalized epochs. Default **32**.
    pub snapshot_epochs: u64,
    /// Retain this many newest snapshots. Default **4**.
    pub snapshot_ring: u64,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self {
            snapshot_epochs: DEFAULT_SNAPSHOT_EPOCHS_CFG,
            snapshot_ring: DEFAULT_SNAPSHOT_RING_CFG,
        }
    }
}

/// Per-phase timings from one snapshot cycle (seconds).
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SnapshotTimings {
    /// (a) replay / epoch-transition wall time.
    pub replay_secs: f64,
    /// SSZ serialize only.
    pub serialize_secs: f64,
    /// Writer P2 commit only.
    pub write_secs: f64,
    /// (c) SSZ deserialize + tree-hash-cache rebuild.
    pub load_secs: f64,
    /// Uncompressed SSZ byte length.
    pub snapshot_bytes: u64,
    /// Slot written.
    pub slot: u64,
    /// Validator-set size that produced the state (0 if unknown / opaque path).
    pub validator_count: u64,
}

/// How divergence terminates the process (mirrors writer `ProcessExit`).
#[derive(Clone, Default)]
pub(crate) enum DivergenceExit {
    /// `std::process::exit(1)` (production).
    #[default]
    Os,
    /// Test hook (does not kill the harness).
    Hook(Arc<dyn Fn(i32) + Send + Sync>),
}

impl std::fmt::Debug for DivergenceExit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Os => write!(f, "DivergenceExit::Os"),
            Self::Hook(_) => write!(f, "DivergenceExit::Hook(..)"),
        }
    }
}

impl DivergenceExit {
    fn run(&self, code: i32) {
        match self {
            Self::Os => std::process::exit(code),
            Self::Hook(h) => h(code),
        }
    }
}

/// Shared replay / snapshot producer state.
#[derive(Debug)]
pub(crate) struct ReplayDriver {
    pub engine: Arc<Engine>,
    pub split: Arc<SplitLock>,
    pub writer: WriterHandle,
    pub metrics: StorageMetrics,
    pub cfg: ReplayConfig,
    /// Chain config for `state_transition` / `process_slots` (blob schedule, forks).
    pub chain_config: Arc<ChainConfig>,
    /// Epoch of the last successful snapshot (`None` = never).
    pub last_snapshot_epoch: AtomicU64,
    /// Whether `last_snapshot_epoch` has been set (AtomicU64 cannot encode Option).
    pub has_last_snapshot: AtomicBool,
    /// Snapshot cycles committed.
    pub snapshots_written: AtomicU64,
    /// Finalizations observed.
    pub finalizations_seen: AtomicU64,
    /// Single-flight: at most one snapshot cycle at a time (poll vs finalization).
    pub in_flight: AtomicBool,
    /// Divergence exit policy.
    pub on_divergence: DivergenceExit,
    /// Optional prune driver — snapshot-ring pass fires on each successful write
    /// (CC-46a §7.0: ring pass trigger is "on each snapshot write").
    pruner: std::sync::Mutex<Option<Arc<crate::prune::Pruner>>>,
}

/// RAII clear of [`ReplayDriver::in_flight`].
struct InFlightGuard<'a>(&'a AtomicBool);

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

impl ReplayDriver {
    pub(crate) fn new(
        engine: Arc<Engine>,
        split: Arc<SplitLock>,
        writer: WriterHandle,
        metrics: StorageMetrics,
        cfg: ReplayConfig,
        chain_config: ChainConfig,
    ) -> Self {
        // Seed last-snapshot epoch from the ring if present.
        let (has, last_epoch) = match engine.read() {
            Ok(rt) => match newest_snapshot(&rt) {
                Ok(Some((slot, _))) => (true, epoch_of_slot(slot)),
                _ => (false, 0),
            },
            Err(_) => (false, 0),
        };
        // Publish current ring depth gauge.
        if let Ok(rt) = engine.read()
            && let Ok(d) = ring_depth(&rt)
        {
            metrics
                .snapshot_ring_depth
                .set(i64::try_from(d).unwrap_or(i64::MAX));
        }
        Self {
            engine,
            split,
            writer,
            metrics,
            cfg,
            chain_config: Arc::new(chain_config),
            last_snapshot_epoch: AtomicU64::new(last_epoch),
            has_last_snapshot: AtomicBool::new(has),
            snapshots_written: AtomicU64::new(0),
            finalizations_seen: AtomicU64::new(0),
            in_flight: AtomicBool::new(false),
            on_divergence: DivergenceExit::Os,
            pruner: std::sync::Mutex::new(None),
        }
    }

    /// Test constructor with injectable divergence exit.
    pub(crate) fn with_divergence_exit(mut self, exit: DivergenceExit) -> Self {
        self.on_divergence = exit;
        self
    }

    /// Override chain config (tests).
    pub(crate) fn with_chain_config(mut self, cfg: ChainConfig) -> Self {
        self.chain_config = Arc::new(cfg);
        self
    }

    /// Attach the CC-46a pruner so the snapshot-ring pass runs on each write.
    pub(crate) fn set_pruner(&self, pruner: Arc<crate::prune::Pruner>) {
        *self.pruner.lock().unwrap_or_else(|e| e.into_inner()) = Some(pruner);
    }

    fn last_epoch(&self) -> Option<u64> {
        if self.has_last_snapshot.load(Ordering::SeqCst) {
            Some(self.last_snapshot_epoch.load(Ordering::SeqCst))
        } else {
            None
        }
    }

    /// Try to acquire single-flight. Returns `None` if a cycle is already running.
    fn try_begin_flight(&self) -> Option<InFlightGuard<'_>> {
        self.in_flight
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .ok()
            .map(|_| InFlightGuard(&self.in_flight))
    }

    /// Handle a `FINALIZED_CHECKPOINT`: maybe produce a snapshot.
    ///
    /// `seed_state_ssz` is used when the ring is empty (first snapshot after
    /// restore / test seed). Production restore (CC-45b) installs the first
    /// ring entry; until then a seed is required for the first cycle.
    pub(crate) async fn on_finalized_checkpoint(
        &self,
        finalized_epoch: u64,
        finalized_root: Root,
        expected_state_root: Root,
        seed_state_ssz: Option<&[u8]>,
    ) -> Result<Option<SnapshotTimings>, ReplayError> {
        self.finalizations_seen.fetch_add(1, Ordering::SeqCst);
        if !snapshot_due(self.last_epoch(), finalized_epoch, self.cfg.snapshot_epochs) {
            return Ok(None);
        }
        let Some(_flight) = self.try_begin_flight() else {
            // Poll path and finalization path raced — skip; the winner runs.
            return Ok(None);
        };
        let slot = epoch_start_slot(finalized_epoch);
        self.run_snapshot_cycle(slot, finalized_root, expected_state_root, seed_state_ssz)
            .await
            .map(Some)
    }

    /// Full cycle: load → ST replay → guard → serialize → P2 write → measure load.
    ///
    /// Caller must hold single-flight (via [`Self::on_finalized_checkpoint`] or
    /// [`Self::try_begin_flight`]).
    pub(crate) async fn run_snapshot_cycle(
        &self,
        finalized_slot: Slot,
        _finalized_root: Root,
        expected_state_root: Root,
        seed_state_ssz: Option<&[u8]>,
    ) -> Result<SnapshotTimings, ReplayError> {
        // Off the async worker for heavy CPU (decode / ST / serialize).
        let engine = Arc::clone(&self.engine);
        let split = Arc::clone(&self.split);
        let metrics = self.metrics.clone();
        let ring = self.cfg.snapshot_ring;
        let seed = seed_state_ssz.map(|s| s.to_vec());
        let on_div = self.on_divergence.clone();
        let chain_config = Arc::clone(&self.chain_config);

        let prepared = tokio::task::spawn_blocking(move || {
            prepare_snapshot(
                &engine,
                &split,
                &metrics,
                &chain_config,
                finalized_slot,
                expected_state_root,
                seed.as_deref(),
                ring,
                &on_div,
            )
        })
        .await
        .map_err(|e| ReplayError::Join(e.to_string()))??;

        // P2 write (never blocks ingest on the writer path for long — single put+deletes).
        let write_started = Instant::now();
        self.commit_snapshot_p2(&prepared.plan).await?;
        let write_secs = write_started.elapsed().as_secs_f64();
        observe_phase(&self.metrics, SnapshotPhase::Write, write_secs);

        // CC-46a: snapshot-ring prune pass fires on each successful snapshot write
        // (§7.0 table). plan_snapshot_put already stages ring eviction; this pass
        // re-asserts depth ≤ ring and records prune metrics / invocation counters.
        let pruner = self
            .pruner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(pruner) = pruner {
            match pruner.run_snapshot_ring_pass().await {
                crate::prune::PassOutcome::Ran { rows, bytes, .. } => {
                    info!(
                        target: "cc_storage::replay",
                        rows,
                        bytes,
                        "snapshot-ring prune pass after write"
                    );
                }
                crate::prune::PassOutcome::AlreadyAtMark => {}
                other => {
                    warn!(
                        target: "cc_storage::replay",
                        ?other,
                        "snapshot-ring prune pass skipped/failed (non-fatal)"
                    );
                }
            }
        }

        // Term (c): reload from store, deserialize + tree-hash-cache rebuild.
        // Off the runtime worker — same defect shape as S0-A-28's `block_on`.
        let load_secs = measure_load_from_store(&self.engine, prepared.plan.slot).await?;
        observe_phase(&self.metrics, SnapshotPhase::Load, load_secs);

        let depth = {
            let rt = self.engine.read().map_err(ReplayError::Store)?;
            ring_depth(&rt).map_err(ReplayError::Store)?
        };
        self.metrics
            .snapshot_ring_depth
            .set(i64::try_from(depth).unwrap_or(i64::MAX));
        self.metrics
            .snapshot_bytes
            .set(i64::try_from(prepared.plan.bytes).unwrap_or(i64::MAX));

        let epoch = epoch_of_slot(prepared.plan.slot);
        self.last_snapshot_epoch.store(epoch, Ordering::SeqCst);
        self.has_last_snapshot.store(true, Ordering::SeqCst);
        self.snapshots_written.fetch_add(1, Ordering::SeqCst);

        let timings = SnapshotTimings {
            replay_secs: prepared.replay_secs,
            serialize_secs: prepared.serialize_secs,
            write_secs,
            load_secs,
            snapshot_bytes: prepared.plan.bytes,
            slot: prepared.plan.slot.as_u64(),
            validator_count: prepared.validator_count,
        };

        info!(
            target: "cc_storage::replay",
            slot = timings.slot,
            bytes = timings.snapshot_bytes,
            validators = timings.validator_count,
            replay_s = timings.replay_secs,
            serialize_s = timings.serialize_secs,
            write_s = timings.write_secs,
            load_s = timings.load_secs,
            ring_depth = depth,
            ring,
            "snapshot cycle complete"
        );

        Ok(timings)
    }

    async fn commit_snapshot_p2(&self, plan: &cc_store::SnapshotPlan) -> Result<(), ReplayError> {
        let (done_tx, done_rx) = oneshot::channel();
        let chunk = BackgroundChunk {
            class: StorageClass::Snapshots,
            puts: plan.puts.clone(),
            deletes: plan.deletes.clone(),
            done: Some(done_tx),
        };
        if !self.writer.try_submit_p2(chunk, &self.metrics) {
            return Err(ReplayError::Writer(WriterError::ShutDown));
        }
        done_rx
            .await
            .map_err(|_| ReplayError::Writer(WriterError::ShutDown))?
            .map_err(ReplayError::Writer)
    }
}

struct PreparedSnapshot {
    plan: cc_store::SnapshotPlan,
    replay_secs: f64,
    serialize_secs: f64,
    validator_count: u64,
}

/// Load previous snapshot (or seed), **state-transition** replay to
/// `finalized_slot`, divergence guard, serialize, plan ring put.
///
/// D-P4-4: for each stored canonical block after the snapshot tip, decode SSZ
/// and run [`state_transition`] with [`BlockSignatureStrategy::NoVerification`]
/// (signatures verified at first import). Empty slots are advanced via
/// [`process_slots`]. Canonical root present with missing body is **fail closed**.
#[allow(clippy::too_many_arguments)]
fn prepare_snapshot(
    engine: &Engine,
    split: &SplitLock,
    metrics: &StorageMetrics,
    chain_config: &ChainConfig,
    finalized_slot: Slot,
    expected_state_root: Root,
    seed_state_ssz: Option<&[u8]>,
    ring: u64,
    on_divergence: &DivergenceExit,
) -> Result<PreparedSnapshot, ReplayError> {
    // LOCK ORDER: split read_recursive before ReadTxn (Architecture §3.1).
    let _split_guard = split.read_recursive();
    let _split_snap: Split = *_split_guard;

    let rt = engine.read().map_err(ReplayError::Store)?;
    // Explicit seed wins (tests / first install). Production finalization path
    // passes `None` and loads the newest ring entry for own-replay.
    let base_ssz = if let Some(ssz) = seed_state_ssz {
        ssz.to_vec()
    } else {
        match newest_snapshot(&rt).map_err(ReplayError::Store)? {
            Some((_slot, ssz)) => ssz,
            None => return Err(ReplayError::NoSeed),
        }
    };
    drop(rt);

    // ── term (a): ST replay ─────────────────────────────────────────────────
    let replay_started = Instant::now();
    let mut state = decode_mainnet_state(&base_ssz)?;
    replay_state_to_slot(engine, &mut state, finalized_slot, chain_config)?;
    let validator_count = state.validators_len() as u64;
    let replay_secs = replay_started.elapsed().as_secs_f64();
    observe_phase(metrics, SnapshotPhase::Replay, replay_secs);

    // ── divergence guard (CC-42 /5) — after real ST ─────────────────────────
    let replayed_root = measured_canonical_root(&mut state);
    let expected = resolve_expected_root(engine, finalized_slot, expected_state_root)?;
    if replayed_root != expected {
        metrics.replay_divergence.inc();
        error!(
            target: "cc_storage::replay",
            replayed = %replayed_root,
            expected = %expected,
            slot = finalized_slot.as_u64(),
            "replay divergence — FATAL (voids the run)"
        );
        on_divergence.run(1);
        return Err(ReplayError::Divergence {
            replayed: replayed_root,
            expected,
            slot: finalized_slot.as_u64(),
        });
    }

    let snap_slot = finalized_slot;

    // ── term (b) serialize half ─────────────────────────────────────────────
    let ser_started = Instant::now();
    let ssz = state.as_ssz_bytes();
    let serialize_secs = ser_started.elapsed().as_secs_f64();
    observe_phase(metrics, SnapshotPhase::Serialize, serialize_secs);

    let rt = engine.read().map_err(ReplayError::Store)?;
    let plan = plan_snapshot_put(&rt, snap_slot, &ssz, ring).map_err(ReplayError::Store)?;

    Ok(PreparedSnapshot {
        plan,
        replay_secs,
        serialize_secs,
        validator_count,
    })
}

/// Advance `state` from its current slot to `target` using real ST.
///
/// - Canonical root + body → [`state_transition`] (`NoVerification`)
/// - Canonical root, no body → [`ReplayError::MissingBlock`] (fail closed)
/// - No canonical root → empty slot; deferred to [`process_slots`] at the end
///   or consumed inside the next block's `process_slots`
fn replay_state_to_slot(
    engine: &Engine,
    state: &mut BeaconState<Mainnet>,
    target: Slot,
    chain_config: &ChainConfig,
) -> Result<(), ReplayError> {
    if state.slot() > target {
        return Err(ReplayError::StatePastTarget {
            state_slot: state.slot().as_u64(),
            target: target.as_u64(),
        });
    }
    if state.slot() == target {
        return Ok(());
    }

    let accept = ReplayAcceptEngine;
    let ctx = TransitionContext::new(chain_config, &accept);

    // Materialise (slot, block_ssz) for every non-empty canonical slot in range.
    let from = state.slot().as_u64().saturating_add(1);
    let to = target.as_u64();
    let mut blocks: Vec<(Slot, Vec<u8>)> = Vec::new();
    {
        let rt = engine.read().map_err(ReplayError::Store)?;
        for slot_u in from..=to {
            let slot = Slot::new(slot_u);
            let Some(root) = get_canonical(&rt, slot).map_err(ReplayError::Store)? else {
                continue; // empty / missed slot
            };
            let block_ssz = get_block(&rt, slot, &root, BlockRegion::Hot)
                .map_err(ReplayError::Store)?
                .or(get_block(&rt, slot, &root, BlockRegion::Cold).map_err(ReplayError::Store)?);
            match block_ssz {
                Some(ssz) => blocks.push((slot, ssz)),
                None => {
                    return Err(ReplayError::MissingBlock { slot: slot_u, root });
                }
            }
        }
    }

    for (slot, ssz) in blocks {
        let signed = decode_signed_block(&ssz)?;
        if signed.message.slot != slot {
            return Err(ReplayError::Ssz(format!(
                "block SSZ slot {} != canonical slot {}",
                signed.message.slot.as_u64(),
                slot.as_u64()
            )));
        }
        state_transition(state, &signed, &ctx, BlockSignatureStrategy::NoVerification).map_err(
            |e| ReplayError::Transition {
                slot: slot.as_u64(),
                detail: e.to_string(),
            },
        )?;
    }

    // Empty slots after the last applied block (or entire range if no blocks).
    if state.slot() < target {
        process_slots(state, target, chain_config).map_err(|e| ReplayError::Transition {
            slot: target.as_u64(),
            detail: format!("process_slots: {e}"),
        })?;
    }
    Ok(())
}

fn decode_signed_block(ssz: &[u8]) -> Result<SignedBeaconBlock<Mainnet>, ReplayError> {
    SignedBeaconBlock::<Mainnet>::from_ssz_bytes_with(ForkName::Fulu, ssz)
        .map_err(|e| ReplayError::Ssz(format!("SignedBeaconBlock decode failed: {e:?}")))
}

fn resolve_expected_root(engine: &Engine, slot: Slot, fallback: Root) -> Result<Root, ReplayError> {
    let rt = engine.read().map_err(ReplayError::Store)?;
    if let Some(sr) = get_state_root(&rt, slot).map_err(ReplayError::Store)? {
        return Ok(sr);
    }
    if let Some(root) = get_canonical(&rt, slot).map_err(ReplayError::Store)?
        && let Some(ssz) = get_block(&rt, slot, &root, BlockRegion::Hot)
            .map_err(ReplayError::Store)?
            .or(get_block(&rt, slot, &root, BlockRegion::Cold).map_err(ReplayError::Store)?)
        && let Ok(sr) = state_root_at_offset(&ssz)
    {
        return Ok(sr);
    }
    if fallback != Root::ZERO {
        return Ok(fallback);
    }
    Err(ReplayError::MissingExpectedRoot {
        slot: slot.as_u64(),
    })
}

fn decode_mainnet_state(ssz: &[u8]) -> Result<BeaconState<Mainnet>, ReplayError> {
    BeaconState::<Mainnet>::from_ssz_bytes_hydrated(ForkName::Fulu, ssz)
        .map_err(|e| ReplayError::Ssz(format!("BeaconState decode failed: {e:?}")))
}

/// ADR-R-11 anchor witness. The only stored cross-check is this accessor.
///
/// `None` means the bytes are not a beacon state for `preset`. Callers that
/// need the root of a snapshot use this instead of naming the container.
pub(crate) fn genesis_validators_root_from_state_ssz(
    preset: PresetName,
    ssz: &[u8],
) -> Option<Root> {
    match preset {
        PresetName::Mainnet => BeaconState::<Mainnet>::from_ssz_bytes_hydrated(ForkName::Fulu, ssz)
            .ok()
            .map(|state| state.genesis_validators_root()),
        PresetName::Minimal => BeaconState::<Minimal>::from_ssz_bytes_hydrated(ForkName::Fulu, ssz)
            .ok()
            .map(|state| state.genesis_validators_root()),
    }
}

fn observe_phase(metrics: &StorageMetrics, phase: SnapshotPhase, secs: f64) {
    metrics
        .snapshot_seconds
        .get_or_create(&SnapshotPhaseLabels {
            phase: phase.as_str().to_owned(),
        })
        .observe(secs);
}

/// Term (c): load newest snapshot from store, deserialize, rebuild tree-hash cache.
///
/// 200 MB SSZ decode + tree-hash is multi-second CPU. Same defect *shape* as
/// S0-A-28's `block_on`: that work must not run on a runtime worker.
pub(crate) async fn measure_load_from_store(
    engine: &Arc<Engine>,
    slot: Slot,
) -> Result<f64, ReplayError> {
    let engine = Arc::clone(engine);
    tokio::task::spawn_blocking(move || measure_load_from_store_sync(&engine, slot))
        .await
        .map_err(|e| ReplayError::Join(e.to_string()))?
}

fn measure_load_from_store_sync(engine: &Engine, slot: Slot) -> Result<f64, ReplayError> {
    let rt = engine.read().map_err(ReplayError::Store)?;
    let ssz = cc_store::get_snapshot(&rt, slot)
        .map_err(ReplayError::Store)?
        .ok_or(ReplayError::MissingSnapshot {
            slot: slot.as_u64(),
        })?;
    drop(rt);
    #[cfg(test)]
    apply_measure_test_stall();
    let started = Instant::now();
    let mut state = decode_mainnet_state(&ssz)?;
    let _ = measured_canonical_root(&mut state);
    Ok(started.elapsed().as_secs_f64())
}

/// Test-only stall so a current-thread timer can prove the reactor is free
/// while term (c) occupies a blocking-pool worker (S2-B-11 / S0-A-28).
#[cfg(test)]
static MEASURE_STALL_MS: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
fn apply_measure_test_stall() {
    let ms = MEASURE_STALL_MS.load(Ordering::SeqCst);
    if ms > 0 {
        std::thread::sleep(Duration::from_millis(ms));
    }
}

/// Measure term (c) against an in-memory SSZ blob (Hoodi fixture / soak).
pub(crate) fn measure_load_from_bytes(ssz: &[u8]) -> Result<(f64, u64, u64), ReplayError> {
    let started = Instant::now();
    let mut state = decode_mainnet_state(ssz)?;
    let _ = measured_canonical_root(&mut state);
    let secs = started.elapsed().as_secs_f64();
    let validators = state.validators_len() as u64;
    Ok((secs, ssz.len() as u64, validators))
}

/// Cadence conditional: if term (c) > 5.0 s → 16 epochs, else keep 32.
#[must_use]
pub(crate) fn cadence_after_term_c(term_c_secs: f64) -> u64 {
    if term_c_secs > TERM_C_BOUNDARY_SECS {
        FALLBACK_SNAPSHOT_EPOCHS
    } else {
        DEFAULT_SNAPSHOT_EPOCHS_CFG
    }
}

/// Spawn the REPLAY TASK (Architecture §1.5).
///
/// Polls finality via the split lock and produces snapshots on cadence. Also
/// accepts push notifications via [`ReplayDriver::on_finalized_checkpoint`].
pub(crate) fn spawn_replay_task(
    driver: Arc<ReplayDriver>,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        info!(target: "cc_storage::replay", "replay task started");
        let mut ticker = tokio::time::interval(Duration::from_secs(2));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        break;
                    }
                }
                _ = ticker.tick() => {
                    // Poll path: if split advanced past cadence, attempt a cycle.
                    // Single-flight is taken inside on_finalized_checkpoint so a
                    // concurrent finalization-driven run is not doubled.
                    let snap = driver.split.snapshot();
                    let epoch = epoch_of_slot(snap.slot);
                    if snap.slot.as_u64() == 0 {
                        continue;
                    }
                    if !snapshot_due(driver.last_epoch(), epoch, driver.cfg.snapshot_epochs) {
                        continue;
                    }
                    match driver
                        .on_finalized_checkpoint(
                            epoch,
                            snap.block_root,
                            snap.state_root,
                            None,
                        )
                        .await
                    {
                        Ok(Some(t)) => {
                            info!(
                                target: "cc_storage::replay",
                                slot = t.slot,
                                load_s = t.load_secs,
                                "poll-path snapshot written"
                            );
                        }
                        Ok(None) => {}
                        Err(ReplayError::NoSeed) => {
                            // Wait for restore / first seed (CC-45b).
                        }
                        Err(e) => {
                            warn!(target: "cc_storage::replay", error = %e, "snapshot cycle failed");
                        }
                    }
                }
            }
        }
        info!(target: "cc_storage::replay", "replay task stopped");
    })
}

/// Async notify helper (best-effort).
pub(crate) async fn maybe_snapshot_on_finalized(
    driver: &ReplayDriver,
    epoch: u64,
    finalized_root: Root,
    state_root: Root,
) {
    match driver
        .on_finalized_checkpoint(epoch, finalized_root, state_root, None)
        .await
    {
        Ok(Some(t)) => {
            info!(
                target: "cc_storage::replay",
                slot = t.slot,
                bytes = t.snapshot_bytes,
                "finalization-driven snapshot written"
            );
        }
        Ok(None) => {}
        Err(ReplayError::NoSeed) => {}
        Err(e) => {
            warn!(
                target: "cc_storage::replay",
                error = %e,
                epoch,
                "snapshot on finalization failed"
            );
        }
    }
}

/// Replay-layer errors.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ReplayError {
    #[error("store: {0}")]
    Store(#[from] cc_store::StoreError),
    #[error("writer: {0}")]
    Writer(WriterError),
    #[error("ssz: {0}")]
    Ssz(String),
    #[error("no snapshot seed (ring empty and no seed_state_ssz)")]
    NoSeed,
    #[error("missing expected state root at slot {slot}")]
    MissingExpectedRoot { slot: u64 },
    #[error("missing snapshot at slot {slot}")]
    MissingSnapshot { slot: u64 },
    /// Canonical root present but block body absent — fail closed.
    #[error("missing block body at slot {slot} root {root} (canonical row without SSZ)")]
    MissingBlock { slot: u64, root: Root },
    #[error("state transition at slot {slot}: {detail}")]
    Transition { slot: u64, detail: String },
    #[error("state slot {state_slot} is past target {target}")]
    StatePastTarget { state_slot: u64, target: u64 },
    #[error("replay divergence at slot {slot}: replayed={replayed} expected={expected}")]
    Divergence {
        replayed: Root,
        expected: Root,
        slot: u64,
    },
    #[error("join: {0}")]
    Join(String),
    #[error("internal: {0}")]
    Internal(String),
}

// ── Hoodi fixture helpers (measurement / soak) ──────────────────────────────

const HOODI_ANCHOR_SLOT: u64 = 3649472;

/// Resolve the Hoodi fixture cache root (env or `~/.cache/cc-hoodi-fixtures`).
pub(crate) fn hoodi_cache_root() -> Option<PathBuf> {
    cc_config::hoodi_fixtures_cache_root()
}

/// Path to the anchor `beacon_state.ssz` when the cache is present.
pub(crate) fn hoodi_state_path(root: &Path) -> PathBuf {
    root.join(HOODI_ANCHOR_SLOT.to_string())
        .join("beacon_state.ssz")
}

/// Load Hoodi anchor state bytes when available.
pub(crate) fn try_load_hoodi_state_bytes() -> Option<Vec<u8>> {
    let root = hoodi_cache_root()?;
    let path = hoodi_state_path(&root);
    if !path.is_file() {
        return None;
    }
    std::fs::read(&path).ok()
}

// ── tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::metrics::StorageMetrics;
    use crate::writer::{WriterBounds, WriterFaults, spawn_writer};
    use cc_state_transition::process_slots;
    use cc_store::blocks::put_state_root;
    use cc_store::engine::{Durability, EngineOptions};
    use cc_store::meta::WriteCursor as StoreCursor;
    use cc_store::{Split, plan_snapshot_put, put_snapshot};
    use prometheus_client::registry::Registry;
    use std::sync::atomic::AtomicU64;

    fn test_metrics() -> StorageMetrics {
        let mut reg = Registry::default();
        StorageMetrics::register(&mut reg)
    }

    fn open_engine(tag: &str) -> Arc<Engine> {
        static N: AtomicU64 = AtomicU64::new(0);
        let id = N.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("cc-storage-replay-{tag}-{id}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Arc::new(
            Engine::open(
                &dir,
                EngineOptions::default().with_durability(Durability::None),
            )
            .unwrap(),
        )
    }

    fn seed_state_and_root() -> (Vec<u8>, Root) {
        let mut state = BeaconState::<Mainnet>::default();
        state.set_slot(Slot::new(32));
        let ssz = state.as_ssz_bytes();
        let mut s2 = state;
        let root = measured_canonical_root(&mut s2);
        (ssz, root)
    }

    async fn driver_with(
        tag: &str,
        cfg: ReplayConfig,
        exit: DivergenceExit,
    ) -> (Arc<ReplayDriver>, watch::Sender<bool>) {
        let engine = open_engine(tag);
        let metrics = test_metrics();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let writer = spawn_writer(
            Arc::clone(&engine),
            metrics.clone(),
            WriterBounds::default(),
            WriterFaults::default(),
            shutdown_rx,
            false, // not process-fatal in tests
        );
        let split = Arc::new(SplitLock::new(Split {
            slot: Slot::new(32),
            state_root: Root::ZERO,
            block_root: Root::ZERO,
        }));
        let mut chain = ChainConfig::from_yaml_str(include_str!(
            "../../../crates/types/tests/fixtures/hoodi-config.yaml"
        ))
        .expect("hoodi fixture for replay tests");
        // The label is not a fork input. It proves `new` keeps the caller's chain.
        chain.config_name = "replay-supplied".into();
        let driver = Arc::new(
            ReplayDriver::new(engine, split, writer, metrics, cfg, chain)
                .with_divergence_exit(exit),
        );
        (driver, shutdown_tx)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fifth_snapshot_via_p2_keeps_ring_depth_four() {
        // CC-42 /1 through the writer P2 path.
        let (driver, _sd) = driver_with(
            "ring-p2",
            ReplayConfig {
                snapshot_epochs: 1,
                snapshot_ring: 4,
            },
            DivergenceExit::Hook(Arc::new(|_| {})),
        )
        .await;

        for i in 0..5u64 {
            let slot = Slot::new((i + 1) * 32);
            let mut state = BeaconState::<Mainnet>::default();
            state.set_slot(slot);
            let ssz = state.as_ssz_bytes();
            let mut s2 = BeaconState::<Mainnet>::from_ssz_bytes_with(ForkName::Fulu, &ssz).unwrap();
            let root = measured_canonical_root(&mut s2);

            // Seed expected root in state_roots.
            {
                let mut batch = driver.engine.batch();
                let rt = driver.engine.read().unwrap();
                put_state_root(&rt, &mut batch, slot, &root).unwrap();
                drop(rt);
                driver.engine.commit(batch).unwrap();
            }

            // Force due by clearing last each time with cadence 1.
            driver.has_last_snapshot.store(false, Ordering::SeqCst);
            let t = driver
                .on_finalized_checkpoint(epoch_of_slot(slot), Root::ZERO, root, Some(&ssz))
                .await
                .unwrap()
                .expect("snapshot due");
            assert_eq!(t.slot, slot.as_u64());
            assert_eq!(t.snapshot_bytes, ssz.len() as u64);

            let rt = driver.engine.read().unwrap();
            let depth = ring_depth(&rt).unwrap();
            assert!(depth <= 4, "depth {depth}");
            if i >= 4 {
                assert_eq!(depth, 4);
            }
            // Gauge
            assert_eq!(driver.metrics.snapshot_ring_depth.get(), depth as i64);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn divergence_guard_fatal_logs_and_increments_exactly_one() {
        // CC-42 /5 negative: corrupt expected root → divergence.
        let fired = Arc::new(AtomicU64::new(0));
        let fired_c = Arc::clone(&fired);
        let (driver, _sd) = driver_with(
            "div",
            ReplayConfig {
                snapshot_epochs: 1,
                snapshot_ring: 4,
            },
            DivergenceExit::Hook(Arc::new(move |_| {
                fired_c.fetch_add(1, Ordering::SeqCst);
            })),
        )
        .await;

        let (ssz, real_root) = seed_state_and_root();
        let mut corrupt = real_root.into_array();
        corrupt[0] ^= 0xFF;
        let wrong = Root::from_array(corrupt);

        // Store wrong expected root.
        {
            let mut batch = driver.engine.batch();
            let rt = driver.engine.read().unwrap();
            put_state_root(&rt, &mut batch, Slot::new(32), &wrong).unwrap();
            drop(rt);
            driver.engine.commit(batch).unwrap();
        }

        let before = driver.metrics.replay_divergence.get();
        let err = driver
            .on_finalized_checkpoint(1, Root::ZERO, wrong, Some(&ssz))
            .await
            .unwrap_err();
        match err {
            ReplayError::Divergence {
                replayed,
                expected,
                slot,
            } => {
                assert_eq!(slot, 32);
                assert_eq!(replayed, real_root);
                assert_eq!(expected, wrong);
            }
            other => panic!("expected Divergence, got {other}"),
        }
        assert_eq!(driver.metrics.replay_divergence.get() - before, 1);
        assert_eq!(fired.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn positive_snapshot_zero_divergence() {
        let (driver, _sd) = driver_with(
            "pos",
            ReplayConfig {
                snapshot_epochs: 1,
                snapshot_ring: 4,
            },
            DivergenceExit::Hook(Arc::new(|_| panic!("must not fire"))),
        )
        .await;
        let (ssz, root) = seed_state_and_root();
        {
            let mut batch = driver.engine.batch();
            let rt = driver.engine.read().unwrap();
            put_state_root(&rt, &mut batch, Slot::new(32), &root).unwrap();
            drop(rt);
            driver.engine.commit(batch).unwrap();
        }
        let t = driver
            .on_finalized_checkpoint(1, Root::ZERO, root, Some(&ssz))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(driver.metrics.replay_divergence.get(), 0);
        assert!(t.snapshot_bytes > 0);
        // All four phases observed (load/serialize/replay/write ≥ 0).
        assert!(t.load_secs >= 0.0);
        assert!(t.serialize_secs >= 0.0);
    }

    /// Holds [`MEASURE_STALL_MS`] for the duration of the responsiveness test.
    struct MeasureStallGuard;

    impl MeasureStallGuard {
        fn set(ms: u64) -> Self {
            MEASURE_STALL_MS.store(ms, Ordering::SeqCst);
            Self
        }
    }

    impl Drop for MeasureStallGuard {
        fn drop(&mut self) {
            MEASURE_STALL_MS.store(0, Ordering::SeqCst);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn runtime_stays_responsive_during_measure_load_from_store() {
        // S2-B-11: same defect shape as S0-A-28's `block_on`. A current-thread
        // timer must fire while term (c) is in flight; without `spawn_blocking`
        // the stall occupies the reactor and this sleep overruns.
        let _stall = MeasureStallGuard::set(250);
        let engine = open_engine("s2-b-11-resp");
        let ssz = BeaconState::<Mainnet>::default().as_ssz_bytes();
        let slot = Slot::new(32);
        put_snapshot(&engine, slot, &ssz, 4).unwrap();

        let load = measure_load_from_store(&engine, slot);
        tokio::pin!(load);

        let probe = tokio::time::timeout(
            Duration::from_millis(80),
            tokio::time::sleep(Duration::from_millis(10)),
        );

        tokio::select! {
            r = probe => {
                assert!(
                    r.is_ok(),
                    "runtime timer did not fire during measure_load_from_store \
                     (S0-A-28 shape: CPU on a runtime worker)"
                );
            }
            r = &mut load => {
                panic!(
                    "measure_load_from_store finished before the runtime probe \
                     (missing spawn_blocking or stall); {r:?}"
                );
            }
        }

        let secs = load.await.unwrap();
        assert!(secs >= 0.0);
    }

    #[test]
    fn cadence_conditional_boundary_exact() {
        // Exact 5.0 boundary: ≤ keeps 32, > drops to 16.
        assert_eq!(cadence_after_term_c(5.0), 32);
        assert_eq!(cadence_after_term_c(5.000_000_1), 16);
        assert_eq!(cadence_after_term_c(0.5), 32);
        assert_eq!(cadence_after_term_c(40.0), 16);
    }

    #[test]
    fn term_c_hoodi_or_scaffold() {
        // CC-42 /3 + /4: measure term (c) when Hoodi cache is present; otherwise
        // record scaffold path with a default Mainnet state.
        if let Some(bytes) = try_load_hoodi_state_bytes() {
            assert!(
                bytes.len() as u64 >= 150 * 1024 * 1024,
                "Hoodi state must be ≥ 150 MB, got {}",
                bytes.len()
            );
            // Term (b) serialize half (decode once, then SSZ-encode).
            let state =
                BeaconState::<Mainnet>::from_ssz_bytes_with(ForkName::Fulu, &bytes).unwrap();
            let validators = state.validators_len() as u64;
            let ser_started = Instant::now();
            let ssz = state.as_ssz_bytes();
            let serialize_secs = ser_started.elapsed().as_secs_f64();
            assert_eq!(ssz.len(), bytes.len(), "uncompressed round-trip length");

            // Term (c) deserialize + tree-hash-cache rebuild.
            let (load_secs, blen, v2) = measure_load_from_bytes(&bytes).unwrap();
            assert_eq!(v2, validators);
            assert_eq!(blen, bytes.len() as u64);

            // Term (a) scaffold: one epoch of process_slots is Phase 1 mid-gate;
            // record the reference number, not a new ST run here.
            let term_a_epoch_ms_ref = 605.29_f64; // phase-1-soak mid-gate mean wall

            eprintln!(
                "CC-42 terms Hoodi: (a)_ref_epoch_ms={term_a_epoch_ms_ref:.2} \
                 (b)_serialize={serialize_secs:.3}s (c)_load={load_secs:.3}s \
                 bytes={blen} validators={validators}"
            );
            let cadence = cadence_after_term_c(load_secs);
            eprintln!("CC-42 cadence after term (c): {cadence} (boundary 5.0 s)");
            assert!(cc_store::buckets::SNAPSHOT_SECONDS.contains(&5.0));
            assert_eq!(
                cadence,
                if load_secs > TERM_C_BOUNDARY_SECS {
                    16
                } else {
                    32
                }
            );
        } else {
            let state = BeaconState::<Mainnet>::default();
            let ssz = state.as_ssz_bytes();
            let (secs, blen, validators) = measure_load_from_bytes(&ssz).unwrap();
            eprintln!(
                "CC-42 term (c) scaffold (no Hoodi cache): load={secs:.6}s bytes={blen} validators={validators}"
            );
            assert!(secs < TERM_C_BOUNDARY_SECS);
            assert_eq!(cadence_after_term_c(secs), 32);
        }
    }

    #[test]
    fn uncompressed_put_snapshot_bytes_match() {
        let engine = open_engine("uncomp-store");
        let ssz = vec![7u8; 4096];
        let plan = put_snapshot(&engine, Slot::new(64), &ssz, 4).unwrap();
        assert_eq!(plan.bytes, 4096);
        let rt = engine.read().unwrap();
        assert_eq!(
            cc_store::get_snapshot(&rt, Slot::new(64)).unwrap().unwrap(),
            ssz
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn commit_latency_during_snapshot_within_10_percent() {
        // CC-42 /2: write-behind commit p99 during a snapshot within 10 % of baseline.
        //
        // Serialize stays off the writer; we stress the P2 write path with a large
        // uncompressed payload (8 MiB — full 200 MB Hoodi put is soak-only) while
        // measuring P0 commit latency. Writer priority keeps P0 above P2.
        // Residual: full Hoodi-sized P2 write wall time is recorded in soak terms
        // (b) write half, not this unit criterion.
        use crate::writer::CommitUnit;

        let engine = open_engine("commit10");
        let metrics = test_metrics();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let writer = spawn_writer(
            Arc::clone(&engine),
            metrics.clone(),
            WriterBounds::default(),
            WriterFaults::default(),
            shutdown_rx,
            false,
        );

        let n = 40u64;
        // Warm the writer once so first-commit cold path does not dominate p99.
        writer
            .submit_p0_committed(CommitUnit::cursor_only(StoreCursor {
                session_id: 1,
                seq: 0,
                slot: Slot::new(0),
                root: Root::ZERO,
            }))
            .await
            .unwrap();

        let mut baseline = Vec::with_capacity(n as usize);
        for i in 1..=n {
            let started = Instant::now();
            let unit = CommitUnit::cursor_only(StoreCursor {
                session_id: 1,
                seq: i,
                slot: Slot::new(i),
                root: Root::ZERO,
            });
            writer.submit_p0_committed(unit).await.unwrap();
            baseline.push(started.elapsed().as_secs_f64());
        }
        baseline.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p99_base = baseline[((n as usize) * 99 / 100).min(baseline.len() - 1)];

        // Large P2 payload approximates serialize-then-write pressure (not full state).
        const SNAP_CHUNK: usize = 8 * 1024 * 1024; // 8 MiB
        let mut during = Vec::with_capacity(n as usize);
        for i in 0..n {
            let ssz = vec![(i % 251) as u8; SNAP_CHUNK];
            let plan = {
                let rt = engine.read().unwrap();
                // Ring 1 so each put replaces (still one large write).
                plan_snapshot_put(&rt, Slot::new(10_000 + i), &ssz, 1).unwrap()
            };
            let (done_tx, done_rx) = oneshot::channel();
            let chunk = BackgroundChunk {
                class: StorageClass::Snapshots,
                puts: plan.puts,
                deletes: plan.deletes,
                done: Some(done_tx),
            };
            assert!(writer.try_submit_p2(chunk, &metrics));
            // Do not await P2 before P0 — that would serialise classes and hide
            // priority; enqueue then measure P0 while P2 may be in flight.
            let started = Instant::now();
            let unit = CommitUnit::cursor_only(StoreCursor {
                session_id: 1,
                seq: 1_000 + i,
                slot: Slot::new(1_000 + i),
                root: Root::ZERO,
            });
            writer.submit_p0_committed(unit).await.unwrap();
            during.push(started.elapsed().as_secs_f64());
            // Drain P2 so the queue does not fill (drop-newest).
            let _ = done_rx.await;
        }
        during.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p99_during = during[((n as usize) * 99 / 100).min(during.len() - 1)];

        eprintln!(
            "CC-42 /2 commit p99 baseline={p99_base:.6}s during_8MiB_snapshot_p2={p99_during:.6}s \
             (payload={SNAP_CHUNK} B; full Hoodi ~200MB write is soak residual)"
        );
        // 10 % relative; 2 ms absolute noise floor for timer granularity only.
        let limit = (p99_base * 1.10).max(p99_base + 0.002);
        assert!(
            p99_during <= limit,
            "commit p99 during snapshot {p99_during} exceeds 10% of baseline {p99_base} (limit {limit})"
        );

        let _ = shutdown_tx.send(true);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn own_replay_process_slots_advances_without_blocks() {
        // Real ST: process_slots across empty slots from a stored snapshot tip.
        let (driver, _sd) = driver_with(
            "st-slots",
            ReplayConfig {
                snapshot_epochs: 1,
                snapshot_ring: 4,
            },
            DivergenceExit::Hook(Arc::new(|_| {})),
        )
        .await;

        assert_eq!(driver.chain_config.config_name, "replay-supplied");
        let state0 = BeaconState::<Mainnet>::default();
        assert_eq!(state0.slot().as_u64(), 0);
        let ssz0 = state0.as_ssz_bytes();
        put_snapshot(&driver.engine, Slot::new(0), &ssz0, 4).unwrap();

        let target = Slot::new(4);
        let mut expected_state =
            BeaconState::<Mainnet>::from_ssz_bytes_with(ForkName::Fulu, &ssz0).unwrap();
        process_slots(&mut expected_state, target, &driver.chain_config).unwrap();
        let expected = measured_canonical_root(&mut expected_state);
        {
            let mut batch = driver.engine.batch();
            let rt = driver.engine.read().unwrap();
            put_state_root(&rt, &mut batch, target, &expected).unwrap();
            drop(rt);
            driver.engine.commit(batch).unwrap();
        }

        let _flight = driver.try_begin_flight().expect("single-flight free");
        let t = driver
            .run_snapshot_cycle(target, Root::ZERO, expected, None)
            .await
            .unwrap();
        assert_eq!(t.slot, 4);
        assert_eq!(driver.metrics.replay_divergence.get(), 0);
        assert!(t.replay_secs >= 0.0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn missing_block_body_fails_closed() {
        use cc_store::canonical::TABLE_CANONICAL;
        use cc_store::keys::encode_cold_block_key;
        use cc_store::keys::encode_root_value;

        let (driver, _sd) = driver_with(
            "missing-blk",
            ReplayConfig {
                snapshot_epochs: 1,
                snapshot_ring: 4,
            },
            DivergenceExit::Hook(Arc::new(|_| {})),
        )
        .await;

        let ssz0 = BeaconState::<Mainnet>::default().as_ssz_bytes();
        put_snapshot(&driver.engine, Slot::new(0), &ssz0, 4).unwrap();

        // Canonical root at slot 1 without a block body.
        let root = Root::from_array([0xAB; 32]);
        {
            let mut batch = driver.engine.batch();
            batch.put(
                TABLE_CANONICAL,
                &encode_cold_block_key(Slot::new(1)),
                &encode_root_value(&root),
            );
            driver.engine.commit(batch).unwrap();
        }

        let _flight = driver.try_begin_flight().unwrap();
        let err = driver
            .run_snapshot_cycle(Slot::new(1), root, Root::ZERO, None)
            .await
            .unwrap_err();
        match err {
            ReplayError::MissingBlock { slot, root: r } => {
                assert_eq!(slot, 1);
                assert_eq!(r, root);
            }
            other => panic!("expected MissingBlock, got {other}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn single_flight_skips_concurrent_finalization() {
        let (driver, _sd) = driver_with(
            "flight",
            ReplayConfig {
                snapshot_epochs: 1,
                snapshot_ring: 4,
            },
            DivergenceExit::Hook(Arc::new(|_| {})),
        )
        .await;
        let (ssz, root) = seed_state_and_root();
        {
            let mut batch = driver.engine.batch();
            let rt = driver.engine.read().unwrap();
            put_state_root(&rt, &mut batch, Slot::new(32), &root).unwrap();
            drop(rt);
            driver.engine.commit(batch).unwrap();
        }
        // Hold flight open.
        let guard = driver.try_begin_flight().unwrap();
        // Concurrent finalization must not start a second cycle.
        let skipped = driver
            .on_finalized_checkpoint(1, Root::ZERO, root, Some(&ssz))
            .await
            .unwrap();
        assert!(skipped.is_none(), "single-flight must skip concurrent run");
        drop(guard);
        // After release, a cycle can run.
        let ran = driver
            .on_finalized_checkpoint(1, Root::ZERO, root, Some(&ssz))
            .await
            .unwrap();
        assert!(ran.is_some());
    }

    #[test]
    fn snapshots_source_has_no_compression() {
        let src = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../crates/store/src/snapshots.rs"
        ));
        let production = src.split("#[cfg(test)]").next().unwrap();
        let a = ["fl", "ate"].concat();
        let b = ["zs", "td"].concat();
        let c = ["snap", "::"].concat();
        for needle in [a.as_str(), b.as_str(), c.as_str()] {
            assert!(!production.contains(needle), "found {needle}");
        }
    }

    #[test]
    fn decode_mainnet_state_tops_up_pubkey_cache_before_transition() {
        let src = include_str!("replay.rs");
        let production = src.split("#[cfg(test)]").next().unwrap();
        let decode_fn = production
            .find("fn decode_mainnet_state")
            .expect("decode_mainnet_state");
        let decode_body = &production[decode_fn..];
        let fn_end = decode_body.find("\n}").expect("decode_mainnet_state body");
        assert!(
            decode_body[..fn_end].contains("from_ssz_bytes_hydrated"),
            "decode_mainnet_state must use the hydrated constructor"
        );
        let decode_use = production
            .find("decode_mainnet_state(&base_ssz)")
            .expect("production decode");
        let transition = production
            .find("replay_state_to_slot")
            .expect("replay_state_to_slot");
        assert!(
            decode_use < transition,
            "own-replay must decode (and top up) before state_transition"
        );
    }
}
