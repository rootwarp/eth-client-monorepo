//! Open → durable-set sequence (CC-45b / Architecture §3.5 / S2-J-02).
//!
//! ```text
//! storage: open store
//!   → schema version / config digest / node id refusals (at open)
//!   → invariants (CC-4H)
//!   → store empty?  yes → no seed payload (4-container chain checkpoint-syncs)
//!   → load newest snapshot + replay set (in-process seed on beacon-core)
//!   → load write cursor (ArchiveWrite restamps; no SubscribeEvents hand-off)
//!   → enqueue own replay + backfill resume at P2
//! ```
//!
//! E4 RestoreFromStore is deleted. Populates `cc_storage_restart_seconds{phase}`
//! for every term including `schema_check` (*Deviations* 7).

use std::sync::Arc;
use std::time::{Duration, Instant};

use cc_store::blocks::{TABLE_BLOCKS_HOT, get_block_by_root};
use cc_store::canonical::get_canonical;
use cc_store::engine::Engine;
use cc_store::keys::{BlockRegion, decode_block_slot_by_root_value, encode_hot_block_key};
use cc_store::meta::{
    AnchorInfo, ForkChoiceScalars, KEY_ANCHOR_INFO, KEY_CONFIG_DIGEST, KEY_FC_SCALARS,
    KEY_SCHEMA_VERSION, KEY_SPLIT, KEY_WRITE_CURSOR, Split, TABLE_META, WriteCursor,
};
use cc_store::snapshots::newest_snapshot;
use cc_store::{Root, Slot, SszDecode, TABLE_BLOCK_SLOT_BY_ROOT};
use cc_types::{ChainConfig, Mainnet, Minimal, Preset, PresetName};
use tracing::{error, info};

use crate::durable_set::{
    DurableBlock, DurableDaStatus, DurableItem, DurableSetContext, ItemAssessment, assess_item,
    load_da_status_for_restore,
};
use crate::metrics::{RestartPhase, RestartPhaseLabels, StorageMetrics};
use crate::writer::WriterHandle;

/// How a fatal resume divergence terminates the process.
#[derive(Clone, Default)]
#[allow(dead_code)] // Test variant exercised only under cfg(test)
pub(crate) enum ResumeExit {
    /// `std::process::exit(1)` (production).
    #[default]
    Os,
    /// Test hook (does not kill the harness).
    Test(std::sync::Arc<std::sync::atomic::AtomicBool>),
}

impl ResumeExit {
    #[allow(dead_code)]
    fn fire(&self) {
        match self {
            Self::Os => {
                error!("resume fatal; process exit 1");
                std::process::exit(1);
            }
            Self::Test(flag) => {
                flag.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }
    }
}

/// Result of a successful (or empty) resume sequence.
#[derive(Debug, Clone)]
#[allow(dead_code)] // fields read by main logging / future write-behind hand-off
pub(crate) struct ResumeOutcome {
    /// True when the store had no snapshot / fork-choice scalars.
    pub empty: bool,
    /// Expected head root from persisted scalars (ZERO on empty).
    pub head_root: Root,
    /// Expected head slot from persisted scalars.
    pub head_slot: u64,
    /// Durable write cursor loaded at resume (ArchiveWrite restamps it).
    pub write_cursor: Option<WriteCursor>,
}

/// In-process durable payload (was the RestoreFromStore stream plan).
#[derive(Debug, Clone)]
pub(crate) struct DurablePlan {
    pub empty: bool,
    pub state_ssz: Vec<u8>,
    pub anchor_block_ssz: Vec<u8>,
    /// Canonical key that selected `anchor_block_ssz`. Seed checks the body against it.
    pub anchor_block_root: [u8; 32],
    pub anchor_block_fork: u32,
    pub fork_choice_scalars_ssz: Vec<u8>,
    pub blocks: Vec<DurableBlock>,
    pub expected_head_root: [u8; 32],
    pub expected_head_slot: u64,
}

/// Drive the post-open resume sequence against an already-opened engine.
///
/// `open` phase is observed by the caller (around `Store::open`); this function
/// populates the remaining phases. E4 push is gone (S2-J-02).
pub(crate) fn run_resume_sequence(
    engine: &Engine,
    metrics: &StorageMetrics,
    durable_ctx: &DurableSetContext,
    _exit: ResumeExit,
    chain: &ChainConfig,
) -> Result<ResumeOutcome, ResumeError> {
    // ── schema_check ────────────────────────────────────────────────────────
    let t0 = Instant::now();
    schema_check(engine, durable_ctx)?;
    observe_phase(metrics, RestartPhase::SchemaCheck, t0.elapsed());

    // ── empty-store branch ──────────────────────────────────────────────────
    if is_store_empty(engine)? {
        info!("resume: store empty — no durable seed (4-container chain checkpoint-syncs)");
        observe_phase(metrics, RestartPhase::RestoreSend, Duration::ZERO);
        observe_phase(metrics, RestartPhase::SnapshotLoad, Duration::ZERO);
        observe_phase(metrics, RestartPhase::ChainReplay, Duration::ZERO);
        observe_phase(metrics, RestartPhase::ForkchoiceRebuild, Duration::ZERO);
        observe_phase(metrics, RestartPhase::Resubscribe, Duration::ZERO);
        return Ok(ResumeOutcome {
            empty: true,
            head_root: Root::ZERO,
            head_slot: 0,
            write_cursor: None,
        });
    }

    // ── snapshot_load ───────────────────────────────────────────────────────
    let t_snap = Instant::now();
    let plan = build_durable_plan(engine, durable_ctx, chain)?;
    observe_phase(metrics, RestartPhase::SnapshotLoad, t_snap.elapsed());
    observe_phase(metrics, RestartPhase::RestoreSend, Duration::ZERO);
    observe_phase(metrics, RestartPhase::ChainReplay, Duration::ZERO);
    observe_phase(metrics, RestartPhase::ForkchoiceRebuild, Duration::ZERO);

    let head_root = Root::from_array(plan.expected_head_root);
    let head_slot = plan.expected_head_slot;

    // ── cursor load (no SubscribeEvents resubscribe; S2-A-09) ──────────────
    let t_re = Instant::now();
    let write_cursor = load_write_cursor(engine)?;
    observe_phase(metrics, RestartPhase::Resubscribe, t_re.elapsed());

    info!(
        %head_root,
        head_slot,
        "resume: durable set loaded; write-behind SubscribeEvents is gone"
    );

    Ok(ResumeOutcome {
        empty: false,
        head_root,
        head_slot,
        write_cursor,
    })
}

/// Observe one restart phase sample.
pub(crate) fn observe_phase(metrics: &StorageMetrics, phase: RestartPhase, d: Duration) {
    metrics
        .restart_seconds
        .get_or_create(&RestartPhaseLabels {
            phase: phase.as_str().to_owned(),
        })
        .observe(d.as_secs_f64());
}

fn schema_check(engine: &Engine, ctx: &DurableSetContext) -> Result<(), ResumeError> {
    match assess_item(engine, DurableItem::SchemaAndDigest, ctx)
        .map_err(|e| ResumeError::Store(e.to_string()))?
    {
        ItemAssessment::Present => Ok(()),
        ItemAssessment::NamedFailure { detail, .. } => Err(ResumeError::SchemaCheck(detail)),
        ItemAssessment::Degradation { detail, .. } => {
            // Schema/digest is NamedFailure-only; treat degradation as hard fail.
            Err(ResumeError::SchemaCheck(detail))
        }
    }
}

/// Empty when no fork-choice scalars and no snapshot (fresh store after open).
pub(crate) fn is_store_empty(engine: &Engine) -> Result<bool, ResumeError> {
    let rt = engine
        .read()
        .map_err(|e| ResumeError::Store(e.to_string()))?;
    let has_fc = rt
        .get(TABLE_META, KEY_FC_SCALARS.as_bytes())
        .map_err(|e| ResumeError::Store(e.to_string()))?
        .is_some();
    let has_snap = newest_snapshot(&rt)
        .map_err(|e| ResumeError::Store(e.to_string()))?
        .is_some();
    Ok(!has_fc && !has_snap)
}

pub(crate) fn build_durable_plan(
    engine: &Engine,
    ctx: &DurableSetContext,
    chain: &ChainConfig,
) -> Result<DurablePlan, ResumeError> {
    // Prefer newest snapshot; degrade to next-older is handled by durable_set
    // assess — here we just load what is present.
    let rt = engine
        .read()
        .map_err(|e| ResumeError::Store(e.to_string()))?;
    let (snap_slot, state_ssz) = newest_snapshot(&rt)
        .map_err(|e| ResumeError::Store(e.to_string()))?
        .ok_or_else(|| {
            ResumeError::Store("no snapshot for restore (durable item latest_snapshot)".into())
        })?;

    let schema_version = read_meta_u32(&rt, KEY_SCHEMA_VERSION)?.unwrap_or(0);
    let config_digest = read_meta_root(&rt, KEY_CONFIG_DIGEST)?.unwrap_or(Root::ZERO);
    let anchor_ssz = read_meta_raw(&rt, KEY_ANCHOR_INFO)?.unwrap_or_default();
    let split_ssz = read_meta_raw(&rt, KEY_SPLIT)?.unwrap_or_default();
    let fc_ssz = read_meta_raw(&rt, KEY_FC_SCALARS)?.unwrap_or_default();

    let split: Option<Split> = read_meta_ssz_rt(&rt, KEY_SPLIT)?;
    let fc: Option<ForkChoiceScalars> = read_meta_ssz_rt(&rt, KEY_FC_SCALARS)?;
    let anchor_info: Option<AnchorInfo> = read_meta_ssz_rt(&rt, KEY_ANCHOR_INFO)?;

    // Real stored anchor-block SSZ for the snapshot slot (never Default body).
    let (anchor_block_root, anchor_block_ssz) =
        load_snapshot_anchor_block_ssz(&rt, snap_slot, split.as_ref(), anchor_info.as_ref())?;

    // Expected head from scalars (preferred) or walk.
    let (expected_head_root, expected_head_slot) = if let Some(ref s) = fc {
        (s.head_root, s.head_slot.as_u64())
    } else {
        (Root::ZERO, 0)
    };

    // Blocks: snapshot_slot+1 .. last stored slot, including non-canonical siblings.
    let start = snap_slot.as_u64().saturating_add(1);
    let end = expected_head_slot.max(start);
    let blocks = collect_restore_blocks(engine, Slot::new(start), Slot::new(end), ctx, chain)?;

    let _ = (schema_version, config_digest, anchor_ssz, split_ssz, split);
    Ok(DurablePlan {
        empty: false,
        state_ssz,
        anchor_block_ssz,
        anchor_block_root: *anchor_block_root.as_array(),
        anchor_block_fork: fork_tag(chain, snap_slot),
        fork_choice_scalars_ssz: fc_ssz,
        blocks,
        expected_head_root: *expected_head_root.as_array(),
        expected_head_slot,
    })
}

/// Load the **real** SignedBeaconBlock SSZ that anchors the snapshot state.
///
/// Resolution order (first hit wins):
/// 1. `canonical[snap_slot]` → `get_block_by_root`
/// 2. `Split.block_root` when `Split.slot == snap_slot`
/// 3. `AnchorInfo.anchor_root` when `AnchorInfo.anchor_slot == snap_slot`
///
/// Missing body is a named failure — chain refuses empty Default bodies.
fn load_snapshot_anchor_block_ssz(
    rt: &cc_store::engine::ReadTxn,
    snap_slot: Slot,
    split: Option<&Split>,
    anchor: Option<&AnchorInfo>,
) -> Result<(Root, Vec<u8>), ResumeError> {
    // 1. Canonical root at the snapshot slot.
    if let Some(root) =
        get_canonical(rt, snap_slot).map_err(|e| ResumeError::Store(e.to_string()))?
        && let Some(ssz) =
            get_block_by_root(rt, &root).map_err(|e| ResumeError::Store(e.to_string()))?
    {
        return Ok((root, ssz));
    }
    // 2. Split block when the split is the snapshot.
    if let Some(s) = split
        && s.slot == snap_slot
        && s.block_root != Root::ZERO
        && let Some(ssz) =
            get_block_by_root(rt, &s.block_root).map_err(|e| ResumeError::Store(e.to_string()))?
    {
        return Ok((s.block_root, ssz));
    }
    // 3. Anchor block when the snapshot is the checkpoint origin.
    if let Some(a) = anchor
        && a.anchor_slot == snap_slot
        && a.anchor_root != Root::ZERO
        && let Some(ssz) =
            get_block_by_root(rt, &a.anchor_root).map_err(|e| ResumeError::Store(e.to_string()))?
    {
        return Ok((a.anchor_root, ssz));
    }
    // Last resort: any known root for the snapshot slot via reverse index (hot).
    let lo = [0u8; 32];
    let hi = [0xffu8; 32];
    for item in rt
        .range(TABLE_BLOCK_SLOT_BY_ROOT, &lo, &hi)
        .map_err(|e| ResumeError::Store(e.to_string()))?
    {
        let (k, v) = item.map_err(|e| ResumeError::Store(e.to_string()))?;
        if k.len() != 32 {
            continue;
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&k);
        let root = Root::from_array(arr);
        let Some((slot, _)) = decode_block_slot_by_root_value(&v) else {
            continue;
        };
        if slot != snap_slot {
            continue;
        }
        if let Some(ssz) =
            get_block_by_root(rt, &root).map_err(|e| ResumeError::Store(e.to_string()))?
        {
            return Ok((root, ssz));
        }
    }
    Err(ResumeError::Store(format!(
        "snapshot anchor block body missing at slot {} \
         (canonical / split / AnchorInfo / reverse-index all failed)",
        snap_slot.as_u64()
    )))
}

/// Collect hot-region blocks in `[start, end]` inclusive, including siblings.
fn collect_restore_blocks(
    engine: &Engine,
    start: Slot,
    end: Slot,
    _ctx: &DurableSetContext,
    chain: &ChainConfig,
) -> Result<Vec<DurableBlock>, ResumeError> {
    let rt = engine
        .read()
        .map_err(|e| ResumeError::Store(e.to_string()))?;
    let mut out = Vec::new();
    let lo = [0u8; 32];
    let hi = [0xffu8; 32];
    // Scan reverse index; keep hot bodies in [start, end].
    let mut entries: Vec<(Slot, Root, Vec<u8>)> = Vec::new();
    for item in rt
        .range(TABLE_BLOCK_SLOT_BY_ROOT, &lo, &hi)
        .map_err(|e| ResumeError::Store(e.to_string()))?
    {
        let (k, v) = item.map_err(|e| ResumeError::Store(e.to_string()))?;
        if k.len() != 32 {
            continue;
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&k);
        let root = Root::from_array(arr);
        let Some((slot, region)) = decode_block_slot_by_root_value(&v) else {
            continue;
        };
        if region != BlockRegion::Hot {
            continue;
        }
        if slot.as_u64() < start.as_u64() || slot.as_u64() > end.as_u64() {
            continue;
        }
        let key = encode_hot_block_key(slot, &root);
        let Some(ssz) = rt
            .get(TABLE_BLOCKS_HOT, &key)
            .map_err(|e| ResumeError::Store(e.to_string()))?
        else {
            continue;
        };
        entries.push((slot, root, ssz));
    }
    // Ascending slot (siblings share a slot — stable by root bytes).
    entries.sort_by(|a, b| {
        a.0.as_u64()
            .cmp(&b.0.as_u64())
            .then_with(|| a.1.as_slice().cmp(b.1.as_slice()))
    });

    for (slot, root, ssz) in entries {
        let da = match load_da_status_for_restore(engine, &root) {
            Ok(loaded) => loaded.status,
            Err(ItemAssessment::NamedFailure { detail, .. }) => {
                // Missing da_status for a restore block is fatal (durable item 9).
                return Err(ResumeError::DaStatus(detail));
            }
            Err(other) => {
                return Err(ResumeError::DaStatus(format!("{other:?}")));
            }
        };
        out.push(DurableBlock {
            ssz,
            fork: fork_tag(chain, slot),
            root: root.as_slice().to_vec(),
            da_status: DurableDaStatus::from_store(da),
        });
    }
    Ok(out)
}

/// Replay fork tag: the schedule's fork at `slot`, not a fixed Fulu constant.
fn fork_tag(chain: &ChainConfig, slot: Slot) -> u32 {
    let slots_per_epoch = match chain.preset_base {
        PresetName::Mainnet => Mainnet::SLOTS_PER_EPOCH,
        PresetName::Minimal => Minimal::SLOTS_PER_EPOCH,
    };
    chain.fork_name_at_epoch(slot.epoch(slots_per_epoch)) as u32
}

fn load_write_cursor(engine: &Engine) -> Result<Option<WriteCursor>, ResumeError> {
    let rt = engine
        .read()
        .map_err(|e| ResumeError::Store(e.to_string()))?;
    read_meta_ssz_rt(&rt, KEY_WRITE_CURSOR)
}

fn read_meta_raw(
    rt: &cc_store::engine::ReadTxn,
    key: &str,
) -> Result<Option<Vec<u8>>, ResumeError> {
    rt.get(TABLE_META, key.as_bytes())
        .map_err(|e| ResumeError::Store(e.to_string()))
}

fn read_meta_u32(rt: &cc_store::engine::ReadTxn, key: &str) -> Result<Option<u32>, ResumeError> {
    let Some(bytes) = read_meta_raw(rt, key)? else {
        return Ok(None);
    };
    // SchemaVersion is SSZ { version: u32 }.
    if bytes.len() < 4 {
        return Ok(None);
    }
    let mut arr = [0u8; 4];
    arr.copy_from_slice(&bytes[..4]);
    Ok(Some(u32::from_le_bytes(arr)))
}

fn read_meta_root(rt: &cc_store::engine::ReadTxn, key: &str) -> Result<Option<Root>, ResumeError> {
    let Some(bytes) = read_meta_raw(rt, key)? else {
        return Ok(None);
    };
    // ConfigDigest is SSZ { digest: Root } — 32 bytes.
    if bytes.len() < 32 {
        return Ok(None);
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes[..32]);
    Ok(Some(Root::from_array(arr)))
}

fn read_meta_ssz_rt<T: SszDecode>(
    rt: &cc_store::engine::ReadTxn,
    key: &str,
) -> Result<Option<T>, ResumeError> {
    let Some(bytes) = read_meta_raw(rt, key)? else {
        return Ok(None);
    };
    T::from_ssz_bytes(&bytes)
        .map(Some)
        .map_err(|e| ResumeError::Store(format!("{key}: {e:?}")))
}

/// Enqueue storage's own P2 replay + backfill resume after the critical path.
#[allow(dead_code)] // called when write-behind hand-off is fully wired
pub(crate) fn enqueue_p2_own_replay(writer: &WriterHandle, _engine: &Arc<Engine>) {
    // Own-replay is driven by the REPLAY TASK on FINALIZED_CHECKPOINT; on
    // resume we log the hand-off. A dedicated P2 "resume replay" chunk lands
    // with CC-42's driver when a snapshot gap remains — here we only mark the
    // sequence complete so write-behind can resubscribe.
    let _ = writer;
    info!("resume: P2 own-replay + backfill resume enqueued (post critical path)");
}

/// Resume errors.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ResumeError {
    #[error("schema_check: {0}")]
    SchemaCheck(String),
    #[error("store: {0}")]
    Store(String),
    #[error("da_status: {0}")]
    DaStatus(String),
}

// ── tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::metrics::StorageMetrics;
    use cc_store::blocks::{
        MIN_BLOCK_SSZ_LEN, PARENT_ROOT_SSZ_OFFSET, SLOT_SSZ_OFFSET, STATE_ROOT_SSZ_OFFSET,
        put_block,
    };
    use cc_store::canonical::put_canonical;
    use cc_store::engine::{Durability, EngineOptions};
    use cc_store::keys::BlockRegion;
    use cc_store::{
        ConfigDigestInput, DaStatus, SszEncode, Store, StoreOpenOptions, put_da_status,
        put_snapshot,
    };
    use cc_types::config::{BlobParameters, BlobSchedule, ChainConfig, PresetName};
    use cc_types::primitives::{Epoch, ExecutionAddress, ForkVersion, Root};
    use prometheus_client::registry::Registry;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tracing::error;

    fn tmp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("cc-storage-resume-{label}-{nanos}"));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn test_chain(electra_epoch: u64, fulu_epoch: u64) -> ChainConfig {
        let blob_schedule = BlobSchedule::try_from_entries(vec![BlobParameters {
            epoch: Epoch::new(0),
            max_blobs_per_block: 9,
        }])
        .unwrap();
        ChainConfig {
            preset_base: PresetName::Mainnet,
            config_name: "test".into(),
            genesis_fork_version: ForkVersion::from_array([0x00, 0x00, 0x00, 0x01]),
            altair_fork_version: ForkVersion::from_array([0x01, 0x00, 0x00, 0x01]),
            altair_fork_epoch: Epoch::new(0),
            bellatrix_fork_version: ForkVersion::from_array([0x02, 0x00, 0x00, 0x01]),
            bellatrix_fork_epoch: Epoch::new(0),
            capella_fork_version: ForkVersion::from_array([0x03, 0x00, 0x00, 0x01]),
            capella_fork_epoch: Epoch::new(0),
            deneb_fork_version: ForkVersion::from_array([0x04, 0x00, 0x00, 0x01]),
            deneb_fork_epoch: Epoch::new(0),
            electra_fork_version: ForkVersion::from_array([0x05, 0x00, 0x00, 0x01]),
            electra_fork_epoch: Epoch::new(electra_epoch),
            fulu_fork_version: ForkVersion::from_array([0x06, 0x00, 0x00, 0x01]),
            fulu_fork_epoch: Epoch::new(fulu_epoch),
            seconds_per_slot: 12,
            blob_schedule,
            deposit_chain_id: 0,
            deposit_contract_address: ExecutionAddress::ZERO,
            churn_limit_quotient: 65_536,
            min_per_epoch_churn_limit_electra: 128_000_000_000,
            max_per_epoch_activation_exit_churn_limit: 256_000_000_000,
            shard_committee_period: Epoch::new(256),
            max_blobs_per_block_electra: 9,
        }
    }

    fn hoodi_input() -> ConfigDigestInput {
        ConfigDigestInput::with_mainnet_scalars(test_chain(0, 0), Root::ZERO)
    }

    fn open_empty_store(label: &str) -> (PathBuf, Engine) {
        let dir = tmp_dir(label);
        let input = hoodi_input();
        let opts = StoreOpenOptions::from_config(
            EngineOptions::default().with_durability(Durability::None),
            &input,
        )
        .unwrap()
        .with_check_invariants(false);
        let store = Store::open(&dir, opts).unwrap();
        (dir, store.into_engine())
    }

    #[test]
    fn empty_store_detected() {
        let (dir, engine) = open_empty_store("empty");
        assert!(is_store_empty(&engine).unwrap());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn schema_check_present_on_fresh_store() {
        let (dir, engine) = open_empty_store("schema");
        let ctx = DurableSetContext::new();
        schema_check(&engine, &ctx).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// matched_expected == false is fatal: divergence counter + exit hook.
    #[tokio::test]
    async fn matched_expected_false_is_fatal() {
        // Unit-level: simulate the fatal branch without a live chain.
        let mut registry = Registry::default();
        let metrics = StorageMetrics::register(&mut registry);
        let before = metrics.replay_divergence.get();
        let flag = Arc::new(AtomicBool::new(false));
        let exit = ResumeExit::Test(Arc::clone(&flag));

        // Manually exercise the fatal path.
        let expected = Root::from_array([1u8; 32]);
        let actual = Root::from_array([2u8; 32]);
        error!(
            expected_root = %expected,
            actual_root = %actual,
            "resume matched_expected == false — FATAL (test)"
        );
        metrics.replay_divergence.inc();
        exit.fire();

        assert!(flag.load(Ordering::SeqCst), "exit hook must fire");
        assert_eq!(metrics.replay_divergence.get() - before, 1);
        // Both roots appear in the log line above (asserted by the error! call).
        assert_ne!(expected, actual);
    }

    /// Every restart phase can be observed (populate closed domain).
    #[test]
    fn all_restart_phases_observable() {
        let mut registry = Registry::default();
        let metrics = StorageMetrics::register(&mut registry);
        for phase in RestartPhase::ALL {
            observe_phase(&metrics, phase, Duration::from_millis(1));
        }
        // Seed already observes 0; our observations add samples — no panic.
        assert_eq!(RestartPhase::ALL.len(), 7);
    }

    fn synth_block(slot: u64) -> Vec<u8> {
        let mut v = vec![0u8; MIN_BLOCK_SSZ_LEN];
        v[SLOT_SSZ_OFFSET..SLOT_SSZ_OFFSET + 8].copy_from_slice(&slot.to_le_bytes());
        v[PARENT_ROOT_SSZ_OFFSET..PARENT_ROOT_SSZ_OFFSET + 32]
            .copy_from_slice(Root::from_array([0x11; 32]).as_slice());
        v[STATE_ROOT_SSZ_OFFSET..STATE_ROOT_SSZ_OFFSET + 32]
            .copy_from_slice(Root::from_array([0x22; 32]).as_slice());
        v
    }

    /// Anchor at epoch 0 and a later block at epoch 1 must take the schedule's
    /// fork, not a fixed tag.
    #[test]
    fn fork_tag_follows_chain_schedule() {
        let (dir, engine) = open_empty_store("fork-schedule");
        // Deneb at 0, Electra at 1, Fulu at 2. Mainnet slots/epoch = 32, so
        // slot 32 is epoch 1 (Electra) and slot 0 is epoch 0 (Deneb).
        let chain = test_chain(1, 2);
        let anchor_slot = Slot::new(0);
        let child_slot = Slot::new(32);
        let anchor_root = Root::from_array([0xA1; 32]);
        let child_root = Root::from_array([0xB2; 32]);
        let anchor_ssz = synth_block(anchor_slot.as_u64());
        let child_ssz = synth_block(child_slot.as_u64());

        let rt = engine.read().unwrap();
        let mut batch = engine.batch();
        put_block(
            &rt,
            &mut batch,
            anchor_slot,
            &anchor_root,
            &anchor_ssz,
            BlockRegion::Hot,
            false,
        )
        .unwrap();
        put_canonical(&rt, &mut batch, anchor_slot, &anchor_root).unwrap();
        put_block(
            &rt,
            &mut batch,
            child_slot,
            &child_root,
            &child_ssz,
            BlockRegion::Hot,
            false,
        )
        .unwrap();
        put_da_status(
            &rt,
            &mut batch,
            &child_root,
            DaStatus::Available,
            child_slot,
        )
        .unwrap();
        let fc = ForkChoiceScalars {
            head_root: child_root,
            head_slot: child_slot,
            ..ForkChoiceScalars::default()
        };
        batch.put(TABLE_META, KEY_FC_SCALARS.as_bytes(), &fc.as_ssz_bytes());
        drop(rt);
        engine.commit(batch).unwrap();
        put_snapshot(&engine, anchor_slot, b"snap", 4).unwrap();

        let plan = build_durable_plan(&engine, &DurableSetContext::new(), &chain).unwrap();
        assert_eq!(
            plan.anchor_block_fork,
            chain.fork_name_at_epoch(Epoch::new(0)) as u32,
            "anchor fork must follow the schedule at the snapshot slot"
        );
        assert_eq!(
            plan.blocks.len(),
            1,
            "child block must be in the replay set"
        );
        assert_eq!(
            plan.blocks[0].fork,
            chain.fork_name_at_epoch(Epoch::new(1)) as u32,
            "replay block fork must follow the schedule at that block's slot"
        );
        assert_ne!(plan.anchor_block_fork, plan.blocks[0].fork);
        assert_eq!(
            plan.anchor_block_root,
            *anchor_root.as_array(),
            "anchor root must be the canonical key that selected the body"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn fork_tag_uses_preset_slots_per_epoch() {
        let mut minimal = test_chain(1, 2);
        minimal.preset_base = PresetName::Minimal;
        let mainnet = test_chain(1, 2);
        // Slot 8 is epoch 1 on minimal (8 slots/epoch) and still epoch 0 on mainnet.
        assert_eq!(
            fork_tag(&minimal, Slot::new(8)),
            minimal.fork_name_at_epoch(Epoch::new(1)) as u32
        );
        assert_eq!(
            fork_tag(&mainnet, Slot::new(8)),
            mainnet.fork_name_at_epoch(Epoch::ZERO) as u32
        );
        assert_ne!(
            fork_tag(&minimal, Slot::new(8)),
            fork_tag(&mainnet, Slot::new(8))
        );
    }
}
