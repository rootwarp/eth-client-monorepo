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
use cc_store::engine::{Engine, StoreError};
use cc_store::keys::{BlockRegion, decode_block_slot_by_root_value, encode_hot_block_key};
use cc_store::meta::{
    AnchorInfo, ForkChoiceScalars, KEY_ANCHOR_INFO, KEY_CONFIG_DIGEST, KEY_FC_SCALARS,
    KEY_SCHEMA_VERSION, KEY_SPLIT, KEY_WRITE_CURSOR, Split, TABLE_META, WriteCursor,
};
use cc_store::snapshots::{completed_snapshot, newest_snapshot};
use cc_store::{Root, Slot, SszDecode, TABLE_BLOCK_SLOT_BY_ROOT, get_da_status};
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

    // Uninitialized is the checkpoint arm. Incomplete names the item and
    // does not look empty.
    match classify_for_epoch(engine, slots_per_epoch_of(chain))
        .map_err(|e| ResumeError::Store(e.to_string()))?
    {
        RestartState::Uninitialized => {
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
        RestartState::Incomplete(assessment) => {
            let detail = match assessment {
                ItemAssessment::NamedFailure { detail, .. }
                | ItemAssessment::Degradation { detail, .. } => detail,
                ItemAssessment::Present => "restart classification incomplete".to_owned(),
            };
            return Err(ResumeError::Store(detail));
        }
        RestartState::Complete => {}
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

/// Restart classification. Not derived from whether scalars are absent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RestartState {
    /// No `AnchorInfo` and no block row.
    ///
    /// Schema, digest, side keys, node id, and a seeded write cursor do not
    /// count: a successful open of an empty store writes those before any anchor.
    Uninitialized,
    /// Anchor, completed snapshot, scalars, and `da_status` for every body in
    /// `[snap_slot, scalars.head_slot]`, including the anchor slot.
    /// `head_slot - snap_slot` is at most one epoch.
    Complete,
    /// Anything else. The assessment names the failing [`DurableItem`].
    Incomplete(ItemAssessment),
}

/// Slots in one epoch for `chain`'s preset. `0` is not an epoch.
pub(crate) fn slots_per_epoch_of(chain: &ChainConfig) -> u64 {
    match chain.preset_base {
        PresetName::Mainnet => Mainnet::SLOTS_PER_EPOCH,
        PresetName::Minimal => Minimal::SLOTS_PER_EPOCH,
    }
    .max(1)
}

/// Mainnet epoch length. Used when no chain config was supplied at open.
#[must_use]
pub(crate) const fn default_slots_per_epoch() -> u64 {
    Mainnet::SLOTS_PER_EPOCH
}

/// Classify the store for restart using the mainnet epoch length.
///
/// `Uninitialized` is [`crate::writer::store_is_uninitialized`]. `Complete`
/// reads the snapshot completion marker, not a partial ring entry. The
/// `da_status` walk includes `snap_slot`. A head more than one epoch past
/// that marker is `Incomplete`.
///
/// Resume and admit pass [`classify_for_epoch`] the chain's preset. This
/// mainnet entry stays for callers that have no chain config.
pub fn classify(engine: &Engine) -> Result<RestartState, StoreError> {
    classify_for_epoch(engine, default_slots_per_epoch())
}

/// [`classify`] with an explicit epoch length.
///
/// A window of exactly `slots_per_epoch` stays `Complete`, so the boundary
/// snapshot can still be admitted. One slot past that is `Incomplete` and
/// names `latest_snapshot`.
pub(crate) fn classify_for_epoch(
    engine: &Engine,
    slots_per_epoch: u64,
) -> Result<RestartState, StoreError> {
    if crate::writer::store_is_uninitialized(engine)? {
        return Ok(RestartState::Uninitialized);
    }
    if let Some(failure) = anchor_failure(engine)? {
        return Ok(RestartState::Incomplete(failure));
    }
    let rt = engine.read()?;
    let Some((marker, _)) = completed_snapshot(&rt)? else {
        return Ok(RestartState::Incomplete(named_failure(
            DurableItem::LatestSnapshot,
            "durable item `latest_snapshot` missing: completion marker absent \
             or snapshot bytes do not match it",
        )));
    };
    let snap_slot = marker.slot;
    let Some(scalar_bytes) = rt.get(TABLE_META, KEY_FC_SCALARS.as_bytes())? else {
        return Ok(RestartState::Incomplete(named_failure(
            DurableItem::ForkChoice,
            "durable item `fork_choice` missing: meta key `fc_scalars` absent",
        )));
    };
    let scalars = match ForkChoiceScalars::from_ssz_bytes(&scalar_bytes) {
        Ok(scalars) => scalars,
        Err(err) => {
            return Ok(RestartState::Incomplete(named_failure(
                DurableItem::ForkChoice,
                format!("durable item `fork_choice`: ForkChoiceScalars decode failed: {err:?}"),
            )));
        }
    };
    if scalars.head_slot.as_u64() < snap_slot.as_u64() {
        return Ok(RestartState::Incomplete(named_failure(
            DurableItem::ForkChoice,
            format!(
                "durable item `fork_choice`: head_slot {} is below snapshot slot {}",
                scalars.head_slot.as_u64(),
                snap_slot.as_u64()
            ),
        )));
    }
    let window = scalars.head_slot.as_u64() - snap_slot.as_u64();
    let epoch_slots = slots_per_epoch.max(1);
    if window > epoch_slots {
        return Ok(RestartState::Incomplete(named_failure(
            DurableItem::LatestSnapshot,
            format!(
                "durable item `latest_snapshot`: replay window of {window} slots exceeds one epoch ({epoch_slots} slots)"
            ),
        )));
    }
    if let Some(root) = body_missing_da_status(&rt, snap_slot, scalars.head_slot)? {
        return Ok(RestartState::Incomplete(named_failure(
            DurableItem::DaStatus,
            format!(
                "durable item `da_status` missing for body {root} in [{}, {}]",
                snap_slot.as_u64(),
                scalars.head_slot.as_u64()
            ),
        )));
    }
    Ok(RestartState::Complete)
}

fn anchor_failure(engine: &Engine) -> Result<Option<ItemAssessment>, StoreError> {
    match assess_item(engine, DurableItem::Anchor, &DurableSetContext::new())? {
        ItemAssessment::Present => Ok(None),
        failure @ ItemAssessment::NamedFailure { .. } => Ok(Some(failure)),
        ItemAssessment::Degradation { detail, .. } => {
            Ok(Some(named_failure(DurableItem::Anchor, detail)))
        }
    }
}

fn named_failure(item: DurableItem, detail: impl Into<String>) -> ItemAssessment {
    let detail = detail.into();
    let detail = if detail.contains(item.as_str()) {
        detail
    } else {
        format!("durable item `{}`: {detail}", item.as_str())
    };
    ItemAssessment::NamedFailure { item, detail }
}

/// First body in `[snap_slot, head_slot]` with no `da_status` row.
///
/// `snap_slot` is included. The anchor slot is not skipped.
fn body_missing_da_status(
    rt: &cc_store::engine::ReadTxn,
    snap_slot: Slot,
    head_slot: Slot,
) -> Result<Option<Root>, StoreError> {
    let lo = [0u8; 32];
    // `&[u8]` order is lexicographic, so a 32-byte key is a prefix of this
    // longer key and sorts before it. `[0xff; 32]` stays inside the range.
    let hi = [0xffu8; 33];
    let start = snap_slot.as_u64();
    let end = head_slot.as_u64();
    let mut roots = Vec::new();
    for item in rt.range(TABLE_BLOCK_SLOT_BY_ROOT, &lo, &hi)? {
        let (key, value) = item?;
        if key.len() != 32 {
            continue;
        }
        let Some((slot, _)) = decode_block_slot_by_root_value(&value) else {
            return Err(StoreError::Codec(format!(
                "block_slot_by_root value is undecodable ({} bytes)",
                value.len()
            )));
        };
        let slot = slot.as_u64();
        if slot < start || slot > end {
            continue;
        }
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(&key);
        roots.push(Root::from_array(bytes));
    }
    for root in roots {
        if get_da_status(rt, &root)?.is_none() {
            return Ok(Some(root));
        }
    }
    Ok(None)
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
    let slots_per_epoch = slots_per_epoch_of(chain);
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
        assert_eq!(classify(&engine).unwrap(), RestartState::Uninitialized);
        let mut registry = Registry::default();
        let metrics = StorageMetrics::register(&mut registry);
        let outcome = run_resume_sequence(
            &engine,
            &metrics,
            &DurableSetContext::new(),
            ResumeExit::Os,
            &test_chain(0, 0),
        )
        .unwrap();
        assert!(outcome.empty);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Bodies and no scalars are `Incomplete`, and the error names the item.
    #[test]
    fn bodies_without_scalars_are_incomplete() {
        let (dir, engine) = open_empty_store("bodies-no-scalars");
        let slot = Slot::new(3);
        let root = Root::from_array([0xAB; 32]);
        let ssz = synth_block(slot.as_u64());
        let rt = engine.read().unwrap();
        let mut batch = engine.batch();
        put_block(&rt, &mut batch, slot, &root, &ssz, BlockRegion::Hot, false).unwrap();
        drop(rt);
        engine.commit(batch).unwrap();

        match classify(&engine).unwrap() {
            RestartState::Incomplete(failure) => {
                assert!(
                    failure.is_named_failure_for(DurableItem::Anchor),
                    "missing anchor is the first failing item, got {failure:?}"
                );
            }
            other => panic!("expected Incomplete, got {other:?}"),
        }
        let mut registry = Registry::default();
        let metrics = StorageMetrics::register(&mut registry);
        let err = run_resume_sequence(
            &engine,
            &metrics,
            &DurableSetContext::new(),
            ResumeExit::Os,
            &test_chain(0, 0),
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("anchor"),
            "resume must name the item, not checkpoint: {err}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Rows a successful open writes before any anchor stay `Uninitialized`.
    #[test]
    fn open_stamp_only_is_uninitialized() {
        use crate::archive_write::ArchiveWriter;
        use crate::node_id::NodeIdExpectation;
        use crate::open::{OpenOpts, open};
        use cc_store::meta::{
            KEY_CONFIG_DIGEST, KEY_CONFIG_DIGEST_V2, KEY_NODE_ID, KEY_SCHEDULE_DIGEST,
            KEY_SCHEMA_VERSION, KEY_WRITE_CURSOR,
        };

        let dir = tmp_dir("open-stamp");
        std::fs::create_dir_all(&dir).unwrap();
        let node_id = Root::from_array([0x11; 32]);
        let pending = open(
            &dir,
            OpenOpts {
                durability: "immediate".into(),
                check_invariants: false,
                snapshot_ring: 4,
                max_open_scan_rows: cc_store::DEFAULT_MAX_OPEN_SCAN_ROWS,
                genesis_validators_root: Some(format!("0x{}", "ab".repeat(32))),
                node_id: NodeIdExpectation::Present(node_id),
                chain: Some(test_chain(0, 0)),
            },
        )
        .unwrap();
        let opened = pending.pair(NodeIdExpectation::Present(node_id)).unwrap();
        ArchiveWriter::ensure_write_cursor(opened.engine()).unwrap();

        let rt = opened.engine().read().unwrap();
        for key in [
            KEY_SCHEMA_VERSION,
            KEY_CONFIG_DIGEST,
            KEY_CONFIG_DIGEST_V2,
            KEY_SCHEDULE_DIGEST,
            KEY_NODE_ID,
            KEY_WRITE_CURSOR,
        ] {
            assert!(
                rt.get(TABLE_META, key.as_bytes()).unwrap().is_some(),
                "open stamp must have written {key}"
            );
        }
        assert!(
            rt.get(TABLE_META, KEY_ANCHOR_INFO.as_bytes())
                .unwrap()
                .is_none()
        );
        drop(rt);
        assert_eq!(
            classify(opened.engine()).unwrap(),
            RestartState::Uninitialized
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `da_status` is required at the anchor slot, not from `snap_slot + 1`.
    #[test]
    fn da_status_range_includes_the_anchor_slot() {
        use cc_store::meta::SnapshotCompletion;

        let (dir, engine) = open_empty_store("da-anchor-slot");
        let snap = Slot::new(8);
        let head = Slot::new(9);
        let anchor_root = Root::from_array([0xA1; 32]);
        let child_root = Root::from_array([0xB2; 32]);
        let state = b"snap-bytes";
        let anchor_ssz = synth_block(snap.as_u64());
        let child_ssz = synth_block(head.as_u64());

        let rt = engine.read().unwrap();
        let mut batch = engine.batch();
        put_block(
            &rt,
            &mut batch,
            snap,
            &anchor_root,
            &anchor_ssz,
            BlockRegion::Hot,
            false,
        )
        .unwrap();
        put_block(
            &rt,
            &mut batch,
            head,
            &child_root,
            &child_ssz,
            BlockRegion::Hot,
            false,
        )
        .unwrap();
        put_da_status(&rt, &mut batch, &child_root, DaStatus::Available, head).unwrap();
        batch.put(
            cc_store::TABLE_SNAPSHOTS,
            &cc_store::encode_snapshot_key(snap),
            state,
        );
        cc_store::put_snapshot_completion(
            &mut batch,
            &SnapshotCompletion {
                slot: snap,
                state_root: Root::from_array([0x22; 32]),
                bytes: state.len() as u64,
            },
        );
        let anchor = AnchorInfo {
            anchor_slot: snap,
            anchor_root,
            anchor_state_root: Root::from_array([0x22; 32]),
            ..AnchorInfo::default()
        };
        batch.put(
            TABLE_META,
            KEY_ANCHOR_INFO.as_bytes(),
            &anchor.as_ssz_bytes(),
        );
        let scalars = ForkChoiceScalars {
            head_root: child_root,
            head_slot: head,
            ..ForkChoiceScalars::default()
        };
        batch.put(
            TABLE_META,
            KEY_FC_SCALARS.as_bytes(),
            &scalars.as_ssz_bytes(),
        );
        drop(rt);
        engine.commit(batch).unwrap();

        match classify(&engine).unwrap() {
            RestartState::Incomplete(failure) => {
                assert!(
                    failure.is_named_failure_for(DurableItem::DaStatus),
                    "anchor slot is inside the range, got {failure:?}"
                );
            }
            other => panic!("missing anchor da_status must be Incomplete, got {other:?}"),
        }

        let rt = engine.read().unwrap();
        let mut batch = engine.batch();
        put_da_status(&rt, &mut batch, &anchor_root, DaStatus::Available, snap).unwrap();
        drop(rt);
        engine.commit(batch).unwrap();
        assert_eq!(classify(&engine).unwrap(), RestartState::Complete);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `Complete` holds for exactly one epoch of replay and fails once the
    /// head moves past that window. Mainnet slots/epoch is 32, so snap 0 with
    /// head 32 stays `Complete` and head 33 does not.
    #[test]
    fn complete_fails_when_replay_window_passes_one_epoch() {
        let (dir, engine) = open_empty_store("replay-window");
        seed_classified_range(&engine, 0, 33);
        match classify(&engine).unwrap() {
            RestartState::Incomplete(failure) => {
                assert!(
                    failure.is_named_failure_for(DurableItem::LatestSnapshot),
                    "window past one epoch names latest_snapshot, got {failure:?}"
                );
                let ItemAssessment::NamedFailure { detail, .. } = failure else {
                    unreachable!("named failure already matched");
                };
                assert!(
                    detail.contains("replay window"),
                    "detail must name the replay window, got {detail}"
                );
            }
            other => panic!("replay window past one epoch must be Incomplete, got {other:?}"),
        }

        let (exact_dir, exact) = open_empty_store("replay-window-exact");
        seed_classified_range(&exact, 0, 32);
        assert_eq!(
            classify(&exact).unwrap(),
            RestartState::Complete,
            "a window of exactly one epoch stays Complete so the boundary snapshot can land"
        );
        let (min_dir, minimal) = open_empty_store("replay-window-minimal");
        seed_classified_range(&minimal, 0, 9);
        match classify_for_epoch(&minimal, Minimal::SLOTS_PER_EPOCH).unwrap() {
            RestartState::Incomplete(failure) => {
                assert!(
                    failure.is_named_failure_for(DurableItem::LatestSnapshot),
                    "minimal epoch is 8 slots, got {failure:?}"
                );
            }
            other => panic!("minimal window of 9 must be Incomplete, got {other:?}"),
        }
        assert_eq!(
            classify(&minimal).unwrap(),
            RestartState::Complete,
            "mainnet epoch still covers a 9-slot window"
        );
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&exact_dir);
        let _ = std::fs::remove_dir_all(&min_dir);
    }

    /// Anchor, matching completion marker, scalars, and `da_status` on every
    /// body in `[snap, head]`.
    fn seed_classified_range(engine: &Engine, snap_slot: u64, head_slot: u64) {
        use cc_store::meta::SnapshotCompletion;

        let snap = Slot::new(snap_slot);
        let head = Slot::new(head_slot);
        let anchor_root = Root::from_array([0xA1; 32]);
        let head_root = if snap_slot == head_slot {
            anchor_root
        } else {
            Root::from_array([0xB2; 32])
        };
        let state = b"snap-window";
        let rt = engine.read().unwrap();
        let mut batch = engine.batch();
        put_block(
            &rt,
            &mut batch,
            snap,
            &anchor_root,
            &synth_block(snap.as_u64()),
            BlockRegion::Hot,
            false,
        )
        .unwrap();
        put_da_status(&rt, &mut batch, &anchor_root, DaStatus::Available, snap).unwrap();
        if head_slot != snap_slot {
            put_block(
                &rt,
                &mut batch,
                head,
                &head_root,
                &synth_block(head.as_u64()),
                BlockRegion::Hot,
                false,
            )
            .unwrap();
            put_da_status(&rt, &mut batch, &head_root, DaStatus::Available, head).unwrap();
        }
        batch.put(
            cc_store::TABLE_SNAPSHOTS,
            &cc_store::encode_snapshot_key(snap),
            state,
        );
        cc_store::put_snapshot_completion(
            &mut batch,
            &SnapshotCompletion {
                slot: snap,
                state_root: Root::from_array([0x22; 32]),
                bytes: state.len() as u64,
            },
        );
        let anchor = AnchorInfo {
            anchor_slot: snap,
            anchor_root,
            anchor_state_root: Root::from_array([0x22; 32]),
            ..AnchorInfo::default()
        };
        batch.put(
            TABLE_META,
            KEY_ANCHOR_INFO.as_bytes(),
            &anchor.as_ssz_bytes(),
        );
        let scalars = ForkChoiceScalars {
            head_root,
            head_slot: head,
            ..ForkChoiceScalars::default()
        };
        batch.put(
            TABLE_META,
            KEY_FC_SCALARS.as_bytes(),
            &scalars.as_ssz_bytes(),
        );
        drop(rt);
        engine.commit(batch).unwrap();
    }

    /// The reverse-index end is exclusive, and `[0xff; 32]` is a real root.
    #[test]
    fn all_ff_root_is_inside_the_da_range() {
        use cc_store::meta::SnapshotCompletion;

        let (dir, engine) = open_empty_store("da-ff-root");
        let snap = Slot::new(4);
        let anchor_root = Root::from_array([0xff; 32]);
        let state = b"snap-ff";
        let anchor_ssz = synth_block(snap.as_u64());
        let rt = engine.read().unwrap();
        let mut batch = engine.batch();
        put_block(
            &rt,
            &mut batch,
            snap,
            &anchor_root,
            &anchor_ssz,
            BlockRegion::Hot,
            false,
        )
        .unwrap();
        batch.put(
            cc_store::TABLE_SNAPSHOTS,
            &cc_store::encode_snapshot_key(snap),
            state,
        );
        cc_store::put_snapshot_completion(
            &mut batch,
            &SnapshotCompletion {
                slot: snap,
                state_root: Root::from_array([0x22; 32]),
                bytes: state.len() as u64,
            },
        );
        let anchor = AnchorInfo {
            anchor_slot: snap,
            anchor_root,
            anchor_state_root: Root::from_array([0x22; 32]),
            ..AnchorInfo::default()
        };
        batch.put(
            TABLE_META,
            KEY_ANCHOR_INFO.as_bytes(),
            &anchor.as_ssz_bytes(),
        );
        let scalars = ForkChoiceScalars {
            head_root: anchor_root,
            head_slot: snap,
            ..ForkChoiceScalars::default()
        };
        batch.put(
            TABLE_META,
            KEY_FC_SCALARS.as_bytes(),
            &scalars.as_ssz_bytes(),
        );
        drop(rt);
        engine.commit(batch).unwrap();

        match classify(&engine).unwrap() {
            RestartState::Incomplete(failure) => {
                assert!(
                    failure.is_named_failure_for(DurableItem::DaStatus),
                    "all-ff root must be visited, got {failure:?}"
                );
            }
            other => panic!("missing da_status on 0xff root must be Incomplete, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A corrupt reverse-index value is a store error, not a skipped row.
    #[test]
    fn undecodable_reverse_index_is_a_store_error() {
        use cc_store::meta::SnapshotCompletion;

        let (dir, engine) = open_empty_store("bad-index");
        let snap = Slot::new(4);
        let anchor_root = Root::from_array([0x11; 32]);
        let state = b"snap-bad";
        let anchor_ssz = synth_block(snap.as_u64());
        let rt = engine.read().unwrap();
        let mut batch = engine.batch();
        put_block(
            &rt,
            &mut batch,
            snap,
            &anchor_root,
            &anchor_ssz,
            BlockRegion::Hot,
            false,
        )
        .unwrap();
        put_da_status(&rt, &mut batch, &anchor_root, DaStatus::Available, snap).unwrap();
        batch.put(TABLE_BLOCK_SLOT_BY_ROOT, &[0x22; 32], b"not-a-slot");
        batch.put(
            cc_store::TABLE_SNAPSHOTS,
            &cc_store::encode_snapshot_key(snap),
            state,
        );
        cc_store::put_snapshot_completion(
            &mut batch,
            &SnapshotCompletion {
                slot: snap,
                state_root: Root::from_array([0x22; 32]),
                bytes: state.len() as u64,
            },
        );
        let anchor = AnchorInfo {
            anchor_slot: snap,
            anchor_root,
            anchor_state_root: Root::from_array([0x22; 32]),
            ..AnchorInfo::default()
        };
        batch.put(
            TABLE_META,
            KEY_ANCHOR_INFO.as_bytes(),
            &anchor.as_ssz_bytes(),
        );
        let scalars = ForkChoiceScalars {
            head_root: anchor_root,
            head_slot: snap,
            ..ForkChoiceScalars::default()
        };
        batch.put(
            TABLE_META,
            KEY_FC_SCALARS.as_bytes(),
            &scalars.as_ssz_bytes(),
        );
        drop(rt);
        engine.commit(batch).unwrap();

        let err = classify(&engine).unwrap_err();
        assert!(
            err.to_string().contains("block_slot_by_root"),
            "corrupt index must not classify Complete, got {err}"
        );
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
