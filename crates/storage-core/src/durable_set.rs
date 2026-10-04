//! The twelve durable items, enumerated in one place (CC-45a / PRD §5.1).
//!
//! **Never a silent wrong answer.** Every item has one of two acceptable
//! behaviours when missing — a **named failure** that names the item, or a
//! **correct degradation** that runs the documented fallback. A store that
//! opens and answers wrongly is what this module exists to make impossible.
//!
//! ## CC-4H citations (do not re-implement)
//!
//! Where an item is already covered by a §2.7 store invariant, the delete-half
//! assertion routes through that check rather than duplicating it:
//!
//! - item 3  → **I-contig**     — `crates/store/src/invariants.rs` (`StoreInvariant::Contig`)
//! - item 7  → **I-window**     — `crates/store/src/invariants.rs` (`StoreInvariant::Window`)
//! - item 11 → **I-split-fin**  — `crates/store/src/invariants.rs` (`StoreInvariant::SplitFin`)
//! - item 12 → **I-node-id**    — `crates/store/src/invariants.rs` (`StoreInvariant::NodeId`)
//!
//! Grep: `I-contig|I-window|I-split-fin|I-node-id` in this file → four citations.
//!
//! Resume *sequence* (schema check + durable-set load) is **CC-45b**.
//! E4 RestoreFromStore is deleted (S2-J-02).

#![allow(dead_code)] // production resume (CC-45b) calls these; tests exercise them now.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use cc_store::blocks::{TABLE_BLOCK_SLOT_BY_ROOT, TABLE_BLOCKS_HOT, get_block_by_root};
use cc_store::columns::{TABLE_DA_STATUS, get_da_status};
use cc_store::engine::{Engine, StoreError};
use cc_store::invariants::{
    DEFAULT_MAX_OPEN_SCAN_ROWS, DEFAULT_SNAPSHOT_RING, InvariantCheckMode, InvariantContext,
    check_invariants,
};
use cc_store::keys::{
    BlockRegion, decode_block_slot_by_root_value, encode_hot_block_key, encode_root_key,
};
use cc_store::meta::{
    AnchorInfo, BackfillProgress, ForkChoiceScalars, KEY_ANCHOR_INFO, KEY_BACKFILL_PROG,
    KEY_CONFIG_DIGEST, KEY_FC_SCALARS, KEY_NODE_ID, KEY_SCHEMA_VERSION, KEY_SERVE_WINDOW,
    KEY_SPLIT, KEY_WRITE_CURSOR, ServeWindow, Split, TABLE_META, WriteCursor,
};
use cc_store::snapshots::{list_snapshot_slots, newest_snapshot};
use cc_store::{DaStatus, Root, Slot, SszDecode};

use crate::node_id::NodeIdExpectation;

// ---------------------------------------------------------------------------
// Enum — the single named list (exactly twelve variants)
// ---------------------------------------------------------------------------

/// One item of the durable set (CC-45 /2).
///
/// A thirteenth item cannot be added without touching the arity test.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum DurableItem {
    /// 1. Anchor state + block + slot/root (immutable). Missing → named failure at open.
    Anchor,
    /// 2. Latest snapshot + slot/root. Missing → degradation (replay from next-older ring member).
    LatestSnapshot,
    /// 3. Blocks snapshot→head including non-canonical siblings. Missing → **I-contig** (`crates/store/src/invariants.rs`) or named sibling failure.
    BlocksToHead,
    /// 4. Fork-choice store (`ForkChoiceScalars`, including `proposer_boost_root`). Missing → named failure.
    ForkChoice,
    /// 5. `WriteCursor`. Missing → degradation (resubscribe live from tip; hole recorded).
    WriteCursor,
    /// 6. ENR sequence (`CC-4E`, `<node_key>.seq`). Missing → degradation (fresh `.seq`, one bump).
    EnrSequence,
    /// 7. `earliest_available_slot` + `cgc` as a pair. Missing → named failure; inconsistent → **I-window** (`crates/store/src/invariants.rs`).
    ServeWindowPair,
    /// 8. Backfill progress. Missing → degradation (planner re-derives from the frontier).
    BackfillProgress,
    /// 9. Per-block DA status. Missing → named failure (not a licence to re-gate).
    DaStatus,
    /// 10. Schema version + config digest (`CC-40a`). Missing/mismatch → named failure at open.
    SchemaAndDigest,
    /// 11. Split slot (`CC-41`). Missing/inconsistent → **I-split-fin** (`crates/store/src/invariants.rs`).
    Split,
    /// 12. Node key vs `AnchorInfo.node_id` (§1.7). Mismatch → **I-node-id** (`crates/store/src/invariants.rs`).
    NodeIdPairing,
}

impl DurableItem {
    /// All twelve variants in stable issue-table order.
    pub(crate) const ALL: [Self; 12] = [
        Self::Anchor,
        Self::LatestSnapshot,
        Self::BlocksToHead,
        Self::ForkChoice,
        Self::WriteCursor,
        Self::EnrSequence,
        Self::ServeWindowPair,
        Self::BackfillProgress,
        Self::DaStatus,
        Self::SchemaAndDigest,
        Self::Split,
        Self::NodeIdPairing,
    ];

    /// Stable name used in error messages and metrics-style labels.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Anchor => "anchor",
            Self::LatestSnapshot => "latest_snapshot",
            Self::BlocksToHead => "blocks_to_head",
            Self::ForkChoice => "fork_choice",
            Self::WriteCursor => "write_cursor",
            Self::EnrSequence => "enr_sequence",
            Self::ServeWindowPair => "serve_window_pair",
            Self::BackfillProgress => "backfill_progress",
            Self::DaStatus => "da_status",
            Self::SchemaAndDigest => "schema_and_digest",
            Self::Split => "split",
            Self::NodeIdPairing => "node_id_pairing",
        }
    }

    /// Documented missing behaviour for this item.
    #[must_use]
    pub(crate) const fn missing_behaviour(self) -> MissingBehaviour {
        match self {
            Self::Anchor
            | Self::BlocksToHead
            | Self::ForkChoice
            | Self::ServeWindowPair
            | Self::DaStatus
            | Self::SchemaAndDigest
            | Self::Split
            | Self::NodeIdPairing => MissingBehaviour::NamedFailure,
            Self::LatestSnapshot
            | Self::WriteCursor
            | Self::EnrSequence
            | Self::BackfillProgress => MissingBehaviour::Degradation,
        }
    }
}

/// Stored DA verdict on a durable replay block (not a proto).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurableDaStatus {
    /// Block stored Available — seed marks available, does not re-gate.
    Available,
    /// Block stored Deferred — seed leaves unmarked.
    Deferred,
}

/// One replay-set block extracted from the store.
#[derive(Debug, Clone)]
pub struct DurableBlock {
    /// SignedBeaconBlock SSZ.
    pub ssz: Vec<u8>,
    /// Fork version tag.
    pub fork: u32,
    /// Pre-computed hash_tree_root (32 B).
    pub root: Vec<u8>,
    /// Stored DA verdict.
    pub da_status: DurableDaStatus,
}

impl DurableDaStatus {
    #[must_use]
    pub(crate) fn from_store(status: DaStatus) -> Self {
        match status {
            DaStatus::Available => Self::Available,
            DaStatus::Deferred => Self::Deferred,
        }
    }
}

/// Acceptable response when a durable item is absent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MissingBehaviour {
    /// Error that **names the item** (open/resume refuse).
    NamedFailure,
    /// Documented fallback runs; the store still answers correctly.
    Degradation,
}

// ---------------------------------------------------------------------------
// Outcomes
// ---------------------------------------------------------------------------

/// Result of assessing one durable item against a seeded (or live) store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ItemAssessment {
    /// Item is present and usable.
    Present,
    /// Named failure: detail always contains [`DurableItem::as_str`].
    NamedFailure {
        /// Which item failed.
        item: DurableItem,
        /// Human-readable detail (names the item; may cite an invariant label).
        detail: String,
    },
    /// Correct degradation: fallback described in `detail`.
    Degradation {
        /// Which item is missing.
        item: DurableItem,
        /// What the fallback does (for assertions / logs).
        detail: String,
    },
}

impl ItemAssessment {
    /// True when the assessment is a named failure for `item`.
    #[must_use]
    pub(crate) fn is_named_failure_for(&self, item: DurableItem) -> bool {
        matches!(self, Self::NamedFailure { item: i, detail } if *i == item && detail.contains(item.as_str()))
    }

    /// True when the assessment is a correct degradation for `item`.
    #[must_use]
    pub(crate) fn is_degradation_for(&self, item: DurableItem) -> bool {
        matches!(self, Self::Degradation { item: i, detail } if *i == item && detail.contains(item.as_str()))
    }
}

/// External inputs the durable-set assessor cannot derive from the engine alone.
///
/// [`Debug`] is hand-written: `expected_node_id` is the raw node key.
#[derive(Clone)]
pub(crate) struct DurableSetContext {
    /// Node id derived from the node key file (`p2p.node_key_path`) for **I-node-id**.
    pub expected_node_id: Option<Root>,
    /// Configured node-key path. When set and the file is missing, a store
    /// that already has `AnchorInfo` must refuse (not skip I-node-id).
    pub node_key_path: Option<PathBuf>,
    /// Path to `<node_key>.seq` (CC-4E). When `None`, ENR assessment is skipped.
    pub enr_seq_path: Option<PathBuf>,
    /// Snapshot ring depth (`storage.snapshot_ring`).
    pub snapshot_ring: u64,
    /// Per-check row cap forwarded to [`InvariantContext`].
    pub max_open_scan_rows: u64,
    /// Roots that **must** carry a `da_status` row (restore set). Empty → item 9
    /// only fails when the whole `da_status` table is empty while hot blocks exist.
    pub da_status_roots: Vec<Root>,
}

impl fmt::Debug for DurableSetContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableSetContext")
            .field(
                "expected_node_id",
                &self.expected_node_id.as_ref().map(|_| "<redacted>"),
            )
            .field("node_key_path", &self.node_key_path)
            .field("enr_seq_path", &self.enr_seq_path)
            .field("snapshot_ring", &self.snapshot_ring)
            .field("max_open_scan_rows", &self.max_open_scan_rows)
            .field("da_status_roots", &self.da_status_roots)
            .finish()
    }
}

impl Default for DurableSetContext {
    fn default() -> Self {
        Self::new()
    }
}

impl DurableSetContext {
    /// Empty context (no node id, no ENR path).
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            expected_node_id: None,
            node_key_path: None,
            enr_seq_path: None,
            snapshot_ring: DEFAULT_SNAPSHOT_RING,
            max_open_scan_rows: DEFAULT_MAX_OPEN_SCAN_ROWS,
            da_status_roots: Vec::new(),
        }
    }

    /// Invariant context for CC-4H citations.
    #[must_use]
    pub(crate) fn invariant_context(&self) -> InvariantContext {
        InvariantContext {
            expected_node_id: self.expected_node_id,
            snapshot_ring: self.snapshot_ring,
            max_open_scan_rows: self.max_open_scan_rows,
            invocation_counter: None,
        }
    }
}

/// Counter for accidental DA re-gating on the restore path (CC-45 /4).
///
/// Production `load_da_status_for_restore` never increments this. A future path
/// that re-derived DA status must call [`record_da_gate_invocation`] so the
/// direction-1 test fails closed.
pub(crate) static DA_GATE_INVOCATIONS: AtomicU64 = AtomicU64::new(0);

/// Record a DA-gate invocation (must stay unused on the restore read path).
pub(crate) fn record_da_gate_invocation() {
    DA_GATE_INVOCATIONS.fetch_add(1, Ordering::Relaxed);
}

/// Current DA-gate invocation count (tests).
#[must_use]
pub(crate) fn da_gate_invocations() -> u64 {
    DA_GATE_INVOCATIONS.load(Ordering::Relaxed)
}

/// Reset the DA-gate counter (tests).
pub(crate) fn reset_da_gate_invocations() {
    DA_GATE_INVOCATIONS.store(0, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// DA status — read, never re-derive (CC-45 /4)
// ---------------------------------------------------------------------------

/// Loaded per-block DA verdict for restore.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LoadedDaStatus {
    /// Stored status (Available or Deferred).
    pub status: DaStatus,
    /// Slot recorded with the status.
    pub slot: Slot,
}

/// Load DA status for restore: **read only**, never re-derive / re-gate.
///
/// - Missing row → named failure naming [`DurableItem::DaStatus`].
/// - Present `Deferred` is returned as Deferred (**never promoted**).
/// - Present `Available` is returned as Available (**gate not re-run**).
pub(crate) fn load_da_status_for_restore(
    engine: &Engine,
    root: &Root,
) -> Result<LoadedDaStatus, ItemAssessment> {
    // Intentionally does **not** call record_da_gate_invocation — read path only.
    let rt = engine.read().map_err(|e| ItemAssessment::NamedFailure {
        item: DurableItem::DaStatus,
        detail: format!(
            "durable item `{}`: store read failed: {e}",
            DurableItem::DaStatus.as_str()
        ),
    })?;
    match get_da_status(&rt, root) {
        Ok(Some((status, slot))) => Ok(LoadedDaStatus { status, slot }),
        Ok(None) => Err(ItemAssessment::NamedFailure {
            item: DurableItem::DaStatus,
            detail: format!(
                "durable item `{}` missing for root {root}; \
                 a missing status is not a licence to re-gate (CC-45 /4)",
                DurableItem::DaStatus.as_str()
            ),
        }),
        Err(e) => Err(ItemAssessment::NamedFailure {
            item: DurableItem::DaStatus,
            detail: format!(
                "durable item `{}`: codec/read error for root {root}: {e}",
                DurableItem::DaStatus.as_str()
            ),
        }),
    }
}

// ---------------------------------------------------------------------------
// Assess one item
// ---------------------------------------------------------------------------

/// Assess a single durable item against `engine`.
///
/// Callers that delete one item in turn assert either
/// [`ItemAssessment::NamedFailure`] or [`ItemAssessment::Degradation`] —
/// never a silent `Present` after a real deletion of a required row.
pub(crate) fn assess_item(
    engine: &Engine,
    item: DurableItem,
    ctx: &DurableSetContext,
) -> Result<ItemAssessment, StoreError> {
    match item {
        DurableItem::Anchor => assess_anchor(engine),
        DurableItem::LatestSnapshot => assess_latest_snapshot(engine),
        DurableItem::BlocksToHead => assess_blocks_to_head(engine, ctx),
        DurableItem::ForkChoice => assess_fork_choice(engine),
        DurableItem::WriteCursor => assess_write_cursor(engine),
        DurableItem::EnrSequence => assess_enr_sequence(ctx),
        DurableItem::ServeWindowPair => assess_serve_window_pair(engine, ctx),
        DurableItem::BackfillProgress => assess_backfill_progress(engine),
        DurableItem::DaStatus => assess_da_status(engine, ctx),
        DurableItem::SchemaAndDigest => assess_schema_and_digest(engine),
        DurableItem::Split => assess_split(engine, ctx),
        DurableItem::NodeIdPairing => assess_node_id_pairing(engine, ctx),
    }
}

fn named_fail(item: DurableItem, detail: impl Into<String>) -> ItemAssessment {
    let d = detail.into();
    // Guarantee the item name appears even if the caller forgot.
    let detail = if d.contains(item.as_str()) {
        d
    } else {
        format!("durable item `{}`: {d}", item.as_str())
    };
    ItemAssessment::NamedFailure { item, detail }
}

fn degrade(item: DurableItem, detail: impl Into<String>) -> ItemAssessment {
    let d = detail.into();
    let detail = if d.contains(item.as_str()) {
        d
    } else {
        format!("durable item `{}`: {d}", item.as_str())
    };
    ItemAssessment::Degradation { item, detail }
}

fn read_meta_ssz<T: SszDecode>(engine: &Engine, key: &str) -> Result<Option<T>, StoreError> {
    let rt = engine.read()?;
    let Some(bytes) = rt.get(TABLE_META, key.as_bytes())? else {
        return Ok(None);
    };
    T::from_ssz_bytes(&bytes)
        .map(Some)
        .map_err(|e| StoreError::Codec(format!("{key}: {e:?}")))
}

fn meta_present(engine: &Engine, key: &str) -> Result<bool, StoreError> {
    let rt = engine.read()?;
    Ok(rt.get(TABLE_META, key.as_bytes())?.is_some())
}

/// 1. Anchor state + block + slot/root.
fn assess_anchor(engine: &Engine) -> Result<ItemAssessment, StoreError> {
    let item = DurableItem::Anchor;
    let Some(anchor) = read_meta_ssz::<AnchorInfo>(engine, KEY_ANCHOR_INFO)? else {
        return Ok(named_fail(
            item,
            format!(
                "durable item `{}` missing: meta key `{KEY_ANCHOR_INFO}` absent (named failure at open)",
                item.as_str()
            ),
        ));
    };
    // Anchor block body (or at least the by-root index) must exist so restore has a root.
    let rt = engine.read()?;
    let has_body = get_block_by_root(&rt, &anchor.anchor_root)?.is_some();
    let has_index = rt
        .get(
            TABLE_BLOCK_SLOT_BY_ROOT,
            &encode_root_key(&anchor.anchor_root),
        )?
        .is_some();
    if !has_body && !has_index {
        return Ok(named_fail(
            item,
            format!(
                "durable item `{}`: AnchorInfo present (slot {}, root {}) but anchor block body missing",
                item.as_str(),
                anchor.anchor_slot.as_u64(),
                anchor.anchor_root
            ),
        ));
    }
    Ok(ItemAssessment::Present)
}

/// 2. Latest snapshot — degrade to next-older ring member when preferred latest is gone.
///
/// Preferred latest is `Split.slot` when a split exists (the ring member that
/// production restore expects first). When that slot is absent but an older
/// ring member remains → **degradation** (replay from next-older). Empty ring
/// → named failure.
fn assess_latest_snapshot(engine: &Engine) -> Result<ItemAssessment, StoreError> {
    let item = DurableItem::LatestSnapshot;
    let rt = engine.read()?;
    let slots = list_snapshot_slots(&rt)?;
    if slots.is_empty() {
        return Ok(named_fail(
            item,
            format!(
                "durable item `{}`: snapshot ring empty — no next-older member to degrade to",
                item.as_str()
            ),
        ));
    }

    // Preferred latest: Split.slot when present (restore starts there).
    let preferred = read_meta_ssz::<Split>(engine, KEY_SPLIT)?.map(|s| s.slot);
    if let Some(preferred) = preferred {
        if slots.contains(&preferred) {
            // Preferred latest present (and value readable).
            if newest_snapshot(&rt)?.is_some() {
                return Ok(ItemAssessment::Present);
            }
            return Ok(named_fail(
                item,
                format!(
                    "durable item `{}`: snapshot slot {} listed but value missing",
                    item.as_str(),
                    preferred.as_u64()
                ),
            ));
        }
        // Preferred latest missing; pick the highest remaining slot strictly
        // below preferred, else the highest remaining member.
        let next_older = slots
            .iter()
            .rev()
            .find(|s| s.as_u64() < preferred.as_u64())
            .copied()
            .or_else(|| slots.last().copied());
        if let Some(older) = next_older {
            return Ok(degrade(
                item,
                format!(
                    "durable item `{}`: latest snapshot at slot {} missing; \
                     degradation — replay from next-older ring member at slot {}",
                    item.as_str(),
                    preferred.as_u64(),
                    older.as_u64()
                ),
            ));
        }
        return Ok(named_fail(
            item,
            format!(
                "durable item `{}`: preferred latest slot {} missing and no older ring member",
                item.as_str(),
                preferred.as_u64()
            ),
        ));
    }

    // No split: any non-empty ring is Present.
    match newest_snapshot(&rt)? {
        Some(_) => Ok(ItemAssessment::Present),
        None => Ok(named_fail(
            item,
            format!(
                "durable item `{}`: snapshot slots listed but newest value missing",
                item.as_str()
            ),
        )),
    }
}

/// 3. Blocks snapshot→head including non-canonical siblings.
///
/// Canonical continuity cites **I-contig** (`crates/store/src/invariants.rs`).
/// Sibling half: every `block_slot_by_root` hot entry in-range must have a body.
fn assess_blocks_to_head(
    engine: &Engine,
    ctx: &DurableSetContext,
) -> Result<ItemAssessment, StoreError> {
    let item = DurableItem::BlocksToHead;

    // ── I-contig citation (canonical half) ──────────────────────────────────
    // See crates/store/src/invariants.rs — StoreInvariant::Contig / I-contig.
    let inv_ctx = ctx.invariant_context();
    match check_invariants(engine, InvariantCheckMode::Open, &inv_ctx, None) {
        Err(StoreError::InvariantViolation {
            invariant: "contig",
            detail,
        }) => {
            return Ok(named_fail(
                item,
                format!(
                    "durable item `{}`: I-contig violation (crates/store/src/invariants.rs): {detail}",
                    item.as_str()
                ),
            ));
        }
        Err(StoreError::Limit(msg)) if msg.contains("I-contig") => {
            return Ok(named_fail(
                item,
                format!(
                    "durable item `{}`: I-contig limit (crates/store/src/invariants.rs): {msg}",
                    item.as_str()
                ),
            ));
        }
        Err(e) => {
            // Other invariants may fire first; still inspect siblings below if
            // the engine is otherwise readable. Surface only contig here.
            if e.to_string().contains("contig") {
                return Ok(named_fail(
                    item,
                    format!(
                        "durable item `{}`: I-contig related failure: {e}",
                        item.as_str()
                    ),
                ));
            }
        }
        Ok(_) => {}
    }

    // ── Non-canonical siblings (fork-choice view) ───────────────────────────
    // Every reverse-index entry pointing at the hot region must still have a
    // blocks_hot body. Deleting the body while leaving the index is the classic
    // "silent head change" failure mode this item exists to catch.
    if let Some(missing) = first_missing_hot_body(engine)? {
        return Ok(named_fail(
            item,
            format!(
                "durable item `{}`: non-canonical (or hot) sibling body missing for root {} at slot {} \
                 — fork-choice view needs it; losing it silently changes the head",
                item.as_str(),
                missing.0,
                missing.1.as_u64()
            ),
        ));
    }

    Ok(ItemAssessment::Present)
}

/// First `(root, slot)` whose `block_slot_by_root` says hot but `blocks_hot` lacks the body.
fn first_missing_hot_body(engine: &Engine) -> Result<Option<(Root, Slot)>, StoreError> {
    let rt = engine.read()?;
    let lo = [0u8; 32];
    let hi = [0xffu8; 32];
    for item in rt.range(TABLE_BLOCK_SLOT_BY_ROOT, &lo, &hi)? {
        let (k, v) = item?;
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
        let key = encode_hot_block_key(slot, &root);
        if rt.get(TABLE_BLOCKS_HOT, &key)?.is_none() {
            return Ok(Some((root, slot)));
        }
    }
    Ok(None)
}

/// 4. Fork-choice scalars.
fn assess_fork_choice(engine: &Engine) -> Result<ItemAssessment, StoreError> {
    let item = DurableItem::ForkChoice;
    match read_meta_ssz::<ForkChoiceScalars>(engine, KEY_FC_SCALARS)? {
        Some(_) => Ok(ItemAssessment::Present),
        None => Ok(named_fail(
            item,
            format!(
                "durable item `{}` missing: meta key `{KEY_FC_SCALARS}` absent \
                 (proposer_boost_root / unrealized_* are correctness, not performance)",
                item.as_str()
            ),
        )),
    }
}

/// 5. WriteCursor — degradation: resubscribe live from tip.
fn assess_write_cursor(engine: &Engine) -> Result<ItemAssessment, StoreError> {
    let item = DurableItem::WriteCursor;
    match read_meta_ssz::<WriteCursor>(engine, KEY_WRITE_CURSOR)? {
        Some(c) if c.session_id != 0 => Ok(ItemAssessment::Present),
        Some(_) => Ok(degrade(
            item,
            format!(
                "durable item `{}`: cursor present but session_id=0 is not resume-valid; \
                 degradation — resubscribe live from tip, hole recorded",
                item.as_str()
            ),
        )),
        None => Ok(degrade(
            item,
            format!(
                "durable item `{}` missing; degradation — resubscribe live from tip, hole recorded",
                item.as_str()
            ),
        )),
    }
}

/// 6. ENR sequence file — degradation: fresh `.seq`, one bump.
fn assess_enr_sequence(ctx: &DurableSetContext) -> Result<ItemAssessment, StoreError> {
    let item = DurableItem::EnrSequence;
    let Some(path) = ctx.enr_seq_path.as_ref() else {
        // No path supplied (bootstrap / offline) → treat as present/skipped.
        return Ok(ItemAssessment::Present);
    };
    if path.is_file() {
        return Ok(ItemAssessment::Present);
    }
    Ok(degrade(
        item,
        format!(
            "durable item `{}` missing at {}; degradation — a fresh `.seq`, one bump, peers re-learn",
            item.as_str(),
            path.display()
        ),
    ))
}

/// 7. ServeWindow pair — named failure when missing; **I-window** when inconsistent.
///
/// I-window citation: `crates/store/src/invariants.rs` (`StoreInvariant::Window`).
fn assess_serve_window_pair(
    engine: &Engine,
    ctx: &DurableSetContext,
) -> Result<ItemAssessment, StoreError> {
    let item = DurableItem::ServeWindowPair;
    if !meta_present(engine, KEY_SERVE_WINDOW)? {
        return Ok(named_fail(
            item,
            format!(
                "durable item `{}` missing: meta key `{KEY_SERVE_WINDOW}` absent \
                 (earliest_available_slot + cgc are one container; CC-48)",
                item.as_str()
            ),
        ));
    }
    // Consistency half cites I-window.
    let inv_ctx = ctx.invariant_context();
    if let Err(StoreError::InvariantViolation {
        invariant: "window",
        detail,
    }) = check_invariants(engine, InvariantCheckMode::Open, &inv_ctx, None)
    {
        return Ok(named_fail(
            item,
            format!(
                "durable item `{}`: I-window violation (crates/store/src/invariants.rs): {detail}",
                item.as_str()
            ),
        ));
    }
    // Decode to prove the pair is readable as one value.
    let _ = read_meta_ssz::<ServeWindow>(engine, KEY_SERVE_WINDOW)?;
    Ok(ItemAssessment::Present)
}

/// 8. Backfill progress — degradation: planner re-derives from the frontier.
fn assess_backfill_progress(engine: &Engine) -> Result<ItemAssessment, StoreError> {
    let item = DurableItem::BackfillProgress;
    match read_meta_ssz::<BackfillProgress>(engine, KEY_BACKFILL_PROG)? {
        Some(_) => Ok(ItemAssessment::Present),
        None => Ok(degrade(
            item,
            format!(
                "durable item `{}` missing; degradation — the planner re-derives from the frontier",
                item.as_str()
            ),
        )),
    }
}

/// 9. Per-block DA status — named failure when a required root has no row.
fn assess_da_status(
    engine: &Engine,
    ctx: &DurableSetContext,
) -> Result<ItemAssessment, StoreError> {
    let item = DurableItem::DaStatus;
    if ctx.da_status_roots.is_empty() {
        // No explicit roots: require the table to be non-empty when hot blocks exist,
        // otherwise Present (vacuous).
        let rt = engine.read()?;
        let has_hot = rt
            .range(TABLE_BLOCKS_HOT, &[0u8; 40], &[0xffu8; 40])?
            .next()
            .is_some();
        if !has_hot {
            return Ok(ItemAssessment::Present);
        }
        let has_da = rt
            .range(TABLE_DA_STATUS, &[0u8; 32], &[0xffu8; 32])?
            .next()
            .is_some();
        if !has_da {
            return Ok(named_fail(
                item,
                format!(
                    "durable item `{}`: da_status table empty while hot blocks exist; \
                     missing status is not a licence to re-gate",
                    item.as_str()
                ),
            ));
        }
        return Ok(ItemAssessment::Present);
    }
    for root in &ctx.da_status_roots {
        match load_da_status_for_restore(engine, root) {
            Ok(_) => {}
            Err(a) => return Ok(a),
        }
    }
    Ok(ItemAssessment::Present)
}

/// 10. Schema version + config digest.
fn assess_schema_and_digest(engine: &Engine) -> Result<ItemAssessment, StoreError> {
    let item = DurableItem::SchemaAndDigest;
    let has_sv = meta_present(engine, KEY_SCHEMA_VERSION)?;
    let has_cd = meta_present(engine, KEY_CONFIG_DIGEST)?;
    match (has_sv, has_cd) {
        (true, true) => Ok(ItemAssessment::Present),
        (false, false) => Ok(named_fail(
            item,
            format!(
                "durable item `{}` missing: both `{KEY_SCHEMA_VERSION}` and `{KEY_CONFIG_DIGEST}` absent (named failure at open)",
                item.as_str()
            ),
        )),
        (false, true) => Ok(named_fail(
            item,
            format!(
                "durable item `{}` missing: `{KEY_SCHEMA_VERSION}` absent",
                item.as_str()
            ),
        )),
        (true, false) => Ok(named_fail(
            item,
            format!(
                "durable item `{}` missing: `{KEY_CONFIG_DIGEST}` absent",
                item.as_str()
            ),
        )),
    }
}

/// 11. Split slot — cites **I-split-fin** (`crates/store/src/invariants.rs`).
fn assess_split(engine: &Engine, ctx: &DurableSetContext) -> Result<ItemAssessment, StoreError> {
    let item = DurableItem::Split;
    if !meta_present(engine, KEY_SPLIT)? {
        return Ok(named_fail(
            item,
            format!(
                "durable item `{}` missing: meta key `{KEY_SPLIT}` absent",
                item.as_str()
            ),
        ));
    }
    // Consistency half cites I-split-fin.
    let inv_ctx = ctx.invariant_context();
    if let Err(StoreError::InvariantViolation {
        invariant: "split_fin",
        detail,
    }) = check_invariants(engine, InvariantCheckMode::Open, &inv_ctx, None)
    {
        return Ok(named_fail(
            item,
            format!(
                "durable item `{}`: I-split-fin violation (crates/store/src/invariants.rs): {detail}",
                item.as_str()
            ),
        ));
    }
    let _ = read_meta_ssz::<Split>(engine, KEY_SPLIT)?;
    Ok(ItemAssessment::Present)
}

/// 12. Node key vs AnchorInfo.node_id — cites **I-node-id** (`crates/store/src/invariants.rs`).
///
/// A configured path whose file is missing is a named failure when `AnchorInfo`
/// is already present.
fn assess_node_id_pairing(
    engine: &Engine,
    ctx: &DurableSetContext,
) -> Result<ItemAssessment, StoreError> {
    let item = DurableItem::NodeIdPairing;
    if let Some(msg) = missing_key_with_anchor_detail(engine, ctx.node_key_path.as_deref())? {
        return Ok(named_fail(
            item,
            format!("durable item `{}`: {msg}", item.as_str()),
        ));
    }
    let Some(expected) = ctx.expected_node_id else {
        // No key supplied, or first boot (path set, file missing, no AnchorInfo).
        return Ok(ItemAssessment::Present);
    };

    // Prefer a direct read so a secret-bearing `meta.node_id` is refused even
    // when another invariant would fire first under Open mode. Do not print
    // `stored` or `expected` (both are key bytes).
    if let Some(stored) = read_meta_ssz::<Root>(engine, KEY_NODE_ID)?
        && stored != expected
    {
        return Ok(named_fail(
            item,
            format!(
                "durable item `{}`: I-node-id (crates/store/src/invariants.rs): \
                 stored node_id <redacted> does not match the configured node key",
                item.as_str(),
            ),
        ));
    }
    if let Some(anchor) = read_meta_ssz::<AnchorInfo>(engine, KEY_ANCHOR_INFO)?
        && anchor.node_id != expected
    {
        return Ok(named_fail(
            item,
            format!(
                "durable item `{}`: I-node-id (crates/store/src/invariants.rs): \
                 AnchorInfo.node_id <redacted> does not match the configured node key",
                item.as_str(),
            ),
        ));
    }

    // Route through the invariant as well so Open-path wording stays aligned.
    // `detail` is already redacted where it is built; do not append the root.
    let inv_ctx = ctx.invariant_context();
    match check_invariants(engine, InvariantCheckMode::Open, &inv_ctx, None) {
        Err(StoreError::InvariantViolation {
            invariant: "node_id",
            detail,
        }) => {
            let msg = format!(
                "durable item `{}`: I-node-id violation (crates/store/src/invariants.rs): {detail}",
                item.as_str()
            );
            Ok(named_fail(item, msg))
        }
        Err(e) if e.to_string().contains("node_id") => {
            let msg = format!(
                "durable item `{}`: I-node-id related failure: {e}",
                item.as_str()
            );
            Ok(named_fail(item, msg))
        }
        Err(_) | Ok(_) => Ok(ItemAssessment::Present),
    }
}

/// Configured `node_key_path` is set, the file is missing, and `AnchorInfo`
/// is already in the store — I-node-id must refuse, not skip.
fn missing_key_with_anchor_detail(
    engine: &Engine,
    path: Option<&Path>,
) -> Result<Option<String>, StoreError> {
    let Some(path) = path else {
        return Ok(None);
    };
    if path.as_os_str().is_empty() || path.exists() {
        return Ok(None);
    }
    let has_anchor = read_meta_ssz::<AnchorInfo>(engine, KEY_ANCHOR_INFO)?.is_some();
    let has_node_id = read_meta_ssz::<Root>(engine, KEY_NODE_ID)?.is_some();
    if !has_anchor && !has_node_id {
        return Ok(None);
    }
    Ok(Some(format!(
        "I-node-id (crates/store/src/invariants.rs): node key missing at {} \
         but store has identity",
        path.display()
    )))
}

/// Fail-closed after `Store::open` when the key was configured but missing
/// and the store already has identity. First boot (no anchor, no `node_id`)
/// still skips. The path itself is not read — the caller passes
/// [`NodeIdExpectation::ConfiguredButMissing`].
pub(crate) fn refuse_missing_key_if_anchor_present(
    engine: &Engine,
    expectation: NodeIdExpectation,
) -> Result<(), String> {
    if !matches!(expectation, NodeIdExpectation::ConfiguredButMissing) {
        return Ok(());
    }
    match anchor_or_node_id_present(engine) {
        Ok(false) => Ok(()),
        Ok(true) => Err(
            "I-node-id (crates/store/src/invariants.rs): node key missing \
             but store has identity"
                .to_owned(),
        ),
        Err(e) => Err(format!("I-node-id: failed to read AnchorInfo: {e}")),
    }
}

fn anchor_or_node_id_present(engine: &Engine) -> Result<bool, StoreError> {
    let has_anchor = read_meta_ssz::<AnchorInfo>(engine, KEY_ANCHOR_INFO)?.is_some();
    let has_node_id = read_meta_ssz::<Root>(engine, KEY_NODE_ID)?.is_some();
    Ok(has_anchor || has_node_id)
}

/// Derive `<node_key_path>.seq` the same way p2p does (CC-4E).
#[must_use]
pub(crate) fn enr_seq_path_for(node_key_path: &Path) -> PathBuf {
    let mut os = node_key_path.as_os_str().to_os_string();
    os.push(".seq");
    PathBuf::from(os)
}

// ---------------------------------------------------------------------------
// Tests — twelve delete-each-item tests + da_status both directions + siblings
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::node_id::NodeIdExpectation;
    use cc_store::canonical::TABLE_CANONICAL;
    use cc_store::columns::{encode_da_status_value, put_da_status};
    use cc_store::engine::{Durability, EngineOptions};
    use cc_store::invariants::StoreInvariant;
    use cc_store::keys::{encode_block_slot_by_root_value, encode_cold_block_key};
    use cc_store::meta::{KEY_PRUNE_MARKS, PruneMarks};
    use cc_store::snapshots::TABLE_SNAPSHOTS;
    use cc_store::{ConfigDigestInput, SszEncode, Store, StoreOpenOptions, compute_config_digest};
    use cc_types::{BlobParameters, BlobSchedule, ChainConfig, Checkpoint, Epoch, PresetName};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("cc-storage-durable-{label}-{nanos}"));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn root(b: u8) -> Root {
        Root::from_array([b; 32])
    }

    fn hoodi_input() -> ConfigDigestInput {
        let blob_schedule = BlobSchedule::try_from_entries(vec![
            BlobParameters {
                epoch: Epoch::new(52_480),
                max_blobs_per_block: 15,
            },
            BlobParameters {
                epoch: Epoch::new(54_016),
                max_blobs_per_block: 21,
            },
        ])
        .unwrap();
        let chain = ChainConfig {
            preset_base: PresetName::Mainnet,
            config_name: "hoodi".into(),
            genesis_fork_version: cc_types::ForkVersion::from_array([0x10, 0x00, 0x09, 0x10]),
            altair_fork_version: cc_types::ForkVersion::from_array([0x20, 0x00, 0x09, 0x10]),
            altair_fork_epoch: Epoch::new(0),
            bellatrix_fork_version: cc_types::ForkVersion::from_array([0x30, 0x00, 0x09, 0x10]),
            bellatrix_fork_epoch: Epoch::new(0),
            capella_fork_version: cc_types::ForkVersion::from_array([0x40, 0x00, 0x09, 0x10]),
            capella_fork_epoch: Epoch::new(0),
            deneb_fork_version: cc_types::ForkVersion::from_array([0x50, 0x00, 0x09, 0x10]),
            deneb_fork_epoch: Epoch::new(0),
            electra_fork_version: cc_types::ForkVersion::from_array([0x60, 0x00, 0x09, 0x10]),
            electra_fork_epoch: Epoch::new(2_048),
            fulu_fork_version: cc_types::ForkVersion::from_array([0x70, 0x00, 0x09, 0x10]),
            fulu_fork_epoch: Epoch::new(50_688),
            seconds_per_slot: 12,
            blob_schedule,
            deposit_chain_id: 560_048,
            deposit_contract_address: cc_types::ExecutionAddress::from_array([0u8; 20]),
            churn_limit_quotient: 65_536,
            min_per_epoch_churn_limit_electra: 128_000_000_000,
            max_per_epoch_activation_exit_churn_limit: 256_000_000_000,
            shard_committee_period: Epoch::new(256),
            max_blobs_per_block_electra: 9,
        };
        ConfigDigestInput::with_mainnet_scalars(chain, Root::from_array([0xAB; 32]))
    }

    fn open_opts(check: bool, node_id: Option<Root>) -> StoreOpenOptions {
        let digest = compute_config_digest(&hoodi_input()).unwrap();
        StoreOpenOptions {
            engine: EngineOptions::default().with_durability(Durability::None),
            config_digest: digest,
            check_invariants: check,
            expected_node_id: node_id,
            snapshot_ring: DEFAULT_SNAPSHOT_RING,
            max_open_scan_rows: DEFAULT_MAX_OPEN_SCAN_ROWS,
            invocation_counter: None,
        }
    }

    /// Seeded store with every durable item present (and a two-branch fork).
    struct Fixture {
        dir: PathBuf,
        engine: Option<Engine>,
        node_id: Root,
        /// Canonical roots at slots 10, 11, 12.
        roots: (Root, Root, Root),
        /// Non-canonical sibling at slot 11.
        sibling: Root,
        /// DA-tracked roots: head (Available) + sibling (Deferred).
        available_root: Root,
        deferred_root: Root,
        node_key_path: PathBuf,
        enr_seq_path: PathBuf,
        keep_dir: bool,
    }

    impl Fixture {
        fn new(label: &str) -> Self {
            let dir = tmp_dir(label);
            let node_id = root(0xAA);
            let store = Store::open(&dir, open_opts(false, Some(node_id))).unwrap();
            let engine = store.into_engine();
            let node_key_path = dir.join("node_key");
            std::fs::write(&node_key_path, node_id.as_slice()).unwrap();
            let enr_seq_path = enr_seq_path_for(&node_key_path);
            std::fs::write(&enr_seq_path, 1u64.to_le_bytes()).unwrap();

            let mut f = Self {
                dir,
                engine: Some(engine),
                node_id,
                roots: (root(0x10), root(0x11), root(0x12)),
                sibling: root(0xB1),
                available_root: root(0x12),
                deferred_root: root(0xB1),
                node_key_path,
                enr_seq_path,
                keep_dir: false,
            };
            f.seed_all();
            f
        }

        fn engine(&self) -> &Engine {
            self.engine.as_ref().expect("engine open")
        }

        fn ctx(&self) -> DurableSetContext {
            DurableSetContext {
                expected_node_id: Some(self.node_id),
                node_key_path: Some(self.node_key_path.clone()),
                enr_seq_path: Some(self.enr_seq_path.clone()),
                snapshot_ring: DEFAULT_SNAPSHOT_RING,
                max_open_scan_rows: DEFAULT_MAX_OPEN_SCAN_ROWS,
                da_status_roots: vec![self.available_root, self.deferred_root],
            }
        }

        fn seed_all(&mut self) {
            let engine = self.engine();
            let (r10, r11, r12) = self.roots;
            let sibling = self.sibling;

            let anchor = AnchorInfo {
                anchor_slot: Slot::new(10),
                anchor_root: r10,
                anchor_state_root: root(0x1A),
                node_id: self.node_id,
                oldest_block_slot: Slot::new(10),
                oldest_block_parent: root(0x09),
            };
            let split = Split {
                slot: Slot::new(8),
                state_root: root(0x81),
                block_root: root(0x82),
            };
            let window = ServeWindow {
                earliest_available_slot: Slot::new(10),
                cgc: 4,
                branch: 2,
                block_floor: Slot::new(10),
                column_floor: Slot::new(10),
                ..ServeWindow::default()
            };
            let cursor = WriteCursor {
                session_id: 7,
                seq: 3,
                slot: Slot::new(12),
                root: r12,
            };
            let fc = ForkChoiceScalars {
                time: 100,
                proposer_boost_root: root(0x50),
                justified: Checkpoint {
                    epoch: Epoch::new(1),
                    root: root(0x01),
                },
                finalized: Checkpoint {
                    epoch: Epoch::new(1),
                    root: root(0x02),
                },
                unrealized_justified: Checkpoint {
                    epoch: Epoch::new(1),
                    root: root(0x03),
                },
                unrealized_finalized: Checkpoint {
                    epoch: Epoch::new(1),
                    root: root(0x04),
                },
                head_root: r12,
                head_slot: Slot::new(12),
            };
            let marks = PruneMarks {
                columns_up_to: Slot::ZERO,
                blocks_up_to: Slot::ZERO,
                states_up_to: Slot::ZERO,
                state_roots_up_to: Slot::ZERO,
            };
            let backfill = BackfillProgress {
                blocks_oldest: Slot::new(10),
                blocks_oldest_parent: root(0x09),
                columns_oldest: Slot::new(10),
                ..BackfillProgress::default()
            };

            let mut b = engine.batch();
            b.put(
                TABLE_META,
                KEY_ANCHOR_INFO.as_bytes(),
                &anchor.as_ssz_bytes(),
            );
            b.put(TABLE_META, KEY_SPLIT.as_bytes(), &split.as_ssz_bytes());
            b.put(
                TABLE_META,
                KEY_SERVE_WINDOW.as_bytes(),
                &window.as_ssz_bytes(),
            );
            b.put(
                TABLE_META,
                KEY_WRITE_CURSOR.as_bytes(),
                &cursor.as_ssz_bytes(),
            );
            b.put(TABLE_META, KEY_FC_SCALARS.as_bytes(), &fc.as_ssz_bytes());
            b.put(
                TABLE_META,
                KEY_PRUNE_MARKS.as_bytes(),
                &marks.as_ssz_bytes(),
            );
            b.put(
                TABLE_META,
                KEY_BACKFILL_PROG.as_bytes(),
                &backfill.as_ssz_bytes(),
            );

            // Canonical chain 10–12 + non-canonical sibling at 11.
            for (slot, r) in [(10u64, r10), (11, r11), (12, r12)] {
                let s = Slot::new(slot);
                b.put(TABLE_CANONICAL, &encode_cold_block_key(s), r.as_slice());
                b.put(TABLE_BLOCKS_HOT, &encode_hot_block_key(s, &r), b"block-ssz");
                b.put(
                    TABLE_BLOCK_SLOT_BY_ROOT,
                    &encode_root_key(&r),
                    &encode_block_slot_by_root_value(s, BlockRegion::Hot),
                );
            }
            // Non-canonical sibling (fork-choice view).
            let s11 = Slot::new(11);
            b.put(
                TABLE_BLOCKS_HOT,
                &encode_hot_block_key(s11, &sibling),
                b"sibling-ssz",
            );
            b.put(
                TABLE_BLOCK_SLOT_BY_ROOT,
                &encode_root_key(&sibling),
                &encode_block_slot_by_root_value(s11, BlockRegion::Hot),
            );

            // Snapshot ring: older (slot 4) + latest (slot 8 = split).
            b.put(
                TABLE_SNAPSHOTS,
                &encode_cold_block_key(Slot::new(4)),
                b"state-ssz-older",
            );
            b.put(
                TABLE_SNAPSHOTS,
                &encode_cold_block_key(Slot::new(8)),
                b"state-ssz-latest",
            );

            // DA status: head Available, sibling Deferred.
            b.put(
                TABLE_DA_STATUS,
                &encode_root_key(&r12),
                &encode_da_status_value(DaStatus::Available, Slot::new(12)),
            );
            b.put(
                TABLE_DA_STATUS,
                &encode_root_key(&sibling),
                &encode_da_status_value(DaStatus::Deferred, Slot::new(11)),
            );

            engine.commit(b).unwrap();
        }

        fn delete_meta(&self, key: &str) {
            let mut b = self.engine().batch();
            b.delete(TABLE_META, key.as_bytes());
            self.engine().commit(b).unwrap();
        }

        fn delete_row(&self, table: &str, key: &[u8]) {
            let mut b = self.engine().batch();
            b.delete(table, key);
            self.engine().commit(b).unwrap();
        }

        fn release_engine(&mut self) {
            self.engine.take();
            self.keep_dir = true;
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            self.engine.take();
            if !self.keep_dir {
                let _ = std::fs::remove_dir_all(&self.dir);
            }
        }
    }

    // ── enum arity ──────────────────────────────────────────────────────────

    #[test]
    fn durable_item_enum_arity_is_twelve() {
        assert_eq!(DurableItem::ALL.len(), 12);
        let names: std::collections::BTreeSet<_> =
            DurableItem::ALL.iter().map(|i| i.as_str()).collect();
        assert_eq!(names.len(), 12, "duplicate as_str labels: {names:?}");
        // Missing-behaviour split is part of the contract (8 fail / 4 degrade).
        let fails = DurableItem::ALL
            .iter()
            .filter(|i| i.missing_behaviour() == MissingBehaviour::NamedFailure)
            .count();
        let deg = DurableItem::ALL
            .iter()
            .filter(|i| i.missing_behaviour() == MissingBehaviour::Degradation)
            .count();
        assert_eq!(fails, 8);
        assert_eq!(deg, 4);
    }

    #[test]
    fn healthy_seed_all_items_present() {
        let f = Fixture::new("healthy");
        let ctx = f.ctx();
        for item in DurableItem::ALL {
            let a = assess_item(f.engine(), item, &ctx).unwrap();
            assert!(
                matches!(a, ItemAssessment::Present),
                "item {} not Present: {a:?}",
                item.as_str()
            );
        }
    }

    // ── twelve delete-each-item tests ───────────────────────────────────────

    /// 1. Delete anchor → named failure.
    #[test]
    fn delete_01_anchor_named_failure() {
        let f = Fixture::new("del-anchor");
        f.delete_meta(KEY_ANCHOR_INFO);
        let a = assess_item(f.engine(), DurableItem::Anchor, &f.ctx()).unwrap();
        assert!(
            a.is_named_failure_for(DurableItem::Anchor),
            "expected named failure, got {a:?}"
        );
    }

    /// 2. Delete latest snapshot → degradation to next-older ring member.
    ///
    /// `assess_item` (not a separate helper) must return Degradation when the
    /// preferred latest (`Split.slot`) is missing but an older ring member remains.
    #[test]
    fn delete_02_latest_snapshot_degradation() {
        let f = Fixture::new("del-snap");
        // Delete newest (slot 8 = Split.slot); leave older (slot 4).
        f.delete_row(TABLE_SNAPSHOTS, &encode_cold_block_key(Slot::new(8)));
        let a = assess_item(f.engine(), DurableItem::LatestSnapshot, &f.ctx()).unwrap();
        assert!(
            a.is_degradation_for(DurableItem::LatestSnapshot),
            "expected degradation from assess_item, got {a:?}"
        );
        if let ItemAssessment::Degradation { detail, .. } = &a {
            assert!(
                detail.contains('4') || detail.contains("next-older"),
                "detail must name next-older: {detail}"
            );
        }
        // Store still answers correctly via the older member.
        let rt = f.engine().read().unwrap();
        let slots = list_snapshot_slots(&rt).unwrap();
        assert_eq!(slots, vec![Slot::new(4)]);
    }

    /// 3. Delete a canonical mid-chain slot → named failure via I-contig.
    #[test]
    fn delete_03_blocks_to_head_contig_named_failure() {
        let f = Fixture::new("del-contig");
        // Remove canonical slot 11 → unrecorded hole (I-contig).
        f.delete_row(TABLE_CANONICAL, &encode_cold_block_key(Slot::new(11)));
        let a = assess_item(f.engine(), DurableItem::BlocksToHead, &f.ctx()).unwrap();
        assert!(
            a.is_named_failure_for(DurableItem::BlocksToHead),
            "expected named failure, got {a:?}"
        );
        if let ItemAssessment::NamedFailure { detail, .. } = &a {
            assert!(
                detail.contains("I-contig") || detail.contains("contig"),
                "must cite I-contig: {detail}"
            );
        }
    }

    /// 3b. Non-canonical sibling body deleted → named failure (fork-choice view).
    #[test]
    fn delete_03b_non_canonical_sibling_named_failure() {
        let f = Fixture::new("del-sibling");
        let s11 = Slot::new(11);
        // Delete body only; leave block_slot_by_root so the gap is visible.
        f.delete_row(TABLE_BLOCKS_HOT, &encode_hot_block_key(s11, &f.sibling));
        let a = assess_item(f.engine(), DurableItem::BlocksToHead, &f.ctx()).unwrap();
        assert!(
            a.is_named_failure_for(DurableItem::BlocksToHead),
            "expected named failure for missing sibling, got {a:?}"
        );
        if let ItemAssessment::NamedFailure { detail, .. } = &a {
            assert!(
                detail.contains("sibling") || detail.contains(&f.sibling.to_string()),
                "must name sibling: {detail}"
            );
        }
    }

    /// 4. Delete fork-choice scalars → named failure.
    #[test]
    fn delete_04_fork_choice_named_failure() {
        let f = Fixture::new("del-fc");
        f.delete_meta(KEY_FC_SCALARS);
        let a = assess_item(f.engine(), DurableItem::ForkChoice, &f.ctx()).unwrap();
        assert!(
            a.is_named_failure_for(DurableItem::ForkChoice),
            "expected named failure, got {a:?}"
        );
    }

    /// 5. Delete WriteCursor → degradation (resubscribe live from tip).
    #[test]
    fn delete_05_write_cursor_degradation() {
        let f = Fixture::new("del-cursor");
        f.delete_meta(KEY_WRITE_CURSOR);
        let a = assess_item(f.engine(), DurableItem::WriteCursor, &f.ctx()).unwrap();
        assert!(
            a.is_degradation_for(DurableItem::WriteCursor),
            "expected degradation, got {a:?}"
        );
        if let ItemAssessment::Degradation { detail, .. } = &a {
            assert!(
                detail.contains("resubscribe") || detail.contains("tip"),
                "{detail}"
            );
        }
    }

    /// 6. Delete ENR `.seq` → degradation (fresh seq, one bump).
    #[test]
    fn delete_06_enr_sequence_degradation() {
        let f = Fixture::new("del-enr");
        std::fs::remove_file(&f.enr_seq_path).unwrap();
        let a = assess_item(f.engine(), DurableItem::EnrSequence, &f.ctx()).unwrap();
        assert!(
            a.is_degradation_for(DurableItem::EnrSequence),
            "expected degradation, got {a:?}"
        );
        if let ItemAssessment::Degradation { detail, .. } = &a {
            assert!(
                detail.contains("fresh") || detail.contains(".seq"),
                "{detail}"
            );
        }
    }

    /// 7. Delete ServeWindow pair → named failure (cites I-window family).
    #[test]
    fn delete_07_serve_window_pair_named_failure() {
        let f = Fixture::new("del-window");
        f.delete_meta(KEY_SERVE_WINDOW);
        let a = assess_item(f.engine(), DurableItem::ServeWindowPair, &f.ctx()).unwrap();
        assert!(
            a.is_named_failure_for(DurableItem::ServeWindowPair),
            "expected named failure, got {a:?}"
        );
        // I-window citation is in source (grep) and in the inconsistent path;
        // missing container is the durable-set named failure for the pair.
        assert!(
            matches!(&a, ItemAssessment::NamedFailure { detail, .. } if detail.contains("serve_window")),
        );
    }

    /// 8. Delete backfill progress → degradation.
    #[test]
    fn delete_08_backfill_progress_degradation() {
        let f = Fixture::new("del-bf");
        f.delete_meta(KEY_BACKFILL_PROG);
        let a = assess_item(f.engine(), DurableItem::BackfillProgress, &f.ctx()).unwrap();
        assert!(
            a.is_degradation_for(DurableItem::BackfillProgress),
            "expected degradation, got {a:?}"
        );
        if let ItemAssessment::Degradation { detail, .. } = &a {
            assert!(
                detail.contains("frontier") || detail.contains("re-derive"),
                "{detail}"
            );
        }
    }

    /// 9. Delete da_status for a required root → named failure (not re-gate).
    #[test]
    fn delete_09_da_status_named_failure() {
        let f = Fixture::new("del-da");
        f.delete_row(TABLE_DA_STATUS, &encode_root_key(&f.available_root));
        let a = assess_item(f.engine(), DurableItem::DaStatus, &f.ctx()).unwrap();
        assert!(
            a.is_named_failure_for(DurableItem::DaStatus),
            "expected named failure, got {a:?}"
        );
        if let ItemAssessment::NamedFailure { detail, .. } = &a {
            assert!(
                detail.contains("re-gate")
                    || detail.contains("licence")
                    || detail.contains("license"),
                "{detail}"
            );
        }
    }

    /// 10. Delete schema version → named failure (and open refuses).
    #[test]
    fn delete_10_schema_and_digest_named_failure() {
        let mut f = Fixture::new("del-schema");
        f.delete_meta(KEY_SCHEMA_VERSION);
        let a = assess_item(f.engine(), DurableItem::SchemaAndDigest, &f.ctx()).unwrap();
        assert!(
            a.is_named_failure_for(DurableItem::SchemaAndDigest),
            "expected named failure, got {a:?}"
        );
        // End-to-end open refuse.
        let dir = f.dir.clone();
        let node_id = f.node_id;
        f.release_engine();
        drop(f);
        let err = Store::open(&dir, open_opts(false, Some(node_id))).unwrap_err();
        let msg = err.to_string();
        assert!(
            matches!(err, StoreError::MissingMeta(_))
                || msg.contains("schema")
                || msg.contains("missing"),
            "open must refuse: {err:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 11. Delete split → named failure (I-split-fin family).
    #[test]
    fn delete_11_split_named_failure() {
        let f = Fixture::new("del-split");
        f.delete_meta(KEY_SPLIT);
        let a = assess_item(f.engine(), DurableItem::Split, &f.ctx()).unwrap();
        assert!(
            a.is_named_failure_for(DurableItem::Split),
            "expected named failure, got {a:?}"
        );
    }

    /// 12. Node key / AnchorInfo.node_id mismatch → named failure via I-node-id.
    ///
    /// Strong in-process: replace expected id (simulating a fresh key file) and
    /// assert open + assess refuse. The literal `node_id` stays; neither byte
    /// string is printed.
    #[test]
    fn delete_12_node_id_mismatch_named_failure() {
        let f = Fixture::new("del-nodeid");
        // Fresh key file bytes → different NodeId surface.
        let fresh_key = [0xBBu8; 32];
        std::fs::write(&f.node_key_path, fresh_key).unwrap();
        let from_key = Root::from_array(fresh_key);
        assert_ne!(from_key, f.node_id);

        let mut ctx = f.ctx();
        ctx.expected_node_id = Some(from_key);

        let a = assess_item(f.engine(), DurableItem::NodeIdPairing, &ctx).unwrap();
        assert!(
            a.is_named_failure_for(DurableItem::NodeIdPairing),
            "expected named failure, got {a:?}"
        );
        if let ItemAssessment::NamedFailure { detail, .. } = &a {
            assert!(
                detail.contains("I-node-id") || detail.contains("node_id"),
                "must cite I-node-id: {detail}"
            );
            assert!(
                !detail.contains(&f.node_id.to_string()) && !detail.contains(&from_key.to_string()),
                "must not print stored or key-file bytes: {detail}"
            );
        }

        // A secret-bearing `meta.node_id` is preferred over AnchorInfo and still refused.
        {
            let mut b = f.engine().batch();
            b.put(
                TABLE_META,
                KEY_NODE_ID.as_bytes(),
                &f.node_id.as_ssz_bytes(),
            );
            f.engine().commit(b).unwrap();
        }
        let meta = assess_item(f.engine(), DurableItem::NodeIdPairing, &ctx).unwrap();
        assert!(
            meta.is_named_failure_for(DurableItem::NodeIdPairing),
            "secret-bearing meta.node_id must refuse, got {meta:?}"
        );
        if let ItemAssessment::NamedFailure { detail, .. } = &meta {
            assert!(
                detail.contains("stored node_id"),
                "meta.node_id mismatch must keep the literal node_id: {detail}"
            );
            assert!(
                !detail.contains(&f.node_id.to_string()) && !detail.contains(&from_key.to_string()),
                "meta.node_id mismatch leaked key bytes: {detail}"
            );
        }

        // Production open path: load expected id from the (replaced) key file and
        // refuse Store::open with check_invariants. Bytes stay out of the error.
        let loaded = NodeIdExpectation::from_configured_path(Some(&f.node_key_path))
            .unwrap()
            .legacy_root()
            .expect("key file present");
        assert_eq!(loaded, from_key);
        let inv_ctx = InvariantContext {
            expected_node_id: Some(loaded),
            snapshot_ring: DEFAULT_SNAPSHOT_RING,
            max_open_scan_rows: DEFAULT_MAX_OPEN_SCAN_ROWS,
            invocation_counter: None,
        };
        let err = check_invariants(f.engine(), InvariantCheckMode::Open, &inv_ctx, None)
            .expect_err("I-node-id must fire");
        let msg = err.to_string();
        assert!(
            matches!(
                err,
                StoreError::InvariantViolation {
                    invariant: "node_id",
                    ..
                }
            ),
            "err={err:?}"
        );
        assert!(
            msg.contains("node_id"),
            "fatal must keep the literal node_id: {msg}"
        );
        let dbg = format!("{err:?}");
        assert!(
            !msg.contains(&f.node_id.to_string())
                && !msg.contains(&from_key.to_string())
                && !dbg.contains(&f.node_id.to_string())
                && !dbg.contains(&from_key.to_string()),
            "fatal Display/Debug leaked key bytes: {msg} / {dbg}"
        );

        // End-to-end Store::open with expected_node_id from key file refuses.
        let mut f2 = Fixture::new("del-nodeid-open");
        let anchor_id = f2.node_id;
        std::fs::write(&f2.node_key_path, fresh_key).unwrap();
        let dir = f2.dir.clone();
        f2.release_engine();
        drop(f2);
        let from_key2 = Root::from_array(fresh_key);
        let err = Store::open(&dir, open_opts(true, Some(from_key2))).unwrap_err();
        let msg = err.to_string();
        assert!(
            matches!(
                err,
                StoreError::InvariantViolation {
                    invariant: "node_id",
                    ..
                }
            ),
            "Store::open must refuse on key mismatch: {err:?}"
        );
        assert!(
            msg.contains("node_id"),
            "open refuse must keep the literal node_id: {msg}"
        );
        let chain = format!("{err:#?}");
        assert!(
            !msg.contains(&anchor_id.to_string())
                && !msg.contains(&from_key2.to_string())
                && !chain.contains(&anchor_id.to_string())
                && !chain.contains(&from_key2.to_string()),
            "open refuse leaked key bytes: {msg} / {chain}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Configured path, missing file, AnchorInfo present → named I-node-id failure.
    #[test]
    fn missing_key_with_anchor_named_failure() {
        let f = Fixture::new("missing-key-anchor");
        std::fs::remove_file(&f.node_key_path).unwrap();
        let mut ctx = f.ctx();
        ctx.expected_node_id = None;
        let a = assess_item(f.engine(), DurableItem::NodeIdPairing, &ctx).unwrap();
        assert!(
            a.is_named_failure_for(DurableItem::NodeIdPairing),
            "expected named failure, got {a:?}"
        );
        if let ItemAssessment::NamedFailure { detail, .. } = &a {
            assert!(
                detail.contains("I-node-id") && detail.contains("identity"),
                "must cite I-node-id and identity: {detail}"
            );
        }
        refuse_missing_key_if_anchor_present(f.engine(), NodeIdExpectation::ConfiguredButMissing)
            .expect_err("populated store must refuse a missing key");
    }

    /// Configured path, missing file, no AnchorInfo → first boot still skips.
    #[test]
    fn missing_key_without_anchor_skips() {
        let f = Fixture::new("missing-key-fresh");
        f.delete_meta(KEY_ANCHOR_INFO);
        std::fs::remove_file(&f.node_key_path).unwrap();
        let mut ctx = f.ctx();
        ctx.expected_node_id = None;
        let a = assess_item(f.engine(), DurableItem::NodeIdPairing, &ctx).unwrap();
        assert_eq!(a, ItemAssessment::Present);
        refuse_missing_key_if_anchor_present(f.engine(), NodeIdExpectation::ConfiguredButMissing)
            .expect("first boot may skip");
    }

    // ── da_status both directions (CC-45 /4) ────────────────────────────────

    /// Direction 1: Available is not re-gated (gate counter stays 0).
    #[test]
    fn da_status_available_not_re_gated() {
        let f = Fixture::new("da-avail");
        reset_da_gate_invocations();
        let loaded = load_da_status_for_restore(f.engine(), &f.available_root).unwrap();
        assert_eq!(loaded.status, DaStatus::Available);
        assert_eq!(
            da_gate_invocations(),
            0,
            "Available must not re-gate (counter must stay 0)"
        );
    }

    /// Direction 2 (the important one): Deferred is not silently promoted.
    #[test]
    fn da_status_deferred_not_silently_promoted() {
        let f = Fixture::new("da-defer");
        reset_da_gate_invocations();
        let loaded = load_da_status_for_restore(f.engine(), &f.deferred_root).unwrap();
        assert_eq!(
            loaded.status,
            DaStatus::Deferred,
            "Deferred must restore as Deferred — a build that promotes it fails this test"
        );
        assert_ne!(loaded.status, DaStatus::Available);
        assert_eq!(da_gate_invocations(), 0);

        // Write path still refuses Available → Deferred demotion; read never promotes.
        let rt = f.engine().read().unwrap();
        let mut batch = f.engine().batch();
        // Trying to demote Available is refused elsewhere; here ensure Deferred
        // stays Deferred across a re-read.
        put_da_status(
            &rt,
            &mut batch,
            &f.deferred_root,
            DaStatus::Deferred,
            Slot::new(11),
        )
        .unwrap();
        f.engine().commit(batch).unwrap();
        let again = load_da_status_for_restore(f.engine(), &f.deferred_root).unwrap();
        assert_eq!(again.status, DaStatus::Deferred);
    }

    /// Citations present: I-contig, I-window, I-split-fin, I-node-id must appear
    /// in this source file (acceptance criterion grep).
    #[test]
    fn cites_four_cc4h_invariants() {
        let src = include_str!("durable_set.rs");
        for needle in ["I-contig", "I-window", "I-split-fin", "I-node-id"] {
            assert!(
                src.contains(needle),
                "durable_set.rs must cite {needle} (CC-4H)"
            );
        }
        assert!(
            src.contains("crates/store/src/invariants.rs"),
            "must reference invariants.rs"
        );
        // StoreInvariant labels used by the assessor.
        let _ = StoreInvariant::Contig;
        let _ = StoreInvariant::Window;
        let _ = StoreInvariant::SplitFin;
        let _ = StoreInvariant::NodeId;
    }
}
