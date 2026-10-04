//! Import path on the core thread (Architecture §7.2, ADR-P1-10, CC-27c).
//!
//! ```text
//! decode-free dedup probe → decode → root check → parent → proposer →
//! finalized descent → **block proposer BLS** (always on gossip path; H1)
//!   → **early gossip ACCEPT**  (CC-27c fast path)
//!   → DA → ST → FC → get_head → **durable commit** → pin residency → prune
//!   → snapshot → events → import result (not a second gossip verdict)
//! ```
//!
//! The pre-computed `ImportBlockRequest.root` is a **probe only**: a hit that is
//! **fully imported** (`store.blocks` **and** proto-array) skips transition.
//! If an archive handle is present, that path still persists when durable
//! rows are missing (M2). A partial (header without proto-array) falls
//! through so `on_block` can resume (SEC-4).
//!
//! # Gossip-verify fast path (CC-27c)
//!
//! Cheap gossip conditions answer the block **acceptance** before
//! `state_transition` so verdict latency p95 ≤ 100 ms is reachable. A late
//! `Reject` from the transition does **not** re-report gossip; it is an
//! application-score penalty (`import_invalid`). `Internal` never penalises.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use bytes::Bytes;
use cc_fork_choice::{
    BlockImport, ChainReorg, DeferralReason, OnBlockError, Store, get_checkpoint_block, get_head,
    on_block_with_context,
};
use cc_proto::chain::{ImportBlockRequest, ImportBlockResponse, ImportBlockVerdict};
use cc_state_transition::helpers::misc::compute_start_slot_at_epoch;
use cc_state_transition::{
    BlockError, BlockSignatureSet, BlockSignatureStrategy, GossipClass, TransitionContext,
    compute_epoch_at_slot, push_block_proposer_signature,
};
use cc_types::config::ChainConfig;
use cc_types::containers::Checkpoint;
use cc_types::preset::Preset;
use cc_types::primitives::{Root, Slot};
use cc_types::{ForkName, SignedBeaconBlock};
use ssz::Encode;
use ssz_derive::Encode as SszEncode;
use tonic::Status;
use tree_hash::TreeHash;

use crate::ArchiveWriteHandle;
use crate::da::{BlockBranchTrigger, PendingDa, PendingDaEntry, block_branch_trigger_from_signed};
use crate::epoch_context::EpochContext;
use crate::events::EventInput;
use crate::head::{HeadSnapshot, HeadSnapshotStore};
use crate::metrics::{ChainMetrics, ImportResult, ImportStage};
use crate::pending_engine::{PendingEngine, PendingEngineEntry};
use crate::tick::{GossipClock, admit_block_slot_if_within_disparity};

/// First payload byte of `BLOCK_IMPORTED` (Architecture §4.2).
///
/// Typed so the ring payload is not a bare `u8` with a silent-default reader.
/// Unknown discriminants must fail closed ([`BlockImportedPayloadVerdict::from_u8`]).
pub use cc_seam::BlockImportedVerdict as BlockImportedPayloadVerdict;

const _: () = {
    assert!(ImportBlockVerdict::Imported as u8 == BlockImportedPayloadVerdict::Imported as u8);
    assert!(ImportBlockVerdict::DeferredDa as u8 == BlockImportedPayloadVerdict::DeferredDa as u8);
};

/// First payload byte for `BLOCK_IMPORTED` after a successful import (Architecture §4.2).
pub const BLOCK_PAYLOAD_VERDICT_IMPORTED: u8 = BlockImportedPayloadVerdict::Imported as u8;
/// First payload byte for `BLOCK_IMPORTED` after `DEFERRED_DA` (same kind, different disc.).
pub const BLOCK_PAYLOAD_VERDICT_DEFERRED_DA: u8 = BlockImportedPayloadVerdict::DeferredDa as u8;

/// Machine-readable `ImportBlockResponse.reason` (P1-D/11 / S1-A-14).
///
/// The proto field stays a string for unary RPC / logs. Cross-service mapping
/// (p2p stream) must match this enum, not ad-hoc string equality.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImportReason {
    None,
    DataUnavailable,
    UnknownParent,
    FutureSlot,
    ExecutionEngineUnavailable,
    TooOld,
    NotDescendedFromFinalized,
    ProposerSigParentStateMissing,
    InternalProposerSig(String),
    Other(String),
}

impl ImportReason {
    /// Wire string written to `ImportBlockResponse.reason`.
    #[must_use]
    pub fn to_wire(&self) -> String {
        match self {
            Self::None => String::new(),
            Self::DataUnavailable => "data_unavailable".into(),
            Self::UnknownParent => "unknown_parent".into(),
            Self::FutureSlot => "future_slot".into(),
            Self::ExecutionEngineUnavailable => "execution_engine_unavailable".into(),
            Self::TooOld => "too_old".into(),
            Self::NotDescendedFromFinalized => "not_descended_from_finalized".into(),
            Self::ProposerSigParentStateMissing => "proposer_sig_parent_state_missing".into(),
            Self::InternalProposerSig(e) => format!("internal_proposer_sig: {e}"),
            Self::Other(s) => s.clone(),
        }
    }
}

/// SSZ layout matching `cc_store::meta::ForkChoiceScalars` (Architecture §2.5 / §4.2).
///
/// Defined here so `cc-chain` does not take a `cc-store` dependency (DAG forbids
/// `cc-chain → cc-store`). **Must stay field-for-field identical** to
/// `crates/store/src/meta.rs::ForkChoiceScalars`:
///
/// ```text
/// time: u64
/// proposer_boost_root: Root          // 32
/// justified: Checkpoint              // epoch:u64 ‖ root:32
/// finalized: Checkpoint
/// unrealized_justified: Checkpoint
/// unrealized_finalized: Checkpoint
/// head_root: Root                    // 32
/// head_slot: Slot                    // u64
/// ```
///
/// Fixed-part length = 8 + 32 + 4×40 + 32 + 8 = **240** bytes. A unit test pins
/// that length and the field order; storage decodes with the store type.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, SszEncode)]
pub struct ForkChoiceScalarsPayload {
    pub time: u64,
    pub proposer_boost_root: Root,
    pub justified: Checkpoint,
    pub finalized: Checkpoint,
    pub unrealized_justified: Checkpoint,
    pub unrealized_finalized: Checkpoint,
    pub head_root: Root,
    pub head_slot: Slot,
}

/// Fixed SSZ byte length of [`ForkChoiceScalarsPayload`] / store `ForkChoiceScalars`.
pub const FORK_CHOICE_SCALARS_SSZ_LEN: usize = 240;

/// Build `BLOCK_IMPORTED` payload: `[verdict_byte] ‖ SignedBeaconBlock SSZ`.
pub fn block_imported_payload(verdict: BlockImportedPayloadVerdict, block_ssz: &[u8]) -> Bytes {
    cc_seam::BlockImportedPayload::encode(verdict, block_ssz)
}

/// Fail-closed split of a `BLOCK_IMPORTED` payload. Unknown first byte is `None`.
#[must_use]
pub fn split_block_imported_payload(
    payload: &[u8],
) -> Option<(BlockImportedPayloadVerdict, &[u8])> {
    let parsed = cc_seam::BlockImportedPayload::decode(payload)?;
    Some((parsed.verdict, parsed.block_ssz))
}

fn import_response(verdict: ImportBlockVerdict, reason: &ImportReason) -> ImportBlockResponse {
    ImportBlockResponse {
        verdict: verdict as i32,
        reason: reason.to_wire(),
    }
}

/// Snapshot fork-choice scalars for a `FINALIZED_CHECKPOINT` payload (SSZ).
pub fn fork_choice_scalars_ssz<P: Preset>(
    store: &Store<P>,
    head_root: Root,
    head_slot: Slot,
) -> Bytes {
    let scalars = ForkChoiceScalarsPayload {
        time: store.time(),
        proposer_boost_root: store.proposer_boost_root(),
        justified: store.justified_checkpoint(),
        finalized: store.finalized_checkpoint(),
        unrealized_justified: store.unrealized_justified_checkpoint(),
        unrealized_finalized: store.unrealized_finalized_checkpoint(),
        head_root,
        head_slot,
    };
    Bytes::from(scalars.as_ssz_bytes())
}

fn parent_of<P: Preset>(store: &Store<P>, root: Root) -> Option<Root> {
    if let Some(node) = store.proto_array().get(&root) {
        if let Some(p_idx) = node.parent {
            return store.proto_array().nodes().get(p_idx).map(|n| n.root);
        }
        return None;
    }
    store.blocks().get(&root).map(|h| h.parent_root)
}

fn slot_of<P: Preset>(store: &Store<P>, root: Root) -> Slot {
    store
        .proto_array()
        .get(&root)
        .map(|n| n.slot)
        .or_else(|| store.blocks().get(&root).map(|h| h.slot))
        .unwrap_or(Slot::new(0))
}

/// Common-ancestor slot of a reorg (for `CHAIN_REORG` payload; Architecture §4.2).
pub fn common_ancestor_slot<P: Preset>(store: &Store<P>, old_head: Root, new_head: Root) -> Slot {
    let bound = store
        .proto_array()
        .len()
        .saturating_add(store.blocks().len())
        .saturating_add(1);
    let mut new_chain: std::collections::HashMap<Root, Slot> = std::collections::HashMap::new();
    let mut cur = new_head;
    for _ in 0..bound {
        new_chain.insert(cur, slot_of(store, cur));
        match parent_of(store, cur) {
            Some(p) if p != cur && p != Root::ZERO => cur = p,
            _ => break,
        }
    }
    let mut cur = old_head;
    for _ in 0..bound {
        if let Some(&slot) = new_chain.get(&cur) {
            return slot;
        }
        match parent_of(store, cur) {
            Some(p) if p != cur && p != Root::ZERO => cur = p,
            _ => break,
        }
    }
    Slot::new(0)
}
use crate::residency::Residency;

/// Outcome of a single import attempt (core thread).
#[derive(Debug)]
pub struct ImportOutcome {
    pub response: ImportBlockResponse,
    /// Typed reason that produced [`Self::response`].reason (stream mapping).
    pub reason: ImportReason,
    /// True when state_transition / on_block was invoked (for DUPLICATE tests).
    pub transition_invoked: bool,
    /// Cheap gossip conditions passed and early ACCEPT was (or would be) emitted.
    pub early_accept: bool,
    /// After early ACCEPT, import failed with [`GossipClass::Reject`] — late
    /// application-score penalty (`import_invalid`), **no** second gossip report.
    pub late_import_reject: bool,
    /// After early ACCEPT, import failed with [`GossipClass::Internal`] — no
    /// penalty and no REJECT.
    pub late_import_internal: bool,
    /// CC-38a block-branch fast-path trigger (template-sized only).
    ///
    /// Set when import parks on `Deferred(DataUnavailable)` with non-empty
    /// `blob_kzg_commitments`. Core fires unary `FetchBlobs` — never cells.
    pub block_branch: Option<BlockBranchTrigger>,
    /// Whether core may emit import-path fcU for this outcome.
    ///
    /// False for `Invalid` and `DeferredDa`: those calls do not durably move
    /// the head. Core also skips fcU when this process has not recorded a
    /// durable head.
    pub publish_fcu: bool,
}

/// `Invalid` and `DeferredDa` do not move canonical, so they do not fcU.
fn publishes_import_fcu(verdict: ImportBlockVerdict) -> bool {
    !matches!(
        verdict,
        ImportBlockVerdict::Invalid | ImportBlockVerdict::DeferredDa
    )
}

/// Map `(early_accept, class)` → late-import flags (CC-27c / §5.3).
///
/// Pure helper so tests can force Reject vs Internal without a full ST.
#[must_use]
pub fn late_import_flags(early_accept: bool, class: GossipClass) -> (bool, bool) {
    (
        early_accept && class == GossipClass::Reject,
        early_accept && class == GossipClass::Internal,
    )
}

/// Shared counters observed by tests (DUPLICATE short-circuit).
#[derive(Debug, Default)]
pub struct ImportCounters {
    /// Times the import path entered `on_block` (post-probe).
    pub transition_invocations: AtomicU64,
}

impl ImportCounters {
    pub fn transition_count(&self) -> u64 {
        self.transition_invocations.load(Ordering::Relaxed)
    }
}

/// Fully imported iff header **and** proto-array node exist (matches `on_block`).
#[inline]
fn is_fully_imported<P: Preset>(store: &Store<P>, root: &Root) -> bool {
    store.blocks().contains_key(root) && store.proto_array().contains(root)
}

/// Run the full import path against a live store (core thread only).
///
/// Unary `ImportBlock` callers use this without an early-accept hook; the
/// P2pStream path passes [`Some`] so the gossip verdict can leave the process
/// **before** the state transition runs (CC-27c).
#[allow(clippy::too_many_arguments)]
pub fn import_block<P: Preset>(
    store: &mut Store<P>,
    residency: &mut Residency<P>,
    config: &ChainConfig,
    head_store: &HeadSnapshotStore,
    event_tx: &tokio::sync::mpsc::Sender<EventInput>,
    metrics: &ChainMetrics,
    counters: &ImportCounters,
    snapshot_sequence: &mut u64,
    request: ImportBlockRequest,
    verify: BlockSignatureStrategy,
) -> Result<ImportOutcome, Status> {
    import_block_with_early(
        store,
        residency,
        config,
        head_store,
        event_tx,
        metrics,
        counters,
        snapshot_sequence,
        request,
        verify,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )
}

/// Like [`import_block`], with optional epoch context and early-accept oneshot.
///
/// When cheap gossip conditions pass, `early_accept` is completed **before**
/// `on_block` / state transition (non-blocking channel send only).
///
/// **Gossip BLS (H1):** when `early_accept_tx` is `Some`, the cheap path **always**
/// verifies the block proposer signature with [`BlockSignatureStrategy::VerifyIndividual`]
/// and **fails closed** (no early ACCEPT) on failure — never skip BLS for re-gossip.
///
/// `inject_after_early` (tests only): if set, after early ACCEPT skip `on_block` and
/// treat the injected error as the import failure (non-vacuous late-flag tests).
///
/// `pending_da` (CC-24d): when `on_block` returns `Deferred(DataUnavailable)`,
/// the signed block is parked for re-drive on `DataAvailable`.
///
/// `pending_engine` (CC-36a): when `on_block` returns
/// `Deferred(ExecutionEngineUnavailable)`, the signed block is parked for
/// re-drive when the engine returns (separate map, 64 / 8 slots).
///
/// `gossip_clock`: when set, a future `block.slot` inside
/// `MAXIMUM_GOSSIP_CLOCK_DISPARITY` of *that* slot's start is admitted by
/// ticking the store to that slot start. Current-slot imports do not jump.
#[allow(clippy::too_many_arguments)]
pub fn import_block_with_early<P: Preset>(
    store: &mut Store<P>,
    residency: &mut Residency<P>,
    config: &ChainConfig,
    head_store: &HeadSnapshotStore,
    event_tx: &tokio::sync::mpsc::Sender<EventInput>,
    metrics: &ChainMetrics,
    counters: &ImportCounters,
    snapshot_sequence: &mut u64,
    request: ImportBlockRequest,
    verify: BlockSignatureStrategy,
    epoch_ctx: Option<&EpochContext>,
    early_accept_tx: Option<tokio::sync::oneshot::Sender<()>>,
    inject_after_early: Option<OnBlockError>,
    pending_da: Option<&mut PendingDa>,
    pending_engine: Option<&mut PendingEngine>,
    gossip_clock: Option<GossipClock>,
    archive: Option<&ArchiveWriteHandle>,
) -> Result<ImportOutcome, Status> {
    let gossip_path = early_accept_tx.is_some() || inject_after_early.is_some();
    // --- 1. decode-free dedup probe (ADR-P1-10 / SEC-4) ----------------------
    let probe = parse_root(&request.root)?;
    if is_fully_imported(store, &probe) {
        // FC already has the block (e.g. persist failed after on_block). Persist
        // only when that block's durable rows are missing. Re-ingest of an
        // already-durable ancestor with update_canonical rewinds the tip (H3).
        if let Some(archive) = archive {
            let signed = decode_signed_block::<P>(&request.ssz, request.fork)?;
            let true_root = Root::from_hash256(TreeHash::tree_hash_root(&signed.message));
            if true_root != probe {
                metrics.inc_import_root_mismatch();
                return Err(Status::invalid_argument(format!(
                    "supplied root {probe} does not match decoded hash_tree_root {true_root}"
                )));
            }
            commit_duplicate_if_missing(archive, store, head_store, &signed, probe, &request.ssz)?;
        }
        metrics.inc_import_result(ImportResult::Duplicate);
        return Ok(ImportOutcome {
            response: import_response(ImportBlockVerdict::Duplicate, &ImportReason::None),
            reason: ImportReason::None,
            transition_invoked: false,
            early_accept: false,
            late_import_reject: false,
            late_import_internal: false,
            block_branch: None,
            publish_fcu: publishes_import_fcu(ImportBlockVerdict::Duplicate),
        });
    }

    // --- 2. decode ----------------------------------------------------------
    let decode_start = Instant::now();
    let signed = decode_signed_block::<P>(&request.ssz, request.fork)?;
    metrics.observe_import_stage(ImportStage::Decode, decode_start.elapsed().as_secs_f64());

    // --- 3. true root check -------------------------------------------------
    let true_root = Root::from_hash256(TreeHash::tree_hash_root(&signed.message));
    if true_root != probe {
        metrics.inc_import_root_mismatch();
        return Err(Status::invalid_argument(format!(
            "supplied root {probe} does not match decoded hash_tree_root {true_root}"
        )));
    }

    // --- 4. cheap gossip conditions (CC-27c) --------------------------------
    // Parent presence / residency, proposer index, finalized descent, signature.
    // Terminal failures here become the **only** verdict (no early ACCEPT).
    if let Some(terminal) = cheap_gossip_terminal(
        store,
        residency,
        config,
        epoch_ctx,
        &signed,
        verify,
        gossip_path,
        metrics,
        gossip_clock,
    )? {
        return Ok(ImportOutcome {
            response: import_response(terminal.verdict, &terminal.reason),
            reason: terminal.reason,
            transition_invoked: false,
            early_accept: false,
            late_import_reject: false,
            late_import_internal: false,
            block_branch: None,
            publish_fcu: publishes_import_fcu(terminal.verdict),
        });
    }

    // Parent durability and the empty-store gate are knowable before
    // `on_block`. A refusal is an ordinary import `Err`: no early ACCEPT,
    // no transition, nothing visible. Absent archive keeps fixture imports.
    if let Some(archive) = archive {
        archive
            .import_precondition(seam_root(signed.message.parent_root))
            .map_err(map_archive_err)?;
    }

    // --- 4b. early gossip ACCEPT (before state transition) ------------------
    let early_accept = true;
    if let Some(tx) = early_accept_tx {
        let _ = tx.send(());
    }

    // Test inject: force a classified import failure without a full ST (F1).
    if let Some(err) = inject_after_early {
        let class = on_block_error_gossip_class(&err);
        let (late_import_reject, late_import_internal) = late_import_flags(early_accept, class);
        metrics.inc_import_result(ImportResult::Invalid);
        let reason = ImportReason::Other(err.to_string());
        return Ok(ImportOutcome {
            response: import_response(ImportBlockVerdict::Invalid, &reason),
            reason,
            transition_invoked: false,
            early_accept,
            late_import_reject,
            late_import_internal,
            block_branch: None,
            publish_fcu: publishes_import_fcu(ImportBlockVerdict::Invalid),
        });
    }

    // --- 5–6. DA → ST → FC via on_block ------------------------------------
    counters
        .transition_invocations
        .fetch_add(1, Ordering::Relaxed);
    // M13: emit cache vs registry lengths on the parent state *before* STF
    // so a short cache is visible even if on_block returns CachePoisoned.
    // Cheap gossip already required the header + `ensure_in_store`; a missing
    // state here is a programming bug, not a skippable scrape.
    let engine = Arc::clone(store.engine_arc());
    let ctx = TransitionContext::new(config, engine.as_ref());
    {
        let Some(parent_state) = store.block_state(&signed.message.parent_root) else {
            debug_assert!(
                false,
                "parent state resident after cheap-gossip ensure_in_store"
            );
            return Err(Status::internal(
                "parent state missing after cheap-gossip ensure_in_store",
            ));
        };
        ctx.top_up_pubkey_cache(parent_state);
        metrics.observe_import_state_with_pubkeys(parent_state, ctx.pubkeys().len());
    }
    let on_block_start = Instant::now();
    let outcome = on_block_with_context(store, &signed, &ctx, verify);
    let on_block_secs = on_block_start.elapsed().as_secs_f64();
    metrics.observe_import_stage(ImportStage::Transition, on_block_secs);

    match outcome {
        Ok(BlockImport::Imported(b)) => finish_imported(
            store,
            residency,
            head_store,
            event_tx,
            metrics,
            snapshot_sequence,
            &signed,
            // F2: arrival bytes — byte-identical to what crossed the RPC, not
            // a re-encode of the decoded container.
            &request.ssz,
            b.root,
            on_block_secs,
            early_accept,
            archive,
        ),
        Ok(BlockImport::Deferred(DeferralReason::DataUnavailable)) => {
            // Park for re-drive when DataAvailable lands (CC-24d / §8.3).
            // Prefer arrival `request.ssz` (F2) for both parking and the event.
            // The deferred row is durable before the event. `head` stays
            // `None`: DA deferral does not move canonical. A later
            // `DataAvailable` upgrades `da` to `Available`.
            let arrival_ssz = &request.ssz;
            if let Some(archive) = archive {
                let (scalar_root, scalar_slot) = current_scalar_head(store);
                commit_durable_import(
                    archive,
                    assemble_durable_import(
                        &signed,
                        true_root,
                        arrival_ssz,
                        store,
                        cc_seam::DaVerdict::Deferred,
                        None,
                        scalar_root,
                        scalar_slot,
                    ),
                )?;
            }
            if let Some(pending) = pending_da {
                let entry = PendingDaEntry {
                    root: true_root,
                    ssz: Bytes::copy_from_slice(arrival_ssz),
                    fork: request.fork,
                    source: request.source,
                    slot: signed.message.slot.as_u64(),
                    parked_at_slot: store.get_current_slot().as_u64(),
                };
                if let Some(evicted) = pending.insert(entry) {
                    metrics.inc_da_pending_dropped(1);
                    tracing::debug!(
                        root = %evicted.root,
                        "pending_da capacity eviction"
                    );
                }
                metrics.set_da_pending_occupancy(pending.len() as u64);
            }
            // CC-44a / §4.2: same BLOCK_IMPORTED kind with deferred discriminator
            // so storage can write da_status in the same batch as the block.
            publish_deferred_block_event(
                event_tx,
                metrics,
                signed.message.slot.as_u64(),
                true_root,
                arrival_ssz,
            );
            metrics.inc_import_result(ImportResult::Deferred);
            // CC-38a: template-sized block-branch trigger for unary FetchBlobs.
            // Never carries cells; core fires engine FetchBlobs when present.
            let block_branch = block_branch_trigger_from_signed(&signed, true_root);
            Ok(ImportOutcome {
                response: import_response(
                    ImportBlockVerdict::DeferredDa,
                    &ImportReason::DataUnavailable,
                ),
                reason: ImportReason::DataUnavailable,
                transition_invoked: true,
                early_accept,
                late_import_reject: false,
                late_import_internal: false,
                block_branch,
                publish_fcu: publishes_import_fcu(ImportBlockVerdict::DeferredDa),
            })
        }
        Ok(BlockImport::Deferred(DeferralReason::UnknownParent)) => {
            // Parent was present at the cheap check; race/reorg edge.
            metrics.inc_import_result(ImportResult::UnknownParent);
            Ok(ImportOutcome {
                response: import_response(
                    ImportBlockVerdict::UnknownParent,
                    &ImportReason::UnknownParent,
                ),
                reason: ImportReason::UnknownParent,
                transition_invoked: true,
                early_accept,
                late_import_reject: false,
                late_import_internal: false,
                block_branch: None,
                publish_fcu: publishes_import_fcu(ImportBlockVerdict::UnknownParent),
            })
        }
        Ok(BlockImport::Deferred(DeferralReason::FutureSlot)) => {
            metrics.inc_import_result(ImportResult::Invalid);
            Ok(ImportOutcome {
                response: import_response(ImportBlockVerdict::Invalid, &ImportReason::FutureSlot),
                reason: ImportReason::FutureSlot,
                transition_invoked: true,
                early_accept,
                // Future slot is Ignore-class for gossip; not a late Reject penalty.
                late_import_reject: false,
                late_import_internal: false,
                block_branch: None,
                publish_fcu: publishes_import_fcu(ImportBlockVerdict::Invalid),
            })
        }
        Ok(BlockImport::Deferred(DeferralReason::ExecutionEngineUnavailable)) => {
            // Park for re-drive when the engine returns (CC-36a / §4.9).
            if let Some(pending) = pending_engine {
                let entry = PendingEngineEntry {
                    root: true_root,
                    ssz: Bytes::from(signed.as_ssz_bytes()),
                    fork: request.fork,
                    source: request.source,
                    slot: signed.message.slot.as_u64(),
                    parked_at_slot: store.get_current_slot().as_u64(),
                };
                if let Some(evicted) = pending.insert(entry) {
                    metrics.inc_pending_engine_dropped(1);
                    tracing::debug!(
                        root = %evicted.root,
                        "pending_engine capacity eviction"
                    );
                }
                metrics.set_pending_engine_occupancy(pending.len() as u64);
            }
            metrics.inc_import_result(ImportResult::DeferredEngine);
            Ok(ImportOutcome {
                response: import_response(
                    ImportBlockVerdict::DeferredDa,
                    &ImportReason::ExecutionEngineUnavailable,
                ),
                reason: ImportReason::ExecutionEngineUnavailable,
                transition_invoked: true,
                early_accept,
                late_import_reject: false,
                late_import_internal: false,
                block_branch: None,
                publish_fcu: publishes_import_fcu(ImportBlockVerdict::DeferredDa),
            })
        }
        Err(e) => {
            let class = on_block_error_gossip_class(&e);
            metrics.inc_import_result(ImportResult::Invalid);
            let (late_import_reject, late_import_internal) = late_import_flags(early_accept, class);
            let reason = ImportReason::Other(e.to_string());
            Ok(ImportOutcome {
                response: import_response(ImportBlockVerdict::Invalid, &reason),
                reason,
                transition_invoked: true,
                early_accept,
                late_import_reject,
                late_import_internal,
                block_branch: None,
                publish_fcu: publishes_import_fcu(ImportBlockVerdict::Invalid),
            })
        }
    }
}

/// Exhaustive [`OnBlockError`] → [`GossipClass`] (no catch-all).
///
/// Delegates transition errors to [`BlockError::gossip_class`]. Keeping the
/// match total is Phase 1 §5.3 / R-13: a new variant fails to compile.
pub fn on_block_error_gossip_class(err: &OnBlockError) -> GossipClass {
    match err {
        OnBlockError::NotDescendedFromFinalized => GossipClass::Reject,
        OnBlockError::Transition(e) => e.gossip_class(),
        OnBlockError::ProtoArray(_)
        | OnBlockError::PulledUpTip(_)
        | OnBlockError::PartialImportNeedsBody
        | OnBlockError::Validation(_) => GossipClass::Internal,
    }
}

/// Cheap-path terminal: typed verdict + reason (proto string is derived).
struct CheapTerminal {
    verdict: ImportBlockVerdict,
    reason: ImportReason,
}

/// Cheap gossip conditions. `Ok(Some(response))` is a terminal import result
/// without running the state transition. `Ok(None)` means ACCEPT-and-continue.
///
/// # H3 mapping notes
/// - `FutureSlot` → stream IGNORE (`Reason::FutureSlot`)
/// - `TooOld` (slot ≤ finalized) → stream IGNORE (`Reason::AlreadyKnown`)
/// - checkpoint non-descent → still Reject-class (`NotDescendedFromFinalized`)
#[allow(clippy::too_many_arguments)]
fn cheap_gossip_terminal<P: Preset>(
    store: &mut Store<P>,
    residency: &mut Residency<P>,
    config: &ChainConfig,
    _epoch_ctx: Option<&EpochContext>,
    signed: &SignedBeaconBlock<P>,
    verify: BlockSignatureStrategy,
    gossip_path: bool,
    metrics: &ChainMetrics,
    gossip_clock: Option<GossipClock>,
) -> Result<Option<CheapTerminal>, Status> {
    let block = &signed.message;
    let parent_root = block.parent_root;

    // Parent presence (header).
    if !store.blocks().contains_key(&parent_root) {
        metrics.inc_import_result(ImportResult::UnknownParent);
        return Ok(Some(CheapTerminal {
            verdict: ImportBlockVerdict::UnknownParent,
            reason: ImportReason::UnknownParent,
        }));
    }

    // Ensure parent state is resident (same as pre-fast-path import).
    if let Err(e) = residency.ensure_in_store(store, parent_root, config) {
        metrics.inc_import_result(ImportResult::Invalid);
        return Ok(Some(CheapTerminal {
            verdict: ImportBlockVerdict::Invalid,
            reason: ImportReason::Other(format!("reorg gap: {e}")),
        }));
    }

    // Future slot relative to store time → IGNORE-class (H3).
    // Disparity admits *this* block's slot only — never an unconditional
    // on_tick into whatever slot comes next.
    if store.get_current_slot().as_u64() < block.slot.as_u64() {
        let admitted = gossip_clock.is_some_and(|clock| {
            admit_block_slot_if_within_disparity(
                store,
                block.slot.as_u64(),
                clock.now_millis,
                clock.disparity,
            )
        });
        if !admitted {
            metrics.inc_import_result(ImportResult::Invalid);
            return Ok(Some(CheapTerminal {
                // Proto has no FUTURE_SLOT verdict; reason drives stream IGNORE.
                verdict: ImportBlockVerdict::Invalid,
                reason: ImportReason::FutureSlot,
            }));
        }
    }

    // Too old (slot at/before finalized epoch start) → IGNORE-class (H3 / eth2 gossip).
    let finalized_slot = compute_start_slot_at_epoch::<P>(store.finalized_checkpoint().epoch);
    if block.slot.as_u64() <= finalized_slot.as_u64() {
        metrics.inc_import_result(ImportResult::Invalid);
        return Ok(Some(CheapTerminal {
            verdict: ImportBlockVerdict::Invalid,
            reason: ImportReason::TooOld,
        }));
    }

    // Not a descendant of the finalized checkpoint → Reject-class (malicious / invalid chain).
    let finalized_checkpoint_block =
        get_checkpoint_block(store, parent_root, store.finalized_checkpoint().epoch);
    if store.finalized_checkpoint().root != finalized_checkpoint_block {
        metrics.inc_import_result(ImportResult::Invalid);
        return Ok(Some(CheapTerminal {
            verdict: ImportBlockVerdict::Invalid,
            reason: ImportReason::NotDescendedFromFinalized,
        }));
    }

    // Proposer index against the parent-state Fulu window only.
    // Head EpochContext is not consulted: it is the head schedule and would
    // cheap-Reject a valid parent-lineage proposer (H1).
    if let Some(expected) = expected_proposer_index::<P>(store, block.slot.as_u64(), parent_root)
        && expected != block.proposer_index.as_u64()
    {
        metrics.inc_import_result(ImportResult::Invalid);
        return Ok(Some(CheapTerminal {
            verdict: ImportBlockVerdict::Invalid,
            reason: ImportReason::Other(format!(
                "proposer mismatch: block={} expected={expected}",
                block.proposer_index.as_u64()
            )),
        }));
    }

    // Block proposer signature.
    // H1: gossip path **always** verifies with VerifyIndividual and fails closed.
    // Unary ImportBlock uses the configured strategy (`CoreConfig::default` is
    // VerifyIndividual). Restore replay overrides via `on_block`, not this path.
    let sig_strategy = if gossip_path {
        BlockSignatureStrategy::VerifyIndividual
    } else {
        verify
    };
    if !matches!(sig_strategy, BlockSignatureStrategy::NoVerification) {
        let Some(parent_state) = store.block_state(&parent_root) else {
            // Fail closed on gossip: never early-ACCEPT without a parent state to verify against.
            metrics.inc_import_result(ImportResult::Invalid);
            return Ok(Some(CheapTerminal {
                verdict: ImportBlockVerdict::Invalid,
                reason: ImportReason::ProposerSigParentStateMissing,
            }));
        };
        match verify_block_proposer_sig(parent_state, signed, sig_strategy) {
            Ok(()) => {}
            Err(e) => {
                // Fail closed: any classified failure blocks early ACCEPT.
                // Internal (state BLS) → no peer Reject; still no early ACCEPT.
                metrics.inc_import_result(ImportResult::Invalid);
                let reason = match e.gossip_class() {
                    GossipClass::Internal => ImportReason::InternalProposerSig(e.to_string()),
                    GossipClass::Reject | GossipClass::Ignore => ImportReason::Other(e.to_string()),
                };
                return Ok(Some(CheapTerminal {
                    verdict: ImportBlockVerdict::Invalid,
                    reason,
                }));
            }
        }
    }

    Ok(None)
}

/// Look up expected proposer for `slot` from the **parent** post-state lookahead.
///
/// Outside the Fulu window, or if the parent state is not resident, this
/// returns `None` (unknown) so `on_block` is the authority. Never wrap to
/// `slot % SPE`, and never invent a proposer from head `EpochContext` or
/// `last_head_root` (those are the other fork's schedule).
fn expected_proposer_index<P: Preset>(
    store: &Store<P>,
    slot: u64,
    parent_root: Root,
) -> Option<u64> {
    let state = store.block_state(&parent_root)?;
    proposer_from_state_lookahead(state, slot)
}

/// Index into `state.proposer_lookahead` for `slot`, if in the Fulu window
/// `[epoch_start, epoch_start + lookahead_len)`.
fn proposer_from_state_lookahead<P: Preset>(
    state: &cc_types::BeaconState<P>,
    slot: u64,
) -> Option<u64> {
    let len = state.proposer_lookahead_len();
    if len == 0 {
        return None;
    }
    let epoch = compute_epoch_at_slot::<P>(state.slot());
    let start_slot = compute_start_slot_at_epoch::<P>(epoch).as_u64();
    if slot < start_slot {
        return None;
    }
    let offset = (slot - start_slot) as usize;
    if offset >= len {
        return None;
    }
    state.proposer_lookahead_get(offset).map(|v| v.as_u64())
}

fn verify_block_proposer_sig<P: Preset>(
    state: &cc_types::BeaconState<P>,
    signed: &SignedBeaconBlock<P>,
    strategy: BlockSignatureStrategy,
) -> Result<(), BlockError> {
    let mut set = BlockSignatureSet::default();
    push_block_proposer_signature(&mut set, state, signed)?;
    set.verify(strategy)
}

pub(crate) fn seam_root(root: Root) -> cc_seam::Root {
    let mut arr = [0u8; 32];
    arr.copy_from_slice(root.as_slice());
    arr
}

pub(crate) fn map_archive_err(err: cc_seam::SeamError) -> Status {
    match err {
        cc_seam::SeamError::Backpressure { .. } => Status::resource_exhausted(err.to_string()),
        cc_seam::SeamError::InvalidArgument(msg) => Status::invalid_argument(msg),
        cc_seam::SeamError::FailedPrecondition { reason } => {
            Status::failed_precondition(reason.as_str())
        }
        other => Status::unavailable(other.to_string()),
    }
}

/// Fork-choice head to stamp into scalars when this call does not move it.
pub(crate) fn current_scalar_head<P: Preset>(store: &Store<P>) -> (Root, Slot) {
    if let Some(root) = store.cached_head_root() {
        let slot = store
            .blocks()
            .get(&root)
            .map(|h| h.slot)
            .unwrap_or_else(|| store.get_current_slot());
        return (root, slot);
    }
    let justified = store.justified_checkpoint().root;
    let slot = store
        .blocks()
        .get(&justified)
        .map(|h| h.slot)
        .unwrap_or_else(|| store.get_current_slot());
    (justified, slot)
}

/// One `commit_import` body. `head` is set only when it names this body.
#[allow(clippy::too_many_arguments)]
pub(crate) fn assemble_durable_import<P: Preset>(
    signed: &SignedBeaconBlock<P>,
    block_root: Root,
    arrival_ssz: &[u8],
    store: &Store<P>,
    da: cc_seam::DaVerdict,
    head: Option<cc_seam::HeadChange>,
    scalar_head: Root,
    scalar_slot: Slot,
) -> cc_seam::DurableImport {
    cc_seam::DurableImport {
        block_root: seam_root(block_root),
        parent_root: seam_root(signed.message.parent_root),
        slot: signed.message.slot.as_u64(),
        state_root: seam_root(signed.message.state_root),
        ssz: Bytes::copy_from_slice(arrival_ssz),
        da,
        scalars: fork_choice_scalars_ssz(store, scalar_head, scalar_slot),
        head,
    }
}

pub(crate) fn commit_durable_import(
    archive: &ArchiveWriteHandle,
    import: cc_seam::DurableImport,
) -> Result<(), Status> {
    archive
        .commit_import_blocking(import)
        .map_err(map_archive_err)
}

pub(crate) fn commit_set_head(
    archive: &ArchiveWriteHandle,
    head: cc_seam::HeadChange,
    scalars: Bytes,
    snapshot: Option<cc_seam::Snapshot>,
) -> Result<(), Status> {
    archive
        .set_head_blocking(head, scalars)
        .map_err(map_archive_err)?;
    if let Some(snapshot) = snapshot {
        persist_boundary_snapshot(
            archive,
            snapshot.slot,
            snapshot.state_root,
            Some(snapshot.state_ssz),
        )?;
    }
    Ok(())
}

/// Resident post-state of a head that sits on an epoch boundary.
///
/// `None` when the slot is not a boundary or the post-state is not resident.
/// A missing state does not fail the head write that asked for this snapshot.
pub(crate) fn resident_epoch_snapshot<P: Preset>(
    store: &Store<P>,
    head_root: Root,
    head_slot: Slot,
) -> Option<cc_seam::Snapshot> {
    let slots = P::SLOTS_PER_EPOCH.max(1);
    if !head_slot.as_u64().is_multiple_of(slots) {
        return None;
    }
    let state_root = store.blocks().get(&head_root)?.state_root;
    let state = store.block_state(&head_root)?;
    Some(cc_seam::Snapshot {
        slot: head_slot.as_u64(),
        state_root: seam_root(state_root),
        state_ssz: Bytes::from(state.as_ssz_bytes()),
    })
}

/// Realign the one completion marker, then write it if this slot is still behind.
///
/// Realign is a no-op when the marker slot differs. A same-slot payload whose
/// canonical block does not carry `state_root` is refused before the write.
fn persist_boundary_snapshot(
    archive: &ArchiveWriteHandle,
    slot: u64,
    state_root: cc_seam::Root,
    state_ssz: Option<Bytes>,
) -> Result<(), Status> {
    if let Some(bytes) = state_ssz.clone() {
        archive
            .realign_head_snapshot_blocking(cc_seam::Snapshot {
                slot,
                state_root,
                state_ssz: bytes,
            })
            .map_err(map_archive_err)?;
    }
    maybe_commit_epoch_snapshot(true, Some(archive), slot, state_root, state_ssz)
}

/// One snapshot per epoch boundary. `false` writes nothing.
///
/// The post-state bytes belong to chain-core. Storage sees an opaque
/// [`cc_seam::Snapshot`] and does not name a state type. A missing post-state
/// on a boundary is an error: the replay window would otherwise stay open.
pub(crate) fn maybe_commit_epoch_snapshot(
    is_epoch_boundary: bool,
    archive: Option<&ArchiveWriteHandle>,
    slot: u64,
    state_root: cc_seam::Root,
    state_ssz: Option<Bytes>,
) -> Result<(), Status> {
    if !is_epoch_boundary {
        return Ok(());
    }
    let Some(archive) = archive else {
        return Ok(());
    };
    let Some(state_ssz) = state_ssz else {
        return Err(Status::internal(
            "epoch-boundary import has no post-state to snapshot",
        ));
    };
    futures::executor::block_on(archive.commit_snapshot(cc_seam::Snapshot {
        slot,
        state_root,
        state_ssz,
    }))
    .map_err(map_archive_err)
}

/// DUPLICATE retry: commit a missing body, or `set_head` when the body is
/// already durable but canonical was not moved onto it.
///
/// The SSZ parent is stored as-is. A missing body carries `head` only when
/// `get_head` selected that body — a non-durable anchor is not `set_head`.
/// When this call does not durably write the fork-choice head, the latch
/// `get_head` stored is restored unless that head was already committed.
fn commit_duplicate_if_missing<P: Preset>(
    archive: &ArchiveWriteHandle,
    store: &mut Store<P>,
    head_store: &HeadSnapshotStore,
    signed: &SignedBeaconBlock<P>,
    block_root: Root,
    arrival_ssz: &[u8],
) -> Result<(), Status> {
    let latch = store.head_latch();
    let (pre_scalar_root, pre_scalar_slot) = current_scalar_head(store);
    let (fc_head, _) = match get_head(store) {
        Ok(head) => head,
        Err(e) => {
            store.restore_head_latch(latch);
            return Err(Status::internal(format!(
                "get_head failed on duplicate repair: {e}"
            )));
        }
    };
    let body_durable = archive
        .block_is_durable(seam_root(block_root))
        .map_err(map_archive_err)?;
    let fc_is_this = fc_head == block_root;
    let slot = signed.message.slot.as_u64();
    let is_epoch_boundary = slot.is_multiple_of(P::SLOTS_PER_EPOCH.max(1));
    let mut wrote_fc_head = false;
    let mut snapshotted = false;
    if body_durable {
        if fc_is_this && head_store.durable_head() != Some(block_root) {
            let head_slot = signed.message.slot;
            let snapshot = resident_epoch_snapshot::<P>(store, block_root, head_slot);
            snapshotted = snapshot.is_some();
            if let Err(e) = commit_set_head(
                archive,
                cc_seam::HeadChange {
                    head_root: seam_root(block_root),
                    head_slot: head_slot.as_u64(),
                    cause: cc_seam::HeadCause::Import,
                },
                fork_choice_scalars_ssz(store, block_root, head_slot),
                snapshot,
            ) {
                store.restore_head_latch(latch);
                return Err(e);
            }
            head_store.set_durable_head(block_root);
            wrote_fc_head = true;
        }
    } else {
        // Body is missing: the head rides on `commit_import`, not `set_head`.
        let head = fc_is_this.then(|| cc_seam::HeadChange {
            head_root: seam_root(block_root),
            head_slot: signed.message.slot.as_u64(),
            cause: cc_seam::HeadCause::Import,
        });
        let (scalar_root, scalar_slot) = if fc_is_this {
            (block_root, signed.message.slot)
        } else {
            (pre_scalar_root, pre_scalar_slot)
        };
        if let Err(e) = commit_durable_import(
            archive,
            assemble_durable_import(
                signed,
                block_root,
                arrival_ssz,
                store,
                cc_seam::DaVerdict::Available,
                head,
                scalar_root,
                scalar_slot,
            ),
        ) {
            store.restore_head_latch(latch);
            return Err(e);
        }
        if fc_is_this {
            head_store.set_durable_head(block_root);
            wrote_fc_head = true;
        }
    }
    // The body can already be the durable head while the completion marker
    // is still behind (a dropped P2 chunk). Re-issue while the window is
    // still exactly one epoch. A non-head duplicate does not snapshot.
    if fc_is_this && is_epoch_boundary && !snapshotted {
        let state_ssz = store
            .block_state(&block_root)
            .map(|state| Bytes::from(state.as_ssz_bytes()));
        if let Err(e) = persist_boundary_snapshot(
            archive,
            slot,
            seam_root(signed.message.state_root),
            state_ssz,
        ) {
            store.restore_head_latch(latch);
            return Err(e);
        }
    }
    if !wrote_fc_head && head_store.durable_head() != Some(fc_head) {
        store.restore_head_latch(latch);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn finish_imported<P: Preset>(
    store: &mut Store<P>,
    residency: &mut Residency<P>,
    head_store: &HeadSnapshotStore,
    event_tx: &tokio::sync::mpsc::Sender<EventInput>,
    metrics: &ChainMetrics,
    snapshot_sequence: &mut u64,
    signed: &SignedBeaconBlock<P>,
    // Arrival `ImportBlockRequest.ssz` (F2 — not a re-encode).
    arrival_ssz: &[u8],
    block_root: Root,
    on_block_secs: f64,
    early_accept: bool,
    archive: Option<&ArchiveWriteHandle>,
) -> Result<ImportOutcome, Status> {
    let slot = signed.message.slot.as_u64();
    let slots_per_epoch = P::SLOTS_PER_EPOCH.max(1);
    let is_epoch_boundary = slot.is_multiple_of(slots_per_epoch);

    let validator_count = store
        .block_state(&block_root)
        .map(|s| s.validators_len() as u64)
        .unwrap_or(0);
    // Inclusive + exclusive dual observation (CC-3Aa / §6.4). Single call site
    // so pre-engine local ≡ inclusive; after the engine is in the path only
    // local stays exclusive (CC-32b will split the span).
    metrics.observe_process_block_with_local(
        on_block_secs,
        slot,
        slot / slots_per_epoch,
        validator_count,
    );

    // --- 7. head recompute, then the durable commit -----------------------
    // `get_head` selects the head. The published snapshot, residency, and
    // events wait until that commit returns. A failed write restores the
    // latch `get_head` stored. "Changed" is versus the last head this
    // process durably committed, not the latch alone.
    let publish_start = Instant::now();
    let latch = store.head_latch();
    let (pre_scalar_root, pre_scalar_slot) = current_scalar_head(store);
    let (head_root, reorg) = match get_head(store) {
        Ok(head) => head,
        Err(e) => {
            store.restore_head_latch(latch);
            return Err(Status::internal(format!(
                "get_head failed after import: {e}"
            )));
        }
    };
    let head_slot = store
        .blocks()
        .get(&head_root)
        .map(|h| h.slot)
        .unwrap_or(signed.message.slot);
    let head_state_root = store
        .blocks()
        .get(&head_root)
        .map(|h| h.state_root)
        .unwrap_or(Root::ZERO);
    if let Some(archive) = archive {
        let needs_head_write = head_store.durable_head() != Some(head_root);
        // This body's transaction carries `head` only when the new head is
        // the body it writes. Any other new head is `set_head`. Scalars on
        // a body that does not carry that head stay the pre-`get_head` head
        // so the writer does not persist the uncommitted root.
        let head_on_body = needs_head_write && head_root == block_root;
        let (scalar_root, scalar_slot) = if head_on_body {
            (block_root, signed.message.slot)
        } else if needs_head_write {
            (pre_scalar_root, pre_scalar_slot)
        } else {
            (head_root, head_slot)
        };
        let head_field = head_on_body.then(|| cc_seam::HeadChange {
            head_root: seam_root(block_root),
            head_slot: slot,
            cause: cc_seam::HeadCause::Import,
        });
        if let Err(e) = commit_durable_import(
            archive,
            assemble_durable_import(
                signed,
                block_root,
                arrival_ssz,
                store,
                cc_seam::DaVerdict::Available,
                head_field,
                scalar_root,
                scalar_slot,
            ),
        ) {
            store.restore_head_latch(latch);
            return Err(e);
        }
        if head_on_body {
            head_store.set_durable_head(head_root);
        }
        if needs_head_write && head_root != block_root {
            if let Err(e) = commit_set_head(
                archive,
                cc_seam::HeadChange {
                    head_root: seam_root(head_root),
                    head_slot: head_slot.as_u64(),
                    cause: cc_seam::HeadCause::Import,
                },
                fork_choice_scalars_ssz(store, head_root, head_slot),
                resident_epoch_snapshot::<P>(store, head_root, head_slot),
            ) {
                store.restore_head_latch(latch);
                return Err(e);
            }
            head_store.set_durable_head(head_root);
        }
        if is_epoch_boundary && head_root == block_root {
            let state_root = seam_root(signed.message.state_root);
            let state_ssz = store
                .block_state(&block_root)
                .map(|state| Bytes::from(state.as_ssz_bytes()));
            if let Some(bytes) = state_ssz.clone()
                && let Err(status) = archive
                    .realign_head_snapshot_blocking(cc_seam::Snapshot {
                        slot,
                        state_root,
                        state_ssz: bytes,
                    })
                    .map_err(map_archive_err)
            {
                store.restore_head_latch(latch);
                return Err(status);
            }
            if let Err(status) = maybe_commit_epoch_snapshot(
                is_epoch_boundary,
                Some(archive),
                slot,
                state_root,
                state_ssz,
            ) {
                store.restore_head_latch(latch);
                return Err(status);
            }
        }
    }

    if let Some(post) = store.block_state(&block_root).cloned() {
        residency.record_imported_body(block_root, Arc::new(signed.clone()), post);
    }

    // --- 7b. pin Head = FC head, then prune (H2) ----------------------------
    residency.settle_after_head(store, head_root, block_root, is_epoch_boundary);
    metrics.set_resident_states(residency.resident_count() as u64);
    metrics.set_body_ring_len(residency.body_ring_len() as u64);

    // --- 8. snapshot publish BEFORE events (ordering guarantee) ------------
    // Capture prior finalized *before* overwriting the snapshot so we can emit
    // FINALIZED_CHECKPOINT only on a real change.
    let prev_finalized = head_store.load().finalized;
    *snapshot_sequence = snapshot_sequence.saturating_add(1);
    let optimistic = cc_fork_choice::is_optimistic_node(store);
    let finalized = store.finalized_checkpoint();
    let snapshot = HeadSnapshot {
        head_root,
        head_slot,
        head_state_root,
        justified: store.justified_checkpoint(),
        finalized,
        unrealized_justified: store.unrealized_justified_checkpoint(),
        unrealized_finalized: store.unrealized_finalized_checkpoint(),
        current_epoch_target_root: Root::ZERO,
        dependent_root: Root::ZERO,
        // CC-3B: node-level optimistic from fork choice after fresh get_head.
        is_optimistic: optimistic,
        sequence: *snapshot_sequence,
    };
    head_store.store(snapshot);
    metrics.set_head(head_slot.as_u64(), 0, finalized.epoch.as_u64());
    metrics.is_optimistic.set(i64::from(optimistic));
    metrics
        .optimistic_nodes
        .set(store.proto_array().optimistic_node_count() as i64);

    // --- 9. event publish (backpressure; Phase 1 §7.3 / F1) ----------------
    // Data-carrying events use `blocking_send` so a slow events task applies
    // backpressure rather than silently dropping BLOCK_IMPORTED / FINALIZED.
    let finalized_state_root = store
        .blocks()
        .get(&finalized.root)
        .map(|h| h.state_root)
        .unwrap_or(Root::ZERO);
    publish_import_events(
        event_tx,
        metrics,
        store,
        slot,
        block_root,
        arrival_ssz,
        BlockImportedPayloadVerdict::Imported,
        head_root,
        head_slot,
        reorg,
        prev_finalized,
        finalized,
        finalized_state_root,
    );
    // Prune may have dropped optimistic side-branches; refresh after events.
    metrics
        .optimistic_nodes
        .set(store.proto_array().optimistic_node_count() as i64);
    metrics.observe_import_stage(ImportStage::Publish, publish_start.elapsed().as_secs_f64());

    metrics.inc_import_result(ImportResult::Imported);
    Ok(ImportOutcome {
        response: import_response(ImportBlockVerdict::Imported, &ImportReason::None),
        reason: ImportReason::None,
        transition_invoked: true,
        early_accept,
        late_import_reject: false,
        late_import_internal: false,
        block_branch: None,
        publish_fcu: publishes_import_fcu(ImportBlockVerdict::Imported),
    })
}

/// Publish import events with **backpressure** (Phase 1 §7.3 / F1).
///
/// Data-carrying events (`BLOCK_IMPORTED`, `FINALIZED_CHECKPOINT`) and the
/// accompanying HEAD/REORG use `blocking_send` on the core thread so a wedged
/// events task stalls the producer rather than silently dropping payload
/// bytes that storage needs for the same-transaction write-behind.
///
/// Payloads follow Architecture §4.2. `verdict_byte` is the first payload byte
/// of `BLOCK_IMPORTED` so `DEFERRED_DA` and `IMPORTED` share one kind.
/// `arrival_ssz` is the wire `ImportBlockRequest.ssz` (F2).
#[allow(clippy::too_many_arguments)]
fn publish_import_events<P: Preset>(
    event_tx: &tokio::sync::mpsc::Sender<EventInput>,
    metrics: &ChainMetrics,
    store: &mut Store<P>,
    slot: u64,
    block_root: Root,
    arrival_ssz: &[u8],
    verdict: BlockImportedPayloadVerdict,
    head_root: Root,
    head_slot: Slot,
    reorg: Option<ChainReorg>,
    prev_finalized: Checkpoint,
    finalized: Checkpoint,
    finalized_state_root: Root,
) {
    publish_event_blocking(
        event_tx,
        metrics,
        EventInput::block_imported_with_payload(
            slot,
            Bytes::copy_from_slice(block_root.as_slice()),
            block_imported_payload(verdict, arrival_ssz),
        ),
    );
    publish_event_blocking(
        event_tx,
        metrics,
        EventInput::head(
            head_slot.as_u64(),
            Bytes::copy_from_slice(head_root.as_slice()),
        ),
    );
    if let Some(reorg) = reorg {
        let ancestor_slot = common_ancestor_slot(store, reorg.old_head, reorg.new_head);
        publish_event_blocking(
            event_tx,
            metrics,
            EventInput::chain_reorg(
                reorg.new_head_slot.as_u64(),
                Bytes::copy_from_slice(reorg.new_head.as_slice()),
                Bytes::copy_from_slice(reorg.old_head.as_slice()),
                ancestor_slot.as_u64(),
            ),
        );
    }
    if finalized != prev_finalized {
        // P0-10 / S0-A-21: proto-array prune hangs off FINALIZED, after the
        // REORG walk above still had the pre-prune tree (old head may be a
        // now-invalid side branch).
        store.prune_on_finalized();
        let scalars = fork_choice_scalars_ssz(store, head_root, head_slot);
        publish_event_blocking(
            event_tx,
            metrics,
            EventInput::finalized_checkpoint(
                finalized.epoch.as_u64(),
                Bytes::copy_from_slice(finalized.root.as_slice()),
                Bytes::copy_from_slice(finalized_state_root.as_slice()),
                scalars,
            ),
        );
    }
}

/// Publish a standalone `BLOCK_IMPORTED` for a DA-deferred import (same kind,
/// deferred discriminator — Architecture §4.2 rule 3). Uses backpressure (F1).
fn publish_deferred_block_event(
    event_tx: &tokio::sync::mpsc::Sender<EventInput>,
    metrics: &ChainMetrics,
    slot: u64,
    block_root: Root,
    arrival_ssz: &[u8],
) {
    publish_event_blocking(
        event_tx,
        metrics,
        EventInput::block_imported_with_payload(
            slot,
            Bytes::copy_from_slice(block_root.as_slice()),
            block_imported_payload(BlockImportedPayloadVerdict::DeferredDa, arrival_ssz),
        ),
    );
}

/// Core-thread publish with backpressure (F1 / Phase 1 §7.3).
///
/// - Oversize payload → metric + **error** log; not enqueued (SEC-44a-2).
/// - Channel full → `blocking_send` waits (backpressure to the core).
/// - Channel closed → metric + **error** log (loud failure, not a silent drop).
fn publish_event_blocking(
    event_tx: &tokio::sync::mpsc::Sender<EventInput>,
    metrics: &ChainMetrics,
    input: EventInput,
) {
    if !input.payload_within_cap() {
        metrics.inc_event_payload_rejected();
        tracing::error!(
            kind = ?input.kind,
            payload_len = input.payload.len(),
            cap = crate::events::MAX_EVENT_PAYLOAD_BYTES,
            "rejected oversize event payload before ring (SEC-44a-2)"
        );
        return;
    }
    match event_tx.blocking_send(input) {
        Ok(()) => {}
        Err(tokio::sync::mpsc::error::SendError(lost)) => {
            metrics.inc_event_publish_dropped();
            tracing::error!(
                kind = ?lost.kind,
                slot = lost.slot,
                payload_len = lost.payload.len(),
                "events channel closed; lost data-carrying event (F1 loud path)"
            );
        }
    }
}

/// Test / ordering helper: non-blocking publish (does not apply backpressure).
fn try_publish_event(
    event_tx: &tokio::sync::mpsc::Sender<EventInput>,
    metrics: &ChainMetrics,
    input: EventInput,
) {
    if !input.payload_within_cap() {
        metrics.inc_event_payload_rejected();
        tracing::error!(
            kind = ?input.kind,
            payload_len = input.payload.len(),
            "rejected oversize event payload (SEC-44a-2)"
        );
        return;
    }
    match event_tx.try_send(input) {
        Ok(()) => {}
        Err(tokio::sync::mpsc::error::TrySendError::Full(lost)) => {
            metrics.inc_event_publish_dropped();
            tracing::error!(
                kind = ?lost.kind,
                "events channel full; dropped event (test helper try_send)"
            );
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(lost)) => {
            metrics.inc_event_publish_dropped();
            tracing::error!(
                kind = ?lost.kind,
                "events channel closed; dropped event"
            );
        }
    }
}

/// Parse a 32-byte root from the request probe field.
pub fn parse_root(bytes: &[u8]) -> Result<Root, Status> {
    if bytes.len() != 32 {
        return Err(Status::invalid_argument(format!(
            "root must be 32 bytes, got {}",
            bytes.len()
        )));
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(bytes);
    Ok(Root::from_array(arr))
}

/// Decode a `SignedBeaconBlock` from SSZ bytes.
///
/// `fork` is reserved for multi-fork decode; Phase 1 is Fulu-only.
pub fn decode_signed_block<P: Preset>(
    ssz: &[u8],
    _fork: u32,
) -> Result<SignedBeaconBlock<P>, Status> {
    SignedBeaconBlock::<P>::from_ssz_bytes_with(ForkName::Fulu, ssz).map_err(|e| {
        Status::invalid_argument(format!("failed to decode SignedBeaconBlock SSZ: {e:?}"))
    })
}

/// Encode a signed block to SSZ (tests).
pub fn encode_signed_block<P: Preset>(block: &SignedBeaconBlock<P>) -> Vec<u8> {
    block.as_ssz_bytes()
}

/// Publish snapshot then events in core order (unit-testable ordering helper).
///
/// Used by tests to assert snapshot-before-event without a full state transition.
/// Payload for `BLOCK_IMPORTED` is a minimal `[IMPORTED] ‖ []` so ordering tests
/// do not need a live store.
pub fn publish_snapshot_then_events(
    head_store: &HeadSnapshotStore,
    event_tx: &tokio::sync::mpsc::Sender<EventInput>,
    metrics: &ChainMetrics,
    snapshot: HeadSnapshot,
    slot: u64,
    block_root: Root,
) {
    let head_root = snapshot.head_root;
    let head_slot = snapshot.head_slot;
    head_store.store(snapshot);
    // Ordering-only helper: no store → skip reorg/finalized; payload disc only.
    try_publish_event(
        event_tx,
        metrics,
        EventInput::block_imported_with_payload(
            slot,
            Bytes::copy_from_slice(block_root.as_slice()),
            block_imported_payload(BlockImportedPayloadVerdict::Imported, &[]),
        ),
    );
    try_publish_event(
        event_tx,
        metrics,
        EventInput::head(
            head_slot.as_u64(),
            Bytes::copy_from_slice(head_root.as_slice()),
        ),
    );
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use cc_fork_choice::{
        ExecutionStatus, HarnessAvailability, ProtoNodeBlock, get_forkchoice_store, get_head,
    };
    use cc_types::containers::BeaconBlockHeader;
    use cc_types::preset::Minimal;
    use cc_types::primitives::{Epoch, Hash256, Slot, ValidatorIndex};
    use cc_types::{BeaconBlock, BeaconState};
    use prometheus_client::registry::Registry;
    use tokio::sync::mpsc;
    use tree_hash::TreeHash;

    /// Private always-Valid engine (same shape as the crate's other import tests).
    #[derive(Debug, Default, Clone, Copy)]
    struct AcceptEngine;

    impl<P: Preset> cc_state_transition::ExecutionEngine<P> for AcceptEngine {
        fn verify_and_notify_new_payload(
            &self,
            _request: cc_state_transition::NewPayloadRequest<'_, P>,
        ) -> Result<cc_state_transition::PayloadStatus, cc_state_transition::EngineError> {
            Ok(cc_state_transition::PayloadStatus::Valid)
        }
    }

    #[test]
    fn epoch_boundary_drives_one_snapshot() {
        use std::sync::Mutex;

        struct Rec {
            snaps: Mutex<Vec<cc_seam::Snapshot>>,
        }

        #[async_trait::async_trait]
        impl cc_seam::ArchiveWrite for Rec {
            async fn ingest_columns(
                &self,
                _batch: cc_seam::ColumnBatch,
            ) -> Result<(), cc_seam::SeamError> {
                Ok(())
            }

            async fn commit_snapshot(
                &self,
                snapshot: cc_seam::Snapshot,
            ) -> Result<(), cc_seam::SeamError> {
                self.snaps.lock().expect("snap lock").push(snapshot);
                Ok(())
            }
        }

        let rec = Arc::new(Rec {
            snaps: Mutex::new(Vec::new()),
        });
        let handle: ArchiveWriteHandle = rec.clone();
        let root = [7u8; 32];
        maybe_commit_epoch_snapshot(
            false,
            Some(&handle),
            8,
            root,
            Some(Bytes::from(b"state".to_vec())),
        )
        .unwrap();
        assert!(rec.snaps.lock().unwrap().is_empty());

        maybe_commit_epoch_snapshot(
            true,
            Some(&handle),
            8,
            root,
            Some(Bytes::from(b"state".to_vec())),
        )
        .unwrap();
        {
            let snaps = rec.snaps.lock().unwrap();
            assert_eq!(snaps.len(), 1);
            assert_eq!(snaps[0].slot, 8);
            assert_eq!(snaps[0].state_root, root);
            assert_eq!(snaps[0].state_ssz.as_ref(), b"state");
        }
        let missing = maybe_commit_epoch_snapshot(true, Some(&handle), 8, root, None).unwrap_err();
        assert!(missing.to_string().contains("post-state"), "{missing}");

        let body = include_str!("import.rs")
            .split("fn finish_imported")
            .nth(1)
            .unwrap()
            .split("mod tests")
            .next()
            .unwrap();
        assert!(
            body.contains("maybe_commit_epoch_snapshot("),
            "finish_imported must drive the snapshot from the epoch flag"
        );
        assert!(
            body.contains("is_epoch_boundary && head_root == block_root"),
            "only the selected head at an epoch boundary is snapshotted"
        );
    }

    struct SnapRec {
        snaps: std::sync::Mutex<Vec<cc_seam::Snapshot>>,
        attempts: std::sync::atomic::AtomicU32,
        fail_first: u32,
        durable: std::sync::Mutex<Vec<[u8; 32]>>,
    }

    #[async_trait::async_trait]
    impl cc_seam::ArchiveWrite for SnapRec {
        async fn ingest_columns(
            &self,
            _batch: cc_seam::ColumnBatch,
        ) -> Result<(), cc_seam::SeamError> {
            Ok(())
        }

        fn commit_import_blocking(
            &self,
            import: cc_seam::DurableImport,
        ) -> Result<(), cc_seam::SeamError> {
            self.durable.lock().unwrap().push(import.block_root);
            Ok(())
        }

        fn block_is_durable(&self, root: cc_seam::Root) -> Result<bool, cc_seam::SeamError> {
            Ok(self.durable.lock().unwrap().contains(&root))
        }

        async fn commit_snapshot(
            &self,
            snapshot: cc_seam::Snapshot,
        ) -> Result<(), cc_seam::SeamError> {
            let n = self.attempts.fetch_add(1, Ordering::SeqCst);
            if n < self.fail_first {
                return Err(cc_seam::SeamError::Unavailable(
                    "dropped boundary snapshot".into(),
                ));
            }
            self.snaps.lock().unwrap().push(snapshot);
            Ok(())
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn import_recorded(
        store: &mut cc_fork_choice::Store<Minimal>,
        config: &ChainConfig,
        head: &HeadSnapshotStore,
        metrics: &ChainMetrics,
        residency: &mut crate::residency::Residency<Minimal>,
        snap_seq: &mut u64,
        request: ImportBlockRequest,
        archive: &ArchiveWriteHandle,
    ) -> Result<ImportOutcome, Status> {
        let (event_tx, _) = mpsc::channel(1);
        let counters = ImportCounters::default();
        import_block_with_early(
            store,
            residency,
            config,
            head,
            &event_tx,
            metrics,
            &counters,
            snap_seq,
            request,
            BlockSignatureStrategy::NoVerification,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(archive),
        )
    }

    fn signed_boundary(
        parent: Root,
        state_root: Root,
    ) -> (SignedBeaconBlock<Minimal>, Root, Vec<u8>) {
        let signed = SignedBeaconBlock {
            message: BeaconBlock {
                slot: Slot::new(Minimal::SLOTS_PER_EPOCH),
                proposer_index: ValidatorIndex::new(0),
                parent_root: parent,
                state_root,
                body: Default::default(),
            },
            signature: Default::default(),
        };
        let root = Root::from_hash256(TreeHash::tree_hash_root(&signed.message));
        let ssz = encode_signed_block(&signed);
        (signed, root, ssz)
    }

    fn install_boundary(
        store: &mut cc_fork_choice::Store<Minimal>,
        parent: Root,
        root: Root,
        state_root: Root,
        state: BeaconState<Minimal>,
    ) {
        store.insert_block(
            root,
            BeaconBlockHeader {
                slot: Slot::new(Minimal::SLOTS_PER_EPOCH),
                proposer_index: ValidatorIndex::new(0),
                parent_root: parent,
                state_root,
                body_root: Root::ZERO,
            },
            state,
        );
        let justified = store.justified_checkpoint();
        let finalized = store.finalized_checkpoint();
        store
            .proto_array_mut()
            .on_block(ProtoNodeBlock {
                slot: Slot::new(Minimal::SLOTS_PER_EPOCH),
                root,
                parent_root: Some(parent),
                state_root,
                target_root: root,
                justified_checkpoint: justified,
                finalized_checkpoint: finalized,
                unrealized_justified_checkpoint: justified,
                unrealized_finalized_checkpoint: finalized,
                execution_status: ExecutionStatus::Valid,
                execution_block_hash: Hash256::ZERO,
            })
            .unwrap();
    }

    /// A losing block at the epoch boundary must not replace the head snapshot.
    #[test]
    fn non_head_epoch_boundary_block_does_not_replace_the_marker() {
        let (mut store, config, _request, _child) = persist_retry_child();
        let anchor = store.finalized_checkpoint().root;
        assert_eq!(anchor_slot_distance(), Minimal::SLOTS_PER_EPOCH);
        let head_state_root = Root::from_array([0xA1; 32]);
        let lose_state_root = Root::from_array([0xB1; 32]);
        let (head_signed, head_root, head_ssz) = signed_boundary(anchor, head_state_root);
        let (lose_signed, lose_root, lose_ssz) = signed_boundary(anchor, lose_state_root);
        install_boundary(
            &mut store,
            anchor,
            head_root,
            head_state_root,
            wrap_marker_state(1),
        );
        install_boundary(
            &mut store,
            anchor,
            lose_root,
            lose_state_root,
            wrap_marker_state(2),
        );
        store.set_proposer_boost_root(head_root);
        assert_eq!(get_head(&mut store).unwrap().0, head_root);

        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let head_store = HeadSnapshotStore::new();
        head_store.set_durable_head(head_root);
        let mut residency = crate::residency::Residency::<Minimal>::new(64, 32);
        let mut snap_seq = 0u64;
        let (event_tx, _event_rx) = mpsc::channel(8);
        let recorded = Arc::new(SnapRec {
            snaps: std::sync::Mutex::new(Vec::new()),
            attempts: std::sync::atomic::AtomicU32::new(0),
            fail_first: 0,
            durable: std::sync::Mutex::new(Vec::new()),
        });
        let archive: ArchiveWriteHandle = recorded.clone();

        finish_imported(
            &mut store,
            &mut residency,
            &head_store,
            &event_tx,
            &metrics,
            &mut snap_seq,
            &lose_signed,
            &lose_ssz,
            lose_root,
            0.0,
            false,
            Some(&archive),
        )
        .unwrap();
        assert!(
            recorded.snaps.lock().unwrap().is_empty(),
            "a non-head epoch-boundary block must not replace the marker"
        );

        finish_imported(
            &mut store,
            &mut residency,
            &head_store,
            &event_tx,
            &metrics,
            &mut snap_seq,
            &head_signed,
            &head_ssz,
            head_root,
            0.0,
            false,
            Some(&archive),
        )
        .unwrap();
        let snaps = recorded.snaps.lock().unwrap();
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].slot, Minimal::SLOTS_PER_EPOCH);
        assert_eq!(snaps[0].state_root, seam_root(head_state_root));
        let _ = config;
    }

    /// A dropped boundary snapshot is retried while the window is still one epoch.
    #[test]
    fn dropped_boundary_snapshot_retries_while_the_window_is_one_epoch() {
        let (mut store, config, _request, _child) = persist_retry_child();
        let anchor = store.finalized_checkpoint().root;
        let state_root = Root::from_array([0xA1; 32]);
        let (signed, block_root, ssz) = signed_boundary(anchor, state_root);
        install_boundary(
            &mut store,
            anchor,
            block_root,
            state_root,
            wrap_marker_state(1),
        );
        store.set_proposer_boost_root(block_root);
        assert_eq!(get_head(&mut store).unwrap().0, block_root);
        assert_eq!(
            signed.message.slot.as_u64(),
            Minimal::SLOTS_PER_EPOCH,
            "anchor slot 0 to this block is exactly one epoch"
        );
        assert!(is_fully_imported(&store, &block_root));

        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let head = HeadSnapshotStore::new();
        head.set_durable_head(block_root);
        let mut residency = crate::residency::Residency::<Minimal>::new(64, 32);
        let mut snap_seq = 0u64;
        let recorded = Arc::new(SnapRec {
            snaps: std::sync::Mutex::new(Vec::new()),
            attempts: std::sync::atomic::AtomicU32::new(0),
            fail_first: 1,
            durable: std::sync::Mutex::new(vec![seam_root(block_root)]),
        });
        let archive: ArchiveWriteHandle = recorded.clone();
        let request = ImportBlockRequest {
            ssz,
            fork: 0,
            root: block_root.as_slice().to_vec(),
            source: 0,
        };

        let first = import_recorded(
            &mut store,
            &config,
            &head,
            &metrics,
            &mut residency,
            &mut snap_seq,
            request.clone(),
            &archive,
        );
        assert!(
            first.is_err(),
            "the dropped snapshot fails the duplicate import: {first:?}"
        );
        assert_eq!(recorded.attempts.load(Ordering::SeqCst), 1);
        assert!(recorded.snaps.lock().unwrap().is_empty());

        let second = import_recorded(
            &mut store,
            &config,
            &head,
            &metrics,
            &mut residency,
            &mut snap_seq,
            request,
            &archive,
        )
        .expect("duplicate path retries the snapshot");
        assert_eq!(
            second.response.verdict,
            ImportBlockVerdict::Duplicate as i32
        );
        assert_eq!(recorded.attempts.load(Ordering::SeqCst), 2);
        let snaps = recorded.snaps.lock().unwrap();
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].slot, Minimal::SLOTS_PER_EPOCH);
        assert_eq!(snaps[0].state_root, seam_root(state_root));
    }

    fn anchor_slot_distance() -> u64 {
        Minimal::SLOTS_PER_EPOCH
    }

    fn persist_retry_config() -> ChainConfig {
        use cc_types::config::{BlobParameters, BlobSchedule, PresetName};
        use cc_types::primitives::{ExecutionAddress, ForkVersion};
        ChainConfig {
            preset_base: PresetName::Minimal,
            config_name: "minimal".into(),
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
            electra_fork_epoch: Epoch::new(0),
            fulu_fork_version: ForkVersion::from_array([0x06, 0x00, 0x00, 0x01]),
            fulu_fork_epoch: Epoch::new(0),
            seconds_per_slot: 6,
            blob_schedule: BlobSchedule::try_from_entries(vec![BlobParameters {
                epoch: Epoch::new(0),
                max_blobs_per_block: 9,
            }])
            .unwrap(),
            deposit_chain_id: 0,
            deposit_contract_address: ExecutionAddress::ZERO,
            churn_limit_quotient: 32,
            min_per_epoch_churn_limit_electra: 64_000_000_000,
            max_per_epoch_activation_exit_churn_limit: 128_000_000_000,
            shard_committee_period: Epoch::new(64),
            max_blobs_per_block_electra: 9,
        }
    }

    fn persist_retry_child() -> (
        cc_fork_choice::Store<Minimal>,
        ChainConfig,
        ImportBlockRequest,
        Root,
    ) {
        use cc_fork_choice::HarnessAvailability;
        use std::sync::Arc;
        persist_retry_child_da(Arc::new(HarnessAvailability))
    }

    fn persist_retry_child_da(
        da: Arc<dyn cc_fork_choice::DataAvailability>,
    ) -> (
        cc_fork_choice::Store<Minimal>,
        ChainConfig,
        ImportBlockRequest,
        Root,
    ) {
        use cc_fork_choice::{get_forkchoice_store, on_tick};
        use cc_types::BeaconBlockBody;
        use cc_types::containers::Validator;
        use cc_types::primitives::{BlsPublicKey, Gwei};
        use ssz_types::FixedVector;
        use std::sync::Arc;

        let config = persist_retry_config();
        let mut state = BeaconState::<Minimal>::default();
        state.set_genesis_time(0);
        state.set_slot(Slot::new(0));
        for i in 0u8..3 {
            let mut raw = [0u8; 48];
            raw[0] = i.saturating_add(1);
            state
                .validators_push(Validator {
                    pubkey: BlsPublicKey::from_array(raw),
                    ..Validator::default()
                })
                .unwrap();
            state.balances_push(Gwei::new(32_000_000_000)).unwrap();
        }
        let committee_keys: Vec<BlsPublicKey> = (0..Minimal::SYNC_COMMITTEE_SIZE as usize)
            .map(|i| {
                let mut raw = [0u8; 48];
                raw[0] = (i % 3) as u8 + 1;
                BlsPublicKey::from_array(raw)
            })
            .collect();
        let committee = cc_types::containers::SyncCommittee {
            pubkeys: FixedVector::new(committee_keys.clone()).expect("sync committee size"),
            aggregate_pubkey: committee_keys[0],
        };
        state.set_current_sync_committee(committee.clone());
        state.set_next_sync_committee(committee);
        let body = BeaconBlockBody::<Minimal>::default();
        let body_root = Root::from_hash256(TreeHash::tree_hash_root(&body));
        state.set_latest_block_header(BeaconBlockHeader {
            slot: Slot::new(0),
            proposer_index: ValidatorIndex::new(0),
            parent_root: Root::ZERO,
            state_root: Root::ZERO,
            body_root,
        });
        let state_root = state.canonical_root();
        let mut header = *state.latest_block_header();
        header.state_root = state_root;
        state.set_latest_block_header(header);
        let anchor_block = BeaconBlock {
            slot: Slot::new(0),
            proposer_index: ValidatorIndex::new(0),
            parent_root: Root::ZERO,
            state_root,
            body,
        };
        let mut store = get_forkchoice_store(
            state.clone(),
            &anchor_block,
            Arc::new(AcceptEngine),
            da,
            config.seconds_per_slot,
        )
        .unwrap();
        on_tick(&mut store, config.seconds_per_slot.saturating_mul(2)).unwrap();
        let anchor_root = Root::from_hash256(TreeHash::tree_hash_root(&anchor_block));
        let (request, true_root) = valid_child_request(&store, anchor_root, &config, 0);
        (store, config, request, true_root)
    }

    fn valid_child_request(
        store: &cc_fork_choice::Store<Minimal>,
        parent_root: Root,
        config: &ChainConfig,
        block_number_bump: u64,
    ) -> (ImportBlockRequest, Root) {
        use cc_crypto::INFINITY_SIGNATURE;
        use cc_state_transition::{
            TransitionContext, get_beacon_proposer_index, get_current_epoch,
            get_expected_withdrawals, get_randao_mix, process_slots,
        };
        use cc_types::containers::SyncAggregate;
        use cc_types::execution::ExecutionPayload;
        use cc_types::{BeaconBlockBody, SignedBeaconBlock};
        use ssz_types::VariableList;

        let parent_state = store.block_state(&parent_root).unwrap().clone();
        let engine = AcceptEngine;
        let ctx = TransitionContext::new(config, &engine);
        ctx.top_up_pubkey_cache(&parent_state);
        let mut st = parent_state.clone();
        let next_slot = Slot::new(st.slot().as_u64() + 1);
        let _ = process_slots(&mut st, next_slot, config).expect("process_slots");
        let proposer = get_beacon_proposer_index(&st).expect("proposer");
        let (withdrawals, _) = get_expected_withdrawals(&st).expect("withdrawals");
        let epoch = get_current_epoch(&st);
        let prev_randao = get_randao_mix(&st, epoch).expect("randao");
        let timestamp = cc_state_transition::compute_time_at_slot(
            st.genesis_time(),
            next_slot,
            config.seconds_per_slot,
        );
        let parent_hash = st.latest_execution_payload_header().block_hash;
        let payload = ExecutionPayload::<Minimal> {
            parent_hash,
            prev_randao,
            timestamp,
            block_number: st.latest_execution_payload_header().block_number + 1 + block_number_bump,
            gas_limit: st.latest_execution_payload_header().gas_limit,
            withdrawals: VariableList::new(withdrawals).expect("withdrawals list"),
            ..Default::default()
        };
        let body = BeaconBlockBody::<Minimal> {
            execution_payload: payload,
            eth1_data: st.eth1_data(),
            sync_aggregate: SyncAggregate {
                sync_committee_bits: Default::default(),
                sync_committee_signature: cc_types::primitives::BlsSignature::from_array(
                    INFINITY_SIGNATURE,
                ),
            },
            ..Default::default()
        };
        let mut message = BeaconBlock {
            slot: next_slot,
            proposer_index: proposer,
            parent_root,
            state_root: Root::ZERO,
            body,
        };
        let trial_signed = SignedBeaconBlock {
            message: message.clone(),
            signature: Default::default(),
        };
        let mut trial = parent_state;
        match cc_state_transition::state_transition(
            &mut trial,
            &trial_signed,
            &ctx,
            BlockSignatureStrategy::NoVerification,
        ) {
            Err(cc_state_transition::BlockError::StateRootMismatch { actual, .. }) => {
                message.state_root = actual;
            }
            Ok(()) => {
                message.state_root = trial.canonical_root();
            }
            Err(e) => panic!("state_transition for fixture: {e}"),
        }
        let child = SignedBeaconBlock {
            message,
            signature: Default::default(),
        };
        let true_root = Root::from_hash256(TreeHash::tree_hash_root(&child.message));
        let request = ImportBlockRequest {
            ssz: encode_signed_block(&child),
            fork: 0,
            root: true_root.as_slice().to_vec(),
            source: 0,
        };
        (request, true_root)
    }

    fn wrap_marker_state(marker: u64) -> BeaconState<Minimal> {
        let mut state = BeaconState::<Minimal>::default();
        state.set_slot(Slot::new(0));
        for i in 0..state.proposer_lookahead_len() {
            state
                .proposer_lookahead_set(i, ValidatorIndex::new(marker))
                .unwrap();
        }
        state
    }

    #[test]
    fn parse_root_rejects_wrong_length() {
        let err = parse_root(&[0u8; 16]).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn encode_decode_roundtrip_empty_block() {
        let block = SignedBeaconBlock::<Minimal> {
            message: Default::default(),
            signature: Default::default(),
        };
        let bytes = encode_signed_block(&block);
        let decoded = decode_signed_block::<Minimal>(&bytes, 0).unwrap();
        assert_eq!(decoded.message.slot, block.message.slot);
    }

    #[tokio::test]
    async fn snapshot_stored_before_event_deliverable() {
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let head = HeadSnapshotStore::new();
        let (tx, mut rx) = mpsc::channel(4);
        let root = Root::from_array([0x11; 32]);
        let snap = HeadSnapshot {
            head_root: root,
            head_slot: Slot::new(7),
            sequence: 3,
            ..HeadSnapshot::default()
        };
        publish_snapshot_then_events(&head, &tx, &metrics, snap, 7, root);
        // Snapshot must already be visible before we read any event.
        assert_eq!(head.load().sequence, 3);
        assert_eq!(head.load().head_root, root);
        let ev = rx.recv().await.expect("block_imported");
        assert_eq!(ev.kind, cc_proto::chain::EventKind::BlockImported);
        assert_eq!(
            ev.payload.first().copied(),
            Some(BLOCK_PAYLOAD_VERDICT_IMPORTED)
        );
        let ev = rx.recv().await.expect("head");
        assert_eq!(ev.kind, cc_proto::chain::EventKind::Head);
        assert_eq!(ev.payload.len(), 8);
        assert_eq!(head.load().sequence, 3);
    }

    /// F3: `ForkChoiceScalarsPayload` layout matches store meta field order/size.
    ///
    /// Storage decodes with `cc_store::meta::ForkChoiceScalars` — both must be
    /// 240-byte fixed containers with identical field order (see type doc).
    #[test]
    fn fork_choice_scalars_payload_layout_matches_store_meta() {
        use cc_types::primitives::Epoch;

        let v = ForkChoiceScalarsPayload {
            time: 0x0102_0304_0506_0708,
            proposer_boost_root: Root::from_array([0x11; 32]),
            justified: Checkpoint {
                epoch: Epoch::new(3),
                root: Root::from_array([0x22; 32]),
            },
            finalized: Checkpoint {
                epoch: Epoch::new(2),
                root: Root::from_array([0x33; 32]),
            },
            unrealized_justified: Checkpoint {
                epoch: Epoch::new(3),
                root: Root::from_array([0x44; 32]),
            },
            unrealized_finalized: Checkpoint {
                epoch: Epoch::new(1),
                root: Root::from_array([0x55; 32]),
            },
            head_root: Root::from_array([0x66; 32]),
            head_slot: Slot::new(99),
        };
        let bytes = v.as_ssz_bytes();
        assert_eq!(
            bytes.len(),
            FORK_CHOICE_SCALARS_SSZ_LEN,
            "fixed SSZ length must match store ForkChoiceScalars (240 B)"
        );
        // Field order: time | proposer_boost | justified | finalized | …
        assert_eq!(&bytes[0..8], &0x0102_0304_0506_0708u64.to_le_bytes());
        assert_eq!(&bytes[8..40], &[0x11u8; 32]);
        assert_eq!(&bytes[40..48], &3u64.to_le_bytes()); // justified.epoch
        assert_eq!(&bytes[48..80], &[0x22u8; 32]); // justified.root
        assert_eq!(&bytes[80..88], &2u64.to_le_bytes()); // finalized.epoch
        assert_eq!(&bytes[88..120], &[0x33u8; 32]);
        // head_slot is the last 8 bytes.
        assert_eq!(&bytes[232..240], &99u64.to_le_bytes());
        // Default encodes to the same length (store Default path).
        assert_eq!(
            ForkChoiceScalarsPayload::default().as_ssz_bytes().len(),
            FORK_CHOICE_SCALARS_SSZ_LEN
        );
    }

    /// SEC-44a-2: oversize payload is rejected with metric, not enqueued.
    #[test]
    fn oversize_payload_rejected_before_enqueue() {
        use crate::events::MAX_EVENT_PAYLOAD_BYTES;

        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let (tx, mut rx) = mpsc::channel(4);
        let huge = vec![0u8; MAX_EVENT_PAYLOAD_BYTES + 1];
        let input = EventInput::block_imported_with_payload(
            1,
            Bytes::from(vec![0u8; 32]),
            Bytes::from(huge),
        );
        assert!(!input.payload_within_cap());
        try_publish_event(&tx, &metrics, input);
        assert_eq!(metrics.event_payload_rejected_count(), 1);
        assert!(
            rx.try_recv().is_err(),
            "oversize must not enter the channel"
        );
    }

    /// F2: block_imported_payload preserves arrival bytes after the discriminator.
    #[test]
    fn block_imported_payload_preserves_arrival_bytes() {
        let arrival = b"wire-ssz-bytes-not-reencoded";
        let payload = block_imported_payload(BlockImportedPayloadVerdict::Imported, arrival);
        assert_eq!(payload[0], BLOCK_PAYLOAD_VERDICT_IMPORTED);
        assert_eq!(&payload[1..], arrival.as_slice());
        let (v, rest) = split_block_imported_payload(&payload).unwrap();
        assert_eq!(v, BlockImportedPayloadVerdict::Imported);
        assert_eq!(rest, arrival);
        assert!(split_block_imported_payload(&[]).is_none());
        assert!(split_block_imported_payload(&[0]).is_none());
        assert_eq!(ImportReason::FutureSlot.to_wire(), "future_slot");
        assert_eq!(
            ImportReason::ExecutionEngineUnavailable.to_wire(),
            "execution_engine_unavailable"
        );
    }

    /// Exhaustive `OnBlockError` → class (no catch-all) — R-13 / §5.3.
    #[test]
    fn on_block_error_gossip_class_is_total() {
        use cc_fork_choice::ProtoArrayError;
        use cc_state_transition::{BlockError, SignatureKind};
        use cc_types::primitives::{Hash256, Slot, ValidatorIndex};

        let samples: &[(OnBlockError, GossipClass)] = &[
            (OnBlockError::NotDescendedFromFinalized, GossipClass::Reject),
            (
                OnBlockError::Transition(BlockError::InvalidSignature {
                    which: SignatureKind::BlockProposer,
                }),
                GossipClass::Reject,
            ),
            (
                OnBlockError::Transition(BlockError::StateRootMismatch {
                    expected: Root::ZERO,
                    actual: Root::from_array([1u8; 32]),
                }),
                GossipClass::Reject,
            ),
            (
                OnBlockError::Transition(BlockError::Engine(
                    cc_state_transition::EngineError::Transport("x".into()),
                )),
                GossipClass::Internal,
            ),
            (
                OnBlockError::Transition(BlockError::CachePoisoned),
                GossipClass::Internal,
            ),
            (
                OnBlockError::ProtoArray(ProtoArrayError::UnknownParent(Root::ZERO)),
                GossipClass::Internal,
            ),
            (OnBlockError::PulledUpTip("x".into()), GossipClass::Internal),
            (
                OnBlockError::Transition(BlockError::UnknownParent),
                GossipClass::Ignore,
            ),
            (
                OnBlockError::Transition(BlockError::FutureSlot {
                    block_slot: Slot::new(2),
                    current_slot: Slot::new(1),
                }),
                GossipClass::Ignore,
            ),
            (
                OnBlockError::Transition(BlockError::ProposerMismatch {
                    block: ValidatorIndex::new(0),
                    expected: ValidatorIndex::new(1),
                }),
                GossipClass::Reject,
            ),
            // CC-34b / §4.8 — EL consensus failure is Internal, not peer descore.
            (
                OnBlockError::Validation(
                    cc_fork_choice::ValidationError::ValidExecutionStatusBecameInvalid {
                        block_root: Root::ZERO,
                        payload_block_hash: Hash256::ZERO,
                    },
                ),
                GossipClass::Internal,
            ),
        ];
        for (err, expected) in samples {
            assert_eq!(
                on_block_error_gossip_class(err),
                *expected,
                "mismatch for {err:?}"
            );
            // Cross-check against OnBlockError's own method when present.
            assert_eq!(err.gossip_class(), *expected, "OnBlockError method {err:?}");
        }
    }

    /// M13 / S2-A-11: import tops up the context map and reports that length,
    /// so a populated registry does not fire the short-cache alarm.
    #[test]
    fn import_block_with_early_reports_topped_up_pubkey_cache() {
        use crate::residency::Residency;
        use cc_fork_choice::{HarnessAvailability, get_forkchoice_store, on_tick};
        use cc_types::config::{BlobParameters, BlobSchedule, PresetName};
        use cc_types::containers::Validator;
        use cc_types::primitives::{BlsPublicKey, ExecutionAddress, ForkVersion};
        use std::sync::Arc;

        #[derive(Debug, Default, Clone, Copy)]
        struct M13AcceptEngine;
        impl<P: Preset> cc_state_transition::ExecutionEngine<P> for M13AcceptEngine {
            fn verify_and_notify_new_payload(
                &self,
                _request: cc_state_transition::NewPayloadRequest<'_, P>,
            ) -> Result<cc_state_transition::PayloadStatus, cc_state_transition::EngineError>
            {
                Ok(cc_state_transition::PayloadStatus::Valid)
            }
        }

        let config = ChainConfig {
            preset_base: PresetName::Minimal,
            config_name: "minimal".into(),
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
            electra_fork_epoch: Epoch::new(0),
            fulu_fork_version: ForkVersion::from_array([0x06, 0x00, 0x00, 0x01]),
            fulu_fork_epoch: Epoch::new(0),
            seconds_per_slot: 6,
            blob_schedule: BlobSchedule::try_from_entries(vec![BlobParameters {
                epoch: Epoch::new(0),
                max_blobs_per_block: 9,
            }])
            .unwrap(),
            deposit_chain_id: 0,
            deposit_contract_address: ExecutionAddress::ZERO,
            churn_limit_quotient: 32,
            min_per_epoch_churn_limit_electra: 64_000_000_000,
            max_per_epoch_activation_exit_churn_limit: 128_000_000_000,
            shard_committee_period: Epoch::new(64),
            max_blobs_per_block_electra: 9,
        };

        let mut state = BeaconState::<Minimal>::default();
        state.set_genesis_time(0);
        state.set_slot(Slot::new(0));
        for i in 0u8..3 {
            let mut raw = [0u8; 48];
            raw[0] = i.saturating_add(1);
            state
                .validators_push(Validator {
                    pubkey: BlsPublicKey::from_array(raw),
                    ..Validator::default()
                })
                .unwrap();
        }
        assert!(state.validators_len() > 0);

        let anchor_block = BeaconBlock {
            slot: Slot::new(0),
            proposer_index: ValidatorIndex::new(0),
            parent_root: Root::ZERO,
            state_root: Root::ZERO,
            body: Default::default(),
        };
        let mut store = get_forkchoice_store(
            state,
            &anchor_block,
            Arc::new(M13AcceptEngine),
            Arc::new(HarnessAvailability),
            config.seconds_per_slot,
        )
        .unwrap();
        on_tick(&mut store, config.seconds_per_slot * 2).unwrap();
        let anchor_root = Root::from_hash256(TreeHash::tree_hash_root(&anchor_block));
        let parent = store.block_state(&anchor_root).unwrap();
        let validators_len = parent.validators_len();
        assert!(validators_len > 0);

        let child = SignedBeaconBlock::<Minimal> {
            message: BeaconBlock {
                slot: Slot::new(1),
                proposer_index: ValidatorIndex::new(0),
                parent_root: anchor_root,
                state_root: Root::ZERO,
                body: Default::default(),
            },
            signature: Default::default(),
        };
        let true_root = Root::from_hash256(TreeHash::tree_hash_root(&child.message));
        let request = ImportBlockRequest {
            ssz: encode_signed_block(&child),
            fork: 0,
            root: true_root.as_slice().to_vec(),
            source: 0,
        };

        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let head = HeadSnapshotStore::new();
        let (event_tx, _) = mpsc::channel(4);
        let counters = ImportCounters::default();
        let mut residency = Residency::<Minimal>::new(64, 32);
        let mut snap_seq = 0u64;
        let _ = import_block_with_early(
            &mut store,
            &mut residency,
            &config,
            &head,
            &event_tx,
            &metrics,
            &counters,
            &mut snap_seq,
            request,
            BlockSignatureStrategy::NoVerification,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(metrics.pubkey_cache_len_value() as usize, validators_len);
        assert!(
            !metrics.pubkey_cache_alert_firing(),
            "topped-up context map must match the parent registry"
        );
    }

    /// M2: persist Err after `on_block` must not report success, and a later
    /// DUPLICATE retry must still persist.
    #[test]
    fn persist_fail_after_import_retries_on_duplicate() {
        use crate::ArchiveWriteHandle;
        use crate::residency::Residency;
        use cc_seam::{ArchiveWrite, DurableImport, SeamError};
        use std::sync::atomic::AtomicU32;
        use std::sync::{Arc, Mutex};

        #[derive(Debug)]
        struct FailThenRecord {
            fails_left: AtomicU32,
            persisted: Mutex<Vec<[u8; 32]>>,
        }

        #[async_trait::async_trait]
        impl ArchiveWrite for FailThenRecord {
            async fn ingest_columns(&self, _batch: cc_seam::ColumnBatch) -> Result<(), SeamError> {
                Ok(())
            }

            fn commit_import_blocking(&self, import: DurableImport) -> Result<(), SeamError> {
                if self.fails_left.load(Ordering::SeqCst) > 0 {
                    self.fails_left.fetch_sub(1, Ordering::SeqCst);
                    return Err(SeamError::Unavailable("injected persist fail".into()));
                }
                self.persisted.lock().unwrap().push(import.block_root);
                Ok(())
            }
        }

        let (mut store, config, request, true_root) = persist_retry_child();

        let archive_impl = Arc::new(FailThenRecord {
            fails_left: AtomicU32::new(1),
            persisted: Mutex::new(Vec::new()),
        });
        let archive: ArchiveWriteHandle = archive_impl.clone();
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let head = HeadSnapshotStore::new();
        let (event_tx, _) = mpsc::channel(4);
        let counters = ImportCounters::default();
        let mut residency = Residency::<Minimal>::new(64, 32);
        let mut snap_seq = 0u64;

        let first = import_block_with_early(
            &mut store,
            &mut residency,
            &config,
            &head,
            &event_tx,
            &metrics,
            &counters,
            &mut snap_seq,
            request.clone(),
            BlockSignatureStrategy::NoVerification,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(&archive),
        );
        assert!(
            first.is_err(),
            "persist fail after on_block must not report success: {first:?}"
        );
        assert!(
            is_fully_imported(&store, &true_root),
            "on_block already applied; retry must see DUPLICATE"
        );
        assert_ne!(
            store.cached_head_root(),
            Some(true_root),
            "a failed head write restores the pre-call latch"
        );
        assert_eq!(
            snap_seq, 0,
            "the failing call does not publish a head snapshot"
        );
        assert_eq!(head.load().sequence, 0);

        let second = import_block_with_early(
            &mut store,
            &mut residency,
            &config,
            &head,
            &event_tx,
            &metrics,
            &counters,
            &mut snap_seq,
            request,
            BlockSignatureStrategy::NoVerification,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(&archive),
        )
        .expect("duplicate retry must persist then return DUPLICATE");
        assert_eq!(
            second.response.verdict,
            ImportBlockVerdict::Duplicate as i32
        );
        assert!(!second.transition_invoked);
        let persisted = archive_impl.persisted.lock().unwrap();
        assert_eq!(persisted.len(), 1, "retry must persist the missing rows");
        assert_eq!(persisted[0], seam_root(true_root));
    }

    /// H3: after A then B are durable, re-import A must not persist again.
    #[test]
    fn duplicate_already_durable_ancestor_does_not_repersist() {
        use crate::ArchiveWriteHandle;
        use crate::residency::Residency;
        use cc_fork_choice::on_tick;
        use cc_seam::{ArchiveWrite, DaVerdict, DurableImport, HeadCause, SeamError};
        use std::collections::HashSet;
        use std::sync::{Arc, Mutex};

        #[derive(Debug, Default)]
        struct RecordDurable {
            persisted: Mutex<Vec<DurableImport>>,
        }

        #[async_trait::async_trait]
        impl ArchiveWrite for RecordDurable {
            async fn ingest_columns(&self, _batch: cc_seam::ColumnBatch) -> Result<(), SeamError> {
                Ok(())
            }

            fn commit_import_blocking(&self, import: DurableImport) -> Result<(), SeamError> {
                self.persisted.lock().unwrap().push(import);
                Ok(())
            }

            fn block_is_durable(&self, root: cc_seam::Root) -> Result<bool, SeamError> {
                Ok(self
                    .persisted
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|row| row.block_root == root))
            }
        }

        let (mut store, config, req_a, root_a) = persist_retry_child();
        let archive_impl = Arc::new(RecordDurable::default());
        let archive: ArchiveWriteHandle = archive_impl.clone();
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let head = HeadSnapshotStore::new();
        let (event_tx, _) = mpsc::channel(4);
        let counters = ImportCounters::default();
        let mut residency = Residency::<Minimal>::new(64, 32);
        let mut snap_seq = 0u64;

        let first = import_block_with_early(
            &mut store,
            &mut residency,
            &config,
            &head,
            &event_tx,
            &metrics,
            &counters,
            &mut snap_seq,
            req_a.clone(),
            BlockSignatureStrategy::NoVerification,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(&archive),
        )
        .expect("import A");
        assert_eq!(first.response.verdict, ImportBlockVerdict::Imported as i32);

        on_tick(&mut store, config.seconds_per_slot.saturating_mul(3)).unwrap();
        let (req_b, root_b) = valid_child_request(&store, root_a, &config, 0);
        let second = import_block_with_early(
            &mut store,
            &mut residency,
            &config,
            &head,
            &event_tx,
            &metrics,
            &counters,
            &mut snap_seq,
            req_b,
            BlockSignatureStrategy::NoVerification,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(&archive),
        )
        .expect("import B");
        assert_eq!(second.response.verdict, ImportBlockVerdict::Imported as i32);

        let replay = import_block_with_early(
            &mut store,
            &mut residency,
            &config,
            &head,
            &event_tx,
            &metrics,
            &counters,
            &mut snap_seq,
            req_a,
            BlockSignatureStrategy::NoVerification,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(&archive),
        )
        .expect("re-import A is DUPLICATE");
        assert_eq!(
            replay.response.verdict,
            ImportBlockVerdict::Duplicate as i32
        );
        assert!(!replay.transition_invoked);
        let persisted = archive_impl.persisted.lock().unwrap();
        assert_eq!(
            persisted.len(),
            2,
            "already-durable A must not be committed again: {persisted:?}"
        );
        let set: HashSet<_> = persisted.iter().map(|row| row.block_root).collect();
        assert!(set.contains(&seam_root(root_a)));
        assert!(set.contains(&seam_root(root_b)));
        let first = &persisted[0];
        assert_eq!(first.da, DaVerdict::Available);
        assert_eq!(first.block_root, seam_root(root_a));
        assert_ne!(
            first.parent_root, first.block_root,
            "SSZ parent is stored as-is; a zero parent is not remapped"
        );
        let head = first.head.as_ref().expect("import A moves head onto A");
        assert_eq!(head.head_root, first.block_root);
        assert_eq!(head.head_slot, first.slot);
        assert_eq!(head.cause, HeadCause::Import);
    }

    fn test_root(b: u8) -> Root {
        let mut a = [0u8; 32];
        a[0] = b;
        Root::from_array(a)
    }

    fn insert_child(store: &mut Store<Minimal>, parent: Root, child: Root, slot: u64) {
        let justified = store.justified_checkpoint();
        let finalized = store.finalized_checkpoint();
        let mut child_state = store.block_state(&parent).unwrap().clone();
        child_state.set_slot(Slot::new(slot));
        store.insert_block(
            child,
            BeaconBlockHeader {
                slot: Slot::new(slot),
                proposer_index: ValidatorIndex::new(0),
                parent_root: parent,
                state_root: Root::ZERO,
                body_root: Root::ZERO,
            },
            child_state,
        );
        store
            .proto_array_mut()
            .on_block(ProtoNodeBlock {
                slot: Slot::new(slot),
                root: child,
                parent_root: Some(parent),
                state_root: Root::ZERO,
                target_root: child,
                justified_checkpoint: justified,
                finalized_checkpoint: finalized,
                unrealized_justified_checkpoint: justified,
                unrealized_finalized_checkpoint: finalized,
                execution_status: ExecutionStatus::Valid,
                execution_block_hash: Hash256::ZERO,
            })
            .unwrap();
    }

    fn drain_finalized(rx: &mut mpsc::Receiver<EventInput>) -> usize {
        let mut n = 0;
        while let Ok(ev) = rx.try_recv() {
            if ev.kind == cc_proto::chain::EventKind::FinalizedCheckpoint {
                n += 1;
            }
        }
        n
    }

    /// P0-10 / S0-A-21: two FINALIZED publishes shrink the proto-array.
    #[test]
    fn two_finalized_events_decrease_proto_array_node_count() {
        let mut state = BeaconState::<Minimal>::default();
        state.set_genesis_time(0);
        state.set_slot(Slot::new(0));
        let anchor_block = BeaconBlock {
            slot: Slot::new(0),
            proposer_index: ValidatorIndex::new(0),
            parent_root: Root::ZERO,
            state_root: Root::ZERO,
            body: Default::default(),
        };
        let mut store = get_forkchoice_store(
            state,
            &anchor_block,
            Arc::new(AcceptEngine),
            Arc::new(HarnessAvailability),
            6,
        )
        .unwrap();
        let anchor = Root::from_hash256(TreeHash::tree_hash_root(&anchor_block));
        let a = test_root(0xA1);
        let b = test_root(0xB1);
        let side = test_root(0x51);
        let c = test_root(0xC1);
        let d = test_root(0xD1);
        insert_child(&mut store, anchor, a, 1);
        insert_child(&mut store, a, b, 8);
        insert_child(&mut store, a, side, 8);
        insert_child(&mut store, b, c, 9);
        let before_first = store.proto_array().len();
        assert_eq!(before_first, 5);

        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let (tx, mut rx) = mpsc::channel(16);
        let prev0 = store.finalized_checkpoint();
        store.update_checkpoints(
            Checkpoint {
                epoch: Epoch::new(1),
                root: b,
            },
            Checkpoint {
                epoch: Epoch::new(1),
                root: b,
            },
        );
        let f1 = store.finalized_checkpoint();
        publish_import_events(
            &tx,
            &metrics,
            &mut store,
            9,
            c,
            &[],
            BlockImportedPayloadVerdict::Imported,
            c,
            Slot::new(9),
            None,
            prev0,
            f1,
            Root::ZERO,
        );
        let after_first = store.proto_array().len();
        assert!(
            after_first < before_first,
            "first FINALIZED must prune (before={before_first} after={after_first})"
        );
        assert_eq!(drain_finalized(&mut rx), 1);
        assert!(!store.proto_array().contains(&side));

        insert_child(&mut store, c, d, 16);
        let before_second = store.proto_array().len();
        let prev1 = store.finalized_checkpoint();
        store.update_checkpoints(
            Checkpoint {
                epoch: Epoch::new(2),
                root: d,
            },
            Checkpoint {
                epoch: Epoch::new(2),
                root: d,
            },
        );
        let f2 = store.finalized_checkpoint();
        publish_import_events(
            &tx,
            &metrics,
            &mut store,
            16,
            d,
            &[],
            BlockImportedPayloadVerdict::Imported,
            d,
            Slot::new(16),
            None,
            prev1,
            f2,
            Root::ZERO,
        );
        let after_second = store.proto_array().len();
        assert!(
            after_second < before_second,
            "second FINALIZED must prune (before={before_second} after={after_second})"
        );
        assert_eq!(drain_finalized(&mut rx), 1);
        assert!(store.proto_array().contains(&d));
        assert!(!store.proto_array().contains(&b));
    }

    /// S0-A-26 / P0-11: a slot past the Fulu window is unknown, not `slot % SPE`.
    #[test]
    fn proposer_from_state_lookahead_none_three_epochs_past() {
        let state = wrap_marker_state(99);
        assert_eq!(
            proposer_from_state_lookahead::<Minimal>(&state, 0),
            Some(99)
        );
        let last_in_window = (state.proposer_lookahead_len() as u64).saturating_sub(1);
        assert_eq!(
            proposer_from_state_lookahead::<Minimal>(&state, last_in_window),
            Some(99)
        );
        let gap_slot = 3 * Minimal::SLOTS_PER_EPOCH;
        assert!(
            (gap_slot as usize) >= state.proposer_lookahead_len(),
            "fixture must sit outside the Fulu window"
        );
        assert_eq!(
            proposer_from_state_lookahead::<Minimal>(&state, gap_slot),
            None,
            "must not wrap to the current-epoch row"
        );
    }

    /// S0-A-26: lookup reads the parent state's window, not the head's.
    #[test]
    fn expected_proposer_index_prefers_parent_state() {
        let head_state = wrap_marker_state(1);
        let anchor = BeaconBlock {
            slot: Slot::new(0),
            proposer_index: ValidatorIndex::new(0),
            parent_root: Root::ZERO,
            state_root: Root::ZERO,
            body: Default::default(),
        };
        let mut store = get_forkchoice_store(
            head_state,
            &anchor,
            Arc::new(AcceptEngine),
            Arc::new(HarnessAvailability),
            6,
        )
        .unwrap();
        let head_root = Root::from_hash256(TreeHash::tree_hash_root(&anchor));
        let _ = get_head(&mut store).unwrap();

        let parent_root = Root::from_array([0xAB; 32]);
        let parent_state = wrap_marker_state(2);
        store.insert_block(
            parent_root,
            BeaconBlockHeader {
                slot: Slot::new(0),
                proposer_index: ValidatorIndex::new(0),
                parent_root: Root::ZERO,
                state_root: Root::ZERO,
                body_root: Root::ZERO,
            },
            parent_state,
        );

        assert_eq!(
            expected_proposer_index::<Minimal>(&store, 0, parent_root),
            Some(2),
            "parent lookahead must win over the head state's row"
        );
        assert_eq!(
            expected_proposer_index::<Minimal>(&store, 0, head_root),
            Some(1)
        );
        assert_eq!(
            expected_proposer_index::<Minimal>(&store, 3 * Minimal::SLOTS_PER_EPOCH, parent_root),
            None
        );
    }

    /// H1: an in-window head EpochContext must not override a parent miss.
    #[test]
    fn expected_proposer_index_ignores_head_epoch_context() {
        let head_state = wrap_marker_state(1);
        let anchor = BeaconBlock {
            slot: Slot::new(0),
            proposer_index: ValidatorIndex::new(0),
            parent_root: Root::ZERO,
            state_root: Root::ZERO,
            body: Default::default(),
        };
        let mut store = get_forkchoice_store(
            head_state,
            &anchor,
            Arc::new(AcceptEngine),
            Arc::new(HarnessAvailability),
            6,
        )
        .unwrap();
        let parent_root = Root::from_array([0xAB; 32]);
        store.insert_block(
            parent_root,
            BeaconBlockHeader {
                slot: Slot::new(0),
                proposer_index: ValidatorIndex::new(0),
                parent_root: Root::ZERO,
                state_root: Root::ZERO,
                body_root: Root::ZERO,
            },
            wrap_marker_state(2),
        );

        // Slot 24 is outside the slot-0 parent window (len=16) but inside an
        // epoch-3 head context. Parent miss must stay None — not Some(head).
        let gap_slot = 3 * Minimal::SLOTS_PER_EPOCH;
        assert_eq!(
            expected_proposer_index::<Minimal>(&store, gap_slot, parent_root),
            None
        );
        assert_eq!(
            expected_proposer_index::<Minimal>(&store, 0, parent_root),
            Some(2)
        );
    }

    /// L1: missing parent state is unknown — do not invent from last_head_root.
    #[test]
    fn expected_proposer_index_none_when_parent_state_missing() {
        let head_state = wrap_marker_state(1);
        let anchor = BeaconBlock {
            slot: Slot::new(0),
            proposer_index: ValidatorIndex::new(0),
            parent_root: Root::ZERO,
            state_root: Root::ZERO,
            body: Default::default(),
        };
        let mut store = get_forkchoice_store(
            head_state,
            &anchor,
            Arc::new(AcceptEngine),
            Arc::new(HarnessAvailability),
            6,
        )
        .unwrap();
        let _ = get_head(&mut store).unwrap();
        let missing = Root::from_array([0xCD; 32]);
        assert_eq!(
            expected_proposer_index::<Minimal>(&store, 0, missing),
            None,
            "must not fall back to the head state's in-window row"
        );
    }

    /// A refused commit (parent not durable, or store incomplete) must not
    /// advance the head, move residency, publish BLOCK_IMPORTED / head /
    /// finalized, or take the import-path fcU. The check is before `on_block`.
    #[test]
    fn refused_commit_leaves_no_visible_import_mutation() {
        use crate::residency::Residency;
        use cc_seam::{ArchiveWrite, FailedPreconditionReason, SeamError};
        use std::sync::Arc;

        struct Refuse {
            reason: FailedPreconditionReason,
        }

        #[async_trait::async_trait]
        impl ArchiveWrite for Refuse {
            async fn ingest_columns(&self, _batch: cc_seam::ColumnBatch) -> Result<(), SeamError> {
                Ok(())
            }

            fn import_precondition(&self, _parent_root: cc_seam::Root) -> Result<(), SeamError> {
                Err(SeamError::FailedPrecondition {
                    reason: self.reason,
                })
            }
        }

        for reason in [
            FailedPreconditionReason::ParentNotDurable,
            FailedPreconditionReason::StoreIncomplete,
        ] {
            let (mut store, config, request, true_root) = persist_retry_child();
            let archive: crate::ArchiveWriteHandle = Arc::new(Refuse { reason });
            let mut registry = Registry::default();
            let metrics = ChainMetrics::register(&mut registry);
            let head = HeadSnapshotStore::new();
            let head_before = head.load();
            let (event_tx, mut event_rx) = mpsc::channel(8);
            let counters = ImportCounters::default();
            let mut residency = Residency::<Minimal>::new(64, 32);
            let mut snap_seq = 0u64;

            let outcome = import_block_with_early(
                &mut store,
                &mut residency,
                &config,
                &head,
                &event_tx,
                &metrics,
                &counters,
                &mut snap_seq,
                request,
                BlockSignatureStrategy::NoVerification,
                None,
                None,
                None,
                None,
                None,
                None,
                Some(&archive),
            );
            // Import-path fcU (`emit_fcu_head`) runs only after `Ok`.
            let fcu_emissions = u32::from(outcome.is_ok());

            assert!(
                outcome
                    .as_ref()
                    .is_err_and(|e| e.to_string().contains(reason.as_str())),
                "{reason}: refused commit must be an ordinary Err, got {outcome:?}"
            );
            assert_eq!(
                counters.transition_count(),
                0,
                "{reason}: parent/store refusal is checked before on_block"
            );
            assert!(
                !is_fully_imported(&store, &true_root),
                "{reason}: on_block must not integrate the block"
            );
            assert_eq!(
                residency.resident_count(),
                0,
                "{reason}: residency must not move"
            );
            assert_eq!(
                residency.body_ring_len(),
                0,
                "{reason}: body ring must not move"
            );
            let published = head.load();
            assert_eq!(published.sequence, head_before.sequence);
            assert_eq!(
                published.head_root, head_before.head_root,
                "{reason}: head snapshot must not advance"
            );
            assert!(
                event_rx.try_recv().is_err(),
                "{reason}: no BLOCK_IMPORTED / head / finalized event"
            );
            assert_eq!(
                fcu_emissions, 0,
                "{reason}: no import-path fcU after a refused commit"
            );
        }
    }

    /// DA deferral writes `da: Deferred` with `head: None` before the park
    /// and the deferred `BLOCK_IMPORTED`. A refused commit does neither.
    #[test]
    fn deferred_da_commits_before_park_and_event() {
        use crate::da::PendingDa;
        use crate::residency::Residency;
        use cc_fork_choice::PeerDasAvailability;
        use cc_seam::{ArchiveWrite, DaVerdict, DurableImport, SeamError};
        use std::sync::{Arc, Mutex};

        struct Record {
            rows: Mutex<Vec<DurableImport>>,
            fail: bool,
        }

        #[async_trait::async_trait]
        impl ArchiveWrite for Record {
            async fn ingest_columns(&self, _batch: cc_seam::ColumnBatch) -> Result<(), SeamError> {
                Ok(())
            }

            fn commit_import_blocking(&self, import: DurableImport) -> Result<(), SeamError> {
                if self.fail {
                    return Err(SeamError::Unavailable("deferred commit refused".into()));
                }
                self.rows.lock().unwrap().push(import);
                Ok(())
            }
        }

        for fail in [false, true] {
            let (mut store, config, request, true_root) =
                persist_retry_child_da(Arc::new(PeerDasAvailability::new()));
            let archive_impl = Arc::new(Record {
                rows: Mutex::new(Vec::new()),
                fail,
            });
            let archive: crate::ArchiveWriteHandle = archive_impl.clone();
            let mut registry = Registry::default();
            let metrics = ChainMetrics::register(&mut registry);
            let head = HeadSnapshotStore::new();
            let (event_tx, mut event_rx) = mpsc::channel(8);
            let counters = ImportCounters::default();
            let mut residency = Residency::<Minimal>::new(64, 32);
            let mut snap_seq = 0u64;
            let mut pending = PendingDa::new();

            let outcome = import_block_with_early(
                &mut store,
                &mut residency,
                &config,
                &head,
                &event_tx,
                &metrics,
                &counters,
                &mut snap_seq,
                request,
                BlockSignatureStrategy::NoVerification,
                None,
                None,
                None,
                Some(&mut pending),
                None,
                None,
                Some(&archive),
            );

            assert!(
                !is_fully_imported(&store, &true_root),
                "a DA deferral must not integrate"
            );
            assert_eq!(residency.resident_count(), 0);
            assert_eq!(head.load().sequence, 0);

            if fail {
                assert!(outcome.is_err(), "refused deferred commit is an Err");
                assert!(
                    pending.is_empty(),
                    "a refused deferred commit must not park"
                );
                assert!(event_rx.try_recv().is_err(), "no deferred event");
                assert!(archive_impl.rows.lock().unwrap().is_empty());
            } else {
                let outcome = outcome.expect("deferred import");
                assert_eq!(
                    outcome.response.verdict,
                    ImportBlockVerdict::DeferredDa as i32
                );
                assert!(
                    !outcome.publish_fcu,
                    "a deferred commit does not move the head"
                );
                assert!(pending.contains(&true_root));
                assert!(event_rx.try_recv().is_ok(), "deferred BLOCK_IMPORTED");
                let rows = archive_impl.rows.lock().unwrap();
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].da, DaVerdict::Deferred);
                assert!(rows[0].head.is_none(), "deferral does not move head");
                assert_eq!(rows[0].block_root, seam_root(true_root));
            }
        }
    }

    /// A successful DataAvailable re-drive commits `da: Available`.
    /// The handler does not promote the parked row before that import.
    #[test]
    fn data_available_redrive_commits_import_as_available() {
        use crate::da::PendingDa;
        use crate::residency::Residency;
        use cc_fork_choice::PeerDasAvailability;
        use cc_seam::{ArchiveWrite, DaVerdict, DurableImport, SeamError};
        use std::sync::{Arc, Mutex};

        struct Record {
            rows: Mutex<Vec<DurableImport>>,
        }

        #[async_trait::async_trait]
        impl ArchiveWrite for Record {
            async fn ingest_columns(&self, _batch: cc_seam::ColumnBatch) -> Result<(), SeamError> {
                Ok(())
            }

            fn commit_import_blocking(&self, import: DurableImport) -> Result<(), SeamError> {
                self.rows.lock().unwrap().push(import);
                Ok(())
            }
        }

        let da = Arc::new(PeerDasAvailability::new());
        let (mut store, config, request, true_root) = persist_retry_child_da(da.clone());
        let archive_impl = Arc::new(Record {
            rows: Mutex::new(Vec::new()),
        });
        let archive: crate::ArchiveWriteHandle = archive_impl.clone();
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let head = HeadSnapshotStore::new();
        let (event_tx, _event_rx) = mpsc::channel(8);
        let counters = ImportCounters::default();
        let mut residency = Residency::<Minimal>::new(64, 32);
        let mut snap_seq = 0u64;
        let mut pending = PendingDa::new();

        let parked = import_block_with_early(
            &mut store,
            &mut residency,
            &config,
            &head,
            &event_tx,
            &metrics,
            &counters,
            &mut snap_seq,
            request.clone(),
            BlockSignatureStrategy::NoVerification,
            None,
            None,
            None,
            Some(&mut pending),
            None,
            None,
            Some(&archive),
        )
        .expect("first attempt parks on DA");
        assert_eq!(
            parked.response.verdict,
            ImportBlockVerdict::DeferredDa as i32
        );
        assert!(pending.contains(&true_root));
        assert_eq!(archive_impl.rows.lock().unwrap()[0].da, DaVerdict::Deferred);

        da.mark_available(true_root);
        let imported = import_block_with_early(
            &mut store,
            &mut residency,
            &config,
            &head,
            &event_tx,
            &metrics,
            &counters,
            &mut snap_seq,
            request,
            BlockSignatureStrategy::NoVerification,
            None,
            None,
            None,
            Some(&mut pending),
            None,
            None,
            Some(&archive),
        )
        .expect("re-drive imports");
        assert_eq!(
            imported.response.verdict,
            ImportBlockVerdict::Imported as i32
        );
        assert!(imported.publish_fcu);
        let rows = archive_impl.rows.lock().unwrap();
        assert_eq!(
            rows.len(),
            2,
            "re-drive commits the body again as Available"
        );
        assert_eq!(rows[1].da, DaVerdict::Available);
        assert_eq!(rows[1].block_root, seam_root(true_root));
        assert_eq!(head.durable_head(), Some(true_root));
    }
}
