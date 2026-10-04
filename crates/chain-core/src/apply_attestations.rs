//! `ApplyAttestations` core-path handler (CC-1E, Architecture §6.3 / §7.7).
//!
//! ```text
//! bound check → for each SSZ: decode IndexedAttestation → on_attestation
//!   → (if any applied) get_head once → publish HeadSnapshot
//! ```
//!
//! **No per-attestation head computation.** `compute_deltas` runs only inside
//! the single trailing `get_head` (at most once per batch).
//!
//! # Trust boundary (SEC-1E-1 residual)
//!
//! This RPC is a **privileged weight-injection surface**. Phase 1 does **not**
//! verify BLS signatures, committee membership, or structural
//! `is_valid_indexed_attestation` (sorted / unique / non-empty). Any principal
//! that can call `ApplyAttestations` can set vote trackers for in-range
//! validator indices toward known blocks that pass free-floating validation.
//!
//! **Until Phase 5** verifies signatures against CC-1F cached shufflings and is
//! the sole live producer:
//! - treat this as a **trusted internal** RPC only (loopback / private mesh /
//!   mTLS / network policy — not a public gateway);
//! - do not expose chain gRPC to untrusted networks without authz.
//!
//! Residual High (SEC-1E-1): unsigned SSZ is accepted by design; operational
//! isolation is the control until Phase 5.

use bytes::Bytes;
use cc_fork_choice::{Store, get_head, on_attestation};
use cc_proto::chain::{
    ApplyAttestationsRequest, ApplyAttestationsResponse, AttestationApplyResult,
    AttestationApplyVerdict,
};
use cc_types::config::ChainConfig;
use cc_types::operations::IndexedAttestation;
use cc_types::preset::Preset;
use cc_types::primitives::Root;
use ssz::Decode;
use tonic::Status;

use crate::events::EventInput;
use crate::head::{HeadSnapshot, HeadSnapshotStore};
use crate::metrics::ChainMetrics;

/// Maximum attestations accepted in one `ApplyAttestations` batch (Architecture §7.7).
pub const MAX_APPLY_ATTESTATIONS: usize = 128;

/// Per-item results plus whether the caller may emit import-path fcU.
///
/// `publish_fcu` is false only when the trailing recompute or its `set_head`
/// failed. Vote results are still returned (SEC-1E-2).
#[derive(Debug)]
pub struct AttestationApply {
    pub response: ApplyAttestationsResponse,
    pub publish_fcu: bool,
}

/// Apply a batch of free-floating attestations on the core thread.
///
/// Oversized batches return `INVALID_ARGUMENT` **before** any store mutation
/// (no silent truncation, no partial apply).
///
/// # Trailing `get_head` failure (SEC-1E-2)
///
/// Vote trackers are committed before the trailing recompute. If `get_head`
/// or the coalesced `set_head` fails after one or more applies, this still
/// returns **`Ok` with per-item results** so the client learns what was
/// applied. The head snapshot is left unchanged (may be **stale** relative to
/// the store until the next successful recompute via import / a later batch).
/// An error is logged; it is not surfaced as gRPC `INTERNAL` that would drop
/// the results vector. `publish_fcu` is false in that case.
#[allow(clippy::too_many_arguments)]
pub fn apply_attestations<P: Preset>(
    store: &mut Store<P>,
    head_store: &HeadSnapshotStore,
    event_tx: &tokio::sync::mpsc::Sender<EventInput>,
    metrics: &ChainMetrics,
    snapshot_sequence: &mut u64,
    request: ApplyAttestationsRequest,
    config: &ChainConfig,
    archive: Option<&crate::ArchiveWriteHandle>,
) -> Result<AttestationApply, Status> {
    let n = request.attestations_ssz.len();
    if n > MAX_APPLY_ATTESTATIONS {
        return Err(Status::invalid_argument(format!(
            "ApplyAttestations batch size {n} exceeds bound of {MAX_APPLY_ATTESTATIONS}"
        )));
    }

    let mut results = Vec::with_capacity(n);
    let mut any_applied = false;

    for ssz in &request.attestations_ssz {
        match apply_one::<P>(store, ssz, config) {
            Ok(()) => {
                any_applied = true;
                results.push(AttestationApplyResult {
                    verdict: AttestationApplyVerdict::Applied as i32,
                    reason: String::new(),
                });
            }
            Err(reason) => {
                results.push(AttestationApplyResult {
                    verdict: AttestationApplyVerdict::Rejected as i32,
                    reason,
                });
            }
        }
    }

    // Single head recompute after the batch so GetHead (ArcSwap) observes weight
    // through the honest observation point (§6.3). No get_head per attestation.
    // On failure: keep results (SEC-1E-2) — do not discard applied outcomes.
    let mut publish_fcu = true;
    if any_applied
        && let Err(e) = recompute_and_publish_head(
            store,
            head_store,
            event_tx,
            metrics,
            snapshot_sequence,
            archive,
        )
    {
        publish_fcu = false;
        tracing::error!(
            error = %e,
            applied = results
                .iter()
                .filter(|r| r.verdict == AttestationApplyVerdict::Applied as i32)
                .count(),
            "ApplyAttestations: trailing get_head failed; vote trackers applied but \
             HeadSnapshot not updated (GetHead may be stale until next recompute)"
        );
    }

    Ok(AttestationApply {
        response: ApplyAttestationsResponse { results },
        publish_fcu,
    })
}

fn apply_one<P: Preset>(
    store: &mut Store<P>,
    ssz: &[u8],
    config: &ChainConfig,
) -> Result<(), String> {
    let indexed = IndexedAttestation::<P>::from_ssz_bytes(ssz)
        .map_err(|e| format!("failed to decode IndexedAttestation SSZ: {e:?}"))?;
    // Free-floating path (Phase 5 producer): is_from_block = false.
    on_attestation(store, &indexed, false, config).map_err(|e| e.to_string())
}

fn recompute_and_publish_head<P: Preset>(
    store: &mut Store<P>,
    head_store: &HeadSnapshotStore,
    event_tx: &tokio::sync::mpsc::Sender<EventInput>,
    metrics: &ChainMetrics,
    snapshot_sequence: &mut u64,
    archive: Option<&crate::ArchiveWriteHandle>,
) -> Result<(), Status> {
    let latch = store.head_latch();
    let (head_root, reorg) = match get_head(store) {
        Ok(head) => head,
        Err(e) => {
            store.restore_head_latch(latch);
            return Err(Status::internal(format!(
                "get_head failed after ApplyAttestations: {e}"
            )));
        }
    };
    let head_slot = store
        .blocks()
        .get(&head_root)
        .map(|h| h.slot)
        .unwrap_or_else(|| store.get_current_slot());
    let head_state_root = store
        .blocks()
        .get(&head_root)
        .map(|h| h.state_root)
        .unwrap_or(Root::ZERO);

    // One `set_head` per batch, only when this head is not the last one
    // `set_head` or `commit_import.head` committed. The latch alone is not
    // that ack: `get_head` updates it before the write returns.
    if let Some(archive) = archive
        && head_store.durable_head() != Some(head_root)
    {
        if let Err(e) = crate::import::commit_set_head(
            archive,
            cc_seam::HeadChange {
                head_root: crate::import::seam_root(head_root),
                head_slot: head_slot.as_u64(),
                cause: cc_seam::HeadCause::Attestation,
            },
            crate::import::fork_choice_scalars_ssz(store, head_root, head_slot),
        ) {
            store.restore_head_latch(latch);
            return Err(e);
        }
        head_store.set_durable_head(head_root);
    }

    *snapshot_sequence = snapshot_sequence.saturating_add(1);
    let optimistic = cc_fork_choice::is_optimistic_node(store);
    head_store.store(HeadSnapshot {
        head_root,
        head_slot,
        head_state_root,
        justified: store.justified_checkpoint(),
        finalized: store.finalized_checkpoint(),
        unrealized_justified: store.unrealized_justified_checkpoint(),
        unrealized_finalized: store.unrealized_finalized_checkpoint(),
        current_epoch_target_root: Root::ZERO,
        dependent_root: Root::ZERO,
        // CC-3B: node-level optimistic from fork choice after fresh get_head.
        is_optimistic: optimistic,
        sequence: *snapshot_sequence,
    });
    metrics.set_head(
        head_slot.as_u64(),
        0,
        store.finalized_checkpoint().epoch.as_u64(),
    );
    metrics.is_optimistic.set(i64::from(optimistic));

    // HEAD / REORG with §4.2 payloads. Core thread uses blocking_send (F1 /
    // Phase 1 §7.3) so storage-facing events are not silently dropped.
    if let Err(tokio::sync::mpsc::error::SendError(lost)) =
        event_tx.blocking_send(EventInput::head(
            head_slot.as_u64(),
            Bytes::copy_from_slice(head_root.as_slice()),
        ))
    {
        metrics.inc_event_publish_dropped();
        tracing::error!(
            kind = ?lost.kind,
            "events channel closed; lost HEAD after ApplyAttestations (F1)"
        );
    }
    if let Some(reorg) = reorg {
        let ancestor = crate::import::common_ancestor_slot(store, reorg.old_head, reorg.new_head);
        if let Err(tokio::sync::mpsc::error::SendError(lost)) =
            event_tx.blocking_send(EventInput::chain_reorg(
                reorg.new_head_slot.as_u64(),
                Bytes::copy_from_slice(reorg.new_head.as_slice()),
                Bytes::copy_from_slice(reorg.old_head.as_slice()),
                ancestor.as_u64(),
            ))
        {
            metrics.inc_event_publish_dropped();
            tracing::error!(
                kind = ?lost.kind,
                "events channel closed; lost CHAIN_REORG after ApplyAttestations (F1)"
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    /// Private always-Valid test harness (CC-32b: production stub deleted; not exported).
    #[derive(Debug, Default, Clone, Copy)]
    struct AcceptEngine;

    impl<P: cc_types::preset::Preset> cc_state_transition::ExecutionEngine<P> for AcceptEngine {
        fn verify_and_notify_new_payload(
            &self,
            _request: cc_state_transition::NewPayloadRequest<'_, P>,
        ) -> Result<cc_state_transition::PayloadStatus, cc_state_transition::EngineError> {
            Ok(cc_state_transition::PayloadStatus::Valid)
        }
    }

    use super::*;
    use std::sync::{Arc, Mutex};

    use cc_fork_choice::{ExecutionStatus, HarnessAvailability, get_forkchoice_store};
    use cc_types::config::{BlobParameters, BlobSchedule, PresetName};
    use cc_types::containers::{AttestationData, BeaconBlockHeader, Checkpoint};
    use cc_types::operations::IndexedAttestation;
    use cc_types::preset::Minimal;
    use cc_types::primitives::{
        Epoch, ExecutionAddress, ForkVersion, Hash256, Root, Slot, ValidatorIndex,
    };

    fn test_config() -> ChainConfig {
        ChainConfig {
            preset_base: PresetName::Minimal,
            config_name: "minimal".into(),
            genesis_fork_version: ForkVersion::from_array([0, 0, 0, 1]),
            altair_fork_version: ForkVersion::from_array([1, 0, 0, 1]),
            altair_fork_epoch: Epoch::new(0),
            bellatrix_fork_version: ForkVersion::from_array([2, 0, 0, 1]),
            bellatrix_fork_epoch: Epoch::new(0),
            capella_fork_version: ForkVersion::from_array([3, 0, 0, 1]),
            capella_fork_epoch: Epoch::new(0),
            deneb_fork_version: ForkVersion::from_array([4, 0, 0, 1]),
            deneb_fork_epoch: Epoch::new(0),
            electra_fork_version: ForkVersion::from_array([5, 0, 0, 1]),
            electra_fork_epoch: Epoch::new(0),
            fulu_fork_version: ForkVersion::from_array([6, 0, 0, 1]),
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
    use cc_types::{BeaconBlock, BeaconState};
    use prometheus_client::registry::Registry;
    use ssz::Encode;
    use ssz_types::VariableList;
    use tokio::sync::mpsc;
    use tonic::Code;
    use tree_hash::TreeHash;

    use crate::metrics::ChainMetrics;

    fn root(b: u8) -> Root {
        let mut a = [0u8; 32];
        a[0] = b;
        Root::from_array(a)
    }

    fn cp(epoch: u64, r: Root) -> Checkpoint {
        Checkpoint {
            epoch: Epoch::new(epoch),
            root: r,
        }
    }

    fn indexed(
        indices: &[u64],
        slot: u64,
        beacon_block_root: Root,
        target: Checkpoint,
    ) -> IndexedAttestation<Minimal> {
        let attesting_indices = VariableList::new(
            indices
                .iter()
                .map(|i| ValidatorIndex::new(*i))
                .collect::<Vec<_>>(),
        )
        .unwrap();
        IndexedAttestation {
            attesting_indices,
            data: AttestationData {
                slot: Slot::new(slot),
                index: Default::default(),
                beacon_block_root,
                source: cp(0, beacon_block_root),
                target,
            },
            signature: Default::default(),
        }
    }

    fn seeded_store(n: usize) -> (Store<Minimal>, Root) {
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
        store.resize_votes(n);
        store.set_justified_balances(vec![32_000_000_000u64; n]);
        // Advance past slot 0 so free-floating slot-0 atts pass FutureSlot.
        cc_fork_choice::on_tick(&mut store, 12).unwrap();
        let anchor = Root::from_hash256(TreeHash::tree_hash_root(&anchor_block));
        (store, anchor)
    }

    /// Anchor whose justified context has `n` active, non-zero balances.
    fn weighted_store(n: usize) -> (Store<Minimal>, Root) {
        let mut state = BeaconState::<Minimal>::default();
        state.set_genesis_time(0);
        state.set_slot(Slot::new(0));
        for _ in 0..n {
            state
                .validators_push(cc_types::containers::Validator {
                    effective_balance: cc_types::primitives::Gwei::new(32_000_000_000),
                    activation_epoch: Epoch::new(0),
                    exit_epoch: Epoch::new(u64::MAX),
                    withdrawable_epoch: Epoch::new(u64::MAX),
                    ..Default::default()
                })
                .unwrap();
            state
                .balances_push(cc_types::primitives::Gwei::new(32_000_000_000))
                .unwrap();
        }
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
        cc_fork_choice::on_tick(&mut store, 12).unwrap();
        let anchor = Root::from_hash256(TreeHash::tree_hash_root(&anchor_block));
        (store, anchor)
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
            .on_block(cc_fork_choice::ProtoNodeBlock {
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

    #[test]
    fn oversized_batch_rejected_with_no_partial_apply() {
        let (mut store, anchor) = seeded_store(2);
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let head = HeadSnapshotStore::new();
        let (tx, _rx) = mpsc::channel(4);
        let mut seq = 0u64;

        let att = indexed(&[0], 0, anchor, cp(0, anchor));
        let ssz = att.as_ssz_bytes();
        let mut batch = Vec::with_capacity(129);
        for _ in 0..129 {
            batch.push(ssz.clone());
        }

        let before = store.mutation_counter();
        let err = apply_attestations(
            &mut store,
            &head,
            &tx,
            &metrics,
            &mut seq,
            ApplyAttestationsRequest {
                attestations_ssz: batch,
            },
            &test_config(),
            None,
        )
        .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
        assert!(
            err.message().contains("128"),
            "status must name the bound: {}",
            err.message()
        );
        assert_eq!(
            store.mutation_counter(),
            before,
            "oversized batch must not mutate the store"
        );
    }

    #[test]
    fn mixed_batch_reports_applied_and_rejected() {
        let (mut store, anchor) = seeded_store(2);
        let child = root(0x44);
        insert_child(&mut store, anchor, child, 1);
        // Store already at slot 2 via on_tick(12).
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let head = HeadSnapshotStore::new();
        let (tx, _rx) = mpsc::channel(4);
        let mut seq = 0u64;

        let valid = indexed(&[0], 1, child, cp(0, anchor));
        let unknown = indexed(&[0], 1, root(0xEE), cp(0, anchor));
        let resp = apply_attestations(
            &mut store,
            &head,
            &tx,
            &metrics,
            &mut seq,
            ApplyAttestationsRequest {
                attestations_ssz: vec![valid.as_ssz_bytes(), unknown.as_ssz_bytes()],
            },
            &test_config(),
            None,
        )
        .unwrap()
        .response;
        assert_eq!(resp.results.len(), 2);
        assert_eq!(
            resp.results[0].verdict,
            AttestationApplyVerdict::Applied as i32
        );
        assert_eq!(
            resp.results[1].verdict,
            AttestationApplyVerdict::Rejected as i32
        );
        assert!(!resp.results[1].reason.is_empty());
    }

    /// CC-1E: a 128-attestation batch performs exactly one trailing `get_head`
    /// (the sole `compute_deltas` call site on this path), never per attestation.
    ///
    /// Asserted via `snapshot_sequence` (+1) and `compute_deltas_call_count`
    /// (process-wide; concurrent lib tests can only *increase* the delta, so
    /// `delta < 128` rules out a per-attestation `get_head` path).
    #[test]
    fn single_trailing_get_head_across_128_batch() {
        let (mut store, anchor) = seeded_store(8);
        let child = root(0x55);
        insert_child(&mut store, anchor, child, 1);
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let head = HeadSnapshotStore::new();
        let (tx, _rx) = mpsc::channel(4);
        let mut seq = 0u64;

        let seq_before = seq;
        let deltas_before = cc_fork_choice::compute_deltas_call_count();
        let batch: Vec<Vec<u8>> = (0..MAX_APPLY_ATTESTATIONS)
            .map(|i| indexed(&[(i % 8) as u64], 1, child, cp(0, anchor)).as_ssz_bytes())
            .collect();
        let resp = apply_attestations(
            &mut store,
            &head,
            &tx,
            &metrics,
            &mut seq,
            ApplyAttestationsRequest {
                attestations_ssz: batch,
            },
            &test_config(),
            None,
        )
        .unwrap()
        .response;
        assert_eq!(resp.results.len(), MAX_APPLY_ATTESTATIONS);
        assert!(
            resp.results
                .iter()
                .all(|r| r.verdict == AttestationApplyVerdict::Applied as i32)
        );
        assert_eq!(
            seq,
            seq_before + 1,
            "exactly one trailing get_head/publish for a 128-att batch (no per-att head)"
        );
        let deltas_after = cc_fork_choice::compute_deltas_call_count();
        let delta = deltas_after.saturating_sub(deltas_before);
        assert!(
            delta >= 1,
            "trailing get_head must invoke compute_deltas at least once"
        );
        // Per-att get_head would contribute ≥128; concurrent tests may add a few.
        assert!(
            delta < MAX_APPLY_ATTESTATIONS as u64,
            "compute_deltas ran {delta} times across a 128-att batch (expected 1, \
             <128 even under concurrent test noise) — per-att head path?"
        );
    }

    /// One `set_head` per batch, and only when the head root actually changes.
    #[test]
    fn one_set_head_per_batch_only_on_real_head_change() {
        use cc_seam::{ArchiveWrite, HeadCause, HeadChange, SeamError};
        use std::sync::Mutex;

        struct CountHeads {
            heads: Mutex<Vec<HeadChange>>,
        }

        #[async_trait::async_trait]
        impl ArchiveWrite for CountHeads {
            async fn ingest_columns(&self, _batch: cc_seam::ColumnBatch) -> Result<(), SeamError> {
                Ok(())
            }

            fn set_head_blocking(
                &self,
                head: HeadChange,
                _scalars: bytes::Bytes,
            ) -> Result<(), SeamError> {
                self.heads.lock().unwrap().push(head);
                Ok(())
            }
        }

        let (mut store, anchor) = weighted_store(8);
        // Two equal children: zero-weight LMD already walks to one leaf.
        // Votes for the other leaf are the real head change.
        let child_a = root(0x55);
        let child_b = root(0x66);
        insert_child(&mut store, anchor, child_a, 1);
        insert_child(&mut store, anchor, child_b, 1);
        let (before, _) = get_head(&mut store).unwrap();
        assert!(
            before == child_a || before == child_b,
            "head starts on one of the two children, got {before:?}"
        );
        let other = if before == child_a { child_b } else { child_a };

        let archive_impl = Arc::new(CountHeads {
            heads: Mutex::new(Vec::new()),
        });
        let archive: crate::ArchiveWriteHandle = archive_impl.clone();
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let head = HeadSnapshotStore::new();
        let (tx, _rx) = mpsc::channel(4);
        let mut seq = 0u64;
        let moving: Vec<Vec<u8>> = (0..MAX_APPLY_ATTESTATIONS)
            .map(|i| indexed(&[(i % 8) as u64], 1, other, cp(0, anchor)).as_ssz_bytes())
            .collect();
        let moved = apply_attestations(
            &mut store,
            &head,
            &tx,
            &metrics,
            &mut seq,
            ApplyAttestationsRequest {
                attestations_ssz: moving,
            },
            &test_config(),
            Some(&archive),
        )
        .unwrap();
        assert!(moved.publish_fcu);
        assert!(
            moved
                .response
                .results
                .iter()
                .all(|r| r.verdict == AttestationApplyVerdict::Applied as i32)
        );
        let (after, _) = get_head(&mut store).unwrap();
        assert_eq!(after, other, "eight equal votes must move LMD head");
        let heads = archive_impl.heads.lock().unwrap();
        assert_eq!(heads.len(), 1, "one set_head for the batch, not per vote");
        assert_eq!(heads[0].cause, HeadCause::Attestation);
        assert_eq!(heads[0].head_root, *other.as_array());
        drop(heads);

        let steady: Vec<Vec<u8>> = (0..8u64)
            .map(|i| indexed(&[i], 1, other, cp(0, anchor)).as_ssz_bytes())
            .collect();
        let _ = apply_attestations(
            &mut store,
            &head,
            &tx,
            &metrics,
            &mut seq,
            ApplyAttestationsRequest {
                attestations_ssz: steady,
            },
            &test_config(),
            Some(&archive),
        )
        .unwrap();
        assert_eq!(
            archive_impl.heads.lock().unwrap().len(),
            1,
            "a batch that does not move the head must not set_head"
        );
    }

    /// A failed `set_head` restores the latch and publishes nothing. The next
    /// batch, whose fork-choice head did not move again, still writes it once.
    #[test]
    fn failed_head_write_is_not_visible_on_the_next_batch() {
        use cc_seam::{ArchiveWrite, HeadCause, HeadChange, SeamError};
        use std::sync::atomic::{AtomicU32, Ordering};

        struct FailThenHead {
            fails_left: AtomicU32,
            heads: Mutex<Vec<HeadChange>>,
        }

        #[async_trait::async_trait]
        impl ArchiveWrite for FailThenHead {
            async fn ingest_columns(&self, _batch: cc_seam::ColumnBatch) -> Result<(), SeamError> {
                Ok(())
            }

            fn set_head_blocking(
                &self,
                head: HeadChange,
                _scalars: bytes::Bytes,
            ) -> Result<(), SeamError> {
                if self.fails_left.load(Ordering::SeqCst) > 0 {
                    self.fails_left.fetch_sub(1, Ordering::SeqCst);
                    return Err(SeamError::Unavailable("head write failed".into()));
                }
                self.heads.lock().unwrap().push(head);
                Ok(())
            }
        }

        let (mut store, anchor) = weighted_store(8);
        let child_a = root(0x55);
        let child_b = root(0x66);
        insert_child(&mut store, anchor, child_a, 1);
        insert_child(&mut store, anchor, child_b, 1);
        let (before, _) = get_head(&mut store).unwrap();
        let other = if before == child_a { child_b } else { child_a };

        let archive_impl = Arc::new(FailThenHead {
            fails_left: AtomicU32::new(1),
            heads: Mutex::new(Vec::new()),
        });
        let archive: crate::ArchiveWriteHandle = archive_impl.clone();
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let head = HeadSnapshotStore::new();
        let (tx, mut rx) = mpsc::channel(8);
        let mut seq = 0u64;
        let moving: Vec<Vec<u8>> = (0..MAX_APPLY_ATTESTATIONS)
            .map(|i| indexed(&[(i % 8) as u64], 1, other, cp(0, anchor)).as_ssz_bytes())
            .collect();
        let failed = apply_attestations(
            &mut store,
            &head,
            &tx,
            &metrics,
            &mut seq,
            ApplyAttestationsRequest {
                attestations_ssz: moving,
            },
            &test_config(),
            Some(&archive),
        )
        .unwrap();
        assert!(!failed.publish_fcu);
        assert_eq!(seq, 0);
        assert_eq!(head.load().sequence, 0);
        assert_eq!(store.cached_head_root(), Some(before));
        assert!(head.durable_head().is_none());
        assert!(archive_impl.heads.lock().unwrap().is_empty());
        assert!(
            rx.try_recv().is_err(),
            "no HEAD event after a failed set_head"
        );

        let steady: Vec<Vec<u8>> = (0..8u64)
            .map(|i| indexed(&[i], 1, other, cp(0, anchor)).as_ssz_bytes())
            .collect();
        let recovered = apply_attestations(
            &mut store,
            &head,
            &tx,
            &metrics,
            &mut seq,
            ApplyAttestationsRequest {
                attestations_ssz: steady,
            },
            &test_config(),
            Some(&archive),
        )
        .unwrap();
        assert!(recovered.publish_fcu);
        assert_eq!(seq, 1);
        let heads = archive_impl.heads.lock().unwrap();
        assert_eq!(heads.len(), 1, "the next batch still set_head once");
        assert_eq!(heads[0].cause, HeadCause::Attestation);
        assert_eq!(heads[0].head_root, *other.as_array());
        drop(heads);
        assert_eq!(head.durable_head(), Some(other));
        assert!(
            rx.try_recv()
                .is_ok_and(|ev| ev.kind == cc_proto::chain::EventKind::Head),
            "HEAD follows the durable write"
        );
    }

    /// An unchanged fork-choice head is still one `set_head` when this
    /// process has not committed it. A later unchanged batch does not write.
    #[test]
    fn unchanged_head_sets_head_once_until_durable() {
        use cc_seam::{ArchiveWrite, HeadCause, HeadChange, SeamError};

        struct CountHeads {
            heads: Mutex<Vec<HeadChange>>,
        }

        #[async_trait::async_trait]
        impl ArchiveWrite for CountHeads {
            async fn ingest_columns(&self, _batch: cc_seam::ColumnBatch) -> Result<(), SeamError> {
                Ok(())
            }

            fn set_head_blocking(
                &self,
                head: HeadChange,
                _scalars: bytes::Bytes,
            ) -> Result<(), SeamError> {
                self.heads.lock().unwrap().push(head);
                Ok(())
            }
        }

        let (mut store, anchor) = weighted_store(8);
        let child_a = root(0x55);
        let child_b = root(0x66);
        insert_child(&mut store, anchor, child_a, 1);
        insert_child(&mut store, anchor, child_b, 1);
        let (before, _) = get_head(&mut store).unwrap();
        let archive_impl = Arc::new(CountHeads {
            heads: Mutex::new(Vec::new()),
        });
        let archive: crate::ArchiveWriteHandle = archive_impl.clone();
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let head = HeadSnapshotStore::new();
        let (tx, _rx) = mpsc::channel(4);
        let mut seq = 0u64;
        let steady: Vec<Vec<u8>> = (0..8u64)
            .map(|i| indexed(&[i], 1, before, cp(0, anchor)).as_ssz_bytes())
            .collect();
        let first = apply_attestations(
            &mut store,
            &head,
            &tx,
            &metrics,
            &mut seq,
            ApplyAttestationsRequest {
                attestations_ssz: steady.clone(),
            },
            &test_config(),
            Some(&archive),
        )
        .unwrap();
        assert!(first.publish_fcu);
        let (after, _) = get_head(&mut store).unwrap();
        assert_eq!(after, before);
        let heads = archive_impl.heads.lock().unwrap();
        assert_eq!(heads.len(), 1);
        assert_eq!(heads[0].cause, HeadCause::Attestation);
        assert_eq!(heads[0].head_root, *before.as_array());
        drop(heads);
        assert_eq!(head.durable_head(), Some(before));

        let _ = apply_attestations(
            &mut store,
            &head,
            &tx,
            &metrics,
            &mut seq,
            ApplyAttestationsRequest {
                attestations_ssz: steady,
            },
            &test_config(),
            Some(&archive),
        )
        .unwrap();
        assert_eq!(archive_impl.heads.lock().unwrap().len(), 1);
    }
}
