//! `state_transition` and `process_block` (Architecture §5.1–5.2).
//!
//! `process_block` is a **flat list of calls in spec order with no
//! conditionals** — the order *is* the spec.

pub mod eth1_data;
pub mod execution_payload;
pub mod header;
pub mod operations;
pub mod randao;
pub mod sync_aggregate;
pub mod withdrawals;

use std::cell::RefCell;
use std::marker::PhantomData;

use cc_types::config::ChainConfig;
use cc_types::preset::Preset;
use cc_types::primitives::Root;
use cc_types::{BeaconBlock, BeaconState, PubkeyIndexMap, SignedBeaconBlock};

use crate::BlockSignatureStrategy;
use crate::engine_seam::{ExecutionEngine, PayloadStatus};
use crate::error::BlockError;
use crate::root_measure::measured_canonical_root;
use crate::signatures::verify_block_signatures;
use crate::slots::process_slots;

pub use eth1_data::process_eth1_data;
pub use execution_payload::process_execution_payload;
pub use header::process_block_header;
pub use operations::{
    ProcessAttestationOpts, process_attestation, process_attester_slashing,
    process_bls_to_execution_change, process_consolidation_request, process_deposit,
    process_deposit_request, process_operations, process_proposer_slashing, process_voluntary_exit,
    process_withdrawal_request,
};
pub use randao::process_randao;
pub use sync_aggregate::{process_sync_aggregate, process_sync_aggregate_with_opts};
pub use withdrawals::{get_expected_withdrawals, process_withdrawals};

// ---------------------------------------------------------------------------
// TransitionContext (engine trait lives in `engine_seam.rs`, CC-14)
// ---------------------------------------------------------------------------

/// Per-transition context (config + engine + payload-status outbox + pubkey cache).
///
/// The outbox carries the EL's [`PayloadStatus`] out of the state transition
/// without a second `verify_and_notify_new_payload` call site (CC-14/1, ADR P3-03).
/// Written at the sole call site in [`super::execution_payload::process_execution_payload`];
/// read by `on_block` from `CC-34a` onward. This commit writes and leaves unread (D-4).
///
/// `PubkeyIndexMap` lives here, not on `BeaconState` (S2-A-10 / P0-19/3):
/// `StateCaches` derives `Clone`, and a ~80–100 MB map with 48-byte keys must
/// not deep-copy on each of the 3–5 state clones per import.
///
/// `RefCell` rather than `Mutex`: `TransitionContext` is stack-local per import and is
/// not required to be `Sync` (§12/8 compile check).
pub struct TransitionContext<'a, P: Preset> {
    /// Runtime chain config (blob schedule, forks, …).
    pub config: &'a ChainConfig,
    /// Execution-engine seam (CC-14).
    pub engine: &'a dyn ExecutionEngine<P>,
    /// CC-32/6 — payload status leaves the transition through here.
    ///
    /// Written exactly once at the sole engine call site (both Valid/NOT_VALIDATED
    /// and INVALIDATED paths). Not read in this commit (D-4).
    payload_status_outbox: RefCell<Option<PayloadStatus>>,
    /// Pubkey → validator index. Off `BeaconState` (S2-A-10).
    pubkeys: RefCell<PubkeyIndexMap>,
    _phantom: PhantomData<P>,
}

impl<'a, P: Preset> std::fmt::Debug for TransitionContext<'a, P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransitionContext")
            .field("config_name", &self.config.config_name)
            .field("pubkeys_len", &self.pubkeys.borrow().len())
            .finish_non_exhaustive()
    }
}

impl<'a, P: Preset> TransitionContext<'a, P> {
    /// Construct a context.
    pub fn new(config: &'a ChainConfig, engine: &'a dyn ExecutionEngine<P>) -> Self {
        Self {
            config,
            engine,
            payload_status_outbox: RefCell::new(None),
            pubkeys: RefCell::new(PubkeyIndexMap::default()),
            _phantom: PhantomData,
        }
    }

    /// Borrow the pubkey → index map.
    pub fn pubkeys(&self) -> std::cell::Ref<'_, PubkeyIndexMap> {
        self.pubkeys.borrow()
    }

    /// Mutably borrow the pubkey → index map.
    pub fn pubkeys_mut(&self) -> std::cell::RefMut<'_, PubkeyIndexMap> {
        self.pubkeys.borrow_mut()
    }

    /// Interior-mutable map for lookups that may backfill (S2-A-11).
    pub fn pubkey_index_map(&self) -> &RefCell<PubkeyIndexMap> {
        &self.pubkeys
    }

    /// Fill [`Self::pubkeys`] from the validator registry.
    ///
    /// Append-only and idempotent. Called at the start of
    /// [`process_block`] / [`state_transition`] so decode sites do not have
    /// to hydrate a cache on `BeaconState` (S2-A-10).
    pub fn top_up_pubkey_cache(&self, state: &BeaconState<P>) {
        self.pubkeys.borrow_mut().import_from_registry(state);
    }

    /// Record the payload status returned by the sole engine call site.
    ///
    /// `pub(crate)` so only this crate's `process_execution_payload` may write;
    /// external crates (fork-choice) consume via [`Self::take_payload_status`] only.
    /// Overwrites any prior value so a shared context can drive multi-block
    /// vector runners without an intervening take (CC-32a outbox; CC-32b).
    pub(crate) fn set_payload_status(&self, status: PayloadStatus) {
        *self.payload_status_outbox.borrow_mut() = Some(status);
    }

    /// Take the recorded payload status (if any). Used by tests; production
    /// readers arrive in `CC-34a`.
    pub fn take_payload_status(&self) -> Option<PayloadStatus> {
        self.payload_status_outbox.borrow_mut().take()
    }
}

// ---------------------------------------------------------------------------
// state_transition / process_block
// ---------------------------------------------------------------------------

/// Spec `state_transition`.
///
/// 1. [`process_slots`] → pre-state root (threaded)
/// 2. signature verification per [`BlockSignatureStrategy`]
/// 3. [`process_block`]
/// 4. post-state root check (second `canonical_root` on a one-slot advance)
pub fn state_transition<P: Preset>(
    state: &mut BeaconState<P>,
    block: &SignedBeaconBlock<P>,
    ctx: &TransitionContext<'_, P>,
    verify: BlockSignatureStrategy,
) -> Result<(), BlockError> {
    let message = &block.message;

    // Process slots (including those with no blocks) since the previous block.
    let pre_state_root = process_slots(state, message.slot, ctx.config)?;

    // Verify signature(s): proposer + RANDAO + CC-12c operation set (§5.2).
    // process_operations then runs with verify_signatures=false.
    // Sync-aggregate is *not* in that set — process_block honours `verify`.
    verify_block_signatures(state, block, ctx.config, verify)?;

    // Process block.
    process_block_with_strategy(state, message, ctx, pre_state_root, verify)?;

    // Verify state root — second measured canonical_root on a block slot.
    let post_root = measured_canonical_root(state);
    if post_root != message.state_root {
        return Err(BlockError::StateRootMismatch {
            expected: message.state_root,
            actual: post_root,
        });
    }

    Ok(())
}

/// Spec `process_block` — flat call list in Fulu beacon-chain order.
///
/// Calls [`BeaconState::commit`] at the end (§3.4).
///
/// Standalone callers (spec `process_block`) always verify the sync-aggregate.
/// [`state_transition`] threads [`BlockSignatureStrategy`] so [`BlockSignatureStrategy::NoVerification`]
/// skips that BLS check (P1-A/18).
pub fn process_block<P: Preset>(
    state: &mut BeaconState<P>,
    block: &BeaconBlock<P>,
    ctx: &TransitionContext<'_, P>,
    pre_state_root: Root,
) -> Result<(), BlockError> {
    process_block_with_strategy(
        state,
        block,
        ctx,
        pre_state_root,
        BlockSignatureStrategy::VerifyIndividual,
    )
}

fn process_block_with_strategy<P: Preset>(
    state: &mut BeaconState<P>,
    block: &BeaconBlock<P>,
    ctx: &TransitionContext<'_, P>,
    pre_state_root: Root,
    verify: BlockSignatureStrategy,
) -> Result<(), BlockError> {
    // S2-A-10: cache is on the context. Top up from the registry before any
    // handler that resolves pubkeys (deposits, sync-aggregate).
    ctx.top_up_pubkey_cache(state);
    process_block_header(state, block, pre_state_root)?;
    process_withdrawals(state, block)?;
    process_execution_payload(state, block, ctx)?;
    process_randao(state, block)?;
    process_eth1_data(state, block)?;
    // Operation signatures already verified in `state_transition` under the
    // chosen strategy. Sync-aggregate is not in that set (P1-A/18).
    process_operations(state, block, ctx, false)?;
    process_sync_aggregate_with_opts(
        state,
        &block.body.sync_aggregate,
        !matches!(verify, BlockSignatureStrategy::NoVerification),
        ctx,
    )?;
    state.commit();
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::engine_seam::{ExecutionEngine, NewPayloadRequest, PayloadStatus};
    use crate::error::{BlockError, EngineError, SignatureKind};
    use crate::root_measure::{canonical_root_call_count, take_canonical_root_call_count};
    use cc_crypto::{BLS_SIGNATURE_DST, take_bls_verify_count};
    use cc_types::primitives::{BlsPublicKey, BlsSignature};

    /// Private always-Valid test harness (CC-32b: production stub deleted; not exported).
    #[derive(Debug, Default, Clone, Copy)]
    struct AcceptEngine;

    impl<P: Preset> ExecutionEngine<P> for AcceptEngine {
        fn verify_and_notify_new_payload(
            &self,
            _request: NewPayloadRequest<'_, P>,
        ) -> Result<PayloadStatus, EngineError> {
            Ok(PayloadStatus::Valid)
        }
    }

    use crate::slots::{process_slot, process_slots};
    use cc_types::containers::{BeaconBlockHeader, Validator};
    use cc_types::primitives::{Gwei, Slot, ValidatorIndex};
    use cc_types::{BeaconState, Minimal};

    fn seed(state: &mut BeaconState<Minimal>) {
        for i in 0..state.proposer_lookahead_len() {
            state
                .proposer_lookahead_set(i, ValidatorIndex::new(0))
                .unwrap();
        }
        state
            .validators_push(Validator {
                pubkey: Default::default(),
                withdrawal_credentials: Root::ZERO,
                effective_balance: Gwei::new(32_000_000_000),
                slashed: false,
                activation_eligibility_epoch: Default::default(),
                activation_epoch: Default::default(),
                exit_epoch: cc_types::primitives::Epoch::new(u64::MAX),
                withdrawable_epoch: cc_types::primitives::Epoch::new(u64::MAX),
            })
            .unwrap();
        state.balances_push(Gwei::new(32_000_000_000)).unwrap();
        state.set_latest_block_header(BeaconBlockHeader {
            slot: Slot::new(0),
            proposer_index: ValidatorIndex::new(0),
            parent_root: Root::ZERO,
            state_root: Root::ZERO,
            body_root: Root::ZERO,
        });
        state.set_slot(Slot::new(0));
    }

    /// A block-slot advance of one empty slot performs exactly **one**
    /// `canonical_root` inside `process_slots`; the post-state check is the
    /// second. Together they are the §5.1 budget of two.
    #[test]
    fn block_slot_transition_two_canonical_roots() {
        let mut state = BeaconState::<Minimal>::default();
        seed(&mut state);

        let _ = take_canonical_root_call_count();

        // Pre-state root via process_slots (one call).
        let config = minimal_test_config();
        let pre_root = process_slots(&mut state, Slot::new(1), &config).unwrap();
        assert_eq!(canonical_root_call_count(), 1);
        assert_eq!(state.state_roots_get(0), Some(pre_root));
        assert_eq!(state.latest_block_header().state_root, pre_root);

        // Simulate the post-state check that state_transition performs.
        let post = measured_canonical_root(&mut state);
        assert_eq!(canonical_root_call_count(), 2);
        let _ = post;

        // Confirm process_slot itself is a single call.
        let mut s2 = BeaconState::<Minimal>::default();
        seed(&mut s2);
        s2.set_slot(Slot::new(2));
        let _ = take_canonical_root_call_count();
        let _ = process_slot(&mut s2).unwrap();
        assert_eq!(canonical_root_call_count(), 1);
    }

    #[test]
    fn process_block_completes_with_empty_ops_and_empty_sync() {
        let mut state = BeaconState::<Minimal>::default();
        seed(&mut state);
        // eth1 deposits disabled (unset start index) so empty deposits list is ok.
        state.set_deposit_requests_start_index(u64::MAX);

        let config = minimal_test_config();
        let pre = process_slots(&mut state, Slot::new(1), &config).unwrap();
        let parent = Root::from_hash256(tree_hash::TreeHash::tree_hash_root(
            state.latest_block_header(),
        ));
        use crate::helpers::accessors::{get_current_epoch, get_randao_mix};
        let epoch = get_current_epoch(&state);
        let mix = get_randao_mix(&state, epoch).unwrap();
        let mut body = cc_types::BeaconBlockBody::<Minimal>::default();
        body.execution_payload.prev_randao = mix;
        body.execution_payload.timestamp = state.genesis_time() + state.slot().as_u64() * 6; // minimal seconds_per_slot
        body.execution_payload.parent_hash = state.latest_execution_payload_header().block_hash;
        // Empty participant set requires the infinity signature (eth_fast_aggregate_verify).
        body.sync_aggregate.sync_committee_signature =
            cc_types::primitives::BlsSignature::from_array(cc_crypto::INFINITY_SIGNATURE);
        let block = BeaconBlock {
            slot: Slot::new(1),
            proposer_index: ValidatorIndex::new(0),
            parent_root: parent,
            state_root: Root::ZERO,
            body,
        };
        let engine = AcceptEngine;
        let ctx = TransitionContext::<Minimal>::new(&config, &engine);
        ctx.top_up_pubkey_cache(&state);
        process_block(&mut state, &block, &ctx, pre).expect("full process_block should complete");
    }

    /// Valid-encoded BLS pair that is *not* a correct sync-aggregate signature.
    ///
    /// Empty-participant + infinity short-circuits in `eth_fast_aggregate_verify`
    /// without incrementing the counter, so it cannot falsify P1-A/18.
    fn junk_sync_bls_pair() -> (BlsPublicKey, BlsSignature) {
        let sk = blst::min_pk::SecretKey::key_gen(&[1u8; 32], &[]).expect("key_gen");
        let pk = BlsPublicKey::from_array(sk.sk_to_pk().compress());
        let sig = BlsSignature::from_array(sk.sign(&[9u8; 32], BLS_SIGNATURE_DST, &[]).compress());
        (pk, sig)
    }

    fn state_with_participant_sync_committee() -> BeaconState<Minimal> {
        let mut state = BeaconState::<Minimal>::default();
        seed(&mut state);
        state.set_deposit_requests_start_index(u64::MAX);
        let (pk, _) = junk_sync_bls_pair();
        let mut committee = state.current_sync_committee().clone();
        committee.pubkeys[0] = pk;
        state.set_current_sync_committee(committee);
        state
    }

    /// S0-A-33 / P1-A/18 — `NoVerification` must not BLS-verify the sync-aggregate.
    #[test]
    fn no_verification_skips_sync_aggregate_bls() {
        let mut pre = state_with_participant_sync_committee();
        let config = minimal_test_config();
        let engine = AcceptEngine;
        let ctx = TransitionContext::<Minimal>::new(&config, &engine);
        ctx.top_up_pubkey_cache(&pre);

        let mut advanced = pre.clone();
        let _pre_root = process_slots(&mut advanced, Slot::new(1), &config).unwrap();
        let parent = Root::from_hash256(tree_hash::TreeHash::tree_hash_root(
            advanced.latest_block_header(),
        ));
        use crate::helpers::accessors::{get_current_epoch, get_randao_mix};
        let epoch = get_current_epoch(&advanced);
        let mix = get_randao_mix(&advanced, epoch).unwrap();
        let (_, junk_sig) = junk_sync_bls_pair();
        let mut body = cc_types::BeaconBlockBody::<Minimal>::default();
        body.execution_payload.prev_randao = mix;
        body.execution_payload.timestamp = advanced.genesis_time() + advanced.slot().as_u64() * 6;
        body.execution_payload.parent_hash = advanced.latest_execution_payload_header().block_hash;
        body.sync_aggregate
            .sync_committee_bits
            .set(0, true)
            .unwrap();
        body.sync_aggregate.sync_committee_signature = junk_sig;
        let block = BeaconBlock {
            slot: Slot::new(1),
            proposer_index: ValidatorIndex::new(0),
            parent_root: parent,
            state_root: Root::ZERO,
            body,
        };

        // Control: verifying the same aggregate reaches crypto and increments.
        let mut control = advanced;
        let _ = take_bls_verify_count();
        let err =
            process_sync_aggregate_with_opts(&mut control, &block.body.sync_aggregate, true, &ctx)
                .expect_err("junk participant aggregate must fail when verified");
        assert!(
            matches!(
                err,
                BlockError::InvalidSignature {
                    which: SignatureKind::SyncAggregate
                }
            ),
            "expected SyncAggregate InvalidSignature, got {err:?}"
        );
        assert!(
            take_bls_verify_count() >= 1,
            "instrument must count the verifying path so a 0 under NoVerification is meaningful"
        );

        let signed = SignedBeaconBlock {
            message: block,
            signature: Default::default(),
        };
        let _ = take_bls_verify_count();
        let result = state_transition(
            &mut pre,
            &signed,
            &ctx,
            BlockSignatureStrategy::NoVerification,
        );
        assert!(
            !matches!(
                result,
                Err(BlockError::InvalidSignature {
                    which: SignatureKind::SyncAggregate
                })
            ),
            "sync-aggregate must not be verified under NoVerification: {result:?}"
        );
        assert_eq!(
            take_bls_verify_count(),
            0,
            "NoVerification must not reach cc_crypto verify for the sync-aggregate"
        );
    }

    #[test]
    fn transition_context_top_up_pubkey_cache_is_idempotent() {
        let mut state = BeaconState::<Minimal>::default();
        seed(&mut state);
        let config = minimal_test_config();
        let engine = AcceptEngine;
        let ctx = TransitionContext::<Minimal>::new(&config, &engine);
        assert!(ctx.pubkeys().is_empty());
        ctx.top_up_pubkey_cache(&state);
        let first = ctx.pubkeys().len();
        assert_eq!(first, state.validators_len());
        ctx.top_up_pubkey_cache(&state);
        ctx.top_up_pubkey_cache(&state);
        assert_eq!(ctx.pubkeys().len(), first);
        let pk = state.validators_get(0).unwrap().pubkey;
        assert_eq!(ctx.pubkeys().get(&pk), Some(ValidatorIndex::new(0)));
    }

    /// One [`TransitionContext`] across two sibling registries.
    ///
    /// Unreachable on a valid chain: post-Electra `process_pending_deposits`
    /// applies a deposit only once its slot is finalized, so two siblings
    /// cannot disagree about a registry index. This state is synthetic.
    /// A future reader must not re-tier the row off it.
    #[test]
    fn shared_transition_context_revalidates_sibling_registry_hits() {
        use crate::helpers::accessors::get_validator_index_by_pubkey;

        // Unreachable on a valid chain: post-Electra `process_pending_deposits`
        // applies a deposit only once its slot is finalized, so two siblings
        // cannot disagree about a registry index. Synthetic state only — do not
        // re-tier this row off it.
        fn pubkey_byte(b: u8) -> BlsPublicKey {
            let mut raw = [0u8; 48];
            raw[0] = b;
            BlsPublicKey::from_array(raw)
        }
        fn push_validator(state: &mut BeaconState<Minimal>, pubkey: BlsPublicKey) {
            state
                .validators_push(Validator {
                    pubkey,
                    ..Validator::default()
                })
                .unwrap();
        }

        let prefix = pubkey_byte(1);
        let pk_a = pubkey_byte(2);
        let pk_b = pubkey_byte(3);
        let pk_c = pubkey_byte(4);

        let mut sibling_a = BeaconState::<Minimal>::default();
        push_validator(&mut sibling_a, prefix);
        push_validator(&mut sibling_a, pk_a);
        let mut sibling_b = BeaconState::<Minimal>::default();
        push_validator(&mut sibling_b, prefix);
        push_validator(&mut sibling_b, pk_b);

        let config = minimal_test_config();
        let engine = AcceptEngine;
        let ctx = TransitionContext::<Minimal>::new(&config, &engine);
        ctx.top_up_pubkey_cache(&sibling_a);
        ctx.top_up_pubkey_cache(&sibling_b);

        let on_b = get_validator_index_by_pubkey(&sibling_b, &pk_a, ctx.pubkey_index_map());
        assert_eq!(
            on_b, None,
            "one context reused across two sibling registries resolved {on_b:?} \
             (the wrong index) for the other branch's pubkey"
        );
        assert!(
            ctx.pubkeys().linear_scan_count() >= 1,
            "a pubkey-cache hit that mismatches the supplied registry must fall back to a scan"
        );

        assert_eq!(
            get_validator_index_by_pubkey(&sibling_b, &pk_b, ctx.pubkey_index_map()),
            Some(ValidatorIndex::new(1)),
            "sibling B's own pubkey must resolve to the index it appended"
        );
        assert_eq!(
            get_validator_index_by_pubkey(&sibling_a, &pk_a, ctx.pubkey_index_map()),
            Some(ValidatorIndex::new(1)),
            "sibling A's own pubkey must still resolve after the shared context saw B"
        );
        assert_eq!(
            get_validator_index_by_pubkey(&sibling_a, &pk_b, ctx.pubkey_index_map()),
            None,
            "sibling B's pubkey must not resolve on sibling A"
        );
        assert_eq!(
            get_validator_index_by_pubkey(&sibling_a, &prefix, ctx.pubkey_index_map()),
            Some(ValidatorIndex::new(0))
        );
        assert_eq!(
            get_validator_index_by_pubkey(&sibling_b, &prefix, ctx.pubkey_index_map()),
            Some(ValidatorIndex::new(0))
        );

        // The cross-branch insert grows cardinality past the walked prefix.
        // Fill progress is not `map.len()`: extending A must import `pk_c`
        // without another registry scan.
        let cardinality = ctx.pubkeys().len();
        assert!(
            cardinality > sibling_a.validators_len(),
            "cross-branch insert must make map.len() ({cardinality}) exceed the walked prefix"
        );
        assert_eq!(ctx.pubkeys().imported_len(), sibling_a.validators_len());
        assert_ne!(
            ctx.pubkeys().imported_len(),
            cardinality,
            "fill progress is tracked apart from map.len()"
        );
        let scans_before_extend = ctx.pubkeys().linear_scan_count();
        push_validator(&mut sibling_a, pk_c);
        ctx.top_up_pubkey_cache(&sibling_a);
        assert_eq!(
            get_validator_index_by_pubkey(&sibling_a, &pk_c, ctx.pubkey_index_map()),
            Some(ValidatorIndex::new(2))
        );
        assert_eq!(
            ctx.pubkeys().linear_scan_count(),
            scans_before_extend,
            "fill progress is tracked apart from map.len(); the new index must be imported, not scanned"
        );
        assert_eq!(ctx.pubkeys().imported_len(), sibling_a.validators_len());
    }

    fn minimal_test_config() -> ChainConfig {
        use cc_types::config::{BlobParameters, BlobSchedule, PresetName};
        use cc_types::primitives::{Epoch, ExecutionAddress, ForkVersion};
        ChainConfig {
            preset_base: PresetName::Minimal,
            config_name: "minimal".into(),
            genesis_fork_version: ForkVersion::from_array([0; 4]),
            altair_fork_version: ForkVersion::from_array([1; 4]),
            altair_fork_epoch: Epoch::new(0),
            bellatrix_fork_version: ForkVersion::from_array([2; 4]),
            bellatrix_fork_epoch: Epoch::new(0),
            capella_fork_version: ForkVersion::from_array([3; 4]),
            capella_fork_epoch: Epoch::new(0),
            deneb_fork_version: ForkVersion::from_array([4; 4]),
            deneb_fork_epoch: Epoch::new(0),
            electra_fork_version: ForkVersion::from_array([5; 4]),
            electra_fork_epoch: Epoch::new(0),
            fulu_fork_version: ForkVersion::from_array([6; 4]),
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
}
