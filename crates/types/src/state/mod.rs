//! `BeaconState` flat struct + cached hashing + List/Vector seam (Architecture §3.4).
//!
//! Spec fields are private and reached through accessors. `StateCaches` holds list-hash
//! caches, field roots, and epoch/shuffling shells. The pubkey index map lives on
//! `TransitionContext` (S2-A-10 / P0-19/3), not here. Caches are excluded from
//! SSZ / tree-hash and from `PartialEq`.
//!
//! ## Compile-fail: external crates cannot index state lists directly
//!
//! Spec list fields are private. The following must not compile outside this crate:
//!
//! ```compile_fail,E0616
//! use cc_types::{BeaconState, Mainnet};
//! let state = BeaconState::<Mainnet>::default();
//! let _ = state.validators[0];
//! ```

mod accessors;
mod caches;

pub use accessors::StateAccessError;
pub use caches::{
    BEACON_STATE_FIELD_COUNT, EpochCache, FieldRootCache, ListHashCache, PubkeyIndexMap,
    SHUFFLING_CACHE_DEFAULT_CAPACITY, ShuffledCommitteeEpoch, ShufflingCache, ShufflingCacheKey,
    StateCaches, StateField, list_id,
};

use std::fmt;

use ssz::DecodeError;
use ssz_derive::{Decode, Encode};
use ssz_types::BitVector;
use tree_hash_derive::TreeHash;
use typenum::U4;

use crate::containers::{
    BeaconBlockHeader, Checkpoint, Eth1Data, HistoricalSummary, SyncCommittee, Validator,
};
use crate::execution::ExecutionPayloadHeader;
use crate::fork::{Fork, ForkName};
use crate::operations::{PendingConsolidation, PendingDeposit, PendingPartialWithdrawal};
use crate::preset::Preset;
use crate::primitives::{Epoch, Gwei, Root, Slot, ValidatorIndex};

// ---------------------------------------------------------------------------
// CC-1H seam (Architecture §3.4): only this module should name these backends.
// ---------------------------------------------------------------------------

/// Variable-length list. Phase 1: `ssz_types::VariableList`. CC-1H: milhouse.
pub type List<T, N> = ssz_types::VariableList<T, N>;

/// Fixed-length vector. Phase 1: `ssz_types::FixedVector`. CC-1H: milhouse.
pub type Vector<T, N> = ssz_types::FixedVector<T, N>;

/// `JUSTIFICATION_BITS_LENGTH = 4`.
pub type JustificationBitsLength = U4;

/// Participation flag byte (`ParticipationFlags` in the spec).
pub type ParticipationFlags = u8;

/// Spec `BeaconState` (Electra fields + Fulu `proposer_lookahead`).
///
/// All spec fields are private. `caches` is excluded from SSZ, tree-hash, and
/// equality.
///
/// Production SSZ decode must use [`Self::from_ssz_bytes_hydrated`] (S0-A-02 /
/// P0-19/1b). Raw [`ssz::Decode::from_ssz_bytes`] leaves `caches` empty.
/// Pubkey-index hydration is on `TransitionContext` (S2-A-10).
#[derive(Clone, Encode, Decode, TreeHash)]
pub struct BeaconState<P: Preset> {
    genesis_time: u64,
    genesis_validators_root: Root,
    slot: Slot,
    fork: Fork,
    latest_block_header: BeaconBlockHeader,
    block_roots: Vector<Root, P::SlotsPerHistoricalRoot>,
    state_roots: Vector<Root, P::SlotsPerHistoricalRoot>,
    historical_roots: List<Root, P::HistoricalRootsLimit>,
    eth1_data: Eth1Data,
    eth1_data_votes: List<Eth1Data, P::Eth1DataVotesLength>,
    eth1_deposit_index: u64,
    validators: List<Validator, P::ValidatorRegistryLimit>,
    balances: List<Gwei, P::ValidatorRegistryLimit>,
    randao_mixes: Vector<Root, P::EpochsPerHistoricalVector>,
    slashings: Vector<Gwei, P::EpochsPerSlashingsVector>,
    previous_epoch_participation: List<ParticipationFlags, P::ValidatorRegistryLimit>,
    current_epoch_participation: List<ParticipationFlags, P::ValidatorRegistryLimit>,
    justification_bits: BitVector<JustificationBitsLength>,
    previous_justified_checkpoint: Checkpoint,
    current_justified_checkpoint: Checkpoint,
    finalized_checkpoint: Checkpoint,
    inactivity_scores: List<u64, P::ValidatorRegistryLimit>,
    current_sync_committee: SyncCommittee<P>,
    next_sync_committee: SyncCommittee<P>,
    latest_execution_payload_header: ExecutionPayloadHeader<P>,
    next_withdrawal_index: u64,
    next_withdrawal_validator_index: ValidatorIndex,
    historical_summaries: List<HistoricalSummary, P::HistoricalRootsLimit>,
    deposit_requests_start_index: u64,
    deposit_balance_to_consume: Gwei,
    exit_balance_to_consume: Gwei,
    earliest_exit_epoch: Epoch,
    consolidation_balance_to_consume: Gwei,
    earliest_consolidation_epoch: Epoch,
    pending_deposits: List<PendingDeposit, P::PendingDepositsLimit>,
    pending_partial_withdrawals: List<PendingPartialWithdrawal, P::PendingPartialWithdrawalsLimit>,
    pending_consolidations: List<PendingConsolidation, P::PendingConsolidationsLimit>,
    /// Fulu EIP-7917 proposer lookahead.
    proposer_lookahead: Vector<ValidatorIndex, P::ProposerLookaheadLen>,

    #[ssz(skip_serializing, skip_deserializing)]
    #[tree_hash(skip_hashing)]
    caches: StateCaches<P>,
}

impl<P: Preset> Default for BeaconState<P> {
    fn default() -> Self {
        Self {
            genesis_time: 0,
            genesis_validators_root: Root::default(),
            slot: Slot::default(),
            fork: Fork {
                previous_version: Default::default(),
                current_version: Default::default(),
                epoch: Epoch::default(),
            },
            latest_block_header: BeaconBlockHeader::default(),
            block_roots: Vector::default(),
            state_roots: Vector::default(),
            historical_roots: List::default(),
            eth1_data: Eth1Data::default(),
            eth1_data_votes: List::default(),
            eth1_deposit_index: 0,
            validators: List::default(),
            balances: List::default(),
            randao_mixes: Vector::default(),
            slashings: Vector::default(),
            previous_epoch_participation: List::default(),
            current_epoch_participation: List::default(),
            justification_bits: BitVector::default(),
            previous_justified_checkpoint: Checkpoint::default(),
            current_justified_checkpoint: Checkpoint::default(),
            finalized_checkpoint: Checkpoint::default(),
            inactivity_scores: List::default(),
            current_sync_committee: SyncCommittee::default(),
            next_sync_committee: SyncCommittee::default(),
            latest_execution_payload_header: ExecutionPayloadHeader::default(),
            next_withdrawal_index: 0,
            next_withdrawal_validator_index: ValidatorIndex::default(),
            historical_summaries: List::default(),
            deposit_requests_start_index: 0,
            deposit_balance_to_consume: Gwei::default(),
            exit_balance_to_consume: Gwei::default(),
            earliest_exit_epoch: Epoch::default(),
            consolidation_balance_to_consume: Gwei::default(),
            earliest_consolidation_epoch: Epoch::default(),
            pending_deposits: List::default(),
            pending_partial_withdrawals: List::default(),
            pending_consolidations: List::default(),
            proposer_lookahead: Vector::default(),
            caches: StateCaches::default(),
        }
    }
}

impl<P: Preset> fmt::Debug for BeaconState<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BeaconState")
            .field("slot", &self.slot)
            .field("genesis_time", &self.genesis_time)
            .field("validators_len", &self.validators.len())
            .field("caches", &self.caches)
            .finish_non_exhaustive()
    }
}

// PartialEq compares spec fields only — never caches (Architecture §3.2 / §3.4).
impl<P: Preset> PartialEq for BeaconState<P> {
    fn eq(&self, other: &Self) -> bool {
        self.genesis_time == other.genesis_time
            && self.genesis_validators_root == other.genesis_validators_root
            && self.slot == other.slot
            && self.fork == other.fork
            && self.latest_block_header == other.latest_block_header
            && self.block_roots == other.block_roots
            && self.state_roots == other.state_roots
            && self.historical_roots == other.historical_roots
            && self.eth1_data == other.eth1_data
            && self.eth1_data_votes == other.eth1_data_votes
            && self.eth1_deposit_index == other.eth1_deposit_index
            && self.validators == other.validators
            && self.balances == other.balances
            && self.randao_mixes == other.randao_mixes
            && self.slashings == other.slashings
            && self.previous_epoch_participation == other.previous_epoch_participation
            && self.current_epoch_participation == other.current_epoch_participation
            && self.justification_bits == other.justification_bits
            && self.previous_justified_checkpoint == other.previous_justified_checkpoint
            && self.current_justified_checkpoint == other.current_justified_checkpoint
            && self.finalized_checkpoint == other.finalized_checkpoint
            && self.inactivity_scores == other.inactivity_scores
            && self.current_sync_committee == other.current_sync_committee
            && self.next_sync_committee == other.next_sync_committee
            && self.latest_execution_payload_header == other.latest_execution_payload_header
            && self.next_withdrawal_index == other.next_withdrawal_index
            && self.next_withdrawal_validator_index == other.next_withdrawal_validator_index
            && self.historical_summaries == other.historical_summaries
            && self.deposit_requests_start_index == other.deposit_requests_start_index
            && self.deposit_balance_to_consume == other.deposit_balance_to_consume
            && self.exit_balance_to_consume == other.exit_balance_to_consume
            && self.earliest_exit_epoch == other.earliest_exit_epoch
            && self.consolidation_balance_to_consume == other.consolidation_balance_to_consume
            && self.earliest_consolidation_epoch == other.earliest_consolidation_epoch
            && self.pending_deposits == other.pending_deposits
            && self.pending_partial_withdrawals == other.pending_partial_withdrawals
            && self.pending_consolidations == other.pending_consolidations
            && self.proposer_lookahead == other.proposer_lookahead
    }
}

impl<P: Preset> Eq for BeaconState<P> {}

impl<P: Preset> BeaconState<P> {
    /// Decode SSZ bytes under an explicit fork context.
    ///
    /// Production chokepoint (S0-A-02 / P0-19/1b). Routes through
    /// [`Self::from_ssz_bytes_with`] (fork gate). Pubkey-index hydration
    /// moved to `TransitionContext::top_up_pubkey_cache` (S2-A-10).
    pub fn from_ssz_bytes_hydrated(fork_name: ForkName, bytes: &[u8]) -> Result<Self, DecodeError> {
        Self::from_ssz_bytes_with(fork_name, bytes)
    }

    /// Decode SSZ bytes under an explicit fork context.
    ///
    /// Leaves `caches` empty (`#[ssz(skip_deserializing)]`). Production must
    /// use [`Self::from_ssz_bytes_hydrated`]. Phase 1 supports only
    /// [`ForkName::Fulu`]. Same long-term boundary as
    /// [`crate::SignedBeaconBlock::from_ssz_bytes_with`].
    pub fn from_ssz_bytes_with(fork_name: ForkName, bytes: &[u8]) -> Result<Self, DecodeError> {
        match fork_name {
            ForkName::Fulu => Self::from_ssz_bytes(bytes),
            other => Err(DecodeError::BytesInvalid(format!(
                "unsupported fork for BeaconState SSZ decode: {other} (Phase 1 is Fulu-only)"
            ))),
        }
    }

    /// Raw SSZ decode (S0-A-02 / P0-19/1b).
    ///
    /// Leaves `caches` empty and skips the fork gate. Production must use
    /// [`Self::from_ssz_bytes_hydrated`].
    #[doc(hidden)]
    pub(crate) fn from_ssz_bytes(bytes: &[u8]) -> Result<Self, DecodeError> {
        <Self as ssz::Decode>::from_ssz_bytes(bytes)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::containers::Validator;
    use crate::preset::{Mainnet, Minimal};
    use crate::primitives::BlsPublicKey;
    use ssz::Encode;
    use tree_hash::TreeHash;
    use typenum::Unsigned;

    #[test]
    fn proposer_lookahead_capacity_mainnet() {
        let expected = (Mainnet::MIN_SEED_LOOKAHEAD + 1) * Mainnet::SLOTS_PER_EPOCH;
        assert_eq!(Mainnet::PROPOSER_LOOKAHEAD_LEN, expected);
        assert_eq!(
            <Mainnet as Preset>::ProposerLookaheadLen::to_u64(),
            expected
        );
        assert_eq!(expected, 64);
        let state = BeaconState::<Mainnet>::default();
        assert_eq!(state.proposer_lookahead().len(), expected as usize);
    }

    #[test]
    fn proposer_lookahead_capacity_minimal() {
        let expected = (Minimal::MIN_SEED_LOOKAHEAD + 1) * Minimal::SLOTS_PER_EPOCH;
        assert_eq!(Minimal::PROPOSER_LOOKAHEAD_LEN, expected);
        assert_eq!(expected, 16);
        let state = BeaconState::<Minimal>::default();
        assert_eq!(state.proposer_lookahead().len(), expected as usize);
    }

    #[test]
    fn partial_eq_ignores_caches() {
        let mut a = BeaconState::<Mainnet>::default();
        let mut b = a.clone();
        a.caches_mut().tag = 1;
        b.caches_mut().tag = 999;
        assert_eq!(a, b, "states that differ only in caches must compare equal");
        // Sanity: a real field difference is unequal.
        let mut c = BeaconState::<Mainnet>::default();
        c.set_slot(Slot::new(1));
        assert_ne!(a, c);
    }

    #[test]
    fn default_ssz_roundtrip_and_tree_hash() {
        let state = BeaconState::<Minimal>::default();
        let bytes = state.as_ssz_bytes();
        let decoded =
            BeaconState::<Minimal>::from_ssz_bytes(&bytes).unwrap_or_else(|e| panic!("{e:?}"));
        assert_eq!(decoded, state);
        // Caches on decode are default (tag 0), independent of source tag.
        let mut tagged = state.clone();
        tagged.caches_mut().tag = 42;
        let re = BeaconState::<Minimal>::from_ssz_bytes(&tagged.as_ssz_bytes())
            .unwrap_or_else(|e| panic!("{e:?}"));
        assert_eq!(re.caches().tag, 0);
        let _ = state.tree_hash_root();
    }

    #[test]
    fn from_ssz_bytes_with_fulu_ok() {
        let state = BeaconState::<Minimal>::default();
        let bytes = state.as_ssz_bytes();
        let decoded = BeaconState::<Minimal>::from_ssz_bytes_with(ForkName::Fulu, &bytes)
            .unwrap_or_else(|e| panic!("{e:?}"));
        assert_eq!(decoded, state);
    }

    #[test]
    fn from_ssz_bytes_with_rejects_non_fulu() {
        let state = BeaconState::<Minimal>::default();
        let bytes = state.as_ssz_bytes();
        let err =
            BeaconState::<Minimal>::from_ssz_bytes_with(ForkName::Electra, &bytes).unwrap_err();
        match err {
            DecodeError::BytesInvalid(msg) => assert!(msg.contains("unsupported fork"), "{msg}"),
            other => panic!("expected BytesInvalid, got {other:?}"),
        }
    }

    #[test]
    fn from_ssz_bytes_hydrated_does_not_own_pubkey_cache() {
        let state = registry_state(4);
        let bytes = state.as_ssz_bytes();
        let decoded = BeaconState::<Minimal>::from_ssz_bytes_hydrated(ForkName::Fulu, &bytes)
            .unwrap_or_else(|e| panic!("{e:?}"));
        assert_eq!(decoded.validators_len(), 4);
        // S2-A-10: the map is on TransitionContext, not StateCaches.
        let mut map = PubkeyIndexMap::default();
        map.import_from_registry(&decoded);
        assert_eq!(map.len(), decoded.validators_len());
        for i in 0..4 {
            let pk = decoded.validators_get(i).unwrap().pubkey;
            assert_eq!(map.get(&pk), Some(ValidatorIndex::new(i as u64)));
        }
    }

    #[test]
    fn from_ssz_bytes_hydrated_rejects_non_fulu() {
        let state = BeaconState::<Minimal>::default();
        let bytes = state.as_ssz_bytes();
        let err =
            BeaconState::<Minimal>::from_ssz_bytes_hydrated(ForkName::Electra, &bytes).unwrap_err();
        match err {
            DecodeError::BytesInvalid(msg) => assert!(msg.contains("unsupported fork"), "{msg}"),
            other => panic!("expected BytesInvalid, got {other:?}"),
        }
    }

    #[test]
    fn commit_is_noop_under_ssz_types() {
        let mut state = BeaconState::<Minimal>::default();
        state.commit();
    }

    #[test]
    fn canonical_root_matches_tree_hash_on_default() {
        let mut state = BeaconState::<Minimal>::default();
        let cold = state.tree_hash_root();
        let cached = state.canonical_root();
        assert_eq!(cached.to_hash256(), cold);
        // Warm path agrees.
        let warm = state.canonical_root();
        assert_eq!(warm.to_hash256(), cold);
    }

    #[test]
    fn canonical_root_without_external_commit() {
        let mut state = BeaconState::<Minimal>::default();
        state.set_slot(Slot::new(7));
        // No explicit commit() — internal call must suffice.
        let cached = state.canonical_root();
        let cold = BeaconState::<Minimal> {
            // Reconstruct via mutation-free compare: clone then cold hash.
            ..state.clone()
        }
        .tree_hash_root();
        // state was mutated; compare against tree_hash of current state.
        let cold_now = TreeHash::tree_hash_root(&state);
        assert_eq!(cached.to_hash256(), cold_now);
        let _ = cold;
    }

    #[test]
    fn no_public_spec_fields() {
        let state = BeaconState::<Minimal>::default();
        let _ = state.slot();
        let _ = state.proposer_lookahead();
    }

    #[test]
    fn clone_preserves_caches() {
        let mut state = BeaconState::<Minimal>::default();
        let _ = state.canonical_root();
        assert!(state.caches().list_hashes[list_id::VALIDATORS].is_some());
        let cloned = state.clone();
        assert!(cloned.caches().list_hashes[list_id::VALIDATORS].is_some());
    }

    fn registry_state(n: usize) -> BeaconState<Minimal> {
        let mut state = BeaconState::<Minimal>::default();
        for i in 0..n {
            let mut raw = [0u8; 48];
            raw[0] = (i as u8).saturating_add(1);
            state
                .validators_push(Validator {
                    pubkey: BlsPublicKey::from_array(raw),
                    ..Validator::default()
                })
                .unwrap();
        }
        state
    }

    fn cache_mappings(
        state: &BeaconState<Minimal>,
        map: &PubkeyIndexMap,
    ) -> Vec<Option<ValidatorIndex>> {
        (0..state.validators_len())
            .map(|i| {
                let pk = state.validators_get(i).unwrap().pubkey;
                map.get(&pk)
            })
            .collect()
    }

    #[test]
    fn import_from_registry_fills_after_ssz_decode() {
        let state = registry_state(4);
        let bytes = state.as_ssz_bytes();
        let decoded =
            BeaconState::<Minimal>::from_ssz_bytes(&bytes).unwrap_or_else(|e| panic!("{e:?}"));
        let mut map = PubkeyIndexMap::default();
        assert!(map.is_empty(), "fresh map must start empty");
        map.import_from_registry(&decoded);
        assert_eq!(map.len(), decoded.validators_len());
        for i in 0..4 {
            let pk = decoded.validators_get(i).unwrap().pubkey;
            assert_eq!(map.get(&pk), Some(ValidatorIndex::new(i as u64)));
        }
    }

    #[test]
    fn import_from_registry_is_idempotent() {
        let state = registry_state(3);
        let bytes = state.as_ssz_bytes();
        let decoded =
            BeaconState::<Minimal>::from_ssz_bytes(&bytes).unwrap_or_else(|e| panic!("{e:?}"));
        let mut map = PubkeyIndexMap::default();
        map.import_from_registry(&decoded);
        let first_len = map.len();
        let first = cache_mappings(&decoded, &map);
        map.import_from_registry(&decoded);
        map.import_from_registry(&decoded);
        assert_eq!(map.len(), first_len);
        assert_eq!(cache_mappings(&decoded, &map), first);
    }

    #[test]
    fn import_from_registry_is_first_wins_on_duplicate_pubkey() {
        let mut state = BeaconState::<Minimal>::default();
        let pk = BlsPublicKey::from_array([0xABu8; 48]);
        for _ in 0..2 {
            state
                .validators_push(Validator {
                    pubkey: pk,
                    ..Validator::default()
                })
                .unwrap();
        }
        let mut map = PubkeyIndexMap::default();
        map.import_from_registry(&state);
        assert_eq!(map.len(), 1);
        assert_eq!(
            map.imported_len(),
            2,
            "fill progress is tracked apart from map.len()"
        );
        assert_eq!(map.get(&pk), Some(ValidatorIndex::new(0)));
        map.import_from_registry(&state);
        assert_eq!(map.imported_len(), 2);
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn import_from_registry_does_not_skip_tail_when_map_len_is_ahead() {
        let mut state = registry_state(2);
        let mut map = PubkeyIndexMap::default();
        map.import_from_registry(&state);
        assert_eq!(map.imported_len(), 2);
        assert_eq!(map.len(), 2);

        let extra = BlsPublicKey::from_array([0xEE; 48]);
        map.insert(extra, ValidatorIndex::new(1));
        assert_eq!(map.len(), 3);
        assert_eq!(
            map.imported_len(),
            2,
            "a scan insert must not advance fill progress"
        );

        let mut raw = [0u8; 48];
        raw[0] = 9;
        let pk_c = BlsPublicKey::from_array(raw);
        state
            .validators_push(Validator {
                pubkey: pk_c,
                ..Validator::default()
            })
            .unwrap();
        map.import_from_registry(&state);
        assert_eq!(map.get(&pk_c), Some(ValidatorIndex::new(2)));
        assert_eq!(map.imported_len(), 3);
        assert_ne!(map.imported_len(), map.len());

        let short = registry_state(1);
        map.import_from_registry(&short);
        assert_eq!(
            map.imported_len(),
            3,
            "a shorter registry must not rewind fill progress"
        );
    }
}
