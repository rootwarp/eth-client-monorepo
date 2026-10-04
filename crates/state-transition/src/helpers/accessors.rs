//! Spec accessors over [`BeaconState`] (Architecture §5.1 / §5.4).
//!
//! Fulu EIP-7917: `get_beacon_proposer_index` is a `proposer_lookahead` read.
//! [`get_beacon_committee`] is provided by [`crate::shuffling`] (cached) and
//! re-exported here so existing call sites keep the same import path.

use std::cell::RefCell;

use cc_crypto::{
    DOMAIN_BEACON_ATTESTER, compute_domain, compute_signing_root, fast_aggregate_verify, get_domain,
};
use cc_types::BeaconState;
use cc_types::config::ChainConfig;
use cc_types::containers::{AttestationData, Checkpoint, Validator};
use cc_types::operations::{Attestation, IndexedAttestation};
use cc_types::preset::Preset;
use cc_types::primitives::{CommitteeIndex, DomainType, Epoch, Gwei, Root, Slot, ValidatorIndex};
use ssz_types::BitVector;

use crate::error::BlockError;
use crate::helpers::constants::{
    BASE_REWARD_FACTOR, EFFECTIVE_BALANCE_INCREMENT, GENESIS_EPOCH,
    MIN_ATTESTATION_INCLUSION_DELAY, MIN_EPOCHS_TO_INACTIVITY_PENALTY, PARTICIPATION_FLAG_WEIGHTS,
    TIMELY_HEAD_FLAG_INDEX, TIMELY_SOURCE_FLAG_INDEX, TIMELY_TARGET_FLAG_INDEX,
};
use crate::helpers::misc::{
    compute_epoch_at_slot, compute_start_slot_at_epoch, integer_squareroot, u64_to_bytes_le,
};
use crate::helpers::predicates::is_active_validator;
use crate::signatures::{decode_signature, decode_state_pubkey};

/// `get_current_epoch(state)`.
#[inline]
pub fn get_current_epoch<P: Preset>(state: &BeaconState<P>) -> Epoch {
    compute_epoch_at_slot::<P>(state.slot())
}

/// Spec `get_previous_epoch`.
#[inline]
pub fn get_previous_epoch<P: Preset>(state: &BeaconState<P>) -> Epoch {
    let current = get_current_epoch(state);
    if current == GENESIS_EPOCH {
        GENESIS_EPOCH
    } else {
        Epoch::new(current.as_u64().saturating_sub(1))
    }
}

/// `get_beacon_proposer_index(state)` — Fulu EIP-7917.
///
/// ```text
/// state.proposer_lookahead[state.slot % SLOTS_PER_EPOCH]
/// ```
///
/// Does **not** touch the shuffling cache (Architecture §5.4).
pub fn get_beacon_proposer_index<P: Preset>(
    state: &BeaconState<P>,
) -> Result<ValidatorIndex, BlockError> {
    let offset = (state.slot().as_u64() % P::SLOTS_PER_EPOCH) as usize;
    state
        .proposer_lookahead_get(offset)
        .ok_or(BlockError::ArithmeticOverflow)
}

/// Spec `get_randao_mix(state, epoch)`.
#[inline]
pub fn get_randao_mix<P: Preset>(state: &BeaconState<P>, epoch: Epoch) -> Result<Root, BlockError> {
    let i = (epoch.as_u64() % P::EPOCHS_PER_HISTORICAL_VECTOR) as usize;
    state
        .randao_mixes_get(i)
        .ok_or(BlockError::ArithmeticOverflow)
}

/// Spec `compute_time_at_slot(state, slot)` with runtime `seconds_per_slot`.
#[inline]
pub fn compute_time_at_slot(genesis_time: u64, slot: Slot, seconds_per_slot: u64) -> u64 {
    genesis_time.saturating_add(slot.as_u64().saturating_mul(seconds_per_slot))
}

/// Spec `get_block_root_at_slot`.
pub fn get_block_root_at_slot<P: Preset>(
    state: &BeaconState<P>,
    slot: Slot,
) -> Result<Root, BlockError> {
    let state_slot = state.slot().as_u64();
    let s = slot.as_u64();
    if !(s < state_slot && state_slot <= s.saturating_add(P::SLOTS_PER_HISTORICAL_ROOT)) {
        return Err(invalid_op(
            "block_root",
            format!("slot {s} out of historical range (state_slot={state_slot})"),
        ));
    }
    let i = (s % P::SLOTS_PER_HISTORICAL_ROOT) as usize;
    state
        .block_roots_get(i)
        .ok_or(BlockError::ArithmeticOverflow)
}

/// Spec `get_block_root(state, epoch)`.
pub fn get_block_root<P: Preset>(state: &BeaconState<P>, epoch: Epoch) -> Result<Root, BlockError> {
    get_block_root_at_slot(state, compute_start_slot_at_epoch::<P>(epoch))
}

/// Spec `get_active_validator_indices`.
pub fn get_active_validator_indices<P: Preset>(
    state: &BeaconState<P>,
    epoch: Epoch,
) -> Vec<ValidatorIndex> {
    state
        .validators_iter()
        .enumerate()
        .filter(|(_, v)| is_active_validator(v, epoch))
        .map(|(i, _)| ValidatorIndex::new(i as u64))
        .collect()
}

/// Spec `get_total_balance` (min `EFFECTIVE_BALANCE_INCREMENT`).
pub fn get_total_balance<P: Preset>(
    state: &BeaconState<P>,
    indices: &[ValidatorIndex],
) -> Result<Gwei, BlockError> {
    let mut total = 0u64;
    for idx in indices {
        let v = state
            .validators_get(idx.as_u64() as usize)
            .ok_or(BlockError::ArithmeticOverflow)?;
        total = total
            .checked_add(v.effective_balance.as_u64())
            .ok_or(BlockError::ArithmeticOverflow)?;
    }
    Ok(Gwei::new(total.max(EFFECTIVE_BALANCE_INCREMENT.as_u64())))
}

/// Spec `get_total_active_balance`.
pub fn get_total_active_balance<P: Preset>(state: &BeaconState<P>) -> Result<Gwei, BlockError> {
    let indices = get_active_validator_indices(state, get_current_epoch(state));
    get_total_balance(state, &indices)
}

/// Spec `get_seed`.
pub fn get_seed<P: Preset>(
    state: &BeaconState<P>,
    epoch: Epoch,
    domain_type: DomainType,
) -> Result<Root, BlockError> {
    // Avoid underflow: epoch + EPOCHS_PER_HISTORICAL_VECTOR - MIN_SEED_LOOKAHEAD - 1
    let mix_epoch = Epoch::new(
        epoch
            .as_u64()
            .saturating_add(P::EPOCHS_PER_HISTORICAL_VECTOR)
            .saturating_sub(P::MIN_SEED_LOOKAHEAD)
            .saturating_sub(1),
    );
    let mix = get_randao_mix(state, mix_epoch)?;
    let mut input = [0u8; 4 + 8 + 32];
    input[0..4].copy_from_slice(domain_type.as_slice());
    input[4..12].copy_from_slice(&u64_to_bytes_le(epoch.as_u64()));
    input[12..44].copy_from_slice(mix.as_slice());
    Ok(Root::from_array(cc_crypto::hash_fixed(&input)))
}

/// Spec `get_committee_count_per_slot`.
pub fn get_committee_count_per_slot<P: Preset>(state: &BeaconState<P>, epoch: Epoch) -> u64 {
    let active = get_active_validator_indices(state, epoch).len() as u64;
    let count = active / P::SLOTS_PER_EPOCH / P::TARGET_COMMITTEE_SIZE;
    count.clamp(1, P::MAX_COMMITTEES_PER_SLOT)
}

/// Spec `get_beacon_committee` — cached via [`crate::shuffling`] (CC-13a).
///
/// Signature unchanged from the uncached helper: attestation handlers keep working.
#[inline]
pub fn get_beacon_committee<P: Preset>(
    state: &BeaconState<P>,
    slot: Slot,
    index: CommitteeIndex,
) -> Result<Vec<ValidatorIndex>, BlockError> {
    crate::shuffling::get_beacon_committee(state, slot, index)
}

/// Spec `get_committee_indices` (Electra).
pub fn get_committee_indices<P: Preset>(
    committee_bits: &BitVector<P::MaxCommitteesPerSlot>,
) -> Result<Vec<CommitteeIndex>, BlockError> {
    let mut out = Vec::new();
    let n = committee_bits.len();
    for i in 0..n {
        let bit = committee_bits
            .get(i)
            .map_err(|_| BlockError::ArithmeticOverflow)?;
        if bit {
            out.push(CommitteeIndex::new(i as u64));
        }
    }
    Ok(out)
}

/// Spec `get_attesting_indices` (Electra / EIP-7549).
pub fn get_attesting_indices<P: Preset>(
    state: &BeaconState<P>,
    attestation: &Attestation<P>,
) -> Result<Vec<ValidatorIndex>, BlockError> {
    let mut output = Vec::new();
    let committee_indices = get_committee_indices::<P>(&attestation.committee_bits)?;
    let mut committee_offset = 0usize;
    for committee_index in committee_indices {
        let committee = get_beacon_committee(state, attestation.data.slot, committee_index)?;
        for (i, attester_index) in committee.iter().enumerate() {
            let bit_i = committee_offset + i;
            if bit_i >= attestation.aggregation_bits.len() {
                return Err(invalid_op(
                    "attestation",
                    "aggregation_bits shorter than committee union",
                ));
            }
            let bit = attestation
                .aggregation_bits
                .get(bit_i)
                .map_err(|_| BlockError::ArithmeticOverflow)?;
            if bit {
                output.push(*attester_index);
            }
        }
        committee_offset += committee.len();
    }
    // Stable unique: keep first-seen order then sort for indexed attestation.
    Ok(output)
}

/// Spec `get_indexed_attestation` (Electra).
pub fn get_indexed_attestation<P: Preset>(
    state: &BeaconState<P>,
    attestation: &Attestation<P>,
) -> Result<IndexedAttestation<P>, BlockError> {
    let mut indices = get_attesting_indices(state, attestation)?;
    indices.sort_by_key(|i| i.as_u64());
    indices.dedup();
    let attesting_indices = ssz_types::VariableList::new(indices).map_err(|_| {
        invalid_op(
            "attestation",
            "attesting_indices exceeds MaxValidatorsPerSlot",
        )
    })?;
    Ok(IndexedAttestation {
        attesting_indices,
        data: attestation.data,
        signature: attestation.signature,
    })
}

/// Spec `is_valid_indexed_attestation`.
///
/// When `verify_signatures` is false, only structural checks run (sorted, unique,
/// non-empty). BLS is skipped for `bls_setting: 0` vector cases.
pub fn is_valid_indexed_attestation<P: Preset>(
    state: &BeaconState<P>,
    indexed: &IndexedAttestation<P>,
    verify_signatures: bool,
) -> Result<bool, BlockError> {
    let indices = &indexed.attesting_indices;
    if indices.is_empty() {
        return Ok(false);
    }
    // Sorted and unique.
    for window in indices.windows(2) {
        if window[0].as_u64() >= window[1].as_u64() {
            return Ok(false);
        }
    }

    if !verify_signatures {
        return Ok(true);
    }

    let mut pubkeys = Vec::with_capacity(indices.len());
    for idx in indices.iter() {
        let v = match state.validators_get(idx.as_u64() as usize) {
            Some(v) => v,
            None => return Ok(false), // out-of-range index → invalid attestation
        };
        pubkeys.push(decode_state_pubkey(&v.pubkey)?);
    }

    let domain = get_domain(
        &state.fork(),
        DOMAIN_BEACON_ATTESTER,
        Some(indexed.data.target.epoch),
        state.genesis_validators_root(),
    );
    let message = *compute_signing_root(&indexed.data, domain).as_array();
    let signature = decode_signature(&indexed.signature)?;
    Ok(fast_aggregate_verify(&pubkeys, &message, &signature))
}

/// Spec `get_base_reward_per_increment`.
pub fn get_base_reward_per_increment<P: Preset>(
    state: &BeaconState<P>,
) -> Result<Gwei, BlockError> {
    let total = get_total_active_balance(state)?;
    let sqrt = integer_squareroot(total.as_u64());
    if sqrt == 0 {
        return Err(BlockError::ArithmeticOverflow);
    }
    Ok(Gwei::new(
        EFFECTIVE_BALANCE_INCREMENT
            .as_u64()
            .saturating_mul(BASE_REWARD_FACTOR)
            / sqrt,
    ))
}

/// Spec `get_base_reward`.
///
/// Uses the epoch cache's `base_reward_per_increment` when valid for the
/// current epoch so an epoch-boundary rebuild supplies it once for all
/// validators (CC-13b); falls back to a direct compute on miss.
pub fn get_base_reward<P: Preset>(
    state: &BeaconState<P>,
    index: ValidatorIndex,
) -> Result<Gwei, BlockError> {
    let v = state
        .validators_get(index.as_u64() as usize)
        .ok_or(BlockError::ArithmeticOverflow)?;
    let increments = v.effective_balance.as_u64() / EFFECTIVE_BALANCE_INCREMENT.as_u64();
    let current = get_current_epoch(state);
    let per = if let Some(epoch) = state.caches().epoch.epoch
        && epoch == current
        && let Some(v) = state.caches().epoch.base_reward_per_increment
    {
        Gwei::new(v)
    } else {
        get_base_reward_per_increment(state)?
    };
    Ok(Gwei::new(increments.saturating_mul(per.as_u64())))
}

/// Spec `get_finality_delay`.
#[inline]
pub fn get_finality_delay<P: Preset>(state: &BeaconState<P>) -> u64 {
    get_previous_epoch(state)
        .as_u64()
        .saturating_sub(state.finalized_checkpoint().epoch.as_u64())
}

/// Spec `is_in_inactivity_leak`.
#[inline]
pub fn is_in_inactivity_leak<P: Preset>(state: &BeaconState<P>) -> bool {
    get_finality_delay(state) > MIN_EPOCHS_TO_INACTIVITY_PENALTY
}

/// Spec `get_eligible_validator_indices`.
pub fn get_eligible_validator_indices<P: Preset>(state: &BeaconState<P>) -> Vec<ValidatorIndex> {
    let previous_epoch = get_previous_epoch(state);
    state
        .validators_iter()
        .enumerate()
        .filter(|(_, v)| {
            is_active_validator(v, previous_epoch)
                || (v.slashed
                    && previous_epoch.as_u64().saturating_add(1) < v.withdrawable_epoch.as_u64())
        })
        .map(|(i, _)| ValidatorIndex::new(i as u64))
        .collect()
}

/// Spec `get_unslashed_participating_indices`.
pub fn get_unslashed_participating_indices<P: Preset>(
    state: &BeaconState<P>,
    flag_index: usize,
    epoch: Epoch,
) -> Result<Vec<ValidatorIndex>, BlockError> {
    let current = get_current_epoch(state);
    let previous = get_previous_epoch(state);
    if epoch != current && epoch != previous {
        return Err(invalid_op(
            "epoch",
            format!(
                "epoch {} is neither current ({}) nor previous ({})",
                epoch.as_u64(),
                current.as_u64(),
                previous.as_u64()
            ),
        ));
    }
    let active = get_active_validator_indices(state, epoch);
    let mut out = Vec::new();
    for index in active {
        let i = index.as_u64() as usize;
        let flags = if epoch == current {
            state
                .current_epoch_participation_get(i)
                .ok_or(BlockError::ArithmeticOverflow)?
        } else {
            state
                .previous_epoch_participation_get(i)
                .ok_or(BlockError::ArithmeticOverflow)?
        };
        if !crate::helpers::predicates::has_flag(flags, flag_index) {
            continue;
        }
        let v = state
            .validators_get(i)
            .ok_or(BlockError::ArithmeticOverflow)?;
        if !v.slashed {
            out.push(index);
        }
    }
    Ok(out)
}

/// Spec `get_attestation_participation_flag_indices`.
pub fn get_attestation_participation_flag_indices<P: Preset>(
    state: &BeaconState<P>,
    data: &AttestationData,
    inclusion_delay: u64,
) -> Result<Vec<usize>, BlockError> {
    let justified_checkpoint = if data.target.epoch == get_current_epoch(state) {
        state.current_justified_checkpoint()
    } else {
        state.previous_justified_checkpoint()
    };
    let is_matching_source = data.source == justified_checkpoint;

    let target_root = get_block_root(state, data.target.epoch)?;
    let is_matching_target = is_matching_source && data.target.root == target_root;

    let head_root = get_block_root_at_slot(state, data.slot)?;
    let is_matching_head = is_matching_target && data.beacon_block_root == head_root;

    if !is_matching_source {
        return Err(invalid_op(
            "attestation",
            "source does not match justified checkpoint",
        ));
    }

    let mut flags = Vec::new();
    if is_matching_source && inclusion_delay <= integer_squareroot(P::SLOTS_PER_EPOCH) {
        flags.push(TIMELY_SOURCE_FLAG_INDEX);
    }
    // Deneb EIP-7045: timely target ignores inclusion_delay.
    if is_matching_target {
        flags.push(TIMELY_TARGET_FLAG_INDEX);
    }
    if is_matching_head && inclusion_delay == MIN_ATTESTATION_INCLUSION_DELAY {
        flags.push(TIMELY_HEAD_FLAG_INDEX);
    }
    let _ = PARTICIPATION_FLAG_WEIGHTS; // referenced for docs linkage
    Ok(flags)
}

/// Spec `get_pending_balance_to_withdraw`.
pub fn get_pending_balance_to_withdraw<P: Preset>(
    state: &BeaconState<P>,
    validator_index: ValidatorIndex,
) -> Gwei {
    let mut sum = 0u64;
    for w in state.pending_partial_withdrawals_iter() {
        if w.validator_index == validator_index {
            sum = sum.saturating_add(w.amount.as_u64());
        }
    }
    Gwei::new(sum)
}

/// Spec `get_balance_churn_limit`.
///
/// `CHURN_LIMIT_QUOTIENT == 0` is [`BlockError::ArithmeticOverflow`], not a panic.
pub fn get_balance_churn_limit<P: Preset>(
    state: &BeaconState<P>,
    config: &ChainConfig,
) -> Result<Gwei, BlockError> {
    let total = get_total_active_balance(state)?;
    let by_quotient = total
        .as_u64()
        .checked_div(config.churn_limit_quotient)
        .ok_or(BlockError::ArithmeticOverflow)?;
    let churn = by_quotient.max(config.min_per_epoch_churn_limit_electra);
    // Align down to EFFECTIVE_BALANCE_INCREMENT.
    let aligned = churn - (churn % EFFECTIVE_BALANCE_INCREMENT.as_u64());
    Ok(Gwei::new(aligned))
}

/// Spec `get_activation_exit_churn_limit`.
pub fn get_activation_exit_churn_limit<P: Preset>(
    state: &BeaconState<P>,
    config: &ChainConfig,
) -> Result<Gwei, BlockError> {
    let balance_churn = get_balance_churn_limit(state, config)?;
    Ok(Gwei::new(
        balance_churn
            .as_u64()
            .min(config.max_per_epoch_activation_exit_churn_limit),
    ))
}

/// Spec `get_consolidation_churn_limit`.
pub fn get_consolidation_churn_limit<P: Preset>(
    state: &BeaconState<P>,
    config: &ChainConfig,
) -> Result<Gwei, BlockError> {
    let balance = get_balance_churn_limit(state, config)?;
    let activation_exit = get_activation_exit_churn_limit(state, config)?;
    Ok(Gwei::new(
        balance.as_u64().saturating_sub(activation_exit.as_u64()),
    ))
}

/// Resolve a validator index by pubkey via [`cc_types::PubkeyIndexMap`], falling
/// back to a linear registry scan that is counted on the map (CC-12d).
///
/// A hit is revalidated against `state`: the validator at the cached index must
/// carry `pubkey`. A mismatch — one context shared across sibling registries —
/// falls through to the scan. The stale mapping is left in place when the scan
/// misses, so the other sibling still hits. Prefer this when a miss is possible;
/// `process_sync_aggregate` uses the map only (no scan). The map lives on
/// `TransitionContext` (S2-A-10 / S2-A-11).
pub fn get_validator_index_by_pubkey<P: Preset>(
    state: &BeaconState<P>,
    pubkey: &cc_types::primitives::BlsPublicKey,
    cache: &RefCell<cc_types::PubkeyIndexMap>,
) -> Option<ValidatorIndex> {
    let validated_hit = {
        let map = cache.borrow();
        map.get(pubkey).filter(|idx| {
            state
                .validators_get(idx.as_u64() as usize)
                .is_some_and(|v| &v.pubkey == pubkey)
        })
    };
    if let Some(idx) = validated_hit {
        return Some(idx);
    }
    cache.borrow_mut().note_linear_scan();
    let found = state
        .validators_iter()
        .enumerate()
        .find(|(_, v)| v.pubkey == *pubkey)
        .map(|(i, _)| ValidatorIndex::new(i as u64));
    if let Some(idx) = found {
        cache.borrow_mut().insert(*pubkey, idx);
    }
    found
}

/// Domain helper matching `get_domain(state, domain_type, epoch)`.
pub fn state_get_domain<P: Preset>(
    state: &BeaconState<P>,
    domain_type: DomainType,
    epoch: Option<Epoch>,
) -> cc_types::primitives::Domain {
    let epoch = epoch.unwrap_or_else(|| get_current_epoch(state));
    get_domain(
        &state.fork(),
        domain_type,
        Some(epoch),
        state.genesis_validators_root(),
    )
}

/// Build a deposit domain (fork-agnostic, zero genesis validators root).
pub fn deposit_domain() -> cc_types::primitives::Domain {
    compute_domain(cc_crypto::DOMAIN_DEPOSIT, None, None)
}

fn invalid_op(op: &'static str, detail: impl Into<String>) -> BlockError {
    BlockError::InvalidOperation(crate::error::OperationError::Invalid {
        op,
        detail: detail.into(),
    })
}

/// Re-export checkpoint equality helper for tests.
#[inline]
pub fn checkpoints_equal(a: Checkpoint, b: Checkpoint) -> bool {
    a == b
}

/// Validator borrow by index with operation error.
pub fn validator_at<'a, P: Preset>(
    state: &'a BeaconState<P>,
    index: ValidatorIndex,
    op: &'static str,
) -> Result<&'a Validator, BlockError> {
    state
        .validators_get(index.as_u64() as usize)
        .ok_or_else(|| {
            invalid_op(
                op,
                format!("validator index {} out of range", index.as_u64()),
            )
        })
}

/// Mutable validator borrow.
pub fn validator_at_mut<'a, P: Preset>(
    state: &'a mut BeaconState<P>,
    index: ValidatorIndex,
    op: &'static str,
) -> Result<&'a mut Validator, BlockError> {
    let i = index.as_u64() as usize;
    // Force dirty mark via get_mut.
    if state.validators_get(i).is_none() {
        return Err(invalid_op(
            op,
            format!("validator index {} out of range", index.as_u64()),
        ));
    }
    state.validators_get_mut(i).ok_or_else(|| {
        invalid_op(
            op,
            format!("validator index {} out of range", index.as_u64()),
        )
    })
}
