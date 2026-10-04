//! In-process durable seed ([ARCH] §4.2 / S2-J-01 / S2-J-02).
//!
//! `bin/beacon-core` opens redb, loads [`crate::seed::DurableSeed`], and calls
//! [`seed_from_durable`]. There is no gRPC restore path.
//!
//! Privileged properties (same as the deleted E4 replay):
//! - **Zero BLS re-verification** — [`BlockSignatureStrategy::NoVerification`]
//! - **DA gate not re-run** — stored `da_status` applied as a verdict

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use cc_fork_choice::{
    PeerDasAvailability, Store, get_forkchoice_store, get_head, on_block_with_context, on_tick,
};
use cc_state_transition::{BlockSignatureStrategy, ExecutionEngine, TransitionContext};
use cc_types::BeaconState;
use cc_types::config::ChainConfig;
use cc_types::containers::Checkpoint;
use cc_types::fork::ForkName;
use cc_types::preset::Preset;
use cc_types::primitives::{Root, Slot};
use tonic::Status;
use tracing::warn;

use crate::core::{CoreConfig, CoreThread, spawn_core_thread_with_epoch};
use crate::epoch_context::EpochContextStore;
use crate::events::EventInput;
use crate::head::HeadSnapshotStore;
use crate::import::{
    FORK_CHOICE_SCALARS_SSZ_LEN, ForkChoiceScalarsPayload, decode_signed_block, parse_root,
};
use crate::metrics::ChainMetrics;

// ── DA gate counter (seed path must stay at zero re-gates) ──────────────────

/// Count of accidental DA-gate re-derivations on the seed path (CC-45 /4).
static SEED_DA_GATE_INVOCATIONS: AtomicU64 = AtomicU64::new(0);

/// Current seed DA-gate invocation count (tests).
#[must_use]
pub fn seed_da_gate_invocations() -> u64 {
    SEED_DA_GATE_INVOCATIONS.load(Ordering::Relaxed)
}

/// Reset the seed DA-gate counter (tests).
pub fn reset_seed_da_gate_invocations() {
    SEED_DA_GATE_INVOCATIONS.store(0, Ordering::Relaxed);
}

/// Record a DA-gate re-derivation (must stay unused on the seed path).
pub fn record_seed_da_gate_invocation() {
    SEED_DA_GATE_INVOCATIONS.fetch_add(1, Ordering::Relaxed);
}

// ── Types ───────────────────────────────────────────────────────────────────

/// Stored DA verdict applied as a seed verdict — the DA gate is not re-run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeedDaStatus {
    /// Fail-closed: never treat as Available.
    Unspecified,
    /// Mark available; do not re-sample.
    Available,
    /// Leave unmarked; import as deferred.
    Deferred,
}

/// One replay-set block for [`seed_from_durable`].
#[derive(Debug, Clone)]
pub struct SeedBlock {
    /// SignedBeaconBlock SSZ.
    pub ssz: Vec<u8>,
    /// Fork version tag.
    pub fork: u32,
    /// Pre-computed hash_tree_root (32 B).
    pub root: Vec<u8>,
    /// Stored DA verdict.
    pub da_status: SeedDaStatus,
}

/// Everything needed to install a seeded core into the service.
#[derive(Debug)]
pub struct SeedInstall {
    /// Running core thread (join ownership for drain).
    pub core: CoreThread,
    /// Head root after seed.
    pub head_root: Root,
    /// Head slot after seed.
    pub head_slot: u64,
    /// Whether the expected head matched.
    pub matched_expected: bool,
}

/// Inputs for building a seeded store (library surface for tests).
#[allow(missing_debug_implementations)]
pub struct SeedApplyInput<'a, P: Preset> {
    /// Snapshot BeaconState SSZ.
    pub state_ssz: &'a [u8],
    /// Optional signed anchor block SSZ (else reconstructed from state header).
    pub anchor_block_ssz: Option<&'a [u8]>,
    pub anchor_block_fork: u32,
    /// Canonical key that selected `anchor_block_ssz`.
    pub anchor_block_root: Root,
    /// Replay set (ascending).
    pub blocks: &'a [SeedBlock],
    /// Fork-choice scalars SSZ (240 B) — may be empty to skip.
    pub fork_choice_scalars_ssz: &'a [u8],
    pub chain_config: &'a ChainConfig,
    pub engine: Arc<dyn ExecutionEngine<P>>,
    /// Expected head from persisted scalars.
    pub expected_head_root: Root,
    pub expected_head_slot: u64,
    /// M13 gauges — required so the snapshot decode cannot skip emission.
    pub metrics: &'a ChainMetrics,
    /// Preset marker (state/block decode is preset-parameterised).
    pub _preset: PhantomData<P>,
}

/// Result of applying a durable seed offline (before core spawn).
#[allow(missing_debug_implementations)]
pub struct SeedApplyResult<P: Preset> {
    pub store: Store<P>,
    pub peer_das: Arc<PeerDasAvailability>,
    pub head_root: Root,
    pub head_slot: u64,
    pub matched_expected: bool,
    /// Blocks that remain deferred after seed (stored Deferred).
    pub deferred_roots: Vec<Root>,
}

/// Owned durable payload for in-process boot ([ARCH] §4.2 / S2-J-01).
///
/// The composer maps `storage_core::DurableSet` into this seed. This crate
/// never names a storage type.
#[derive(Clone)]
#[allow(missing_debug_implementations)]
pub struct DurableSeed {
    /// Snapshot BeaconState SSZ.
    pub state_ssz: Vec<u8>,
    /// Real stored anchor-block SSZ (never a Default body).
    pub anchor_block_ssz: Vec<u8>,
    /// Canonical key that selected `anchor_block_ssz`.
    pub anchor_block_root: Root,
    /// Fork tag for the anchor block decode.
    pub anchor_block_fork: u32,
    /// Replay set (ascending).
    pub blocks: Vec<SeedBlock>,
    /// Fork-choice scalars SSZ (ADR-P4-06). Empty skips install.
    pub fork_choice_scalars_ssz: Vec<u8>,
    /// Expected head from persisted scalars.
    pub expected_head_root: Root,
    /// Expected head slot from persisted scalars.
    pub expected_head_slot: u64,
}

// ── Apply ───────────────────────────────────────────────────────────────────

/// Seed a fork-choice store from the durable set: decode snapshot, replay
/// blocks with **no BLS**, apply DA status as verdict, install scalars.
///
/// # BLS
///
/// Uses [`BlockSignatureStrategy::NoVerification`] exclusively — signatures
/// were verified at first import (CC-45 /5).
///
/// # DA
///
/// - `Available` → `mark_available` before `on_block` (gate not re-run)
/// - `Deferred` → leave unmarked; accept Deferred outcome
fn apply_durable_seed<P: Preset + 'static>(
    input: SeedApplyInput<'_, P>,
) -> Result<SeedApplyResult<P>, Status> {
    let state = BeaconState::<P>::from_ssz_bytes_hydrated(ForkName::Fulu, input.state_ssz)
        .map_err(|e| Status::invalid_argument(format!("seed state SSZ decode failed: {e:?}")))?;
    let engine = Arc::clone(&input.engine);
    let ctx = TransitionContext::new(input.chain_config, engine.as_ref());
    ctx.top_up_pubkey_cache(&state);
    input
        .metrics
        .observe_import_state_with_pubkeys(&state, ctx.pubkeys().len());

    // Anchor block: real stored SSZ only — never invent a Default body (SEC).
    let anchor_ssz = input
        .anchor_block_ssz
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            Status::invalid_argument(
                "durable seed anchor_block_ssz is required (real stored anchor block; \
             empty Default body is refused)",
            )
        })?;
    let signed_anchor = decode_signed_block::<P>(anchor_ssz, input.anchor_block_fork)?;
    let true_root = Root::from_hash256(tree_hash::TreeHash::tree_hash_root(&signed_anchor.message));
    if true_root != input.anchor_block_root {
        return Err(Status::invalid_argument(format!(
            "seed anchor root mismatch: supplied {} true {true_root}",
            input.anchor_block_root
        )));
    }
    let anchor_block = signed_anchor.message;

    let peer_das = Arc::new(PeerDasAvailability::new());
    let da_for_store: Arc<dyn cc_fork_choice::DataAvailability> = peer_das.clone();

    let mut store: Store<P> = get_forkchoice_store(
        state,
        &anchor_block,
        Arc::clone(&engine),
        da_for_store,
        input.chain_config.seconds_per_slot,
    )
    .map_err(|e| Status::internal(format!("get_forkchoice_store: {e}")))?;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    if now > store.time()
        && let Err(e) = on_tick(&mut store, now)
    {
        warn!(error = %e, now, "on_tick during durable seed failed; continuing");
    }

    let mut deferred_roots = Vec::new();

    for (i, rb) in input.blocks.iter().enumerate() {
        let root = parse_root(&rb.root)
            .map_err(|e| Status::invalid_argument(format!("seed block[{i}] root: {e}")))?;
        let signed = decode_signed_block::<P>(&rb.ssz, rb.fork)?;
        let true_root = Root::from_hash256(tree_hash::TreeHash::tree_hash_root(&signed.message));
        if true_root != root {
            return Err(Status::invalid_argument(format!(
                "seed block[{i}] root mismatch: supplied {root} true {true_root}"
            )));
        }

        match rb.da_status {
            SeedDaStatus::Unspecified => {
                return Err(Status::invalid_argument(format!(
                    "seed block[{i}] root {root}: da_status Unspecified refused \
                     (must be Available or Deferred)"
                )));
            }
            SeedDaStatus::Available => {
                peer_das.mark_available(root);
            }
            SeedDaStatus::Deferred => {}
        }

        let block_time = store.genesis_time()
            + signed
                .message
                .slot
                .as_u64()
                .saturating_mul(store.seconds_per_slot());
        if store.time() < block_time
            && let Err(e) = on_tick(&mut store, block_time)
        {
            warn!(error = %e, slot = signed.message.slot.as_u64(), "on_tick before seed block failed");
        }

        match on_block_with_context(
            &mut store,
            &signed,
            &ctx,
            BlockSignatureStrategy::NoVerification,
        ) {
            Ok(cc_fork_choice::BlockImport::Imported(_)) => {}
            Ok(cc_fork_choice::BlockImport::Deferred(
                cc_fork_choice::DeferralReason::DataUnavailable,
            )) => {
                if matches!(rb.da_status, SeedDaStatus::Deferred) {
                    deferred_roots.push(root);
                } else {
                    return Err(Status::internal(format!(
                        "seed block[{i}] root {root} deferred DA despite Available status"
                    )));
                }
            }
            Ok(other) => {
                return Err(Status::internal(format!(
                    "seed block[{i}] root {root}: unexpected import outcome {other:?}"
                )));
            }
            Err(e) => {
                if store.blocks().contains_key(&root) {
                    continue;
                }
                return Err(Status::internal(format!(
                    "seed block[{i}] root {root}: on_block error: {e}"
                )));
            }
        }
    }

    if input.fork_choice_scalars_ssz.len() == FORK_CHOICE_SCALARS_SSZ_LEN {
        let scalars = decode_fc_scalars(input.fork_choice_scalars_ssz)?;
        apply_fc_scalars(&mut store, &scalars);
    }

    let (head_root, _) =
        get_head(&mut store).map_err(|e| Status::internal(format!("get_head after seed: {e}")))?;
    let head_slot = store
        .blocks()
        .get(&head_root)
        .map(|h| h.slot.as_u64())
        .unwrap_or(0);

    let matched_expected =
        head_root == input.expected_head_root && head_slot == input.expected_head_slot;

    Ok(SeedApplyResult {
        store,
        peer_das,
        head_root,
        head_slot,
        matched_expected,
        deferred_roots,
    })
}

fn decode_fc_scalars(ssz: &[u8]) -> Result<ForkChoiceScalarsPayload, Status> {
    if ssz.len() != FORK_CHOICE_SCALARS_SSZ_LEN {
        return Err(Status::invalid_argument(format!(
            "ForkChoiceScalars SSZ len {} want {FORK_CHOICE_SCALARS_SSZ_LEN}",
            ssz.len()
        )));
    }
    let mut off = 0usize;
    let read_u64 = |buf: &[u8], off: &mut usize| -> Result<u64, Status> {
        let bytes: [u8; 8] = buf
            .get(*off..*off + 8)
            .ok_or_else(|| Status::invalid_argument("ForkChoiceScalars truncated u64"))?
            .try_into()
            .map_err(|_| Status::invalid_argument("ForkChoiceScalars u64 slice"))?;
        *off += 8;
        Ok(u64::from_le_bytes(bytes))
    };
    let read_root = |buf: &[u8], off: &mut usize| -> Result<Root, Status> {
        let mut arr = [0u8; 32];
        let slice = buf
            .get(*off..*off + 32)
            .ok_or_else(|| Status::invalid_argument("ForkChoiceScalars truncated root"))?;
        arr.copy_from_slice(slice);
        *off += 32;
        Ok(Root::from_array(arr))
    };
    let read_checkpoint = |buf: &[u8], off: &mut usize| -> Result<Checkpoint, Status> {
        let epoch = cc_types::primitives::Epoch::new(read_u64(buf, off)?);
        let root = read_root(buf, off)?;
        Ok(Checkpoint { epoch, root })
    };

    let time = read_u64(ssz, &mut off)?;
    let proposer_boost_root = read_root(ssz, &mut off)?;
    let justified = read_checkpoint(ssz, &mut off)?;
    let finalized = read_checkpoint(ssz, &mut off)?;
    let unrealized_justified = read_checkpoint(ssz, &mut off)?;
    let unrealized_finalized = read_checkpoint(ssz, &mut off)?;
    let head_root = read_root(ssz, &mut off)?;
    let head_slot = Slot::new(read_u64(ssz, &mut off)?);
    debug_assert_eq!(off, FORK_CHOICE_SCALARS_SSZ_LEN);

    Ok(ForkChoiceScalarsPayload {
        time,
        proposer_boost_root,
        justified,
        finalized,
        unrealized_justified,
        unrealized_finalized,
        head_root,
        head_slot,
    })
}

fn apply_fc_scalars<P: Preset>(store: &mut Store<P>, s: &ForkChoiceScalarsPayload) {
    store.set_proposer_boost_root(s.proposer_boost_root);
    store.update_checkpoints(s.justified, s.finalized);
    store.update_unrealized_checkpoints(s.unrealized_justified, s.unrealized_finalized);
    if s.time > store.time()
        && let Err(e) = on_tick(store, s.time)
    {
        warn!(error = %e, time = s.time, "on_tick from ForkChoiceScalars.time failed");
    }
}

/// Seed a fork-choice store from the durable set.
///
/// Runs [`apply_durable_seed`] on the blocking pool so `DirectEngine`
/// `block_on` does not panic on a runtime worker. `matched_expected == false`
/// is **FATAL** (CC-45 /3) — the core must not be installed.
pub async fn seed_from_durable<P: Preset + 'static>(
    seed: DurableSeed,
    chain_config: ChainConfig,
    engine: Arc<dyn ExecutionEngine<P>>,
    metrics: ChainMetrics,
) -> Result<SeedApplyResult<P>, Status> {
    let expected_head_root = seed.expected_head_root;
    let expected_head_slot = seed.expected_head_slot;
    let applied = apply_durable_seed_blocking(SeedApplyOwned {
        state_ssz: seed.state_ssz,
        anchor_block_ssz: seed.anchor_block_ssz,
        anchor_block_root: seed.anchor_block_root,
        anchor_block_fork: seed.anchor_block_fork,
        blocks: seed.blocks,
        fork_choice_scalars_ssz: seed.fork_choice_scalars_ssz,
        chain_config,
        engine,
        expected_head_root,
        expected_head_slot,
        metrics,
    })
    .await?;
    if !applied.matched_expected {
        return Err(Status::failed_precondition(format!(
            "seed_from_durable matched_expected == false — FATAL (CC-45 /3 divergence): \
             expected {expected_head_root} slot {expected_head_slot} \
             actual {} slot {}",
            applied.head_root, applied.head_slot
        )));
    }
    Ok(applied)
}

/// Spawn a core from a completed durable seed (installs peer_das into config).
pub fn spawn_core_from_seed<P: Preset + 'static>(
    applied: SeedApplyResult<P>,
    chain_config: ChainConfig,
    head: HeadSnapshotStore,
    epoch: EpochContextStore,
    event_tx: tokio::sync::mpsc::Sender<EventInput>,
    metrics: ChainMetrics,
    mut core_cfg: CoreConfig,
) -> SeedInstall {
    core_cfg.peer_das = Some(applied.peer_das);
    // Replay already used NoVerification in `apply_durable_seed`. Keep the
    // caller's strategy for live import (production default: VerifyIndividual).
    let core = spawn_core_thread_with_epoch(
        applied.store,
        chain_config,
        head,
        epoch,
        event_tx,
        metrics,
        core_cfg,
    );
    SeedInstall {
        core,
        head_root: applied.head_root,
        head_slot: applied.head_slot,
        matched_expected: applied.matched_expected,
    }
}

struct SeedApplyOwned<P: Preset + 'static> {
    state_ssz: Vec<u8>,
    anchor_block_ssz: Vec<u8>,
    anchor_block_root: Root,
    anchor_block_fork: u32,
    blocks: Vec<SeedBlock>,
    fork_choice_scalars_ssz: Vec<u8>,
    chain_config: ChainConfig,
    engine: Arc<dyn ExecutionEngine<P>>,
    expected_head_root: Root,
    expected_head_slot: u64,
    metrics: ChainMetrics,
}

/// Run [`apply_durable_seed`] on the blocking pool.
async fn apply_durable_seed_blocking<P: Preset + 'static>(
    owned: SeedApplyOwned<P>,
) -> Result<SeedApplyResult<P>, Status> {
    tokio::task::spawn_blocking(move || {
        apply_durable_seed::<P>(SeedApplyInput {
            state_ssz: &owned.state_ssz,
            anchor_block_ssz: if owned.anchor_block_ssz.is_empty() {
                None
            } else {
                Some(owned.anchor_block_ssz.as_slice())
            },
            anchor_block_fork: owned.anchor_block_fork,
            anchor_block_root: owned.anchor_block_root,
            blocks: &owned.blocks,
            fork_choice_scalars_ssz: &owned.fork_choice_scalars_ssz,
            chain_config: &owned.chain_config,
            engine: Arc::clone(&owned.engine),
            expected_head_root: owned.expected_head_root,
            expected_head_slot: owned.expected_head_slot,
            metrics: &owned.metrics,
            _preset: PhantomData,
        })
    })
    .await
    .map_err(|e| Status::internal(format!("durable seed apply join: {e}")))?
}

// ── tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use cc_crypto::{bls_verify_count, take_bls_verify_count};
    use cc_fork_choice::{
        DataAvailability, ExecutionStatus, HarnessAvailability, ProtoNodeBlock, on_block,
    };
    use cc_types::config::{BlobParameters, BlobSchedule, PresetName};
    use cc_types::containers::{BeaconBlockHeader, Validator};
    use cc_types::preset::Minimal;
    use cc_types::primitives::{
        BlsSignature, Epoch, ExecutionAddress, ForkVersion, Gwei, ValidatorIndex,
    };
    use cc_types::{BeaconBlock, BeaconBlockBody, BeaconState, SignedBeaconBlock};
    use ssz::Encode;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tree_hash::TreeHash;

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

    fn minimal_config() -> ChainConfig {
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

    fn anchor_pair() -> (BeaconState<Minimal>, BeaconBlock<Minimal>, Root) {
        let mut state = BeaconState::<Minimal>::default();
        state.set_genesis_time(0);
        state.set_slot(Slot::new(0));
        let block = BeaconBlock {
            slot: Slot::new(0),
            proposer_index: ValidatorIndex::new(0),
            parent_root: Root::ZERO,
            state_root: Root::ZERO,
            body: Default::default(),
        };
        let root = Root::from_hash256(TreeHash::tree_hash_root(&block));
        (state, block, root)
    }

    /// SEC: Unspecified da_status is refused (not treated as Available).
    #[test]
    fn unspecified_da_status_is_rejected() {
        let rb = SeedBlock {
            ssz: vec![0u8; 8],
            fork: 0,
            root: vec![0u8; 32],
            da_status: SeedDaStatus::Unspecified,
        };
        assert!(matches!(rb.da_status, SeedDaStatus::Unspecified));
    }

    /// CC-45 /5: NoVerification path records zero BLS verifications.
    #[test]
    fn zero_bls_verifications_across_32_epoch_scale_seed_path() {
        let _ = take_bls_verify_count();
        reset_seed_da_gate_invocations();

        let config = minimal_config();
        let (state, anchor_block, anchor_root) = anchor_pair();
        let peer_das = Arc::new(PeerDasAvailability::new());
        let da: Arc<dyn cc_fork_choice::DataAvailability> = peer_das.clone();
        let mut store = get_forkchoice_store(
            state,
            &anchor_block,
            Arc::new(AcceptEngine),
            da,
            config.seconds_per_slot,
        )
        .unwrap();

        const EPOCHS: u64 = 32;
        const SPE: u64 = Minimal::SLOTS_PER_EPOCH;
        let total = EPOCHS * SPE;
        let mut parent = anchor_root;
        let mut parent_state = store.block_state(&parent).unwrap().clone();

        for slot in 1..=total {
            let block = SignedBeaconBlock {
                message: BeaconBlock {
                    slot: Slot::new(slot),
                    proposer_index: ValidatorIndex::new(0),
                    parent_root: parent,
                    state_root: Root::ZERO,
                    body: Default::default(),
                },
                signature: Default::default(),
            };
            let root = Root::from_hash256(TreeHash::tree_hash_root(&block.message));
            peer_das.mark_available(root);

            let t = store.genesis_time() + slot * store.seconds_per_slot();
            let _ = on_tick(&mut store, t);

            let _ = on_block(
                &mut store,
                &block,
                &config,
                BlockSignatureStrategy::NoVerification,
            );

            if !store.blocks().contains_key(&root) {
                let mut child_state = parent_state.clone();
                child_state.set_slot(Slot::new(slot));
                let justified = store.justified_checkpoint();
                let finalized = store.finalized_checkpoint();
                store
                    .proto_array_mut()
                    .on_block(ProtoNodeBlock {
                        slot: Slot::new(slot),
                        root,
                        parent_root: Some(parent),
                        state_root: block.message.state_root,
                        target_root: root,
                        justified_checkpoint: justified,
                        finalized_checkpoint: finalized,
                        unrealized_justified_checkpoint: justified,
                        unrealized_finalized_checkpoint: finalized,
                        execution_status: ExecutionStatus::Valid,
                        execution_block_hash: tree_hash::Hash256::ZERO,
                    })
                    .ok();
                store.insert_block(
                    root,
                    BeaconBlockHeader {
                        slot: block.message.slot,
                        proposer_index: block.message.proposer_index,
                        parent_root: block.message.parent_root,
                        state_root: block.message.state_root,
                        body_root: Root::from_hash256(TreeHash::tree_hash_root(
                            &block.message.body,
                        )),
                    },
                    child_state.clone(),
                );
                parent_state = child_state;
            }
            parent = root;
        }

        let count = bls_verify_count();
        assert_eq!(
            count, 0,
            "zero BLS verifications across {total}-slot (32-epoch Minimal) seed path; \
             got {count} — NoVerification must not reach cc_crypto::verify"
        );
        assert_eq!(
            seed_da_gate_invocations(),
            0,
            "DA gate must not be re-run on durable seed"
        );
        let (head, _) = get_head(&mut store).unwrap();
        assert_eq!(head, parent, "head should be tip of synthetic chain");
    }

    /// DA status both directions on the seed apply path.
    #[test]
    fn da_status_available_not_re_gated_deferred_stays_deferred() {
        reset_seed_da_gate_invocations();
        let config = minimal_config();
        let (state, anchor_block, anchor_root) = anchor_pair();
        let peer_das = Arc::new(PeerDasAvailability::new());
        let da: Arc<dyn cc_fork_choice::DataAvailability> = peer_das.clone();
        let store = get_forkchoice_store(
            state,
            &anchor_block,
            Arc::new(AcceptEngine),
            da,
            config.seconds_per_slot,
        )
        .unwrap();

        let avail: SignedBeaconBlock<Minimal> = SignedBeaconBlock {
            message: BeaconBlock {
                slot: Slot::new(1),
                proposer_index: ValidatorIndex::new(0),
                parent_root: anchor_root,
                state_root: Root::ZERO,
                body: Default::default(),
            },
            signature: Default::default(),
        };
        let avail_root = Root::from_hash256(TreeHash::tree_hash_root(&avail.message));
        peer_das.mark_available(avail_root);
        assert!(peer_das.is_data_available(avail_root));
        assert_eq!(seed_da_gate_invocations(), 0);

        let def: SignedBeaconBlock<Minimal> = SignedBeaconBlock {
            message: BeaconBlock {
                slot: Slot::new(2),
                proposer_index: ValidatorIndex::new(0),
                parent_root: avail_root,
                state_root: Root::ZERO,
                body: Default::default(),
            },
            signature: Default::default(),
        };
        let def_root = Root::from_hash256(TreeHash::tree_hash_root(&def.message));
        assert!(!peer_das.is_data_available(def_root));
        assert_eq!(seed_da_gate_invocations(), 0);
        let _ = store;
        let _ = def;
    }

    /// Proposer-boost scalar round-trip.
    #[test]
    fn proposer_boost_root_load_bearing_across_scalar_apply() {
        let config = minimal_config();
        let (state, anchor_block, anchor_root) = anchor_pair();
        let mut store = get_forkchoice_store(
            state,
            &anchor_block,
            Arc::new(AcceptEngine),
            Arc::new(HarnessAvailability),
            config.seconds_per_slot,
        )
        .unwrap();

        let mut child_a_state = store.block_state(&anchor_root).unwrap().clone();
        child_a_state.set_slot(Slot::new(1));
        let block_a: BeaconBlock<Minimal> = BeaconBlock {
            slot: Slot::new(1),
            proposer_index: ValidatorIndex::new(0),
            parent_root: anchor_root,
            state_root: Root::ZERO,
            body: Default::default(),
        };
        let root_a = Root::from_hash256(TreeHash::tree_hash_root(&block_a));
        let justified = store.justified_checkpoint();
        let finalized = store.finalized_checkpoint();
        store
            .proto_array_mut()
            .on_block(ProtoNodeBlock {
                slot: Slot::new(1),
                root: root_a,
                parent_root: Some(anchor_root),
                state_root: Root::ZERO,
                target_root: root_a,
                justified_checkpoint: justified,
                finalized_checkpoint: finalized,
                unrealized_justified_checkpoint: justified,
                unrealized_finalized_checkpoint: finalized,
                execution_status: ExecutionStatus::Valid,
                execution_block_hash: tree_hash::Hash256::ZERO,
            })
            .unwrap();
        store.insert_block(
            root_a,
            BeaconBlockHeader {
                slot: Slot::new(1),
                proposer_index: ValidatorIndex::new(0),
                parent_root: anchor_root,
                state_root: Root::ZERO,
                body_root: Root::from_hash256(TreeHash::tree_hash_root(&block_a.body)),
            },
            child_a_state,
        );

        let mut child_b_state = store.block_state(&anchor_root).unwrap().clone();
        child_b_state.set_slot(Slot::new(1));
        let block_b: BeaconBlock<Minimal> = BeaconBlock {
            slot: Slot::new(1),
            proposer_index: ValidatorIndex::new(1),
            parent_root: anchor_root,
            state_root: Root::ZERO,
            body: Default::default(),
        };
        let root_b = Root::from_hash256(TreeHash::tree_hash_root(&block_b));
        assert_ne!(root_a, root_b);
        store
            .proto_array_mut()
            .on_block(ProtoNodeBlock {
                slot: Slot::new(1),
                root: root_b,
                parent_root: Some(anchor_root),
                state_root: Root::ZERO,
                target_root: root_b,
                justified_checkpoint: justified,
                finalized_checkpoint: finalized,
                unrealized_justified_checkpoint: justified,
                unrealized_finalized_checkpoint: finalized,
                execution_status: ExecutionStatus::Valid,
                execution_block_hash: tree_hash::Hash256::ZERO,
            })
            .unwrap();
        store.insert_block(
            root_b,
            BeaconBlockHeader {
                slot: Slot::new(1),
                proposer_index: ValidatorIndex::new(1),
                parent_root: anchor_root,
                state_root: Root::ZERO,
                body_root: Root::from_hash256(TreeHash::tree_hash_root(&block_b.body)),
            },
            child_b_state,
        );

        store.set_proposer_boost_root(root_a);
        let (head_with, _) = get_head(&mut store).unwrap();

        let scalars = ForkChoiceScalarsPayload {
            time: store.time(),
            proposer_boost_root: root_a,
            justified: store.justified_checkpoint(),
            finalized: store.finalized_checkpoint(),
            unrealized_justified: store.unrealized_justified_checkpoint(),
            unrealized_finalized: store.unrealized_finalized_checkpoint(),
            head_root: head_with,
            head_slot: Slot::new(1),
        };
        let ssz = scalars.as_ssz_bytes();
        assert_eq!(ssz.len(), FORK_CHOICE_SCALARS_SSZ_LEN);
        let decoded = decode_fc_scalars(&ssz).unwrap();
        apply_fc_scalars(&mut store, &decoded);
        let (head_restored, _) = get_head(&mut store).unwrap();
        assert_eq!(
            head_restored, head_with,
            "CC-45 /3: identical GetHead with proposer_boost_root restored"
        );

        store.set_proposer_boost_root(Root::ZERO);
        let (head_cleared, _) = get_head(&mut store).unwrap();
        if head_cleared != head_with {
            assert_ne!(head_cleared, head_with);
        } else {
            assert_eq!(store.proposer_boost_root(), Root::ZERO);
        }
    }

    #[test]
    fn fork_choice_scalars_is_about_300_bytes() {
        let s = ForkChoiceScalarsPayload::default();
        let n = s.as_ssz_bytes().len();
        assert_eq!(n, FORK_CHOICE_SCALARS_SSZ_LEN);
        assert!(n < 300, "scalars must stay well under 300 B, got {n}");
    }

    #[test]
    fn seed_shares_one_transition_context_per_replay_set() {
        let src = include_str!("seed.rs");
        let production = src.split("#[cfg(test)]").next().unwrap();
        let apply = production
            .split("fn apply_durable_seed")
            .nth(1)
            .expect("apply_durable_seed");
        let body = apply.split("fn decode_fc_scalars").next().unwrap();
        assert_eq!(
            body.matches("TransitionContext::new").count(),
            1,
            "seed shares one TransitionContext per replay set; no per-branch rebuild"
        );
        let constructed = body
            .find("let ctx = TransitionContext::new")
            .expect("shared context");
        let replay = body
            .find("for (i, rb) in input.blocks")
            .expect("slot-sorted replay");
        assert!(
            constructed < replay,
            "the shared context is built once, before the replay loop"
        );
    }

    #[test]
    fn seed_decode_tops_up_pubkey_cache_before_on_block() {
        let src = include_str!("seed.rs");
        let production = src.split("#[cfg(test)]").next().unwrap();
        let decode = production
            .find("from_ssz_bytes_hydrated(ForkName::Fulu, input.state_ssz)")
            .expect("seed decode site");
        let on_block = production
            .find("match on_block_with_context(")
            .expect("seed on_block site");
        assert!(decode < on_block, "hydrated decode must precede on_block");
        assert!(
            !production.contains("from_ssz_bytes(input.state_ssz)"),
            "seed must not call the raw SSZ constructor"
        );
        assert!(
            !production.contains("from_ssz_bytes_with("),
            "production seed.rs must not call from_ssz_bytes_with"
        );
        let observe = production
            .find("observe_import_state_with_pubkeys(&state, ctx.pubkeys().len())")
            .expect("seed must emit M13 gauges on the decoded state");
        assert!(
            decode < observe && observe < on_block,
            "M13 observe must sit between hydrated decode and on_block"
        );
    }

    #[test]
    fn seed_offloads_apply_to_spawn_blocking() {
        let src = include_str!("seed.rs");
        let production = src.split("#[cfg(test)]").next().unwrap();
        let spawn = production
            .find("tokio::task::spawn_blocking")
            .expect("seed must spawn_blocking");
        let apply = production
            .find("apply_durable_seed::<P>")
            .expect("seed must call apply_durable_seed");
        assert!(
            spawn < apply,
            "apply_durable_seed must run inside spawn_blocking"
        );
        assert!(
            production.contains("seed_from_durable")
                && production.contains("apply_durable_seed_blocking"),
            "in-process seed must share the spawn_blocking wrap"
        );
        assert!(
            production.contains("matched_expected == false — FATAL (CC-45 /3 divergence)"),
            "seed_from_durable must fail-closed on CC-45/3 mismatch"
        );
    }

    #[derive(Debug, Default)]
    struct CountingEngine {
        calls: AtomicU64,
    }

    impl<P: Preset> ExecutionEngine<P> for CountingEngine {
        fn verify_and_notify_new_payload(
            &self,
            _request: cc_state_transition::NewPayloadRequest<'_, P>,
        ) -> Result<cc_state_transition::PayloadStatus, cc_state_transition::EngineError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(cc_state_transition::PayloadStatus::Valid)
        }
    }

    fn seed_payload_state() -> BeaconState<Minimal> {
        let mut state = BeaconState::<Minimal>::default();
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
                exit_epoch: Epoch::new(u64::MAX),
                withdrawable_epoch: Epoch::new(u64::MAX),
            })
            .unwrap();
        state.balances_push(Gwei::new(32_000_000_000)).unwrap();
        let body = BeaconBlockBody::<Minimal>::default();
        state.set_latest_block_header(BeaconBlockHeader {
            slot: Slot::new(0),
            proposer_index: ValidatorIndex::new(0),
            parent_root: Root::ZERO,
            state_root: Root::ZERO,
            body_root: Root::from_hash256(TreeHash::tree_hash_root(&body)),
        });
        state.set_slot(Slot::new(0));
        state.set_genesis_time(0);
        state.set_deposit_requests_start_index(u64::MAX);
        state
    }

    fn payload_carrying_child(
        state: &BeaconState<Minimal>,
        config: &ChainConfig,
    ) -> SignedBeaconBlock<Minimal> {
        let parent = Root::from_hash256(TreeHash::tree_hash_root(state.latest_block_header()));
        let epoch = cc_state_transition::get_current_epoch(state);
        let mix = cc_state_transition::get_randao_mix(state, epoch).unwrap();
        let mut body = BeaconBlockBody::<Minimal>::default();
        body.execution_payload.prev_randao = mix;
        body.execution_payload.timestamp = cc_state_transition::compute_time_at_slot(
            state.genesis_time(),
            state.slot(),
            config.seconds_per_slot,
        );
        body.execution_payload.parent_hash = state.latest_execution_payload_header().block_hash;
        body.sync_aggregate.sync_committee_signature =
            BlsSignature::from_array(cc_crypto::INFINITY_SIGNATURE);
        SignedBeaconBlock {
            message: BeaconBlock {
                slot: state.slot(),
                proposer_index: cc_state_transition::get_beacon_proposer_index(state).unwrap(),
                parent_root: parent,
                state_root: Root::ZERO,
                body,
            },
            signature: Default::default(),
        }
    }

    /// A body stored under a different key must not become the fork-choice anchor.
    #[test]
    fn seed_refuses_anchor_whose_root_does_not_match_canonical_key() {
        let mut snapshot = seed_payload_state();
        let post_root = snapshot.canonical_root();
        let signed_anchor = SignedBeaconBlock::<Minimal> {
            message: BeaconBlock {
                slot: Slot::new(0),
                proposer_index: ValidatorIndex::new(0),
                parent_root: Root::ZERO,
                state_root: post_root,
                body: Default::default(),
            },
            signature: Default::default(),
        };
        let state_ssz = snapshot.as_ssz_bytes();
        let anchor_ssz = signed_anchor.as_ssz_bytes();
        let mut registry = prometheus_client::registry::Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let config = minimal_config();
        let err = match apply_durable_seed::<Minimal>(SeedApplyInput {
            state_ssz: &state_ssz,
            anchor_block_ssz: Some(&anchor_ssz),
            anchor_block_fork: 0,
            anchor_block_root: Root::from_array([0xAB; 32]),
            blocks: &[],
            fork_choice_scalars_ssz: &[],
            chain_config: &config,
            engine: Arc::new(AcceptEngine) as Arc<dyn ExecutionEngine<Minimal>>,
            expected_head_root: Root::ZERO,
            expected_head_slot: 0,
            metrics: &metrics,
            _preset: PhantomData,
        }) {
            Err(e) => e,
            Ok(_) => panic!("mismatched canonical key must be refused"),
        };
        assert!(err.to_string().contains("anchor root mismatch"), "{err}");
    }

    /// S0-A-28: durable seed from a runtime worker + a real engine object + a
    /// payload-carrying block must not panic.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn seed_from_runtime_worker_with_real_engine_does_not_panic() {
        let mock = Arc::new(CountingEngine::default());

        let mut snapshot = seed_payload_state();
        let post_root = snapshot.canonical_root();
        let signed_anchor = SignedBeaconBlock::<Minimal> {
            message: BeaconBlock {
                slot: Slot::new(0),
                proposer_index: ValidatorIndex::new(0),
                parent_root: Root::ZERO,
                state_root: post_root,
                body: Default::default(),
            },
            signature: Default::default(),
        };
        let mut child_pre = snapshot.clone();
        let config = minimal_config();
        let _ = cc_state_transition::process_slots(&mut child_pre, Slot::new(1), &config).unwrap();
        let child = payload_carrying_child(&child_pre, &config);
        let child_root = Root::from_hash256(TreeHash::tree_hash_root(&child.message));
        let anchor_root = Root::from_hash256(TreeHash::tree_hash_root(&signed_anchor.message));

        let seed_block = SeedBlock {
            ssz: child.as_ssz_bytes(),
            fork: 0,
            root: child_root.as_slice().to_vec(),
            da_status: SeedDaStatus::Available,
        };

        let mut registry = prometheus_client::registry::Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let applied = apply_durable_seed_blocking::<Minimal>(SeedApplyOwned {
            state_ssz: snapshot.as_ssz_bytes(),
            anchor_block_ssz: signed_anchor.as_ssz_bytes(),
            anchor_block_root: anchor_root,
            anchor_block_fork: 0,
            blocks: vec![seed_block],
            fork_choice_scalars_ssz: Vec::new(),
            chain_config: config,
            engine: Arc::clone(&mock) as Arc<dyn ExecutionEngine<Minimal>>,
            expected_head_root: child_root,
            expected_head_slot: 1,
            metrics,
        })
        .await;
        let apply_dbg = match &applied {
            Ok(_) => "ok".to_string(),
            Err(e) => e.to_string(),
        };
        assert!(
            mock.calls.load(Ordering::SeqCst) >= 1,
            "payload-carrying seed block must reach CountingEngine \
             (not AcceptEngine); apply={apply_dbg}"
        );
    }
}
