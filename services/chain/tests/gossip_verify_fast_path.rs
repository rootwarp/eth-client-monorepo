//! CC-27c — gossip-verify fast path: verdict before state transition.
//!
//! Review fixes: H1 (always BLS before early ACCEPT), H2 (late_open pin — p2p),
//! H3 (future/too-old → IGNORE), non-vacuous late Reject/Internal (inject).

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use cc_chain::core::{CoreConfig, spawn_core_thread_with_epoch};
use cc_chain::epoch_context::{EpochContext, EpochContextStore};
use cc_chain::events::{EventsConfig, EventsHandle};
use cc_chain::head::HeadSnapshotStore;
use cc_chain::import::{
    ImportCounters, encode_signed_block, import_block_with_early, late_import_flags,
    on_block_error_gossip_class,
};
use cc_chain::metrics::ChainMetrics;
use cc_chain::residency::Residency;
use cc_chain::service::ChainServiceImpl;
use cc_crypto::{
    DOMAIN_BEACON_PROPOSER, INFINITY_SIGNATURE, SecretKey, compute_signing_root, get_domain,
};
use cc_fork_choice::{
    DataAvailability, HarnessAvailability, OnBlockError, get_forkchoice_store, get_head, on_tick,
};
use cc_proto::p2p::{
    Acceptance, GossipObject, ImportResult, ObjectKind, P2pToChain, Reason, StreamHello,
    chain_to_p2p, p2p_to_chain,
};
use cc_state_transition::helpers::constants::{FAR_FUTURE_EPOCH, MAX_EFFECTIVE_BALANCE};
use cc_state_transition::{
    BlockError, EngineError, GossipClass, SignatureKind, TransitionContext, compute_time_at_slot,
    get_beacon_proposer_index, get_current_epoch, get_expected_withdrawals, get_randao_mix,
    process_block, process_slots,
};
use cc_types::config::{BlobParameters, BlobSchedule, ChainConfig, PresetName};
use cc_types::containers::{BeaconBlockHeader, SyncAggregate, Validator};
use cc_types::execution::ExecutionPayload;
use cc_types::preset::{Minimal, Preset};
use cc_types::primitives::{
    BlsPublicKey, BlsSignature, Epoch, ExecutionAddress, ForkVersion, Root, Slot, ValidatorIndex,
};
use cc_types::{BeaconBlock, BeaconBlockBody, BeaconState, SignedBeaconBlock};
use futures::StreamExt;
use prometheus_client::registry::Registry;
use ssz_types::VariableList;
use tokio::sync::oneshot;
use tree_hash::TreeHash;

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

/// DA seam that stalls 2 s then returns available (stalls post-early-ACCEPT).
#[derive(Debug)]
struct SlowDa {
    entered: Arc<AtomicBool>,
}

impl DataAvailability for SlowDa {
    fn is_data_available(&self, _beacon_block_root: Root) -> bool {
        self.entered.store(true, Ordering::SeqCst);
        std::thread::sleep(Duration::from_secs(2));
        true
    }
}

fn proposer_sk() -> SecretKey {
    SecretKey::from_ikm(&[42u8; 32]).expect("sk")
}

fn active_validator_with_pk(pk: &BlsPublicKey) -> Validator {
    Validator {
        pubkey: *pk,
        withdrawal_credentials: Root::from_array({
            let mut c = [0u8; 32];
            c[0] = 0x01;
            c
        }),
        effective_balance: MAX_EFFECTIVE_BALANCE,
        slashed: false,
        activation_eligibility_epoch: Epoch::new(0),
        activation_epoch: Epoch::new(0),
        exit_epoch: FAR_FUTURE_EPOCH,
        withdrawable_epoch: FAR_FUTURE_EPOCH,
    }
}

fn seeded_store_signed(
    da: Arc<dyn DataAvailability>,
) -> (cc_fork_choice::Store<Minimal>, Root, ChainConfig, SecretKey) {
    let config = minimal_config();
    let sk = proposer_sk();
    let pk_bytes = sk.public_key().serialize();
    let pk = BlsPublicKey::from_array(pk_bytes);

    let mut state = BeaconState::<Minimal>::default();
    state.set_genesis_time(0);
    state.set_slot(Slot::new(0));
    state.set_genesis_validators_root(Root::from_array([0x11; 32]));
    // Fork versions matching minimal_config fulu at epoch 0.
    let mut fork = state.fork();
    fork.current_version = config.fulu_fork_version;
    fork.previous_version = config.fulu_fork_version;
    fork.epoch = Epoch::new(0);
    state.set_fork(fork);

    state
        .validators_push(active_validator_with_pk(&pk))
        .unwrap();
    state.balances_push(MAX_EFFECTIVE_BALANCE).unwrap();
    // Pad a few more validators so registry is non-trivial.
    for i in 1..8 {
        let ski = SecretKey::from_seed_index(&[9u8; 32], i).unwrap();
        let pki = BlsPublicKey::from_array(ski.public_key().serialize());
        state
            .validators_push(active_validator_with_pk(&pki))
            .unwrap();
        state.balances_push(MAX_EFFECTIVE_BALANCE).unwrap();
    }
    for i in 0..state.proposer_lookahead_len() {
        let _ = state.proposer_lookahead_set(i, ValidatorIndex::new(0));
    }

    let anchor_block = BeaconBlock {
        slot: Slot::new(0),
        proposer_index: ValidatorIndex::new(0),
        parent_root: Root::ZERO,
        state_root: Root::ZERO,
        body: Default::default(),
    };
    // Align latest_block_header with the anchor so ST parent checks can pass later.
    let body_root = Root::from_hash256(TreeHash::tree_hash_root(&anchor_block.body));
    state.set_latest_block_header(BeaconBlockHeader {
        slot: anchor_block.slot,
        proposer_index: anchor_block.proposer_index,
        parent_root: anchor_block.parent_root,
        state_root: Root::ZERO,
        body_root,
    });

    let mut store = get_forkchoice_store(
        state,
        &anchor_block,
        Arc::new(AcceptEngine),
        da,
        config.seconds_per_slot,
    )
    .unwrap();
    on_tick(&mut store, config.seconds_per_slot * 2).unwrap();
    let anchor_root = Root::from_hash256(TreeHash::tree_hash_root(&anchor_block));
    (store, anchor_root, config, sk)
}

fn sign_block(
    state: &BeaconState<Minimal>,
    sk: &SecretKey,
    message: BeaconBlock<Minimal>,
) -> SignedBeaconBlock<Minimal> {
    let epoch = Epoch::new(message.slot.as_u64() / Minimal::SLOTS_PER_EPOCH);
    let domain = get_domain(
        &state.fork(),
        DOMAIN_BEACON_PROPOSER,
        Some(epoch),
        state.genesis_validators_root(),
    );
    let root = compute_signing_root(&message, domain);
    let sig = sk.sign(root.as_array());
    SignedBeaconBlock {
        message,
        signature: BlsSignature::from_array(sig.serialize()),
    }
}

fn signed_child(
    store: &cc_fork_choice::Store<Minimal>,
    parent: Root,
    sk: &SecretKey,
    slot: u64,
) -> SignedBeaconBlock<Minimal> {
    let parent_state = store.block_state(&parent).expect("parent state");
    let message = BeaconBlock {
        slot: Slot::new(slot),
        proposer_index: ValidatorIndex::new(0),
        parent_root: parent,
        state_root: Root::ZERO,
        body: Default::default(),
    };
    sign_block(parent_state, sk, message)
}

// ── H1: no early ACCEPT without BLS ────────────────────────────────────────

#[test]
fn gossip_path_no_early_accept_without_valid_proposer_sig() {
    let (mut store, anchor, config, _sk) = seeded_store_signed(Arc::new(HarnessAvailability));
    // Unsigned / zero signature block.
    let block = SignedBeaconBlock::<Minimal> {
        message: BeaconBlock {
            slot: Slot::new(1),
            proposer_index: ValidatorIndex::new(0),
            parent_root: anchor,
            state_root: Root::ZERO,
            body: Default::default(),
        },
        signature: Default::default(),
    };
    let true_root = Root::from_hash256(TreeHash::tree_hash_root(&block.message));
    let request = cc_proto::chain::ImportBlockRequest {
        ssz: encode_signed_block(&block),
        fork: 0,
        root: true_root.as_slice().to_vec(),
        source: 0,
    };

    let mut registry = Registry::default();
    let metrics = ChainMetrics::register(&mut registry);
    let head = HeadSnapshotStore::new();
    let (event_tx, _) = tokio::sync::mpsc::channel(4);
    let counters = ImportCounters::default();
    let mut residency = Residency::<Minimal>::new(64, 32);
    let mut snap_seq = 0u64;
    let (early_tx, early_rx) = oneshot::channel();

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
        cc_state_transition::BlockSignatureStrategy::NoVerification, // ST strategy ignored for gossip BLS
        None,
        Some(early_tx),
        None,
        None,
        None,
        None,
        None,
    )
    .expect("outcome");

    assert!(
        !outcome.early_accept,
        "H1: must not early-ACCEPT when proposer sig fails (even if ST is NoVerification)"
    );
    assert!(
        early_rx.blocking_recv().is_err(),
        "early oneshot must not fire"
    );
    assert!(!outcome.transition_invoked);
}

// ── S0-A-25 / P0-03: unary ImportBlock verifies proposer signature ─────────

#[test]
fn unary_path_verifies_proposer_signature_under_default_config() {
    let (mut store, anchor, config, _sk) = seeded_store_signed(Arc::new(HarnessAvailability));
    let block = SignedBeaconBlock::<Minimal> {
        message: BeaconBlock {
            slot: Slot::new(1),
            proposer_index: ValidatorIndex::new(0),
            parent_root: anchor,
            state_root: Root::ZERO,
            body: Default::default(),
        },
        signature: Default::default(),
    };
    let true_root = Root::from_hash256(TreeHash::tree_hash_root(&block.message));
    let request = cc_proto::chain::ImportBlockRequest {
        ssz: encode_signed_block(&block),
        fork: 0,
        root: true_root.as_slice().to_vec(),
        source: 0,
    };

    let mut registry = Registry::default();
    let metrics = ChainMetrics::register(&mut registry);
    let head = HeadSnapshotStore::new();
    let (event_tx, _) = tokio::sync::mpsc::channel(4);
    let counters = ImportCounters::default();
    let mut residency = Residency::<Minimal>::new(64, 32);
    let mut snap_seq = 0u64;

    // Unary path: no early-accept oneshot. Strategy is the production default.
    assert_eq!(
        CoreConfig::default().verify,
        cc_state_transition::BlockSignatureStrategy::VerifyIndividual
    );
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
        CoreConfig::default().verify,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .expect("outcome");

    assert_eq!(
        outcome.response.verdict,
        cc_proto::chain::ImportBlockVerdict::Invalid as i32,
        "unary default must reject a missing proposer signature; reason={}",
        outcome.response.reason
    );
    assert!(
        !outcome.transition_invoked,
        "proposer-sig failure must be a cheap-path terminal (no ST)"
    );
    assert!(!outcome.early_accept);
    assert!(
        outcome.response.reason.contains("signature")
            || outcome.response.reason.contains("bls")
            || outcome.response.reason.contains("proposer"),
        "reason should name the proposer-sig failure, got {:?}",
        outcome.response.reason
    );
}

/// M5: default unary strategy is threaded into `on_block` / ST, not only the
/// cheap-path proposer check. Valid proposer + junk RANDAO must invoke ST and
/// fail closed on a body signature (`S0-A-33` / sync-aggregate is out of scope).
#[test]
fn unary_default_config_verifies_body_signatures_in_state_transition() {
    let (mut store, anchor, config, sk) = seeded_store_signed(Arc::new(HarnessAvailability));
    let block = signed_child(&store, anchor, &sk, 1);
    let true_root = Root::from_hash256(TreeHash::tree_hash_root(&block.message));
    let request = cc_proto::chain::ImportBlockRequest {
        ssz: encode_signed_block(&block),
        fork: 0,
        root: true_root.as_slice().to_vec(),
        source: 0,
    };

    let mut registry = Registry::default();
    let metrics = ChainMetrics::register(&mut registry);
    let head = HeadSnapshotStore::new();
    let (event_tx, _) = tokio::sync::mpsc::channel(4);
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
        CoreConfig::default().verify,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )
    .expect("outcome");

    assert!(
        outcome.transition_invoked,
        "valid proposer sig must reach on_block / ST; reason={}",
        outcome.response.reason
    );
    assert_eq!(
        outcome.response.verdict,
        cc_proto::chain::ImportBlockVerdict::Invalid as i32,
        "junk RANDAO under VerifyIndividual must fail ST; reason={}",
        outcome.response.reason
    );
    let reason = outcome.response.reason.to_ascii_lowercase();
    assert!(
        reason.contains("randao") || reason.contains("invalid signature") || reason.contains("bls"),
        "ST failure must be a body-signature reject, got {:?}",
        outcome.response.reason
    );
}

// ── Early ACCEPT before stalled transition (signed) ────────────────────────

#[test]
fn early_accept_fires_before_two_second_da_stall() {
    let entered = Arc::new(AtomicBool::new(false));
    let da = Arc::new(SlowDa {
        entered: Arc::clone(&entered),
    });
    let (mut store, anchor, config, sk) = seeded_store_signed(da);
    let block = signed_child(&store, anchor, &sk, 1);
    let true_root = Root::from_hash256(TreeHash::tree_hash_root(&block.message));
    let request = cc_proto::chain::ImportBlockRequest {
        ssz: encode_signed_block(&block),
        fork: 0,
        root: true_root.as_slice().to_vec(),
        source: 0,
    };

    let mut registry = Registry::default();
    let metrics = ChainMetrics::register(&mut registry);
    let head = HeadSnapshotStore::new();
    let (event_tx, _event_rx) = tokio::sync::mpsc::channel(8);
    let counters = ImportCounters::default();
    let mut residency = Residency::<Minimal>::new(64, 32);
    let mut snap_seq = 0u64;

    let (early_tx, early_rx) = oneshot::channel();
    let start = Instant::now();

    let handle = std::thread::spawn(move || {
        import_block_with_early(
            &mut store,
            &mut residency,
            &config,
            &head,
            &event_tx,
            &metrics,
            &counters,
            &mut snap_seq,
            request,
            cc_state_transition::BlockSignatureStrategy::NoVerification,
            None,
            Some(early_tx),
            None,
            None,
            None,
            None,
            None,
        )
    });

    early_rx
        .blocking_recv()
        .expect("early ACCEPT oneshot closed without send");
    let early_latency = start.elapsed();
    assert!(
        early_latency < Duration::from_millis(100),
        "early ACCEPT took {early_latency:?}; must be < 100 ms (before 2 s DA stall)"
    );

    let outcome = handle.join().expect("import thread").expect("import ok");
    assert!(outcome.early_accept);
    assert!(
        entered.load(Ordering::SeqCst),
        "DA gate must run after early ACCEPT"
    );
    assert!(start.elapsed() >= Duration::from_secs(2));
}

// ── Non-vacuous late Reject / Internal (inject after early) ────────────────

#[test]
fn late_import_reject_forced_after_early_accept() {
    let (mut store, anchor, config, sk) = seeded_store_signed(Arc::new(HarnessAvailability));
    let block = signed_child(&store, anchor, &sk, 1);
    let true_root = Root::from_hash256(TreeHash::tree_hash_root(&block.message));
    let request = cc_proto::chain::ImportBlockRequest {
        ssz: encode_signed_block(&block),
        fork: 0,
        root: true_root.as_slice().to_vec(),
        source: 0,
    };

    let mut registry = Registry::default();
    let metrics = ChainMetrics::register(&mut registry);
    let head = HeadSnapshotStore::new();
    let (event_tx, _) = tokio::sync::mpsc::channel(4);
    let counters = ImportCounters::default();
    let mut residency = Residency::<Minimal>::new(64, 32);
    let mut snap_seq = 0u64;
    let (early_tx, early_rx) = oneshot::channel();

    let inject = OnBlockError::Transition(BlockError::InvalidPayload);
    assert_eq!(
        on_block_error_gossip_class(&inject),
        GossipClass::Reject,
        "inject must be Reject-class"
    );

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
        cc_state_transition::BlockSignatureStrategy::NoVerification,
        None,
        Some(early_tx),
        Some(inject),
        None,
        None,
        None,
        None,
    )
    .expect("outcome");

    early_rx.blocking_recv().expect("early ACCEPT");
    assert!(outcome.early_accept);
    assert!(
        outcome.late_import_reject,
        "must assert late Reject (not vacuous)"
    );
    assert!(!outcome.late_import_internal);
    assert!(!outcome.transition_invoked, "inject skips on_block");
}

#[test]
fn late_import_internal_forced_after_early_accept() {
    let (mut store, anchor, config, sk) = seeded_store_signed(Arc::new(HarnessAvailability));
    let block = signed_child(&store, anchor, &sk, 1);
    let true_root = Root::from_hash256(TreeHash::tree_hash_root(&block.message));
    let request = cc_proto::chain::ImportBlockRequest {
        ssz: encode_signed_block(&block),
        fork: 0,
        root: true_root.as_slice().to_vec(),
        source: 0,
    };

    let mut registry = Registry::default();
    let metrics = ChainMetrics::register(&mut registry);
    let head = HeadSnapshotStore::new();
    let (event_tx, _) = tokio::sync::mpsc::channel(4);
    let counters = ImportCounters::default();
    let mut residency = Residency::<Minimal>::new(64, 32);
    let mut snap_seq = 0u64;
    let (early_tx, early_rx) = oneshot::channel();

    let inject =
        OnBlockError::Transition(BlockError::Engine(EngineError::Transport("induced".into())));
    assert_eq!(
        on_block_error_gossip_class(&inject),
        GossipClass::Internal,
        "inject must be Internal-class"
    );

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
        cc_state_transition::BlockSignatureStrategy::NoVerification,
        None,
        Some(early_tx),
        Some(inject),
        None,
        None,
        None,
        None,
    )
    .expect("outcome");

    early_rx.blocking_recv().expect("early ACCEPT");
    assert!(outcome.early_accept);
    assert!(
        outcome.late_import_internal,
        "must assert late Internal (not vacuous)"
    );
    assert!(!outcome.late_import_reject);
}

#[test]
fn late_import_flags_helper_exhaustive() {
    assert_eq!(late_import_flags(true, GossipClass::Reject), (true, false));
    assert_eq!(
        late_import_flags(true, GossipClass::Internal),
        (false, true)
    );
    assert_eq!(late_import_flags(true, GossipClass::Ignore), (false, false));
    assert_eq!(
        late_import_flags(false, GossipClass::Reject),
        (false, false)
    );
}

#[test]
fn gossip_class_mapping_reject_vs_internal() {
    assert_eq!(
        on_block_error_gossip_class(&OnBlockError::NotDescendedFromFinalized),
        GossipClass::Reject
    );
    assert_eq!(
        on_block_error_gossip_class(&OnBlockError::Transition(BlockError::InvalidSignature {
            which: SignatureKind::BlockProposer,
        })),
        GossipClass::Reject
    );
    assert_eq!(
        on_block_error_gossip_class(&OnBlockError::Transition(BlockError::Engine(
            EngineError::Transport("x".into())
        ))),
        GossipClass::Internal
    );
    assert_eq!(
        on_block_error_gossip_class(&OnBlockError::Transition(BlockError::InvalidPayload)),
        GossipClass::Reject
    );
    assert_eq!(
        on_block_error_gossip_class(&OnBlockError::PulledUpTip("x".into())),
        GossipClass::Internal
    );
}

// ── H3: future_slot → IGNORE ───────────────────────────────────────────────

#[test]
fn future_slot_is_terminal_ignore_reason() {
    let (mut store, anchor, config, sk) = seeded_store_signed(Arc::new(HarnessAvailability));
    // Store is at slot ~2; request a far-future slot.
    let parent_state = store.block_state(&anchor).unwrap().clone();
    let message = BeaconBlock {
        slot: Slot::new(10_000),
        proposer_index: ValidatorIndex::new(0),
        parent_root: anchor,
        state_root: Root::ZERO,
        body: Default::default(),
    };
    let block = sign_block(&parent_state, &sk, message);
    let true_root = Root::from_hash256(TreeHash::tree_hash_root(&block.message));
    let request = cc_proto::chain::ImportBlockRequest {
        ssz: encode_signed_block(&block),
        fork: 0,
        root: true_root.as_slice().to_vec(),
        source: 0,
    };

    let mut registry = Registry::default();
    let metrics = ChainMetrics::register(&mut registry);
    let head = HeadSnapshotStore::new();
    let (event_tx, _) = tokio::sync::mpsc::channel(4);
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
        cc_state_transition::BlockSignatureStrategy::NoVerification,
        None,
        None, // unary-style — still hits cheap future_slot
        None,
        None,
        None,
        None,
        None,
    )
    .expect("outcome");

    assert!(!outcome.early_accept);
    assert_eq!(outcome.response.reason, "future_slot");
}

// ── Stream: Verdict arrives before 2 s stall ───────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_verdict_before_two_second_stall() {
    let entered = Arc::new(AtomicBool::new(false));
    let da = Arc::new(SlowDa {
        entered: Arc::clone(&entered),
    });
    let (store, anchor, config, sk) = seeded_store_signed(da);
    let block = signed_child(&store, anchor, &sk, 1);
    let true_root = Root::from_hash256(TreeHash::tree_hash_root(&block.message));

    let mut registry = Registry::default();
    let metrics = ChainMetrics::register(&mut registry);
    let events = EventsHandle::spawn(EventsConfig::default());
    let head = HeadSnapshotStore::new();
    let epoch = EpochContextStore::new();
    let core = spawn_core_thread_with_epoch(
        store,
        config,
        head.clone(),
        epoch.clone(),
        events.event_sender(),
        metrics.clone(),
        CoreConfig::default(),
    );
    let svc = ChainServiceImpl::with_epoch(
        Some(core.handle.clone()),
        head,
        epoch,
        events.clone(),
        metrics,
    );

    let (in_tx, in_rx) = tokio::sync::mpsc::channel(8);
    let mut outbound = svc
        .open_p2p_stream(tokio_stream::wrappers::ReceiverStream::new(in_rx))
        .await
        .unwrap()
        .into_inner();

    in_tx
        .send(Ok(P2pToChain {
            seq: 1,
            msg: Some(p2p_to_chain::Msg::Hello(StreamHello {
                session_id: 27,
                resume_seq: 0,
            })),
        }))
        .await
        .unwrap();
    let _ = outbound.next().await.unwrap().unwrap();

    let start = Instant::now();
    in_tx
        .send(Ok(P2pToChain {
            seq: 2,
            msg: Some(p2p_to_chain::Msg::Object(GossipObject {
                ssz: encode_signed_block(&block),
                fork: 0,
                root: true_root.as_slice().to_vec(),
                source: 0,
                kind: ObjectKind::Block as i32,
                subnet_id: 0,
            })),
        }))
        .await
        .unwrap();

    let msg = tokio::time::timeout(Duration::from_millis(100), outbound.next())
        .await
        .expect("Verdict must arrive in < 100 ms while DA stalls 2 s")
        .expect("stream ended")
        .expect("status");
    let latency = start.elapsed();
    assert!(
        latency < Duration::from_millis(100),
        "stream Verdict latency {latency:?}"
    );

    match msg.msg {
        Some(chain_to_p2p::Msg::Verdict(v)) => {
            assert_eq!(v.acceptance, Acceptance::Accept as i32);
            assert_eq!(v.reason, Reason::Valid as i32);
            assert_eq!(v.import, ImportResult::None as i32);
            assert_eq!(v.correlation_id, true_root.as_slice());
        }
        other => panic!("expected Verdict, got {other:?}"),
    }

    tokio::time::sleep(Duration::from_millis(2100)).await;

    drop(in_tx);
    core.handle.shutdown().await;
    core.join();
    events.shutdown().await;
}

#[test]
fn no_spawn_in_import_rs() {
    let src = include_str!("../../../crates/chain-core/src/import.rs");
    assert!(
        !src.contains("spawn_blocking") && !src.contains("tokio::spawn"),
        "import.rs must not move work off the core thread"
    );
}

// ── S0-A-26 / P0-11: gap past Fulu lookahead is unknown, not a cheap Reject ─

/// Distinctive current-epoch row used so `slot % SPE` would never match a real proposer.
const LOOKAHEAD_WRAP_MARKER: u64 = 99;

fn seeded_store_for_gap() -> (
    cc_fork_choice::Store<Minimal>,
    Root,
    ChainConfig,
    Vec<SecretKey>,
) {
    let config = minimal_config();
    let mut keys = Vec::with_capacity(8);
    let mut validator_pubkeys = Vec::with_capacity(8);
    let mut state = BeaconState::<Minimal>::default();
    state.set_genesis_time(0);
    state.set_slot(Slot::new(0));
    state.set_genesis_validators_root(Root::from_array([0x11; 32]));
    state.set_deposit_requests_start_index(u64::MAX);
    let mut fork = state.fork();
    fork.current_version = config.fulu_fork_version;
    fork.previous_version = config.fulu_fork_version;
    fork.epoch = Epoch::new(0);
    state.set_fork(fork);

    for i in 0..8u64 {
        let ski = if i == 0 {
            proposer_sk()
        } else {
            SecretKey::from_seed_index(&[9u8; 32], i).unwrap()
        };
        let pki = BlsPublicKey::from_array(ski.public_key().serialize());
        state
            .validators_push(active_validator_with_pk(&pki))
            .unwrap();
        state.balances_push(MAX_EFFECTIVE_BALANCE).unwrap();
        state.previous_epoch_participation_push(0).unwrap();
        state.current_epoch_participation_push(0).unwrap();
        state.inactivity_scores_push(0).unwrap();
        keys.push(ski);
        validator_pubkeys.push(pki);
    }
    // process_sync_aggregate looks up every committee key and has no scan
    // fallback. Default (zero) committee keys are not in the registry, so an
    // empty TransitionContext map is CachePoisoned.
    let committee_keys: Vec<BlsPublicKey> = (0..Minimal::SYNC_COMMITTEE_SIZE as usize)
        .map(|i| validator_pubkeys[i % validator_pubkeys.len()])
        .collect();
    let committee = cc_types::containers::SyncCommittee {
        pubkeys: ssz_types::FixedVector::new(committee_keys.clone()).expect("sync committee size"),
        aggregate_pubkey: committee_keys[0],
    };
    state.set_current_sync_committee(committee.clone());
    state.set_next_sync_committee(committee);

    for i in 0..state.proposer_lookahead_len() {
        state
            .proposer_lookahead_set(i, ValidatorIndex::new(LOOKAHEAD_WRAP_MARKER))
            .unwrap();
    }

    // Genesis header carries a zero state_root so `process_slot` can fill it
    // with `hash(state)` — the same value we stamp on the anchor block, so
    // `hash(filled header) == hash(anchor)` and the child's parent_root hits
    // the store key.
    let mut anchor_block = BeaconBlock {
        slot: Slot::new(0),
        proposer_index: ValidatorIndex::new(0),
        parent_root: Root::ZERO,
        state_root: Root::ZERO,
        body: Default::default(),
    };
    let body_root = Root::from_hash256(TreeHash::tree_hash_root(&anchor_block.body));
    state.set_latest_block_header(BeaconBlockHeader {
        slot: anchor_block.slot,
        proposer_index: anchor_block.proposer_index,
        parent_root: anchor_block.parent_root,
        state_root: Root::ZERO,
        body_root,
    });
    anchor_block.state_root = state.canonical_root();

    let mut store = get_forkchoice_store(
        state,
        &anchor_block,
        Arc::new(AcceptEngine),
        Arc::new(HarnessAvailability),
        config.seconds_per_slot,
    )
    .unwrap();
    let gap_slot = 3 * Minimal::SLOTS_PER_EPOCH;
    on_tick(&mut store, config.seconds_per_slot * gap_slot).unwrap();
    let _ = get_head(&mut store).unwrap();
    let anchor_root = Root::from_hash256(TreeHash::tree_hash_root(&anchor_block));
    (store, anchor_root, config, keys)
}

fn valid_signed_gap_block(
    store: &cc_fork_choice::Store<Minimal>,
    parent: Root,
    keys: &[SecretKey],
    config: &ChainConfig,
    slot: u64,
) -> SignedBeaconBlock<Minimal> {
    let parent_state = store.block_state(&parent).expect("parent state").clone();
    let mut st = parent_state.clone();
    let target = Slot::new(slot);
    let pre_root = process_slots(&mut st, target, config).expect("process_slots across gap");
    let proposer = get_beacon_proposer_index(&st).expect("proposer after process_slots");
    assert_ne!(
        proposer.as_u64(),
        LOOKAHEAD_WRAP_MARKER,
        "real proposer must differ from the wrap-around marker so the old fallback would Reject"
    );
    let (withdrawals, _) = get_expected_withdrawals(&st).expect("withdrawals");
    let epoch = get_current_epoch(&st);
    let prev_randao = get_randao_mix(&st, epoch).expect("randao");
    let timestamp = compute_time_at_slot(st.genesis_time(), target, config.seconds_per_slot);
    let parent_hash = st.latest_execution_payload_header().block_hash;
    let payload = ExecutionPayload::<Minimal> {
        parent_hash,
        prev_randao,
        timestamp,
        block_number: st.latest_execution_payload_header().block_number + 1,
        gas_limit: st.latest_execution_payload_header().gas_limit,
        withdrawals: VariableList::new(withdrawals).expect("withdrawals list"),
        ..Default::default()
    };
    let body = BeaconBlockBody::<Minimal> {
        execution_payload: payload,
        eth1_data: st.eth1_data(),
        sync_aggregate: SyncAggregate {
            sync_committee_bits: Default::default(),
            sync_committee_signature: BlsSignature::from_array(INFINITY_SIGNATURE),
        },
        ..Default::default()
    };
    let mut message = BeaconBlock {
        slot: target,
        proposer_index: proposer,
        parent_root: parent,
        state_root: Root::ZERO,
        body,
    };
    let engine = AcceptEngine;
    let ctx = TransitionContext::new(config, &engine);
    ctx.top_up_pubkey_cache(&st);
    process_block(&mut st, &message, &ctx, pre_root).expect("process_block for post-state root");
    message.state_root = st.canonical_root();
    let sk = keys
        .get(proposer.as_u64() as usize)
        .expect("proposer key in fixture");
    sign_block(&parent_state, sk, message)
}

/// A valid block 3 epochs past the head state must not cheap-Reject (and must
/// not descore) — outside the Fulu window the proposer is unknown and `on_block`
/// is the authority.
#[test]
fn block_three_epochs_past_head_is_not_proposer_reject() {
    let (mut store, anchor, config, keys) = seeded_store_for_gap();
    let gap_slot = 3 * Minimal::SLOTS_PER_EPOCH;
    let block = valid_signed_gap_block(&store, anchor, &keys, &config, gap_slot);
    assert_ne!(block.message.proposer_index.as_u64(), LOOKAHEAD_WRAP_MARKER);
    let true_root = Root::from_hash256(TreeHash::tree_hash_root(&block.message));
    let request = cc_proto::chain::ImportBlockRequest {
        ssz: encode_signed_block(&block),
        fork: 0,
        root: true_root.as_slice().to_vec(),
        source: 0,
    };

    let mut registry = Registry::default();
    let metrics = ChainMetrics::register(&mut registry);
    let head = HeadSnapshotStore::new();
    let (event_tx, _) = tokio::sync::mpsc::channel(8);
    let counters = ImportCounters::default();
    let mut residency = Residency::<Minimal>::new(64, 32);
    let mut snap_seq = 0u64;
    let (early_tx, early_rx) = oneshot::channel();

    // Stale epoch-0 context with the wrap marker — same shape as a head that
    // has not advanced. Slot 24 is outside this 16-entry window.
    let epoch_ctx = EpochContext {
        epoch: Epoch::new(0),
        proposer_lookahead: vec![LOOKAHEAD_WRAP_MARKER; Minimal::PROPOSER_LOOKAHEAD_LEN as usize],
        slots_per_epoch: Minimal::SLOTS_PER_EPOCH,
        seconds_per_slot: config.seconds_per_slot,
        ..EpochContext::default()
    };

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
        cc_state_transition::BlockSignatureStrategy::NoVerification,
        Some(&epoch_ctx),
        Some(early_tx),
        None,
        None,
        None,
        None,
        None,
    )
    .expect("outcome");

    assert!(
        !outcome.response.reason.contains("proposer mismatch"),
        "cheap path must not wrap the current-epoch row; reason={}",
        outcome.response.reason
    );
    assert!(
        outcome.early_accept,
        "outside the window the cheap path must ACCEPT-and-continue; reason={}",
        outcome.response.reason
    );
    early_rx.blocking_recv().expect("early ACCEPT");
    assert!(
        outcome.transition_invoked,
        "on_block must validate the gap block"
    );
    assert!(
        !outcome.late_import_reject,
        "valid gap block must not descore; verdict={} reason={}",
        outcome.response.verdict, outcome.response.reason
    );
    assert_eq!(
        outcome.response.verdict,
        cc_proto::chain::ImportBlockVerdict::Imported as i32,
        "on_block must accept the valid gap block; reason={}",
        outcome.response.reason
    );
}
