//! Minimal Fulu anchor and the anchor's first signed child.
//!
//! The anchor is epoch-aligned (slot 0) so `verify_checkpoint` accepts it.
//! `block.state_root` is `hash_tree_root(state)` with `latest_block_header.state_root`
//! left at zero — the same shape genesis uses. The child is built against that
//! state: RANDAO reveal first (it is mixed into state), then a no-verification
//! trial to fill `state_root`, then the proposer signature.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use cc_beacon_core::boot::{BeaconCoreConfig, CheckpointProviderSlot, GenesisAnchorBytes};
use cc_chain::checkpoint_sync::{
    GenesisInfo, InMemoryCheckpointProvider, REQUIRED_CONSENSUS_VERSION,
};
use cc_crypto::{
    DOMAIN_BEACON_PROPOSER, DOMAIN_RANDAO, INFINITY_SIGNATURE, SecretKey, compute_signing_root,
    get_domain,
};
use cc_state_transition::{
    BlockSignatureStrategy, ExecutionEngine, NewPayloadRequest, PayloadStatus,
    compute_time_at_slot, get_beacon_proposer_index, get_current_epoch, get_expected_withdrawals,
    get_randao_mix, process_slots, state_transition,
};
use cc_types::config::{BlobParameters, BlobSchedule, ChainConfig, PresetName};
use cc_types::containers::{BeaconBlockHeader, SyncAggregate, SyncCommittee, Validator};
use cc_types::execution::ExecutionPayload;
use cc_types::preset::{Minimal, Preset};
use cc_types::primitives::{
    BlsPublicKey, BlsSignature, Epoch, ExecutionAddress, ForkVersion, Gwei, Root, Slot,
    ValidatorIndex,
};
use cc_types::{BeaconBlock, BeaconBlockBody, BeaconState, ForkName, SignedBeaconBlock};
use ssz::Encode;
use ssz_types::{FixedVector, VariableList};
use tree_hash::TreeHash;

#[derive(Debug, Default, Clone, Copy)]
struct AcceptEngine;

impl<P: Preset> ExecutionEngine<P> for AcceptEngine {
    fn verify_and_notify_new_payload(
        &self,
        _request: NewPayloadRequest<'_, P>,
    ) -> Result<PayloadStatus, cc_state_transition::EngineError> {
        Ok(PayloadStatus::Valid)
    }
}

pub(crate) struct AnchorFixture {
    pub chain: ChainConfig,
    pub sk: SecretKey,
    pub state_ssz: Vec<u8>,
    pub block_ssz: Vec<u8>,
    pub anchor_root: Root,
    pub genesis: GenesisInfo,
    pub spec: BTreeMap<String, String>,
}

pub(crate) struct SignedChild {
    pub signed: SignedBeaconBlock<Minimal>,
    pub root: Root,
}

pub(crate) fn minimal_config() -> ChainConfig {
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
        .expect("blob schedule"),
        deposit_chain_id: 0,
        deposit_contract_address: ExecutionAddress::ZERO,
        churn_limit_quotient: 32,
        min_per_epoch_churn_limit_electra: 64_000_000_000,
        max_per_epoch_activation_exit_churn_limit: 128_000_000_000,
        shard_committee_period: Epoch::new(64),
        max_blobs_per_block_electra: 9,
    }
}

fn local_spec_map(cfg: &ChainConfig) -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    m.insert(
        "GENESIS_FORK_VERSION".into(),
        format!("{}", cfg.genesis_fork_version),
    );
    m.insert(
        "ALTAIR_FORK_VERSION".into(),
        format!("{}", cfg.altair_fork_version),
    );
    m.insert(
        "BELLATRIX_FORK_VERSION".into(),
        format!("{}", cfg.bellatrix_fork_version),
    );
    m.insert(
        "CAPELLA_FORK_VERSION".into(),
        format!("{}", cfg.capella_fork_version),
    );
    m.insert(
        "DENEB_FORK_VERSION".into(),
        format!("{}", cfg.deneb_fork_version),
    );
    m.insert(
        "ELECTRA_FORK_VERSION".into(),
        format!("{}", cfg.electra_fork_version),
    );
    m.insert(
        "FULU_FORK_VERSION".into(),
        format!("{}", cfg.fulu_fork_version),
    );
    m.insert(
        "FULU_FORK_EPOCH".into(),
        cfg.fulu_fork_epoch.as_u64().to_string(),
    );
    m.insert("SECONDS_PER_SLOT".into(), cfg.seconds_per_slot.to_string());
    m.insert(
        "BLOB_SCHEDULE".into(),
        serde_json::to_string(
            &cfg.blob_schedule
                .entries()
                .iter()
                .map(|e| {
                    serde_json::json!({
                        "EPOCH": e.epoch.as_u64(),
                        "MAX_BLOBS_PER_BLOCK": e.max_blobs_per_block,
                    })
                })
                .collect::<Vec<_>>(),
        )
        .expect("blob schedule json"),
    );
    m
}

fn active_validator(pk: BlsPublicKey) -> Validator {
    let mut credentials = [0u8; 32];
    credentials[0] = 0x01;
    Validator {
        pubkey: pk,
        withdrawal_credentials: Root::from_array(credentials),
        effective_balance: Gwei::new(32_000_000_000),
        slashed: false,
        activation_eligibility_epoch: Epoch::new(0),
        activation_epoch: Epoch::new(0),
        exit_epoch: cc_types::FAR_FUTURE_EPOCH,
        withdrawable_epoch: cc_types::FAR_FUTURE_EPOCH,
    }
}

pub(crate) fn anchor_fixture() -> AnchorFixture {
    let chain = minimal_config();
    let sk = SecretKey::from_ikm(&[42u8; 32]).expect("proposer key");
    let pk = BlsPublicKey::from_array(sk.public_key().serialize());
    let gvr = Root::from_array([0x11; 32]);

    let mut state = BeaconState::<Minimal>::default();
    state.set_genesis_time(0);
    state.set_slot(Slot::new(0));
    state.set_genesis_validators_root(gvr);
    let mut fork = state.fork();
    fork.current_version = chain.fulu_fork_version;
    fork.previous_version = chain.fulu_fork_version;
    fork.epoch = Epoch::new(0);
    state.set_fork(fork);
    state
        .validators_push(active_validator(pk))
        .expect("validator");
    state
        .balances_push(Gwei::new(32_000_000_000))
        .expect("balance");
    for i in 0..state.proposer_lookahead_len() {
        state
            .proposer_lookahead_set(i, ValidatorIndex::new(0))
            .expect("lookahead");
    }
    let committee_keys = vec![pk; Minimal::SYNC_COMMITTEE_SIZE as usize];
    let committee = SyncCommittee {
        pubkeys: FixedVector::new(committee_keys).expect("sync committee"),
        aggregate_pubkey: pk,
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

    let state_ssz = state.as_ssz_bytes();
    let mut state = BeaconState::<Minimal>::from_ssz_bytes_hydrated(ForkName::Fulu, &state_ssz)
        .expect("anchor state roundtrip");
    let state_root = Root::from_hash256(TreeHash::tree_hash_root(&state));
    assert_eq!(
        state.canonical_root(),
        state_root,
        "anchor tree hash and canonical root diverged"
    );

    let block = BeaconBlock {
        slot: Slot::new(0),
        proposer_index: ValidatorIndex::new(0),
        parent_root: Root::ZERO,
        state_root,
        body,
    };
    let anchor_root = Root::from_hash256(TreeHash::tree_hash_root(&block));
    let signed = SignedBeaconBlock {
        message: block,
        signature: BlsSignature::default(),
    };
    AnchorFixture {
        spec: local_spec_map(&chain),
        chain,
        sk,
        state_ssz,
        block_ssz: signed.as_ssz_bytes(),
        anchor_root,
        genesis: GenesisInfo {
            genesis_time: 0,
            genesis_validators_root: gvr,
        },
    }
}

pub(crate) fn provider_slot(fixture: &AnchorFixture) -> CheckpointProviderSlot {
    let provider = InMemoryCheckpointProvider::new(
        fixture.genesis,
        fixture.spec.clone(),
        REQUIRED_CONSENSUS_VERSION,
        cc_seam::Bytes::from(fixture.block_ssz.clone()),
        cc_seam::Bytes::from(fixture.state_ssz.clone()),
    );
    CheckpointProviderSlot::new(Arc::new(provider))
}

pub(crate) fn genesis_bytes(fixture: &AnchorFixture) -> GenesisAnchorBytes {
    GenesisAnchorBytes {
        state_ssz: fixture.state_ssz.clone(),
        block_ssz: fixture.block_ssz.clone(),
        expected_block_root: Some(fixture.anchor_root),
    }
}

fn sign_randao(state: &BeaconState<Minimal>, sk: &SecretKey, epoch: Epoch) -> BlsSignature {
    let domain = get_domain(
        &state.fork(),
        DOMAIN_RANDAO,
        Some(epoch),
        state.genesis_validators_root(),
    );
    let root = compute_signing_root(&epoch, domain);
    BlsSignature::from_array(sk.sign(root.as_array()).serialize())
}

fn sign_proposer(
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
    SignedBeaconBlock {
        message,
        signature: BlsSignature::from_array(sk.sign(root.as_array()).serialize()),
    }
}

/// Anchor state `boot` commits. The post-state of a [`ChainLink`] is the
/// parent for the next call.
pub(crate) fn anchor_state(fixture: &AnchorFixture) -> BeaconState<Minimal> {
    BeaconState::<Minimal>::from_ssz_bytes_hydrated(ForkName::Fulu, &fixture.state_ssz)
        .expect("anchor state")
}

/// One signed block and the post-state it produces.
pub(crate) struct ChainLink {
    pub signed: SignedBeaconBlock<Minimal>,
    pub root: Root,
    pub state: BeaconState<Minimal>,
}

/// Next block on `parent_state`. `payload_tag` distinguishes siblings at one slot.
pub(crate) fn extend_block(
    fixture: &AnchorFixture,
    parent_state: &BeaconState<Minimal>,
    parent_root: Root,
    payload_tag: u8,
) -> ChainLink {
    let engine = AcceptEngine;
    let ctx = cc_state_transition::TransitionContext::new(&fixture.chain, &engine);
    ctx.top_up_pubkey_cache(parent_state);

    let mut st = parent_state.clone();
    let next_slot = Slot::new(st.slot().as_u64() + 1);
    process_slots(&mut st, next_slot, &fixture.chain).expect("process_slots");
    let proposer = get_beacon_proposer_index(&st).expect("proposer");
    let (withdrawals, _) = get_expected_withdrawals(&st).expect("withdrawals");
    let epoch = get_current_epoch(&st);
    let prev_randao = get_randao_mix(&st, epoch).expect("randao mix");
    let timestamp =
        compute_time_at_slot(st.genesis_time(), next_slot, fixture.chain.seconds_per_slot);
    let parent_hash = st.latest_execution_payload_header().block_hash;
    let randao_reveal = sign_randao(&st, &fixture.sk, epoch);

    let payload = ExecutionPayload::<Minimal> {
        parent_hash,
        prev_randao,
        timestamp,
        block_number: st.latest_execution_payload_header().block_number + 1,
        gas_limit: st.latest_execution_payload_header().gas_limit,
        block_hash: Root::from_array([payload_tag; 32]),
        withdrawals: VariableList::new(withdrawals).expect("withdrawals list"),
        ..Default::default()
    };
    let body = BeaconBlockBody::<Minimal> {
        randao_reveal,
        eth1_data: st.eth1_data(),
        execution_payload: payload,
        sync_aggregate: SyncAggregate {
            sync_committee_bits: Default::default(),
            sync_committee_signature: BlsSignature::from_array(INFINITY_SIGNATURE),
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
        signature: BlsSignature::default(),
    };
    let mut trial = parent_state.clone();
    match state_transition(
        &mut trial,
        &trial_signed,
        &ctx,
        BlockSignatureStrategy::NoVerification,
    ) {
        Err(cc_state_transition::BlockError::StateRootMismatch { actual, .. }) => {
            message.state_root = actual;
        }
        Ok(()) => message.state_root = trial.canonical_root(),
        Err(e) => panic!("state_transition for child: {e}"),
    }
    let signed = sign_proposer(&st, &fixture.sk, message);
    let root = Root::from_hash256(TreeHash::tree_hash_root(&signed.message));
    let mut post = parent_state.clone();
    state_transition(
        &mut post,
        &signed,
        &ctx,
        BlockSignatureStrategy::NoVerification,
    )
    .expect("post-state");
    ChainLink {
        signed,
        root,
        state: post,
    }
}

/// First normal child of the anchor (slot 1). Not epoch-aligned.
pub(crate) fn first_child(fixture: &AnchorFixture) -> SignedChild {
    let parent_state =
        BeaconState::<Minimal>::from_ssz_bytes_hydrated(ForkName::Fulu, &fixture.state_ssz)
            .expect("parent state");
    let engine = AcceptEngine;
    let ctx = cc_state_transition::TransitionContext::new(&fixture.chain, &engine);
    ctx.top_up_pubkey_cache(&parent_state);

    let mut st = parent_state.clone();
    let next_slot = Slot::new(st.slot().as_u64() + 1);
    process_slots(&mut st, next_slot, &fixture.chain).expect("process_slots");
    let proposer = get_beacon_proposer_index(&st).expect("proposer");
    let (withdrawals, _) = get_expected_withdrawals(&st).expect("withdrawals");
    let epoch = get_current_epoch(&st);
    let prev_randao = get_randao_mix(&st, epoch).expect("randao mix");
    let timestamp =
        compute_time_at_slot(st.genesis_time(), next_slot, fixture.chain.seconds_per_slot);
    let parent_hash = st.latest_execution_payload_header().block_hash;
    let randao_reveal = sign_randao(&st, &fixture.sk, epoch);

    let payload = ExecutionPayload::<Minimal> {
        parent_hash,
        prev_randao,
        timestamp,
        block_number: st.latest_execution_payload_header().block_number + 1,
        gas_limit: st.latest_execution_payload_header().gas_limit,
        block_hash: Root::from_array([0xab; 32]),
        withdrawals: VariableList::new(withdrawals).expect("withdrawals list"),
        ..Default::default()
    };
    let body = BeaconBlockBody::<Minimal> {
        randao_reveal,
        eth1_data: st.eth1_data(),
        execution_payload: payload,
        sync_aggregate: SyncAggregate {
            sync_committee_bits: Default::default(),
            sync_committee_signature: BlsSignature::from_array(INFINITY_SIGNATURE),
        },
        ..Default::default()
    };
    let mut message = BeaconBlock {
        slot: next_slot,
        proposer_index: proposer,
        parent_root: fixture.anchor_root,
        state_root: Root::ZERO,
        body,
    };
    let trial_signed = SignedBeaconBlock {
        message: message.clone(),
        signature: BlsSignature::default(),
    };
    let mut trial = parent_state;
    match state_transition(
        &mut trial,
        &trial_signed,
        &ctx,
        BlockSignatureStrategy::NoVerification,
    ) {
        Err(cc_state_transition::BlockError::StateRootMismatch { actual, .. }) => {
            message.state_root = actual;
        }
        Ok(()) => message.state_root = trial.canonical_root(),
        Err(e) => panic!("state_transition for child: {e}"),
    }
    let signed = sign_proposer(&st, &fixture.sk, message);
    let root = Root::from_hash256(TreeHash::tree_hash_root(&signed.message));
    SignedChild { signed, root }
}

pub(crate) fn beacon_config(root: &std::path::Path, el_endpoint: String) -> BeaconCoreConfig {
    let jwt = root.join("jwt.hex");
    std::fs::write(
        &jwt,
        "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
    )
    .expect("jwt");
    let mut perms = std::fs::metadata(&jwt).expect("jwt meta").permissions();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        perms.set_mode(0o600);
    }
    std::fs::set_permissions(&jwt, perms).expect("jwt mode");

    BeaconCoreConfig {
        service: cc_config::ServiceConfig {
            grpc_addr: "127.0.0.1:0".parse().expect("grpc"),
            metrics_addr: "127.0.0.1:0".parse().expect("metrics"),
            peers: BTreeMap::new(),
            log_format: "json".into(),
            log_filter: "info".into(),
        },
        data_dir: root.to_path_buf(),
        durability: "immediate".into(),
        check_invariants: true,
        snapshot_ring: 4,
        genesis_validators_root: None,
        node_key_path: root.join("node_key"),
        max_resident_states: 4,
        body_ring_capacity: 64,
        event_ring_events: 64,
        event_ring_bytes: 1_048_576,
        subscriber_queue_capacity: 16,
        checkpoint_providers: Vec::new(),
        checkpoint_provider: None,
        genesis_anchor: None,
        chain_config: Some(minimal_config()),
        checkpoint_root: None,
        network_config: None,
        engine: cc_engine_api::config::EngineTransportConfig {
            el_endpoint,
            jwt_secret_path: jwt,
            timeouts: cc_engine_api::config::TimeoutKnobs {
                new_payload_ms: 1_000,
                forkchoice_updated_ms: 1_000,
                get_blobs_ms: 500,
                exchange_capabilities_ms: 500,
                eth_syncing_ms: 500,
                multiplier: 1.0,
            },
            slot_duration_ms: 200,
            el_forks: Some(cc_engine_api::config::ElForksConfig {
                osaka_time: 0,
                bpo1_time: None,
                bpo2_time: None,
                amsterdam_time: None,
            }),
            ..cc_engine_api::config::EngineTransportConfig::default()
        },
        maximum_gossip_clock_disparity_ms: 500,
    }
}

/// Loopback EL. Counts `engine_forkchoiceUpdated*` so a refused anchor can
/// show the stub saw no further call. Responses stay VALID.
pub(crate) struct ElStub {
    endpoint: String,
    forkchoice_updated: Arc<AtomicU64>,
}

impl ElStub {
    pub(crate) fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub(crate) fn forkchoice_updated_calls(&self) -> u64 {
        self.forkchoice_updated.load(Ordering::Relaxed)
    }
}

/// JSON-RPC EL on loopback. Answers the upcheck and `newPayloadV4` / fcU as VALID.
pub(crate) fn spawn_el_stub() -> ElStub {
    let listener = TcpListener::bind("127.0.0.1:0").expect("el bind");
    let addr = listener.local_addr().expect("el addr");
    let forkchoice_updated = Arc::new(AtomicU64::new(0));
    let counter = Arc::clone(&forkchoice_updated);
    thread::spawn(move || {
        for conn in listener.incoming() {
            let Ok(stream) = conn else { continue };
            let counter = Arc::clone(&counter);
            thread::spawn(move || serve_el(stream, counter));
        }
    });
    ElStub {
        endpoint: format!("http://{addr}"),
        forkchoice_updated,
    }
}

fn serve_el(mut stream: TcpStream, forkchoice_updated: Arc<AtomicU64>) {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
    while let Some(req) = read_jsonrpc(&mut stream) {
        let id = req.get("id").cloned().unwrap_or(serde_json::json!(1));
        let method = req.get("method").and_then(|m| m.as_str()).unwrap_or("");
        if method.contains("forkchoiceUpdated") {
            forkchoice_updated.fetch_add(1, Ordering::Relaxed);
        }
        let result = match method {
            "eth_syncing" => serde_json::json!(false),
            "eth_chainId" => serde_json::json!("0x1"),
            "engine_exchangeCapabilities" => serde_json::json!([
                "engine_newPayloadV4",
                "engine_forkchoiceUpdatedV3",
                "engine_getBlobsV2",
                "eth_syncing",
                "eth_chainId"
            ]),
            "engine_newPayloadV4" => serde_json::json!({
                "status": "VALID",
                "latestValidHash": null,
                "validationError": null
            }),
            "engine_forkchoiceUpdatedV3" => serde_json::json!({
                "payloadStatus": {
                    "status": "VALID",
                    "latestValidHash": null,
                    "validationError": null
                },
                "payloadId": null
            }),
            "engine_getBlobsV2" => serde_json::json!([]),
            _ => serde_json::json!(null),
        };
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": result
        })
        .to_string();
        let header = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
            body.len()
        );
        if stream.write_all(header.as_bytes()).is_err()
            || stream.write_all(body.as_bytes()).is_err()
        {
            break;
        }
    }
}

fn read_jsonrpc(stream: &mut TcpStream) -> Option<serde_json::Value> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    loop {
        if let Some(header_end) = find_header_end(&buf) {
            let header = String::from_utf8_lossy(&buf[..header_end]).to_string();
            let len = content_length(&header)?;
            let body_at = header_end + 4;
            while buf.len() < body_at + len {
                match stream.read(&mut tmp) {
                    Ok(0) => return None,
                    Ok(n) => buf.extend_from_slice(&tmp[..n]),
                    Err(_) => return None,
                }
            }
            return serde_json::from_slice(&buf[body_at..body_at + len]).ok();
        }
        match stream.read(&mut tmp) {
            Ok(0) => return None,
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
            Err(_) => return None,
        }
    }
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

fn content_length(header: &str) -> Option<usize> {
    for line in header.lines() {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("content-length") {
            return value.trim().parse().ok();
        }
    }
    None
}
