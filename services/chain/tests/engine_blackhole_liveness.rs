//! S1-A-17 / E1.2 — injected engine black-hole flips aggregate health red.
//!
//! After S1-A-06 the engine path is in-process `DirectEngine` → EL HTTP.
//! A TCP sink accepts the connection and never answers `newPayload` / `fcU`.
//! An in-flight import parks the consensus OS thread on that path;
//! `probe_core_liveness` then misses N=3 times and aggregate `""` goes
//! `NOT_SERVING`. `GetHead` is a snapshot load (ADR-P1-09) and still answers.
//!
//! E1.2 citation is
//! [`black_holed_new_payload_flips_production_probe_budget`]: ADR-R-04
//! deadline / interval / N=3 and a hang `> 8 s × N`. The compressed-sampler
//! test is a smoke, not the demonstration. Live compose with production 8 s
//! caps stays SERVING on one RPC — do not paste CI `eprintln` as a scrape.
//!
//! The commit deadline is on async `commit_import` only (ARCH §4.5.6).
//! The core thread still persists through the blocking path with no
//! deadline. Probe coverage of an archive stall starts when that thread
//! blocks in `commit_import` (S2R-A-05). Aborting is not backpressure
//! (ADR-R-08). No live archive-stall probe was run; this sentence is not
//! a soak result.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use cc_bootstrap::{
    AGGREGATE_HEALTH, LocalReadyHandle, ServeOptions, ServiceSpec, bootstrap_without_tracing,
    serve_with_shutdown_options,
};
use cc_chain::DirectEngine;
use cc_chain::core::{CoreConfig, spawn_core_thread};
use cc_chain::events::{EventsConfig, EventsHandle};
use cc_chain::head::HeadSnapshotStore;
use cc_chain::import::encode_signed_block;
use cc_chain::metrics::ChainMetrics;
use cc_chain::service::ChainServiceImpl;
use cc_chain::{
    CONSECUTIVE_MISS_THRESHOLD, DEFAULT_ENGINE_NEW_PAYLOAD_TIMEOUT, DEFAULT_SLOT_DURATION_MS,
    LivenessError, default_liveness_deadline, probe_core_liveness, run_core_liveness_loop,
    sample_interval,
};
use cc_engine_api::EngineApi;
use cc_engine_api::capabilities::CapabilityCache;
use cc_engine_api::config::{TimeoutKnobs, TransportTimeouts};
use cc_engine_api::fastpath::FastpathLane;
use cc_engine_api::fastpath::filter::SubscriptionSet;
use cc_engine_api::methods::eth_syncing::EthSyncingResult;
use cc_engine_api::state::{EngineStateHandle, UpcheckOutcome};
use cc_engine_api::transport::EngineTransport;
use cc_engine_api::version::ElForkSchedule;
use cc_fork_choice::{HarnessAvailability, get_forkchoice_store, on_tick};
use cc_proto::chain::chain_service_server::{ChainService, ChainServiceServer};
use cc_proto::chain::{GetHeadRequest, ImportBlockRequest};
use cc_state_transition::BlockSignatureStrategy;
use cc_state_transition::helpers::accessors::{get_current_epoch, get_randao_mix};
use cc_types::config::{BlobParameters, BlobSchedule, ChainConfig, PresetName};
use cc_types::containers::{BeaconBlockHeader, Validator};
use cc_types::preset::Minimal;
use cc_types::primitives::{
    Epoch, ExecutionAddress, ForkVersion, Gwei, Root, Slot, ValidatorIndex,
};
use cc_types::{BeaconBlock, BeaconBlockBody, BeaconState, SignedBeaconBlock};
use prometheus_client::registry::Registry;
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tokio::time::Instant;
use tonic::Request;
use tonic::service::Routes;
use tonic::transport::Endpoint;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus as WireStatus;
use tonic_health::pb::health_client::HealthClient;
use tree_hash::TreeHash;

const HEALTH_SERVICE_NAME: &str = "eth.chain.v1.ChainService";

/// Hang long enough to outlast 8 s × N and the ADR-R-04 miss horizon.
fn park_timeouts() -> TransportTimeouts {
    TransportTimeouts::from_knobs(&TimeoutKnobs {
        new_payload_ms: 30_000,
        forkchoice_updated_ms: 30_000,
        get_blobs_ms: 80,
        exchange_capabilities_ms: 80,
        eth_syncing_ms: 80,
        multiplier: 1.0,
    })
}

fn ephemeral() -> SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
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

/// Genesis whose slot-1 child reaches `process_execution_payload`.
fn seeded_store_with_engine(
    engine: Arc<DirectEngine>,
) -> (
    cc_fork_choice::Store<Minimal>,
    ChainConfig,
    SignedBeaconBlock<Minimal>,
) {
    let config = minimal_config();
    let mut state = BeaconState::<Minimal>::default();
    state.set_genesis_time(0);
    state.set_slot(Slot::new(0));
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
    state.set_latest_block_header(BeaconBlockHeader {
        slot: Slot::new(0),
        proposer_index: ValidatorIndex::new(0),
        parent_root: Root::ZERO,
        state_root: Root::ZERO,
        body_root: Root::from_hash256(TreeHash::tree_hash_root(
            &BeaconBlockBody::<Minimal>::default(),
        )),
    });

    let state_root = Root::from_hash256(TreeHash::tree_hash_root(&state));
    let anchor_block = BeaconBlock {
        slot: Slot::new(0),
        proposer_index: ValidatorIndex::new(0),
        parent_root: Root::ZERO,
        state_root,
        body: BeaconBlockBody::default(),
    };
    let mut store = get_forkchoice_store(
        state,
        &anchor_block,
        engine,
        Arc::new(HarnessAvailability),
        config.seconds_per_slot,
    )
    .unwrap();
    on_tick(&mut store, 12).unwrap();

    let parent = store
        .block_state(&Root::from_hash256(TreeHash::tree_hash_root(&anchor_block)))
        .expect("anchor state");
    let epoch = get_current_epoch::<Minimal>(parent);
    let mut body = BeaconBlockBody::<Minimal>::default();
    body.execution_payload.parent_hash = parent.latest_execution_payload_header().block_hash;
    body.execution_payload.prev_randao = get_randao_mix(parent, epoch).unwrap();
    body.execution_payload.timestamp = parent.genesis_time() + config.seconds_per_slot;

    let signed = SignedBeaconBlock {
        message: BeaconBlock {
            slot: Slot::new(1),
            proposer_index: ValidatorIndex::new(0),
            parent_root: Root::from_hash256(TreeHash::tree_hash_root(&anchor_block)),
            state_root: Root::ZERO,
            body,
        },
        signature: Default::default(),
    };
    (store, config, signed)
}

/// EL that accepts TCP and never answers (post-A-06 HTTP engine path).
async fn spawn_black_hole() -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let _stream = stream;
                std::future::pending::<()>().await;
            });
        }
    });
    (addr, server)
}

async fn black_holed_engine(addr: SocketAddr, timeouts: TransportTimeouts) -> Arc<DirectEngine> {
    let transport = Arc::new(EngineTransport::from_secret_bytes(
        format!("http://{addr}"),
        [1u8; 32],
        timeouts.clone(),
        Duration::from_millis(80),
        None,
    ));
    let schedule = ElForkSchedule {
        osaka_time: 0,
        bpo1_time: None,
        bpo2_time: None,
        amsterdam_time: None,
    };
    let state = EngineStateHandle::new(
        Arc::new(CapabilityCache::new()),
        None,
        Duration::from_millis(80),
    );
    // Admit EL calls so the Duration cap (not the Offline gate) is the hang.
    state
        .apply(UpcheckOutcome::Ok(EthSyncingResult::NotSyncing))
        .await;
    let cfg = cc_types::ChainConfig::from_yaml_str(include_str!(
        "../../../crates/types/tests/fixtures/hoodi-config.yaml"
    ))
    .unwrap();
    let bound = cc_engine_api::fastpath::fetch::BlobBound::from_chain_config(&cfg).unwrap();
    let lane = FastpathLane::new(
        Arc::clone(&transport),
        None,
        bound,
        None,
        None,
        SubscriptionSet::empty(),
    );
    let api = EngineApi::from_parts(transport, schedule, Handle::current(), None, state, lane);
    Arc::new(DirectEngine::new(api, timeouts))
}

async fn health_status(addr: SocketAddr, service: &str) -> Result<i32, tonic::Status> {
    let channel = Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .map_err(|e| tonic::Status::unavailable(e.to_string()))?;
    let mut client = HealthClient::new(channel);
    let resp = client
        .check(HealthCheckRequest {
            service: service.to_owned(),
        })
        .await?;
    Ok(resp.into_inner().status)
}

async fn wait_health(addr: SocketAddr, service: &str, want: WireStatus, budget: Duration) {
    let deadline = Instant::now() + budget;
    loop {
        match health_status(addr, service).await {
            Ok(status) if status == want as i32 => return,
            Ok(status) => {
                if Instant::now() >= deadline {
                    panic!(
                        "health {service:?} at {addr} still {status}, want {want:?} within {budget:?}"
                    );
                }
            }
            Err(e) => {
                if Instant::now() >= deadline {
                    panic!("health {service:?} at {addr} unreachable: {e} within {budget:?}");
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn chain_spec(grpc: SocketAddr, metrics: SocketAddr) -> ServiceSpec {
    ServiceSpec {
        name: "chain",
        health_service_name: HEALTH_SERVICE_NAME,
        grpc_addr: grpc,
        metrics_addr: metrics,
        peers: vec![],
        descriptor_set: cc_proto::FILE_DESCRIPTOR_SET,
        known_methods: vec![
            "/eth.chain.v1.ChainService/GetInfo".into(),
            "/eth.chain.v1.ChainService/ImportBlock".into(),
            "/eth.chain.v1.ChainService/GetHead".into(),
        ],
    }
}

/// Parked core via black-holed newPayload → aggregate `""` is NOT_SERVING.
async fn assert_black_hole_flips_not_serving(
    probe_deadline: Duration,
    sample_every: Duration,
    health_budget: Duration,
) {
    let (engine_addr, engine_server) = spawn_black_hole().await;
    let timeouts = park_timeouts();
    let engine = black_holed_engine(engine_addr, timeouts).await;

    let (store, config, signed) = seeded_store_with_engine(Arc::clone(&engine));
    let mut registry = Registry::default();
    let chain_metrics = ChainMetrics::register(&mut registry);
    let events = EventsHandle::spawn(EventsConfig::default());
    let head = HeadSnapshotStore::new();
    let core_cfg = CoreConfig {
        verify: BlockSignatureStrategy::NoVerification,
        engine: Some(engine),
        slot_tick_enabled: false,
        ..CoreConfig::default()
    };
    let core = spawn_core_thread(
        store,
        config,
        head.clone(),
        events.event_sender(),
        chain_metrics.clone(),
        core_cfg,
    );

    let grpc = ephemeral();
    let metrics_addr = ephemeral();
    let bs = bootstrap_without_tracing("chain-engine-blackhole");
    let svc = ChainServiceImpl::new(
        Some(core.handle.clone()),
        head,
        events,
        chain_metrics.clone(),
    );
    let svc_query = svc.clone();
    let routes = Routes::default().add_service(ChainServiceServer::new(svc));

    let (ready_tx, ready_rx) = oneshot::channel::<LocalReadyHandle>();
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let serve = tokio::spawn(async move {
        serve_with_shutdown_options(
            bs,
            chain_spec(grpc, metrics_addr),
            routes,
            ServeOptions {
                require_local_ready: true,
                local_ready_tx: Some(ready_tx),
                on_pre_drain: None,
            },
            async move {
                let _ = stop_rx.await;
            },
        )
        .await
    });

    let gate = ready_rx.await.expect("local ready handle");
    gate.mark_ready().await;
    wait_health(
        grpc,
        AGGREGATE_HEALTH,
        WireStatus::Serving,
        Duration::from_secs(5),
    )
    .await;

    let root = Root::from_hash256(TreeHash::tree_hash_root(&signed.message));
    let import_handle = core.handle.clone();
    let import = tokio::spawn(async move {
        import_handle
            .import_block(ImportBlockRequest {
                ssz: encode_signed_block(&signed),
                fork: 0,
                root: root.as_slice().to_vec(),
                source: 0,
            })
            .await
    });

    // Tick wins over import. Do not run the sampler until the core is already
    // parked on newPayload — otherwise pings starve the hang.
    let parked_by = Instant::now() + Duration::from_secs(2);
    loop {
        let miss = probe_core_liveness(&core.handle, Duration::from_millis(30)).await;
        if matches!(miss, Err(LivenessError::DeadlineExceeded { .. })) {
            break;
        }
        if Instant::now() >= parked_by {
            panic!("core never parked on black-holed newPayload: {miss:?}");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
    let sampler = tokio::spawn(run_core_liveness_loop(
        core.handle.clone(),
        gate,
        probe_deadline,
        sample_every,
        cancel_rx,
        Some(chain_metrics),
    ));

    wait_health(
        grpc,
        AGGREGATE_HEALTH,
        WireStatus::NotServing,
        health_budget,
    )
    .await;
    assert!(
        !import.is_finished(),
        "import must still be on the black-holed newPayload when aggregate flips"
    );

    // GetHead is not liveness (ADR-P1-09): snapshot still answers.
    let snap = svc_query
        .get_head(Request::new(GetHeadRequest {}))
        .await
        .expect("GetHead must not park with the core");
    assert_eq!(snap.into_inner().head_slot, 0);

    let self_st = health_status(grpc, HEALTH_SERVICE_NAME).await.unwrap();
    assert_eq!(
        self_st,
        WireStatus::Serving as i32,
        "FQ self health stays SERVING; only aggregate reflects the parked core"
    );

    eprintln!(
        "S1-A-17 CI: aggregate \"\" NOT_SERVING after N={} samples \
         (deadline={probe_deadline:?} interval={sample_every:?}); not a live compose scrape",
        CONSECUTIVE_MISS_THRESHOLD
    );

    engine_server.abort();
    let _ = cancel_tx.send(true);
    let _ = sampler.await;
    let _ = tokio::time::timeout(Duration::from_secs(5), import).await;
    let _ = stop_tx.send(());
    core.shutdown_and_join().await;
    let result = tokio::time::timeout(Duration::from_secs(5), serve)
        .await
        .expect("serve join timeout")
        .expect("serve task");
    assert!(result.is_ok(), "serve: {result:?}");
}

/// Smoke: compressed sampler. Not the E1.2 citation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn black_holed_new_payload_flips_compressed_sampler() {
    assert_black_hole_flips_not_serving(
        Duration::from_millis(30),
        Duration::from_millis(10),
        Duration::from_secs(2),
    )
    .await;
}

/// E1.2: production ADR-R-04 N=3 budget + hang > one Engine RPC deadline × N.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn black_holed_new_payload_flips_production_probe_budget() {
    let deadline = default_liveness_deadline();
    let interval = sample_interval(DEFAULT_SLOT_DURATION_MS);
    let hang = park_timeouts().new_payload;
    let n = CONSECUTIVE_MISS_THRESHOLD;
    assert!(
        hang > DEFAULT_ENGINE_NEW_PAYLOAD_TIMEOUT.saturating_mul(n),
        "hang {hang:?} must exceed 8s × N={n}"
    );
    let horizon = deadline
        .saturating_mul(n)
        .saturating_add(interval.saturating_mul(n - 1));
    assert!(
        hang > horizon,
        "hang {hang:?} must outlast production miss horizon {horizon:?}"
    );
    assert_black_hole_flips_not_serving(deadline, interval, Duration::from_secs(30)).await;
}

/// Idle core + black-holed engine stays SERVING (engine is not a health peer).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_core_with_black_holed_engine_stays_serving() {
    let (engine_addr, engine_server) = spawn_black_hole().await;
    let engine = black_holed_engine(engine_addr, park_timeouts()).await;

    let (store, config, _signed) = seeded_store_with_engine(Arc::clone(&engine));
    let mut registry = Registry::default();
    let chain_metrics = ChainMetrics::register(&mut registry);
    let events = EventsHandle::spawn(EventsConfig::default());
    let head = HeadSnapshotStore::new();
    let core_cfg = CoreConfig {
        engine: Some(engine),
        slot_tick_enabled: false,
        ..CoreConfig::default()
    };
    let core = spawn_core_thread(
        store,
        config,
        head.clone(),
        events.event_sender(),
        chain_metrics.clone(),
        core_cfg,
    );

    let grpc = ephemeral();
    let metrics_addr = ephemeral();
    let bs = bootstrap_without_tracing("chain-engine-blackhole-idle");
    let svc = ChainServiceImpl::new(Some(core.handle.clone()), head, events, chain_metrics);
    let routes = Routes::default().add_service(ChainServiceServer::new(svc));

    let (ready_tx, ready_rx) = oneshot::channel::<LocalReadyHandle>();
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let serve = tokio::spawn(async move {
        serve_with_shutdown_options(
            bs,
            chain_spec(grpc, metrics_addr),
            routes,
            ServeOptions {
                require_local_ready: true,
                local_ready_tx: Some(ready_tx),
                on_pre_drain: None,
            },
            async move {
                let _ = stop_rx.await;
            },
        )
        .await
    });

    let gate = ready_rx.await.expect("local ready handle");
    gate.mark_ready().await;
    wait_health(
        grpc,
        AGGREGATE_HEALTH,
        WireStatus::Serving,
        Duration::from_secs(5),
    )
    .await;

    let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
    let sampler = tokio::spawn(run_core_liveness_loop(
        core.handle.clone(),
        gate,
        Duration::from_millis(30),
        Duration::from_millis(10),
        cancel_rx,
        None,
    ));

    // Longer than N=3 miss horizon at these deadlines. No import → no park.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let agg = health_status(grpc, AGGREGATE_HEALTH).await.unwrap();
    assert_eq!(
        agg,
        WireStatus::Serving as i32,
        "black-holed engine with an idle core must not flip aggregate health"
    );

    engine_server.abort();
    let _ = cancel_tx.send(true);
    let _ = sampler.await;
    let _ = stop_tx.send(());
    core.shutdown_and_join().await;
    let result = tokio::time::timeout(Duration::from_secs(5), serve)
        .await
        .expect("serve join timeout")
        .expect("serve task");
    assert!(result.is_ok(), "serve: {result:?}");
}
