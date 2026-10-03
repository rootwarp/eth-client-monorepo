//! CC-19b — anchor store construction, lifecycle, and health.
//!
//! Covers:
//! - bind-before-bootstrap: self SERVING while aggregate NOT_SERVING, then flip
//! - `NOT_BOOTSTRAPPED` on ImportBlock / GetHead before core install
//! - warm `canonical_root` at bootstrap (cold metric count does not grow on first import)
//! - core join on pre-drain within the 2 s / 5 s shutdown budget

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use cc_bootstrap::{
    AGGREGATE_HEALTH, LocalReadyHandle, ServeOptions, ServiceSpec, bootstrap_without_tracing,
    serve_with_shutdown_options,
};
use cc_chain::checkpoint_sync::{
    FetchedCheckpoint, GenesisInfo, spawn_core_from_checkpoint, warm_canonical_root,
};
use cc_chain::core::{CoreConfig, CoreThread, spawn_core_thread};
use cc_chain::events::{EventsConfig, EventsHandle};
use cc_chain::head::HeadSnapshotStore;
use cc_chain::import::encode_signed_block;
use cc_chain::metrics::{ChainMetrics, HashPath};
use cc_chain::service::{ChainServiceImpl, REASON_NOT_BOOTSTRAPPED};
use cc_fork_choice::{HarnessAvailability, get_forkchoice_store};
use cc_proto::chain::chain_service_server::{ChainService, ChainServiceServer};
use cc_proto::chain::{GetHeadRequest, ImportBlockRequest, ImportBlockVerdict};
use cc_proto::error_info_from_status;
use cc_types::config::{BlobParameters, BlobSchedule, ChainConfig, PresetName};
use cc_types::preset::Minimal;
use cc_types::primitives::{Epoch, ExecutionAddress, ForkVersion, Root, Slot, ValidatorIndex};
use cc_types::{BeaconBlock, BeaconState, SignedBeaconBlock};
use prometheus_client::registry::Registry;
use tokio::sync::oneshot;
use tonic::Code;
use tonic::Request;
use tonic::service::Routes;
use tonic::transport::Endpoint;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus as WireStatus;
use tonic_health::pb::health_client::HealthClient;
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

const HEALTH_SERVICE_NAME: &str = "eth.chain.v1.ChainService";

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

/// In-process engine required by checkpoint seed.
///
/// `EngineApi::from_parts` spawns the upcheck, which POSTs `eth_syncing`.
/// The listener is bound to `127.0.0.1:0` and accepts without a JSON-RPC
/// body, so that dial stays on a socket this test owns. Anchor re-import
/// still returns before `newPayload`.
async fn in_process_engine() -> Arc<cc_chain::DirectEngine> {
    use cc_engine_api::EngineApi;
    use cc_engine_api::capabilities::CapabilityCache;
    use cc_engine_api::config::{TimeoutKnobs, TransportTimeouts};
    use cc_engine_api::fastpath::FastpathLane;
    use cc_engine_api::fastpath::filter::SubscriptionSet;
    use cc_engine_api::state::EngineStateHandle;
    use cc_engine_api::transport::EngineTransport;
    use cc_engine_api::version::ElForkSchedule;
    use tokio::runtime::Handle;

    // Same accept-and-hold shape as `spawn_black_hole` in
    // `engine_blackhole_liveness`. That helper is a sibling test binary, so
    // this fixture binds its own listener.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
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

    let timeouts = TransportTimeouts::from_knobs(&TimeoutKnobs {
        new_payload_ms: 50,
        forkchoice_updated_ms: 50,
        get_blobs_ms: 50,
        exchange_capabilities_ms: 50,
        eth_syncing_ms: 50,
        multiplier: 1.0,
    });
    let transport = Arc::new(EngineTransport::from_secret_bytes(
        format!("http://{addr}"),
        [1u8; 32],
        timeouts.clone(),
        Duration::from_millis(50),
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
        Duration::from_millis(50),
    );
    let bound = cc_engine_api::fastpath::fetch::BlobBound::from_chain_config(&minimal_config())
        .expect("minimal blob schedule");
    let lane = FastpathLane::new(
        Arc::clone(&transport),
        None,
        bound,
        None,
        None,
        SubscriptionSet::empty(),
    );
    let api = EngineApi::from_parts(transport, schedule, Handle::current(), None, state, lane);
    Arc::new(cc_chain::DirectEngine::new(api, timeouts))
}

fn anchor_fixture() -> (BeaconState<Minimal>, SignedBeaconBlock<Minimal>, Root) {
    let mut state = BeaconState::<Minimal>::default();
    state.set_genesis_time(1_600_000_000);
    state.set_slot(Slot::new(0));
    let mut block = BeaconBlock {
        slot: Slot::new(0),
        proposer_index: ValidatorIndex::new(0),
        parent_root: Root::ZERO,
        state_root: Root::ZERO,
        body: Default::default(),
    };
    // Align state_root with the block field for store seeding.
    let state_root = state.canonical_root();
    // Reset caches so spawn_core_from_checkpoint's warm path is meaningful.
    state.caches_mut().field_roots.mark_all_dirty();
    block.state_root = state_root;
    let signed = SignedBeaconBlock {
        message: block,
        signature: Default::default(),
    };
    let block_root = Root::from_hash256(TreeHash::tree_hash_root(&signed.message));
    (state, signed, block_root)
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

// ── health: self SERVING during bootstrap, aggregate flips after ───────────

/// Self FQ name is SERVING while aggregate stays NOT_SERVING until local ready;
/// then aggregate flips SERVING (no peers).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn self_serving_while_aggregate_not_serving_until_bootstrap() {
    let grpc = ephemeral();
    let metrics_addr = ephemeral();
    let bs = bootstrap_without_tracing("chain-lifecycle-health");
    let mut registry = Registry::default();
    let chain_metrics = ChainMetrics::register(&mut registry);
    let events = EventsHandle::spawn(EventsConfig::default());
    let head = HeadSnapshotStore::new();
    let svc = ChainServiceImpl::new(None, head, events, chain_metrics);
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

    // Port bound + self health SERVING; aggregate still gated.
    wait_health(
        grpc,
        HEALTH_SERVICE_NAME,
        WireStatus::Serving,
        Duration::from_secs(5),
    )
    .await;
    // Assert aggregate is NOT_SERVING at least once while bootstrap is "in progress".
    let agg = health_status(grpc, AGGREGATE_HEALTH)
        .await
        .expect("aggregate health");
    assert_eq!(
        agg,
        WireStatus::NotServing as i32,
        "aggregate must stay NOT_SERVING until mark_ready"
    );

    let gate = ready_rx.await.expect("local ready handle");
    // Still NOT_SERVING until we mark ready.
    let agg2 = health_status(grpc, AGGREGATE_HEALTH).await.unwrap();
    assert_eq!(agg2, WireStatus::NotServing as i32);

    gate.mark_ready().await;
    wait_health(
        grpc,
        AGGREGATE_HEALTH,
        WireStatus::Serving,
        Duration::from_secs(2),
    )
    .await;

    // Self stays SERVING across the flip.
    let self_st = health_status(grpc, HEALTH_SERVICE_NAME).await.unwrap();
    assert_eq!(self_st, WireStatus::Serving as i32);

    let _ = stop_tx.send(());
    let result = tokio::time::timeout(Duration::from_secs(5), serve)
        .await
        .expect("serve join timeout")
        .expect("serve task");
    assert!(result.is_ok(), "serve: {result:?}");
}

/// Compose `grpc-health-probe` reads aggregate `""`. N consecutive
/// `probe_core_liveness` misses must flip that bit — process-up is not enough.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn three_core_liveness_misses_flip_aggregate_not_serving() {
    let grpc = ephemeral();
    let metrics_addr = ephemeral();
    let bs = bootstrap_without_tracing("chain-lifecycle-liveness");
    let mut registry = Registry::default();
    let chain_metrics = ChainMetrics::register(&mut registry);
    let events = EventsHandle::spawn(EventsConfig::default());
    let head = HeadSnapshotStore::new();
    let svc = ChainServiceImpl::new(None, head, events, chain_metrics);
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

    wait_health(
        grpc,
        HEALTH_SERVICE_NAME,
        WireStatus::Serving,
        Duration::from_secs(5),
    )
    .await;

    let gate = ready_rx.await.expect("local ready handle");
    gate.mark_ready().await;
    wait_health(
        grpc,
        AGGREGATE_HEALTH,
        WireStatus::Serving,
        Duration::from_secs(2),
    )
    .await;

    struct SilentCore;
    impl cc_chain::CoreLiveness for SilentCore {
        fn ping(
            &self,
        ) -> impl std::future::Future<Output = Result<(), cc_chain::LivenessError>> + Send {
            std::future::pending()
        }
    }

    let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
    let sampler = tokio::spawn(cc_chain::run_core_liveness_loop(
        SilentCore,
        gate.clone(),
        Duration::from_millis(30),
        Duration::from_millis(10),
        cancel_rx,
        None,
    ));

    wait_health(
        grpc,
        AGGREGATE_HEALTH,
        WireStatus::NotServing,
        Duration::from_secs(2),
    )
    .await;
    let self_st = health_status(grpc, HEALTH_SERVICE_NAME).await.unwrap();
    assert_eq!(
        self_st,
        WireStatus::Serving as i32,
        "FQ self health stays SERVING; only aggregate reflects the parked core"
    );

    let _ = cancel_tx.send(true);
    let _ = sampler.await;
    let _ = stop_tx.send(());
    let result = tokio::time::timeout(Duration::from_secs(5), serve)
        .await
        .expect("serve join timeout")
        .expect("serve task");
    assert!(result.is_ok(), "serve: {result:?}");
}

/// One miss is not a park: compose must not restart on a single long sample.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_core_liveness_miss_keeps_aggregate_serving() {
    let grpc = ephemeral();
    let metrics_addr = ephemeral();
    let bs = bootstrap_without_tracing("chain-lifecycle-liveness-one");
    let mut registry = Registry::default();
    let chain_metrics = ChainMetrics::register(&mut registry);
    let events = EventsHandle::spawn(EventsConfig::default());
    let head = HeadSnapshotStore::new();
    let svc = ChainServiceImpl::new(None, head, events, chain_metrics);
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

    struct OneMissThenLive {
        hits: std::sync::atomic::AtomicUsize,
    }
    impl cc_chain::CoreLiveness for OneMissThenLive {
        fn ping(
            &self,
        ) -> impl std::future::Future<Output = Result<(), cc_chain::LivenessError>> + Send {
            let n = self.hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async move {
                if n == 0 {
                    std::future::pending::<()>().await;
                }
                Ok(())
            }
        }
    }

    let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
    let sampler = tokio::spawn(cc_chain::run_core_liveness_loop(
        OneMissThenLive {
            hits: std::sync::atomic::AtomicUsize::new(0),
        },
        gate,
        Duration::from_millis(30),
        Duration::from_millis(10),
        cancel_rx,
        None,
    ));

    // First sample misses (~30 ms) then successes. Stay SERVING throughout
    // (N=3; one miss cannot flip).
    tokio::time::sleep(Duration::from_millis(150)).await;
    let agg = health_status(grpc, AGGREGATE_HEALTH).await.unwrap();
    assert_eq!(agg, WireStatus::Serving as i32);

    let _ = cancel_tx.send(true);
    let _ = sampler.await;
    let _ = stop_tx.send(());
    let result = tokio::time::timeout(Duration::from_secs(5), serve)
        .await
        .expect("serve join timeout")
        .expect("serve task");
    assert!(result.is_ok(), "serve: {result:?}");
}

/// Recover needs the same N successes as misses (S-A16-2). One later ping
/// must not flap aggregate `""` back to SERVING.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn three_successes_restore_aggregate_serving() {
    let grpc = ephemeral();
    let metrics_addr = ephemeral();
    let bs = bootstrap_without_tracing("chain-lifecycle-liveness-restore");
    let mut registry = Registry::default();
    let chain_metrics = ChainMetrics::register(&mut registry);
    let events = EventsHandle::spawn(EventsConfig::default());
    let head = HeadSnapshotStore::new();
    let svc = ChainServiceImpl::new(None, head, events, chain_metrics);
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

    struct FailThenLive {
        remaining: std::sync::atomic::AtomicUsize,
    }
    impl cc_chain::CoreLiveness for FailThenLive {
        fn ping(
            &self,
        ) -> impl std::future::Future<Output = Result<(), cc_chain::LivenessError>> + Send {
            let left = self.remaining.fetch_update(
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
                |n| Some(n.saturating_sub(1)),
            );
            async move {
                if left.unwrap_or(0) > 0 {
                    std::future::pending().await
                } else {
                    Ok(())
                }
            }
        }
    }

    let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
    let sampler = tokio::spawn(cc_chain::run_core_liveness_loop(
        FailThenLive {
            remaining: std::sync::atomic::AtomicUsize::new(3),
        },
        gate,
        Duration::from_millis(30),
        Duration::from_millis(10),
        cancel_rx,
        None,
    ));

    wait_health(
        grpc,
        AGGREGATE_HEALTH,
        WireStatus::NotServing,
        Duration::from_secs(2),
    )
    .await;
    wait_health(
        grpc,
        AGGREGATE_HEALTH,
        WireStatus::Serving,
        Duration::from_secs(2),
    )
    .await;

    let _ = cancel_tx.send(true);
    let _ = sampler.await;
    let _ = stop_tx.send(());
    let result = tokio::time::timeout(Duration::from_secs(5), serve)
        .await
        .expect("serve join timeout")
        .expect("serve task");
    assert!(result.is_ok(), "serve: {result:?}");
}

// ── NOT_BOOTSTRAPPED before core install ───────────────────────────────────

#[tokio::test]
async fn import_and_get_head_not_bootstrapped_before_core() {
    let mut registry = Registry::default();
    let metrics = ChainMetrics::register(&mut registry);
    let events = EventsHandle::spawn(EventsConfig::default());
    let svc = ChainServiceImpl::new(None, HeadSnapshotStore::new(), events, metrics);

    let err = svc
        .import_block(Request::new(ImportBlockRequest {
            ssz: vec![],
            fork: 0,
            root: vec![0u8; 32],
            source: 0,
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    let info = error_info_from_status(&err).unwrap().unwrap();
    assert_eq!(info.reason, REASON_NOT_BOOTSTRAPPED);

    let err = svc
        .get_head(Request::new(GetHeadRequest {}))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    let info = error_info_from_status(&err).unwrap().unwrap();
    assert_eq!(info.reason, REASON_NOT_BOOTSTRAPPED);
}

#[tokio::test]
async fn install_core_clears_not_bootstrapped_for_get_head() {
    let (store, _, config, _) = {
        let config = minimal_config();
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
        let store = get_forkchoice_store(
            state,
            &anchor_block,
            Arc::new(AcceptEngine),
            Arc::new(HarnessAvailability),
            config.seconds_per_slot,
        )
        .unwrap();
        (store, Root::ZERO, config, ())
    };
    let mut registry = Registry::default();
    let metrics = ChainMetrics::register(&mut registry);
    let events = EventsHandle::spawn(EventsConfig::default());
    let head = HeadSnapshotStore::new();
    let svc = ChainServiceImpl::new(None, head.clone(), events.clone(), metrics.clone());

    // Pre-install: not bootstrapped.
    assert!(!svc.is_bootstrapped());
    assert!(svc.get_head(Request::new(GetHeadRequest {})).await.is_err());

    let core = spawn_core_thread(
        store,
        config,
        head,
        events.event_sender(),
        metrics,
        CoreConfig::default(),
    );
    svc.install_core(core.handle.clone());
    assert!(svc.is_bootstrapped());

    let resp = svc
        .get_head(Request::new(GetHeadRequest {}))
        .await
        .expect("GetHead after install")
        .into_inner();
    assert_eq!(resp.head_root.len(), 32);

    core.handle.shutdown().await;
    core.join();
}

// ── warm canonical_root ────────────────────────────────────────────────────

#[test]
fn warm_canonical_root_populates_caches_and_records_cold_once() {
    let mut registry = Registry::default();
    let metrics = ChainMetrics::register(&mut registry);
    let mut state = BeaconState::<Minimal>::default();
    state.set_slot(Slot::new(3));
    assert!(
        state.caches().field_roots.needs_recompute(),
        "decoded/default state starts cold"
    );

    // seed_exposition observes 0.0 once per path at register — baseline.
    let cold_before = metrics.state_hash_tree_root_count(HashPath::Cold);
    let root = warm_canonical_root(&mut state, &metrics);
    assert!(!state.caches().field_roots.needs_recompute());
    assert_eq!(
        metrics.state_hash_tree_root_count(HashPath::Cold),
        cold_before + 1,
        "warm_canonical_root must record exactly one cold observation"
    );

    // Second call without the metric helper must not bump the counter.
    let root2 = state.canonical_root();
    assert_eq!(root, root2);
    assert_eq!(
        metrics.state_hash_tree_root_count(HashPath::Cold),
        cold_before + 1,
        "second call without metric observe must not bump cold count"
    );
}

/// Warm-before-store-seed retains warm field-root caches on the resident state
/// (the failure mode the issue cares about — not just the helper in isolation).
#[test]
fn warm_then_forkchoice_store_seed_retains_warm_caches() {
    let (mut state, signed, block_root) = anchor_fixture();
    state.caches_mut().field_roots.mark_all_dirty();
    assert!(state.caches().field_roots.needs_recompute());

    let mut registry = Registry::default();
    let metrics = ChainMetrics::register(&mut registry);
    let _ = warm_canonical_root(&mut state, &metrics);
    assert!(!state.caches().field_roots.needs_recompute());

    let store = get_forkchoice_store(
        state,
        &signed.message,
        Arc::new(AcceptEngine),
        Arc::new(HarnessAvailability),
        6,
    )
    .expect("get_forkchoice_store");
    let resident = store
        .block_state(&block_root)
        .expect("anchor state seeded into store");
    assert!(
        !resident.caches().field_roots.needs_recompute(),
        "store seed must retain warm caches so the first real import does not pay cold full root"
    );
    assert_eq!(store.justified_checkpoint().root, block_root);
    assert_eq!(store.finalized_checkpoint().root, block_root);
}

#[tokio::test]
async fn spawn_core_from_checkpoint_warms_caches_first_import_no_extra_cold() {
    let (mut state, signed, block_root) = anchor_fixture();
    // Ensure cold before spawn.
    state.caches_mut().field_roots.mark_all_dirty();
    assert!(state.caches().field_roots.needs_recompute());

    let mut registry = Registry::default();
    let metrics = ChainMetrics::register(&mut registry);
    let events = EventsHandle::spawn(EventsConfig::default());
    let head = HeadSnapshotStore::new();
    let config = minimal_config();

    let fetched = FetchedCheckpoint {
        genesis: GenesisInfo {
            genesis_time: state.genesis_time(),
            genesis_validators_root: state.genesis_validators_root(),
        },
        signed_block: signed.clone(),
        state,
        block_root,
        provider: "test://fixture".into(),
    };

    // Seed refuses a missing in-process engine. Anchor re-import returns
    // before newPayload; the upcheck still dials the listener this test owns.
    let core = spawn_core_from_checkpoint(
        fetched,
        config.clone(),
        head.clone(),
        events.event_sender(),
        metrics.clone(),
        CoreConfig {
            engine: Some(in_process_engine().await),
            ..CoreConfig::default()
        },
    )
    .expect("spawn from checkpoint");

    // seed_exposition + one warm-up observation. First post-bootstrap import
    // must not add another cold sample (AC: cold count delta on first import = 0).
    let cold_after_boot = metrics.state_hash_tree_root_count(HashPath::Cold);
    assert!(
        cold_after_boot >= 1,
        "bootstrap must record at least the warm-up cold observation"
    );

    // First import of the anchor as DUPLICATE: no state transition → no extra cold.
    let ssz = encode_signed_block(&signed);
    let resp = core
        .handle
        .import_block(ImportBlockRequest {
            ssz,
            fork: 0,
            root: block_root.as_slice().to_vec(),
            source: 0,
        })
        .await
        .expect("import anchor");
    assert_eq!(resp.verdict, ImportBlockVerdict::Duplicate as i32);

    assert_eq!(
        metrics.state_hash_tree_root_count(HashPath::Cold),
        cold_after_boot,
        "first post-bootstrap import must not increment cold hash count"
    );

    // Snapshot published for GetHead.
    let snap = head.load();
    assert_eq!(snap.head_root, block_root);
    assert_eq!(snap.justified.root, block_root);
    assert_eq!(snap.finalized.root, block_root);

    core.handle.shutdown().await;
    core.join();
}

// ── shutdown: core join within budget ──────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pre_drain_joins_core_within_shutdown_budget() {
    let grpc = ephemeral();
    let metrics_addr = ephemeral();
    let bs = bootstrap_without_tracing("chain-lifecycle-shutdown");

    let mut registry = Registry::default();
    let chain_metrics = ChainMetrics::register(&mut registry);
    let events = EventsHandle::spawn(EventsConfig::default());
    let head = HeadSnapshotStore::new();

    let config = minimal_config();
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
    let store = get_forkchoice_store(
        state,
        &anchor_block,
        Arc::new(AcceptEngine),
        Arc::new(HarnessAvailability),
        config.seconds_per_slot,
    )
    .unwrap();
    let core = spawn_core_thread(
        store,
        config,
        head.clone(),
        events.event_sender(),
        chain_metrics.clone(),
        CoreConfig::default(),
    );

    // Brief core work before stop (under the single 2 s Shutdown+join budget).
    let block_done = {
        let handle = core.handle.clone();
        tokio::spawn(async move {
            handle.block_for(Duration::from_millis(200)).await.unwrap();
        })
    };

    let svc = ChainServiceImpl::new(Some(core.handle.clone()), head, events, chain_metrics);
    let routes = Routes::default().add_service(ChainServiceServer::new(svc));

    let core_holder: Arc<Mutex<Option<CoreThread>>> = Arc::new(Mutex::new(Some(core)));
    let core_holder_shutdown = Arc::clone(&core_holder);
    let joined = Arc::new(Mutex::new(false));
    let joined_flag = Arc::clone(&joined);

    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let started = Instant::now();
    let serve = tokio::spawn(async move {
        serve_with_shutdown_options(
            bs,
            chain_spec(grpc, metrics_addr),
            routes,
            ServeOptions {
                require_local_ready: false,
                local_ready_tx: None,
                on_pre_drain: Some(Box::new(move || {
                    let holder = core_holder_shutdown;
                    let flag = joined_flag;
                    Box::pin(async move {
                        let core = holder.lock().ok().and_then(|mut g| g.take());
                        if let Some(core) = core {
                            // Single 2 s Shutdown+join envelope (matches production).
                            core.shutdown_and_join().await;
                            if let Ok(mut g) = flag.lock() {
                                *g = true;
                            }
                        }
                    })
                })),
            },
            async move {
                let _ = stop_rx.await;
            },
        )
        .await
    });

    wait_health(
        grpc,
        AGGREGATE_HEALTH,
        WireStatus::Serving,
        Duration::from_secs(5),
    )
    .await;

    // Wait for the simulated work to be on the core, then stop.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let _ = stop_tx.send(());

    let result = tokio::time::timeout(Duration::from_secs(5), serve)
        .await
        .expect("serve must finish within 5 s of cancel")
        .expect("serve task join");
    assert!(result.is_ok(), "serve: {result:?}");
    assert!(
        *joined.lock().unwrap(),
        "pre-drain hook must have joined the core"
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "full shutdown budget is 5 s"
    );

    // Ensure block_for task completed or was abandoned cleanly.
    let _ = tokio::time::timeout(Duration::from_secs(2), block_done).await;
}

// ── ErrorInfo construction locality ────────────────────────────────────────

#[test]
fn not_bootstrapped_reason_constant_is_stable() {
    // Pin the third ErrorInfo reason value (alongside CURSOR_*).
    assert_eq!(REASON_NOT_BOOTSTRAPPED, "NOT_BOOTSTRAPPED");
}
