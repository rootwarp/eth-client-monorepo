//! Boot sequence ([ARCH] §4.2).
//!
//! ```text
//! open redb  →  durable_set  →  seed_from_durable | checkpoint_sync
//!            →  start_writer + chain-core  →  serve
//! ```
//!
//! [`boot`] runs that sequence and returns. [`run`] is the only env-config
//! load, then [`BootedNode::serve`]. No 30 s restore grace — that was a
//! two-process synchronisation device.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cc_bootstrap::{
    Bootstrap, LocalReadyHandle, PeerSpec, ServeOptions, ServiceSpec, SignalTrigger,
    TelemetrySettings, serve_with_options,
};
use cc_chain::checkpoint_sync::{
    CheckpointBootstrapConfig, bootstrap_core_from_providers_with_epoch, parse_optional_root,
};
use cc_chain_core::core::{CoreConfig, CoreThread};
use cc_chain_core::engine::DirectEngine;
use cc_chain_core::epoch_context::EpochContextStore;
use cc_chain_core::events::{EventsConfig, EventsHandle};
use cc_chain_core::head::HeadSnapshotStore;
use cc_chain_core::liveness::{
    DEFAULT_ATTESTATION_DUE_BPS, liveness_deadline, run_core_liveness_loop, sample_interval,
};
use cc_chain_core::metrics::ChainMetrics;
use cc_chain_core::seed::{
    DurableSeed, SeedBlock, SeedDaStatus, SeedInstall, seed_from_durable, spawn_core_from_seed,
};
use cc_chain_core::service::ChainServiceImpl;
use cc_config::ServiceConfig;
use cc_proto::chain::chain_service_server::ChainServiceServer;
use cc_storage_core::{OpenOpts, OpenedStore, StorageMetrics, StorageRuntime, durable_set, open};
use cc_types::config::ChainConfig as NetworkChainConfig;
use cc_types::preset::Mainnet;
use cc_types::primitives::Root;
use serde::Deserialize;
use tokio::sync::watch;
use tonic::service::Routes;

/// Process name and config slug (`config/beacon-core.toml`, `CC_BEACON_CORE_*`).
const SERVICE: &str = "beacon-core";

const HEALTH_SERVICE_NAME: &str = "eth.chain.v1.ChainService";

const KNOWN_METHODS: &[&str] = &[
    "/eth.chain.v1.ChainService/GetInfo",
    "/eth.chain.v1.ChainService/ImportBlock",
    "/eth.chain.v1.ChainService/GetHead",
    "/eth.chain.v1.ChainService/SubscribeEvents",
    "/eth.chain.v1.ChainService/ApplyAttestations",
    "/eth.chain.v1.ChainService/IsOptimistic",
    "/eth.chain.v1.ChainService/GetCanonicalRoots",
];

/// Ordered boot phases. [`BootPhase::Open`] is always first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootPhase {
    /// `storage_core::open` returned. No writer / core yet.
    Open,
    /// Durable set loaded (`None` = empty store).
    DurableSet,
    /// Single writer started.
    Writer,
    /// Chain-core seeded (durable or checkpoint). Absent when both empty.
    Chain,
}

/// Inputs for the in-process boot path (tests + [`run`]).
#[derive(Debug, Clone)]
pub struct BootConfig {
    /// On-disk store directory.
    pub data_dir: PathBuf,
    /// Durability token (`immediate` | `paranoid`).
    pub durability: String,
    /// Run store invariants at open (includes `I-node-id`).
    pub check_invariants: bool,
    /// Snapshot ring depth.
    pub snapshot_ring: u64,
    /// Optional GVR for the config digest.
    pub genesis_validators_root: Option<String>,
    /// Node key path for `I-node-id`.
    pub node_key_path: Option<PathBuf>,
    /// Writer is process-fatal on panic (production). Tests set `false`.
    pub writer_process_fatal: bool,
}

impl Default for BootConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("data/storage"),
            durability: "immediate".to_owned(),
            check_invariants: true,
            snapshot_ring: 4,
            genesis_validators_root: None,
            node_key_path: None,
            writer_process_fatal: true,
        }
    }
}

/// Result of [`boot_in_process`]: one handle, one writer, ordered phases.
#[derive(Debug)]
pub struct Booted {
    /// Opened store if the writer was not started (tests that only open).
    pub opened: Option<OpenedStore>,
    /// In-process storage writer (one).
    pub storage: Option<StorageRuntime>,
    /// Ordered phases. First element is always [`BootPhase::Open`] on success.
    pub phases: Vec<BootPhase>,
}

/// Open redb, then start storage-core's writer. No gRPC. No 30 s grace.
///
/// Chain seed/checkpoint is a separate step in [`boot`] so tests can assert
/// that **no** subsystem starts before [`open`] returns.
///
/// S2-A-13: proto-free twin lives in crates/beacon-inproc (one TempDir, one
/// redb; cargo tree without tonic / cc-proto).
pub fn boot_in_process(
    cfg: &BootConfig,
    metrics: StorageMetrics,
    start_writer: bool,
) -> anyhow::Result<Booted> {
    let mut phases = Vec::new();
    let opened = open_and_stamp(cfg)?;
    phases.push(BootPhase::Open);

    let chain = bundled_hoodi_config()?;
    let _durable = durable_set(&opened, &chain)?;
    phases.push(BootPhase::DurableSet);

    if !start_writer {
        return Ok(Booted {
            opened: Some(opened),
            storage: None,
            phases,
        });
    }

    let storage = cc_storage_core::start_writer(opened, metrics, cfg.writer_process_fatal);
    phases.push(BootPhase::Writer);
    Ok(Booted {
        opened: None,
        storage: Some(storage),
        phases,
    })
}

/// Process configuration for [`boot`] (`config/beacon-core.toml`, `CC_BEACON_CORE_*`).
///
/// [`run`] is the only place this is loaded from the environment. Callers — and
/// tests — pass an already-built value so [`boot`] does not read the process env.
#[derive(Debug, Deserialize)]
pub struct BeaconCoreConfig {
    /// Shared bind, peer, and telemetry settings.
    #[serde(flatten)]
    pub service: ServiceConfig,
    /// On-disk store directory.
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
    /// Durability token (`immediate` | `paranoid`).
    #[serde(default = "default_durability")]
    pub durability: String,
    /// Run store invariants at open (includes `I-node-id`).
    #[serde(default = "default_check_invariants")]
    pub check_invariants: bool,
    /// Snapshot ring depth.
    #[serde(default = "default_snapshot_ring")]
    pub snapshot_ring: u64,
    /// Optional GVR for the config digest.
    #[serde(default)]
    pub genesis_validators_root: Option<String>,
    /// Node key path for `I-node-id`.
    #[serde(default = "default_node_key_path")]
    pub node_key_path: PathBuf,
    /// Chain-core resident state cap.
    #[serde(default = "default_max_resident_states")]
    pub max_resident_states: usize,
    /// Chain-core body ring capacity.
    #[serde(default = "default_body_ring_capacity")]
    pub body_ring_capacity: usize,
    /// Event ring event cap.
    #[serde(default = "default_event_ring_events")]
    pub event_ring_events: usize,
    /// Event ring byte cap.
    #[serde(default = "default_event_ring_bytes")]
    pub event_ring_bytes: usize,
    /// Per-subscriber event queue.
    #[serde(default = "default_subscriber_queue_capacity")]
    pub subscriber_queue_capacity: usize,
    /// Checkpoint sync URLs. Empty leaves the core absent on an empty store.
    #[serde(default)]
    pub checkpoint_providers: Vec<String>,
    /// Optional expected checkpoint root.
    #[serde(default)]
    pub checkpoint_root: Option<String>,
    /// Chain config YAML. Absent uses bundled Hoodi when providers are empty.
    #[serde(default)]
    pub network_config: Option<String>,
    /// Engine API transport (flattened beside the service fields).
    #[serde(flatten)]
    pub engine: cc_engine_api::config::EngineTransportConfig,
    /// `MAXIMUM_GOSSIP_CLOCK_DISPARITY` in milliseconds.
    #[serde(default = "default_maximum_gossip_clock_disparity_ms")]
    pub maximum_gossip_clock_disparity_ms: u64,
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("data/storage")
}
fn default_node_key_path() -> PathBuf {
    PathBuf::from("./data/node_key")
}
fn default_durability() -> String {
    "immediate".to_owned()
}
fn default_check_invariants() -> bool {
    true
}
fn default_snapshot_ring() -> u64 {
    4
}
fn default_max_resident_states() -> usize {
    cc_chain_core::residency::DEFAULT_MAX_RESIDENT_STATES
}
fn default_body_ring_capacity() -> usize {
    cc_chain_core::residency::DEFAULT_BODY_RING_CAPACITY
}
fn default_event_ring_events() -> usize {
    cc_chain_core::events::DEFAULT_RING_CAPACITY
}
fn default_event_ring_bytes() -> usize {
    cc_chain_core::events::DEFAULT_RING_BYTES
}
fn default_subscriber_queue_capacity() -> usize {
    cc_chain_core::events::DEFAULT_SUBSCRIBER_QUEUE_CAPACITY
}
fn default_maximum_gossip_clock_disparity_ms() -> u64 {
    u64::try_from(cc_chain_core::tick::DEFAULT_MAXIMUM_GOSSIP_CLOCK_DISPARITY.as_millis())
        .unwrap_or(500)
}

impl BeaconCoreConfig {
    fn service_spec(&self) -> ServiceSpec {
        ServiceSpec {
            name: SERVICE,
            health_service_name: HEALTH_SERVICE_NAME,
            grpc_addr: self.service.grpc_addr,
            metrics_addr: self.service.metrics_addr,
            peers: self
                .service
                .peers
                .iter()
                .map(|(name, uri)| PeerSpec {
                    name: name.clone(),
                    uri: uri.clone(),
                })
                .collect(),
            descriptor_set: cc_proto::FILE_DESCRIPTOR_SET,
            known_methods: KNOWN_METHODS.iter().map(|s| (*s).to_owned()).collect(),
        }
    }
}

fn open_and_stamp(cfg: &BootConfig) -> anyhow::Result<OpenedStore> {
    let opened = open(
        &cfg.data_dir,
        OpenOpts {
            durability: cfg.durability.clone(),
            check_invariants: cfg.check_invariants,
            snapshot_ring: cfg.snapshot_ring,
            genesis_validators_root: cfg.genesis_validators_root.clone(),
            node_key_path: cfg.node_key_path.clone(),
            ..OpenOpts::default()
        },
    )?;
    if let Some(id) = opened.configured_node_id() {
        opened.persist_anchor_node_id(id)?;
    }
    Ok(opened)
}

fn load_network(cfg: &BeaconCoreConfig) -> anyhow::Result<NetworkChainConfig> {
    if let Some(path) = cfg.network_config.as_deref() {
        return NetworkChainConfig::from_yaml_file(path)
            .map_err(|e| anyhow::anyhow!("failed to load network_config {path}: {e}"));
    }
    if !cfg.checkpoint_providers.is_empty() {
        return Err(anyhow::anyhow!(
            "network_config is required when checkpoint_providers is non-empty"
        ));
    }
    bundled_hoodi_config()
}

fn bundled_hoodi_config() -> anyhow::Result<NetworkChainConfig> {
    let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../crates/types/tests/fixtures/hoodi-config.yaml");
    match NetworkChainConfig::from_yaml_file(&fixture) {
        Ok(cfg) => Ok(cfg),
        Err(e) => {
            tracing::warn!(error = %e, "hoodi fixture load failed — trying bundled YAML");
            NetworkChainConfig::from_yaml_str(include_str!(
                "../../../crates/types/tests/fixtures/hoodi-config.yaml"
            ))
            .map_err(|e2| anyhow::anyhow!("bundled hoodi-config.yaml: {e2}"))
        }
    }
}

/// Ensure the I-node-id key file exists (32 raw bytes, same surface as storage).
fn ensure_node_key(path: &std::path::Path) -> anyhow::Result<[u8; 32]> {
    if path.exists() {
        let bytes = std::fs::read(path)
            .map_err(|e| anyhow::anyhow!("node_key_path {}: {e}", path.display()))?;
        if bytes.len() != 32 {
            anyhow::bail!(
                "node_key_path {} has length {}, expected 32",
                path.display(),
                bytes.len()
            );
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        return Ok(arr);
    }
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .map_err(|e| anyhow::anyhow!("node_key_path parent {}: {e}", parent.display()))?;
    }
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).map_err(|e| anyhow::anyhow!("node key generate: {e}"))?;
    std::fs::write(path, bytes)
        .map_err(|e| anyhow::anyhow!("node_key_path write {}: {e}", path.display()))?;
    Ok(bytes)
}

fn map_da(status: cc_storage_core::DurableDaStatus) -> SeedDaStatus {
    match status {
        cc_storage_core::DurableDaStatus::Available => SeedDaStatus::Available,
        cc_storage_core::DurableDaStatus::Deferred => SeedDaStatus::Deferred,
    }
}

fn map_durable(d: cc_storage_core::DurableSet) -> DurableSeed {
    DurableSeed {
        state_ssz: d.state_ssz,
        anchor_block_ssz: d.anchor_block_ssz,
        anchor_block_root: Root::from_array(d.anchor_block_root),
        anchor_block_fork: d.anchor_block_fork,
        blocks: d
            .blocks
            .into_iter()
            .map(|b| SeedBlock {
                ssz: b.ssz,
                fork: b.fork,
                root: b.root,
                da_status: map_da(b.da_status),
            })
            .collect(),
        fork_choice_scalars_ssz: d.fork_choice_scalars_ssz,
        expected_head_root: Root::from_array(d.expected_head_root),
        expected_head_slot: d.expected_head_slot,
    }
}

#[derive(Debug, Default)]
struct CoreJoinOwner {
    thread: Option<CoreThread>,
    liveness_cancel: Option<watch::Sender<bool>>,
    shutting_down: bool,
}

impl CoreJoinOwner {
    fn try_install(&mut self, svc: &ChainServiceImpl, core: CoreThread) -> Option<CoreThread> {
        if self.shutting_down {
            return Some(core);
        }
        svc.install_core(core.handle.clone());
        self.thread = Some(core);
        None
    }

    fn take_for_shutdown(&mut self) -> Option<CoreThread> {
        self.shutting_down = true;
        if let Some(tx) = self.liveness_cancel.take() {
            let _ = tx.send(true);
        }
        self.thread.take()
    }

    fn spawn_liveness(
        &mut self,
        local_ready: LocalReadyHandle,
        metrics: ChainMetrics,
        deadline: Duration,
        interval: Duration,
    ) {
        let Some(core) = self.thread.as_ref() else {
            return;
        };
        if self.shutting_down {
            return;
        }
        let (tx, rx) = watch::channel(false);
        if let Some(prev) = self.liveness_cancel.replace(tx) {
            let _ = prev.send(true);
        }
        let handle = core.handle.clone();
        tokio::spawn(async move {
            run_core_liveness_loop(handle, local_ready, deadline, interval, rx, Some(metrics))
                .await;
        });
    }
}

/// Production sequence stopped before the listen loop.
///
/// [`BootedNode::serve`] binds. Holding this is what lets a test enter [`boot`]
/// and return. `core` keeps an empty-store engine alive until serve returns —
/// that local used to drop only after `serve_with_options`.
#[allow(missing_debug_implementations)]
pub struct BootedNode {
    bootstrap: Bootstrap,
    spec: ServiceSpec,
    routes: Routes,
    options: ServeOptions,
    core: Option<CoreConfig>,
    storage: StorageRuntime,
}

impl BootedNode {
    /// Bind gRPC and metrics and block until SIGTERM/SIGINT.
    pub async fn serve(self) -> anyhow::Result<()> {
        let Self {
            bootstrap,
            spec,
            routes,
            options,
            core: _core,
            storage: _storage,
        } = self;
        serve_with_options(bootstrap, spec, routes, options, SignalTrigger::UnixSignals).await?;
        Ok(())
    }
}

/// Load env config and run the production sequence. The only env-config load.
pub async fn run() -> anyhow::Result<()> {
    boot(cc_config::load(SERVICE)?).await?.serve().await
}

/// Production sequence: JWT abort-before-bind, open redb, then start subsystems.
///
/// Does not read the process environment and does not listen.
pub async fn boot(cfg: BeaconCoreConfig) -> anyhow::Result<BootedNode> {
    let network = load_network(&cfg)?;
    let prepared =
        cc_engine_api::EngineApi::prepare_with_chain_config(&cfg.engine, network.clone())
            .map_err(|e| anyhow::anyhow!("{e}"))?;

    // Open redb before telemetry bind / writer / chain-core.
    // I-node-id: key file is the 32-byte identity surface (ADR-P4-13).
    let _node_key = ensure_node_key(&cfg.node_key_path)?;
    let boot_cfg = BootConfig {
        data_dir: cfg.data_dir.clone(),
        durability: cfg.durability.clone(),
        check_invariants: cfg.check_invariants,
        snapshot_ring: cfg.snapshot_ring,
        genesis_validators_root: cfg.genesis_validators_root.clone(),
        node_key_path: Some(cfg.node_key_path.clone()),
        writer_process_fatal: true,
    };
    let opened = open_and_stamp(&boot_cfg)?;
    let durable = durable_set(&opened, &network)?;

    let mut bs = cc_bootstrap::init(SERVICE, TelemetrySettings::from(&cfg.service))?;
    let chain_metrics = ChainMetrics::register(&mut bs.registry);
    let storage_metrics = StorageMetrics::register(&mut bs.registry);

    let storage = cc_storage_core::start_writer(opened, storage_metrics, true);
    tracing::info!(
        writers = storage.writer_count(),
        "storage-core writer started (one handle)"
    );
    let archive: cc_chain_core::ArchiveWriteHandle = std::sync::Arc::new(storage.archive());

    let events = EventsHandle::spawn(EventsConfig {
        ring_capacity: cfg.event_ring_events,
        ring_bytes: cfg.event_ring_bytes,
        subscriber_queue_capacity: cfg.subscriber_queue_capacity,
        session_id: None,
    });
    let head = HeadSnapshotStore::new();
    let epoch = EpochContextStore::new();
    let api = prepared.finish(None).map_err(|e| anyhow::anyhow!("{e}"))?;
    let engine = Arc::new(DirectEngine::new(api, cfg.engine.transport_timeouts()));
    let core_cfg = CoreConfig {
        max_resident_states: cfg.max_resident_states,
        body_ring_capacity: cfg.body_ring_capacity,
        engine: Some(engine),
        slot_tick_enabled: true,
        maximum_gossip_clock_disparity: Duration::from_millis(
            cfg.maximum_gossip_clock_disparity_ms,
        ),
        archive: Some(archive),
        ..CoreConfig::default()
    };

    let svc = ChainServiceImpl::with_epoch(
        None,
        head.clone(),
        epoch.clone(),
        events.clone(),
        chain_metrics.clone(),
    );
    let core_owner: Arc<Mutex<CoreJoinOwner>> = Arc::new(Mutex::new(CoreJoinOwner::default()));

    let core = match durable {
        Some(d) => {
            let applied = seed_from_durable::<Mainnet>(
                map_durable(d),
                network.clone(),
                core_cfg
                    .engine
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("in-process engine not configured"))?,
                chain_metrics.clone(),
            )
            .await
            .map_err(|e| anyhow::anyhow!("seed_from_durable: {e}"))?;
            let install = spawn_core_from_seed(
                applied,
                network.clone(),
                head.clone(),
                epoch.clone(),
                events.event_sender(),
                chain_metrics.clone(),
                core_cfg.clone(),
            );
            install_core(&svc, &core_owner, install)?;
            Some(core_cfg)
        }
        None if !cfg.checkpoint_providers.is_empty() => {
            let expected = parse_optional_root(cfg.checkpoint_root.as_deref())
                .map_err(|e| anyhow::anyhow!("{e}"))?;
            let boot_cfg = CheckpointBootstrapConfig {
                providers: cfg.checkpoint_providers.clone(),
                expected_checkpoint_root: expected,
                chain_config: network.clone(),
                connect_timeout: cc_chain::PROVIDER_CONNECT_TIMEOUT,
                total_timeout: cc_chain::PROVIDER_TOTAL_TIMEOUT,
                network_retries: cc_chain::NETWORK_RETRIES,
                triple_attempts: cc_chain::TRIPLE_ATTEMPTS,
            };
            let (core, summary) = bootstrap_core_from_providers_with_epoch::<Mainnet>(
                &boot_cfg,
                head,
                epoch,
                events.event_sender(),
                chain_metrics.clone(),
                core_cfg,
            )
            .await
            .map_err(|e| anyhow::anyhow!("checkpoint_sync: {e}"))?;
            tracing::info!(
                provider = %summary.provider,
                slot = summary.slot,
                "checkpoint fallback complete"
            );
            install_core(
                &svc,
                &core_owner,
                SeedInstall {
                    core,
                    head_root: summary.block_root,
                    head_slot: summary.slot,
                    matched_expected: true,
                },
            )?;
            None
        }
        None => {
            tracing::info!(
                "empty store and no checkpoint_providers; core remains absent (NOT_BOOTSTRAPPED)"
            );
            Some(core_cfg)
        }
    };

    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let core_owner_live = Arc::clone(&core_owner);
    let metrics_live = chain_metrics.clone();
    let seconds_per_slot = network.seconds_per_slot;
    tokio::spawn(async move {
        let local_ready: LocalReadyHandle = match ready_rx.await {
            Ok(g) => g,
            Err(_) => return,
        };
        local_ready.mark_ready().await;
        let slot_ms = seconds_per_slot.max(1).saturating_mul(1_000);
        let deadline = liveness_deadline(DEFAULT_ATTESTATION_DUE_BPS, slot_ms);
        let interval = sample_interval(slot_ms);
        let mut guard = core_owner_live.lock().unwrap_or_else(|p| p.into_inner());
        guard.spawn_liveness(local_ready, metrics_live, deadline, interval);
    });

    let core_owner_shutdown = Arc::clone(&core_owner);
    let options = ServeOptions {
        require_local_ready: true,
        local_ready_tx: Some(ready_tx),
        on_pre_drain: Some(Box::new(move || {
            Box::pin(async move {
                let core = {
                    let mut guard = core_owner_shutdown
                        .lock()
                        .unwrap_or_else(|p| p.into_inner());
                    guard.take_for_shutdown()
                };
                if let Some(core) = core {
                    core.shutdown_and_join().await;
                }
            })
        })),
    };

    let routes = Routes::default().add_service(ChainServiceServer::new(svc));
    let spec = cfg.service_spec();
    Ok(BootedNode {
        bootstrap: bs,
        spec,
        routes,
        options,
        core,
        storage,
    })
}

fn install_core(
    svc: &ChainServiceImpl,
    owner: &Arc<Mutex<CoreJoinOwner>>,
    install: SeedInstall,
) -> anyhow::Result<()> {
    let orphan = {
        let mut guard = owner.lock().unwrap_or_else(|p| p.into_inner());
        guard.try_install(svc, install.core)
    };
    if orphan.is_some() {
        anyhow::bail!("pre-drain already active; late core not installed");
    }
    Ok(())
}
