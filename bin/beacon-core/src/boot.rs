//! Boot sequence ([ARCH] §4.2).
//!
//! ```text
//! load_or_create node key
//!   → open → peek_node_id → pair
//!   → classify
//!        Incomplete → refuse, naming the DurableItem (no subsystem)
//!        else → start_writer
//!             Uninitialized: AnchorSource → verify_anchor → commit_anchor → seed
//!             Complete: resume → seed_from_durable → set_head
//!   → serve exit → drain_and_shutdown
//! ```
//!
//! [`boot`] runs that sequence and returns. [`run`] is the only env-config
//! load, then [`BootedNode::serve`]. No 30 s restore grace — that was a
//! two-process synchronisation device.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cc_bootstrap::{
    Bootstrap, LocalReadyHandle, PeerSpec, ServeOptions, ServiceSpec, SignalTrigger,
    TelemetrySettings, serve_with_options,
};
use cc_chain::checkpoint_sync::{
    AnchorSource, CheckpointBootstrapConfig, CheckpointClient, CheckpointProvider, VerifiedAnchor,
    parse_optional_root, verify_anchor,
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
use cc_seam::{Bytes, HeadCause, HeadChange};
use cc_storage_core::{
    ItemAssessment, NodeIdExpectation, OpenOpts, OpenedStore, RestartState, StorageMetrics,
    StorageRuntime, classify, durable_set, load_or_create_node_key, open,
};
use cc_types::config::ChainConfig as NetworkChainConfig;
use cc_types::config::PresetName;
use cc_types::preset::{Mainnet, Minimal, Preset};
use cc_types::primitives::{Epoch, Root};
use cc_types::{BeaconState, ForkName, SignedBeaconBlock};
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

/// SSZ layout of fork-choice scalars: time, proposer-boost root, four
/// checkpoints, head root, head slot. The head root starts at byte 200.
const SCALAR_HEAD_ROOT_OFFSET: usize = 8 + 32 + (4 * 40);
const SCALAR_HEAD_SLOT_OFFSET: usize = SCALAR_HEAD_ROOT_OFFSET + 32;

const _: () =
    assert!(SCALAR_HEAD_SLOT_OFFSET + 8 == cc_chain_core::import::FORK_CHOICE_SCALARS_SSZ_LEN);

/// One observed step of the Complete arm. Order is the assertion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComposerStep {
    /// `seed_from_durable` returned.
    SeedFromDurable,
    /// `set_head` returned after that seed.
    SetHead,
}

static COMPOSER_STEPS: Mutex<Vec<ComposerStep>> = Mutex::new(Vec::new());
static SERVE_PRE_DRAIN_DRAINED: AtomicBool = AtomicBool::new(false);
static SERVE_EXIT_DRAINED: AtomicBool = AtomicBool::new(false);

/// Steps recorded by the Complete arm since the last [`boot`].
#[must_use]
pub fn composer_steps() -> Vec<ComposerStep> {
    COMPOSER_STEPS
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .clone()
}

fn clear_composer_steps() {
    COMPOSER_STEPS
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .clear();
}

fn record_composer_step(step: ComposerStep) {
    COMPOSER_STEPS
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .push(step);
}

fn clear_serve_drain_flags() {
    SERVE_PRE_DRAIN_DRAINED.store(false, Ordering::SeqCst);
    SERVE_EXIT_DRAINED.store(false, Ordering::SeqCst);
}

/// `true` after the pre-drain hook's mailbox drain returned `Ok`.
#[must_use]
pub fn serve_pre_drain_drained() -> bool {
    SERVE_PRE_DRAIN_DRAINED.load(Ordering::SeqCst)
}

/// `true` after [`BootedNode::serve`]'s exit drain returned `Ok`.
#[must_use]
pub fn serve_exit_drained() -> bool {
    SERVE_EXIT_DRAINED.load(Ordering::SeqCst)
}

/// Post-pair restart arm. `Incomplete` has already returned.
enum RestartArm {
    Uninitialized,
    Complete,
}

/// Ordered boot phases. [`BootPhase::Open`] is always first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootPhase {
    /// `storage_core::open` returned. `I-node-id` is still pending.
    Open,
    /// `I-node-id` compared. Nothing starts between [`Open`](Self::Open) and this.
    Pair,
    /// Durable payload loaded. `Incomplete` is an error, not an empty store.
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
    let chain = bundled_hoodi_config()?;
    let node_id = NodeIdExpectation::from_configured_path(cfg.node_key_path.as_deref())
        .map_err(|e| anyhow::anyhow!("node_key_path: {e}"))?;
    let mut pending = open(
        &cfg.data_dir,
        OpenOpts {
            durability: cfg.durability.clone(),
            check_invariants: cfg.check_invariants,
            snapshot_ring: cfg.snapshot_ring,
            genesis_validators_root: cfg.genesis_validators_root.clone(),
            node_id,
            chain: Some(chain.clone()),
            ..OpenOpts::default()
        },
    )?;
    phases.push(BootPhase::Open);
    let _peek = pending.peek_node_id()?;
    let opened = pending.pair(node_id)?;
    phases.push(BootPhase::Pair);

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
    /// Checkpoint sync URLs. Empty leaves the core absent on an empty store
    /// unless [`Self::checkpoint_provider`] or [`Self::genesis_anchor`] is set.
    #[serde(default)]
    pub checkpoint_providers: Vec<String>,
    /// Injected checkpoint double. `run` leaves this empty and uses
    /// [`CheckpointClient`] when [`Self::checkpoint_providers`] is non-empty.
    #[serde(skip)]
    pub checkpoint_provider: Option<CheckpointProviderSlot>,
    /// Local genesis SSZ pair. Mutually exclusive with a checkpoint source.
    #[serde(skip)]
    pub genesis_anchor: Option<GenesisAnchorBytes>,
    /// Chain config override. `run` leaves this empty and loads YAML or Hoodi.
    #[serde(skip)]
    pub chain_config: Option<NetworkChainConfig>,
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

/// In-process [`CheckpointProvider`]. Not serialized; `run` never sets it.
pub struct CheckpointProviderSlot {
    provider: Arc<dyn CheckpointProvider>,
}

impl std::fmt::Debug for CheckpointProviderSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CheckpointProviderSlot(..)")
    }
}

impl CheckpointProviderSlot {
    /// Wrap a provider double or client.
    #[must_use]
    pub fn new(provider: Arc<dyn CheckpointProvider>) -> Self {
        Self { provider }
    }

    fn provider(&self) -> Arc<dyn CheckpointProvider> {
        Arc::clone(&self.provider)
    }
}

/// Genesis block and state SSZ for [`AnchorSource::Genesis`].
///
/// Parent stays the block's parent (`Root::ZERO` for genesis). This path does
/// not remap a zero parent onto the block root.
#[derive(Clone)]
pub struct GenesisAnchorBytes {
    /// `BeaconState` SSZ (Fulu).
    pub state_ssz: Vec<u8>,
    /// `SignedBeaconBlock` SSZ (Fulu).
    pub block_ssz: Vec<u8>,
    /// Optional operator block root. Same check as checkpoint sync.
    pub expected_block_root: Option<Root>,
}

impl std::fmt::Debug for GenesisAnchorBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenesisAnchorBytes")
            .field("state_ssz_len", &self.state_ssz.len())
            .field("block_ssz_len", &self.block_ssz.len())
            .field("expected_block_root", &self.expected_block_root)
            .finish()
    }
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

fn load_network(cfg: &BeaconCoreConfig) -> anyhow::Result<NetworkChainConfig> {
    if let Some(chain) = cfg.chain_config.clone() {
        return Ok(chain);
    }
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
    storage: Arc<StorageRuntime>,
    chain: ChainServiceImpl,
}

impl BootedNode {
    /// Bind gRPC and metrics and block until SIGTERM/SIGINT.
    ///
    /// SIGTERM runs the pre-drain hook (core join, then
    /// [`StorageRuntime::drain_and_shutdown`]) before the listener returns.
    /// This method awaits that drain again on the way out. The second call
    /// is `Ok` when the mailbox drain already finished.
    pub async fn serve(self) -> anyhow::Result<()> {
        clear_serve_drain_flags();
        let Self {
            bootstrap,
            spec,
            routes,
            options,
            core: _core,
            storage,
            chain: _chain,
        } = self;
        let served =
            serve_with_options(bootstrap, spec, routes, options, SignalTrigger::UnixSignals).await;
        storage.drain_and_shutdown().await?;
        SERVE_EXIT_DRAINED.store(true, Ordering::SeqCst);
        served?;
        Ok(())
    }

    /// Chain service installed by [`boot`]. Clone shares the core slot.
    #[must_use]
    pub fn chain(&self) -> &ChainServiceImpl {
        &self.chain
    }

    /// Archive handle on the store [`boot`] opened. Tests use this for the
    /// second `commit_anchor`, which must not be a second composer call.
    #[must_use]
    pub fn archive(&self) -> cc_storage_core::ArchiveWriter {
        self.storage.archive()
    }

    /// Store head, cursor, and canonical rows. No fcU or event spy.
    pub fn durable_frontier(
        &self,
        canonical_slots: &[u64],
    ) -> anyhow::Result<cc_storage_core::DurableFrontier> {
        cc_storage_core::durable_frontier(self.storage.engine(), canonical_slots)
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
    match network.preset_base {
        PresetName::Mainnet => boot_with_preset::<Mainnet>(cfg, network).await,
        PresetName::Minimal => boot_with_preset::<Minimal>(cfg, network).await,
    }
}

async fn boot_with_preset<P: Preset + 'static>(
    cfg: BeaconCoreConfig,
    network: NetworkChainConfig,
) -> anyhow::Result<BootedNode> {
    let prepared =
        cc_engine_api::EngineApi::prepare_with_chain_config(&cfg.engine, network.clone())
            .map_err(|e| anyhow::anyhow!("{e}"))?;

    // Key first, then open. Schema and digest gates run inside open.
    // I-node-id runs at pair, before telemetry, the writer, or chain-core.
    // Pairing stays on the legacy root. The fingerprint is not compared.
    let loaded = load_or_create_node_key(&cfg.node_key_path)?;
    let _fingerprint = loaded.fingerprint();
    let node_id = NodeIdExpectation::Present(loaded.legacy());
    let mut pending = open(
        &cfg.data_dir,
        OpenOpts {
            durability: cfg.durability.clone(),
            check_invariants: cfg.check_invariants,
            snapshot_ring: cfg.snapshot_ring,
            genesis_validators_root: cfg.genesis_validators_root.clone(),
            node_id,
            chain: Some(network.clone()),
            ..OpenOpts::default()
        },
    )?;
    let _peek = pending.peek_node_id()?;
    let opened = pending.pair(node_id)?;
    clear_composer_steps();
    // Incomplete refuses before telemetry, the writer, or chain-core.
    let arm = match classify(opened.engine()).map_err(|e| anyhow::anyhow!("classify: {e}"))? {
        RestartState::Uninitialized => RestartArm::Uninitialized,
        RestartState::Complete => RestartArm::Complete,
        RestartState::Incomplete(assessment) => {
            anyhow::bail!("restart incomplete: {}", restart_detail(&assessment));
        }
    };

    let mut bs = cc_bootstrap::init(SERVICE, TelemetrySettings::from(&cfg.service))?;
    let chain_metrics = ChainMetrics::register(&mut bs.registry);
    let storage_metrics = StorageMetrics::register(&mut bs.registry);

    let storage = Arc::new(cc_storage_core::start_writer(opened, storage_metrics, true));
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
        archive: Some(Arc::clone(&archive)),
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

    let core = match arm {
        RestartArm::Uninitialized => {
            // Empty store: one verify, one commit, then the durable seed.
            // Checkpoint and genesis are the two `AnchorSource` arms; the call
            // below is the only non-test `commit_anchor` call site.
            let seeded = if let Some(source) = anchor_source::<P>(&cfg, &network)? {
                let verified = verify_anchor(source, &chain_metrics)
                    .await
                    .map_err(|e| anyhow::anyhow!("verify_anchor: {e}"))?;
                archive
                    .commit_anchor(verified.trusted.clone())
                    .await
                    .map_err(|e| anyhow::anyhow!("commit_anchor: {e}"))?;
                tracing::info!(
                    slot = verified.slot,
                    root = %verified.block_root,
                    kind = ?verified.kind,
                    "anchor committed"
                );
                Some(durable_set_from_verified::<P>(&verified, &network))
            } else {
                None
            };
            if let Some(plan) = seeded {
                let applied = seed_from_durable::<P>(
                    map_durable(plan),
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
            } else {
                tracing::info!(
                    "empty store and no anchor source; core remains absent (NOT_BOOTSTRAPPED)"
                );
            }
            Some(core_cfg)
        }
        RestartArm::Complete => {
            // [ARCH] §4.6.1: the replay window ends at `scalars.head_slot`.
            // Restart drops non-canonical branches above the selected head.
            // Scalars carry no vote table and no proposer-boost window, so
            // the recomputed head may differ. `set_head` writes that head
            // after the seed. Restart does not preserve the previous head.
            let Some(plan) = storage.resume(&network).await? else {
                anyhow::bail!("complete store resumed uninitialized");
            };
            let scalar_bytes = plan.fork_choice_scalars_ssz.clone();
            let applied = seed_from_durable::<P>(
                map_durable(plan),
                network.clone(),
                core_cfg
                    .engine
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("in-process engine not configured"))?,
                chain_metrics.clone(),
            )
            .await
            .map_err(|e| anyhow::anyhow!("seed_from_durable: {e}"))?;
            record_composer_step(ComposerStep::SeedFromDurable);
            let stamped =
                stamp_recomputed_head(scalar_bytes, &applied.head_root, applied.head_slot)?;
            archive
                .set_head(
                    HeadChange {
                        head_root: *applied.head_root.as_array(),
                        head_slot: applied.head_slot,
                        cause: HeadCause::Import,
                    },
                    Bytes::from(stamped),
                )
                .await
                .map_err(|e| anyhow::anyhow!("set_head: {e}"))?;
            record_composer_step(ComposerStep::SetHead);
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
    let storage_for_drain = Arc::clone(&storage);
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
                match storage_for_drain.drain_and_shutdown().await {
                    Ok(()) => SERVE_PRE_DRAIN_DRAINED.store(true, Ordering::SeqCst),
                    Err(err) => {
                        tracing::error!(
                            error = %err,
                            "pre-drain storage mailbox drain failed"
                        );
                    }
                }
            })
        })),
    };

    let chain = svc.clone();
    let routes = Routes::default().add_service(ChainServiceServer::new(svc));
    let spec = cfg.service_spec();
    Ok(BootedNode {
        bootstrap: bs,
        spec,
        routes,
        options,
        core,
        storage,
        chain,
    })
}

/// Checkpoint (injected double or operator URLs) or local genesis. One of them.
fn anchor_source<P: Preset>(
    cfg: &BeaconCoreConfig,
    network: &NetworkChainConfig,
) -> anyhow::Result<Option<AnchorSource<P>>> {
    let checkpoint = cfg.checkpoint_provider.is_some() || !cfg.checkpoint_providers.is_empty();
    if checkpoint && cfg.genesis_anchor.is_some() {
        anyhow::bail!("checkpoint source and genesis anchor are mutually exclusive");
    }
    if let Some(genesis) = cfg.genesis_anchor.as_ref() {
        let state = BeaconState::<P>::from_ssz_bytes_hydrated(ForkName::Fulu, &genesis.state_ssz)
            .map_err(|e| anyhow::anyhow!("genesis state SSZ: {e:?}"))?;
        let signed_block =
            SignedBeaconBlock::<P>::from_ssz_bytes_with(ForkName::Fulu, &genesis.block_ssz)
                .map_err(|e| anyhow::anyhow!("genesis block SSZ: {e:?}"))?;
        return Ok(Some(AnchorSource::Genesis {
            state: Box::new(state),
            signed_block: Box::new(signed_block),
            chain_config: network.clone(),
            expected_block_root: genesis.expected_block_root,
        }));
    }
    if !checkpoint {
        return Ok(None);
    }
    // Fetch validates every base even when the bytes come from a double.
    let providers = if cfg.checkpoint_providers.is_empty() {
        vec!["http://127.0.0.1:9".to_owned()]
    } else {
        cfg.checkpoint_providers.clone()
    };
    let provider: Arc<dyn CheckpointProvider> = match cfg.checkpoint_provider.as_ref() {
        Some(slot) => slot.provider(),
        None => Arc::new(
            CheckpointClient::new(
                cc_chain::PROVIDER_CONNECT_TIMEOUT,
                cc_chain::PROVIDER_TOTAL_TIMEOUT,
            )
            .map_err(|e| anyhow::anyhow!("checkpoint client: {e}"))?,
        ),
    };
    let expected =
        parse_optional_root(cfg.checkpoint_root.as_deref()).map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(Some(AnchorSource::Checkpoint {
        config: CheckpointBootstrapConfig {
            providers,
            expected_checkpoint_root: expected,
            chain_config: network.clone(),
            connect_timeout: cc_chain::PROVIDER_CONNECT_TIMEOUT,
            total_timeout: cc_chain::PROVIDER_TOTAL_TIMEOUT,
            network_retries: cc_chain::NETWORK_RETRIES,
            triple_attempts: cc_chain::TRIPLE_ATTEMPTS,
        },
        provider,
    }))
}

fn durable_set_from_verified<P: Preset>(
    verified: &VerifiedAnchor<P>,
    network: &NetworkChainConfig,
) -> cc_storage_core::DurableSet {
    let epoch = Epoch::new(verified.slot / P::SLOTS_PER_EPOCH.max(1));
    cc_storage_core::DurableSet {
        state_ssz: verified.trusted.state_ssz.to_vec(),
        anchor_block_ssz: verified.trusted.block_ssz.to_vec(),
        anchor_block_root: verified.trusted.block_root,
        anchor_block_fork: network.fork_name_at_epoch(epoch) as u32,
        blocks: Vec::new(),
        fork_choice_scalars_ssz: verified.trusted.scalars.to_vec(),
        expected_head_root: verified.trusted.block_root,
        expected_head_slot: verified.slot,
    }
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

fn restart_detail(assessment: &ItemAssessment) -> String {
    match assessment {
        ItemAssessment::NamedFailure { detail, .. }
        | ItemAssessment::Degradation { detail, .. } => detail.clone(),
        ItemAssessment::Present => "restart classification incomplete".to_owned(),
    }
}

/// Stamp the recomputed head into a copy of the durable scalar blob.
fn stamp_recomputed_head(
    mut scalars: Vec<u8>,
    head_root: &Root,
    head_slot: u64,
) -> anyhow::Result<Vec<u8>> {
    let len = cc_chain_core::import::FORK_CHOICE_SCALARS_SSZ_LEN;
    if scalars.len() != len {
        anyhow::bail!("fork-choice scalars len {} want {len}", scalars.len());
    }
    let root_end = SCALAR_HEAD_ROOT_OFFSET + 32;
    scalars[SCALAR_HEAD_ROOT_OFFSET..root_end].copy_from_slice(head_root.as_array());
    scalars[SCALAR_HEAD_SLOT_OFFSET..SCALAR_HEAD_SLOT_OFFSET + 8]
        .copy_from_slice(&head_slot.to_le_bytes());
    Ok(scalars)
}
