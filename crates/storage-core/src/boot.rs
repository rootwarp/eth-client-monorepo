//! Storage process host (S2-B-03). Former `services/storage/src/main.rs`.
//!
//! Architecture §4.1, CC-01b / CC-4Ca / CC-44b / CC-4F.
//! Health peer: `chain` (§6.3).
//!
//! CC-4Ca: §10.1 metric families are registered between `init` and `serve`.
//! CC-44b: single writer spawn (append-only here).
//! S2-A-09: no write-behind / SubscribeEvents consumer. The ring is
//! API/observer only; columns go `ArchiveWrite`.
//! CC-4F / CC-4I: ten-RPC `StorageService` serve pool (materialise-and-drop).
//!
//! `services/storage` stays a thin shim so the previous topology remains
//! runnable (`[ARCH]` §9.1).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::archive_write::ArchiveWriter;
use crate::durable_set;
use crate::metrics::{self, StorageMetrics};
use crate::migrate::{self, MigrationConfig, Migrator};
use crate::prune::{
    DEFAULT_DISK_ALARM_BYTES, DEFAULT_PRUNE_BLOCKS_EPOCHS, DEFAULT_PRUNE_COLUMNS_EPOCHS,
    DEFAULT_PRUNE_MARGIN_EPOCHS, PruneConfig, Pruner,
    chunk::{DEFAULT_PRUNE_CHUNK_KEYS, DEFAULT_PRUNE_DEADLINE},
    columns::DEFAULT_COLUMNS_RETENTION_EPOCHS,
    genesis_time_from_fixture, spawn_prune_task,
};
use crate::replay::{self, ReplayConfig, ReplayDriver, spawn_replay_task};
use crate::resume;
use crate::serve::{self, ServeConfig, StorageServer, UnaryPermitService};
use crate::writer::{self, WriterBounds, WriterFaults, WriterHandle, spawn_writer};
use cc_bootstrap::{
    PeerSpec, ServeOptions, ServiceSpec, SignalTrigger, TelemetrySettings, serve_with_options,
};
use cc_config::ServiceConfig;
use cc_proto::storage::storage_service_server::StorageServiceServer;
use cc_store::{BlockServeWindowCfg, SplitLock, Store, check_min_epochs_for_block_requests};
use cc_types::ChainConfig;
use serde::Deserialize;
use tokio::sync::watch;
use tonic::service::Routes;

/// Process name and config slug (`config/storage.toml`, `CC_STORAGE_*`).
const SERVICE: &str = "storage";

/// Fully-qualified gRPC service name for self-only health.
const HEALTH_SERVICE_NAME: &str = "eth.storage.v1.StorageService";

/// Full gRPC paths for known methods (metrics label normalisation).
const KNOWN_METHODS: &[&str] = &[
    "/eth.storage.v1.StorageService/GetInfo",
    "/eth.storage.v1.StorageService/GetBlocksByRange",
    "/eth.storage.v1.StorageService/GetBlocksByRoot",
    "/eth.storage.v1.StorageService/GetColumnsByRange",
    "/eth.storage.v1.StorageService/GetColumnsByRoot",
    "/eth.storage.v1.StorageService/PutBackfillBatch",
    "/eth.storage.v1.StorageService/WatchServeWindow",
    "/eth.storage.v1.StorageService/GetHistoricalBlock",
    "/eth.storage.v1.StorageService/GetSnapshotState",
    "/eth.storage.v1.StorageService/GetFinalizedCheckpointHistory",
];

/// Per-service config: shared [`ServiceConfig`] plus storage-only fields (D-1).
#[derive(Debug, Deserialize)]
struct StorageConfig {
    #[serde(flatten)]
    service: ServiceConfig,
    /// §2.7 / CC-4H: run store invariants at open and after migration/prune.
    ///
    /// Devnet/local default in `config/storage.toml` is `true`. Hoodi soak should
    /// set `CC_STORAGE_CHECK_INVARIANTS=false` (no multi-file profiles yet).
    #[serde(default = "default_check_invariants")]
    check_invariants: bool,
    /// Per-check row cap for §2.7 scans at `Store::open`.
    ///
    /// Named default [`cc_store::DEFAULT_MAX_OPEN_SCAN_ROWS`]. Exceeding it is
    /// `StoreError::Limit`. Override: `CC_STORAGE_MAX_OPEN_SCAN_ROWS`.
    #[serde(default = "default_max_open_scan_rows")]
    max_open_scan_rows: u64,
    /// On-disk store directory (CC-44b).
    #[serde(default = "default_data_dir")]
    data_dir: PathBuf,
    /// Engine durability token (`immediate` | `paranoid`).
    #[serde(default = "default_durability")]
    durability: String,
    /// Network identity for the CC-4D dangerous-knob guard / config digest.
    ///
    /// Required when [`Self::retention_override`] or [`StorageDebug::crash_point`]
    /// is set; must be neither Hoodi's nor mainnet's.
    #[serde(default)]
    genesis_validators_root: Option<String>,
    /// Compressed-retention venue override (CC-4D / Architecture §10.4).
    #[serde(default)]
    retention_override: Option<RetentionOverride>,
    /// Fault-injection knobs (CC-4D / Architecture §2.5). Config, not env.
    #[serde(default)]
    debug: StorageDebug,
    // ── CC-46a prune ────────────────────────────────────────────────────────
    /// Column prune cadence in epochs (default **32**).
    #[serde(default = "default_prune_columns_epochs")]
    prune_columns_epochs: u64,
    /// Block (+ state_roots) prune cadence in epochs (default **256**).
    #[serde(default = "default_prune_blocks_epochs")]
    prune_blocks_epochs: u64,
    /// Margin epochs baked into both watermarks (default **1**).
    #[serde(default = "default_prune_margin_epochs")]
    prune_margin_epochs: u64,
    /// Keys per P2 prune chunk (default **512**). CC-46b / §7.4.
    #[serde(default = "default_prune_chunk_keys")]
    prune_chunk_keys: usize,
    /// Prune-pass wall-clock deadline in milliseconds (default **2000**). CC-46b.
    #[serde(default = "default_prune_deadline_ms")]
    prune_deadline_ms: u64,
    /// Disk alarm threshold in bytes (default **96 GiB**). Alarm only, never a trigger.
    #[serde(default = "default_disk_alarm_bytes")]
    disk_alarm_bytes: u64,
    /// Wall-clock genesis unix seconds for prune epoch ticks (CC-46a).
    ///
    /// Hoodi = `MIN_GENESIS_TIME + GENESIS_DELAY` (1742213400). When absent,
    /// resolved from the network fixture, then from a store snapshot's
    /// `BeaconState.genesis_time`. Override: `CC_STORAGE_GENESIS_TIME`.
    #[serde(default)]
    genesis_time: Option<u64>,
    /// Consensus-specs network YAML supplying the CC-4A live scalars
    /// (`MIN_VALIDATOR_WITHDRAWABILITY_DELAY`, `CHURN_LIMIT_QUOTIENT`, and
    /// optional vestigial `MIN_EPOCHS_FOR_BLOCK_REQUESTS`).
    ///
    /// Required to compute the block retention floor when
    /// [`Self::retention_override`] is unset. A missing or unreadable path is a
    /// startup error — a silent 0 floor is maximally destructive for retention
    /// (P1-A/5). Override: `CC_STORAGE_NETWORK_CONFIG`.
    #[serde(default)]
    network_config: Option<PathBuf>,
    // ── CC-44b writer mailbox ───────────────────────────────────────────────
    /// Historical write-behind flush knobs. Loaded so committed toml stays
    /// valid; S2-A-09 deleted the SubscribeEvents consumer they configured.
    #[serde(default = "default_commit_slots")]
    commit_slots: u64,
    #[serde(default = "default_commit_max_events")]
    commit_max_events: usize,
    #[serde(default = "default_commit_max_latency_ms")]
    commit_max_latency_ms: u64,
    /// P0 channel bound (slots' commit units). Default 32; on full **block**.
    #[serde(default = "default_writer_p0_bound")]
    writer_p0_bound: usize,
    /// P1 channel bound. Default 64; on full **block**.
    #[serde(default = "default_writer_p1_bound")]
    writer_p1_bound: usize,
    /// P2 channel bound. Default 256; on full **drop newest**.
    #[serde(default = "default_writer_p2_bound")]
    writer_p2_bound: usize,
    /// When false, skip opening the store / spawning the writer
    /// (Phase 0 compose without a data volume). Default **true**.
    #[serde(default = "default_enable_write_path")]
    enable_write_path: bool,
    /// CC-41: hot/cold migration cadence in epochs (default **1**).
    #[serde(default = "default_epochs_per_migration")]
    epochs_per_migration: u64,
    /// CC-42: snapshot cadence in epochs (default **32**).
    #[serde(default = "default_snapshot_epochs")]
    snapshot_epochs: u64,
    /// CC-42: snapshot ring depth (default **4**).
    #[serde(default = "default_snapshot_ring")]
    snapshot_ring: u64,
    // ── CC-4F serve path ────────────────────────────────────────────────────
    /// Per-response materialise buffer (default 64 MiB).
    #[serde(default = "default_serve_buffer_bytes")]
    serve_buffer_bytes: u64,
    /// Admission semaphore permits (default 4).
    #[serde(default = "default_serve_permits")]
    serve_permits: usize,
    /// Max wait for a permit in milliseconds (default 2000).
    #[serde(default = "default_serve_queue_timeout_ms")]
    serve_queue_timeout_ms: u64,
    /// Path to the p2p node key file (CC-20b / §1.7 / ADR P4-13).
    ///
    /// When set and the file exists, `Store::open` runs **I-node-id** against
    /// the 32-byte key surface so a store restored beside a different key
    /// **refuses to start** rather than silently re-backfilling. When set and
    /// the file is missing, first boot (no `AnchorInfo`) still skips; a
    /// populated store **refuses**. Same path as `p2p.node_key_path` when the
    /// identity volume is mounted (or a host path for local dev). Override:
    /// `CC_STORAGE_NODE_KEY_PATH`.
    #[serde(default)]
    node_key_path: Option<PathBuf>,
}

/// `storage.retention_override` — non-spec retention windows for the discharging
/// prune venue (columns 64 / blocks 256 on the compressed profile).
#[derive(Debug, Clone, Deserialize)]
struct RetentionOverride {
    /// Column retention depth in epochs (overrides the 4096-epoch Fulu default).
    columns_epochs: u64,
    /// Block retention depth in epochs (overrides the CC-4A computed floor).
    blocks_epochs: u64,
}

/// `storage.debug.*` — fault injection (CC-48 /3 uses `crash_point`).
#[derive(Debug, Clone, Default, Deserialize)]
struct StorageDebug {
    /// e.g. `"after_put_before_commit"`. Absent / empty → inactive.
    #[serde(default)]
    #[allow(dead_code)] // engine seam abort when CC-48 /3 lands
    crash_point: Option<String>,
}

fn default_check_invariants() -> bool {
    true
}
fn default_max_open_scan_rows() -> u64 {
    cc_store::DEFAULT_MAX_OPEN_SCAN_ROWS
}
fn default_data_dir() -> PathBuf {
    PathBuf::from("data/storage")
}
fn default_durability() -> String {
    "immediate".to_owned()
}
fn default_commit_slots() -> u64 {
    1
}
fn default_commit_max_events() -> usize {
    64
}
fn default_commit_max_latency_ms() -> u64 {
    4_000
}
fn default_writer_p0_bound() -> usize {
    writer::WRITER_P0_BOUND
}
fn default_writer_p1_bound() -> usize {
    writer::WRITER_P1_BOUND
}
fn default_writer_p2_bound() -> usize {
    writer::WRITER_P2_BOUND
}
fn default_enable_write_path() -> bool {
    true
}
fn default_epochs_per_migration() -> u64 {
    migrate::DEFAULT_EPOCHS_PER_MIGRATION
}
fn default_snapshot_epochs() -> u64 {
    replay::DEFAULT_SNAPSHOT_EPOCHS_CFG
}
fn default_snapshot_ring() -> u64 {
    replay::DEFAULT_SNAPSHOT_RING_CFG
}
fn default_serve_buffer_bytes() -> u64 {
    serve::DEFAULT_SERVE_BUFFER_BYTES
}
fn default_serve_permits() -> usize {
    serve::DEFAULT_SERVE_PERMITS
}
fn default_serve_queue_timeout_ms() -> u64 {
    serve::DEFAULT_SERVE_QUEUE_TIMEOUT.as_millis() as u64
}
fn default_prune_columns_epochs() -> u64 {
    DEFAULT_PRUNE_COLUMNS_EPOCHS
}
fn default_prune_blocks_epochs() -> u64 {
    DEFAULT_PRUNE_BLOCKS_EPOCHS
}
fn default_prune_margin_epochs() -> u64 {
    DEFAULT_PRUNE_MARGIN_EPOCHS
}
fn default_prune_chunk_keys() -> usize {
    DEFAULT_PRUNE_CHUNK_KEYS
}
fn default_prune_deadline_ms() -> u64 {
    DEFAULT_PRUNE_DEADLINE.as_millis() as u64
}
fn default_disk_alarm_bytes() -> u64 {
    DEFAULT_DISK_ALARM_BYTES
}

impl StorageConfig {
    /// CC-4D: refuse retention override / crash_point unless GVR is devnet-like.
    fn check_dangerous_knobs(&self) -> Result<(), cc_config::DangerousKnobError> {
        cc_config::check_dangerous_knobs(
            self.genesis_validators_root.as_deref(),
            self.retention_override.is_some(),
            self.debug.crash_point.as_deref(),
            None,
        )
    }

    /// Build the bootstrap [`ServiceSpec`] (D-1: lives in L3, never in `cc-config`).
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

    fn serve_config(&self) -> ServeConfig {
        ServeConfig {
            buffer_bytes: self.serve_buffer_bytes.max(1),
            permits: self.serve_permits.max(1),
            // SEC-4I-1: dedicated snapshot stream pool (default 1) + size budget.
            snapshot_permits: serve::DEFAULT_SNAPSHOT_PERMITS,
            snapshot_buffer_bytes: cc_store::MAX_SNAPSHOT_BYTES,
            queue_timeout: Duration::from_millis(self.serve_queue_timeout_ms.max(1)),
            materialise_mode: serve::MaterialiseMode::AdmissionTime,
        }
    }

    fn writer_bounds(&self) -> WriterBounds {
        WriterBounds {
            p0: self.writer_p0_bound.max(1),
            p1: self.writer_p1_bound.max(1),
            p2: self.writer_p2_bound.max(1),
        }
    }

    fn migration_config(&self) -> MigrationConfig {
        MigrationConfig {
            epochs_per_migration: self.epochs_per_migration.max(1),
        }
    }

    fn replay_config(&self) -> ReplayConfig {
        ReplayConfig {
            snapshot_epochs: self.snapshot_epochs.max(1),
            snapshot_ring: self.snapshot_ring.max(1),
        }
    }

    /// Build prune config: cadence/margin/alarm from toml; retention from
    /// the CC-4A floor computed off `network_config` (or `retention_override`
    /// on the self-devnet).
    fn prune_config(&self, chain: &ChainConfig) -> anyhow::Result<PruneConfig> {
        let (columns_retention, blocks_retention) = match &self.retention_override {
            Some(ro) => (ro.columns_epochs.max(1), ro.blocks_epochs.max(1)),
            None => (DEFAULT_COLUMNS_RETENTION_EPOCHS, self.block_serve_floor()?),
        };

        // genesis_time: explicit config → network fixture (MIN_GENESIS_TIME+GENESIS_DELAY).
        // Store-snapshot fallback is applied inside Pruner::new / effective_genesis_time.
        let genesis_time = self
            .genesis_time
            .filter(|t| *t > 0)
            .or_else(genesis_time_from_fixture)
            .unwrap_or(0);

        Ok(PruneConfig {
            prune_columns_epochs: self.prune_columns_epochs.max(1),
            prune_blocks_epochs: self.prune_blocks_epochs.max(1),
            prune_margin_epochs: self.prune_margin_epochs,
            prune_chunk_keys: self.prune_chunk_keys.max(1),
            prune_deadline: Duration::from_millis(self.prune_deadline_ms.max(1)),
            disk_alarm_bytes: self.disk_alarm_bytes.max(1),
            columns_retention_epochs: columns_retention,
            blocks_retention_epochs: blocks_retention,
            fulu_fork_epoch: chain.fulu_fork_epoch.as_u64(),
            snapshot_ring: self.snapshot_ring.max(1),
            genesis_time,
            seconds_per_slot: chain.seconds_per_slot.max(1),
            slots_per_epoch: 32,
        })
    }

    /// CC-4A: compute the block retention floor from the configured network YAML.
    ///
    /// Missing / unreadable / mismatched / overflowing sources fail closed.
    /// A 0 result is refused — it would prune the entire history.
    fn block_serve_floor(&self) -> anyhow::Result<u64> {
        let path = self.network_config.as_ref().ok_or_else(|| {
            anyhow::anyhow!(
                "storage.network_config is required to compute the CC-4A block floor \
                 (a missing source must not become a 0 retention floor)"
            )
        })?;
        let window = BlockServeWindowCfg::from_yaml_file(path).map_err(|e| {
            anyhow::anyhow!(
                "storage.network_config {}: {e} \
                 (unreadable source is a startup error; a 0 floor is maximally destructive)",
                path.display()
            )
        })?;
        let floor = check_min_epochs_for_block_requests(&window)
            .map_err(|e| anyhow::anyhow!("CC-4A block floor from {}: {e}", path.display()))?;
        if floor == 0 {
            anyhow::bail!(
                "CC-4A block floor from {} computed to 0 \
                 (maximally destructive for retention)",
                path.display()
            );
        }
        Ok(floor)
    }
}

/// Open the store under `data_dir` with durability + digest from config.
///
/// When [`StorageConfig::node_key_path`] is set and present, loads the expected
/// NodeId surface for **I-node-id** (§1.7) so a mismatched key refuses open.
fn open_store(cfg: &StorageConfig) -> anyhow::Result<Store> {
    Ok(crate::open(
        &cfg.data_dir,
        crate::OpenOpts {
            durability: cfg.durability.clone(),
            check_invariants: cfg.check_invariants,
            snapshot_ring: cfg.snapshot_ring.max(1),
            max_open_scan_rows: cfg.max_open_scan_rows,
            genesis_validators_root: cfg.genesis_validators_root.clone(),
            node_key_path: cfg.node_key_path.clone(),
        },
    )?
    .into_store())
}

/// Fire the process shutdown watch from bootstrap's SIGTERM/SIGINT pre-drain hook.
fn pre_drain_fire_shutdown(shutdown_tx: watch::Sender<bool>) -> cc_bootstrap::PreDrainHook {
    Box::new(move || {
        Box::pin(async move {
            tracing::info!("pre-drain: firing storage shutdown watch");
            let _ = shutdown_tx.send(true);
        })
    })
}

/// Minimal chain config for the config-digest input when no network YAML is set.
trait MainnetLikeDigest {
    fn mainnet_like_for_digest() -> Self;
}

impl MainnetLikeDigest for ChainConfig {
    fn mainnet_like_for_digest() -> Self {
        // Prefer loading the committed Hoodi fixture when present; fall back to
        // a compile-time skeleton so unit tests / bare binaries still open.
        let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../crates/types/tests/fixtures/hoodi-config.yaml");
        if fixture.is_file()
            && let Ok(cfg) = ChainConfig::from_yaml_file(&fixture)
        {
            return cfg;
        }
        // Last-resort skeleton (digest is still well-defined).
        match ChainConfig::from_yaml_str(include_str!(
            "../../../crates/types/tests/fixtures/hoodi-config.yaml"
        )) {
            Ok(cfg) => cfg,
            Err(e) => {
                // Bundle is compile-time; a parse failure is a shipping bug.
                tracing::error!(error = %e, "bundled hoodi-config.yaml failed to parse");
                // Return a zeroed-epoch skeleton via re-parse of empty is impossible;
                // panic is process-fatal at startup before bind (same as open fail).
                std::process::exit(1);
            }
        }
    }
}

/// Fail-before-bind storage host: config, open, resume, writer, serve.
pub async fn run() -> anyhow::Result<()> {
    // Fail before any bind (CC-09/2): load config, then telemetry, then serve.
    let cfg = cc_config::load::<StorageConfig>(SERVICE)?;
    // CC-4D: one guard, three knobs — retention_override + crash_point here.
    cfg.check_dangerous_knobs()
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let mut bs = cc_bootstrap::init(SERVICE, TelemetrySettings::from(&cfg.service))?;
    if let Some(ref ro) = cfg.retention_override {
        tracing::warn!(
            columns_epochs = ro.columns_epochs,
            blocks_epochs = ro.blocks_epochs,
            "storage.retention_override active (devnet-only compressed retention)"
        );
    }
    if let Some(ref cp) = cfg.debug.crash_point
        && !cp.is_empty()
    {
        tracing::warn!(crash_point = %cp, "storage.debug.crash_point active (devnet-only)");
    }
    // CC-4Ca: §10.1 families into bs.registry between init and serve (Phase 0 seam).
    let storage_metrics = StorageMetrics::register(&mut bs.registry);

    // CC-44b: open store + spawn writer (process-fatal).
    // S2-A-09: no write-behind SubscribeEvents consumer.
    // CC-4F: keep engine Arc for the serve pool (no mem::forget).
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let mut serve_engine: Option<Arc<cc_store::engine::Engine>> = None;
    let mut serve_writer: Option<WriterHandle> = None;
    // Keep split / pruner alive for process lifetime when write path is on.
    let mut _split_keep: Option<Arc<SplitLock>> = None;
    let mut _pruner_keep: Option<Arc<Pruner>> = None;

    if cfg.enable_write_path {
        let open_t0 = std::time::Instant::now();
        match open_store(&cfg) {
            Ok(store) => {
                let engine = Arc::new(store.into_engine());
                // CC-45b: populate open phase of restart_seconds.
                resume::observe_phase(
                    &storage_metrics,
                    metrics::RestartPhase::Open,
                    open_t0.elapsed(),
                );
                // CC-45b: schema check + durable-set load. E4 push is gone.
                let durable_ctx = durable_set::DurableSetContext {
                    expected_node_id: durable_set::load_expected_node_id_from_key_path(
                        cfg.node_key_path.as_deref(),
                    )
                    .ok()
                    .flatten(),
                    node_key_path: cfg.node_key_path.clone(),
                    enr_seq_path: None,
                    snapshot_ring: cfg.snapshot_ring.max(1),
                    max_open_scan_rows: cfg.max_open_scan_rows,
                    da_status_roots: Vec::new(),
                };
                // Resume drops the plan; fork tags never leave this process.
                let chain = ChainConfig::mainnet_like_for_digest();
                match resume::run_resume_sequence(
                    &engine,
                    &storage_metrics,
                    &durable_ctx,
                    resume::ResumeExit::Os,
                    &chain,
                ) {
                    Ok(outcome) => {
                        if outcome.empty {
                            tracing::info!(
                                "resume: empty store; 4-container chain checkpoint-syncs (CC-19)"
                            );
                        } else {
                            tracing::info!(
                                head_root = %outcome.head_root,
                                head_slot = outcome.head_slot,
                                "resume: durable set loaded; continuing write path"
                            );
                        }
                    }
                    Err(e) => {
                        return Err(anyhow::anyhow!("resume sequence failed: {e}"));
                    }
                }
                let faults = WriterFaults::default();
                let writer = spawn_writer(
                    Arc::clone(&engine),
                    storage_metrics.clone(),
                    cfg.writer_bounds(),
                    faults,
                    shutdown_rx.clone(),
                    true, // process-fatal on panic (§1.5)
                );
                // S2-A-05: typed ingest over the live P0 mailbox. J-01 injects
                // this handle into chain-core. Until then the chain path
                // fail-closes (no AlreadyKnown after a drop).
                let _archive = ArchiveWriter::new(writer.clone(), Arc::clone(&engine));
                // CC-41: split lock + migrator (FINALIZED_CHECKPOINT cadence).
                let split = Arc::new(SplitLock::load(&engine).unwrap_or_else(|e| {
                    tracing::warn!(error = %e, "split load failed; defaulting to zero");
                    SplitLock::new(cc_store::Split::default())
                }));
                let migrator = Arc::new(Migrator::new(
                    Arc::clone(&split),
                    Arc::clone(&engine),
                    writer.clone(),
                    cfg.migration_config(),
                    storage_metrics.clone(),
                ));
                // CC-42: REPLAY TASK — own finalized-state replay + snapshot serialize.
                let replayer = Arc::new(ReplayDriver::new(
                    Arc::clone(&engine),
                    Arc::clone(&split),
                    writer.clone(),
                    storage_metrics.clone(),
                    cfg.replay_config(),
                ));
                let _replay_join = spawn_replay_task(Arc::clone(&replayer), shutdown_rx.clone());
                // CC-46a: five prune passes (wall-clock epoch ticks; P2 chunks).
                let chain_for_prune = ChainConfig::mainnet_like_for_digest();
                let prune_cfg = cfg.prune_config(&chain_for_prune)?;
                tracing::info!(
                    genesis_time = prune_cfg.genesis_time,
                    blocks_retention_epochs = prune_cfg.blocks_retention_epochs,
                    "prune config genesis_time (0 → resolve from store snapshot later)"
                );
                let pruner = Arc::new(Pruner::new(
                    Arc::clone(&engine),
                    writer.clone(),
                    prune_cfg,
                    storage_metrics.clone(),
                ));
                // Snapshot-ring pass trigger: on each successful snapshot write (§7.0).
                replayer.set_pruner(Arc::clone(&pruner));
                let _prune_join = spawn_prune_task(Arc::clone(&pruner), shutdown_rx);
                // Migrator stays live for P1 split advance; S2-A-09 no longer
                // drives it from SubscribeEvents (J-01 / later notify).
                let _migrator_keep = migrator;
                tracing::info!(
                    data_dir = %cfg.data_dir.display(),
                    commit_slots = cfg.commit_slots,
                    commit_max_events = cfg.commit_max_events,
                    commit_max_latency_ms = cfg.commit_max_latency_ms,
                    epochs_per_migration = cfg.epochs_per_migration,
                    snapshot_epochs = cfg.snapshot_epochs,
                    snapshot_ring = cfg.snapshot_ring,
                    prune_columns_epochs = cfg.prune_columns_epochs,
                    prune_blocks_epochs = cfg.prune_blocks_epochs,
                    prune_margin_epochs = cfg.prune_margin_epochs,
                    prune_chunk_keys = cfg.prune_chunk_keys,
                    prune_deadline_ms = cfg.prune_deadline_ms,
                    disk_alarm_bytes = cfg.disk_alarm_bytes,
                    serve_buffer_bytes = cfg.serve_buffer_bytes,
                    serve_permits = cfg.serve_permits,
                    "writer + migrator + replay + prune + serve pool ready"
                );
                serve_engine = Some(engine);
                serve_writer = Some(writer);
                _split_keep = Some(split);
                _pruner_keep = Some(pruner);
            }
            Err(e) => {
                // Fail closed on open errors when write path is enabled.
                return Err(e);
            }
        }
    } else {
        tracing::warn!("enable_write_path=false — writer not started; serve stub only");
    }

    let storage_svc = match serve_engine {
        Some(engine) => {
            StorageServer::new(engine, serve_writer, storage_metrics, cfg.serve_config())
        }
        None => StorageServer::stub(storage_metrics, cfg.serve_config()),
    };
    let routes = Routes::default().add_service(UnaryPermitService::new(StorageServiceServer::new(
        storage_svc,
    )));
    let options = ServeOptions {
        on_pre_drain: Some(pre_drain_fire_shutdown(shutdown_tx)),
        ..ServeOptions::default()
    };
    serve_with_options(
        bs,
        cfg.service_spec(),
        routes,
        options,
        SignalTrigger::UnixSignals,
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod shutdown_watch_tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use std::time::Duration;

    /// Falsifier for the old "keep the sender, never send" wiring: `changed()`
    /// must complete with `true`, not hang and not return `Err`.
    #[tokio::test]
    async fn pre_drain_hook_fires_shutdown_watch() {
        let (tx, mut rx) = watch::channel(false);
        assert!(!*rx.borrow());
        let hook = pre_drain_fire_shutdown(tx);
        hook().await;
        tokio::time::timeout(Duration::from_millis(200), rx.changed())
            .await
            .expect("shutdown watch must fire")
            .expect("shutdown watch must be marked true, not closed");
        assert!(*rx.borrow());
    }
}

#[cfg(test)]
mod config_tests {
    // Edition 2024: env mutation is unsafe and must be serialised.
    #![allow(unsafe_code)]
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use std::path::Path;
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }

    fn storage_toml_path() -> std::path::PathBuf {
        // services/storage → repo root `config/storage.toml` (CWD-independent).
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../config/storage.toml")
    }

    fn hoodi_network_config() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../crates/types/tests/fixtures/hoodi-config.yaml")
    }

    fn load_storage_toml() -> StorageConfig {
        let path = storage_toml_path();
        cc_config::load_from::<StorageConfig>("storage", &path)
            .unwrap_or_else(|e| panic!("load {}: {e}", path.display()))
    }

    fn load_storage_toml_for_prune() -> StorageConfig {
        let mut cfg = load_storage_toml();
        // cargo test CWD is the package root, not the repo root.
        cfg.network_config = Some(hoodi_network_config());
        cfg
    }

    #[test]
    fn max_open_scan_rows_default_in_storage_toml() {
        let _g = env_lock();
        unsafe { std::env::remove_var("CC_STORAGE_MAX_OPEN_SCAN_ROWS") };
        let path = storage_toml_path();
        let cfg = cc_config::load_from::<StorageConfig>("storage", &path)
            .unwrap_or_else(|e| panic!("load {}: {e}", path.display()));
        assert_eq!(
            cfg.max_open_scan_rows,
            cc_store::DEFAULT_MAX_OPEN_SCAN_ROWS,
            "storage.toml must carry DEFAULT_MAX_OPEN_SCAN_ROWS"
        );
        assert!(
            cfg.max_open_scan_rows >= cc_store::MAX_CONTIG_WALK_SLOTS,
            "named default must cover a full serve-window canonical walk"
        );
    }

    #[test]
    fn check_invariants_true_in_storage_toml() {
        // AC: config-loading test (not file-string grep). Devnet/local profile polarity.
        let _g = env_lock();
        // Ensure no stale override from a parallel env test.
        // SAFETY: exclusive via env_lock.
        unsafe { std::env::remove_var("CC_STORAGE_CHECK_INVARIANTS") };
        let path = storage_toml_path();
        let cfg = cc_config::load_from::<StorageConfig>("storage", &path)
            .unwrap_or_else(|e| panic!("load {}: {e}", path.display()));
        assert!(
            cfg.check_invariants,
            "devnet/local storage.toml must default check_invariants = true"
        );
    }

    #[test]
    fn check_invariants_env_override_false_for_hoodi_soak() {
        // Documented Hoodi soak polarity: CC_STORAGE_CHECK_INVARIANTS=false.
        let _g = env_lock();
        let key = "CC_STORAGE_CHECK_INVARIANTS";
        let prev = std::env::var(key).ok();
        // SAFETY: exclusive via env_lock; restored below.
        unsafe { std::env::set_var(key, "false") };
        let path = storage_toml_path();
        let cfg = cc_config::load_from::<StorageConfig>("storage", &path)
            .unwrap_or_else(|e| panic!("load {}: {e}", path.display()));
        match prev {
            Some(v) => unsafe { std::env::set_var(key, v) },
            None => unsafe { std::env::remove_var(key) },
        }
        assert!(
            !cfg.check_invariants,
            "Hoodi soak override CC_STORAGE_CHECK_INVARIANTS=false must load as false"
        );
    }

    /// CC-4D /1: committed storage.toml has no dangerous knobs active, so the
    /// guard is a no-op even without a GVR.
    #[test]
    fn committed_storage_toml_passes_dangerous_knob_guard() {
        let _g = env_lock();
        unsafe {
            std::env::remove_var("CC_STORAGE_CHECK_INVARIANTS");
            std::env::remove_var("CC_STORAGE_GENESIS_VALIDATORS_ROOT");
        }
        let path = storage_toml_path();
        let cfg = cc_config::load_from::<StorageConfig>("storage", &path)
            .unwrap_or_else(|e| panic!("load {}: {e}", path.display()));
        assert!(cfg.retention_override.is_none());
        assert!(cfg.debug.crash_point.as_deref().unwrap_or("").is_empty());
        cfg.check_dangerous_knobs()
            .expect("default storage.toml must start without GVR");
    }

    /// CC-44 /5: `commit_slots = 1` with the loss-bound comment in storage.toml.
    #[test]
    fn commit_slots_default_is_one_loss_bound() {
        let _g = env_lock();
        unsafe {
            std::env::remove_var("CC_STORAGE_COMMIT_SLOTS");
        }
        let path = storage_toml_path();
        let cfg = cc_config::load_from::<StorageConfig>("storage", &path)
            .unwrap_or_else(|e| panic!("load {}: {e}", path.display()));
        assert_eq!(cfg.commit_slots, 1, "commit_slots IS the loss bound (§4.4)");
        assert_eq!(cfg.commit_max_events, 64);
        assert_eq!(cfg.commit_max_latency_ms, 4_000);
        assert_eq!(cfg.writer_p0_bound, 32);
        assert_eq!(cfg.writer_p1_bound, 64);
        assert_eq!(cfg.writer_p2_bound, 256);
        // Comment presence (loss-bound wording from §4.4).
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(
            text.contains("this value IS the loss bound"),
            "storage.toml must carry the §4.4 loss-bound comment verbatim-ish"
        );
    }

    /// CC-41: `epochs_per_migration = 1` default in storage.toml.
    #[test]
    fn epochs_per_migration_default_is_one() {
        let _g = env_lock();
        unsafe {
            std::env::remove_var("CC_STORAGE_EPOCHS_PER_MIGRATION");
        }
        let path = storage_toml_path();
        let cfg = cc_config::load_from::<StorageConfig>("storage", &path)
            .unwrap_or_else(|e| panic!("load {}: {e}", path.display()));
        assert_eq!(
            cfg.epochs_per_migration, 1,
            "Lighthouse --epochs-per-migration default"
        );
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.contains("epochs_per_migration"));
    }

    /// CC-42 /1: snapshot_epochs=32, snapshot_ring=4 in storage.toml.
    #[test]
    fn snapshot_epochs_and_ring_defaults() {
        let _g = env_lock();
        unsafe {
            std::env::remove_var("CC_STORAGE_SNAPSHOT_EPOCHS");
            std::env::remove_var("CC_STORAGE_SNAPSHOT_RING");
        }
        let path = storage_toml_path();
        let cfg = cc_config::load_from::<StorageConfig>("storage", &path)
            .unwrap_or_else(|e| panic!("load {}: {e}", path.display()));
        assert_eq!(
            cfg.snapshot_epochs, 32,
            "Grandine archival interval default"
        );
        assert_eq!(cfg.snapshot_ring, 4, "ring depth default");
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.contains("snapshot_epochs"));
        assert!(text.contains("snapshot_ring"));
    }

    /// CC-4F: serve_buffer_bytes / serve_permits / serve_queue_timeout defaults.
    #[test]
    fn serve_path_ceiling_defaults() {
        let _g = env_lock();
        unsafe {
            std::env::remove_var("CC_STORAGE_SERVE_BUFFER_BYTES");
            std::env::remove_var("CC_STORAGE_SERVE_PERMITS");
            std::env::remove_var("CC_STORAGE_SERVE_QUEUE_TIMEOUT_MS");
        }
        let path = storage_toml_path();
        let cfg = cc_config::load_from::<StorageConfig>("storage", &path)
            .unwrap_or_else(|e| panic!("load {}: {e}", path.display()));
        assert_eq!(cfg.serve_buffer_bytes, 64 * 1024 * 1024);
        assert_eq!(cfg.serve_permits, 4);
        assert_eq!(cfg.serve_queue_timeout_ms, 2_000);
        // Stated 256 MiB serve-path ceiling.
        assert_eq!(
            cfg.serve_buffer_bytes * cfg.serve_permits as u64,
            256 * 1024 * 1024
        );
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.contains("serve_buffer_bytes"));
        assert!(text.contains("serve_permits"));
        assert!(text.contains("serve_queue_timeout_ms"));
    }

    /// CC-46a/b: prune cadence / margin / chunk / deadline / disk alarm defaults.
    #[test]
    fn prune_knobs_defaults() {
        let _g = env_lock();
        unsafe {
            std::env::remove_var("CC_STORAGE_PRUNE_COLUMNS_EPOCHS");
            std::env::remove_var("CC_STORAGE_PRUNE_BLOCKS_EPOCHS");
            std::env::remove_var("CC_STORAGE_PRUNE_MARGIN_EPOCHS");
            std::env::remove_var("CC_STORAGE_PRUNE_CHUNK_KEYS");
            std::env::remove_var("CC_STORAGE_PRUNE_DEADLINE_MS");
            std::env::remove_var("CC_STORAGE_DISK_ALARM_BYTES");
            std::env::remove_var("CC_STORAGE_GENESIS_TIME");
        }
        let path = storage_toml_path();
        let cfg = cc_config::load_from::<StorageConfig>("storage", &path)
            .unwrap_or_else(|e| panic!("load {}: {e}", path.display()));
        assert_eq!(cfg.prune_columns_epochs, 32);
        assert_eq!(cfg.prune_blocks_epochs, 256);
        assert_eq!(cfg.prune_margin_epochs, 1);
        assert_eq!(cfg.prune_chunk_keys, 512);
        assert_eq!(cfg.prune_deadline_ms, 2_000);
        assert_eq!(cfg.disk_alarm_bytes, 96 * 1024 * 1024 * 1024);
        // Hoodi wall-clock genesis (MIN_GENESIS_TIME + GENESIS_DELAY).
        assert_eq!(cfg.genesis_time, Some(1_742_213_400));
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(text.contains("prune_columns_epochs"));
        assert!(text.contains("prune_blocks_epochs"));
        assert!(text.contains("prune_margin_epochs"));
        assert!(text.contains("prune_chunk_keys"));
        assert!(text.contains("prune_deadline_ms"));
        assert!(text.contains("disk_alarm_bytes"));
        assert!(text.contains("genesis_time"));
        assert!(
            text.contains("alarm only") || text.contains("never a trigger"),
            "disk alarm comment must state alarm-only semantics"
        );
        assert!(
            text.contains("6.9 MiB") || text.contains("do **not** copy"),
            "margin comment must state cost / not copying Lighthouse 0"
        );
    }

    /// CC-46a: prune_config resolves genesis_time (config or fixture).
    #[test]
    fn prune_config_resolves_genesis_time() {
        let _g = env_lock();
        unsafe {
            std::env::remove_var("CC_STORAGE_GENESIS_TIME");
            std::env::remove_var("CC_STORAGE_NETWORK_CONFIG");
        }
        let cfg = load_storage_toml_for_prune();
        let chain = ChainConfig::mainnet_like_for_digest();
        let prune = cfg.prune_config(&chain).expect("prune_config");
        assert!(
            prune.genesis_time > 0,
            "genesis_time must be non-zero so wall_clock_epoch works"
        );
        assert_eq!(prune.genesis_time, 1_742_213_400);
    }

    /// P1-A/5: committed storage.toml names a network YAML; the floor is
    /// computed from its two live scalars (Hoodi = 33_024).
    #[test]
    fn prune_config_floor_comes_from_network_config() {
        let _g = env_lock();
        unsafe {
            std::env::remove_var("CC_STORAGE_NETWORK_CONFIG");
        }
        let path = storage_toml_path();
        let loaded = load_storage_toml();
        assert_eq!(
            loaded.network_config.as_deref(),
            Some(Path::new("crates/types/tests/fixtures/hoodi-config.yaml")),
            "storage.toml must configure network_config (not a CARGO_MANIFEST_DIR fixture)"
        );
        let text = std::fs::read_to_string(&path).unwrap();
        assert!(
            text.contains("network_config"),
            "storage.toml must carry the CC-4A network_config key"
        );

        let cfg = load_storage_toml_for_prune();
        let chain = ChainConfig::mainnet_like_for_digest();
        let prune = cfg.prune_config(&chain).expect("prune_config");
        assert_eq!(
            prune.blocks_retention_epochs, 33_024,
            "Hoodi MIN_VALIDATOR_WITHDRAWABILITY_DELAY + CHURN_LIMIT_QUOTIENT / 2"
        );
        assert_eq!(
            prune.columns_retention_epochs,
            DEFAULT_COLUMNS_RETENTION_EPOCHS
        );
    }

    /// P1-A/5: a missing source is a startup error, not a silent 0 floor.
    #[test]
    fn prune_config_missing_network_config_is_startup_error() {
        let _g = env_lock();
        unsafe {
            std::env::remove_var("CC_STORAGE_NETWORK_CONFIG");
        }
        let mut cfg = load_storage_toml();
        cfg.network_config = None;
        cfg.retention_override = None;
        let chain = ChainConfig::mainnet_like_for_digest();
        let err = cfg
            .prune_config(&chain)
            .expect_err("missing network_config must refuse to start");
        let msg = err.to_string();
        assert!(
            msg.contains("network_config") && msg.contains("must not become a 0"),
            "error must name the missing source and refuse a 0 floor: {msg}"
        );
    }

    /// P1-A/5: an unreadable configured path is a startup error, not unwrap_or(0).
    #[test]
    fn prune_config_unreadable_network_config_is_startup_error() {
        let _g = env_lock();
        unsafe {
            std::env::remove_var("CC_STORAGE_NETWORK_CONFIG");
        }
        let mut cfg = load_storage_toml();
        cfg.network_config = Some(PathBuf::from(
            "/no/such/cc-storage-network-config-p1a5.yaml",
        ));
        cfg.retention_override = None;
        let chain = ChainConfig::mainnet_like_for_digest();
        let err = cfg
            .prune_config(&chain)
            .expect_err("unreadable network_config must refuse to start");
        let msg = err.to_string();
        assert!(
            msg.contains("network_config") && msg.contains("cc-storage-network-config-p1a5"),
            "error must name the unreadable path: {msg}"
        );
    }

    /// P1-A/5 / CC-4A /2: vestigial MIN_EPOCHS_FOR_BLOCK_REQUESTS mismatch refuses.
    #[test]
    fn prune_config_vestigial_mismatch_is_startup_error() {
        let _g = env_lock();
        unsafe {
            std::env::remove_var("CC_STORAGE_NETWORK_CONFIG");
        }
        let dir = std::env::temp_dir().join(format!(
            "cc-storage-floor-mismatch-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let yaml = dir.join("bad.yaml");
        std::fs::write(
            &yaml,
            "MIN_VALIDATOR_WITHDRAWABILITY_DELAY: 256\n\
             CHURN_LIMIT_QUOTIENT: 65536\n\
             MIN_EPOCHS_FOR_BLOCK_REQUESTS: 33023\n",
        )
        .unwrap();

        let mut cfg = load_storage_toml();
        cfg.network_config = Some(yaml);
        cfg.retention_override = None;
        let chain = ChainConfig::mainnet_like_for_digest();
        let err = cfg
            .prune_config(&chain)
            .expect_err("vestigial mismatch must refuse to start");
        let msg = err.to_string();
        assert!(
            msg.contains("33023") && msg.contains("33024"),
            "error must name both numbers: {msg}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// P1-A/5: a computed 0 floor is refused (maximally destructive retention).
    #[test]
    fn prune_config_zero_floor_is_startup_error() {
        let _g = env_lock();
        unsafe {
            std::env::remove_var("CC_STORAGE_NETWORK_CONFIG");
        }
        let dir = std::env::temp_dir().join(format!(
            "cc-storage-floor-zero-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let yaml = dir.join("zero.yaml");
        std::fs::write(
            &yaml,
            "MIN_VALIDATOR_WITHDRAWABILITY_DELAY: 0\n\
             CHURN_LIMIT_QUOTIENT: 0\n",
        )
        .unwrap();

        let mut cfg = load_storage_toml();
        cfg.network_config = Some(yaml);
        cfg.retention_override = None;
        let chain = ChainConfig::mainnet_like_for_digest();
        let err = cfg
            .prune_config(&chain)
            .expect_err("0 floor must refuse to start");
        let msg = err.to_string();
        assert!(
            msg.contains("0") && msg.contains("destructive"),
            "error must name the 0-floor hazard: {msg}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CC-4D override still wins and does not require a readable network YAML.
    #[test]
    fn prune_config_retention_override_skips_network_config() {
        let _g = env_lock();
        unsafe {
            std::env::remove_var("CC_STORAGE_NETWORK_CONFIG");
        }
        let mut cfg = load_storage_toml();
        cfg.network_config = None;
        cfg.retention_override = Some(RetentionOverride {
            columns_epochs: 64,
            blocks_epochs: 256,
        });
        let chain = ChainConfig::mainnet_like_for_digest();
        let prune = cfg
            .prune_config(&chain)
            .expect("retention_override must not require network_config");
        assert_eq!(prune.columns_retention_epochs, 64);
        assert_eq!(prune.blocks_retention_epochs, 256);
    }

    /// CC-45a / §1.7: production `open_store` wires `expected_node_id` from
    /// `node_key_path` so I-node-id runs at start. Mismatched key refuses open.
    /// The literal `node_id` stays; neither byte string is printed.
    #[test]
    fn open_store_mismatched_node_key_refuses() {
        use cc_store::engine::{Durability, EngineOptions};
        use cc_store::meta::{AnchorInfo, KEY_ANCHOR_INFO, TABLE_META};
        use cc_store::{
            ConfigDigestInput, SszEncode, Store, StoreOpenOptions, compute_config_digest,
        };
        use cc_types::Root;
        use std::time::{SystemTime, UNIX_EPOCH};

        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("cc-storage-open-nodeid-{nanos}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let anchor_id = Root::from_array([0xAAu8; 32]);
        let key_id = Root::from_array([0xBBu8; 32]);
        assert_ne!(anchor_id, key_id);

        // Bootstrap store with AnchorInfo.node_id = anchor_id (no invariant check yet).
        let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../crates/types/tests/fixtures/hoodi-config.yaml");
        let chain = ChainConfig::from_yaml_file(&fixture).unwrap_or_else(|_| {
            ChainConfig::from_yaml_str(include_str!(
                "../../../crates/types/tests/fixtures/hoodi-config.yaml"
            ))
            .unwrap()
        });
        let digest_input = ConfigDigestInput::with_mainnet_scalars(chain, Root::ZERO);
        let digest = compute_config_digest(&digest_input).unwrap();
        let store = Store::open(
            &dir,
            StoreOpenOptions::with_digest(
                EngineOptions::default().with_durability(Durability::None),
                digest,
            )
            .with_check_invariants(false),
        )
        .unwrap();
        let engine = store.into_engine();
        let anchor = AnchorInfo {
            anchor_slot: cc_store::Slot::new(10),
            anchor_root: Root::from_array([0x10; 32]),
            anchor_state_root: Root::from_array([0x11; 32]),
            node_id: anchor_id,
            oldest_block_slot: cc_store::Slot::new(10),
            oldest_block_parent: Root::from_array([0x09; 32]),
        };
        let mut b = engine.batch();
        b.put(
            TABLE_META,
            KEY_ANCHOR_INFO.as_bytes(),
            &anchor.as_ssz_bytes(),
        );
        engine.commit(b).unwrap();
        drop(engine);

        // Write the mismatched node key and open via the production helper path.
        let key_path = dir.join("node_key");
        std::fs::write(&key_path, key_id.as_slice()).unwrap();

        let mut cfg = {
            let path = storage_toml_path();
            let _g = env_lock();
            unsafe {
                std::env::remove_var("CC_STORAGE_NODE_KEY_PATH");
            }
            cc_config::load_from::<StorageConfig>("storage", &path).unwrap()
        };
        cfg.data_dir = dir.clone();
        cfg.node_key_path = Some(key_path);
        cfg.check_invariants = true;
        cfg.durability = "immediate".into();
        // open_store uses Durability::parse which rejects none; use immediate for the
        // production path (engine still opens an existing db).
        let err = open_store(&cfg).expect_err("mismatched node key must refuse open");
        let msg = err.to_string();
        assert!(
            msg.contains("node_id") || msg.contains("I-node-id") || msg.contains("invariant"),
            "error must name I-node-id: {msg}"
        );
        let chain = format!("{err:#}");
        assert!(
            chain.contains("node_id"),
            "error chain must keep the literal node_id: {chain}"
        );
        assert!(
            !msg.contains(&anchor_id.to_string())
                && !msg.contains(&key_id.to_string())
                && !chain.contains(&anchor_id.to_string())
                && !chain.contains(&key_id.to_string()),
            "error chain must not print stored or key-file bytes: {chain}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Path set, file missing, store already has AnchorInfo → refuse (H1).
    #[test]
    fn open_store_missing_key_with_anchor_refuses() {
        use cc_store::engine::{Durability, EngineOptions};
        use cc_store::meta::{AnchorInfo, KEY_ANCHOR_INFO, TABLE_META};
        use cc_store::{
            ConfigDigestInput, SszEncode, Store, StoreOpenOptions, compute_config_digest,
        };
        use cc_types::Root;
        use std::time::{SystemTime, UNIX_EPOCH};

        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("cc-storage-open-missing-key-{nanos}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let anchor_id = Root::from_array([0xAAu8; 32]);
        let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../crates/types/tests/fixtures/hoodi-config.yaml");
        let chain = ChainConfig::from_yaml_file(&fixture).unwrap_or_else(|_| {
            ChainConfig::from_yaml_str(include_str!(
                "../../../crates/types/tests/fixtures/hoodi-config.yaml"
            ))
            .unwrap()
        });
        let digest_input = ConfigDigestInput::with_mainnet_scalars(chain, Root::ZERO);
        let digest = compute_config_digest(&digest_input).unwrap();
        let store = Store::open(
            &dir,
            StoreOpenOptions::with_digest(
                EngineOptions::default().with_durability(Durability::None),
                digest,
            )
            .with_check_invariants(false),
        )
        .unwrap();
        let engine = store.into_engine();
        let anchor = AnchorInfo {
            anchor_slot: cc_store::Slot::new(10),
            anchor_root: Root::from_array([0x10; 32]),
            anchor_state_root: Root::from_array([0x11; 32]),
            node_id: anchor_id,
            oldest_block_slot: cc_store::Slot::new(10),
            oldest_block_parent: Root::from_array([0x09; 32]),
        };
        let mut b = engine.batch();
        b.put(
            TABLE_META,
            KEY_ANCHOR_INFO.as_bytes(),
            &anchor.as_ssz_bytes(),
        );
        engine.commit(b).unwrap();
        drop(engine);

        let key_path = dir.join("node_key");
        let _ = std::fs::remove_file(&key_path);

        let mut cfg = {
            let path = storage_toml_path();
            let _g = env_lock();
            unsafe {
                std::env::remove_var("CC_STORAGE_NODE_KEY_PATH");
            }
            cc_config::load_from::<StorageConfig>("storage", &path).unwrap()
        };
        cfg.data_dir = dir.clone();
        cfg.node_key_path = Some(key_path);
        cfg.check_invariants = false;
        cfg.durability = "immediate".into();
        let err = open_store(&cfg).expect_err("missing key with identity must refuse");
        let msg = err.to_string();
        assert!(
            msg.contains("I-node-id") && msg.contains("identity"),
            "error must cite I-node-id and identity: {msg}"
        );
        assert!(
            !msg.contains(&anchor_id.to_string()),
            "error must not print stored node id bytes: {msg}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Path set, file missing, empty store (no AnchorInfo) → first boot still opens.
    #[test]
    fn open_store_missing_key_without_anchor_ok() {
        use std::time::{SystemTime, UNIX_EPOCH};

        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("cc-storage-open-first-boot-{nanos}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let key_path = dir.join("node_key");
        let _ = std::fs::remove_file(&key_path);

        let mut cfg = {
            let path = storage_toml_path();
            let _g = env_lock();
            unsafe {
                std::env::remove_var("CC_STORAGE_NODE_KEY_PATH");
            }
            cc_config::load_from::<StorageConfig>("storage", &path).unwrap()
        };
        cfg.data_dir = dir.clone();
        cfg.node_key_path = Some(key_path);
        cfg.check_invariants = false;
        cfg.durability = "immediate".into();
        open_store(&cfg).expect("first boot with missing key must still open");

        let _ = std::fs::remove_dir_all(&dir);
    }
}
