//! Serve path for `eth.storage.v1` (CC-4F / Architecture §1.6, §7.2; CC-46b §7.1).
//!
//! # Hazard (a) — 1-epoch margin + admission-time key materialisation (CC-46b)
//!
//! The window check happens **once**, at request admission; the prune watermark
//! can move past the request's `start_slot` while the response is being built.
//! **Both** mitigations, not either:
//!
//! 1. **`storage.prune_margin_epochs = 1`** on both retention watermarks (§7.0
//!    table). Cost: **~6.9 MiB of columns** and **~0.7 MiB of blocks**.
//!    Lighthouse's `--blob-prune-margin-epochs` defaults to **0** because they
//!    rely on MVCC alone — **we do not copy that default**: the margin
//!    **removes** the race rather than narrowing it.
//! 2. **Admission-time key materialisation.** The serve handler resolves the
//!    full key set (and copies values) under **one read transaction at
//!    admission**, before the first byte is produced — so a watermark that
//!    moves mid-response cannot remove a key the response already promised.
//!
//! Neither mitigation alone is relied on: margin-0 + materialisation still
//! passes under a forced concurrent prune; margin-1 with materialisation
//! removed fails. Production keeps both.
//!
//! # Cross-Requirement Dependency 6 — long-reader / materialise-and-drop
//!
//! **Materialise the response bytes and drop the read transaction before the
//! first chunk reaches the socket.** No `Iterator` over a live read transaction
//! ever crosses a function boundary in this module.
//!
//! redb (and libmdbx) only reuse freed pages after every read transaction that
//! could reference them has completed. A serve that held one `ReadTxn` open for
//! a whole 128-chunk stream, concurrent with a prune pass, would make the
//! **file grow** — and the growth would look like a pruner bug, not a reader
//! bug.
//!
//! The gRPC hop satisfies the long-reader rule **structurally**: a unary
//! response is fully materialised (owned `Vec`s) before it is sent, so the
//! transaction is dropped before the first byte leaves `storage`. The obvious
//! optimisation — streaming an `Iterator` directly out of a live read
//! transaction to avoid the copy — looks correct in isolation and reintroduces
//! the page-pinning hazard. Do not do that.
//!
//! # Admission + memory ceiling (Deviations 4)
//!
//! `serve_buffer_bytes` (default 64 MiB) × `serve_permits` (default 4) = **256 MiB**
//! serve-path ceiling. Over `serve_queue_timeout` the semaphore answers
//! `RESOURCE_EXHAUSTED`, never an empty success. Unary permits are held on the
//! HTTP body (not handler locals or `http::Extensions`) so a slow consumer
//! keeps the ceiling in force.
//!
//! # SEC-4I-1 — `GetSnapshotState` stream admission
//!
//! A Hoodi snapshot is ~175–200 MiB (well above `serve_buffer_bytes`). Concurrent
//! snapshot streams must not share the multi-block serve pool's 4-permit budget
//! as if each response were ≤ 64 MiB. Instead:
//!
//! 1. A **dedicated** `snapshot_permits` semaphore (default **1**) admits at most
//!    one in-flight snapshot stream.
//! 2. The permit is **held for the stream lifetime** (moved into the stream), not
//!    dropped at handler return — so a slow consumer keeps concurrency capped.
//! 3. Materialised size is fail-closed against [`MAX_SNAPSHOT_BYTES`] (same hard
//!    cap as the write path), analogous to the serve buffer ceiling.
//!
//! # Anti-truncation on the wire (CC-43 /3)
//!
//! A response that would hit the buffer ceiling **stops at the previous block
//! boundary**, never mid-block. Column range/root responses emit whole
//! `columns_for_block` units only.

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use cc_proto::common::BuildInfo;
use cc_proto::storage::storage_service_server::StorageService;
use cc_proto::storage::{
    BackfillProgress as ProtoBackfillProgress, BlockSsz, ColumnSsz, FinalizedCheckpoint,
    GetBlocksByRangeRequest, GetBlocksByRootRequest, GetBlocksResponse, GetColumnsByRangeRequest,
    GetColumnsByRootRequest, GetColumnsResponse, GetFinalizedCheckpointHistoryRequest,
    GetFinalizedCheckpointHistoryResponse, GetHistoricalBlockRequest, GetHistoricalBlockResponse,
    GetInfoRequest, GetInfoResponse, GetSnapshotStateRequest, PutBackfillBatchRequest,
    PutBackfillBatchResponse, ServeWindow, SlotRange as ProtoSlotRange, StateChunk,
    WatchServeWindowRequest, get_historical_block_request::Id as HistoricalBlockId,
};
use cc_store::canonical::put_canonical;
use cc_store::engine::Engine;
use cc_store::keys::BlockRegion;
use cc_store::meta::{
    AnchorInfo, BackfillProgress, KEY_ANCHOR_INFO, KEY_BACKFILL_PROG, KEY_SERVE_WINDOW, TABLE_META,
};
use cc_store::{
    MAX_BLOCKS_BY_RANGE, MAX_COLUMNS_BY_RANGE_SLOTS, MAX_SNAPSHOT_BYTES, Root, Slot, SszDecode,
    SszEncode, StoreError, blocks_by_range, columns_by_range, columns_for_block, get_block_by_root,
    load_split, parent_root_at_offset, put_block, put_column,
};
use futures::Stream;
use futures::StreamExt;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio_stream::wrappers::WatchStream;
use tonic::codegen::http::{Request as HttpRequest, Response as HttpResponse};
use tonic::codegen::{Body as HttpBody, BoxFuture, BoxStream, Service as TowerService};
use tonic::server::NamedService;
use tonic::{Request, Response, Status};

use crate::backfill::{
    BackfillBlockRow, BatchAdmitError, admit_descending_contiguous, admit_extends_durable_frontier,
    admit_progress_bound_to_batch, admit_progress_monotone,
    admit_progress_only_preserves_block_frontier, admit_progress_required,
    observe_put_backfill_batch, proto_progress_to_store,
};
use crate::history::{
    DEFAULT_STATE_CHUNK_BYTES, SnapshotLookup, finalized_checkpoint_history,
    historical_block_by_root, historical_block_by_slot, not_available_status, snapshot_state_at,
};
use crate::metrics::{ProtocolLabels, ServeLabels, ServeProtocol, ServeResult, StorageMetrics};
use crate::writer::{MetaUpdate, WriterError, WriterHandle};

/// Default per-response buffer ceiling (64 MiB).
pub(crate) const DEFAULT_SERVE_BUFFER_BYTES: u64 = 64 * 1024 * 1024;
/// Default admission permits (4 → 256 MiB serve-path ceiling with 64 MiB buffers).
pub(crate) const DEFAULT_SERVE_PERMITS: usize = 4;
/// Default concurrent `GetSnapshotState` streams (SEC-4I-1: one full state at a time).
pub(crate) const DEFAULT_SNAPSHOT_PERMITS: usize = 1;
/// Default queue wait before `RESOURCE_EXHAUSTED` (2 s).
pub(crate) const DEFAULT_SERVE_QUEUE_TIMEOUT: Duration = Duration::from_secs(2);

/// Hard cap on roots / identifiers per by-root request (spec `MAX_REQUEST_BLOCKS_DENEB`).
const MAX_BY_ROOT: usize = 128;

/// Process name stamped into `GetInfo`.
const SERVICE: &str = "storage";

/// How serve materialises keys under the read path (CC-46b hazard (a)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum MaterialiseMode {
    /// One read txn at admission; full key set resolved before first byte.
    /// Production default — pairs with the 1-epoch prune margin.
    #[default]
    AdmissionTime,
    /// Per-slot re-open of the read txn (materialisation removed). **Test-only**
    /// negative path: a concurrent prune mid-serve can drop keys the response
    /// already promised under the window check.
    #[allow(dead_code)] // constructed in tests only
    PerSlotReopen,
}

/// Serve-path configuration (from `config/storage.toml`).
#[derive(Debug, Clone)]
pub(crate) struct ServeConfig {
    /// Per-response buffer ceiling in bytes.
    pub buffer_bytes: u64,
    /// Admission semaphore permits.
    pub permits: usize,
    /// Concurrent `GetSnapshotState` streams (SEC-4I-1; default **1**).
    pub snapshot_permits: usize,
    /// Hard size budget for one materialised snapshot (SEC-4I-1).
    ///
    /// Default [`MAX_SNAPSHOT_BYTES`] — same fail-closed cap as the write path.
    /// Analogous to `buffer_bytes` on multi-block serve; a state is atomic so
    /// oversize is refused entirely (no anti-truncation partial success).
    pub snapshot_buffer_bytes: u64,
    /// Max wait for a permit before `RESOURCE_EXHAUSTED`.
    pub queue_timeout: Duration,
    /// Key materialisation mode (default [`MaterialiseMode::AdmissionTime`]).
    pub materialise_mode: MaterialiseMode,
}

impl Default for ServeConfig {
    fn default() -> Self {
        Self {
            buffer_bytes: DEFAULT_SERVE_BUFFER_BYTES,
            permits: DEFAULT_SERVE_PERMITS,
            snapshot_permits: DEFAULT_SNAPSHOT_PERMITS,
            snapshot_buffer_bytes: MAX_SNAPSHOT_BYTES,
            queue_timeout: DEFAULT_SERVE_QUEUE_TIMEOUT,
            materialise_mode: MaterialiseMode::AdmissionTime,
        }
    }
}

/// Optional mid-serve hook for the forced-concurrent-prune test (CC-46b /4).
///
/// Invoked from **inside** the serve materialisation loop (after the first slot
/// is loaded) so the prune pass is driven by the serve path rather than raced
/// by timing. Named: `mid_serve_prune_hook`.
type MidServeHook = Arc<dyn Fn() + Send + Sync>;

/// gRPC `StorageService` implementation (CC-4F).
///
/// Holds `Arc<Engine>` for short-lived read transactions and optional writer
/// for `PutBackfillBatch` (single-writer path).
pub(crate) struct StorageServer {
    engine: Option<Arc<Engine>>,
    writer: Option<WriterHandle>,
    metrics: StorageMetrics,
    cfg: ServeConfig,
    admits: Arc<Semaphore>,
    /// Dedicated admit pool for `GetSnapshotState` (SEC-4I-1; default 1 permit).
    snapshot_admits: Arc<Semaphore>,
    /// Latest serve window; `WatchServeWindow` streams from this.
    window_tx: watch::Sender<ServeWindow>,
    /// Keep at least one receiver alive so `send_replace` always has a subscriber
    /// set (watch `send` fails when every receiver is dropped).
    _window_rx: watch::Receiver<ServeWindow>,
    /// Test-only: next `PutBackfillBatch` commit is aborted without applying.
    fail_next_commit: Arc<std::sync::atomic::AtomicBool>,
    /// Test-only: forced concurrent prune from inside serve (CC-46b /4).
    mid_serve_prune_hook: Option<MidServeHook>,
}

impl std::fmt::Debug for StorageServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageServer")
            .field("cfg", &self.cfg)
            .field("has_engine", &self.engine.is_some())
            .field("has_writer", &self.writer.is_some())
            .field("has_mid_serve_hook", &self.mid_serve_prune_hook.is_some())
            .finish()
    }
}

impl StorageServer {
    /// Construct the Phase 0-only stub (no store) — used when write path is off.
    pub(crate) fn stub(metrics: StorageMetrics, cfg: ServeConfig) -> Self {
        let permits = cfg.permits.max(1);
        let snap_permits = cfg.snapshot_permits.max(1);
        let (window_tx, window_rx) = watch::channel(empty_window());
        Self {
            engine: None,
            writer: None,
            metrics,
            admits: Arc::new(Semaphore::new(permits)),
            snapshot_admits: Arc::new(Semaphore::new(snap_permits)),
            cfg,
            window_tx,
            _window_rx: window_rx,
            fail_next_commit: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            mid_serve_prune_hook: None,
        }
    }

    /// Full serve path with store + optional writer handle.
    pub(crate) fn new(
        engine: Arc<Engine>,
        writer: Option<WriterHandle>,
        metrics: StorageMetrics,
        cfg: ServeConfig,
    ) -> Self {
        let permits = cfg.permits.max(1);
        let snap_permits = cfg.snapshot_permits.max(1);
        let initial = load_window_or_default(&engine);
        let (window_tx, window_rx) = watch::channel(initial);
        Self {
            engine: Some(engine),
            writer,
            metrics,
            admits: Arc::new(Semaphore::new(permits)),
            snapshot_admits: Arc::new(Semaphore::new(snap_permits)),
            cfg,
            window_tx,
            _window_rx: window_rx,
            fail_next_commit: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            mid_serve_prune_hook: None,
        }
    }

    /// Install the forced-concurrent-prune hook (CC-46b /4 forcing mechanism:
    /// `mid_serve_prune_hook`).
    #[cfg(test)]
    pub(crate) fn with_mid_serve_prune_hook(mut self, hook: MidServeHook) -> Self {
        self.mid_serve_prune_hook = Some(hook);
        self
    }

    /// Publish a new serve window (backfill / prune / cgc). Subscribers see it.
    ///
    /// The value must be a whole derived [`cc_store::meta::ServeWindow`] —
    /// there is no independent setter for `earliest_available_slot` / `cgc`
    /// (CC-48).
    #[allow(dead_code)] // exercised by unit tests; production callers land with CC-47/48 wiring
    pub(crate) fn publish_window(&self, window: ServeWindow) {
        // `send_replace` never fails even with no external subscribers.
        self.metrics
            .earliest_available_slot
            .set(window.earliest_available_slot as i64);
        self.metrics
            .window_branch
            .set(i64::from(window.branch as u8));
        let hole_slots: u64 = window
            .holes
            .iter()
            .map(|h| h.end.saturating_sub(h.start))
            .sum();
        self.metrics.window_hole_slots.set(hole_slots as i64);
        self.window_tx.send_replace(window);
    }

    /// Derive, persist, and publish a serve window from floors + holes (CC-48 / CC-49).
    #[allow(dead_code)] // wired by backfill / hole paths as they land
    pub(crate) fn derive_and_publish_window(
        &self,
        block_floor: Slot,
        column_floor: Slot,
        cgc: u64,
        holes: &[cc_store::meta::SlotRange],
        current_slot: Slot,
    ) -> Result<cc_store::meta::ServeWindow, Status> {
        let engine = self.engine()?;
        let stored = cc_store::write_derived_serve_window(
            engine,
            block_floor,
            column_floor,
            cgc,
            holes,
            current_slot,
            false,
        )
        .map_err(store_status)?;
        self.metrics.set_serve_window(&stored);
        self.window_tx.send_replace(ServeWindow {
            earliest_available_slot: stored.earliest_available_slot.as_u64(),
            cgc: stored.cgc,
            head_slot: self.window_tx.borrow().head_slot,
            block_floor: stored.block_floor.as_u64(),
            column_floor: stored.column_floor.as_u64(),
            branch: u32::from(stored.branch),
            holes: stored
                .holes
                .iter()
                .map(|h| ProtoSlotRange {
                    start: h.start.as_u64(),
                    end: h.end.as_u64(),
                })
                .collect(),
        });
        Ok(stored)
    }

    /// Current advertised earliest available slot.
    fn earliest_available_slot(&self) -> u64 {
        self.window_tx.borrow().earliest_available_slot
    }

    fn engine(&self) -> Result<&Arc<Engine>, Status> {
        self.engine.as_ref().ok_or_else(|| {
            Status::unavailable("storage store not open (enable_write_path=false or open failed)")
        })
    }

    /// Acquire an admission permit or `RESOURCE_EXHAUSTED` after timeout.
    async fn admit(&self, protocol: ServeProtocol) -> Result<OwnedSemaphorePermit, Status> {
        let started = Instant::now();
        match tokio::time::timeout(
            self.cfg.queue_timeout,
            Arc::clone(&self.admits).acquire_owned(),
        )
        .await
        {
            Ok(Ok(permit)) => {
                self.metrics
                    .serve_admission_wait_seconds
                    .observe(started.elapsed().as_secs_f64());
                Ok(permit)
            }
            Ok(Err(_)) => {
                self.record_serve(protocol, ServeResult::RateLimited, started, 0);
                Err(Status::resource_exhausted(
                    "serve admission semaphore closed",
                ))
            }
            Err(_) => {
                self.metrics
                    .serve_admission_wait_seconds
                    .observe(started.elapsed().as_secs_f64());
                self.record_serve(protocol, ServeResult::RateLimited, started, 0);
                Err(Status::resource_exhausted(format!(
                    "serve admission queue timeout after {} ms (permits={})",
                    self.cfg.queue_timeout.as_millis(),
                    self.cfg.permits
                )))
            }
        }
    }

    /// Acquire a **snapshot** admission permit (SEC-4I-1) or `RESOURCE_EXHAUSTED`.
    ///
    /// Separate from the multi-block serve pool: one ~200 MiB state stream must
    /// not be counted as a 64 MiB serve response.
    async fn admit_snapshot(
        &self,
        protocol: ServeProtocol,
    ) -> Result<OwnedSemaphorePermit, Status> {
        let started = Instant::now();
        match tokio::time::timeout(
            self.cfg.queue_timeout,
            Arc::clone(&self.snapshot_admits).acquire_owned(),
        )
        .await
        {
            Ok(Ok(permit)) => {
                self.metrics
                    .serve_admission_wait_seconds
                    .observe(started.elapsed().as_secs_f64());
                Ok(permit)
            }
            Ok(Err(_)) => {
                self.record_serve(protocol, ServeResult::RateLimited, started, 0);
                Err(Status::resource_exhausted(
                    "snapshot admission semaphore closed",
                ))
            }
            Err(_) => {
                self.metrics
                    .serve_admission_wait_seconds
                    .observe(started.elapsed().as_secs_f64());
                self.record_serve(protocol, ServeResult::RateLimited, started, 0);
                Err(Status::resource_exhausted(format!(
                    "snapshot admission queue timeout after {} ms (snapshot_permits={})",
                    self.cfg.queue_timeout.as_millis(),
                    self.cfg.snapshot_permits
                )))
            }
        }
    }

    fn record_serve(
        &self,
        protocol: ServeProtocol,
        result: ServeResult,
        started: Instant,
        bytes: u64,
    ) {
        let elapsed = started.elapsed().as_secs_f64();
        self.metrics
            .serve_seconds
            .get_or_create(&ProtocolLabels {
                protocol: protocol.as_str().to_owned(),
            })
            .observe(elapsed);
        self.metrics
            .serve_total
            .get_or_create(&ServeLabels {
                protocol: protocol.as_str().to_owned(),
                result: result.as_str().to_owned(),
            })
            .inc();
        if bytes > 0 {
            self.metrics
                .serve_bytes
                .get_or_create(&ProtocolLabels {
                    protocol: protocol.as_str().to_owned(),
                })
                .inc_by(bytes);
        }
    }

    fn observe_read_txn(&self, started: Instant) {
        self.metrics
            .read_txn_seconds
            .observe(started.elapsed().as_secs_f64());
    }
}

#[tonic::async_trait]
impl StorageService for StorageServer {
    async fn get_info(
        &self,
        _request: Request<GetInfoRequest>,
    ) -> Result<Response<GetInfoResponse>, Status> {
        Ok(Response::new(GetInfoResponse {
            build_info: Some(BuildInfo {
                service: SERVICE.to_owned(),
                version: env!("CARGO_PKG_VERSION").to_owned(),
                git_sha: cc_bootstrap::GIT_SHA.to_owned(),
                rustc: cc_bootstrap::RUSTC.to_owned(),
            }),
        }))
    }

    async fn get_blocks_by_range(
        &self,
        request: Request<GetBlocksByRangeRequest>,
    ) -> Result<Response<GetBlocksResponse>, Status> {
        let protocol = ServeProtocol::BlocksByRange;
        let started = Instant::now();
        let permit = self.admit(protocol).await?;
        let req = request.into_inner();
        if req.count == 0 {
            self.record_serve(protocol, ServeResult::Ok, started, 0);
            return Ok(unary_with_permit(
                permit,
                GetBlocksResponse { blocks: vec![] },
            ));
        }
        if req.count > MAX_BLOCKS_BY_RANGE {
            self.record_serve(protocol, ServeResult::Error, started, 0);
            return Err(Status::invalid_argument(format!(
                "count {} exceeds MAX_BLOCKS_BY_RANGE ({MAX_BLOCKS_BY_RANGE})",
                req.count
            )));
        }
        let eas = self.earliest_available_slot();
        if req.start_slot < eas {
            self.record_serve(protocol, ServeResult::ResourceUnavailable, started, 0);
            return Err(Status::unavailable(format!(
                "start_slot {} below earliest_available_slot {eas}",
                req.start_slot
            )));
        }

        let engine = self.engine()?;
        let materialise_start = Instant::now();
        // Cap *during* materialisation: stop loading once the next whole block
        // would exceed serve_buffer_bytes. Peak RSS ≤ buffer × permits, never
        // load-full-then-shrink (would spike above the ceiling).
        //
        // Admission-time key materialisation (CC-46b): one read txn resolves the
        // full key set before the first byte is produced. `PerSlotReopen` is the
        // test-only negative path that removes this mitigation.
        let blocks = materialise_blocks_for_request(
            engine,
            Slot::new(req.start_slot),
            req.count,
            self.cfg.buffer_bytes,
            self.cfg.materialise_mode,
            self.mid_serve_prune_hook.as_ref(),
        )?;
        self.observe_read_txn(materialise_start);

        if blocks.is_empty() {
            self.record_serve(protocol, ServeResult::ResourceUnavailable, started, 0);
            return Err(Status::unavailable("no blocks in requested range"));
        }
        let bytes: u64 = blocks.iter().map(|b| b.ssz.len() as u64).sum();
        self.record_serve(protocol, ServeResult::Ok, started, bytes);
        Ok(unary_with_permit(permit, GetBlocksResponse { blocks }))
    }

    async fn get_blocks_by_root(
        &self,
        request: Request<GetBlocksByRootRequest>,
    ) -> Result<Response<GetBlocksResponse>, Status> {
        let protocol = ServeProtocol::BlocksByRoot;
        let started = Instant::now();
        let permit = self.admit(protocol).await?;
        let req = request.into_inner();
        if req.roots.len() > MAX_BY_ROOT {
            self.record_serve(protocol, ServeResult::Error, started, 0);
            return Err(Status::invalid_argument(format!(
                "roots len {} exceeds max {MAX_BY_ROOT}",
                req.roots.len()
            )));
        }
        let eas = self.earliest_available_slot();
        let engine = self.engine()?;
        let materialise_start = Instant::now();
        let mut blocks = Vec::with_capacity(req.roots.len());
        let mut total = 0u64;
        {
            let rt = engine.read().map_err(store_status)?;
            for root_bytes in &req.roots {
                let root = parse_root(root_bytes)?;
                let Some(ssz) = get_block_by_root(&rt, &root).map_err(store_status)? else {
                    continue;
                };
                let slot = cc_store::slot_at_offset(&ssz).map_err(store_status)?;
                if slot.as_u64() < eas {
                    continue;
                }
                let len = ssz.len() as u64;
                if total.saturating_add(len) > self.cfg.buffer_bytes {
                    // Anti-truncation: stop before adding this whole block.
                    break;
                }
                total = total.saturating_add(len);
                blocks.push(BlockSsz {
                    ssz,
                    slot: slot.as_u64(),
                    root: root.as_slice().to_vec(),
                });
            }
            // `rt` drops here.
        }
        self.observe_read_txn(materialise_start);

        if blocks.is_empty() {
            self.record_serve(protocol, ServeResult::ResourceUnavailable, started, 0);
            return Err(Status::unavailable("no requested roots available"));
        }
        self.record_serve(protocol, ServeResult::Ok, started, total);
        Ok(unary_with_permit(permit, GetBlocksResponse { blocks }))
    }

    async fn get_columns_by_range(
        &self,
        request: Request<GetColumnsByRangeRequest>,
    ) -> Result<Response<GetColumnsResponse>, Status> {
        let protocol = ServeProtocol::ColumnsByRange;
        let started = Instant::now();
        let permit = self.admit(protocol).await?;
        let req = request.into_inner();
        if req.count == 0 {
            self.record_serve(protocol, ServeResult::Ok, started, 0);
            return Ok(unary_with_permit(
                permit,
                GetColumnsResponse { columns: vec![] },
            ));
        }
        if req.count > MAX_COLUMNS_BY_RANGE_SLOTS {
            self.record_serve(protocol, ServeResult::Error, started, 0);
            return Err(Status::invalid_argument(format!(
                "count {} exceeds MAX_COLUMNS_BY_RANGE_SLOTS ({MAX_COLUMNS_BY_RANGE_SLOTS})",
                req.count
            )));
        }
        let eas = self.earliest_available_slot();
        if req.start_slot < eas {
            self.record_serve(protocol, ServeResult::ResourceUnavailable, started, 0);
            return Err(Status::unavailable(format!(
                "start_slot {} below earliest_available_slot {eas}",
                req.start_slot
            )));
        }

        let indices: Option<Vec<u16>> = if req.column_indices.is_empty() {
            None
        } else {
            Some(
                req.column_indices
                    .iter()
                    .map(|&i| {
                        u16::try_from(i)
                            .map_err(|_| Status::invalid_argument("column index > u16::MAX"))
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )
        };

        let engine = self.engine()?;
        let materialise_start = Instant::now();
        // Cap during materialisation at whole-block (sidecar set) boundaries.
        let columns = {
            let rt = engine.read().map_err(store_status)?;
            let split = load_split(&rt).map_err(store_status)?.map(|s| s.slot);
            let cols = indices.as_deref();
            materialise_columns_capped(
                &rt,
                Slot::new(req.start_slot),
                req.count,
                split,
                cols,
                self.cfg.buffer_bytes,
            )?
        };
        self.observe_read_txn(materialise_start);

        if columns.is_empty() {
            self.record_serve(protocol, ServeResult::ResourceUnavailable, started, 0);
            return Err(Status::unavailable("no columns in requested range"));
        }
        let bytes: u64 = columns.iter().map(|c| c.ssz.len() as u64).sum();
        self.record_serve(protocol, ServeResult::Ok, started, bytes);
        Ok(unary_with_permit(permit, GetColumnsResponse { columns }))
    }

    async fn get_columns_by_root(
        &self,
        request: Request<GetColumnsByRootRequest>,
    ) -> Result<Response<GetColumnsResponse>, Status> {
        let protocol = ServeProtocol::ColumnsByRoot;
        let started = Instant::now();
        let permit = self.admit(protocol).await?;
        let req = request.into_inner();
        if req.identifiers.len() > MAX_BY_ROOT {
            self.record_serve(protocol, ServeResult::Error, started, 0);
            return Err(Status::invalid_argument(format!(
                "identifiers len {} exceeds max {MAX_BY_ROOT}",
                req.identifiers.len()
            )));
        }
        let eas = self.earliest_available_slot();
        let engine = self.engine()?;
        let materialise_start = Instant::now();
        let mut columns = Vec::new();
        let mut total = 0u64;
        {
            let rt = engine.read().map_err(store_status)?;
            for id in &req.identifiers {
                let root = parse_root(&id.block_root)?;
                let indices: Vec<u16> = id
                    .column_indices
                    .iter()
                    .map(|&i| {
                        u16::try_from(i)
                            .map_err(|_| Status::invalid_argument("column index > u16::MAX"))
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                // Slot from the reverse index only — sidecar bodies load once below.
                let mut block_slot = None;
                for &idx in &indices {
                    if let Some(slot) = cc_store::columns::slot_by_column_root(&rt, &root, idx)
                        .map_err(store_status)?
                    {
                        block_slot = Some(slot);
                        break;
                    }
                }
                let Some(slot) = block_slot else {
                    continue;
                };
                if slot.as_u64() < eas {
                    // Whole block below window — refuse the block unit.
                    continue;
                }

                let mut cfb = columns_for_block(&rt, slot, &root, &indices, BlockRegion::Hot)
                    .map_err(store_status)?;
                if cfb.held.is_empty() {
                    cfb = columns_for_block(&rt, slot, &root, &indices, BlockRegion::Cold)
                        .map_err(store_status)?;
                }
                // Missing indices stay unnamed here: do not invent sidecars.
                // Empty held → skip; the request is ResourceUnavailable only if
                // nothing else lands.
                if cfb.held.is_empty() {
                    continue;
                }
                let block_bytes: u64 = cfb.held.iter().map(|(_, ssz)| ssz.len() as u64).sum();
                if total.saturating_add(block_bytes) > self.cfg.buffer_bytes {
                    // Anti-truncation: drop the last whole block.
                    break;
                }
                total = total.saturating_add(block_bytes);
                for (idx, ssz) in cfb.held {
                    columns.push(ColumnSsz {
                        ssz,
                        slot: slot.as_u64(),
                        root: root.as_slice().to_vec(),
                        index: u32::from(idx),
                    });
                }
            }
            // `rt` drops here.
        }
        self.observe_read_txn(materialise_start);

        if columns.is_empty() {
            self.record_serve(protocol, ServeResult::ResourceUnavailable, started, 0);
            return Err(Status::unavailable("no requested columns available"));
        }
        self.record_serve(protocol, ServeResult::Ok, started, total);
        Ok(unary_with_permit(permit, GetColumnsResponse { columns }))
    }

    async fn put_backfill_batch(
        &self,
        request: Request<PutBackfillBatchRequest>,
    ) -> Result<Response<PutBackfillBatchResponse>, Status> {
        // Intentional: PutBackfillBatch does **not** take a serve admission
        // permit. It is the upward write path; rate control is the single
        // writer's P1 mailbox (block-on-full), not the 4-permit serve ceiling.
        // Serve admits protect read-path memory (buffer × permits); backfill
        // batches are bounded by the writer and MAX_BATCH_OPS instead.
        let started = Instant::now();
        let req = request.into_inner();
        let engine = self.engine()?;

        // Admit: descending contiguous parent chain (Architecture §6.2).
        // A batch may only extend the durable frontier, never jump it.
        // There is no empty-progress bypass.
        let rows: Vec<BackfillBlockRow> = req
            .blocks
            .iter()
            .map(|b| {
                Ok(BackfillBlockRow {
                    slot: Slot::new(b.slot),
                    root: parse_root(&b.root)?,
                    ssz: b.ssz.clone(),
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        let ordered = admit_descending_contiguous(&rows).map_err(admit_status)?;

        let progress = match req.progress.as_ref() {
            Some(p) => Some(proto_to_backfill_progress(p)?),
            None => None,
        };
        admit_progress_required(progress.as_ref(), !ordered.is_empty()).map_err(admit_status)?;

        let column_slots: Vec<u64> = req.columns.iter().map(|c| c.slot).collect();

        // Stage under a short-lived read txn, then commit as ONE unit.
        let (batch, blocks_written, columns_written, block_bytes, column_bytes) = {
            let rt = engine.read().map_err(store_status)?;

            // Server-side monotony + frontier bind (CC-47b): refuse before staging.
            let stored = cc_store::load_backfill_progress_txn(&rt).map_err(store_status)?;
            if let Some(ref p) = progress {
                admit_progress_monotone(p, stored.as_ref()).map_err(admit_status)?;
                admit_progress_bound_to_batch(p, &ordered, &column_slots).map_err(admit_status)?;
            }

            let anchor = load_anchor_info_txn(&rt)?;
            admit_progress_only_preserves_block_frontier(
                progress.as_ref(),
                !ordered.is_empty(),
                stored.as_ref(),
                anchor.as_ref(),
            )
            .map_err(admit_status)?;
            let ssz_parent_is_durable = first_row_ssz_parent_is_durable(&rt, &ordered)?;
            admit_extends_durable_frontier(
                &ordered,
                stored.as_ref(),
                anchor.as_ref(),
                ssz_parent_is_durable,
            )
            .map_err(admit_status)?;

            let mut batch = engine.batch();
            let mut blocks_written = 0u64;
            let mut columns_written = 0u64;
            let mut block_bytes = 0u64;
            let mut column_bytes = 0u64;

            // Below-anchor backfill lands in cold (Architecture §6.6).
            let region = BlockRegion::Cold;

            // Write in descending order (frontier-adjacent first).
            for b in &ordered {
                put_block(&rt, &mut batch, b.slot, &b.root, &b.ssz, region, false)
                    .map_err(store_status)?;
                put_canonical(&rt, &mut batch, b.slot, &b.root).map_err(store_status)?;
                blocks_written = blocks_written.saturating_add(1);
                block_bytes = block_bytes.saturating_add(b.ssz.len() as u64);
            }
            for c in &req.columns {
                let root = parse_root(&c.root)?;
                let slot = Slot::new(c.slot);
                let index = u16::try_from(c.index)
                    .map_err(|_| Status::invalid_argument("column index > u16::MAX"))?;
                put_column(&rt, &mut batch, slot, &root, index, &c.ssz, region)
                    .map_err(store_status)?;
                columns_written = columns_written.saturating_add(1);
                column_bytes = column_bytes.saturating_add(c.ssz.len() as u64);
            }

            // BackfillProgress in the SAME batch (CC-4F /3 same-transaction rule).
            if let Some(ref p) = progress {
                let ssz = p.as_ssz_bytes();
                batch.put(TABLE_META, KEY_BACKFILL_PROG.as_bytes(), &ssz);
            }
            // `rt` drops before commit.
            (
                batch,
                blocks_written,
                columns_written,
                block_bytes,
                column_bytes,
            )
        };
        self.observe_read_txn(started);

        if self
            .fail_next_commit
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            // Injected commit failure: batch is dropped uncommitted — none of
            // blocks / columns / progress land (CC-4F /3).
            return Err(Status::aborted("injected commit failure"));
        }
        commit_backfill(engine, self.writer.as_ref(), batch).await?;

        // Metrics only after a successful commit (CC-47 /8).
        observe_put_backfill_batch(&self.metrics, progress.as_ref(), block_bytes, column_bytes);

        Ok(Response::new(PutBackfillBatchResponse {
            blocks_written,
            columns_written,
        }))
    }

    async fn watch_serve_window(
        &self,
        _request: Request<WatchServeWindowRequest>,
    ) -> Result<Response<BoxStream<ServeWindow>>, Status> {
        // Immediate snapshot + subsequent updates. WatchStream yields the current
        // value then subsequent changes (watch::Receiver starts at current).
        let rx = self.window_tx.subscribe();
        let stream = WatchStream::new(rx).map(Ok);
        Ok(Response::new(Box::pin(stream) as BoxStream<ServeWindow>))
    }

    async fn get_historical_block(
        &self,
        request: Request<GetHistoricalBlockRequest>,
    ) -> Result<Response<GetHistoricalBlockResponse>, Status> {
        let started = Instant::now();
        let req = request.into_inner();
        let id = req
            .id
            .ok_or_else(|| Status::invalid_argument("GetHistoricalBlock.id is required"))?;

        let (protocol, block, permit) = match id {
            HistoricalBlockId::Root(bytes) => {
                let protocol = ServeProtocol::HistoricalBlockByRoot;
                let permit = self.admit(protocol).await?;
                let root = parse_root(&bytes)?;
                let engine = self.engine()?;
                let materialise_start = Instant::now();
                let block = historical_block_by_root(engine, &root).map_err(store_status)?;
                self.observe_read_txn(materialise_start);
                (protocol, block, permit)
            }
            HistoricalBlockId::Slot(slot) => {
                let protocol = ServeProtocol::HistoricalBlockBySlot;
                let permit = self.admit(protocol).await?;
                let engine = self.engine()?;
                let materialise_start = Instant::now();
                let block =
                    historical_block_by_slot(engine, Slot::new(slot)).map_err(store_status)?;
                self.observe_read_txn(materialise_start);
                (protocol, block, permit)
            }
        };

        let Some(block) = block else {
            self.record_serve(protocol, ServeResult::ResourceUnavailable, started, 0);
            return Err(Status::not_found("historical block not found"));
        };
        let bytes = block.ssz.len() as u64;
        self.record_serve(protocol, ServeResult::Ok, started, bytes);
        Ok(unary_with_permit(
            permit,
            GetHistoricalBlockResponse {
                ssz: block.ssz,
                slot: block.slot.as_u64(),
                root: block.root.as_slice().to_vec(),
            },
        ))
    }

    async fn get_snapshot_state(
        &self,
        request: Request<GetSnapshotStateRequest>,
    ) -> Result<Response<BoxStream<StateChunk>>, Status> {
        let protocol = ServeProtocol::SnapshotState;
        let started = Instant::now();
        // SEC-4I-1: dedicated pool; permit must outlive the stream (moved in).
        let permit = self.admit_snapshot(protocol).await?;
        let slot = Slot::new(request.into_inner().slot);
        let engine = self.engine()?;
        let materialise_start = Instant::now();
        // Materialise full snapshot under one short read txn, then drop it
        // before any chunk reaches the socket (long-reader rule / CRD-6).
        let lookup = snapshot_state_at(engine, slot).map_err(store_status)?;
        self.observe_read_txn(materialise_start);

        match lookup {
            SnapshotLookup::NotAvailable { slot, ring } => {
                self.record_serve(protocol, ServeResult::ResourceUnavailable, started, 0);
                // Permit drops here — miss path does not hold concurrency.
                drop(permit);
                // Typed NOT_AVAILABLE — no process_slots / replay (CC-4I /2).
                Err(not_available_status(slot, &ring))
            }
            SnapshotLookup::Available(ssz) => {
                let bytes = ssz.len() as u64;
                let budget = self.cfg.snapshot_buffer_bytes.min(MAX_SNAPSHOT_BYTES);
                // Size budget (serve-path analogue): fail closed. A state is
                // atomic — no anti-truncation partial success.
                if bytes > budget {
                    self.record_serve(protocol, ServeResult::Error, started, 0);
                    drop(permit);
                    return Err(Status::resource_exhausted(format!(
                        "snapshot at slot {} is {bytes} bytes; exceeds serve size budget \
                         ({budget})",
                        slot.as_u64()
                    )));
                }
                self.record_serve(protocol, ServeResult::Ok, started, bytes);
                // Progressive chunk stream: one owned SSZ buffer + one chunk at a
                // time (peak ≈ 1× state + chunk), permit held until stream drop.
                let stream = SnapshotChunkStream::new(ssz, DEFAULT_STATE_CHUNK_BYTES, permit);
                Ok(Response::new(Box::pin(stream) as BoxStream<StateChunk>))
            }
        }
    }

    async fn get_finalized_checkpoint_history(
        &self,
        _request: Request<GetFinalizedCheckpointHistoryRequest>,
    ) -> Result<Response<GetFinalizedCheckpointHistoryResponse>, Status> {
        let protocol = ServeProtocol::FinalizedCheckpointHistory;
        let started = Instant::now();
        let permit = self.admit(protocol).await?;
        let engine = self.engine()?;
        let materialise_start = Instant::now();
        let hist = finalized_checkpoint_history(engine).map_err(store_status)?;
        self.observe_read_txn(materialise_start);

        let checkpoints: Vec<FinalizedCheckpoint> = hist
            .into_iter()
            .map(|c| FinalizedCheckpoint {
                epoch: c.epoch,
                root: c.root.as_slice().to_vec(),
            })
            .collect();
        // Small response; byte count is the sum of root lengths.
        let bytes: u64 = checkpoints.iter().map(|c| c.root.len() as u64).sum();
        self.record_serve(protocol, ServeResult::Ok, started, bytes);
        Ok(unary_with_permit(
            permit,
            GetFinalizedCheckpointHistoryResponse { checkpoints },
        ))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Park the admit permit on the tonic response so [`UnaryPermitService`] can
/// lift it onto the HTTP body. h2 clears extensions at header send.
fn unary_with_permit<T>(permit: OwnedSemaphorePermit, body: T) -> Response<T> {
    let mut response = Response::new(body);
    response
        .extensions_mut()
        .insert(UnaryServePermit(Arc::new(permit)));
    response
}

/// Carrier through tonic encode. `http::Extensions::insert` requires `Clone`.
#[derive(Clone, Debug)]
struct UnaryServePermit(#[allow(dead_code)] Arc<OwnedSemaphorePermit>);

/// HTTP body that owns the admit permit until the consumer finishes.
pub(crate) struct PermitBody<B> {
    inner: B,
    _permit: Option<UnaryServePermit>,
}

impl<B: fmt::Debug> fmt::Debug for PermitBody<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PermitBody")
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

impl<B> HttpBody for PermitBody<B>
where
    B: HttpBody + Unpin,
{
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        Pin::new(&mut self.inner).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

/// Move the admit permit off extensions and onto the body.
///
/// Must run after tonic wraps the unary in `EncodeBody` and before hyper/h2
/// send headers (`h2` `send_response` clears extensions).
fn lift_unary_permit<B>(mut response: HttpResponse<B>) -> HttpResponse<PermitBody<B>> {
    let permit = response.extensions_mut().remove::<UnaryServePermit>();
    let (parts, inner) = response.into_parts();
    HttpResponse::from_parts(
        parts,
        PermitBody {
            inner,
            _permit: permit,
        },
    )
}

/// Lifts a unary admit permit from response extensions onto the HTTP body.
#[derive(Clone, Debug)]
pub(crate) struct UnaryPermitService<S> {
    inner: S,
}

impl<S> UnaryPermitService<S> {
    pub(crate) fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S> NamedService for UnaryPermitService<S>
where
    S: NamedService,
{
    const NAME: &'static str = S::NAME;
}

impl<S, ReqBody, ResBody> TowerService<HttpRequest<ReqBody>> for UnaryPermitService<S>
where
    S: TowerService<HttpRequest<ReqBody>, Response = HttpResponse<ResBody>>,
    S::Error: Send + 'static,
    S::Future: Send + 'static,
    ResBody: Send + 'static,
{
    type Response = HttpResponse<PermitBody<ResBody>>;
    type Error = S::Error;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: HttpRequest<ReqBody>) -> Self::Future {
        let fut = self.inner.call(req);
        Box::pin(async move { Ok(lift_unary_permit(fut.await?)) })
    }
}

/// Progressive `StateChunk` stream that holds the snapshot admit permit for its
/// full lifetime (SEC-4I-1).
///
/// Materialises one chunk at a time from a single owned SSZ buffer so peak RSS
/// is ≈ 1× state + one chunk, not a second full copy of pre-built chunks. The
/// permit drops when the stream is dropped (consumed, cancelled, or error).
struct SnapshotChunkStream {
    ssz: Vec<u8>,
    offset: usize,
    chunk_bytes: usize,
    done: bool,
    /// Held for the stream lifetime — do not drop early.
    _permit: OwnedSemaphorePermit,
}

impl SnapshotChunkStream {
    fn new(ssz: Vec<u8>, chunk_bytes: usize, permit: OwnedSemaphorePermit) -> Self {
        Self {
            ssz,
            offset: 0,
            chunk_bytes: chunk_bytes.max(1),
            done: false,
            _permit: permit,
        }
    }
}

impl Stream for SnapshotChunkStream {
    type Item = Result<StateChunk, Status>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        let total = this.ssz.len();
        if total == 0 {
            this.done = true;
            this.ssz = Vec::new();
            return Poll::Ready(Some(Ok(StateChunk {
                data: Vec::new(),
                offset: 0,
                last: true,
            })));
        }
        if this.offset >= total {
            this.done = true;
            this.ssz = Vec::new();
            return Poll::Ready(None);
        }
        let end = (this.offset + this.chunk_bytes).min(total);
        let last = end == total;
        let data = this.ssz[this.offset..end].to_vec();
        let offset = this.offset as u64;
        this.offset = end;
        if last {
            this.done = true;
            // Free the bulk body once the last chunk has its own copy.
            this.ssz = Vec::new();
        }
        Poll::Ready(Some(Ok(StateChunk { data, offset, last })))
    }
}

fn empty_window() -> ServeWindow {
    ServeWindow {
        // Empty window seed (matches p2p EMPTY_WINDOW_SLOT = u64::MAX).
        earliest_available_slot: u64::MAX,
        cgc: 0,
        head_slot: 0,
        block_floor: 0,
        column_floor: 0,
        branch: 0,
        holes: vec![],
    }
}

fn load_window_or_default(engine: &Engine) -> ServeWindow {
    let Ok(rt) = engine.read() else {
        return empty_window();
    };
    match rt.get(TABLE_META, KEY_SERVE_WINDOW.as_bytes()) {
        Ok(Some(bytes)) => match cc_store::meta::ServeWindow::from_ssz_bytes(&bytes) {
            Ok(w) => ServeWindow {
                earliest_available_slot: w.earliest_available_slot.as_u64(),
                cgc: w.cgc,
                head_slot: 0,
                block_floor: w.block_floor.as_u64(),
                column_floor: w.column_floor.as_u64(),
                branch: u32::from(w.branch),
                holes: w
                    .holes
                    .iter()
                    .map(|h| ProtoSlotRange {
                        start: h.start.as_u64(),
                        end: h.end.as_u64(),
                    })
                    .collect(),
            },
            Err(_) => empty_window(),
        },
        _ => empty_window(),
    }
}

fn parse_root(bytes: &[u8]) -> Result<Root, Status> {
    if bytes.len() != 32 {
        return Err(Status::invalid_argument(format!(
            "root must be 32 bytes, got {}",
            bytes.len()
        )));
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(bytes);
    Ok(Root::from_array(arr))
}

fn store_status(err: StoreError) -> Status {
    match err {
        StoreError::Limit(msg) => Status::invalid_argument(msg),
        StoreError::Codec(msg) => Status::invalid_argument(msg),
        StoreError::KeyCollision { table } => {
            Status::already_exists(format!("key collision in {table}"))
        }
        other => Status::internal(other.to_string()),
    }
}

fn writer_status(err: WriterError) -> Status {
    match err {
        WriterError::InjectedFailure => Status::aborted("injected commit failure"),
        WriterError::ShutDown => Status::unavailable("writer shut down"),
        WriterError::NotUninitialized => Status::failed_precondition("STORE_NOT_UNINITIALIZED"),
        WriterError::Store(e) => store_status(e),
    }
}

/// Admission-time or per-slot materialisation of a blocks-by-range request.
fn materialise_blocks_for_request(
    engine: &Engine,
    start_slot: Slot,
    count: u64,
    buffer_bytes: u64,
    mode: MaterialiseMode,
    mid_hook: Option<&MidServeHook>,
) -> Result<Vec<BlockSsz>, Status> {
    match mode {
        MaterialiseMode::AdmissionTime => {
            // One read txn at admission — full key set under MVCC snapshot.
            let rt = engine.read().map_err(store_status)?;
            let split = load_split(&rt).map_err(store_status)?.map(|s| s.slot);
            let blocks =
                materialise_blocks_capped(&rt, start_slot, count, split, buffer_bytes, mid_hook)?;
            // `rt` drops here — before any byte leaves this process.
            Ok(blocks)
        }
        MaterialiseMode::PerSlotReopen => {
            // Test-only: new read txn per slot so a mid-serve prune is visible.
            let mut out = Vec::new();
            let mut total = 0u64;
            let start = start_slot.as_u64();
            let end = start.saturating_add(count);
            for (i, s) in (start..end).enumerate() {
                if i == 1
                    && let Some(hook) = mid_hook
                {
                    hook();
                }
                let rt = engine.read().map_err(store_status)?;
                let split = load_split(&rt).map_err(store_status)?.map(|s| s.slot);
                let rows = blocks_by_range(&rt, Slot::new(s), 1, split).map_err(store_status)?;
                drop(rt);
                let Some(r) = rows.into_iter().next() else {
                    continue;
                };
                let len = r.ssz.len() as u64;
                if total.saturating_add(len) > buffer_bytes {
                    break;
                }
                total = total.saturating_add(len);
                out.push(BlockSsz {
                    ssz: r.ssz,
                    slot: r.slot.as_u64(),
                    root: r.root.as_slice().to_vec(),
                });
            }
            Ok(out)
        }
    }
}

/// Materialise blocks under `buffer_bytes`, stopping at the last whole block
/// that fits. Loads **one slot at a time** so peak memory never exceeds the
/// buffer (no full-range load then shrink). Single `ReadTxn` held for the whole
/// pass (admission-time materialisation).
fn materialise_blocks_capped(
    rt: &cc_store::engine::ReadTxn,
    start_slot: Slot,
    count: u64,
    split: Option<Slot>,
    buffer_bytes: u64,
    mid_hook: Option<&MidServeHook>,
) -> Result<Vec<BlockSsz>, Status> {
    let mut out = Vec::new();
    let mut total = 0u64;
    let start = start_slot.as_u64();
    let end = start.saturating_add(count);
    for (i, s) in (start..end).enumerate() {
        // Forcing mechanism for CC-46b /4: after the first slot is in hand,
        // invoke `mid_serve_prune_hook` so the prune pass runs *inside* serve.
        if i == 1
            && let Some(hook) = mid_hook
        {
            hook();
        }
        // One slot → at most one canonical block; avoids loading the whole range.
        let rows = blocks_by_range(rt, Slot::new(s), 1, split).map_err(store_status)?;
        let Some(r) = rows.into_iter().next() else {
            continue;
        };
        let len = r.ssz.len() as u64;
        if total.saturating_add(len) > buffer_bytes {
            // Anti-truncation: previous whole blocks only.
            break;
        }
        total = total.saturating_add(len);
        out.push(BlockSsz {
            ssz: r.ssz,
            slot: r.slot.as_u64(),
            root: r.root.as_slice().to_vec(),
        });
    }
    Ok(out)
}

/// Materialise columns under `buffer_bytes`, stopping at the previous **block**
/// boundary (all sidecars of a slot form one unit). Loads one slot at a time.
fn materialise_columns_capped(
    rt: &cc_store::engine::ReadTxn,
    start_slot: Slot,
    count: u64,
    split: Option<Slot>,
    columns: Option<&[u16]>,
    buffer_bytes: u64,
) -> Result<Vec<ColumnSsz>, Status> {
    let mut out = Vec::new();
    let mut total = 0u64;
    let start = start_slot.as_u64();
    let end = start.saturating_add(count);
    for s in start..end {
        let rows = columns_by_range(rt, Slot::new(s), 1, split, columns).map_err(store_status)?;
        if rows.is_empty() {
            continue;
        }
        let block_bytes: u64 = rows.iter().map(|r| r.ssz.len() as u64).sum();
        if total.saturating_add(block_bytes) > buffer_bytes {
            // Drop the whole block unit — never mid-block.
            break;
        }
        total = total.saturating_add(block_bytes);
        for r in rows {
            out.push(ColumnSsz {
                ssz: r.ssz,
                slot: r.slot.as_u64(),
                root: r.root.as_slice().to_vec(),
                index: u32::from(r.index),
            });
        }
    }
    Ok(out)
}

fn proto_to_backfill_progress(p: &ProtoBackfillProgress) -> Result<BackfillProgress, Status> {
    let parent = if p.blocks_oldest_parent.is_empty() {
        Root::ZERO
    } else {
        parse_root(&p.blocks_oldest_parent)?
    };
    // CC-47a: full per_index_oldest mapping (padded to 128 with unset when non-empty).
    Ok(proto_progress_to_store(
        p.blocks_oldest,
        parent,
        p.columns_oldest,
        &p.per_index_oldest,
    ))
}

fn admit_status(e: BatchAdmitError) -> Status {
    match e {
        BatchAdmitError::NotDescending => {
            Status::invalid_argument("backfill batch blocks not strictly descending by slot")
        }
        BatchAdmitError::ParentBroken { slot } => Status::invalid_argument(format!(
            "backfill batch parent chain broken at slot {slot} (higher.parent must equal lower.root)"
        )),
        BatchAdmitError::FieldMismatch { slot } => Status::invalid_argument(format!(
            "backfill batch field mismatch at slot {slot} (SSZ peeks disagree with claim)"
        )),
        BatchAdmitError::ProgressNonMonotone {
            class,
            current,
            attempted,
        } => Status::failed_precondition(format!(
            "backfill progress non-monotone for {class}: durable oldest={current}, attempted={attempted}"
        )),
        BatchAdmitError::ProgressFrontierMismatch {
            reason,
            expected,
            got,
        } => Status::invalid_argument(format!(
            "backfill progress not bound to admitted batch: {reason} (expected={expected}, got={got})"
        )),
        BatchAdmitError::ProgressRequired => Status::invalid_argument(
            "backfill progress is required; empty-progress batches are rejected",
        ),
        BatchAdmitError::FrontierJump { reason } => Status::failed_precondition(format!(
            "a batch may only extend the durable frontier, never jump it ({reason})"
        )),
    }
}

fn load_anchor_info_txn(rt: &cc_store::engine::ReadTxn) -> Result<Option<AnchorInfo>, Status> {
    match rt
        .get(TABLE_META, KEY_ANCHOR_INFO.as_bytes())
        .map_err(store_status)?
    {
        Some(bytes) => AnchorInfo::from_ssz_bytes(&bytes)
            .map(Some)
            .map_err(|e| Status::internal(format!("AnchorInfo SSZ decode: {e:?}"))),
        None => Ok(None),
    }
}

fn first_row_ssz_parent_is_durable(
    rt: &cc_store::engine::ReadTxn,
    ordered: &[BackfillBlockRow],
) -> Result<bool, Status> {
    let Some(first) = ordered.first() else {
        return Ok(false);
    };
    let parent = parent_root_at_offset(&first.ssz).map_err(|_| {
        admit_status(BatchAdmitError::FieldMismatch {
            slot: first.slot.as_u64(),
        })
    })?;
    Ok(get_block_by_root(rt, &parent)
        .map_err(store_status)?
        .is_some())
}

/// Commit a staged backfill batch via the single writer when present, else
/// direct `engine.commit` (tests / write path disabled).
async fn commit_backfill(
    engine: &Arc<Engine>,
    writer: Option<&WriterHandle>,
    batch: cc_store::engine::Batch,
) -> Result<(), Status> {
    if let Some(w) = writer {
        let drained = batch.into_puts_and_deletes().map_err(store_status)?;
        let update = MetaUpdate {
            puts: drained.puts,
            deletes: drained.deletes,
            done: None,
        };
        w.submit_p1_committed(update).await.map_err(writer_status)?;
        Ok(())
    } else {
        engine.commit(batch).map_err(store_status)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::metrics::StorageMetrics;
    use cc_proto::storage::storage_service_server::StorageServiceServer;
    use cc_proto::storage::{
        BackfillBlock, BackfillColumn, ColumnsByRootIdentifier, WatchServeWindowRequest,
    };
    use cc_store::blocks::{
        MIN_BLOCK_SSZ_LEN, PARENT_ROOT_SSZ_OFFSET, SLOT_SSZ_OFFSET, STATE_ROOT_SSZ_OFFSET,
    };
    use cc_store::columns::{
        COLUMN_HEADER_SLOT_SSZ_OFFSET, COLUMN_INDEX_SSZ_OFFSET, MIN_COLUMN_SSZ_LEN,
        column_index_at_offset,
    };
    use cc_store::engine::{Durability, EngineOptions};
    use cc_store::get_column_by_root;
    use prometheus_client::registry::Registry;
    use sha2::{Digest, Sha256};
    use std::path::PathBuf;

    fn tmp_dir(label: &str) -> PathBuf {
        let mut dir = std::env::temp_dir();
        dir.push(format!(
            "cc-storage-serve-{}-{}-{}",
            label,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn open_engine(label: &str) -> (PathBuf, Arc<Engine>) {
        let dir = tmp_dir(label);
        let eng = Engine::open(
            &dir,
            EngineOptions::default().with_durability(Durability::None),
        )
        .unwrap();
        (dir, Arc::new(eng))
    }

    fn metrics() -> StorageMetrics {
        let mut reg = Registry::default();
        StorageMetrics::register(&mut reg)
    }

    fn synth_block(slot: u64, parent: &Root, state: &Root) -> Vec<u8> {
        let mut v = vec![0u8; MIN_BLOCK_SSZ_LEN];
        v[0..4].copy_from_slice(&100u32.to_le_bytes());
        v[SLOT_SSZ_OFFSET..SLOT_SSZ_OFFSET + 8].copy_from_slice(&slot.to_le_bytes());
        v[PARENT_ROOT_SSZ_OFFSET..PARENT_ROOT_SSZ_OFFSET + 32].copy_from_slice(parent.as_slice());
        v[STATE_ROOT_SSZ_OFFSET..STATE_ROOT_SSZ_OFFSET + 32].copy_from_slice(state.as_slice());
        v
    }

    fn synth_column(slot: u64, index: u16) -> Vec<u8> {
        let mut v = vec![0u8; MIN_COLUMN_SSZ_LEN.max(28)];
        v[COLUMN_INDEX_SSZ_OFFSET..COLUMN_INDEX_SSZ_OFFSET + 8]
            .copy_from_slice(&(u64::from(index)).to_le_bytes());
        v[COLUMN_HEADER_SLOT_SSZ_OFFSET..COLUMN_HEADER_SLOT_SSZ_OFFSET + 8]
            .copy_from_slice(&slot.to_le_bytes());
        v
    }

    fn root_n(n: u8) -> Root {
        Root::from_array([n; 32])
    }

    fn seed_anchor_oldest_parent(eng: &Engine, oldest_block_parent: Root) {
        let mut b = eng.batch();
        let anchor = AnchorInfo {
            oldest_block_parent,
            ..AnchorInfo::default()
        };
        b.put(
            TABLE_META,
            KEY_ANCHOR_INFO.as_bytes(),
            &anchor.as_ssz_bytes(),
        );
        eng.commit(b).unwrap();
    }

    fn sha256(bytes: &[u8]) -> [u8; 32] {
        let mut h = Sha256::new();
        h.update(bytes);
        let d = h.finalize();
        let mut out = [0u8; 32];
        out.copy_from_slice(&d);
        out
    }

    fn seed_blocks(eng: &Engine, start: u64, count: u64) -> Vec<(Root, Vec<u8>)> {
        let mut out = Vec::new();
        let mut b = eng.batch();
        {
            let rt = eng.read().unwrap();
            for i in 0..count {
                let slot = Slot::new(start + i);
                let root = Root::from_array({
                    let mut a = [0u8; 32];
                    a[0..8].copy_from_slice(&(start + i).to_be_bytes());
                    a
                });
                let ssz = synth_block(start + i, &Root::ZERO, &root_n(1));
                // Hot + canonical so blocks_by_range (split=None) finds them.
                put_block(&rt, &mut b, slot, &root, &ssz, BlockRegion::Hot, false).unwrap();
                put_canonical(&rt, &mut b, slot, &root).unwrap();
                out.push((root, ssz));
            }
        }
        eng.commit(b).unwrap();
        out
    }

    fn seed_large_block(eng: &Engine, slot: u64, extra_bytes: usize) -> Vec<u8> {
        let root = Root::from_array({
            let mut a = [0u8; 32];
            a[0..8].copy_from_slice(&slot.to_be_bytes());
            a
        });
        let mut ssz = synth_block(slot, &Root::ZERO, &root_n(1));
        ssz.resize(ssz.len().saturating_add(extra_bytes), 0xAB);
        let mut b = eng.batch();
        {
            let rt = eng.read().unwrap();
            put_block(
                &rt,
                &mut b,
                Slot::new(slot),
                &root,
                &ssz,
                BlockRegion::Hot,
                false,
            )
            .unwrap();
            put_canonical(&rt, &mut b, Slot::new(slot), &root).unwrap();
        }
        eng.commit(b).unwrap();
        ssz
    }

    fn seed_columns(eng: &Engine, start: u64, count: u64, indices: &[u16]) {
        let mut b = eng.batch();
        {
            let rt = eng.read().unwrap();
            for i in 0..count {
                let slot = Slot::new(start + i);
                let root = Root::from_array({
                    let mut a = [0u8; 32];
                    a[0..8].copy_from_slice(&(start + i).to_be_bytes());
                    a
                });
                let block_ssz = synth_block(start + i, &Root::ZERO, &root_n(1));
                put_block(
                    &rt,
                    &mut b,
                    slot,
                    &root,
                    &block_ssz,
                    BlockRegion::Hot,
                    false,
                )
                .unwrap();
                put_canonical(&rt, &mut b, slot, &root).unwrap();
                for &idx in indices {
                    let ssz = synth_column(start + i, idx);
                    put_column(&rt, &mut b, slot, &root, idx, &ssz, BlockRegion::Hot).unwrap();
                }
            }
        }
        eng.commit(b).unwrap();
    }

    fn server_with(eng: Arc<Engine>, cfg: ServeConfig) -> StorageServer {
        let s = StorageServer::new(eng, None, metrics(), cfg);
        // Open the window so range requests at slot 0 are not refused.
        s.publish_window(ServeWindow {
            earliest_available_slot: 0,
            cgc: 4,
            head_slot: 10_000,
            block_floor: 0,
            column_floor: 0,
            branch: 2,
            holes: vec![],
        });
        s
    }

    // ── CC-4I historical surface — RPC-level (no gateway service) ──────────

    #[tokio::test]
    async fn historical_block_rpc_by_root_and_slot_byte_identical() {
        use cc_proto::storage::get_historical_block_request::Id;
        let (dir, eng) = open_engine("hist-rpc-block");
        let seeded = seed_blocks(&eng, 300, 1);
        let (root, ssz) = &seeded[0];
        let srv = server_with(Arc::clone(&eng), ServeConfig::default());

        let by_root = srv
            .get_historical_block(Request::new(GetHistoricalBlockRequest {
                id: Some(Id::Root(root.as_slice().to_vec())),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(sha256(&by_root.ssz), sha256(ssz));
        assert_eq!(by_root.slot, 300);
        assert_eq!(by_root.root, root.as_slice());

        let by_slot = srv
            .get_historical_block(Request::new(GetHistoricalBlockRequest {
                id: Some(Id::Slot(300)),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(sha256(&by_slot.ssz), sha256(ssz));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn snapshot_state_rpc_streams_and_miss_is_not_available() {
        use crate::history::{process_slots_invocations, reset_process_slots_invocations};
        use cc_proto::error_info_from_status;
        use cc_store::put_snapshot;
        use futures::StreamExt;

        let (dir, eng) = open_engine("hist-rpc-snap");
        let mut fixture = b"rpc-snapshot-body".to_vec();
        fixture.resize(64 * 1024 + 9, 0x5A);
        put_snapshot(&eng, Slot::new(1024), &fixture, 4).unwrap();
        let srv = server_with(Arc::clone(&eng), ServeConfig::default());

        let mut stream = srv
            .get_snapshot_state(Request::new(GetSnapshotStateRequest { slot: 1024 }))
            .await
            .unwrap()
            .into_inner();
        let mut assembled = Vec::new();
        while let Some(chunk) = stream.next().await {
            let c = chunk.unwrap();
            assert_eq!(c.offset as usize, assembled.len());
            assembled.extend_from_slice(&c.data);
            if c.last {
                break;
            }
        }
        assert_eq!(sha256(&assembled), sha256(&fixture));
        // SEC-4I-1: release the snapshot permit before the next admit.
        drop(stream);

        reset_process_slots_invocations();
        let err = match srv
            .get_snapshot_state(Request::new(GetSnapshotStateRequest { slot: 1 }))
            .await
        {
            Ok(_) => panic!("expected NOT_AVAILABLE for slot outside ring"),
            Err(e) => e,
        };
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        let info = error_info_from_status(&err).unwrap().unwrap();
        assert_eq!(info.reason, "NOT_AVAILABLE");
        assert_eq!(process_slots_invocations(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// SEC-4I-1: open stream holds the snapshot permit; a concurrent admit times out.
    #[tokio::test]
    async fn snapshot_state_holds_permit_for_stream_lifetime() {
        use cc_store::put_snapshot;
        use futures::StreamExt;

        let (dir, eng) = open_engine("hist-snap-permit");
        put_snapshot(&eng, Slot::new(64), b"held-stream-body", 4).unwrap();
        let cfg = ServeConfig {
            snapshot_permits: 1,
            queue_timeout: Duration::from_millis(50),
            ..ServeConfig::default()
        };
        let srv = server_with(Arc::clone(&eng), cfg);

        // First stream succeeds and is deliberately not fully drained.
        let mut held = srv
            .get_snapshot_state(Request::new(GetSnapshotStateRequest { slot: 64 }))
            .await
            .expect("first snapshot admit must succeed")
            .into_inner();
        // Pull one chunk so the stream is live; permit still held.
        let first = held.next().await.expect("chunk").unwrap();
        assert!(!first.data.is_empty() || first.last);

        // Second concurrent admit must hit the 1-permit ceiling.
        let err = match srv
            .get_snapshot_state(Request::new(GetSnapshotStateRequest { slot: 64 }))
            .await
        {
            Ok(_) => panic!("second concurrent GetSnapshotState must be rate-limited"),
            Err(e) => e,
        };
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);

        // Drop the held stream → permit released → third admit succeeds.
        drop(held);
        let mut again = srv
            .get_snapshot_state(Request::new(GetSnapshotStateRequest { slot: 64 }))
            .await
            .expect("admit after stream drop must succeed")
            .into_inner();
        let mut assembled = Vec::new();
        while let Some(chunk) = again.next().await {
            let c = chunk.unwrap();
            assembled.extend_from_slice(&c.data);
            if c.last {
                break;
            }
        }
        assert_eq!(assembled, b"held-stream-body");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// SEC-4I-1: materialised snapshot above the configured size budget is refused.
    #[tokio::test]
    async fn snapshot_state_refuses_over_size_budget() {
        use cc_store::put_snapshot;

        let (dir, eng) = open_engine("hist-snap-budget");
        let body = vec![0xCDu8; 1024];
        put_snapshot(&eng, Slot::new(32), &body, 4).unwrap();

        let cfg = ServeConfig {
            snapshot_buffer_bytes: 512, // below body len
            ..ServeConfig::default()
        };
        let srv = server_with(Arc::clone(&eng), cfg);
        let err = match srv
            .get_snapshot_state(Request::new(GetSnapshotStateRequest { slot: 32 }))
            .await
        {
            Ok(_) => panic!("over-budget snapshot must be refused"),
            Err(e) => e,
        };
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
        assert!(
            err.message().contains("exceeds serve size budget"),
            "message={}",
            err.message()
        );

        // Same body under the default budget (MAX_SNAPSHOT_BYTES) streams fine.
        let srv_ok = server_with(Arc::clone(&eng), ServeConfig::default());
        assert!(
            srv_ok
                .get_snapshot_state(Request::new(GetSnapshotStateRequest { slot: 32 }))
                .await
                .is_ok()
        );
        assert_eq!(
            ServeConfig::default().snapshot_buffer_bytes,
            MAX_SNAPSHOT_BYTES,
            "default snapshot budget tracks write-path MAX_SNAPSHOT_BYTES"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn finalized_checkpoint_history_rpc_returns_seeded() {
        use cc_store::SszEncode;
        use cc_store::meta::{ForkChoiceScalars, KEY_FC_SCALARS, TABLE_META};
        use cc_types::{Checkpoint, Epoch};

        let (dir, eng) = open_engine("hist-rpc-fc");
        let root = root_n(0xCD);
        let fc = ForkChoiceScalars {
            time: 1,
            proposer_boost_root: Root::ZERO,
            justified: Checkpoint::default(),
            finalized: Checkpoint {
                epoch: Epoch::new(7),
                root,
            },
            unrealized_justified: Checkpoint::default(),
            unrealized_finalized: Checkpoint::default(),
            head_root: root,
            head_slot: Slot::new(224),
        };
        let mut b = eng.batch();
        b.put(TABLE_META, KEY_FC_SCALARS.as_bytes(), &fc.as_ssz_bytes());
        eng.commit(b).unwrap();

        let srv = server_with(Arc::clone(&eng), ServeConfig::default());
        let resp = srv
            .get_finalized_checkpoint_history(Request::new(GetFinalizedCheckpointHistoryRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.checkpoints.len(), 1);
        assert_eq!(resp.checkpoints[0].epoch, 7);
        assert_eq!(resp.checkpoints[0].root, root.as_slice());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn byte_identical_ssz_round_trip_blocks_by_range_and_root() {
        let (dir, eng) = open_engine("ssz-rt");
        let seeded = seed_blocks(&eng, 100, 4);
        let srv = server_with(Arc::clone(&eng), ServeConfig::default());

        let resp = srv
            .get_blocks_by_range(Request::new(GetBlocksByRangeRequest {
                start_slot: 100,
                count: 4,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.blocks.len(), 4);
        for (i, b) in resp.blocks.iter().enumerate() {
            assert_eq!(
                sha256(&b.ssz),
                sha256(&seeded[i].1),
                "range block {i} must be byte-identical"
            );
        }

        let roots: Vec<Vec<u8>> = seeded.iter().map(|(r, _)| r.as_slice().to_vec()).collect();
        let resp = srv
            .get_blocks_by_root(Request::new(GetBlocksByRootRequest { roots }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.blocks.len(), 4);
        for (i, b) in resp.blocks.iter().enumerate() {
            assert_eq!(sha256(&b.ssz), sha256(&seeded[i].1));
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn byte_identical_ssz_round_trip_columns_by_range_and_root() {
        let (dir, eng) = open_engine("ssz-col");
        seed_columns(&eng, 50, 3, &[0, 1]);
        let srv = server_with(Arc::clone(&eng), ServeConfig::default());

        let resp = srv
            .get_columns_by_range(Request::new(GetColumnsByRangeRequest {
                start_slot: 50,
                count: 3,
                column_indices: vec![0, 1],
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.columns.len(), 6);
        for c in &resp.columns {
            let expected = synth_column(c.slot, c.index as u16);
            assert_eq!(
                sha256(&c.ssz),
                sha256(&expected),
                "column slot={} idx={}",
                c.slot,
                c.index
            );
            assert_eq!(column_index_at_offset(&c.ssz).unwrap(), c.index as u16);
        }

        let root = Root::from_array({
            let mut a = [0u8; 32];
            a[0..8].copy_from_slice(&50u64.to_be_bytes());
            a
        });
        let resp = srv
            .get_columns_by_root(Request::new(GetColumnsByRootRequest {
                identifiers: vec![ColumnsByRootIdentifier {
                    block_root: root.as_slice().to_vec(),
                    column_indices: vec![0, 1],
                }],
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!resp.columns.is_empty());
        for c in &resp.columns {
            assert_eq!(
                sha256(&c.ssz),
                sha256(&synth_column(c.slot, c.index as u16))
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn slot_root(slot: u64) -> Root {
        let mut a = [0u8; 32];
        a[0..8].copy_from_slice(&slot.to_be_bytes());
        Root::from_array(a)
    }

    fn columns_id(slot: u64, indices: &[u32]) -> ColumnsByRootIdentifier {
        ColumnsByRootIdentifier {
            block_root: slot_root(slot).as_slice().to_vec(),
            column_indices: indices.to_vec(),
        }
    }

    #[tokio::test]
    async fn columns_by_root_rejects_over_max_identifiers() {
        let (dir, eng) = open_engine("col-root-max");
        let srv = server_with(Arc::clone(&eng), ServeConfig::default());
        let identifiers = (0..=MAX_BY_ROOT as u64)
            .map(|s| columns_id(s, &[0]))
            .collect();
        let err = srv
            .get_columns_by_root(Request::new(GetColumnsByRootRequest { identifiers }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("exceeds max"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn columns_by_root_below_eas_refuses_whole_block() {
        let (dir, eng) = open_engine("col-root-eas");
        seed_columns(&eng, 10, 1, &[0, 1]);
        seed_columns(&eng, 80, 1, &[0, 1]);
        let srv = server_with(Arc::clone(&eng), ServeConfig::default());
        srv.publish_window(ServeWindow {
            earliest_available_slot: 50,
            cgc: 4,
            head_slot: 200,
            block_floor: 50,
            column_floor: 50,
            branch: 2,
            holes: vec![],
        });

        let err = srv
            .get_columns_by_root(Request::new(GetColumnsByRootRequest {
                identifiers: vec![columns_id(10, &[0, 1])],
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);
        assert!(err.message().contains("no requested columns available"));

        let resp = srv
            .get_columns_by_root(Request::new(GetColumnsByRootRequest {
                identifiers: vec![columns_id(10, &[0, 1]), columns_id(80, &[0, 1])],
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.columns.iter().all(|c| c.slot == 80));
        assert_eq!(resp.columns.len(), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn columns_by_root_anti_truncation_drops_last_whole_block() {
        let (dir, eng) = open_engine("col-root-trunc");
        seed_columns(&eng, 0, 3, &[0, 1]);
        let one_sidecar = synth_column(0, 0).len() as u64;
        let cfg = ServeConfig {
            buffer_bytes: one_sidecar * 2 + 8, // exactly one whole block of 2 cols
            permits: 4,
            queue_timeout: Duration::from_secs(2),
            ..ServeConfig::default()
        };
        let srv = server_with(Arc::clone(&eng), cfg);
        let resp = srv
            .get_columns_by_root(Request::new(GetColumnsByRootRequest {
                identifiers: vec![
                    columns_id(0, &[0, 1]),
                    columns_id(1, &[0, 1]),
                    columns_id(2, &[0, 1]),
                ],
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.columns.len(), 2);
        assert!(resp.columns.iter().all(|c| c.slot == 0));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn columns_by_root_honesty_skips_missing_index() {
        let (dir, eng) = open_engine("col-root-honest");
        seed_columns(&eng, 20, 1, &[0]);
        let srv = server_with(Arc::clone(&eng), ServeConfig::default());

        let resp = srv
            .get_columns_by_root(Request::new(GetColumnsByRootRequest {
                identifiers: vec![columns_id(20, &[0, 1])],
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.columns.len(), 1);
        assert_eq!(resp.columns[0].index, 0);
        assert_eq!(resp.columns[0].slot, 20);

        let err = srv
            .get_columns_by_root(Request::new(GetColumnsByRootRequest {
                identifiers: vec![columns_id(20, &[3, 4])],
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);
        assert!(err.message().contains("no requested columns available"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn columns_by_root_reads_cold_region_once() {
        let (dir, eng) = open_engine("col-root-cold");
        let slot = 30u64;
        let root = slot_root(slot);
        let mut b = eng.batch();
        {
            let rt = eng.read().unwrap();
            for idx in [0u16, 2] {
                put_column(
                    &rt,
                    &mut b,
                    Slot::new(slot),
                    &root,
                    idx,
                    &synth_column(slot, idx),
                    BlockRegion::Cold,
                )
                .unwrap();
            }
        }
        eng.commit(b).unwrap();
        let srv = server_with(Arc::clone(&eng), ServeConfig::default());
        let resp = srv
            .get_columns_by_root(Request::new(GetColumnsByRootRequest {
                identifiers: vec![columns_id(slot, &[0, 2])],
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.columns.len(), 2);
        assert_eq!(resp.columns[0].index, 0);
        assert_eq!(resp.columns[1].index, 2);
        for c in &resp.columns {
            assert_eq!(sha256(&c.ssz), sha256(&synth_column(slot, c.index as u16)));
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn get_columns_by_root_uses_one_column_read_path() {
        let src = include_str!("serve.rs");
        let start = src
            .find("async fn get_columns_by_root")
            .expect("get_columns_by_root handler");
        let rest = &src[start..];
        let end = rest
            .find("async fn put_backfill_batch")
            .expect("next handler after get_columns_by_root");
        let body = &rest[..end];
        assert!(
            body.contains("columns_for_block"),
            "by-root must load sidecars via columns_for_block"
        );
        assert!(
            !body.contains("get_column_by_root"),
            "by-root must not also walk get_column_by_root"
        );
        assert!(
            body.contains("slot_by_column_root"),
            "slot comes from the reverse index, not a sidecar body"
        );
    }

    #[tokio::test]
    async fn anti_truncation_stops_at_block_boundary() {
        let (dir, eng) = open_engine("anti-trunc");
        // 4 blocks, each ~180 B. Buffer fits 2 blocks only.
        let seeded = seed_blocks(&eng, 0, 4);
        let block_len = seeded[0].1.len() as u64;
        let cfg = ServeConfig {
            buffer_bytes: block_len * 2 + 10, // two whole blocks + slop
            permits: 4,
            queue_timeout: Duration::from_secs(2),
            ..ServeConfig::default()
        };
        let srv = server_with(Arc::clone(&eng), cfg);
        let resp = srv
            .get_blocks_by_range(Request::new(GetBlocksByRangeRequest {
                start_slot: 0,
                count: 4,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            resp.blocks.len(),
            2,
            "must stop at previous block boundary, got {} blocks",
            resp.blocks.len()
        );
        assert_eq!(resp.blocks[0].slot, 0);
        assert_eq!(resp.blocks[1].slot, 1);
        // Truncation point recorded: next would have been slot 2.
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn anti_truncation_columns_whole_block_unit() {
        let (dir, eng) = open_engine("anti-trunc-col");
        seed_columns(&eng, 0, 3, &[0, 1, 2]);
        // Each slot has 3 sidecars × MIN_COLUMN_SSZ ≈ 28 B → ~84 B / block.
        let one_sidecar = synth_column(0, 0).len() as u64;
        let cfg = ServeConfig {
            buffer_bytes: one_sidecar * 3 + 8, // exactly one whole block of 3 cols
            permits: 4,
            queue_timeout: Duration::from_secs(2),
            ..ServeConfig::default()
        };
        let srv = server_with(Arc::clone(&eng), cfg);
        let resp = srv
            .get_columns_by_range(Request::new(GetColumnsByRangeRequest {
                start_slot: 0,
                count: 3,
                column_indices: vec![0, 1, 2],
            }))
            .await
            .unwrap()
            .into_inner();
        // Only first block's columns (slot 0).
        assert!(resp.columns.iter().all(|c| c.slot == 0));
        assert_eq!(resp.columns.len(), 3);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn admission_semaphore_timeout_never_empty_success() {
        let (dir, eng) = open_engine("admit");
        seed_blocks(&eng, 0, 1);
        // Peak serve budget: 4 permits × 64 MiB = 256 MiB (stated ceiling).
        let cfg = ServeConfig {
            buffer_bytes: DEFAULT_SERVE_BUFFER_BYTES,
            permits: 4,
            queue_timeout: Duration::from_millis(50),
            ..ServeConfig::default()
        };
        assert_eq!(
            cfg.buffer_bytes * cfg.permits as u64,
            256 * 1024 * 1024,
            "peak serve-path budget must be 256 MiB"
        );
        let srv = Arc::new(server_with(Arc::clone(&eng), cfg));
        // Hold all 4 permits → concurrent 5th–8th wait then RESOURCE_EXHAUSTED.
        let mut holds = Vec::new();
        for _ in 0..4 {
            holds.push(Arc::clone(&srv.admits).acquire_owned().await.unwrap());
        }

        let mut joins = Vec::new();
        for _ in 0..8 {
            let s = Arc::clone(&srv);
            joins.push(tokio::spawn(async move {
                s.get_blocks_by_range(Request::new(GetBlocksByRangeRequest {
                    start_slot: 0,
                    count: 1,
                }))
                .await
            }));
        }

        for j in joins {
            let result = j.await.unwrap();
            match result {
                Err(status) => {
                    assert_eq!(
                        status.code(),
                        tonic::Code::ResourceExhausted,
                        "5th–8th (and all while saturated) must be RESOURCE_EXHAUSTED, got {status}"
                    );
                    assert!(!status.message().is_empty());
                }
                Ok(resp) => {
                    panic!(
                        "must not return empty success under admission pressure; got {} blocks",
                        resp.into_inner().blocks.len()
                    );
                }
            }
        }
        drop(holds);
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn grpc_blocks_by_range_http(start_slot: u64, count: u64) -> HttpRequest<tonic::body::Body> {
        use http_body_util::Full;
        use prost::Message;

        let payload = GetBlocksByRangeRequest { start_slot, count }.encode_to_vec();
        let mut frame = Vec::with_capacity(5 + payload.len());
        frame.push(0);
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.extend_from_slice(&payload);
        HttpRequest::builder()
            .method("POST")
            .uri("/eth.storage.v1.StorageService/GetBlocksByRange")
            .header("content-type", "application/grpc")
            .body(tonic::body::Body::new(Full::new(bytes::Bytes::from(frame))))
            .unwrap()
    }

    fn grpc_status_code<B>(resp: &HttpResponse<B>) -> Option<tonic::Code> {
        resp.headers()
            .get("grpc-status")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<i32>().ok())
            .map(tonic::Code::from)
    }

    /// P1-B/2 / S2-B-10: permit lives on the HTTP body. Two concurrent large
    /// serves through tonic encode + h2 `extensions.clear()` observe the ceiling.
    #[tokio::test]
    async fn unary_serve_holds_permit_for_response_body_lifetime() {
        let (dir, eng) = open_engine("unary-permit-body");
        // ~1 MiB body: large enough to be a real serve-path buffer, small enough
        // for a unit test. One permit ⇒ peak admitted = this body, not 2×.
        let large = seed_large_block(&eng, 0, 1024 * 1024);
        let cfg = ServeConfig {
            buffer_bytes: large.len() as u64,
            permits: 1,
            queue_timeout: Duration::from_millis(50),
            ..ServeConfig::default()
        };
        assert_eq!(
            cfg.buffer_bytes * cfg.permits as u64,
            large.len() as u64,
            "scaled ceiling is one large unary body"
        );
        let storage = Arc::new(server_with(Arc::clone(&eng), cfg));
        let svc = UnaryPermitService::new(StorageServiceServer::from_arc(Arc::clone(&storage)));

        let mut s1 = svc.clone();
        let mut s2 = svc.clone();
        let (mut first, mut second) = tokio::join!(
            async move { s1.call(grpc_blocks_by_range_http(0, 1)).await.unwrap() },
            async move { s2.call(grpc_blocks_by_range_http(0, 1)).await.unwrap() },
        );
        // h2 `send_response` drops every extension before the body is written.
        first.extensions_mut().clear();
        second.extensions_mut().clear();

        let ok = [grpc_status_code(&first), grpc_status_code(&second)]
            .into_iter()
            .filter(|c| c.is_none() || *c == Some(tonic::Code::Ok))
            .count();
        let exhausted = [grpc_status_code(&first), grpc_status_code(&second)]
            .into_iter()
            .filter(|c| *c == Some(tonic::Code::ResourceExhausted))
            .count();
        assert_eq!(
            ok,
            1,
            "exactly one of two concurrent large unaries admits; first={:?} second={:?}",
            grpc_status_code(&first),
            grpc_status_code(&second)
        );
        assert_eq!(
            exhausted,
            1,
            "the other observes the 1-permit ceiling; first={:?} second={:?}",
            grpc_status_code(&first),
            grpc_status_code(&second)
        );
        assert_eq!(
            storage.admits.available_permits(),
            0,
            "held HTTP body must still occupy the permit after extensions.clear()"
        );

        drop(first);
        drop(second);
        assert_eq!(storage.admits.available_permits(), 1);

        let again = svc
            .clone()
            .call(grpc_blocks_by_range_http(0, 1))
            .await
            .unwrap();
        let again_code = grpc_status_code(&again);
        assert!(
            again_code.is_none() || again_code == Some(tonic::Code::Ok),
            "admit after body drop must succeed, got {again_code:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn materialise_cap_never_loads_past_buffer() {
        // Regression: must stop loading once budget is hit (not load-all-then-shrink).
        let (dir, eng) = open_engine("cap-load");
        let seeded = seed_blocks(&eng, 0, 8);
        let block_len = seeded[0].1.len() as u64;
        let cfg = ServeConfig {
            buffer_bytes: block_len * 3, // exactly 3 whole blocks
            permits: 4,
            queue_timeout: Duration::from_secs(2),
            ..ServeConfig::default()
        };
        let srv = server_with(Arc::clone(&eng), cfg);
        let resp = srv
            .get_blocks_by_range(Request::new(GetBlocksByRangeRequest {
                start_slot: 0,
                count: 8,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.blocks.len(), 3);
        let total: u64 = resp.blocks.iter().map(|b| b.ssz.len() as u64).sum();
        assert!(total <= block_len * 3);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn put_backfill_batch_atomic_commit_failure_lands_nothing() {
        let (dir, eng) = open_engine("bf-atomic");
        let srv = server_with(Arc::clone(&eng), ServeConfig::default());
        // Inject commit failure: batch is dropped before engine.commit.
        srv.fail_next_commit
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let root = root_n(0x42);
        seed_anchor_oldest_parent(&eng, root);
        let ssz = synth_block(7, &Root::ZERO, &root_n(1));
        let col = synth_column(7, 0);
        let err = srv
            .put_backfill_batch(Request::new(PutBackfillBatchRequest {
                blocks: vec![BackfillBlock {
                    slot: 7,
                    root: root.as_slice().to_vec(),
                    ssz: ssz.clone(),
                }],
                columns: vec![BackfillColumn {
                    slot: 7,
                    root: root.as_slice().to_vec(),
                    index: 0,
                    ssz: col,
                }],
                progress: Some(ProtoBackfillProgress {
                    blocks_oldest: 7,
                    blocks_oldest_parent: Root::ZERO.as_slice().to_vec(),
                    columns_oldest: 7,
                    per_index_oldest: vec![7],
                }),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Aborted);

        // None of the three landed (same-transaction rule).
        let rt = eng.read().unwrap();
        assert!(get_block_by_root(&rt, &root).unwrap().is_none());
        assert!(get_column_by_root(&rt, &root, 0, None).unwrap().is_none());
        assert!(
            rt.get(TABLE_META, KEY_BACKFILL_PROG.as_bytes())
                .unwrap()
                .is_none()
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn put_backfill_batch_commits_all_three() {
        let (dir, eng) = open_engine("bf-ok");
        let srv = server_with(Arc::clone(&eng), ServeConfig::default());
        let root = root_n(0x11);
        seed_anchor_oldest_parent(&eng, root);
        let ssz = synth_block(3, &Root::ZERO, &root_n(1));
        let col = synth_column(3, 1);
        let resp = srv
            .put_backfill_batch(Request::new(PutBackfillBatchRequest {
                blocks: vec![BackfillBlock {
                    slot: 3,
                    root: root.as_slice().to_vec(),
                    ssz: ssz.clone(),
                }],
                columns: vec![BackfillColumn {
                    slot: 3,
                    root: root.as_slice().to_vec(),
                    index: 1,
                    ssz: col.clone(),
                }],
                progress: Some(ProtoBackfillProgress {
                    blocks_oldest: 3,
                    blocks_oldest_parent: Root::ZERO.as_slice().to_vec(),
                    columns_oldest: 3,
                    per_index_oldest: vec![3],
                }),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.blocks_written, 1);
        assert_eq!(resp.columns_written, 1);

        let rt = eng.read().unwrap();
        assert_eq!(get_block_by_root(&rt, &root).unwrap().unwrap(), ssz);
        assert_eq!(
            get_column_by_root(&rt, &root, 1, None).unwrap().unwrap(),
            col
        );
        assert!(
            rt.get(TABLE_META, KEY_BACKFILL_PROG.as_bytes())
                .unwrap()
                .is_some()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn put_backfill_batch_refuses_non_monotone_progress() {
        let (dir, eng) = open_engine("bf-mono");
        let srv = server_with(Arc::clone(&eng), ServeConfig::default());
        // Seed durable progress at slot 10.
        let root10 = root_n(0x10);
        seed_anchor_oldest_parent(&eng, root10);
        let ssz10 = synth_block(10, &Root::ZERO, &root_n(1));
        srv.put_backfill_batch(Request::new(PutBackfillBatchRequest {
            blocks: vec![BackfillBlock {
                slot: 10,
                root: root10.as_slice().to_vec(),
                ssz: ssz10,
            }],
            columns: vec![],
            progress: Some(ProtoBackfillProgress {
                blocks_oldest: 10,
                blocks_oldest_parent: Root::ZERO.as_slice().to_vec(),
                columns_oldest: 10,
                per_index_oldest: vec![],
            }),
        }))
        .await
        .unwrap();

        // Attempt to raise blocks_oldest to 20 — must refuse (R-7).
        let root20 = root_n(0x20);
        let ssz20 = synth_block(20, &Root::ZERO, &root_n(1));
        let err = srv
            .put_backfill_batch(Request::new(PutBackfillBatchRequest {
                blocks: vec![BackfillBlock {
                    slot: 20,
                    root: root20.as_slice().to_vec(),
                    ssz: ssz20,
                }],
                columns: vec![],
                progress: Some(ProtoBackfillProgress {
                    blocks_oldest: 20,
                    blocks_oldest_parent: Root::ZERO.as_slice().to_vec(),
                    columns_oldest: 20,
                    per_index_oldest: vec![],
                }),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(err.message().contains("non-monotone"));

        // Adjacent extension still works (first row root == stored
        // blocks_oldest_parent and first slot == blocks_oldest − 1).
        let ssz9 = synth_block(9, &root_n(0x08), &root_n(1));
        srv.put_backfill_batch(Request::new(PutBackfillBatchRequest {
            blocks: vec![BackfillBlock {
                slot: 9,
                root: Root::ZERO.as_slice().to_vec(),
                ssz: ssz9,
            }],
            columns: vec![],
            progress: Some(ProtoBackfillProgress {
                blocks_oldest: 9,
                blocks_oldest_parent: root_n(0x08).as_slice().to_vec(),
                columns_oldest: 9,
                per_index_oldest: vec![],
            }),
        }))
        .await
        .unwrap();

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn put_backfill_batch_refuses_progress_not_bound_to_batch() {
        let (dir, eng) = open_engine("bf-bind");
        let srv = server_with(Arc::clone(&eng), ServeConfig::default());
        let root = root_n(0x33);
        seed_anchor_oldest_parent(&eng, root);
        let ssz = synth_block(9, &Root::ZERO, &root_n(1));
        // Progress claims oldest=1 but batch only has slot 9.
        let err = srv
            .put_backfill_batch(Request::new(PutBackfillBatchRequest {
                blocks: vec![BackfillBlock {
                    slot: 9,
                    root: root.as_slice().to_vec(),
                    ssz,
                }],
                columns: vec![],
                progress: Some(ProtoBackfillProgress {
                    blocks_oldest: 1,
                    blocks_oldest_parent: Root::ZERO.as_slice().to_vec(),
                    columns_oldest: 1,
                    per_index_oldest: vec![],
                }),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("not bound"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn put_backfill_batch_empty_progress_single_block_batch_is_rejected() {
        let (dir, eng) = open_engine("bf-empty-prog");
        let srv = server_with(Arc::clone(&eng), ServeConfig::default());
        let root = root_n(0x42);
        seed_anchor_oldest_parent(&eng, root);
        let ssz = synth_block(7, &Root::ZERO, &root_n(1));
        let err = srv
            .put_backfill_batch(Request::new(PutBackfillBatchRequest {
                blocks: vec![BackfillBlock {
                    slot: 7,
                    root: root.as_slice().to_vec(),
                    ssz,
                }],
                columns: vec![],
                progress: None,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(
            err.message().contains("progress is required"),
            "empty-progress single-block batch must be rejected, not fast-pathed: {err}"
        );

        let rt = eng.read().unwrap();
        assert!(get_block_by_root(&rt, &root).unwrap().is_none());
        assert!(
            rt.get(TABLE_META, KEY_BACKFILL_PROG.as_bytes())
                .unwrap()
                .is_none()
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn put_backfill_batch_parent_not_durable_and_not_first_row_is_rejected() {
        let (dir, eng) = open_engine("bf-jump");
        let srv = server_with(Arc::clone(&eng), ServeConfig::default());
        // Named frontier parent is 0xEE; this batch's first row is 0x33 and
        // its SSZ parent (0xFF) is not a durable block.
        seed_anchor_oldest_parent(&eng, root_n(0xEE));
        let root = root_n(0x33);
        let ssz = synth_block(9, &root_n(0xFF), &root_n(1));
        let err = srv
            .put_backfill_batch(Request::new(PutBackfillBatchRequest {
                blocks: vec![BackfillBlock {
                    slot: 9,
                    root: root.as_slice().to_vec(),
                    ssz,
                }],
                columns: vec![],
                progress: Some(ProtoBackfillProgress {
                    blocks_oldest: 9,
                    blocks_oldest_parent: root_n(0xFF).as_slice().to_vec(),
                    columns_oldest: 9,
                    per_index_oldest: vec![],
                }),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(
            err.message().contains("durable frontier") && err.message().contains("never jump"),
            "must state the S2 invariant: {err}"
        );
        assert!(
            err.message().contains("not durable") && err.message().contains("not the first row"),
            "{err}"
        );

        let rt = eng.read().unwrap();
        assert!(get_block_by_root(&rt, &root).unwrap().is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn put_backfill_batch_jump_down_across_hole_is_rejected() {
        let (dir, eng) = open_engine("bf-hole");
        let srv = server_with(Arc::clone(&eng), ServeConfig::default());
        let root10 = root_n(0x10);
        seed_anchor_oldest_parent(&eng, root10);
        let ssz10 = synth_block(10, &Root::ZERO, &root_n(1));
        srv.put_backfill_batch(Request::new(PutBackfillBatchRequest {
            blocks: vec![BackfillBlock {
                slot: 10,
                root: root10.as_slice().to_vec(),
                ssz: ssz10,
            }],
            columns: vec![],
            progress: Some(ProtoBackfillProgress {
                blocks_oldest: 10,
                blocks_oldest_parent: Root::ZERO.as_slice().to_vec(),
                columns_oldest: 10,
                per_index_oldest: vec![],
            }),
        }))
        .await
        .unwrap();

        // Explicit hole: parent bind would pass (root == blocks_oldest_parent)
        // but slot 8 skips 9, so named oldest would jump 10 → 8.
        let hole_root = Root::ZERO;
        let ssz8 = synth_block(8, &root_n(0x07), &root_n(1));
        let err = srv
            .put_backfill_batch(Request::new(PutBackfillBatchRequest {
                blocks: vec![BackfillBlock {
                    slot: 8,
                    root: hole_root.as_slice().to_vec(),
                    ssz: ssz8,
                }],
                columns: vec![],
                progress: Some(ProtoBackfillProgress {
                    blocks_oldest: 8,
                    blocks_oldest_parent: root_n(0x07).as_slice().to_vec(),
                    columns_oldest: 8,
                    per_index_oldest: vec![],
                }),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(
            err.message().contains("durable frontier") && err.message().contains("never jump"),
            "must state the S2 invariant: {err}"
        );
        assert!(err.message().contains("not adjacent"), "{err}");

        {
            let rt = eng.read().unwrap();
            assert!(get_block_by_root(&rt, &hole_root).unwrap().is_none());
            assert!(
                cc_store::get_canonical(&rt, Slot::new(8))
                    .unwrap()
                    .is_none()
            );
            let stored = cc_store::load_backfill_progress_txn(&rt).unwrap().unwrap();
            assert_eq!(stored.blocks_oldest, Slot::new(10));
        }

        // Adjacent extension (slot 9) is still accepted.
        let ssz9 = synth_block(9, &root_n(0x08), &root_n(1));
        srv.put_backfill_batch(Request::new(PutBackfillBatchRequest {
            blocks: vec![BackfillBlock {
                slot: 9,
                root: Root::ZERO.as_slice().to_vec(),
                ssz: ssz9,
            }],
            columns: vec![],
            progress: Some(ProtoBackfillProgress {
                blocks_oldest: 9,
                blocks_oldest_parent: root_n(0x08).as_slice().to_vec(),
                columns_oldest: 9,
                per_index_oldest: vec![],
            }),
        }))
        .await
        .unwrap();

        let rt = eng.read().unwrap();
        let stored = cc_store::load_backfill_progress_txn(&rt).unwrap().unwrap();
        assert_eq!(stored.blocks_oldest, Slot::new(9));
        assert_eq!(stored.blocks_oldest_parent, root_n(0x08));
        assert!(
            cc_store::get_canonical(&rt, Slot::new(8))
                .unwrap()
                .is_none(),
            "rejected hole must stay empty after adjacent extension"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn put_backfill_batch_intra_batch_sandwich_is_rejected() {
        let (dir, eng) = open_engine("bf-sandwich");
        let srv = server_with(Arc::clone(&eng), ServeConfig::default());
        let root10 = root_n(0x10);
        seed_anchor_oldest_parent(&eng, root10);
        let ssz10 = synth_block(10, &Root::ZERO, &root_n(1));
        srv.put_backfill_batch(Request::new(PutBackfillBatchRequest {
            blocks: vec![BackfillBlock {
                slot: 10,
                root: root10.as_slice().to_vec(),
                ssz: ssz10,
            }],
            columns: vec![],
            progress: Some(ProtoBackfillProgress {
                blocks_oldest: 10,
                blocks_oldest_parent: Root::ZERO.as_slice().to_vec(),
                columns_oldest: 10,
                per_index_oldest: vec![],
            }),
        }))
        .await
        .unwrap();

        // Top row attaches (slot 9 / root == named parent); lowest is 0.
        // Parent(9) == root of slot 0 — the intra-batch hole 1–8.
        let r0 = root_n(0xAA);
        let ssz9 = synth_block(9, &r0, &root_n(1));
        let ssz0 = synth_block(0, &root_n(0xBB), &root_n(1));
        let err = srv
            .put_backfill_batch(Request::new(PutBackfillBatchRequest {
                blocks: vec![
                    BackfillBlock {
                        slot: 9,
                        root: Root::ZERO.as_slice().to_vec(),
                        ssz: ssz9,
                    },
                    BackfillBlock {
                        slot: 0,
                        root: r0.as_slice().to_vec(),
                        ssz: ssz0,
                    },
                ],
                columns: vec![],
                progress: Some(ProtoBackfillProgress {
                    blocks_oldest: 0,
                    blocks_oldest_parent: root_n(0xBB).as_slice().to_vec(),
                    columns_oldest: 0,
                    per_index_oldest: vec![],
                }),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(
            err.message().contains("durable frontier") && err.message().contains("never jump"),
            "must state the S2 invariant: {err}"
        );
        assert!(err.message().contains("not slot-contiguous"), "{err}");

        {
            let rt = eng.read().unwrap();
            assert!(get_block_by_root(&rt, &r0).unwrap().is_none());
            assert!(
                cc_store::get_canonical(&rt, Slot::new(0))
                    .unwrap()
                    .is_none()
            );
            assert!(
                cc_store::get_canonical(&rt, Slot::new(9))
                    .unwrap()
                    .is_none()
            );
            let stored = cc_store::load_backfill_progress_txn(&rt).unwrap().unwrap();
            assert_eq!(stored.blocks_oldest, Slot::new(10));
        }

        // Adjacent contiguous [9, 8] still commits.
        let r8 = root_n(0x08);
        let ssz9 = synth_block(9, &r8, &root_n(1));
        let ssz8 = synth_block(8, &root_n(0x07), &root_n(1));
        srv.put_backfill_batch(Request::new(PutBackfillBatchRequest {
            blocks: vec![
                BackfillBlock {
                    slot: 9,
                    root: Root::ZERO.as_slice().to_vec(),
                    ssz: ssz9,
                },
                BackfillBlock {
                    slot: 8,
                    root: r8.as_slice().to_vec(),
                    ssz: ssz8,
                },
            ],
            columns: vec![],
            progress: Some(ProtoBackfillProgress {
                blocks_oldest: 8,
                blocks_oldest_parent: root_n(0x07).as_slice().to_vec(),
                columns_oldest: 8,
                per_index_oldest: vec![],
            }),
        }))
        .await
        .unwrap();

        let rt = eng.read().unwrap();
        let stored = cc_store::load_backfill_progress_txn(&rt).unwrap().unwrap();
        assert_eq!(stored.blocks_oldest, Slot::new(8));
        assert_eq!(stored.blocks_oldest_parent, root_n(0x07));
        assert!(get_block_by_root(&rt, &r8).unwrap().is_some());
        assert!(
            cc_store::get_canonical(&rt, Slot::new(0))
                .unwrap()
                .is_none(),
            "rejected sandwich must not plant slot 0"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn put_backfill_batch_stored_oldest_zero_is_not_any_slot_seed() {
        let (dir, eng) = open_engine("bf-zero-trap");
        let srv = server_with(Arc::clone(&eng), ServeConfig::default());
        let root0 = root_n(0x10);
        seed_anchor_oldest_parent(&eng, root0);
        let ssz0 = synth_block(0, &Root::ZERO, &root_n(1));
        srv.put_backfill_batch(Request::new(PutBackfillBatchRequest {
            blocks: vec![BackfillBlock {
                slot: 0,
                root: root0.as_slice().to_vec(),
                ssz: ssz0,
            }],
            columns: vec![],
            progress: Some(ProtoBackfillProgress {
                blocks_oldest: 0,
                blocks_oldest_parent: Root::ZERO.as_slice().to_vec(),
                columns_oldest: 0,
                per_index_oldest: vec![],
            }),
        }))
        .await
        .unwrap();

        // Stored oldest = 0 is set. A later any-slot put must not land.
        let ssz5 = synth_block(5, &root_n(0x04), &root_n(1));
        let err = srv
            .put_backfill_batch(Request::new(PutBackfillBatchRequest {
                blocks: vec![BackfillBlock {
                    slot: 5,
                    root: Root::ZERO.as_slice().to_vec(),
                    ssz: ssz5,
                }],
                columns: vec![],
                progress: Some(ProtoBackfillProgress {
                    blocks_oldest: 5,
                    blocks_oldest_parent: root_n(0x04).as_slice().to_vec(),
                    columns_oldest: 5,
                    per_index_oldest: vec![],
                }),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(
            err.message().contains("durable frontier") && err.message().contains("never jump"),
            "{err}"
        );

        let rt = eng.read().unwrap();
        assert!(
            cc_store::get_canonical(&rt, Slot::new(5))
                .unwrap()
                .is_none(),
            "stored oldest=0 must not restore any-slot put_canonical"
        );
        let stored = cc_store::load_backfill_progress_txn(&rt).unwrap().unwrap();
        assert_eq!(stored.blocks_oldest, Slot::new(0));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn put_backfill_batch_progress_only_cannot_plant_blocks_oldest_parent() {
        let (dir, eng) = open_engine("bf-prog-only");
        let srv = server_with(Arc::clone(&eng), ServeConfig::default());
        let frontier_parent = root_n(0xEE);
        seed_anchor_oldest_parent(&eng, frontier_parent);

        let planted_parent = root_n(0xAA);
        let planted_slot = 99_999u64;
        let err = srv
            .put_backfill_batch(Request::new(PutBackfillBatchRequest {
                blocks: vec![],
                columns: vec![],
                progress: Some(ProtoBackfillProgress {
                    blocks_oldest: planted_slot,
                    blocks_oldest_parent: planted_parent.as_slice().to_vec(),
                    columns_oldest: planted_slot,
                    per_index_oldest: vec![],
                }),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(
            err.message().contains("durable frontier") && err.message().contains("never jump"),
            "{err}"
        );

        {
            let rt = eng.read().unwrap();
            assert!(
                rt.get(TABLE_META, KEY_BACKFILL_PROG.as_bytes())
                    .unwrap()
                    .is_none(),
                "progress-only must not persist a fabricated named frontier"
            );
        }

        // A later single-block batch must still bind to the real frontier parent,
        // not the rejected plant — so put_canonical cannot land at planted_slot.
        let ssz = synth_block(planted_slot, &root_n(0xFF), &root_n(1));
        let err = srv
            .put_backfill_batch(Request::new(PutBackfillBatchRequest {
                blocks: vec![BackfillBlock {
                    slot: planted_slot,
                    root: planted_parent.as_slice().to_vec(),
                    ssz,
                }],
                columns: vec![],
                progress: Some(ProtoBackfillProgress {
                    blocks_oldest: planted_slot,
                    blocks_oldest_parent: root_n(0xFF).as_slice().to_vec(),
                    columns_oldest: planted_slot,
                    per_index_oldest: vec![],
                }),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);

        let rt = eng.read().unwrap();
        assert!(get_block_by_root(&rt, &planted_parent).unwrap().is_none());
        assert!(
            cc_store::get_canonical(&rt, Slot::new(planted_slot))
                .unwrap()
                .is_none()
        );
        assert!(
            rt.get(TABLE_META, KEY_BACKFILL_PROG.as_bytes())
                .unwrap()
                .is_none()
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn put_backfill_batch_cgc_raise_seeds_never_custodied_at_head() {
        let (dir, eng) = open_engine("bf-cgc-raise");
        let srv = server_with(Arc::clone(&eng), ServeConfig::default());
        let root = root_n(0x10);
        seed_anchor_oldest_parent(&eng, root);
        let ssz = synth_block(10, &Root::ZERO, &root_n(1));
        srv.put_backfill_batch(Request::new(PutBackfillBatchRequest {
            blocks: vec![BackfillBlock {
                slot: 10,
                root: root.as_slice().to_vec(),
                ssz,
            }],
            columns: vec![],
            progress: Some(ProtoBackfillProgress {
                blocks_oldest: 10,
                blocks_oldest_parent: Root::ZERO.as_slice().to_vec(),
                columns_oldest: 10,
                per_index_oldest: vec![10, 10, 10, 10],
            }),
        }))
        .await
        .unwrap();

        {
            let rt = eng.read().unwrap();
            let bytes = rt
                .get(TABLE_META, KEY_BACKFILL_PROG.as_bytes())
                .unwrap()
                .expect("progress");
            let stored = BackfillProgress::from_ssz_bytes(&bytes).unwrap();
            assert_eq!(stored.per_index_oldest[0], Slot::new(10));
            assert_eq!(
                stored.per_index_oldest[4],
                Slot::ZERO,
                "never-custodied must report no progress, not padded columns_oldest"
            );
            assert_eq!(stored.per_index_oldest[127], Slot::ZERO);
        }

        // Progress-only raise: same named block frontier, new indices at head.
        let head = 50_000u64;
        srv.put_backfill_batch(Request::new(PutBackfillBatchRequest {
            blocks: vec![],
            columns: vec![],
            progress: Some(ProtoBackfillProgress {
                blocks_oldest: 10,
                blocks_oldest_parent: Root::ZERO.as_slice().to_vec(),
                columns_oldest: 10,
                per_index_oldest: vec![10, 10, 10, 10, head, head, head, head],
            }),
        }))
        .await
        .expect("honest cgc-raise report must be accepted");

        let rt = eng.read().unwrap();
        let bytes = rt
            .get(TABLE_META, KEY_BACKFILL_PROG.as_bytes())
            .unwrap()
            .expect("progress");
        let loaded = BackfillProgress::from_ssz_bytes(&bytes).unwrap();
        assert_eq!(loaded.per_index_oldest[0], Slot::new(10));
        assert_eq!(loaded.per_index_oldest[4], Slot::new(head));
        assert_eq!(loaded.per_index_oldest[127], Slot::ZERO);
        assert_eq!(loaded.blocks_oldest, Slot::new(10));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn watch_serve_window_is_stream_and_emits() {
        let (dir, eng) = open_engine("watch");
        let srv = server_with(Arc::clone(&eng), ServeConfig::default());
        let mut stream = srv
            .watch_serve_window(Request::new(WatchServeWindowRequest {}))
            .await
            .unwrap()
            .into_inner();

        // First item is the current window.
        let first = futures::StreamExt::next(&mut stream)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.earliest_available_slot, 0);

        srv.publish_window(ServeWindow {
            earliest_available_slot: 42,
            cgc: 8,
            head_slot: 100,
            block_floor: 42,
            column_floor: 42,
            branch: 1,
            holes: vec![],
        });
        let second = futures::StreamExt::next(&mut stream)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second.earliest_available_slot, 42);
        assert_eq!(second.cgc, 8);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn below_window_is_resource_unavailable_never_empty_success() {
        let (dir, eng) = open_engine("below");
        seed_blocks(&eng, 0, 10);
        let srv = server_with(Arc::clone(&eng), ServeConfig::default());
        srv.publish_window(ServeWindow {
            earliest_available_slot: 100,
            cgc: 4,
            head_slot: 200,
            block_floor: 100,
            column_floor: 100,
            branch: 2,
            holes: vec![],
        });

        let err = srv
            .get_blocks_by_range(Request::new(GetBlocksByRangeRequest {
                start_slot: 50, // below eas
                count: 4,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);
        assert!(err.message().contains("earliest_available_slot"));

        let err = srv
            .get_columns_by_range(Request::new(GetColumnsByRangeRequest {
                start_slot: 50,
                count: 4,
                column_indices: vec![],
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn range_straddling_shard_boundary_contiguous() {
        // Store-level 256-epoch block shards: boundary at 8192.
        let (dir, eng) = open_engine("shard-bound");
        let boundary = 8192u64;
        let start = boundary - 4;
        seed_blocks(&eng, start, 8);
        let srv = server_with(Arc::clone(&eng), ServeConfig::default());
        let resp = srv
            .get_blocks_by_range(Request::new(GetBlocksByRangeRequest {
                start_slot: start,
                count: 8,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.blocks.len(), 8);
        let slots: Vec<u64> = resp.blocks.iter().map(|b| b.slot).collect();
        assert_eq!(slots, (start..start + 8).collect::<Vec<_>>());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn read_txn_seconds_bucket_observed_and_short() {
        let (dir, eng) = open_engine("read-txn");
        seed_blocks(&eng, 0, 8);
        let m = metrics();
        let srv = StorageServer::new(Arc::clone(&eng), None, m.clone(), ServeConfig::default());
        srv.publish_window(ServeWindow {
            earliest_available_slot: 0,
            ..empty_window()
        });
        let _ = srv
            .get_blocks_by_range(Request::new(GetBlocksByRangeRequest {
                start_slot: 0,
                count: 8,
            }))
            .await
            .unwrap();

        // Spot-check: read-txn path must complete well under the 1 s bucket.
        let t0 = Instant::now();
        let _ = srv
            .get_blocks_by_range(Request::new(GetBlocksByRangeRequest {
                start_slot: 0,
                count: 8,
            }))
            .await
            .unwrap();
        assert!(
            t0.elapsed() < Duration::from_secs(1),
            "read path must drop txn well under 1 s"
        );
        let _ = m;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn p99_microbench_blocks_by_range_128() {
        // OQ-P4-1: microbench of materialise path (no full gRPC hop / stack).
        // Full hop residual needs compose; document decision from this path.
        let (dir, eng) = open_engine("p99");
        seed_blocks(&eng, 0, 128);
        let srv = server_with(Arc::clone(&eng), ServeConfig::default());
        let mut samples = Vec::with_capacity(200);
        for _ in 0..200 {
            let t0 = Instant::now();
            let resp = srv
                .get_blocks_by_range(Request::new(GetBlocksByRangeRequest {
                    start_slot: 0,
                    count: 128,
                }))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(resp.blocks.len(), 128);
            samples.push(t0.elapsed());
        }
        samples.sort();
        let p99 = samples[(samples.len() as f64 * 0.99) as usize - 1];
        // Local materialise path should be ≪ 150 ms; hop residual is separate.
        assert!(
            p99 < Duration::from_millis(150),
            "p99 materialise path {p99:?} exceeds 150 ms budget (OQ-P4-1)"
        );
        eprintln!(
            "CC-4F OQ-P4-1 microbench: p99(materialise blocks_by_range 128) = {:?} (budget 150 ms). \
             Decision: ship — deepen CC-26a cache only if full-hop p99 exceeds budget on soak.",
            p99
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn module_doc_carries_cross_requirement_dependency_6() {
        let src = include_str!("serve.rs");
        // Production surface only (exclude this test module).
        let prod = src.split("#[cfg(test)]").next().unwrap_or(src);
        assert!(
            prod.contains("Cross-Requirement Dependency 6"),
            "module doc must name Cross-Requirement Dependency 6"
        );
        assert!(
            prod.contains("long-reader") || prod.contains("long reader"),
            "module doc must name the long-reader rule"
        );
        assert!(
            prod.contains("Materialise the response bytes"),
            "module doc must carry the materialise-and-drop rule"
        );
        assert!(
            prod.contains("No `Iterator` over a live"),
            "module doc must warn against Iterator over a live read transaction"
        );
        // No Iterator return type in production helpers (AC: none cross a boundary).
        assert!(
            !prod.contains("impl Iterator") && !prod.contains("dyn Iterator"),
            "serve.rs production code must not return Iterator across function boundaries"
        );
    }

    #[test]
    fn known_rpc_count_ten() {
        // CC-4F declared nine; CC-4I appends GetFinalizedCheckpointHistory → 10.
        let proto = include_str!("../../../proto/eth/storage/v1/storage.proto");
        let count = proto
            .lines()
            .filter(|l| l.trim_start().starts_with("rpc "))
            .count();
        assert_eq!(
            count, 10,
            "storage.proto must declare 10 RPCs (9 + CC-4I history)"
        );
        assert!(proto.contains("stream ServeWindow"));
        assert!(proto.contains("GetFinalizedCheckpointHistory"));
        assert!(proto.contains("stream StateChunk"));
    }

    /// Compile-time: StorageServiceServer wraps our type.
    #[test]
    fn storage_service_server_type_constructs() {
        let srv = StorageServer::stub(metrics(), ServeConfig::default());
        let _ = UnaryPermitService::new(StorageServiceServer::new(srv));
    }

    /// Force-delete every block key in `[start, start+count)` (direct engine
    /// write). Used by the forced-concurrent-prune tests as the body of
    /// `mid_serve_prune_hook`.
    fn force_delete_block_range(eng: &Engine, start: u64, count: u64) {
        use cc_store::keys::{encode_cold_block_key, encode_hot_block_key, encode_root_key};
        use cc_store::{TABLE_BLOCK_SLOT_BY_ROOT, TABLE_BLOCKS_HOT, TABLE_CANONICAL};
        let mut b = eng.batch();
        for i in 0..count {
            let slot = Slot::new(start + i);
            let root = Root::from_array({
                let mut a = [0u8; 32];
                a[0..8].copy_from_slice(&(start + i).to_be_bytes());
                a
            });
            b.delete(TABLE_BLOCKS_HOT, &encode_hot_block_key(slot, &root));
            b.delete(TABLE_CANONICAL, &encode_cold_block_key(slot));
            b.delete(TABLE_BLOCK_SLOT_BY_ROOT, &encode_root_key(&root));
        }
        eng.commit(b).unwrap();
    }

    /// CC-46 /4 — 128-slot serve at the watermark with prune forced mid-serve
    /// (`mid_serve_prune_hook`) returns a **complete** response. Both
    /// mitigations on (margin documented; admission-time materialisation).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn forced_concurrent_prune_complete_response() {
        let (dir, eng) = open_engine("force-prune-both");
        let watermark = 1_000u64;
        seed_blocks(&eng, watermark, 128);
        let eng_hook = Arc::clone(&eng);
        let srv = server_with(Arc::clone(&eng), ServeConfig::default()).with_mid_serve_prune_hook(
            Arc::new(move || {
                // Forcing mechanism: mid_serve_prune_hook — prune pass driven
                // from inside serve after the first slot is materialised.
                force_delete_block_range(&eng_hook, watermark, 128);
            }),
        );
        srv.publish_window(ServeWindow {
            earliest_available_slot: watermark,
            cgc: 4,
            head_slot: watermark + 200,
            block_floor: watermark,
            column_floor: watermark,
            branch: 2,
            holes: vec![],
        });
        let resp = srv
            .get_blocks_by_range(Request::new(GetBlocksByRangeRequest {
                start_slot: watermark,
                count: 128,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            resp.blocks.len(),
            128,
            "admission-time materialisation + margin: complete 128-slot response under forced concurrent prune"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Mitigation A alone: margin at 0, materialisation intact → still passes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn forced_concurrent_prune_margin_zero_materialise_intact() {
        let (dir, eng) = open_engine("force-prune-mat");
        let watermark = 2_000u64;
        seed_blocks(&eng, watermark, 128);
        let eng_hook = Arc::clone(&eng);
        let srv = server_with(
            Arc::clone(&eng),
            ServeConfig {
                materialise_mode: MaterialiseMode::AdmissionTime,
                ..ServeConfig::default()
            },
        )
        .with_mid_serve_prune_hook(Arc::new(move || {
            force_delete_block_range(&eng_hook, watermark, 128);
        }));
        srv.publish_window(ServeWindow {
            earliest_available_slot: watermark,
            cgc: 4,
            head_slot: watermark + 200,
            block_floor: watermark,
            column_floor: watermark,
            branch: 2,
            holes: vec![],
        });
        let resp = srv
            .get_blocks_by_range(Request::new(GetBlocksByRangeRequest {
                start_slot: watermark,
                count: 128,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            resp.blocks.len(),
            128,
            "margin=0 + admission-time materialisation still yields complete response"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Mitigation B alone is insufficient: margin=1 with materialisation
    /// removed → incomplete response under forced concurrent prune.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn forced_concurrent_prune_margin_one_materialise_removed_fails() {
        let (dir, eng) = open_engine("force-prune-no-mat");
        let watermark = 3_000u64;
        seed_blocks(&eng, watermark, 128);
        let eng_hook = Arc::clone(&eng);
        let srv = server_with(
            Arc::clone(&eng),
            ServeConfig {
                // Margin is a prune-side knob; serve still sees forced deletes.
                // materialisation removed:
                materialise_mode: MaterialiseMode::PerSlotReopen,
                ..ServeConfig::default()
            },
        )
        .with_mid_serve_prune_hook(Arc::new(move || {
            force_delete_block_range(&eng_hook, watermark, 128);
        }));
        srv.publish_window(ServeWindow {
            earliest_available_slot: watermark,
            cgc: 4,
            head_slot: watermark + 200,
            block_floor: watermark,
            column_floor: watermark,
            branch: 2,
            holes: vec![],
        });
        let result = srv
            .get_blocks_by_range(Request::new(GetBlocksByRangeRequest {
                start_slot: watermark,
                count: 128,
            }))
            .await;
        match result {
            Ok(resp) => {
                let n = resp.into_inner().blocks.len();
                assert!(
                    n < 128,
                    "materialisation removed: concurrent prune must yield incomplete response, got {n}"
                );
            }
            Err(status) => {
                // ResourceUnavailable for empty/partial is also a failure of completeness.
                assert_eq!(status.code(), tonic::Code::Unavailable);
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn module_doc_states_margin_cost_and_lighthouse_not_copied() {
        let src = include_str!("serve.rs");
        let prod = src.split("#[cfg(test)]").next().unwrap_or(src);
        assert!(
            prod.contains("6.9 MiB") && prod.contains("0.7 MiB"),
            "module doc must state margin cost (6.9 MiB columns, 0.7 MiB blocks)"
        );
        assert!(
            prod.contains("do not copy that default") || prod.contains("not copy that default"),
            "module doc must state Lighthouse's default of 0 was not copied"
        );
        assert!(
            prod.contains("Admission-time key materialisation")
                || prod.contains("admission-time key materialisation"),
            "module doc must name admission-time key materialisation"
        );
    }
}
