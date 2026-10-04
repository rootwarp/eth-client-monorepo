//! Dedicated OS core thread owning fork-choice [`Store`] by value (ADR-P1-09).
//!
//! Communication: every producer pushes into the five-lane [`Manager`]
//! (S0-A-17). The core thread first-match-wins across tick → import →
//! query_p0 → attestation → query_p1, then parks until a producer notifies.
//! `oneshot` replies. `ImportBlock` uses `send_timeout(2 s)` →
//! `RESOURCE_EXHAUSTED` on backpressure (policy A).
//!
//! Wired lanes ([ARCH] §3.2):
//! - `tick` — never-shed `SlotTick` + `Shutdown` + liveness `Ping` ([S0-A-14] / S1-A-15)
//! - `import` — `ImportBlock` / `ImportBlockGossip` / `DataAvailable` (FIFO 64)
//! - `query_p0` — `Query{Head, IsOptimistic}` + head probes + test `BlockFor`
//! - `attestation` — `ApplyAttestations` (LIFO, sized from active validators,
//!   evict-oldest)
//! - `query_p1` — `Query{CommitteeShuffling, ValidatorPubkeys,
//!   ValidatorRecords, CanonicalRoots}` (FIFO 64)
//!
//! There is no mixed command channel. Queue-depth gauges come from manager
//! snapshots — a five-lane manager has no single `capacity()`.
//!
//! ```text
//! loop { select(); handle(); /* snapshot + events inside import */ }
//! ```
//!
//! CC-1F state-requiring reads (`GetCommitteeShuffling`, `GetValidatorPubkeys`)
//! and CC-27a `GetValidatorRecords` ride `query_p1` — no second copy of the
//! head state is held on the gRPC side. Epoch-scoped data for `ChainView` is
//! published via [`EpochContextStore`] (second `ArcSwap`, Architecture §16/4).

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use cc_fork_choice::{PeerDasAvailability, Store, is_optimistic, is_optimistic_node, on_tick};
use cc_proto::chain::{
    ApplyAttestationsRequest, ApplyAttestationsResponse, ImportBlockRequest, ImportBlockResponse,
    ImportBlockVerdict,
};
use cc_proto::common::Source;
use cc_scheduler::{ChainLane, Enqueue, Manager, QueueSizes, sized_from_validators};
use cc_state_transition::helpers::accessors::get_active_validator_indices;
use cc_state_transition::helpers::misc::compute_start_slot_at_epoch;
use cc_state_transition::{
    BlockSignatureStrategy, compute_shuffled_active_indices, decision_root_for_epoch,
    get_committee_count_per_slot, get_current_epoch, get_or_compute_shuffling,
};
use cc_types::BeaconState;
use cc_types::config::ChainConfig;
use cc_types::preset::Preset;
use cc_types::primitives::{Epoch, Root, Slot};
use ssz::Encode;
use tokio::sync::{mpsc, oneshot};
use tonic::Status;

use crate::apply_attestations::apply_attestations;
use crate::da::{DEFAULT_DA_PENDING_TIMEOUT_SLOTS, PendingDa};
use crate::engine::SharedEngine;
use crate::epoch_context::{EpochContext, EpochContextStore};
use crate::fcu_driver::FcuDriver;
use crate::head::{HeadSnapshot, HeadSnapshotStore};
use crate::import::{ImportCounters, ImportOutcome, import_block_with_early};
use crate::liveness::LivenessError;
use crate::metrics::ChainMetrics;
use crate::pending_engine::{DEFAULT_ENGINE_PENDING_TIMEOUT_SLOTS, PendingEngine};
use crate::residency::{DEFAULT_BODY_RING_CAPACITY, DEFAULT_MAX_RESIDENT_STATES, Residency};
use crate::tick::{
    DEFAULT_MAXIMUM_GOSSIP_CLOCK_DISPARITY, GossipClock, TickWork, advance_store_clock,
    duration_until_next_slot_boundary, unix_now_millis, unix_now_secs,
};

/// `ImportBlock` send timeout before `RESOURCE_EXHAUSTED` (Architecture §7.2).
pub const IMPORT_SEND_TIMEOUT: Duration = Duration::from_secs(2);

/// Shutdown join timeout (Architecture §7.4).
pub const SHUTDOWN_JOIN_TIMEOUT: Duration = Duration::from_secs(2);

/// Max indices accepted by `GetValidatorPubkeys` (CC-1F; same discipline as
/// Phase 2's 256-bound `GetValidatorRecords`).
pub const MAX_VALIDATOR_PUBKEYS_PER_REQUEST: u64 = 256;

/// Max indices accepted by `GetValidatorRecords` (CC-27a / §5.4a).
pub const MAX_VALIDATOR_RECORDS_PER_REQUEST: u64 = 256;

/// Commands handled by the core thread.
#[derive(Debug)]
pub enum CoreCommand {
    /// Full import path.
    ImportBlock {
        request: ImportBlockRequest,
        reply: oneshot::Sender<Result<ImportBlockResponse, Status>>,
    },
    /// Gossip-path import: early ACCEPT notify before state transition (CC-27c).
    ImportBlockGossip {
        request: ImportBlockRequest,
        /// Fired once after cheap gossip checks pass, before `on_block`.
        early_accept: Option<oneshot::Sender<()>>,
        reply: oneshot::Sender<Result<ImportOutcome, Status>>,
    },
    /// Batched free-floating `on_attestation` (CC-1E). No per-item head recompute.
    ApplyAttestations {
        request: ApplyAttestationsRequest,
        reply: oneshot::Sender<Result<ApplyAttestationsResponse, Status>>,
    },
    /// State-requiring read. `query_p0` / `query_p1` variants ride their own
    /// lanes (S0-A-17: no mixed leftover).
    ///
    /// Used by CC-1F (`GetCommitteeShuffling`, `GetValidatorPubkeys`) and head probes.
    Query {
        request: QueryRequest,
        reply: oneshot::Sender<Result<QueryReply, Status>>,
    },
    /// Block the core thread for `duration` (tests: GetHead bypass).
    BlockFor {
        duration: Duration,
        reply: oneshot::Sender<()>,
    },
    /// Sampling complete for `root` (CC-24d / Architecture §8.3).
    ///
    /// Marks [`PeerDasAvailability`] and re-drives a parked `pending_da` entry
    /// when present. Order-independent with block arrival.
    DataAvailable { root: Root, slot: u64 },
    /// Per-slot fcU floor tick (CC-33 /7) — re-points a restarted EL with no block.
    SlotTick,
    /// Core-liveness no-op ([ARCH] §7.2). Tick lane; handler replies immediately.
    Ping {
        issued_at: Instant,
        reply: oneshot::Sender<()>,
    },
    /// Graceful shutdown.
    Shutdown { done: oneshot::Sender<()> },
}

/// Work that rides the `import` lane ([ARCH] §3.2 / S0-A-15).
///
/// FIFO depth [`IMPORT_LANE_DEPTH`], policy A (can shed). `DataAvailable`
/// stays here — it re-drives a parked block, so promoting it starves imports.
#[derive(Debug)]
pub enum ImportWork {
    ImportBlock {
        request: ImportBlockRequest,
        reply: oneshot::Sender<Result<ImportBlockResponse, Status>>,
    },
    ImportBlockGossip {
        request: ImportBlockRequest,
        early_accept: Option<oneshot::Sender<()>>,
        reply: oneshot::Sender<Result<ImportOutcome, Status>>,
    },
    DataAvailable {
        root: Root,
        slot: u64,
    },
}

impl From<ImportWork> for CoreCommand {
    fn from(work: ImportWork) -> Self {
        match work {
            ImportWork::ImportBlock { request, reply } => Self::ImportBlock { request, reply },
            ImportWork::ImportBlockGossip {
                request,
                early_accept,
                reply,
            } => Self::ImportBlockGossip {
                request,
                early_accept,
                reply,
            },
            ImportWork::DataAvailable { root, slot } => Self::DataAvailable { root, slot },
        }
    }
}

/// Work that rides the `query_p0` lane (Lighthouse `ApiRequestP0`).
///
/// FIFO depth [`QUERY_P0_LANE_DEPTH`]. Head probes must not wait behind an
/// attestation flood or a `query_p1` read.
#[derive(Debug)]
pub enum QueryP0Work {
    Query {
        request: QueryRequest,
        reply: oneshot::Sender<Result<QueryReply, Status>>,
    },
    /// Stall the core thread (tests). Rides `query_p0` so it does not share
    /// import-lane capacity.
    BlockFor {
        duration: Duration,
        reply: oneshot::Sender<()>,
    },
}

impl From<QueryP0Work> for CoreCommand {
    fn from(work: QueryP0Work) -> Self {
        match work {
            QueryP0Work::Query { request, reply } => Self::Query { request, reply },
            QueryP0Work::BlockFor { duration, reply } => Self::BlockFor { duration, reply },
        }
    }
}

/// Work that rides the `query_p1` lane (Lighthouse `ApiRequestP1`).
///
/// FIFO depth [`QUERY_P1_LANE_DEPTH`]. Serving reads must not be evicted;
/// drop new under load rather than flush a request. Lower priority than
/// `attestation`.
#[derive(Debug)]
pub enum QueryP1Work {
    Query {
        request: QueryRequest,
        reply: oneshot::Sender<Result<QueryReply, Status>>,
    },
}

impl From<QueryP1Work> for CoreCommand {
    fn from(work: QueryP1Work) -> Self {
        match work {
            QueryP1Work::Query { request, reply } => Self::Query { request, reply },
        }
    }
}

/// Work that rides the `attestation` lane ([ARCH] §3.2 / S0-A-16).
///
/// LIFO, depth [`sized_from_validators`]. A later batch is strictly better
/// information; overflow evicts the oldest.
#[derive(Debug)]
pub enum AttestationWork {
    ApplyAttestations {
        request: ApplyAttestationsRequest,
        reply: oneshot::Sender<Result<ApplyAttestationsResponse, Status>>,
    },
}

impl From<AttestationWork> for CoreCommand {
    fn from(work: AttestationWork) -> Self {
        match work {
            AttestationWork::ApplyAttestations { request, reply } => {
                Self::ApplyAttestations { request, reply }
            }
        }
    }
}

/// Work item stored in the five-lane manager.
#[derive(Debug)]
enum CoreWork {
    Tick(TickWork),
    Import(ImportWork),
    QueryP0(QueryP0Work),
    Attestation(AttestationWork),
    QueryP1(QueryP1Work),
}

impl CoreWork {
    fn lane(&self) -> ChainLane {
        match self {
            Self::Tick(_) => ChainLane::Tick,
            Self::Import(_) => ChainLane::Import,
            Self::QueryP0(_) => ChainLane::QueryP0,
            Self::Attestation(_) => ChainLane::Attestation,
            Self::QueryP1(_) => ChainLane::QueryP1,
        }
    }
}

impl From<CoreWork> for CoreCommand {
    fn from(work: CoreWork) -> Self {
        match work {
            CoreWork::Tick(TickWork::SlotTick) => Self::SlotTick,
            CoreWork::Tick(TickWork::Shutdown { done }) => Self::Shutdown { done },
            CoreWork::Tick(TickWork::Ping { issued_at, reply }) => Self::Ping { issued_at, reply },
            CoreWork::Import(work) => Self::from(work),
            CoreWork::QueryP0(work) => Self::from(work),
            CoreWork::Attestation(work) => Self::from(work),
            CoreWork::QueryP1(work) => Self::from(work),
        }
    }
}

/// Park/unpark so dedicated-lane sends wake an idle core without a second runtime.
#[derive(Debug)]
struct LaneWake {
    signaled: AtomicBool,
    parked: Mutex<Option<thread::Thread>>,
}

impl LaneWake {
    fn new() -> Self {
        Self {
            signaled: AtomicBool::new(false),
            parked: Mutex::new(None),
        }
    }

    fn notify(&self) {
        self.signaled.store(true, Ordering::SeqCst);
        if let Some(t) = self.parked_lock().clone() {
            t.unpark();
        }
    }

    fn park_current(&self) {
        *self.parked_lock() = Some(thread::current());
        if !self.signaled.swap(false, Ordering::SeqCst) {
            thread::park();
            self.signaled.store(false, Ordering::SeqCst);
        }
    }

    fn parked_lock(&self) -> std::sync::MutexGuard<'_, Option<thread::Thread>> {
        self.parked
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Outcome of a manager push from a producer.
enum SchedPush {
    Accepted,
    EvictedOldest(CoreWork),
    Full(CoreWork),
    Closed(CoreWork),
}

struct SchedulerState {
    manager: Manager<ChainLane, CoreWork>,
    producers_closed: bool,
    consumer_gone: bool,
}

/// Shared five-lane manager. Every producer pushes here (S0-A-17).
struct SharedScheduler {
    state: Mutex<SchedulerState>,
    space: Condvar,
    wake: Arc<LaneWake>,
    senders: AtomicUsize,
}

impl std::fmt::Debug for SharedScheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedScheduler")
            .field("senders", &self.senders.load(Ordering::SeqCst))
            .finish_non_exhaustive()
    }
}

impl SharedScheduler {
    fn new(sizes: QueueSizes, wake: Arc<LaneWake>) -> Arc<Self> {
        let manager = Manager::loop_b(sizes).unwrap_or_else(|e| {
            tracing::error!(error = %e, "loop B manager config");
            std::process::abort();
        });
        Arc::new(Self {
            state: Mutex::new(SchedulerState {
                manager,
                producers_closed: false,
                consumer_gone: false,
            }),
            space: Condvar::new(),
            wake,
            senders: AtomicUsize::new(0),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, SchedulerState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn push(&self, work: CoreWork) -> SchedPush {
        let lane = work.lane();
        let mut g = self.lock();
        if g.consumer_gone {
            return SchedPush::Closed(work);
        }
        match g.manager.push(lane, work) {
            Enqueue::Accepted => {
                drop(g);
                self.wake.notify();
                SchedPush::Accepted
            }
            Enqueue::EvictedOldest(old) => {
                drop(g);
                self.wake.notify();
                SchedPush::EvictedOldest(old)
            }
            Enqueue::DroppedNew(w) | Enqueue::WouldShed(w) | Enqueue::UnknownLane(w) => {
                SchedPush::Full(w)
            }
        }
    }

    fn blocking_push(&self, mut work: CoreWork) -> Result<(), CoreWork> {
        let lane = work.lane();
        let mut g = self.lock();
        loop {
            if g.consumer_gone {
                return Err(work);
            }
            match g.manager.push(lane, work) {
                Enqueue::Accepted | Enqueue::EvictedOldest(_) => {
                    drop(g);
                    self.wake.notify();
                    return Ok(());
                }
                Enqueue::DroppedNew(w) | Enqueue::WouldShed(w) | Enqueue::UnknownLane(w) => {
                    work = w;
                    g = self
                        .space
                        .wait(g)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                }
            }
        }
    }

    async fn push_wait(&self, mut work: CoreWork) -> Result<(), CoreWork> {
        loop {
            match self.push(work) {
                SchedPush::Accepted | SchedPush::EvictedOldest(_) => return Ok(()),
                SchedPush::Closed(w) => return Err(w),
                SchedPush::Full(w) => {
                    work = w;
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
        }
    }

    async fn push_timeout(&self, mut work: CoreWork, timeout: Duration) -> Result<(), CoreWork> {
        let deadline = Instant::now() + timeout;
        loop {
            match self.push(work) {
                SchedPush::Accepted | SchedPush::EvictedOldest(_) => return Ok(()),
                SchedPush::Closed(w) => return Err(w),
                SchedPush::Full(w) => {
                    work = w;
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return Err(work);
                    }
                    tokio::time::sleep(remaining.min(Duration::from_millis(5))).await;
                }
            }
        }
    }

    fn select(&self) -> Option<CoreWork> {
        let mut g = self.lock();
        let item = g.manager.select().map(|s| s.item);
        if item.is_some() {
            g.manager.note_idle();
            self.space.notify_all();
        }
        item
    }

    fn set_depth(&self, lane: ChainLane, depth: usize) {
        let mut g = self.lock();
        let _ = g.manager.set_depth(lane, depth);
        self.space.notify_all();
    }

    fn snapshots(&self) -> Vec<cc_scheduler::LaneSnapshot<ChainLane>> {
        self.lock().manager.snapshots()
    }

    fn len(&self, lane: ChainLane) -> usize {
        self.lock().manager.len(lane).unwrap_or(0)
    }

    fn depth(&self, lane: ChainLane) -> usize {
        self.lock().manager.depth(lane).unwrap_or(0)
    }

    fn producers_closed(&self) -> bool {
        self.lock().producers_closed
    }

    fn mark_producers_closed(&self) {
        let mut g = self.lock();
        g.producers_closed = true;
        drop(g);
        self.space.notify_all();
        self.wake.notify();
    }

    fn close_consumer(&self) {
        let pending = {
            let mut g = self.lock();
            g.consumer_gone = true;
            g.manager.drain()
        };
        self.space.notify_all();
        self.wake.notify();
        let status = Status::unavailable("chain core thread is shut down");
        for work in pending {
            reject_core_work(work, status.clone());
        }
    }

    fn observe_depths(&self, metrics: &ChainMetrics) {
        for snap in self.snapshots() {
            metrics.set_lane_queue_depth(snap.id.as_str(), snap.len as u64);
            if snap.id == ChainLane::Import {
                metrics.set_import_queue_depth(snap.len as u64);
            }
        }
    }
}

/// Last-permit drop closes producers so an idle core can exit.
#[derive(Debug)]
struct SchedulerPermit(Arc<SharedScheduler>);

impl SchedulerPermit {
    fn new(sched: &Arc<SharedScheduler>) -> Self {
        sched.senders.fetch_add(1, Ordering::SeqCst);
        Self(Arc::clone(sched))
    }
}

impl Clone for SchedulerPermit {
    fn clone(&self) -> Self {
        Self::new(&self.0)
    }
}

impl Drop for SchedulerPermit {
    fn drop(&mut self) {
        if self.0.senders.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.0.mark_producers_closed();
        }
    }
}

/// Test/producer sender that pushes a FIFO lane through the manager.
pub struct WakingSender<T> {
    sched: Arc<SharedScheduler>,
    _permit: SchedulerPermit,
    enqueue: fn(&SharedScheduler, T) -> Result<(), mpsc::error::TrySendError<T>>,
    depth: fn(&SharedScheduler) -> usize,
}

impl<T> std::fmt::Debug for WakingSender<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WakingSender").finish_non_exhaustive()
    }
}

impl<T> WakingSender<T> {
    /// `try_send` then unpark so an idle core observes the item.
    pub fn try_send(&self, msg: T) -> Result<(), mpsc::error::TrySendError<T>> {
        (self.enqueue)(&self.sched, msg)
    }

    #[must_use]
    pub fn max_capacity(&self) -> usize {
        (self.depth)(&self.sched)
    }
}

impl<T> Clone for WakingSender<T> {
    fn clone(&self) -> Self {
        Self {
            sched: Arc::clone(&self.sched),
            _permit: self._permit.clone(),
            enqueue: self.enqueue,
            depth: self.depth,
        }
    }
}

/// Outcome of [`AttestationSender::try_push`].
#[derive(Debug)]
#[must_use]
pub enum AttestationEnqueue {
    Accepted,
    /// Lane was full; the oldest item was evicted so this one could be kept.
    EvictedOldest(AttestationWork),
    /// Consumer is gone (core thread exited); the item was not taken.
    Closed(AttestationWork),
}

/// LIFO attestation lane ([ARCH] §3.2 / S0-A-16). Pushes through the manager.
#[derive(Debug, Clone)]
pub struct AttestationSender {
    sched: Arc<SharedScheduler>,
    _permit: SchedulerPermit,
}

impl AttestationSender {
    /// Push newest-first. Full: evict oldest and return it.
    pub fn try_push(&self, work: AttestationWork) -> AttestationEnqueue {
        match self.sched.push(CoreWork::Attestation(work)) {
            SchedPush::Accepted => AttestationEnqueue::Accepted,
            SchedPush::EvictedOldest(CoreWork::Attestation(old)) => {
                AttestationEnqueue::EvictedOldest(old)
            }
            SchedPush::Closed(CoreWork::Attestation(w))
            | SchedPush::Full(CoreWork::Attestation(w)) => AttestationEnqueue::Closed(w),
            SchedPush::EvictedOldest(_) | SchedPush::Closed(_) | SchedPush::Full(_) => {
                AttestationEnqueue::Closed(AttestationWork::ApplyAttestations {
                    request: ApplyAttestationsRequest {
                        attestations_ssz: vec![],
                    },
                    reply: oneshot::channel().0,
                })
            }
        }
    }

    #[must_use]
    pub fn max_capacity(&self) -> usize {
        self.sched.depth(ChainLane::Attestation)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.sched.len(ChainLane::Attestation)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

fn reject_attestation_work(work: AttestationWork, status: Status) {
    match work {
        AttestationWork::ApplyAttestations { reply, .. } => {
            let _ = reply.send(Err(status));
        }
    }
}

fn reject_core_work(work: CoreWork, status: Status) {
    match work {
        CoreWork::Tick(TickWork::Shutdown { done }) => {
            let _ = done.send(());
        }
        CoreWork::Tick(TickWork::SlotTick) => {}
        // Drop the reply: a drained ping is unavailable, not a successful no-op.
        CoreWork::Tick(TickWork::Ping { .. }) => {}
        CoreWork::Import(ImportWork::ImportBlock { reply, .. }) => {
            let _ = reply.send(Err(status));
        }
        CoreWork::Import(ImportWork::ImportBlockGossip { reply, .. }) => {
            let _ = reply.send(Err(status));
        }
        CoreWork::Import(ImportWork::DataAvailable { .. }) => {}
        CoreWork::QueryP0(QueryP0Work::Query { reply, .. })
        | CoreWork::QueryP1(QueryP1Work::Query { reply, .. }) => {
            let _ = reply.send(Err(status));
        }
        CoreWork::QueryP0(QueryP0Work::BlockFor { reply, .. }) => {
            let _ = reply.send(());
        }
        CoreWork::Attestation(work) => reject_attestation_work(work, status),
    }
}

fn enqueue_import(
    sched: &SharedScheduler,
    work: ImportWork,
) -> Result<(), mpsc::error::TrySendError<ImportWork>> {
    match sched.push(CoreWork::Import(work)) {
        SchedPush::Accepted | SchedPush::EvictedOldest(_) => Ok(()),
        SchedPush::Full(CoreWork::Import(w)) => Err(mpsc::error::TrySendError::Full(w)),
        SchedPush::Closed(CoreWork::Import(w)) => Err(mpsc::error::TrySendError::Closed(w)),
        SchedPush::Full(_) | SchedPush::Closed(_) => Err(mpsc::error::TrySendError::Closed(
            ImportWork::DataAvailable {
                root: Root::ZERO,
                slot: 0,
            },
        )),
    }
}

fn enqueue_query_p0(
    sched: &SharedScheduler,
    work: QueryP0Work,
) -> Result<(), mpsc::error::TrySendError<QueryP0Work>> {
    match sched.push(CoreWork::QueryP0(work)) {
        SchedPush::Accepted | SchedPush::EvictedOldest(_) => Ok(()),
        SchedPush::Full(CoreWork::QueryP0(w)) => Err(mpsc::error::TrySendError::Full(w)),
        SchedPush::Closed(CoreWork::QueryP0(w)) => Err(mpsc::error::TrySendError::Closed(w)),
        SchedPush::Full(_) | SchedPush::Closed(_) => {
            Err(mpsc::error::TrySendError::Closed(QueryP0Work::Query {
                request: QueryRequest::Head,
                reply: oneshot::channel().0,
            }))
        }
    }
}

fn enqueue_query_p1(
    sched: &SharedScheduler,
    work: QueryP1Work,
) -> Result<(), mpsc::error::TrySendError<QueryP1Work>> {
    match sched.push(CoreWork::QueryP1(work)) {
        SchedPush::Accepted | SchedPush::EvictedOldest(_) => Ok(()),
        SchedPush::Full(CoreWork::QueryP1(w)) => Err(mpsc::error::TrySendError::Full(w)),
        SchedPush::Closed(CoreWork::QueryP1(w)) => Err(mpsc::error::TrySendError::Closed(w)),
        SchedPush::Full(_) | SchedPush::Closed(_) => {
            Err(mpsc::error::TrySendError::Closed(QueryP1Work::Query {
                request: QueryRequest::Head,
                reply: oneshot::channel().0,
            }))
        }
    }
}

fn attestation_depth_from_epoch(epoch: &EpochContextStore) -> usize {
    let snap = epoch.load();
    sized_from_validators(snap.active_validator_count, snap.slots_per_epoch)
}

/// Request variants for [`CoreCommand::Query`].
#[derive(Debug, Clone)]
pub enum QueryRequest {
    /// Head root / slot from the store (diagnostic / tests).
    Head,
    /// Packed committee shuffling for `epoch` from the head state (CC-1F).
    CommitteeShuffling { epoch: u64 },
    /// Validator pubkeys by resolved index list (CC-1F). Indices already bound-checked.
    ValidatorPubkeys { indices: Vec<u64> },
    /// SSZ `Validator` records by index list (CC-27a). Indices already bound-checked.
    ValidatorRecords { indices: Vec<u64> },
    /// CC-3B: optimistic status from fork choice (proto-array only — never engine).
    ///
    /// `root: None` → node-level [`is_optimistic_node`] (CC-34c both branches).
    /// `root: Some` → per-root [`is_optimistic`]; `known=false` when absent.
    IsOptimistic { root: Option<Root> },
    /// CC-44a /3: one canonical root per slot in `[start_slot, end_slot]`.
    CanonicalRoots { start_slot: u64, end_slot: u64 },
    /// Fork-choice store clock (`store.time` / `get_current_slot`).
    StoreClock,
}

impl QueryRequest {
    /// `query_p0` / Lighthouse `ApiRequestP0`: head, optimistic status, clock.
    #[must_use]
    pub const fn is_query_p0(&self) -> bool {
        matches!(
            self,
            Self::Head | Self::IsOptimistic { .. } | Self::StoreClock
        )
    }

    /// `query_p1` / Lighthouse `ApiRequestP1`: state-requiring serving reads.
    #[must_use]
    pub const fn is_query_p1(&self) -> bool {
        matches!(
            self,
            Self::CommitteeShuffling { .. }
                | Self::ValidatorPubkeys { .. }
                | Self::ValidatorRecords { .. }
                | Self::CanonicalRoots { .. }
        )
    }
}

/// Reply for the Phase-1 / CC-27a / CC-3B / CC-44a `Query` command.
#[derive(Debug, Clone)]
pub enum QueryReply {
    /// Head probe.
    Head { head_root: Root, head_slot: u64 },
    /// Served shuffling for one epoch.
    CommitteeShuffling {
        shuffled_indices: Vec<u64>,
        dependent_root: Root,
        epoch: u64,
        committees_per_slot: u64,
    },
    /// Served pubkeys (parallel to the requested indices).
    ValidatorPubkeys {
        indices: Vec<u64>,
        pubkeys: Vec<Vec<u8>>,
    },
    /// Served SSZ validator records + the head slot they were read at.
    ValidatorRecords { ssz: Vec<Vec<u8>>, slot: u64 },
    /// CC-3B: tri-state optimistic answer (`known=false` ⇒ ignore `is_optimistic`).
    IsOptimistic { is_optimistic: bool, known: bool },
    /// CC-44a: canonical roots for the requested inclusive slot range.
    CanonicalRoots { roots: Vec<Root> },
    /// Fork-choice store clock.
    StoreClock { time: u64, slot: u64 },
}

/// Configuration for spawning the core thread.
#[derive(Clone)]
pub struct CoreConfig {
    pub max_resident_states: usize,
    pub body_ring_capacity: usize,
    /// Unary `ImportBlock` / `on_block` signature strategy.
    ///
    /// Default [`BlockSignatureStrategy::VerifyIndividual`]. Durable seed
    /// replay overrides to [`BlockSignatureStrategy::NoVerification`] in
    /// [`crate::seed::seed_from_durable`] (already-verified stored blocks).
    pub verify: BlockSignatureStrategy,
    /// Shared PeerDAS available-root set (same `Arc` as the store's DA).
    ///
    /// `None` when the store was seeded with a non-PeerDAS harness DA (tests).
    pub peer_das: Option<Arc<PeerDasAvailability>>,
    /// Slots a deferred block may wait for `DataAvailable` (default 4).
    pub da_pending_timeout_slots: u64,
    /// Slots a deferred block may wait for the execution engine (default 8).
    pub engine_pending_timeout_slots: u64,
    /// In-process engine API (S1-A-06). `None` in fixture tests with no EL.
    pub engine: Option<SharedEngine>,
    /// Spawn the per-slot `SlotTick` floor (fcU floor + pending_* expiry + wall-clock
    /// `on_tick`).
    ///
    /// **Default `false`.** Fixture/integration tests carefully seed store time
    /// (e.g. Hoodi offline replay); a wall-clock tick would jump past the
    /// imported chain and leave `get_head` stranded. Production `main` enables
    /// this (CC-33 /7, CC-36a). Also gates wall-clock `on_tick` at the top of
    /// each import (P0-12).
    pub slot_tick_enabled: bool,
    /// `MAXIMUM_GOSSIP_CLOCK_DISPARITY` (config; never inlined at the check).
    pub maximum_gossip_clock_disparity: Duration,
    /// Archive persist handle (S2-A-14). `None` in fixture tests.
    pub archive: Option<crate::ArchiveWriteHandle>,
}

impl std::fmt::Debug for CoreConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoreConfig")
            .field("max_resident_states", &self.max_resident_states)
            .field("body_ring_capacity", &self.body_ring_capacity)
            .field("verify", &self.verify)
            .field("slot_tick_enabled", &self.slot_tick_enabled)
            .field("archive", &self.archive.is_some())
            .finish_non_exhaustive()
    }
}

impl Default for CoreConfig {
    fn default() -> Self {
        Self {
            max_resident_states: DEFAULT_MAX_RESIDENT_STATES,
            body_ring_capacity: DEFAULT_BODY_RING_CAPACITY,
            verify: BlockSignatureStrategy::VerifyIndividual,
            peer_das: None,
            da_pending_timeout_slots: DEFAULT_DA_PENDING_TIMEOUT_SLOTS,
            engine_pending_timeout_slots: DEFAULT_ENGINE_PENDING_TIMEOUT_SLOTS,
            engine: None,
            slot_tick_enabled: false,
            maximum_gossip_clock_disparity: DEFAULT_MAXIMUM_GOSSIP_CLOCK_DISPARITY,
            archive: None,
        }
    }
}

/// Handle to a running core thread (non-generic: scheduler + shared state only).
#[derive(Debug, Clone)]
pub struct CoreHandle {
    sched: Arc<SharedScheduler>,
    _permit: SchedulerPermit,
    head: HeadSnapshotStore,
    epoch: EpochContextStore,
    metrics: ChainMetrics,
    counters: Arc<ImportCounters>,
}

impl CoreHandle {
    /// Shared head snapshot (also used by `GetHead`).
    pub fn head(&self) -> &HeadSnapshotStore {
        &self.head
    }

    /// Shared epoch context (used by `ChainView` producer; §16/4).
    pub fn epoch_context(&self) -> &EpochContextStore {
        &self.epoch
    }

    /// Metrics handle.
    pub fn metrics(&self) -> &ChainMetrics {
        &self.metrics
    }

    /// Transition-invocation counter (DUPLICATE short-circuit tests).
    pub fn transition_count(&self) -> u64 {
        self.counters.transition_count()
    }

    /// Clone of the import-lane sender (tests that fill the lane).
    pub fn import_sender(&self) -> WakingSender<ImportWork> {
        WakingSender {
            sched: Arc::clone(&self.sched),
            _permit: SchedulerPermit::new(&self.sched),
            enqueue: enqueue_import,
            depth: |s| s.depth(ChainLane::Import),
        }
    }

    /// Clone of the `query_p0` sender (tests that fill the lane).
    pub fn query_p0_sender(&self) -> WakingSender<QueryP0Work> {
        WakingSender {
            sched: Arc::clone(&self.sched),
            _permit: SchedulerPermit::new(&self.sched),
            enqueue: enqueue_query_p0,
            depth: |s| s.depth(ChainLane::QueryP0),
        }
    }

    /// Clone of the LIFO attestation sender (tests that fill / overflow the lane).
    pub fn attestation_sender(&self) -> AttestationSender {
        AttestationSender {
            sched: Arc::clone(&self.sched),
            _permit: SchedulerPermit::new(&self.sched),
        }
    }

    /// Clone of the `query_p1` sender (tests that fill the lane).
    pub fn query_p1_sender(&self) -> WakingSender<QueryP1Work> {
        WakingSender {
            sched: Arc::clone(&self.sched),
            _permit: SchedulerPermit::new(&self.sched),
            enqueue: enqueue_query_p1,
            depth: |s| s.depth(ChainLane::QueryP1),
        }
    }

    /// `try_send` a [`TickWork::SlotTick`]. `false` if the lane is full or closed.
    ///
    /// The production ticker uses `blocking_push` so a full lane waits rather
    /// than shedding. Tests use this to observe capacity without blocking.
    #[must_use]
    pub fn try_send_slot_tick(&self) -> bool {
        matches!(
            self.sched.push(CoreWork::Tick(TickWork::SlotTick)),
            SchedPush::Accepted
        )
    }

    /// Import a block via the import lane with the 2 s send timeout.
    pub async fn import_block(
        &self,
        request: ImportBlockRequest,
    ) -> Result<ImportBlockResponse, Status> {
        let (reply, rx) = oneshot::channel();
        let cmd = ImportWork::ImportBlock { request, reply };
        if self
            .sched
            .push_timeout(CoreWork::Import(cmd), IMPORT_SEND_TIMEOUT)
            .await
            .is_err()
        {
            if self.sched.lock().consumer_gone {
                return Err(Status::unavailable("chain core thread is shut down"));
            }
            self.metrics.inc_import_rejected_backpressure();
            self.sched.observe_depths(&self.metrics);
            return Err(Status::resource_exhausted(
                "import lane full after 2s send_timeout",
            ));
        }
        self.sched.observe_depths(&self.metrics);
        rx.await
            .map_err(|_| Status::unavailable("core thread dropped import reply"))?
    }

    /// Gossip-path import (CC-27c): optional early-ACCEPT oneshot before transition.
    ///
    /// When cheap gossip conditions pass, `early_accept` is completed **before**
    /// `on_block` runs so the stream can emit a `Verdict` under the 100 ms budget.
    /// The returned [`ImportOutcome`] carries late-reject / late-internal flags
    /// for the post-transition application-score path (no second gossip report).
    pub async fn import_block_for_gossip(
        &self,
        request: ImportBlockRequest,
        early_accept: Option<oneshot::Sender<()>>,
    ) -> Result<ImportOutcome, Status> {
        let (reply, rx) = oneshot::channel();
        let cmd = ImportWork::ImportBlockGossip {
            request,
            early_accept,
            reply,
        };
        if self
            .sched
            .push_timeout(CoreWork::Import(cmd), IMPORT_SEND_TIMEOUT)
            .await
            .is_err()
        {
            if self.sched.lock().consumer_gone {
                return Err(Status::unavailable("chain core thread is shut down"));
            }
            self.metrics.inc_import_rejected_backpressure();
            self.sched.observe_depths(&self.metrics);
            return Err(Status::resource_exhausted(
                "import lane full after 2s send_timeout",
            ));
        }
        self.sched.observe_depths(&self.metrics);
        rx.await
            .map_err(|_| Status::unavailable("core thread dropped gossip import reply"))?
    }

    /// Apply a batch of free-floating attestations (CC-1E).
    ///
    /// LIFO evict-oldest: a full lane keeps this batch and fails the oldest
    /// waiter with `RESOURCE_EXHAUSTED`. No 2 s send wait — later is better.
    pub async fn apply_attestations(
        &self,
        request: ApplyAttestationsRequest,
    ) -> Result<ApplyAttestationsResponse, Status> {
        let (reply, rx) = oneshot::channel();
        let work = AttestationWork::ApplyAttestations { request, reply };
        match self.attestation_sender().try_push(work) {
            AttestationEnqueue::Accepted => {}
            AttestationEnqueue::EvictedOldest(oldest) => {
                reject_attestation_work(
                    oldest,
                    Status::resource_exhausted(
                        "apply_attestations evicted (oldest) from LIFO lane",
                    ),
                );
            }
            AttestationEnqueue::Closed(_) => {
                return Err(Status::unavailable("chain core thread is shut down"));
            }
        }
        self.sched.observe_depths(&self.metrics);
        rx.await
            .map_err(|_| Status::unavailable("core thread dropped apply_attestations reply"))?
    }

    /// `Query` command. P0 / P1 ride their own lanes.
    ///
    /// Uses the same 2 s send timeout as [`Self::import_block`] so a stalled
    /// core does not hang gRPC workers on state reads (CC-1F / CC-27a).
    pub async fn query(&self, request: QueryRequest) -> Result<QueryReply, Status> {
        let (reply, rx) = oneshot::channel();
        let work = if request.is_query_p0() {
            CoreWork::QueryP0(QueryP0Work::Query { request, reply })
        } else {
            CoreWork::QueryP1(QueryP1Work::Query { request, reply })
        };
        if self
            .sched
            .push_timeout(work, IMPORT_SEND_TIMEOUT)
            .await
            .is_err()
        {
            if self.sched.lock().consumer_gone {
                return Err(Status::unavailable("chain core thread is shut down"));
            }
            self.sched.observe_depths(&self.metrics);
            return Err(Status::resource_exhausted(
                "query lane full after 2s send_timeout",
            ));
        }
        self.sched.observe_depths(&self.metrics);
        rx.await
            .map_err(|_| Status::unavailable("core thread dropped query reply"))?
    }

    /// Block the core thread (tests). Rides `query_p0`.
    pub async fn block_for(&self, duration: Duration) -> Result<(), Status> {
        let (reply, rx) = oneshot::channel();
        self.sched
            .push_timeout(
                CoreWork::QueryP0(QueryP0Work::BlockFor { duration, reply }),
                IMPORT_SEND_TIMEOUT,
            )
            .await
            .map_err(|_| Status::unavailable("chain core thread is shut down"))?;
        rx.await
            .map_err(|_| Status::unavailable("core thread dropped block_for reply"))?;
        Ok(())
    }

    /// Notify the core that sampling completed for `root` (CC-24d).
    ///
    /// Fire-and-forget on the import lane (no reply). Uses the same 2 s
    /// send timeout as import so a stalled core surfaces as unavailable.
    pub async fn notify_data_available(&self, root: Root, slot: u64) -> Result<(), Status> {
        let cmd = ImportWork::DataAvailable { root, slot };
        match self
            .sched
            .push_timeout(CoreWork::Import(cmd), IMPORT_SEND_TIMEOUT)
            .await
        {
            Ok(()) => {
                self.sched.observe_depths(&self.metrics);
                Ok(())
            }
            Err(_) if self.sched.lock().consumer_gone => {
                Err(Status::unavailable("chain core thread is shut down"))
            }
            Err(_) => Err(Status::resource_exhausted(
                "data_available lane full after 2s send_timeout",
            )),
        }
    }

    /// No-op through the never-shed tick lane ([ARCH] §7.2 / S1-A-15).
    ///
    /// Performs no fork-choice work. The deadline lives on
    /// [`crate::liveness::probe_core_liveness`], not here, so a parked core
    /// fails the probe instead of hanging the caller forever.
    pub async fn ping(&self) -> Result<(), LivenessError> {
        let (reply, rx) = oneshot::channel();
        if self
            .sched
            .push_wait(CoreWork::Tick(TickWork::Ping {
                issued_at: Instant::now(),
                reply,
            }))
            .await
            .is_err()
        {
            return Err(LivenessError::Unavailable {
                reason: "chain core thread is shut down".into(),
            });
        }
        rx.await.map_err(|_| LivenessError::Unavailable {
            reason: "core thread dropped ping reply".into(),
        })
    }

    /// Enqueue [`TickWork::Shutdown`] on the never-shed tick lane.
    ///
    /// Does not share the import / query / attestation FIFOs, so SIGTERM
    /// pre-drain cannot wait behind a busy import lane. Prefer
    /// [`CoreThread::shutdown_and_join`] for production teardown so the
    /// oneshot wait and OS join share a **single** [`SHUTDOWN_JOIN_TIMEOUT`].
    pub async fn begin_shutdown(&self) -> Option<oneshot::Receiver<()>> {
        let (done, rx) = oneshot::channel();
        match self
            .sched
            .push_wait(CoreWork::Tick(TickWork::Shutdown { done }))
            .await
        {
            Ok(()) => Some(rx),
            Err(_) => None,
        }
    }

    /// Ask the core thread to exit and wait up to [`SHUTDOWN_JOIN_TIMEOUT`] for
    /// the done oneshot only (does **not** join the OS thread).
    ///
    /// Production pre-drain uses [`CoreThread::shutdown_and_join`] so Shutdown
    /// and OS join share one 2 s envelope under the 5 s SIGTERM process budget.
    /// This method remains for tests that join separately.
    pub async fn shutdown(&self) {
        if let Some(rx) = self.begin_shutdown().await {
            let _ = tokio::time::timeout(SHUTDOWN_JOIN_TIMEOUT, rx).await;
        }
    }
}

impl crate::liveness::CoreLiveness for CoreHandle {
    fn ping(&self) -> impl std::future::Future<Output = Result<(), LivenessError>> + Send {
        CoreHandle::ping(self)
    }
}

/// Spawn result: handle + optional join for process shutdown.
#[derive(Debug)]
pub struct CoreThread {
    pub handle: CoreHandle,
    join: Option<JoinHandle<()>>,
}

impl CoreThread {
    /// Join the OS thread (after [`CoreHandle::shutdown`] / [`Self::shutdown_and_join`]).
    pub fn join(mut self) {
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }

    /// Enqueue `Shutdown` and join the OS thread under a **single**
    /// [`SHUTDOWN_JOIN_TIMEOUT`] (Architecture §7.4).
    ///
    /// Avoids stacking two 2 s waits (oneshot + join) that would push SIGTERM
    /// past the 5 s process budget when combined with the 3 s drain.
    pub async fn shutdown_and_join(mut self) {
        use std::time::Instant;

        let deadline = Instant::now() + SHUTDOWN_JOIN_TIMEOUT;
        let rx = self.handle.begin_shutdown().await;
        let join = self.join.take();

        if let Some(rx) = rx {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if !remaining.is_zero() {
                let _ = tokio::time::timeout(remaining, rx).await;
            }
        }

        if let Some(j) = join {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                tracing::warn!(
                    timeout_secs = SHUTDOWN_JOIN_TIMEOUT.as_secs(),
                    "chain-core Shutdown oneshot exhausted budget; abandoning OS join"
                );
                // Detach: JoinHandle drop does not abort the OS thread; process
                // exit reaps it. Prefer not to block drain past the envelope.
                return;
            }
            let join_task = tokio::task::spawn_blocking(move || {
                let _ = j.join();
            });
            match tokio::time::timeout(remaining, join_task).await {
                Ok(Ok(())) => {
                    tracing::info!("chain-core thread joined");
                }
                Ok(Err(e)) => {
                    tracing::warn!(error = %e, "chain-core join task failed");
                }
                Err(_) => {
                    tracing::warn!(
                        timeout_secs = SHUTDOWN_JOIN_TIMEOUT.as_secs(),
                        "chain-core join timed out within single Shutdown+join budget; continuing drain"
                    );
                }
            }
        }
    }
}

/// Spawn the dedicated OS core thread owning `store` **by value**.
///
/// This is a dedicated OS thread (ADR-P1-09) — not a tokio task or blocking pool worker.
///
/// When `epoch` is `None`, a fresh [`EpochContextStore`] is created and published
/// at bootstrap. Callers that need the store before spawn (e.g. gRPC service)
/// pass their own.
pub fn spawn_core_thread<P: Preset + 'static>(
    store: Store<P>,
    config: ChainConfig,
    head: HeadSnapshotStore,
    event_tx: mpsc::Sender<crate::events::EventInput>,
    metrics: ChainMetrics,
    core_cfg: CoreConfig,
) -> CoreThread {
    spawn_core_thread_with_epoch(
        store,
        config,
        head,
        EpochContextStore::new(),
        event_tx,
        metrics,
        core_cfg,
    )
}

/// Like [`spawn_core_thread`] but reuses a caller-owned [`EpochContextStore`].
pub fn spawn_core_thread_with_epoch<P: Preset + 'static>(
    store: Store<P>,
    config: ChainConfig,
    head: HeadSnapshotStore,
    epoch: EpochContextStore,
    event_tx: mpsc::Sender<crate::events::EventInput>,
    metrics: ChainMetrics,
    core_cfg: CoreConfig,
) -> CoreThread {
    let lane_wake = Arc::new(LaneWake::new());
    let counters = Arc::new(ImportCounters::default());
    let counters_thread = Arc::clone(&counters);
    let head_thread = head.clone();
    let epoch_thread = epoch.clone();
    let metrics_thread = metrics.clone();

    // Publish initial snapshots from the seeded store so GetHead / ChainView
    // work immediately after spawn (tests / post-bootstrap).
    publish_initial_snapshot(&store, &head);
    publish_epoch_context_from_store(&store, &config, &epoch, 0);
    let sched = SharedScheduler::new(
        QueueSizes::new(
            epoch.load().active_validator_count,
            epoch.load().slots_per_epoch,
        ),
        Arc::clone(&lane_wake),
    );
    sched.observe_depths(&metrics);

    // Genesis-aligned never-shed ticker (P0-12 / S0-A-14). `blocking_push` on
    // the tick lane. Gated by `slot_tick_enabled` so fixture tests keep
    // exclusive control of store time.
    let _fcu_ticker = if core_cfg.slot_tick_enabled {
        Some(spawn_slot_tick_driver(
            Arc::clone(&sched),
            store.genesis_time(),
            config.seconds_per_slot,
        ))
    } else {
        None
    };

    // Runtime handle is unused after S1-A-06 (E3 is in-process). Kept off
    // the core thread so fixture tests without an engine still compile.
    let sched_thread = Arc::clone(&sched);
    let lane_wake_thread = Arc::clone(&lane_wake);

    let join = thread::Builder::new()
        .name("chain-core".into())
        .spawn(move || {
            core_loop(
                store,
                config,
                head_thread,
                epoch_thread,
                event_tx,
                metrics_thread,
                counters_thread,
                core_cfg,
                sched_thread,
                lane_wake_thread,
            );
        })
        .unwrap_or_else(|e| {
            // OS thread spawn failure is unrecoverable for the process.
            tracing::error!(error = %e, "failed to spawn chain-core thread");
            std::process::abort();
        });

    let permit = SchedulerPermit::new(&sched);
    CoreThread {
        handle: CoreHandle {
            sched,
            _permit: permit,
            head,
            epoch,
            metrics,
            counters,
        },
        join: Some(join),
    }
}

/// Resolve the head block root from the store.
fn head_root_of<P: Preset>(store: &Store<P>) -> Root {
    store
        .last_head_root()
        .or_else(|| store.head_cache().map(|c| c.head_root))
        .unwrap_or_else(|| store.justified_checkpoint().root)
}

/// Head state for Query handlers: store block_states (seeded at bootstrap and
/// kept in sync with residency pins on every import).
fn head_state<'a, P: Preset>(
    store: &'a Store<P>,
    _residency: &'a Residency<P>,
    head_root: Root,
) -> Result<&'a BeaconState<P>, Status> {
    store.block_state(&head_root).ok_or_else(|| {
        Status::failed_precondition("head state not resident; chain not ready for state queries")
    })
}

/// Decision root returned to callers for the served epoch window.
///
/// - When `start_slot(epoch) − 1` is already historical: the true decision root
///   (same as [`decision_root_for_epoch`] / `get_or_compute_shuffling`).
/// - When serving **next** epoch mid-epoch (dependent slot still current/future):
///   the **current** epoch's true decision root — stable for the whole epoch on
///   a branch, branch-distinguishing across forks, and never thrashing per slot.
///   Next-epoch seed is already fixed from RANDAO (`MIN_SEED_LOOKAHEAD`); the
///   assignment does not change as head advances within the epoch.
fn dependent_root_for_epoch_served<P: Preset>(
    state: &BeaconState<P>,
    epoch: Epoch,
) -> Result<Root, Status> {
    match decision_root_for_epoch(state, epoch) {
        Ok(root) => Ok(root),
        Err(_) => {
            // Next-epoch mid-window: stable provisional = current epoch decision root.
            let current = get_current_epoch(state);
            decision_root_for_epoch(state, current).map_err(|e| {
                Status::internal(format!(
                    "stable provisional dependent root (current epoch {}) for next epoch {}: {e}",
                    current.as_u64(),
                    epoch.as_u64()
                ))
            })
        }
    }
}

/// Packed shuffling for `epoch`.
///
/// When the strict decision root is available, uses [`get_or_compute_shuffling`]
/// so the head-state [`cc_types::ShufflingCache`] key matches in-process
/// `get_beacon_committee`. When only a provisional root is available, computes
/// without inserting under a foreign key (avoids duplicate/orphan cache entries).
fn shuffled_for_epoch<P: Preset>(state: &BeaconState<P>, epoch: Epoch) -> Result<Vec<u64>, Status> {
    if decision_root_for_epoch(state, epoch).is_ok() {
        let shuffling = get_or_compute_shuffling(state, epoch).map_err(|e| {
            Status::internal(format!(
                "compute shuffling for epoch {}: {e}",
                epoch.as_u64()
            ))
        })?;
        return Ok(shuffling.shuffled.iter().map(|vi| vi.as_u64()).collect());
    }
    // Provisional next-epoch path: seed-only compute; do not warm ShufflingCache
    // under a non-strict key (that would never match get_or_compute_shuffling).
    let computed = compute_shuffled_active_indices(state, epoch).map_err(|e| {
        Status::internal(format!(
            "compute shuffling for epoch {}: {e}",
            epoch.as_u64()
        ))
    })?;
    Ok(computed.shuffled.iter().map(|vi| vi.as_u64()).collect())
}

fn handle_query<P: Preset>(
    store: &Store<P>,
    residency: &Residency<P>,
    request: QueryRequest,
) -> Result<QueryReply, Status> {
    let head_root = head_root_of(store);
    match request {
        QueryRequest::Head => {
            let head_slot = store
                .blocks()
                .get(&head_root)
                .map(|h| h.slot.as_u64())
                .unwrap_or(0);
            Ok(QueryReply::Head {
                head_root,
                head_slot,
            })
        }
        QueryRequest::CommitteeShuffling { epoch } => {
            let state = head_state(store, residency, head_root)?;
            let current = get_current_epoch(state).as_u64();
            let next = current.saturating_add(1);
            if epoch != current && epoch != next {
                return Err(Status::failed_precondition(format!(
                    "GetCommitteeShuffling serves only head current and next epoch \
                     (current={current}, next={next}, requested={epoch})"
                )));
            }
            let epoch_ty = Epoch::new(epoch);
            let dependent_root = dependent_root_for_epoch_served(state, epoch_ty)?;
            let shuffled_indices = shuffled_for_epoch(state, epoch_ty)?;
            let committees_per_slot = get_committee_count_per_slot(state, epoch_ty);
            Ok(QueryReply::CommitteeShuffling {
                shuffled_indices,
                dependent_root,
                epoch,
                committees_per_slot,
            })
        }
        QueryRequest::ValidatorPubkeys { indices } => {
            if indices.len() as u64 > MAX_VALIDATOR_PUBKEYS_PER_REQUEST {
                return Err(Status::invalid_argument(format!(
                    "GetValidatorPubkeys bound is {MAX_VALIDATOR_PUBKEYS_PER_REQUEST} indices; \
                     got {}",
                    indices.len()
                )));
            }
            let state = head_state(store, residency, head_root)?;
            let mut pubkeys = Vec::with_capacity(indices.len());
            for &idx in &indices {
                let v = state.validators_get(idx as usize).ok_or_else(|| {
                    Status::invalid_argument(format!(
                        "validator index {idx} out of range (registry len {})",
                        state.validators_len()
                    ))
                })?;
                pubkeys.push(v.pubkey.as_slice().to_vec());
            }
            Ok(QueryReply::ValidatorPubkeys { indices, pubkeys })
        }
        QueryRequest::ValidatorRecords { indices } => {
            if indices.len() as u64 > MAX_VALIDATOR_RECORDS_PER_REQUEST {
                return Err(Status::invalid_argument(format!(
                    "GetValidatorRecords bound is {MAX_VALIDATOR_RECORDS_PER_REQUEST} indices; \
                     got {}",
                    indices.len()
                )));
            }
            let state = head_state(store, residency, head_root)?;
            let slot = state.slot().as_u64();
            let mut ssz = Vec::with_capacity(indices.len());
            for &idx in &indices {
                let v = state.validators_get(idx as usize).ok_or_else(|| {
                    Status::invalid_argument(format!(
                        "validator index {idx} out of range (registry len {})",
                        state.validators_len()
                    ))
                })?;
                ssz.push(v.as_ssz_bytes());
            }
            Ok(QueryReply::ValidatorRecords { ssz, slot })
        }
        // CC-3B: answers from fork choice only (is_optimistic / is_optimistic_node).
        // EngineState is never consulted here — el_offline is EngineService's job.
        QueryRequest::IsOptimistic { root } => match root {
            None => Ok(QueryReply::IsOptimistic {
                is_optimistic: is_optimistic_node(store),
                known: true,
            }),
            Some(r) => match is_optimistic(store, r) {
                Some(flag) => Ok(QueryReply::IsOptimistic {
                    is_optimistic: flag,
                    known: true,
                }),
                // Unknown root: known=false so Phase 6 cannot invent is_optimistic=false.
                None => Ok(QueryReply::IsOptimistic {
                    is_optimistic: false,
                    known: false,
                }),
            },
        },
        // CC-44a /3: walk the head's parent chain; one root per slot in range.
        QueryRequest::CanonicalRoots {
            start_slot,
            end_slot,
        } => {
            let finalized = store.finalized_checkpoint();
            let finalized_slot = compute_start_slot_at_epoch::<P>(finalized.epoch).as_u64();
            if start_slot < finalized_slot {
                return Err(crate::service::status_below_finalized(
                    start_slot,
                    finalized_slot,
                ));
            }
            let head = head_root_of(store);
            let mut roots =
                Vec::with_capacity(end_slot.saturating_sub(start_slot).saturating_add(1) as usize);
            for s in start_slot..=end_slot {
                let root = store
                    .proto_array()
                    .get_ancestor(head, Slot::new(s))
                    .unwrap_or(head);
                roots.push(root);
            }
            Ok(QueryReply::CanonicalRoots { roots })
        }
        QueryRequest::StoreClock => Ok(QueryReply::StoreClock {
            time: store.time(),
            slot: store.get_current_slot().as_u64(),
        }),
    }
}

/// Build and publish [`EpochContext`] from the head state's registry / lookahead.
pub fn build_epoch_context<P: Preset>(
    state: &BeaconState<P>,
    config: &ChainConfig,
    sequence: u64,
) -> EpochContext {
    let epoch = get_current_epoch(state);
    let active = get_active_validator_indices(state, epoch);
    let mut proposer_lookahead = Vec::with_capacity(state.proposer_lookahead_len());
    let mut proposer_pubkeys = Vec::with_capacity(state.proposer_lookahead_len());
    for i in 0..state.proposer_lookahead_len() {
        let idx = state
            .proposer_lookahead_get(i)
            .map(|v| v.as_u64())
            .unwrap_or(0);
        proposer_lookahead.push(idx);
        let pk = state
            .validators_get(idx as usize)
            .map(|v| v.pubkey.as_slice().to_vec())
            .unwrap_or_else(|| vec![0u8; 48]);
        proposer_pubkeys.push(pk);
    }
    EpochContext {
        epoch,
        proposer_lookahead,
        proposer_pubkeys,
        active_validator_count: active.len() as u64,
        genesis_time: state.genesis_time(),
        genesis_validators_root: state.genesis_validators_root(),
        seconds_per_slot: config.seconds_per_slot,
        slots_per_epoch: P::SLOTS_PER_EPOCH,
        sequence,
    }
}

fn publish_epoch_context_from_store<P: Preset>(
    store: &Store<P>,
    config: &ChainConfig,
    epoch_store: &EpochContextStore,
    sequence: u64,
) {
    let head_root = head_root_of(store);
    if let Some(state) = store.block_state(&head_root) {
        epoch_store.store(build_epoch_context(state, config, sequence));
    } else {
        // No resident state yet — publish stable chain params only.
        epoch_store.store(EpochContext {
            seconds_per_slot: config.seconds_per_slot,
            slots_per_epoch: P::SLOTS_PER_EPOCH,
            sequence,
            ..EpochContext::default()
        });
    }
}

fn publish_initial_snapshot<P: Preset>(store: &Store<P>, head: &HeadSnapshotStore) {
    let justified = store.justified_checkpoint();
    let finalized = store.finalized_checkpoint();
    let head_root = store.last_head_root().unwrap_or(justified.root);
    let head_slot = store
        .blocks()
        .get(&head_root)
        .map(|h| h.slot)
        .unwrap_or_else(|| store.get_current_slot());
    let head_state_root = store
        .blocks()
        .get(&head_root)
        .map(|h| h.state_root)
        .unwrap_or(Root::ZERO);
    head.store(HeadSnapshot {
        head_root,
        head_slot,
        head_state_root,
        justified,
        finalized,
        unrealized_justified: store.unrealized_justified_checkpoint(),
        unrealized_finalized: store.unrealized_finalized_checkpoint(),
        current_epoch_target_root: Root::ZERO,
        dependent_root: Root::ZERO,
        // CC-3B: node-level predicate from fork choice (not engine liveness).
        is_optimistic: is_optimistic_node(store),
        sequence: 0,
    });
}

#[allow(clippy::too_many_arguments)]
fn core_loop<P: Preset>(
    mut store: Store<P>,
    config: ChainConfig,
    head: HeadSnapshotStore,
    epoch: EpochContextStore,
    event_tx: mpsc::Sender<crate::events::EventInput>,
    metrics: ChainMetrics,
    counters: Arc<ImportCounters>,
    core_cfg: CoreConfig,
    sched: Arc<SharedScheduler>,
    lane_wake: Arc<LaneWake>,
) {
    let mut residency =
        Residency::<P>::new(core_cfg.max_resident_states, core_cfg.body_ring_capacity);
    // Seed residency from the anchor (finalized / justified root).
    let anchor_root = store.finalized_checkpoint().root;
    if let Some(st) = store.block_state(&anchor_root).cloned() {
        residency.seed_anchor(anchor_root, st);
        metrics.set_resident_states(residency.resident_count() as u64);
    }

    let mut snapshot_sequence: u64 = 0;
    let mut epoch_sequence: u64 = epoch.load().sequence;
    let mut last_published_epoch = epoch.load().epoch.as_u64();
    let verify = core_cfg.verify;
    let slot_tick_enabled = core_cfg.slot_tick_enabled;
    let gossip_disparity = core_cfg.maximum_gossip_clock_disparity;
    let peer_das = core_cfg.peer_das;
    let da_timeout_slots = core_cfg.da_pending_timeout_slots.max(1);
    let engine_timeout_slots = core_cfg.engine_pending_timeout_slots.max(1);
    let mut pending_da = PendingDa::new();
    let mut pending_engine = PendingEngine::new();
    // Last observed engine Online bit (CC-36a Offline→Online redrive edge).
    let mut last_engine_online = false;
    let engine = core_cfg.engine.clone();
    let archive = core_cfg.archive.clone();

    // CC-33: forkchoiceUpdated driver (off attestation path — after import /
    // on slot tick). Direct `cc-engine-api` call (S1-A-06).
    let fcu: Option<FcuDriver<crate::engine::DirectEngine>> = engine.as_ref().map(|e| {
        tracing::info!(session_id = e.session_id(), "fcU driver armed (CC-33)");
        FcuDriver::new(Arc::clone(e))
    });
    if fcu.is_none() {
        tracing::debug!("fcU driver disabled (no in-process engine on core spawn)");
    }

    loop {
        sched.set_depth(ChainLane::Attestation, attestation_depth_from_epoch(&epoch));
        // Slot-bounded timeout: drop permanently unavailable parked blocks.
        expire_pending_da(
            &mut pending_da,
            store.get_current_slot().as_u64(),
            da_timeout_slots,
            &metrics,
        );
        expire_pending_engine(
            &mut pending_engine,
            store.get_current_slot().as_u64(),
            engine_timeout_slots,
            &metrics,
        );
        if let Some(ref da) = peer_das {
            metrics.set_da_available_occupancy(da.len() as u64);
        }
        let incoming = match sched.select() {
            Some(w) => w,
            None if sched.producers_closed() => {
                break;
            }
            None => {
                lane_wake.park_current();
                continue;
            }
        };
        sched.observe_depths(&metrics);
        let cmd = CoreCommand::from(incoming);
        match cmd {
            CoreCommand::ImportBlock { request, reply } => {
                if slot_tick_enabled {
                    advance_store_clock(&mut store);
                }
                let outcome = import_block_with_early(
                    &mut store,
                    &mut residency,
                    &config,
                    &head,
                    &event_tx,
                    &metrics,
                    &counters,
                    &mut snapshot_sequence,
                    request,
                    verify,
                    None,
                    None,
                    None,
                    Some(&mut pending_da),
                    Some(&mut pending_engine),
                    import_gossip_clock(slot_tick_enabled, gossip_disparity),
                    archive.as_ref(),
                );
                metrics.set_da_pending_occupancy(pending_da.len() as u64);
                metrics.set_pending_engine_occupancy(pending_engine.len() as u64);
                // CC-38a: fire template-sized FetchBlobs on DA-defer (never cells).
                if let Ok(ref o) = outcome
                    && let Some(trigger) = o.block_branch.as_ref()
                    && let Some(e) = engine.as_ref()
                {
                    e.fetch_blobs(trigger);
                }
                // Republish EpochContext when the head epoch advances (§16/4).
                if outcome.is_ok() {
                    maybe_publish_epoch_context(
                        &store,
                        &config,
                        &epoch,
                        &mut epoch_sequence,
                        &mut last_published_epoch,
                    );
                }
                // Post-import fcU only for a head this call may publish.
                if outcome.as_ref().is_ok_and(|o| o.publish_fcu)
                    && durable_head_may_fcu(&store, &head, archive.as_ref())
                {
                    emit_fcu_head(&store, fcu.as_ref());
                }
                let _ = reply.send(outcome.map(|o| o.response));
            }
            CoreCommand::ImportBlockGossip {
                request,
                early_accept,
                reply,
            } => {
                if slot_tick_enabled {
                    advance_store_clock(&mut store);
                }
                let epoch_snapshot = epoch.load();
                let outcome = import_block_with_early(
                    &mut store,
                    &mut residency,
                    &config,
                    &head,
                    &event_tx,
                    &metrics,
                    &counters,
                    &mut snapshot_sequence,
                    request,
                    verify,
                    Some(epoch_snapshot.as_ref()),
                    early_accept,
                    None, // production: no inject
                    Some(&mut pending_da),
                    Some(&mut pending_engine),
                    import_gossip_clock(slot_tick_enabled, gossip_disparity),
                    archive.as_ref(),
                );
                metrics.set_da_pending_occupancy(pending_da.len() as u64);
                metrics.set_pending_engine_occupancy(pending_engine.len() as u64);
                // CC-38a block-branch (same as ImportBlock; template-only).
                if let Ok(ref o) = outcome
                    && let Some(trigger) = o.block_branch.as_ref()
                    && let Some(e) = engine.as_ref()
                {
                    e.fetch_blobs(trigger);
                }
                if outcome.is_ok() {
                    maybe_publish_epoch_context(
                        &store,
                        &config,
                        &epoch,
                        &mut epoch_sequence,
                        &mut last_published_epoch,
                    );
                }
                if outcome.as_ref().is_ok_and(|o| o.publish_fcu)
                    && durable_head_may_fcu(&store, &head, archive.as_ref())
                {
                    emit_fcu_head(&store, fcu.as_ref());
                }
                let _ = reply.send(outcome);
            }
            CoreCommand::ApplyAttestations { request, reply } => {
                let outcome = apply_attestations(
                    &mut store,
                    &head,
                    &event_tx,
                    &metrics,
                    &mut snapshot_sequence,
                    request,
                    &config,
                    archive.as_ref(),
                );
                // Attestations can move head; re-point EL when they do.
                // A failed `set_head` keeps the per-item response and skips fcU.
                if outcome.as_ref().is_ok_and(|done| done.publish_fcu)
                    && durable_head_may_fcu(&store, &head, archive.as_ref())
                {
                    emit_fcu_head(&store, fcu.as_ref());
                }
                let _ = reply.send(outcome.map(|done| done.response));
            }
            CoreCommand::Query { request, reply } => {
                let outcome = handle_query(&store, &residency, request);
                let _ = reply.send(outcome);
            }
            CoreCommand::BlockFor { duration, reply } => {
                thread::sleep(duration);
                let _ = reply.send(());
            }
            CoreCommand::DataAvailable { root, slot } => {
                if slot_tick_enabled {
                    advance_store_clock(&mut store);
                }
                let publish_fcu = handle_data_available(
                    &mut store,
                    &mut residency,
                    &config,
                    &head,
                    &event_tx,
                    &metrics,
                    &counters,
                    &mut snapshot_sequence,
                    &mut pending_da,
                    peer_das.as_ref(),
                    verify,
                    root,
                    slot,
                    &mut epoch_sequence,
                    &mut last_published_epoch,
                    &epoch,
                    import_gossip_clock(slot_tick_enabled, gossip_disparity),
                    archive.as_ref(),
                );
                // Re-import may have moved head. Invalid, deferred, and errors do not.
                if publish_fcu && durable_head_may_fcu(&store, &head, archive.as_ref()) {
                    emit_fcu_head(&store, fcu.as_ref());
                }
            }
            CoreCommand::Ping { issued_at, reply } => {
                tracing::trace!(?issued_at, "core liveness ping");
                let _ = reply.send(());
            }
            CoreCommand::SlotTick => {
                handle_slot_tick(
                    &mut store,
                    &mut pending_da,
                    &mut pending_engine,
                    da_timeout_slots,
                    engine_timeout_slots,
                    &metrics,
                    engine.as_ref(),
                    &mut last_engine_online,
                    &mut residency,
                    &config,
                    &head,
                    &event_tx,
                    &counters,
                    &mut snapshot_sequence,
                    &mut epoch_sequence,
                    &mut last_published_epoch,
                    &epoch,
                    fcu.as_ref(),
                    verify,
                    archive.as_ref(),
                );
            }
            CoreCommand::Shutdown { done } => {
                let _ = done.send(());
                break;
            }
        }
        sched.observe_depths(&metrics);
    }
    sched.close_consumer();
}

fn import_gossip_clock(enabled: bool, disparity: Duration) -> Option<GossipClock> {
    enabled.then_some(GossipClock {
        now_millis: unix_now_millis(),
        disparity,
    })
}

/// Genesis-aligned never-shed ticker. `blocking_push` on the tick lane.
fn spawn_slot_tick_driver(
    sched: Arc<SharedScheduler>,
    genesis_time: u64,
    seconds_per_slot: u64,
) -> thread::JoinHandle<()> {
    thread::Builder::new()
        .name("chain-slot-tick".into())
        .spawn(move || {
            loop {
                let wait = duration_until_next_slot_boundary(
                    std::time::SystemTime::now(),
                    genesis_time,
                    seconds_per_slot,
                );
                thread::sleep(wait);
                if sched
                    .blocking_push(CoreWork::Tick(TickWork::SlotTick))
                    .is_err()
                {
                    break;
                }
            }
        })
        .unwrap_or_else(|e| {
            tracing::error!(error = %e, "failed to spawn chain-slot-tick thread");
            std::process::abort();
        })
}

fn apply_tick_clock<P: Preset>(
    store: &mut Store<P>,
    pending_da: &mut PendingDa,
    pending_engine: &mut PendingEngine,
    da_timeout_slots: u64,
    engine_timeout_slots: u64,
    metrics: &ChainMetrics,
) {
    let now = unix_now_secs();
    if let Err(e) = on_tick(store, now) {
        tracing::debug!(error = %e, now, "on_tick on SlotTick skipped");
    }
    let current_slot = store.get_current_slot().as_u64();
    expire_pending_da(pending_da, current_slot, da_timeout_slots, metrics);
    expire_pending_engine(pending_engine, current_slot, engine_timeout_slots, metrics);
}

#[allow(clippy::too_many_arguments)]
fn handle_slot_tick<P: Preset>(
    store: &mut Store<P>,
    pending_da: &mut PendingDa,
    pending_engine: &mut PendingEngine,
    da_timeout_slots: u64,
    engine_timeout_slots: u64,
    metrics: &ChainMetrics,
    engine: Option<&SharedEngine>,
    last_engine_online: &mut bool,
    residency: &mut Residency<P>,
    config: &ChainConfig,
    head: &HeadSnapshotStore,
    event_tx: &mpsc::Sender<crate::events::EventInput>,
    counters: &Arc<ImportCounters>,
    snapshot_sequence: &mut u64,
    epoch_sequence: &mut u64,
    last_published_epoch: &mut u64,
    epoch: &EpochContextStore,
    fcu: Option<&FcuDriver<crate::engine::DirectEngine>>,
    verify: BlockSignatureStrategy,
    archive: Option<&crate::ArchiveWriteHandle>,
) {
    apply_tick_clock(
        store,
        pending_da,
        pending_engine,
        da_timeout_slots,
        engine_timeout_slots,
        metrics,
    );

    if let Some(e) = engine {
        let online = e.is_online();
        if online && !*last_engine_online && !pending_engine.is_empty() {
            tracing::info!(
                n = pending_engine.len(),
                "engine online; re-driving pending_engine"
            );
            let publish_fcu = redrive_pending_engine(
                store,
                residency,
                config,
                head,
                event_tx,
                metrics,
                counters,
                snapshot_sequence,
                pending_da,
                pending_engine,
                verify,
                epoch_sequence,
                last_published_epoch,
                epoch,
                archive,
            );
            if publish_fcu && durable_head_may_fcu(store, head, archive) {
                emit_fcu_head(store, fcu);
            }
        }
        *last_engine_online = online;
    }

    if let Some(driver) = fcu {
        let slot = store.get_current_slot();
        if let Err(e) = driver.on_slot(slot) {
            tracing::warn!(error = %e, slot = slot.as_u64(), "fcU per-slot floor failed");
        }
    }
}

/// fcU only for a head this process has durably committed.
///
/// No archive handle means a fixture import: there is no durable write to
/// wait for. With an archive, the head root about to be sent must be the
/// last `set_head` / `commit_import.head` that returned `Ok`.
fn durable_head_may_fcu<P: Preset>(
    store: &Store<P>,
    head: &HeadSnapshotStore,
    archive: Option<&crate::ArchiveWriteHandle>,
) -> bool {
    if archive.is_none() {
        return true;
    }
    match head.durable_head() {
        Some(root) => head_root_of(store) == root,
        None => false,
    }
}

/// Post-import / post-attestation fcU emission (errors are logged, never fatal).
fn emit_fcu_head<P: Preset>(
    store: &Store<P>,
    fcu: Option<&FcuDriver<crate::engine::DirectEngine>>,
) {
    let Some(driver) = fcu else {
        return;
    };
    let head_root = head_root_of(store);
    match driver.on_head_update(store, head_root) {
        Ok(true) => {
            tracing::debug!(?head_root, "fcU emitted after head update");
        }
        Ok(false) => {
            tracing::debug!(?head_root, "fcU dropped as superseded");
        }
        Err(e) => {
            tracing::warn!(error = %e, ?head_root, "fcU emission failed");
        }
    }
}

/// Drop timed-out `pending_da` entries and bump the metric.
fn expire_pending_da(
    pending: &mut PendingDa,
    current_slot: u64,
    timeout_slots: u64,
    metrics: &ChainMetrics,
) {
    let dropped = pending.expire(current_slot, timeout_slots);
    if !dropped.is_empty() {
        metrics.inc_da_pending_dropped(dropped.len() as u64);
        for e in &dropped {
            tracing::debug!(
                root = %e.root,
                slot = e.slot,
                parked_at_slot = e.parked_at_slot,
                current_slot,
                "pending_da entry dropped after timeout"
            );
        }
    }
    metrics.set_da_pending_occupancy(pending.len() as u64);
}

/// Mark root available and re-drive a parked block when present (CC-24d).
#[allow(clippy::too_many_arguments)]
fn handle_data_available<P: Preset>(
    store: &mut Store<P>,
    residency: &mut Residency<P>,
    config: &ChainConfig,
    head: &HeadSnapshotStore,
    event_tx: &mpsc::Sender<crate::events::EventInput>,
    metrics: &ChainMetrics,
    counters: &ImportCounters,
    snapshot_sequence: &mut u64,
    pending_da: &mut PendingDa,
    peer_das: Option<&Arc<PeerDasAvailability>>,
    verify: BlockSignatureStrategy,
    root: Root,
    slot: u64,
    epoch_sequence: &mut u64,
    last_published_epoch: &mut u64,
    epoch: &EpochContextStore,
    gossip_clock: Option<GossipClock>,
    archive: Option<&crate::ArchiveWriteHandle>,
) -> bool {
    if let Some(da) = peer_das {
        da.mark_available(root);
        metrics.set_da_available_occupancy(da.len() as u64);
    } else {
        tracing::trace!(
            %root,
            slot,
            "DataAvailable received but no PeerDasAvailability handle; mark skipped"
        );
    }

    let Some(entry) = pending_da.take(&root) else {
        // Signal arrived before the block — import will succeed on first attempt.
        metrics.set_da_pending_occupancy(pending_da.len() as u64);
        tracing::debug!(%root, slot, "DataAvailable; no pending_da entry");
        return true;
    };
    metrics.set_da_pending_occupancy(pending_da.len() as u64);
    // The parked row stays `Deferred` until the re-drive returns `Imported`.
    // `finish_imported` writes `da: Available`. Invalid and errors put the
    // entry back and do not fcU.
    tracing::debug!(%root, slot, "DataAvailable; re-driving pending_da entry");

    let request = ImportBlockRequest {
        ssz: entry.ssz.to_vec(),
        fork: entry.fork,
        root: root.as_slice().to_vec(),
        source: if entry.source == 0 {
            Source::Gossip as i32
        } else {
            entry.source
        },
    };
    let outcome = import_block_with_early(
        store,
        residency,
        config,
        head,
        event_tx,
        metrics,
        counters,
        snapshot_sequence,
        request,
        verify,
        None,
        None,
        None,
        Some(pending_da),
        None, // re-drive is DA-only; engine map is separate
        gossip_clock,
        archive,
    );
    if outcome.is_ok() {
        maybe_publish_epoch_context(store, config, epoch, epoch_sequence, last_published_epoch);
    }
    let imported = outcome.as_ref().is_ok_and(|o| {
        let verdict = o.response.verdict;
        verdict == ImportBlockVerdict::Imported as i32
            || verdict == ImportBlockVerdict::Duplicate as i32
    });
    if !imported {
        if !pending_da.contains(&root) {
            pending_da.insert(entry);
            metrics.set_da_pending_occupancy(pending_da.len() as u64);
        }
        return false;
    }
    outcome.is_ok_and(|o| o.publish_fcu)
}

/// Drop timed-out `pending_engine` entries and bump the metric (CC-36a).
fn expire_pending_engine(
    pending: &mut PendingEngine,
    current_slot: u64,
    timeout_slots: u64,
    metrics: &ChainMetrics,
) {
    let dropped = pending.expire(current_slot, timeout_slots);
    if !dropped.is_empty() {
        metrics.inc_pending_engine_dropped(dropped.len() as u64);
        for e in &dropped {
            tracing::debug!(
                root = %e.root,
                parked_at_slot = e.parked_at_slot,
                current_slot,
                "pending_engine entry dropped after timeout"
            );
        }
    }
    metrics.set_pending_engine_occupancy(pending.len() as u64);
}

/// Re-import every parked engine-deferred block (Offline → Online edge).
#[allow(clippy::too_many_arguments)]
fn redrive_pending_engine<P: Preset>(
    store: &mut Store<P>,
    residency: &mut Residency<P>,
    config: &ChainConfig,
    head: &HeadSnapshotStore,
    event_tx: &mpsc::Sender<crate::events::EventInput>,
    metrics: &ChainMetrics,
    counters: &ImportCounters,
    snapshot_sequence: &mut u64,
    pending_da: &mut PendingDa,
    pending_engine: &mut PendingEngine,
    verify: BlockSignatureStrategy,
    epoch_sequence: &mut u64,
    last_published_epoch: &mut u64,
    epoch: &EpochContextStore,
    archive: Option<&crate::ArchiveWriteHandle>,
) -> bool {
    let entries = pending_engine.drain_oldest_first();
    metrics.set_pending_engine_occupancy(pending_engine.len() as u64);
    let mut publish_fcu = false;
    for entry in entries {
        let root = entry.root;
        let request = ImportBlockRequest {
            ssz: entry.ssz.to_vec(),
            fork: entry.fork,
            root: root.as_slice().to_vec(),
            source: if entry.source == 0 {
                Source::Gossip as i32
            } else {
                entry.source
            },
        };
        tracing::debug!(%root, slot = entry.slot, "re-driving pending_engine entry");
        let outcome = import_block_with_early(
            store,
            residency,
            config,
            head,
            event_tx,
            metrics,
            counters,
            snapshot_sequence,
            request,
            verify,
            None,
            None,
            None,
            Some(pending_da),
            Some(pending_engine),
            None,
            archive,
        );
        metrics.set_da_pending_occupancy(pending_da.len() as u64);
        metrics.set_pending_engine_occupancy(pending_engine.len() as u64);
        if outcome.is_ok() {
            maybe_publish_epoch_context(store, config, epoch, epoch_sequence, last_published_epoch);
        }
        if outcome.is_ok_and(|o| o.publish_fcu) {
            publish_fcu = true;
        }
    }
    publish_fcu
}

/// Publish a new [`EpochContext`] when the head state's epoch has advanced.
fn maybe_publish_epoch_context<P: Preset>(
    store: &Store<P>,
    config: &ChainConfig,
    epoch_store: &EpochContextStore,
    epoch_sequence: &mut u64,
    last_published_epoch: &mut u64,
) {
    let head_root = head_root_of(store);
    let Some(state) = store.block_state(&head_root) else {
        return;
    };
    let current = get_current_epoch(state).as_u64();
    if current == *last_published_epoch && epoch_store.load().sequence > 0 {
        return;
    }
    *epoch_sequence = epoch_sequence.saturating_add(1);
    *last_published_epoch = current;
    epoch_store.store(build_epoch_context(state, config, *epoch_sequence));
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

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

    use super::*;
    use std::sync::Arc;

    use cc_fork_choice::{HarnessAvailability, get_forkchoice_store};
    use cc_proto::chain::{ApplyAttestationsRequest, ImportBlockRequest};
    use cc_scheduler::{
        IMPORT_LANE_DEPTH, MIN_QUEUE_LEN, QUERY_P0_LANE_DEPTH, QUERY_P1_LANE_DEPTH,
        TICK_LANE_DEPTH, sized_from_validators,
    };
    use cc_types::config::{BlobParameters, BlobSchedule, PresetName};
    use cc_types::preset::Minimal;
    use cc_types::primitives::{Epoch, ExecutionAddress, ForkVersion, Slot, ValidatorIndex};
    use cc_types::{BeaconBlock, BeaconState};
    use prometheus_client::registry::Registry;
    use tokio::sync::{mpsc, oneshot};
    use tree_hash::TreeHash;

    use crate::events::{EventsConfig, EventsHandle};
    use crate::tick::unix_now_secs;

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

    #[test]
    fn core_config_default_verify_is_verify_individual() {
        assert_eq!(
            CoreConfig::default().verify,
            BlockSignatureStrategy::VerifyIndividual
        );
    }

    fn seeded_store() -> (Store<Minimal>, Root, ChainConfig) {
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
        let anchor_root = Root::from_hash256(TreeHash::tree_hash_root(&anchor_block));
        (store, anchor_root, config)
    }

    #[tokio::test]
    async fn core_thread_is_os_thread_query_works() {
        let (store, anchor, config) = seeded_store();
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let events = EventsHandle::spawn(EventsConfig {
            ring_capacity: 16,
            subscriber_queue_capacity: 8,
            session_id: Some(1),
            ring_bytes: usize::MAX,
        });
        let head = HeadSnapshotStore::new();
        let core = spawn_core_thread(
            store,
            config,
            head.clone(),
            events.event_sender(),
            metrics,
            CoreConfig::default(),
        );
        let q = core.handle.query(QueryRequest::Head).await.unwrap();
        let head_root = match q {
            QueryReply::Head {
                head_root,
                head_slot: _,
            } => head_root,
            QueryReply::CommitteeShuffling { .. }
            | QueryReply::ValidatorPubkeys { .. }
            | QueryReply::ValidatorRecords { .. }
            | QueryReply::IsOptimistic { .. }
            | QueryReply::CanonicalRoots { .. }
            | QueryReply::StoreClock { .. } => {
                unreachable!("Head request must yield Head reply")
            }
        };
        assert_eq!(head_root, anchor);
        // GetHead snapshot was seeded.
        assert_eq!(head.load().head_root, anchor);
        core.handle.shutdown().await;
        core.join();
        events.shutdown().await;
    }

    #[tokio::test]
    async fn get_head_unaffected_while_core_blocked() {
        let (store, _anchor, config) = seeded_store();
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let events = EventsHandle::spawn(EventsConfig::default());
        let head = HeadSnapshotStore::new();
        let core = spawn_core_thread(
            store,
            config,
            head.clone(),
            events.event_sender(),
            metrics,
            CoreConfig::default(),
        );

        // Kick off a 2s block on the core without waiting.
        let h = core.handle.clone();
        let block_task = tokio::spawn(async move {
            h.block_for(Duration::from_secs(2)).await.unwrap();
        });
        // Give the core thread time to enter sleep.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let start = std::time::Instant::now();
        let snap = head.load();
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(10),
            "GetHead snapshot load took {elapsed:?}, expected < 10ms"
        );
        assert_eq!(snap.sequence, 0);

        block_task.await.unwrap();
        core.handle.shutdown().await;
        core.join();
        events.shutdown().await;
    }

    async fn store_clock(handle: &CoreHandle) -> (u64, u64) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            match handle.query(QueryRequest::StoreClock).await {
                Ok(QueryReply::StoreClock { time, slot }) => return (time, slot),
                Ok(other) => panic!("expected StoreClock, got {other:?}"),
                Err(e) if e.code() == tonic::Code::ResourceExhausted => {
                    if std::time::Instant::now() >= deadline {
                        panic!("StoreClock still backpressured: {e}");
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(e) => panic!("StoreClock query failed: {e}"),
            }
        }
    }

    /// [ARCH] §2.2 policy-D conformance (`S1-A-11`): ticks are not silently
    /// dropped when every other lane is saturated. Landed `S0-A-14`.
    #[tokio::test]
    async fn slot_tick_is_never_shed() {
        let (store, _anchor, config) = seeded_store();
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let events = EventsHandle::spawn(EventsConfig::default());
        let head = HeadSnapshotStore::new();
        let core = spawn_core_thread(
            store,
            config,
            head,
            events.event_sender(),
            metrics,
            CoreConfig::default(),
        );

        let h = core.handle.clone();
        let blocker = tokio::spawn(async move {
            h.block_for(Duration::from_millis(200)).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let held = saturate_other_lanes(&core.handle);

        for i in 0..TICK_LANE_DEPTH {
            assert!(
                core.handle.try_send_slot_tick(),
                "tick {i} shed while every other lane was full"
            );
        }
        // Lane at capacity refuses rather than silently dropping.
        assert!(
            !core.handle.try_send_slot_tick(),
            "full never-shed lane must refuse, not grow"
        );

        drop(held);
        let _ = blocker.await;

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut time = 0;
        while std::time::Instant::now() < deadline {
            time = store_clock(&core.handle).await.0;
            if time > 1_000_000 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            time > 1_000_000,
            "never-shed ticks must advance store.time; got {time}"
        );

        core.handle.shutdown().await;
        core.join();
        events.shutdown().await;
    }

    #[tokio::test]
    async fn ping_round_trips_the_tick_lane() {
        let (store, _anchor, config) = seeded_store();
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let events = EventsHandle::spawn(EventsConfig::default());
        let head = HeadSnapshotStore::new();
        let core = spawn_core_thread(
            store,
            config,
            head,
            events.event_sender(),
            metrics,
            CoreConfig::default(),
        );

        let rtt = crate::liveness::probe_core_liveness(&core.handle, Duration::from_secs(1))
            .await
            .expect("live core must answer a tick-lane ping");
        assert!(rtt < Duration::from_secs(1));

        core.handle.shutdown().await;
        core.join();
        events.shutdown().await;
    }

    fn saturate_other_lanes(
        handle: &CoreHandle,
    ) -> Vec<oneshot::Receiver<Result<QueryReply, Status>>> {
        let mut held = Vec::new();
        let import = handle.import_sender();
        for _ in 0..IMPORT_LANE_DEPTH {
            let (reply, rx) = oneshot::channel();
            let _held_import = rx;
            import
                .try_send(ImportWork::ImportBlock {
                    request: ImportBlockRequest::default(),
                    reply,
                })
                .expect("import lane has room");
        }
        let p0 = handle.query_p0_sender();
        for _ in 0..QUERY_P0_LANE_DEPTH {
            let (reply, rx) = oneshot::channel();
            held.push(rx);
            p0.try_send(QueryP0Work::Query {
                request: QueryRequest::Head,
                reply,
            })
            .expect("query_p0 has room");
        }
        let p1 = handle.query_p1_sender();
        for _ in 0..QUERY_P1_LANE_DEPTH {
            let (reply, rx) = oneshot::channel();
            held.push(rx);
            p1.try_send(QueryP1Work::Query {
                request: QueryRequest::CommitteeShuffling { epoch: 0 },
                reply,
            })
            .expect("query_p1 has room");
        }
        let att = handle.attestation_sender();
        for _ in 0..att.max_capacity() {
            let (reply, _rx) = oneshot::channel();
            assert!(matches!(
                att.try_push(AttestationWork::ApplyAttestations {
                    request: empty_attestations(),
                    reply,
                }),
                AttestationEnqueue::Accepted
            ));
        }
        held
    }

    /// Saturating every other lane still advances `store.time` within one slot.
    #[tokio::test]
    async fn slot_tick_advances_store_time_when_other_work_saturated() {
        let (store, _anchor, config) = seeded_store();
        let slot_secs = config.seconds_per_slot.max(1);
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let events = EventsHandle::spawn(EventsConfig::default());
        let head = HeadSnapshotStore::new();
        let core = spawn_core_thread(
            store,
            config,
            head,
            events.event_sender(),
            metrics,
            CoreConfig::default(),
        );

        let before = store_clock(&core.handle).await.0;

        let h = core.handle.clone();
        let blocker = tokio::spawn(async move {
            h.block_for(Duration::from_millis(200)).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(30)).await;

        let held = saturate_other_lanes(&core.handle);
        assert!(
            core.handle.try_send_slot_tick(),
            "tick must be accepted while every other lane is saturated"
        );
        let sent_at = std::time::Instant::now();

        drop(held);
        let _ = blocker.await;

        let deadline = sent_at + Duration::from_secs(slot_secs);
        let mut time = before;
        while std::time::Instant::now() < deadline {
            time = store_clock(&core.handle).await.0;
            if time > before {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            time > before,
            "store.time {time} did not advance past {before} within one slot"
        );
        assert!(
            sent_at.elapsed() < Duration::from_secs(slot_secs),
            "store.time advanced but not within one slot ({:?})",
            sent_at.elapsed()
        );

        core.handle.shutdown().await;
        core.join();
        events.shutdown().await;
    }

    /// A block arriving early in its slot is not IGNOREd as `future_slot`.
    #[tokio::test]
    async fn block_gossiped_early_in_slot_is_not_ignored_as_future_slot() {
        let now = unix_now_secs();
        let mut config = minimal_config();
        let sps = config.seconds_per_slot.max(1);
        // One second into slot 1; store seeds at genesis (slot 0).
        let genesis = now.saturating_sub(sps.saturating_add(1));
        config.seconds_per_slot = sps;

        let mut state = BeaconState::<Minimal>::default();
        state.set_genesis_time(genesis);
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
            sps,
        )
        .unwrap();
        assert_eq!(store.get_current_slot().as_u64(), 0);
        let anchor = Root::from_hash256(TreeHash::tree_hash_root(&anchor_block));

        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let events = EventsHandle::spawn(EventsConfig::default());
        let head = HeadSnapshotStore::new();
        let core = spawn_core_thread(
            store,
            config,
            head,
            events.event_sender(),
            metrics,
            CoreConfig {
                slot_tick_enabled: true,
                ..CoreConfig::default()
            },
        );

        let block = cc_types::SignedBeaconBlock::<Minimal> {
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
        let req = ImportBlockRequest {
            ssz: crate::import::encode_signed_block(&block),
            fork: 0,
            root: true_root.as_slice().to_vec(),
            source: 0,
        };
        let resp = core.handle.import_block(req).await.unwrap();
        assert_ne!(
            resp.reason, "future_slot",
            "block early in its slot must not be IGNOREd; verdict={} reason={}",
            resp.verdict, resp.reason
        );

        core.handle.shutdown().await;
        core.join();
        events.shutdown().await;
    }

    #[test]
    fn query_p0_variants_are_head_probes() {
        assert!(QueryRequest::Head.is_query_p0());
        assert!(QueryRequest::IsOptimistic { root: None }.is_query_p0());
        assert!(QueryRequest::StoreClock.is_query_p0());
        assert!(!QueryRequest::CommitteeShuffling { epoch: 0 }.is_query_p0());
        assert!(!QueryRequest::ValidatorPubkeys { indices: vec![] }.is_query_p0());
        assert!(!QueryRequest::ValidatorRecords { indices: vec![] }.is_query_p0());
        assert!(
            !QueryRequest::CanonicalRoots {
                start_slot: 0,
                end_slot: 0
            }
            .is_query_p0()
        );
    }

    #[test]
    fn query_p1_variants_are_serving_reads() {
        assert!(QueryRequest::CommitteeShuffling { epoch: 0 }.is_query_p1());
        assert!(QueryRequest::ValidatorPubkeys { indices: vec![] }.is_query_p1());
        assert!(QueryRequest::ValidatorRecords { indices: vec![] }.is_query_p1());
        assert!(
            QueryRequest::CanonicalRoots {
                start_slot: 0,
                end_slot: 0
            }
            .is_query_p1()
        );
        assert!(!QueryRequest::Head.is_query_p1());
        assert!(!QueryRequest::IsOptimistic { root: None }.is_query_p1());
        assert!(!QueryRequest::StoreClock.is_query_p1());
    }

    fn empty_attestations() -> ApplyAttestationsRequest {
        ApplyAttestationsRequest {
            attestations_ssz: vec![],
        }
    }

    #[tokio::test]
    async fn query_p0_head_is_served_before_query_p1() {
        let (store, _anchor, config) = seeded_store();
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let events = EventsHandle::spawn(EventsConfig::default());
        let head = HeadSnapshotStore::new();
        let core = spawn_core_thread(
            store,
            config,
            head,
            events.event_sender(),
            metrics,
            CoreConfig::default(),
        );

        let h = core.handle.clone();
        let blocker = tokio::spawn(async move {
            h.block_for(Duration::from_millis(200)).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let p1 = core.handle.query_p1_sender();
        let mut p1_rxs = Vec::new();
        for _ in 0..3 {
            let (reply, rx) = oneshot::channel();
            p1_rxs.push(rx);
            p1.try_send(QueryP1Work::Query {
                request: QueryRequest::CommitteeShuffling { epoch: 0 },
                reply,
            })
            .expect("query_p1 must accept");
        }

        let (head_reply, head_rx) = oneshot::channel();
        core.handle
            .query_p0_sender()
            .try_send(QueryP0Work::Query {
                request: QueryRequest::Head,
                reply: head_reply,
            })
            .expect("query_p0 must accept Head while query_p1 is queued");

        let (order_tx, mut order_rx) = mpsc::channel(4);
        let order_head = order_tx.clone();
        tokio::spawn(async move {
            let _ = head_rx.await;
            let _ = order_head.send("head").await;
        });
        let p1_first = p1_rxs.remove(0);
        tokio::spawn(async move {
            let _ = p1_first.await;
            let _ = order_tx.send("p1").await;
        });

        let _ = blocker.await;
        let first = tokio::time::timeout(Duration::from_secs(2), order_rx.recv())
            .await
            .expect("lane reply timed out")
            .expect("order channel closed");
        assert_eq!(first, "head", "query_p0 must not wait behind query_p1");

        drop(p1_rxs);
        core.handle.shutdown().await;
        core.join();
        events.shutdown().await;
    }

    #[tokio::test]
    async fn get_head_behind_three_imports_waits_for_import_lane() {
        let (store, _anchor, config) = seeded_store();
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let events = EventsHandle::spawn(EventsConfig::default());
        let head = HeadSnapshotStore::new();
        let core = spawn_core_thread(
            store,
            config,
            head,
            events.event_sender(),
            metrics,
            CoreConfig::default(),
        );

        let h = core.handle.clone();
        let blocker = tokio::spawn(async move {
            h.block_for(Duration::from_millis(200)).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let import = core.handle.import_sender();
        let mut import_rxs = Vec::new();
        for _ in 0..3 {
            let (reply, rx) = oneshot::channel();
            import_rxs.push(rx);
            import
                .try_send(ImportWork::ImportBlock {
                    request: ImportBlockRequest {
                        ssz: vec![],
                        fork: 0,
                        root: vec![0; 32],
                        source: 0,
                    },
                    reply,
                })
                .expect("import lane must accept");
        }

        let (head_reply, head_rx) = oneshot::channel();
        core.handle
            .query_p0_sender()
            .try_send(QueryP0Work::Query {
                request: QueryRequest::Head,
                reply: head_reply,
            })
            .expect("query_p0 must accept Head behind imports");

        let (order_tx, mut order_rx) = mpsc::channel(4);
        let order_import = order_tx.clone();
        let first_import = import_rxs.remove(0);
        tokio::spawn(async move {
            let _ = first_import.await;
            let _ = order_import.send("import").await;
        });
        tokio::spawn(async move {
            let _ = head_rx.await;
            let _ = order_tx.send("head").await;
        });

        let _ = blocker.await;
        let first = tokio::time::timeout(Duration::from_secs(2), order_rx.recv())
            .await
            .expect("lane reply timed out")
            .expect("order channel closed");
        assert_eq!(
            first, "import",
            "import outranks query_p0; GetHead still waits for queued imports"
        );

        drop(import_rxs);
        core.handle.shutdown().await;
        core.join();
        events.shutdown().await;
    }

    #[tokio::test]
    async fn data_available_shares_import_lane_capacity() {
        let (store, _anchor, config) = seeded_store();
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let events = EventsHandle::spawn(EventsConfig::default());
        let head = HeadSnapshotStore::new();
        let core = spawn_core_thread(
            store,
            config,
            head,
            events.event_sender(),
            metrics,
            CoreConfig::default(),
        );

        let h = core.handle.clone();
        let blocker = tokio::spawn(async move {
            h.block_for(Duration::from_millis(300)).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let import = core.handle.import_sender();
        let mut held = Vec::new();
        for _ in 0..(IMPORT_LANE_DEPTH - 1) {
            let (reply, rx) = oneshot::channel();
            held.push(rx);
            import
                .try_send(ImportWork::ImportBlock {
                    request: ImportBlockRequest::default(),
                    reply,
                })
                .expect("import lane has room");
        }
        import
            .try_send(ImportWork::DataAvailable {
                root: Root::ZERO,
                slot: 0,
            })
            .expect("DataAvailable takes the last import slot");
        assert!(
            matches!(
                import.try_send(ImportWork::DataAvailable {
                    root: Root::ZERO,
                    slot: 1,
                }),
                Err(mpsc::error::TrySendError::Full(_))
            ),
            "DataAvailable must share the import FIFO, not sit above it"
        );
        assert_eq!(import.max_capacity(), IMPORT_LANE_DEPTH);

        let (reply, _rx) = oneshot::channel();
        core.handle
            .query_p0_sender()
            .try_send(QueryP0Work::Query {
                request: QueryRequest::Head,
                reply,
            })
            .expect("query_p0 stays independent of a full import lane");
        assert_eq!(
            core.handle.query_p0_sender().max_capacity(),
            QUERY_P0_LANE_DEPTH
        );
        let (reply, _rx) = oneshot::channel();
        core.handle
            .query_p1_sender()
            .try_send(QueryP1Work::Query {
                request: QueryRequest::CommitteeShuffling { epoch: 0 },
                reply,
            })
            .expect("query_p1 stays independent of a full import lane");
        assert_eq!(
            core.handle.query_p1_sender().max_capacity(),
            QUERY_P1_LANE_DEPTH
        );

        drop(held);
        let _ = blocker.await;
        core.handle.shutdown().await;
        core.join();
        events.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_is_served_while_import_lane_is_full() {
        let (store, _anchor, config) = seeded_store();
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let events = EventsHandle::spawn(EventsConfig::default());
        let head = HeadSnapshotStore::new();
        let core = spawn_core_thread(
            store,
            config,
            head,
            events.event_sender(),
            metrics,
            CoreConfig::default(),
        );

        let h = core.handle.clone();
        let blocker = tokio::spawn(async move {
            h.block_for(Duration::from_millis(200)).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let import = core.handle.import_sender();
        let mut held = Vec::new();
        for _ in 0..IMPORT_LANE_DEPTH {
            let (reply, rx) = oneshot::channel();
            held.push(rx);
            import
                .try_send(ImportWork::ImportBlock {
                    request: ImportBlockRequest::default(),
                    reply,
                })
                .expect("import lane has room");
        }

        let sent_at = std::time::Instant::now();
        let rx = core
            .handle
            .begin_shutdown()
            .await
            .expect("shutdown must enqueue on the tick lane");
        assert!(
            sent_at.elapsed() < Duration::from_millis(200),
            "begin_shutdown send must not wait behind a full import lane ({:?})",
            sent_at.elapsed()
        );

        drop(held);
        let _ = blocker.await;
        tokio::time::timeout(Duration::from_secs(2), rx)
            .await
            .expect("shutdown oneshot")
            .expect("core dropped shutdown reply");
        core.join();
        events.shutdown().await;
    }

    #[tokio::test]
    async fn last_handle_drop_unparks_idle_core() {
        let (store, _anchor, config) = seeded_store();
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let events = EventsHandle::spawn(EventsConfig::default());
        let head = HeadSnapshotStore::new();
        let mut core = spawn_core_thread(
            store,
            config,
            head,
            events.event_sender(),
            metrics,
            CoreConfig::default(),
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        let join = core.join.take();
        drop(core.handle);
        tokio::time::timeout(
            Duration::from_secs(2),
            tokio::task::spawn_blocking(move || join.expect("join handle").join()),
        )
        .await
        .expect("core stayed parked after last sender drop")
        .expect("join task")
        .expect("core thread panicked");
        events.shutdown().await;
    }

    /// Core Shutdown drops the LIFO receiver; queued `ApplyAttestations`
    /// oneshots must fail immediately even while another handle stays alive
    /// (the gRPC service keeps one through drain).
    #[tokio::test]
    async fn shutdown_fails_pending_attestation_waiters() {
        let (store, _anchor, config) = seeded_store();
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let events = EventsHandle::spawn(EventsConfig::default());
        let head = HeadSnapshotStore::new();
        let core = spawn_core_thread(
            store,
            config,
            head,
            events.event_sender(),
            metrics,
            CoreConfig::default(),
        );
        let keeper = core.handle.clone();

        let h = core.handle.clone();
        let blocker = tokio::spawn(async move {
            h.block_for(Duration::from_millis(200)).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let h = core.handle.clone();
        let waiter = tokio::spawn(async move { h.apply_attestations(empty_attestations()).await });
        tokio::time::sleep(Duration::from_millis(20)).await;

        let sent_at = std::time::Instant::now();
        core.handle.shutdown().await;
        let result = tokio::time::timeout(Duration::from_millis(500), waiter)
            .await
            .expect("attestation waiter hung after core Shutdown (receiver must close)")
            .expect("waiter task");
        let err = result.expect_err("pending ApplyAttestations must fail when the core exits");
        assert_eq!(err.code(), tonic::Code::Unavailable);
        assert!(
            sent_at.elapsed() < Duration::from_secs(1),
            "waiter must not sit until drain timeout ({:?})",
            sent_at.elapsed()
        );

        match keeper
            .attestation_sender()
            .try_push(AttestationWork::ApplyAttestations {
                request: empty_attestations(),
                reply: oneshot::channel().0,
            }) {
            AttestationEnqueue::Closed(_) => {}
            other => panic!("post-shutdown try_push must be Closed, got {other:?}"),
        }

        let _ = blocker.await;
        drop(keeper);
        core.join();
        events.shutdown().await;
    }

    #[tokio::test]
    async fn query_p0_head_is_served_before_attestation() {
        let (store, _anchor, config) = seeded_store();
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let events = EventsHandle::spawn(EventsConfig::default());
        let head = HeadSnapshotStore::new();
        let core = spawn_core_thread(
            store,
            config,
            head,
            events.event_sender(),
            metrics,
            CoreConfig::default(),
        );

        let h = core.handle.clone();
        let blocker = tokio::spawn(async move {
            h.block_for(Duration::from_millis(200)).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let att = core.handle.attestation_sender();
        let mut att_rxs = Vec::new();
        for _ in 0..3 {
            let (reply, rx) = oneshot::channel();
            att_rxs.push(rx);
            assert!(matches!(
                att.try_push(AttestationWork::ApplyAttestations {
                    request: empty_attestations(),
                    reply,
                }),
                AttestationEnqueue::Accepted
            ));
        }

        let (head_reply, head_rx) = oneshot::channel();
        core.handle
            .query_p0_sender()
            .try_send(QueryP0Work::Query {
                request: QueryRequest::Head,
                reply: head_reply,
            })
            .expect("query_p0 must accept Head while attestation is queued");

        let (order_tx, mut order_rx) = mpsc::channel(4);
        let order_head = order_tx.clone();
        tokio::spawn(async move {
            let _ = head_rx.await;
            let _ = order_head.send("head").await;
        });
        let att_first = att_rxs.remove(0);
        tokio::spawn(async move {
            let _ = att_first.await;
            let _ = order_tx.send("att").await;
        });

        let _ = blocker.await;
        let first = tokio::time::timeout(Duration::from_secs(2), order_rx.recv())
            .await
            .expect("lane reply timed out")
            .expect("order channel closed");
        assert_eq!(
            first, "head",
            "query_p0 must not wait behind an attestation flood"
        );

        drop(att_rxs);
        core.handle.shutdown().await;
        core.join();
        events.shutdown().await;
    }

    #[tokio::test]
    async fn attestation_is_served_before_query_p1() {
        let (store, _anchor, config) = seeded_store();
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let events = EventsHandle::spawn(EventsConfig::default());
        let head = HeadSnapshotStore::new();
        let core = spawn_core_thread(
            store,
            config,
            head,
            events.event_sender(),
            metrics,
            CoreConfig::default(),
        );

        let h = core.handle.clone();
        let blocker = tokio::spawn(async move {
            h.block_for(Duration::from_millis(200)).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let p1 = core.handle.query_p1_sender();
        let mut p1_rxs = Vec::new();
        for _ in 0..3 {
            let (reply, rx) = oneshot::channel();
            p1_rxs.push(rx);
            p1.try_send(QueryP1Work::Query {
                request: QueryRequest::CommitteeShuffling { epoch: 0 },
                reply,
            })
            .expect("query_p1 must accept");
        }

        let (att_reply, att_rx) = oneshot::channel();
        assert!(matches!(
            core.handle
                .attestation_sender()
                .try_push(AttestationWork::ApplyAttestations {
                    request: empty_attestations(),
                    reply: att_reply,
                }),
            AttestationEnqueue::Accepted
        ));

        let (order_tx, mut order_rx) = mpsc::channel(4);
        let order_att = order_tx.clone();
        tokio::spawn(async move {
            let _ = att_rx.await;
            let _ = order_att.send("att").await;
        });
        let p1_first = p1_rxs.remove(0);
        tokio::spawn(async move {
            let _ = p1_first.await;
            let _ = order_tx.send("p1").await;
        });

        let _ = blocker.await;
        let first = tokio::time::timeout(Duration::from_secs(2), order_rx.recv())
            .await
            .expect("lane reply timed out")
            .expect("order channel closed");
        assert_eq!(first, "att", "attestation must outrank query_p1");

        drop(p1_rxs);
        core.handle.shutdown().await;
        core.join();
        events.shutdown().await;
    }

    #[tokio::test]
    async fn attestation_lifo_evicts_oldest_and_serves_newest() {
        let (store, _anchor, config) = seeded_store();
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let events = EventsHandle::spawn(EventsConfig::default());
        let head = HeadSnapshotStore::new();
        let core = spawn_core_thread(
            store,
            config,
            head,
            events.event_sender(),
            metrics,
            CoreConfig::default(),
        );

        let depth = core.handle.attestation_sender().max_capacity();
        assert!(
            depth >= MIN_QUEUE_LEN,
            "attestation depth {depth} must be sized_from_validators (floor {MIN_QUEUE_LEN})"
        );
        assert_eq!(
            depth,
            sized_from_validators(
                core.handle.epoch_context().load().active_validator_count,
                core.handle.epoch_context().load().slots_per_epoch,
            )
        );

        let h = core.handle.clone();
        let blocker = tokio::spawn(async move {
            h.block_for(Duration::from_millis(300)).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let att = core.handle.attestation_sender();
        let mut held = Vec::new();
        for _ in 0..depth {
            let (reply, rx) = oneshot::channel();
            held.push(rx);
            assert!(matches!(
                att.try_push(AttestationWork::ApplyAttestations {
                    request: empty_attestations(),
                    reply,
                }),
                AttestationEnqueue::Accepted
            ));
        }

        let (fresh_reply, fresh_rx) = oneshot::channel();
        match att.try_push(AttestationWork::ApplyAttestations {
            request: empty_attestations(),
            reply: fresh_reply,
        }) {
            AttestationEnqueue::EvictedOldest(AttestationWork::ApplyAttestations {
                reply, ..
            }) => {
                let _ = reply.send(Err(Status::resource_exhausted("evicted oldest")));
            }
            other => panic!("expected EvictedOldest, got {other:?}"),
        }
        assert_eq!(att.len(), depth);

        let oldest = held.remove(0);
        let oldest_err = oldest
            .await
            .expect("evicted oneshot dropped")
            .expect_err("oldest must be rejected");
        assert_eq!(oldest_err.code(), tonic::Code::ResourceExhausted);

        let (order_tx, mut order_rx) = mpsc::channel(4);
        let order_fresh = order_tx.clone();
        tokio::spawn(async move {
            let _ = fresh_rx.await;
            let _ = order_fresh.send("fresh").await;
        });
        let previous_newest = held.pop().expect("filled lane has a previous newest");
        tokio::spawn(async move {
            let _ = previous_newest.await;
            let _ = order_tx.send("old").await;
        });

        drop(held);
        let _ = blocker.await;
        let first = tokio::time::timeout(Duration::from_secs(2), order_rx.recv())
            .await
            .expect("lane reply timed out")
            .expect("order channel closed");
        assert_eq!(
            first, "fresh",
            "LIFO must serve the newest attestation first"
        );

        core.handle.shutdown().await;
        core.join();
        events.shutdown().await;
    }

    #[tokio::test]
    async fn query_p1_fifo_drops_new_and_is_independent_of_import() {
        let (store, _anchor, config) = seeded_store();
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let events = EventsHandle::spawn(EventsConfig::default());
        let head = HeadSnapshotStore::new();
        let core = spawn_core_thread(
            store,
            config,
            head,
            events.event_sender(),
            metrics,
            CoreConfig::default(),
        );

        let h = core.handle.clone();
        let blocker = tokio::spawn(async move {
            h.block_for(Duration::from_millis(300)).await.unwrap();
        });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let p1 = core.handle.query_p1_sender();
        let mut held = Vec::new();
        for _ in 0..QUERY_P1_LANE_DEPTH {
            let (reply, rx) = oneshot::channel();
            held.push(rx);
            p1.try_send(QueryP1Work::Query {
                request: QueryRequest::ValidatorRecords { indices: vec![] },
                reply,
            })
            .expect("query_p1 has room");
        }
        let (reply, _rx) = oneshot::channel();
        assert!(
            matches!(
                p1.try_send(QueryP1Work::Query {
                    request: QueryRequest::ValidatorPubkeys { indices: vec![] },
                    reply,
                }),
                Err(mpsc::error::TrySendError::Full(_))
            ),
            "query_p1 FIFO must drop new when full"
        );
        assert_eq!(p1.max_capacity(), QUERY_P1_LANE_DEPTH);

        let (reply, _rx) = oneshot::channel();
        core.handle
            .import_sender()
            .try_send(ImportWork::ImportBlock {
                request: ImportBlockRequest::default(),
                reply,
            })
            .expect("import stays independent of a full query_p1 lane");

        drop(held);
        let _ = blocker.await;
        core.handle.shutdown().await;
        core.join();
        events.shutdown().await;
    }
}
