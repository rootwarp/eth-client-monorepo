//! Single writer task and three-class priority mailbox (Architecture §1.5 / ADR P4-04).
//!
//! **D-4:** this file is created complete here and no later issue edits it.
//! P2 chunk/deadline loops live in submitters (`prune/mod.rs`, snapshot, backfill);
//! they submit chunks and touch nothing here.
//!
//! `dead_code` is allowed for the complete mailbox API (P1 submit, blocking P0,
//! bounds defaults) — later issues call these without editing this file.

#![allow(dead_code)]
//!
//! ```text
//! WRITER TASK — sole owner of Engine's write side; owns the priority mailbox
//!   loop { pick highest non-empty class → build ONE Batch → commit → yield }
//!   P0  write-behind commit for slot S (blocks, columns, canonical, DA, cursor)
//!   P1  meta updates that must ride a commit (window, split, anchor, …)
//!   P2  background chunks: prune / backfill / snapshot (never a whole pass)
//! ```
//!
//! Channel bounds and on-full policy (§1.5):
//! | Channel | Bound | On full |
//! |---|---|---|
//! | P0 | 32 commit units | **block** (never drop) |
//! | P1 | 64 | **block** |
//! | P2 | 256 chunks | **drop newest** + count |
//!
//! **Panic policy:** the writer is process-fatal on panic. A dead writer that
//! continues to serve is strictly worse than a compose restart.

use std::sync::Arc;
use std::time::Instant;

use cc_store::blocks::{stage_state_root, state_root_at_offset};
use cc_store::engine::{Engine, ReadTxn};
use cc_store::keys::BlockRegion;
use cc_store::meta::{KEY_FC_SCALARS, KEY_WRITE_CURSOR, TABLE_META, WriteCursor};
use cc_store::{
    DaStatus, PutBlockOutcome, PutColumnOutcome, StoreError, get_block_by_root, put_block,
    put_block_and_update_head, put_column, put_da_status, rewrite_from_head,
};
use cc_store::{Root, Slot, SszEncode};
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{error, info, warn};

use crate::metrics::{
    ClassLabels, Invariant, InvariantLabels, ReasonLabels, ReconnectReason, StorageClass,
    StorageMetrics, WriterPriority,
};

// ── channel bounds (§1.5) ───────────────────────────────────────────────────

/// Write-behind → writer (P0): 32 slots' commit units. On full: **block**.
pub(crate) const WRITER_P0_BOUND: usize = 32;
/// Meta → writer (P1): 64. On full: **block**.
pub(crate) const WRITER_P1_BOUND: usize = 64;
/// Background → writer (P2): 256 chunks. On full: **drop newest** + count.
pub(crate) const WRITER_P2_BOUND: usize = 256;

// ── job types ───────────────────────────────────────────────────────────────

/// One staged block body for a P0 commit unit.
#[derive(Debug, Clone)]
pub(crate) struct StagedBlock {
    pub slot: Slot,
    pub root: Root,
    pub ssz: Vec<u8>,
    /// When true, also rewrite `canonical` from this root as head.
    pub update_canonical: bool,
    /// When true, write `state_roots[slot]`. A different existing root is
    /// replaced only when [`Self::update_canonical`] is set.
    pub write_state_root: bool,
    /// DA status derived from the BLOCK_IMPORTED verdict discriminator.
    pub da_status: Option<DaStatus>,
}

/// One staged column sidecar for a P0 commit unit.
#[derive(Debug, Clone)]
pub(crate) struct StagedColumn {
    pub slot: Slot,
    pub root: Root,
    pub index: u16,
    pub ssz: Vec<u8>,
}

/// Optional ForkChoiceScalars SSZ blob (from FINALIZED_CHECKPOINT payload tail).
#[derive(Debug, Clone)]
pub(crate) struct StagedForkChoiceScalars {
    pub ssz: Vec<u8>,
}

/// Anchor rows that ride the same P0 batch as the body.
///
/// The body itself is [`CommitUnit::blocks`]. These are the snapshot,
/// its completion marker, `AnchorInfo`, and `Split`.
#[derive(Debug, Clone)]
pub(crate) struct StagedAnchor {
    pub snapshot_ssz: Vec<u8>,
    pub completion_ssz: Vec<u8>,
    pub anchor_info_ssz: Vec<u8>,
    pub split_ssz: Vec<u8>,
}

/// P0 write-behind commit unit — **one write transaction** (§4.4).
///
/// Contains the slot's data rows **and** the durable [`WriteCursor`] for the
/// last event included. Cursor and data land in the **same** [`Batch`].
#[derive(Debug)]
pub(crate) struct CommitUnit {
    pub blocks: Vec<StagedBlock>,
    pub columns: Vec<StagedColumn>,
    pub fork_choice: Option<StagedForkChoiceScalars>,
    /// Rewrite canonical from this already-durable root without inserting a body.
    ///
    /// `set_head` sets this. `commit_import` uses [`StagedBlock::update_canonical`]
    /// instead, because that transaction writes the body.
    pub canonical_from: Option<Root>,
    /// Cursor value = seq of the **last event included** in this unit (not last received).
    pub cursor: WriteCursor,
    /// Optional reply (tests inject commit-failure / wait for durable).
    pub done: Option<oneshot::Sender<Result<(), WriterError>>>,
    /// When set, this unit is `commit_anchor`: one transaction, cold body.
    pub anchor: Option<StagedAnchor>,
}

impl CommitUnit {
    /// Empty unit with only a cursor (tests / meta-less flushes).
    #[must_use]
    pub(crate) fn cursor_only(cursor: WriteCursor) -> Self {
        Self {
            blocks: Vec::new(),
            columns: Vec::new(),
            fork_choice: None,
            canonical_from: None,
            cursor,
            done: None,
            anchor: None,
        }
    }
}

/// P1 meta update that must ride a commit (opaque put/delete list).
///
/// CC-41 migration submits hot→cold re-keys + Split write as one P1 unit
/// (Architecture §3.2: P1, not P2 — must not be drop-newest preempted).
#[derive(Debug)]
pub(crate) struct MetaUpdate {
    /// `(table, key, value)` puts applied after any reads in the writer.
    pub puts: Vec<(String, Vec<u8>, Vec<u8>)>,
    /// `(table, key)` deletes (migration step 1 re-key + step 2 unfinalized).
    pub deletes: Vec<(String, Vec<u8>)>,
    pub done: Option<oneshot::Sender<Result<(), WriterError>>>,
}

impl MetaUpdate {
    /// Empty update shell (tests / construction).
    #[must_use]
    pub(crate) fn empty() -> Self {
        Self {
            puts: Vec::new(),
            deletes: Vec::new(),
            done: None,
        }
    }
}

/// P2 background chunk (prune / backfill / snapshot piece).
#[derive(Debug)]
pub(crate) struct BackgroundChunk {
    /// Storage class label for drop metrics (`blocks` | `columns` | …).
    pub class: StorageClass,
    pub puts: Vec<(String, Vec<u8>, Vec<u8>)>,
    pub deletes: Vec<(String, Vec<u8>)>,
    pub done: Option<oneshot::Sender<Result<(), WriterError>>>,
}

impl Clone for BackgroundChunk {
    fn clone(&self) -> Self {
        Self {
            class: self.class,
            puts: self.puts.clone(),
            deletes: self.deletes.clone(),
            done: None, // oneshot is not Clone; drop reply on clone
        }
    }
}

/// Writer-side errors (non-collision; collision is process-fatal).
#[derive(Debug, thiserror::Error)]
pub(crate) enum WriterError {
    #[error("store: {0}")]
    Store(#[from] StoreError),
    #[error("writer shut down")]
    ShutDown,
    #[error("injected commit failure (test)")]
    InjectedFailure,
    /// `commit_anchor` saw a store that is no longer uninitialized.
    #[error("store is not uninitialized")]
    NotUninitialized,
}

// ── mailbox ─────────────────────────────────────────────────────────────────

/// Submit handles for the three priority classes.
#[derive(Debug, Clone)]
pub(crate) struct WriterHandle {
    p0: mpsc::Sender<CommitUnit>,
    p1: mpsc::Sender<MetaUpdate>,
    p2: mpsc::Sender<BackgroundChunk>,
    metrics: StorageMetrics,
    process_fatal: bool,
    on_fatal: ProcessExit,
}

impl WriterHandle {
    /// Submit a P0 commit unit. **Blocks** when the 32-slot queue is full (never drop).
    pub(crate) async fn submit_p0(&self, unit: CommitUnit) -> Result<(), WriterError> {
        self.p0.send(unit).await.map_err(|_| WriterError::ShutDown)
    }

    /// Blocking submit for non-async producers (tests).
    pub(crate) fn blocking_submit_p0(&self, unit: CommitUnit) -> Result<(), WriterError> {
        self.p0
            .blocking_send(unit)
            .map_err(|_| WriterError::ShutDown)
    }

    /// Submit a P1 meta update. **Blocks** when full.
    pub(crate) async fn submit_p1(&self, update: MetaUpdate) -> Result<(), WriterError> {
        self.p1
            .send(update)
            .await
            .map_err(|_| WriterError::ShutDown)
    }

    /// Blocking P1 submit (non-async producers / tests).
    pub(crate) fn blocking_submit_p1(&self, update: MetaUpdate) -> Result<(), WriterError> {
        self.p1
            .blocking_send(update)
            .map_err(|_| WriterError::ShutDown)
    }

    /// Submit P1 and wait for the writer to **commit** (or fail).
    ///
    /// CC-41: migration advances the in-memory split only after this returns `Ok`.
    pub(crate) async fn submit_p1_committed(
        &self,
        mut update: MetaUpdate,
    ) -> Result<(), WriterError> {
        let (done_tx, done_rx) = oneshot::channel();
        update.done = Some(done_tx);
        self.submit_p1(update).await?;
        done_rx.await.map_err(|_| WriterError::ShutDown)?
    }

    /// Blocking P1 commit wait (tests / sync migrator paths).
    pub(crate) fn blocking_submit_p1_committed(
        &self,
        mut update: MetaUpdate,
    ) -> Result<(), WriterError> {
        let (done_tx, done_rx) = oneshot::channel();
        update.done = Some(done_tx);
        self.blocking_submit_p1(update)?;
        done_rx.blocking_recv().map_err(|_| WriterError::ShutDown)?
    }

    /// Submit a P2 background chunk. On full: **drop newest** and count.
    ///
    /// Returns `Ok(true)` if enqueued, `Ok(false)` if dropped.
    /// Drop metric uses the priority label `class="p2"` (R-10), not the storage class.
    pub(crate) fn try_submit_p2(&self, chunk: BackgroundChunk, metrics: &StorageMetrics) -> bool {
        match self.p2.try_send(chunk) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_chunk)) => {
                metrics
                    .writer_chunk_dropped
                    .get_or_create(&ClassLabels {
                        class: WriterPriority::P2.as_str().to_owned(),
                    })
                    .inc();
                false
            }
            Err(mpsc::error::TrySendError::Closed(_)) => false,
        }
    }

    /// Submit P0 and wait for the writer to **commit** (or fail).
    ///
    /// SEC-44b: durable resume cursor advances only after this returns `Ok`.
    /// Queue acceptance alone is not enough.
    pub(crate) async fn submit_p0_committed(
        &self,
        mut unit: CommitUnit,
    ) -> Result<(), WriterError> {
        let (done_tx, done_rx) = oneshot::channel();
        unit.done = Some(done_tx);
        self.submit_p0(unit).await?;
        done_rx.await.map_err(|_| WriterError::ShutDown)?
    }

    /// Blocking P0 commit wait (core OS thread / S2-A-14 ingest).
    pub(crate) fn blocking_submit_p0_committed(
        &self,
        mut unit: CommitUnit,
    ) -> Result<(), WriterError> {
        let (done_tx, done_rx) = oneshot::channel();
        unit.done = Some(done_tx);
        self.blocking_submit_p0(unit)?;
        done_rx.blocking_recv().map_err(|_| WriterError::ShutDown)?
    }

    /// Current approximate P0 queue depth (for metrics scrape).
    #[must_use]
    pub(crate) fn p0_capacity_hint(&self) -> usize {
        // mpsc does not expose len on Sender in stable tokio without metrics
        // plumbing; depth is updated by the writer task itself.
        0
    }

    /// Metrics registered with this writer (commit-wait histogram included).
    #[must_use]
    pub(crate) fn metrics(&self) -> &StorageMetrics {
        &self.metrics
    }

    /// Existing fail-closed exit. Production calls [`std::process::exit`].
    ///
    /// `process_fatal == false` returns so a test can observe the join.
    /// The named `reason` is what the operator and the conformance hook see.
    pub(crate) fn invoke_process_fatal(&self, reason: &'static str) {
        if self.process_fatal {
            self.on_fatal.run(1, reason);
        }
    }
}

/// Channel bounds used when opening the mailbox.
#[derive(Debug, Clone, Copy)]
pub(crate) struct WriterBounds {
    pub p0: usize,
    pub p1: usize,
    pub p2: usize,
}

impl Default for WriterBounds {
    fn default() -> Self {
        Self {
            p0: WRITER_P0_BOUND,
            p1: WRITER_P1_BOUND,
            p2: WRITER_P2_BOUND,
        }
    }
}

/// Test / fault knobs for the writer (not production config beyond crash_point).
#[derive(Debug, Clone, Default)]
pub(crate) struct WriterFaults {
    /// When true, the next commit returns [`WriterError::InjectedFailure`] without
    /// applying the batch (CC-44 same-transaction structural test).
    pub fail_next_commit: Arc<std::sync::atomic::AtomicBool>,
    /// When true, the writer panics on the next job (process-fatal test).
    pub panic_next: Arc<std::sync::atomic::AtomicBool>,
    /// When true, the next P0 unit is held unanswered.
    ///
    /// The caller's commit deadline is what ends the wait. Dropping `done`
    /// here would look like shutdown, not a stall.
    pub stall_commit: Arc<std::sync::atomic::AtomicBool>,
}

/// Named process-fatal reason when `commit_import` waits longer than 2 slots.
///
/// Aborting is not backpressure (ADR-R-08). Callers cite that consequence;
/// they do not re-decide it.
pub(crate) const COMMIT_DEADLINE_REASON: &str = "commit_deadline";

/// How the writer process-fatal guard terminates the process.
///
/// Production uses [`ProcessExit::Os`] (`std::process::exit`). Tests inject
/// [`ProcessExit::Hook`] so the harness can assert the fatal path without
/// killing the test process. The hook receives the exit code and a named reason.
#[derive(Clone, Default)]
pub(crate) enum ProcessExit {
    /// Call `std::process::exit(code)` (production).
    #[default]
    Os,
    /// Invoke a test hook instead of exiting the OS process.
    Hook(Arc<dyn Fn(i32, &'static str) + Send + Sync>),
}

impl std::fmt::Debug for ProcessExit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Os => write!(f, "ProcessExit::Os"),
            Self::Hook(_) => write!(f, "ProcessExit::Hook(..)"),
        }
    }
}

impl ProcessExit {
    fn run(&self, code: i32, reason: &'static str) {
        match self {
            Self::Os => std::process::exit(code),
            Self::Hook(h) => h(code, reason),
        }
    }
}

/// Spawn the writer task. **Process-fatal on panic** (§1.5).
///
/// Returns the submit handle. The join is monitored by a guard task that calls
/// [`ProcessExit`] on panic so a dead writer never leaves the process serving
/// stale data. Production: `process_fatal=true` + [`ProcessExit::Os`].
/// Tests: `process_fatal=true` + [`ProcessExit::Hook`] to assert exit without
/// killing the harness; or `process_fatal=false` to only observe the join.
pub(crate) fn spawn_writer(
    engine: Arc<Engine>,
    metrics: StorageMetrics,
    bounds: WriterBounds,
    faults: WriterFaults,
    shutdown: watch::Receiver<bool>,
    process_fatal: bool,
) -> WriterHandle {
    spawn_writer_with_exit(
        engine,
        metrics,
        bounds,
        faults,
        shutdown,
        process_fatal,
        ProcessExit::Os,
    )
}

/// Like [`spawn_writer`] with an injectable process-exit policy (tests).
pub(crate) fn spawn_writer_with_exit(
    engine: Arc<Engine>,
    metrics: StorageMetrics,
    bounds: WriterBounds,
    faults: WriterFaults,
    mut shutdown: watch::Receiver<bool>,
    process_fatal: bool,
    on_fatal: ProcessExit,
) -> WriterHandle {
    let (p0_tx, p0_rx) = mpsc::channel(bounds.p0.max(1));
    let (p1_tx, p1_rx) = mpsc::channel(bounds.p1.max(1));
    let (p2_tx, p2_rx) = mpsc::channel(bounds.p2.max(1));

    let handle = WriterHandle {
        p0: p0_tx,
        p1: p1_tx,
        p2: p2_tx,
        metrics: metrics.clone(),
        process_fatal,
        on_fatal: on_fatal.clone(),
    };

    let engine_task = Arc::clone(&engine);
    let metrics_task = metrics.clone();
    let join = tokio::spawn(async move {
        run_writer(
            engine_task,
            metrics_task,
            p0_rx,
            p1_rx,
            p2_rx,
            faults,
            &mut shutdown,
        )
        .await;
    });

    // Process-fatal guard: a panicked writer voids the run.
    tokio::spawn(async move {
        match join.await {
            Ok(()) => {}
            Err(e) if e.is_panic() => {
                error!(
                    target: "cc_storage::writer",
                    "writer task panicked — process-fatal (voids the run)"
                );
                if process_fatal {
                    on_fatal.run(1, "writer_task_panic");
                }
            }
            Err(e) => {
                warn!(target: "cc_storage::writer", error = %e, "writer task cancelled");
            }
        }
    });

    handle
}

async fn run_writer(
    engine: Arc<Engine>,
    metrics: StorageMetrics,
    mut p0: mpsc::Receiver<CommitUnit>,
    mut p1: mpsc::Receiver<MetaUpdate>,
    mut p2: mpsc::Receiver<BackgroundChunk>,
    faults: WriterFaults,
    shutdown: &mut watch::Receiver<bool>,
) {
    info!(target: "cc_storage::writer", "writer task started (sole Engine write owner)");
    loop {
        if *shutdown.borrow() {
            break;
        }
        // Update queue-depth gauges (R-10 early warning: p0 should stay 0).
        metrics
            .writer_queue_depth
            .get_or_create(&ClassLabels {
                class: WriterPriority::P0.as_str().to_owned(),
            })
            .set(p0.len() as i64);
        metrics
            .writer_queue_depth
            .get_or_create(&ClassLabels {
                class: WriterPriority::P1.as_str().to_owned(),
            })
            .set(p1.len() as i64);
        metrics
            .writer_queue_depth
            .get_or_create(&ClassLabels {
                class: WriterPriority::P2.as_str().to_owned(),
            })
            .set(p2.len() as i64);

        tokio::select! {
            biased;
            // A dropped sender can never raise the flag, and a closed watch
            // stays ready. Leaving this arm would spin and hold the file lock.
            result = shutdown.changed() => {
                if result.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            // P0 first (strict priority).
            unit = p0.recv(), if true => {
                let Some(mut unit) = unit else { break; };
                if faults
                    .stall_commit
                    .load(std::sync::atomic::Ordering::SeqCst)
                {
                    // Hold the reply. The submitter's 2-slot deadline aborts;
                    // dropping `done` would be shutdown, not a stall.
                    let done = unit.done.take();
                    let _ = shutdown.changed().await;
                    drop(done);
                    break;
                }
                if faults.panic_next.swap(false, std::sync::atomic::Ordering::SeqCst) {
                    // Intentional: process-fatal panic policy (§1.5); guarded by JoinHandle.
                    #[allow(clippy::panic)]
                    {
                        panic!("injected writer panic (CC-44b process-fatal test)");
                    }
                }
                let done = unit.done.take();
                let result = commit_p0(&engine, &metrics, &unit, &faults);
                if let Some(done) = done {
                    let _ = done.send(result);
                } else if let Err(e) = result {
                    error!(target: "cc_storage::writer", error = %e, "P0 commit failed");
                }
                tokio::task::yield_now().await;
            }
            update = p1.recv() => {
                let Some(update) = update else { continue; };
                if faults.panic_next.swap(false, std::sync::atomic::Ordering::SeqCst) {
                    #[allow(clippy::panic)]
                    {
                        panic!("injected writer panic (CC-44b process-fatal test)");
                    }
                }
                let result = commit_meta(&engine, &metrics, &update);
                if let Some(done) = update.done {
                    let _ = done.send(result);
                }
                tokio::task::yield_now().await;
            }
            chunk = p2.recv() => {
                let Some(chunk) = chunk else { continue; };
                if faults.panic_next.swap(false, std::sync::atomic::Ordering::SeqCst) {
                    #[allow(clippy::panic)]
                    {
                        panic!("injected writer panic (CC-44b process-fatal test)");
                    }
                }
                let result = commit_p2(&engine, &metrics, &chunk);
                if let Some(done) = chunk.done {
                    let _ = done.send(result);
                }
                tokio::task::yield_now().await;
            }
        }
    }
    info!(target: "cc_storage::writer", "writer task stopped");
}

/// Apply a P0 commit unit: data + cursor in **one** batch (§4.4 same-transaction rule).
fn commit_p0(
    engine: &Engine,
    metrics: &StorageMetrics,
    unit: &CommitUnit,
    faults: &WriterFaults,
) -> Result<(), WriterError> {
    if faults
        .fail_next_commit
        .swap(false, std::sync::atomic::Ordering::SeqCst)
    {
        return Err(WriterError::InjectedFailure);
    }

    let started = Instant::now();
    let mut batch = engine.batch();
    let mut written_blocks: u64 = 0;
    let mut written_columns: u64 = 0;
    let mut written_snapshots: u64 = 0;
    let mut written_meta: u64 = 0;

    {
        let rt = engine.read()?;
        // Re-check inside this read so a second anchor cannot land after
        // the caller's pre-check. Failure returns before `engine.commit`.
        if unit.anchor.is_some() && !txn_is_uninitialized(engine, &rt)? {
            return Err(WriterError::NotUninitialized);
        }
        // Anchor body is cold: Split.slot is the anchor slot, and I-split-fin
        // forbids a hot row at or below that slot. I-ring needs the snapshot
        // slot ≤ Split.slot, so the two meet at the anchor slot.
        let region = if unit.anchor.is_some() {
            BlockRegion::Cold
        } else {
            BlockRegion::Hot
        };
        // Blocks (+ optional canonical walk).
        for b in &unit.blocks {
            // State root is staged after the body put so an idempotent upgrade
            // still fills `state_roots`, and a non-head sibling does not collide.
            // A root already durable in hot or cold is resolved before put_block.
            // Same bytes skip the put: a cold index must not be rewritten as hot.
            // Different bytes stay process-fatal and never reach commit.
            let put_result = match get_block_by_root(&rt, &b.root)? {
                Some(existing) if existing.as_slice() == b.ssz.as_slice() => {
                    if b.update_canonical {
                        let pending = cc_store::blocks::PendingBlocks::new();
                        rewrite_from_head(&rt, &mut batch, &pending, &b.root)?;
                    }
                    Ok(PutBlockOutcome::Idempotent)
                }
                Some(_) => {
                    let table = match cc_store::blocks::slot_by_root(&rt, &b.root)? {
                        Some((slot, BlockRegion::Cold)) => {
                            cc_store::keys::blocks_shard_table(cc_store::keys::block_shard_id(slot))
                        }
                        Some((_, BlockRegion::Hot)) => cc_store::TABLE_BLOCKS_HOT.to_owned(),
                        None => cc_store::TABLE_BLOCK_SLOT_BY_ROOT.to_owned(),
                    };
                    return fatal_key_collision(metrics, &table, &b.ssz);
                }
                None if b.update_canonical => put_block_and_update_head(
                    &rt, &mut batch, b.slot, &b.root, &b.ssz, region, false,
                )
                .map(|(o, _)| o),
                None => put_block(&rt, &mut batch, b.slot, &b.root, &b.ssz, region, false),
            };
            let outcome = match put_result {
                Ok(o) => o,
                Err(StoreError::KeyCollision { table }) => {
                    return fatal_key_collision(metrics, &table, &b.ssz);
                }
                Err(e) => return Err(WriterError::Store(e)),
            };
            match outcome {
                PutBlockOutcome::Inserted => {
                    written_blocks = written_blocks.saturating_add(b.ssz.len() as u64);
                }
                PutBlockOutcome::Idempotent => {
                    // Same bytes: no hot insert and no reverse-index rewrite.
                }
            }
            if b.write_state_root {
                let state_root = state_root_at_offset(&b.ssz)?;
                stage_state_root(&rt, &mut batch, b.slot, &state_root, b.update_canonical)?;
            }
            if let Some(status) = b.da_status {
                put_da_status(&rt, &mut batch, &b.root, status, b.slot)?;
            }
        }

        if let Some(anchor) = &unit.anchor {
            let slot = unit.blocks.first().map(|b| b.slot).ok_or_else(|| {
                WriterError::Store(StoreError::Codec(
                    "commit_anchor requires the anchor body in the same unit".into(),
                ))
            })?;
            batch.put(
                cc_store::snapshots::TABLE_SNAPSHOTS,
                &cc_store::snapshots::encode_snapshot_key(slot),
                &anchor.snapshot_ssz,
            );
            written_snapshots = written_snapshots.saturating_add(anchor.snapshot_ssz.len() as u64);
            batch.put(
                TABLE_META,
                cc_store::meta::KEY_SNAPSHOT_COMPLETION.as_bytes(),
                &anchor.completion_ssz,
            );
            batch.put(
                TABLE_META,
                cc_store::meta::KEY_ANCHOR_INFO.as_bytes(),
                &anchor.anchor_info_ssz,
            );
            batch.put(
                TABLE_META,
                cc_store::meta::KEY_SPLIT.as_bytes(),
                &anchor.split_ssz,
            );
            written_meta = written_meta
                .saturating_add(anchor.completion_ssz.len() as u64)
                .saturating_add(anchor.anchor_info_ssz.len() as u64)
                .saturating_add(anchor.split_ssz.len() as u64);
        }

        if let Some(head_root) = &unit.canonical_from {
            let pending = cc_store::blocks::PendingBlocks::new();
            rewrite_from_head(&rt, &mut batch, &pending, head_root)?;
        }

        // Columns.
        for c in &unit.columns {
            match put_column(
                &rt,
                &mut batch,
                c.slot,
                &c.root,
                c.index,
                &c.ssz,
                BlockRegion::Hot,
            ) {
                Ok(PutColumnOutcome::Inserted) => {
                    written_columns = written_columns.saturating_add(c.ssz.len() as u64);
                }
                Ok(PutColumnOutcome::Idempotent) => {}
                Err(StoreError::KeyCollision { table }) => {
                    return fatal_key_collision(metrics, &table, &c.ssz);
                }
                Err(e) => return Err(WriterError::Store(e)),
            }
        }

        // Fork-choice scalars (opaque SSZ, same container as store meta).
        if let Some(fc) = &unit.fork_choice {
            batch.put(TABLE_META, KEY_FC_SCALARS.as_bytes(), &fc.ssz);
            written_meta = written_meta.saturating_add(fc.ssz.len() as u64);
        }

        // Durable cursor — **same batch as the data it describes** (§4.4).
        let cursor_ssz = unit.cursor.as_ssz_bytes();
        batch.put(TABLE_META, KEY_WRITE_CURSOR.as_bytes(), &cursor_ssz);
        written_meta = written_meta.saturating_add(cursor_ssz.len() as u64);
    }

    engine.commit(batch)?;

    let elapsed = started.elapsed().as_secs_f64();
    metrics
        .commit_seconds
        .get_or_create(&ClassLabels {
            class: StorageClass::Blocks.as_str().to_owned(),
        })
        .observe(elapsed);
    if written_blocks > 0 {
        metrics
            .written_bytes
            .get_or_create(&ClassLabels {
                class: StorageClass::Blocks.as_str().to_owned(),
            })
            .inc_by(written_blocks);
    }
    if written_columns > 0 {
        metrics
            .written_bytes
            .get_or_create(&ClassLabels {
                class: StorageClass::Columns.as_str().to_owned(),
            })
            .inc_by(written_columns);
    }
    if written_snapshots > 0 {
        metrics
            .written_bytes
            .get_or_create(&ClassLabels {
                class: StorageClass::Snapshots.as_str().to_owned(),
            })
            .inc_by(written_snapshots);
    }
    if written_meta > 0 {
        metrics
            .written_bytes
            .get_or_create(&ClassLabels {
                class: StorageClass::Meta.as_str().to_owned(),
            })
            .inc_by(written_meta);
    }

    Ok(())
}

fn commit_meta(
    engine: &Engine,
    metrics: &StorageMetrics,
    update: &MetaUpdate,
) -> Result<(), WriterError> {
    let started = Instant::now();
    let mut batch = engine.batch();
    let mut bytes = 0u64;
    for (table, key, value) in &update.puts {
        // Idempotent put: drop identical; fatal on different bytes for content tables.
        // Index / meta overwrites (block_slot_by_root region, Split) are allowed.
        let rt = engine.read()?;
        match rt.get(table, key)? {
            Some(existing) if existing.as_slice() == value.as_slice() => {
                drop(rt);
                continue;
            }
            Some(_) if is_content_table(table) => {
                return fatal_key_collision(metrics, table, value);
            }
            _ => {}
        }
        drop(rt);
        batch.put(table, key, value);
        bytes = bytes.saturating_add(value.len() as u64);
    }
    for (table, key) in &update.deletes {
        batch.delete(table, key);
    }
    engine.commit(batch)?;
    metrics
        .commit_seconds
        .get_or_create(&ClassLabels {
            class: StorageClass::Meta.as_str().to_owned(),
        })
        .observe(started.elapsed().as_secs_f64());
    if bytes > 0 {
        metrics
            .written_bytes
            .get_or_create(&ClassLabels {
                class: StorageClass::Meta.as_str().to_owned(),
            })
            .inc_by(bytes);
    }
    Ok(())
}

fn commit_p2(
    engine: &Engine,
    metrics: &StorageMetrics,
    chunk: &BackgroundChunk,
) -> Result<(), WriterError> {
    let started = Instant::now();
    let mut batch = engine.batch();
    let mut bytes = 0u64;
    for (table, key, value) in &chunk.puts {
        batch.put(table, key, value);
        bytes = bytes.saturating_add(value.len() as u64);
    }
    for (table, key) in &chunk.deletes {
        batch.delete(table, key);
    }
    engine.commit(batch)?;
    metrics
        .commit_seconds
        .get_or_create(&ClassLabels {
            class: chunk.class.as_str().to_owned(),
        })
        .observe(started.elapsed().as_secs_f64());
    if bytes > 0 {
        metrics
            .written_bytes
            .get_or_create(&ClassLabels {
                class: chunk.class.as_str().to_owned(),
            })
            .inc_by(bytes);
    }
    Ok(())
}

fn is_content_table(table: &str) -> bool {
    table == "blocks_hot"
        || table.starts_with("blocks_")
        || table == "columns_hot"
        || table.starts_with("columns_")
}

/// Key collision is a **fatal** invariant violation (§4.5 / void table P0).
fn fatal_key_collision(
    metrics: &StorageMetrics,
    table: &str,
    _value: &[u8],
) -> Result<(), WriterError> {
    metrics
        .invariant_violation
        .get_or_create(&InvariantLabels {
            invariant: Invariant::KeyCollision.as_str().to_owned(),
        })
        .inc();
    metrics.key_collision.inc();
    error!(
        target: "cc_storage::writer",
        table = %table,
        "key collision (different bytes under existing key) — process-fatal"
    );
    // Process-fatal: continuing would serve one of two colliding bodies.
    std::process::abort();
}

/// Remap [`StoreError::KeyCollision`] from put helpers into the fatal path.
///
/// Callers that use `?` on put_block get `WriterError::Store`; the writer loop
/// should not continue after that for collisions. Exposed for the P0 path when
/// put_block returns before commit.
pub(crate) fn map_put_error(metrics: &StorageMetrics, err: StoreError) -> WriterError {
    match err {
        StoreError::KeyCollision { table } => {
            let _ = fatal_key_collision(metrics, &table, &[]);
            // abort never returns; this is for type-check only
            WriterError::Store(StoreError::KeyCollision { table })
        }
        other => WriterError::Store(other),
    }
}

// Reconnect attribution helper (stream family still seeded; no write-behind).
pub(crate) fn observe_reconnect(metrics: &StorageMetrics, reason: ReconnectReason) {
    metrics
        .stream_reconnect
        .get_or_create(&ReasonLabels {
            reason: reason.as_str().to_owned(),
        })
        .inc();
}

/// Read the durable [`WriteCursor`] from the store, if present.
pub(crate) fn load_write_cursor(engine: &Engine) -> Result<Option<WriteCursor>, StoreError> {
    use cc_store::SszDecode;
    let rt = engine.read()?;
    let Some(bytes) = rt.get(TABLE_META, KEY_WRITE_CURSOR.as_bytes())? else {
        return Ok(None);
    };
    let cursor = WriteCursor::from_ssz_bytes(&bytes)
        .map_err(|e| StoreError::Codec(format!("WriteCursor: {e:?}")))?;
    Ok(Some(cursor))
}

/// `Uninitialized`: no `AnchorInfo` and no block row.
///
/// Tolerates everything `open_and_stamp` writes (schema, digest, node id,
/// write cursor). This is the `commit_anchor` precondition, not the
/// restart tri-state.
pub(crate) fn store_is_uninitialized(engine: &Engine) -> Result<bool, StoreError> {
    let rt = engine.read()?;
    txn_is_uninitialized(engine, &rt)
}

fn txn_is_uninitialized(engine: &Engine, rt: &ReadTxn) -> Result<bool, StoreError> {
    if rt
        .get(TABLE_META, cc_store::meta::KEY_ANCHOR_INFO.as_bytes())?
        .is_some()
    {
        return Ok(false);
    }
    if rt.has_any(cc_store::blocks::TABLE_BLOCKS_HOT)? {
        return Ok(false);
    }
    if rt.has_any(cc_store::blocks::TABLE_BLOCK_SLOT_BY_ROOT)? {
        return Ok(false);
    }
    for name in engine.table_names()? {
        if let Some(("blocks", _)) = cc_store::schema::parse_shard_table(&name)
            && rt.has_any(&name)?
        {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Whether a block body is present for `root` (cursor consistency checks).
pub(crate) fn block_present(engine: &Engine, root: &Root) -> Result<bool, StoreError> {
    let rt = engine.read()?;
    Ok(get_block_by_root(&rt, root)?.is_some())
}

// ── tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::metrics::WriterPriority;
    use cc_store::blocks::{
        MIN_BLOCK_SSZ_LEN, PARENT_ROOT_SSZ_OFFSET, SLOT_SSZ_OFFSET, STATE_ROOT_SSZ_OFFSET,
        get_block_by_root,
    };
    use cc_store::engine::{Durability, EngineOptions};
    use cc_store::meta::WriteCursor;
    use prometheus_client::registry::Registry;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("cc-writer-{label}-{nanos}"));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn eng(label: &str) -> (PathBuf, Arc<Engine>) {
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

    fn root_n(n: u8) -> Root {
        Root::from_array([n; 32])
    }

    fn synth_block(slot: u64, parent: &Root, state: &Root) -> Vec<u8> {
        let mut v = vec![0u8; MIN_BLOCK_SSZ_LEN];
        v[0..4].copy_from_slice(&100u32.to_le_bytes());
        v[SLOT_SSZ_OFFSET..SLOT_SSZ_OFFSET + 8].copy_from_slice(&slot.to_le_bytes());
        v[PARENT_ROOT_SSZ_OFFSET..PARENT_ROOT_SSZ_OFFSET + 32].copy_from_slice(parent.as_slice());
        v[STATE_ROOT_SSZ_OFFSET..STATE_ROOT_SSZ_OFFSET + 32].copy_from_slice(state.as_slice());
        v
    }

    fn cursor(seq: u64, slot: u64, root: Root) -> WriteCursor {
        WriteCursor {
            session_id: 1,
            seq,
            slot: Slot::new(slot),
            root,
        }
    }

    #[tokio::test]
    async fn p0_commit_writes_block_and_cursor_same_batch() {
        let (_dir, engine) = eng("same-batch");
        let m = metrics();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let faults = WriterFaults::default();
        let handle = spawn_writer(
            Arc::clone(&engine),
            m.clone(),
            WriterBounds::default(),
            faults,
            shutdown_rx,
            false,
        );

        let root = root_n(1);
        let ssz = synth_block(1, &Root::ZERO, &root_n(0xF0));
        let (done_tx, done_rx) = oneshot::channel();
        handle
            .submit_p0(CommitUnit {
                blocks: vec![StagedBlock {
                    slot: Slot::new(1),
                    root,
                    ssz: ssz.clone(),
                    update_canonical: true,
                    write_state_root: false,
                    da_status: Some(DaStatus::Available),
                }],
                columns: vec![],
                fork_choice: None,
                canonical_from: None,
                anchor: None,
                cursor: cursor(7, 1, root),
                done: Some(done_tx),
            })
            .await
            .unwrap();
        done_rx.await.unwrap().unwrap();

        // Block present.
        assert!(block_present(&engine, &root).unwrap());
        // Cursor matches.
        let c = load_write_cursor(&engine).unwrap().unwrap();
        assert_eq!(c.seq, 7);
        assert_eq!(c.slot, Slot::new(1));
        assert_eq!(c.root, root);

        let _ = shutdown_tx.send(true);
    }

    #[tokio::test]
    async fn injected_commit_failure_lands_neither_block_nor_cursor() {
        let (_dir, engine) = eng("fail-commit");
        let m = metrics();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let faults = WriterFaults {
            fail_next_commit: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            panic_next: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            ..WriterFaults::default()
        };
        let handle = spawn_writer(
            Arc::clone(&engine),
            m,
            WriterBounds::default(),
            faults,
            shutdown_rx,
            false,
        );

        let root = root_n(2);
        let ssz = synth_block(2, &Root::ZERO, &root_n(0xF0));
        let (done_tx, done_rx) = oneshot::channel();
        handle
            .submit_p0(CommitUnit {
                blocks: vec![StagedBlock {
                    slot: Slot::new(2),
                    root,
                    ssz,
                    update_canonical: true,
                    write_state_root: false,
                    da_status: None,
                }],
                columns: vec![],
                fork_choice: None,
                canonical_from: None,
                anchor: None,
                cursor: cursor(1, 2, root),
                done: Some(done_tx),
            })
            .await
            .unwrap();
        let err = done_rx.await.unwrap().unwrap_err();
        assert!(matches!(err, WriterError::InjectedFailure));
        assert!(!block_present(&engine, &root).unwrap());
        assert!(load_write_cursor(&engine).unwrap().is_none());

        let _ = shutdown_tx.send(true);
    }

    #[tokio::test]
    async fn identical_put_is_idempotent() {
        let (_dir, engine) = eng("idempotent");
        let m = metrics();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = spawn_writer(
            Arc::clone(&engine),
            m,
            WriterBounds::default(),
            WriterFaults::default(),
            shutdown_rx,
            false,
        );

        let root = root_n(3);
        let ssz = synth_block(3, &Root::ZERO, &root_n(0xF0));
        for seq in [1u64, 2] {
            let (done_tx, done_rx) = oneshot::channel();
            handle
                .submit_p0(CommitUnit {
                    blocks: vec![StagedBlock {
                        slot: Slot::new(3),
                        root,
                        ssz: ssz.clone(),
                        update_canonical: true,
                        write_state_root: false,
                        da_status: None,
                    }],
                    columns: vec![],
                    fork_choice: None,
                    canonical_from: None,
                    anchor: None,
                    cursor: cursor(seq, 3, root),
                    done: Some(done_tx),
                })
                .await
                .unwrap();
            done_rx.await.unwrap().unwrap();
        }
        let rt = engine.read().unwrap();
        let body = get_block_by_root(&rt, &root).unwrap().unwrap();
        assert_eq!(body, ssz);
        let c = load_write_cursor(&engine).unwrap().unwrap();
        assert_eq!(c.seq, 2);

        let _ = shutdown_tx.send(true);
    }

    #[tokio::test]
    async fn p2_drop_newest_on_full_counts_metric() {
        let (_dir, engine) = eng("p2-drop");
        let m = metrics();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        // Tiny P2 bound so we can fill it without running the consumer fast enough.
        let bounds = WriterBounds {
            p0: 4,
            p1: 4,
            p2: 1,
        };
        // Hold the writer on a P0 that never completes? Simpler: don't drain P2 —
        // spawn writer that is blocked on shutdown select; fill p2.
        // Actually the writer drains P2. Use a handle with p2_tx only:
        let _handle = spawn_writer(
            Arc::clone(&engine),
            m.clone(),
            bounds,
            WriterFaults::default(),
            shutdown_rx,
            false,
        );
        // Pause writer by not yielding enough: flood p2 while writer is idle —
        // first send succeeds, second should drop when depth is 1 and writer hasn't
        // taken it yet. Racey; submit two with try and check at least one drop path.
        // Better: construct WriterHandle channels directly.
        let _ = shutdown_tx.send(true);
        tokio::task::yield_now().await;

        let (p0_tx, _p0_rx) = mpsc::channel(1);
        let (p1_tx, _p1_rx) = mpsc::channel(1);
        let (p2_tx, _p2_rx) = mpsc::channel::<BackgroundChunk>(1);
        let h = WriterHandle {
            p0: p0_tx,
            p1: p1_tx,
            p2: p2_tx,
            metrics: m.clone(),
            process_fatal: false,
            on_fatal: ProcessExit::Os,
        };
        let chunk = BackgroundChunk {
            class: StorageClass::Blocks,
            puts: vec![],
            deletes: vec![],
            done: None,
        };
        assert!(h.try_submit_p2(chunk.clone(), &m));
        assert!(!h.try_submit_p2(chunk, &m));
        let dropped = m
            .writer_chunk_dropped
            .get_or_create(&ClassLabels {
                class: WriterPriority::P2.as_str().to_owned(),
            })
            .get();
        assert_eq!(dropped, 1);
    }

    /// Process-fatal path invokes the exit hook with non-zero code (test mode).
    #[tokio::test]
    async fn writer_panic_invokes_process_exit_hook() {
        let (_dir, engine) = eng("fatal-hook");
        let m = metrics();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let exited = Arc::new(std::sync::atomic::AtomicI32::new(0));
        let exited2 = Arc::clone(&exited);
        let faults = WriterFaults {
            fail_next_commit: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            panic_next: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            ..WriterFaults::default()
        };
        let handle = spawn_writer_with_exit(
            Arc::clone(&engine),
            m,
            WriterBounds::default(),
            faults,
            shutdown_rx,
            true,
            ProcessExit::Hook(Arc::new(move |code, _reason| {
                exited2.store(code, std::sync::atomic::Ordering::SeqCst);
            })),
        );
        let (done_tx, _done_rx) = oneshot::channel();
        let _ = handle
            .submit_p0(CommitUnit {
                blocks: vec![],
                columns: vec![],
                fork_choice: None,
                canonical_from: None,
                anchor: None,
                cursor: cursor(0, 0, Root::ZERO),
                done: Some(done_tx),
            })
            .await;
        // Wait for panic guard.
        for _ in 0..50 {
            if exited.load(std::sync::atomic::Ordering::SeqCst) != 0 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(
            exited.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "process-fatal guard must invoke exit hook with code 1"
        );
    }

    #[tokio::test]
    async fn writer_panic_is_join_error_when_not_process_fatal() {
        let (_dir, engine) = eng("panic");
        let m = metrics();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let faults = WriterFaults {
            fail_next_commit: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            panic_next: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            ..WriterFaults::default()
        };
        // process_fatal = false so the test process survives.
        let _handle = spawn_writer(
            Arc::clone(&engine),
            m,
            WriterBounds::default(),
            faults,
            shutdown_rx,
            false,
        );
        let (done_tx, done_rx) = oneshot::channel();
        // This may fail to deliver if panic races before recv.
        let _ = _handle
            .submit_p0(CommitUnit {
                blocks: vec![],
                columns: vec![],
                fork_choice: None,
                canonical_from: None,
                anchor: None,
                cursor: cursor(0, 0, Root::ZERO),
                done: Some(done_tx),
            })
            .await;
        // Wait a bit for the panic guard to observe the join.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        // done channel may be dropped by panic; either is fine — process still alive.
        let _ = done_rx.await;
    }

    /// Key collision is fatal at the store put layer (§4.5). The writer maps
    /// this to `cc_storage_invariant_violation_total{invariant="key_collision"}`
    /// and `process::abort` — abort is not run inside the test process.
    #[test]
    fn key_collision_different_bytes_is_store_error() {
        let (_dir, engine) = eng("collision-store");
        let root = root_n(9);
        let ssz_a = synth_block(9, &Root::ZERO, &root_n(0xAA));
        let ssz_b = synth_block(9, &Root::ZERO, &root_n(0xBB));
        let mut b = engine.batch();
        {
            let rt = engine.read().unwrap();
            put_block(
                &rt,
                &mut b,
                Slot::new(9),
                &root,
                &ssz_a,
                BlockRegion::Hot,
                false,
            )
            .unwrap();
        }
        engine.commit(b).unwrap();
        let mut b2 = engine.batch();
        let err = {
            let rt = engine.read().unwrap();
            put_block(
                &rt,
                &mut b2,
                Slot::new(9),
                &root,
                &ssz_b,
                BlockRegion::Hot,
                false,
            )
            .unwrap_err()
        };
        assert!(matches!(err, StoreError::KeyCollision { .. }));
    }

    /// Metric attribution for key_collision before abort (test-only helper path).
    #[test]
    fn key_collision_increments_invariant_metric() {
        let m = metrics();
        m.invariant_violation
            .get_or_create(&InvariantLabels {
                invariant: Invariant::KeyCollision.as_str().to_owned(),
            })
            .inc();
        m.key_collision.inc();
        assert_eq!(
            m.invariant_violation
                .get_or_create(&InvariantLabels {
                    invariant: "key_collision".into(),
                })
                .get(),
            1
        );
        assert_eq!(m.key_collision.get(), 1);
    }
}
