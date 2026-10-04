//! [`ArchiveWrite`] over the live P0 writer mailbox (S2-A-05 / S2-A-06).
//!
//! Uses [`ColumnBatch::index`] as the durable key. Does not peek a sidecar
//! byte offset and does not fall back to index 0.
//!
//! Every batch submitted to the writer carries, at its head, the
//! `(parent_root, slot)` of the block the batch's columns and canonical
//! rows attach to. The writer rejects the batch unless that parent is
//! already durable, or is the first row of the same batch. There is no
//! "progress optional" path and no empty-progress bypass.
//!
//! A batch may only extend the durable frontier, never jump it.
//! `commit_anchor` is the only op that may write a body whose parent is
//! not durable, and only on an uninitialized store, once.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cc_seam::{
    ArchiveWrite, Bytes, ColumnBatch, DaVerdict, DurableImport, FailedPreconditionReason,
    HeadChange, SeamError, TrustedAnchor,
};
use cc_store::blocks::{parent_root_at_offset, slot_at_offset, state_root_at_offset};
use cc_store::columns::{
    COLUMN_HEADER_PARENT_ROOT_SSZ_OFFSET, MIN_COLUMN_SSZ_LEN, NUMBER_OF_COLUMNS,
    column_parent_root_at_offset,
};
use cc_store::engine::{Engine, StoreError};
use cc_store::meta::{
    AnchorInfo, ForkChoiceScalars, KEY_NODE_ID, SnapshotCompletion, Split, TABLE_META, WriteCursor,
};
use cc_store::{DaStatus, Root, Slot, SszDecode, SszEncode, get_block_by_root};

use crate::metrics::{StorageClass, StorageMetrics};
use crate::resume::{RestartState, classify_for_epoch};
use crate::writer::{
    BackgroundChunk, CommitUnit, StagedAnchor, StagedBlock, StagedColumn, StagedForkChoiceScalars,
    WriterError, WriterHandle, block_present, load_write_cursor, store_is_uninitialized,
};

pub(crate) use crate::writer::COMMIT_DEADLINE_REASON;

/// Staging slice size for [`ArchiveWrite::commit_snapshot`].
///
/// Same 1 MiB as the serve-side state chunk. The final P2 chunk still carries
/// the full SSZ plus the completion marker; these slices stay off the P0 path.
const DEFAULT_SNAPSHOT_CHUNK_BYTES: usize = 1024 * 1024;

/// How many staging keys the final chunk deletes, including leftovers from an
/// earlier interrupted attempt at the same slot.
const SNAPSHOT_STAGING_DELETE_SPAN: u32 = 1024;

const SNAPSHOT_STAGING_PREFIX: &[u8] = b"snap_chunk:";

fn detached_metrics() -> StorageMetrics {
    let mut registry = prometheus_client::registry::Registry::default();
    StorageMetrics::register(&mut registry)
}

/// One block from by-range serve: canonical lookup, then that body's SSZ.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServedCanonicalBlock {
    /// Slot the canonical index named.
    pub slot: u64,
    /// Root stored at that slot.
    pub root: [u8; 32],
    /// Body bytes served for that root. Not a separate table read.
    pub ssz: Vec<u8>,
}

/// Typed ingest adapter. Holds the live writer handle — no second mailbox.
#[derive(Debug, Clone)]
pub struct ArchiveWriter {
    writer: WriterHandle,
    engine: Arc<Engine>,
    /// Running chain `seconds_per_slot`. The commit deadline is twice this.
    seconds_per_slot: u64,
    /// Preset epoch length. Epoch-boundary snapshots use this, not a fixed 32.
    slots_per_epoch: u64,
    /// `OpenOpts.snapshot_ring`. `0` is stored as 1.
    snapshot_ring: u64,
    /// Staging slice size for a P2 snapshot. The final chunk still writes the
    /// full SSZ with the completion marker.
    snapshot_chunk_bytes: usize,
    /// Drop counter for [`WriterHandle::try_submit_p2`].
    metrics: StorageMetrics,
    /// Test-only: return after this many staging chunks, before the marker.
    #[cfg(test)]
    snapshot_interrupt_after: Option<u32>,
}

impl ArchiveWriter {
    pub(crate) fn new(writer: WriterHandle, engine: Arc<Engine>) -> Self {
        Self {
            writer,
            engine,
            // Caller has not supplied a network. The existing mainnet-shaped
            // slot length is the stand-in; the deadline is still twice that,
            // not a fixed 24 s. `with_seconds_per_slot` replaces it.
            seconds_per_slot: crate::prune::DEFAULT_SECONDS_PER_SLOT.max(1),
            slots_per_epoch: crate::resume::default_slots_per_epoch(),
            snapshot_ring: cc_store::DEFAULT_SNAPSHOT_RING.max(1),
            snapshot_chunk_bytes: DEFAULT_SNAPSHOT_CHUNK_BYTES,
            metrics: detached_metrics(),
            #[cfg(test)]
            snapshot_interrupt_after: None,
        }
    }

    /// Slot length from the running chain config. `0` is not a slot.
    #[must_use]
    pub(crate) fn with_seconds_per_slot(mut self, seconds_per_slot: u64) -> Self {
        self.seconds_per_slot = seconds_per_slot.max(1);
        self
    }

    /// Epoch length from the running chain preset. `0` is not an epoch.
    #[must_use]
    pub(crate) fn with_slots_per_epoch(mut self, slots_per_epoch: u64) -> Self {
        self.slots_per_epoch = slots_per_epoch.max(1);
        self
    }

    /// Ring depth. Trim deletes are part of the final snapshot chunk.
    #[must_use]
    pub(crate) fn with_snapshot_ring(mut self, snapshot_ring: u64) -> Self {
        self.snapshot_ring = snapshot_ring.max(1);
        self
    }

    /// Staging chunk size. `0` becomes 1 byte so `chunks` cannot panic.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn with_snapshot_chunk_bytes(mut self, snapshot_chunk_bytes: usize) -> Self {
        self.snapshot_chunk_bytes = snapshot_chunk_bytes.max(1);
        self
    }

    /// Metrics clone shared with the writer so a P2 drop is observable.
    #[must_use]
    pub(crate) fn with_metrics(mut self, metrics: StorageMetrics) -> Self {
        self.metrics = metrics;
        self
    }

    /// Stop after `chunks` staging commits and do not write the marker.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn with_snapshot_interrupt_after(mut self, chunks: u32) -> Self {
        self.snapshot_interrupt_after = Some(chunks);
        self
    }

    #[cfg(test)]
    fn snapshot_interrupted(&self, staged: u32) -> bool {
        self.snapshot_interrupt_after
            .is_some_and(|limit| staged >= limit)
    }

    #[cfg(not(test))]
    fn snapshot_interrupted(&self, _staged: u32) -> bool {
        false
    }

    /// Seed a genesis write cursor when the store has none (S2-A-14).
    ///
    /// Ingest restamps this record; it does not invent `session_id=0`/`seq=0`
    /// at submit time.
    pub(crate) fn ensure_write_cursor(engine: &Engine) -> Result<(), StoreError> {
        if load_write_cursor(engine)?.is_some() {
            return Ok(());
        }
        let cursor = WriteCursor {
            session_id: 1,
            seq: 0,
            slot: Slot::new(0),
            root: Root::ZERO,
        };
        let mut batch = engine.batch();
        batch.put(
            cc_store::meta::TABLE_META,
            cc_store::meta::KEY_WRITE_CURSOR.as_bytes(),
            &cursor.as_ssz_bytes(),
        );
        engine.commit(batch)
    }

    /// By-range serve of the canonical view at the selected head.
    ///
    /// Same read as `GetBlocksByRange`: the split, then canonical lookup,
    /// then the body. A direct `canonical` table scan is not this path.
    pub fn serve_blocks_by_range(
        &self,
        start_slot: u64,
        count: u64,
    ) -> Result<Vec<ServedCanonicalBlock>, SeamError> {
        if count == 0 {
            return Ok(Vec::new());
        }
        if count > cc_store::MAX_BLOCKS_BY_RANGE {
            return Err(SeamError::InvalidArgument(format!(
                "count {count} exceeds MAX_BLOCKS_BY_RANGE ({})",
                cc_store::MAX_BLOCKS_BY_RANGE
            )));
        }
        let rt = self
            .engine
            .read()
            .map_err(|e| SeamError::Unavailable(e.to_string()))?;
        let split = cc_store::load_split(&rt)
            .map_err(map_store_read_err)?
            .map(|split| split.slot);
        let rows = cc_store::blocks_by_range(&rt, Slot::new(start_slot), count, split)
            .map_err(map_store_read_err)?;
        Ok(rows
            .into_iter()
            .map(|row| ServedCanonicalBlock {
                slot: row.slot.as_u64(),
                root: *row.root.as_array(),
                ssz: row.ssz,
            })
            .collect())
    }
}

fn staged_from_batch(batch: ColumnBatch) -> Result<StagedColumn, SeamError> {
    let index = u16::try_from(batch.index).map_err(|_| {
        SeamError::InvalidArgument(format!("column index {} exceeds u16 domain", batch.index))
    })?;
    if index >= NUMBER_OF_COLUMNS {
        return Err(SeamError::InvalidArgument(format!(
            "column index {index} ≥ NUMBER_OF_COLUMNS ({NUMBER_OF_COLUMNS})"
        )));
    }
    if batch.ssz.len() < MIN_COLUMN_SSZ_LEN {
        return Err(SeamError::InvalidArgument(format!(
            "malformed column sidecar: len {} below MIN_COLUMN_SSZ_LEN ({MIN_COLUMN_SSZ_LEN})",
            batch.ssz.len()
        )));
    }
    let ssz_parent = column_parent_root_at_offset(batch.ssz.as_ref())
        .map_err(|e| SeamError::InvalidArgument(e.to_string()))?;
    if ssz_parent.as_slice() != batch.parent_root.as_slice() {
        return Err(SeamError::InvalidArgument(format!(
            "column parent_root mismatch: caller != SSZ header parent_root at offset {COLUMN_HEADER_PARENT_ROOT_SSZ_OFFSET}"
        )));
    }
    Ok(StagedColumn {
        slot: Slot::new(batch.slot),
        root: Root::from_array(batch.block_root),
        index,
        ssz: batch.ssz.to_vec(),
    })
}

fn map_store_read_err(err: StoreError) -> SeamError {
    match err {
        StoreError::Codec(msg) | StoreError::Limit(msg) => SeamError::InvalidArgument(msg),
        other => SeamError::Unavailable(other.to_string()),
    }
}

fn map_writer_err(err: WriterError) -> SeamError {
    match err {
        WriterError::ShutDown => SeamError::Unavailable("writer shut down".into()),
        WriterError::Store(StoreError::Codec(msg) | StoreError::Limit(msg)) => {
            SeamError::InvalidArgument(msg)
        }
        WriterError::Store(e) => SeamError::Unavailable(e.to_string()),
        WriterError::InjectedFailure => SeamError::Unavailable("injected commit failure".into()),
        WriterError::HotAtOrBelowSplit => {
            SeamError::InvalidArgument("hot block slot is at or below the split".into())
        }
        WriterError::NotUninitialized => SeamError::FailedPrecondition {
            reason: FailedPreconditionReason::StoreNotUninitialized,
        },
    }
}

/// Head of a writer batch: the `(parent_root, slot)` rows attach to.
#[derive(Debug, Clone, Copy)]
struct ContinuityHead {
    parent_root: Root,
    slot: Slot,
}

/// Writer-facing batch. [`None`] head is only the rejected empty-progress encoding.
#[derive(Debug)]
struct WriterBatch {
    head: Option<ContinuityHead>,
    blocks: Vec<StagedBlock>,
    columns: Vec<StagedColumn>,
}

/// A batch may only extend the durable frontier, never jump it.
///
/// Same invariant as `S0-B-10` on `PutBackfillBatch`. The writer rejects
/// the batch unless the named parent is already durable, or is the first
/// row of the same batch. There is no "progress optional" path and no
/// empty-progress bypass.
fn admit_top_of_batch_continuity(engine: &Engine, batch: &WriterBatch) -> Result<(), SeamError> {
    let Some(head) = batch.head else {
        return Err(SeamError::InvalidArgument(
            "continuity bind is required; empty-progress batches are rejected".into(),
        ));
    };
    if batch
        .blocks
        .first()
        .is_some_and(|row| row.root == head.parent_root)
    {
        return Ok(());
    }
    match block_present(engine, &head.parent_root) {
        Ok(true) => Ok(()),
        Ok(false) => Err(SeamError::InvalidArgument(format!(
            "a batch may only extend the durable frontier, never jump it \
             (parent is not durable and is not the first row of the same batch; \
              slot={})",
            head.slot.as_u64()
        ))),
        Err(e) => Err(SeamError::Unavailable(e.to_string())),
    }
}

impl ArchiveWriter {
    fn load_or_missing_cursor(&self) -> Result<WriteCursor, SeamError> {
        load_write_cursor(&self.engine)
            .map_err(|e| SeamError::Unavailable(e.to_string()))?
            .ok_or_else(|| {
                SeamError::Unavailable(
                    "no durable write cursor; refuse to invent a zero cursor".into(),
                )
            })
    }

    fn unit_for_batch(
        &self,
        batch: WriterBatch,
        cursor: WriteCursor,
    ) -> Result<CommitUnit, SeamError> {
        admit_top_of_batch_continuity(&self.engine, &batch)?;
        Ok(CommitUnit {
            blocks: batch.blocks,
            columns: batch.columns,
            fork_choice: None,
            canonical_from: None,
            cursor,
            done: None,
            anchor: None,
        })
    }

    async fn submit_writer_batch(&self, batch: WriterBatch) -> Result<(), SeamError> {
        // Columns restamp the existing cursor (batch-seq unchanged).
        let cursor = self.load_or_missing_cursor()?;
        let unit = self.unit_for_batch(batch, cursor)?;
        self.writer
            .submit_p0_committed(unit)
            .await
            .map_err(map_writer_err)
    }

    /// Async `commit_import` waits `2 * seconds_per_slot` on the P0 writer.
    ///
    /// The deadline is on this async call only. The core thread still
    /// persists through the blocking path with no deadline. Probe coverage
    /// of an archive stall starts when that thread blocks in `commit_import`
    /// (S2R-A-05). Past this wait the existing process-fatal path aborts
    /// with [`COMMIT_DEADLINE_REASON`]. Aborting is not backpressure; this
    /// cites that consequence and does not re-decide it.
    async fn submit_commit_import(&self, unit: CommitUnit) -> Result<(), SeamError> {
        let deadline = Duration::from_secs(self.seconds_per_slot.max(1).saturating_mul(2));
        let started = tokio::time::Instant::now();
        let outcome = tokio::time::timeout(deadline, self.writer.submit_p0_committed(unit)).await;
        self.writer
            .metrics()
            .commit_wait_seconds
            .observe(started.elapsed().as_secs_f64());
        match outcome {
            Ok(result) => result.map_err(map_writer_err),
            Err(_elapsed) => {
                tracing::error!(
                    target: "cc_storage::writer",
                    reason = COMMIT_DEADLINE_REASON,
                    seconds_per_slot = self.seconds_per_slot,
                    "commit deadline exceeded; aborting is not backpressure (ADR-R-08)"
                );
                self.writer.invoke_process_fatal(COMMIT_DEADLINE_REASON);
                Err(SeamError::Unavailable(COMMIT_DEADLINE_REASON.to_owned()))
            }
        }
    }
}

#[async_trait]
impl ArchiveWrite for ArchiveWriter {
    async fn ingest_columns(&self, batch: ColumnBatch) -> Result<(), SeamError> {
        let head = ContinuityHead {
            parent_root: Root::from_array(batch.parent_root),
            slot: Slot::new(batch.slot),
        };
        let column = staged_from_batch(batch)?;
        self.submit_writer_batch(WriterBatch {
            head: Some(head),
            blocks: Vec::new(),
            columns: vec![column],
        })
        .await
    }

    async fn commit_import(&self, import: DurableImport) -> Result<(), SeamError> {
        let unit = bind_durable_import(&self.engine, self.slots_per_epoch, import)?;
        self.submit_commit_import(unit).await
    }

    async fn set_head(&self, head: HeadChange, scalars: Bytes) -> Result<(), SeamError> {
        let unit = bind_set_head(&self.engine, self.slots_per_epoch, head, scalars)?;
        self.writer
            .submit_p0_committed(unit)
            .await
            .map_err(map_writer_err)
    }

    fn block_is_durable(&self, root: cc_seam::Root) -> Result<bool, SeamError> {
        block_present(&self.engine, &Root::from_array(root))
            .map_err(|e| SeamError::Unavailable(e.to_string()))
    }

    fn import_precondition(&self, parent_root: cc_seam::Root) -> Result<(), SeamError> {
        // Same predicate `commit_import` already applies. A store that is not
        // `Complete` is `STORE_INCOMPLETE`, then a missing parent is
        // `PARENT_NOT_DURABLE`.
        admit_import_store(&self.engine, self.slots_per_epoch)?;
        match block_present(&self.engine, &Root::from_array(parent_root)) {
            Ok(true) => Ok(()),
            Ok(false) => Err(precondition(FailedPreconditionReason::ParentNotDurable)),
            Err(e) => Err(SeamError::Unavailable(e.to_string())),
        }
    }

    fn commit_import_blocking(&self, import: DurableImport) -> Result<(), SeamError> {
        let unit = bind_durable_import(&self.engine, self.slots_per_epoch, import)?;
        self.writer
            .blocking_submit_p0_committed(unit)
            .map_err(map_writer_err)
    }

    fn set_head_blocking(&self, head: HeadChange, scalars: Bytes) -> Result<(), SeamError> {
        let unit = bind_set_head(&self.engine, self.slots_per_epoch, head, scalars)?;
        self.writer
            .blocking_submit_p0_committed(unit)
            .map_err(map_writer_err)
    }

    async fn commit_anchor(&self, anchor: TrustedAnchor) -> Result<(), SeamError> {
        if !store_is_uninitialized(&self.engine)
            .map_err(|e| SeamError::Unavailable(e.to_string()))?
        {
            return Err(SeamError::FailedPrecondition {
                reason: FailedPreconditionReason::StoreNotUninitialized,
            });
        }
        let unit = anchor_unit(&self.engine, anchor)?;
        self.writer
            .submit_p0_committed(unit)
            .await
            .map_err(map_writer_err)
    }

    async fn commit_snapshot(&self, snapshot: cc_seam::Snapshot) -> Result<(), SeamError> {
        admit_import_store(&self.engine, self.slots_per_epoch)?;
        let slot = Slot::new(snapshot.slot);
        let ssz = snapshot.state_ssz.as_ref();
        cc_store::check_snapshot_len(slot, ssz.len() as u64).map_err(map_store_read_err)?;
        let epoch_slots = self.slots_per_epoch.max(1);
        if !snapshot.slot.is_multiple_of(epoch_slots) {
            return Err(SeamError::InvalidArgument(format!(
                "snapshot slot {} is not an epoch boundary (slots_per_epoch {epoch_slots})",
                snapshot.slot
            )));
        }
        let head = durable_head_slot(&self.engine)?;
        if snapshot.slot > head {
            return Err(SeamError::InvalidArgument(format!(
                "snapshot slot {} is ahead of durable head {head}",
                snapshot.slot
            )));
        }
        if let Some(marker) = completed_marker(&self.engine)? {
            let completed = marker.slot.as_u64();
            if snapshot.slot < completed {
                return Err(SeamError::InvalidArgument(format!(
                    "snapshot slot {} is behind completed snapshot {completed}",
                    snapshot.slot
                )));
            }
            if snapshot.slot == completed {
                let same = marker.state_root == Root::from_array(snapshot.state_root)
                    && marker.bytes == ssz.len() as u64;
                if same {
                    return Ok(());
                }
                return Err(SeamError::InvalidArgument(format!(
                    "snapshot slot {} does not match the completion marker",
                    snapshot.slot
                )));
            }
        }

        // ADR-R-08: P2 bound 256 drop-newest. The literal is `WRITER_P2_BOUND`.
        // A dropped chunk is not a durable snapshot. The ring entry becomes
        // newest only when `SnapshotCompletion` lands in the final chunk.
        let pieces: Vec<&[u8]> = ssz.chunks(self.snapshot_chunk_bytes.max(1)).collect();
        let mut staged = 0u32;
        for (index, piece) in pieces.iter().enumerate() {
            if self.snapshot_interrupted(staged) {
                return Err(interrupted_snapshot());
            }
            let index = u32::try_from(index).map_err(|_| {
                SeamError::InvalidArgument("snapshot staging index exceeds u32".into())
            })?;
            self.submit_p2_chunk(BackgroundChunk {
                class: StorageClass::Snapshots,
                puts: vec![(
                    TABLE_META.to_owned(),
                    snapshot_staging_key(snapshot.slot, index),
                    piece.to_vec(),
                )],
                deletes: Vec::new(),
                done: None,
            })
            .await?;
            staged = staged.saturating_add(1);
        }
        if self.snapshot_interrupted(staged) {
            return Err(interrupted_snapshot());
        }

        self.submit_completed_snapshot(&snapshot).await
    }

    async fn realign_head_snapshot(&self, snapshot: cc_seam::Snapshot) -> Result<(), SeamError> {
        admit_import_store(&self.engine, self.slots_per_epoch)?;
        let slot = Slot::new(snapshot.slot);
        let ssz = snapshot.state_ssz.as_ref();
        cc_store::check_snapshot_len(slot, ssz.len() as u64).map_err(map_store_read_err)?;
        let Some(marker) = completed_marker(&self.engine)? else {
            return Ok(());
        };
        if marker.slot.as_u64() != snapshot.slot {
            return Ok(());
        }
        let state_root = Root::from_array(snapshot.state_root);
        if marker.state_root == state_root && marker.bytes == ssz.len() as u64 {
            return Ok(());
        }
        // The head write already made this block canonical. A losing block's
        // state root does not match that header, so it cannot move the marker.
        let canonical = canonical_state_root(&self.engine, marker.slot)?;
        if canonical != Some(state_root) {
            return Err(SeamError::InvalidArgument(
                "head snapshot state_root does not match the canonical block at the marker slot"
                    .into(),
            ));
        }
        self.submit_completed_snapshot(&snapshot).await
    }
}

impl ArchiveWriter {
    /// Final P2 chunk: full SSZ, the one `snap_complete` marker, and ring trim.
    ///
    /// The write cursor stays the block root the boundary `commit_import` or
    /// `set_head` already stored. Putting a cursor sampled here onto P2 would
    /// let a later P0 commit rewind `seq`.
    async fn submit_completed_snapshot(
        &self,
        snapshot: &cc_seam::Snapshot,
    ) -> Result<(), SeamError> {
        let slot = Slot::new(snapshot.slot);
        let ssz = snapshot.state_ssz.as_ref();
        let plan = crate::open::StorageRuntime::plan_snapshot_ring(
            &self.engine,
            slot,
            ssz,
            self.snapshot_ring,
        )
        .map_err(map_store_read_err)?;
        let marker = SnapshotCompletion {
            slot,
            state_root: Root::from_array(snapshot.state_root),
            bytes: ssz.len() as u64,
        };
        let mut puts = plan.puts;
        puts.push((
            TABLE_META.to_owned(),
            cc_store::meta::KEY_SNAPSHOT_COMPLETION.as_bytes().to_vec(),
            marker.as_ssz_bytes(),
        ));
        let mut deletes = plan.deletes;
        for index in 0..SNAPSHOT_STAGING_DELETE_SPAN {
            deletes.push((
                TABLE_META.to_owned(),
                snapshot_staging_key(snapshot.slot, index),
            ));
        }
        self.submit_p2_chunk(BackgroundChunk {
            class: StorageClass::Snapshots,
            puts,
            deletes,
            done: None,
        })
        .await?;
        crate::open::StorageRuntime::enforce_snapshot_ring_on(&self.engine, self.snapshot_ring)
            .map_err(|err| SeamError::Unavailable(format!("I-ring: {err}")))?;
        Ok(())
    }
}

fn anchor_unit(engine: &Engine, anchor: TrustedAnchor) -> Result<CommitUnit, SeamError> {
    let ssz = anchor.block_ssz.as_ref();
    let ssz_slot = slot_at_offset(ssz).map_err(|e| SeamError::InvalidArgument(e.to_string()))?;
    let slot = Slot::new(anchor.slot);
    if ssz_slot != slot {
        return Err(SeamError::InvalidArgument(format!(
            "slot mismatch: caller {} != SSZ header slot {}",
            slot.as_u64(),
            ssz_slot.as_u64()
        )));
    }

    let ssz_parent =
        parent_root_at_offset(ssz).map_err(|e| SeamError::InvalidArgument(e.to_string()))?;
    let parent = Root::from_array(anchor.parent_root);
    if ssz_parent != parent {
        return Err(SeamError::InvalidArgument(format!(
            "parent_root mismatch: caller != SSZ header parent_root at offset {}",
            cc_store::PARENT_ROOT_SSZ_OFFSET
        )));
    }

    let ssz_state =
        state_root_at_offset(ssz).map_err(|e| SeamError::InvalidArgument(e.to_string()))?;
    let state_root = Root::from_array(anchor.state_root);
    if ssz_state != state_root {
        return Err(SeamError::InvalidArgument(format!(
            "state_root mismatch: caller != SSZ header state_root at offset {}",
            cc_store::STATE_ROOT_SSZ_OFFSET
        )));
    }

    cc_store::check_snapshot_len(slot, anchor.state_ssz.len() as u64).map_err(|e| match e {
        StoreError::Codec(msg) | StoreError::Limit(msg) => SeamError::InvalidArgument(msg),
        other => SeamError::Unavailable(other.to_string()),
    })?;

    let root = Root::from_array(anchor.block_root);
    let node_id = load_node_id(engine)?;
    let info = AnchorInfo {
        anchor_slot: slot,
        anchor_root: root,
        anchor_state_root: state_root,
        node_id,
        oldest_block_slot: slot,
        oldest_block_parent: parent,
    };
    let split = Split {
        slot,
        state_root,
        block_root: root,
    };
    let completion = SnapshotCompletion {
        slot,
        state_root,
        bytes: anchor.state_ssz.len() as u64,
    };
    let prev = load_write_cursor(engine)
        .map_err(|e| SeamError::Unavailable(e.to_string()))?
        .ok_or_else(|| {
            SeamError::Unavailable("no durable write cursor; refuse to invent a zero cursor".into())
        })?;
    let cursor = WriteCursor {
        session_id: prev.session_id,
        seq: prev.seq.saturating_add(1),
        slot,
        root,
    };
    let da_status = match anchor.da {
        DaVerdict::Available => DaStatus::Available,
        DaVerdict::Deferred => DaStatus::Deferred,
    };

    Ok(CommitUnit {
        blocks: vec![StagedBlock {
            slot,
            root,
            ssz: anchor.block_ssz.to_vec(),
            update_canonical: true,
            write_state_root: true,
            da_status: Some(da_status),
        }],
        columns: Vec::new(),
        fork_choice: Some(StagedForkChoiceScalars {
            ssz: anchor.scalars.to_vec(),
        }),
        canonical_from: None,
        cursor,
        done: None,
        anchor: Some(StagedAnchor {
            snapshot_ssz: anchor.state_ssz.to_vec(),
            completion_ssz: completion.as_ssz_bytes(),
            anchor_info_ssz: info.as_ssz_bytes(),
            split_ssz: split.as_ssz_bytes(),
        }),
    })
}

fn load_node_id(engine: &Engine) -> Result<Root, SeamError> {
    let rt = engine
        .read()
        .map_err(|e| SeamError::Unavailable(e.to_string()))?;
    let Some(bytes) = rt
        .get(TABLE_META, KEY_NODE_ID.as_bytes())
        .map_err(|e| SeamError::Unavailable(e.to_string()))?
    else {
        return Ok(Root::ZERO);
    };
    Root::from_ssz_bytes(&bytes).map_err(|e| {
        SeamError::Unavailable(format!(
            "node_id SSZ decode failed ({e:?}); anchor not written"
        ))
    })
}

fn precondition(reason: FailedPreconditionReason) -> SeamError {
    SeamError::FailedPrecondition { reason }
}

/// A committed scalar blob must still decode, or the next admit is `Incomplete`.
fn decode_fork_choice_scalars(bytes: &[u8]) -> Result<ForkChoiceScalars, SeamError> {
    ForkChoiceScalars::from_ssz_bytes(bytes).map_err(|err| {
        SeamError::InvalidArgument(format!("ForkChoiceScalars decode failed: {err:?}"))
    })
}

/// The head this commit persists must not open a replay window past one epoch.
///
/// An exact window (`head - snap == slots_per_epoch`) stays `Complete`, so the
/// boundary snapshot and its retry can still be admitted.
fn refuse_if_head_crosses_window(
    engine: &Engine,
    slots_per_epoch: u64,
    head_slot: u64,
) -> Result<(), SeamError> {
    let Some(snap) = completed_slot(engine)? else {
        return Ok(());
    };
    let epoch_slots = slots_per_epoch.max(1);
    if head_slot > snap && head_slot - snap > epoch_slots {
        return Err(precondition(FailedPreconditionReason::StoreIncomplete));
    }
    Ok(())
}

fn da_status_of(verdict: DaVerdict) -> DaStatus {
    match verdict {
        DaVerdict::Available => DaStatus::Available,
        DaVerdict::Deferred => DaStatus::Deferred,
    }
}

fn snapshot_staging_key(slot: u64, index: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(SNAPSHOT_STAGING_PREFIX.len() + 12);
    key.extend_from_slice(SNAPSHOT_STAGING_PREFIX);
    key.extend_from_slice(&slot.to_be_bytes());
    key.extend_from_slice(&index.to_be_bytes());
    key
}

fn interrupted_snapshot() -> SeamError {
    SeamError::Unavailable(
        "snapshot interrupted before the completion marker; a partial ring entry is not newest"
            .into(),
    )
}

fn durable_head_slot(engine: &Engine) -> Result<u64, SeamError> {
    let rt = engine
        .read()
        .map_err(|e| SeamError::Unavailable(e.to_string()))?;
    let Some(bytes) = rt
        .get(TABLE_META, cc_store::meta::KEY_FC_SCALARS.as_bytes())
        .map_err(|e| SeamError::Unavailable(e.to_string()))?
    else {
        return Err(SeamError::Unavailable(
            "fc_scalars missing after Complete admit".into(),
        ));
    };
    ForkChoiceScalars::from_ssz_bytes(&bytes)
        .map(|scalars| scalars.head_slot.as_u64())
        .map_err(|err| SeamError::Unavailable(format!("ForkChoiceScalars decode failed: {err:?}")))
}

fn completed_marker(engine: &Engine) -> Result<Option<SnapshotCompletion>, SeamError> {
    let rt = engine
        .read()
        .map_err(|e| SeamError::Unavailable(e.to_string()))?;
    cc_store::load_snapshot_completion(&rt).map_err(|e| SeamError::Unavailable(e.to_string()))
}

fn completed_slot(engine: &Engine) -> Result<Option<u64>, SeamError> {
    Ok(completed_marker(engine)?.map(|marker| marker.slot.as_u64()))
}

fn canonical_state_root(engine: &Engine, slot: Slot) -> Result<Option<Root>, SeamError> {
    let rt = engine
        .read()
        .map_err(|e| SeamError::Unavailable(e.to_string()))?;
    let Some(root) =
        cc_store::get_canonical(&rt, slot).map_err(|e| SeamError::Unavailable(e.to_string()))?
    else {
        return Ok(None);
    };
    let Some(ssz) =
        get_block_by_root(&rt, &root).map_err(|e| SeamError::Unavailable(e.to_string()))?
    else {
        return Ok(None);
    };
    state_root_at_offset(&ssz)
        .map(Some)
        .map_err(|e| SeamError::InvalidArgument(e.to_string()))
}

impl ArchiveWriter {
    async fn submit_p2_chunk(&self, mut chunk: BackgroundChunk) -> Result<(), SeamError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        chunk.done = Some(tx);
        // ADR-R-08 exemption: drop-newest stays the P2 policy. Cite the
        // existing bound; do not introduce another overflow class.
        if !self.writer.try_submit_p2(chunk, &self.metrics) {
            return Err(SeamError::Unavailable(format!(
                "P2 drop-newest refused a snapshot chunk (ADR-R-08); bound WRITER_P2_BOUND={}; a dropped chunk is not a durable snapshot",
                crate::writer::WRITER_P2_BOUND
            )));
        }
        rx.await
            .map_err(|_| {
                SeamError::Unavailable(
                    "writer shut down before the snapshot chunk committed".into(),
                )
            })?
            .map_err(map_writer_err)
    }
}

/// `commit_import` and `set_head` are legal only on [`RestartState::Complete`].
///
/// `Uninitialized` and `Incomplete` are both
/// [`FailedPreconditionReason::StoreIncomplete`]. Head durability is a
/// separate check and is not relaxed here.
fn admit_import_store(engine: &Engine, slots_per_epoch: u64) -> Result<(), SeamError> {
    match classify_for_epoch(engine, slots_per_epoch)
        .map_err(|e| SeamError::Unavailable(e.to_string()))?
    {
        RestartState::Complete => Ok(()),
        RestartState::Uninitialized | RestartState::Incomplete(_) => {
            Err(precondition(FailedPreconditionReason::StoreIncomplete))
        }
    }
}

fn next_cursor(engine: &Engine, slot: Slot, root: Root) -> Result<WriteCursor, SeamError> {
    let prev = load_write_cursor(engine)
        .map_err(|e| SeamError::Unavailable(e.to_string()))?
        .ok_or_else(|| {
            SeamError::Unavailable("no durable write cursor; refuse to invent a zero cursor".into())
        })?;
    Ok(WriteCursor {
        session_id: prev.session_id,
        seq: prev.seq.saturating_add(1),
        slot,
        root,
    })
}

fn durable_body_slot(engine: &Engine, root: &Root) -> Result<Option<Slot>, SeamError> {
    let rt = engine
        .read()
        .map_err(|e| SeamError::Unavailable(e.to_string()))?;
    let Some(ssz) =
        get_block_by_root(&rt, root).map_err(|e| SeamError::Unavailable(e.to_string()))?
    else {
        return Ok(None);
    };
    slot_at_offset(&ssz)
        .map(Some)
        .map_err(|e| SeamError::InvalidArgument(e.to_string()))
}

/// One `commit_import` unit: body (idempotent if already durable), state root,
/// `da_status`, scalars, and the cursor. Canonical moves only when `head` names
/// this body.
fn bind_durable_import(
    engine: &Engine,
    slots_per_epoch: u64,
    import: DurableImport,
) -> Result<CommitUnit, SeamError> {
    let ssz_slot = slot_at_offset(import.ssz.as_ref())
        .map_err(|e| SeamError::InvalidArgument(e.to_string()))?;
    if ssz_slot.as_u64() != import.slot {
        return Err(SeamError::InvalidArgument(format!(
            "slot mismatch: caller {} != SSZ header slot {}",
            import.slot,
            ssz_slot.as_u64()
        )));
    }
    let ssz_parent = parent_root_at_offset(import.ssz.as_ref())
        .map_err(|e| SeamError::InvalidArgument(e.to_string()))?;
    let claimed_parent = Root::from_array(import.parent_root);
    if ssz_parent != claimed_parent {
        return Err(SeamError::InvalidArgument(format!(
            "parent_root mismatch: caller != SSZ header parent_root at offset {}",
            cc_store::PARENT_ROOT_SSZ_OFFSET
        )));
    }
    let ssz_state = state_root_at_offset(import.ssz.as_ref())
        .map_err(|e| SeamError::InvalidArgument(e.to_string()))?;
    if ssz_state != Root::from_array(import.state_root) {
        return Err(SeamError::InvalidArgument(
            "state_root mismatch: caller != SSZ header state_root".into(),
        ));
    }

    admit_import_store(engine, slots_per_epoch)?;

    match block_present(engine, &claimed_parent) {
        Ok(true) => {}
        Ok(false) => return Err(precondition(FailedPreconditionReason::ParentNotDurable)),
        Err(e) => return Err(SeamError::Unavailable(e.to_string())),
    }

    let update_canonical = match &import.head {
        Some(head) if head.head_root != import.block_root => {
            return Err(precondition(FailedPreconditionReason::HeadNotDurable));
        }
        Some(head) if head.head_slot != import.slot => {
            return Err(SeamError::InvalidArgument(
                "head_slot does not match the body this commit writes".into(),
            ));
        }
        Some(_) => true,
        None => false,
    };
    let persisted_head = decode_fork_choice_scalars(import.scalars.as_ref())?;
    refuse_if_head_crosses_window(engine, slots_per_epoch, persisted_head.head_slot.as_u64())?;

    let claimed_root = Root::from_array(import.block_root);
    let cursor = next_cursor(engine, ssz_slot, claimed_root)?;
    Ok(CommitUnit {
        blocks: vec![StagedBlock {
            slot: ssz_slot,
            root: claimed_root,
            ssz: import.ssz.to_vec(),
            update_canonical,
            write_state_root: true,
            da_status: Some(da_status_of(import.da)),
        }],
        columns: Vec::new(),
        fork_choice: Some(StagedForkChoiceScalars {
            ssz: import.scalars.to_vec(),
        }),
        canonical_from: None,
        cursor,
        done: None,
        anchor: None,
    })
}

/// `set_head` rewrites canonical from an already-durable root. It does not
/// insert the body. A non-complete store is `STORE_INCOMPLETE`; a missing
/// body on a complete store is `HEAD_NOT_DURABLE`.
fn bind_set_head(
    engine: &Engine,
    slots_per_epoch: u64,
    head: HeadChange,
    scalars: Bytes,
) -> Result<CommitUnit, SeamError> {
    admit_import_store(engine, slots_per_epoch)?;
    let root = Root::from_array(head.head_root);
    let Some(slot) = durable_body_slot(engine, &root)? else {
        return Err(precondition(FailedPreconditionReason::HeadNotDurable));
    };
    if slot.as_u64() != head.head_slot {
        return Err(SeamError::InvalidArgument(
            "head_slot does not match the durable body".into(),
        ));
    }
    let persisted_head = decode_fork_choice_scalars(scalars.as_ref())?;
    refuse_if_head_crosses_window(engine, slots_per_epoch, persisted_head.head_slot.as_u64())?;
    let cursor = next_cursor(engine, slot, root)?;
    Ok(CommitUnit {
        blocks: Vec::new(),
        columns: Vec::new(),
        fork_choice: Some(StagedForkChoiceScalars {
            ssz: scalars.to_vec(),
        }),
        canonical_from: Some(root),
        cursor,
        done: None,
        anchor: None,
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::metrics::{ClassLabels, StorageClass, StorageMetrics, WriterPriority};
    use crate::open::StorageRuntime;
    use crate::writer::{
        BackgroundChunk, WriterBounds, WriterFaults, WriterHandle, load_write_cursor, spawn_writer,
        store_is_uninitialized,
    };
    use cc_seam::{Bytes, DaVerdict, FailedPreconditionReason, SeamError, TrustedAnchor};
    use cc_seam::{DurableImport, HeadCause, HeadChange};
    use cc_store::canonical::put_canonical;
    use cc_store::keys::{
        block_shard_id, blocks_shard_table, encode_cold_block_key, encode_hot_block_key,
        encode_root_key,
    };
    use cc_store::{TABLE_BLOCK_SLOT_BY_ROOT, TABLE_BLOCKS_HOT, measure_class_stats};
    use std::sync::atomic::{AtomicBool, Ordering};

    use cc_store::blocks::{
        MIN_BLOCK_SSZ_LEN, PARENT_ROOT_SSZ_OFFSET, SLOT_SSZ_OFFSET, STATE_ROOT_SSZ_OFFSET,
        get_state_root, slot_by_root,
    };
    use cc_store::canonical::get_canonical;
    use cc_store::columns::{
        COLUMN_HEADER_PARENT_ROOT_SSZ_OFFSET, COLUMN_HEADER_SLOT_SSZ_OFFSET,
        COLUMN_INDEX_SSZ_OFFSET, DATA_COLUMN_SIDECAR_FIXED_BYTES, get_column_by_root,
        get_da_status,
    };
    use cc_store::engine::{Durability, EngineOptions};
    use cc_store::keys::BlockRegion;
    use cc_store::meta::{
        AnchorInfo, ForkChoiceScalars, KEY_ANCHOR_INFO, KEY_FC_SCALARS, KEY_NODE_ID,
        KEY_SNAPSHOT_COMPLETION, KEY_SPLIT, SnapshotCompletion, TABLE_META, WriteCursor,
    };
    use cc_store::{
        SszDecode, completed_snapshot, get_snapshot, list_snapshot_slots, load_split,
        newest_snapshot,
    };
    use cc_store::{get_block_by_root, put_block};
    use cc_types::{Checkpoint, Epoch};
    use prometheus_client::registry::Registry;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::sync::watch;

    fn tmp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("cc-archive-write-{label}-{nanos}"));
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

    fn synth_sidecar(index: u16, slot: u64) -> Vec<u8> {
        synth_sidecar_with_parent(index, slot, &parent_root())
    }

    fn synth_sidecar_with_parent(index: u16, slot: u64, parent: &Root) -> Vec<u8> {
        let mut v = vec![0u8; DATA_COLUMN_SIDECAR_FIXED_BYTES];
        v[COLUMN_INDEX_SSZ_OFFSET..COLUMN_INDEX_SSZ_OFFSET + 8]
            .copy_from_slice(&u64::from(index).to_le_bytes());
        v[COLUMN_HEADER_SLOT_SSZ_OFFSET..COLUMN_HEADER_SLOT_SSZ_OFFSET + 8]
            .copy_from_slice(&slot.to_le_bytes());
        v[COLUMN_HEADER_PARENT_ROOT_SSZ_OFFSET..COLUMN_HEADER_PARENT_ROOT_SSZ_OFFSET + 32]
            .copy_from_slice(parent.as_slice());
        v
    }

    fn parent_root() -> Root {
        Root::from_array([0x11; 32])
    }

    fn synth_block(slot: u64, parent: &Root, state: &Root) -> Vec<u8> {
        let mut v = vec![0u8; MIN_BLOCK_SSZ_LEN];
        v[0..4].copy_from_slice(&100u32.to_le_bytes());
        v[SLOT_SSZ_OFFSET..SLOT_SSZ_OFFSET + 8].copy_from_slice(&slot.to_le_bytes());
        v[PARENT_ROOT_SSZ_OFFSET..PARENT_ROOT_SSZ_OFFSET + 32].copy_from_slice(parent.as_slice());
        v[STATE_ROOT_SSZ_OFFSET..STATE_ROOT_SSZ_OFFSET + 32].copy_from_slice(state.as_slice());
        v
    }

    fn batch(index: u64, slot: u64, ssz: Vec<u8>) -> ColumnBatch {
        ColumnBatch {
            parent_root: parent_root().into_array(),
            slot,
            block_root: [0xAB; 32],
            index,
            ssz: Bytes::from(ssz),
        }
    }

    fn staged_parent_block(root: Root, slot: u64) -> StagedBlock {
        StagedBlock {
            slot: Slot::new(slot),
            root,
            ssz: synth_block(slot, &Root::ZERO, &Root::from_array([0xF0; 32])),
            update_canonical: false,
            write_state_root: false,
            da_status: None,
        }
    }

    async fn seed_cursor_and_parent(handle: &WriterHandle) {
        let parent = parent_root();
        handle
            .submit_p0_committed(CommitUnit {
                blocks: vec![staged_parent_block(parent, 19)],
                columns: vec![],
                fork_choice: None,
                canonical_from: None,
                anchor: None,
                cursor: WriteCursor {
                    session_id: 7,
                    seq: 11,
                    slot: Slot::new(19),
                    root: parent,
                },
                done: None,
            })
            .await
            .unwrap();
    }

    fn seed_parent_direct(engine: &Engine) {
        let parent = parent_root();
        let ssz = synth_block(19, &Root::ZERO, &Root::from_array([0xF0; 32]));
        let rt = engine.read().unwrap();
        let mut batch = engine.batch();
        put_block(
            &rt,
            &mut batch,
            Slot::new(19),
            &parent,
            &ssz,
            BlockRegion::Hot,
            false,
        )
        .unwrap();
        drop(rt);
        engine.commit(batch).unwrap();
    }

    #[test]
    fn staged_uses_batch_index_not_ssz_guess() {
        let ssz = synth_sidecar(7, 11);
        let col = staged_from_batch(batch(7, 11, ssz.clone())).unwrap();
        assert_eq!(col.index, 7);
        assert_eq!(col.slot, Slot::new(11));
        assert_eq!(col.ssz, ssz);
    }

    #[test]
    fn staged_rejects_parent_root_ssz_mismatch() {
        let ssz = synth_sidecar_with_parent(7, 11, &Root::from_array([0x22; 32]));
        let err = staged_from_batch(batch(7, 11, ssz)).unwrap_err();
        assert!(matches!(err, SeamError::InvalidArgument(_)));
        assert!(err.to_string().contains("parent_root mismatch"), "{err}");
    }

    #[test]
    fn short_sidecar_is_rejected() {
        let err = staged_from_batch(batch(0, 1, vec![0u8; 2])).unwrap_err();
        assert!(matches!(err, SeamError::InvalidArgument(_)));
    }

    #[test]
    fn index_over_u16_is_rejected() {
        let ssz = synth_sidecar(0, 1);
        let err = staged_from_batch(batch(u64::from(u16::MAX) + 1, 1, ssz)).unwrap_err();
        assert!(matches!(err, SeamError::InvalidArgument(_)));
    }

    #[tokio::test]
    async fn ingest_persists_typed_index() {
        let (dir, engine) = eng("persist");
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = spawn_writer(
            Arc::clone(&engine),
            metrics(),
            WriterBounds::default(),
            WriterFaults::default(),
            shutdown_rx,
            false,
        );
        let archive = ArchiveWriter::new(handle.clone(), Arc::clone(&engine));
        seed_cursor_and_parent(&handle).await;
        let ssz = synth_sidecar(5, 20);
        archive
            .ingest_columns(batch(5, 20, ssz.clone()))
            .await
            .unwrap();

        let rt = engine.read().unwrap();
        let got = get_column_by_root(
            &rt,
            &Root::from_array([0xAB; 32]),
            5,
            Some(BlockRegion::Hot),
        )
        .unwrap()
        .unwrap();
        assert_eq!(got, ssz);
        let cursor = load_write_cursor(&engine).unwrap().unwrap();
        assert_eq!(cursor.session_id, 7);
        assert_eq!(cursor.seq, 11, "ingest must not rewind KEY_WRITE_CURSOR");
        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn ingest_rejects_index_ssz_mismatch() {
        let (dir, engine) = eng("mismatch");
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = spawn_writer(
            Arc::clone(&engine),
            metrics(),
            WriterBounds::default(),
            WriterFaults::default(),
            shutdown_rx,
            false,
        );
        let archive = ArchiveWriter::new(handle.clone(), Arc::clone(&engine));
        seed_cursor_and_parent(&handle).await;
        // Caller names index 3; SSZ body is column 1 — reject, do not store as 0.
        let err = archive
            .ingest_columns(batch(3, 20, synth_sidecar(1, 20)))
            .await
            .unwrap_err();
        assert!(matches!(err, SeamError::InvalidArgument(_)));
        let msg = err.to_string();
        assert!(
            msg.contains("index mismatch"),
            "must reach put_column index bind, not frontier admit: {msg}"
        );
        assert!(
            !msg.contains("durable frontier"),
            "must reach put_column index bind, not frontier admit: {msg}"
        );
        let rt = engine.read().unwrap();
        assert!(
            get_column_by_root(
                &rt,
                &Root::from_array([0xAB; 32]),
                0,
                Some(BlockRegion::Hot)
            )
            .unwrap()
            .is_none()
        );
        assert!(
            get_column_by_root(
                &rt,
                &Root::from_array([0xAB; 32]),
                3,
                Some(BlockRegion::Hot)
            )
            .unwrap()
            .is_none()
        );
        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn ingest_without_durable_cursor_is_rejected() {
        let (dir, engine) = eng("no-cursor");
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = spawn_writer(
            Arc::clone(&engine),
            metrics(),
            WriterBounds::default(),
            WriterFaults::default(),
            shutdown_rx,
            false,
        );
        let archive = ArchiveWriter::new(handle, Arc::clone(&engine));
        seed_parent_direct(&engine);
        let err = archive
            .ingest_columns(batch(5, 20, synth_sidecar(5, 20)))
            .await
            .unwrap_err();
        assert!(matches!(err, SeamError::Unavailable(_)));
        assert!(load_write_cursor(&engine).unwrap().is_none());
        let rt = engine.read().unwrap();
        assert!(
            get_column_by_root(
                &rt,
                &Root::from_array([0xAB; 32]),
                5,
                Some(BlockRegion::Hot)
            )
            .unwrap()
            .is_none()
        );
        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn first_row_of_same_batch_is_accepted_without_durable_parent() {
        let (_dir, engine) = eng("first-row");
        let parent = parent_root();
        let batch = WriterBatch {
            head: Some(ContinuityHead {
                parent_root: parent,
                slot: Slot::new(19),
            }),
            blocks: vec![staged_parent_block(parent, 19)],
            columns: vec![],
        };
        assert!(admit_top_of_batch_continuity(&engine, &batch).is_ok());
        assert!(!block_present(&engine, &parent).unwrap());
        let _ = std::fs::remove_dir_all(&_dir);
    }

    // Named identically to S0-B-10 so the pair is greppable.
    #[tokio::test]
    async fn put_backfill_batch_empty_progress_single_block_batch_is_rejected() {
        let (dir, engine) = eng("empty-progress");
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = spawn_writer(
            Arc::clone(&engine),
            metrics(),
            WriterBounds::default(),
            WriterFaults::default(),
            shutdown_rx,
            false,
        );
        let archive = ArchiveWriter::new(handle.clone(), Arc::clone(&engine));
        handle
            .submit_p0_committed(CommitUnit::cursor_only(WriteCursor {
                session_id: 1,
                seq: 1,
                slot: Slot::new(0),
                root: Root::ZERO,
            }))
            .await
            .unwrap();
        let root = Root::from_array([0x42; 32]);
        let err = archive
            .submit_writer_batch(WriterBatch {
                head: None,
                blocks: vec![staged_parent_block(root, 7)],
                columns: vec![],
            })
            .await
            .unwrap_err();
        assert!(
            matches!(err, SeamError::InvalidArgument(_)),
            "empty-progress single-block batch must be rejected, not fast-pathed: {err}"
        );
        let msg = err.to_string();
        assert!(
            msg.contains("progress is required") || msg.contains("empty-progress"),
            "empty-progress single-block batch must be rejected, not fast-pathed: {msg}"
        );
        let rt = engine.read().unwrap();
        assert!(
            get_block_by_root(&rt, &root).unwrap().is_none(),
            "empty-progress must not persist the block"
        );

        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // Named identically to S0-B-10 so the pair is greppable.
    #[tokio::test]
    async fn put_backfill_batch_parent_not_durable_and_not_first_row_is_rejected() {
        let (dir, engine) = eng("frontier-jump");
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = spawn_writer(
            Arc::clone(&engine),
            metrics(),
            WriterBounds::default(),
            WriterFaults::default(),
            shutdown_rx,
            false,
        );
        let archive = ArchiveWriter::new(handle.clone(), Arc::clone(&engine));
        handle
            .submit_p0_committed(CommitUnit::cursor_only(WriteCursor {
                session_id: 1,
                seq: 1,
                slot: Slot::new(1),
                root: Root::from_array([0xEE; 32]),
            }))
            .await
            .unwrap();
        // Parent 0xFF is not durable; the batch has no first block row.
        let jumped = Root::from_array([0xFF; 32]);
        let err = archive
            .ingest_columns(ColumnBatch {
                parent_root: jumped.into_array(),
                slot: 9,
                block_root: [0x33; 32],
                index: 5,
                ssz: Bytes::from(synth_sidecar_with_parent(5, 9, &jumped)),
            })
            .await
            .unwrap_err();
        assert!(
            matches!(err, SeamError::InvalidArgument(_)),
            "must state the S0-B-10 / S2-A-06 invariant: {err}"
        );
        let msg = err.to_string();
        assert!(
            msg.contains("durable frontier") && msg.contains("never jump"),
            "must state the S0-B-10 / S2-A-06 invariant: {msg}"
        );
        assert!(
            msg.contains("not durable") && msg.contains("not the first row"),
            "{msg}"
        );
        let rt = engine.read().unwrap();
        assert!(
            get_column_by_root(
                &rt,
                &Root::from_array([0x33; 32]),
                5,
                Some(BlockRegion::Hot)
            )
            .unwrap()
            .is_none()
        );

        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn block_archive(label: &str) -> (PathBuf, Arc<Engine>, ArchiveWriter, watch::Sender<bool>) {
        let (dir, engine) = eng(label);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = spawn_writer(
            Arc::clone(&engine),
            metrics(),
            WriterBounds::default(),
            WriterFaults::default(),
            shutdown_rx,
            false,
        );
        ArchiveWriter::ensure_write_cursor(&engine).unwrap();
        let archive = ArchiveWriter::new(handle, Arc::clone(&engine));
        (dir, engine, archive, shutdown_tx)
    }

    fn set_durable_head(engine: &Engine, head: &Root, slot: u64) {
        let mut batch = engine.batch();
        batch.put(
            TABLE_META,
            KEY_FC_SCALARS.as_bytes(),
            &scalars_ssz(head, slot),
        );
        engine.commit(batch).unwrap();
    }

    async fn anchor_at_zero(archive: &ArchiveWriter) -> (Root, Root) {
        let parent = Root::from_array([0x11; 32]);
        let root = Root::from_array([0x42; 32]);
        let state = Root::from_array([0xF0; 32]);
        archive
            .commit_anchor(trusted_anchor(
                &root,
                &parent,
                &state,
                0,
                synth_block(0, &parent, &state),
                b"anchor-state".to_vec(),
            ))
            .await
            .unwrap();
        (root, state)
    }

    fn snap(slot: u64, state: &Root, bytes: &[u8]) -> cc_seam::Snapshot {
        cc_seam::Snapshot {
            slot,
            state_root: state.into_array(),
            state_ssz: Bytes::from(bytes.to_vec()),
        }
    }

    /// Chunked P2 snapshot: the marker and `newest_snapshot` move together,
    /// and only on the final chunk.
    #[tokio::test]
    async fn commit_snapshot_advances_newest_only_with_the_marker() {
        let (dir, engine, archive, shutdown_tx) = block_archive("snap-chunks");
        let archive = archive.with_snapshot_chunk_bytes(4).with_snapshot_ring(4);
        let (root, state) = anchor_at_zero(&archive).await;
        set_durable_head(&engine, &root, 32);
        let payload = b"0123456789abcdef";
        archive
            .commit_snapshot(snap(32, &state, payload))
            .await
            .unwrap();

        let rt = engine.read().unwrap();
        let (newest_slot, newest_bytes) = newest_snapshot(&rt).unwrap().unwrap();
        assert_eq!(newest_slot.as_u64(), 32);
        assert_eq!(newest_bytes, payload);
        let (marker, completed_bytes) = completed_snapshot(&rt).unwrap().unwrap();
        assert_eq!(marker.slot.as_u64(), 32);
        assert_eq!(marker.bytes, payload.len() as u64);
        assert_eq!(completed_bytes, payload);
        assert!(
            rt.get(TABLE_META, &snapshot_staging_key(32, 0))
                .unwrap()
                .is_none(),
            "final chunk deletes staging keys"
        );
        drop(rt);
        StorageRuntime::enforce_snapshot_ring_on(&engine, 4).unwrap();

        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A chunk that commits before the marker leaves the previous ring entry newest.
    #[tokio::test]
    async fn interrupted_snapshot_keeps_the_previous_ring_entry() {
        let (dir, engine, archive, shutdown_tx) = block_archive("snap-interrupt");
        let archive = archive
            .with_snapshot_chunk_bytes(4)
            .with_snapshot_interrupt_after(1)
            .with_snapshot_ring(4);
        let (root, state) = anchor_at_zero(&archive).await;
        set_durable_head(&engine, &root, 32);
        let err = archive
            .commit_snapshot(snap(32, &state, b"0123456789"))
            .await
            .unwrap_err();
        assert!(
            matches!(err, SeamError::Unavailable(_)),
            "interrupt is not a durable snapshot: {err}"
        );

        let rt = engine.read().unwrap();
        let (newest_slot, _) = newest_snapshot(&rt).unwrap().unwrap();
        assert_eq!(newest_slot.as_u64(), 0, "partial entry must not be newest");
        let (marker, _) = completed_snapshot(&rt).unwrap().unwrap();
        assert_eq!(marker.slot.as_u64(), 0);
        assert!(get_snapshot(&rt, Slot::new(32)).unwrap().is_none());
        assert!(
            rt.get(TABLE_META, &snapshot_staging_key(32, 0))
                .unwrap()
                .is_some(),
            "the staging chunk committed"
        );
        drop(rt);

        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Ring trim runs in the final chunk. `I-ring` then accepts depth and rejects empty.
    #[tokio::test]
    async fn snapshot_ring_trims_on_commit_and_runtime_enforces_ring() {
        let (dir, engine, archive, shutdown_tx) = block_archive("snap-ring");
        let archive = archive.with_snapshot_ring(2).with_snapshot_chunk_bytes(8);
        let (root, state) = anchor_at_zero(&archive).await;
        set_durable_head(&engine, &root, 32);
        archive
            .commit_snapshot(snap(32, &state, b"epoch-1"))
            .await
            .unwrap();
        set_durable_head(&engine, &root, 64);
        archive
            .commit_snapshot(snap(64, &state, b"epoch-2"))
            .await
            .unwrap();

        let rt = engine.read().unwrap();
        let slots = list_snapshot_slots(&rt).unwrap();
        assert_eq!(
            slots.iter().map(|s| s.as_u64()).collect::<Vec<_>>(),
            vec![32, 64],
            "ring 2 drops the anchor snapshot"
        );
        drop(rt);
        StorageRuntime::enforce_snapshot_ring_on(&engine, 2).unwrap();

        {
            let mut batch = engine.batch();
            for slot in [32u64, 64] {
                batch.delete(
                    cc_store::TABLE_SNAPSHOTS,
                    &cc_store::encode_snapshot_key(Slot::new(slot)),
                );
            }
            engine.commit(batch).unwrap();
        }
        let err = StorageRuntime::enforce_snapshot_ring_on(&engine, 2).unwrap_err();
        assert!(
            err.to_string().contains("ring"),
            "empty ring is I-ring: {err}"
        );

        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// P2 drop-newest does not advance the completion marker (ADR-R-08).
    #[tokio::test]
    async fn dropped_p2_chunk_does_not_advance_newest_snapshot() {
        let (dir, engine, archive, shutdown_tx) = block_archive("snap-drop");
        let (root, state) = anchor_at_zero(&archive).await;
        set_durable_head(&engine, &root, 32);
        let mut done = archive.writer.writer_done();
        let _ = shutdown_tx.send(true);
        tokio::time::timeout(std::time::Duration::from_secs(2), done.changed())
            .await
            .expect("writer should stop")
            .unwrap();

        let metrics = metrics();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = spawn_writer(
            Arc::clone(&engine),
            metrics.clone(),
            WriterBounds {
                p2: 1,
                ..WriterBounds::default()
            },
            WriterFaults {
                start_paused: true,
                ..WriterFaults::default()
            },
            shutdown_rx,
            false,
        );
        let archive = ArchiveWriter::new(handle, Arc::clone(&engine)).with_metrics(metrics.clone());
        let queued = archive.writer.try_submit_p2(
            BackgroundChunk {
                class: StorageClass::Snapshots,
                puts: vec![(TABLE_META.to_owned(), b"filler".to_vec(), b"x".to_vec())],
                deletes: Vec::new(),
                done: None,
            },
            &metrics,
        );
        assert!(
            queued,
            "bound 1 accepts the filler chunk while the writer is paused"
        );
        let before = metrics
            .writer_chunk_dropped
            .get_or_create(&ClassLabels {
                class: WriterPriority::P2.as_str().to_owned(),
            })
            .get();
        let err = archive
            .commit_snapshot(snap(32, &state, b"dropped"))
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            matches!(err, SeamError::Unavailable(_)),
            "drop-newest is unavailable, not backpressure: {msg}"
        );
        assert!(
            msg.contains("ADR-R-08") && msg.contains("drop-newest"),
            "{msg}"
        );
        assert!(
            msg.contains(&crate::writer::WRITER_P2_BOUND.to_string()),
            "the error cites the existing bound: {msg}"
        );
        let after = metrics
            .writer_chunk_dropped
            .get_or_create(&ClassLabels {
                class: WriterPriority::P2.as_str().to_owned(),
            })
            .get();
        assert!(after > before, "drop metric is class p2");
        let rt = engine.read().unwrap();
        let (marker, _) = completed_snapshot(&rt).unwrap().unwrap();
        assert_eq!(marker.slot.as_u64(), 0);
        let (newest, _) = newest_snapshot(&rt).unwrap().unwrap();
        assert_eq!(newest.as_u64(), 0);
        drop(rt);

        archive.writer.unpause_loop();
        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn commit_snapshot_refuses_a_non_boundary_and_an_incomplete_store() {
        let (dir, engine, archive, shutdown_tx) = block_archive("snap-refuse");
        let err = archive
            .commit_snapshot(snap(32, &Root::ZERO, b"no-anchor"))
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                SeamError::FailedPrecondition {
                    reason: FailedPreconditionReason::StoreIncomplete,
                }
            ),
            "{err}"
        );

        let (root, state) = anchor_at_zero(&archive).await;
        set_durable_head(&engine, &root, 31);
        let err = archive
            .commit_snapshot(snap(31, &state, b"mid-epoch"))
            .await
            .unwrap_err();
        assert!(matches!(err, SeamError::InvalidArgument(_)), "{err}");
        assert!(err.to_string().contains("epoch boundary"), "{err}");

        set_durable_head(&engine, &root, 0);
        let err = archive
            .commit_snapshot(snap(32, &state, b"ahead"))
            .await
            .unwrap_err();
        assert!(matches!(err, SeamError::InvalidArgument(_)), "{err}");
        assert!(err.to_string().contains("ahead of durable head"), "{err}");

        let rt = engine.read().unwrap();
        let (marker, _) = completed_snapshot(&rt).unwrap().unwrap();
        assert_eq!(marker.slot.as_u64(), 0);
        drop(rt);

        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Same slot is idempotent only when the state root and length match.
    #[tokio::test]
    async fn same_slot_snapshot_state_root_is_refused() {
        let (dir, engine, archive, shutdown_tx) = block_archive("snap-same-slot");
        let (block, state) = anchor_at_zero(&archive).await;
        set_durable_head(&engine, &block, 32);
        archive
            .commit_snapshot(snap(32, &state, b"winner"))
            .await
            .unwrap();

        let other = Root::from_array([0xAB; 32]);
        let err = archive
            .commit_snapshot(snap(32, &other, b"loser!!"))
            .await
            .unwrap_err();
        assert!(
            matches!(err, SeamError::InvalidArgument(_)),
            "a different state_root at the marker slot is refused: {err}"
        );

        let err = archive
            .commit_snapshot(snap(32, &state, b"winner!"))
            .await
            .unwrap_err();
        assert!(
            matches!(err, SeamError::InvalidArgument(_)),
            "a different length at the marker slot is refused: {err}"
        );

        archive
            .commit_snapshot(snap(32, &state, b"winner"))
            .await
            .expect("matching root and length is idempotent");

        let rt = engine.read().unwrap();
        let (marker, bytes) = completed_snapshot(&rt).unwrap().unwrap();
        assert_eq!(marker.state_root, state);
        assert_eq!(bytes, b"winner");
        drop(rt);

        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The next head past one epoch is refused while the exact window stays open.
    #[tokio::test]
    async fn import_past_snapshot_window_is_refused() {
        let (dir, engine, archive, shutdown_tx) = block_archive("snap-window");
        let (anchor, state) = anchor_at_zero(&archive).await;
        let root32 = Root::from_array([0x32; 32]);
        archive
            .commit_import(durable_import(
                32,
                &anchor,
                &root32,
                &state,
                DaVerdict::Available,
                &scalars_ssz(&root32, 32),
                true,
            ))
            .await
            .expect("exact one-epoch head stays Complete");

        let root33 = Root::from_array([0x33; 32]);
        let err = archive
            .commit_import(durable_import(
                33,
                &anchor,
                &root33,
                &state,
                DaVerdict::Available,
                &scalars_ssz(&root33, 33),
                true,
            ))
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                SeamError::FailedPrecondition {
                    reason: FailedPreconditionReason::StoreIncomplete,
                }
            ),
            "post-commit head past one epoch is refused: {err}"
        );
        assert!(!archive.block_is_durable(root33.into_array()).unwrap());

        archive
            .commit_import(durable_import(
                33,
                &anchor,
                &root33,
                &state,
                DaVerdict::Available,
                &scalars_ssz(&root32, 32),
                false,
            ))
            .await
            .expect("a body that does not advance the head stays inside the window");
        let err = archive
            .set_head(
                HeadChange {
                    head_root: root33.into_array(),
                    head_slot: 33,
                    cause: HeadCause::Import,
                },
                Bytes::from(scalars_ssz(&root33, 33)),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                SeamError::FailedPrecondition {
                    reason: FailedPreconditionReason::StoreIncomplete,
                }
            ),
            "set_head past one epoch is refused: {err}"
        );

        archive
            .commit_snapshot(snap(32, &state, b"boundary"))
            .await
            .expect("the boundary snapshot is still admitted at exactly one epoch");
        let rt = engine.read().unwrap();
        let (marker, _) = completed_snapshot(&rt).unwrap().unwrap();
        assert_eq!(marker.slot.as_u64(), 32);
        drop(rt);

        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The snapshot chunk must not replace the block root on the write cursor.
    #[tokio::test]
    async fn snapshot_cursor_root_stays_the_block_root() {
        let (dir, engine, archive, shutdown_tx) = block_archive("snap-cursor");
        let (block, _state) = anchor_at_zero(&archive).await;
        let before = load_write_cursor(&engine).unwrap().unwrap();
        assert_eq!(before.root, block);
        set_durable_head(&engine, &block, 32);
        let state_root = Root::from_array([0xAB; 32]);
        archive
            .commit_snapshot(snap(32, &state_root, b"state-bytes"))
            .await
            .unwrap();
        let after = load_write_cursor(&engine).unwrap().unwrap();
        assert_eq!(
            after.root, block,
            "durable cursor root stays the block root from the boundary commit"
        );
        assert_ne!(after.root, state_root);
        assert_eq!(
            after.seq, before.seq,
            "a snapshot must not publish a cursor sampled outside the writer onto P2"
        );

        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `set_head` at the marker slot moves the marker onto that block's state.
    #[tokio::test]
    async fn set_head_realigns_snapshot_at_the_marker_slot() {
        let (dir, engine, archive, shutdown_tx) = block_archive("snap-realign");
        let (anchor, state_a) = anchor_at_zero(&archive).await;
        let root_a = Root::from_array([0x32; 32]);
        archive
            .commit_import(durable_import(
                32,
                &anchor,
                &root_a,
                &state_a,
                DaVerdict::Available,
                &scalars_ssz(&root_a, 32),
                true,
            ))
            .await
            .unwrap();
        archive
            .commit_snapshot(snap(32, &state_a, b"winner"))
            .await
            .unwrap();

        let state_b = Root::from_array([0xB2; 32]);
        let root_b = Root::from_array([0x33; 32]);
        archive
            .commit_import(durable_import(
                32,
                &anchor,
                &root_b,
                &state_b,
                DaVerdict::Available,
                &scalars_ssz(&root_a, 32),
                false,
            ))
            .await
            .unwrap();

        let err = archive
            .realign_head_snapshot(snap(32, &state_b, b"replaced"))
            .await
            .unwrap_err();
        assert!(
            matches!(err, SeamError::InvalidArgument(_)),
            "a non-canonical state must not move the marker: {err}"
        );
        {
            let rt = engine.read().unwrap();
            let (marker, bytes) = completed_snapshot(&rt).unwrap().unwrap();
            assert_eq!(marker.state_root, state_a);
            assert_eq!(bytes, b"winner");
        }

        archive
            .set_head(
                HeadChange {
                    head_root: root_b.into_array(),
                    head_slot: 32,
                    cause: HeadCause::Import,
                },
                Bytes::from(scalars_ssz(&root_b, 32)),
            )
            .await
            .unwrap();
        let cursor_after_head = load_write_cursor(&engine).unwrap().unwrap();
        archive
            .realign_head_snapshot(snap(32, &state_b, b"replaced"))
            .await
            .unwrap();

        let rt = engine.read().unwrap();
        let (marker, bytes) = completed_snapshot(&rt).unwrap().unwrap();
        assert_eq!(marker.slot.as_u64(), 32);
        assert_eq!(marker.state_root, state_b);
        assert_eq!(bytes, b"replaced");
        let (newest, newest_bytes) = newest_snapshot(&rt).unwrap().unwrap();
        assert_eq!(newest.as_u64(), 32);
        assert_eq!(newest_bytes, b"replaced");
        drop(rt);
        let cursor = load_write_cursor(&engine).unwrap().unwrap();
        assert_eq!(cursor.root, root_b);
        assert_eq!(cursor.seq, cursor_after_head.seq);

        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn commit_import_rejects_slot_mismatch() {
        let (dir, _engine, archive, shutdown_tx) = block_archive("slot-bind");
        let root = Root::from_array([0x42; 32]);
        let state = Root::from_array([0xF0; 32]);
        let mut import = durable_import(
            1,
            &Root::ZERO,
            &root,
            &state,
            DaVerdict::Available,
            b"scalars",
            false,
        );
        import.slot = 9;
        let err = archive.commit_import(import).await.unwrap_err();
        assert!(matches!(err, SeamError::InvalidArgument(_)));
        assert!(err.to_string().contains("slot mismatch"), "{err}");
        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn commit_import_rejects_parent_mismatch() {
        let (dir, _engine, archive, shutdown_tx) = block_archive("parent-bind");
        let state = Root::from_array([0xF0; 32]);
        let mut import = durable_import(
            1,
            &Root::ZERO,
            &Root::from_array([0xCD; 32]),
            &state,
            DaVerdict::Available,
            b"scalars",
            false,
        );
        import.parent_root = [0xAB; 32];
        let err = archive.commit_import(import).await.unwrap_err();
        assert!(matches!(err, SeamError::InvalidArgument(_)));
        assert!(err.to_string().contains("parent_root mismatch"), "{err}");
        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn commit_import_refuses_self_parent_that_is_not_durable() {
        let (dir, engine, archive, shutdown_tx) = block_archive("self-parent");
        let g_root = Root::from_array([0x01; 32]);
        let state = Root::from_array([0xF0; 32]);
        let g_ssz = synth_block(0, &Root::ZERO, &state);
        archive
            .commit_anchor(trusted_anchor(
                &g_root,
                &Root::ZERO,
                &state,
                0,
                g_ssz,
                b"genesis-state".to_vec(),
            ))
            .await
            .unwrap();
        assert!(archive.block_is_durable(g_root.into_array()).unwrap());

        let fake_root = Root::from_array([0x02; 32]);
        let err = archive
            .commit_import(durable_import(
                3,
                &fake_root,
                &fake_root,
                &Root::from_array([0xF1; 32]),
                DaVerdict::Available,
                b"scalars",
                false,
            ))
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                SeamError::FailedPrecondition {
                    reason: FailedPreconditionReason::ParentNotDurable,
                }
            ),
            "{err}"
        );
        let rt = engine.read().unwrap();
        assert!(get_block_by_root(&rt, &fake_root).unwrap().is_none());
        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn reimport_ancestor_keeps_durable_head() {
        let (dir, engine, archive, shutdown_tx) = block_archive("h3-no-rewind");
        let g_root = Root::from_array([0x01; 32]);
        let a_root = Root::from_array([0x02; 32]);
        let b_root = Root::from_array([0x03; 32]);
        let state = Root::from_array([0xF0; 32]);
        let g_ssz = synth_block(0, &Root::ZERO, &state);
        archive
            .commit_anchor(trusted_anchor(
                &g_root,
                &Root::ZERO,
                &state,
                0,
                g_ssz,
                b"genesis-state".to_vec(),
            ))
            .await
            .unwrap();
        let a_ssz = synth_block(1, &g_root, &state);
        archive
            .commit_import(durable_import(
                1,
                &g_root,
                &a_root,
                &state,
                DaVerdict::Available,
                &scalars_ssz(&a_root, 1),
                true,
            ))
            .await
            .unwrap();
        let b_ssz = synth_block(2, &a_root, &state);
        archive
            .commit_import(durable_import(
                2,
                &a_root,
                &b_root,
                &state,
                DaVerdict::Available,
                &scalars_ssz(&b_root, 2),
                true,
            ))
            .await
            .unwrap();

        archive
            .commit_import(durable_import(
                1,
                &g_root,
                &a_root,
                &state,
                DaVerdict::Available,
                &scalars_ssz(&a_root, 1),
                false,
            ))
            .await
            .expect("re-import of durable A must not fail");

        let rt = engine.read().unwrap();
        assert_eq!(get_canonical(&rt, Slot::new(1)).unwrap(), Some(a_root));
        assert_eq!(
            get_canonical(&rt, Slot::new(2)).unwrap(),
            Some(b_root),
            "re-import of A must not rewind durable head off B"
        );
        assert_eq!(
            get_block_by_root(&rt, &a_root).unwrap().as_deref(),
            Some(a_ssz.as_slice())
        );
        assert_eq!(
            get_block_by_root(&rt, &b_root).unwrap().as_deref(),
            Some(b_ssz.as_slice())
        );
        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn commit_import_persists_caller_block_root_without_decode() {
        let (dir, engine, archive, shutdown_tx) = block_archive("caller-root");
        let anchor_root = Root::from_array([0x41; 32]);
        let state = Root::from_array([0xF0; 32]);
        let anchor_ssz = synth_block(0, &Root::ZERO, &state);
        archive
            .commit_anchor(trusted_anchor(
                &anchor_root,
                &Root::ZERO,
                &state,
                0,
                anchor_ssz.clone(),
                b"genesis-state".to_vec(),
            ))
            .await
            .unwrap();
        let root = Root::from_array([0x42; 32]);
        let ssz = synth_block(1, &anchor_root, &state);
        archive
            .commit_import(durable_import(
                1,
                &anchor_root,
                &root,
                &state,
                DaVerdict::Available,
                &scalars_ssz(&root, 1),
                true,
            ))
            .await
            .expect("caller block_root is the bind; storage must not decode the body");
        let rt = engine.read().unwrap();
        assert_eq!(
            get_block_by_root(&rt, &anchor_root).unwrap().as_deref(),
            Some(anchor_ssz.as_slice()),
            "anchor key must be the caller's block_root"
        );
        assert_eq!(
            get_block_by_root(&rt, &root).unwrap().as_deref(),
            Some(ssz.as_slice()),
            "stored key must be the caller's block_root"
        );
        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn scalars_ssz(head: &Root, slot: u64) -> Vec<u8> {
        scalars_ssz_time(head, slot, slot)
    }

    fn scalars_ssz_time(head: &Root, slot: u64, time: u64) -> Vec<u8> {
        let checkpoint = Checkpoint {
            epoch: Epoch::new(slot / 32),
            root: *head,
        };
        ForkChoiceScalars {
            time,
            proposer_boost_root: Root::ZERO,
            justified: checkpoint,
            finalized: checkpoint,
            unrealized_justified: checkpoint,
            unrealized_finalized: checkpoint,
            head_root: *head,
            head_slot: Slot::new(slot),
        }
        .as_ssz_bytes()
    }

    fn trusted_anchor(
        root: &Root,
        parent: &Root,
        state: &Root,
        slot: u64,
        block: Vec<u8>,
        state_ssz: Vec<u8>,
    ) -> TrustedAnchor {
        TrustedAnchor {
            block_root: root.into_array(),
            parent_root: parent.into_array(),
            slot,
            state_root: state.into_array(),
            block_ssz: Bytes::from(block),
            state_ssz: Bytes::from(state_ssz),
            scalars: Bytes::from(scalars_ssz(root, slot)),
            da: DaVerdict::Available,
        }
    }

    /// An empty store refuses `commit_import`. `commit_anchor` seeds once,
    /// then the child is admitted.
    #[tokio::test]
    async fn empty_store_refuses_import_then_anchor_admits_child() {
        let (dir, engine, archive, shutdown_tx) = block_archive("nongenesis-anchor");
        let parent = Root::from_array([0x11; 32]);
        let root = Root::from_array([0x42; 32]);
        let state = Root::from_array([0xF0; 32]);
        let slot = 64u64;
        let block = synth_block(slot, &parent, &state);
        let state_ssz = b"anchor-state-ssz".to_vec();

        let err = archive
            .commit_import(durable_import(
                slot,
                &parent,
                &root,
                &state,
                DaVerdict::Available,
                b"scalars",
                true,
            ))
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                SeamError::FailedPrecondition {
                    reason: FailedPreconditionReason::StoreIncomplete,
                }
            ),
            "empty store must not seed through commit_import: {err}"
        );
        assert!(!archive.block_is_durable(root.into_array()).unwrap());

        let node_id = Root::from_array([0x5A; 32]);
        {
            let mut batch = engine.batch();
            batch.put(TABLE_META, KEY_NODE_ID.as_bytes(), &node_id.as_ssz_bytes());
            engine.commit(batch).unwrap();
        }
        let before = load_write_cursor(&engine).unwrap().unwrap();
        let scalars = scalars_ssz(&root, slot);

        archive
            .commit_anchor(trusted_anchor(
                &root,
                &parent,
                &state,
                slot,
                block.clone(),
                state_ssz.clone(),
            ))
            .await
            .expect("commit_anchor seeds a non-genesis anchor");
        assert!(
            archive.block_is_durable(root.into_array()).unwrap(),
            "anchor body is durable after commit_anchor"
        );
        assert_anchor_rows(&ExpectedAnchor {
            engine: &engine,
            root: &root,
            parent: &parent,
            state: &state,
            node_id: &node_id,
            slot,
            block: &block,
            state_ssz: &state_ssz,
            scalars: &scalars,
            cursor_seq: before.seq.saturating_add(1),
            session_id: before.session_id,
        });

        let child_root = Root::from_array([0x43; 32]);
        archive
            .commit_import(durable_import(
                slot + 1,
                &root,
                &child_root,
                &state,
                DaVerdict::Available,
                &scalars_ssz(&child_root, slot + 1),
                true,
            ))
            .await
            .expect("parent durability admits the anchor's first child");
        assert!(archive.block_is_durable(child_root.into_array()).unwrap());

        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    struct ExpectedAnchor<'a> {
        engine: &'a Engine,
        root: &'a Root,
        parent: &'a Root,
        state: &'a Root,
        node_id: &'a Root,
        slot: u64,
        block: &'a [u8],
        state_ssz: &'a [u8],
        scalars: &'a [u8],
        cursor_seq: u64,
        session_id: u64,
    }

    fn assert_anchor_rows(expected: &ExpectedAnchor<'_>) {
        let engine = expected.engine;
        let root = *expected.root;
        let parent = *expected.parent;
        let state = *expected.state;
        let node_id = *expected.node_id;
        let block = expected.block;
        let state_ssz = expected.state_ssz;
        let scalars = expected.scalars;
        let cursor_seq = expected.cursor_seq;
        let session_id = expected.session_id;
        let rt = engine.read().unwrap();
        let slot = Slot::new(expected.slot);
        assert_eq!(
            get_block_by_root(&rt, &root).unwrap().as_deref(),
            Some(block),
            "body"
        );
        assert_eq!(get_canonical(&rt, slot).unwrap(), Some(root), "canonical");
        assert_eq!(
            slot_by_root(&rt, &root).unwrap(),
            Some((slot, BlockRegion::Cold)),
            "block_slot_by_root"
        );
        assert_eq!(
            get_state_root(&rt, slot).unwrap(),
            Some(state),
            "state_roots"
        );
        assert_eq!(
            get_da_status(&rt, &root).unwrap(),
            Some((cc_store::DaStatus::Available, slot)),
            "da_status"
        );
        assert_eq!(
            get_snapshot(&rt, slot).unwrap().as_deref(),
            Some(state_ssz),
            "snapshot"
        );
        let info = AnchorInfo::from_ssz_bytes(
            &rt.get(TABLE_META, KEY_ANCHOR_INFO.as_bytes())
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            info,
            AnchorInfo {
                anchor_slot: slot,
                anchor_root: root,
                anchor_state_root: state,
                node_id,
                oldest_block_slot: slot,
                oldest_block_parent: parent,
            }
        );
        assert_eq!(
            load_split(&rt).unwrap(),
            Some(cc_store::Split {
                slot,
                state_root: state,
                block_root: root,
            })
        );
        assert_eq!(
            rt.get(TABLE_META, KEY_FC_SCALARS.as_bytes())
                .unwrap()
                .unwrap(),
            scalars,
            "scalars"
        );
        let cursor = WriteCursor::from_ssz_bytes(
            &rt.get(TABLE_META, cc_store::meta::KEY_WRITE_CURSOR.as_bytes())
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(cursor.session_id, session_id);
        assert_eq!(cursor.seq, cursor_seq);
        assert_eq!(cursor.slot, slot);
        assert_eq!(cursor.root, root);
        let marker = SnapshotCompletion::from_ssz_bytes(
            &rt.get(TABLE_META, KEY_SNAPSHOT_COMPLETION.as_bytes())
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            marker,
            SnapshotCompletion {
                slot,
                state_root: state,
                bytes: state_ssz.len() as u64,
            }
        );
        let (completed, completed_ssz) = completed_snapshot(&rt).unwrap().unwrap();
        assert_eq!(completed, marker);
        assert_eq!(completed_ssz, state_ssz);
    }

    #[tokio::test]
    async fn second_commit_anchor_is_refused() {
        let (dir, _engine, archive, shutdown_tx) = block_archive("second-anchor");
        let parent = Root::from_array([0x11; 32]);
        let root = Root::from_array([0x42; 32]);
        let state = Root::from_array([0xF0; 32]);
        let slot = 64u64;
        let block = synth_block(slot, &parent, &state);
        let anchor = trusted_anchor(
            &root,
            &parent,
            &state,
            slot,
            block,
            b"anchor-state".to_vec(),
        );
        archive.commit_anchor(anchor.clone()).await.unwrap();
        let err = archive.commit_anchor(anchor).await.unwrap_err();
        assert!(
            matches!(
                err,
                SeamError::FailedPrecondition {
                    reason: FailedPreconditionReason::StoreNotUninitialized,
                }
            ),
            "{err}"
        );
        assert_eq!(
            err.to_string(),
            "seam failed precondition: STORE_NOT_UNINITIALIZED"
        );
        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn commit_anchor_on_incomplete_store_is_refused() {
        let (dir, engine, archive, shutdown_tx) = block_archive("incomplete-anchor");
        let planted = Root::from_array([0x77; 32]);
        let planted_ssz = synth_block(
            3,
            &Root::from_array([0x10; 32]),
            &Root::from_array([0xF2; 32]),
        );
        {
            let rt = engine.read().unwrap();
            let mut batch = engine.batch();
            put_block(
                &rt,
                &mut batch,
                Slot::new(3),
                &planted,
                &planted_ssz,
                BlockRegion::Hot,
                false,
            )
            .unwrap();
            drop(rt);
            engine.commit(batch).unwrap();
        }
        let parent = Root::from_array([0x11; 32]);
        let root = Root::from_array([0x42; 32]);
        let state = Root::from_array([0xF0; 32]);
        let err = archive
            .commit_anchor(trusted_anchor(
                &root,
                &parent,
                &state,
                64,
                synth_block(64, &parent, &state),
                b"anchor-state".to_vec(),
            ))
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                SeamError::FailedPrecondition {
                    reason: FailedPreconditionReason::StoreNotUninitialized,
                }
            ),
            "block row without AnchorInfo is not Uninitialized: {err}"
        );
        let rt = engine.read().unwrap();
        assert!(
            rt.get(TABLE_META, KEY_ANCHOR_INFO.as_bytes())
                .unwrap()
                .is_none()
        );
        assert!(get_block_by_root(&rt, &root).unwrap().is_none());
        assert_eq!(
            get_block_by_root(&rt, &planted).unwrap().as_deref(),
            Some(planted_ssz.as_slice())
        );
        drop(rt);

        let (dir_only, engine_only, archive_only, shutdown_only) =
            block_archive("anchor-info-only");
        let stray = AnchorInfo {
            anchor_slot: Slot::new(1),
            anchor_root: Root::from_array([0x01; 32]),
            ..AnchorInfo::default()
        };
        {
            let mut batch = engine_only.batch();
            batch.put(
                TABLE_META,
                KEY_ANCHOR_INFO.as_bytes(),
                &stray.as_ssz_bytes(),
            );
            engine_only.commit(batch).unwrap();
        }
        let err = archive_only
            .commit_anchor(trusted_anchor(
                &root,
                &parent,
                &state,
                64,
                synth_block(64, &parent, &state),
                b"anchor-state".to_vec(),
            ))
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                SeamError::FailedPrecondition {
                    reason: FailedPreconditionReason::StoreNotUninitialized,
                }
            ),
            "AnchorInfo without a body is not Uninitialized: {err}"
        );
        assert!(!archive_only.block_is_durable(root.into_array()).unwrap());

        let _ = shutdown_tx.send(true);
        let _ = shutdown_only.send(true);
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&dir_only);
    }

    #[tokio::test]
    async fn commit_anchor_failure_leaves_store_uninitialized() {
        let (dir, engine) = eng("anchor-crash");
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let faults = WriterFaults {
            fail_next_commit: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)),
            panic_next: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            ..WriterFaults::default()
        };
        let handle = spawn_writer(
            std::sync::Arc::clone(&engine),
            metrics(),
            WriterBounds::default(),
            faults,
            shutdown_rx,
            false,
        );
        ArchiveWriter::ensure_write_cursor(&engine).unwrap();
        let archive = ArchiveWriter::new(handle, std::sync::Arc::clone(&engine));
        let before = load_write_cursor(&engine).unwrap().unwrap();

        let parent = Root::from_array([0x11; 32]);
        let root = Root::from_array([0x42; 32]);
        let state = Root::from_array([0xF0; 32]);
        let slot = 64u64;
        let block = synth_block(slot, &parent, &state);
        let state_ssz = b"anchor-state-ssz".to_vec();
        let anchor = trusted_anchor(
            &root,
            &parent,
            &state,
            slot,
            block.clone(),
            state_ssz.clone(),
        );
        let err = archive.commit_anchor(anchor.clone()).await.unwrap_err();
        assert!(
            matches!(err, SeamError::Unavailable(_)),
            "injected failure before commit: {err}"
        );
        assert!(!archive.block_is_durable(root.into_array()).unwrap());
        {
            let rt = engine.read().unwrap();
            assert!(
                rt.get(TABLE_META, KEY_ANCHOR_INFO.as_bytes())
                    .unwrap()
                    .is_none()
            );
            assert!(rt.get(TABLE_META, KEY_SPLIT.as_bytes()).unwrap().is_none());
            assert!(
                rt.get(TABLE_META, KEY_FC_SCALARS.as_bytes())
                    .unwrap()
                    .is_none()
            );
            assert!(
                rt.get(TABLE_META, KEY_SNAPSHOT_COMPLETION.as_bytes())
                    .unwrap()
                    .is_none()
            );
            assert!(get_snapshot(&rt, Slot::new(slot)).unwrap().is_none());
            assert!(get_state_root(&rt, Slot::new(slot)).unwrap().is_none());
            assert!(get_da_status(&rt, &root).unwrap().is_none());
            assert!(get_canonical(&rt, Slot::new(slot)).unwrap().is_none());
        }
        let cursor = load_write_cursor(&engine).unwrap().unwrap();
        assert_eq!(cursor.seq, before.seq);
        assert_eq!(cursor.root, before.root);
        assert!(
            store_is_uninitialized(&engine).unwrap(),
            "a failed anchor leaves the store Uninitialized"
        );

        archive.commit_anchor(anchor).await.unwrap();
        assert!(archive.block_is_durable(root.into_array()).unwrap());
        assert!(!store_is_uninitialized(&engine).unwrap());

        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }
    fn durable_import(
        slot: u64,
        parent: &Root,
        root: &Root,
        state: &Root,
        da: DaVerdict,
        scalars: &[u8],
        head: bool,
    ) -> DurableImport {
        DurableImport {
            block_root: root.into_array(),
            parent_root: parent.into_array(),
            slot,
            state_root: state.into_array(),
            ssz: Bytes::from(synth_block(slot, parent, state)),
            da,
            scalars: Bytes::copy_from_slice(scalars),
            head: head.then(|| HeadChange {
                head_root: root.into_array(),
                head_slot: slot,
                cause: HeadCause::Import,
            }),
        }
    }

    async fn seed_chain(
        archive: &ArchiveWriter,
        slot: u64,
        parent: &Root,
        root: &Root,
        state: &Root,
    ) {
        // A body whose parent is itself is an anchor, not an import.
        if parent == root {
            let ssz = synth_block(slot, parent, state);
            archive
                .commit_anchor(trusted_anchor(
                    root,
                    parent,
                    state,
                    slot,
                    ssz,
                    b"anchor-state".to_vec(),
                ))
                .await
                .expect("self-parent genesis is commit_anchor");
            return;
        }
        let scalars = scalars_ssz(root, slot);
        archive
            .commit_import(durable_import(
                slot,
                parent,
                root,
                state,
                DaVerdict::Available,
                &scalars,
                true,
            ))
            .await
            .unwrap();
    }

    /// `commit_import` with `head: None` stores the body and leaves canonical
    /// rows, including every row above the sibling, untouched.
    #[tokio::test]
    async fn losing_sibling_head_none_does_not_rewrite_canonical() {
        let state_g = Root::from_array([0xF0; 32]);
        let state_a = Root::from_array([0xF1; 32]);
        let state_b = Root::from_array([0xF2; 32]);
        let state_s = Root::from_array([0xF3; 32]);
        let g = Root::from_array([0x01; 32]);
        let a = Root::from_array([0x02; 32]);
        let b = Root::from_array([0x03; 32]);
        let sibling = Root::from_array([0x04; 32]);

        // The sibling with head: None is durable and canonical rows stay put.
        {
            let (dir, engine, archive, shutdown_tx) = block_archive("sibling-new");
            seed_chain(&archive, 0, &g, &g, &state_g).await;
            seed_chain(&archive, 1, &g, &a, &state_a).await;
            seed_chain(&archive, 2, &a, &b, &state_b).await;
            let rt = engine.read().unwrap();
            assert_eq!(get_canonical(&rt, Slot::new(1)).unwrap(), Some(a));
            assert_eq!(get_canonical(&rt, Slot::new(2)).unwrap(), Some(b));
            drop(rt);

            let before = load_write_cursor(&engine).unwrap().unwrap();
            let sibling_scalars = scalars_ssz(&b, 2);
            archive
                .commit_import(durable_import(
                    2,
                    &a,
                    &sibling,
                    &state_s,
                    DaVerdict::Deferred,
                    &sibling_scalars,
                    false,
                ))
                .await
                .expect("losing sibling with head: None is a legal body commit");

            let rt = engine.read().unwrap();
            assert_eq!(
                get_canonical(&rt, Slot::new(2)).unwrap(),
                Some(b),
                "head: None must not rewrite canonical[slot]"
            );
            assert_eq!(get_canonical(&rt, Slot::new(1)).unwrap(), Some(a));
            assert!(get_block_by_root(&rt, &sibling).unwrap().is_some());
            assert_eq!(
                get_state_root(&rt, Slot::new(2)).unwrap(),
                Some(state_b),
                "head: None must not replace the canonical slot's state root"
            );
            let (da, da_slot) = get_da_status(&rt, &sibling).unwrap().unwrap();
            assert_eq!(da, DaStatus::Deferred);
            assert_eq!(da_slot, Slot::new(2));
            assert_eq!(
                rt.get(TABLE_META, KEY_FC_SCALARS.as_bytes())
                    .unwrap()
                    .unwrap()
                    .as_slice(),
                sibling_scalars.as_slice()
            );
            drop(rt);
            let after = load_write_cursor(&engine).unwrap().unwrap();
            assert_eq!(after.seq, before.seq + 1);
            assert_eq!(after.root, sibling);
            assert_eq!(after.slot, Slot::new(2));

            // Shorter sibling at slot 1 must not delete canonical[2].
            let shorter = Root::from_array([0x05; 32]);
            archive
                .commit_import(durable_import(
                    1,
                    &g,
                    &shorter,
                    &Root::from_array([0xF4; 32]),
                    DaVerdict::Available,
                    &scalars_ssz(&shorter, 1),
                    false,
                ))
                .await
                .expect("shorter sibling with head: None is a legal body commit");

            let rt = engine.read().unwrap();
            assert_eq!(
                get_canonical(&rt, Slot::new(2)).unwrap(),
                Some(b),
                "head: None must not rewrite canonical[slot]"
            );
            assert_eq!(
                get_canonical(&rt, Slot::new(1)).unwrap(),
                Some(a),
                "head: None must not rewrite an earlier canonical row"
            );
            assert!(
                get_block_by_root(&rt, &sibling).unwrap().is_some(),
                "the losing sibling body must still be durable"
            );
            assert!(
                get_block_by_root(&rt, &shorter).unwrap().is_some(),
                "the shorter sibling body must still be durable"
            );
            let _ = shutdown_tx.send(true);
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    #[tokio::test]
    async fn head_none_does_not_replace_an_existing_state_root() {
        let (dir, engine, archive, shutdown_tx) = block_archive("state-root-keep");
        let g = Root::from_array([0x11; 32]);
        let a = Root::from_array([0x12; 32]);
        let sibling = Root::from_array([0x13; 32]);
        let state_g = Root::from_array([0xA0; 32]);
        let state_a = Root::from_array([0xA1; 32]);
        let state_s = Root::from_array([0xA2; 32]);
        seed_chain(&archive, 0, &g, &g, &state_g).await;
        archive
            .commit_import(durable_import(
                1,
                &g,
                &a,
                &state_a,
                DaVerdict::Available,
                &scalars_ssz(&a, 1),
                true,
            ))
            .await
            .unwrap();
        archive
            .commit_import(durable_import(
                1,
                &g,
                &sibling,
                &state_s,
                DaVerdict::Deferred,
                &scalars_ssz(&sibling, 1),
                false,
            ))
            .await
            .unwrap();
        let rt = engine.read().unwrap();
        assert_eq!(get_canonical(&rt, Slot::new(1)).unwrap(), Some(a));
        assert_eq!(get_state_root(&rt, Slot::new(1)).unwrap(), Some(state_a));
        assert!(get_block_by_root(&rt, &sibling).unwrap().is_some());
        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn commit_import_with_head_rewrites_canonical_and_state_root() {
        let (dir, engine, archive, shutdown_tx) = block_archive("head-some");
        let g = Root::from_array([0x21; 32]);
        let a = Root::from_array([0x22; 32]);
        let b = Root::from_array([0x23; 32]);
        let sibling = Root::from_array([0x24; 32]);
        let state_g = Root::from_array([0xB0; 32]);
        let state_a = Root::from_array([0xB1; 32]);
        let state_b = Root::from_array([0xB2; 32]);
        let state_s = Root::from_array([0xB3; 32]);
        seed_chain(&archive, 0, &g, &g, &state_g).await;
        archive
            .commit_import(durable_import(
                1,
                &g,
                &a,
                &state_a,
                DaVerdict::Available,
                &scalars_ssz(&a, 1),
                true,
            ))
            .await
            .unwrap();
        archive
            .commit_import(durable_import(
                2,
                &a,
                &b,
                &state_b,
                DaVerdict::Available,
                &scalars_ssz(&b, 2),
                true,
            ))
            .await
            .unwrap();
        archive
            .commit_import(durable_import(
                1,
                &g,
                &sibling,
                &state_s,
                DaVerdict::Deferred,
                &scalars_ssz(&sibling, 1),
                true,
            ))
            .await
            .unwrap();
        let rt = engine.read().unwrap();
        assert_eq!(get_canonical(&rt, Slot::new(1)).unwrap(), Some(sibling));
        assert_eq!(
            get_canonical(&rt, Slot::new(2)).unwrap(),
            None,
            "a head at slot 1 deletes canonical rows above it"
        );
        assert_eq!(get_state_root(&rt, Slot::new(1)).unwrap(), Some(state_s));
        assert_eq!(get_canonical(&rt, Slot::new(0)).unwrap(), Some(g));
        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn second_commit_import_upgrades_da_and_scalars_without_a_second_body() {
        let (dir, engine, archive, shutdown_tx) = block_archive("upgrade");
        let g = Root::from_array([0x31; 32]);
        let a = Root::from_array([0x32; 32]);
        let state_g = Root::from_array([0xC0; 32]);
        let state_a = Root::from_array([0xC1; 32]);
        seed_chain(&archive, 0, &g, &g, &state_g).await;
        archive
            .commit_import(durable_import(
                1,
                &g,
                &a,
                &state_a,
                DaVerdict::Deferred,
                &scalars_ssz(&a, 1),
                false,
            ))
            .await
            .unwrap();
        let rows = measure_class_stats(&engine).unwrap().blocks_rows;
        let body = {
            let rt = engine.read().unwrap();
            assert_eq!(get_state_root(&rt, Slot::new(1)).unwrap(), Some(state_a));
            let (da, _) = get_da_status(&rt, &a).unwrap().unwrap();
            assert_eq!(da, DaStatus::Deferred);
            get_block_by_root(&rt, &a).unwrap().unwrap()
        };
        let before = load_write_cursor(&engine).unwrap().unwrap();
        let v2 = scalars_ssz_time(&a, 1, 9);
        archive
            .commit_import(durable_import(
                1,
                &g,
                &a,
                &state_a,
                DaVerdict::Available,
                &v2,
                false,
            ))
            .await
            .unwrap();
        assert_eq!(
            measure_class_stats(&engine).unwrap().blocks_rows,
            rows,
            "an upgrade must not insert a second body"
        );
        let rt = engine.read().unwrap();
        assert_eq!(get_block_by_root(&rt, &a).unwrap().unwrap(), body);
        let (da, _) = get_da_status(&rt, &a).unwrap().unwrap();
        assert_eq!(da, DaStatus::Available);
        assert_eq!(
            rt.get(TABLE_META, KEY_FC_SCALARS.as_bytes())
                .unwrap()
                .unwrap()
                .as_slice(),
            v2.as_slice()
        );
        assert_eq!(get_state_root(&rt, Slot::new(1)).unwrap(), Some(state_a));
        assert_eq!(
            get_canonical(&rt, Slot::new(1)).unwrap(),
            None,
            "head: None upgrade must not create a canonical row"
        );
        drop(rt);
        let after = load_write_cursor(&engine).unwrap().unwrap();
        assert_eq!(after.seq, before.seq + 1);
        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn upgrade_with_head_rewrites_canonical_without_a_second_body() {
        let (dir, engine, archive, shutdown_tx) = block_archive("upgrade-head");
        let g = Root::from_array([0x41; 32]);
        let a = Root::from_array([0x42; 32]);
        let b = Root::from_array([0x43; 32]);
        let state_g = Root::from_array([0xD0; 32]);
        let state_a = Root::from_array([0xD1; 32]);
        let state_b = Root::from_array([0xD2; 32]);
        seed_chain(&archive, 0, &g, &g, &state_g).await;
        archive
            .commit_import(durable_import(
                1,
                &g,
                &a,
                &state_a,
                DaVerdict::Deferred,
                &scalars_ssz(&a, 1),
                false,
            ))
            .await
            .unwrap();
        archive
            .commit_import(durable_import(
                2,
                &a,
                &b,
                &state_b,
                DaVerdict::Available,
                &scalars_ssz(&b, 2),
                true,
            ))
            .await
            .unwrap();
        let rows = measure_class_stats(&engine).unwrap().blocks_rows;
        let a2 = scalars_ssz_time(&a, 1, 9);
        archive
            .commit_import(durable_import(
                1,
                &g,
                &a,
                &state_a,
                DaVerdict::Available,
                &a2,
                true,
            ))
            .await
            .unwrap();
        assert_eq!(measure_class_stats(&engine).unwrap().blocks_rows, rows);
        let rt = engine.read().unwrap();
        assert_eq!(get_canonical(&rt, Slot::new(1)).unwrap(), Some(a));
        assert_eq!(get_canonical(&rt, Slot::new(2)).unwrap(), None);
        let (da, _) = get_da_status(&rt, &a).unwrap().unwrap();
        assert_eq!(da, DaStatus::Available);
        assert_eq!(
            rt.get(TABLE_META, KEY_FC_SCALARS.as_bytes())
                .unwrap()
                .unwrap()
                .as_slice(),
            a2.as_slice()
        );
        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn set_head_refuses_a_root_that_is_not_durable() {
        let (dir, engine, archive, shutdown_tx) = block_archive("head-refuse");
        let g = Root::from_array([0x51; 32]);
        let state_g = Root::from_array([0xE0; 32]);
        seed_chain(&archive, 0, &g, &g, &state_g).await;
        let before = load_write_cursor(&engine).unwrap().unwrap();
        let scalars_before = {
            let rt = engine.read().unwrap();
            rt.get(TABLE_META, KEY_FC_SCALARS.as_bytes()).unwrap()
        };
        let err = archive
            .set_head(
                HeadChange {
                    head_root: [0xAB; 32],
                    head_slot: 4,
                    cause: HeadCause::Attestation,
                },
                Bytes::from_static(b"scalars-missing"),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                SeamError::FailedPrecondition {
                    reason: FailedPreconditionReason::HeadNotDurable,
                }
            ),
            "{err}"
        );
        let after = load_write_cursor(&engine).unwrap().unwrap();
        assert_eq!(after.seq, before.seq);
        let rt = engine.read().unwrap();
        assert_eq!(get_canonical(&rt, Slot::new(0)).unwrap(), Some(g));
        assert_eq!(
            rt.get(TABLE_META, KEY_FC_SCALARS.as_bytes()).unwrap(),
            scalars_before,
            "a refused set_head must not replace scalars"
        );
        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn set_head_on_an_empty_store_is_store_incomplete() {
        let (dir, engine, archive, shutdown_tx) = block_archive("head-empty");
        let err = archive
            .set_head(
                HeadChange {
                    head_root: [0xAB; 32],
                    head_slot: 1,
                    cause: HeadCause::EngineInvalidation,
                },
                Bytes::from_static(b"scalars"),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                SeamError::FailedPrecondition {
                    reason: FailedPreconditionReason::StoreIncomplete,
                }
            ),
            "{err}"
        );
        assert!(load_write_cursor(&engine).unwrap().unwrap().seq == 0);
        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn set_head_rewrites_canonical_from_a_durable_body() {
        let (dir, engine, archive, shutdown_tx) = block_archive("set-head");
        let g = Root::from_array([0x61; 32]);
        let a = Root::from_array([0x62; 32]);
        let b = Root::from_array([0x63; 32]);
        let sibling = Root::from_array([0x64; 32]);
        let state_g = Root::from_array([0xE1; 32]);
        let state_a = Root::from_array([0xE2; 32]);
        let state_b = Root::from_array([0xE3; 32]);
        let state_s = Root::from_array([0xE4; 32]);
        seed_chain(&archive, 0, &g, &g, &state_g).await;
        archive
            .commit_import(durable_import(
                1,
                &g,
                &a,
                &state_a,
                DaVerdict::Available,
                &scalars_ssz(&a, 1),
                true,
            ))
            .await
            .unwrap();
        archive
            .commit_import(durable_import(
                2,
                &a,
                &b,
                &state_b,
                DaVerdict::Available,
                &scalars_ssz(&b, 2),
                true,
            ))
            .await
            .unwrap();
        archive
            .commit_import(durable_import(
                1,
                &g,
                &sibling,
                &state_s,
                DaVerdict::Deferred,
                &scalars_ssz(&sibling, 1),
                false,
            ))
            .await
            .unwrap();
        let rows = measure_class_stats(&engine).unwrap().blocks_rows;
        let before = load_write_cursor(&engine).unwrap().unwrap();
        archive
            .set_head(
                HeadChange {
                    head_root: sibling.into_array(),
                    head_slot: 1,
                    cause: HeadCause::Attestation,
                },
                Bytes::from(scalars_ssz(&sibling, 1)),
            )
            .await
            .unwrap();
        assert_eq!(measure_class_stats(&engine).unwrap().blocks_rows, rows);
        let rt = engine.read().unwrap();
        assert_eq!(get_canonical(&rt, Slot::new(1)).unwrap(), Some(sibling));
        assert_eq!(get_canonical(&rt, Slot::new(2)).unwrap(), None);
        assert_eq!(get_canonical(&rt, Slot::new(0)).unwrap(), Some(g));
        assert_eq!(
            rt.get(TABLE_META, KEY_FC_SCALARS.as_bytes())
                .unwrap()
                .unwrap()
                .as_slice(),
            scalars_ssz(&sibling, 1).as_slice()
        );
        // set_head does not insert a body and does not move state_roots.
        assert_eq!(get_state_root(&rt, Slot::new(1)).unwrap(), Some(state_a));
        drop(rt);
        let after = load_write_cursor(&engine).unwrap().unwrap();
        assert_eq!(after.seq, before.seq + 1);
        assert_eq!(after.root, sibling);
        assert_eq!(after.slot, Slot::new(1));
        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn failed_commit_import_does_not_advance_the_cursor() {
        let (dir, engine) = eng("fail-import");
        let flag = Arc::new(AtomicBool::new(false));
        let faults = WriterFaults {
            fail_next_commit: Arc::clone(&flag),
            panic_next: Arc::new(AtomicBool::new(false)),
            ..WriterFaults::default()
        };
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = spawn_writer(
            Arc::clone(&engine),
            metrics(),
            WriterBounds::default(),
            faults,
            shutdown_rx,
            false,
        );
        ArchiveWriter::ensure_write_cursor(&engine).unwrap();
        let archive = ArchiveWriter::new(handle, Arc::clone(&engine));
        let g = Root::from_array([0x71; 32]);
        let state_g = Root::from_array([0xE5; 32]);
        seed_chain(&archive, 0, &g, &g, &state_g).await;
        let before = load_write_cursor(&engine).unwrap().unwrap();
        let scalars_before = {
            let rt = engine.read().unwrap();
            rt.get(TABLE_META, KEY_FC_SCALARS.as_bytes()).unwrap()
        };
        flag.store(true, Ordering::SeqCst);
        let a = Root::from_array([0x72; 32]);
        let err = archive
            .commit_import(durable_import(
                1,
                &g,
                &a,
                &Root::from_array([0xE6; 32]),
                DaVerdict::Available,
                &scalars_ssz(&a, 1),
                true,
            ))
            .await
            .unwrap_err();
        assert!(matches!(err, SeamError::Unavailable(_)), "{err}");
        let after = load_write_cursor(&engine).unwrap().unwrap();
        assert_eq!(after, before, "a failed unit must not advance the cursor");
        let rt = engine.read().unwrap();
        assert!(get_block_by_root(&rt, &a).unwrap().is_none());
        assert!(get_state_root(&rt, Slot::new(1)).unwrap().is_none());
        assert!(get_da_status(&rt, &a).unwrap().is_none());
        assert!(get_canonical(&rt, Slot::new(1)).unwrap().is_none());
        assert_eq!(
            rt.get(TABLE_META, KEY_FC_SCALARS.as_bytes()).unwrap(),
            scalars_before,
            "a failed unit must not replace scalars"
        );
        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn da_demotion_fails_without_advancing_the_cursor() {
        let (dir, engine, archive, shutdown_tx) = block_archive("demote");
        let g = Root::from_array([0x81; 32]);
        let a = Root::from_array([0x82; 32]);
        let state_g = Root::from_array([0xE7; 32]);
        let state_a = Root::from_array([0xE8; 32]);
        seed_chain(&archive, 0, &g, &g, &state_g).await;
        let avail_scalars = scalars_ssz(&a, 1);
        archive
            .commit_import(durable_import(
                1,
                &g,
                &a,
                &state_a,
                DaVerdict::Available,
                &avail_scalars,
                false,
            ))
            .await
            .unwrap();
        let before = load_write_cursor(&engine).unwrap().unwrap();
        let err = archive
            .commit_import(durable_import(
                1,
                &g,
                &a,
                &state_a,
                DaVerdict::Deferred,
                &scalars_ssz(&a, 1),
                false,
            ))
            .await
            .unwrap_err();
        assert!(matches!(err, SeamError::InvalidArgument(_)), "{err}");
        assert!(err.to_string().contains("demotion"), "{err}");
        assert_eq!(load_write_cursor(&engine).unwrap().unwrap(), before);
        let rt = engine.read().unwrap();
        let (da, _) = get_da_status(&rt, &a).unwrap().unwrap();
        assert_eq!(da, DaStatus::Available);
        assert_eq!(
            rt.get(TABLE_META, KEY_FC_SCALARS.as_bytes())
                .unwrap()
                .unwrap()
                .as_slice(),
            avail_scalars.as_slice()
        );
        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn commit_import_refuses_parent_that_is_not_durable() {
        let (dir, engine, archive, shutdown_tx) = block_archive("parent-missing");
        let g = Root::from_array([0x91; 32]);
        let state_g = Root::from_array([0xE9; 32]);
        seed_chain(&archive, 0, &g, &g, &state_g).await;
        let before = load_write_cursor(&engine).unwrap().unwrap();
        let missing = Root::from_array([0x92; 32]);
        let child = Root::from_array([0x93; 32]);
        let err = archive
            .commit_import(durable_import(
                1,
                &missing,
                &child,
                &Root::from_array([0xEA; 32]),
                DaVerdict::Available,
                b"scalars",
                false,
            ))
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                SeamError::FailedPrecondition {
                    reason: FailedPreconditionReason::ParentNotDurable,
                }
            ),
            "{err}"
        );
        assert_eq!(load_write_cursor(&engine).unwrap().unwrap(), before);
        let rt = engine.read().unwrap();
        assert!(get_block_by_root(&rt, &child).unwrap().is_none());
        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn commit_import_refuses_a_head_root_other_than_the_body() {
        let (dir, engine, archive, shutdown_tx) = block_archive("head-other");
        let g = Root::from_array([0xA1; 32]);
        let state_g = Root::from_array([0xEB; 32]);
        seed_chain(&archive, 0, &g, &g, &state_g).await;
        let before = load_write_cursor(&engine).unwrap().unwrap();
        let a = Root::from_array([0xA2; 32]);
        let mut import = durable_import(
            1,
            &g,
            &a,
            &Root::from_array([0xEC; 32]),
            DaVerdict::Available,
            b"scalars",
            true,
        );
        import.head = Some(HeadChange {
            head_root: g.into_array(),
            head_slot: 1,
            cause: HeadCause::Import,
        });
        let err = archive.commit_import(import).await.unwrap_err();
        assert!(
            matches!(
                err,
                SeamError::FailedPrecondition {
                    reason: FailedPreconditionReason::HeadNotDurable,
                }
            ),
            "{err}"
        );
        assert_eq!(load_write_cursor(&engine).unwrap().unwrap(), before);
        let rt = engine.read().unwrap();
        assert!(get_block_by_root(&rt, &a).unwrap().is_none());
        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn commit_import_on_an_empty_store_is_store_incomplete() {
        let (dir, _engine, archive, shutdown_tx) = block_archive("import-empty");
        let g = Root::from_array([0xB1; 32]);
        let err = archive
            .commit_import(durable_import(
                0,
                &Root::ZERO,
                &g,
                &Root::from_array([0xED; 32]),
                DaVerdict::Available,
                b"scalars",
                true,
            ))
            .await
            .unwrap_err();
        // SSZ parent is zero and the caller parent is zero, so the bind is
        // structural-ok; the empty store is refused before parent durability.
        assert!(
            matches!(
                err,
                SeamError::FailedPrecondition {
                    reason: FailedPreconditionReason::StoreIncomplete,
                }
            ),
            "{err}"
        );
        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Conformance case 12. A body without the `Complete` set is not an empty
    /// store and is not a legal import. The parent is durable, so the refusal
    /// is the store, not `PARENT_NOT_DURABLE`.
    #[tokio::test]
    async fn commit_import_on_incomplete_store_is_failed_precondition() {
        let (dir, engine, archive, shutdown_tx) = block_archive("import-incomplete");
        let parent = Root::from_array([0xC1; 32]);
        let parent_state = Root::from_array([0xF1; 32]);
        let parent_ssz = synth_block(4, &Root::from_array([0x10; 32]), &parent_state);
        {
            let rt = engine.read().unwrap();
            let mut batch = engine.batch();
            put_block(
                &rt,
                &mut batch,
                Slot::new(4),
                &parent,
                &parent_ssz,
                BlockRegion::Hot,
                false,
            )
            .unwrap();
            drop(rt);
            engine.commit(batch).unwrap();
        }
        let before = load_write_cursor(&engine).unwrap().unwrap();
        let child = Root::from_array([0xC2; 32]);
        let err = archive
            .commit_import(durable_import(
                5,
                &parent,
                &child,
                &Root::from_array([0xF2; 32]),
                DaVerdict::Available,
                b"scalars",
                true,
            ))
            .await
            .unwrap_err();
        assert!(
            matches!(
                err,
                SeamError::FailedPrecondition {
                    reason: FailedPreconditionReason::StoreIncomplete,
                }
            ),
            "{err}"
        );
        assert_eq!(
            err.to_string(),
            "seam failed precondition: STORE_INCOMPLETE"
        );
        assert_eq!(load_write_cursor(&engine).unwrap().unwrap(), before);
        let rt = engine.read().unwrap();
        assert!(get_block_by_root(&rt, &child).unwrap().is_none());
        assert_eq!(
            get_block_by_root(&rt, &parent).unwrap().as_deref(),
            Some(parent_ssz.as_slice())
        );
        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Opaque scalar bytes must not commit. The next admit would be `Incomplete`.
    #[tokio::test]
    async fn undecodable_scalars_are_rejected_before_commit() {
        let (dir, engine, archive, shutdown_tx) = block_archive("bad-scalars");
        let g = Root::from_array([0xD1; 32]);
        let state = Root::from_array([0xD2; 32]);
        seed_chain(&archive, 0, &g, &g, &state).await;
        let before = {
            let rt = engine.read().unwrap();
            rt.get(TABLE_META, KEY_FC_SCALARS.as_bytes()).unwrap()
        };
        let child = Root::from_array([0xD3; 32]);
        let err = archive
            .commit_import(durable_import(
                1,
                &g,
                &child,
                &state,
                DaVerdict::Available,
                b"not-ssz",
                true,
            ))
            .await
            .unwrap_err();
        assert!(matches!(err, SeamError::InvalidArgument(_)), "{err}");
        assert!(err.to_string().contains("ForkChoiceScalars"), "{err}");
        {
            let rt = engine.read().unwrap();
            assert!(get_block_by_root(&rt, &child).unwrap().is_none());
            assert_eq!(
                rt.get(TABLE_META, KEY_FC_SCALARS.as_bytes()).unwrap(),
                before
            );
        }
        archive
            .commit_import(durable_import(
                1,
                &g,
                &child,
                &state,
                DaVerdict::Available,
                &scalars_ssz(&child, 1),
                true,
            ))
            .await
            .unwrap();
        let err = archive
            .set_head(
                HeadChange {
                    head_root: child.into_array(),
                    head_slot: 1,
                    cause: HeadCause::Attestation,
                },
                Bytes::from_static(b"not-ssz"),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, SeamError::InvalidArgument(_)), "{err}");
        assert!(err.to_string().contains("ForkChoiceScalars"), "{err}");
        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn import_precondition_matches_commit_import_refusal_tokens() {
        let (dir, _engine, archive, shutdown_tx) = block_archive("precondition-empty");
        let missing = [0x11u8; 32];
        let err = archive.import_precondition(missing).unwrap_err();
        assert!(
            matches!(
                err,
                SeamError::FailedPrecondition {
                    reason: FailedPreconditionReason::StoreIncomplete,
                }
            ),
            "empty store is STORE_INCOMPLETE, not a new classifier: {err}"
        );
        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);

        let (dir, _engine, archive, shutdown_tx) = block_archive("precondition-parent");
        let parent = Root::from_array([0x21; 32]);
        let state = Root::from_array([0x22; 32]);
        seed_chain(&archive, 0, &parent, &parent, &state).await;
        archive
            .import_precondition(parent.into_array())
            .expect("durable parent is admitted");
        let err = archive.import_precondition([0x23; 32]).unwrap_err();
        assert!(
            matches!(
                err,
                SeamError::FailedPrecondition {
                    reason: FailedPreconditionReason::ParentNotDurable,
                }
            ),
            "missing parent on a non-empty store is PARENT_NOT_DURABLE: {err}"
        );
        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn plant_cold_body(
        engine: &Engine,
        slot: u64,
        parent: &Root,
        root: &Root,
        state: &Root,
    ) -> Vec<u8> {
        let ssz = synth_block(slot, parent, state);
        let rt = engine.read().unwrap();
        let mut batch = engine.batch();
        put_block(
            &rt,
            &mut batch,
            Slot::new(slot),
            root,
            &ssz,
            BlockRegion::Cold,
            false,
        )
        .unwrap();
        drop(rt);
        engine.commit(batch).unwrap();
        ssz
    }

    fn assert_cold_index_untouched(
        engine: &Engine,
        slot: u64,
        root: &Root,
        ssz: &[u8],
        index: &[u8],
        rows: u64,
    ) {
        assert_eq!(measure_class_stats(engine).unwrap().blocks_rows, rows);
        let rt = engine.read().unwrap();
        assert_eq!(get_block_by_root(&rt, root).unwrap().as_deref(), Some(ssz));
        let (got_slot, region) = slot_by_root(&rt, root).unwrap().unwrap();
        assert_eq!(got_slot, Slot::new(slot));
        assert_eq!(region, BlockRegion::Cold);
        assert_eq!(
            rt.get(TABLE_BLOCK_SLOT_BY_ROOT, &encode_root_key(root))
                .unwrap()
                .unwrap()
                .as_slice(),
            index,
            "cold reverse index must not be rewritten as hot"
        );
        assert!(
            rt.get(
                TABLE_BLOCKS_HOT,
                &encode_hot_block_key(Slot::new(slot), root)
            )
            .unwrap()
            .is_none(),
            "an upgrade must not insert a hot row"
        );
        let cold = blocks_shard_table(block_shard_id(Slot::new(slot)));
        assert_eq!(
            rt.get(&cold, &encode_cold_block_key(Slot::new(slot)))
                .unwrap()
                .unwrap()
                .as_slice(),
            ssz
        );
    }

    /// Genesis plus one cold child. Shared by the in-process upgrade and the
    /// collision child so the parent can reopen the child's store.
    fn cold_collision_ids() -> (Root, Root, Root, Root) {
        (
            Root::from_array([0x71; 32]),
            Root::from_array([0x72; 32]),
            Root::from_array([0x73; 32]),
            Root::from_array([0x74; 32]),
        )
    }

    async fn cold_reimport_different_bytes_child(dir: PathBuf) {
        let (g, a, state_g, state_a) = cold_collision_ids();
        // Immediate, not None: the process aborts on the colliding commit, and
        // None can drop the planted body before the parent reopens this dir.
        let engine = Arc::new(
            Engine::open(
                &dir,
                EngineOptions::default().with_durability(Durability::Immediate),
            )
            .unwrap(),
        );
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = spawn_writer(
            Arc::clone(&engine),
            metrics(),
            WriterBounds::default(),
            WriterFaults::default(),
            shutdown_rx,
            false,
        );
        ArchiveWriter::ensure_write_cursor(&engine).unwrap();
        let archive = ArchiveWriter::new(handle, Arc::clone(&engine));
        seed_chain(&archive, 0, &g, &g, &state_g).await;
        plant_cold_body(&engine, 1, &g, &a, &state_a);
        let mut import = durable_import(
            1,
            &g,
            &a,
            &state_a,
            DaVerdict::Available,
            &scalars_ssz(&a, 1),
            true,
        );
        let other = Root::from_array([0x75; 32]);
        import.state_root = other.into_array();
        import.ssz = Bytes::from(synth_block(1, &g, &other));
        let result = archive.commit_import(import).await;
        let _ = shutdown_tx.send(true);
        panic!("different bytes under a cold root must abort before commit, got {result:?}");
    }

    /// A body already durable in the cold region is an upgrade: same bytes
    /// refresh da/scalars and may rewrite canonical, without a hot row or a
    /// new reverse-index value. Different bytes stay a process-fatal collision
    /// and commit nothing.
    #[tokio::test]
    async fn cold_reimport_keeps_the_cold_index_and_refuses_different_bytes() {
        if std::env::var_os("CC_COLD_REIMPORT_CHILD").is_some() {
            let dir = PathBuf::from(std::env::var("CC_COLD_REIMPORT_DIR").unwrap());
            cold_reimport_different_bytes_child(dir).await;
            return;
        }

        let (dir, engine, archive, shutdown_tx) = block_archive("cold-upgrade");
        let g = Root::from_array([0x61; 32]);
        let a = Root::from_array([0x62; 32]);
        let state_g = Root::from_array([0x63; 32]);
        let state_a = Root::from_array([0x64; 32]);
        let above = Root::from_array([0x65; 32]);
        seed_chain(&archive, 0, &g, &g, &state_g).await;
        let ssz = plant_cold_body(&engine, 1, &g, &a, &state_a);
        {
            let rt = engine.read().unwrap();
            let mut batch = engine.batch();
            put_canonical(&rt, &mut batch, Slot::new(2), &above).unwrap();
            drop(rt);
            engine.commit(batch).unwrap();
        }
        let rows = measure_class_stats(&engine).unwrap().blocks_rows;
        let index = {
            let rt = engine.read().unwrap();
            assert_eq!(get_canonical(&rt, Slot::new(1)).unwrap(), None);
            assert_eq!(get_canonical(&rt, Slot::new(2)).unwrap(), Some(above));
            rt.get(TABLE_BLOCK_SLOT_BY_ROOT, &encode_root_key(&a))
                .unwrap()
                .unwrap()
        };
        let before = load_write_cursor(&engine).unwrap().unwrap();
        let cold_v1 = scalars_ssz(&a, 1);

        archive
            .commit_import(durable_import(
                1,
                &g,
                &a,
                &state_a,
                DaVerdict::Deferred,
                &cold_v1,
                false,
            ))
            .await
            .expect("same bytes of a cold body are an upgrade");

        assert_cold_index_untouched(&engine, 1, &a, &ssz, &index, rows);
        {
            let rt = engine.read().unwrap();
            assert_eq!(get_canonical(&rt, Slot::new(1)).unwrap(), None);
            assert_eq!(
                get_canonical(&rt, Slot::new(2)).unwrap(),
                Some(above),
                "head: None must not delete canonical rows above the body"
            );
            assert_eq!(get_state_root(&rt, Slot::new(1)).unwrap(), Some(state_a));
            let (da, da_slot) = get_da_status(&rt, &a).unwrap().unwrap();
            assert_eq!(da, DaStatus::Deferred);
            assert_eq!(da_slot, Slot::new(1));
            assert_eq!(
                rt.get(TABLE_META, KEY_FC_SCALARS.as_bytes())
                    .unwrap()
                    .unwrap()
                    .as_slice(),
                cold_v1.as_slice()
            );
        }
        let mid = load_write_cursor(&engine).unwrap().unwrap();
        assert_eq!(mid.seq, before.seq + 1);

        archive
            .commit_import(durable_import(
                1,
                &g,
                &a,
                &state_a,
                DaVerdict::Available,
                &scalars_ssz_time(&a, 1, 9),
                true,
            ))
            .await
            .expect("head: Some on a cold body still rewrites canonical");

        assert_cold_index_untouched(&engine, 1, &a, &ssz, &index, rows);
        {
            let rt = engine.read().unwrap();
            assert_eq!(get_canonical(&rt, Slot::new(1)).unwrap(), Some(a));
            assert_eq!(
                get_canonical(&rt, Slot::new(2)).unwrap(),
                None,
                "head: Some must delete canonical rows above the body"
            );
            assert_eq!(get_canonical(&rt, Slot::new(0)).unwrap(), Some(g));
            let (da, _) = get_da_status(&rt, &a).unwrap().unwrap();
            assert_eq!(da, DaStatus::Available);
            assert_eq!(
                rt.get(TABLE_META, KEY_FC_SCALARS.as_bytes())
                    .unwrap()
                    .unwrap()
                    .as_slice(),
                scalars_ssz_time(&a, 1, 9).as_slice()
            );
        }
        let after = load_write_cursor(&engine).unwrap().unwrap();
        assert_eq!(after.seq, mid.seq + 1);
        assert_eq!(after.root, a);
        assert_eq!(after.slot, Slot::new(1));
        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);

        // Different bytes abort the writer process. Run that in a child so
        // this test process survives, then reopen the store.
        let collision_dir = tmp_dir("cold-collision");
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("archive_write::tests::cold_reimport_keeps_the_cold_index_and_refuses_different_bytes")
            .env("CC_COLD_REIMPORT_CHILD", "1")
            .env("CC_COLD_REIMPORT_DIR", &collision_dir)
            .output()
            .unwrap();
        let signaled = {
            #[cfg(unix)]
            {
                use std::os::unix::process::ExitStatusExt;
                output.status.signal() == Some(6)
            }
            #[cfg(not(unix))]
            {
                false
            }
        };
        assert!(
            signaled,
            "different cold bytes must SIGABRT; status {:?} stderr {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
        let (g, a, _state_g, state_a) = cold_collision_ids();
        let engine = Engine::open(
            &collision_dir,
            EngineOptions::default().with_durability(Durability::Immediate),
        )
        .unwrap();
        let ssz = synth_block(1, &g, &state_a);
        {
            let rt = engine.read().unwrap();
            assert_eq!(
                get_block_by_root(&rt, &a).unwrap().as_deref(),
                Some(ssz.as_slice())
            );
            assert_eq!(slot_by_root(&rt, &a).unwrap().unwrap().1, BlockRegion::Cold);
            assert!(
                rt.get(TABLE_BLOCKS_HOT, &encode_hot_block_key(Slot::new(1), &a))
                    .unwrap()
                    .is_none()
            );
            assert!(get_da_status(&rt, &a).unwrap().is_none());
            assert_ne!(
                rt.get(TABLE_META, KEY_FC_SCALARS.as_bytes())
                    .unwrap()
                    .unwrap()
                    .as_slice(),
                b"scalars-bad",
                "the colliding unit must not commit its scalars"
            );
            assert_eq!(get_canonical(&rt, Slot::new(1)).unwrap(), None);
            assert_eq!(get_canonical(&rt, Slot::new(0)).unwrap(), Some(g));
        }
        assert_eq!(load_write_cursor(&engine).unwrap().unwrap().seq, 1);
        assert_eq!(measure_class_stats(&engine).unwrap().blocks_rows, 2);
        let _ = std::fs::remove_dir_all(&collision_dir);
    }

    /// Conformance case 14. A stalled P0 writer must not park `commit_import`
    /// and must not surface [`SeamError::Backpressure`]. Past `2 * seconds_per_slot`
    /// the existing process-fatal path aborts with [`COMMIT_DEADLINE_REASON`].
    /// Aborting is not backpressure (ADR-R-08); this test cites that consequence.
    #[tokio::test(start_paused = true)]
    async fn commit_deadline_is_fail_closed_not_backpressure() {
        use crate::writer::{ProcessExit, spawn_writer_with_exit};
        use prometheus_client::encoding::text::encode;
        use std::sync::{Mutex, atomic::AtomicBool};
        use std::time::Duration;

        // Not 12: the deadline is `2 * seconds_per_slot` from the running config.
        let seconds_per_slot = 3u64;
        let deadline = Duration::from_secs(seconds_per_slot.saturating_mul(2));
        let mut registry = Registry::default();
        let metrics = StorageMetrics::register(&mut registry);
        let (dir, engine) = eng("commit-deadline");
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let aborted = Arc::new(Mutex::new(None));
        let aborted_hook = Arc::clone(&aborted);
        let faults = WriterFaults {
            fail_next_commit: Arc::new(AtomicBool::new(false)),
            panic_next: Arc::new(AtomicBool::new(false)),
            stall_commit: Arc::new(AtomicBool::new(true)),
            start_paused: false,
        };
        let handle = spawn_writer_with_exit(
            Arc::clone(&engine),
            metrics,
            WriterBounds::default(),
            faults,
            shutdown_rx,
            true,
            ProcessExit::Hook(Arc::new(move |code, reason| {
                *aborted_hook.lock().expect("abort record") = Some((code, reason));
            })),
        );
        ArchiveWriter::ensure_write_cursor(&engine).unwrap();
        let parent = Root::from_array([0x21; 32]);
        let child = Root::from_array([0x22; 32]);
        let state = Root::from_array([0xF4; 32]);
        {
            // Admit refuses an incomplete store before the writer can stall.
            let ssz = synth_block(0, &Root::ZERO, &state);
            let slot = Slot::new(0);
            let rt = engine.read().unwrap();
            let mut batch = engine.batch();
            put_block(
                &rt,
                &mut batch,
                slot,
                &parent,
                &ssz,
                BlockRegion::Hot,
                false,
            )
            .unwrap();
            cc_store::put_da_status(&rt, &mut batch, &parent, DaStatus::Available, slot).unwrap();
            let snap = b"deadline-snap";
            batch.put(
                cc_store::TABLE_SNAPSHOTS,
                &cc_store::encode_snapshot_key(slot),
                snap,
            );
            cc_store::put_snapshot_completion(
                &mut batch,
                &SnapshotCompletion {
                    slot,
                    state_root: state,
                    bytes: snap.len() as u64,
                },
            );
            let anchor = AnchorInfo {
                anchor_slot: slot,
                anchor_root: parent,
                anchor_state_root: state,
                ..AnchorInfo::default()
            };
            batch.put(
                TABLE_META,
                KEY_ANCHOR_INFO.as_bytes(),
                &anchor.as_ssz_bytes(),
            );
            batch.put(
                TABLE_META,
                KEY_FC_SCALARS.as_bytes(),
                &scalars_ssz(&parent, 0),
            );
            drop(rt);
            engine.commit(batch).unwrap();
        }
        let archive =
            ArchiveWriter::new(handle, Arc::clone(&engine)).with_seconds_per_slot(seconds_per_slot);

        let started = tokio::time::Instant::now();
        let err = archive
            .commit_import(durable_import(
                1,
                &parent,
                &child,
                &state,
                DaVerdict::Available,
                &scalars_ssz(&child, 1),
                true,
            ))
            .await
            .expect_err("a stalled writer must not report a durable commit");
        let elapsed = started.elapsed();

        assert!(
            elapsed >= deadline,
            "must wait 2 slots ({deadline:?}), elapsed {elapsed:?}"
        );
        assert!(
            elapsed < deadline + deadline,
            "must not park past the deadline, elapsed {elapsed:?}"
        );
        assert!(
            !matches!(err, SeamError::Backpressure { .. }),
            "aborting is not backpressure (ADR-R-08), got {err:?}"
        );
        assert!(
            err.to_string().contains(COMMIT_DEADLINE_REASON),
            "named reason {COMMIT_DEADLINE_REASON}, got {err}"
        );
        let (code, reason) = aborted
            .lock()
            .expect("abort record")
            .expect("process-fatal hook must run");
        assert_eq!(code, 1);
        assert_eq!(reason, COMMIT_DEADLINE_REASON);

        let mut buf = String::new();
        encode(&mut buf, &registry).unwrap();
        let sum = commit_wait_sum(&buf);
        assert!(
            sum > 0.0,
            "cc_storage_commit_wait_seconds must be non-empty under the stall:\n{buf}"
        );
        assert!(
            buf.contains("cc_storage_commit_wait_seconds"),
            "histogram must be emitted:\n{buf}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    fn commit_wait_sum(exposition: &str) -> f64 {
        exposition
            .lines()
            .find_map(|line| {
                let rest = line.strip_prefix("cc_storage_commit_wait_seconds_sum ")?;
                rest.trim().parse().ok()
            })
            .unwrap_or(0.0)
    }
}
