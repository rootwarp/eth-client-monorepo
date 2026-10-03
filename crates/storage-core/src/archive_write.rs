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

use std::sync::Arc;

use async_trait::async_trait;
use cc_seam::{ArchiveWrite, ColumnBatch, IngestBlock, SeamError};
use cc_store::blocks::{parent_root_at_offset, slot_at_offset};
use cc_store::columns::{
    COLUMN_HEADER_PARENT_ROOT_SSZ_OFFSET, MIN_COLUMN_SSZ_LEN, NUMBER_OF_COLUMNS,
    column_parent_root_at_offset,
};
use cc_store::engine::{Engine, StoreError};
use cc_store::meta::WriteCursor;
use cc_store::{Root, Slot, SszEncode, TABLE_BLOCKS_HOT, TABLE_CANONICAL};

use crate::writer::{
    CommitUnit, StagedBlock, StagedColumn, WriterError, WriterHandle, block_present,
    load_write_cursor,
};

/// Typed ingest adapter. Holds the live writer handle — no second mailbox.
#[derive(Debug, Clone)]
pub struct ArchiveWriter {
    writer: WriterHandle,
    engine: Arc<Engine>,
}

impl ArchiveWriter {
    pub(crate) fn new(writer: WriterHandle, engine: Arc<Engine>) -> Self {
        Self { writer, engine }
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

fn map_writer_err(err: WriterError) -> SeamError {
    match err {
        WriterError::ShutDown => SeamError::Unavailable("writer shut down".into()),
        WriterError::Store(StoreError::Codec(msg) | StoreError::Limit(msg)) => {
            SeamError::InvalidArgument(msg)
        }
        WriterError::Store(e) => SeamError::Unavailable(e.to_string()),
        WriterError::InjectedFailure => SeamError::Unavailable("injected commit failure".into()),
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
            cursor,
            done: None,
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

    fn block_unit(&self, batch: WriterBatch) -> Result<CommitUnit, SeamError> {
        let prev = self.load_or_missing_cursor()?;
        let (slot, root) = batch
            .blocks
            .last()
            .map(|b| (b.slot, b.root))
            .unwrap_or((prev.slot, prev.root));
        let cursor = WriteCursor {
            session_id: prev.session_id,
            seq: prev.seq.saturating_add(1),
            slot,
            root,
        };
        self.unit_for_batch(batch, cursor)
    }

    async fn submit_block_batch(&self, batch: WriterBatch) -> Result<(), SeamError> {
        let unit = self.block_unit(batch)?;
        self.writer
            .submit_p0_committed(unit)
            .await
            .map_err(map_writer_err)
    }

    fn submit_block_batch_blocking(&self, batch: WriterBatch) -> Result<(), SeamError> {
        let unit = self.block_unit(batch)?;
        self.writer
            .blocking_submit_p0_committed(unit)
            .map_err(map_writer_err)
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

    async fn ingest_block(&self, block: IngestBlock) -> Result<(), SeamError> {
        match bind_ingest_block(&self.engine, block)? {
            Some(batch) => self.submit_block_batch(batch).await,
            None => Ok(()),
        }
    }

    fn ingest_block_blocking(&self, block: IngestBlock) -> Result<(), SeamError> {
        match bind_ingest_block(&self.engine, block)? {
            Some(batch) => self.submit_block_batch_blocking(batch),
            None => Ok(()),
        }
    }

    fn block_is_durable(&self, root: cc_seam::Root) -> Result<bool, SeamError> {
        block_present(&self.engine, &Root::from_array(root))
            .map_err(|e| SeamError::Unavailable(e.to_string()))
    }
}

/// Bind caller `(slot, parent_root, block_root)` to the SSZ header.
///
/// Slot and parent_root are fixed-offset peeks. `block_root` is the caller's
/// value — storage does not decode the body to recompute it. Continuity is
/// `block_present` on that parent (or the parent row is first in this batch).
/// Self-parent is first-seed only.
fn bind_ingest_block(
    engine: &Engine,
    block: IngestBlock,
) -> Result<Option<WriterBatch>, SeamError> {
    let ssz = block.ssz.as_ref();
    let ssz_slot = slot_at_offset(ssz).map_err(|e| SeamError::InvalidArgument(e.to_string()))?;
    let claimed_slot = Slot::new(block.slot);
    if ssz_slot != claimed_slot {
        return Err(SeamError::InvalidArgument(format!(
            "slot mismatch: caller {} != SSZ header slot {}",
            claimed_slot.as_u64(),
            ssz_slot.as_u64()
        )));
    }

    let ssz_parent =
        parent_root_at_offset(ssz).map_err(|e| SeamError::InvalidArgument(e.to_string()))?;
    let claimed_parent = Root::from_array(block.parent_root);
    let claimed_root = Root::from_array(block.block_root);

    let genesis_remap = ssz_parent == Root::ZERO && claimed_parent == claimed_root;
    if ssz_parent != claimed_parent && !genesis_remap {
        return Err(SeamError::InvalidArgument(format!(
            "parent_root mismatch: caller != SSZ header parent_root at offset {}",
            cc_store::PARENT_ROOT_SSZ_OFFSET
        )));
    }

    if claimed_parent == claimed_root && durable_head_present(engine)? {
        return Err(SeamError::InvalidArgument(
            "self-parent is only allowed as the first seed (empty store / no canonical head)"
                .into(),
        ));
    }

    // Already-durable body: do not submit with update_canonical (H3).
    // rewrite_from_head from this root would delete every canonical row above.
    if block_present(engine, &claimed_root).map_err(|e| SeamError::Unavailable(e.to_string()))? {
        return Ok(None);
    }

    Ok(Some(WriterBatch {
        head: Some(ContinuityHead {
            parent_root: claimed_parent,
            slot: claimed_slot,
        }),
        blocks: vec![StagedBlock {
            slot: claimed_slot,
            root: claimed_root,
            ssz: block.ssz.to_vec(),
            update_canonical: true,
            write_state_root: false,
            da_status: None,
        }],
        columns: Vec::new(),
    }))
}

fn durable_head_present(engine: &Engine) -> Result<bool, SeamError> {
    let rt = engine
        .read()
        .map_err(|e| SeamError::Unavailable(e.to_string()))?;
    let lo = cc_store::keys::encode_cold_block_key(Slot::ZERO);
    let hi = cc_store::keys::encode_cold_block_key(Slot::new(u64::MAX));
    let mut canon = rt
        .range_max(TABLE_CANONICAL, &lo, &hi, 1)
        .map_err(|e| SeamError::Unavailable(e.to_string()))?;
    if canon.next().is_some() {
        return Ok(true);
    }
    let blo = [0u8; 40];
    let bhi = [0xffu8; 40];
    let mut blocks = rt
        .range_max(TABLE_BLOCKS_HOT, &blo, &bhi, 1)
        .map_err(|e| SeamError::Unavailable(e.to_string()))?;
    Ok(blocks.next().is_some())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::metrics::StorageMetrics;
    use crate::writer::{WriterBounds, WriterFaults, WriterHandle, spawn_writer};
    use cc_seam::Bytes;
    use cc_store::blocks::{
        MIN_BLOCK_SSZ_LEN, PARENT_ROOT_SSZ_OFFSET, SLOT_SSZ_OFFSET, STATE_ROOT_SSZ_OFFSET,
    };
    use cc_store::canonical::get_canonical;
    use cc_store::columns::{
        COLUMN_HEADER_PARENT_ROOT_SSZ_OFFSET, COLUMN_HEADER_SLOT_SSZ_OFFSET,
        COLUMN_INDEX_SSZ_OFFSET, DATA_COLUMN_SIDECAR_FIXED_BYTES, get_column_by_root,
    };
    use cc_store::engine::{Durability, EngineOptions};
    use cc_store::keys::BlockRegion;
    use cc_store::meta::WriteCursor;
    use cc_store::{get_block_by_root, put_block};
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

    #[tokio::test]
    async fn ingest_block_rejects_slot_mismatch() {
        let (dir, _engine, archive, shutdown_tx) = block_archive("slot-bind");
        let root = Root::from_array([0x42; 32]);
        let ssz = synth_block(1, &Root::ZERO, &Root::from_array([0xF0; 32]));
        let err = archive
            .ingest_block(IngestBlock {
                parent_root: root.into_array(),
                slot: 9,
                block_root: root.into_array(),
                ssz: Bytes::from(ssz),
            })
            .await
            .unwrap_err();
        assert!(matches!(err, SeamError::InvalidArgument(_)));
        assert!(err.to_string().contains("slot mismatch"), "{err}");
        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn ingest_block_rejects_parent_mismatch() {
        let (dir, _engine, archive, shutdown_tx) = block_archive("parent-bind");
        let ssz = synth_block(1, &Root::ZERO, &Root::from_array([0xF0; 32]));
        let err = archive
            .ingest_block(IngestBlock {
                parent_root: [0xAB; 32],
                slot: 1,
                block_root: [0xCD; 32],
                ssz: Bytes::from(ssz),
            })
            .await
            .unwrap_err();
        assert!(matches!(err, SeamError::InvalidArgument(_)));
        assert!(err.to_string().contains("parent_root mismatch"), "{err}");
        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn ingest_block_rejects_self_parent_after_head_exists() {
        let (dir, engine, archive, shutdown_tx) = block_archive("self-parent");
        let g_root = Root::from_array([0x01; 32]);
        let g_ssz = synth_block(0, &Root::ZERO, &Root::from_array([0xF0; 32]));
        archive
            .ingest_block(IngestBlock {
                parent_root: g_root.into_array(),
                slot: 0,
                block_root: g_root.into_array(),
                ssz: Bytes::from(g_ssz),
            })
            .await
            .unwrap();
        assert!(durable_head_present(&engine).unwrap());

        let fake_root = Root::from_array([0x02; 32]);
        let fake_ssz = synth_block(3, &Root::ZERO, &Root::from_array([0xF1; 32]));
        let err = archive
            .ingest_block(IngestBlock {
                parent_root: fake_root.into_array(),
                slot: 3,
                block_root: fake_root.into_array(),
                ssz: Bytes::from(fake_ssz),
            })
            .await
            .unwrap_err();
        assert!(matches!(err, SeamError::InvalidArgument(_)));
        assert!(err.to_string().contains("self-parent"), "{err}");
        let rt = engine.read().unwrap();
        assert!(get_block_by_root(&rt, &fake_root).unwrap().is_none());
        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn ingest_block_reimport_ancestor_keeps_durable_head() {
        let (dir, engine, archive, shutdown_tx) = block_archive("h3-no-rewind");
        let g_root = Root::from_array([0x01; 32]);
        let a_root = Root::from_array([0x02; 32]);
        let b_root = Root::from_array([0x03; 32]);
        let state = Root::from_array([0xF0; 32]);
        let g_ssz = synth_block(0, &Root::ZERO, &state);
        archive
            .ingest_block(IngestBlock {
                parent_root: g_root.into_array(),
                slot: 0,
                block_root: g_root.into_array(),
                ssz: Bytes::from(g_ssz),
            })
            .await
            .unwrap();
        let a_ssz = synth_block(1, &g_root, &state);
        archive
            .ingest_block(IngestBlock {
                parent_root: g_root.into_array(),
                slot: 1,
                block_root: a_root.into_array(),
                ssz: Bytes::from(a_ssz.clone()),
            })
            .await
            .unwrap();
        let b_ssz = synth_block(2, &a_root, &state);
        archive
            .ingest_block(IngestBlock {
                parent_root: a_root.into_array(),
                slot: 2,
                block_root: b_root.into_array(),
                ssz: Bytes::from(b_ssz.clone()),
            })
            .await
            .unwrap();

        archive
            .ingest_block(IngestBlock {
                parent_root: g_root.into_array(),
                slot: 1,
                block_root: a_root.into_array(),
                ssz: Bytes::from(a_ssz.clone()),
            })
            .await
            .expect("re-ingest of durable A must not fail");

        let rt = engine.read().unwrap();
        assert_eq!(get_canonical(&rt, Slot::new(1)).unwrap(), Some(a_root));
        assert_eq!(
            get_canonical(&rt, Slot::new(2)).unwrap(),
            Some(b_root),
            "re-ingest of A must not rewind durable head off B"
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
    async fn ingest_block_persists_caller_block_root_without_decode() {
        let (dir, engine, archive, shutdown_tx) = block_archive("caller-root");
        let root = Root::from_array([0x42; 32]);
        let ssz = synth_block(0, &Root::ZERO, &Root::from_array([0xF0; 32]));
        archive
            .ingest_block(IngestBlock {
                parent_root: root.into_array(),
                slot: 0,
                block_root: root.into_array(),
                ssz: Bytes::from(ssz.clone()),
            })
            .await
            .expect("caller block_root is the bind; storage must not decode the body");
        let rt = engine.read().unwrap();
        assert_eq!(
            get_block_by_root(&rt, &root).unwrap().as_deref(),
            Some(ssz.as_slice()),
            "stored key must be the caller's block_root"
        );
        let _ = shutdown_tx.send(true);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
