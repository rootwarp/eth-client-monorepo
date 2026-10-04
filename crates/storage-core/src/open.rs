//! Public store open + durable-set load ([ARCH] §4.2 / S2-J-01).
//!
//! `bin/beacon-core` calls [`open`] **before** any subsystem starts. Schema and
//! config-digest gates run here. A [`NodeIdExpectation::Present`] mismatch is
//! refused here, before side-key writes. [`PendingStore::pair`] stamps a legacy
//! id that was absent or already equal.

use std::fmt;
use std::path::Path;
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};

use cc_store::blocks::{get_block_by_root, get_state_root};
use cc_store::canonical::get_canonical;
use cc_store::columns::{DaStatus, get_da_status};
use cc_store::engine::{Durability, Engine, EngineOptions};
use cc_store::meta::{
    ForkChoiceScalars, KEY_FC_SCALARS, KEY_WRITE_CURSOR, Split, TABLE_META, WriteCursor,
};
use cc_store::snapshots::newest_snapshot;
use cc_store::{
    AnchorGenesisValidatorsRoot, Slot, SszDecode, Store, StoreOpenOptions, compute_identity_digest,
    compute_schedule_digest, legacy_config_digest, legacy_open_needs_anchor_witness,
    reconcile_config_side_keys,
};
use cc_types::{ChainConfig, Root};
use tokio::sync::watch;

use crate::archive_write::ArchiveWriter;
use crate::durable_set::{
    DurableDaStatus, DurableSetContext, ItemAssessment, refuse_missing_key_if_anchor_present,
};
use crate::metrics::StorageMetrics;
use crate::node_id::{NodeIdExpectation, NodeIdScheme};
use crate::resume::{self, ResumeError};
use crate::writer::{WriterBounds, WriterFaults, WriterHandle, WriterStop, spawn_writer};

/// Test stall inside [`StorageRuntime::resume`]'s blocking load.
///
/// A non-zero value sleeps on the blocking pool so a current-thread probe can
/// prove the runtime worker is not occupied. Production stays at zero.
#[cfg(test)]
static RESUME_BLOCKING_STALL_MS: AtomicU64 = AtomicU64::new(0);

/// Options for [`open`]. Fail-closed gates match the storage host.
///
/// [`Debug`] is hand-written. [`OpenOpts::node_id`] redacts a present root.
#[derive(Clone)]
pub struct OpenOpts {
    /// Engine durability token (`immediate` | `paranoid`).
    pub durability: String,
    /// Run §2.7 invariants at open. A Present `I-node-id` mismatch is refused in
    /// [`open`]; [`PendingStore::pair`] stamps an id that was absent or already equal.
    pub check_invariants: bool,
    /// Snapshot ring depth for `I-ring`.
    pub snapshot_ring: u64,
    /// Per-check row cap at open (`storage.max_open_scan_rows`).
    pub max_open_scan_rows: u64,
    /// Network identity for the running digest (`0x` + 64 hex).
    ///
    /// Absent is not [`Root::ZERO`]. A populated store without this value is refused.
    pub genesis_validators_root: Option<String>,
    /// Legacy node-id expectation. Compared in [`PendingStore::pair`], not here.
    pub node_id: NodeIdExpectation,
    /// Running network for the identity and schedule digests, and for
    /// `seconds_per_slot` on the writer deadline.
    ///
    /// `None` is not a fixture fallback. `meta.config_digest` stays
    /// [`legacy_config_digest`] either way. The commit deadline is
    /// `2 * chain.seconds_per_slot` only when this is `Some`.
    pub chain: Option<ChainConfig>,
}

impl fmt::Debug for OpenOpts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenOpts")
            .field("durability", &self.durability)
            .field("check_invariants", &self.check_invariants)
            .field("snapshot_ring", &self.snapshot_ring)
            .field("max_open_scan_rows", &self.max_open_scan_rows)
            .field("genesis_validators_root", &self.genesis_validators_root)
            .field("node_id", &self.node_id)
            .field(
                "chain",
                &self.chain.as_ref().map(|chain| chain.config_name.as_str()),
            )
            .finish()
    }
}

impl Default for OpenOpts {
    fn default() -> Self {
        Self {
            durability: "immediate".to_owned(),
            check_invariants: true,
            snapshot_ring: 4,
            max_open_scan_rows: cc_store::DEFAULT_MAX_OPEN_SCAN_ROWS,
            genesis_validators_root: None,
            node_id: NodeIdExpectation::Unset,
            chain: None,
        }
    }
}

/// One opened redb handle. The composer starts subsystems only after this exists.
///
/// [`Debug`] is hand-written: `expected_node_id` is the raw node key.
pub struct OpenedStore {
    store: Store,
    snapshot_ring: u64,
    max_open_scan_rows: u64,
    expected_node_id: Option<Root>,
    /// Identity digest of [`OpenOpts::chain`], when both the network and a GVR were supplied.
    identity_digest: Option<Root>,
    /// Schedule digest of [`OpenOpts::chain`], when a network was supplied.
    schedule_digest: Option<Root>,
    /// Slot length of [`OpenOpts::chain`], else [`crate::prune::DEFAULT_SECONDS_PER_SLOT`].
    ///
    /// When a chain is supplied the commit deadline is `2 * seconds_per_slot`.
    /// Do not hardcode the slot length: Hoodi and mainnet happen to be 12.
    seconds_per_slot: u64,
}

impl fmt::Debug for OpenedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenedStore")
            .field("store", &self.store)
            .field("snapshot_ring", &self.snapshot_ring)
            .field("max_open_scan_rows", &self.max_open_scan_rows)
            .field(
                "expected_node_id",
                &self.expected_node_id.as_ref().map(|_| "<redacted>"),
            )
            .field("identity_digest", &self.identity_digest)
            .field("schedule_digest", &self.schedule_digest)
            .field("seconds_per_slot", &self.seconds_per_slot)
            .finish()
    }
}

impl OpenedStore {
    /// Borrow the engine (storage-core only; chain-core never names this type).
    #[must_use]
    pub fn engine(&self) -> &Engine {
        self.store.engine()
    }

    /// Consume into the [`Store`] (storage host).
    #[must_use]
    pub fn into_store(self) -> Store {
        self.store
    }

    /// Consume into the engine for the single writer.
    #[must_use]
    pub fn into_engine(self) -> Engine {
        self.store.into_engine()
    }

    /// Legacy root passed to [`PendingStore::pair`], if it was [`NodeIdExpectation::Present`].
    #[must_use]
    pub fn configured_node_id(&self) -> Option<Root> {
        self.expected_node_id
    }

    /// Persist identity so a later [`open`] can run `I-node-id`.
    ///
    /// Writes dedicated `meta.node_id`. If `AnchorInfo` already exists, only
    /// its `node_id` field is updated (refuse overwrite of a different id).
    /// A missing `AnchorInfo` is **not** created — `I-contig` stays vacuous.
    pub fn persist_anchor_node_id(&self, node_id: Root) -> anyhow::Result<()> {
        use cc_store::meta::{AnchorInfo, KEY_ANCHOR_INFO, KEY_NODE_ID, TABLE_META};
        use cc_store::{SszDecode, SszEncode};
        let engine = self.store.engine();
        let (existing_anchor, existing_id) = {
            let rt = engine
                .read()
                .map_err(|e| anyhow::anyhow!("read identity: {e}"))?;
            let id = rt
                .get(TABLE_META, KEY_NODE_ID.as_bytes())
                .map_err(|e| anyhow::anyhow!("read node_id: {e}"))?
                .map(|b| {
                    Root::from_ssz_bytes(&b).map_err(|e| anyhow::anyhow!("node_id decode: {e:?}"))
                })
                .transpose()?;
            let anchor = rt
                .get(TABLE_META, KEY_ANCHOR_INFO.as_bytes())
                .map_err(|e| anyhow::anyhow!("read AnchorInfo: {e}"))?
                .map(|b| {
                    AnchorInfo::from_ssz_bytes(&b)
                        .map_err(|e| anyhow::anyhow!("AnchorInfo decode: {e:?}"))
                })
                .transpose()?;
            (anchor, id)
        };
        if let Some(stored) = existing_id
            && stored != Root::ZERO
            && stored != node_id
        {
            anyhow::bail!(
                "I-node-id (crates/store/src/invariants.rs): refuse persist overwrite \
                 of stored node_id <redacted>"
            );
        }
        if let Some(anchor) = &existing_anchor
            && anchor.node_id != Root::ZERO
            && anchor.node_id != node_id
        {
            anyhow::bail!(
                "I-node-id (crates/store/src/invariants.rs): refuse persist overwrite \
                 of stored AnchorInfo.node_id <redacted>"
            );
        }
        let mut batch = engine.batch();
        batch.put(TABLE_META, KEY_NODE_ID.as_bytes(), &node_id.as_ssz_bytes());
        if let Some(mut anchor) = existing_anchor {
            anchor.node_id = node_id;
            batch.put(
                TABLE_META,
                KEY_ANCHOR_INFO.as_bytes(),
                &anchor.as_ssz_bytes(),
            );
        }
        engine
            .commit(batch)
            .map_err(|e| anyhow::anyhow!("persist node_id: {e}"))
    }
}

/// Store whose schema and config-digest gates have passed.
///
/// A [`NodeIdExpectation::Present`] mismatch never produces this handle.
/// [`PendingStore::pair`] stamps an id that is absent or already equal.
/// [`start_writer`] takes [`OpenedStore`], so this handle cannot start a writer.
/// Dropping it without [`PendingStore::pair`] is a debug assertion.
#[must_use = "call pair() before starting a subsystem"]
pub struct PendingStore {
    inner: Option<PendingInner>,
    paired: bool,
}

struct PendingInner {
    store: Store,
    snapshot_ring: u64,
    max_open_scan_rows: u64,
    identity_digest: Option<Root>,
    schedule_digest: Option<Root>,
    seconds_per_slot: u64,
}

impl fmt::Debug for PendingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PendingStore")
            .field("paired", &self.paired)
            .field("open", &self.inner.is_some())
            .finish()
    }
}

impl PendingStore {
    /// Read-only identity. `None` is an unstamped store. No scheme row means
    /// [`NodeIdScheme::Legacy`]. Does not write.
    pub fn peek_node_id(&mut self) -> anyhow::Result<Option<(Root, NodeIdScheme)>> {
        let Some(inner) = self.inner.as_ref() else {
            self.paired = true;
            anyhow::bail!("pending store already consumed");
        };
        match read_stored_node_id(inner.store.engine()) {
            Ok(Some(root)) => Ok(Some((root, NodeIdScheme::Legacy))),
            Ok(None) => Ok(None),
            Err(e) => {
                // A read error must not become a drop assertion on `?`.
                self.paired = true;
                Err(e)
            }
        }
    }

    /// Run `I-node-id` against the legacy root and, on [`NodeIdExpectation::Present`],
    /// stamp `meta.node_id` when the stored id matches or is absent.
    ///
    /// A mismatch does not write. The compare is not gated on `check_invariants`.
    /// [`NodeIdExpectation::Unset`] and [`NodeIdExpectation::ConfiguredButMissing`]
    /// do not stamp. The fingerprint is not compared.
    pub fn pair(mut self, expected: NodeIdExpectation) -> anyhow::Result<OpenedStore> {
        self.paired = true;
        let inner = self
            .inner
            .take()
            .ok_or_else(|| anyhow::anyhow!("pending store already consumed"))?;
        // `open` already refused a Present mismatch. Repeat the compare so a
        // different id passed only to `pair` cannot stamp.
        refuse_legacy_node_id_mismatch(inner.store.engine(), expected)?;
        let opened = OpenedStore {
            store: inner.store,
            snapshot_ring: inner.snapshot_ring,
            max_open_scan_rows: inner.max_open_scan_rows,
            expected_node_id: expected.legacy_root(),
            identity_digest: inner.identity_digest,
            schedule_digest: inner.schedule_digest,
            seconds_per_slot: inner.seconds_per_slot,
        };
        if let Some(id) = expected.legacy_root() {
            opened.persist_anchor_node_id(id)?;
        }
        Ok(opened)
    }
}

impl Drop for PendingStore {
    fn drop(&mut self) {
        // Release the redb lock before the assertion can panic.
        self.inner.take();
        debug_assert!(self.paired, "pending store dropped without pair()");
    }
}

/// Durable seed payload extracted from an opened store.
///
/// Composer-shaped so `chain-core` never names a storage type. Empty store →
/// [`None`] from [`durable_set`] (checkpoint-sync fallback).
#[derive(Debug, Clone)]
pub struct DurableSet {
    /// Snapshot BeaconState SSZ.
    pub state_ssz: Vec<u8>,
    /// Real stored anchor-block SSZ (never a Default body).
    pub anchor_block_ssz: Vec<u8>,
    /// Canonical key that selected `anchor_block_ssz`. Seed checks the body against it.
    pub anchor_block_root: [u8; 32],
    /// Fork tag for the anchor block decode.
    pub anchor_block_fork: u32,
    /// Replay set (ascending), including non-canonical siblings.
    pub blocks: Vec<crate::durable_set::DurableBlock>,
    /// Fork-choice scalars SSZ (ADR-P4-06; ~300 B).
    pub fork_choice_scalars_ssz: Vec<u8>,
    /// Expected head root from persisted scalars.
    pub expected_head_root: [u8; 32],
    /// Expected head slot from persisted scalars.
    pub expected_head_slot: u64,
}

/// Replay payload [`StorageRuntime::resume`] returns.
///
/// Same value as [`DurableSet`]. The runtime owns this load. Callers that
/// only have an [`OpenedStore`] still use [`durable_set`].
pub type DurableSeed = DurableSet;

/// One writer started from an [`OpenedStore`]. Exactly one per process.
///
/// `finalize`, `migrate`, and `prune` are deferred past S2R and are not
/// implemented on this type. The split rule that is implemented is the
/// precondition only: the open unit is flushed before a split row is written.
#[derive(Debug)]
pub struct StorageRuntime {
    engine: Arc<Engine>,
    writer: WriterHandle,
    archive: ArchiveWriter,
    shutdown_tx: watch::Sender<bool>,
    durable_ctx: DurableSetContext,
}

impl StorageRuntime {
    /// Always 1 — the composer must not spawn a second writer.
    #[must_use]
    pub fn writer_count(&self) -> usize {
        1
    }

    /// Live archive ingest (P0 mailbox). S2-A-14 import persist.
    #[must_use]
    pub fn archive(&self) -> ArchiveWriter {
        self.archive.clone()
    }

    /// Borrow the opened engine (same redb as boot).
    #[must_use]
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Signal writer shutdown (tests / pre-drain).
    ///
    /// Does not wait, and does not drain the mailbox. [`Self::drain_and_shutdown`]
    /// is the path that commits the open unit and fsyncs before returning.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }

    /// Classify, build the durable plan, and load the replay set.
    ///
    /// The load runs on `spawn_blocking`, same shape as the snapshot reload in
    /// `replay`: a multi-second decode must not occupy a runtime worker.
    /// `None` is uninitialized (checkpoint). `Incomplete` is an error naming
    /// the `DurableItem`, never an empty store.
    ///
    /// `finalize`, `migrate`, and `prune` are deferred past S2R and are not
    /// run here.
    pub async fn resume(&self, chain: &ChainConfig) -> anyhow::Result<Option<DurableSeed>> {
        let engine = Arc::clone(&self.engine);
        let chain = chain.clone();
        let ctx = self.durable_ctx.clone();
        let loaded = tokio::task::spawn_blocking(move || {
            block_resume_for_test();
            load_durable_seed_sync(&engine, &ctx, &chain)
        })
        .await
        .map_err(|e| anyhow::anyhow!("resume join: {e}"))??;
        Ok(loaded)
    }

    /// Stop admitting, drain the mailbox, and return only after that drain
    /// has finished and the receivers are dropped.
    ///
    /// `Ok` means every drained unit committed. A commit failure, an abort
    /// ([`Self::shutdown`] or a task that already left without draining), and
    /// a writer that disappears before a terminal state are errors.
    /// [`Self::shutdown`] only raises the abort flag and does not wait. After
    /// this method has signaled drain, the writer prefers that drain over the
    /// abort flag.
    ///
    /// `finalize`, `migrate`, and `prune` are deferred past S2R and are not
    /// run here.
    pub async fn drain_and_shutdown(&self) -> anyhow::Result<()> {
        // `close_and_wait` blocks on a condvar. Run it off the runtime so an
        // in-flight submit can still finish and drop its admit guard.
        let writer = self.writer.clone();
        tokio::task::spawn_blocking(move || writer.close_admitting())
            .await
            .map_err(|e| anyhow::anyhow!("drain admit: {e}"))?;
        self.writer.signal_drain();
        let mut done = self.writer.writer_done();
        loop {
            let stop = *done.borrow_and_update();
            match stop {
                WriterStop::Drained => return Ok(()),
                WriterStop::Aborted => {
                    anyhow::bail!("writer aborted before the mailbox was drained");
                }
                WriterStop::CommitFailed => {
                    anyhow::bail!("drained unit commit failed");
                }
                WriterStop::Running => {
                    done.changed()
                        .await
                        .map_err(|_| anyhow::anyhow!("writer exited before drain finished"))?;
                }
            }
        }
    }

    /// Commit and fsync the open unit, then write `new_split`.
    ///
    /// A hot unit already queued is committed before the split row. A hot
    /// unit at or below `new_split.slot` that is still in flight, including
    /// one blocked on the submit lock, is rejected until this returns. The
    /// same exclusion covers every split-writing P1, not only this method.
    /// This is the precondition only.
    ///
    /// `finalize`, `migrate`, and `prune` are deferred past S2R and are not
    /// implemented here. Hot rows are not re-keyed and nothing is pruned.
    /// In-process, the import call is the delivery; the deleted write-behind
    /// cursor algebra is not restored.
    pub async fn flush_before_split(&self, new_split: Split) -> anyhow::Result<()> {
        self.writer
            .flush_split(new_split)
            .await
            .map_err(|e| anyhow::anyhow!("flush before split: {e}"))
    }
}

fn block_resume_for_test() {
    #[cfg(test)]
    {
        let ms = RESUME_BLOCKING_STALL_MS.load(Ordering::SeqCst);
        if ms > 0 {
            std::thread::sleep(std::time::Duration::from_millis(ms));
        }
    }
}

/// Durable write cursor as stored under `meta.write_cursor`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorSnap {
    /// Producer session.
    pub session_id: u64,
    /// Monotonic sequence within the session.
    pub seq: u64,
    /// Slot covered by this cursor.
    pub slot: u64,
    /// Block root at this cursor.
    pub root: [u8; 32],
}

/// Store head, cursor, and selected canonical rows.
///
/// Beacon-core has no fcU/event test spy. A refused `commit_anchor` is checked
/// against this snapshot: the precondition returns before any core work, so
/// these rows must not move.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableFrontier {
    /// Absent when the store has no write cursor.
    pub cursor: Option<CursorSnap>,
    /// `meta.fc_scalars` decoded.
    pub scalars_present: bool,
    /// Fork-choice head root. Zeros when scalars are absent.
    pub head_root: [u8; 32],
    /// Fork-choice head slot. Zero when scalars are absent.
    pub head_slot: u64,
    /// `canonical[slot]` for each requested slot, in order. `None` is no row.
    pub canonical: Vec<Option<[u8; 32]>>,
    /// Body at `canonical[slot]`, aligned with [`Self::canonical`].
    pub bodies: Vec<Option<Vec<u8>>>,
    /// `state_roots[slot]` for each requested slot.
    pub state_roots: Vec<Option<[u8; 32]>>,
    /// DA verdict for `canonical[slot]`'s root. `None` when that row is absent.
    pub da_status: Vec<Option<DurableDaStatus>>,
}

/// Read the durable head, cursor, and canonical rows at `canonical_slots`.
///
/// Does not write. Callers compare two snapshots around a refused commit.
pub fn durable_frontier(
    engine: &Engine,
    canonical_slots: &[u64],
) -> anyhow::Result<DurableFrontier> {
    let rt = engine
        .read()
        .map_err(|e| anyhow::anyhow!("store read: {e}"))?;
    let cursor = match read_meta(&rt, KEY_WRITE_CURSOR)? {
        Some(bytes) => {
            let cursor = WriteCursor::from_ssz_bytes(&bytes)
                .map_err(|e| anyhow::anyhow!("write cursor SSZ: {e:?}"))?;
            Some(CursorSnap {
                session_id: cursor.session_id,
                seq: cursor.seq,
                slot: cursor.slot.as_u64(),
                root: root_bytes(&cursor.root),
            })
        }
        None => None,
    };
    let scalars = match read_meta(&rt, KEY_FC_SCALARS)? {
        Some(bytes) => Some(
            ForkChoiceScalars::from_ssz_bytes(&bytes)
                .map_err(|e| anyhow::anyhow!("fork-choice scalars SSZ: {e:?}"))?,
        ),
        None => None,
    };
    let mut canonical = Vec::with_capacity(canonical_slots.len());
    for slot in canonical_slots {
        let row = get_canonical(&rt, Slot::new(*slot))
            .map_err(|e| anyhow::anyhow!("canonical slot {slot}: {e}"))?;
        canonical.push(row.as_ref().map(root_bytes));
    }
    let mut bodies = Vec::with_capacity(canonical.len());
    let mut state_roots = Vec::with_capacity(canonical.len());
    let mut da_status = Vec::with_capacity(canonical.len());
    for (slot, canon) in canonical_slots.iter().zip(&canonical) {
        let state = get_state_root(&rt, Slot::new(*slot))
            .map_err(|e| anyhow::anyhow!("state root slot {slot}: {e}"))?;
        state_roots.push(state.as_ref().map(root_bytes));
        let Some(root_arr) = canon else {
            bodies.push(None);
            da_status.push(None);
            continue;
        };
        let root = cc_types::Root::from_array(*root_arr);
        let body = get_block_by_root(&rt, &root).map_err(|e| anyhow::anyhow!("block body: {e}"))?;
        bodies.push(body);
        let da = get_da_status(&rt, &root).map_err(|e| anyhow::anyhow!("da status: {e}"))?;
        da_status.push(da.map(|(status, _)| match status {
            DaStatus::Available => DurableDaStatus::Available,
            DaStatus::Deferred => DurableDaStatus::Deferred,
        }));
    }
    Ok(DurableFrontier {
        cursor,
        scalars_present: scalars.is_some(),
        head_root: scalars
            .as_ref()
            .map(|s| root_bytes(&s.head_root))
            .unwrap_or([0; 32]),
        head_slot: scalars.as_ref().map(|s| s.head_slot.as_u64()).unwrap_or(0),
        canonical,
        bodies,
        state_roots,
        da_status,
    })
}

/// Read [`durable_frontier`] after the writer has released the file.
///
/// This is the engine open, not `boot`: a second `boot` is one bootstrap
/// per process, and a populated store without a genesis validators root is
/// refused by the boot gates. The committed rows are still in the file.
pub fn reopen_durable_frontier(
    data_dir: &Path,
    canonical_slots: &[u64],
) -> anyhow::Result<DurableFrontier> {
    let engine = Engine::open(data_dir, EngineOptions::default())
        .map_err(|e| anyhow::anyhow!("reopen store: {e}"))?;
    durable_frontier(&engine, canonical_slots)
}

fn read_meta(rt: &cc_store::engine::ReadTxn, key: &str) -> anyhow::Result<Option<Vec<u8>>> {
    rt.get(TABLE_META, key.as_bytes())
        .map_err(|e| anyhow::anyhow!("meta {key}: {e}"))
}

fn root_bytes(root: &cc_types::Root) -> [u8; 32] {
    let mut out = [0u8; 32];
    out.copy_from_slice(root.as_slice());
    out
}

/// Open (or create) the store. Schema and config-digest gates run here.
///
/// A [`NodeIdExpectation::Present`] mismatch is refused before side-key writes.
/// [`StoreOpenOptions::with_expected_node_id`] stays `None`: the store layer
/// does not own this compare. [`PendingStore::pair`] is still mandatory and
/// stamps a legacy id that was absent or already equal.
pub fn open(data_dir: impl AsRef<Path>, opts: OpenOpts) -> anyhow::Result<PendingStore> {
    let data_dir = data_dir.as_ref();
    let durability =
        Durability::parse(&opts.durability).map_err(|e| anyhow::anyhow!("durability: {e}"))?;
    // Absent GVR stays absent. Root::ZERO is the legacy constant's input, not a substitute.
    let gvr = parse_gvr(opts.genesis_validators_root.as_deref())?;
    tracing::debug!(node_id = ?opts.node_id, "store open");
    // The on-disk legacy key stays the constant so a pre-fold binary still opens.
    // The running network is digested separately and is not written under that key.
    let store_opts = StoreOpenOptions::with_digest(
        EngineOptions::default().with_durability(durability),
        legacy_config_digest(),
    )
    .with_check_invariants(opts.check_invariants)
    .with_snapshot_ring(opts.snapshot_ring.max(1))
    .with_max_open_scan_rows(opts.max_open_scan_rows.max(1))
    .with_expected_node_id(None);
    let store =
        Store::open(data_dir, store_opts).map_err(|e| anyhow::anyhow!("store open: {e}"))?;
    refuse_missing_key_if_anchor_present(store.engine(), opts.node_id)
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    // Side keys are one-shot. Refuse a different legacy id before any of them
    // are written, including when `check_invariants` is false. A matching or
    // absent id still hits the digest gates below, so a wrong network is
    // refused before pair can stamp node_id.
    refuse_legacy_node_id_mismatch(store.engine(), opts.node_id)?;
    let anchor_gvr = anchor_genesis_validators_root(store.engine(), opts.chain.as_ref(), gvr)
        .map_err(|e| anyhow::anyhow!("store open: {e}"))?;
    reconcile_config_side_keys(store.engine(), opts.chain.as_ref(), gvr, anchor_gvr)
        .map_err(|e| anyhow::anyhow!("store open: {e}"))?;
    let (identity_digest, schedule_digest) = match &opts.chain {
        Some(chain) => {
            let schedule = compute_schedule_digest(chain)
                .map_err(|e| anyhow::anyhow!("schedule digest: {e}"))?;
            let identity = gvr.map(|gvr| compute_identity_digest(chain, gvr));
            if let Some(identity) = identity {
                tracing::debug!(
                    identity = %identity,
                    schedule = %schedule,
                    config_name = %chain.config_name,
                    "running config digests compared under side keys (ADR-R-11)"
                );
            } else {
                tracing::debug!(
                    schedule = %schedule,
                    config_name = %chain.config_name,
                    "schedule digest from the running config; identity skipped without a GVR"
                );
            }
            (identity, Some(schedule))
        }
        None => (None, None),
    };
    let seconds_per_slot = opts
        .chain
        .as_ref()
        .map(|chain| chain.seconds_per_slot.max(1))
        .unwrap_or(crate::prune::DEFAULT_SECONDS_PER_SLOT.max(1));
    Ok(PendingStore {
        paired: false,
        inner: Some(PendingInner {
            store,
            snapshot_ring: opts.snapshot_ring.max(1),
            max_open_scan_rows: opts.max_open_scan_rows.max(1),
            identity_digest,
            schedule_digest,
            seconds_per_slot,
        }),
    })
}

/// `Present` versus the stored legacy id. Absent, or equal, is not a refusal.
/// Not gated on `check_invariants`. Does not write.
fn refuse_legacy_node_id_mismatch(
    engine: &Engine,
    expected: NodeIdExpectation,
) -> anyhow::Result<()> {
    let Some(want) = expected.legacy_root() else {
        return Ok(());
    };
    if let Some(found) = read_stored_node_id(engine)?
        && found != want
    {
        anyhow::bail!(
            "I-node-id (crates/store/src/invariants.rs): \
             stored node_id <redacted> does not match the configured node key"
        );
    }
    Ok(())
}

/// `meta.node_id`, else `AnchorInfo.node_id`. Same preference as `I-node-id`.
fn read_stored_node_id(engine: &Engine) -> anyhow::Result<Option<Root>> {
    use cc_store::SszDecode;
    use cc_store::meta::{AnchorInfo, KEY_ANCHOR_INFO, KEY_NODE_ID, TABLE_META};
    let rt = engine
        .read()
        .map_err(|e| anyhow::anyhow!("read identity: {e}"))?;
    if let Some(bytes) = rt
        .get(TABLE_META, KEY_NODE_ID.as_bytes())
        .map_err(|e| anyhow::anyhow!("read node_id: {e}"))?
    {
        let id =
            Root::from_ssz_bytes(&bytes).map_err(|e| anyhow::anyhow!("node_id decode: {e:?}"))?;
        return Ok(Some(id));
    }
    if let Some(bytes) = rt
        .get(TABLE_META, KEY_ANCHOR_INFO.as_bytes())
        .map_err(|e| anyhow::anyhow!("read AnchorInfo: {e}"))?
    {
        let anchor = AnchorInfo::from_ssz_bytes(&bytes)
            .map_err(|e| anyhow::anyhow!("AnchorInfo decode: {e:?}"))?;
        return Ok(Some(anchor.node_id));
    }
    Ok(None)
}

/// Load the durable set from an already-opened store. `None` = uninitialized (checkpoint).
///
/// `Incomplete` is an error naming the `DurableItem`. It is not `None`.
/// [`StorageRuntime::resume`] is this load once the writer exists.
pub fn durable_set(db: &OpenedStore, chain: &ChainConfig) -> anyhow::Result<Option<DurableSet>> {
    load_durable_seed_sync(db.store.engine(), &durable_ctx_from_opened(db), chain)
}

fn durable_ctx_from_opened(db: &OpenedStore) -> DurableSetContext {
    DurableSetContext {
        expected_node_id: db.expected_node_id,
        // Missing-key refusal already ran in `open`. The path is not retained.
        node_key_path: None,
        enr_seq_path: None,
        snapshot_ring: db.snapshot_ring,
        max_open_scan_rows: db.max_open_scan_rows,
        da_status_roots: Vec::new(),
    }
}

fn load_durable_seed_sync(
    engine: &Engine,
    ctx: &DurableSetContext,
    chain: &ChainConfig,
) -> anyhow::Result<Option<DurableSet>> {
    match resume::classify(engine).map_err(|e| anyhow::anyhow!("{e}"))? {
        resume::RestartState::Uninitialized => return Ok(None),
        resume::RestartState::Incomplete(assessment) => {
            let detail = match assessment {
                ItemAssessment::NamedFailure { detail, .. }
                | ItemAssessment::Degradation { detail, .. } => detail,
                ItemAssessment::Present => "restart classification incomplete".to_owned(),
            };
            anyhow::bail!("{detail}");
        }
        resume::RestartState::Complete => {}
    }
    let plan = resume::build_durable_plan(engine, ctx, chain).map_err(map_resume)?;
    if plan.empty {
        return Ok(None);
    }
    Ok(Some(DurableSet {
        state_ssz: plan.state_ssz,
        anchor_block_ssz: plan.anchor_block_ssz,
        anchor_block_root: plan.anchor_block_root,
        anchor_block_fork: plan.anchor_block_fork,
        blocks: plan.blocks,
        fork_choice_scalars_ssz: plan.fork_choice_scalars_ssz,
        expected_head_root: plan.expected_head_root,
        expected_head_slot: plan.expected_head_slot,
    }))
}

/// Start the **one** writer against the paired handle. Call only after [`PendingStore::pair`].
pub fn start_writer(
    db: OpenedStore,
    metrics: StorageMetrics,
    process_fatal: bool,
) -> StorageRuntime {
    start_writer_with_faults(db, metrics, process_fatal, WriterFaults::default())
}

fn start_writer_with_faults(
    db: OpenedStore,
    metrics: StorageMetrics,
    process_fatal: bool,
    faults: WriterFaults,
) -> StorageRuntime {
    let seconds_per_slot = db.seconds_per_slot;
    let durable_ctx = durable_ctx_from_opened(&db);
    start_writer_on_engine(
        Arc::new(db.into_engine()),
        metrics,
        process_fatal,
        seconds_per_slot,
        durable_ctx,
        faults,
    )
}

/// Start the writer on an already-opened [`cc_store::Store`] (A-13 → A-14).
pub fn start_writer_from_store(
    store: cc_store::Store,
    metrics: StorageMetrics,
    process_fatal: bool,
) -> StorageRuntime {
    start_writer_on_engine(
        Arc::new(store.into_engine()),
        metrics,
        process_fatal,
        crate::prune::DEFAULT_SECONDS_PER_SLOT.max(1),
        DurableSetContext::new(),
        WriterFaults::default(),
    )
}

fn start_writer_on_engine(
    engine: Arc<Engine>,
    metrics: StorageMetrics,
    process_fatal: bool,
    seconds_per_slot: u64,
    durable_ctx: DurableSetContext,
    faults: WriterFaults,
) -> StorageRuntime {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    if let Err(e) = ArchiveWriter::ensure_write_cursor(&engine) {
        tracing::warn!(error = %e, "ensure_write_cursor failed");
    }
    let writer = spawn_writer(
        Arc::clone(&engine),
        metrics,
        WriterBounds::default(),
        faults,
        shutdown_rx,
        process_fatal,
    );
    let archive = ArchiveWriter::new(writer.clone(), Arc::clone(&engine))
        .with_seconds_per_slot(seconds_per_slot);
    StorageRuntime {
        engine,
        writer,
        archive,
        shutdown_tx,
        durable_ctx,
    }
}

fn map_resume(e: ResumeError) -> anyhow::Error {
    anyhow::anyhow!("{e}")
}

/// Decode the anchor witness only when a populated store lacks both side keys
/// and still holds the legacy constant. The result cannot authorize a stamp.
fn anchor_genesis_validators_root(
    engine: &Engine,
    chain: Option<&ChainConfig>,
    gvr: Option<Root>,
) -> Result<AnchorGenesisValidatorsRoot, cc_store::StoreError> {
    if gvr.is_none() {
        return Ok(AnchorGenesisValidatorsRoot::Missing);
    }
    let Some(chain) = chain else {
        return Ok(AnchorGenesisValidatorsRoot::Missing);
    };
    if !legacy_open_needs_anchor_witness(engine)? {
        return Ok(AnchorGenesisValidatorsRoot::Missing);
    }
    let rt = engine.read()?;
    let Some((_, ssz)) = newest_snapshot(&rt)? else {
        return Ok(AnchorGenesisValidatorsRoot::Missing);
    };
    drop(rt);
    Ok(
        match crate::replay::genesis_validators_root_from_state_ssz(chain.preset_base, &ssz) {
            Some(root) => AnchorGenesisValidatorsRoot::Value(root),
            None => AnchorGenesisValidatorsRoot::Undecodable,
        },
    )
}

fn parse_gvr(s: Option<&str>) -> anyhow::Result<Option<Root>> {
    let Some(raw) = s.filter(|s| !s.is_empty()) else {
        return Ok(None);
    };
    let hex = raw.strip_prefix("0x").unwrap_or(raw);
    if hex.len() != 64 {
        anyhow::bail!(
            "genesis_validators_root must be 32-byte hex, got len {}",
            hex.len()
        );
    }
    let mut arr = [0u8; 32];
    for i in 0..32 {
        arr[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
            .map_err(|e| anyhow::anyhow!("genesis_validators_root hex: {e}"))?;
    }
    Ok(Some(Root::from_array(arr)))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use std::path::PathBuf;

    use crate::test_tmpdir::unique_temp_dir;
    use cc_store::schedule_side_key_bytes;
    use prometheus_client::registry::Registry;

    /// Tests pair immediately. Call [`super::open`] to hold the pending handle.
    fn open(data_dir: impl AsRef<Path>, opts: OpenOpts) -> anyhow::Result<OpenedStore> {
        let expectation = opts.node_id;
        let mut pending = super::open(data_dir, opts)?;
        let _ = pending.peek_node_id()?;
        pending.pair(expectation)
    }

    fn test_opts(node_id: NodeIdExpectation) -> OpenOpts {
        OpenOpts {
            durability: "immediate".to_owned(),
            check_invariants: true,
            snapshot_ring: 4,
            max_open_scan_rows: cc_store::DEFAULT_MAX_OPEN_SCAN_ROWS,
            genesis_validators_root: None,
            node_id,
            chain: None,
        }
    }

    #[test]
    fn peek_reads_legacy_stamp_without_writing_again() {
        let dir = unique_temp_dir("pending-peek");
        std::fs::create_dir_all(&dir).unwrap();
        let id = Root::from_array([0x11; 32]);
        let mut pending = super::open(&dir, test_opts(NodeIdExpectation::Present(id))).unwrap();
        assert_eq!(pending.peek_node_id().unwrap(), None);
        drop(pending.pair(NodeIdExpectation::Present(id)).unwrap());

        let mut pending = super::open(&dir, test_opts(NodeIdExpectation::Present(id))).unwrap();
        assert_eq!(
            pending.peek_node_id().unwrap(),
            Some((id, NodeIdScheme::Legacy))
        );
        drop(pending.pair(NodeIdExpectation::Present(id)).unwrap());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn dropping_a_pending_store_without_pair_is_a_debug_failure() {
        let dir = unique_temp_dir("pending-drop");
        std::fs::create_dir_all(&dir).unwrap();
        let pending = super::open(&dir, test_opts(NodeIdExpectation::Unset)).unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(pending)));
        if cfg!(debug_assertions) {
            assert!(result.is_err(), "debug drop without pair must assert");
        } else {
            assert!(result.is_ok(), "release drop without pair must not assert");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn seconds_per_slot_follows_the_supplied_chain() {
        let dir = unique_temp_dir("slot-len");
        std::fs::create_dir_all(&dir).unwrap();
        let mut chain = fixture_chain("hoodi-config.yaml");
        chain.seconds_per_slot = 6;
        let mut opts = test_opts(NodeIdExpectation::Unset);
        opts.chain = Some(chain);
        let opened = open(&dir, opts).unwrap();
        assert_eq!(opened.seconds_per_slot, 6);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// History-less store, legacy id already stamped, no side keys. A different
    /// `Present` id must refuse before `stamp_empty` writes them. The compare
    /// is not gated on `check_invariants`.
    #[test]
    fn present_mismatch_on_historyless_store_writes_no_side_keys() {
        use cc_store::SszDecode;
        use cc_store::meta::{KEY_NODE_ID, TABLE_META};

        let dir = unique_temp_dir("node-id-before-side-keys");
        std::fs::create_dir_all(&dir).unwrap();
        let stored = Root::from_array([0x11; 32]);
        let opened = open(&dir, test_opts(NodeIdExpectation::Present(stored))).unwrap();
        drop(opened);
        assert!(meta_is_absent(&dir, cc_store::meta::KEY_CONFIG_DIGEST_V2));
        assert!(meta_is_absent(&dir, cc_store::meta::KEY_SCHEDULE_DIGEST));

        let mut opts = test_opts(NodeIdExpectation::Present(Root::from_array([0x22; 32])));
        opts.check_invariants = false;
        opts.chain = Some(fixture_chain("hoodi-config.yaml"));
        opts.genesis_validators_root = Some(gvr_hex(0x44));
        let err = open(&dir, opts).expect_err("node-id mismatch");
        let msg = err.to_string();
        assert!(
            msg.contains("node_id") || msg.contains("I-node-id"),
            "{msg}"
        );
        assert!(
            meta_is_absent(&dir, cc_store::meta::KEY_CONFIG_DIGEST_V2),
            "config_digest_v2 must stay absent"
        );
        assert!(
            meta_is_absent(&dir, cc_store::meta::KEY_SCHEDULE_DIGEST),
            "schedule side key must stay absent"
        );
        let engine =
            cc_store::engine::Engine::open(&dir, cc_store::engine::EngineOptions::default())
                .unwrap();
        let rt = engine.read().unwrap();
        let id = Root::from_ssz_bytes(
            &rt.get(TABLE_META, KEY_NODE_ID.as_bytes())
                .unwrap()
                .expect("stored node id"),
        )
        .unwrap();
        assert_eq!(id, stored);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_then_second_opener_mismatched_node_id_fails_inode_id() {
        let dir = unique_temp_dir("s2-j-01-inode");
        std::fs::create_dir_all(&dir).unwrap();
        let key_a = dir.join("node_key");
        let id_a = Root::from_array([0xAAu8; 32]);
        std::fs::write(&key_a, id_a.as_slice()).unwrap();

        let opened = open(&dir, test_opts(NodeIdExpectation::Present(id_a))).expect("first open");
        opened.persist_anchor_node_id(id_a).unwrap();
        drop(opened);

        let key_b = dir.join("node_key_b");
        std::fs::write(&key_b, [0xBBu8; 32]).unwrap();
        let err = open(
            &dir,
            test_opts(NodeIdExpectation::Present(Root::from_array([0xBBu8; 32]))),
        )
        .expect_err("I-node-id must refuse");
        let msg = err.to_string();
        assert!(
            msg.contains("node_id") || msg.contains("I-node-id"),
            "second opener must fail I-node-id, got: {msg}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_holds_exclusive_handle() {
        let dir = unique_temp_dir("s2-j-01-excl");
        std::fs::create_dir_all(&dir).unwrap();
        let first = open(&dir, test_opts(NodeIdExpectation::Unset)).expect("first open");
        let err = open(&dir, test_opts(NodeIdExpectation::Unset))
            .expect_err("second live open must fail");
        let msg = err.to_string();
        assert!(
            msg.contains("locked") || msg.contains("Database"),
            "exclusive redb handle: {msg}"
        );
        drop(first);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn start_writer_is_one_handle() {
        let dir = unique_temp_dir("s2-j-01-writer");
        std::fs::create_dir_all(&dir).unwrap();
        let opened = open(&dir, test_opts(NodeIdExpectation::Unset)).unwrap();
        let mut registry = Registry::default();
        let metrics = StorageMetrics::register(&mut registry);
        let rt = start_writer(opened, metrics, false);
        assert_eq!(rt.writer_count(), 1);
        rt.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn put_canonical_slot(opened: &OpenedStore, slot: u64, tag: u8) {
        use cc_store::blocks::{MIN_BLOCK_SSZ_LEN, SLOT_SSZ_OFFSET};
        use cc_store::canonical::put_canonical;
        use cc_store::keys::BlockRegion;
        use cc_store::{Slot, put_block};

        let slot = Slot::new(slot);
        let root = Root::from_array([tag; 32]);
        let mut ssz = vec![0u8; MIN_BLOCK_SSZ_LEN];
        ssz[SLOT_SSZ_OFFSET..SLOT_SSZ_OFFSET + 8].copy_from_slice(&slot.as_u64().to_le_bytes());
        let engine = opened.engine();
        let mut batch = engine.batch();
        {
            let rt = engine.read().unwrap();
            put_block(&rt, &mut batch, slot, &root, &ssz, BlockRegion::Hot, false).unwrap();
            put_canonical(&rt, &mut batch, slot, &root).unwrap();
        }
        engine.commit(batch).unwrap();
    }

    /// Bodies without an anchor are not the checkpoint arm.
    #[test]
    fn durable_set_on_bodies_without_anchor_names_the_item() {
        let dir = unique_temp_dir("bodies-no-anchor");
        std::fs::create_dir_all(&dir).unwrap();
        let opened = open(&dir, test_opts(NodeIdExpectation::Unset)).expect("empty open");
        let chain = fixture_chain("hoodi-config.yaml");
        assert!(durable_set(&opened, &chain).unwrap().is_none());
        put_canonical_slot(&opened, 3, 0xAB);
        let err = durable_set(&opened, &chain).unwrap_err();
        assert!(
            err.to_string().contains("anchor"),
            "incomplete must name the item, not return None: {err}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Gappy parent-walk canonical (slots 100 and 110) must survive same-key
    /// reopen after identity stamp — I-contig must stay vacuous.
    #[test]
    fn persist_node_id_gappy_canonical_same_key_reopen_succeeds() {
        use cc_store::SszDecode;
        use cc_store::meta::{KEY_ANCHOR_INFO, KEY_NODE_ID, TABLE_META};

        let dir = unique_temp_dir("s2-j-01-gappy");
        std::fs::create_dir_all(&dir).unwrap();
        let key_a = dir.join("node_key");
        let id_a = Root::from_array([0xAAu8; 32]);
        std::fs::write(&key_a, id_a.as_slice()).unwrap();

        let opened = open(&dir, test_opts(NodeIdExpectation::Present(id_a))).expect("first open");
        put_canonical_slot(&opened, 100, 0x10);
        put_canonical_slot(&opened, 110, 0x11);
        {
            let rt = opened.engine().read().unwrap();
            assert!(
                rt.get(TABLE_META, KEY_ANCHOR_INFO.as_bytes())
                    .unwrap()
                    .is_none(),
                "fixture must start without AnchorInfo"
            );
        }

        opened.persist_anchor_node_id(id_a).unwrap();
        {
            let rt = opened.engine().read().unwrap();
            assert!(
                rt.get(TABLE_META, KEY_ANCHOR_INFO.as_bytes())
                    .unwrap()
                    .is_none(),
                "identity stamp must not create AnchorInfo"
            );
            let stored = Root::from_ssz_bytes(
                &rt.get(TABLE_META, KEY_NODE_ID.as_bytes())
                    .unwrap()
                    .expect("stamp wrote node_id"),
            )
            .unwrap();
            assert_eq!(stored, id_a);
        }
        drop(opened);

        // Host open refuses: the store holds canonical rows and no GVR.
        // The store layer still opens, so the refusal is not I-contig.
        let err = open(&dir, test_opts(NodeIdExpectation::Present(id_a)))
            .expect_err("populated store must not open on the legacy constant");
        assert!(
            err.to_string().contains("genesis_validators_root"),
            "absent GVR is refused, not Root::ZERO: {err}"
        );
        let direct = Store::open(
            &dir,
            StoreOpenOptions::with_digest(
                cc_store::engine::EngineOptions::default(),
                legacy_config_digest(),
            )
            .with_check_invariants(true)
            .with_expected_node_id(Some(id_a))
            .with_snapshot_ring(4)
            .with_max_open_scan_rows(cc_store::DEFAULT_MAX_OPEN_SCAN_ROWS),
        )
        .expect("I-contig stays vacuous on gappy canonical");
        drop(direct);

        let other = Root::from_array([0xBBu8; 32]);
        let err = open(&dir, test_opts(NodeIdExpectation::Present(other)))
            .expect_err("a different node id is refused before side-key writes");
        assert!(
            err.to_string().contains("node_id") || err.to_string().contains("I-node-id"),
            "Present mismatch is refused before the digest gate: {err}"
        );
        {
            use cc_store::engine::{Engine, EngineOptions};
            let engine = Engine::open(&dir, EngineOptions::default()).unwrap();
            let rt = engine.read().unwrap();
            let stored = Root::from_ssz_bytes(
                &rt.get(TABLE_META, KEY_NODE_ID.as_bytes())
                    .unwrap()
                    .expect("stamp wrote node_id"),
            )
            .unwrap();
            assert_eq!(stored, id_a, "digest refusal must not rewrite node_id");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Populated canonical, no AnchorInfo: stamp must not plant oldest=0.
    #[test]
    fn persist_node_id_on_populated_canonical_does_not_plant_slot_zero() {
        use cc_store::meta::{KEY_ANCHOR_INFO, TABLE_META};

        let dir = unique_temp_dir("s2-j-01-contig");
        std::fs::create_dir_all(&dir).unwrap();
        let key_a = dir.join("node_key");
        let id_a = Root::from_array([0xAAu8; 32]);
        std::fs::write(&key_a, id_a.as_slice()).unwrap();

        let opened = open(&dir, test_opts(NodeIdExpectation::Present(id_a))).expect("first open");
        put_canonical_slot(&opened, 1_000_000, 0xCC);
        opened.persist_anchor_node_id(id_a).unwrap();
        {
            let rt = opened.engine().read().unwrap();
            assert!(
                rt.get(TABLE_META, KEY_ANCHOR_INFO.as_bytes())
                    .unwrap()
                    .is_none(),
                "must not invent AnchorInfo / oldest_block_slot over populated canonical"
            );
        }
        drop(opened);

        let err = open(&dir, test_opts(NodeIdExpectation::Present(id_a)))
            .expect_err("populated store must not open on the legacy constant");
        assert!(
            err.to_string().contains("genesis_validators_root"),
            "absent GVR is refused, not a planted slot 0: {err}"
        );
        let direct = Store::open(
            &dir,
            StoreOpenOptions::with_digest(
                cc_store::engine::EngineOptions::default(),
                legacy_config_digest(),
            )
            .with_check_invariants(true)
            .with_expected_node_id(Some(id_a))
            .with_snapshot_ring(4)
            .with_max_open_scan_rows(cc_store::DEFAULT_MAX_OPEN_SCAN_ROWS),
        )
        .expect("restart at the store layer must not plant slot 0 / fail I-contig");
        drop(direct);

        let other = Root::from_array([0xBBu8; 32]);
        let err = open(&dir, test_opts(NodeIdExpectation::Present(other)))
            .expect_err("a different node id is refused before side-key writes");
        assert!(
            err.to_string().contains("node_id") || err.to_string().contains("I-node-id"),
            "Present mismatch is refused before the digest gate: {err}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 32 consecutive hex digits (a Root / secp256k1 secret), with or without `0x`.
    fn contains_32_byte_hex(text: &str) -> bool {
        let mut run = 0u32;
        for b in text.bytes() {
            if b.is_ascii_hexdigit() {
                run += 1;
                if run >= 64 {
                    return true;
                }
            } else {
                run = 0;
            }
        }
        false
    }

    /// Secret-bearing `meta.node_id` is still compared on open. `{e:#}` walks the
    /// anyhow chain, so a wrapper around the production error must not hide bytes
    /// that were interpolated into a cause.
    #[test]
    fn secret_bearing_node_id_mismatch_chain_redacts_key_bytes() {
        use anyhow::Context;

        let dir = unique_temp_dir("s2r-node-id-redact");
        std::fs::create_dir_all(&dir).unwrap();
        let key_a = dir.join("node_key");
        let secret_a = [0xAAu8; 32];
        let secret_b = [0xBBu8; 32];
        let stored = Root::from_array(secret_a);
        let configured = Root::from_array(secret_b);
        std::fs::write(&key_a, secret_a).unwrap();

        let opened = open(&dir, test_opts(NodeIdExpectation::Present(stored))).expect("first open");
        opened
            .persist_anchor_node_id(stored)
            .expect("stamp secret-bearing meta.node_id");
        let persist_err = opened
            .persist_anchor_node_id(configured)
            .expect_err("persist must refuse a different secret-bearing node_id");
        let persist_chain = format!("{persist_err:#}");
        assert!(
            persist_chain.contains("node_id"),
            "persist mismatch must keep the literal node_id: {persist_chain}"
        );
        assert!(
            !contains_32_byte_hex(&persist_chain),
            "persist chain leaked key bytes: {persist_chain}"
        );
        drop(opened);

        let key_b = dir.join("node_key_b");
        std::fs::write(&key_b, secret_b).unwrap();
        let err = open(&dir, test_opts(NodeIdExpectation::Present(configured)))
            .expect_err("existing secret-bearing meta.node_id must refuse, not be ignored");
        let chain = format!("{err:#}");
        assert!(
            chain.contains("node_id"),
            "open mismatch must keep the literal node_id: {chain}"
        );
        assert!(
            !contains_32_byte_hex(&chain),
            "open chain leaked key bytes: {chain}"
        );
        assert!(
            !chain.contains(&stored.to_string()) && !chain.contains(&configured.to_string()),
            "open chain named a node-id root: {chain}"
        );

        let wrapped = Err::<(), _>(err)
            .context("outer wrapper")
            .expect_err("wrapper keeps the cause");
        let wrapped_chain = format!("{wrapped:#}");
        assert!(
            wrapped_chain.contains("outer wrapper") && wrapped_chain.contains("node_id"),
            "formatted chain must include the cause, not only the wrapper: {wrapped_chain}"
        );
        assert!(
            !contains_32_byte_hex(&wrapped_chain),
            "wrapper chain leaked key bytes: {wrapped_chain}"
        );
    }

    /// `AnchorInfo.node_id` is the same secret-bearing surface when `meta.node_id`
    /// is absent. Persist refusal must still name `node_id` and omit the bytes.
    #[test]
    fn persist_anchor_info_node_id_mismatch_redacts_bytes() {
        use cc_store::Slot;
        use cc_store::SszEncode;
        use cc_store::meta::{AnchorInfo, KEY_ANCHOR_INFO, TABLE_META};

        let dir = unique_temp_dir("s2r-anchor-node-id-redact");
        std::fs::create_dir_all(&dir).unwrap();
        let opened = open(&dir, test_opts(NodeIdExpectation::Unset)).expect("open");
        let stored = Root::from_array([0xDDu8; 32]);
        let anchor = AnchorInfo {
            anchor_slot: Slot::new(1),
            anchor_root: Root::from_array([1; 32]),
            anchor_state_root: Root::from_array([2; 32]),
            node_id: stored,
            oldest_block_slot: Slot::new(1),
            oldest_block_parent: Root::from_array([3; 32]),
        };
        let engine = opened.engine();
        let mut batch = engine.batch();
        batch.put(
            TABLE_META,
            KEY_ANCHOR_INFO.as_bytes(),
            &anchor.as_ssz_bytes(),
        );
        engine.commit(batch).unwrap();

        let err = opened
            .persist_anchor_node_id(Root::from_array([0xEEu8; 32]))
            .expect_err("AnchorInfo.node_id mismatch must refuse");
        let chain = format!("{err:#}");
        assert!(
            chain.contains("AnchorInfo.node_id"),
            "literal node_id must survive: {chain}"
        );
        assert!(
            !contains_32_byte_hex(&chain) && !chain.contains(&stored.to_string()),
            "persist chain leaked AnchorInfo.node_id bytes: {chain}"
        );
    }

    /// `{:?}` of every type that can carry a node id prints `<redacted>`,
    /// not the 32 key bytes.
    #[test]
    fn debug_redacts_expected_node_id() {
        use crate::durable_set::DurableSetContext;
        use cc_store::{InvariantContext, StoreOpenOptions};

        let dir = unique_temp_dir("s2r-node-id-debug");
        std::fs::create_dir_all(&dir).unwrap();
        let key = dir.join("node_key");
        let secret = [0xCCu8; 32];
        let root = Root::from_array(secret);
        std::fs::write(&key, secret).unwrap();
        let opts = test_opts(NodeIdExpectation::Present(root));
        let opened = open(&dir, opts.clone()).expect("open");

        let opened_dbg = format!("{opened:?}");
        assert!(
            opened_dbg.contains("<redacted>"),
            "OpenedStore Debug must redact expected_node_id: {opened_dbg}"
        );
        assert!(
            !opened_dbg.contains(&root.to_string()) && !contains_32_byte_hex(&opened_dbg),
            "OpenedStore Debug leaked key bytes: {opened_dbg}"
        );

        let ctx = InvariantContext {
            expected_node_id: Some(root),
            ..InvariantContext::new()
        };
        let ctx_dbg = format!("{ctx:?}");
        assert!(
            ctx_dbg.contains("expected_node_id") && ctx_dbg.contains("<redacted>"),
            "InvariantContext Debug must keep the field name and redact: {ctx_dbg}"
        );
        assert!(
            !ctx_dbg.contains(&root.to_string()),
            "InvariantContext Debug leaked key bytes: {ctx_dbg}"
        );

        let durable = DurableSetContext {
            expected_node_id: Some(root),
            node_key_path: Some(key.clone()),
            ..DurableSetContext::new()
        };
        let durable_dbg = format!("{durable:?}");
        assert!(
            durable_dbg.contains("expected_node_id") && durable_dbg.contains("<redacted>"),
            "DurableSetContext Debug must redact: {durable_dbg}"
        );
        assert!(
            !durable_dbg.contains(&root.to_string()),
            "DurableSetContext Debug leaked key bytes: {durable_dbg}"
        );

        let opts_dbg = format!("{opts:?}");
        assert!(
            opts_dbg.contains("OpenOpts")
                && opts_dbg.contains("node_id")
                && opts_dbg.contains("<redacted>"),
            "OpenOpts Debug must redact a present node id: {opts_dbg}"
        );
        assert!(
            !opts_dbg.contains(&root.to_string()) && !contains_32_byte_hex(&opts_dbg),
            "OpenOpts Debug leaked key bytes: {opts_dbg}"
        );

        let store_opts = StoreOpenOptions::with_digest(
            cc_store::engine::EngineOptions::default(),
            legacy_config_digest(),
        )
        .with_expected_node_id(Some(root));
        let store_dbg = format!("{store_opts:?}");
        assert!(
            store_dbg.contains("<redacted>") && !store_dbg.contains(&root.to_string()),
            "StoreOpenOptions Debug leaked expected_node_id: {store_dbg}"
        );

        drop(opened);
    }

    fn fixture_chain(name: &str) -> ChainConfig {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../crates/types/tests/fixtures")
            .join(name);
        ChainConfig::from_yaml_file(&path).expect("fixture chain yaml")
    }

    fn gvr_hex(byte: u8) -> String {
        format!("0x{}", hex_byte(byte).repeat(32))
    }

    fn hex_byte(byte: u8) -> String {
        format!("{byte:02x}")
    }

    fn stored_config_digest(opened: &OpenedStore) -> Root {
        use cc_store::SszDecode;
        use cc_store::meta::{ConfigDigest, KEY_CONFIG_DIGEST, TABLE_META};
        let rt = opened.engine().read().unwrap();
        let bytes = rt
            .get(TABLE_META, KEY_CONFIG_DIGEST.as_bytes())
            .unwrap()
            .unwrap();
        ConfigDigest::from_ssz_bytes(&bytes).unwrap().digest
    }

    #[test]
    fn open_digests_the_supplied_network_and_keeps_the_legacy_key() {
        let gvr = gvr_hex(0x11);
        let hoodi_dir = unique_temp_dir("digest-hoodi");
        let mainnet_dir = unique_temp_dir("digest-mainnet");
        std::fs::create_dir_all(&hoodi_dir).unwrap();
        std::fs::create_dir_all(&mainnet_dir).unwrap();

        let mut hoodi_opts = test_opts(NodeIdExpectation::Unset);
        hoodi_opts.chain = Some(fixture_chain("hoodi-config.yaml"));
        hoodi_opts.genesis_validators_root = Some(gvr.clone());
        let mut mainnet_opts = test_opts(NodeIdExpectation::Unset);
        mainnet_opts.chain = Some(fixture_chain("mainnet-config.yaml"));
        mainnet_opts.genesis_validators_root = Some(gvr);

        let hoodi = open(&hoodi_dir, hoodi_opts).expect("empty hoodi open");
        let mainnet = open(&mainnet_dir, mainnet_opts).expect("empty mainnet open");
        assert_ne!(
            hoodi.identity_digest, mainnet.identity_digest,
            "two networks must not share an identity digest"
        );
        assert_ne!(
            hoodi.schedule_digest, mainnet.schedule_digest,
            "two networks must not share a schedule digest"
        );
        assert!(hoodi.identity_digest.is_some() && mainnet.identity_digest.is_some());
        let legacy = legacy_config_digest();
        assert_eq!(stored_config_digest(&hoodi), legacy);
        assert_eq!(stored_config_digest(&mainnet), legacy);
        assert_ne!(hoodi.identity_digest, Some(legacy));
        drop(hoodi);
        drop(mainnet);
        let _ = std::fs::remove_dir_all(&hoodi_dir);
        let _ = std::fs::remove_dir_all(&mainnet_dir);
    }

    #[test]
    fn populated_store_without_gvr_is_refused() {
        let dir = unique_temp_dir("populated-no-gvr");
        std::fs::create_dir_all(&dir).unwrap();
        let mut opts = test_opts(NodeIdExpectation::Unset);
        opts.check_invariants = false;
        let opened = open(&dir, opts.clone()).expect("empty open");
        {
            use cc_store::Slot;
            use cc_store::canonical::put_canonical;
            let engine = opened.engine();
            let rt = engine.read().unwrap();
            let mut batch = engine.batch();
            put_canonical(&rt, &mut batch, Slot::new(1), &Root::from_array([9; 32])).unwrap();
            engine.commit(batch).unwrap();
        }
        drop(opened);

        let err = open(&dir, opts).expect_err("populated store without GVR must refuse");
        let msg = err.to_string();
        assert!(
            msg.contains("genesis_validators_root") && msg.contains("not Root::ZERO"),
            "must refuse instead of substituting Root::ZERO: {msg}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// An empty open with a network stamps the side keys. History written
    /// after that stamp reopens by comparing them. `meta.config_digest`
    /// staying the legacy constant is not the permission (ADR-R-11).
    #[test]
    fn populated_store_reopens_on_matching_side_keys_not_the_legacy_constant() {
        let dir = unique_temp_dir("populated-with-gvr");
        std::fs::create_dir_all(&dir).unwrap();
        let mut opts = test_opts(NodeIdExpectation::Unset);
        opts.check_invariants = false;
        opts.chain = Some(fixture_chain("hoodi-config.yaml"));
        opts.genesis_validators_root = Some(gvr_hex(0xab));
        let opened = open(&dir, opts.clone()).expect("empty open stamps side keys");
        {
            use cc_store::Slot;
            use cc_store::canonical::put_canonical;
            let engine = opened.engine();
            let rt = engine.read().unwrap();
            let mut batch = engine.batch();
            put_canonical(&rt, &mut batch, Slot::new(4), &Root::from_array([3; 32])).unwrap();
            engine.commit(batch).unwrap();
        }
        drop(opened);

        let reopened = open(&dir, opts.clone()).expect("matching side keys reopen");
        let gvr = parse_gvr(opts.genesis_validators_root.as_deref())
            .unwrap()
            .unwrap();
        let chain = opts.chain.as_ref().unwrap();
        assert_eq!(
            reopened.identity_digest,
            Some(compute_identity_digest(chain, gvr))
        );
        assert_ne!(reopened.identity_digest, Some(legacy_config_digest()));
        assert_eq!(stored_config_digest(&reopened), legacy_config_digest());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// ADR-R-11: an empty store takes the running config under the two side keys.
    /// `meta.config_digest` stays the legacy constant. `SCHEMA_VERSION` stays 1.
    #[test]
    fn empty_store_stamps_side_keys_and_keeps_legacy_digest() {
        use cc_store::SszDecode;
        use cc_store::meta::{
            KEY_CONFIG_DIGEST_V2, KEY_SCHEDULE_DIGEST, KEY_SCHEMA_VERSION, SchemaVersion,
            TABLE_META,
        };
        use cc_store::{identity_side_key_bytes, schedule_side_key_bytes};

        let dir = unique_temp_dir("side-key-empty");
        std::fs::create_dir_all(&dir).unwrap();
        let mut opts = test_opts(NodeIdExpectation::Unset);
        opts.check_invariants = false;
        let chain = fixture_chain("hoodi-config.yaml");
        let gvr = gvr_hex(0x11);
        opts.chain = Some(chain.clone());
        opts.genesis_validators_root = Some(gvr.clone());
        let opened = open(&dir, opts).expect("empty store stamps side keys");
        let rt = opened.engine().read().unwrap();
        let version = SchemaVersion::from_ssz_bytes(
            &rt.get(TABLE_META, KEY_SCHEMA_VERSION.as_bytes())
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(version.version, 1, "SCHEMA_VERSION is not bumped");
        assert_eq!(stored_config_digest(&opened), legacy_config_digest());
        let gvr_root = parse_gvr(Some(&gvr)).unwrap().unwrap();
        assert_eq!(
            rt.get(TABLE_META, KEY_CONFIG_DIGEST_V2.as_bytes())
                .unwrap()
                .unwrap(),
            identity_side_key_bytes(&chain, gvr_root)
        );
        assert_eq!(
            rt.get(TABLE_META, KEY_SCHEDULE_DIGEST.as_bytes())
                .unwrap()
                .unwrap(),
            schedule_side_key_bytes(&chain).unwrap()
        );
        assert_ne!(KEY_SCHEDULE_DIGEST, "config_schedule_digest");
        assert!(KEY_SCHEDULE_DIGEST.len() <= 16);
        assert_eq!(KEY_CONFIG_DIGEST_V2, "config_digest_v2");
        drop(rt);
        drop(opened);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `open` is the gate before `start_writer`. A populated store whose identity
    /// bucket changed never returns an [`OpenedStore`], so no subsystem starts.
    fn assert_identity_field_refuses(label: &str, mutate: impl FnOnce(&mut ChainConfig)) {
        let gvr = gvr_hex(0x44);
        let chain = fixture_chain("hoodi-config.yaml");
        let (dir, mut opts) = stamped_populated(label, chain, &gvr, None);
        let identity_before = read_meta(&dir, cc_store::meta::KEY_CONFIG_DIGEST_V2);
        let legacy_before = read_meta(&dir, cc_store::meta::KEY_CONFIG_DIGEST);
        mutate(opts.chain.as_mut().unwrap());
        let err = open(&dir, opts).expect_err("identity mismatch must refuse inside open");
        let msg = err.to_string();
        assert!(
            msg.contains("store open") && msg.contains("identity digest mismatch"),
            "refusal must come from open, before any subsystem: {msg}"
        );
        assert_eq!(
            read_meta(&dir, cc_store::meta::KEY_CONFIG_DIGEST_V2),
            identity_before,
            "a refusal must not rewrite the identity side key"
        );
        assert_eq!(
            read_meta(&dir, cc_store::meta::KEY_CONFIG_DIGEST),
            legacy_before
        );
        assert_eq!(schema_version_of(&dir), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn identity_genesis_fork_version_refuses_populated_store() {
        assert_identity_field_refuses("id-gfv", |chain| {
            chain.genesis_fork_version =
                cc_types::ForkVersion::from_array([0x11, 0x22, 0x33, 0x44]);
        });
    }

    #[test]
    fn identity_deposit_contract_address_refuses_populated_store() {
        assert_identity_field_refuses("id-deposit-addr", |chain| {
            chain.deposit_contract_address = cc_types::ExecutionAddress::from_array([0xAB; 20]);
        });
    }

    #[test]
    fn identity_deposit_chain_id_refuses_populated_store() {
        assert_identity_field_refuses("id-deposit-chain", |chain| {
            chain.deposit_chain_id = chain.deposit_chain_id.saturating_add(1);
        });
    }

    #[test]
    fn identity_seconds_per_slot_refuses_populated_store() {
        assert_identity_field_refuses("id-slot-seconds", |chain| {
            chain.seconds_per_slot = 6;
        });
    }

    #[test]
    fn identity_genesis_validators_root_refuses_populated_store() {
        let gvr = gvr_hex(0x44);
        let chain = fixture_chain("hoodi-config.yaml");
        let (dir, mut opts) = stamped_populated("id-gvr", chain, &gvr, None);
        opts.genesis_validators_root = Some(gvr_hex(0x45));
        let err = open(&dir, opts).expect_err("gvr mismatch must refuse inside open");
        let msg = err.to_string();
        assert!(
            msg.contains("store open") && msg.contains("identity digest mismatch"),
            "{msg}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn renaming_config_name_does_not_refuse_populated_store() {
        let gvr = gvr_hex(0x44);
        let chain = fixture_chain("hoodi-config.yaml");
        let (dir, mut opts) = stamped_populated("id-name", chain, &gvr, None);
        let identity_before = read_meta(&dir, cc_store::meta::KEY_CONFIG_DIGEST_V2);
        opts.chain.as_mut().unwrap().config_name = "renamed-hoodi".into();
        let opened = open(&dir, opts).expect("config_name is not identity");
        drop(opened);
        assert_eq!(
            read_meta(&dir, cc_store::meta::KEY_CONFIG_DIGEST_V2),
            identity_before
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn churn_limit_quotient_is_not_an_identity_or_schedule_refusal() {
        let gvr = gvr_hex(0x44);
        let chain = fixture_chain("hoodi-config.yaml");
        let (dir, mut opts) = stamped_populated("id-churn", chain, &gvr, None);
        opts.chain.as_mut().unwrap().churn_limit_quotient = 1;
        open(&dir, opts).expect("the live-payload scalars are in neither bucket");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Identity matches and a schedule move is strictly above finalized, so
    /// reconcile would re-stamp. A different legacy id must refuse first and
    /// leave the schedule bytes alone.
    #[test]
    fn present_mismatch_does_not_restamp_schedule_on_populated_store() {
        use cc_store::SszDecode;
        use cc_store::SszEncode;
        use cc_store::engine::{Engine, EngineOptions};
        use cc_store::meta::{KEY_NODE_ID, TABLE_META};

        let gvr = gvr_hex(0x44);
        let chain = fixture_chain("hoodi-config.yaml");
        let (dir, mut opts) =
            stamped_populated("node-id-before-schedule", chain, &gvr, Some(1_000));
        let stored = Root::from_array([0x11; 32]);
        {
            let engine = Engine::open(&dir, EngineOptions::default()).unwrap();
            let mut batch = engine.batch();
            batch.put(TABLE_META, KEY_NODE_ID.as_bytes(), &stored.as_ssz_bytes());
            engine.commit(batch).unwrap();
        }
        let schedule_before = read_meta(&dir, cc_store::meta::KEY_SCHEDULE_DIGEST);
        opts.node_id = NodeIdExpectation::Present(Root::from_array([0x22; 32]));
        opts.chain.as_mut().unwrap().fulu_fork_epoch = cc_types::Epoch::new(80_000);
        let err = open(&dir, opts).expect_err("node id refuses before schedule re-stamp");
        let msg = err.to_string();
        assert!(
            msg.contains("node_id") || msg.contains("I-node-id"),
            "{msg}"
        );
        assert_eq!(
            read_meta(&dir, cc_store::meta::KEY_SCHEDULE_DIGEST),
            schedule_before
        );
        let engine = Engine::open(&dir, EngineOptions::default()).unwrap();
        let rt = engine.read().unwrap();
        let id =
            Root::from_ssz_bytes(&rt.get(TABLE_META, KEY_NODE_ID.as_bytes()).unwrap().unwrap())
                .unwrap();
        assert_eq!(id, stored);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn fork_epoch_above_finalized_warns_and_restamps_schedule() {
        let gvr = gvr_hex(0x44);
        let chain = fixture_chain("hoodi-config.yaml");
        let (dir, mut opts) = stamped_populated("sched-above", chain, &gvr, Some(1_000));
        let identity_before = read_meta(&dir, cc_store::meta::KEY_CONFIG_DIGEST_V2);
        let legacy_before = read_meta(&dir, cc_store::meta::KEY_CONFIG_DIGEST);
        let schedule_before = read_meta(&dir, cc_store::meta::KEY_SCHEDULE_DIGEST);
        opts.chain.as_mut().unwrap().fulu_fork_epoch = cc_types::Epoch::new(80_000);
        let flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let opened = capture_schedule_restamp(std::sync::Arc::clone(&flag), || {
            open(&dir, opts.clone()).expect("a future fork epoch must not refuse")
        });
        drop(opened);
        assert!(
            flag.load(std::sync::atomic::Ordering::SeqCst),
            "a move above finalized must log at WARN and say it is re-stamping"
        );
        let expected = schedule_side_key_bytes(opts.chain.as_ref().unwrap()).unwrap();
        assert_eq!(
            read_meta(&dir, cc_store::meta::KEY_SCHEDULE_DIGEST),
            expected
        );
        assert_ne!(
            read_meta(&dir, cc_store::meta::KEY_SCHEDULE_DIGEST),
            schedule_before
        );
        assert_eq!(
            read_meta(&dir, cc_store::meta::KEY_CONFIG_DIGEST_V2),
            identity_before
        );
        assert_eq!(
            read_meta(&dir, cc_store::meta::KEY_CONFIG_DIGEST),
            legacy_before
        );
        assert_eq!(schema_version_of(&dir), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn schedule_move_above_decoded_epoch_zero_restamps() {
        let gvr = gvr_hex(0x44);
        let chain = fixture_chain("hoodi-config.yaml");
        let (dir, mut opts) = stamped_populated("sched-epoch-zero", chain, &gvr, Some(0));
        opts.chain.as_mut().unwrap().fulu_fork_epoch = cc_types::Epoch::new(80_000);
        let flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let opened = capture_schedule_restamp(std::sync::Arc::clone(&flag), || {
            open(&dir, opts.clone())
                .expect("a decoded finalized epoch of 0 still proves a later move")
        });
        drop(opened);
        assert!(flag.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(
            read_meta(&dir, cc_store::meta::KEY_SCHEDULE_DIGEST),
            schedule_side_key_bytes(opts.chain.as_ref().unwrap()).unwrap()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn schedule_move_without_fc_scalars_is_not_restamped() {
        let gvr = gvr_hex(0x44);
        let chain = fixture_chain("hoodi-config.yaml");
        let (dir, mut opts) = stamped_populated("sched-no-fc", chain, &gvr, None);
        let schedule_before = read_meta(&dir, cc_store::meta::KEY_SCHEDULE_DIGEST);
        opts.chain.as_mut().unwrap().fulu_fork_epoch = cc_types::Epoch::new(80_000);
        let flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let err = capture_schedule_restamp(std::sync::Arc::clone(&flag), || {
            open(&dir, opts).expect_err("a missing finalized epoch must not re-stamp")
        });
        let msg = err.to_string();
        assert!(
            msg.contains("store open") && msg.contains("fc_scalars") && msg.contains("missing"),
            "{msg}"
        );
        assert!(!flag.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(
            read_meta(&dir, cc_store::meta::KEY_SCHEDULE_DIGEST),
            schedule_before
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn schedule_move_with_undecodable_fc_scalars_is_not_restamped() {
        let gvr = gvr_hex(0x44);
        let chain = fixture_chain("hoodi-config.yaml");
        let (dir, mut opts) = stamped_populated("sched-bad-fc", chain, &gvr, None);
        let schedule_before = read_meta(&dir, cc_store::meta::KEY_SCHEDULE_DIGEST);
        {
            use cc_store::engine::{Engine, EngineOptions};
            use cc_store::meta::{KEY_FC_SCALARS, TABLE_META};
            let engine = Engine::open(&dir, EngineOptions::default()).unwrap();
            let mut batch = engine.batch();
            batch.put(TABLE_META, KEY_FC_SCALARS.as_bytes(), &[0xFF, 0x00]);
            engine.commit(batch).unwrap();
        }
        opts.chain.as_mut().unwrap().fulu_fork_epoch = cc_types::Epoch::new(80_000);
        let flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let err = capture_schedule_restamp(std::sync::Arc::clone(&flag), || {
            open(&dir, opts).expect_err("undecodable fc_scalars must not re-stamp")
        });
        let msg = err.to_string();
        assert!(
            msg.contains("store open")
                && msg.contains("fc_scalars")
                && msg.contains("did not decode"),
            "{msg}"
        );
        assert!(!flag.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(
            read_meta(&dir, cc_store::meta::KEY_SCHEDULE_DIGEST),
            schedule_before
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn fork_epoch_at_or_below_finalized_refuses() {
        let gvr = gvr_hex(0x44);
        let chain = fixture_chain("hoodi-config.yaml");
        // Hoodi fulu is 50688, which is at or below a finalized epoch of 60_000.
        let (dir, mut opts) = stamped_populated("sched-below", chain, &gvr, Some(60_000));
        let schedule_before = read_meta(&dir, cc_store::meta::KEY_SCHEDULE_DIGEST);
        opts.chain.as_mut().unwrap().fulu_fork_epoch = cc_types::Epoch::new(70_000);
        let flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let err = capture_schedule_restamp(std::sync::Arc::clone(&flag), || {
            open(&dir, opts).expect_err("retroactive fork epoch must refuse inside open")
        });
        let msg = err.to_string();
        assert!(
            msg.contains("store open") && msg.contains("at or below finalized epoch"),
            "{msg}"
        );
        assert!(
            !flag.load(std::sync::atomic::Ordering::SeqCst),
            "a refusal must not re-stamp or log the forward update"
        );
        assert_eq!(
            read_meta(&dir, cc_store::meta::KEY_SCHEDULE_DIGEST),
            schedule_before
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn blob_schedule_above_finalized_restamps() {
        let gvr = gvr_hex(0x44);
        let chain = fixture_chain("hoodi-config.yaml");
        let (dir, mut opts) = stamped_populated("blob-above", chain, &gvr, Some(1_000));
        let chain = opts.chain.as_mut().unwrap();
        let mut entries = chain.blob_schedule.entries().to_vec();
        entries.last_mut().unwrap().max_blobs_per_block = 22;
        chain.blob_schedule = cc_types::BlobSchedule::try_from_entries(entries).unwrap();
        let flag = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let opened = capture_schedule_restamp(std::sync::Arc::clone(&flag), || {
            open(&dir, opts.clone()).expect("future BLOB_SCHEDULE edit must not refuse")
        });
        drop(opened);
        assert!(flag.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(
            read_meta(&dir, cc_store::meta::KEY_SCHEDULE_DIGEST),
            schedule_side_key_bytes(opts.chain.as_ref().unwrap()).unwrap()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn blob_schedule_at_or_below_finalized_refuses() {
        let gvr = gvr_hex(0x44);
        let chain = fixture_chain("hoodi-config.yaml");
        let (dir, mut opts) = stamped_populated("blob-below", chain, &gvr, Some(60_000));
        let schedule_before = read_meta(&dir, cc_store::meta::KEY_SCHEDULE_DIGEST);
        let chain = opts.chain.as_mut().unwrap();
        let mut entries = chain.blob_schedule.entries().to_vec();
        entries.last_mut().unwrap().max_blobs_per_block = 22;
        chain.blob_schedule = cc_types::BlobSchedule::try_from_entries(entries).unwrap();
        let err = open(&dir, opts).expect_err("historical BLOB_SCHEDULE edit must refuse");
        assert!(
            err.to_string().contains("at or below finalized epoch"),
            "{}",
            err
        );
        assert_eq!(
            read_meta(&dir, cc_store::meta::KEY_SCHEDULE_DIGEST),
            schedule_before
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn deleting_one_side_key_does_not_restamp() {
        let gvr = gvr_hex(0x44);
        let chain = fixture_chain("hoodi-config.yaml");
        let (dir, opts) = stamped_populated("one-key", chain, &gvr, None);
        let identity_before = read_meta(&dir, cc_store::meta::KEY_CONFIG_DIGEST_V2);
        delete_meta(&dir, cc_store::meta::KEY_SCHEDULE_DIGEST);
        let err = open(&dir, opts).expect_err("one side key must not open a second stamp");
        assert!(
            err.to_string().contains("does not open a second stamp"),
            "{err}"
        );
        assert_eq!(
            read_meta(&dir, cc_store::meta::KEY_CONFIG_DIGEST_V2),
            identity_before
        );
        assert!(meta_is_absent(&dir, cc_store::meta::KEY_SCHEDULE_DIGEST));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn populated_store_whose_legacy_digest_is_not_the_constant_is_refused() {
        let dir = unique_temp_dir("non-legacy");
        std::fs::create_dir_all(&dir).unwrap();
        let mut opts = test_opts(NodeIdExpectation::Unset);
        opts.check_invariants = false;
        let opened = open(&dir, opts.clone()).expect("empty open without a network");
        drop(opened);
        overwrite_config_digest(&dir, Root::from_array([0xEE; 32]));
        {
            use cc_store::Slot;
            use cc_store::canonical::put_canonical;
            use cc_store::engine::{Engine, EngineOptions};
            let engine = Engine::open(&dir, EngineOptions::default()).unwrap();
            let rt = engine.read().unwrap();
            let mut batch = engine.batch();
            put_canonical(&rt, &mut batch, Slot::new(3), &Root::from_array([5; 32])).unwrap();
            engine.commit(batch).unwrap();
        }
        opts.chain = Some(fixture_chain("hoodi-config.yaml"));
        opts.genesis_validators_root = Some(gvr_hex(0x44));
        let err = open(&dir, opts).expect_err("a non-legacy digest must not be re-stamped");
        assert!(err.to_string().contains("config digest mismatch"), "{err}");
        assert!(meta_is_absent(&dir, cc_store::meta::KEY_CONFIG_DIGEST_V2));
        assert!(meta_is_absent(&dir, cc_store::meta::KEY_SCHEDULE_DIGEST));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn legacy_constant_without_anchor_state_is_not_restamped() {
        let dir = unique_temp_dir("legacy-no-anchor");
        std::fs::create_dir_all(&dir).unwrap();
        let mut opts = test_opts(NodeIdExpectation::Unset);
        opts.check_invariants = false;
        let opened = open(&dir, opts.clone()).expect("empty");
        {
            use cc_store::Slot;
            use cc_store::canonical::put_canonical;
            let engine = opened.engine();
            let rt = engine.read().unwrap();
            let mut batch = engine.batch();
            put_canonical(&rt, &mut batch, Slot::new(8), &Root::from_array([6; 32])).unwrap();
            engine.commit(batch).unwrap();
        }
        drop(opened);
        opts.chain = Some(fixture_chain("hoodi-config.yaml"));
        opts.genesis_validators_root = Some(gvr_hex(0x44));
        let err = open(&dir, opts).expect_err("no anchor witness, no re-stamp");
        let msg = err.to_string();
        assert!(
            msg.contains("populated store refused") && msg.contains("cross-check"),
            "{msg}"
        );
        assert!(meta_is_absent(&dir, cc_store::meta::KEY_CONFIG_DIGEST_V2));
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn stamped_populated(
        label: &str,
        chain: ChainConfig,
        gvr: &str,
        finalized_epoch: Option<u64>,
    ) -> (PathBuf, OpenOpts) {
        let dir = unique_temp_dir(label);
        std::fs::create_dir_all(&dir).unwrap();
        let mut opts = test_opts(NodeIdExpectation::Unset);
        opts.check_invariants = false;
        opts.chain = Some(chain);
        opts.genesis_validators_root = Some(gvr.to_owned());
        let opened = open(&dir, opts.clone()).expect("stamp empty store");
        {
            use cc_store::Slot;
            use cc_store::SszEncode;
            use cc_store::canonical::put_canonical;
            use cc_store::meta::{ForkChoiceScalars, KEY_FC_SCALARS, TABLE_META};
            use cc_types::{Checkpoint, Epoch};
            let engine = opened.engine();
            let rt = engine.read().unwrap();
            let mut batch = engine.batch();
            put_canonical(&rt, &mut batch, Slot::new(32), &Root::from_array([4; 32])).unwrap();
            if let Some(epoch) = finalized_epoch {
                let scalars = ForkChoiceScalars {
                    finalized: Checkpoint {
                        epoch: Epoch::new(epoch),
                        root: Root::from_array([1; 32]),
                    },
                    ..ForkChoiceScalars::default()
                };
                batch.put(
                    TABLE_META,
                    KEY_FC_SCALARS.as_bytes(),
                    &scalars.as_ssz_bytes(),
                );
            }
            engine.commit(batch).unwrap();
        }
        drop(opened);
        (dir, opts)
    }

    fn read_meta(dir: &Path, key: &str) -> Vec<u8> {
        use cc_store::engine::{Engine, EngineOptions};
        use cc_store::meta::TABLE_META;
        let engine = Engine::open(dir, EngineOptions::default()).unwrap();
        let rt = engine.read().unwrap();
        rt.get(TABLE_META, key.as_bytes()).unwrap().expect(key)
    }

    fn meta_is_absent(dir: &Path, key: &str) -> bool {
        use cc_store::engine::{Engine, EngineOptions};
        use cc_store::meta::TABLE_META;
        let engine = Engine::open(dir, EngineOptions::default()).unwrap();
        let rt = engine.read().unwrap();
        rt.get(TABLE_META, key.as_bytes()).unwrap().is_none()
    }

    fn schema_version_of(dir: &Path) -> u32 {
        use cc_store::SszDecode;
        use cc_store::meta::SchemaVersion;
        let bytes = read_meta(dir, cc_store::meta::KEY_SCHEMA_VERSION);
        SchemaVersion::from_ssz_bytes(&bytes).unwrap().version
    }

    fn delete_meta(dir: &Path, key: &str) {
        use cc_store::engine::{Engine, EngineOptions};
        use cc_store::meta::TABLE_META;
        let engine = Engine::open(dir, EngineOptions::default()).unwrap();
        let mut batch = engine.batch();
        batch.delete(TABLE_META, key.as_bytes());
        engine.commit(batch).unwrap();
    }

    fn overwrite_config_digest(dir: &Path, digest: Root) {
        use cc_store::SszEncode;
        use cc_store::engine::{Engine, EngineOptions};
        use cc_store::meta::{ConfigDigest, KEY_CONFIG_DIGEST, TABLE_META};
        let engine = Engine::open(dir, EngineOptions::default()).unwrap();
        let record = ConfigDigest { digest };
        let mut batch = engine.batch();
        batch.put(
            TABLE_META,
            KEY_CONFIG_DIGEST.as_bytes(),
            &record.as_ssz_bytes(),
        );
        engine.commit(batch).unwrap();
    }

    fn capture_schedule_restamp<T>(
        flag: std::sync::Arc<std::sync::atomic::AtomicBool>,
        body: impl FnOnce() -> T,
    ) -> T {
        tracing::subscriber::with_default(ScheduleWarn(flag), body)
    }

    struct ScheduleWarn(std::sync::Arc<std::sync::atomic::AtomicBool>);

    impl tracing::Subscriber for ScheduleWarn {
        fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
            *metadata.level() == tracing::Level::WARN
        }

        fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_non_zero_u64(std::num::NonZeroU64::MIN)
        }

        fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

        fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

        fn event(&self, event: &tracing::Event<'_>) {
            event.record(&mut ScheduleWarnVisit(&self.0));
        }

        fn enter(&self, _span: &tracing::span::Id) {}

        fn exit(&self, _span: &tracing::span::Id) {}
    }

    struct ScheduleWarnVisit<'a>(&'a std::sync::atomic::AtomicBool);

    impl tracing::field::Visit for ScheduleWarnVisit<'_> {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                let text = format!("{value:?}");
                if text.contains("re-stamping schedule digest") {
                    self.0.store(true, std::sync::atomic::Ordering::SeqCst);
                }
            }
        }
    }

    struct ResumeStallGuard;

    impl ResumeStallGuard {
        fn set(ms: u64) -> Self {
            RESUME_BLOCKING_STALL_MS.store(ms, Ordering::SeqCst);
            Self
        }
    }

    impl Drop for ResumeStallGuard {
        fn drop(&mut self) {
            RESUME_BLOCKING_STALL_MS.store(0, Ordering::SeqCst);
        }
    }

    struct Running {
        dir: PathBuf,
        rt: StorageRuntime,
        _registry: Registry,
    }

    fn start_runtime(label: &str, faults: WriterFaults) -> Running {
        let dir = unique_temp_dir(label);
        std::fs::create_dir_all(&dir).unwrap();
        let opened = open(&dir, test_opts(NodeIdExpectation::Unset)).unwrap();
        let mut registry = Registry::default();
        let metrics = StorageMetrics::register(&mut registry);
        let rt = start_writer_with_faults(opened, metrics, false, faults);
        Running {
            dir,
            rt,
            _registry: registry,
        }
    }

    fn synth_block(slot: u64, parent: &Root, state: &Root) -> Vec<u8> {
        use cc_store::blocks::{
            MIN_BLOCK_SSZ_LEN, PARENT_ROOT_SSZ_OFFSET, SLOT_SSZ_OFFSET, STATE_ROOT_SSZ_OFFSET,
        };
        let mut body = vec![0u8; MIN_BLOCK_SSZ_LEN];
        body[0..4].copy_from_slice(&100u32.to_le_bytes());
        body[SLOT_SSZ_OFFSET..SLOT_SSZ_OFFSET + 8].copy_from_slice(&slot.to_le_bytes());
        body[PARENT_ROOT_SSZ_OFFSET..PARENT_ROOT_SSZ_OFFSET + 32]
            .copy_from_slice(parent.as_slice());
        body[STATE_ROOT_SSZ_OFFSET..STATE_ROOT_SSZ_OFFSET + 32].copy_from_slice(state.as_slice());
        body
    }

    fn hot_unit(slot: u64, root: Root, ssz: Vec<u8>, seq: u64) -> crate::writer::CommitUnit {
        use crate::writer::{CommitUnit, StagedBlock};
        CommitUnit {
            blocks: vec![StagedBlock {
                slot: Slot::new(slot),
                root,
                ssz,
                update_canonical: true,
                write_state_root: false,
                da_status: Some(DaStatus::Available),
            }],
            columns: Vec::new(),
            fork_choice: None,
            canonical_from: None,
            anchor: None,
            cursor: WriteCursor {
                session_id: 1,
                seq,
                slot: Slot::new(slot),
                root,
            },
            done: None,
        }
    }

    fn stored_body(engine: &Engine, root: &Root) -> Option<Vec<u8>> {
        let read = engine.read().unwrap();
        get_block_by_root(&read, root).unwrap()
    }

    /// `resume` is the runtime's durable load. The blocking classify/plan
    /// must not occupy the runtime worker.
    #[tokio::test(flavor = "current_thread")]
    async fn resume_returns_durable_seed_while_runtime_stays_responsive() {
        let _stall = ResumeStallGuard::set(250);
        let running = start_runtime("resume-responsive", WriterFaults::default());
        let chain = fixture_chain("hoodi-config.yaml");
        {
            let load = running.rt.resume(&chain);
            tokio::pin!(load);
            let probe = tokio::time::timeout(
                std::time::Duration::from_millis(80),
                tokio::time::sleep(std::time::Duration::from_millis(10)),
            );
            let probe_won = tokio::select! {
                biased;
                _ = &mut load => false,
                result = probe => result.is_ok(),
            };
            assert!(
                probe_won,
                "runtime timer did not fire during resume (blocking load on a runtime worker)"
            );
            let seed: Option<DurableSeed> = load.await.unwrap();
            assert!(seed.is_none(), "an empty store resumes as uninitialized");
        }
        running.rt.drain_and_shutdown().await.unwrap();
    }

    /// A committed anchor is `Complete`: `resume` returns that seed.
    #[tokio::test]
    async fn resume_on_a_complete_store_returns_the_durable_seed() {
        use cc_seam::{ArchiveWrite, Bytes, DaVerdict, TrustedAnchor};
        use cc_store::SszEncode;
        use cc_types::{Checkpoint, Epoch};

        let running = start_runtime("resume-complete", WriterFaults::default());
        let parent = Root::from_array([0x11; 32]);
        let root = Root::from_array([0x42; 32]);
        let state = Root::from_array([0xF0; 32]);
        let slot = 64u64;
        let block = synth_block(slot, &parent, &state);
        let state_ssz = b"anchor-state-ssz".to_vec();
        let checkpoint = Checkpoint {
            epoch: Epoch::new(slot / 32),
            root,
        };
        let scalars = ForkChoiceScalars {
            time: slot,
            proposer_boost_root: Root::ZERO,
            justified: checkpoint,
            finalized: checkpoint,
            unrealized_justified: checkpoint,
            unrealized_finalized: checkpoint,
            head_root: root,
            head_slot: Slot::new(slot),
        }
        .as_ssz_bytes();
        running
            .rt
            .archive()
            .commit_anchor(TrustedAnchor {
                block_root: root.into_array(),
                parent_root: parent.into_array(),
                slot,
                state_root: state.into_array(),
                block_ssz: Bytes::from(block.clone()),
                state_ssz: Bytes::from(state_ssz.clone()),
                scalars: Bytes::from(scalars),
                da: DaVerdict::Available,
            })
            .await
            .unwrap();

        let chain = fixture_chain("hoodi-config.yaml");
        let seed = running
            .rt
            .resume(&chain)
            .await
            .unwrap()
            .expect("complete seed");
        assert_eq!(seed.state_ssz, state_ssz);
        assert_eq!(seed.anchor_block_ssz, block);
        assert_eq!(seed.anchor_block_root, root.into_array());
        assert_eq!(seed.expected_head_slot, slot);
        running.rt.drain_and_shutdown().await.unwrap();
    }

    /// Bodies without an anchor are `Incomplete`, including through `resume`.
    #[tokio::test]
    async fn resume_on_an_incomplete_store_names_the_item() {
        let dir = unique_temp_dir("resume-incomplete");
        std::fs::create_dir_all(&dir).unwrap();
        let opened = open(&dir, test_opts(NodeIdExpectation::Unset)).unwrap();
        put_canonical_slot(&opened, 3, 0xAB);
        let mut registry = Registry::default();
        let metrics = StorageMetrics::register(&mut registry);
        let rt = start_writer(opened, metrics, false);
        let chain = fixture_chain("hoodi-config.yaml");
        let err = rt.resume(&chain).await.unwrap_err();
        assert!(
            err.to_string().contains("anchor"),
            "incomplete resume must name the item, not return None: {err}"
        );
        rt.drain_and_shutdown().await.unwrap();
        drop(rt);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Drain returns only after the open unit is committed and fsynced.
    #[tokio::test]
    async fn drain_and_shutdown_fsyncs_the_last_submitted_body() {
        let running = start_runtime("drain-fsync", WriterFaults::default());
        let root = Root::from_array([0x33; 32]);
        let ssz = synth_block(3, &Root::ZERO, &Root::from_array([0xF0; 32]));
        let writer = running.rt.writer.clone();
        let engine = Arc::clone(&running.rt.engine);
        writer.hold_commits();
        writer
            .submit_p0(hot_unit(3, root, ssz.clone(), 1))
            .await
            .unwrap();
        let entered = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if writer.commit_entered() >= 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(
            entered.is_ok(),
            "writer did not reach the held commit before drain"
        );

        {
            let drain = running.rt.drain_and_shutdown();
            tokio::pin!(drain);
            let still_open = tokio::select! {
                biased;
                result = &mut drain => {
                    let _ = result;
                    false
                }
                _ = tokio::time::sleep(std::time::Duration::from_millis(200)) => true,
            };
            assert!(
                still_open,
                "drain_and_shutdown returned before the held commit was released"
            );
            assert!(
                stored_body(&engine, &root).is_none(),
                "body must stay absent until drain finishes the commit"
            );
            let rejected = writer
                .submit_p0(hot_unit(4, Root::from_array([0x44; 32]), ssz.clone(), 2))
                .await;
            assert!(
                matches!(rejected, Err(crate::writer::WriterError::ShutDown)),
                "drain must stop admitting, got {rejected:?}"
            );

            writer.release_commits();
            let finished =
                tokio::time::timeout(std::time::Duration::from_secs(2), &mut drain).await;
            assert!(finished.is_ok(), "drain did not return after the commit");
            finished.unwrap().unwrap();
            assert_eq!(stored_body(&engine, &root).as_deref(), Some(ssz.as_slice()));
        }

        let dir = running.dir.clone();
        drop(engine);
        drop(writer);
        drop(running);
        let frontier = reopen_durable_frontier(&dir, &[3]).unwrap();
        assert_eq!(frontier.bodies[0].as_deref(), Some(ssz.as_slice()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Queued hot data is committed before the split row. The hot row is not
    /// migrated: `finalize`, `migrate`, and `prune` stay deferred.
    #[tokio::test]
    async fn no_uncommitted_hot_row_lands_after_split() {
        let running = start_runtime(
            "split-precondition",
            WriterFaults {
                start_paused: true,
                ..WriterFaults::default()
            },
        );
        let writer = running.rt.writer.clone();
        let engine = Arc::clone(&running.rt.engine);
        writer.enable_commit_trace();
        let root = Root::from_array([0x55; 32]);
        let ssz = synth_block(5, &Root::ZERO, &Root::from_array([0xF1; 32]));
        writer
            .submit_p0(hot_unit(5, root, ssz.clone(), 1))
            .await
            .unwrap();
        writer.prefer_split_over_hot_once();

        let split = Split {
            slot: Slot::new(8),
            state_root: Root::ZERO,
            block_root: Root::ZERO,
        };
        {
            let flush = running.rt.flush_before_split(split);
            tokio::pin!(flush);
            let mut wait_reason: &str = "split was not queued while the writer stayed paused";
            for _ in 0..200 {
                tokio::select! {
                    biased;
                    result = &mut flush => {
                        let _ = result;
                        wait_reason = "flush finished while the writer was paused";
                        break;
                    }
                    _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {
                        if writer.split_is_queued() {
                            wait_reason = "";
                            break;
                        }
                    }
                }
            }
            assert!(wait_reason.is_empty(), "{wait_reason}");
            assert!(
                stored_body(&engine, &root).is_none(),
                "hot body must still be uncommitted when the split is only queued"
            );
            let read = engine.read().unwrap();
            assert!(
                cc_store::split::load_split(&read).unwrap().is_none(),
                "split row must not land before the queued hot unit"
            );
            drop(read);

            writer.unpause_loop();
            let finished =
                tokio::time::timeout(std::time::Duration::from_secs(2), &mut flush).await;
            assert!(finished.is_ok(), "flush_before_split did not finish");
            finished.unwrap().unwrap();
        }
        assert_eq!(
            running.rt.writer.commit_order(),
            ["hot", "split"],
            "a queued hot unit must commit before the split row"
        );
        assert_eq!(
            stored_body(running.rt.engine(), &root).as_deref(),
            Some(ssz.as_slice())
        );
        let read = running.rt.engine().read().unwrap();
        let (slot, region) = cc_store::blocks::slot_by_root(&read, &root)
            .unwrap()
            .unwrap();
        assert_eq!(slot, Slot::new(5));
        assert_eq!(
            region,
            cc_store::BlockRegion::Hot,
            "the precondition does not migrate the hot row"
        );
        let stored = cc_store::split::load_split(&read).unwrap().unwrap();
        assert_eq!(stored.slot, Slot::new(8));
        drop(read);

        running.rt.drain_and_shutdown().await.unwrap();
    }

    fn split_update(slot: u64) -> crate::writer::MetaUpdate {
        use cc_store::SszEncode;
        let split = Split {
            slot: Slot::new(slot),
            state_root: Root::ZERO,
            block_root: Root::ZERO,
        };
        crate::writer::MetaUpdate {
            puts: vec![(
                TABLE_META.to_owned(),
                cc_store::meta::KEY_SPLIT.as_bytes().to_vec(),
                split.as_ssz_bytes(),
            )],
            deletes: Vec::new(),
            done: None,
        }
    }

    fn paused_runtime(label: &str) -> Running {
        start_runtime(
            label,
            WriterFaults {
                start_paused: true,
                ..WriterFaults::default()
            },
        )
    }

    async fn wait_split_queued(writer: &crate::writer::WriterHandle) {
        let finished = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !writer.split_is_queued() {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(
            finished.is_ok(),
            "split was not queued while the writer stayed paused"
        );
    }

    async fn wait_writer(writer: &crate::writer::WriterHandle, want: WriterStop) {
        let mut done = writer.writer_done();
        let finished = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                if *done.borrow_and_update() == want {
                    return;
                }
                if done.changed().await.is_err() {
                    return;
                }
            }
        })
        .await;
        assert!(finished.is_ok(), "writer did not reach {want:?}");
        assert_eq!(*writer.writer_done().borrow(), want);
    }

    /// Hot slot ≤ the in-flight split is refused before the writer runs.
    /// Returns the refused root so the caller can show it stays absent.
    async fn refuse_hot_at_split(
        writer: &crate::writer::WriterHandle,
        engine: &Engine,
        split_slot: u64,
    ) -> Root {
        let root = Root::from_array([0x66; 32]);
        let ssz = synth_block(split_slot, &Root::ZERO, &Root::from_array([0xF2; 32]));
        let rejected = writer.submit_p0(hot_unit(split_slot, root, ssz, 2)).await;
        assert!(
            matches!(rejected, Err(crate::writer::WriterError::HotAtOrBelowSplit)),
            "hot slot {split_slot} must be refused while the split oneshot is outstanding, got {rejected:?}"
        );
        assert!(
            stored_body(engine, &root).is_none(),
            "a refused hot body must not be queued"
        );
        root
    }

    fn assert_split_stored_and_hot_absent(engine: &Engine, split_slot: u64, root: &Root) {
        assert!(stored_body(engine, root).is_none());
        let read = engine.read().unwrap();
        let stored = cc_store::split::load_split(&read).unwrap().unwrap();
        assert_eq!(stored.slot, Slot::new(split_slot));
    }

    /// The paused queue-before-flush test is not this window: the hot submit
    /// runs only after the split is armed and before its oneshot completes.
    #[tokio::test]
    async fn hot_submit_while_flush_split_is_outstanding_is_refused() {
        let running = paused_runtime("split-flush-window");
        let writer = running.rt.writer.clone();
        let engine = Arc::clone(&running.rt.engine);
        let split_slot = 8u64;
        let flush = running.rt.flush_before_split(Split {
            slot: Slot::new(split_slot),
            state_root: Root::ZERO,
            block_root: Root::ZERO,
        });
        tokio::pin!(flush);
        let queued = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                tokio::select! {
                    biased;
                    result = &mut flush => {
                        let _ = result;
                        return false;
                    }
                    _ = tokio::task::yield_now() => {
                        if writer.split_is_queued() {
                            return true;
                        }
                    }
                }
            }
        })
        .await;
        assert!(
            matches!(queued, Ok(true)),
            "split was not queued while the writer stayed paused"
        );
        let refused = refuse_hot_at_split(&writer, &engine, split_slot).await;
        let above = Root::from_array([0x67; 32]);
        let above_ssz = synth_block(split_slot + 1, &Root::ZERO, &Root::from_array([0xF3; 32]));
        writer
            .submit_p0(hot_unit(split_slot + 1, above, above_ssz.clone(), 3))
            .await
            .unwrap();
        writer.unpause_loop();
        let finished = tokio::time::timeout(std::time::Duration::from_secs(2), &mut flush).await;
        assert!(finished.is_ok(), "flush_before_split did not finish");
        finished.unwrap().unwrap();
        assert_split_stored_and_hot_absent(&engine, split_slot, &refused);
        assert_eq!(
            stored_body(&engine, &above).as_deref(),
            Some(above_ssz.as_slice())
        );

        let late = Root::from_array([0x68; 32]);
        let late_ssz = synth_block(split_slot, &Root::ZERO, &Root::from_array([0xF4; 32]));
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let mut late_unit = hot_unit(split_slot, late, late_ssz, 4);
        late_unit.done = Some(done_tx);
        writer.submit_p0(late_unit).await.unwrap();
        let late_err = done_rx.await.unwrap().unwrap_err();
        assert!(
            matches!(late_err, crate::writer::WriterError::HotAtOrBelowSplit),
            "a new hot put at the stored split must fail the commit, got {late_err:?}"
        );
        assert!(stored_body(&engine, &late).is_none());
        running.rt.drain_and_shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn hot_submit_while_submit_p1_split_is_outstanding_is_refused() {
        let running = paused_runtime("split-submit-p1-window");
        let writer = running.rt.writer.clone();
        let engine = Arc::clone(&running.rt.engine);
        let split_slot = 8u64;
        let submitter = writer.clone();
        let submitted =
            tokio::spawn(async move { submitter.submit_p1(split_update(split_slot)).await });
        wait_split_queued(&writer).await;
        let refused = refuse_hot_at_split(&writer, &engine, split_slot).await;
        writer.unpause_loop();
        let finished = tokio::time::timeout(std::time::Duration::from_secs(2), submitted).await;
        assert!(finished.is_ok(), "submit_p1 split did not finish");
        finished.unwrap().unwrap().unwrap();
        assert_split_stored_and_hot_absent(&engine, split_slot, &refused);
        running.rt.drain_and_shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn hot_submit_while_blocking_split_commit_is_outstanding_is_refused() {
        let running = paused_runtime("split-blocking-window");
        let writer = running.rt.writer.clone();
        let engine = Arc::clone(&running.rt.engine);
        let split_slot = 8u64;
        let submitter = writer.clone();
        let submitted = tokio::task::spawn_blocking(move || {
            submitter.blocking_submit_p1_committed(split_update(split_slot))
        });
        wait_split_queued(&writer).await;
        let refused = refuse_hot_at_split(&writer, &engine, split_slot).await;
        writer.unpause_loop();
        let finished = tokio::time::timeout(std::time::Duration::from_secs(2), submitted).await;
        assert!(finished.is_ok(), "blocking split commit did not finish");
        finished.unwrap().unwrap().unwrap();
        assert_split_stored_and_hot_absent(&engine, split_slot, &refused);
        running.rt.drain_and_shutdown().await.unwrap();
    }

    /// A hot unit drained ahead of the split fails closed: the split row stays absent.
    #[tokio::test]
    async fn failed_drained_hot_commit_does_not_fsync_the_split() {
        let running = start_runtime(
            "split-hot-fail",
            WriterFaults {
                start_paused: true,
                fail_next_commit: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true)),
                ..WriterFaults::default()
            },
        );
        let writer = running.rt.writer.clone();
        let engine = Arc::clone(&running.rt.engine);
        let root = Root::from_array([0x69; 32]);
        let ssz = synth_block(4, &Root::ZERO, &Root::from_array([0xF5; 32]));
        writer.submit_p0(hot_unit(4, root, ssz, 1)).await.unwrap();
        writer.prefer_split_over_hot_once();
        let flush = running.rt.flush_before_split(Split {
            slot: Slot::new(8),
            state_root: Root::ZERO,
            block_root: Root::ZERO,
        });
        tokio::pin!(flush);
        let queued = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                tokio::select! {
                    biased;
                    result = &mut flush => {
                        let _ = result;
                        return false;
                    }
                    _ = tokio::task::yield_now() => {
                        if writer.split_is_queued() {
                            return true;
                        }
                    }
                }
            }
        })
        .await;
        assert!(
            matches!(queued, Ok(true)),
            "split was not queued while the writer stayed paused"
        );
        writer.unpause_loop();
        let finished = tokio::time::timeout(std::time::Duration::from_secs(2), &mut flush).await;
        assert!(
            finished.is_ok(),
            "flush did not finish after the hot commit failed"
        );
        let err = finished.unwrap().unwrap_err();
        assert!(
            err.to_string().contains("injected"),
            "split must surface the drained hot failure, got {err}"
        );
        assert!(stored_body(&engine, &root).is_none());
        let read = engine.read().unwrap();
        assert!(cc_store::split::load_split(&read).unwrap().is_none());
        drop(read);
        running.rt.drain_and_shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn drain_and_shutdown_after_abort_is_an_error() {
        let running = paused_runtime("drain-after-abort");
        let writer = running.rt.writer.clone();
        let engine = Arc::clone(&running.rt.engine);
        let root = Root::from_array([0x70; 32]);
        let ssz = synth_block(3, &Root::ZERO, &Root::from_array([0xF6; 32]));
        writer.submit_p0(hot_unit(3, root, ssz, 1)).await.unwrap();
        running.rt.shutdown();
        writer.unpause_loop();
        wait_writer(&writer, WriterStop::Aborted).await;
        let err = running.rt.drain_and_shutdown().await.unwrap_err();
        assert!(
            err.to_string().contains("aborted"),
            "an abort exit must not be Ok, got {err}"
        );
        assert!(stored_body(&engine, &root).is_none());
    }

    #[tokio::test]
    async fn drain_and_shutdown_errors_when_the_writer_already_aborted() {
        let running = start_runtime("drain-already-gone", WriterFaults::default());
        running.rt.shutdown();
        wait_writer(&running.rt.writer, WriterStop::Aborted).await;
        let err = running.rt.drain_and_shutdown().await.unwrap_err();
        assert!(
            err.to_string().contains("aborted"),
            "a writer that already left without draining must not be Ok, got {err}"
        );
    }

    #[tokio::test]
    async fn drain_and_shutdown_prefers_drain_after_shutdown() {
        let running = start_runtime("drain-over-shutdown", WriterFaults::default());
        let writer = running.rt.writer.clone();
        let engine = Arc::clone(&running.rt.engine);
        let root = Root::from_array([0x71; 32]);
        let ssz = synth_block(6, &Root::ZERO, &Root::from_array([0xF7; 32]));
        writer.hold_commits();
        writer
            .submit_p0(hot_unit(6, root, ssz.clone(), 1))
            .await
            .unwrap();
        let entered = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while writer.commit_entered() < 1 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(entered.is_ok(), "writer did not reach the held commit");
        {
            let drain = running.rt.drain_and_shutdown();
            tokio::pin!(drain);
            let signaled = tokio::time::timeout(std::time::Duration::from_secs(2), async {
                loop {
                    tokio::select! {
                        biased;
                        result = &mut drain => {
                            let _ = result;
                            return false;
                        }
                        _ = tokio::task::yield_now() => {
                            if writer.drain_requested() {
                                return true;
                            }
                        }
                    }
                }
            })
            .await;
            assert!(
                matches!(signaled, Ok(true)),
                "drain finished or was not signaled before shutdown"
            );
            running.rt.shutdown();
            writer.release_commits();
            let finished =
                tokio::time::timeout(std::time::Duration::from_secs(2), &mut drain).await;
            assert!(
                finished.is_ok(),
                "drain did not finish after the commit was released"
            );
            finished.unwrap().unwrap();
        }
        assert_eq!(stored_body(&engine, &root).as_deref(), Some(ssz.as_slice()));
    }

    #[tokio::test]
    async fn drain_and_shutdown_fails_when_the_drained_commit_fails() {
        let fail = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let running = start_runtime(
            "drain-commit-fails",
            WriterFaults {
                start_paused: true,
                fail_next_commit: std::sync::Arc::clone(&fail),
                ..WriterFaults::default()
            },
        );
        let writer = running.rt.writer.clone();
        let engine = Arc::clone(&running.rt.engine);
        let root = Root::from_array([0x72; 32]);
        let ssz = synth_block(7, &Root::ZERO, &Root::from_array([0xF8; 32]));
        writer.submit_p0(hot_unit(7, root, ssz, 1)).await.unwrap();
        {
            let drain = running.rt.drain_and_shutdown();
            tokio::pin!(drain);
            let signaled = tokio::time::timeout(std::time::Duration::from_secs(2), async {
                loop {
                    tokio::select! {
                        biased;
                        result = &mut drain => {
                            let _ = result;
                            return false;
                        }
                        _ = tokio::task::yield_now() => {
                            if writer.drain_requested() {
                                return true;
                            }
                        }
                    }
                }
            })
            .await;
            assert!(
                matches!(signaled, Ok(true)),
                "drain finished before the paused writer was released"
            );
            writer.unpause_loop();
            let finished =
                tokio::time::timeout(std::time::Duration::from_secs(2), &mut drain).await;
            assert!(finished.is_ok(), "drain did not finish");
            let err = finished.unwrap().unwrap_err();
            assert!(
                err.to_string().contains("drained unit commit failed"),
                "a failed drained commit must fail drain_and_shutdown, got {err}"
            );
        }
        assert!(stored_body(&engine, &root).is_none());
    }

    #[tokio::test]
    async fn p2_try_send_holds_the_admit_guard() {
        use crate::metrics::StorageClass;
        use crate::writer::BackgroundChunk;

        let running = paused_runtime("p2-admit");
        let writer = running.rt.writer.clone();
        let engine = Arc::clone(&running.rt.engine);
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let chunk = BackgroundChunk {
            class: StorageClass::Meta,
            puts: vec![(
                TABLE_META.to_owned(),
                b"p2-probe".to_vec(),
                b"committed".to_vec(),
            )],
            deletes: Vec::new(),
            done: Some(done_tx),
        };
        assert!(writer.try_submit_p2(chunk, writer.metrics()));
        {
            let drain = running.rt.drain_and_shutdown();
            tokio::pin!(drain);
            let signaled = tokio::time::timeout(std::time::Duration::from_secs(2), async {
                loop {
                    tokio::select! {
                        biased;
                        result = &mut drain => {
                            let _ = result;
                            return false;
                        }
                        _ = tokio::task::yield_now() => {
                            if writer.drain_requested() {
                                return true;
                            }
                        }
                    }
                }
            })
            .await;
            assert!(
                matches!(signaled, Ok(true)),
                "drain was not signaled while paused"
            );
            let late = BackgroundChunk {
                class: StorageClass::Meta,
                puts: vec![(TABLE_META.to_owned(), b"p2-late".to_vec(), b"nope".to_vec())],
                deletes: Vec::new(),
                done: None,
            };
            assert!(
                !writer.try_submit_p2(late, writer.metrics()),
                "try_submit_p2 after close must be refused"
            );
            writer.unpause_loop();
            let finished =
                tokio::time::timeout(std::time::Duration::from_secs(2), &mut drain).await;
            assert!(finished.is_ok(), "drain did not finish");
            finished.unwrap().unwrap();
        }
        done_rx.await.unwrap().unwrap();
        let read = engine.read().unwrap();
        let stored = read.get(TABLE_META, b"p2-probe").unwrap().unwrap();
        assert_eq!(stored, b"committed");
        assert!(read.get(TABLE_META, b"p2-late").unwrap().is_none());
    }

    #[test]
    fn deferred_finalize_is_documented_and_write_behind_algebra_stays_gone() {
        let open_prod = include_str!("open.rs").split("mod tests").next().unwrap();
        assert!(
            open_prod.contains("deferred past S2R"),
            "finalize, migrate, and prune deferral must be documented on the runtime"
        );
        for name in ["fn finalize", "fn migrate", "fn prune"] {
            assert!(
                !open_prod.contains(name),
                "{name} must stay unimplemented on the runtime"
            );
        }
        let writer_prod = include_str!("writer.rs").split("mod tests").next().unwrap();
        for name in [
            "PendingRetry",
            "resume_after_panic",
            "predecessor_resume_cursor",
        ] {
            assert!(
                !open_prod.contains(name),
                "open.rs reintroduced write-behind algebra {name}"
            );
            assert!(
                !writer_prod.contains(name),
                "writer.rs reintroduced write-behind algebra {name}"
            );
        }
    }
}
