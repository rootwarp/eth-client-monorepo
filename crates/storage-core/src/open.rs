//! Public store open + durable-set load ([ARCH] §4.2 / S2-J-01).
//!
//! `bin/beacon-core` calls [`open`] **before** any subsystem starts. Fail-closed
//! gates (schema / digest / `I-node-id`) are unchanged from the storage host.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use cc_store::engine::{Durability, Engine, EngineOptions};
use cc_store::{ConfigDigestInput, Store, StoreOpenOptions};
use cc_types::{ChainConfig, Root};
use tokio::sync::watch;

use crate::archive_write::ArchiveWriter;
use crate::durable_set::{
    DurableSetContext, load_expected_node_id_from_key_path, refuse_missing_key_if_anchor_present,
};
use crate::metrics::StorageMetrics;
use crate::resume::{self, ResumeError};
use crate::writer::{WriterBounds, WriterFaults, WriterHandle, spawn_writer};

/// Options for [`open`]. Fail-closed gates match the storage host.
///
/// [`Debug`] is hand-written. This struct holds the key path, not the key
/// bytes; `expected_node_id` lives on [`OpenedStore`] and is redacted there.
#[derive(Clone)]
pub struct OpenOpts {
    /// Engine durability token (`immediate` | `paranoid`).
    pub durability: String,
    /// Run §2.7 invariants at open (includes `I-node-id` when a node key is set).
    pub check_invariants: bool,
    /// Snapshot ring depth for `I-ring`.
    pub snapshot_ring: u64,
    /// Per-check row cap at open (`storage.max_open_scan_rows`).
    pub max_open_scan_rows: u64,
    /// Network identity for the config digest (`0x` + 64 hex).
    pub genesis_validators_root: Option<String>,
    /// Path to the 32-byte p2p node key (`I-node-id`).
    pub node_key_path: Option<PathBuf>,
}

impl fmt::Debug for OpenOpts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenOpts")
            .field("durability", &self.durability)
            .field("check_invariants", &self.check_invariants)
            .field("snapshot_ring", &self.snapshot_ring)
            .field("max_open_scan_rows", &self.max_open_scan_rows)
            .field("genesis_validators_root", &self.genesis_validators_root)
            .field("node_key_path", &self.node_key_path)
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
            node_key_path: None,
        }
    }
}

/// One opened redb handle. The composer starts subsystems only after this exists.
///
/// [`Debug`] is hand-written: `expected_node_id` is the raw node key.
pub struct OpenedStore {
    store: Store,
    node_key_path: Option<PathBuf>,
    snapshot_ring: u64,
    max_open_scan_rows: u64,
    expected_node_id: Option<Root>,
}

impl fmt::Debug for OpenedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenedStore")
            .field("store", &self.store)
            .field("node_key_path", &self.node_key_path)
            .field("snapshot_ring", &self.snapshot_ring)
            .field("max_open_scan_rows", &self.max_open_scan_rows)
            .field(
                "expected_node_id",
                &self.expected_node_id.as_ref().map(|_| "<redacted>"),
            )
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

    /// Node id loaded from `node_key_path` at [`open`], if any.
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

/// One writer started from an [`OpenedStore`]. Exactly one per process.
#[derive(Debug)]
pub struct StorageRuntime {
    engine: Arc<Engine>,
    _writer: WriterHandle,
    archive: ArchiveWriter,
    shutdown_tx: watch::Sender<bool>,
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
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }
}

/// Open (or create) the store. Fail-closed gates run here, before any subsystem.
pub fn open(data_dir: impl AsRef<Path>, opts: OpenOpts) -> anyhow::Result<OpenedStore> {
    let data_dir = data_dir.as_ref();
    let durability =
        Durability::parse(&opts.durability).map_err(|e| anyhow::anyhow!("durability: {e}"))?;
    let gvr = parse_gvr(opts.genesis_validators_root.as_deref())?;
    let chain = digest_chain_config();
    let digest_input = ConfigDigestInput::with_mainnet_scalars(chain, gvr);
    let expected_node_id = load_expected_node_id_from_key_path(opts.node_key_path.as_deref())
        .map_err(|e| anyhow::anyhow!("node_key_path: {e}"))?;
    if expected_node_id.is_some() {
        tracing::info!(
            path = ?opts.node_key_path,
            "I-node-id node key loaded from node_key_path"
        );
    }
    let store_opts = StoreOpenOptions::from_config(
        EngineOptions::default().with_durability(durability),
        &digest_input,
    )?
    .with_check_invariants(opts.check_invariants)
    .with_snapshot_ring(opts.snapshot_ring.max(1))
    .with_max_open_scan_rows(opts.max_open_scan_rows.max(1))
    .with_expected_node_id(expected_node_id);
    let store =
        Store::open(data_dir, store_opts).map_err(|e| anyhow::anyhow!("store open: {e}"))?;
    refuse_missing_key_if_anchor_present(store.engine(), opts.node_key_path.as_deref())
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(OpenedStore {
        store,
        node_key_path: opts.node_key_path,
        snapshot_ring: opts.snapshot_ring.max(1),
        max_open_scan_rows: opts.max_open_scan_rows.max(1),
        expected_node_id,
    })
}

/// Load the durable set from an already-opened store. `None` = empty (checkpoint).
pub fn durable_set(db: &OpenedStore, chain: &ChainConfig) -> anyhow::Result<Option<DurableSet>> {
    let engine = db.store.engine();
    if resume::is_store_empty(engine).map_err(|e| anyhow::anyhow!("{e}"))? {
        return Ok(None);
    }
    let ctx = DurableSetContext {
        expected_node_id: db.expected_node_id,
        node_key_path: db.node_key_path.clone(),
        enr_seq_path: None,
        snapshot_ring: db.snapshot_ring,
        max_open_scan_rows: db.max_open_scan_rows,
        da_status_roots: Vec::new(),
    };
    let plan = resume::build_durable_plan(engine, &ctx, chain).map_err(map_resume)?;
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

/// Start the **one** writer against the opened handle. Call only after [`open`].
pub fn start_writer(
    db: OpenedStore,
    metrics: StorageMetrics,
    process_fatal: bool,
) -> StorageRuntime {
    start_writer_on_engine(Arc::new(db.into_engine()), metrics, process_fatal)
}

/// Start the writer on an already-opened [`cc_store::Store`] (A-13 → A-14).
pub fn start_writer_from_store(
    store: cc_store::Store,
    metrics: StorageMetrics,
    process_fatal: bool,
) -> StorageRuntime {
    start_writer_on_engine(Arc::new(store.into_engine()), metrics, process_fatal)
}

fn start_writer_on_engine(
    engine: Arc<Engine>,
    metrics: StorageMetrics,
    process_fatal: bool,
) -> StorageRuntime {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    if let Err(e) = ArchiveWriter::ensure_write_cursor(&engine) {
        tracing::warn!(error = %e, "ensure_write_cursor failed");
    }
    let writer = spawn_writer(
        Arc::clone(&engine),
        metrics,
        WriterBounds::default(),
        WriterFaults::default(),
        shutdown_rx,
        process_fatal,
    );
    let archive = ArchiveWriter::new(writer.clone(), Arc::clone(&engine));
    StorageRuntime {
        engine,
        _writer: writer,
        archive,
        shutdown_tx,
    }
}

fn map_resume(e: ResumeError) -> anyhow::Error {
    anyhow::anyhow!("{e}")
}

fn parse_gvr(s: Option<&str>) -> anyhow::Result<Root> {
    let Some(raw) = s.filter(|s| !s.is_empty()) else {
        return Ok(Root::ZERO);
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
    Ok(Root::from_array(arr))
}

fn digest_chain_config() -> ChainConfig {
    let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../crates/types/tests/fixtures/hoodi-config.yaml");
    if fixture.is_file()
        && let Ok(cfg) = ChainConfig::from_yaml_file(&fixture)
    {
        return cfg;
    }
    match ChainConfig::from_yaml_str(include_str!(
        "../../../crates/types/tests/fixtures/hoodi-config.yaml"
    )) {
        Ok(cfg) => cfg,
        Err(e) => {
            tracing::error!(error = %e, "bundled hoodi-config.yaml failed to parse");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::test_tmpdir::unique_temp_dir;
    use prometheus_client::registry::Registry;

    fn test_opts(node_key: Option<PathBuf>) -> OpenOpts {
        OpenOpts {
            durability: "immediate".to_owned(),
            check_invariants: true,
            snapshot_ring: 4,
            max_open_scan_rows: cc_store::DEFAULT_MAX_OPEN_SCAN_ROWS,
            genesis_validators_root: None,
            node_key_path: node_key,
        }
    }

    #[test]
    fn open_then_second_opener_mismatched_node_id_fails_inode_id() {
        let dir = unique_temp_dir("s2-j-01-inode");
        std::fs::create_dir_all(&dir).unwrap();
        let key_a = dir.join("node_key");
        let id_a = Root::from_array([0xAAu8; 32]);
        std::fs::write(&key_a, id_a.as_slice()).unwrap();

        let opened = open(&dir, test_opts(Some(key_a.clone()))).expect("first open");
        opened.persist_anchor_node_id(id_a).unwrap();
        drop(opened);

        let key_b = dir.join("node_key_b");
        std::fs::write(&key_b, [0xBBu8; 32]).unwrap();
        let err = open(&dir, test_opts(Some(key_b))).expect_err("I-node-id must refuse");
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
        let first = open(&dir, test_opts(None)).expect("first open");
        let err = open(&dir, test_opts(None)).expect_err("second live open must fail");
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
        let opened = open(&dir, test_opts(None)).unwrap();
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

        let opened = open(&dir, test_opts(Some(key_a.clone()))).expect("first open");
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

        let _reopen = open(&dir, test_opts(Some(key_a)))
            .expect("same-key reopen of gappy canonical must succeed");
        drop(_reopen);

        let key_b = dir.join("node_key_b");
        std::fs::write(&key_b, [0xBBu8; 32]).unwrap();
        let err = open(&dir, test_opts(Some(key_b))).expect_err("I-node-id must refuse");
        let msg = err.to_string();
        assert!(
            msg.contains("node_id") || msg.contains("I-node-id"),
            "second identity must fail I-node-id, got: {msg}"
        );
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

        let opened = open(&dir, test_opts(Some(key_a.clone()))).expect("first open");
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

        let _reopen = open(&dir, test_opts(Some(key_a)))
            .expect("same-key restart must succeed without planted slot 0");
        drop(_reopen);

        let key_b = dir.join("node_key_b");
        std::fs::write(&key_b, [0xBBu8; 32]).unwrap();
        let err = open(&dir, test_opts(Some(key_b))).expect_err("I-node-id must refuse");
        let msg = err.to_string();
        assert!(
            msg.contains("node_id") || msg.contains("I-node-id"),
            "second identity must fail I-node-id, got: {msg}"
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

        let opened = open(&dir, test_opts(Some(key_a))).expect("first open");
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
        let err = open(&dir, test_opts(Some(key_b)))
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
        let opened = open(&dir, test_opts(None)).expect("open");
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

    /// `{:?}` of every type that can carry `expected_node_id` prints `<redacted>`,
    /// not the 32 key bytes. `OpenOpts` has no such field; its hand-written
    /// `Debug` must still not grow a hex dump of a key path's contents.
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
        let opts = test_opts(Some(key.clone()));
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
            opts_dbg.contains("OpenOpts") && opts_dbg.contains("node_key_path"),
            "OpenOpts Debug must stay hand-written and name the path: {opts_dbg}"
        );
        assert!(
            !opts_dbg.contains(&root.to_string()) && !contains_32_byte_hex(&opts_dbg),
            "OpenOpts Debug leaked key bytes: {opts_dbg}"
        );

        let store_opts = StoreOpenOptions::from_config(
            cc_store::engine::EngineOptions::default(),
            &cc_store::ConfigDigestInput::with_mainnet_scalars(digest_chain_config(), Root::ZERO),
        )
        .unwrap()
        .with_expected_node_id(Some(root));
        let store_dbg = format!("{store_opts:?}");
        assert!(
            store_dbg.contains("<redacted>") && !store_dbg.contains(&root.to_string()),
            "StoreOpenOptions Debug leaked expected_node_id: {store_dbg}"
        );

        drop(opened);
    }
}
