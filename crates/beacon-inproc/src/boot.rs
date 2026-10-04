//! Proto-free twin of `bin/beacon-core/src/boot.rs` `boot_in_process`.
//!
//! ```text
//! open redb  →  durable_set (empty | present)  —  no writer, no Server
//! ```
//!
//! Fail-closed gates match storage-core `open` (schema / digest / `I-node-id`).

use std::path::{Path, PathBuf};

use cc_store::engine::{Durability, Engine, EngineOptions};
use cc_store::meta::{AnchorInfo, KEY_ANCHOR_INFO, KEY_FC_SCALARS, KEY_NODE_ID, TABLE_META};
use cc_store::snapshots::newest_snapshot;
use cc_store::{
    Root, SszDecode, SszEncode, Store, StoreOpenOptions, legacy_config_digest,
    refuse_populated_legacy_open,
};

/// Ordered boot phases. [`BootPhase::Open`] is always first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BootPhase {
    /// `Store::open` returned. No writer / core yet.
    Open,
    /// Durable set probed (`None` = empty store).
    DurableSet,
}

/// Inputs for the in-process boot path (mirrors `bin/beacon-core` `BootConfig`).
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
        }
    }
}

/// Result of [`boot_in_process`]: one redb handle, ordered phases.
#[derive(Debug)]
pub struct Booted {
    /// The one opened redb. Exclusive while this value lives.
    pub store: Store,
    /// Ordered phases. First element is always [`BootPhase::Open`] on success.
    pub phases: Vec<BootPhase>,
    /// `true` when the store has no fork-choice scalars and no snapshot.
    pub durable_empty: bool,
}

/// Open redb, then probe the durable set. No gRPC. No 30 s grace. No writer.
///
/// Same first two phases as `bin/beacon-core/src/boot.rs` `boot_in_process`.
pub fn boot_in_process(cfg: &BootConfig) -> anyhow::Result<Booted> {
    let mut phases = Vec::new();
    let opened = open_and_stamp(cfg)?;
    phases.push(BootPhase::Open);

    let durable_empty = is_store_empty(opened.store.engine())?;
    phases.push(BootPhase::DurableSet);

    Ok(Booted {
        store: opened.store,
        phases,
        durable_empty,
    })
}

struct Opened {
    store: Store,
}

fn open_and_stamp(cfg: &BootConfig) -> anyhow::Result<Opened> {
    let opened = open_store(cfg)?;
    if let Some(id) = opened.expected_node_id {
        persist_anchor_node_id(opened.store.engine(), id)?;
    }
    Ok(Opened {
        store: opened.store,
    })
}

struct OpenedStore {
    store: Store,
    expected_node_id: Option<Root>,
}

fn open_store(cfg: &BootConfig) -> anyhow::Result<OpenedStore> {
    let durability =
        Durability::parse(&cfg.durability).map_err(|e| anyhow::anyhow!("durability: {e}"))?;
    // Absent GVR stays absent. The running network is threaded by beacon-core later;
    // this opener does not read a source-tree fixture in its place.
    let gvr_present = parse_gvr(cfg.genesis_validators_root.as_deref())?.is_some();
    let expected_node_id = legacy_node_id_from_file(cfg.node_key_path.as_deref())
        .map_err(|e| anyhow::anyhow!("node_key_path: {e}"))?;
    if expected_node_id.is_some() {
        tracing::info!(
            path = ?cfg.node_key_path,
            "I-node-id node key loaded from node_key_path"
        );
    }
    let store_opts = StoreOpenOptions::with_digest(
        EngineOptions::default().with_durability(durability),
        legacy_config_digest(),
    )
    .with_check_invariants(cfg.check_invariants)
    .with_snapshot_ring(cfg.snapshot_ring.max(1))
    .with_max_open_scan_rows(cc_store::DEFAULT_MAX_OPEN_SCAN_ROWS.max(1))
    .with_expected_node_id(expected_node_id);
    let store =
        Store::open(&cfg.data_dir, store_opts).map_err(|e| anyhow::anyhow!("store open: {e}"))?;
    refuse_missing_key_if_anchor_present(store.engine(), cfg.node_key_path.as_deref())
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    refuse_populated_legacy_open(store.engine(), gvr_present)
        .map_err(|e| anyhow::anyhow!("store open: {e}"))?;
    Ok(OpenedStore {
        store,
        expected_node_id,
    })
}

fn persist_anchor_node_id(engine: &Engine, node_id: Root) -> anyhow::Result<()> {
    let (existing_anchor, existing_id) = {
        let rt = engine
            .read()
            .map_err(|e| anyhow::anyhow!("read identity: {e}"))?;
        let id = rt
            .get(TABLE_META, KEY_NODE_ID.as_bytes())
            .map_err(|e| anyhow::anyhow!("read node_id: {e}"))?
            .map(|b| Root::from_ssz_bytes(&b).map_err(|e| anyhow::anyhow!("node_id decode: {e:?}")))
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

/// Empty when no fork-choice scalars and no snapshot (fresh store after open).
fn is_store_empty(engine: &Engine) -> anyhow::Result<bool> {
    let rt = engine
        .read()
        .map_err(|e| anyhow::anyhow!("read empty-check: {e}"))?;
    let has_fc = rt
        .get(TABLE_META, KEY_FC_SCALARS.as_bytes())
        .map_err(|e| anyhow::anyhow!("read fc_scalars: {e}"))?
        .is_some();
    let has_snap = newest_snapshot(&rt).map_err(|e| anyhow::anyhow!("newest snapshot: {e}"))?;
    Ok(!has_fc && has_snap.is_none())
}

fn legacy_node_id_from_file(path: Option<&Path>) -> Result<Option<Root>, String> {
    let Some(path) = path else {
        return Ok(None);
    };
    if path.as_os_str().is_empty() {
        return Ok(None);
    }
    if !path.exists() {
        return Ok(None);
    }
    let bytes = std::fs::read(path)
        .map_err(|e| format!("node key read failed at {}: {e}", path.display()))?;
    if bytes.len() != 32 {
        return Err(format!(
            "node key at {} has length {}, expected 32",
            path.display(),
            bytes.len()
        ));
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Ok(Some(Root::from_array(arr)))
}

fn refuse_missing_key_if_anchor_present(
    engine: &Engine,
    path: Option<&Path>,
) -> Result<(), String> {
    let Some(path) = path else {
        return Ok(());
    };
    if path.as_os_str().is_empty() || path.exists() {
        return Ok(());
    }
    let rt = engine
        .read()
        .map_err(|e| format!("I-node-id: failed to read store: {e}"))?;
    let has_anchor = rt
        .get(TABLE_META, KEY_ANCHOR_INFO.as_bytes())
        .map_err(|e| format!("I-node-id: failed to read AnchorInfo: {e}"))?
        .is_some();
    let has_node_id = rt
        .get(TABLE_META, KEY_NODE_ID.as_bytes())
        .map_err(|e| format!("I-node-id: failed to read node_id: {e}"))?
        .is_some();
    if !has_anchor && !has_node_id {
        return Ok(());
    }
    Err(format!(
        "I-node-id (crates/store/src/invariants.rs): node key missing at {} \
         but store has identity",
        path.display()
    ))
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
