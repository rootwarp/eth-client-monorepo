//! Schema version, config digest, table registry, and open-or-refuse (CC-40a).
//!
//! ## Config digest field list (§2.5)
//!
//! The digest is a defined function of **exactly** these named inputs and
//! nothing else (grep anchors for the AC):
//!
//! - fork epochs (`altair` … `fulu`)
//! - `BLOB_SCHEDULE`
//! - `SECONDS_PER_SLOT`
//! - `genesis_validators_root`
//! - `MIN_VALIDATOR_WITHDRAWABILITY_DELAY`
//! - `CHURN_LIMIT_QUOTIENT`
//!
//! Naming the list in source stops the digest from silently widening to a
//! runtime knob and refusing every restart.

use std::fmt;
use std::path::Path;

use sha2::{Digest, Sha256};
use ssz::{Decode, Encode};
use ssz_derive::{Decode as SszDecode, Encode as SszEncode};
use ssz_types::VariableList;
use typenum::U256;

use cc_types::{ChainConfig, Epoch, Root};

use crate::engine::{Engine, EngineOptions, StoreError};
use crate::keys::{
    BLOCK_SHARD_EPOCHS, COLUMN_SHARD_EPOCHS, blocks_shard_table, columns_shard_table,
    parse_shard_suffix,
};
use crate::meta::{ConfigDigest, KEY_CONFIG_DIGEST, KEY_SCHEMA_VERSION, SchemaVersion, TABLE_META};

/// Current on-disk schema version. Phase 4: one value, no migration path.
pub const SCHEMA_VERSION: u32 = 1;

// ---------------------------------------------------------------------------
// Fixed table names (§2.2 inventory)
// ---------------------------------------------------------------------------

/// Fixed (non-sharded) table names from Architecture §2.2.
pub const FIXED_TABLES: &[&str] = &[
    "meta",
    "blocks_hot",
    "block_slot_by_root",
    "canonical",
    "columns_hot",
    "column_slot_by_root",
    "da_status",
    "snapshots",
    "state_roots",
    "fork_choice",
];

/// Prefix for cold block shard tables (`blocks_{suffix}`).
pub use crate::keys::BLOCKS_SHARD_PREFIX;
/// Prefix for cold column shard tables (`columns_{suffix}`).
pub use crate::keys::COLUMNS_SHARD_PREFIX;

/// Shard widths recorded for docs / registry (Deviation 1 / ADR P4-10).
pub const COLUMN_SHARD_WIDTH_EPOCHS: u64 = COLUMN_SHARD_EPOCHS;
/// Block / state-root class shard width in epochs.
pub const BLOCK_SHARD_WIDTH_EPOCHS: u64 = BLOCK_SHARD_EPOCHS;

/// Whether `name` is a registered table (fixed inventory or shard pattern).
///
/// Shard names match [`crate::keys`] / CC-40b: canonical suffixes from
/// [`crate::keys::format_shard_suffix`] (`blocks_00042`, `columns_00042`,
/// `blocks_100000` once the five-digit pad overflows). Unregistered names
/// fail open (I-shards reconciliation light for CC-40a).
pub fn is_registered_table(name: &str) -> bool {
    if FIXED_TABLES.contains(&name) {
        return true;
    }
    parse_shard_table(name).is_some()
}

/// Parse `blocks_{suffix}` / `columns_{suffix}` → `(class, shard_id)`.
pub fn parse_shard_table(name: &str) -> Option<(&'static str, u64)> {
    if let Some(rest) = name.strip_prefix(BLOCKS_SHARD_PREFIX) {
        return parse_shard_suffix(rest).map(|id| ("blocks", id));
    }
    if let Some(rest) = name.strip_prefix(COLUMNS_SHARD_PREFIX) {
        return parse_shard_suffix(rest).map(|id| ("columns", id));
    }
    None
}

/// Shard tables present in `names` (from [`Engine::table_names`]).
///
/// Cost is O(`names.len()`) — the live on-disk set — never O(all shard ids
/// ever assigned). Consumers that used to walk `0..=head_shard` should use
/// this instead (P0-18/1).
pub fn iter_shard_tables(names: &[String]) -> impl Iterator<Item = (&str, &'static str, u64)> {
    names
        .iter()
        .filter_map(|n| parse_shard_table(n).map(|(class, id)| (n.as_str(), class, id)))
}

/// Expected shard table name for documentation / tests (delegates to keys).
pub fn registered_blocks_shard(shard_id: u64) -> String {
    blocks_shard_table(shard_id)
}

/// Expected column shard table name.
pub fn registered_columns_shard(shard_id: u64) -> String {
    columns_shard_table(shard_id)
}

/// Reconcile engine table names against the registry.
///
/// Returns the first unregistered name, if any.
///
/// Walks only `names` (the live on-disk set from [`Engine::table_names`]).
/// Does not materialise expected names for shard ids `0..=head` — that
/// would be O(all shards ever) and is how a 30-day node would grow the
/// intern pool and the open path together (P0-18/1).
pub fn find_unregistered_table(names: &[String]) -> Option<&str> {
    names
        .iter()
        .find(|n| !is_registered_table(n))
        .map(String::as_str)
}

// ---------------------------------------------------------------------------
// Config digest
// ---------------------------------------------------------------------------

/// One `BLOB_SCHEDULE` entry as digested (epoch + max blobs).
#[derive(Debug, Clone, Copy, PartialEq, Eq, SszEncode, SszDecode)]
struct BlobScheduleDigestEntry {
    epoch: u64,
    max_blobs_per_block: u64,
}

/// Canonical SSZ payload for the config digest.
///
/// Field names / comments carry the §2.5 grep anchors; do not add fields without
/// a schema version bump.
#[derive(Debug, Clone, PartialEq, Eq, SszEncode, SszDecode)]
struct ConfigDigestPayload {
    // --- fork epochs ---
    altair_fork_epoch: u64,
    bellatrix_fork_epoch: u64,
    capella_fork_epoch: u64,
    deneb_fork_epoch: u64,
    electra_fork_epoch: u64,
    fulu_fork_epoch: u64,
    /// `SECONDS_PER_SLOT`
    seconds_per_slot: u64,
    /// `BLOB_SCHEDULE` entries in activation order.
    blob_schedule: VariableList<BlobScheduleDigestEntry, U256>,
    /// `genesis_validators_root`
    genesis_validators_root: Root,
    /// `MIN_VALIDATOR_WITHDRAWABILITY_DELAY`
    min_validator_withdrawability_delay: u64,
    /// `CHURN_LIMIT_QUOTIENT`
    churn_limit_quotient: u64,
}

/// Inputs that enter [`compute_config_digest`] — the named §2.5 list only.
///
/// Built from a [`ChainConfig`] plus `genesis_validators_root` and
/// `MIN_VALIDATOR_WITHDRAWABILITY_DELAY`. `CHURN_LIMIT_QUOTIENT` is also on
/// [`ChainConfig`] but the digest keeps its own copy (named §2.5 list).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigDigestInput {
    /// Fork epochs + `BLOB_SCHEDULE` + `SECONDS_PER_SLOT` source.
    pub chain: ChainConfig,
    /// `genesis_validators_root` (beacon-state / genesis field).
    pub genesis_validators_root: Root,
    /// `MIN_VALIDATOR_WITHDRAWABILITY_DELAY`.
    pub min_validator_withdrawability_delay: u64,
    /// `CHURN_LIMIT_QUOTIENT`.
    pub churn_limit_quotient: u64,
}

impl ConfigDigestInput {
    /// Construct from named pieces (tests / open path).
    pub fn new(
        chain: ChainConfig,
        genesis_validators_root: Root,
        min_validator_withdrawability_delay: u64,
        churn_limit_quotient: u64,
    ) -> Self {
        Self {
            chain,
            genesis_validators_root,
            min_validator_withdrawability_delay,
            churn_limit_quotient,
        }
    }

    /// Hoodi mainnet-preset defaults for the two scalar config constants.
    pub fn with_mainnet_scalars(chain: ChainConfig, genesis_validators_root: Root) -> Self {
        Self {
            chain,
            genesis_validators_root,
            // MIN_VALIDATOR_WITHDRAWABILITY_DELAY (mainnet / hoodi)
            min_validator_withdrawability_delay: 256,
            // CHURN_LIMIT_QUOTIENT (mainnet / hoodi)
            churn_limit_quotient: 65_536,
        }
    }
}

/// Maximum `BLOB_SCHEDULE` entries the digest encoding accepts (`VariableList<…, U256>`).
///
/// Real networks ship a handful of BPO entries; exceeding this is a config error,
/// not a silent truncation (SEC-40a-1).
pub const CONFIG_DIGEST_BLOB_SCHEDULE_MAX: usize = 256;

/// Compute the config digest over the §2.5 named field list only.
///
/// Encoding: SSZ of [`ConfigDigestPayload`], then SHA-256 → [`Root`].
///
/// Fails closed if `BLOB_SCHEDULE` does not fit the encoding capacity
/// ([`CONFIG_DIGEST_BLOB_SCHEDULE_MAX`]) — never truncates or digests a default.
///
/// Grep-visible field list:
/// `genesis_validators_root`, `BLOB_SCHEDULE`, `SECONDS_PER_SLOT`,
/// `MIN_VALIDATOR_WITHDRAWABILITY_DELAY`, `CHURN_LIMIT_QUOTIENT`, fork epochs.
pub fn compute_config_digest(input: &ConfigDigestInput) -> Result<Root, StoreError> {
    let n = input.chain.blob_schedule.entries().len();
    if n > CONFIG_DIGEST_BLOB_SCHEDULE_MAX {
        return Err(StoreError::Config(format!(
            "BLOB_SCHEDULE has {n} entries; digest encoding capacity is \
             {CONFIG_DIGEST_BLOB_SCHEDULE_MAX} (refuse to truncate)"
        )));
    }
    let entries: Vec<BlobScheduleDigestEntry> = input
        .chain
        .blob_schedule
        .entries()
        .iter()
        .map(|e| BlobScheduleDigestEntry {
            epoch: e.epoch.as_u64(),
            max_blobs_per_block: e.max_blobs_per_block,
        })
        .collect();
    // Fail closed on encode capacity — no take(N) / unwrap_or_default (SEC-40a-1).
    let blob_schedule = VariableList::new(entries).map_err(|e| {
        StoreError::Config(format!(
            "BLOB_SCHEDULE does not fit digest encoding (capacity \
             {CONFIG_DIGEST_BLOB_SCHEDULE_MAX}): {e}"
        ))
    })?;

    let payload = ConfigDigestPayload {
        altair_fork_epoch: epoch_u64(input.chain.altair_fork_epoch),
        bellatrix_fork_epoch: epoch_u64(input.chain.bellatrix_fork_epoch),
        capella_fork_epoch: epoch_u64(input.chain.capella_fork_epoch),
        deneb_fork_epoch: epoch_u64(input.chain.deneb_fork_epoch),
        electra_fork_epoch: epoch_u64(input.chain.electra_fork_epoch),
        fulu_fork_epoch: epoch_u64(input.chain.fulu_fork_epoch),
        // SECONDS_PER_SLOT
        seconds_per_slot: input.chain.seconds_per_slot,
        // BLOB_SCHEDULE
        blob_schedule,
        // genesis_validators_root
        genesis_validators_root: input.genesis_validators_root,
        // MIN_VALIDATOR_WITHDRAWABILITY_DELAY
        min_validator_withdrawability_delay: input.min_validator_withdrawability_delay,
        // CHURN_LIMIT_QUOTIENT
        churn_limit_quotient: input.churn_limit_quotient,
    };

    let bytes = payload.as_ssz_bytes();
    let hash = Sha256::digest(&bytes);
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&hash);
    Ok(Root::from_array(arr))
}

fn epoch_u64(e: Epoch) -> u64 {
    e.as_u64()
}

fn root_hex(r: &Root) -> String {
    let mut s = String::with_capacity(66);
    s.push_str("0x");
    for b in r.as_slice() {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

// ---------------------------------------------------------------------------
// Store open-or-refuse
// ---------------------------------------------------------------------------

/// Options for [`Store::open`].
///
/// [`Debug`] is hand-written: `expected_node_id` is the raw node key.
#[derive(Clone)]
pub struct StoreOpenOptions {
    /// Engine durability / open knobs.
    pub engine: EngineOptions,
    /// Expected config digest (from [`compute_config_digest`] at process start).
    pub config_digest: Root,
    /// Run §2.7 invariants at open (`storage.check_invariants`).
    pub check_invariants: bool,
    /// Node id derived from the node key (`I-node-id`). `None` skips that check.
    pub expected_node_id: Option<Root>,
    /// Snapshot ring depth for `I-ring` (`storage.snapshot_ring`, default 4).
    pub snapshot_ring: u64,
    /// Per-check row cap at open (`storage.max_open_scan_rows`).
    pub max_open_scan_rows: u64,
    /// Optional invocation counter for tests (CC-4H /2). Production leaves `None`.
    pub invocation_counter: Option<std::sync::Arc<std::sync::atomic::AtomicU64>>,
}

impl fmt::Debug for StoreOpenOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoreOpenOptions")
            .field("engine", &self.engine)
            .field("config_digest", &self.config_digest)
            .field("check_invariants", &self.check_invariants)
            .field(
                "expected_node_id",
                &self.expected_node_id.as_ref().map(|_| "<redacted>"),
            )
            .field("snapshot_ring", &self.snapshot_ring)
            .field("max_open_scan_rows", &self.max_open_scan_rows)
            .field("invocation_counter", &self.invocation_counter)
            .finish()
    }
}

impl StoreOpenOptions {
    /// Build options from a digest input (computes the expected digest).
    ///
    /// Propagates [`compute_config_digest`] errors (e.g. oversized `BLOB_SCHEDULE`).
    /// Invariant checks default **off** here; set [`Self::check_invariants`] from config.
    pub fn from_config(
        engine: EngineOptions,
        input: &ConfigDigestInput,
    ) -> Result<Self, StoreError> {
        Ok(Self {
            engine,
            config_digest: compute_config_digest(input)?,
            check_invariants: false,
            expected_node_id: None,
            snapshot_ring: crate::invariants::DEFAULT_SNAPSHOT_RING,
            max_open_scan_rows: crate::invariants::DEFAULT_MAX_OPEN_SCAN_ROWS,
            invocation_counter: None,
        })
    }

    /// Explicit expected digest.
    pub fn with_digest(engine: EngineOptions, config_digest: Root) -> Self {
        Self {
            engine,
            config_digest,
            check_invariants: false,
            expected_node_id: None,
            snapshot_ring: crate::invariants::DEFAULT_SNAPSHOT_RING,
            max_open_scan_rows: crate::invariants::DEFAULT_MAX_OPEN_SCAN_ROWS,
            invocation_counter: None,
        }
    }

    /// Enable or disable §2.7 checks at open / post-pass call sites.
    pub fn with_check_invariants(mut self, enabled: bool) -> Self {
        self.check_invariants = enabled;
        self
    }

    /// Set the expected node id for `I-node-id`.
    pub fn with_expected_node_id(mut self, node_id: Option<Root>) -> Self {
        self.expected_node_id = node_id;
        self
    }

    /// Set snapshot ring depth for `I-ring`.
    pub fn with_snapshot_ring(mut self, ring: u64) -> Self {
        self.snapshot_ring = ring;
        self
    }

    /// Set the per-check open-scan row budget (`storage.max_open_scan_rows`).
    pub fn with_max_open_scan_rows(mut self, max_open_scan_rows: u64) -> Self {
        self.max_open_scan_rows = max_open_scan_rows;
        self
    }

    /// Attach a shared invocation counter for tests (CC-4H /2).
    pub fn with_invocation_counter(
        mut self,
        counter: std::sync::Arc<std::sync::atomic::AtomicU64>,
    ) -> Self {
        self.invocation_counter = Some(counter);
        self
    }

    /// Invariant context derived from these options.
    pub fn invariant_context(&self) -> crate::invariants::InvariantContext {
        crate::invariants::InvariantContext {
            expected_node_id: self.expected_node_id,
            snapshot_ring: self.snapshot_ring,
            max_open_scan_rows: self.max_open_scan_rows,
            invocation_counter: self.invocation_counter.clone(),
        }
    }
}

/// Opened store: engine + schema/config gates already passed.
#[derive(Debug)]
pub struct Store {
    engine: Engine,
    /// Whether §2.7 checks run at open and after migration/prune passes.
    check_invariants: bool,
    /// Snapshot ring depth + expected node id for post-pass checks.
    invariant_ctx: crate::invariants::InvariantContext,
}

impl Store {
    /// Open (or create) a store under `path` with open-or-refuse semantics.
    ///
    /// 1. Open the engine.
    /// 2. Reconcile `table_names()` against the registry (unregistered → error).
    /// 3. Read `SchemaVersion` / `ConfigDigest` from `meta`:
    ///    - missing on a **fresh** (no tables) store → write expected values;
    ///    - missing on a non-empty store → refuse;
    ///    - present → compare; mismatch names **found** and **expected**.
    /// 4. When `opts.check_invariants`, run §2.7 checks (**fatal** on violation).
    pub fn open(path: &Path, opts: StoreOpenOptions) -> Result<Self, StoreError> {
        let engine = Engine::open(path, opts.engine.clone())?;
        Self::bind(engine, &opts)
    }

    /// Bind an already-opened engine (tests).
    pub fn bind(engine: Engine, opts: &StoreOpenOptions) -> Result<Self, StoreError> {
        let expected_digest = opts.config_digest;
        // Registry reconciliation is O(on-disk tables) = O(active shards after
        // prune), never O(all shard ids ever assigned). I-shards depth is CC-4H.
        let names = engine.table_names()?;
        if let Some(bad) = find_unregistered_table(&names) {
            return Err(StoreError::UnregisteredTable(bad.to_owned()));
        }

        let rt = engine.read()?;
        let version_bytes = rt.get(TABLE_META, KEY_SCHEMA_VERSION.as_bytes())?;
        let digest_bytes = rt.get(TABLE_META, KEY_CONFIG_DIGEST.as_bytes())?;
        drop(rt);

        match (version_bytes, digest_bytes) {
            (None, None) => {
                // Fresh store only when no tables exist yet.
                if !names.is_empty() {
                    return Err(StoreError::MissingMeta(KEY_SCHEMA_VERSION));
                }
                Self::write_bootstrap(&engine, expected_digest)?;
            }
            (Some(vb), Some(db)) => {
                let found_sv = SchemaVersion::from_ssz_bytes(&vb)
                    .map_err(|e| StoreError::Codec(format!("SchemaVersion: {e:?}")))?;
                if found_sv.version != SCHEMA_VERSION {
                    return Err(StoreError::SchemaVersionMismatch {
                        found: found_sv.version,
                        expected: SCHEMA_VERSION,
                    });
                }
                let found_cd = ConfigDigest::from_ssz_bytes(&db)
                    .map_err(|e| StoreError::Codec(format!("ConfigDigest: {e:?}")))?;
                if found_cd.digest != expected_digest {
                    return Err(StoreError::ConfigDigestMismatch {
                        found: root_hex(&found_cd.digest),
                        expected: root_hex(&expected_digest),
                    });
                }
            }
            (None, Some(_)) => return Err(StoreError::MissingMeta(KEY_SCHEMA_VERSION)),
            (Some(_), None) => return Err(StoreError::MissingMeta(KEY_CONFIG_DIGEST)),
        }

        let invariant_ctx = opts.invariant_context();
        crate::invariants::run_invariant_checks_if_enabled(
            &engine,
            opts.check_invariants,
            crate::invariants::InvariantCheckMode::Open,
            &invariant_ctx,
            None,
        )?;

        Ok(Self {
            engine,
            check_invariants: opts.check_invariants,
            invariant_ctx,
        })
    }

    /// Whether `storage.check_invariants` is enabled for this open.
    pub fn check_invariants_enabled(&self) -> bool {
        self.check_invariants
    }

    /// Run §2.7 checks after a migration pass (logged+counted when enabled).
    pub fn after_migration_pass(
        &self,
        sink: Option<&dyn crate::invariants::InvariantSink>,
    ) -> Result<u64, StoreError> {
        crate::invariants::run_invariant_checks_if_enabled(
            &self.engine,
            self.check_invariants,
            crate::invariants::InvariantCheckMode::PostPass,
            &self.invariant_ctx,
            sink,
        )
    }

    /// Run §2.7 checks after a prune pass (logged+counted when enabled).
    pub fn after_prune_pass(
        &self,
        sink: Option<&dyn crate::invariants::InvariantSink>,
    ) -> Result<u64, StoreError> {
        crate::invariants::run_invariant_checks_if_enabled(
            &self.engine,
            self.check_invariants,
            crate::invariants::InvariantCheckMode::PostPass,
            &self.invariant_ctx,
            sink,
        )
    }

    fn write_bootstrap(engine: &Engine, digest: Root) -> Result<(), StoreError> {
        let sv = SchemaVersion {
            version: SCHEMA_VERSION,
        };
        let cd = ConfigDigest { digest };
        let mut batch = engine.batch();
        batch.put(
            TABLE_META,
            KEY_SCHEMA_VERSION.as_bytes(),
            &sv.as_ssz_bytes(),
        );
        batch.put(TABLE_META, KEY_CONFIG_DIGEST.as_bytes(), &cd.as_ssz_bytes());
        engine.commit(batch)
    }

    /// Borrow the underlying engine.
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Consume into the engine (tests / advanced use).
    pub fn into_engine(self) -> Engine {
        self.engine
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::meta::{ConfigDigest as MetaConfigDigest, SchemaVersion as MetaSchemaVersion};
    use cc_types::{BlobParameters, BlobSchedule, ChainConfig, Epoch, PresetName};
    use ssz::Encode;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("cc-store-schema-{label}-{nanos}"));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn hoodi_like_chain() -> ChainConfig {
        // Minimal ChainConfig shaped like Hoodi for digest tests (fork epochs + BLOB_SCHEDULE).
        let blob_schedule = BlobSchedule::try_from_entries(vec![
            BlobParameters {
                epoch: Epoch::new(52_480),
                max_blobs_per_block: 15,
            },
            BlobParameters {
                epoch: Epoch::new(54_016),
                max_blobs_per_block: 21,
            },
        ])
        .unwrap();
        ChainConfig {
            preset_base: PresetName::Mainnet,
            config_name: "hoodi".into(),
            genesis_fork_version: cc_types::ForkVersion::from_array([0x10, 0x00, 0x09, 0x10]),
            altair_fork_version: cc_types::ForkVersion::from_array([0x20, 0x00, 0x09, 0x10]),
            altair_fork_epoch: Epoch::new(0),
            bellatrix_fork_version: cc_types::ForkVersion::from_array([0x30, 0x00, 0x09, 0x10]),
            bellatrix_fork_epoch: Epoch::new(0),
            capella_fork_version: cc_types::ForkVersion::from_array([0x40, 0x00, 0x09, 0x10]),
            capella_fork_epoch: Epoch::new(0),
            deneb_fork_version: cc_types::ForkVersion::from_array([0x50, 0x00, 0x09, 0x10]),
            deneb_fork_epoch: Epoch::new(0),
            electra_fork_version: cc_types::ForkVersion::from_array([0x60, 0x00, 0x09, 0x10]),
            electra_fork_epoch: Epoch::new(2_048),
            fulu_fork_version: cc_types::ForkVersion::from_array([0x70, 0x00, 0x09, 0x10]),
            fulu_fork_epoch: Epoch::new(50_688),
            seconds_per_slot: 12,
            blob_schedule,
            deposit_chain_id: 560_048,
            deposit_contract_address: cc_types::ExecutionAddress::from_array([0u8; 20]),
            churn_limit_quotient: 65_536,
            min_per_epoch_churn_limit_electra: 128_000_000_000,
            max_per_epoch_activation_exit_churn_limit: 256_000_000_000,
            shard_committee_period: Epoch::new(256),
            max_blobs_per_block_electra: 9,
        }
    }

    fn hoodi_input() -> ConfigDigestInput {
        ConfigDigestInput::with_mainnet_scalars(hoodi_like_chain(), Root::from_array([0xAB; 32]))
    }

    fn open_opts(input: &ConfigDigestInput) -> StoreOpenOptions {
        StoreOpenOptions::from_config(EngineOptions::default(), input)
            .unwrap()
            // Schema tests focus on version/digest gates; invariants are CC-4H.
            .with_check_invariants(false)
    }

    #[test]
    fn open_fresh_writes_schema_and_digest() {
        let dir = tmp_dir("fresh");
        let input = hoodi_input();
        let digest = compute_config_digest(&input).unwrap();
        let store = Store::open(&dir, open_opts(&input)).unwrap();
        let rt = store.engine().read().unwrap();
        let sv = MetaSchemaVersion::from_ssz_bytes(
            &rt.get(TABLE_META, KEY_SCHEMA_VERSION.as_bytes())
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(sv.version, SCHEMA_VERSION);
        let cd = MetaConfigDigest::from_ssz_bytes(
            &rt.get(TABLE_META, KEY_CONFIG_DIGEST.as_bytes())
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(cd.digest, digest);
        // Re-open succeeds with same digest.
        drop(rt);
        drop(store);
        Store::open(&dir, open_opts(&input)).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn schema_version_mismatch_refuses_with_found_and_expected() {
        // CC-40 /5: rewrite SchemaVersion to v+1 → refuse naming both values.
        let dir = tmp_dir("sv-mismatch");
        let input = hoodi_input();
        let store = Store::open(&dir, open_opts(&input)).unwrap();
        let eng = store.into_engine();
        let bumped = MetaSchemaVersion {
            version: SCHEMA_VERSION + 1,
        };
        let mut b = eng.batch();
        b.put(
            TABLE_META,
            KEY_SCHEMA_VERSION.as_bytes(),
            &bumped.as_ssz_bytes(),
        );
        eng.commit(b).unwrap();
        drop(eng);

        let err = Store::open(&dir, open_opts(&input)).unwrap_err();
        let msg = err.to_string();
        assert!(
            matches!(
                err,
                StoreError::SchemaVersionMismatch {
                    found,
                    expected
                } if found == SCHEMA_VERSION + 1 && expected == SCHEMA_VERSION
            ),
            "err={err:?}"
        );
        assert!(
            msg.contains(&(SCHEMA_VERSION + 1).to_string())
                && msg.contains(&SCHEMA_VERSION.to_string()),
            "Display must name found and expected: {msg}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn blob_schedule_mutation_refuses_config_digest() {
        // CC-40 /6: Hoodi BLOB_SCHEDULE 54016→21 becomes 54016→20 → refuse.
        let dir = tmp_dir("blob-digest");
        let input = hoodi_input();
        Store::open(&dir, open_opts(&input)).unwrap();

        let mut mutated = hoodi_like_chain();
        let entries = mutated.blob_schedule.entries().to_vec();
        assert_eq!(entries[1].epoch, Epoch::new(54_016));
        assert_eq!(entries[1].max_blobs_per_block, 21);
        let mut new_entries = entries;
        new_entries[1].max_blobs_per_block = 20; // 54016 → 20
        mutated.blob_schedule = BlobSchedule::try_from_entries(new_entries).unwrap();
        let mutated_input =
            ConfigDigestInput::with_mainnet_scalars(mutated, input.genesis_validators_root);

        let err = Store::open(&dir, open_opts(&mutated_input)).unwrap_err();
        let msg = err.to_string();
        assert!(
            matches!(err, StoreError::ConfigDigestMismatch { .. }),
            "err={err:?}"
        );
        assert!(
            msg.to_ascii_lowercase().contains("digest"),
            "error must name config digest: {msg}"
        );
        // Found + expected both present as hex.
        if let StoreError::ConfigDigestMismatch { found, expected } = &err {
            assert_ne!(found, expected);
            assert!(msg.contains(found) && msg.contains(expected), "{msg}");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn irrelevant_config_field_change_still_opens() {
        // Digest ignores fields outside §2.5 list (e.g. deposit_chain_id, config_name).
        let dir = tmp_dir("irrelevant");
        let input = hoodi_input();
        Store::open(&dir, open_opts(&input)).unwrap();

        let mut tweaked = hoodi_like_chain();
        tweaked.deposit_chain_id = 999_999_999;
        tweaked.config_name = "not-hoodi".into();
        tweaked.deposit_contract_address = cc_types::ExecutionAddress::from_array([0xFF; 20]);
        // Fork *versions* are also outside the digest list (epochs are in).
        tweaked.genesis_fork_version = cc_types::ForkVersion::from_array([0xFF; 4]);

        let tweaked_input =
            ConfigDigestInput::with_mainnet_scalars(tweaked, input.genesis_validators_root);
        assert_eq!(
            compute_config_digest(&input).unwrap(),
            compute_config_digest(&tweaked_input).unwrap(),
            "digest must be stable across irrelevant fields"
        );
        Store::open(&dir, open_opts(&tweaked_input)).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn unregistered_table_reported_at_open() {
        let dir = tmp_dir("unreg");
        let input = hoodi_input();
        let store = Store::open(&dir, open_opts(&input)).unwrap();
        let eng = store.into_engine();
        let mut b = eng.batch();
        b.put("evil_not_in_registry", b"k", b"v");
        eng.commit(b).unwrap();
        drop(eng);

        let err = Store::open(&dir, open_opts(&input)).unwrap_err();
        assert!(
            matches!(
                &err,
                StoreError::UnregisteredTable(n) if n == "evil_not_in_registry"
            ),
            "err={err:?}"
        );
        let msg = err.to_string();
        assert!(msg.contains("evil_not_in_registry"), "{msg}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn oversized_blob_schedule_fails_closed() {
        // SEC-40a-1: BLOB_SCHEDULE above digest capacity must error, not truncate.
        let mut chain = hoodi_like_chain();
        let entries: Vec<BlobParameters> = (0..=CONFIG_DIGEST_BLOB_SCHEDULE_MAX as u64)
            .map(|i| BlobParameters {
                epoch: Epoch::new(i),
                max_blobs_per_block: 6,
            })
            .collect();
        assert_eq!(entries.len(), CONFIG_DIGEST_BLOB_SCHEDULE_MAX + 1);
        chain.blob_schedule = BlobSchedule::try_from_entries(entries).unwrap();
        let input = ConfigDigestInput::with_mainnet_scalars(chain, Root::from_array([0xAB; 32]));
        let err = compute_config_digest(&input).unwrap_err();
        assert!(matches!(err, StoreError::Config(_)), "err={err:?}");
        let msg = err.to_string();
        assert!(
            msg.contains("BLOB_SCHEDULE") && msg.contains("capacity"),
            "{msg}"
        );
        // from_config must also refuse (no silent open path).
        let err2 = StoreOpenOptions::from_config(EngineOptions::default(), &input).unwrap_err();
        assert!(matches!(err2, StoreError::Config(_)), "err={err2:?}");
    }

    #[test]
    fn missing_schema_version_on_nonempty_store_refuses() {
        let dir = tmp_dir("missing-sv");
        let input = hoodi_input();
        let store = Store::open(&dir, open_opts(&input)).unwrap();
        let eng = store.into_engine();
        // Delete schema_version; leave config_digest + meta table present.
        let mut b = eng.batch();
        b.delete(TABLE_META, KEY_SCHEMA_VERSION.as_bytes());
        eng.commit(b).unwrap();
        drop(eng);

        let err = Store::open(&dir, open_opts(&input)).unwrap_err();
        assert!(
            matches!(err, StoreError::MissingMeta(KEY_SCHEMA_VERSION)),
            "err={err:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn corrupt_schema_version_bytes_refuse_codec() {
        let dir = tmp_dir("corrupt-sv");
        let input = hoodi_input();
        let store = Store::open(&dir, open_opts(&input)).unwrap();
        let eng = store.into_engine();
        let mut b = eng.batch();
        b.put(TABLE_META, KEY_SCHEMA_VERSION.as_bytes(), b"\x00\x01"); // truncated SSZ
        eng.commit(b).unwrap();
        drop(eng);

        let err = Store::open(&dir, open_opts(&input)).unwrap_err();
        assert!(matches!(err, StoreError::Codec(_)), "err={err:?}");
        let msg = err.to_string();
        assert!(
            msg.contains("SchemaVersion") || msg.contains("codec"),
            "{msg}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn registered_shard_names_match_keys() {
        assert!(is_registered_table("meta"));
        assert!(is_registered_table(&blocks_shard_table(0)));
        assert!(is_registered_table(&columns_shard_table(42)));
        assert!(is_registered_table(&blocks_shard_table(100_000)));
        assert!(!is_registered_table("blocks_42")); // not zero-padded
        assert!(!is_registered_table("blocks_000042")); // wrong width
        assert!(!is_registered_table("state_roots_00001"));
        assert_eq!(COLUMN_SHARD_WIDTH_EPOCHS, 32);
        assert_eq!(BLOCK_SHARD_WIDTH_EPOCHS, 256);
    }

    #[test]
    fn registry_reconcile_is_o_active_names_not_history() {
        // A 30-day (or year-10) node that has pruned 0..N must not force the
        // registry to materialise N historical names. Only the live set is walked.
        let live = vec![
            "meta".to_owned(),
            columns_shard_table(211),
            blocks_shard_table(26),
        ];
        assert!(find_unregistered_table(&live).is_none());
        let active: Vec<_> = iter_shard_tables(&live).collect();
        assert_eq!(active.len(), 2);
        assert_eq!(active[0], ("columns_00211", "columns", 211));
        assert_eq!(active[1], ("blocks_00026", "blocks", 26));
        // Pattern match is O(1) per name — high ids are registered without a
        // 0..=id set.
        assert!(is_registered_table(&columns_shard_table(10_000)));
        let with_evil = [live.as_slice(), &["not_a_table".to_owned()]].concat();
        assert_eq!(find_unregistered_table(&with_evil), Some("not_a_table"));
    }

    #[test]
    fn thirty_plus_day_shard_rollover_does_not_exhaust_namespace() {
        // P0-18/1: 30 days of mainnet slots ≈ 211 column shards + 27 block shards.
        // Roll past the old 512 unique-name intern cap while dropping retired
        // shards; the live intern set and the registry stay O(active).
        use crate::engine::MAX_INTERNED_TABLE_NAMES;
        use crate::keys::{SLOTS_PER_EPOCH, block_shard_id, column_shard_id};
        use cc_types::Slot;

        const SECONDS_PER_SLOT: u64 = 12;
        const DAYS: u64 = 45;
        let slots = DAYS * 24 * 60 * 60 / SECONDS_PER_SLOT;
        let last_col = column_shard_id(Slot::new(slots));
        let last_blk = block_shard_id(Slot::new(slots));
        assert!(
            last_col >= 211,
            "45d must cover 30d of column shards; last_col={last_col}"
        );
        // Extra unique names so the old leak-until-512 cap would trip.
        let unique_cols = (MAX_INTERNED_TABLE_NAMES as u64 + 32).max(last_col + 1);

        let dir = tmp_dir("s2-b-04-rollover");
        let input = hoodi_input();
        let store = Store::open(&dir, open_opts(&input)).unwrap();
        let eng = store.engine();

        // Keep a small live window (retention-shaped), drop the rest.
        const LIVE_COL: u64 = 8;
        const LIVE_BLK: u64 = 2;
        for id in 0..unique_cols {
            let name = columns_shard_table(id);
            let mut b = eng.batch();
            b.put(&name, &id.to_be_bytes(), b"c");
            eng.commit(b).unwrap();
            if id >= LIVE_COL {
                eng.drop_table(&columns_shard_table(id - LIVE_COL)).unwrap();
            }
            assert!(
                eng.interned_name_count() <= LIVE_COL as usize + 8,
                "intern pool grew to {}",
                eng.interned_name_count()
            );
        }
        for id in 0..=last_blk {
            let name = blocks_shard_table(id);
            let mut b = eng.batch();
            b.put(&name, &id.to_be_bytes(), b"b");
            eng.commit(b).unwrap();
            if id >= LIVE_BLK {
                eng.drop_table(&blocks_shard_table(id - LIVE_BLK)).unwrap();
            }
        }

        let names = eng.table_names().unwrap();
        assert!(find_unregistered_table(&names).is_none());
        let shards: Vec<_> = iter_shard_tables(&names).collect();
        // meta + live column shards + live block shards (meta is not a shard).
        let col_live = shards.iter().filter(|(_, c, _)| *c == "columns").count();
        let blk_live = shards.iter().filter(|(_, c, _)| *c == "blocks").count();
        assert_eq!(col_live, LIVE_COL as usize);
        assert_eq!(blk_live, LIVE_BLK as usize);
        assert!(
            shards.len() < MAX_INTERNED_TABLE_NAMES,
            "active shards {} must stay under intern cap",
            shards.len()
        );
        // Epoch widths unchanged (ADR-P4-10).
        assert_eq!(
            crate::keys::column_shard_start_slot(1).as_u64(),
            COLUMN_SHARD_EPOCHS * SLOTS_PER_EPOCH
        );
        assert_eq!(
            crate::keys::block_shard_start_slot(1).as_u64(),
            BLOCK_SHARD_EPOCHS * SLOTS_PER_EPOCH
        );

        drop(store);
        // Re-open: registry walks only the live tables.
        Store::open(&dir, open_opts(&input)).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn digest_field_list_grep_anchors_present() {
        // AC: these strings appear in this source file (this test file is schema.rs tests;
        // the production path above is the greppable site). Mirror assert for CI clarity.
        let src = include_str!("schema.rs");
        for needle in [
            "genesis_validators_root",
            "BLOB_SCHEDULE",
            "SECONDS_PER_SLOT",
            "MIN_VALIDATOR_WITHDRAWABILITY_DELAY",
            "CHURN_LIMIT_QUOTIENT",
        ] {
            assert!(
                src.contains(needle),
                "schema.rs must name digest field {needle}"
            );
        }
    }
}
