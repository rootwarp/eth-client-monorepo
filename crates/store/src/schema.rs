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
//!
//! Running-config comparison is two side keys. The policy is ADR-R-11
//! (`docs/adr/ADR-R-11.md`): identity is fatal, schedule refuses only a move
//! at or below the store's finalized epoch, and this file does not choose a
//! different split. The legacy payload stays under `meta.config_digest`.

use std::fmt;
use std::path::Path;

use sha2::{Digest, Sha256};
use ssz::{Decode, Encode};
use ssz_derive::{Decode as SszDecode, Encode as SszEncode};
use ssz_types::VariableList;
use typenum::U256;

use cc_types::{ChainConfig, Epoch, ExecutionAddress, ForkVersion, Root};

use crate::blocks::TABLE_BLOCKS_HOT;
use crate::canonical::TABLE_CANONICAL;
use crate::engine::{Engine, EngineOptions, StoreError};
use crate::keys::{
    BLOCK_SHARD_EPOCHS, COLUMN_SHARD_EPOCHS, blocks_shard_table, columns_shard_table,
    parse_shard_suffix,
};
use crate::meta::{
    ConfigDigest, ForkChoiceScalars, KEY_CONFIG_DIGEST, KEY_CONFIG_DIGEST_V2, KEY_FC_SCALARS,
    KEY_SCHEDULE_DIGEST, KEY_SCHEMA_VERSION, SchemaVersion, TABLE_META,
};
use crate::snapshots::TABLE_SNAPSHOTS;

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

/// Legacy SSZ payload still stored under `meta.config_digest`.
///
/// Field names carry the §2.5 grep anchors. Do not add `digest_version` or any
/// other field: that would change the bytes the rollback binary compares.
/// Running-config buckets carry [`DIGEST_VERSION`] on their own payloads.
/// [`SCHEMA_VERSION`] is not bumped for that split.
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

/// Version mixed into both running-config bucket payloads.
///
/// Adding a digested field bumps this integer. It does not bump [`SCHEMA_VERSION`].
/// The legacy payload does not carry it.
pub const DIGEST_VERSION: u16 = 1;

/// Identity bucket: fatal network fields. `config_name` is not included.
#[derive(Debug, Clone, PartialEq, Eq, SszEncode, SszDecode)]
struct IdentityDigestPayload {
    digest_version: u16,
    genesis_fork_version: ForkVersion,
    deposit_contract_address: ExecutionAddress,
    deposit_chain_id: u64,
    genesis_validators_root: Root,
    seconds_per_slot: u64,
}

/// Schedule bucket: fork epochs and `BLOB_SCHEDULE`. Comparison is a later open.
#[derive(Debug, Clone, PartialEq, Eq, SszEncode, SszDecode)]
struct ScheduleDigestPayload {
    digest_version: u16,
    altair_fork_epoch: u64,
    bellatrix_fork_epoch: u64,
    capella_fork_epoch: u64,
    deneb_fork_epoch: u64,
    electra_fork_epoch: u64,
    fulu_fork_epoch: u64,
    blob_schedule: VariableList<BlobScheduleDigestEntry, U256>,
}

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
    let blob_schedule = blob_schedule_entries(&input.chain)?;
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
    Ok(sha256_root(&payload.as_ssz_bytes()))
}

/// Identity-bucket digest of the running network.
///
/// `digest_version` is part of the hashed payload, so bumping [`DIGEST_VERSION`]
/// changes the bytes without a [`SCHEMA_VERSION`] bump.
pub fn compute_identity_digest(chain: &ChainConfig, genesis_validators_root: Root) -> Root {
    identity_digest_at(chain, genesis_validators_root, DIGEST_VERSION)
}

/// Schedule-bucket digest of the running network.
///
/// Fails closed on an oversized `BLOB_SCHEDULE`, same as [`compute_config_digest`].
pub fn compute_schedule_digest(chain: &ChainConfig) -> Result<Root, StoreError> {
    schedule_digest_at(chain, DIGEST_VERSION)
}

fn identity_digest_at(
    chain: &ChainConfig,
    genesis_validators_root: Root,
    digest_version: u16,
) -> Root {
    let payload = IdentityDigestPayload {
        digest_version,
        genesis_fork_version: chain.genesis_fork_version,
        deposit_contract_address: chain.deposit_contract_address,
        deposit_chain_id: chain.deposit_chain_id,
        genesis_validators_root,
        seconds_per_slot: chain.seconds_per_slot,
    };
    sha256_root(&payload.as_ssz_bytes())
}

fn schedule_payload(
    chain: &ChainConfig,
    digest_version: u16,
) -> Result<ScheduleDigestPayload, StoreError> {
    Ok(ScheduleDigestPayload {
        digest_version,
        altair_fork_epoch: epoch_u64(chain.altair_fork_epoch),
        bellatrix_fork_epoch: epoch_u64(chain.bellatrix_fork_epoch),
        capella_fork_epoch: epoch_u64(chain.capella_fork_epoch),
        deneb_fork_epoch: epoch_u64(chain.deneb_fork_epoch),
        electra_fork_epoch: epoch_u64(chain.electra_fork_epoch),
        fulu_fork_epoch: epoch_u64(chain.fulu_fork_epoch),
        blob_schedule: blob_schedule_entries(chain)?,
    })
}

fn schedule_digest_at(chain: &ChainConfig, digest_version: u16) -> Result<Root, StoreError> {
    Ok(sha256_root(
        &schedule_payload(chain, digest_version)?.as_ssz_bytes(),
    ))
}

/// SSZ stored under [`KEY_CONFIG_DIGEST_V2`]: the identity-bucket hash.
///
/// `digest_version` is inside the hashed payload, not a second field.
#[must_use]
pub fn identity_side_key_bytes(chain: &ChainConfig, genesis_validators_root: Root) -> Vec<u8> {
    ConfigDigest {
        digest: compute_identity_digest(chain, genesis_validators_root),
    }
    .as_ssz_bytes()
}

/// SSZ stored under [`KEY_SCHEDULE_DIGEST`].
///
/// The preimage, not the hash: a forward schedule move has to be told apart
/// from a move at or below the finalized epoch, and a hash cannot say which.
pub fn schedule_side_key_bytes(chain: &ChainConfig) -> Result<Vec<u8>, StoreError> {
    Ok(schedule_payload(chain, DIGEST_VERSION)?.as_ssz_bytes())
}

/// Witness the caller decoded from a stored beacon state.
///
/// ADR-R-11: `BeaconState::genesis_validators_root` is the only field a
/// populated store can prove. The other four identity fields are not on the
/// anchor. This crate does not name the consensus container; the caller does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnchorGenesisValidatorsRoot {
    /// No snapshot row.
    Missing,
    /// Snapshot bytes were not a beacon state.
    Undecodable,
    /// Decoded genesis validators root.
    Value(Root),
}

/// Compare or stamp the two side keys. Policy: ADR-R-11.
///
/// An empty store (no canonical rows, no blocks, no snapshots) is stamped
/// from `chain` when both a network and a genesis validators root were
/// supplied. A populated store that already has both keys is compared: an
/// identity mismatch refuses, and a schedule move at or below
/// [`ForkChoiceScalars::finalized`] refuses. A move strictly above a decoded
/// finalized epoch logs at WARN and re-stamps the schedule key only.
///
/// A populated store with either side key absent is refused. Equality with
/// [`legacy_config_digest`] is not permission to write side keys. A matching
/// `anchor_gvr` is not permission either. There is no successful re-stamp of
/// a populated store. One key without the other is not repaired.
pub fn reconcile_config_side_keys(
    engine: &Engine,
    chain: Option<&ChainConfig>,
    genesis_validators_root: Option<Root>,
    anchor_gvr: AnchorGenesisValidatorsRoot,
) -> Result<(), StoreError> {
    let rt = engine.read()?;
    let identity = rt.get(TABLE_META, KEY_CONFIG_DIGEST_V2.as_bytes())?;
    let schedule = rt.get(TABLE_META, KEY_SCHEDULE_DIGEST.as_bytes())?;
    let stored_legacy = rt
        .get(TABLE_META, KEY_CONFIG_DIGEST.as_bytes())?
        .map(|bytes| {
            ConfigDigest::from_ssz_bytes(&bytes)
                .map(|record| record.digest)
                .map_err(|e| StoreError::Codec(format!("ConfigDigest: {e:?}")))
        })
        .transpose()?;
    drop(rt);
    let populated = store_holds_chain_history(engine)?;

    match (identity, schedule) {
        (Some(identity), Some(schedule)) => {
            compare_side_keys(engine, chain, genesis_validators_root, &identity, &schedule)
        }
        (None, None) if !populated => stamp_empty(engine, chain, genesis_validators_root),
        (None, None) => refuse_populated_without_side_keys(
            chain,
            genesis_validators_root,
            stored_legacy,
            anchor_gvr,
        ),
        _ => Err(StoreError::Config(
            "side key missing: deleting a side key does not open a second stamp (ADR-R-11)".into(),
        )),
    }
}

/// True when a populated store has neither side key and still holds the legacy
/// constant, so the caller should decode the anchor witness. The witness is
/// fail-closed only. A match does not permit a stamp.
pub fn legacy_open_needs_anchor_witness(engine: &Engine) -> Result<bool, StoreError> {
    if !store_holds_chain_history(engine)? {
        return Ok(false);
    }
    let rt = engine.read()?;
    let identity = rt.get(TABLE_META, KEY_CONFIG_DIGEST_V2.as_bytes())?;
    let schedule = rt.get(TABLE_META, KEY_SCHEDULE_DIGEST.as_bytes())?;
    if identity.is_some() || schedule.is_some() {
        return Ok(false);
    }
    let Some(bytes) = rt.get(TABLE_META, KEY_CONFIG_DIGEST.as_bytes())? else {
        return Ok(false);
    };
    let digest = ConfigDigest::from_ssz_bytes(&bytes)
        .map_err(|e| StoreError::Codec(format!("ConfigDigest: {e:?}")))?
        .digest;
    Ok(digest == legacy_config_digest())
}

fn stamp_empty(
    engine: &Engine,
    chain: Option<&ChainConfig>,
    genesis_validators_root: Option<Root>,
) -> Result<(), StoreError> {
    let (Some(chain), Some(gvr)) = (chain, genesis_validators_root) else {
        // No running network yet (beacon-core threads it later), or no GVR.
        // Do not stamp `Root::ZERO`. An empty store has nothing to contradict.
        return Ok(());
    };
    stamp_both(engine, chain, gvr)
}

/// ADR-R-11: history plus a missing side key is a refusal. Never [`stamp_both`].
fn refuse_populated_without_side_keys(
    chain: Option<&ChainConfig>,
    genesis_validators_root: Option<Root>,
    stored_legacy: Option<Root>,
    anchor_gvr: AnchorGenesisValidatorsRoot,
) -> Result<(), StoreError> {
    let Some(gvr) = genesis_validators_root else {
        return Err(StoreError::Config(
            "genesis_validators_root is required when the store holds chain history \
             (absent GVR is not Root::ZERO)"
                .into(),
        ));
    };
    if chain.is_none() {
        return Err(StoreError::Config(
            "populated store refused: config_digest equality with the legacy constant \
             is not an open until side keys are compared"
                .into(),
        ));
    }
    if stored_legacy != Some(legacy_config_digest()) {
        return Err(StoreError::Config(
            "populated store refused: meta.config_digest is not the legacy constant, \
             so it is not re-stamped"
                .into(),
        ));
    }
    // The anchor check only adds a more specific refusal. A match is not a write.
    match anchor_gvr {
        AnchorGenesisValidatorsRoot::Value(found) if found == gvr => Err(StoreError::Config(
            "populated store refused: a matching anchor genesis_validators_root is not \
             permission to write side keys (ADR-R-11)"
                .into(),
        )),
        AnchorGenesisValidatorsRoot::Value(_) => Err(StoreError::Config(
            "populated store refused: anchor genesis_validators_root does not match \
             the running config"
                .into(),
        )),
        AnchorGenesisValidatorsRoot::Missing => Err(StoreError::Config(
            "populated store refused: no anchor genesis_validators_root to cross-check".into(),
        )),
        AnchorGenesisValidatorsRoot::Undecodable => Err(StoreError::Config(
            "populated store refused: anchor state did not yield genesis_validators_root".into(),
        )),
    }
}

fn compare_side_keys(
    engine: &Engine,
    chain: Option<&ChainConfig>,
    genesis_validators_root: Option<Root>,
    identity: &[u8],
    schedule: &[u8],
) -> Result<(), StoreError> {
    let Some(chain) = chain else {
        return Err(StoreError::Config(
            "running config is required to compare config side keys (ADR-R-11)".into(),
        ));
    };
    let Some(gvr) = genesis_validators_root else {
        return Err(StoreError::Config(
            "genesis_validators_root is required when the store holds chain history \
             (absent GVR is not Root::ZERO)"
                .into(),
        ));
    };
    let found = ConfigDigest::from_ssz_bytes(identity)
        .map_err(|e| StoreError::Codec(format!("config_digest_v2: {e:?}")))?;
    let expected = compute_identity_digest(chain, gvr);
    if found.digest != expected {
        return Err(StoreError::Config(
            "identity digest mismatch: config_digest_v2 does not match \
             genesis_fork_version, deposit_contract_address, deposit_chain_id, \
             genesis_validators_root, or seconds_per_slot (ADR-R-11)"
                .into(),
        ));
    }
    compare_schedule(engine, chain, schedule)
}

fn compare_schedule(
    engine: &Engine,
    chain: &ChainConfig,
    schedule: &[u8],
) -> Result<(), StoreError> {
    let stored = ScheduleDigestPayload::from_ssz_bytes(schedule)
        .map_err(|e| StoreError::Codec(format!("schedule_digest: {e:?}")))?;
    if stored.digest_version != DIGEST_VERSION {
        return Err(StoreError::Config(format!(
            "schedule digest_version mismatch: found {}, expected {DIGEST_VERSION}",
            stored.digest_version
        )));
    }
    let running = schedule_payload(chain, DIGEST_VERSION)?;
    if stored == running {
        return Ok(());
    }
    let finalized = finalized_epoch(engine)?;
    match classify_schedule(&stored, &running, finalized) {
        ScheduleVerdict::Unchanged => Ok(()),
        ScheduleVerdict::Historical => Err(StoreError::Config(format!(
            "schedule digest mismatch: a fork epoch or BLOB_SCHEDULE entry moved \
             at or below finalized epoch {finalized} (ADR-R-11)"
        ))),
        ScheduleVerdict::Forward => {
            tracing::warn!(
                finalized_epoch = finalized,
                "ADR-R-11 schedule bucket moved above the finalized epoch; \
                 re-stamping schedule digest"
            );
            let bytes = running.as_ssz_bytes();
            put_meta(engine, &[(KEY_SCHEDULE_DIGEST, bytes.as_slice())])
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScheduleVerdict {
    Unchanged,
    /// A boundary at or below the finalized epoch moved.
    Historical,
    /// Every moved boundary is strictly above the finalized epoch.
    Forward,
}

fn classify_schedule(
    stored: &ScheduleDigestPayload,
    running: &ScheduleDigestPayload,
    finalized_epoch: u64,
) -> ScheduleVerdict {
    if stored == running {
        return ScheduleVerdict::Unchanged;
    }
    let mut historical = false;
    let mut note_move = |old: u64, new: u64| {
        if old != new && (old <= finalized_epoch || new <= finalized_epoch) {
            historical = true;
        }
    };
    note_move(stored.altair_fork_epoch, running.altair_fork_epoch);
    note_move(stored.bellatrix_fork_epoch, running.bellatrix_fork_epoch);
    note_move(stored.capella_fork_epoch, running.capella_fork_epoch);
    note_move(stored.deneb_fork_epoch, running.deneb_fork_epoch);
    note_move(stored.electra_fork_epoch, running.electra_fork_epoch);
    note_move(stored.fulu_fork_epoch, running.fulu_fork_epoch);
    note_blob_edits(
        &stored.blob_schedule,
        &running.blob_schedule,
        finalized_epoch,
        &mut historical,
    );
    if historical {
        ScheduleVerdict::Historical
    } else {
        // No boundary at or below finalized moved. Re-stamp, including a
        // list-order-only difference.
        ScheduleVerdict::Forward
    }
}

fn note_blob_edits(
    stored: &VariableList<BlobScheduleDigestEntry, U256>,
    running: &VariableList<BlobScheduleDigestEntry, U256>,
    finalized_epoch: u64,
    historical: &mut bool,
) {
    let stored_map = blob_map(stored);
    let running_map = blob_map(running);
    let mut note = |epoch: u64| {
        if epoch <= finalized_epoch {
            *historical = true;
        }
    };
    for (epoch, max) in &stored_map {
        match running_map.get(epoch) {
            Some(other) if other == max => {}
            _ => note(*epoch),
        }
    }
    for (epoch, max) in &running_map {
        match stored_map.get(epoch) {
            Some(other) if other == max => {}
            _ => note(*epoch),
        }
    }
}

fn blob_map(
    entries: &VariableList<BlobScheduleDigestEntry, U256>,
) -> std::collections::BTreeMap<u64, u64> {
    entries
        .iter()
        .map(|entry| (entry.epoch, entry.max_blobs_per_block))
        .collect()
}

/// Decoded `ForkChoiceScalars.finalized.epoch`. Missing or undecodable bytes
/// are not epoch 0: a schedule re-stamp needs a proof that every moved
/// boundary is strictly above this epoch (ADR-R-11).
fn finalized_epoch(engine: &Engine) -> Result<u64, StoreError> {
    let rt = engine.read()?;
    let Some(bytes) = rt.get(TABLE_META, KEY_FC_SCALARS.as_bytes())? else {
        return Err(StoreError::Config(
            "schedule re-stamp refused: meta.fc_scalars is missing, so no finalized \
             epoch proves the move is above it (ADR-R-11)"
                .into(),
        ));
    };
    let scalars = ForkChoiceScalars::from_ssz_bytes(&bytes).map_err(|e| {
        StoreError::Config(format!(
            "schedule re-stamp refused: meta.fc_scalars did not decode ({e:?}) (ADR-R-11)"
        ))
    })?;
    Ok(scalars.finalized.epoch.as_u64())
}

fn stamp_both(engine: &Engine, chain: &ChainConfig, gvr: Root) -> Result<(), StoreError> {
    let identity = identity_side_key_bytes(chain, gvr);
    let schedule = schedule_side_key_bytes(chain)?;
    put_meta(
        engine,
        &[
            (KEY_CONFIG_DIGEST_V2, identity.as_slice()),
            (KEY_SCHEDULE_DIGEST, schedule.as_slice()),
        ],
    )
}

fn put_meta(engine: &Engine, writes: &[(&str, &[u8])]) -> Result<(), StoreError> {
    debug_assert!(
        writes.iter().all(|(key, _)| *key != KEY_CONFIG_DIGEST),
        "side-key writes must not replace meta.config_digest"
    );
    let mut batch = engine.batch();
    for (key, value) in writes {
        batch.put(TABLE_META, key.as_bytes(), value);
    }
    engine.commit(batch)
}

fn blob_schedule_entries(
    chain: &ChainConfig,
) -> Result<VariableList<BlobScheduleDigestEntry, U256>, StoreError> {
    let n = chain.blob_schedule.entries().len();
    if n > CONFIG_DIGEST_BLOB_SCHEDULE_MAX {
        return Err(StoreError::Config(format!(
            "BLOB_SCHEDULE has {n} entries; digest encoding capacity is \
             {CONFIG_DIGEST_BLOB_SCHEDULE_MAX} (refuse to truncate)"
        )));
    }
    let entries: Vec<BlobScheduleDigestEntry> = chain
        .blob_schedule
        .entries()
        .iter()
        .map(|e| BlobScheduleDigestEntry {
            epoch: e.epoch.as_u64(),
            max_blobs_per_block: e.max_blobs_per_block,
        })
        .collect();
    // Fail closed on encode capacity — no take(N) / unwrap_or_default (SEC-40a-1).
    VariableList::new(entries).map_err(|e| {
        StoreError::Config(format!(
            "BLOB_SCHEDULE does not fit digest encoding (capacity \
             {CONFIG_DIGEST_BLOB_SCHEDULE_MAX}): {e}"
        ))
    })
}

fn sha256_root(bytes: &[u8]) -> Root {
    let hash = Sha256::digest(bytes);
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&hash);
    Root::from_array(arr)
}

fn epoch_u64(e: Epoch) -> u64 {
    e.as_u64()
}

/// `meta.config_digest` written for a new store and recognised by the rollback binary.
///
/// Hoodi fixture, `Root::ZERO`, and the two `with_mainnet_scalars` constants.
/// Recognising that value is not a running-config input, and it is not permission
/// to open a populated store.
pub fn legacy_config_digest() -> Root {
    Root::from_array([
        0x0c, 0xe1, 0xca, 0xba, 0x76, 0xb2, 0xc6, 0xd0, 0xd5, 0x35, 0xc7, 0xce, 0xd6, 0xcc, 0xf6,
        0x97, 0x40, 0x4e, 0xb5, 0x36, 0xea, 0x2f, 0x0c, 0xb7, 0xde, 0xfa, 0x8d, 0x34, 0x35, 0x4b,
        0x77, 0x1e,
    ])
}

/// Canonical rows, block bodies, or snapshots — history a running config must not adopt.
///
/// Cold `blocks_{shard}` tables are checked before `snapshots`. A snapshot value
/// is a beacon state. Presence is [`ReadTxn::has_any`] (untyped header length;
/// that call does not `get_page` the table root). The snapshot table is not
/// opened once a block row already answers the question.
pub fn store_holds_chain_history(engine: &Engine) -> Result<bool, StoreError> {
    let names = engine.table_names()?;
    let rt = engine.read()?;
    if table_has_any_row(&rt, TABLE_CANONICAL)? || table_has_any_row(&rt, TABLE_BLOCKS_HOT)? {
        return Ok(true);
    }
    for name in &names {
        if parse_shard_table(name).is_some_and(|(class, _)| class == "blocks")
            && table_has_any_row(&rt, name)?
        {
            return Ok(true);
        }
    }
    table_has_any_row(&rt, TABLE_SNAPSHOTS)
}

/// Refuse a populated store that would otherwise open because `config_digest`
/// equals [`legacy_config_digest`].
///
/// An empty store returns `Ok`. Side-key comparison is not performed here.
/// An absent genesis validators root is not replaced with [`Root::ZERO`].
pub fn refuse_populated_legacy_open(engine: &Engine, gvr_present: bool) -> Result<(), StoreError> {
    if !store_holds_chain_history(engine)? {
        return Ok(());
    }
    if !gvr_present {
        return Err(StoreError::Config(
            "genesis_validators_root is required when the store holds chain history \
             (absent GVR is not Root::ZERO)"
                .into(),
        ));
    }
    Err(StoreError::Config(
        "populated store refused: config_digest equality with the legacy constant \
         is not an open until side keys are compared"
            .into(),
    ))
}

fn table_has_any_row(rt: &crate::engine::ReadTxn, table: &str) -> Result<bool, StoreError> {
    // Untyped header length. `range_max` copies the first value.
    rt.has_any(table)
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

    /// Two networks must not share a digest. At the parent this fails: the
    /// running-digest helpers ignore the supplied chain.
    #[test]
    fn two_networks_produce_different_digests() {
        let gvr = Root::from_array([0x11; 32]);
        let hoodi = ChainConfig::from_yaml_str(include_str!(
            "../../types/tests/fixtures/hoodi-config.yaml"
        ))
        .unwrap();
        let mainnet = ChainConfig::from_yaml_str(include_str!(
            "../../types/tests/fixtures/mainnet-config.yaml"
        ))
        .unwrap();
        assert_ne!(
            hoodi.genesis_fork_version, mainnet.genesis_fork_version,
            "fixture pair must actually differ"
        );
        let hoodi_id = compute_identity_digest(&hoodi, gvr);
        let mainnet_id = compute_identity_digest(&mainnet, gvr);
        assert_ne!(
            hoodi_id, mainnet_id,
            "identity digests must differ across networks"
        );
        let hoodi_sched = compute_schedule_digest(&hoodi).unwrap();
        let mainnet_sched = compute_schedule_digest(&mainnet).unwrap();
        assert_ne!(
            hoodi_sched, mainnet_sched,
            "schedule digests must differ across networks"
        );
    }

    /// Bumping [`DIGEST_VERSION`] changes the hashed bytes. The integer is the
    /// mechanism; a comment is not.
    #[test]
    fn digest_version_bump_changes_both_payloads() {
        let gvr = Root::from_array([0x11; 32]);
        let hoodi = ChainConfig::from_yaml_str(include_str!(
            "../../types/tests/fixtures/hoodi-config.yaml"
        ))
        .unwrap();
        assert_ne!(
            identity_digest_at(&hoodi, gvr, 1),
            identity_digest_at(&hoodi, gvr, 2),
            "identity payload must hash digest_version"
        );
        assert_ne!(
            schedule_digest_at(&hoodi, 1).unwrap(),
            schedule_digest_at(&hoodi, 2).unwrap(),
            "schedule payload must hash digest_version"
        );
        assert_eq!(
            compute_identity_digest(&hoodi, gvr),
            identity_digest_at(&hoodi, gvr, DIGEST_VERSION)
        );
        assert_eq!(
            compute_schedule_digest(&hoodi).unwrap(),
            schedule_digest_at(&hoodi, DIGEST_VERSION).unwrap()
        );
    }

    /// An operator label is not a network change.
    #[test]
    fn config_name_is_not_a_running_digest_input() {
        let gvr = Root::from_array([0x22; 32]);
        let hoodi = ChainConfig::from_yaml_str(include_str!(
            "../../types/tests/fixtures/hoodi-config.yaml"
        ))
        .unwrap();
        let mut renamed = hoodi.clone();
        renamed.config_name = "not-the-label".into();
        assert_eq!(
            compute_identity_digest(&hoodi, gvr),
            compute_identity_digest(&renamed, gvr)
        );
        assert_eq!(
            compute_schedule_digest(&hoodi).unwrap(),
            compute_schedule_digest(&renamed).unwrap()
        );
    }

    /// The rollback constant is the Hoodi fixture digested with `Root::ZERO`.
    /// Production does not re-read that fixture to obtain it.
    #[test]
    fn legacy_constant_is_the_hoodi_zero_gvr_digest() {
        let chain = ChainConfig::from_yaml_str(include_str!(
            "../../types/tests/fixtures/hoodi-config.yaml"
        ))
        .unwrap();
        let live =
            compute_config_digest(&ConfigDigestInput::with_mainnet_scalars(chain, Root::ZERO))
                .unwrap();
        assert_eq!(live, legacy_config_digest());
    }

    #[test]
    fn populated_store_is_refused_even_when_legacy_digest_matches() {
        let dir = tmp_dir("populated-refuse");
        let input = hoodi_input();
        let store = Store::open(&dir, open_opts(&input)).unwrap();
        let engine = store.engine();
        let rt = engine.read().unwrap();
        let mut batch = engine.batch();
        crate::canonical::put_canonical(
            &rt,
            &mut batch,
            cc_types::Slot::new(1),
            &Root::from_array([7; 32]),
        )
        .unwrap();
        engine.commit(batch).unwrap();
        drop(rt);
        assert!(store_holds_chain_history(engine).unwrap());
        let missing = refuse_populated_legacy_open(engine, false).unwrap_err();
        assert!(
            missing.to_string().contains("genesis_validators_root"),
            "{missing}"
        );
        let present = refuse_populated_legacy_open(engine, true).unwrap_err();
        assert!(
            present.to_string().contains("populated store refused"),
            "{present}"
        );
        drop(store);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn empty_store_is_not_a_populated_refusal() {
        let dir = tmp_dir("empty-allow");
        let store = Store::open(&dir, open_opts(&hoodi_input())).unwrap();
        assert!(!store_holds_chain_history(store.engine()).unwrap());
        refuse_populated_legacy_open(store.engine(), false).unwrap();
        drop(store);
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn assert_populated_refusal(engine: &Engine) {
        assert!(store_holds_chain_history(engine).unwrap());
        let missing = refuse_populated_legacy_open(engine, false).unwrap_err();
        assert!(
            missing.to_string().contains("genesis_validators_root"),
            "{missing}"
        );
        let present = refuse_populated_legacy_open(engine, true).unwrap_err();
        assert!(
            present.to_string().contains("populated store refused"),
            "{present}"
        );
    }

    /// A snapshot and no blocks is still chain history. The bytes here are
    /// tiny; they do not prove the snapshot page was skipped.
    #[test]
    fn snapshot_only_store_is_a_populated_refusal() {
        use crate::snapshots::put_snapshot;

        let dir = tmp_dir("snap-only");
        let store = Store::open(&dir, open_opts(&hoodi_input())).unwrap();
        let engine = store.engine();
        put_snapshot(engine, cc_types::Slot::new(32), b"snapshot-bytes", 4).unwrap();
        assert_populated_refusal(engine);
        drop(store);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A cold `blocks_{shard}` row, with no hot block and no snapshot, refuses.
    #[test]
    fn cold_block_shard_only_store_is_a_populated_refusal() {
        let dir = tmp_dir("cold-shard-only");
        let store = Store::open(&dir, open_opts(&hoodi_input())).unwrap();
        let engine = store.engine();
        let table = blocks_shard_table(3);
        let mut batch = engine.batch();
        batch.put(&table, &1u64.to_be_bytes(), b"cold-block");
        engine.commit(batch).unwrap();
        assert!(engine.table_names().unwrap().contains(&table));
        assert_populated_refusal(engine);
        drop(store);
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
