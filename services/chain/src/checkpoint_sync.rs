//! Checkpoint fetch, provider fallback, and verification (CC-19a / Architecture §8).
//!
//! **Demoted to fallback (CC-45b / S2-J-02).** In-process durable seed
//! (`seed_from_durable` on `bin/beacon-core`) is the primary load strategy;
//! this module runs on empty store or on 4-container `cc-chain`. CC-45c
//! enforces that the fallback is *unreachable* during the 20-restart proof
//! run.
//!
//! Fetch order is **block-first** (genesis → config/spec → finalized block →
//! state by `state_root`, with `finalized` alias fallback). Verification uses
//! three distinct named errors. Provider fallback records
//! `cc_chain_bootstrap_attempts_total{provider,result}`.
//!
//! # Trust residual (SEC-19a-1) — optional `checkpoint_root`
//!
//! [`CheckpointBootstrapConfig::expected_checkpoint_root`] is **optional**. When
//! set, assertion 1 requires `hash_tree_root(block.message) == checkpoint_root`.
//! When **unset**, assertion 1 is skipped and the resolved root is logged at
//! `warn` with the provider name: the first provider that returns a
//! self-consistent Fulu triple matching the local `network_config` becomes the
//! fork-choice root of trust. That is the standard checkpoint-sync /
//! weak-subjectivity tradeoff — **not** multi-provider quorum. Production and
//! mainnet soak should pin `checkpoint_root` from an out-of-band weak-
//! subjectivity source; treat `checkpoint_providers` as highly privileged
//! config (same class as network YAML).
//!
//! Anchor store construction (warm `canonical_root`, wall-clock `on_tick`) lives
//! in [`spawn_core_from_checkpoint`] (CC-19b) for the 4-container host.
//! [`verify_anchor`] is the Teku split: fetch, state-root check, and
//! expected-root match return a [`VerifiedAnchor`] and do not install a core.
//! `commit_anchor` and [`crate::spawn_core_from_seed`] are not called here.
//! Aggregate health / bind order remain in `main.rs`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use cc_fork_choice::{PeerDasAvailability, Store, get_forkchoice_store, on_tick};
use cc_state_transition::TransitionContext;
use cc_types::config::{BlobParameters, BlobSchedule, BlobScheduleError, ChainConfig};
use cc_types::containers::Checkpoint;
use cc_types::preset::Preset;
use cc_types::primitives::{Epoch, ForkVersion, Root, Slot, parse_hex_bytes};
use cc_types::{BeaconState, ForkName, SignedBeaconBlock};
use futures::StreamExt;
use serde::Deserialize;
use ssz::Encode;
use thiserror::Error;
use tree_hash::TreeHash;

use crate::import::ForkChoiceScalarsPayload;

use crate::core::{CoreConfig, CoreThread, spawn_core_thread_with_epoch};
use crate::epoch_context::EpochContextStore;
use crate::events::EventInput;
use crate::head::HeadSnapshotStore;
use crate::metrics::{BootstrapResult, ChainMetrics, HashPath};

/// Required `Eth-Consensus-Version` on SSZ checkpoint responses (Phase 1 is Fulu-only).
pub const REQUIRED_CONSENSUS_VERSION: &str = "fulu";

/// Connect timeout per provider request (Architecture §8.1).
pub const PROVIDER_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Total timeout covering a ~150–200 MB state body (Architecture §8.1).
pub const PROVIDER_TOTAL_TIMEOUT: Duration = Duration::from_secs(180);

/// Additional network retries after the first attempt (Architecture §8.1).
///
/// Default **2** → **3 total tries** per provider (initial + 2 retries) with
/// exponential backoff, then advance. Verification failures are never retried.
pub const NETWORK_RETRIES: u32 = 2;

/// Whole-triple (block + state + assertions) re-fetch cap on state/block race.
pub const TRIPLE_ATTEMPTS: u32 = 2;

/// Hard cap on finalized block SSZ body (aligned with `fetch-hoodi-fixtures.sh`).
pub const MAX_BLOCK_BYTES: usize = 8 * 1024 * 1024;

/// Hard cap on state SSZ body (aligned with `fetch-hoodi-fixtures.sh` 400 MiB).
pub const MAX_STATE_BYTES: usize = 400 * 1024 * 1024;

/// Hard cap on JSON endpoint bodies (genesis / config/spec).
pub const MAX_JSON_BYTES: usize = 2 * 1024 * 1024;

/// Default exponential backoff base for network retries.
const BACKOFF_BASE: Duration = Duration::from_millis(200);

// ── errors ──────────────────────────────────────────────────────────────────

/// Checkpoint bootstrap errors (CC-19a).
///
/// The three verification assertions are **distinct named variants** so a
/// failure names itself in the log (§8.3).
#[derive(Debug, Error)]
pub enum CheckpointError {
    /// `hash_tree_root(block.message)` ≠ operator-supplied checkpoint root.
    #[error("CheckpointRootMismatch: expected {expected}, actual {actual}")]
    CheckpointRootMismatch {
        /// Operator-supplied root.
        expected: Root,
        /// Computed block root.
        actual: Root,
    },
    /// `hash_tree_root(state)` ≠ `block.message.state_root`.
    #[error("StateRootMismatch: block.state_root={block_state_root}, state_root={state_root}")]
    StateRootMismatch {
        /// `block.message.state_root`.
        block_state_root: Root,
        /// `hash_tree_root(state)`.
        state_root: Root,
    },
    /// Slot mismatch and/or not epoch-aligned.
    #[error(
        "AnchorSlotNotEpochAligned: block_slot={block_slot}, state_slot={state_slot}, slots_per_epoch={slots_per_epoch}"
    )]
    AnchorSlotNotEpochAligned {
        /// Block message slot.
        block_slot: u64,
        /// State slot.
        state_slot: u64,
        /// Preset slots per epoch.
        slots_per_epoch: u64,
    },
    /// SSZ response carried a non-`fulu` consensus version.
    #[error("unsupported fork: Eth-Consensus-Version={version} (provider={provider})")]
    UnsupportedFork {
        /// Header value observed.
        version: String,
        /// Provider base URL.
        provider: String,
    },
    /// `/eth/v1/config/spec` disagreed with the compiled config on implemented keys.
    #[error("config mismatch: {detail}")]
    ConfigMismatch {
        /// Human-readable differing keys with both values.
        detail: String,
    },
    /// HTTP / transport failure against one provider.
    #[error("provider {provider}: {reason}")]
    Provider {
        /// Provider base URL.
        provider: String,
        /// Failure reason.
        reason: String,
    },
    /// SSZ or JSON decode failure.
    #[error("decode error: {0}")]
    Decode(String),
    /// Every configured provider failed.
    #[error("all checkpoint providers failed")]
    AllProvidersFailed,
    /// Store construction from a verified checkpoint failed.
    #[error("fork-choice store construction failed: {0}")]
    Store(String),
    /// Remote `BLOB_SCHEDULE` could not be built through the validating constructor.
    ///
    /// Available for callers of [`blob_schedule_from_spec`] that want a
    /// `CheckpointError`. Bootstrap **cross-check** folds parse/validation
    /// failures into [`CheckpointError::ConfigMismatch`] instead (value and
    /// shape disagreements share one abort path with named keys).
    #[error("BLOB_SCHEDULE from config/spec: {0}")]
    BlobSchedule(#[from] BlobScheduleFromSpecError),
}

impl CheckpointError {
    /// Whether this error is a verification assertion (must not be network-retried).
    pub fn is_verification_failure(&self) -> bool {
        matches!(
            self,
            Self::CheckpointRootMismatch { .. }
                | Self::StateRootMismatch { .. }
                | Self::AnchorSlotNotEpochAligned { .. }
                | Self::UnsupportedFork { .. }
                | Self::ConfigMismatch { .. }
        )
    }
}

// ── public types ────────────────────────────────────────────────────────────

/// Operator / process configuration for checkpoint bootstrap.
#[derive(Debug, Clone)]
pub struct CheckpointBootstrapConfig {
    /// Ordered provider base URLs (`chain.checkpoint_providers`).
    pub providers: Vec<String>,
    /// Optional operator-supplied finalized checkpoint root (hex `0x…`).
    pub expected_checkpoint_root: Option<Root>,
    /// Compiled chain config for `/eth/v1/config/spec` cross-check.
    pub chain_config: ChainConfig,
    /// Connect timeout (default 5 s).
    pub connect_timeout: Duration,
    /// Total request timeout including state body (default 180 s).
    pub total_timeout: Duration,
    /// Additional network retries after the first attempt (default 2 → 3 tries).
    pub network_retries: u32,
    /// Whole-triple re-fetch cap on state/block race (default 2).
    pub triple_attempts: u32,
}

impl CheckpointBootstrapConfig {
    /// Construct with §8.1 timeout defaults.
    pub fn new(providers: Vec<String>, chain_config: ChainConfig) -> Self {
        Self {
            providers,
            expected_checkpoint_root: None,
            chain_config,
            connect_timeout: PROVIDER_CONNECT_TIMEOUT,
            total_timeout: PROVIDER_TOTAL_TIMEOUT,
            network_retries: NETWORK_RETRIES,
            triple_attempts: TRIPLE_ATTEMPTS,
        }
    }
}

/// Genesis fields from `GET /eth/v1/beacon/genesis`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GenesisInfo {
    /// Network genesis time (unix seconds).
    pub genesis_time: u64,
    /// Genesis validators root — required for every signature domain.
    pub genesis_validators_root: Root,
}

/// Verified checkpoint material ready for store construction (CC-19b).
#[derive(Debug)]
pub struct FetchedCheckpoint<P: Preset> {
    /// Genesis fields from the provider (also present on `state`).
    pub genesis: GenesisInfo,
    /// Finalized signed block.
    pub signed_block: SignedBeaconBlock<P>,
    /// Post-state at `signed_block.message.state_root`.
    pub state: BeaconState<P>,
    /// `hash_tree_root(block.message)`.
    pub block_root: Root,
    /// Provider that served the successful triple.
    pub provider: String,
}

/// Which arm produced a [`VerifiedAnchor`]. Neither arm installs a core.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnchorKind {
    /// Checkpoint provider, after the state-root and expected-root checks.
    Checkpoint,
    /// Local genesis block and state. Parent stays the block's parent.
    Genesis,
}

/// Verified anchor material. Storage is not written and no core is spawned.
///
/// [`VerifiedAnchor::trusted`] is the payload the composer passes to
/// `commit_anchor`. This module does not call that op.
#[derive(Debug)]
pub struct VerifiedAnchor<P: Preset> {
    /// Which [`AnchorSource`] arm verified.
    pub kind: AnchorKind,
    /// `hash_tree_root(block.message)`.
    pub block_root: Root,
    /// Block parent as signed. Genesis keeps [`Root::ZERO`]; it is not
    /// rewritten to [`Self::block_root`].
    pub parent_root: Root,
    /// Anchor slot.
    pub slot: u64,
    /// `block.message.state_root` after the state-root check.
    pub state_root: Root,
    /// Genesis time and validators root used for signature domains.
    pub genesis: GenesisInfo,
    /// Provider base URL, or `"genesis"` for [`AnchorKind::Genesis`].
    pub provider: String,
    /// Finalized (or genesis) signed block.
    pub signed_block: SignedBeaconBlock<P>,
    /// Post-state at `signed_block.message.state_root`.
    pub state: BeaconState<P>,
    /// Opaque anchor for a later `commit_anchor`. Not submitted here.
    pub trusted: cc_seam::TrustedAnchor,
}

/// Where a trusted anchor comes from.
///
/// Both arms verify in this crate. Neither builds a fork-choice core.
/// `commit_anchor` stays the composer's one call, and
/// [`crate::spawn_core_from_seed`] stays the only core-install path.
pub enum AnchorSource<P: Preset> {
    /// Fetch from `config.providers` through `provider`, then verify.
    Checkpoint {
        /// Provider list, timeouts, and the optional expected root.
        config: CheckpointBootstrapConfig,
        /// HTTP client or an injected double. Not constructed here.
        provider: Arc<dyn CheckpointProvider>,
    },
    /// Already-decoded genesis block and state. No provider and no
    /// self-parent remap: a zero parent stays zero.
    ///
    /// The block and state are boxed so this variant does not inflate
    /// [`AnchorSource::Checkpoint`] (clippy::large_enum_variant).
    Genesis {
        /// Genesis post-state.
        state: Box<BeaconState<P>>,
        /// Genesis signed block (`parent_root` is typically [`Root::ZERO`]).
        signed_block: Box<SignedBeaconBlock<P>>,
        /// Local chain config. `seconds_per_slot` stamps anchor scalars.
        chain_config: ChainConfig,
        /// Optional operator block root. Same check as checkpoint sync.
        expected_block_root: Option<Root>,
    },
}

impl<P: Preset> std::fmt::Debug for AnchorSource<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Checkpoint { config, .. } => f
                .debug_struct("Checkpoint")
                .field("config", config)
                .finish_non_exhaustive(),
            Self::Genesis {
                signed_block,
                expected_block_root,
                ..
            } => f
                .debug_struct("Genesis")
                .field("slot", &signed_block.message.slot)
                .field("expected_block_root", expected_block_root)
                .finish_non_exhaustive(),
        }
    }
}

/// Beacon API reads checkpoint sync needs. Implementors are injected;
/// [`CheckpointClient`] is the HTTP one and [`InMemoryCheckpointProvider`]
/// is the in-process double (no `reqwest` in the caller).
#[async_trait]
pub trait CheckpointProvider: Send + Sync {
    /// `GET /eth/v1/beacon/genesis`.
    async fn fetch_genesis(&self, base: &str) -> Result<GenesisInfo, CheckpointError>;

    /// `GET /eth/v1/config/spec` as a flat string map.
    async fn fetch_spec(&self, base: &str) -> Result<BTreeMap<String, String>, CheckpointError>;

    /// `GET /eth/v2/beacon/blocks/finalized` as `(consensus version, SSZ)`.
    async fn fetch_finalized_block_ssz(
        &self,
        base: &str,
    ) -> Result<(String, Bytes), CheckpointError>;

    /// `GET /eth/v2/debug/beacon/states/{state_id}` as `(consensus version, SSZ)`.
    async fn fetch_state_ssz(
        &self,
        base: &str,
        state_id: &str,
    ) -> Result<(String, Bytes), CheckpointError>;
}

/// Preloaded checkpoint responses. No HTTP. Tests and the composer inject
/// this instead of a `wiremock` server on `cc-beacon-core`.
#[derive(Debug, Clone)]
pub struct InMemoryCheckpointProvider {
    inner: Arc<InMemoryCheckpoint>,
}

#[derive(Debug)]
struct InMemoryCheckpoint {
    genesis: GenesisInfo,
    spec: BTreeMap<String, String>,
    consensus_version: String,
    block_ssz: Bytes,
    state_ssz: Bytes,
}

impl InMemoryCheckpointProvider {
    /// Serve one genesis, one spec map, and one finalized block/state pair
    /// for every state id.
    pub fn new(
        genesis: GenesisInfo,
        spec: BTreeMap<String, String>,
        consensus_version: impl Into<String>,
        block_ssz: Bytes,
        state_ssz: Bytes,
    ) -> Self {
        Self {
            inner: Arc::new(InMemoryCheckpoint {
                genesis,
                spec,
                consensus_version: consensus_version.into(),
                block_ssz,
                state_ssz,
            }),
        }
    }
}

#[async_trait]
#[allow(clippy::unused_async)]
impl CheckpointProvider for InMemoryCheckpointProvider {
    async fn fetch_genesis(&self, _base: &str) -> Result<GenesisInfo, CheckpointError> {
        Ok(self.inner.genesis)
    }

    async fn fetch_spec(&self, _base: &str) -> Result<BTreeMap<String, String>, CheckpointError> {
        Ok(self.inner.spec.clone())
    }

    async fn fetch_finalized_block_ssz(
        &self,
        _base: &str,
    ) -> Result<(String, Bytes), CheckpointError> {
        Ok((
            self.inner.consensus_version.clone(),
            Bytes::clone(&self.inner.block_ssz),
        ))
    }

    async fn fetch_state_ssz(
        &self,
        _base: &str,
        _state_id: &str,
    ) -> Result<(String, Bytes), CheckpointError> {
        Ok((
            self.inner.consensus_version.clone(),
            Bytes::clone(&self.inner.state_ssz),
        ))
    }
}

// ── verification (pure) ─────────────────────────────────────────────────────

/// Three named verification assertions (§8.3).
///
/// 1. Optional operator checkpoint root vs `hash_tree_root(block.message)`.
/// 2. `hash_tree_root(state)` vs `block.message.state_root`.
/// 3. `state.slot == block.message.slot` and epoch-aligned.
///
/// Returns the resolved block root.
pub fn verify_checkpoint<P: Preset>(
    signed_block: &SignedBeaconBlock<P>,
    state: &BeaconState<P>,
    expected_checkpoint_root: Option<Root>,
) -> Result<Root, CheckpointError> {
    let block = &signed_block.message;
    let block_root = Root::from_hash256(TreeHash::tree_hash_root(block));

    if let Some(expected) = expected_checkpoint_root
        && block_root != expected
    {
        return Err(CheckpointError::CheckpointRootMismatch {
            expected,
            actual: block_root,
        });
    }

    let state_root = Root::from_hash256(TreeHash::tree_hash_root(state));
    if state_root != block.state_root {
        return Err(CheckpointError::StateRootMismatch {
            block_state_root: block.state_root,
            state_root,
        });
    }

    let block_slot = block.slot.as_u64();
    let state_slot = state.slot().as_u64();
    let slots_per_epoch = P::SLOTS_PER_EPOCH.max(1);
    let epoch_aligned = block_slot.is_multiple_of(slots_per_epoch);
    if block_slot != state_slot || !epoch_aligned {
        return Err(CheckpointError::AnchorSlotNotEpochAligned {
            block_slot,
            state_slot,
            slots_per_epoch,
        });
    }

    Ok(block_root)
}

// ── config/spec cross-check + BLOB_SCHEDULE source (CC-1G) ──────────────────

/// Keys we implement and compare against `/eth/v1/config/spec` (§8.3).
const CROSS_CHECK_FORK_VERSIONS: &[&str] = &[
    "GENESIS_FORK_VERSION",
    "ALTAIR_FORK_VERSION",
    "BELLATRIX_FORK_VERSION",
    "CAPELLA_FORK_VERSION",
    "DENEB_FORK_VERSION",
    "ELECTRA_FORK_VERSION",
    "FULU_FORK_VERSION",
];

/// Errors extracting `BLOB_SCHEDULE` from a `/eth/v1/config/spec` value.
///
/// Malformed JSON/shape fails here; non-monotonic / empty / duplicate schedules
/// fail via the shared [`BlobSchedule::try_from_entries`] constructor so both
/// the file source and the API source share one validation path (CC-1G).
#[derive(Debug, Error)]
pub enum BlobScheduleFromSpecError {
    /// Value was not a JSON array of `{EPOCH, MAX_BLOBS_PER_BLOCK}` entries.
    #[error("malformed: {0}")]
    Malformed(String),
    /// Schedule failed the shared validating constructor.
    #[error(transparent)]
    Validation(#[from] BlobScheduleError),
}

/// Build a validated [`BlobSchedule`] from a `/eth/v1/config/spec` `BLOB_SCHEDULE`
/// value (CC-1G remainder).
///
/// Beacon-API clients flatten the nested array to a JSON string (via
/// [`parse_spec_json`]); entries commonly carry stringified numbers
/// (`"EPOCH": "52480"`). Both string and numeric forms are accepted. After
/// edge normalisation the entries are passed through
/// [`BlobSchedule::try_from_entries`] — the same constructor the YAML file
/// source uses — so a malformed or non-monotonic schedule never becomes a
/// usable [`BlobSchedule`] (Architecture §2.3 type-level fail-at-construction).
/// Process bind ordering is owned by the service lifecycle (CC-19b).
pub fn blob_schedule_from_spec(value: &str) -> Result<BlobSchedule, BlobScheduleFromSpecError> {
    let entries = parse_blob_schedule_entries(value)?;
    BlobSchedule::try_from_entries(entries).map_err(BlobScheduleFromSpecError::from)
}

/// Extract `BLOB_SCHEDULE` from a full `/eth/v1/config/spec` string map.
pub fn blob_schedule_from_spec_map(
    remote: &BTreeMap<String, String>,
) -> Result<BlobSchedule, BlobScheduleFromSpecError> {
    let value = remote
        .get("BLOB_SCHEDULE")
        .ok_or_else(|| BlobScheduleFromSpecError::Malformed("BLOB_SCHEDULE key missing".into()))?;
    blob_schedule_from_spec(value)
}

/// Cross-check remote spec against the compiled [`ChainConfig`].
///
/// Compares `FULU_FORK_EPOCH`, every `*_FORK_VERSION`, `SECONDS_PER_SLOT`, and
/// `BLOB_SCHEDULE`. Unknown remote keys are logged at `debug` and ignored.
///
/// `BLOB_SCHEDULE` is compared after building both sides through the shared
/// validating constructor (local already holds a [`BlobSchedule`]; remote is
/// parsed via [`blob_schedule_from_spec`]).
pub fn cross_check_spec(
    remote: &BTreeMap<String, String>,
    local: &ChainConfig,
) -> Result<(), CheckpointError> {
    let mut differing: Vec<(String, String, String)> = Vec::new();

    let local_versions: [(&str, ForkVersion); 7] = [
        ("GENESIS_FORK_VERSION", local.genesis_fork_version),
        ("ALTAIR_FORK_VERSION", local.altair_fork_version),
        ("BELLATRIX_FORK_VERSION", local.bellatrix_fork_version),
        ("CAPELLA_FORK_VERSION", local.capella_fork_version),
        ("DENEB_FORK_VERSION", local.deneb_fork_version),
        ("ELECTRA_FORK_VERSION", local.electra_fork_version),
        ("FULU_FORK_VERSION", local.fulu_fork_version),
    ];
    for (key, local_v) in local_versions {
        let expected = format!("{local_v}");
        match remote.get(key) {
            Some(remote_v) => {
                if !fork_version_eq(remote_v, &expected) {
                    differing.push((key.to_owned(), expected, remote_v.clone()));
                }
            }
            None => differing.push((key.to_owned(), expected, "<missing>".into())),
        }
    }

    let fulu_epoch = local.fulu_fork_epoch.as_u64().to_string();
    match remote.get("FULU_FORK_EPOCH") {
        Some(remote_v) if remote_v.trim() == fulu_epoch => {}
        Some(remote_v) => differing.push(("FULU_FORK_EPOCH".into(), fulu_epoch, remote_v.clone())),
        None => differing.push(("FULU_FORK_EPOCH".into(), fulu_epoch, "<missing>".into())),
    }

    let secs = local.seconds_per_slot.to_string();
    match remote.get("SECONDS_PER_SLOT") {
        Some(remote_v) if remote_v.trim() == secs => {}
        Some(remote_v) => {
            differing.push(("SECONDS_PER_SLOT".into(), secs, remote_v.clone()));
        }
        None => differing.push(("SECONDS_PER_SLOT".into(), secs, "<missing>".into())),
    }

    let local_blob = format_blob_schedule(local.blob_schedule.entries());
    match remote.get("BLOB_SCHEDULE") {
        Some(remote_v) => match blob_schedule_from_spec(remote_v) {
            Ok(remote_sched) => {
                if remote_sched.entries() != local.blob_schedule.entries() {
                    differing.push((
                        "BLOB_SCHEDULE".into(),
                        local_blob,
                        format_blob_schedule(remote_sched.entries()),
                    ));
                }
            }
            Err(e) => differing.push((
                "BLOB_SCHEDULE".into(),
                local_blob,
                format!("<unparseable: {e}>"),
            )),
        },
        None => differing.push(("BLOB_SCHEDULE".into(), local_blob, "<missing>".into())),
    }

    // Unknown keys: debug log, ignore.
    for key in remote.keys() {
        if is_implemented_spec_key(key) {
            continue;
        }
        tracing::debug!(key = %key, "ignoring unknown config/spec key");
    }

    if differing.is_empty() {
        return Ok(());
    }

    let detail = differing
        .iter()
        .map(|(k, exp, got)| format!("{k}: local={exp} remote={got}"))
        .collect::<Vec<_>>()
        .join("; ");
    Err(CheckpointError::ConfigMismatch { detail })
}

fn is_implemented_spec_key(key: &str) -> bool {
    CROSS_CHECK_FORK_VERSIONS.contains(&key)
        || key == "FULU_FORK_EPOCH"
        || key == "SECONDS_PER_SLOT"
        || key == "BLOB_SCHEDULE"
}

fn fork_version_eq(remote: &str, local_display: &str) -> bool {
    let r = remote.trim().to_ascii_lowercase();
    let l = local_display.trim().to_ascii_lowercase();
    r == l || r.trim_start_matches("0x") == l.trim_start_matches("0x")
}

fn format_blob_schedule(entries: &[BlobParameters]) -> String {
    let parts: Vec<String> = entries
        .iter()
        .map(|e| {
            format!(
                "{{EPOCH:{},MAX_BLOBS_PER_BLOCK:{}}}",
                e.epoch.as_u64(),
                e.max_blobs_per_block
            )
        })
        .collect();
    format!("[{}]", parts.join(","))
}

/// Normalise a beacon-API `BLOB_SCHEDULE` string into entry structs.
///
/// Accepts JSON arrays whose fields are numbers or decimal strings, with either
/// SCREAMING_SNAKE or lowercase keys — the shapes public providers emit after
/// `Value::Array` → string flattening in [`parse_spec_json`].
fn parse_blob_schedule_entries(s: &str) -> Result<Vec<BlobParameters>, BlobScheduleFromSpecError> {
    let value: serde_json::Value = serde_json::from_str(s.trim())
        .map_err(|e| BlobScheduleFromSpecError::Malformed(format!("json: {e}")))?;
    let arr = value.as_array().ok_or_else(|| {
        BlobScheduleFromSpecError::Malformed("expected JSON array of schedule entries".into())
    })?;
    let mut entries = Vec::with_capacity(arr.len());
    for (i, item) in arr.iter().enumerate() {
        let obj = item.as_object().ok_or_else(|| {
            BlobScheduleFromSpecError::Malformed(format!("entry {i} is not an object"))
        })?;
        let epoch = field_u64(obj, &["EPOCH", "epoch"], i, "EPOCH")?;
        let max_blobs = field_u64(
            obj,
            &["MAX_BLOBS_PER_BLOCK", "max_blobs_per_block"],
            i,
            "MAX_BLOBS_PER_BLOCK",
        )?;
        entries.push(BlobParameters {
            epoch: Epoch::new(epoch),
            max_blobs_per_block: max_blobs,
        });
    }
    Ok(entries)
}

fn field_u64(
    obj: &serde_json::Map<String, serde_json::Value>,
    keys: &[&str],
    index: usize,
    label: &str,
) -> Result<u64, BlobScheduleFromSpecError> {
    let raw = keys.iter().find_map(|k| obj.get(*k)).ok_or_else(|| {
        BlobScheduleFromSpecError::Malformed(format!("entry {index} missing {label}"))
    })?;
    match raw {
        serde_json::Value::Number(n) => n.as_u64().ok_or_else(|| {
            BlobScheduleFromSpecError::Malformed(format!("entry {index} {label} not a u64: {n}"))
        }),
        serde_json::Value::String(s) => s.trim().parse::<u64>().map_err(|e| {
            BlobScheduleFromSpecError::Malformed(format!("entry {index} {label} parse {s:?}: {e}"))
        }),
        other => Err(BlobScheduleFromSpecError::Malformed(format!(
            "entry {index} {label} unexpected type: {other}"
        ))),
    }
}

// ── HTTP client ─────────────────────────────────────────────────────────────

/// Thin beacon-API client used by checkpoint bootstrap.
#[derive(Debug, Clone)]
pub struct CheckpointClient {
    http: reqwest::Client,
}

impl CheckpointClient {
    /// Build a client with the configured timeouts.
    ///
    /// Redirects that leave HTTPS (or leave loopback HTTP) are refused so a
    /// compromised provider cannot pivot bootstrap GETs into cleartext or
    /// arbitrary internal hosts (SEC-19a-3).
    pub fn new(
        connect_timeout: Duration,
        total_timeout: Duration,
    ) -> Result<Self, CheckpointError> {
        let http = reqwest::Client::builder()
            .connect_timeout(connect_timeout)
            .timeout(total_timeout)
            .user_agent("cc-chain/checkpoint-sync (CC-19a)")
            .redirect(reqwest::redirect::Policy::custom(|attempt| {
                if provider_scheme_allowed(attempt.url()) {
                    attempt.follow()
                } else {
                    let target = attempt.url().to_string();
                    attempt.error(format!(
                        "refusing redirect to disallowed scheme/host {target}"
                    ))
                }
            }))
            .build()
            .map_err(|e| CheckpointError::Decode(format!("http client build: {e}")))?;
        Ok(Self { http })
    }

    /// `GET /eth/v1/beacon/genesis`.
    pub async fn fetch_genesis(&self, base: &str) -> Result<GenesisInfo, CheckpointError> {
        let url = join_url(base, "/eth/v1/beacon/genesis");
        let text = self.get_json_text(base, &url).await?;
        parse_genesis_json(&text).map_err(|e| CheckpointError::Provider {
            provider: base.to_owned(),
            reason: format!("genesis parse: {e}"),
        })
    }

    /// `GET /eth/v1/config/spec` → flat string map.
    pub async fn fetch_spec(
        &self,
        base: &str,
    ) -> Result<BTreeMap<String, String>, CheckpointError> {
        let url = join_url(base, "/eth/v1/config/spec");
        let text = self.get_json_text(base, &url).await?;
        parse_spec_json(&text).map_err(|e| CheckpointError::Provider {
            provider: base.to_owned(),
            reason: format!("spec parse: {e}"),
        })
    }

    /// `GET /eth/v2/beacon/blocks/finalized` as SSZ.
    pub async fn fetch_finalized_block_ssz(
        &self,
        base: &str,
    ) -> Result<(String, Bytes), CheckpointError> {
        let url = join_url(base, "/eth/v2/beacon/blocks/finalized");
        self.get_ssz(base, &url, MAX_BLOCK_BYTES).await
    }

    /// `GET /eth/v2/debug/beacon/states/{id}` as SSZ.
    pub async fn fetch_state_ssz(
        &self,
        base: &str,
        state_id: &str,
    ) -> Result<(String, Bytes), CheckpointError> {
        let url = join_url(base, &format!("/eth/v2/debug/beacon/states/{state_id}"));
        self.get_ssz(base, &url, MAX_STATE_BYTES).await
    }

    async fn get_json_text(&self, provider: &str, url: &str) -> Result<String, CheckpointError> {
        let resp = self
            .http
            .get(url)
            .header(reqwest::header::ACCEPT, "application/json")
            .send()
            .await
            .map_err(|e| CheckpointError::Provider {
                provider: provider.to_owned(),
                reason: format!("GET {url}: {e}"),
            })?;
        let status = resp.status();
        if !status.is_success() {
            // Drain nothing; surface status without reading a huge error body.
            return Err(CheckpointError::Provider {
                provider: provider.to_owned(),
                reason: format!("GET {url}: HTTP {status}"),
            });
        }
        let bytes = read_body_capped(resp, MAX_JSON_BYTES, provider, url).await?;
        String::from_utf8(bytes.to_vec()).map_err(|e| CheckpointError::Provider {
            provider: provider.to_owned(),
            reason: format!("GET {url} utf8: {e}"),
        })
    }

    async fn get_ssz(
        &self,
        provider: &str,
        url: &str,
        max_bytes: usize,
    ) -> Result<(String, Bytes), CheckpointError> {
        let resp = self
            .http
            .get(url)
            .header(reqwest::header::ACCEPT, "application/octet-stream")
            .send()
            .await
            .map_err(|e| CheckpointError::Provider {
                provider: provider.to_owned(),
                reason: format!("GET {url}: {e}"),
            })?;
        let status = resp.status();
        let version = resp
            .headers()
            .get("eth-consensus-version")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_owned();
        if status.as_u16() == 404 {
            return Err(CheckpointError::Provider {
                provider: provider.to_owned(),
                reason: format!("GET {url}: HTTP 404"),
            });
        }
        if !status.is_success() {
            return Err(CheckpointError::Provider {
                provider: provider.to_owned(),
                reason: format!("GET {url}: HTTP {status}"),
            });
        }
        let bytes = read_body_capped(resp, max_bytes, provider, url).await?;
        Ok((version, bytes))
    }
}

#[async_trait]
impl CheckpointProvider for CheckpointClient {
    async fn fetch_genesis(&self, base: &str) -> Result<GenesisInfo, CheckpointError> {
        CheckpointClient::fetch_genesis(self, base).await
    }

    async fn fetch_spec(&self, base: &str) -> Result<BTreeMap<String, String>, CheckpointError> {
        CheckpointClient::fetch_spec(self, base).await
    }

    async fn fetch_finalized_block_ssz(
        &self,
        base: &str,
    ) -> Result<(String, Bytes), CheckpointError> {
        CheckpointClient::fetch_finalized_block_ssz(self, base).await
    }

    async fn fetch_state_ssz(
        &self,
        base: &str,
        state_id: &str,
    ) -> Result<(String, Bytes), CheckpointError> {
        CheckpointClient::fetch_state_ssz(self, base, state_id).await
    }
}

/// Read a response body with a hard byte ceiling (SEC-19a-2).
///
/// Rejects oversized `Content-Length` before streaming; aborts mid-stream if the
/// cumulative size exceeds `max_bytes` (fail closed — no multi-GB buffer).
async fn read_body_capped(
    resp: reqwest::Response,
    max_bytes: usize,
    provider: &str,
    url: &str,
) -> Result<Bytes, CheckpointError> {
    if let Some(cl) = resp.content_length()
        && cl as usize > max_bytes
    {
        return Err(CheckpointError::Provider {
            provider: provider.to_owned(),
            reason: format!("GET {url}: Content-Length {cl} exceeds max {max_bytes} bytes"),
        });
    }

    let mut out = Vec::new();
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| CheckpointError::Provider {
            provider: provider.to_owned(),
            reason: format!("GET {url} body: {e}"),
        })?;
        let next = out.len().saturating_add(chunk.len());
        if next > max_bytes {
            return Err(CheckpointError::Provider {
                provider: provider.to_owned(),
                reason: format!(
                    "GET {url}: body exceeds max {max_bytes} bytes (got at least {next})"
                ),
            });
        }
        out.extend_from_slice(&chunk);
    }
    Ok(Bytes::from(out))
}

fn join_url(base: &str, path: &str) -> String {
    let base = base.trim_end_matches('/');
    if path.starts_with('/') {
        format!("{base}{path}")
    } else {
        format!("{base}/{path}")
    }
}

/// Production providers must be HTTPS. Loopback HTTP is allowed for offline tests.
pub fn validate_provider_base(base: &str) -> Result<(), CheckpointError> {
    let parsed = reqwest::Url::parse(base).map_err(|e| CheckpointError::Provider {
        provider: base.to_owned(),
        reason: format!("invalid provider URL: {e}"),
    })?;
    if provider_scheme_allowed(&parsed) {
        Ok(())
    } else {
        Err(CheckpointError::Provider {
            provider: base.to_owned(),
            reason: format!(
                "provider URL must be https:// (or http:// to loopback for tests); got {}",
                parsed.scheme()
            ),
        })
    }
}

fn provider_scheme_allowed(url: &reqwest::Url) -> bool {
    match url.scheme() {
        "https" => true,
        "http" => is_loopback_host(url.host_str()),
        _ => false,
    }
}

fn is_loopback_host(host: Option<&str>) -> bool {
    match host {
        Some("localhost") | Some("127.0.0.1") | Some("::1") => true,
        Some(h) => h.starts_with("127."),
        None => false,
    }
}

/// Whether a by-root state failure should try the `finalized` alias (cheap status cases).
fn by_root_should_fallback(err: &CheckpointError) -> bool {
    match err {
        CheckpointError::Provider { reason, .. } => {
            // Purpose-built checkpoint servers often 404 / 400 / 405 / 501 on
            // arbitrary roots; finalized alias still works. Do not fallback on
            // body-size violations (same cap would hit finalized).
            if reason.contains("exceeds max") || reason.contains("body exceeds max") {
                return false;
            }
            reason.contains("HTTP 404")
                || reason.contains("HTTP 400")
                || reason.contains("HTTP 405")
                || reason.contains("HTTP 501")
                || reason.contains("HTTP 500")
                || reason.contains("HTTP 502")
                || reason.contains("HTTP 503")
        }
        _ => false,
    }
}

fn require_fulu(version: &str, provider: &str) -> Result<(), CheckpointError> {
    let v = version.trim().to_ascii_lowercase();
    if v.is_empty() {
        // Several purpose-built checkpoint servers omit the header on block
        // responses while still serving Fulu SSZ. Missing ≠ wrong fork: warn and
        // continue; an explicit non-fulu value still aborts (CC-19/2).
        tracing::warn!(
            provider,
            "Eth-Consensus-Version header missing; assuming fulu (Phase 1)"
        );
        return Ok(());
    }
    if v == REQUIRED_CONSENSUS_VERSION {
        Ok(())
    } else {
        Err(CheckpointError::UnsupportedFork {
            version: version.to_owned(),
            provider: provider.to_owned(),
        })
    }
}

// ── JSON parsers ────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct GenesisEnvelope {
    data: GenesisData,
}

#[derive(Debug, Deserialize)]
struct GenesisData {
    genesis_time: serde_json::Value,
    genesis_validators_root: String,
}

fn parse_genesis_json(text: &str) -> Result<GenesisInfo, String> {
    let env: GenesisEnvelope =
        serde_json::from_str(text).map_err(|e| format!("genesis json: {e}"))?;
    let genesis_time = json_u64(&env.data.genesis_time)?;
    let genesis_validators_root = parse_root_hex(&env.data.genesis_validators_root)?;
    Ok(GenesisInfo {
        genesis_time,
        genesis_validators_root,
    })
}

fn parse_spec_json(text: &str) -> Result<BTreeMap<String, String>, String> {
    let v: serde_json::Value = serde_json::from_str(text).map_err(|e| format!("spec json: {e}"))?;
    let data = v
        .get("data")
        .ok_or_else(|| "spec missing data".to_owned())?;
    let obj = data
        .as_object()
        .ok_or_else(|| "spec data not object".to_owned())?;
    let mut out = BTreeMap::new();
    for (k, val) in obj {
        let s = match val {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::Bool(b) => b.to_string(),
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => val.to_string(),
            serde_json::Value::Null => continue,
        };
        out.insert(k.clone(), s);
    }
    Ok(out)
}

fn json_u64(v: &serde_json::Value) -> Result<u64, String> {
    match v {
        serde_json::Value::Number(n) => n.as_u64().ok_or_else(|| format!("not u64: {n}")),
        serde_json::Value::String(s) => s
            .trim()
            .parse::<u64>()
            .map_err(|e| format!("parse u64 {s}: {e}")),
        other => Err(format!("expected number/string, got {other}")),
    }
}

fn parse_root_hex(s: &str) -> Result<Root, String> {
    let bytes = parse_hex_bytes::<32>(s).map_err(|e| format!("root hex: {e}"))?;
    Ok(Root::from_array(bytes))
}

/// Parse an optional operator-supplied checkpoint root from config.
pub fn parse_optional_root(s: Option<&str>) -> Result<Option<Root>, CheckpointError> {
    match s {
        None => Ok(None),
        Some(raw) => {
            let t = raw.trim();
            if t.is_empty() {
                Ok(None)
            } else {
                parse_root_hex(t).map(Some).map_err(CheckpointError::Decode)
            }
        }
    }
}

// ── fetch orchestration ─────────────────────────────────────────────────────

/// Fetch and verify a checkpoint from the configured provider list.
///
/// Per provider: connect/total timeouts, [`NETWORK_RETRIES`] **additional**
/// network retries after the first attempt (default 2 → 3 total tries) with
/// exponential backoff, then advance. Verification failures are **not**
/// network-retried. Records `cc_chain_bootstrap_attempts_total{provider,result}`
/// once per provider outcome (success or terminal failure).
pub async fn fetch_checkpoint<P: Preset>(
    cfg: &CheckpointBootstrapConfig,
    metrics: &ChainMetrics,
) -> Result<FetchedCheckpoint<P>, CheckpointError> {
    if cfg.providers.is_empty() {
        return Err(CheckpointError::AllProvidersFailed);
    }
    let client = CheckpointClient::new(cfg.connect_timeout, cfg.total_timeout)?;
    fetch_checkpoint_with_provider(&client, cfg, metrics).await
}

/// [`fetch_checkpoint`] against an injected [`CheckpointProvider`].
///
/// The HTTP client is one implementor. A double serves the same fetch,
/// state-root check, and expected-root match without binding a socket.
pub async fn fetch_checkpoint_with_provider<P: Preset>(
    checkpoint_provider: &dyn CheckpointProvider,
    cfg: &CheckpointBootstrapConfig,
    metrics: &ChainMetrics,
) -> Result<FetchedCheckpoint<P>, CheckpointError> {
    if cfg.providers.is_empty() {
        return Err(CheckpointError::AllProvidersFailed);
    }

    for base in &cfg.providers {
        let base = base.trim_end_matches('/').to_owned();
        if let Err(e) = validate_provider_base(&base) {
            metrics.inc_bootstrap_attempt(&base, BootstrapResult::Failure);
            tracing::warn!(
                provider = %base,
                error = %e,
                "checkpoint provider URL rejected; advancing"
            );
            continue;
        }
        match fetch_from_provider::<P>(checkpoint_provider, &base, cfg).await {
            Ok(fetched) => {
                metrics.inc_bootstrap_attempt(&base, BootstrapResult::Success);
                tracing::info!(
                    provider = %base,
                    block_root = %fetched.block_root,
                    slot = fetched.signed_block.message.slot.as_u64(),
                    "checkpoint bootstrap succeeded"
                );
                return Ok(fetched);
            }
            Err(e) => {
                metrics.inc_bootstrap_attempt(&base, BootstrapResult::Failure);
                tracing::warn!(
                    provider = %base,
                    error = %e,
                    verification = e.is_verification_failure(),
                    "checkpoint provider failed; advancing"
                );
            }
        }
    }
    Err(CheckpointError::AllProvidersFailed)
}

async fn fetch_from_provider<P: Preset>(
    client: &dyn CheckpointProvider,
    provider: &str,
    cfg: &CheckpointBootstrapConfig,
) -> Result<FetchedCheckpoint<P>, CheckpointError> {
    // network_retries = additional retries after the first attempt.
    // e.g. NETWORK_RETRIES=2 → max_attempts=3 (initial + 2 retries).
    let max_attempts = cfg.network_retries.saturating_add(1).max(1);
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        match fetch_from_provider_once::<P>(client, provider, cfg).await {
            Ok(v) => return Ok(v),
            Err(e) if e.is_verification_failure() => {
                // Verification / unsupported fork / config mismatch: never retry.
                return Err(e);
            }
            Err(e) if attempt >= max_attempts => return Err(e),
            Err(e) => {
                let backoff = BACKOFF_BASE.saturating_mul(1u32 << (attempt.saturating_sub(1)));
                tracing::debug!(
                    provider,
                    attempt,
                    max_attempts,
                    backoff_ms = backoff.as_millis() as u64,
                    error = %e,
                    "network error; retrying provider"
                );
                tokio::time::sleep(backoff).await;
            }
        }
    }
}

async fn fetch_from_provider_once<P: Preset>(
    client: &dyn CheckpointProvider,
    provider: &str,
    cfg: &CheckpointBootstrapConfig,
) -> Result<FetchedCheckpoint<P>, CheckpointError> {
    // 1. Genesis
    let genesis = client.fetch_genesis(provider).await?;

    // 2. Config/spec cross-check
    let remote_spec = client.fetch_spec(provider).await?;
    cross_check_spec(&remote_spec, &cfg.chain_config)?;

    // 3–5. Block-first triple with by-root fallback and race re-fetch.
    let triple_cap = cfg.triple_attempts.max(1);
    let mut last_err: Option<CheckpointError> = None;
    for triple in 1..=triple_cap {
        match fetch_and_verify_triple::<P>(client, provider, cfg, &genesis).await {
            Ok(fetched) => return Ok(fetched),
            Err(e @ CheckpointError::StateRootMismatch { .. }) if triple < triple_cap => {
                // Finalization can advance between block and state; re-fetch whole triple.
                tracing::warn!(
                    provider,
                    triple,
                    error = %e,
                    "state/block root race; re-fetching whole triple"
                );
                last_err = Some(e);
            }
            Err(e) if e.is_verification_failure() => {
                // Named verification / unsupported fork / config: terminal for this provider.
                return Err(e);
            }
            Err(e) => {
                // Transport / decode on the triple path: surface immediately
                // (outer network-retry loop may re-enter the whole provider once).
                return Err(e);
            }
        }
    }
    Err(last_err.unwrap_or_else(|| CheckpointError::Provider {
        provider: provider.to_owned(),
        reason: "triple fetch exhausted".into(),
    }))
}

async fn fetch_and_verify_triple<P: Preset>(
    client: &dyn CheckpointProvider,
    provider: &str,
    cfg: &CheckpointBootstrapConfig,
    genesis: &GenesisInfo,
) -> Result<FetchedCheckpoint<P>, CheckpointError> {
    // 3. Finalized block (SSZ)
    let (block_version, block_bytes) = client.fetch_finalized_block_ssz(provider).await?;
    require_fulu(&block_version, provider)?;
    let signed_block = SignedBeaconBlock::<P>::from_ssz_bytes_with(ForkName::Fulu, &block_bytes)
        .map_err(|e| CheckpointError::Decode(format!("SignedBeaconBlock SSZ: {e:?}")))?;

    let state_root = signed_block.message.state_root;
    let state_id = format!("{state_root}");

    // 4. State by-root, fallback to finalized alias on cheap "path unsupported"
    // statuses (404/400/405/501/5xx) — not on body-size failures.
    let (state_version, state_bytes) = match client.fetch_state_ssz(provider, &state_id).await {
        Ok(pair) => pair,
        Err(e) if by_root_should_fallback(&e) => {
            tracing::info!(
                provider,
                state_root = %state_root,
                error = %e,
                "by-root state failed; falling back to finalized alias"
            );
            client.fetch_state_ssz(provider, "finalized").await?
        }
        Err(e) => return Err(e),
    };
    require_fulu(&state_version, provider)?;
    let state = BeaconState::<P>::from_ssz_bytes_hydrated(ForkName::Fulu, &state_bytes)
        .map_err(|e| CheckpointError::Decode(format!("BeaconState SSZ: {e:?}")))?;

    // Genesis endpoint is the bootstrap source of record for signature domains.
    // State-resident fields should match; log if they diverge (do not mutate
    // the SSZ-decoded state — that would invalidate state_root).
    if state.genesis_time() != genesis.genesis_time
        || state.genesis_validators_root() != genesis.genesis_validators_root
    {
        tracing::warn!(
            provider,
            state_genesis_time = state.genesis_time(),
            endpoint_genesis_time = genesis.genesis_time,
            state_gvr = %state.genesis_validators_root(),
            endpoint_gvr = %genesis.genesis_validators_root,
            "genesis endpoint differs from state-resident fields; using endpoint values for domains"
        );
    }

    // Pre-verify: race detection uses raw state root vs block.state_root
    // (before full verify so we can re-fetch the triple).
    let computed_state_root = Root::from_hash256(TreeHash::tree_hash_root(&state));
    if computed_state_root != signed_block.message.state_root {
        return Err(CheckpointError::StateRootMismatch {
            block_state_root: signed_block.message.state_root,
            state_root: computed_state_root,
        });
    }

    let block_root = verify_checkpoint(&signed_block, &state, cfg.expected_checkpoint_root)?;

    if cfg.expected_checkpoint_root.is_none() {
        tracing::warn!(
            provider,
            block_root = %block_root,
            "checkpoint root not operator-supplied; trusting provider"
        );
    }

    Ok(FetchedCheckpoint {
        genesis: *genesis,
        signed_block,
        state,
        block_root,
        provider: provider.to_owned(),
    })
}

/// Verify [`AnchorSource`] into a [`VerifiedAnchor`].
///
/// Runs the fetch (checkpoint only), the state-root check, and the
/// expected-root match. Does not build a fork-choice store, does not spawn
/// a core, and does not call `commit_anchor`. A genesis block whose parent
/// is [`Root::ZERO`] stays that parent — this path does not depend on the
/// storage self-parent bypass.
pub async fn verify_anchor<P: Preset>(
    source: AnchorSource<P>,
    metrics: &ChainMetrics,
) -> Result<VerifiedAnchor<P>, CheckpointError> {
    match source {
        AnchorSource::Checkpoint { config, provider } => {
            let fetched =
                fetch_checkpoint_with_provider::<P>(provider.as_ref(), &config, metrics).await?;
            Ok(verified_from_parts(
                AnchorKind::Checkpoint,
                fetched.provider,
                fetched.genesis,
                fetched.signed_block,
                fetched.state,
                fetched.block_root,
                &config.chain_config,
            ))
        }
        AnchorSource::Genesis {
            state,
            signed_block,
            chain_config,
            expected_block_root,
        } => {
            let block_root = verify_checkpoint(&signed_block, &state, expected_block_root)?;
            let genesis = GenesisInfo {
                genesis_time: state.genesis_time(),
                genesis_validators_root: state.genesis_validators_root(),
            };
            Ok(verified_from_parts(
                AnchorKind::Genesis,
                "genesis".to_owned(),
                genesis,
                *signed_block,
                *state,
                block_root,
                &chain_config,
            ))
        }
    }
}

fn verified_from_parts<P: Preset>(
    kind: AnchorKind,
    provider: String,
    genesis: GenesisInfo,
    signed_block: SignedBeaconBlock<P>,
    state: BeaconState<P>,
    block_root: Root,
    chain_config: &ChainConfig,
) -> VerifiedAnchor<P> {
    let parent_root = signed_block.message.parent_root;
    let slot = signed_block.message.slot.as_u64();
    let state_root = signed_block.message.state_root;
    let trusted = cc_seam::TrustedAnchor {
        block_root: seam_root(block_root),
        parent_root: seam_root(parent_root),
        slot,
        state_root: seam_root(state_root),
        block_ssz: Bytes::from(signed_block.as_ssz_bytes()),
        state_ssz: Bytes::from(state.as_ssz_bytes()),
        scalars: anchor_scalars::<P>(
            block_root,
            signed_block.message.slot,
            state.genesis_time(),
            chain_config.seconds_per_slot,
        ),
        da: cc_seam::DaVerdict::Available,
    };
    VerifiedAnchor {
        kind,
        block_root,
        parent_root,
        slot,
        state_root,
        genesis,
        provider,
        signed_block,
        state,
        trusted,
    }
}

/// Anchor scalars a later `commit_anchor` persists. Mirrors the anchor
/// `get_forkchoice_store` seeds (justified = finalized = the anchor
/// checkpoint, store time = genesis + slot × seconds) without building a store.
fn anchor_scalars<P: Preset>(
    block_root: Root,
    slot: Slot,
    genesis_time: u64,
    seconds_per_slot: u64,
) -> Bytes {
    let epoch = slot.epoch(P::SLOTS_PER_EPOCH.max(1));
    let checkpoint = Checkpoint {
        epoch,
        root: block_root,
    };
    let time = genesis_time.saturating_add(slot.as_u64().saturating_mul(seconds_per_slot.max(1)));
    let scalars = ForkChoiceScalarsPayload {
        time,
        proposer_boost_root: Root::ZERO,
        justified: checkpoint,
        finalized: checkpoint,
        unrealized_justified: checkpoint,
        unrealized_finalized: checkpoint,
        head_root: block_root,
        head_slot: slot,
    };
    Bytes::from(scalars.as_ssz_bytes())
}

fn seam_root(root: Root) -> cc_seam::Root {
    *root.as_array()
}

// ── core spawn helper (CC-19b anchor store) ─────────────────────────────────

/// Warm state hash caches by computing [`BeaconState::canonical_root`] once.
///
/// A decoded state starts cold by construction (§3.4). Paying the full root
/// here keeps the first post-bootstrap import off the cold path (§8.3).
/// Records `cc_chain_state_hash_tree_root_seconds{path="cold"}` for the warm-up.
pub fn warm_canonical_root<P: Preset>(state: &mut BeaconState<P>, metrics: &ChainMetrics) -> Root {
    metrics.time_state_hash_tree_root(HashPath::Cold, || state.canonical_root())
}

/// Build a fork-choice store from a verified checkpoint and spawn the core.
///
/// CC-19b:
/// - warms `canonical_root()` caches on the fetched state before store seed
/// - seeds proto-array / justified / finalized / blocks via `get_forkchoice_store`
/// - advances store time from wall clock via `on_tick` (genesis + slot already set)
///
/// Aggregate health flip and bind-before-bootstrap ordering live in `main.rs`.
pub fn spawn_core_from_checkpoint<P: Preset + 'static>(
    fetched: FetchedCheckpoint<P>,
    chain_config: ChainConfig,
    head: HeadSnapshotStore,
    event_tx: tokio::sync::mpsc::Sender<EventInput>,
    metrics: ChainMetrics,
    core_cfg: CoreConfig,
) -> Result<CoreThread, CheckpointError> {
    spawn_core_from_checkpoint_with_epoch(
        fetched,
        chain_config,
        head,
        EpochContextStore::new(),
        event_tx,
        metrics,
        core_cfg,
    )
}

/// Like [`spawn_core_from_checkpoint`] but reuses a caller-owned
/// [`EpochContextStore`] so `P2pStream` sessions opened pre-bootstrap share
/// the same ArcSwap identity (CC-27a F2).
pub fn spawn_core_from_checkpoint_with_epoch<P: Preset + 'static>(
    mut fetched: FetchedCheckpoint<P>,
    chain_config: ChainConfig,
    head: HeadSnapshotStore,
    epoch: EpochContextStore,
    event_tx: tokio::sync::mpsc::Sender<EventInput>,
    metrics: ChainMetrics,
    core_cfg: CoreConfig,
) -> Result<CoreThread, CheckpointError> {
    // §8.3: warm caches once so the first import does not pay a cold full root.
    let warm_root = warm_canonical_root(&mut fetched.state, &metrics);
    tracing::info!(
        state_root = %warm_root,
        slot = fetched.signed_block.message.slot.as_u64(),
        "anchor state caches warmed via canonical_root"
    );
    let engine = core_cfg
        .engine
        .clone()
        .ok_or_else(|| CheckpointError::Store("in-process engine not configured".into()))?;
    let ctx = TransitionContext::new(&chain_config, engine.as_ref());
    ctx.top_up_pubkey_cache(&fetched.state);
    metrics.observe_import_state_with_pubkeys(&fetched.state, ctx.pubkeys().len());

    // Shared PeerDAS available set: store DA + core mark/re-drive (CC-24d).
    let peer_das = Arc::new(PeerDasAvailability::new());
    let da_for_store: Arc<dyn cc_fork_choice::DataAvailability> = peer_das.clone();
    let mut store: Store<P> = get_forkchoice_store(
        fetched.state,
        &fetched.signed_block.message,
        engine,
        da_for_store,
        chain_config.seconds_per_slot,
    )
    .map_err(|e| CheckpointError::Store(e.to_string()))?;

    // Wall-clock catch-up: store was seeded to genesis_time + slot * seconds_per_slot.
    // Advance to now so the first import sees a realistic current slot (§7.4 / issue notes).
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    if now > store.time()
        && let Err(e) = on_tick(&mut store, now)
    {
        tracing::warn!(error = %e, now, "on_tick during anchor seed failed; continuing");
    }

    let mut core_cfg = core_cfg;
    core_cfg.peer_das = Some(peer_das);
    Ok(spawn_core_thread_with_epoch(
        store,
        chain_config,
        head,
        epoch,
        event_tx,
        metrics,
        core_cfg,
    ))
}

/// Summary of a successful bootstrap after the state has been moved into the store.
#[derive(Debug, Clone)]
pub struct BootstrapSummary {
    /// Genesis fields from `/eth/v1/beacon/genesis`.
    pub genesis: GenesisInfo,
    /// Verified block root.
    pub block_root: Root,
    /// Provider that served the checkpoint.
    pub provider: String,
    /// Anchor slot.
    pub slot: u64,
}

/// End-to-end: fetch → warm caches → spawn core when providers are configured.
///
/// Returns the running core and a summary (state is owned by the core store).
/// Health / local-ready flip is the caller's responsibility (CC-19b `main`).
pub async fn bootstrap_core_from_providers<P: Preset + 'static>(
    cfg: &CheckpointBootstrapConfig,
    head: HeadSnapshotStore,
    event_tx: tokio::sync::mpsc::Sender<EventInput>,
    metrics: ChainMetrics,
    core_cfg: CoreConfig,
) -> Result<(CoreThread, BootstrapSummary), CheckpointError> {
    bootstrap_core_from_providers_with_epoch::<P>(
        cfg,
        head,
        EpochContextStore::new(),
        event_tx,
        metrics,
        core_cfg,
    )
    .await
}

/// Bootstrap with a shared [`EpochContextStore`] (CC-27a: stream sessions
/// opened before install keep reading the same ArcSwap).
pub async fn bootstrap_core_from_providers_with_epoch<P: Preset + 'static>(
    cfg: &CheckpointBootstrapConfig,
    head: HeadSnapshotStore,
    epoch: EpochContextStore,
    event_tx: tokio::sync::mpsc::Sender<EventInput>,
    metrics: ChainMetrics,
    core_cfg: CoreConfig,
) -> Result<(CoreThread, BootstrapSummary), CheckpointError> {
    let fetched = fetch_checkpoint::<P>(cfg, &metrics).await?;
    let summary = BootstrapSummary {
        genesis: fetched.genesis,
        block_root: fetched.block_root,
        provider: fetched.provider.clone(),
        slot: fetched.signed_block.message.slot.as_u64(),
    };
    let core = spawn_core_from_checkpoint_with_epoch(
        fetched,
        cfg.chain_config.clone(),
        head,
        epoch,
        event_tx,
        metrics,
        core_cfg,
    )?;
    Ok((core, summary))
}

// ── tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use std::convert::Infallible;
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use cc_types::config::{BlobSchedule, PresetName};
    use cc_types::preset::Minimal;
    use cc_types::primitives::{Epoch, ExecutionAddress, ForkVersion, Slot, ValidatorIndex};
    use cc_types::{BeaconBlock, BeaconBlockBody};
    use http_body_util::{BodyExt, Full};
    use hyper::body::Incoming;
    use hyper::server::conn::http1;
    use hyper::service::service_fn;
    use hyper::{Request, Response, StatusCode};
    use hyper_util::rt::TokioIo;
    use prometheus_client::registry::Registry;
    use ssz::Encode;
    use tokio::net::TcpListener;
    use tokio::sync::Mutex;

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

    fn local_spec_map(cfg: &ChainConfig) -> BTreeMap<String, String> {
        let mut m = BTreeMap::new();
        m.insert(
            "GENESIS_FORK_VERSION".into(),
            format!("{}", cfg.genesis_fork_version),
        );
        m.insert(
            "ALTAIR_FORK_VERSION".into(),
            format!("{}", cfg.altair_fork_version),
        );
        m.insert(
            "BELLATRIX_FORK_VERSION".into(),
            format!("{}", cfg.bellatrix_fork_version),
        );
        m.insert(
            "CAPELLA_FORK_VERSION".into(),
            format!("{}", cfg.capella_fork_version),
        );
        m.insert(
            "DENEB_FORK_VERSION".into(),
            format!("{}", cfg.deneb_fork_version),
        );
        m.insert(
            "ELECTRA_FORK_VERSION".into(),
            format!("{}", cfg.electra_fork_version),
        );
        m.insert(
            "FULU_FORK_VERSION".into(),
            format!("{}", cfg.fulu_fork_version),
        );
        m.insert(
            "FULU_FORK_EPOCH".into(),
            cfg.fulu_fork_epoch.as_u64().to_string(),
        );
        m.insert("SECONDS_PER_SLOT".into(), cfg.seconds_per_slot.to_string());
        m.insert(
            "BLOB_SCHEDULE".into(),
            serde_json::to_string(
                &cfg.blob_schedule
                    .entries()
                    .iter()
                    .map(|e| {
                        serde_json::json!({
                            "EPOCH": e.epoch.as_u64(),
                            "MAX_BLOBS_PER_BLOCK": e.max_blobs_per_block,
                        })
                    })
                    .collect::<Vec<_>>(),
            )
            .unwrap(),
        );
        m
    }

    /// Epoch-aligned Minimal block + matching state (slot 8 = epoch 1).
    fn matching_pair(slot: u64) -> (SignedBeaconBlock<Minimal>, BeaconState<Minimal>) {
        let mut state = BeaconState::<Minimal>::default();
        state.set_slot(Slot::new(slot));
        state.set_genesis_time(1_000);
        let gvr = Root::from_array([0x21; 32]);
        state.set_genesis_validators_root(gvr);
        let state_root = Root::from_hash256(TreeHash::tree_hash_root(&state));
        let block = SignedBeaconBlock {
            message: BeaconBlock {
                slot: Slot::new(slot),
                proposer_index: ValidatorIndex::new(0),
                parent_root: Root::ZERO,
                state_root,
                body: BeaconBlockBody::default(),
            },
            signature: Default::default(),
        };
        (block, state)
    }

    // ── CC-19/1: three named verification errors ────────────────────────────

    #[test]
    fn verify_checkpoint_root_mismatch() {
        let (block, state) = matching_pair(8);
        let wrong = Root::from_array([0xAB; 32]);
        let err = verify_checkpoint(&block, &state, Some(wrong)).unwrap_err();
        assert!(
            matches!(err, CheckpointError::CheckpointRootMismatch { .. }),
            "{err:?}"
        );
        let msg = err.to_string();
        assert!(msg.contains("CheckpointRootMismatch"), "{msg}");
    }

    #[test]
    fn verify_state_root_mismatch() {
        let (mut block, state) = matching_pair(8);
        block.message.state_root = Root::from_array([0xCD; 32]);
        let err = verify_checkpoint(&block, &state, None).unwrap_err();
        assert!(
            matches!(err, CheckpointError::StateRootMismatch { .. }),
            "{err:?}"
        );
        assert!(err.to_string().contains("StateRootMismatch"));
    }

    #[test]
    fn verify_anchor_slot_not_epoch_aligned() {
        // Minimal SLOTS_PER_EPOCH = 8; slot 9 is not epoch-aligned.
        let (block, state) = matching_pair(9);
        let err = verify_checkpoint(&block, &state, None).unwrap_err();
        assert!(
            matches!(err, CheckpointError::AnchorSlotNotEpochAligned { .. }),
            "{err:?}"
        );
        assert!(err.to_string().contains("AnchorSlotNotEpochAligned"));
    }

    #[test]
    fn verify_ok_epoch_aligned() {
        let (block, state) = matching_pair(16);
        let root = verify_checkpoint(&block, &state, None).unwrap();
        let expected = Root::from_hash256(TreeHash::tree_hash_root(&block.message));
        assert_eq!(root, expected);
        // With expected root matching:
        verify_checkpoint(&block, &state, Some(root)).unwrap();
    }

    #[test]
    fn provider_url_https_required_except_loopback() {
        validate_provider_base("https://checkpoint-sync.hoodi.ethpandaops.io").unwrap();
        validate_provider_base("http://127.0.0.1:9").unwrap();
        validate_provider_base("http://localhost:8080").unwrap();
        let err = validate_provider_base("http://example.com").unwrap_err();
        assert!(
            err.to_string().contains("https"),
            "expected https rejection, got {err}"
        );
        let err = validate_provider_base("ftp://127.0.0.1").unwrap_err();
        assert!(err.to_string().contains("https") || err.to_string().contains("scheme"));
    }

    #[test]
    fn network_retries_mean_additional_after_first() {
        // Default NETWORK_RETRIES=2 → 3 total attempts.
        assert_eq!(NETWORK_RETRIES.saturating_add(1), 3);
    }

    // ── CC-19/4: config cross-check ─────────────────────────────────────────

    #[test]
    fn config_cross_check_ok() {
        let cfg = minimal_config();
        let remote = local_spec_map(&cfg);
        cross_check_spec(&remote, &cfg).unwrap();
    }

    #[test]
    fn config_cross_check_mismatch_names_keys_and_values() {
        let cfg = minimal_config();
        let mut remote = local_spec_map(&cfg);
        remote.insert("SECONDS_PER_SLOT".into(), "12".into());
        remote.insert("FULU_FORK_EPOCH".into(), "999".into());
        let err = cross_check_spec(&remote, &cfg).unwrap_err();
        match err {
            CheckpointError::ConfigMismatch { detail } => {
                assert!(detail.contains("SECONDS_PER_SLOT"), "{detail}");
                assert!(detail.contains("local=6"), "{detail}");
                assert!(detail.contains("remote=12"), "{detail}");
                assert!(detail.contains("FULU_FORK_EPOCH"), "{detail}");
                assert!(detail.contains("999"), "{detail}");
            }
            other => panic!("expected ConfigMismatch, got {other:?}"),
        }
    }

    #[test]
    fn config_cross_check_unknown_key_ignored() {
        let cfg = minimal_config();
        let mut remote = local_spec_map(&cfg);
        remote.insert("SOME_FUTURE_FIELD".into(), "1".into());
        cross_check_spec(&remote, &cfg).unwrap();
    }

    // ── CC-1G: BLOB_SCHEDULE from /eth/v1/config/spec ───────────────────────

    /// Hoodi / mainnet YAML fixtures live in `cc-types` (CC-10c file source).
    fn types_fixture(name: &str) -> String {
        format!(
            "{}/../../crates/types/tests/fixtures/{name}",
            env!("CARGO_MANIFEST_DIR")
        )
    }

    /// Beacon-API shape after `parse_spec_json` flattens a nested array: string
    /// fields (public providers) or numeric fields (our local_spec_map).
    fn hoodi_spec_blob_schedule_string_fields() -> String {
        r#"[{"EPOCH":"52480","MAX_BLOBS_PER_BLOCK":"15"},{"EPOCH":"54016","MAX_BLOBS_PER_BLOCK":"21"}]"#
            .to_owned()
    }

    fn mainnet_spec_blob_schedule_string_fields() -> String {
        r#"[{"EPOCH":"412672","MAX_BLOBS_PER_BLOCK":"15"},{"EPOCH":"419072","MAX_BLOBS_PER_BLOCK":"21"}]"#
            .to_owned()
    }

    #[test]
    fn blob_schedule_from_spec_matches_file_hoodi_and_mainnet() {
        let hoodi_file = ChainConfig::from_yaml_file(types_fixture("hoodi-config.yaml"))
            .expect("hoodi yaml")
            .blob_schedule;
        let hoodi_api =
            blob_schedule_from_spec(&hoodi_spec_blob_schedule_string_fields()).expect("hoodi api");
        assert_eq!(
            hoodi_api, hoodi_file,
            "Hoodi file and /eth/v1/config/spec sources must yield identical BlobSchedule"
        );
        assert_eq!(hoodi_api.entries()[0].epoch, Epoch::new(52_480));
        assert_eq!(hoodi_api.entries()[0].max_blobs_per_block, 15);
        assert_eq!(hoodi_api.entries()[1].epoch, Epoch::new(54_016));
        assert_eq!(hoodi_api.entries()[1].max_blobs_per_block, 21);

        let mainnet_file = ChainConfig::from_yaml_file(types_fixture("mainnet-config.yaml"))
            .expect("mainnet yaml")
            .blob_schedule;
        let mainnet_api = blob_schedule_from_spec(&mainnet_spec_blob_schedule_string_fields())
            .expect("mainnet api");
        assert_eq!(mainnet_api, mainnet_file);

        // Numeric-field JSON (local_spec_map style) also round-trips.
        let numeric = r#"[{"EPOCH":52480,"MAX_BLOBS_PER_BLOCK":15},{"EPOCH":54016,"MAX_BLOBS_PER_BLOCK":21}]"#;
        assert_eq!(blob_schedule_from_spec(numeric).unwrap(), hoodi_file);
    }

    #[test]
    fn blob_schedule_from_spec_rejects_malformed_and_non_monotonic() {
        // Malformed JSON / shape — rejected at the edge, before constructor.
        assert!(matches!(
            blob_schedule_from_spec("not-json"),
            Err(BlobScheduleFromSpecError::Malformed(_))
        ));
        assert!(matches!(
            blob_schedule_from_spec(r#"{"EPOCH":1}"#),
            Err(BlobScheduleFromSpecError::Malformed(_))
        ));
        assert!(matches!(
            blob_schedule_from_spec(r#"[{"EPOCH":"x","MAX_BLOBS_PER_BLOCK":"1"}]"#),
            Err(BlobScheduleFromSpecError::Malformed(_))
        ));

        // Empty array → Empty via the shared validating constructor.
        let empty = blob_schedule_from_spec("[]").unwrap_err();
        assert!(
            matches!(
                empty,
                BlobScheduleFromSpecError::Validation(BlobScheduleError::Empty)
            ),
            "{empty:?}"
        );

        // Non-monotonic epochs → Unsorted via the same constructor the file path uses.
        let unsorted = blob_schedule_from_spec(
            r#"[{"EPOCH":"100","MAX_BLOBS_PER_BLOCK":"15"},{"EPOCH":"50","MAX_BLOBS_PER_BLOCK":"21"}]"#,
        )
        .unwrap_err();
        assert!(
            matches!(
                unsorted,
                BlobScheduleFromSpecError::Validation(BlobScheduleError::Unsorted { .. })
            ),
            "{unsorted:?}"
        );

        // Duplicate epochs.
        let dup = blob_schedule_from_spec(
            r#"[{"EPOCH":100,"MAX_BLOBS_PER_BLOCK":15},{"EPOCH":100,"MAX_BLOBS_PER_BLOCK":21}]"#,
        )
        .unwrap_err();
        assert!(
            matches!(
                dup,
                BlobScheduleFromSpecError::Validation(BlobScheduleError::DuplicateEpoch(_))
            ),
            "{dup:?}"
        );
    }

    #[test]
    fn blob_schedule_from_spec_map_and_cross_check_use_validating_constructor() {
        let hoodi = ChainConfig::from_yaml_file(types_fixture("hoodi-config.yaml")).unwrap();
        let mut remote = BTreeMap::new();
        remote.insert(
            "BLOB_SCHEDULE".into(),
            hoodi_spec_blob_schedule_string_fields(),
        );
        let from_map = blob_schedule_from_spec_map(&remote).unwrap();
        assert_eq!(from_map, hoodi.blob_schedule);

        // Full cross-check with API-shaped (string-field) BLOB_SCHEDULE passes.
        let mut remote_full = BTreeMap::new();
        remote_full.insert(
            "GENESIS_FORK_VERSION".into(),
            format!("{}", hoodi.genesis_fork_version),
        );
        remote_full.insert(
            "ALTAIR_FORK_VERSION".into(),
            format!("{}", hoodi.altair_fork_version),
        );
        remote_full.insert(
            "BELLATRIX_FORK_VERSION".into(),
            format!("{}", hoodi.bellatrix_fork_version),
        );
        remote_full.insert(
            "CAPELLA_FORK_VERSION".into(),
            format!("{}", hoodi.capella_fork_version),
        );
        remote_full.insert(
            "DENEB_FORK_VERSION".into(),
            format!("{}", hoodi.deneb_fork_version),
        );
        remote_full.insert(
            "ELECTRA_FORK_VERSION".into(),
            format!("{}", hoodi.electra_fork_version),
        );
        remote_full.insert(
            "FULU_FORK_VERSION".into(),
            format!("{}", hoodi.fulu_fork_version),
        );
        remote_full.insert(
            "FULU_FORK_EPOCH".into(),
            hoodi.fulu_fork_epoch.as_u64().to_string(),
        );
        remote_full.insert(
            "SECONDS_PER_SLOT".into(),
            hoodi.seconds_per_slot.to_string(),
        );
        remote_full.insert(
            "BLOB_SCHEDULE".into(),
            hoodi_spec_blob_schedule_string_fields(),
        );
        cross_check_spec(&remote_full, &hoodi).expect("string-field API schedule must match file");

        // Unparseable remote schedule → ConfigMismatch naming BLOB_SCHEDULE.
        remote_full.insert("BLOB_SCHEDULE".into(), "[]".into());
        let err = cross_check_spec(&remote_full, &hoodi).unwrap_err();
        match err {
            CheckpointError::ConfigMismatch { detail } => {
                assert!(detail.contains("BLOB_SCHEDULE"), "{detail}");
            }
            other => panic!("expected ConfigMismatch, got {other:?}"),
        }
    }

    // ── CC-19/2 + /5 + by-root fallback: HTTP fixture server ────────────────

    #[derive(Clone, Default)]
    struct FixtureState {
        /// Force non-fulu on block responses.
        bad_fork: bool,
        /// 404 by-root state; serve finalized.
        by_root_404: bool,
        /// First N finalized state responses are mismatched (race).
        mismatch_state_count: u32,
        /// Count of full triple-relevant block fetches.
        block_fetches: Arc<AtomicU32>,
        /// Count of by-root state fetches.
        by_root_fetches: Arc<AtomicU32>,
        /// Count of finalized state fetches.
        finalized_state_fetches: Arc<AtomicU32>,
        /// How many times verification-path was hit via provider (for retry assert).
        /// Stored genesis/spec/block/state.
        block_ssz: Arc<Mutex<Vec<u8>>>,
        state_ssz: Arc<Mutex<Vec<u8>>>,
        bad_state_ssz: Arc<Mutex<Vec<u8>>>,
        genesis_json: Arc<Mutex<String>>,
        spec_json: Arc<Mutex<String>>,
        mismatch_served: Arc<AtomicU32>,
    }

    async fn run_fixture_server(state: FixtureState) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(state);
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let io = TokioIo::new(stream);
                let st = Arc::clone(&state);
                tokio::spawn(async move {
                    let service = service_fn(move |req: Request<Incoming>| {
                        let st = Arc::clone(&st);
                        async move { handle_fixture(req, &st).await }
                    });
                    let _ = http1::Builder::new().serve_connection(io, service).await;
                });
            }
        });
        // Tiny yield so accept loop is ready.
        tokio::task::yield_now().await;
        addr
    }

    async fn handle_fixture(
        req: Request<Incoming>,
        st: &FixtureState,
    ) -> Result<Response<Full<Bytes>>, Infallible> {
        let path = req.uri().path().to_owned();
        let _ = req.into_body().collect().await;

        if path.ends_with("/eth/v1/beacon/genesis") {
            let body = st.genesis_json.lock().await.clone();
            return Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(Full::new(Bytes::from(body)))
                .unwrap());
        }
        if path.ends_with("/eth/v1/config/spec") {
            let body = st.spec_json.lock().await.clone();
            return Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(Full::new(Bytes::from(body)))
                .unwrap());
        }
        if path.contains("/eth/v2/beacon/blocks/finalized") {
            st.block_fetches.fetch_add(1, Ordering::SeqCst);
            let body = st.block_ssz.lock().await.clone();
            let version = if st.bad_fork { "electra" } else { "fulu" };
            return Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/octet-stream")
                .header("eth-consensus-version", version)
                .body(Full::new(Bytes::from(body)))
                .unwrap());
        }
        if path.contains("/eth/v2/debug/beacon/states/") {
            let id = path.rsplit('/').next().unwrap_or("");
            if id == "finalized" {
                st.finalized_state_fetches.fetch_add(1, Ordering::SeqCst);
                let n = st.mismatch_served.fetch_add(1, Ordering::SeqCst);
                let body = if n < st.mismatch_state_count {
                    st.bad_state_ssz.lock().await.clone()
                } else {
                    st.state_ssz.lock().await.clone()
                };
                return Ok(Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "application/octet-stream")
                    .header("eth-consensus-version", "fulu")
                    .body(Full::new(Bytes::from(body)))
                    .unwrap());
            }
            // by-root
            st.by_root_fetches.fetch_add(1, Ordering::SeqCst);
            if st.by_root_404 {
                return Ok(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Full::new(Bytes::from_static(b"not found")))
                    .unwrap());
            }
            let body = st.state_ssz.lock().await.clone();
            return Ok(Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/octet-stream")
                .header("eth-consensus-version", "fulu")
                .body(Full::new(Bytes::from(body)))
                .unwrap());
        }

        Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Full::new(Bytes::from_static(b"nope")))
            .unwrap())
    }

    async fn load_fixture_payloads(
        st: &FixtureState,
        cfg: &ChainConfig,
    ) -> (SignedBeaconBlock<Minimal>, Root) {
        let (block, state) = matching_pair(8);
        let block_root = Root::from_hash256(TreeHash::tree_hash_root(&block.message));
        let genesis = GenesisInfo {
            genesis_time: state.genesis_time(),
            genesis_validators_root: state.genesis_validators_root(),
        };
        let genesis_json = serde_json::json!({
            "data": {
                "genesis_time": genesis.genesis_time.to_string(),
                "genesis_validators_root": format!("{}", genesis.genesis_validators_root),
                "genesis_fork_version": "0x00000001",
            }
        })
        .to_string();
        let mut spec = local_spec_map(cfg);
        let spec_json = serde_json::json!({ "data": spec }).to_string();
        // clear unused mut warning by re-using
        let _ = &mut spec;

        *st.block_ssz.lock().await = block.as_ssz_bytes();
        *st.state_ssz.lock().await = state.as_ssz_bytes();
        // Bad state: different slot → different root.
        let (_, bad_state) = matching_pair(16);
        *st.bad_state_ssz.lock().await = bad_state.as_ssz_bytes();
        *st.genesis_json.lock().await = genesis_json;
        *st.spec_json.lock().await = spec_json;
        (block, block_root)
    }

    #[tokio::test]
    async fn unsupported_fork_aborts_provider_no_conversion() {
        let cfg = minimal_config();
        let st = FixtureState {
            bad_fork: true,
            ..FixtureState::default()
        };
        let _ = load_fixture_payloads(&st, &cfg).await;
        let addr = run_fixture_server(st).await;
        let base = format!("http://{addr}");

        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let boot = CheckpointBootstrapConfig {
            providers: vec![base.clone()],
            expected_checkpoint_root: None,
            chain_config: cfg,
            connect_timeout: Duration::from_secs(1),
            total_timeout: Duration::from_secs(5),
            network_retries: 2,
            triple_attempts: 2,
        };
        let err = fetch_checkpoint::<Minimal>(&boot, &metrics)
            .await
            .unwrap_err();
        assert!(
            matches!(err, CheckpointError::AllProvidersFailed)
                || err.to_string().contains("unsupported fork")
                || err.to_string().contains("all checkpoint"),
            "{err}"
        );
        // Provider recorded a failure.
        assert_eq!(
            metrics.bootstrap_attempt_count(&base, BootstrapResult::Failure),
            1
        );
        assert_eq!(
            metrics.bootstrap_attempt_count(&base, BootstrapResult::Success),
            0
        );
    }

    #[tokio::test]
    async fn by_root_404_falls_back_to_finalized_alias() {
        let cfg = minimal_config();
        let st = FixtureState {
            by_root_404: true,
            by_root_fetches: Arc::new(AtomicU32::new(0)),
            finalized_state_fetches: Arc::new(AtomicU32::new(0)),
            ..FixtureState::default()
        };
        let by_root = Arc::clone(&st.by_root_fetches);
        let finalized = Arc::clone(&st.finalized_state_fetches);
        let (_block, block_root) = load_fixture_payloads(&st, &cfg).await;
        let addr = run_fixture_server(st).await;
        let base = format!("http://{addr}");

        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let boot = CheckpointBootstrapConfig {
            providers: vec![base.clone()],
            expected_checkpoint_root: Some(block_root),
            chain_config: cfg,
            connect_timeout: Duration::from_secs(1),
            total_timeout: Duration::from_secs(5),
            network_retries: 1,
            triple_attempts: 2,
        };
        let fetched = fetch_checkpoint::<Minimal>(&boot, &metrics)
            .await
            .expect("bootstrap via finalized fallback");
        assert_eq!(fetched.block_root, block_root);
        assert!(by_root.load(Ordering::SeqCst) >= 1);
        assert!(finalized.load(Ordering::SeqCst) >= 1);
        assert_eq!(
            metrics.bootstrap_attempt_count(&base, BootstrapResult::Success),
            1
        );
    }

    #[tokio::test]
    async fn state_block_mismatch_triggers_whole_triple_refetch() {
        let cfg = minimal_config();
        let st = FixtureState {
            by_root_404: true, // force finalized path
            mismatch_state_count: 1,
            block_fetches: Arc::new(AtomicU32::new(0)),
            mismatch_served: Arc::new(AtomicU32::new(0)),
            ..FixtureState::default()
        };
        let block_fetches = Arc::clone(&st.block_fetches);
        let (_block, block_root) = load_fixture_payloads(&st, &cfg).await;
        let addr = run_fixture_server(st).await;
        let base = format!("http://{addr}");

        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let boot = CheckpointBootstrapConfig {
            providers: vec![base],
            expected_checkpoint_root: Some(block_root),
            chain_config: cfg,
            connect_timeout: Duration::from_secs(1),
            total_timeout: Duration::from_secs(5),
            network_retries: 1,
            triple_attempts: 2,
        };
        let fetched = fetch_checkpoint::<Minimal>(&boot, &metrics)
            .await
            .expect("succeed after triple re-fetch");
        assert_eq!(fetched.block_root, block_root);
        // At least two block fetches (first race + second success).
        assert!(
            block_fetches.load(Ordering::SeqCst) >= 2,
            "expected whole-triple re-fetch, got {}",
            block_fetches.load(Ordering::SeqCst)
        );
    }

    #[tokio::test]
    async fn dead_ports_then_success_records_metrics() {
        // Two dead ports + one good fixture server (CC-19/5).
        let cfg = minimal_config();
        let st = FixtureState::default();
        let (_block, block_root) = load_fixture_payloads(&st, &cfg).await;
        let addr = run_fixture_server(st).await;
        let good = format!("http://{addr}");
        // Ephemeral ports that nothing listens on.
        let dead1 = "http://127.0.0.1:1".to_owned();
        let dead2 = "http://127.0.0.1:2".to_owned();

        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let boot = CheckpointBootstrapConfig {
            providers: vec![dead1.clone(), dead2.clone(), good.clone()],
            expected_checkpoint_root: Some(block_root),
            chain_config: cfg,
            connect_timeout: Duration::from_millis(100),
            total_timeout: Duration::from_millis(300),
            network_retries: 1, // keep offline test fast
            triple_attempts: 2,
        };
        let fetched = fetch_checkpoint::<Minimal>(&boot, &metrics)
            .await
            .expect("third provider succeeds");
        assert_eq!(fetched.provider, good);
        assert_eq!(
            metrics.bootstrap_attempt_count(&dead1, BootstrapResult::Failure),
            1
        );
        assert_eq!(
            metrics.bootstrap_attempt_count(&dead2, BootstrapResult::Failure),
            1
        );
        assert_eq!(
            metrics.bootstrap_attempt_count(&good, BootstrapResult::Success),
            1
        );
    }

    #[tokio::test]
    async fn oversize_body_fail_closed() {
        // Stream more than MAX_BLOCK_BYTES without Content-Length; client aborts mid-stream.
        use futures::stream;
        use http_body_util::StreamBody;
        use hyper::body::Frame;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let io = TokioIo::new(stream);
            let service = service_fn(|_req: Request<Incoming>| async move {
                // 9 × 1 MiB chunks = 9 MiB > MAX_BLOCK_BYTES (8 MiB).
                let chunk = Bytes::from(vec![0u8; 1024 * 1024]);
                let chunks = stream::iter(
                    (0..9).map(move |_| Ok::<_, Infallible>(Frame::data(chunk.clone()))),
                );
                let body = StreamBody::new(chunks);
                Ok::<_, Infallible>(
                    Response::builder()
                        .status(StatusCode::OK)
                        .header("content-type", "application/octet-stream")
                        .header("eth-consensus-version", "fulu")
                        .body(body)
                        .unwrap(),
                )
            });
            let _ = http1::Builder::new().serve_connection(io, service).await;
        });
        tokio::task::yield_now().await;

        let client =
            CheckpointClient::new(Duration::from_secs(1), Duration::from_secs(30)).unwrap();
        let base = format!("http://{addr}");
        let err = client.fetch_finalized_block_ssz(&base).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("exceeds max") || msg.contains("body exceeds max"),
            "expected body-size failure, got {msg}"
        );
    }

    #[tokio::test]
    async fn non_https_remote_provider_rejected_before_fetch() {
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let cfg = minimal_config();
        let boot = CheckpointBootstrapConfig {
            providers: vec!["http://example.com".into()],
            expected_checkpoint_root: None,
            chain_config: cfg,
            connect_timeout: Duration::from_millis(100),
            total_timeout: Duration::from_millis(200),
            network_retries: 0,
            triple_attempts: 1,
        };
        let err = fetch_checkpoint::<Minimal>(&boot, &metrics)
            .await
            .unwrap_err();
        assert!(matches!(err, CheckpointError::AllProvidersFailed));
        assert_eq!(
            metrics.bootstrap_attempt_count("http://example.com", BootstrapResult::Failure),
            1
        );
    }

    #[tokio::test]
    async fn verification_failure_not_network_retried() {
        // Operator root wrong → CheckpointRootMismatch; only one attempt metric.
        let cfg = minimal_config();
        let st = FixtureState {
            block_fetches: Arc::new(AtomicU32::new(0)),
            ..FixtureState::default()
        };
        let block_fetches = Arc::clone(&st.block_fetches);
        let (_block, _block_root) = load_fixture_payloads(&st, &cfg).await;
        let addr = run_fixture_server(st).await;
        let base = format!("http://{addr}");

        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let wrong = Root::from_array([0xFF; 32]);
        let boot = CheckpointBootstrapConfig {
            providers: vec![base.clone()],
            expected_checkpoint_root: Some(wrong),
            chain_config: cfg,
            connect_timeout: Duration::from_secs(1),
            total_timeout: Duration::from_secs(5),
            network_retries: 2, // would retry if network; verification must not
            triple_attempts: 2,
        };
        let err = fetch_checkpoint::<Minimal>(&boot, &metrics)
            .await
            .unwrap_err();
        assert!(matches!(err, CheckpointError::AllProvidersFailed));
        // One block fetch only (no network retry loop on verification).
        assert_eq!(
            block_fetches.load(Ordering::SeqCst),
            1,
            "verification failure must not re-fetch via network retries"
        );
        assert_eq!(
            metrics.bootstrap_attempt_count(&base, BootstrapResult::Failure),
            1
        );
    }

    // ── CC-19/3: genesis_validators_root drives compute_domain ──────────────

    #[test]
    fn genesis_validators_root_affects_compute_domain_hoodi() {
        // CC-19/3: fetched GVR is what compute_domain must use. The real Hoodi
        // block signature verifies with it and fails with a wrong root.
        // (Mirror assertion lives in CC-11a / crates/crypto/tests/bls.rs.)
        use cc_crypto::{
            DOMAIN_BEACON_PROPOSER, Signature, compute_domain, compute_signing_root, get_domain,
            verify,
        };
        use cc_types::Fork;
        use std::fs;
        use std::path::PathBuf;

        const FETCH_HINT: &str = "run scripts/fetch-hoodi-fixtures.sh";
        let slot = 3_649_472u64;
        let cache = std::env::var("HOODI_FIXTURES_CACHE")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                PathBuf::from(std::env::var("HOME").unwrap()).join(".cache/cc-hoodi-fixtures")
            });
        let block_path = cache.join(slot.to_string()).join("signed_beacon_block.ssz");
        let state_path = cache.join(slot.to_string()).join("beacon_state.ssz");
        if !block_path.is_file() || !state_path.is_file() {
            eprintln!("skip CC-19/3 hoodi domain test: fixtures missing ({FETCH_HINT})");
            return;
        }

        let block = fs::read(&block_path).expect(FETCH_HINT);
        let state = fs::read(&state_path).expect(FETCH_HINT);

        // genesis_time (8) + genesis_validators_root (32) are the first fixed fields.
        assert!(state.len() >= 40, "state too short");
        let gvr = Root::from_array(state[8..40].try_into().unwrap());
        assert_eq!(
            format!("{gvr}"),
            "0x212f13fc4df078b6cb7db228f1c8307566dcecf900867401a92023d7ba99cb5f"
        );

        assert!(block.len() >= 100);
        let msg_off = u32::from_le_bytes(block[0..4].try_into().unwrap()) as usize;
        let sig_bytes: [u8; 96] = block[4..100].try_into().unwrap();
        let signature = Signature::deserialize(&sig_bytes).expect("sig");
        let msg = &block[msg_off..];
        let slot_u64 = u64::from_le_bytes(msg[0..8].try_into().unwrap());
        let proposer_index = u64::from_le_bytes(msg[8..16].try_into().unwrap()) as usize;
        let epoch = slot_u64 / 32;

        // Hoodi Fulu is active at the anchor; fork.current_version = FULU.
        let fork = Fork {
            previous_version: ForkVersion::from_array([0x60, 0x00, 0x09, 0x10]),
            current_version: ForkVersion::from_array([0x70, 0x00, 0x09, 0x10]),
            epoch: Epoch::new(50_688),
        };
        let block_root = {
            let h = "11e0dfd8bba93823b1100efc4de321da5bcd583661519df2710e266c05712060";
            let mut a = [0u8; 32];
            for i in 0..32 {
                a[i] = u8::from_str_radix(&h[i * 2..i * 2 + 2], 16).unwrap();
            }
            Root::from_array(a)
        };

        let domain_ok = get_domain(&fork, DOMAIN_BEACON_PROPOSER, Some(Epoch::new(epoch)), gvr);
        let domain_bad = get_domain(
            &fork,
            DOMAIN_BEACON_PROPOSER,
            Some(Epoch::new(epoch)),
            Root::from_array([0u8; 32]),
        );
        assert_ne!(domain_ok, domain_bad);
        assert_eq!(
            domain_ok,
            compute_domain(
                DOMAIN_BEACON_PROPOSER,
                Some(fork.current_version),
                Some(gvr),
            )
        );

        // Extract proposer pubkey from the validators list (variable field).
        // Electra/Fulu BeaconState field layout: fixed prefix then offsets.
        // Validators is a known variable field; walk offsets like bls fixtures.
        let pubkey = extract_validator_pubkey(&state, proposer_index)
            .expect("extract proposer pubkey from hoodi state");
        let msg_ok = *compute_signing_root(&block_root, domain_ok).as_array();
        let msg_bad = *compute_signing_root(&block_root, domain_bad).as_array();
        assert!(
            verify(&pubkey, &msg_ok, &signature),
            "Hoodi block signature must verify with fetched genesis_validators_root"
        );
        assert!(
            !verify(&pubkey, &msg_bad, &signature),
            "Hoodi block signature must fail with wrong genesis_validators_root"
        );
        let _ = msg;
    }

    /// Electra/Fulu post-state walker: validators list → pubkey at index (same
    /// layout as `crates/crypto/tests/bls.rs` — avoids a full 200 MB decode).
    fn extract_validator_pubkey(data: &[u8], index: usize) -> Result<cc_crypto::PublicKey, String> {
        use cc_crypto::PublicKey;
        const SLOTS_PER_HISTORICAL_ROOT: usize = 8192;
        const EPOCHS_PER_HISTORICAL_VECTOR: usize = 65536;
        const EPOCHS_PER_SLASHINGS_VECTOR: usize = 8192;
        const SYNC_COMMITTEE_SIZE: usize = 512;
        const PROPOSER_LOOKAHEAD: usize = 64;
        const VALIDATOR_SIZE: usize = 121;

        #[derive(Clone, Copy)]
        enum Field {
            Fixed(usize),
            Var,
        }
        let fields: &[(&str, Field)] = &[
            ("genesis_time", Field::Fixed(8)),
            ("genesis_validators_root", Field::Fixed(32)),
            ("slot", Field::Fixed(8)),
            ("fork", Field::Fixed(16)),
            ("latest_block_header", Field::Fixed(112)),
            ("block_roots", Field::Fixed(SLOTS_PER_HISTORICAL_ROOT * 32)),
            ("state_roots", Field::Fixed(SLOTS_PER_HISTORICAL_ROOT * 32)),
            ("historical_roots", Field::Var),
            ("eth1_data", Field::Fixed(72)),
            ("eth1_data_votes", Field::Var),
            ("eth1_deposit_index", Field::Fixed(8)),
            ("validators", Field::Var),
            ("balances", Field::Var),
            (
                "randao_mixes",
                Field::Fixed(EPOCHS_PER_HISTORICAL_VECTOR * 32),
            ),
            ("slashings", Field::Fixed(EPOCHS_PER_SLASHINGS_VECTOR * 8)),
            ("previous_epoch_participation", Field::Var),
            ("current_epoch_participation", Field::Var),
            ("justification_bits", Field::Fixed(1)),
            ("previous_justified_checkpoint", Field::Fixed(40)),
            ("current_justified_checkpoint", Field::Fixed(40)),
            ("finalized_checkpoint", Field::Fixed(40)),
            ("inactivity_scores", Field::Var),
            (
                "current_sync_committee",
                Field::Fixed(SYNC_COMMITTEE_SIZE * 48 + 48),
            ),
            (
                "next_sync_committee",
                Field::Fixed(SYNC_COMMITTEE_SIZE * 48 + 48),
            ),
            ("latest_execution_payload_header", Field::Var),
            ("next_withdrawal_index", Field::Fixed(8)),
            ("next_withdrawal_validator_index", Field::Fixed(8)),
            ("historical_summaries", Field::Var),
            ("deposit_requests_start_index", Field::Fixed(8)),
            ("deposit_balance_to_consume", Field::Fixed(8)),
            ("exit_balance_to_consume", Field::Fixed(8)),
            ("earliest_exit_epoch", Field::Fixed(8)),
            ("consolidation_balance_to_consume", Field::Fixed(8)),
            ("earliest_consolidation_epoch", Field::Fixed(8)),
            ("pending_deposits", Field::Var),
            ("pending_partial_withdrawals", Field::Var),
            ("pending_consolidations", Field::Var),
            ("proposer_lookahead", Field::Fixed(PROPOSER_LOOKAHEAD * 8)),
        ];

        let mut pos = 0usize;
        let mut var_off: BTreeMap<&str, usize> = BTreeMap::new();
        for (name, field) in fields {
            match field {
                Field::Fixed(n) => {
                    if pos + n > data.len() {
                        return Err(format!("state truncated at {name}"));
                    }
                    pos += n;
                }
                Field::Var => {
                    if pos + 4 > data.len() {
                        return Err(format!("state truncated at offset {name}"));
                    }
                    let off = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
                    var_off.insert(*name, off);
                    pos += 4;
                }
            }
        }
        let val_start = *var_off
            .get("validators")
            .ok_or_else(|| "no validators".to_owned())?;
        let bal_start = *var_off
            .get("balances")
            .ok_or_else(|| "no balances".to_owned())?;
        if bal_start < val_start || bal_start > data.len() {
            return Err("validators range invalid".into());
        }
        let validators_bytes = &data[val_start..bal_start];
        if !validators_bytes.len().is_multiple_of(VALIDATOR_SIZE) {
            return Err(format!(
                "validators length {} not multiple of {VALIDATOR_SIZE}",
                validators_bytes.len()
            ));
        }
        let n = validators_bytes.len() / VALIDATOR_SIZE;
        if index >= n {
            return Err(format!("validator index {index} out of range {n}"));
        }
        let start = index * VALIDATOR_SIZE;
        let pk_bytes: [u8; 48] = validators_bytes[start..start + 48]
            .try_into()
            .map_err(|_| "pubkey slice".to_owned())?;
        PublicKey::deserialize(&pk_bytes).map_err(|e| e.to_string())
    }

    #[test]
    fn checkpoint_decode_tops_up_pubkey_cache_after_state_root_check() {
        let src = include_str!("checkpoint_sync.rs");
        let production = src.split("#[cfg(test)]").next().unwrap();
        let fn_start = production
            .find("async fn fetch_and_verify_triple")
            .expect("fetch_and_verify_triple");
        let body = &production[fn_start..];
        let decode = body
            .find("from_ssz_bytes_hydrated(ForkName::Fulu, &state_bytes)")
            .expect("checkpoint state decode");
        let mismatch = body
            .find("CheckpointError::StateRootMismatch")
            .expect("state-root check");
        assert!(
            decode < mismatch,
            "hydrated decode must precede the state-root check"
        );
        assert!(
            !body.contains("from_ssz_bytes_with(ForkName::Fulu, &state_bytes)"),
            "checkpoint state decode must not use the raw fork constructor"
        );
        let spawn = production
            .find("pub fn spawn_core_from_checkpoint_with_epoch")
            .expect("checkpoint spawn");
        let spawn_body = &production[spawn..];
        let top_up = spawn_body
            .find("ctx.top_up_pubkey_cache(&fetched.state)")
            .expect("checkpoint spawn must top up the context map");
        let observe = spawn_body
            .find("observe_import_state_with_pubkeys(&fetched.state, ctx.pubkeys().len())")
            .expect("checkpoint spawn must emit M13 gauges from ctx.pubkeys().len()");
        let store = spawn_body
            .find("get_forkchoice_store(")
            .expect("checkpoint store seed");
        assert!(
            top_up < observe && observe < store,
            "M13 observe must run after ctx top-up and before the state is moved into the store"
        );
        assert!(
            !spawn_body.contains("PubkeyIndexMap::from_registry"),
            "checkpoint spawn must not scrape a throwaway from_registry length"
        );
    }

    fn anchor_block_root(block: &SignedBeaconBlock<Minimal>) -> Root {
        Root::from_hash256(TreeHash::tree_hash_root(&block.message))
    }

    fn memory_provider(
        block: &SignedBeaconBlock<Minimal>,
        state: &BeaconState<Minimal>,
        cfg: &ChainConfig,
        version: &str,
    ) -> InMemoryCheckpointProvider {
        InMemoryCheckpointProvider::new(
            GenesisInfo {
                genesis_time: state.genesis_time(),
                genesis_validators_root: state.genesis_validators_root(),
            },
            local_spec_map(cfg),
            version,
            Bytes::from(block.as_ssz_bytes()),
            Bytes::from(state.as_ssz_bytes()),
        )
    }

    fn boot_for(cfg: ChainConfig, expected: Option<Root>) -> CheckpointBootstrapConfig {
        CheckpointBootstrapConfig {
            providers: vec!["http://127.0.0.1:9".into()],
            expected_checkpoint_root: expected,
            chain_config: cfg,
            connect_timeout: Duration::from_millis(50),
            total_timeout: Duration::from_millis(50),
            network_retries: 0,
            triple_attempts: 1,
        }
    }

    struct CountingProvider {
        inner: InMemoryCheckpointProvider,
        hits: Arc<AtomicU32>,
    }

    #[async_trait::async_trait]
    impl CheckpointProvider for CountingProvider {
        async fn fetch_genesis(&self, base: &str) -> Result<GenesisInfo, CheckpointError> {
            self.hits.fetch_add(1, Ordering::SeqCst);
            self.inner.fetch_genesis(base).await
        }

        async fn fetch_spec(
            &self,
            base: &str,
        ) -> Result<BTreeMap<String, String>, CheckpointError> {
            self.hits.fetch_add(1, Ordering::SeqCst);
            self.inner.fetch_spec(base).await
        }

        async fn fetch_finalized_block_ssz(
            &self,
            base: &str,
        ) -> Result<(String, Bytes), CheckpointError> {
            self.hits.fetch_add(1, Ordering::SeqCst);
            self.inner.fetch_finalized_block_ssz(base).await
        }

        async fn fetch_state_ssz(
            &self,
            base: &str,
            state_id: &str,
        ) -> Result<(String, Bytes), CheckpointError> {
            self.hits.fetch_add(1, Ordering::SeqCst);
            self.inner.fetch_state_ssz(base, state_id).await
        }
    }

    fn assert_anchor_not_self_parent(verified: &VerifiedAnchor<Minimal>, block_root: Root) {
        assert_ne!(verified.parent_root, block_root);
        assert_ne!(verified.trusted.parent_root, verified.trusted.block_root);
        assert_eq!(
            verified.trusted.parent_root,
            *verified.parent_root.as_array()
        );
        assert_eq!(verified.trusted.block_root, *block_root.as_array());
        assert_eq!(verified.trusted.da, cc_seam::DaVerdict::Available);
        assert!(!verified.trusted.block_ssz.is_empty());
        assert!(!verified.trusted.state_ssz.is_empty());
        assert_eq!(
            verified.trusted.scalars.len(),
            crate::FORK_CHOICE_SCALARS_SSZ_LEN
        );
        let head_root = &verified.trusted.scalars[200..232];
        assert_eq!(head_root, block_root.as_slice());
    }

    #[tokio::test]
    async fn checkpoint_arm_returns_verified_anchor_via_injected_double() {
        let cfg = minimal_config();
        let (mut block, state) = matching_pair(8);
        let parent = Root::from_array([0x11; 32]);
        block.message.parent_root = parent;
        let block_root = anchor_block_root(&block);
        let hits = Arc::new(AtomicU32::new(0));
        let provider = Arc::new(CountingProvider {
            inner: memory_provider(&block, &state, &cfg, "fulu"),
            hits: Arc::clone(&hits),
        });
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let verified = verify_anchor(
            AnchorSource::Checkpoint {
                config: boot_for(cfg, Some(block_root)),
                provider,
            },
            &metrics,
        )
        .await
        .expect("checkpoint double verifies");
        assert_eq!(verified.kind, AnchorKind::Checkpoint);
        assert_eq!(verified.block_root, block_root);
        assert_eq!(verified.parent_root, parent);
        assert_eq!(verified.slot, 8);
        assert!(
            hits.load(Ordering::SeqCst) >= 4,
            "double must serve the fetch"
        );
        assert_anchor_not_self_parent(&verified, block_root);
        let time = u64::from_le_bytes(verified.trusted.scalars[..8].try_into().unwrap());
        assert_eq!(time, 1_000 + 8 * 6);
        assert_eq!(
            metrics.bootstrap_attempt_count("http://127.0.0.1:9", BootstrapResult::Success),
            1
        );
    }

    #[tokio::test]
    async fn genesis_arm_keeps_zero_parent_without_self_parent_remap() {
        let cfg = minimal_config();
        let (block, state) = matching_pair(0);
        let block_root = anchor_block_root(&block);
        assert_eq!(block.message.parent_root, Root::ZERO);
        assert_ne!(block_root, Root::ZERO);
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let verified = verify_anchor(
            AnchorSource::Genesis {
                state: Box::new(state),
                signed_block: Box::new(block),
                chain_config: cfg,
                expected_block_root: Some(block_root),
            },
            &metrics,
        )
        .await
        .expect("genesis anchor is legal");
        assert_eq!(verified.kind, AnchorKind::Genesis);
        assert_eq!(verified.parent_root, Root::ZERO);
        assert_eq!(verified.slot, 0);
        assert_eq!(verified.provider, "genesis");
        assert_anchor_not_self_parent(&verified, block_root);
        let time = u64::from_le_bytes(verified.trusted.scalars[..8].try_into().unwrap());
        assert_eq!(time, 1_000);
    }

    #[tokio::test]
    async fn genesis_arm_rejects_state_root_mismatch() {
        let cfg = minimal_config();
        let (mut block, state) = matching_pair(0);
        block.message.state_root = Root::from_array([0xCD; 32]);
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let err = verify_anchor::<Minimal>(
            AnchorSource::Genesis {
                state: Box::new(state),
                signed_block: Box::new(block),
                chain_config: cfg,
                expected_block_root: None,
            },
            &metrics,
        )
        .await
        .unwrap_err();
        assert!(
            matches!(err, CheckpointError::StateRootMismatch { .. }),
            "{err}"
        );
    }

    #[tokio::test]
    async fn checkpoint_double_on_wrong_fork_does_not_verify() {
        let cfg = minimal_config();
        let (block, state) = matching_pair(8);
        let provider = Arc::new(memory_provider(&block, &state, &cfg, "phase0"));
        let mut registry = Registry::default();
        let metrics = ChainMetrics::register(&mut registry);
        let err = verify_anchor::<Minimal>(
            AnchorSource::Checkpoint {
                config: boot_for(cfg, None),
                provider,
            },
            &metrics,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, CheckpointError::AllProvidersFailed), "{err}");
    }

    #[test]
    fn verify_anchor_does_not_install_a_core_or_commit() {
        let src = include_str!("checkpoint_sync.rs");
        let production = src.split("#[cfg(test)]").next().unwrap();
        let start = production
            .find("pub async fn verify_anchor")
            .expect("verify_anchor");
        let rest = &production[start..];
        let end = ["\nfn ", "\nasync fn ", "\npub "]
            .iter()
            .filter_map(|marker| rest[1..].find(marker))
            .min()
            .map(|i| i + 1)
            .unwrap_or(rest.len());
        let body = &rest[..end];
        assert!(!body.contains("spawn_core"), "{body}");
        assert!(!body.contains("get_forkchoice_store"));
        assert!(!body.contains("commit_anchor"));
    }

    #[test]
    fn spawn_core_from_seed_has_one_non_test_caller() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut callers = Vec::new();
        let mut stack = vec![root.clone()];
        while let Some(dir) = stack.pop() {
            let entries = std::fs::read_dir(&dir).unwrap_or_else(|e| {
                panic!("read {}: {e}", dir.display());
            });
            for entry in entries {
                let entry = entry.unwrap();
                let path = entry.path();
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if name.starts_with('.') || name == "target" {
                    continue;
                }
                if path.is_dir() {
                    if name == "tests" {
                        continue;
                    }
                    stack.push(path);
                    continue;
                }
                if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                    continue;
                }
                let text = std::fs::read_to_string(&path).unwrap_or_default();
                // Split so this test file does not match its own needle.
                let needle = format!("spawn_core_from_{}(", "seed");
                if text.contains(&needle) {
                    let rel = path
                        .strip_prefix(&root)
                        .unwrap_or(&path)
                        .to_string_lossy()
                        .replace('\\', "/");
                    callers.push(rel);
                }
            }
        }
        callers.sort();
        assert_eq!(
            callers,
            vec!["bin/beacon-core/src/boot.rs".to_owned()],
            "spawn_core_from_seed is the only core-install path"
        );
    }
}
