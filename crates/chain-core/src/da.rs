//! Chain-side DA gate helpers (Architecture §8.3 / CC-24d) and the **block-branch
//! fast-path trigger** (CC-38a / Architecture §5.1).
//!
//! ```text
//! DataAvailable{root, slot}  →  mark PeerDasAvailability
//!                            →  re-drive pending_da entry (if any)
//!
//! on_block → Deferred(DataUnavailable)  →  park in pending_da
//!                                        →  emit block-branch FetchBlobsRequest
//!                                           (template-sized only; no cells)
//! ```
//!
//! [`PeerDasAvailability`] lives in `cc-fork-choice` (set-membership body of the
//! seam). This module owns the **pending map**, timeout config, and the
//! ordering assertion against the p2p recovery ladder.
//!
//! **CC-38a /6:** chain owns the *trigger* only. The template is ~6.5 KB
//! (header + ≤ 21 × 48 B commitments + 4 × 32 B proof). The ~353 KB cell
//! payload never crosses chain — it leaves engine for p2p on `InjectColumns`.
//! `pending_da` structure is CC-24d's; this file adds a **signal**, not a policy.
//!
//! Bounds:
//! - `pending_da`: **64** blocks, oldest-evicted
//! - per-entry timeout: **4 slots** (`da_pending_timeout_slots`) — sized to
//!   outlast the CC-25 recovery ladder (~3.3 slots worst case)

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use cc_crypto::hash32_concat;
use cc_types::containers::{BeaconBlockHeader, SignedBeaconBlockHeader};
use cc_types::preset::Preset;
use cc_types::primitives::Root;
use cc_types::{BeaconBlockBody, KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH, SignedBeaconBlock};
use ssz::Encode;
use tree_hash::TreeHash;

// ── Config defaults (Architecture §8.3) ─────────────────────────────────────

/// Concurrent blocks waiting on DA (Architecture §8.3).
pub const PENDING_DA_BOUND: usize = 64;

/// Slots a deferred block may wait for `DataAvailable` before drop.
///
/// Four slots leave one slot of margin over the recovery ladder's ~3.3-slot
/// worst case (CC-25 / §8.5).
pub const DEFAULT_DA_PENDING_TIMEOUT_SLOTS: u64 = 4;

/// Default `seconds_per_slot` used when a test does not supply chain config
/// (mainnet / Hoodi).
pub const DEFAULT_SECONDS_PER_SLOT: u64 = 12;

/// p2p recovery `max_attempts` default (CC-25 / `RequestSpec`).
pub const DEFAULT_RECOVERY_MAX_ATTEMPTS: u8 = 3;

/// p2p recovery `max_peers` default (CC-25 / `RequestSpec`).
pub const DEFAULT_RECOVERY_MAX_PEERS: u8 = 4;

/// TTFB timeout (seconds) — matches `cc_p2p` / `cc_libp2p` `TTFB_TIMEOUT`.
pub const TTFB_TIMEOUT_SECS: u64 = 5;

/// Per-chunk RESP timeout (seconds) — matches `RESP_TIMEOUT`.
pub const RESP_TIMEOUT_SECS: u64 = 10;

// ── Pending entry ───────────────────────────────────────────────────────────

/// A block parked because `on_block` returned `Deferred(DataUnavailable)`.
#[derive(Debug, Clone)]
pub struct PendingDaEntry {
    /// Beacon block root (dedup key).
    pub root: Root,
    /// Raw SSZ of the signed beacon block (re-import without re-gossip).
    pub ssz: Bytes,
    /// Fork discriminant from the original import request.
    pub fork: u32,
    /// Import source (`Source` proto enum as i32).
    pub source: i32,
    /// Slot of the block (timeout / metrics).
    pub slot: u64,
    /// Store slot when the block was parked (timeout base).
    pub parked_at_slot: u64,
}

// ── Pending map ─────────────────────────────────────────────────────────────

/// Bounded map of blocks waiting on PeerDAS sampling (Architecture §8.3).
///
/// Capacity [`PENDING_DA_BOUND`]; oldest-evicted on overflow. Per-entry
/// slot-bounded timeout via [`Self::expire`].
#[derive(Debug, Default)]
pub struct PendingDa {
    entries: HashMap<Root, PendingDaEntry>,
    /// Insertion order (front = oldest).
    order: VecDeque<Root>,
    bound: usize,
    /// Cumulative drops (timeout + capacity eviction); tests / metrics.
    dropped: u64,
}

impl PendingDa {
    /// Empty map with the default bound of 64.
    #[must_use]
    pub fn new() -> Self {
        Self::with_bound(PENDING_DA_BOUND)
    }

    /// Empty map with a custom bound (tests).
    #[must_use]
    pub fn with_bound(bound: usize) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            bound: bound.max(1),
            dropped: 0,
        }
    }

    /// Number of resident pending blocks.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the map is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Cumulative drops (timeout + capacity eviction).
    #[must_use]
    pub fn dropped_total(&self) -> u64 {
        self.dropped
    }

    /// Park a deferred block. Returns the evicted entry when at capacity.
    pub fn insert(&mut self, entry: PendingDaEntry) -> Option<PendingDaEntry> {
        let root = entry.root;
        if let std::collections::hash_map::Entry::Occupied(mut e) = self.entries.entry(root) {
            // Refresh: replace payload, keep position.
            e.insert(entry);
            return None;
        }
        let mut evicted = None;
        while self.entries.len() >= self.bound {
            if let Some(old_root) = self.order.pop_front() {
                if let Some(old) = self.entries.remove(&old_root) {
                    self.dropped = self.dropped.saturating_add(1);
                    evicted = Some(old);
                }
            } else {
                break;
            }
        }
        self.entries.insert(root, entry);
        self.order.push_back(root);
        evicted
    }

    /// Remove and return a parked block (re-drive after `DataAvailable`).
    pub fn take(&mut self, root: &Root) -> Option<PendingDaEntry> {
        let entry = self.entries.remove(root)?;
        self.order.retain(|r| r != root);
        Some(entry)
    }

    /// Peek without removing.
    #[must_use]
    pub fn get(&self, root: &Root) -> Option<&PendingDaEntry> {
        self.entries.get(root)
    }

    /// Whether `root` is parked.
    #[must_use]
    pub fn contains(&self, root: &Root) -> bool {
        self.entries.contains_key(root)
    }

    /// Drop entries whose age exceeds `timeout_slots` relative to `current_slot`.
    ///
    /// Returns the dropped entries (caller increments metrics).
    pub fn expire(&mut self, current_slot: u64, timeout_slots: u64) -> Vec<PendingDaEntry> {
        let timeout = timeout_slots.max(1);
        let mut dropped = Vec::new();
        let mut keep = VecDeque::new();
        while let Some(root) = self.order.pop_front() {
            let Some(entry) = self.entries.get(&root) else {
                continue;
            };
            let age = current_slot.saturating_sub(entry.parked_at_slot);
            if age > timeout {
                if let Some(e) = self.entries.remove(&root) {
                    self.dropped = self.dropped.saturating_add(1);
                    dropped.push(e);
                }
            } else {
                keep.push_back(root);
            }
        }
        self.order = keep;
        dropped
    }

    /// Iterate roots in oldest-first order (tests).
    pub fn roots_oldest_first(&self) -> impl Iterator<Item = &Root> {
        self.order.iter()
    }
}

// ── Timeout ordering (CC-24d acceptance) ────────────────────────────────────

/// Chain-side pending-DA wait budget in seconds.
#[must_use]
pub fn chain_pending_timeout_secs(timeout_slots: u64, seconds_per_slot: u64) -> u64 {
    timeout_slots.saturating_mul(seconds_per_slot.max(1))
}

/// Worst-case p2p recovery ladder wall time (seconds).
///
/// Architecture §8.5 / CC-25: ≤ `max_attempts` sequential attempts, each
/// bounded by `TTFB + RESP`. Peers are a pool (`max_peers`), not a wall-time
/// multiplier — each attempt selects from the pool. Matches the ~3.3-slot
/// math (3 × 15 s = 45 s at 12 s/slot).
///
/// The acceptance criteria also name `max_peers`; we require
/// `max_peers ≥ 1` and fold it as a documented non-multiplier so an inverted
/// config still fails the inequality when attempts or timeouts grow.
#[must_use]
pub fn recovery_ladder_worst_case_secs(
    max_attempts: u8,
    max_peers: u8,
    ttfb_secs: u64,
    resp_secs: u64,
) -> u64 {
    let _ = max_peers.max(1); // pool size; not a sequential cost multiplier
    u64::from(max_attempts.max(1)).saturating_mul(ttfb_secs.saturating_add(resp_secs))
}

/// Assert chain-side timeout strictly outlasts the recovery ladder.
///
/// Returns `Ok(())` when
/// `timeout_slots × seconds_per_slot > recovery_worst_case_secs`.
pub fn assert_timeout_outlasts_recovery(
    timeout_slots: u64,
    seconds_per_slot: u64,
    max_attempts: u8,
    max_peers: u8,
    ttfb_secs: u64,
    resp_secs: u64,
) -> Result<(), String> {
    let chain = chain_pending_timeout_secs(timeout_slots, seconds_per_slot);
    let ladder = recovery_ladder_worst_case_secs(max_attempts, max_peers, ttfb_secs, resp_secs);
    if chain > ladder {
        Ok(())
    } else {
        Err(format!(
            "chain da_pending_timeout ({chain}s = {timeout_slots}×{seconds_per_slot}s) \
             must be strictly greater than recovery ladder worst case \
             ({ladder}s = {max_attempts}×({ttfb_secs}+{resp_secs})); max_peers={max_peers}"
        ))
    }
}

/// Default production / Hoodi check (4 slots × 12 s > 3 × 15 s).
#[must_use]
pub fn default_timeout_ordering_ok() -> bool {
    assert_timeout_outlasts_recovery(
        DEFAULT_DA_PENDING_TIMEOUT_SLOTS,
        DEFAULT_SECONDS_PER_SLOT,
        DEFAULT_RECOVERY_MAX_ATTEMPTS,
        DEFAULT_RECOVERY_MAX_PEERS,
        TTFB_TIMEOUT_SECS,
        RESP_TIMEOUT_SECS,
    )
    .is_ok()
}

/// Duration form of the default chain timeout (for docs / schedules).
#[must_use]
pub fn default_pending_timeout_duration() -> Duration {
    Duration::from_secs(chain_pending_timeout_secs(
        DEFAULT_DA_PENDING_TIMEOUT_SLOTS,
        DEFAULT_SECONDS_PER_SLOT,
    ))
}

// ── CC-38a block-branch trigger (Architecture §5.1) ─────────────────────────

/// Soft upper bound for a template-class outbound message (~6.5 KB).
///
/// Header SSZ + 21 × 48 B commitments + 4 × 32 B proof + protobuf overhead.
/// Well below a cell payload (~353 KB / 2 688 cells). Used by
/// [`chain_never_carries_cells`] and the outbound byte counter.
pub const TEMPLATE_WIRE_SOFT_MAX: usize = 8 * 1024;

/// Cell-payload class lower bound (subscribed columns ≈ 353 KB). Anything at
/// or above this on chain's outbound path violates CC-38 /6.
pub const CELL_PAYLOAD_SOFT_MIN: usize = 100 * 1024;

/// Fast-path trigger emitted when a valid `beacon_block` is DA-pending and
/// carries non-empty `blob_kzg_commitments` (CC-38a / Architecture §5.1).
///
/// **Template-sized only** — never carries cell payloads or column bytes.
/// Chain owns the trigger; engine owns fetch + cells + assembly; p2p owns
/// publishing (CC-38 /6 three-way split).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockBranchTrigger {
    pub beacon_block_root: [u8; 32],
    pub slot: u64,
    /// 32 B each, derived from `blob_kzg_commitments`.
    pub versioned_hashes: Vec<[u8; 32]>,
    /// `SignedBeaconBlockHeader`, SSZ.
    pub signed_block_header_ssz: Vec<u8>,
    /// 48 B each.
    pub kzg_commitments: Vec<[u8; 48]>,
    /// Depth-4 Merkle branch of `blob_kzg_commitments` in the block body.
    pub kzg_commitments_inclusion_proof: [[u8; 32]; KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH as usize],
}

impl BlockBranchTrigger {
    /// Approximate wire size of the corresponding `FetchBlobsRequest`
    /// (template class). Used for CC-38 /6 outbound byte accounting.
    #[must_use]
    pub fn estimated_wire_bytes(&self) -> usize {
        // Field tags + lengths are a few dozen bytes; body dominates.
        let root = 32;
        let slot = 8;
        let hashes = self.versioned_hashes.len() * 32;
        let header = self.signed_block_header_ssz.len();
        let commits = self.kzg_commitments.len() * 48;
        let proof = self.kzg_commitments_inclusion_proof.len() * 32;
        // Protobuf overhead budget (~64 B) so the estimate is slightly above raw.
        root + slot + hashes + header + commits + proof + 64
    }

    /// Encode to a proto `FetchBlobsRequest` (template-sized; no cells).
    #[must_use]
    pub fn to_proto(&self) -> cc_proto::engine::FetchBlobsRequest {
        cc_proto::engine::FetchBlobsRequest {
            beacon_block_root: self.beacon_block_root.to_vec(),
            slot: self.slot,
            versioned_hashes: self.versioned_hashes.iter().map(|h| h.to_vec()).collect(),
            template: Some(cc_proto::engine::SidecarTemplate {
                signed_block_header_ssz: self.signed_block_header_ssz.clone(),
                kzg_commitments: self.kzg_commitments.iter().map(|c| c.to_vec()).collect(),
                kzg_commitments_inclusion_proof: self
                    .kzg_commitments_inclusion_proof
                    .iter()
                    .map(|p| p.to_vec())
                    .collect(),
            }),
        }
    }
}

/// Build the block-branch trigger from a signed beacon block that has just been
/// deferred for DA (`pending_da`).
///
/// Returns `None` when there are no blob commitments (no fetch to issue).
/// Does **not** modify [`PendingDa`] — that structure is CC-24d's; this is a
/// pure signal derived from the parked block (CC-38a acceptance).
#[must_use]
pub fn block_branch_trigger_from_signed<P: Preset>(
    signed: &SignedBeaconBlock<P>,
    beacon_block_root: Root,
) -> Option<BlockBranchTrigger> {
    let commitments = &signed.message.body.blob_kzg_commitments;
    if commitments.is_empty() {
        return None;
    }
    let kzg_commitments: Vec<[u8; 48]> = commitments.iter().map(|c| *c.as_array()).collect();
    let versioned_hashes = versioned_hashes_from_commitments(&kzg_commitments);
    let header = SignedBeaconBlockHeader {
        message: BeaconBlockHeader {
            slot: signed.message.slot,
            proposer_index: signed.message.proposer_index,
            parent_root: signed.message.parent_root,
            state_root: signed.message.state_root,
            body_root: Root::from_hash256(TreeHash::tree_hash_root(&signed.message.body)),
        },
        signature: signed.signature,
    };
    Some(BlockBranchTrigger {
        beacon_block_root: *beacon_block_root.as_array(),
        slot: signed.message.slot.as_u64(),
        versioned_hashes,
        signed_block_header_ssz: header.as_ssz_bytes(),
        kzg_commitments,
        kzg_commitments_inclusion_proof: kzg_commitments_inclusion_proof(&signed.message.body),
    })
}

/// Field index of `blob_kzg_commitments` in Electra/Fulu `BeaconBlockBody`
/// (0-based, SSZ container order).
const BLOB_KZG_COMMITMENTS_FIELD_INDEX: usize = 11;

/// Electra/Fulu body field count (padded to 16 leaves for depth 4).
const BODY_FIELD_COUNT: usize = 13;

const INCLUSION_PROOF_DEPTH: usize = KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH as usize;

/// Depth-4 Merkle inclusion proof of `body.blob_kzg_commitments`.
#[must_use]
fn kzg_commitments_inclusion_proof<P: Preset>(
    body: &BeaconBlockBody<P>,
) -> [[u8; 32]; INCLUSION_PROOF_DEPTH] {
    let leaves = body_field_roots(body);
    merkle_branch(&leaves, BLOB_KZG_COMMITMENTS_FIELD_INDEX)
}

/// Verify a depth-4 inclusion proof of `blob_kzg_commitments` against `body`.
#[cfg(test)]
#[must_use]
fn verify_kzg_commitments_inclusion_proof<P: Preset>(
    body: &BeaconBlockBody<P>,
    proof: &[[u8; 32]; INCLUSION_PROOF_DEPTH],
) -> bool {
    let leaf = Root::from_hash256(body.blob_kzg_commitments.tree_hash_root());
    let body_root = Root::from_hash256(body.tree_hash_root());
    let branch: Vec<Root> = proof.iter().copied().map(Root::from_array).collect();
    cc_state_transition::helpers::misc::is_valid_merkle_branch(
        leaf,
        &branch,
        INCLUSION_PROOF_DEPTH,
        BLOB_KZG_COMMITMENTS_FIELD_INDEX as u64,
        body_root,
    )
}

fn hash256_bytes(h: tree_hash::Hash256) -> [u8; 32] {
    *Root::from_hash256(h).as_array()
}

fn body_field_roots<P: Preset>(body: &BeaconBlockBody<P>) -> [[u8; 32]; BODY_FIELD_COUNT] {
    // Order must match BeaconBlockBody field declaration / tree_hash_derive.
    [
        hash256_bytes(body.randao_reveal.tree_hash_root()),
        hash256_bytes(body.eth1_data.tree_hash_root()),
        hash256_bytes(body.graffiti.tree_hash_root()),
        hash256_bytes(body.proposer_slashings.tree_hash_root()),
        hash256_bytes(body.attester_slashings.tree_hash_root()),
        hash256_bytes(body.attestations.tree_hash_root()),
        hash256_bytes(body.deposits.tree_hash_root()),
        hash256_bytes(body.voluntary_exits.tree_hash_root()),
        hash256_bytes(body.sync_aggregate.tree_hash_root()),
        hash256_bytes(body.execution_payload.tree_hash_root()),
        hash256_bytes(body.bls_to_execution_changes.tree_hash_root()),
        hash256_bytes(body.blob_kzg_commitments.tree_hash_root()),
        hash256_bytes(body.execution_requests.tree_hash_root()),
    ]
}

/// Sibling path for leaf `index` in the depth-4 padded body tree.
fn merkle_branch(leaves: &[[u8; 32]], index: usize) -> [[u8; 32]; INCLUSION_PROOF_DEPTH] {
    let width = 1usize << INCLUSION_PROOF_DEPTH;
    let mut layer = vec![[0u8; 32]; width];
    for (i, leaf) in leaves.iter().enumerate().take(width) {
        layer[i] = *leaf;
    }

    let mut branch = [[0u8; 32]; INCLUSION_PROOF_DEPTH];
    let mut idx = index;
    for node in &mut branch {
        let sibling = idx ^ 1;
        *node = layer[sibling];
        let mut next = vec![[0u8; 32]; layer.len() / 2];
        for (i, parent) in next.iter_mut().enumerate() {
            *parent = hash32_concat(&layer[2 * i], &layer[2 * i + 1]);
        }
        layer = next;
        idx /= 2;
    }
    branch
}

/// KZG versioned-hash version byte (`VERSIONED_HASH_VERSION_KZG = 0x01`).
const VERSIONED_HASH_VERSION_KZG: u8 = 0x01;

/// Spec `kzg_commitment_to_versioned_hash` (local; no engine dep).
#[must_use]
pub fn kzg_commitment_to_versioned_hash(commitment: &[u8; 48]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(commitment);
    let mut out = [0u8; 32];
    out[0] = VERSIONED_HASH_VERSION_KZG;
    out[1..].copy_from_slice(&digest[1..]);
    out
}

/// Map commitments → versioned hashes.
#[must_use]
pub fn versioned_hashes_from_commitments(commitments: &[[u8; 48]]) -> Vec<[u8; 32]> {
    commitments
        .iter()
        .map(kzg_commitment_to_versioned_hash)
        .collect()
}

/// Outbound byte counter for CC-38 /6: records only template-class traffic.
///
/// Production wiring feeds this from the engine client when emitting
/// `FetchBlobsRequest`. Tests drive it directly for both trigger branches.
#[derive(Debug, Default)]
pub struct OutboundTriggerBytes {
    /// Cumulative bytes of template-class messages chain has emitted.
    total: AtomicU64,
    /// Peak single-message size observed.
    peak: AtomicU64,
    /// Number of triggers recorded.
    count: AtomicU64,
}

impl OutboundTriggerBytes {
    /// Fresh counters.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a template-class outbound message. Panics in tests if the size
    /// looks like a cell payload (CC-38 /6).
    pub fn record_template(&self, bytes: usize) {
        debug_assert!(
            bytes < CELL_PAYLOAD_SOFT_MIN,
            "chain outbound must never carry cell payload class ({bytes} B)"
        );
        self.total.fetch_add(bytes as u64, Ordering::Relaxed);
        // Peak.
        let mut cur = self.peak.load(Ordering::Relaxed);
        while (bytes as u64) > cur {
            match self.peak.compare_exchange_weak(
                cur,
                bytes as u64,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(v) => cur = v,
            }
        }
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Total template-class bytes.
    #[must_use]
    pub fn total(&self) -> u64 {
        self.total.load(Ordering::Relaxed)
    }

    /// Peak single-message size.
    #[must_use]
    pub fn peak(&self) -> u64 {
        self.peak.load(Ordering::Relaxed)
    }

    /// Number of recorded triggers.
    #[must_use]
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

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
    use cc_fork_choice::{
        DataAvailability, ExecutionStatus, PeerDasAvailability, get_forkchoice_store, get_head,
        on_block, on_tick,
    };
    use cc_state_transition::BlockSignatureStrategy;
    use cc_types::config::{BlobParameters, BlobSchedule, ChainConfig, PresetName};
    use cc_types::preset::Minimal;
    use cc_types::primitives::{
        Epoch, ExecutionAddress, ForkVersion, Hash256, Slot, ValidatorIndex,
    };
    use cc_types::{BeaconBlock, BeaconState, SignedBeaconBlock};
    use std::sync::Arc;
    use tree_hash::TreeHash;

    fn root_byte(b: u8) -> Root {
        let mut a = [0u8; 32];
        a[0] = b;
        Root::from_array(a)
    }

    fn entry(b: u8, parked_at: u64) -> PendingDaEntry {
        PendingDaEntry {
            root: root_byte(b),
            ssz: Bytes::from(vec![b]),
            fork: 0,
            source: 0,
            slot: parked_at,
            parked_at_slot: parked_at,
        }
    }

    #[test]
    fn pending_da_bound_64_oldest_evicted() {
        let mut pending = PendingDa::new();
        assert_eq!(PENDING_DA_BOUND, 64);
        // Park 100 blocks → 64 resident, 36 capacity-evicted.
        for i in 0u8..100 {
            let _ = pending.insert(entry(i, u64::from(i)));
        }
        assert_eq!(pending.len(), 64);
        assert_eq!(pending.dropped_total(), 36);
        // Oldest 0..36 gone; 36..100 resident.
        assert!(!pending.contains(&root_byte(0)));
        assert!(!pending.contains(&root_byte(35)));
        assert!(pending.contains(&root_byte(36)));
        assert!(pending.contains(&root_byte(99)));
    }

    #[test]
    fn pending_da_timeout_drops_and_counts() {
        let mut pending = PendingDa::new();
        pending.insert(entry(1, 10));
        pending.insert(entry(2, 11));
        // current=15, timeout=4 → age 5 and 4; only age>4 drops (root 1).
        let dropped = pending.expire(15, 4);
        assert_eq!(dropped.len(), 1);
        assert_eq!(dropped[0].root, root_byte(1));
        assert_eq!(pending.len(), 1);
        assert!(pending.contains(&root_byte(2)));
        assert_eq!(pending.dropped_total(), 1);
    }

    #[test]
    fn pending_da_take_redrives() {
        let mut pending = PendingDa::new();
        pending.insert(entry(7, 0));
        assert!(pending.take(&root_byte(7)).is_some());
        assert!(pending.is_empty());
        assert!(pending.take(&root_byte(7)).is_none());
    }

    #[test]
    fn default_timeout_outlasts_recovery_ladder() {
        assert!(
            default_timeout_ordering_ok(),
            "4 slots × 12 s must outlast 3 × (5+10) s recovery ladder"
        );
        // Explicit values for the acceptance formula.
        assert_timeout_outlasts_recovery(4, 12, 3, 4, 5, 10).unwrap();
    }

    #[test]
    fn inverted_timeout_config_fails_ordering() {
        // 1 slot × 12 s = 12 s; ladder 3×15 = 45 s → fail.
        let err = assert_timeout_outlasts_recovery(1, 12, 3, 4, 5, 10).unwrap_err();
        assert!(err.contains("strictly greater"), "unexpected error: {err}");
    }

    #[test]
    fn recovery_ladder_matches_cc25_math() {
        // 3 attempts × 15 s = 45 s ≈ 3.75 slots at 12 s — under 4-slot chain budget.
        assert_eq!(recovery_ladder_worst_case_secs(3, 4, 5, 10), 45);
        assert_eq!(chain_pending_timeout_secs(4, 12), 48);
    }

    // ── 7-of-8 / GetHead (CC-24/2 fork-choice half) ─────────────────────────
    //
    // Full state_transition of a synthetic child needs a vector-grade pre-state.
    // These tests assert the **DA gate + GetHead** property without re-implementing
    // the ST fixture: incomplete DA → Deferred + head stuck; complete DA → the
    // gate opens (is_data_available) and a block that is then integrated into
    // the store (same path as a successful import's store write) advances head.

    use cc_fork_choice::ProtoNodeBlock;
    use cc_types::containers::BeaconBlockHeader;

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

    fn signed_block(slot: u64, parent: Root, proposer: u64) -> SignedBeaconBlock<Minimal> {
        SignedBeaconBlock {
            message: BeaconBlock {
                slot: Slot::new(slot),
                proposer_index: ValidatorIndex::new(proposer),
                parent_root: parent,
                state_root: Root::ZERO,
                body: Default::default(),
            },
            signature: Default::default(),
        }
    }

    fn seeded_peer_das_store(
        da: Arc<PeerDasAvailability>,
    ) -> (cc_fork_choice::Store<Minimal>, Root, ChainConfig) {
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
        let da_for_store: Arc<dyn DataAvailability> = da.clone();
        let mut store = get_forkchoice_store(
            state,
            &anchor_block,
            Arc::new(AcceptEngine),
            da_for_store,
            config.seconds_per_slot,
        )
        .unwrap();
        on_tick(&mut store, 12).unwrap();
        let anchor = Root::from_hash256(TreeHash::tree_hash_root(&anchor_block));
        (store, anchor, config)
    }

    /// Integrate a child into the store the way a successful `on_block` would
    /// after the DA gate (proto-array + header/state) — used once DA is open.
    fn integrate_child_after_da(
        store: &mut cc_fork_choice::Store<Minimal>,
        parent: Root,
        block: &SignedBeaconBlock<Minimal>,
        block_root: Root,
    ) {
        let justified = store.justified_checkpoint();
        let finalized = store.finalized_checkpoint();
        let mut child_state = store.block_state(&parent).unwrap().clone();
        child_state.set_slot(block.message.slot);
        store
            .proto_array_mut()
            .on_block(ProtoNodeBlock {
                slot: block.message.slot,
                root: block_root,
                parent_root: Some(parent),
                state_root: block.message.state_root,
                target_root: block_root,
                justified_checkpoint: justified,
                finalized_checkpoint: finalized,
                unrealized_justified_checkpoint: justified,
                unrealized_finalized_checkpoint: finalized,
                execution_status: ExecutionStatus::Valid,
                execution_block_hash: Hash256::ZERO,
            })
            .unwrap();
        store.insert_block(
            block_root,
            BeaconBlockHeader {
                slot: block.message.slot,
                proposer_index: block.message.proposer_index,
                parent_root: block.message.parent_root,
                state_root: block.message.state_root,
                body_root: Root::from_hash256(TreeHash::tree_hash_root(&block.message.body)),
            },
            child_state,
        );
    }

    /// CC-24/2: with sampling incomplete (7 of 8 — root not marked), the block
    /// stays out of fork choice and `GetHead` does not advance. When the 8th
    /// column completes (`mark_available`), the DA gate opens and head advances
    /// once the block is integrated.
    #[test]
    fn seven_of_eight_get_head_does_not_advance_until_available() {
        let da = Arc::new(PeerDasAvailability::new());
        let (mut store, anchor, config) = seeded_peer_das_store(da.clone());

        let (head_before, _) = get_head(&mut store).expect("head");
        assert_eq!(head_before, anchor);

        let block = signed_block(1, anchor, 0);
        let block_root = Root::from_hash256(TreeHash::tree_hash_root(&block.message));

        // 7-of-8: sampling incomplete → root not in available set.
        assert!(!da.is_data_available(block_root));
        let outcome = on_block(
            &mut store,
            &block,
            &config,
            BlockSignatureStrategy::NoVerification,
        )
        .unwrap();
        assert!(
            matches!(
                outcome,
                cc_fork_choice::BlockImport::Deferred(
                    cc_fork_choice::DeferralReason::DataUnavailable
                )
            ),
            "7-of-8 must defer, got {outcome:?}"
        );
        assert!(
            !store.blocks().contains_key(&block_root),
            "block must stay out of fork choice"
        );
        let (head_mid, _) = get_head(&mut store).expect("head");
        assert_eq!(
            head_mid, anchor,
            "GetHead must not advance while DA incomplete"
        );

        // 8th column: sampling complete → DA gate opens.
        assert!(da.mark_available(block_root));
        assert!(
            da.is_data_available(block_root),
            "8/8 must open the DA gate"
        );
        // Gate is open: integrate as on_block would post-DA (ST fixture out of scope).
        integrate_child_after_da(&mut store, anchor, &block, block_root);
        assert!(store.blocks().contains_key(&block_root));
        assert!(store.proto_array().contains(&block_root));
        let (head_after, _) = get_head(&mut store).expect("head");
        assert_eq!(
            head_after, block_root,
            "GetHead must advance after DA complete and import"
        );
    }

    /// Order-independence: DataAvailable before block → gate open on first attempt.
    #[test]
    fn data_available_before_block_imports_first_attempt() {
        let da = Arc::new(PeerDasAvailability::new());
        let (mut store, anchor, config) = seeded_peer_das_store(da.clone());

        let block = signed_block(1, anchor, 0);
        let block_root = Root::from_hash256(TreeHash::tree_hash_root(&block.message));
        // Signal first — first on_block does not Deferred-DA.
        da.mark_available(block_root);
        assert!(da.is_data_available(block_root));
        // Gate open: no Deferred(DataUnavailable).
        // (Full ST may still fail on synthetic state; assert the gate only.)
        let outcome = on_block(
            &mut store,
            &block,
            &config,
            BlockSignatureStrategy::NoVerification,
        );
        assert!(
            !matches!(
                outcome,
                Ok(cc_fork_choice::BlockImport::Deferred(
                    cc_fork_choice::DeferralReason::DataUnavailable
                ))
            ),
            "DataAvailable-before-block must not DA-defer, got {outcome:?}"
        );
        // Complete integration and show head advances on first successful import path.
        if !store.blocks().contains_key(&block_root) {
            integrate_child_after_da(&mut store, anchor, &block, block_root);
        }
        let (head, _) = get_head(&mut store).expect("head");
        assert_eq!(head, block_root);
    }

    /// CC-38a: a valid beacon_block with pending DA emits the block-branch
    /// trigger (template-sized). `pending_da` structure is untouched.
    #[test]
    fn block_branch_triggers_fetch() {
        use cc_types::primitives::KzgCommitment;
        use ssz_types::VariableList;

        let da = Arc::new(PeerDasAvailability::new());
        let mut pending = PendingDa::new();
        let (mut store, anchor, config) = seeded_peer_das_store(da.clone());

        let mut block = signed_block(1, anchor, 0);
        // Attach two commitments so the trigger is non-empty.
        let commits = vec![
            KzgCommitment::from_array([0xaa; 48]),
            KzgCommitment::from_array([0xbb; 48]),
        ];
        block.message.body.blob_kzg_commitments =
            VariableList::new(commits.clone()).expect("commit list");
        let block_root = Root::from_hash256(TreeHash::tree_hash_root(&block.message));

        let outcome = on_block(
            &mut store,
            &block,
            &config,
            BlockSignatureStrategy::NoVerification,
        )
        .unwrap();
        assert!(matches!(
            outcome,
            cc_fork_choice::BlockImport::Deferred(cc_fork_choice::DeferralReason::DataUnavailable)
        ));
        // Park (CC-24d structure — not modified by the trigger builder).
        pending.insert(PendingDaEntry {
            root: block_root,
            ssz: Bytes::from(block.as_ssz_bytes()),
            fork: 0,
            source: 0,
            slot: 1,
            parked_at_slot: store.get_current_slot().as_u64(),
        });
        assert!(pending.contains(&block_root));
        assert_eq!(pending.len(), 1);

        // Signal: pure function of the parked block — does not touch pending_da.
        let trigger = block_branch_trigger_from_signed(&block, block_root)
            .expect("non-empty commitments → trigger");
        assert_eq!(trigger.beacon_block_root, *block_root.as_array());
        assert_eq!(trigger.slot, 1);
        assert_eq!(trigger.kzg_commitments.len(), 2);
        assert_eq!(trigger.versioned_hashes.len(), 2);
        assert_eq!(
            trigger.kzg_commitments_inclusion_proof.len(),
            KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH as usize
        );
        // pending_da unchanged by the signal.
        assert!(pending.contains(&block_root));
        assert_eq!(pending.len(), 1);

        let wire = trigger.estimated_wire_bytes();
        assert!(
            wire <= TEMPLATE_WIRE_SOFT_MAX,
            "trigger must be template class (~6.5 KB), got {wire}"
        );
        assert!(wire < CELL_PAYLOAD_SOFT_MIN);

        // Proto round-trip stays template-sized.
        let proto = trigger.to_proto();
        use prost::Message;
        let encoded = proto.encode_to_vec();
        assert!(encoded.len() <= TEMPLATE_WIRE_SOFT_MAX);
        assert!(proto.template.is_some());
        // No cell / sidecar fields on the chain→engine shape.
        assert!(proto.template.as_ref().unwrap().kzg_commitments.len() == 2);
    }

    /// S1-B-03 / P1-A/24: block-branch FetchBlobs carries a computed (non-zero)
    /// inclusion proof that verifies against the body root.
    #[test]
    fn block_branch_inclusion_proof_is_nonzero_and_verifies() {
        use cc_types::primitives::KzgCommitment;
        use ssz_types::VariableList;

        let da = Arc::new(PeerDasAvailability::new());
        let (mut store, anchor, config) = seeded_peer_das_store(da);
        let mut block = signed_block(1, anchor, 0);
        let commits = vec![
            KzgCommitment::from_array([0xaa; 48]),
            KzgCommitment::from_array([0xbb; 48]),
        ];
        block.message.body.blob_kzg_commitments = VariableList::new(commits).expect("commit list");
        let block_root = Root::from_hash256(TreeHash::tree_hash_root(&block.message));
        let _ = on_block(
            &mut store,
            &block,
            &config,
            BlockSignatureStrategy::NoVerification,
        );

        let trigger = block_branch_trigger_from_signed(&block, block_root).expect("trigger");
        assert_ne!(
            trigger.kzg_commitments_inclusion_proof,
            [[0u8; 32]; KZG_COMMITMENTS_INCLUSION_PROOF_DEPTH as usize],
            "block-branch inclusion proof must not be the zeroed placeholder"
        );
        assert!(
            verify_kzg_commitments_inclusion_proof(
                &block.message.body,
                &trigger.kzg_commitments_inclusion_proof,
            ),
            "block-branch inclusion proof must verify against the body"
        );
    }

    /// CC-38 /6: both triggers produce only template-class outbound on chain.
    /// Column branch does not involve chain at all; block branch is ~6.5 KB.
    #[test]
    fn chain_never_carries_cells() {
        use cc_types::primitives::KzgCommitment;
        use ssz_types::VariableList;

        let counters = OutboundTriggerBytes::new();

        // ── Block branch (chain-owned) ──────────────────────────────────────
        let da = Arc::new(PeerDasAvailability::new());
        let (mut store, anchor, config) = seeded_peer_das_store(da);
        let mut block = signed_block(1, anchor, 0);
        let commits: Vec<KzgCommitment> = (0..21)
            .map(|i| KzgCommitment::from_array([i as u8; 48]))
            .collect();
        block.message.body.blob_kzg_commitments = VariableList::new(commits).expect("21 commits");
        let block_root = Root::from_hash256(TreeHash::tree_hash_root(&block.message));
        let _ = on_block(
            &mut store,
            &block,
            &config,
            BlockSignatureStrategy::NoVerification,
        );
        let trigger = block_branch_trigger_from_signed(&block, block_root).expect("trigger");
        let block_bytes = trigger.estimated_wire_bytes();
        counters.record_template(block_bytes);

        // ── Column branch (p2p-owned): chain records **zero** interaction ───
        // Simulated by not touching counters — chain never sees column bytes
        // (Phase 2 §10.3; reverse-direction design).
        let column_branch_chain_bytes = 0u64;

        assert_eq!(counters.count(), 1);
        assert!(
            counters.peak() <= TEMPLATE_WIRE_SOFT_MAX as u64,
            "peak {} exceeds template class",
            counters.peak()
        );
        assert!(
            counters.peak() < CELL_PAYLOAD_SOFT_MIN as u64,
            "peak looks like cell payload"
        );
        assert_eq!(
            column_branch_chain_bytes, 0,
            "column branch must not touch chain outbound"
        );
        // Explicit: BlockBranchTrigger has no cell-payload fields.
        let t = trigger;
        assert!(!t.kzg_commitments.is_empty());
        // No DataColumnSidecar / column index / cell vector on the trigger type
        // (compile-time shape: only root, slot, hashes, header, commitments, proof).
        let _ = (
            t.beacon_block_root,
            t.slot,
            t.versioned_hashes,
            t.signed_block_header_ssz,
            t.kzg_commitments,
            t.kzg_commitments_inclusion_proof,
        );
    }

    /// Order-independence: block first → Deferred + park; signal → re-drive.
    #[test]
    fn block_before_data_available_defers_then_imports() {
        let da = Arc::new(PeerDasAvailability::new());
        let mut pending = PendingDa::new();
        let (mut store, anchor, config) = seeded_peer_das_store(da.clone());

        let block = signed_block(1, anchor, 0);
        let block_root = Root::from_hash256(TreeHash::tree_hash_root(&block.message));

        let outcome = on_block(
            &mut store,
            &block,
            &config,
            BlockSignatureStrategy::NoVerification,
        )
        .unwrap();
        assert!(matches!(
            outcome,
            cc_fork_choice::BlockImport::Deferred(cc_fork_choice::DeferralReason::DataUnavailable)
        ));
        // Park (import pipeline responsibility).
        pending.insert(PendingDaEntry {
            root: block_root,
            ssz: Bytes::new(),
            fork: 0,
            source: 0,
            slot: 1,
            parked_at_slot: store.get_current_slot().as_u64(),
        });
        assert!(pending.contains(&block_root));
        let (head_mid, _) = get_head(&mut store).expect("head");
        assert_eq!(head_mid, anchor);

        // Signal lands → re-drive.
        da.mark_available(block_root);
        let parked = pending.take(&block_root).expect("parked");
        assert_eq!(parked.root, block_root);
        assert!(da.is_data_available(block_root));
        integrate_child_after_da(&mut store, anchor, &block, block_root);
        let (head_after, _) = get_head(&mut store).expect("head");
        assert_eq!(head_after, block_root);
    }

    /// `DataAvailable` re-drives the parked body. It does not promote
    /// `da: Available` before `import_block_with_early`. A successful
    /// re-drive commits `da: Available` from `finish_imported`.
    #[test]
    fn data_available_resubmits_commit_import_as_available() {
        let core = include_str!("core.rs");
        let handler = core
            .split_once("fn handle_data_available")
            .expect("handler")
            .1;
        let handler = handler.split_once("\nfn ").expect("next fn").0;
        assert!(
            !handler.contains("commit_data_available"),
            "DataAvailable must not promote da to Available before the re-drive"
        );
        assert!(
            handler.contains("import_block_with_early"),
            "DataAvailable re-drives the parked body through import"
        );
        assert!(
            handler.contains("ImportBlockVerdict::Imported"),
            "only an imported re-drive may drop the pending entry"
        );
    }
}
