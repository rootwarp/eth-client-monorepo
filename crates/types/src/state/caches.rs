//! Cached state hashing: `ListHashCache`, `FieldRootCache`, and `StateCaches` (Architecture §3.4).
//!
//! Invalidation rules:
//! - List hash caches: mark dirty leaf indices on element mutation; grow/rebuild on length change.
//! - Field roots: mark the corresponding top-level leaf dirty on any field mutation.
//! - `ShufflingCache` / `EpochCache`: structs only here; filled/invalidated by `cc-state-transition`
//!   at CC-13a.
//! - `PubkeyIndexMap`: append-only. Owned by `TransitionContext` (S2-A-10 / P0-19/3),
//!   not [`StateCaches`]. Extended by deposit handlers (CC-12c) via the context.

use std::collections::HashMap;
use std::fmt;

use fixedbitset::FixedBitSet;
use tree_hash::{Hash256, TreeHash, mix_in_length};

use crate::primitives::{BlsPublicKey, Epoch, Root, ValidatorIndex};

/// Number of SSZ tree-hash leaves on [`crate::state::BeaconState`] (Fulu, excluding caches).
pub const BEACON_STATE_FIELD_COUNT: usize = 38;

/// Indices into [`StateCaches::list_hashes`].
pub mod list_id {
    /// `validators`
    pub const VALIDATORS: usize = 0;
    /// `balances`
    pub const BALANCES: usize = 1;
    /// `previous_epoch_participation`
    pub const PREV_PARTICIPATION: usize = 2;
    /// `current_epoch_participation`
    pub const CURR_PARTICIPATION: usize = 3;
    /// `inactivity_scores`
    pub const INACTIVITY_SCORES: usize = 4;
    /// Number of list hash caches.
    pub const COUNT: usize = 5;
}

/// Top-level `BeaconState` field indices (tree-hash leaf order).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StateField {
    GenesisTime = 0,
    GenesisValidatorsRoot = 1,
    Slot = 2,
    Fork = 3,
    LatestBlockHeader = 4,
    BlockRoots = 5,
    StateRoots = 6,
    HistoricalRoots = 7,
    Eth1Data = 8,
    Eth1DataVotes = 9,
    Eth1DepositIndex = 10,
    Validators = 11,
    Balances = 12,
    RandaoMixes = 13,
    Slashings = 14,
    PreviousEpochParticipation = 15,
    CurrentEpochParticipation = 16,
    JustificationBits = 17,
    PreviousJustifiedCheckpoint = 18,
    CurrentJustifiedCheckpoint = 19,
    FinalizedCheckpoint = 20,
    InactivityScores = 21,
    CurrentSyncCommittee = 22,
    NextSyncCommittee = 23,
    LatestExecutionPayloadHeader = 24,
    NextWithdrawalIndex = 25,
    NextWithdrawalValidatorIndex = 26,
    HistoricalSummaries = 27,
    DepositRequestsStartIndex = 28,
    DepositBalanceToConsume = 29,
    ExitBalanceToConsume = 30,
    EarliestExitEpoch = 31,
    ConsolidationBalanceToConsume = 32,
    EarliestConsolidationEpoch = 33,
    PendingDeposits = 34,
    PendingPartialWithdrawals = 35,
    PendingConsolidations = 36,
    ProposerLookahead = 37,
}

impl StateField {
    /// Leaf index in the container merkle tree.
    pub const fn index(self) -> usize {
        self as usize
    }
}

/// Flat merkle arena for one large list (Architecture §3.4).
///
/// Layout: breadth-first, leaf layer first. Only a compact power-of-two tree covering the
/// current chunk count is stored; the SSZ limit is applied by hashing the compact root
/// against `ZERO_HASHES` up to the full limit depth, then `mix_in_length`.
#[derive(Clone)]
pub struct ListHashCache {
    /// Contiguous layers: leaves at `[0, leaf_pow2)`, then parents.
    nodes: Vec<Hash256>,
    /// Power-of-two leaf capacity currently allocated (`nodes` leaf layer length).
    leaf_pow2: usize,
    /// SSZ merkle leaf limit (already divided by packing factor); power of two for mainnet.
    limit_leaves: usize,
    /// Elements packed into one 32-byte leaf (`1` for containers).
    packing_factor: usize,
    /// Current list length (element count).
    length: usize,
    /// Dirty leaf indices (chunk indices, not element indices).
    dirty: FixedBitSet,
    /// Cached root including `mix_in_length`, valid when `!dirty_any && initialized`.
    cached_root: Option<Hash256>,
    /// Whether leaf layer has been fully populated at least once.
    initialized: bool,
    /// Tree power-of-two capacity changed; parents must be rebuilt in full.
    shape_dirty: bool,
}

impl fmt::Debug for ListHashCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ListHashCache")
            .field("leaf_pow2", &self.leaf_pow2)
            .field("limit_leaves", &self.limit_leaves)
            .field("packing_factor", &self.packing_factor)
            .field("length", &self.length)
            .field("initialized", &self.initialized)
            .field("cached_root", &self.cached_root)
            .finish_non_exhaustive()
    }
}

impl ListHashCache {
    /// Create a cold cache for a list with SSZ max length `max_len` and `packing_factor`.
    pub fn new(max_len: usize, packing_factor: usize) -> Self {
        assert!(packing_factor > 0, "packing_factor must be > 0");
        let limit_leaves = max_len.div_ceil(packing_factor).next_power_of_two().max(1);
        Self {
            nodes: Vec::new(),
            leaf_pow2: 0,
            limit_leaves,
            packing_factor,
            length: 0,
            dirty: FixedBitSet::new(),
            cached_root: None,
            initialized: false,
            shape_dirty: false,
        }
    }

    /// Packing factor for this cache.
    pub fn packing_factor(&self) -> usize {
        self.packing_factor
    }

    /// Whether any leaf is dirty or the cache was never filled.
    pub fn needs_recompute(&self) -> bool {
        !self.initialized || self.cached_root.is_none() || self.dirty.count_ones(..) > 0
    }

    /// Map an element index to its merkle leaf (chunk) index.
    pub fn leaf_index_for_element(&self, elem_idx: usize) -> usize {
        elem_idx / self.packing_factor
    }

    /// Mark the leaf covering `elem_idx` dirty.
    pub fn mark_element_dirty(&mut self, elem_idx: usize) {
        let leaf = self.leaf_index_for_element(elem_idx);
        self.ensure_dirty_capacity(leaf + 1);
        self.dirty.insert(leaf);
        self.cached_root = None;
    }

    /// Record a new list length (e.g. after `push`). Marks the new element's leaf dirty.
    pub fn note_length(&mut self, new_len: usize) {
        if new_len < self.length {
            // Truncation: full rebuild is simplest and rare for these lists.
            self.initialized = false;
            self.cached_root = None;
            self.dirty.clear();
            self.length = new_len;
            return;
        }
        if new_len > self.length {
            for elem in self.length..new_len {
                self.mark_element_dirty(elem);
            }
            self.length = new_len;
            self.cached_root = None;
        }
    }

    /// Force a full rebuild on next recompute.
    pub fn invalidate_all(&mut self) {
        self.initialized = false;
        self.cached_root = None;
        self.shape_dirty = false;
        self.dirty.clear();
    }

    fn ensure_dirty_capacity(&mut self, bits: usize) {
        if self.dirty.len() < bits {
            self.dirty.grow(bits);
        }
    }

    fn ensure_tree_capacity(&mut self, chunk_count: usize) {
        let need = chunk_count.next_power_of_two().max(1);
        if need <= self.leaf_pow2 && !self.nodes.is_empty() {
            return;
        }
        // Grow structure. Parent layout changes with leaf_pow2, so after copy we must
        // rebuild all parents (shape_dirty) — incremental path indices are wrong.
        let old_leaf_pow2 = self.leaf_pow2;
        let old_nodes = std::mem::take(&mut self.nodes);
        self.leaf_pow2 = need;
        let total = total_nodes(need);
        self.nodes = vec![Hash256::ZERO; total];
        if old_leaf_pow2 > 0 && !old_nodes.is_empty() {
            let copy = old_leaf_pow2.min(need);
            self.nodes[..copy].copy_from_slice(&old_nodes[..copy]);
        }
        self.ensure_dirty_capacity(need);
        if !self.initialized {
            for i in 0..need {
                self.dirty.insert(i);
            }
        } else {
            for i in old_leaf_pow2..need {
                self.dirty.insert(i);
            }
            self.shape_dirty = true;
        }
        self.cached_root = None;
    }

    /// Recompute the list root.
    ///
    /// `leaf_hash(leaf_idx)` must return the 32-byte merkle leaf for that chunk index.
    /// `length` is the current element count (for `mix_in_length`).
    pub fn recompute_with<F>(&mut self, length: usize, mut leaf_hash: F) -> Hash256
    where
        F: FnMut(usize) -> Hash256,
    {
        self.length = length;
        if let Some(root) = self.cached_root
            && self.initialized
            && self.dirty.count_ones(..) == 0
        {
            return root;
        }

        let chunk_count = if length == 0 {
            0
        } else {
            length.div_ceil(self.packing_factor)
        };
        self.ensure_tree_capacity(chunk_count.max(1));

        if !self.initialized {
            // Fill every leaf that may be non-zero; remainder stay ZERO.
            for i in 0..chunk_count {
                self.nodes[i] = leaf_hash(i);
            }
            for i in chunk_count..self.leaf_pow2 {
                self.nodes[i] = Hash256::ZERO;
            }
            self.build_all_parents();
            self.dirty.clear();
            self.initialized = true;
            self.shape_dirty = false;
        } else {
            // Update dirty leaves that fall within the current chunk span.
            let dirty_idx: Vec<usize> = self.dirty.ones().collect();
            for leaf in dirty_idx {
                if leaf < chunk_count {
                    self.nodes[leaf] = leaf_hash(leaf);
                } else if leaf < self.leaf_pow2 {
                    self.nodes[leaf] = Hash256::ZERO;
                }
            }
            if self.shape_dirty {
                // Power-of-two capacity changed: parent index map is new — rebuild all.
                self.build_all_parents();
                self.shape_dirty = false;
            } else {
                self.propagate_dirty();
            }
            self.dirty.clear();
        }

        let compact_root = self.compact_root();
        let limit_h = log2_exact(self.limit_leaves);
        let compact_h = log2_exact(self.leaf_pow2);
        let padded = pad_root_to_limit(compact_root, compact_h, limit_h);
        let root = mix_in_length(&padded, length);
        self.cached_root = Some(root);
        root
    }

    fn compact_root(&self) -> Hash256 {
        if self.leaf_pow2 == 0 {
            return Hash256::ZERO;
        }
        // Root is the single node of the last layer.
        let total = total_nodes(self.leaf_pow2);
        self.nodes[total - 1]
    }

    fn build_all_parents(&mut self) {
        let mut level_size = self.leaf_pow2;
        let mut level_off = 0usize;
        while level_size > 1 {
            let parent_off = level_off + level_size;
            let parent_size = level_size / 2;
            for i in 0..parent_size {
                let left = self.nodes[level_off + 2 * i];
                let right = self.nodes[level_off + 2 * i + 1];
                self.nodes[parent_off + i] = hash_concat(left, right);
            }
            level_off = parent_off;
            level_size = parent_size;
        }
    }

    fn propagate_dirty(&mut self) {
        // Collect dirty leaves and walk parents level by level.
        let mut dirty_at_level: Vec<usize> =
            self.dirty.ones().filter(|&i| i < self.leaf_pow2).collect();
        dirty_at_level.sort_unstable();
        dirty_at_level.dedup();

        let mut level_size = self.leaf_pow2;
        let mut level_off = 0usize;

        while level_size > 1 {
            let parent_off = level_off + level_size;
            let parent_size = level_size / 2;
            let mut parent_dirty: Vec<usize> = Vec::new();
            for &idx in &dirty_at_level {
                let p = idx / 2;
                if parent_dirty.last().copied() != Some(p) {
                    parent_dirty.push(p);
                }
            }
            parent_dirty.sort_unstable();
            parent_dirty.dedup();
            for &p in &parent_dirty {
                if p >= parent_size {
                    continue;
                }
                let left = self.nodes[level_off + 2 * p];
                let right = self.nodes[level_off + 2 * p + 1];
                self.nodes[parent_off + p] = hash_concat(left, right);
            }
            dirty_at_level = parent_dirty;
            level_off = parent_off;
            level_size = parent_size;
        }
    }
}

fn total_nodes(leaf_pow2: usize) -> usize {
    // leaves + parents + ... + root = 2*leaf_pow2 - 1
    leaf_pow2.saturating_mul(2).saturating_sub(1)
}

fn log2_exact(pow2: usize) -> usize {
    assert!(pow2.is_power_of_two() && pow2 > 0);
    pow2.trailing_zeros() as usize
}

fn hash_concat(left: Hash256, right: Hash256) -> Hash256 {
    Hash256::from(ethereum_hashing::hash32_concat(
        left.as_slice(),
        right.as_slice(),
    ))
}

fn pad_root_to_limit(mut root: Hash256, compact_h: usize, limit_h: usize) -> Hash256 {
    // ZERO_HASHES[h] is the root of a 2^h-zero-leaf tree.
    // Sibling at height h is ZERO_HASHES[h].
    for h in compact_h..limit_h {
        let zero = ethereum_hashing::ZERO_HASHES[h];
        root = Hash256::from(ethereum_hashing::hash32_concat(root.as_slice(), &zero));
    }
    root
}

/// Pack basic-type elements into a single 32-byte merkle leaf.
pub(super) fn packed_basic_leaf<T: TreeHash>(
    items: &[T],
    packing_factor: usize,
    leaf_idx: usize,
) -> Hash256 {
    let start = leaf_idx.saturating_mul(packing_factor);
    let mut chunk = [0u8; 32];
    let mut offset = 0usize;
    for i in 0..packing_factor {
        let elem_idx = start + i;
        if elem_idx >= items.len() {
            break;
        }
        let packed = items[elem_idx].tree_hash_packed_encoding();
        let len = packed.len();
        debug_assert!(offset + len <= 32);
        chunk[offset..offset + len].copy_from_slice(&packed);
        offset += len;
    }
    Hash256::from(chunk)
}

/// Container / non-basic leaf: element `tree_hash_root`.
pub(super) fn container_leaf<T: TreeHash>(items: &[T], leaf_idx: usize) -> Hash256 {
    items
        .get(leaf_idx)
        .map(TreeHash::tree_hash_root)
        .unwrap_or(Hash256::ZERO)
}

/// Top-level field-root cache for `BeaconState`.
#[derive(Clone)]
pub struct FieldRootCache {
    /// Per-field tree-hash roots (leaf layer of the container).
    pub(crate) roots: [Hash256; BEACON_STATE_FIELD_COUNT],
    pub(crate) dirty: FixedBitSet,
    /// Cached container root when clean.
    pub(crate) cached_root: Option<Hash256>,
    pub(crate) initialized: bool,
}

impl fmt::Debug for FieldRootCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FieldRootCache")
            .field("initialized", &self.initialized)
            .field("dirty_count", &self.dirty.count_ones(..))
            .field("cached_root", &self.cached_root)
            .finish_non_exhaustive()
    }
}

impl Default for FieldRootCache {
    fn default() -> Self {
        let mut dirty = FixedBitSet::with_capacity(BEACON_STATE_FIELD_COUNT);
        for i in 0..BEACON_STATE_FIELD_COUNT {
            dirty.insert(i);
        }
        Self {
            roots: [Hash256::ZERO; BEACON_STATE_FIELD_COUNT],
            dirty,
            cached_root: None,
            initialized: false,
        }
    }
}

impl FieldRootCache {
    /// Mark one field leaf dirty.
    pub fn mark_dirty(&mut self, field: StateField) {
        self.dirty.insert(field.index());
        self.cached_root = None;
    }

    /// Mark every field dirty.
    pub fn mark_all_dirty(&mut self) {
        for i in 0..BEACON_STATE_FIELD_COUNT {
            self.dirty.insert(i);
        }
        self.cached_root = None;
        self.initialized = false;
    }

    /// Whether recompute is required.
    pub fn needs_recompute(&self) -> bool {
        !self.initialized || self.cached_root.is_none() || self.dirty.count_ones(..) > 0
    }

    /// Store a freshly computed field root and clear its dirty bit.
    pub fn set_field_root(&mut self, field: StateField, root: Hash256) {
        self.roots[field.index()] = root;
        self.dirty.set(field.index(), false);
    }

    /// Recompute the container root from current field roots (dirty bits must already be clear).
    pub fn container_root_from_leaves(&mut self) -> Hash256 {
        if let Some(root) = self.cached_root
            && self.dirty.count_ones(..) == 0
            && self.initialized
        {
            return root;
        }
        let mut hasher = tree_hash::MerkleHasher::with_leaves(BEACON_STATE_FIELD_COUNT);
        for root in &self.roots {
            // Field count is a compile-time constant matching BEACON_STATE_FIELD_COUNT.
            let _ = hasher.write(root.as_slice());
        }
        let root = hasher.finish().unwrap_or(Hash256::ZERO);
        self.cached_root = Some(root);
        self.initialized = true;
        root
    }

    /// Current dirty field indices.
    pub fn dirty_fields(&self) -> impl Iterator<Item = usize> + '_ {
        self.dirty.ones()
    }

    /// Whether field `i` is dirty (or never initialized).
    pub fn is_dirty(&self, i: usize) -> bool {
        !self.initialized || self.dirty.contains(i)
    }
}

/// Append-only pubkey → validator index map (filled by deposit processing).
#[derive(Clone, Default)]
pub struct PubkeyIndexMap {
    map: HashMap<BlsPublicKey, ValidatorIndex>,
    /// Validator slots walked by [`Self::import_from_registry`].
    ///
    /// Independent of [`Self::len`]: duplicate pubkeys and scan inserts from
    /// another registry make cardinality the wrong cursor. A shared context
    /// must not skip a later append, and a shorter sibling must not rewind.
    imported_len: usize,
    /// How many times a full-registry linear scan was performed as a map miss
    /// fallback. Used by CC-12d to assert `process_sync_aggregate` never scans.
    linear_scan_count: u64,
}

impl fmt::Debug for PubkeyIndexMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PubkeyIndexMap")
            .field("len", &self.map.len())
            .field("imported_len", &self.imported_len)
            .field("linear_scan_count", &self.linear_scan_count)
            .finish()
    }
}

impl PubkeyIndexMap {
    /// Lookup without recording a registry scan.
    pub fn get(&self, key: &BlsPublicKey) -> Option<ValidatorIndex> {
        self.map.get(key).copied()
    }

    /// Insert (append-only contract; overwrites only if re-inserted deliberately).
    pub fn insert(&mut self, key: BlsPublicKey, index: ValidatorIndex) {
        self.map.insert(key, index);
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Slots walked by [`Self::import_from_registry`], not [`Self::len`].
    pub fn imported_len(&self) -> usize {
        self.imported_len
    }

    /// Empty map.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Count of full-registry linear scans performed as map-miss fallbacks.
    pub fn linear_scan_count(&self) -> u64 {
        self.linear_scan_count
    }

    /// Record that a full-registry scan occurred (map miss fallback).
    pub fn note_linear_scan(&mut self) {
        self.linear_scan_count = self.linear_scan_count.saturating_add(1);
    }

    /// Reset the linear-scan counter (tests).
    pub fn take_linear_scan_count(&mut self) -> u64 {
        let n = self.linear_scan_count;
        self.linear_scan_count = 0;
        n
    }

    /// Build a map from `state.validators` (S2-A-11 call-site / harness helper).
    pub fn from_registry<P: crate::preset::Preset>(state: &super::BeaconState<P>) -> Self {
        let mut map = Self::default();
        map.import_from_registry(state);
        map
    }

    /// Fill from `state.validators`. Append-only and idempotent (S2-A-10).
    ///
    /// Walks only `self.imported_len()..validators_len()` so a second call
    /// after a full fill is O(new). The cursor is not [`Self::len`]: a
    /// duplicate pubkey or a scan insert from another registry must not skip
    /// the unwalked tail, and a shorter sibling must not rewind it. First-wins
    /// on a duplicate pubkey (same as `get_validator_index_by_pubkey`'s
    /// registry scan). Never removes. A disagreed index is left for hit
    /// revalidation; this does not rebuild the map.
    pub fn import_from_registry<P: crate::preset::Preset>(
        &mut self,
        state: &super::BeaconState<P>,
    ) {
        let len = state.validators_len();
        let start = self.imported_len;
        if start >= len {
            return;
        }
        for i in start..len {
            let Some(v) = state.validators_get(i) else {
                continue;
            };
            self.map
                .entry(v.pubkey)
                .or_insert(ValidatorIndex::new(i as u64));
        }
        self.imported_len = len;
    }
}

/// Default capacity for [`ShufflingCache`] (Architecture §5.4).
pub const SHUFFLING_CACHE_DEFAULT_CAPACITY: usize = 16;

/// Key for a cached epoch shuffling: `(epoch, decision_root)`.
///
/// `decision_root` is the block root at `start_slot(epoch) − 1` (Architecture §5.4).
/// Two branches in the same epoch with different decision roots must not share a
/// shuffling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ShufflingCacheKey {
    /// Target epoch of the shuffling.
    pub epoch: Epoch,
    /// Dependent / decision block root for that epoch.
    pub decision_root: Root,
}

/// One epoch's shuffled active-validator list (content-addressed by
/// [`ShufflingCacheKey`]).
///
/// `shuffled[i]` is the active validator at shuffled position `i` — i.e.
/// `active_indices[compute_shuffled_index(i, len, seed)]`. Committees are
/// contiguous slices of this vector.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShuffledCommitteeEpoch {
    /// Active validators in shuffled order for the epoch.
    pub shuffled: Vec<ValidatorIndex>,
}

/// Committee shuffling cache keyed by `(epoch, decision_root)` with LRU eviction.
///
/// Filled by `cc-state-transition` (CC-13a). Interior-mutable so
/// `get_beacon_committee(&BeaconState, …)` can populate on miss without `&mut`.
/// Never invalidated by state mutation — the key is content-addressed; only LRU
/// eviction removes entries (Architecture §5.4).
pub struct ShufflingCache {
    capacity: usize,
    inner:
        std::sync::Mutex<lru::LruCache<ShufflingCacheKey, std::sync::Arc<ShuffledCommitteeEpoch>>>,
    /// Number of times a shuffling was computed (cache miss fill). Instrumented
    /// for CC-13/6 compute-once assertions.
    compute_count: std::sync::atomic::AtomicU64,
}

impl Default for ShufflingCache {
    fn default() -> Self {
        Self::with_capacity(SHUFFLING_CACHE_DEFAULT_CAPACITY)
    }
}

impl Clone for ShufflingCache {
    fn clone(&self) -> Self {
        let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let cap = std::num::NonZeroUsize::new(self.capacity.max(1))
            .unwrap_or(std::num::NonZeroUsize::MIN);
        let mut new_map = lru::LruCache::new(cap);
        // Preserve LRU order: `iter` yields MRU → LRU; re-insert in reverse.
        let entries: Vec<_> = guard
            .iter()
            .map(|(k, v)| (*k, std::sync::Arc::clone(v)))
            .collect();
        for (k, v) in entries.into_iter().rev() {
            new_map.put(k, v);
        }
        Self {
            capacity: self.capacity,
            inner: std::sync::Mutex::new(new_map),
            compute_count: std::sync::atomic::AtomicU64::new(
                self.compute_count
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
        }
    }
}

impl fmt::Debug for ShufflingCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let len = self.len();
        f.debug_struct("ShufflingCache")
            .field("capacity", &self.capacity)
            .field("len", &len)
            .field(
                "compute_count",
                &self
                    .compute_count
                    .load(std::sync::atomic::Ordering::Relaxed),
            )
            .finish()
    }
}

impl ShufflingCache {
    /// Create a cache with the given maximum entry count (LRU bound).
    pub fn with_capacity(capacity: usize) -> Self {
        let cap = capacity.max(1);
        let nz = std::num::NonZeroUsize::new(cap).unwrap_or(std::num::NonZeroUsize::MIN);
        Self {
            capacity: cap,
            inner: std::sync::Mutex::new(lru::LruCache::new(nz)),
            compute_count: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Configured capacity (LRU bound).
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Current number of cached epochs.
    pub fn len(&self) -> usize {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Instrumented compute-once counter (cache-miss fills).
    pub fn compute_count(&self) -> u64 {
        self.compute_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Reset the compute counter (tests).
    pub fn take_compute_count(&self) -> u64 {
        self.compute_count
            .swap(0, std::sync::atomic::Ordering::Relaxed)
    }

    /// Lookup without computing.
    pub fn get(&self, key: &ShufflingCacheKey) -> Option<std::sync::Arc<ShuffledCommitteeEpoch>> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(key)
            .map(std::sync::Arc::clone)
    }

    /// Return cached value or compute+insert on miss. Increments
    /// [`compute_count`](Self::compute_count) only on miss.
    pub fn get_or_insert_with<F>(
        &self,
        key: ShufflingCacheKey,
        f: F,
    ) -> std::sync::Arc<ShuffledCommitteeEpoch>
    where
        F: FnOnce() -> ShuffledCommitteeEpoch,
    {
        {
            let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(hit) = guard.get(&key) {
                return std::sync::Arc::clone(hit);
            }
        }
        // Compute outside the lock so concurrent misses may double-fill; both
        // results are equivalent for a content-addressed key.
        let value = std::sync::Arc::new(f());
        self.compute_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        // Another thread may have won the race; prefer existing entry.
        if let Some(hit) = guard.get(&key) {
            return std::sync::Arc::clone(hit);
        }
        guard.put(key, std::sync::Arc::clone(&value));
        value
    }

    /// Insert explicitly (tests / warm-start). Does **not** bump compute_count.
    pub fn insert(&self, key: ShufflingCacheKey, value: ShuffledCommitteeEpoch) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .put(key, std::sync::Arc::new(value));
    }

    /// Clear all entries (does not reset compute_count).
    pub fn clear(&self) {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).clear();
    }

    /// Whether `key` is present (does not update LRU order).
    pub fn contains(&self, key: &ShufflingCacheKey) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(key)
    }
}

/// Epoch-derived values for rewards / active sets (Architecture §5.4).
///
/// Keyed by current epoch. Invalidated by any registry or effective-balance
/// change; rebuilt at the epoch boundary by `cc-state-transition`.
#[derive(Clone, Default)]
pub struct EpochCache {
    /// Epoch this cache was built for, when filled.
    pub epoch: Option<Epoch>,
    /// Total active balance for the cached epoch (Gwei).
    pub total_active_balance: Option<u64>,
    /// Base reward per increment (Gwei).
    pub base_reward_per_increment: Option<u64>,
    /// Active validator indices at the current epoch.
    pub current_active_indices: Option<Vec<ValidatorIndex>>,
    /// Active validator indices at the previous epoch.
    pub previous_active_indices: Option<Vec<ValidatorIndex>>,
    /// Active validator indices at the next epoch.
    pub next_active_indices: Option<Vec<ValidatorIndex>>,
    /// Number of times the cache was rebuilt (CC-13b once-per-epoch assert).
    rebuild_count: u64,
}

impl fmt::Debug for EpochCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EpochCache")
            .field("epoch", &self.epoch)
            .field("total_active_balance", &self.total_active_balance)
            .field("base_reward_per_increment", &self.base_reward_per_increment)
            .field(
                "current_active_len",
                &self.current_active_indices.as_ref().map(Vec::len),
            )
            .field(
                "previous_active_len",
                &self.previous_active_indices.as_ref().map(Vec::len),
            )
            .field(
                "next_active_len",
                &self.next_active_indices.as_ref().map(Vec::len),
            )
            .field("rebuild_count", &self.rebuild_count)
            .finish()
    }
}

impl EpochCache {
    /// Whether the cache is fully filled for `epoch`.
    pub fn is_valid_for(&self, epoch: Epoch) -> bool {
        self.epoch == Some(epoch)
            && self.total_active_balance.is_some()
            && self.base_reward_per_increment.is_some()
            && self.current_active_indices.is_some()
            && self.previous_active_indices.is_some()
            && self.next_active_indices.is_some()
    }

    /// Drop all cached values (registry / effective-balance change).
    ///
    /// Preserves [`rebuild_count`](Self::rebuild_count) so once-per-epoch
    /// instrumentation survives invalidation.
    pub fn invalidate(&mut self) {
        let count = self.rebuild_count;
        *self = Self {
            rebuild_count: count,
            ..Self::default()
        };
    }

    /// Whether any field is populated.
    pub fn is_empty(&self) -> bool {
        self.epoch.is_none()
            && self.total_active_balance.is_none()
            && self.base_reward_per_increment.is_none()
            && self.current_active_indices.is_none()
            && self.previous_active_indices.is_none()
            && self.next_active_indices.is_none()
    }

    /// Number of successful rebuilds (instrumentation for CC-13b).
    pub fn rebuild_count(&self) -> u64 {
        self.rebuild_count
    }

    /// Reset rebuild counter (tests).
    pub fn take_rebuild_count(&mut self) -> u64 {
        let n = self.rebuild_count;
        self.rebuild_count = 0;
        n
    }

    /// Bump rebuild counter after a fill (called by `cc-state-transition`).
    pub fn note_rebuild(&mut self) {
        self.rebuild_count = self.rebuild_count.saturating_add(1);
    }
}

/// Non-spec cache field on [`crate::state::BeaconState`] (Architecture §3.4).
#[derive(Clone)]
pub struct StateCaches<P: crate::preset::Preset> {
    /// List hash caches: validators, balances, prev/curr participation, inactivity.
    pub list_hashes: [Option<ListHashCache>; list_id::COUNT],
    /// Top-level field roots + dirty bitmask.
    pub field_roots: FieldRootCache,
    /// Committee shuffling cache (CC-13a).
    pub committees: ShufflingCache,
    /// Epoch-derived values (CC-13a).
    pub epoch: EpochCache,
    /// Non-spec discriminator for PartialEq unit tests; always zero on decode.
    pub(crate) tag: u64,
    _marker: std::marker::PhantomData<P>,
}

impl<P: crate::preset::Preset> Default for StateCaches<P> {
    fn default() -> Self {
        Self {
            list_hashes: std::array::from_fn(|_| None),
            field_roots: FieldRootCache::default(),
            committees: ShufflingCache::default(),
            epoch: EpochCache::default(),
            tag: 0,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<P: crate::preset::Preset> fmt::Debug for StateCaches<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StateCaches")
            .field("tag", &self.tag)
            .field(
                "list_hashes_present",
                &self
                    .list_hashes
                    .iter()
                    .map(Option::is_some)
                    .collect::<Vec<_>>(),
            )
            .field("field_roots", &self.field_roots)
            .finish_non_exhaustive()
    }
}

impl<P: crate::preset::Preset> StateCaches<P> {
    /// Construct caches with an explicit tag (test / debug only).
    pub fn with_tag(tag: u64) -> Self {
        Self {
            tag,
            ..Self::default()
        }
    }

    /// Borrow the list cache slot.
    pub fn list_cache(&self, id: usize) -> Option<&ListHashCache> {
        self.list_hashes.get(id).and_then(|c| c.as_ref())
    }

    /// Mutable list cache slot.
    pub fn list_cache_mut(&mut self, id: usize) -> Option<&mut ListHashCache> {
        self.list_hashes.get_mut(id).and_then(|c| c.as_mut())
    }

    /// Ensure a list cache exists (cold → allocated empty shell).
    pub fn ensure_list_cache(
        &mut self,
        id: usize,
        max_len: usize,
        packing_factor: usize,
    ) -> &mut ListHashCache {
        self.list_hashes[id].get_or_insert_with(|| ListHashCache::new(max_len, packing_factor))
    }

    /// Mark a list element dirty and its field leaf dirty.
    pub fn mark_list_element_dirty(&mut self, id: usize, field: StateField, elem_idx: usize) {
        if let Some(cache) = self.list_hashes[id].as_mut() {
            cache.mark_element_dirty(elem_idx);
        }
        // If still cold, field dirty alone forces recompute path to build the cache.
        self.field_roots.mark_dirty(field);
    }

    /// Note list length change after push.
    pub fn note_list_length(&mut self, id: usize, field: StateField, new_len: usize) {
        if let Some(cache) = self.list_hashes[id].as_mut() {
            cache.note_length(new_len);
        }
        self.field_roots.mark_dirty(field);
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use tree_hash::TreeHash;
    use typenum::U16;

    #[test]
    fn list_cache_matches_variable_list_tree_hash_u64() {
        type L = ssz_types::VariableList<u64, U16>;
        let mut list = L::default();
        for i in 0..10u64 {
            list.push(i).unwrap();
        }
        let expected = list.tree_hash_root();

        let mut cache = ListHashCache::new(16, 4); // u64 packing factor 4
        let got = cache.recompute_with(list.len(), |leaf| packed_basic_leaf(&list[..], 4, leaf));
        assert_eq!(got, expected);

        // Mutate one element and recompute incrementally.
        list[3] = 99;
        cache.mark_element_dirty(3);
        let got2 = cache.recompute_with(list.len(), |leaf| packed_basic_leaf(&list[..], 4, leaf));
        assert_eq!(got2, list.tree_hash_root());
    }

    #[test]
    fn list_cache_matches_variable_list_tree_hash_containers() {
        use crate::containers::Checkpoint;
        use crate::primitives::{Epoch, Root};

        type L = ssz_types::VariableList<Checkpoint, U16>;
        let mut list = L::default();
        for i in 0..5u64 {
            list.push(Checkpoint {
                epoch: Epoch::new(i),
                root: Root::from([i as u8; 32]),
            })
            .unwrap();
        }
        let expected = list.tree_hash_root();
        let mut cache = ListHashCache::new(16, 1);
        let got = cache.recompute_with(list.len(), |leaf| container_leaf(&list[..], leaf));
        assert_eq!(got, expected);

        list[1].epoch = Epoch::new(42);
        cache.mark_element_dirty(1);
        let got2 = cache.recompute_with(list.len(), |leaf| container_leaf(&list[..], leaf));
        assert_eq!(got2, list.tree_hash_root());
    }

    #[test]
    fn empty_list_root() {
        type L = ssz_types::VariableList<u64, U16>;
        let list = L::default();
        let mut cache = ListHashCache::new(16, 4);
        let got = cache.recompute_with(0, |_| Hash256::ZERO);
        assert_eq!(got, list.tree_hash_root());
    }

    /// Compile-time inventory: re-adding `PubkeyIndexMap` here fails this match.
    #[test]
    fn state_caches_field_inventory_excludes_pubkey_index_map() {
        let StateCaches {
            list_hashes: _,
            field_roots: _,
            committees: _,
            epoch: _,
            tag: _,
            _marker: _,
        } = StateCaches::<crate::preset::Minimal>::default();
    }

    fn filled_pubkey_map(n: usize) -> PubkeyIndexMap {
        let mut map = PubkeyIndexMap::default();
        for i in 0..n {
            let mut raw = [0u8; 48];
            raw[..8].copy_from_slice(&(i as u64).to_le_bytes());
            map.insert(BlsPublicKey::from_array(raw), ValidatorIndex::new(i as u64));
        }
        map
    }

    /// hashbrown: one control byte + (K, V) per bucket. Not an allocator sample.
    fn map_table_bytes(map: &PubkeyIndexMap) -> usize {
        map.map.capacity().saturating_mul(
            1 + std::mem::size_of::<BlsPublicKey>() + std::mem::size_of::<ValidatorIndex>(),
        )
    }

    fn median_clone_ns<T>(iters: usize, mut f: impl FnMut() -> T) -> u128 {
        let mut samples = Vec::with_capacity(iters);
        for _ in 0..iters {
            let t0 = std::time::Instant::now();
            std::hint::black_box(f());
            samples.push(t0.elapsed().as_nanos());
        }
        samples.sort_unstable();
        samples[iters / 2]
    }

    /// S2-A-12 / E2.5: BeaconState clone must not pay the pubkey-map table copy.
    #[test]
    fn beacon_state_clone_does_not_scale_with_pubkey_map() {
        use crate::preset::Minimal;
        use crate::state::BeaconState;

        const SMALL: usize = 2_000;
        const LARGE: usize = 50_000;
        const ITERS: usize = 31;
        const WARMUP: usize = 3;

        let state = BeaconState::<Minimal>::default();
        let empty = PubkeyIndexMap::default();
        let small = filled_pubkey_map(SMALL);
        let large = filled_pubkey_map(LARGE);
        assert_eq!(empty.len(), 0);
        assert_eq!(small.len(), SMALL);
        assert_eq!(large.len(), LARGE);

        for _ in 0..WARMUP {
            std::hint::black_box(state.clone());
            std::hint::black_box(empty.clone());
            std::hint::black_box(small.clone());
            std::hint::black_box(large.clone());
        }

        let state_empty_ns = median_clone_ns(ITERS, || {
            let _held = &empty;
            state.clone()
        });
        let state_small_ns = median_clone_ns(ITERS, || {
            let _held = &small;
            state.clone()
        });
        let state_large_ns = median_clone_ns(ITERS, || {
            let _held = &large;
            state.clone()
        });
        let map_empty_ns = median_clone_ns(ITERS, || empty.clone());
        let map_small_ns = median_clone_ns(ITERS, || small.clone());
        let map_large_ns = median_clone_ns(ITERS, || large.clone());

        let empty_bytes = map_table_bytes(&empty);
        let small_bytes = map_table_bytes(&small);
        let large_bytes = map_table_bytes(&large);

        let profile = if cfg!(debug_assertions) {
            "test/dev (debug_assertions, workspace opt-level=1)"
        } else {
            "release"
        };
        eprintln!(
            "S2-A-12 clone cost (cc-types lib test, {profile})\n\
             N_small={SMALL} N_large={LARGE} iters={ITERS} median\n\
             map clone empty: {map_empty_ns} ns  ~{empty_bytes} B (cap={})\n\
             map clone small: {map_small_ns} ns  ~{small_bytes} B (cap={})\n\
             map clone large: {map_large_ns} ns  ~{large_bytes} B (cap={})\n\
             state clone + empty map: {state_empty_ns} ns\n\
             state clone + small map: {state_small_ns} ns\n\
             state clone + large map: {state_large_ns} ns\n\
             sizeof BeaconState<Minimal>={}  StateCaches={}  PubkeyIndexMap={}",
            empty.map.capacity(),
            small.map.capacity(),
            large.map.capacity(),
            std::mem::size_of::<BeaconState<Minimal>>(),
            std::mem::size_of::<StateCaches<Minimal>>(),
            std::mem::size_of::<PubkeyIndexMap>(),
        );

        assert!(
            large_bytes >= SMALL * std::mem::size_of::<BlsPublicKey>(),
            "large map table must be at least the raw key payload: {large_bytes}"
        );
        assert!(
            large_bytes > small_bytes.saturating_mul(10),
            "map table bytes must scale with N: small={small_bytes} large={large_bytes}"
        );
        // Map clone is the legacy cost that used to ride on StateCaches::clone.
        assert!(
            map_large_ns >= map_small_ns.saturating_mul(4),
            "map clone must scale with N: small={map_small_ns} ns large={map_large_ns} ns"
        );
        // Sidecar map size must not move BeaconState clone off its floor (8× + 5 ms slack).
        let state_floor = state_empty_ns.max(state_small_ns);
        assert!(
            state_large_ns <= state_floor.saturating_mul(8).saturating_add(5_000_000),
            "state clone must not scale with sidecar map: empty={state_empty_ns} \
             small={state_small_ns} large={state_large_ns} ns"
        );
        assert!(
            map_large_ns > state_large_ns,
            "legacy map clone must exceed state clone at N={LARGE}: \
             map={map_large_ns} ns state={state_large_ns} ns"
        );
    }
}
