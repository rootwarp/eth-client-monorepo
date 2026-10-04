//! Fork-choice [`Store`] (Architecture §6.2, CC-15a / CC-15b).
//!
//! Field set matches the architecture surface. Method names and argument order
//! are the **spec's own** so the `fork_choice` vector steps map one-to-one.
//!
//! Critical mutable state is private so every write path that can move the head
//! goes through methods that bump [`Store::mutation_counter`] (§6.4).
//!
//! `on_block` / `compute_pulled_up_tip` live in [`crate::on_block`]. The DA trait
//! is owned by [`crate::da_seam`] (CC-17). Checkpoint contexts: [`crate::checkpoint_context`].

use std::collections::{BTreeSet, HashMap};
use std::num::NonZeroUsize;
use std::sync::Arc;

use cc_state_transition::ExecutionEngine;
use cc_types::BeaconState;
use cc_types::containers::{BeaconBlockHeader, Checkpoint};
use cc_types::preset::Preset;
use cc_types::primitives::{Epoch, Root, Slot, ValidatorIndex};
use lru::LruCache;
use thiserror::Error;

use crate::checkpoint_context::{CheckpointContext, checkpoint_context_key};
use crate::da_seam::DataAvailability;
use crate::proto_array::ProtoArray;

/// Default capacity for the checkpoint-context LRU (Architecture §6.6: **8**).
pub const DEFAULT_CHECKPOINT_CONTEXT_CAPACITY: usize = 8;

/// Spec `LatestMessage` — the **next** (unapplied or applied) vote view of a validator.
///
/// Derived from [`VoteTracker`]'s `next_root` / `next_epoch`. Prefer reading via
/// [`Store::latest_message`]; weight bookkeeping uses the full tracker (§6.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LatestMessage {
    /// Target epoch of the latest attestation.
    pub epoch: Epoch,
    /// LMD head root of the latest attestation.
    pub root: Root,
}

/// Per-validator vote tracker for batched weight updates (Architecture §6.3).
///
/// `on_attestation` writes only `next_root` / `next_epoch` (O(1), no weight math).
/// `compute_deltas` (called from `get_head`, never from `on_attestation`) produces
/// a per-node delta vector and promotes `current_root = next_root`.
///
/// **Node weights are only correct immediately after `get_head` applies deltas.**
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct VoteTracker {
    /// Root whose weight currently includes this validator (after last `compute_deltas`).
    pub current_root: Root,
    /// Root of the newest accepted attestation (may differ from `current_root`).
    pub next_root: Root,
    /// Target epoch of the newest accepted attestation.
    pub next_epoch: Epoch,
}

impl VoteTracker {
    /// Whether this tracker has never accepted a vote.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.current_root == Root::ZERO
            && self.next_root == Root::ZERO
            && self.next_epoch.as_u64() == 0
    }

    /// Spec-shaped latest message from the **next** vote, if any has been cast.
    #[inline]
    pub fn latest_message(&self) -> Option<LatestMessage> {
        // `next_*` is written by `on_attestation` and left in place after
        // `compute_deltas` promotes `current_root`. Default zero next means no
        // vote has ever been accepted for this validator.
        if self.next_root == Root::ZERO && self.next_epoch.as_u64() == 0 {
            None
        } else {
            Some(LatestMessage {
                epoch: self.next_epoch,
                root: self.next_root,
            })
        }
    }
}

/// Fork-choice head latch written by `get_head` before a durable commit returns.
///
/// Chain restores this when `commit_import` or `set_head` fails so the next
/// success does not publish a head this process has not committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeadLatch {
    /// `last_head_root` at capture time.
    pub last_head_root: Option<Root>,
    /// Head cache at capture time.
    pub head_cache: Option<CachedHead>,
}

/// Head-cache entry served while the store's mutation counter is unchanged (§6.4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CachedHead {
    /// Cached head root.
    pub head_root: Root,
    /// Cached head slot.
    pub head_slot: Slot,
    /// Justified checkpoint at computation time.
    pub justified: Checkpoint,
    /// Finalized checkpoint at computation time.
    pub finalized: Checkpoint,
    /// Mutation counter value when this cache entry was written.
    pub computed_at_mutation: u64,
}

/// Store construction / query errors.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum StoreError {
    /// `on_tick` must not go backwards.
    #[error("on_tick time {time} is earlier than store.time {store_time}")]
    TimeWentBackwards { time: u64, store_time: u64 },
}

/// Spec-shaped fork-choice store (Architecture §6.2).
///
/// `blocks` stores **headers, not blocks**. Post-states for unfinalized roots
/// live in `block_states` so `on_block` can run `state_transition` and
/// `compute_pulled_up_tip` (residency pruning is owned by the chain service,
/// CC-18b). Vote trackers are a dense `Vec<VoteTracker>` so `compute_deltas`
/// is a linear scan (Architecture §6.3).
///
/// # Mutation discipline
///
/// Fields that affect head are private. Mutators bump `mutation_counter` and
/// clear the head cache. Reviewers: if you write to the store, go through a
/// method that bumps the counter.
pub struct Store<P: Preset> {
    /// Current Unix time (seconds) known to the store.
    time: u64,
    /// Genesis Unix time (seconds).
    genesis_time: u64,
    /// Seconds per slot (from chain config; used by slot helpers).
    seconds_per_slot: u64,
    justified_checkpoint: Checkpoint,
    finalized_checkpoint: Checkpoint,
    unrealized_justified_checkpoint: Checkpoint,
    unrealized_finalized_checkpoint: Checkpoint,
    proposer_boost_root: Root,
    equivocating_indices: BTreeSet<ValidatorIndex>,
    blocks: HashMap<Root, BeaconBlockHeader>,
    /// Post-states keyed by block root (parent lookup + pulled-up tip).
    block_states: HashMap<Root, BeaconState<P>>,
    /// Checkpoint-context LRU (Architecture §6.6 — capacity 8). ADR-P1-08.
    checkpoint_contexts: LruCache<(Epoch, Root), Arc<CheckpointContext>>,
    /// Dense vote trackers indexed by validator index (§6.3 batching contract).
    votes: Vec<VoteTracker>,
    /// Effective-balance snapshot last applied by `compute_deltas` (justified).
    ///
    /// Compared against the new justified [`CheckpointContext`] balances on the
    /// next delta pass so balance changes at justification are correct.
    justified_balances: Vec<u64>,
    proto_array: ProtoArray,
    head_cache: Option<CachedHead>,
    mutation_counter: u64,
    /// Last head returned by `get_head` (for reorg detection).
    last_head_root: Option<Root>,
    /// Spec `block_timeliness` — whether a block was timely at import.
    block_timeliness: HashMap<Root, bool>,
    engine: Arc<dyn ExecutionEngine<P>>,
    da: Arc<dyn DataAvailability>,
    _preset: std::marker::PhantomData<P>,
}

impl<P: Preset> std::fmt::Debug for Store<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Store")
            .field("time", &self.time)
            .field("genesis_time", &self.genesis_time)
            .field("seconds_per_slot", &self.seconds_per_slot)
            .field("justified_checkpoint", &self.justified_checkpoint)
            .field("finalized_checkpoint", &self.finalized_checkpoint)
            .field(
                "unrealized_justified_checkpoint",
                &self.unrealized_justified_checkpoint,
            )
            .field(
                "unrealized_finalized_checkpoint",
                &self.unrealized_finalized_checkpoint,
            )
            .field("proposer_boost_root", &self.proposer_boost_root)
            .field("equivocating_indices_len", &self.equivocating_indices.len())
            .field("blocks_len", &self.blocks.len())
            .field("block_states_len", &self.block_states.len())
            .field("checkpoint_contexts_len", &self.checkpoint_contexts.len())
            .field("votes_len", &self.votes.len())
            .field("justified_balances_len", &self.justified_balances.len())
            .field("proto_array_len", &self.proto_array.len())
            .field("head_cache", &self.head_cache)
            .field("mutation_counter", &self.mutation_counter)
            .finish_non_exhaustive()
    }
}

impl<P: Preset> Store<P> {
    /// Construct a store at the anchor checkpoints with empty trees.
    ///
    /// Prefer [`crate::on_block::get_forkchoice_store`] for a fully seeded store.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        time: u64,
        genesis_time: u64,
        seconds_per_slot: u64,
        justified_checkpoint: Checkpoint,
        finalized_checkpoint: Checkpoint,
        validator_count: usize,
        engine: Arc<dyn ExecutionEngine<P>>,
        da: Arc<dyn DataAvailability>,
    ) -> Self {
        let capacity =
            NonZeroUsize::new(DEFAULT_CHECKPOINT_CONTEXT_CAPACITY).unwrap_or(NonZeroUsize::MIN);
        Self {
            time,
            genesis_time,
            seconds_per_slot: seconds_per_slot.max(1),
            justified_checkpoint,
            finalized_checkpoint,
            unrealized_justified_checkpoint: justified_checkpoint,
            unrealized_finalized_checkpoint: finalized_checkpoint,
            proposer_boost_root: Root::ZERO,
            equivocating_indices: BTreeSet::new(),
            blocks: HashMap::new(),
            block_states: HashMap::new(),
            checkpoint_contexts: LruCache::new(capacity),
            votes: vec![VoteTracker::default(); validator_count],
            justified_balances: vec![0; validator_count],
            proto_array: ProtoArray::new(justified_checkpoint, finalized_checkpoint),
            head_cache: None,
            mutation_counter: 0,
            last_head_root: None,
            block_timeliness: HashMap::new(),
            engine,
            da,
            _preset: std::marker::PhantomData,
        }
    }

    // --- accessors (read) ----------------------------------------------------

    /// Current store time (seconds).
    #[inline]
    pub fn time(&self) -> u64 {
        self.time
    }

    /// Genesis time (seconds).
    #[inline]
    pub fn genesis_time(&self) -> u64 {
        self.genesis_time
    }

    /// Seconds per slot used for slot math.
    #[inline]
    pub fn seconds_per_slot(&self) -> u64 {
        self.seconds_per_slot
    }

    /// Justified checkpoint for LMD-GHOST.
    #[inline]
    pub fn justified_checkpoint(&self) -> Checkpoint {
        self.justified_checkpoint
    }

    /// Highest known finalized checkpoint.
    #[inline]
    pub fn finalized_checkpoint(&self) -> Checkpoint {
        self.finalized_checkpoint
    }

    /// Unrealized justified (pulled-up).
    #[inline]
    pub fn unrealized_justified_checkpoint(&self) -> Checkpoint {
        self.unrealized_justified_checkpoint
    }

    /// Unrealized finalized (pulled-up).
    #[inline]
    pub fn unrealized_finalized_checkpoint(&self) -> Checkpoint {
        self.unrealized_finalized_checkpoint
    }

    /// Root receiving proposer boost this slot, or zero.
    #[inline]
    pub fn proposer_boost_root(&self) -> Root {
        self.proposer_boost_root
    }

    /// Monotonic mutation counter (§6.4).
    #[inline]
    pub fn mutation_counter(&self) -> u64 {
        self.mutation_counter
    }

    /// Borrow the head cache, if any.
    #[inline]
    pub fn head_cache(&self) -> Option<&CachedHead> {
        self.head_cache.as_ref()
    }

    /// Last head root known to the store (after a successful `get_head`).
    #[inline]
    pub fn last_head_root(&self) -> Option<Root> {
        self.last_head_root
    }

    /// Head root for the node-level optimistic predicate (§4.11).
    ///
    /// Prefers the last successful `get_head` root, else the head-cache entry.
    /// Returns `None` when neither is set — branch 1 of
    /// [`crate::is_optimistic_node`] then falls through to branch 2.
    #[inline]
    pub fn cached_head_root(&self) -> Option<Root> {
        self.last_head_root
            .or_else(|| self.head_cache.as_ref().map(|c| c.head_root))
    }

    /// Copy of the head latch `get_head` writes.
    ///
    /// Does not bump the mutation counter. Pair with [`Self::restore_head_latch`]
    /// when a durable head write fails.
    #[inline]
    pub fn head_latch(&self) -> HeadLatch {
        HeadLatch {
            last_head_root: self.last_head_root,
            head_cache: self.head_cache,
        }
    }

    /// Put the head latch back. Does not bump the mutation counter and does
    /// not undo proto-array weights or votes.
    #[inline]
    pub fn restore_head_latch(&mut self, latch: HeadLatch) {
        self.last_head_root = latch.last_head_root;
        self.head_cache = latch.head_cache;
    }

    /// Whether `root` was recorded as timely at import (spec `block_timeliness`).
    #[inline]
    pub fn block_timeliness(&self, root: &Root) -> Option<bool> {
        self.block_timeliness.get(root).copied()
    }

    /// Borrow the proto-array (read-only). Mutations go through store methods.
    #[inline]
    pub fn proto_array(&self) -> &ProtoArray {
        &self.proto_array
    }

    /// Borrow block headers map (read-only).
    #[inline]
    pub fn blocks(&self) -> &HashMap<Root, BeaconBlockHeader> {
        &self.blocks
    }

    /// Borrow post-state for `root`, if resident.
    #[inline]
    pub fn block_state(&self, root: &Root) -> Option<&BeaconState<P>> {
        self.block_states.get(root)
    }

    /// Number of resident block states.
    #[inline]
    pub fn block_states_len(&self) -> usize {
        self.block_states.len()
    }

    /// Borrow vote trackers (read-only).
    #[inline]
    pub fn votes(&self) -> &[VoteTracker] {
        &self.votes
    }

    /// Spec-shaped latest message for `validator_index`, if any.
    #[inline]
    pub fn latest_message(&self, validator_index: ValidatorIndex) -> Option<LatestMessage> {
        self.votes
            .get(validator_index.as_u64() as usize)
            .and_then(VoteTracker::latest_message)
    }

    /// Borrow equivocating validator indices (read-only).
    #[inline]
    pub fn equivocating_indices(&self) -> &BTreeSet<ValidatorIndex> {
        &self.equivocating_indices
    }

    /// Effective balances last applied by `compute_deltas`.
    #[inline]
    pub fn justified_balances(&self) -> &[u64] {
        &self.justified_balances
    }

    /// Borrow the DA seam.
    #[inline]
    pub fn da(&self) -> &dyn DataAvailability {
        self.da.as_ref()
    }

    /// Borrow the execution engine seam.
    #[inline]
    pub fn engine(&self) -> &dyn ExecutionEngine<P> {
        self.engine.as_ref()
    }

    /// Shared engine handle for callers that need an `Arc` (e.g. transition context).
    #[inline]
    pub fn engine_arc(&self) -> &Arc<dyn ExecutionEngine<P>> {
        &self.engine
    }

    /// Current number of checkpoint-context LRU entries.
    #[inline]
    pub fn checkpoint_contexts_len(&self) -> usize {
        self.checkpoint_contexts.len()
    }

    /// Borrow a checkpoint context by checkpoint (promotes to MRU).
    pub fn checkpoint_context(&mut self, checkpoint: Checkpoint) -> Option<Arc<CheckpointContext>> {
        self.checkpoint_contexts
            .get(&checkpoint_context_key(checkpoint))
            .map(Arc::clone)
    }

    // --- slot helpers --------------------------------------------------------

    /// Spec `get_slots_since_genesis`.
    #[inline]
    pub fn get_slots_since_genesis(&self) -> u64 {
        self.time.saturating_sub(self.genesis_time) / self.seconds_per_slot
    }

    /// Spec `get_current_slot`.
    #[inline]
    pub fn get_current_slot(&self) -> Slot {
        Slot::new(self.get_slots_since_genesis())
    }

    /// Spec `get_current_store_epoch`.
    #[inline]
    pub fn get_current_store_epoch(&self) -> Epoch {
        self.get_current_slot().epoch(P::SLOTS_PER_EPOCH)
    }

    /// Spec `compute_slots_since_epoch_start`.
    #[inline]
    pub fn compute_slots_since_epoch_start(slot: Slot) -> u64 {
        let epoch = slot.epoch(P::SLOTS_PER_EPOCH);
        let start = epoch.as_u64().saturating_mul(P::SLOTS_PER_EPOCH);
        slot.as_u64().saturating_sub(start)
    }

    // --- mutation paths (always bump when state changes) ---------------------

    /// Bump the mutation counter and invalidate the head cache.
    ///
    /// Call from every store writer that can affect the head.
    #[inline]
    pub fn bump_mutation_counter(&mut self) {
        self.mutation_counter = self.mutation_counter.wrapping_add(1);
        self.head_cache = None;
    }

    /// Set store time without bumping (mid-slot `on_tick`; head is slot-stable).
    pub(crate) fn set_time(&mut self, time: u64) {
        self.time = time;
    }

    /// Clear proposer boost and bump the mutation counter (slot boundary).
    pub(crate) fn clear_proposer_boost_root(&mut self) {
        if self.proposer_boost_root != Root::ZERO {
            self.proposer_boost_root = Root::ZERO;
            self.bump_mutation_counter();
        } else {
            // Spec still clears (already zero) but a new slot is a mutation for
            // head-cache purposes: boost *application* window closed.
            self.proposer_boost_root = Root::ZERO;
            self.bump_mutation_counter();
        }
    }

    /// Set the proposer-boost root (bumps mutation counter).
    ///
    /// Used by `on_block` after a timely import and by tests.
    pub fn set_proposer_boost_root(&mut self, root: Root) {
        if self.proposer_boost_root != root {
            self.proposer_boost_root = root;
            self.bump_mutation_counter();
        }
    }

    /// Test helper: set proposer boost (always bumps).
    #[cfg(test)]
    pub(crate) fn set_proposer_boost_root_for_test(&mut self, root: Root) {
        self.proposer_boost_root = root;
        self.bump_mutation_counter();
    }

    /// Write the head cache after a successful `get_head` (does **not** bump).
    pub(crate) fn set_head_cache(&mut self, cached: CachedHead) {
        self.head_cache = Some(cached);
    }

    /// Record the head root after `get_head` (does **not** bump).
    pub(crate) fn set_last_head_root(&mut self, root: Root) {
        self.last_head_root = Some(root);
    }

    /// Record block timeliness (spec `record_block_timeliness`).
    pub(crate) fn set_block_timeliness(&mut self, root: Root, timely: bool) {
        self.block_timeliness.insert(root, timely);
    }

    /// Test helper: set unrealized checkpoints without bumping (seed for epoch pull-up).
    #[cfg(test)]
    pub(crate) fn seed_unrealized_for_test(
        &mut self,
        justified: Checkpoint,
        finalized: Checkpoint,
    ) {
        self.unrealized_justified_checkpoint = justified;
        self.unrealized_finalized_checkpoint = finalized;
    }

    /// Insert a block header + post-state and bump the mutation counter.
    pub fn insert_block(&mut self, root: Root, header: BeaconBlockHeader, state: BeaconState<P>) {
        self.blocks.insert(root, header);
        self.block_states.insert(root, state);
        self.bump_mutation_counter();
    }

    /// Insert or replace a post-state without touching headers or the mutation counter.
    ///
    /// Used by chain residency (CC-18b) to re-materialise a pruned parent state
    /// before `on_block` clones it. Does **not** bump — restoring a known state
    /// cannot move the head.
    pub fn put_block_state(&mut self, root: Root, state: BeaconState<P>) {
        self.block_states.insert(root, state);
    }

    /// Drop post-states not selected by `keep` (headers remain).
    ///
    /// Residency pruning surface (CC-18b / ADR-P1-12): keep at most the pinned
    /// roles; headers and proto-array stay so fork-choice identity is intact.
    pub fn retain_block_states(&mut self, mut keep: impl FnMut(&Root) -> bool) {
        self.block_states.retain(|root, _| keep(root));
    }

    /// Insert / refresh a [`CheckpointContext`] in the LRU (capacity 8).
    ///
    /// Does **not** bump `mutation_counter` by itself — callers that also change
    /// justified balances should have already bumped via checkpoint updates.
    pub fn insert_checkpoint_context(
        &mut self,
        checkpoint: Checkpoint,
        context: Arc<CheckpointContext>,
    ) {
        self.checkpoint_contexts
            .put(checkpoint_context_key(checkpoint), context);
    }

    /// Spec `update_checkpoints` — promote justified/finalized when newer.
    ///
    /// Does **not** prune the proto-array. Call [`Self::prune_on_finalized`]
    /// from the FINALIZED publish path after any REORG walk — prune rewrites
    /// indices and must not run while a pending walk still names the old head.
    pub fn update_checkpoints(
        &mut self,
        justified_checkpoint: Checkpoint,
        finalized_checkpoint: Checkpoint,
    ) {
        let mut changed = false;
        if justified_checkpoint.epoch.as_u64() > self.justified_checkpoint.epoch.as_u64() {
            self.justified_checkpoint = justified_checkpoint;
            self.proto_array
                .set_checkpoints(self.justified_checkpoint, self.finalized_checkpoint);
            changed = true;
        }
        if finalized_checkpoint.epoch.as_u64() > self.finalized_checkpoint.epoch.as_u64() {
            self.finalized_checkpoint = finalized_checkpoint;
            self.proto_array
                .set_checkpoints(self.justified_checkpoint, self.finalized_checkpoint);
            changed = true;
        }
        if changed {
            self.bump_mutation_counter();
        }
    }

    /// Compact the proto-array to the store's finalized root and drop matching
    /// `blocks` / `block_states` / `block_timeliness` keys (P0-10 / S0-A-21).
    ///
    /// Production caller: `FINALIZED_CHECKPOINT` publish in the chain import
    /// path, after the REORG walk. Idempotent if the array is already rooted
    /// at `finalized_checkpoint.root`. Unknown finalized root is a no-op
    /// (restore / foreign-checkpoint tests).
    ///
    /// Skips compaction when `justified_checkpoint.root` would not remain in
    /// the array (finalized does not dominate justified).
    ///
    /// Returns the post-prune node count.
    pub fn prune_on_finalized(&mut self) -> usize {
        let finalized_root = self.finalized_checkpoint.root;
        let before = self.proto_array.len();
        if !self.justified_survives_prune(finalized_root) {
            tracing::debug!(
                ?finalized_root,
                justified = ?self.justified_checkpoint.root,
                "proto-array prune skipped (justified root would not survive)"
            );
            return before;
        }
        if let Err(e) = self.proto_array.prune(finalized_root) {
            tracing::debug!(
                error = %e,
                ?finalized_root,
                "proto-array prune skipped (finalized root not in array)"
            );
            return before;
        }

        self.blocks
            .retain(|root, _| self.proto_array.contains(root));
        self.block_states
            .retain(|root, _| self.proto_array.contains(root));
        self.block_timeliness
            .retain(|root, _| self.proto_array.contains(root));

        if self.proposer_boost_root != Root::ZERO
            && !self.proto_array.contains(&self.proposer_boost_root)
        {
            self.proposer_boost_root = Root::ZERO;
        }

        let after = self.proto_array.len();
        if after != before {
            self.bump_mutation_counter();
        }
        after
    }

    /// Whether `justified_checkpoint.root` is `finalized_root` or a descendant
    /// of it (so [`ProtoArray::prune`] would keep it).
    fn justified_survives_prune(&self, finalized_root: Root) -> bool {
        let justified_root = self.justified_checkpoint.root;
        if justified_root == finalized_root {
            return self.proto_array.contains(&finalized_root);
        }
        let Some(fin) = self.proto_array.get(&finalized_root) else {
            return false;
        };
        match self.proto_array.get_ancestor(justified_root, fin.slot) {
            Ok(ancestor) => ancestor == finalized_root,
            Err(_) => false,
        }
    }

    /// Spec `update_unrealized_checkpoints`.
    pub fn update_unrealized_checkpoints(
        &mut self,
        unrealized_justified_checkpoint: Checkpoint,
        unrealized_finalized_checkpoint: Checkpoint,
    ) {
        let mut changed = false;
        if unrealized_justified_checkpoint.epoch.as_u64()
            > self.unrealized_justified_checkpoint.epoch.as_u64()
        {
            self.unrealized_justified_checkpoint = unrealized_justified_checkpoint;
            changed = true;
        }
        if unrealized_finalized_checkpoint.epoch.as_u64()
            > self.unrealized_finalized_checkpoint.epoch.as_u64()
        {
            self.unrealized_finalized_checkpoint = unrealized_finalized_checkpoint;
            changed = true;
        }
        if changed {
            self.bump_mutation_counter();
        }
    }

    /// Mutable proto-array access for insert/weight paths (CC-15b+ / CC-1E tests).
    ///
    /// Callers that mutate must also call [`Self::bump_mutation_counter`] when
    /// the mutation can move the head (insert already bumps via `insert_block`).
    pub fn proto_array_mut(&mut self) -> &mut ProtoArray {
        &mut self.proto_array
    }

    /// Mutable vote-tracker slice for `on_attestation` / `compute_deltas`.
    pub(crate) fn votes_mut(&mut self) -> &mut [VoteTracker] {
        &mut self.votes
    }

    /// Resize the vote-tracker and balance tables to exactly `count` entries.
    ///
    /// Production: [`crate::on_block::get_forkchoice_store`] seeds at the
    /// anchor; [`crate::on_block::on_block`] grows from the trusted post-state
    /// when the registry expands. Attestation handlers must **not** grow
    /// unbounded from arbitrary indices (SEC-16-2) — they reject out-of-range
    /// validator indices instead.
    pub fn resize_votes(&mut self, count: usize) {
        self.votes.resize(count, VoteTracker::default());
        self.justified_balances.resize(count, 0);
    }

    /// Number of vote-tracker slots (validator capacity known to fork choice).
    #[inline]
    pub fn vote_capacity(&self) -> usize {
        self.votes.len()
    }

    /// Insert equivocating indices and bump the mutation counter if any are new.
    ///
    /// A slashed validator's weight is retracted on the next `compute_deltas`
    /// pass (skip + negative delta on `current_root`).
    pub fn insert_equivocating_indices(
        &mut self,
        indices: impl IntoIterator<Item = ValidatorIndex>,
    ) {
        let mut changed = false;
        for index in indices {
            if self.equivocating_indices.insert(index) {
                changed = true;
            }
        }
        if changed {
            self.bump_mutation_counter();
        }
    }

    /// Replace the justified-balance snapshot used by the next `compute_deltas`.
    ///
    /// Called after a successful delta pass (and when seeding from a justified
    /// [`CheckpointContext`]). Does not bump `mutation_counter` by itself —
    /// balance swap alone does not move the head until deltas are applied.
    pub fn set_justified_balances(&mut self, balances: Vec<u64>) {
        self.justified_balances = balances;
    }

    /// Checkpoint-context LRU capacity (Architecture §6.6).
    pub fn checkpoint_context_capacity(&self) -> usize {
        self.checkpoint_contexts.cap().get()
    }
}

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

    use std::sync::Arc;

    use cc_types::containers::{BeaconBlockHeader, Checkpoint};
    use cc_types::fork::Fork;
    use cc_types::preset::Minimal;
    use cc_types::primitives::{Epoch, Gwei, Hash256, Root, Slot, ValidatorIndex};

    use super::*;
    use crate::checkpoint_context::{CheckpointContext, CommitteeCache};
    use crate::da_seam::HarnessAvailability;
    use crate::execution_status::ExecutionStatus;
    use crate::proto_array::ProtoNodeBlock;

    fn root(b: u8) -> Root {
        let mut a = [0u8; 32];
        a[0] = b;
        Root::from_array(a)
    }

    fn cp(epoch: u64, r: Root) -> Checkpoint {
        Checkpoint {
            epoch: Epoch::new(epoch),
            root: r,
        }
    }

    fn empty_ctx(epoch: u64) -> Arc<CheckpointContext> {
        Arc::new(CheckpointContext {
            epoch: Epoch::new(epoch),
            committee_cache: CommitteeCache::default(),
            effective_balances: vec![Gwei::new(32_000_000_000)],
            total_active_balance: Gwei::new(32_000_000_000),
            fork: Fork {
                previous_version: Default::default(),
                current_version: Default::default(),
                epoch: Epoch::new(0),
            },
            genesis_validators_root: Root::ZERO,
        })
    }

    fn new_store() -> Store<Minimal> {
        let anchor = cp(0, root(1));
        Store::new(
            0,
            0,
            6,
            anchor,
            anchor,
            0,
            Arc::new(AcceptEngine),
            Arc::new(HarnessAvailability),
        )
    }

    fn insert_node(store: &mut Store<Minimal>, slot: u64, r: u8, parent: Option<u8>) {
        let this = root(r);
        let parent_root = parent.map(root);
        let justified = store.justified_checkpoint();
        let finalized = store.finalized_checkpoint();
        let state = match parent_root.and_then(|p| store.block_state(&p).cloned()) {
            Some(mut s) => {
                s.set_slot(Slot::new(slot));
                s
            }
            None => BeaconState::default(),
        };
        store
            .proto_array_mut()
            .on_block(ProtoNodeBlock {
                slot: Slot::new(slot),
                root: this,
                parent_root,
                state_root: this,
                target_root: this,
                justified_checkpoint: justified,
                finalized_checkpoint: finalized,
                unrealized_justified_checkpoint: justified,
                unrealized_finalized_checkpoint: finalized,
                execution_status: ExecutionStatus::Valid,
                execution_block_hash: Hash256::ZERO,
            })
            .unwrap();
        store.insert_block(
            this,
            BeaconBlockHeader {
                slot: Slot::new(slot),
                proposer_index: ValidatorIndex::new(0),
                parent_root: parent_root.unwrap_or(Root::ZERO),
                state_root: this,
                body_root: Root::ZERO,
            },
            state,
        );
        store.set_block_timeliness(this, true);
    }

    /// P0-10 / S0-A-21: two finalization advances must shrink the proto-array.
    #[test]
    fn two_finalizations_decrease_proto_array_node_count() {
        // G(1) ─ A(2) ─ B(3) ─ C(4)
        //            └─ X(5)          ← pruned on first finalization
        let mut store = new_store();
        insert_node(&mut store, 0, 1, None);
        insert_node(&mut store, 1, 2, Some(1));
        insert_node(&mut store, 8, 3, Some(2));
        insert_node(&mut store, 9, 4, Some(3));
        insert_node(&mut store, 9, 5, Some(2));
        let before_first = store.proto_array().len();
        assert_eq!(before_first, 5);

        store.update_checkpoints(cp(1, root(3)), cp(1, root(3)));
        let after_first = store.prune_on_finalized();
        assert!(
            after_first < before_first,
            "first finalization must drop non-descendants (before={before_first} after={after_first})"
        );
        assert!(store.proto_array().contains(&root(3)));
        assert!(store.proto_array().contains(&root(4)));
        assert!(!store.proto_array().contains(&root(1)));
        assert!(!store.proto_array().contains(&root(2)));
        assert!(!store.proto_array().contains(&root(5)));
        assert!(!store.blocks().contains_key(&root(5)));
        assert!(store.block_timeliness(&root(5)).is_none());

        // C(4) ─ D(6) ─ E(7)
        insert_node(&mut store, 16, 6, Some(4));
        insert_node(&mut store, 17, 7, Some(6));
        let before_second = store.proto_array().len();
        assert!(before_second > after_first);

        store.update_checkpoints(cp(2, root(6)), cp(2, root(6)));
        let after_second = store.prune_on_finalized();
        assert!(
            after_second < before_second,
            "second finalization must decrease node count (before={before_second} after={after_second})"
        );
        assert_eq!(after_second, 2);
        assert!(store.proto_array().contains(&root(6)));
        assert!(store.proto_array().contains(&root(7)));
        assert!(!store.proto_array().contains(&root(3)));
        assert!(!store.proto_array().contains(&root(4)));
    }

    /// H2: do not compact to a finalized root that would drop justified.
    #[test]
    fn prune_skips_when_justified_would_not_survive() {
        // G(1) ─ J(2)                 ← store justified (epoch 2)
        //     └─ F(3) ─ C(4)          ← would-be finalized (epoch 1)
        let mut store = new_store();
        insert_node(&mut store, 0, 1, None);
        insert_node(&mut store, 16, 2, Some(1));
        insert_node(&mut store, 8, 3, Some(1));
        insert_node(&mut store, 9, 4, Some(3));

        store.update_checkpoints(cp(2, root(2)), cp(0, root(1)));
        store.update_checkpoints(cp(2, root(2)), cp(1, root(3)));
        assert_eq!(store.justified_checkpoint().root, root(2));
        assert_eq!(store.finalized_checkpoint().root, root(3));

        let before = store.proto_array().len();
        assert_eq!(store.prune_on_finalized(), before);
        assert!(store.proto_array().contains(&root(2)));
        assert!(store.proto_array().contains(&root(1)));
        assert_eq!(store.proto_array().len(), 4);
    }

    #[test]
    fn checkpoint_context_lru_capacity_is_eight() {
        let store = new_store();
        assert_eq!(
            store.checkpoint_context_capacity(),
            DEFAULT_CHECKPOINT_CONTEXT_CAPACITY
        );
        assert_eq!(DEFAULT_CHECKPOINT_CONTEXT_CAPACITY, 8);
    }

    /// Push 12 distinct checkpoints; map size never exceeds the bound of 8.
    #[test]
    fn checkpoint_context_lru_evicts_beyond_capacity() {
        let mut store = new_store();
        for i in 0u8..12 {
            let checkpoint = cp(i as u64, root(i.wrapping_add(1)));
            store.insert_checkpoint_context(checkpoint, empty_ctx(i as u64));
            assert!(
                store.checkpoint_contexts_len() <= DEFAULT_CHECKPOINT_CONTEXT_CAPACITY,
                "len {} exceeded capacity after insert {i}",
                store.checkpoint_contexts_len()
            );
        }
        assert_eq!(
            store.checkpoint_contexts_len(),
            DEFAULT_CHECKPOINT_CONTEXT_CAPACITY
        );
        // Oldest entries (0..4) should be gone; newest (4..12) retained.
        assert!(store.checkpoint_context(cp(0, root(1))).is_none());
        assert!(store.checkpoint_context(cp(11, root(12))).is_some());
    }
}
