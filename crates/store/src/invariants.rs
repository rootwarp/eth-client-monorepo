//! §2.7 store invariants (CC-4H).
//!
//! Eight named checks run at [`crate::schema::Store::open`] (**fatal**) and after
//! every migration / prune pass (**logged + counted**, non-fatal), behind
//! `storage.check_invariants`.
//!
//! The metrics label domain is the eight labels below **plus** `key_collision`
//! (CC-44b idempotency; not a store-structure invariant here).
//!
//! ## Bounds (SEC-4H-1)
//!
//! `I-contig` is the expensive check. It never walks every integer slot in a
//! sparse `[oldest, head]` span (that would DoS open on a two-row corrupt
//! store with a huge head). Instead it:
//! 1. Walks the canonical index in slot chunks of
//!    [`crate::engine::MAX_RANGE_ENTRIES`] (one `ReadTxn::range` per chunk),
//! 2. Fails closed with [`StoreError::Limit`] if the advertised span exceeds
//!    [`MAX_CONTIG_WALK_SLOTS`] (sized to the CC-4A block serve window),
//! 3. Verifies holes with an O(|canonical| + |holes|) gap-coverage walk.
//!
//! Each row-scanning check also fails closed if it inspects more than
//! [`InvariantContext::max_open_scan_rows`] (named default
//! [`DEFAULT_MAX_OPEN_SCAN_ROWS`]). The budget is **per check**, not shared.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use ssz::Decode;

use cc_types::{Root, Slot};

use crate::backfill_progress::block_serve_window_slots;
use crate::engine::{Engine, MAX_RANGE_ENTRIES, ReadTxn, StoreError};
use crate::keys::{
    SLOTS_PER_EPOCH, block_shard_id, block_shard_start_slot, column_shard_start_slot,
    decode_canonical_entry, decode_cold_column_key, decode_hot_block_key, decode_hot_column_key,
    decode_snapshot_key, encode_cold_block_key, encode_hot_block_key, hot_block_slot_upper_bound,
};
use crate::meta::{
    AnchorInfo, ForkChoiceScalars, KEY_ANCHOR_INFO, KEY_FC_SCALARS, KEY_NODE_ID, KEY_PRUNE_MARKS,
    KEY_SERVE_WINDOW, KEY_SPLIT, KEY_WRITE_CURSOR, PruneMarks, ServeWindow, SlotRange, Split,
    TABLE_META, WriteCursor,
};
use crate::schema::{is_registered_table, iter_shard_tables};

/// Default `storage.snapshot_ring` when config has not set one (CC-42 / Architecture §3.6).
pub const DEFAULT_SNAPSHOT_RING: u64 = 4;

/// Hoodi / mainnet CC-4A floor (`MIN_VALIDATOR_WITHDRAWABILITY_DELAY +
/// CHURN_LIMIT_QUOTIENT / 2`). Authority is
/// [`crate::window::compute_min_epochs_for_block_requests`]; this is that
/// arithmetic as a `const` so the walk cap can be sized at compile time.
const HOODI_MIN_EPOCHS_FOR_BLOCK_REQUESTS: u64 = 256 + 65_536 / 2;

/// Hard cap on the `I-contig` walk span (`head − oldest + 1`) (SEC-4H-1).
///
/// Must cover a full CC-4A block serve window
/// ([`block_serve_window_slots`]) plus the current epoch so an inclusive
/// `[start_slot(current − min_epochs), last slot of current]` walk does not
/// fail closed. Previously this equalled [`MAX_RANGE_ENTRIES`] (1048576),
/// which sits 8192 slots below the Hoodi/mainnet window (P0-18/2).
/// Anything larger is refused as `StoreError::Limit`.
pub const MAX_CONTIG_WALK_SLOTS: u64 =
    block_serve_window_slots(HOODI_MIN_EPOCHS_FOR_BLOCK_REQUESTS).saturating_add(SLOTS_PER_EPOCH);

const _: () =
    assert!(MAX_CONTIG_WALK_SLOTS >= block_serve_window_slots(HOODI_MIN_EPOCHS_FOR_BLOCK_REQUESTS));
const _: () = assert!(MAX_CONTIG_WALK_SLOTS > MAX_RANGE_ENTRIES as u64);

/// Cap on snapshot rows inspected for `I-ring` before fail-closed Limit.
/// Ring depth is config (default 4); a grossly over-full table is corruption
/// and must not force a multi-million-row materialisation beyond the engine cap.
pub const MAX_RING_SCAN_ROWS: usize = 256;

/// Named default for `storage.max_open_scan_rows`.
///
/// Per-check row cap. Equal to [`MAX_CONTIG_WALK_SLOTS`] so a full
/// serve-window canonical walk completes; a multi-GB column table still
/// `Limit`s rather than scanning unbounded.
pub const DEFAULT_MAX_OPEN_SCAN_ROWS: u64 = MAX_CONTIG_WALK_SLOTS;

const _: () = assert!(DEFAULT_MAX_OPEN_SCAN_ROWS >= MAX_CONTIG_WALK_SLOTS);
const _: () = assert!(DEFAULT_MAX_OPEN_SCAN_ROWS > MAX_RANGE_ENTRIES as u64);

// ---------------------------------------------------------------------------
// Enum + labels
// ---------------------------------------------------------------------------

/// The eight §2.7 store invariants (Architecture table; metrics labels match).
///
/// Label domain for `cc_storage_invariant_violation_total{invariant}` is these
/// eight **plus** `key_collision` (not represented here).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StoreInvariant {
    /// Hole inside the advertised window not recorded in `ServeWindow.holes`.
    Contig,
    /// Column row with no matching block at its slot.
    ColBlock,
    /// Migration half-applied: `blocks_hot` row at or below `Split.slot`, or split past finality.
    SplitFin,
    /// Empty snapshot ring, over-depth ring, or newest snapshot above the split.
    Ring,
    /// Advertisement below oldest stored block or below `PruneMarks.blocks_up_to`.
    Window,
    /// `AnchorInfo.node_id` disagrees with the node key / expected id.
    NodeId,
    /// Shard table present below prune marks (retirement missed).
    Shards,
    /// Write cursor pointing past data that is present.
    Cursor,
}

impl StoreInvariant {
    /// All eight variants in stable order.
    pub const ALL: [Self; 8] = [
        Self::Contig,
        Self::ColBlock,
        Self::SplitFin,
        Self::Ring,
        Self::Window,
        Self::NodeId,
        Self::Shards,
        Self::Cursor,
    ];

    /// Prometheus / §10.1 label value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Contig => "contig",
            Self::ColBlock => "col_block",
            Self::SplitFin => "split_fin",
            Self::Ring => "ring",
            Self::Window => "window",
            Self::NodeId => "node_id",
            Self::Shards => "shards",
            Self::Cursor => "cursor",
        }
    }
}

/// How to apply check results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvariantCheckMode {
    /// First violation → [`StoreError::InvariantViolation`] (open path).
    Open,
    /// Every violation is reported via the sink; never aborts (post-pass).
    PostPass,
}

/// External inputs the structural checks cannot derive from the engine alone.
///
/// [`Debug`] is hand-written: `expected_node_id` is the raw node key.
#[derive(Clone)]
pub struct InvariantContext {
    /// Node id derived from the node key file (`p2p.node_key_path`).
    ///
    /// **When `None`, `I-node-id` is skipped (not failed).** Callers that enforce
    /// §1.7 / ADR P4-13 pairing must supply the `NodeId` derived from the node
    /// key; omitting it is for bootstrap, offline tools without a key, and unit
    /// tests that are not exercising that invariant.
    pub expected_node_id: Option<Root>,
    /// Max snapshot ring depth (`storage.snapshot_ring`).
    pub snapshot_ring: u64,
    /// Per-check row cap (`storage.max_open_scan_rows`).
    ///
    /// Named default [`DEFAULT_MAX_OPEN_SCAN_ROWS`]. Exceeding it is
    /// [`StoreError::Limit`].
    pub max_open_scan_rows: u64,
    /// Optional per-open invocation counter (tests; process-global would race).
    pub invocation_counter: Option<Arc<AtomicU64>>,
}

impl Default for InvariantContext {
    fn default() -> Self {
        Self::new()
    }
}

impl InvariantContext {
    /// Build a context with ring default and no node id.
    #[must_use]
    pub fn new() -> Self {
        Self {
            expected_node_id: None,
            snapshot_ring: DEFAULT_SNAPSHOT_RING,
            max_open_scan_rows: DEFAULT_MAX_OPEN_SCAN_ROWS,
            invocation_counter: None,
        }
    }

    /// Attach a shared invocation counter (CC-4H /2 tests).
    #[must_use]
    pub fn with_invocation_counter(mut self, counter: Arc<AtomicU64>) -> Self {
        self.invocation_counter = Some(counter);
        self
    }

    /// Set the per-check open-scan row budget.
    #[must_use]
    pub fn with_max_open_scan_rows(mut self, max_open_scan_rows: u64) -> Self {
        self.max_open_scan_rows = max_open_scan_rows;
        self
    }
}

impl fmt::Debug for InvariantContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InvariantContext")
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

/// One reported violation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvariantViolation {
    /// Which invariant fired.
    pub invariant: StoreInvariant,
    /// Detail for logs / fatal Display (often names found vs expected).
    pub detail: String,
}

/// Side-effect sink for post-pass mode (log + metrics).
pub trait InvariantSink {
    /// Called once per violation in post-pass mode.
    fn on_violation(&self, violation: &InvariantViolation);
}

/// Sink that only increments a counter (tests / metrics adapters).
#[derive(Debug, Default)]
pub struct CountingSink {
    /// Per-label counts keyed by [`StoreInvariant::as_str`].
    counts: std::sync::Mutex<std::collections::BTreeMap<&'static str, u64>>,
    /// Last details for assertions.
    details: std::sync::Mutex<Vec<InvariantViolation>>,
}

impl CountingSink {
    /// Create an empty sink.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Count for one invariant label.
    pub fn count(&self, inv: StoreInvariant) -> u64 {
        self.counts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(inv.as_str())
            .copied()
            .unwrap_or(0)
    }

    /// All recorded violations in order.
    pub fn violations(&self) -> Vec<InvariantViolation> {
        self.details
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

impl InvariantSink for CountingSink {
    fn on_violation(&self, violation: &InvariantViolation) {
        {
            let mut g = self.counts.lock().unwrap_or_else(|e| e.into_inner());
            *g.entry(violation.invariant.as_str()).or_insert(0) += 1;
        }
        self.details
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(violation.clone());
    }
}

/// Tracing sink: `error!` with invariant label + detail.
#[derive(Debug, Default, Clone, Copy)]
pub struct TracingSink;

impl InvariantSink for TracingSink {
    fn on_violation(&self, violation: &InvariantViolation) {
        tracing::error!(
            invariant = violation.invariant.as_str(),
            detail = %violation.detail,
            "store invariant violation"
        );
    }
}

/// Fan-out sink (tracing + metrics adapter, or test + real).
#[derive(Debug)]
pub struct FanoutSink<'a, A: InvariantSink + std::fmt::Debug, B: InvariantSink + std::fmt::Debug> {
    /// First sink.
    pub a: &'a A,
    /// Second sink.
    pub b: &'a B,
}

impl<A: InvariantSink + std::fmt::Debug, B: InvariantSink + std::fmt::Debug> InvariantSink
    for FanoutSink<'_, A, B>
{
    fn on_violation(&self, violation: &InvariantViolation) {
        self.a.on_violation(violation);
        self.b.on_violation(violation);
    }
}

// ---------------------------------------------------------------------------
// Per-check scan budget
// ---------------------------------------------------------------------------

/// Remaining row allowance for **one** §2.7 check.
struct ScanBudget {
    remaining: u64,
    limit: u64,
}

impl ScanBudget {
    fn new(limit: u64) -> Self {
        Self {
            remaining: limit,
            limit,
        }
    }

    fn cap(&self) -> usize {
        usize::try_from(self.remaining).unwrap_or(usize::MAX)
    }

    fn consume(&mut self, n: u64, what: &'static str) -> Result<(), StoreError> {
        if n > self.remaining {
            return Err(scan_budget_exceeded(self.limit, what));
        }
        self.remaining -= n;
        Ok(())
    }
}

fn scan_budget_exceeded(limit: u64, what: &str) -> StoreError {
    StoreError::limit(format!(
        "{what}: scanned more than storage.max_open_scan_rows ({limit}); \
         refuse rather than unbounded wait"
    ))
}

/// One owned key/value pair from a budgeted range.
type BudgetedRow = (Vec<u8>, Vec<u8>);

/// Materialise `[lo, hi)` charging every row against `budget`.
///
/// Engine [`StoreError::Limit`] (row cap, 512 MiB, or `MAX_RANGE_ENTRIES`) is
/// propagated unchanged.
fn range_budgeted(
    rt: &ReadTxn,
    table: &str,
    lo: &[u8],
    hi: &[u8],
    budget: &mut ScanBudget,
    what: &'static str,
) -> Result<Vec<BudgetedRow>, StoreError> {
    let iter = rt.range_max(table, lo, hi, budget.cap())?;
    let mut rows = Vec::new();
    for item in iter {
        rows.push(item?);
    }
    budget.consume(rows.len() as u64, what)?;
    Ok(rows)
}

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

/// Run all eight invariants against `engine`.
///
/// - [`InvariantCheckMode::Open`]: returns `Err` on the **first** violation.
/// - [`InvariantCheckMode::PostPass`]: reports every violation to `sink` and returns
///   `Ok(n)` where `n` is the number of violations (never fatal).
///
/// Each row-scanning check gets its own [`InvariantContext::max_open_scan_rows`]
/// budget. Exceeding that budget is [`StoreError::Limit`].
///
/// Does **not** consult the `check_invariants` config flag — callers gate via
/// [`run_invariant_checks_if_enabled`].
pub fn check_invariants(
    engine: &Engine,
    mode: InvariantCheckMode,
    ctx: &InvariantContext,
    sink: Option<&dyn InvariantSink>,
) -> Result<u64, StoreError> {
    let rt = engine.read()?;
    let mut violations = Vec::new();
    let limit = ctx.max_open_scan_rows;

    if let Some(v) = check_contig(&rt, &mut ScanBudget::new(limit))? {
        violations.push(v);
    }
    if let Some(v) = check_col_block(engine, &rt, &mut ScanBudget::new(limit))? {
        violations.push(v);
    }
    if let Some(v) = check_split_fin(&rt, &mut ScanBudget::new(limit))? {
        violations.push(v);
    }
    if let Some(v) = check_ring(&rt, ctx.snapshot_ring, &mut ScanBudget::new(limit))? {
        violations.push(v);
    }
    if let Some(v) = check_window(&rt)? {
        violations.push(v);
    }
    if let Some(v) = check_node_id(&rt, ctx.expected_node_id.as_ref())? {
        violations.push(v);
    }
    if let Some(v) = check_shards(engine, &rt)? {
        violations.push(v);
    }
    if let Some(v) = check_cursor(&rt, &mut ScanBudget::new(limit))? {
        violations.push(v);
    }

    match mode {
        InvariantCheckMode::Open => {
            if let Some(first) = violations.into_iter().next() {
                return Err(StoreError::InvariantViolation {
                    invariant: first.invariant.as_str(),
                    detail: first.detail,
                });
            }
            Ok(0)
        }
        InvariantCheckMode::PostPass => {
            let n = violations.len() as u64;
            if let Some(sink) = sink {
                for v in &violations {
                    sink.on_violation(v);
                }
            }
            Ok(n)
        }
    }
}

/// Gated entry used by open / post-migration / post-prune.
///
/// When `enabled` is false, returns `Ok(0)` without reading the store or bumping
/// any invocation counter. When true, increments `ctx.invocation_counter` (if set)
/// and runs [`check_invariants`].
pub fn run_invariant_checks_if_enabled(
    engine: &Engine,
    enabled: bool,
    mode: InvariantCheckMode,
    ctx: &InvariantContext,
    sink: Option<&dyn InvariantSink>,
) -> Result<u64, StoreError> {
    if !enabled {
        return Ok(0);
    }
    if let Some(c) = &ctx.invocation_counter {
        c.fetch_add(1, Ordering::SeqCst);
    }
    check_invariants(engine, mode, ctx, sink)
}

// ---------------------------------------------------------------------------
// Individual checks
// ---------------------------------------------------------------------------

fn read_meta_ssz<T: Decode>(rt: &ReadTxn, key: &str) -> Result<Option<T>, StoreError> {
    let Some(bytes) = rt.get(TABLE_META, key.as_bytes())? else {
        return Ok(None);
    };
    T::from_ssz_bytes(&bytes)
        .map(Some)
        .map_err(|e| StoreError::Codec(format!("{key}: {e:?}")))
}

/// `I-contig`: every slot in `[oldest_block_slot, head]` is either present in
/// `canonical` or covered by `ServeWindow.holes`. Vacuous if no anchor / no canonical.
///
/// **SEC-4H-1:** never walks every integer in a sparse span. Uses gap coverage
/// over the sorted canonical index (O(|canonical| + |holes|)). Span larger than
/// [`MAX_CONTIG_WALK_SLOTS`] → [`StoreError::Limit`] (fail closed at open).
/// Canonical is read in [`MAX_RANGE_ENTRIES`]-slot chunks so a full serve
/// window (wider than one range call) still completes.
fn check_contig(
    rt: &ReadTxn,
    budget: &mut ScanBudget,
) -> Result<Option<InvariantViolation>, StoreError> {
    let Some(anchor) = read_meta_ssz::<AnchorInfo>(rt, KEY_ANCHOR_INFO)? else {
        return Ok(None);
    };
    let holes: Vec<SlotRange> = read_meta_ssz::<ServeWindow>(rt, KEY_SERVE_WINDOW)?
        .map(|w| w.holes.to_vec())
        .unwrap_or_default();

    let oldest = anchor.oldest_block_slot.as_u64();
    // Exclusive end of the allowed walk. A canonical row at or beyond this
    // means span > MAX_CONTIG_WALK_SLOTS.
    let walk_hi = oldest.saturating_add(MAX_CONTIG_WALK_SLOTS);
    let present = collect_canonical_slots_chunked(rt, oldest, walk_hi, budget)?;

    let end = match present.last().copied() {
        Some(head) => head,
        None => {
            if first_canonical_at_or_after(rt, walk_hi, budget)?.is_some() {
                return Err(contig_span_limit(oldest, walk_hi));
            }
            return Ok(None);
        }
    };
    if let Some(far) = first_canonical_at_or_after(rt, walk_hi, budget)? {
        return Err(contig_span_limit(oldest, far.max(end)));
    }

    // Gap-based coverage: O(|present| + |holes|), not O(span).
    let first = present[0];
    if first > oldest
        && let Some(hole_slot) = first_uncovered_slot(oldest, first, &holes)
    {
        return Ok(Some(contig_hole_violation(hole_slot, oldest, end)));
    }
    for w in present.windows(2) {
        let a = w[0];
        let b = w[1];
        let gap_lo = a.saturating_add(1);
        if gap_lo < b
            && let Some(hole_slot) = first_uncovered_slot(gap_lo, b, &holes)
        {
            return Ok(Some(contig_hole_violation(hole_slot, oldest, end)));
        }
    }
    Ok(None)
}

fn contig_span_limit(oldest: u64, end: u64) -> StoreError {
    let span = end.saturating_sub(oldest).saturating_add(1);
    StoreError::limit(format!(
        "I-contig walk span {span} slots (oldest={oldest}, head={end}) exceeds \
         MAX_CONTIG_WALK_SLOTS ({MAX_CONTIG_WALK_SLOTS}); refuse rather than DoS open"
    ))
}

/// Canonical slots in half-open `[oldest, walk_hi)`, chunked so each
/// `ReadTxn::range` stays within [`MAX_RANGE_ENTRIES`].
fn collect_canonical_slots_chunked(
    rt: &ReadTxn,
    oldest: u64,
    walk_hi: u64,
    budget: &mut ScanBudget,
) -> Result<Vec<u64>, StoreError> {
    let mut present = Vec::new();
    let chunk = MAX_RANGE_ENTRIES as u64;
    let mut lo = oldest;
    while lo < walk_hi {
        let hi = walk_hi.min(lo.saturating_add(chunk));
        if hi <= lo {
            break;
        }
        let lo_key = encode_cold_block_key(Slot::new(lo));
        let hi_key = encode_cold_block_key(Slot::new(hi));
        for (k, v) in range_budgeted(rt, "canonical", &lo_key, &hi_key, budget, "I-contig")? {
            let Some((slot, _)) = decode_canonical_entry(&k, &v) else {
                continue;
            };
            if slot.as_u64() >= oldest {
                present.push(slot.as_u64());
            }
        }
        lo = hi;
    }
    Ok(present)
}

/// First canonical slot at or after `start`, or `None` if the tail is empty.
///
/// A [`StoreError::Limit`] from the tail range means the tail is dense — treat
/// that as "something exists at or after `start`" without materialising it.
fn first_canonical_at_or_after(
    rt: &ReadTxn,
    start: u64,
    budget: &mut ScanBudget,
) -> Result<Option<u64>, StoreError> {
    if start == u64::MAX {
        return Ok(None);
    }
    let lo = encode_cold_block_key(Slot::new(start));
    let hi = encode_cold_block_key(Slot::new(u64::MAX));
    // Existence probe: materialise at most one row.
    match rt.range_max("canonical", &lo, &hi, 1) {
        Ok(iter) => {
            for item in iter {
                let (k, v) = item?;
                if let Some((slot, _)) = decode_canonical_entry(&k, &v)
                    && slot.as_u64() >= start
                {
                    budget.consume(1, "I-contig")?;
                    return Ok(Some(slot.as_u64()));
                }
            }
            Ok(None)
        }
        Err(StoreError::Limit(_)) => {
            budget.consume(1, "I-contig")?;
            Ok(Some(start))
        }
        Err(e) => Err(e),
    }
}

fn contig_hole_violation(slot: u64, oldest: u64, end: u64) -> InvariantViolation {
    InvariantViolation {
        invariant: StoreInvariant::Contig,
        detail: format!(
            "canonical hole at slot {slot} (walk [{oldest}, {end}]) not recorded in ServeWindow.holes"
        ),
    }
}

/// First slot in half-open `[lo, hi)` not covered by any hole, or `None` if fully covered.
///
/// Merges overlapping hole intervals — O(|holes| log |holes|), not O(hi − lo).
fn first_uncovered_slot(lo: u64, hi: u64, holes: &[SlotRange]) -> Option<u64> {
    if lo >= hi {
        return None;
    }
    let mut intervals: Vec<(u64, u64)> = holes
        .iter()
        .filter_map(|h| {
            let s = h.start.as_u64();
            let e = h.end.as_u64();
            if e <= lo || s >= hi {
                return None;
            }
            Some((s.max(lo), e.min(hi)))
        })
        .collect();
    intervals.sort_by_key(|(s, _)| *s);
    let mut cur = lo;
    for (s, e) in intervals {
        if s > cur {
            return Some(cur);
        }
        if e > cur {
            cur = e;
        }
        if cur >= hi {
            return None;
        }
    }
    if cur < hi { Some(cur) } else { None }
}

/// `I-col-block`: every column row has a block at its slot (hot root match / cold slot).
fn check_col_block(
    engine: &Engine,
    rt: &ReadTxn,
    budget: &mut ScanBudget,
) -> Result<Option<InvariantViolation>, StoreError> {
    // columns_hot
    let lo = [0u8; 42];
    let hi = [0xffu8; 42];
    for (k, _) in range_budgeted(rt, "columns_hot", &lo, &hi, budget, "I-col-block")? {
        let Some((slot, root, idx)) = decode_hot_column_key(&k) else {
            continue;
        };
        let block_key = encode_hot_block_key(slot, &root);
        if rt.get("blocks_hot", &block_key)?.is_none() {
            // Cold region may hold the block after migration.
            let cold_tbl = crate::keys::blocks_shard_table(block_shard_id(slot));
            let cold_key = encode_cold_block_key(slot);
            if rt.get(&cold_tbl, &cold_key)?.is_none() {
                return Ok(Some(InvariantViolation {
                    invariant: StoreInvariant::ColBlock,
                    detail: format!(
                        "column at slot {} root {} idx {idx} has no block row",
                        slot.as_u64(),
                        root
                    ),
                }));
            }
        }
    }

    // Cold column shards (live tables only — O(active), not O(history)).
    let names = engine.table_names()?;
    for (name, class, shard_id) in iter_shard_tables(&names) {
        if class != "columns" {
            continue;
        }
        let start = column_shard_start_slot(shard_id);
        let end = column_shard_start_slot(shard_id.saturating_add(1));
        let lo = crate::keys::encode_cold_column_key(start, 0);
        let hi = crate::keys::encode_cold_column_key(end, 0);
        for (k, _) in range_budgeted(rt, name, &lo, &hi, budget, "I-col-block")? {
            let Some((slot, idx)) = decode_cold_column_key(&k) else {
                continue;
            };
            if !block_exists_at_slot(rt, slot, budget)? {
                return Ok(Some(InvariantViolation {
                    invariant: StoreInvariant::ColBlock,
                    detail: format!(
                        "cold column at slot {} idx {idx} (table {name}) has no block row",
                        slot.as_u64()
                    ),
                }));
            }
        }
    }
    Ok(None)
}

fn block_exists_at_slot(
    rt: &ReadTxn,
    slot: Slot,
    budget: &mut ScanBudget,
) -> Result<bool, StoreError> {
    // Hot: any root at this slot (probe prefix range).
    let lo = encode_hot_block_key(slot, &Root::ZERO);
    let hi = hot_block_slot_upper_bound(slot);
    for (k, _) in range_budgeted(rt, "blocks_hot", &lo, &hi, budget, "I-col-block")? {
        if let Some((s, _)) = decode_hot_block_key(&k)
            && s == slot
        {
            return Ok(true);
        }
    }
    let cold_tbl = crate::keys::blocks_shard_table(block_shard_id(slot));
    let cold_key = encode_cold_block_key(slot);
    Ok(rt.get(&cold_tbl, &cold_key)?.is_some())
}

/// `I-split-fin`: `Split.slot ≤ finalized_slot` and no `blocks_hot` row with slot ≤ split.
fn check_split_fin(
    rt: &ReadTxn,
    budget: &mut ScanBudget,
) -> Result<Option<InvariantViolation>, StoreError> {
    let Some(split) = read_meta_ssz::<Split>(rt, KEY_SPLIT)? else {
        return Ok(None);
    };
    if let Some(fc) = read_meta_ssz::<ForkChoiceScalars>(rt, KEY_FC_SCALARS)? {
        let finalized_slot = Slot::new(fc.finalized.epoch.as_u64().saturating_mul(SLOTS_PER_EPOCH));
        if split.slot.as_u64() > finalized_slot.as_u64() {
            return Ok(Some(InvariantViolation {
                invariant: StoreInvariant::SplitFin,
                detail: format!(
                    "Split.slot {} > finalized_slot {} (finalized epoch {})",
                    split.slot.as_u64(),
                    finalized_slot.as_u64(),
                    fc.finalized.epoch.as_u64()
                ),
            }));
        }
    }

    let lo = [0u8; 40];
    let hi = hot_block_slot_upper_bound(split.slot);
    for (k, _) in range_budgeted(rt, "blocks_hot", &lo, &hi, budget, "I-split-fin")? {
        if let Some((slot, root)) = decode_hot_block_key(&k)
            && slot.as_u64() <= split.slot.as_u64()
        {
            return Ok(Some(InvariantViolation {
                invariant: StoreInvariant::SplitFin,
                detail: format!(
                    "blocks_hot row at slot {} root {} is ≤ Split.slot {}",
                    slot.as_u64(),
                    root,
                    split.slot.as_u64()
                ),
            }));
        }
    }
    Ok(None)
}

/// `I-ring`: non-empty when Split exists; `|snapshots| ≤ ring`; newest ≤ Split.slot.
///
/// Scans at most `min(MAX_RING_SCAN_ROWS, remaining check budget)` rows;
/// beyond either → [`StoreError::Limit`].
fn check_ring(
    rt: &ReadTxn,
    ring: u64,
    budget: &mut ScanBudget,
) -> Result<Option<InvariantViolation>, StoreError> {
    let Some(split) = read_meta_ssz::<Split>(rt, KEY_SPLIT)? else {
        return Ok(None);
    };
    let lo = encode_cold_block_key(Slot::ZERO);
    let hi = encode_cold_block_key(Slot::new(u64::MAX));
    let ring_cap = (MAX_RING_SCAN_ROWS as u64).min(budget.remaining);
    let rows = match rt.range_max("snapshots", &lo, &hi, ring_cap as usize) {
        Ok(iter) => {
            let mut rows = Vec::new();
            for item in iter {
                rows.push(item?);
            }
            budget.consume(rows.len() as u64, "I-ring")?;
            rows
        }
        Err(StoreError::Limit(msg)) => {
            return Err(if (MAX_RING_SCAN_ROWS as u64) < budget.remaining {
                StoreError::limit(format!(
                    "I-ring: scanned >{MAX_RING_SCAN_ROWS} snapshot rows (ring config {ring}); \
                     refuse rather than unbounded scan"
                ))
            } else {
                StoreError::Limit(msg)
            });
        }
        Err(e) => return Err(e),
    };
    let mut count: u64 = 0;
    let mut newest = 0u64;
    for (k, _) in rows {
        if let Some(slot) = decode_snapshot_key(&k) {
            let s = slot.as_u64();
            newest = newest.max(s);
            count = count.saturating_add(1);
            // Over-depth is an I-ring violation; stop as soon as depth exceeds config
            // so we never scan a multi-million-row corrupt table (SEC-4H-1 medium).
            if count > ring {
                return Ok(Some(InvariantViolation {
                    invariant: StoreInvariant::Ring,
                    detail: format!(
                        "snapshot ring depth at least {count} exceeds storage.snapshot_ring {ring}"
                    ),
                }));
            }
        }
    }
    if count == 0 {
        return Ok(Some(InvariantViolation {
            invariant: StoreInvariant::Ring,
            detail: format!(
                "snapshot ring empty while Split.slot = {}",
                split.slot.as_u64()
            ),
        }));
    }
    if newest > split.slot.as_u64() {
        return Ok(Some(InvariantViolation {
            invariant: StoreInvariant::Ring,
            detail: format!(
                "newest snapshot slot {newest} > Split.slot {}",
                split.slot.as_u64()
            ),
        }));
    }
    Ok(None)
}

/// `I-window`: `earliest_available_slot ≥ oldest_block_slot` and `≥ PruneMarks.blocks_up_to`.
fn check_window(rt: &ReadTxn) -> Result<Option<InvariantViolation>, StoreError> {
    let Some(window) = read_meta_ssz::<ServeWindow>(rt, KEY_SERVE_WINDOW)? else {
        return Ok(None);
    };
    if let Some(anchor) = read_meta_ssz::<AnchorInfo>(rt, KEY_ANCHOR_INFO)?
        && window.earliest_available_slot.as_u64() < anchor.oldest_block_slot.as_u64()
    {
        return Ok(Some(InvariantViolation {
            invariant: StoreInvariant::Window,
            detail: format!(
                "ServeWindow.earliest_available_slot {} < AnchorInfo.oldest_block_slot {}",
                window.earliest_available_slot.as_u64(),
                anchor.oldest_block_slot.as_u64()
            ),
        }));
    }
    if let Some(marks) = read_meta_ssz::<PruneMarks>(rt, KEY_PRUNE_MARKS)?
        && window.earliest_available_slot.as_u64() < marks.blocks_up_to.as_u64()
    {
        return Ok(Some(InvariantViolation {
            invariant: StoreInvariant::Window,
            detail: format!(
                "ServeWindow.earliest_available_slot {} < PruneMarks.blocks_up_to {}",
                window.earliest_available_slot.as_u64(),
                marks.blocks_up_to.as_u64()
            ),
        }));
    }
    Ok(None)
}

/// `I-node-id`: stored identity equals the expected id from the node key.
///
/// Prefers `meta.node_id` (identity-only stamp). Falls back to
/// `AnchorInfo.node_id` so existing origin rows still pair. Skipped when
/// `expected` is `None` or no identity row exists.
fn check_node_id(
    rt: &ReadTxn,
    expected: Option<&Root>,
) -> Result<Option<InvariantViolation>, StoreError> {
    let Some(expected) = expected else {
        // Optional by design: no expected id supplied → do not fail I-node-id.
        return Ok(None);
    };
    let Some(found) = stored_node_id(rt)? else {
        return Ok(None);
    };
    if &found != expected {
        return Ok(Some(InvariantViolation {
            invariant: StoreInvariant::NodeId,
            detail: "stored node_id <redacted> does not match the configured node key".to_owned(),
        }));
    }
    Ok(None)
}

/// Identity for I-node-id: dedicated `node_id` key, else `AnchorInfo.node_id`.
fn stored_node_id(rt: &ReadTxn) -> Result<Option<Root>, StoreError> {
    if let Some(id) = read_meta_ssz::<Root>(rt, KEY_NODE_ID)? {
        return Ok(Some(id));
    }
    Ok(read_meta_ssz::<AnchorInfo>(rt, KEY_ANCHOR_INFO)?.map(|a| a.node_id))
}

/// `I-shards`: all names registered; no shard table fully below prune marks.
fn check_shards(engine: &Engine, rt: &ReadTxn) -> Result<Option<InvariantViolation>, StoreError> {
    let names = engine.table_names()?;
    for name in &names {
        if !is_registered_table(name) {
            return Ok(Some(InvariantViolation {
                invariant: StoreInvariant::Shards,
                detail: format!("unregistered table {name:?}"),
            }));
        }
    }
    let marks = read_meta_ssz::<PruneMarks>(rt, KEY_PRUNE_MARKS)?.unwrap_or_default();
    for (name, class, id) in iter_shard_tables(&names) {
        match class {
            "blocks" => {
                let end = block_shard_start_slot(id.saturating_add(1));
                if marks.blocks_up_to.as_u64() >= end.as_u64() {
                    return Ok(Some(InvariantViolation {
                        invariant: StoreInvariant::Shards,
                        detail: format!(
                            "block shard table {name} ends at slot {} ≤ PruneMarks.blocks_up_to {}",
                            end.as_u64(),
                            marks.blocks_up_to.as_u64()
                        ),
                    }));
                }
            }
            "columns" => {
                let end = column_shard_start_slot(id.saturating_add(1));
                if marks.columns_up_to.as_u64() >= end.as_u64() {
                    return Ok(Some(InvariantViolation {
                        invariant: StoreInvariant::Shards,
                        detail: format!(
                            "column shard table {name} ends at slot {} ≤ PruneMarks.columns_up_to {}",
                            end.as_u64(),
                            marks.columns_up_to.as_u64()
                        ),
                    }));
                }
            }
            _ => {}
        }
    }
    Ok(None)
}

/// `I-cursor`: WriteCursor.slot ≤ newest stored block slot.
fn check_cursor(
    rt: &ReadTxn,
    budget: &mut ScanBudget,
) -> Result<Option<InvariantViolation>, StoreError> {
    let Some(cursor) = read_meta_ssz::<WriteCursor>(rt, KEY_WRITE_CURSOR)? else {
        return Ok(None);
    };
    let Some(newest) = newest_block_slot(rt, budget)? else {
        // Cursor with no blocks is a violation if cursor.slot > 0, else vacuous.
        if cursor.slot.as_u64() > 0 {
            return Ok(Some(InvariantViolation {
                invariant: StoreInvariant::Cursor,
                detail: format!(
                    "WriteCursor.slot {} but no stored blocks",
                    cursor.slot.as_u64()
                ),
            }));
        }
        return Ok(None);
    };
    if cursor.slot.as_u64() > newest.as_u64() {
        return Ok(Some(InvariantViolation {
            invariant: StoreInvariant::Cursor,
            detail: format!(
                "WriteCursor.slot {} > newest stored block slot {}",
                cursor.slot.as_u64(),
                newest.as_u64()
            ),
        }));
    }
    Ok(None)
}

fn newest_block_slot(rt: &ReadTxn, budget: &mut ScanBudget) -> Result<Option<Slot>, StoreError> {
    let mut newest: Option<u64> = None;
    // Prefer canonical head.
    let lo = encode_cold_block_key(Slot::ZERO);
    let hi = encode_cold_block_key(Slot::new(u64::MAX));
    for (k, v) in range_budgeted(rt, "canonical", &lo, &hi, budget, "I-cursor")? {
        if let Some((slot, _)) = decode_canonical_entry(&k, &v) {
            newest = Some(newest.map_or(slot.as_u64(), |n| n.max(slot.as_u64())));
        }
    }
    // Also scan blocks_hot.
    let lo = [0u8; 40];
    let hi = [0xffu8; 40];
    for (k, _) in range_budgeted(rt, "blocks_hot", &lo, &hi, budget, "I-cursor")? {
        if let Some((slot, _)) = decode_hot_block_key(&k) {
            newest = Some(newest.map_or(slot.as_u64(), |n| n.max(slot.as_u64())));
        }
    }
    Ok(newest.map(Slot::new))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::engine::{Durability, EngineOptions};
    use crate::meta::{ConfigDigest, KEY_CONFIG_DIGEST, KEY_SCHEMA_VERSION, SchemaVersion};
    use crate::schema::{
        ConfigDigestInput, SCHEMA_VERSION, Store, StoreOpenOptions, compute_config_digest,
    };
    use cc_types::{BlobParameters, BlobSchedule, ChainConfig, Checkpoint, Epoch, PresetName};
    use ssz::Encode;
    use ssz_types::VariableList;
    use std::collections::BTreeSet;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("cc-store-inv-{label}-{nanos}"));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn hoodi_input() -> ConfigDigestInput {
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
        let chain = ChainConfig {
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
        };
        ConfigDigestInput::with_mainnet_scalars(chain, Root::from_array([0xAB; 32]))
    }

    fn root(b: u8) -> Root {
        Root::from_array([b; 32])
    }

    fn open_opts(check: bool, node_id: Option<Root>) -> StoreOpenOptions {
        let digest = compute_config_digest(&hoodi_input()).unwrap();
        StoreOpenOptions {
            engine: EngineOptions::default().with_durability(Durability::None),
            config_digest: digest,
            check_invariants: check,
            expected_node_id: node_id,
            snapshot_ring: DEFAULT_SNAPSHOT_RING,
            max_open_scan_rows: DEFAULT_MAX_OPEN_SCAN_ROWS,
            invocation_counter: None,
        }
    }

    /// Healthy baseline that passes all eight invariants.
    struct Fixture {
        dir: PathBuf,
        engine: Option<Engine>,
        node_id: Root,
        /// When true, [`Drop`] leaves the data dir for a subsequent `Store::open`.
        keep_dir: bool,
    }

    impl Fixture {
        fn new(label: &str) -> Self {
            let dir = tmp_dir(label);
            let node_id = root(0xAA);
            let store = Store::open(&dir, open_opts(false, Some(node_id))).unwrap();
            let engine = store.into_engine();
            let mut f = Self {
                dir,
                engine: Some(engine),
                node_id,
                keep_dir: false,
            };
            f.write_healthy_baseline();
            f
        }

        fn engine(&self) -> &Engine {
            self.engine.as_ref().expect("engine still open")
        }

        /// Close the engine handle so another process/`Store::open` can attach.
        fn release_engine(&mut self) {
            self.engine.take();
            self.keep_dir = true;
        }

        fn ctx(&self) -> InvariantContext {
            InvariantContext {
                expected_node_id: Some(self.node_id),
                snapshot_ring: DEFAULT_SNAPSHOT_RING,
                max_open_scan_rows: DEFAULT_MAX_OPEN_SCAN_ROWS,
                invocation_counter: None,
            }
        }

        fn write_healthy_baseline(&mut self) {
            let engine = self.engine();
            let anchor = AnchorInfo {
                anchor_slot: Slot::new(10),
                anchor_root: root(0x10),
                anchor_state_root: root(0x11),
                node_id: self.node_id,
                oldest_block_slot: Slot::new(10),
                oldest_block_parent: root(0x09),
            };
            let split = Split {
                slot: Slot::new(8),
                state_root: root(0x81),
                block_root: root(0x82),
            };
            let window = ServeWindow {
                earliest_available_slot: Slot::new(10),
                cgc: 4,
                branch: 0,
                block_floor: Slot::new(10),
                column_floor: Slot::new(10),
                holes: VariableList::default(),
            };
            let cursor = WriteCursor {
                session_id: 1,
                seq: 1,
                slot: Slot::new(12),
                root: root(0x12),
            };
            let fc = ForkChoiceScalars {
                time: 0,
                proposer_boost_root: Root::ZERO,
                justified: Checkpoint {
                    epoch: Epoch::new(1),
                    root: root(0x01),
                },
                finalized: Checkpoint {
                    epoch: Epoch::new(1), // slot 32
                    root: root(0x02),
                },
                unrealized_justified: Checkpoint::default(),
                unrealized_finalized: Checkpoint::default(),
                head_root: root(0x12),
                head_slot: Slot::new(12),
            };
            let marks = PruneMarks {
                columns_up_to: Slot::ZERO,
                blocks_up_to: Slot::ZERO,
                states_up_to: Slot::ZERO,
                state_roots_up_to: Slot::ZERO,
            };

            let mut b = engine.batch();
            b.put(
                TABLE_META,
                KEY_ANCHOR_INFO.as_bytes(),
                &anchor.as_ssz_bytes(),
            );
            b.put(TABLE_META, KEY_SPLIT.as_bytes(), &split.as_ssz_bytes());
            b.put(
                TABLE_META,
                KEY_SERVE_WINDOW.as_bytes(),
                &window.as_ssz_bytes(),
            );
            b.put(
                TABLE_META,
                KEY_WRITE_CURSOR.as_bytes(),
                &cursor.as_ssz_bytes(),
            );
            b.put(TABLE_META, KEY_FC_SCALARS.as_bytes(), &fc.as_ssz_bytes());
            b.put(
                TABLE_META,
                KEY_PRUNE_MARKS.as_bytes(),
                &marks.as_ssz_bytes(),
            );

            // Canonical + hot blocks for slots 10,11,12 (all > split 8).
            for (slot, r) in [(10u64, root(0x10)), (11, root(0x11)), (12, root(0x12))] {
                let s = Slot::new(slot);
                b.put("canonical", &encode_cold_block_key(s), r.as_slice());
                b.put("blocks_hot", &encode_hot_block_key(s, &r), b"block-ssz");
            }
            // Snapshot at split.
            b.put(
                "snapshots",
                &encode_cold_block_key(Slot::new(8)),
                b"state-ssz",
            );
            engine.commit(b).unwrap();
        }

        fn put_meta(&self, key: &str, bytes: &[u8]) {
            let mut b = self.engine().batch();
            b.put(TABLE_META, key.as_bytes(), bytes);
            self.engine().commit(b).unwrap();
        }

        fn put_row(&self, table: &str, key: &[u8], value: &[u8]) {
            let mut b = self.engine().batch();
            b.put(table, key, value);
            self.engine().commit(b).unwrap();
        }

        fn delete_row(&self, table: &str, key: &[u8]) {
            let mut b = self.engine().batch();
            b.delete(table, key);
            self.engine().commit(b).unwrap();
        }

        fn assert_only(&self, expected: StoreInvariant) {
            let sink = CountingSink::new();
            let n = check_invariants(
                self.engine(),
                InvariantCheckMode::PostPass,
                &self.ctx(),
                Some(&sink),
            )
            .unwrap();
            assert_eq!(
                n,
                1,
                "expected exactly one violation, got {n}: {:?}",
                sink.violations()
            );
            assert_eq!(
                sink.count(expected),
                1,
                "violations={:?}",
                sink.violations()
            );
            for inv in StoreInvariant::ALL {
                if inv != expected {
                    assert_eq!(
                        sink.count(inv),
                        0,
                        "also fired {inv:?}: {:?}",
                        sink.violations()
                    );
                }
            }
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            self.engine.take();
            if !self.keep_dir {
                let _ = std::fs::remove_dir_all(&self.dir);
            }
        }
    }

    #[test]
    fn enum_variants_match_label_domain_minus_key_collision() {
        // CC-4H /1: eight variants == §10.1 invariant domain minus key_collision.
        let labels: BTreeSet<&str> = StoreInvariant::ALL.iter().map(|i| i.as_str()).collect();
        assert_eq!(
            labels,
            BTreeSet::from([
                "contig",
                "col_block",
                "split_fin",
                "ring",
                "window",
                "node_id",
                "shards",
                "cursor",
            ])
        );
        assert!(!labels.contains("key_collision"));
        assert_eq!(StoreInvariant::ALL.len(), 8);
    }

    #[test]
    fn first_uncovered_slot_merges_holes() {
        // Gap-coverage unit: contiguous merged holes fully cover; a gap is reported.
        let holes = vec![
            SlotRange {
                start: Slot::new(10),
                end: Slot::new(15),
            },
            SlotRange {
                start: Slot::new(15),
                end: Slot::new(20),
            },
        ];
        assert_eq!(first_uncovered_slot(10, 20, &holes), None);
        assert_eq!(first_uncovered_slot(10, 21, &holes), Some(20));
        assert_eq!(first_uncovered_slot(9, 12, &holes), Some(9));
        assert_eq!(first_uncovered_slot(12, 14, &holes), None);
    }

    /// SEC-4H-1: sparse canonical with span > MAX_CONTIG_WALK_SLOTS → Limit, not O(span) hang.
    #[test]
    fn contig_span_over_cap_fails_closed_limit() {
        let f = Fixture::new("contig-cap");
        // Wipe baseline canonical and plant only oldest + a head far beyond the cap.
        f.delete_row("canonical", &encode_cold_block_key(Slot::new(10)));
        f.delete_row("canonical", &encode_cold_block_key(Slot::new(11)));
        f.delete_row("canonical", &encode_cold_block_key(Slot::new(12)));
        // oldest_block_slot is 10; head at 10 + MAX_CONTIG_WALK_SLOTS (span = MAX+1).
        let head = 10u64.saturating_add(MAX_CONTIG_WALK_SLOTS);
        f.put_row(
            "canonical",
            &encode_cold_block_key(Slot::new(10)),
            root(0x10).as_slice(),
        );
        f.put_row(
            "canonical",
            &encode_cold_block_key(Slot::new(head)),
            root(0xEE).as_slice(),
        );
        // Keep cursor/newest consistent enough that only contig Limit fires as error.
        let cursor = WriteCursor {
            session_id: 1,
            seq: 1,
            slot: Slot::new(head),
            root: root(0xEE),
        };
        f.put_meta(KEY_WRITE_CURSOR, &cursor.as_ssz_bytes());
        f.put_row(
            "blocks_hot",
            &encode_hot_block_key(Slot::new(head), &root(0xEE)),
            b"far",
        );

        let err = check_invariants(f.engine(), InvariantCheckMode::Open, &f.ctx(), None)
            .expect_err("span over cap must fail closed");
        assert!(
            matches!(err, StoreError::Limit(_)),
            "expected Limit, got {err:?}"
        );
        let msg = err.to_string();
        assert!(
            msg.contains("I-contig") && msg.contains("MAX_CONTIG_WALK_SLOTS"),
            "{msg}"
        );
    }

    /// P0-18/2: a CC-4A serve window wider than the legacy `MAX_RANGE_ENTRIES`
    /// cap must complete `I-contig` (cap covers the window; walk is chunked).
    #[test]
    fn contig_check_completes_for_serve_window_wider_than_legacy_range_cap() {
        let min_epochs = 256u64 + 65_536 / 2;
        let serve_slots = block_serve_window_slots(min_epochs);
        assert!(
            serve_slots > MAX_RANGE_ENTRIES as u64,
            "legacy cap was MAX_RANGE_ENTRIES; serve window must exceed it ({serve_slots})"
        );
        assert!(
            MAX_CONTIG_WALK_SLOTS >= serve_slots,
            "walk cap {MAX_CONTIG_WALK_SLOTS} must cover serve window {serve_slots}"
        );

        let f = Fixture::new("contig-serve-window");
        f.delete_row("canonical", &encode_cold_block_key(Slot::new(10)));
        f.delete_row("canonical", &encode_cold_block_key(Slot::new(11)));
        f.delete_row("canonical", &encode_cold_block_key(Slot::new(12)));
        let oldest = 10u64;
        // Inclusive [oldest, head] span equals the configured serve window.
        let head = oldest.saturating_add(serve_slots).saturating_sub(1);
        f.put_row(
            "canonical",
            &encode_cold_block_key(Slot::new(oldest)),
            root(0x10).as_slice(),
        );
        f.put_row(
            "canonical",
            &encode_cold_block_key(Slot::new(head)),
            root(0xEE).as_slice(),
        );
        let hole = SlotRange {
            start: Slot::new(oldest.saturating_add(1)),
            end: Slot::new(head),
        };
        let window = ServeWindow {
            earliest_available_slot: Slot::new(oldest),
            cgc: 4,
            branch: 0,
            block_floor: Slot::new(oldest),
            column_floor: Slot::new(oldest),
            holes: VariableList::new(vec![hole]).expect("one hole fits"),
        };
        f.put_meta(KEY_SERVE_WINDOW, &window.as_ssz_bytes());
        let cursor = WriteCursor {
            session_id: 1,
            seq: 1,
            slot: Slot::new(head),
            root: root(0xEE),
        };
        f.put_meta(KEY_WRITE_CURSOR, &cursor.as_ssz_bytes());
        f.put_row(
            "blocks_hot",
            &encode_hot_block_key(Slot::new(head), &root(0xEE)),
            b"far",
        );

        check_invariants(f.engine(), InvariantCheckMode::Open, &f.ctx(), None)
            .expect("serve window wider than legacy MAX_RANGE_ENTRIES cap must complete");
        let n = check_invariants(f.engine(), InvariantCheckMode::PostPass, &f.ctx(), None)
            .expect("post-pass must complete");
        assert_eq!(n, 0, "recorded hole covers the sparse interior");
    }

    #[test]
    fn node_id_skipped_when_expected_none() {
        // Documented optional behaviour: no expected id → I-node-id does not fire.
        let f = Fixture::new("node-id-skip");
        let ctx = InvariantContext {
            expected_node_id: None,
            snapshot_ring: DEFAULT_SNAPSHOT_RING,
            max_open_scan_rows: DEFAULT_MAX_OPEN_SCAN_ROWS,
            invocation_counter: None,
        };
        // Mismatched anchor would fire if expected were set; with None, healthy pass.
        let n = check_invariants(f.engine(), InvariantCheckMode::PostPass, &ctx, None).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn healthy_baseline_passes() {
        let f = Fixture::new("healthy");
        let n = check_invariants(f.engine(), InvariantCheckMode::PostPass, &f.ctx(), None).unwrap();
        assert_eq!(n, 0);
        check_invariants(f.engine(), InvariantCheckMode::Open, &f.ctx(), None).unwrap();
    }

    /// Negative construction: delete canonical slot 11 → unrecorded hole (I-contig).
    #[test]
    fn violates_only_contig() {
        let f = Fixture::new("contig");
        f.delete_row("canonical", &encode_cold_block_key(Slot::new(11)));
        f.assert_only(StoreInvariant::Contig);
    }

    /// Negative construction: column at slot 10 without any block for that root (I-col-block).
    #[test]
    fn violates_only_col_block() {
        let f = Fixture::new("col-block");
        let orphan_root = root(0xEE);
        let key = crate::keys::encode_hot_column_key(Slot::new(10), &orphan_root, 0);
        f.put_row("columns_hot", &key, b"col-ssz");
        f.assert_only(StoreInvariant::ColBlock);
    }

    /// Negative construction: `blocks_hot` row at slot 5 ≤ Split.slot 8 (I-split-fin).
    #[test]
    fn violates_only_split_fin() {
        let f = Fixture::new("split-fin");
        let key = encode_hot_block_key(Slot::new(5), &root(0x55));
        f.put_row("blocks_hot", &key, b"stale-hot");
        f.assert_only(StoreInvariant::SplitFin);
    }

    /// Negative construction: empty snapshot ring while Split is present (I-ring).
    #[test]
    fn violates_only_ring() {
        let f = Fixture::new("ring");
        f.delete_row("snapshots", &encode_cold_block_key(Slot::new(8)));
        f.assert_only(StoreInvariant::Ring);
    }

    /// Negative construction: ServeWindow.earliest below oldest_block_slot (I-window).
    #[test]
    fn violates_only_window() {
        let f = Fixture::new("window");
        let window = ServeWindow {
            earliest_available_slot: Slot::new(5), // < oldest 10
            cgc: 4,
            branch: 0,
            block_floor: Slot::new(5),
            column_floor: Slot::new(5),
            holes: VariableList::default(),
        };
        f.put_meta(KEY_SERVE_WINDOW, &window.as_ssz_bytes());
        f.assert_only(StoreInvariant::Window);
    }

    /// Negative construction: AnchorInfo.node_id ≠ expected NodeId from node key (I-node-id).
    #[test]
    fn violates_only_node_id() {
        let f = Fixture::new("node-id");
        // Simulate a real key-file id: write a 32-byte secret and treat its bytes as
        // the expected id the storage process compares. Neither side is printed:
        // the stored row is itself key material.
        let key_path = f.dir.join("node_key");
        let key_bytes = [0xBBu8; 32];
        std::fs::write(&key_path, key_bytes).unwrap();
        let from_key = Root::from_array(key_bytes);
        assert_ne!(from_key, f.node_id);

        // Re-point expected_node_id to the key-file-derived id; keep mismatched AnchorInfo.
        let ctx = InvariantContext {
            expected_node_id: Some(from_key),
            snapshot_ring: DEFAULT_SNAPSHOT_RING,
            max_open_scan_rows: DEFAULT_MAX_OPEN_SCAN_ROWS,
            invocation_counter: None,
        };
        let ctx_dbg = format!("{ctx:?}");
        assert!(
            ctx_dbg.contains("<redacted>") && !ctx_dbg.contains(&from_key.to_string()),
            "InvariantContext Debug leaked the key: {ctx_dbg}"
        );
        let sink = CountingSink::new();
        let n =
            check_invariants(f.engine(), InvariantCheckMode::PostPass, &ctx, Some(&sink)).unwrap();
        assert_eq!(n, 1);
        assert_eq!(sink.count(StoreInvariant::NodeId), 1);
        let detail = &sink.violations()[0].detail;
        assert!(
            detail.contains("node_id"),
            "detail must keep the literal node_id: {detail}"
        );
        assert!(
            !detail.contains(&f.node_id.to_string()) && !detail.contains(&from_key.to_string()),
            "detail must not print stored or key-file bytes: {detail}"
        );
        // Fatal open path: Display and Debug of the error both stay redacted.
        let err = check_invariants(f.engine(), InvariantCheckMode::Open, &ctx, None).unwrap_err();
        let msg = err.to_string();
        let dbg = format!("{err:?}");
        assert!(
            matches!(
                err,
                StoreError::InvariantViolation {
                    invariant: "node_id",
                    ..
                }
            ),
            "err={dbg}"
        );
        assert!(
            msg.contains("node_id") && dbg.contains("node_id"),
            "fatal must keep the literal node_id: {msg} / {dbg}"
        );
        assert!(
            !msg.contains(&f.node_id.to_string())
                && !msg.contains(&from_key.to_string())
                && !dbg.contains(&f.node_id.to_string())
                && !dbg.contains(&from_key.to_string()),
            "fatal Display/Debug leaked key bytes: {msg} / {dbg}"
        );
        // Key file exists for the durable-surface criterion.
        assert_eq!(std::fs::read(&key_path).unwrap(), key_bytes);
    }

    /// Negative construction: block shard table fully below PruneMarks.blocks_up_to (I-shards).
    #[test]
    fn violates_only_shards() {
        let f = Fixture::new("shards");
        // Shard 0 ends at slot 256*32 = 8192. Mark prune at 8192 → table must be gone.
        let marks = PruneMarks {
            columns_up_to: Slot::ZERO,
            blocks_up_to: Slot::new(8192),
            states_up_to: Slot::ZERO,
            state_roots_up_to: Slot::ZERO,
        };
        f.put_meta(KEY_PRUNE_MARKS, &marks.as_ssz_bytes());
        // Keep window valid: earliest 10 ≥ blocks_up_to would fail I-window.
        // So also bump earliest to ≥ 8192.
        let window = ServeWindow {
            earliest_available_slot: Slot::new(8192),
            cgc: 4,
            branch: 0,
            block_floor: Slot::new(8192),
            column_floor: Slot::new(8192),
            holes: VariableList::default(),
        };
        f.put_meta(KEY_SERVE_WINDOW, &window.as_ssz_bytes());
        // Anchor oldest must be ≤ walk; for contig, raise oldest to match window.
        let anchor = AnchorInfo {
            anchor_slot: Slot::new(8192),
            anchor_root: root(0x10),
            anchor_state_root: root(0x11),
            node_id: f.node_id,
            oldest_block_slot: Slot::new(8192),
            oldest_block_parent: root(0x09),
        };
        f.put_meta(KEY_ANCHOR_INFO, &anchor.as_ssz_bytes());
        // Clear old canonical 10-12 (would be holes below oldest) and place head at 8192.
        f.delete_row("canonical", &encode_cold_block_key(Slot::new(10)));
        f.delete_row("canonical", &encode_cold_block_key(Slot::new(11)));
        f.delete_row("canonical", &encode_cold_block_key(Slot::new(12)));
        f.put_row(
            "canonical",
            &encode_cold_block_key(Slot::new(8192)),
            root(0x12).as_slice(),
        );
        // Cursor + newest block at 8192.
        let cursor = WriteCursor {
            session_id: 1,
            seq: 1,
            slot: Slot::new(8192),
            root: root(0x12),
        };
        f.put_meta(KEY_WRITE_CURSOR, &cursor.as_ssz_bytes());
        f.put_row(
            "blocks_hot",
            &encode_hot_block_key(Slot::new(8192), &root(0x12)),
            b"block",
        );
        // Drop old hot blocks that are fine, and leave a retired shard table.
        f.put_row(
            &crate::keys::blocks_shard_table(0),
            &encode_cold_block_key(Slot::new(0)),
            b"stale",
        );
        f.assert_only(StoreInvariant::Shards);
    }

    /// Negative construction: WriteCursor past newest stored block (I-cursor).
    #[test]
    fn violates_only_cursor() {
        let f = Fixture::new("cursor");
        let cursor = WriteCursor {
            session_id: 1,
            seq: 99,
            slot: Slot::new(100),
            root: root(0xFF),
        };
        f.put_meta(KEY_WRITE_CURSOR, &cursor.as_ssz_bytes());
        f.assert_only(StoreInvariant::Cursor);
    }

    /// CC-4H /3 at open: I-split-fin is fatal and names the invariant.
    #[test]
    fn open_fatal_on_split_fin() {
        let mut f = Fixture::new("open-split");
        let key = encode_hot_block_key(Slot::new(5), &root(0x55));
        f.put_row("blocks_hot", &key, b"stale-hot");
        let dir = f.dir.clone();
        let node_id = f.node_id;
        f.release_engine();
        drop(f);

        // Re-open with check_invariants = true.
        let err = Store::open(&dir, open_opts(true, Some(node_id))).unwrap_err();
        let msg = err.to_string();
        assert!(
            matches!(
                err,
                StoreError::InvariantViolation {
                    invariant: "split_fin",
                    ..
                }
            ),
            "err={err:?}"
        );
        assert!(
            msg.contains("split_fin") || msg.contains("invariant"),
            "{msg}"
        );
        // Open with flag false still succeeds (process can open but must not serve
        // under the true flag — flag false is the soak path).
        Store::open(&dir, open_opts(false, Some(node_id))).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CC-4H /3 after a pass: logs (tracing sink) + counts + returns.
    #[test]
    fn post_pass_logs_and_counts_split_fin() {
        let f = Fixture::new("post-pass");
        let key = encode_hot_block_key(Slot::new(5), &root(0x55));
        f.put_row("blocks_hot", &key, b"stale-hot");

        let counter = CountingSink::new();
        let tracing = TracingSink;
        let fan = FanoutSink {
            a: &counter,
            b: &tracing,
        };
        let n = check_invariants(
            f.engine(),
            InvariantCheckMode::PostPass,
            &f.ctx(),
            Some(&fan),
        )
        .expect("post-pass must return, not abort");
        assert_eq!(n, 1);
        assert_eq!(counter.count(StoreInvariant::SplitFin), 1);
        assert_eq!(counter.violations().len(), 1);
        assert_eq!(counter.violations()[0].invariant, StoreInvariant::SplitFin);
    }

    /// CC-4H /2: gated path runs at open + migration + prune when enabled; never when disabled.
    #[test]
    fn gated_invocations_respect_flag() {
        let mut f = Fixture::new("gate");
        let counter = Arc::new(AtomicU64::new(0));
        let ctx = InvariantContext {
            expected_node_id: Some(f.node_id),
            snapshot_ring: DEFAULT_SNAPSHOT_RING,
            max_open_scan_rows: DEFAULT_MAX_OPEN_SCAN_ROWS,
            invocation_counter: Some(Arc::clone(&counter)),
        };

        // disabled: three call sites (open / migration / prune), zero invocations.
        for mode in [
            InvariantCheckMode::Open,
            InvariantCheckMode::PostPass,
            InvariantCheckMode::PostPass,
        ] {
            run_invariant_checks_if_enabled(f.engine(), false, mode, &ctx, None).unwrap();
        }
        assert_eq!(counter.load(Ordering::SeqCst), 0);

        // enabled via free functions: open + migration + prune = 3.
        run_invariant_checks_if_enabled(f.engine(), true, InvariantCheckMode::Open, &ctx, None)
            .unwrap();
        run_invariant_checks_if_enabled(f.engine(), true, InvariantCheckMode::PostPass, &ctx, None)
            .unwrap();
        run_invariant_checks_if_enabled(f.engine(), true, InvariantCheckMode::PostPass, &ctx, None)
            .unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 3);

        // Store::after_* hooks honor the flag stored on open.
        let dir = f.dir.clone();
        let node_id = f.node_id;
        f.release_engine();
        drop(f);

        let counter_on = Arc::new(AtomicU64::new(0));
        let store = Store::open(
            &dir,
            open_opts(true, Some(node_id)).with_invocation_counter(Arc::clone(&counter_on)),
        )
        .unwrap();
        // open itself counted once.
        assert_eq!(counter_on.load(Ordering::SeqCst), 1);
        store.after_migration_pass(None).unwrap();
        store.after_prune_pass(None).unwrap();
        assert_eq!(counter_on.load(Ordering::SeqCst), 3);

        drop(store);
        let counter_off = Arc::new(AtomicU64::new(0));
        let store_off = Store::open(
            &dir,
            open_opts(false, Some(node_id)).with_invocation_counter(Arc::clone(&counter_off)),
        )
        .unwrap();
        assert_eq!(counter_off.load(Ordering::SeqCst), 0);
        store_off.after_migration_pass(None).unwrap();
        store_off.after_prune_pass(None).unwrap();
        assert_eq!(counter_off.load(Ordering::SeqCst), 0);
        drop(store_off);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn fresh_open_with_check_invariants_succeeds() {
        let dir = tmp_dir("fresh-check");
        Store::open(&dir, open_opts(true, None)).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn schema_bootstrap_meta_still_present_after_open() {
        // Sanity: open path still writes schema markers (regression guard).
        let dir = tmp_dir("schema-meta");
        let store = Store::open(&dir, open_opts(true, None)).unwrap();
        let rt = store.engine().read().unwrap();
        let sv = SchemaVersion::from_ssz_bytes(
            &rt.get(TABLE_META, KEY_SCHEMA_VERSION.as_bytes())
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(sv.version, SCHEMA_VERSION);
        let _ = ConfigDigest::from_ssz_bytes(
            &rt.get(TABLE_META, KEY_CONFIG_DIGEST.as_bytes())
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        drop(rt);
        drop(store);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn injected_open_scan_budget_is_limit_not_wait() {
        let mut f = Fixture::new("scan-budget");
        let ctx = f.ctx().with_max_open_scan_rows(1);
        let err = check_invariants(f.engine(), InvariantCheckMode::Open, &ctx, None)
            .expect_err("budget of 1 must fail closed on a healthy multi-row fixture");
        assert!(
            matches!(err, StoreError::Limit(_)),
            "expected Limit, got {err:?}"
        );
        let msg = err.to_string();
        assert!(
            msg.contains("max_rows") || msg.contains("max_open_scan_rows"),
            "{msg}"
        );

        let dir = f.dir.clone();
        let node_id = f.node_id;
        f.release_engine();
        drop(f);

        let err = Store::open(
            &dir,
            open_opts(true, Some(node_id)).with_max_open_scan_rows(1),
        )
        .expect_err("Store::open must honour the injected scan budget");
        assert!(
            matches!(err, StoreError::Limit(_)),
            "expected Limit from open, got {err:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Canonical span > legacy 1_048_576 and ≤ the named default must complete.
    /// Sparse: two rows + a recorded hole (not a million-row insert).
    #[test]
    fn per_check_budget_admits_healthy_span_above_legacy_range_cap() {
        let mut f = Fixture::new("scan-budget-span");
        f.delete_row("canonical", &encode_cold_block_key(Slot::new(10)));
        f.delete_row("canonical", &encode_cold_block_key(Slot::new(11)));
        f.delete_row("canonical", &encode_cold_block_key(Slot::new(12)));
        let oldest = 10u64;
        let span = (MAX_RANGE_ENTRIES as u64).saturating_add(1);
        assert!(span <= DEFAULT_MAX_OPEN_SCAN_ROWS);
        let head = oldest.saturating_add(span).saturating_sub(1);
        f.put_row(
            "canonical",
            &encode_cold_block_key(Slot::new(oldest)),
            root(0x10).as_slice(),
        );
        f.put_row(
            "canonical",
            &encode_cold_block_key(Slot::new(head)),
            root(0xEE).as_slice(),
        );
        let hole = SlotRange {
            start: Slot::new(oldest.saturating_add(1)),
            end: Slot::new(head),
        };
        let window = ServeWindow {
            earliest_available_slot: Slot::new(oldest),
            cgc: 4,
            branch: 0,
            block_floor: Slot::new(oldest),
            column_floor: Slot::new(oldest),
            holes: VariableList::new(vec![hole]).expect("one hole fits"),
        };
        f.put_meta(KEY_SERVE_WINDOW, &window.as_ssz_bytes());
        let cursor = WriteCursor {
            session_id: 1,
            seq: 1,
            slot: Slot::new(head),
            root: root(0xEE),
        };
        f.put_meta(KEY_WRITE_CURSOR, &cursor.as_ssz_bytes());
        f.put_row(
            "blocks_hot",
            &encode_hot_block_key(Slot::new(head), &root(0xEE)),
            b"far",
        );

        check_invariants(f.engine(), InvariantCheckMode::Open, &f.ctx(), None)
            .expect("healthy span > 1_048_576 and ≤ DEFAULT_MAX_OPEN_SCAN_ROWS must complete");

        let dir = f.dir.clone();
        let node_id = f.node_id;
        f.release_engine();
        drop(f);
        Store::open(&dir, open_opts(true, Some(node_id)))
            .expect("Store::open with the named default must admit this span");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Manual wall-clock: ~540 MiB of 30 KiB column values (not a 28 GiB supernode).
    /// Run: `cargo test -p cc-store --offline --locked --lib measure_open_scan -- --ignored --nocapture`
    #[test]
    #[ignore = "manual open-scan wall-clock; not CI"]
    fn measure_open_scan_column_byte_cap() {
        let f = Fixture::new("scan-measure");
        let payload = vec![0xABu8; 30_000];
        const N: u64 = 18_000;
        let mut b = f.engine().batch();
        let mut n = 0usize;
        for i in 0..N {
            let key = crate::keys::encode_hot_column_key(Slot::new(100 + i), &root(0xEE), 0);
            b.put("columns_hot", &key, &payload);
            n += 1;
            if n >= 512 {
                f.engine()
                    .commit(std::mem::replace(&mut b, f.engine().batch()))
                    .unwrap();
                n = 0;
            }
        }
        if n > 0 {
            f.engine().commit(b).unwrap();
        }
        let t0 = std::time::Instant::now();
        let res = check_invariants(f.engine(), InvariantCheckMode::Open, &f.ctx(), None);
        let elapsed = t0.elapsed();
        eprintln!(
            "s2-b-06 measure: columns_hot={N} value_bytes=30000 elapsed_ms={} outcome={}",
            elapsed.as_millis(),
            match &res {
                Ok(n) => format!("Ok({n})"),
                Err(e) => format!("{e}"),
            }
        );
        assert!(res.is_err(), "expected Limit or violation, got {res:?}");
    }
}
