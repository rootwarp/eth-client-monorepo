//! Storage service Prometheus metrics (CC-4Ca / Architecture §10.1–10.2).
//!
//! Registered into [`cc_bootstrap::Bootstrap::registry`] between `init` and
//! `serve` — the Phase 0 §4.1 seam. **No producers** in this issue: families
//! are declared, bucket boundaries come from [`cc_store::buckets`] by name
//! (§10.2), and labelled series are seeded to zero so exposition always emits
//! HELP/TYPE (prometheus-client omits empty families).
//!
//! `cc_chain_event_buffer_*` gauges are **not** declared here — they are
//! CC-44a's, in `services/chain`.

use cc_store::buckets;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::{Registry, Unit};

// ── label sets (§10.1 closed domains) ───────────────────────────────────────
//
// SEC-4Ca-1 / SEC-4Ca-3: label fields are `String` for prometheus-client's
// `EncodeLabelSet` ergonomics (same pattern as chain/p2p/engine). **Producers
// MUST only pass allowlisted / fixed enum values** from the closed domains in
// this module (`StorageClass::as_str()`, `PrunePass::as_str()`,
// `ServeProtocol::as_str()`, `ServeResult::as_str()`, `RestartPhase::as_str()`,
// `SnapshotPhase::as_str()`, `ReconnectReason::as_str()`, `Invariant::as_str()`).
// Unbounded or peer-supplied strings explode cardinality (SEC-4Ca-1) and can
// inject label-syntax noise if not constrained at the producer (SEC-4Ca-3).
// Prefer the fixed enums; do not invent catch-all `"other"` buckets for closed
// sets unless Architecture §10.1 adds them.

/// Labels for class-scoped storage families.
///
/// Closed domain for size/prune/write **data** classes is [`StorageClass`].
/// Writer mailbox depth / chunk-drop series also use [`WriterPriority`]
/// (`p0`|`p1`|`p2`) per R-10 (`cc_storage_writer_queue_depth{class="p0"}`).
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub(crate) struct ClassLabels {
    pub class: String,
}

/// Writer priority-mailbox class labels (Architecture §1.5 / R-10).
///
/// Distinct from [`StorageClass`] (blocks|columns|…): these label the three
/// submit queues in front of the single writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum WriterPriority {
    P0,
    P1,
    P2,
}

impl WriterPriority {
    /// Prometheus label value.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::P0 => "p0",
            Self::P1 => "p1",
            Self::P2 => "p2",
        }
    }

    /// All variants (seed + tests).
    pub(crate) const ALL: [Self; 3] = [Self::P0, Self::P1, Self::P2];
}

/// Labels for prune-pass families.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub(crate) struct PassLabels {
    pub pass: String,
}

/// Labels for protocol-only serve families.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub(crate) struct ProtocolLabels {
    pub protocol: String,
}

/// Labels for `cc_storage_serve_total`.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub(crate) struct ServeLabels {
    pub protocol: String,
    pub result: String,
}

/// Labels for restart-phase histogram.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub(crate) struct RestartPhaseLabels {
    pub phase: String,
}

/// Labels for snapshot-phase histogram.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub(crate) struct SnapshotPhaseLabels {
    pub phase: String,
}

/// Labels for stream reconnect counter.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub(crate) struct ReasonLabels {
    pub reason: String,
}

/// Labels for invariant violation counter.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub(crate) struct InvariantLabels {
    pub invariant: String,
}

// ── fixed label enums (closed; no unbounded external strings) ───────────────

/// `class` label values (§10.1) — cardinality 5.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum StorageClass {
    Blocks,
    Columns,
    Snapshots,
    Index,
    Meta,
}

impl StorageClass {
    /// Prometheus label value.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Blocks => "blocks",
            Self::Columns => "columns",
            Self::Snapshots => "snapshots",
            Self::Index => "index",
            Self::Meta => "meta",
        }
    }

    /// All variants (seed + tests).
    pub(crate) const ALL: [Self; 5] = [
        Self::Blocks,
        Self::Columns,
        Self::Snapshots,
        Self::Index,
        Self::Meta,
    ];
}

/// `pass` label values for prune families (§10.1) — cardinality 5.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum PrunePass {
    Columns,
    Blocks,
    StateRoots,
    Snapshots,
    Unfinalized,
}

impl PrunePass {
    /// Prometheus label value.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Columns => "columns",
            Self::Blocks => "blocks",
            Self::StateRoots => "state_roots",
            Self::Snapshots => "snapshots",
            Self::Unfinalized => "unfinalized",
        }
    }

    /// All variants (seed + tests).
    pub(crate) const ALL: [Self; 5] = [
        Self::Columns,
        Self::Blocks,
        Self::StateRoots,
        Self::Snapshots,
        Self::Unfinalized,
    ];
}

/// `protocol` label values for serve families (§10.1 + CC-4I).
///
/// Base five from Architecture §10.1; CC-4I appends the Phase 6 historical
/// surface so `cc_storage_serve_seconds{protocol}` covers the 10–50 ms budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ServeProtocol {
    BlocksByRange,
    BlocksByRoot,
    BlocksByHead,
    ColumnsByRange,
    ColumnsByRoot,
    /// `GetHistoricalBlock` by root (CC-4I).
    HistoricalBlockByRoot,
    /// `GetHistoricalBlock` by slot (CC-4I).
    HistoricalBlockBySlot,
    /// `GetSnapshotState` stream (CC-4I).
    SnapshotState,
    /// `GetFinalizedCheckpointHistory` (CC-4I).
    FinalizedCheckpointHistory,
}

impl ServeProtocol {
    /// Prometheus label value.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::BlocksByRange => "blocks_by_range",
            Self::BlocksByRoot => "blocks_by_root",
            Self::BlocksByHead => "blocks_by_head",
            Self::ColumnsByRange => "columns_by_range",
            Self::ColumnsByRoot => "columns_by_root",
            Self::HistoricalBlockByRoot => "historical_block_by_root",
            Self::HistoricalBlockBySlot => "historical_block_by_slot",
            Self::SnapshotState => "snapshot_state",
            Self::FinalizedCheckpointHistory => "finalized_checkpoint_history",
        }
    }

    /// All variants (seed + tests).
    pub(crate) const ALL: [Self; 9] = [
        Self::BlocksByRange,
        Self::BlocksByRoot,
        Self::BlocksByHead,
        Self::ColumnsByRange,
        Self::ColumnsByRoot,
        Self::HistoricalBlockByRoot,
        Self::HistoricalBlockBySlot,
        Self::SnapshotState,
        Self::FinalizedCheckpointHistory,
    ];
}

/// `result` label values for `cc_storage_serve_total` (§10.1) — cardinality 4.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ServeResult {
    Ok,
    ResourceUnavailable,
    RateLimited,
    Error,
}

impl ServeResult {
    /// Prometheus label value.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::ResourceUnavailable => "resource_unavailable",
            Self::RateLimited => "rate_limited",
            Self::Error => "error",
        }
    }

    /// All variants (seed + tests).
    pub(crate) const ALL: [Self; 4] = [
        Self::Ok,
        Self::ResourceUnavailable,
        Self::RateLimited,
        Self::Error,
    ];
}

/// `phase` label values for `cc_storage_restart_seconds` (§10.1 / CC-45/8).
///
/// Seven values including `restore_send` and `schema_check` (*Deviations* 7).
/// `chain_replay` is spelled to distinguish storage's own P2 replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum RestartPhase {
    Open,
    SchemaCheck,
    SnapshotLoad,
    RestoreSend,
    ChainReplay,
    ForkchoiceRebuild,
    Resubscribe,
}

impl RestartPhase {
    /// Prometheus label value.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::SchemaCheck => "schema_check",
            Self::SnapshotLoad => "snapshot_load",
            Self::RestoreSend => "restore_send",
            Self::ChainReplay => "chain_replay",
            Self::ForkchoiceRebuild => "forkchoice_rebuild",
            Self::Resubscribe => "resubscribe",
        }
    }

    /// All seven variants (seed + tests). Order matches §10.1 prose.
    pub(crate) const ALL: [Self; 7] = [
        Self::Open,
        Self::SchemaCheck,
        Self::SnapshotLoad,
        Self::RestoreSend,
        Self::ChainReplay,
        Self::ForkchoiceRebuild,
        Self::Resubscribe,
    ];
}

/// `phase` label values for `cc_storage_snapshot_seconds` (§10.1) — cardinality 4.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum SnapshotPhase {
    Replay,
    Serialize,
    Write,
    Load,
}

impl SnapshotPhase {
    /// Prometheus label value.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Replay => "replay",
            Self::Serialize => "serialize",
            Self::Write => "write",
            Self::Load => "load",
        }
    }

    /// All variants (seed + tests).
    pub(crate) const ALL: [Self; 4] = [Self::Replay, Self::Serialize, Self::Write, Self::Load];
}

/// `reason` label values for `cc_storage_stream_reconnect_total` (§10.1) — cardinality 4.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum ReconnectReason {
    CursorTooOld,
    CursorUnknownSession,
    ResourceExhausted,
    Transport,
}

impl ReconnectReason {
    /// Prometheus label value.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::CursorTooOld => "cursor_too_old",
            Self::CursorUnknownSession => "cursor_unknown_session",
            Self::ResourceExhausted => "resource_exhausted",
            Self::Transport => "transport",
        }
    }

    /// All variants (seed + tests).
    pub(crate) const ALL: [Self; 4] = [
        Self::CursorTooOld,
        Self::CursorUnknownSession,
        Self::ResourceExhausted,
        Self::Transport,
    ];
}

/// `invariant` label values for `cc_storage_invariant_violation_total` (§10.1) — cardinality 9.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Invariant {
    Contig,
    ColBlock,
    SplitFin,
    Ring,
    Window,
    NodeId,
    Shards,
    Cursor,
    KeyCollision,
}

impl Invariant {
    /// Prometheus label value.
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Contig => "contig",
            Self::ColBlock => "col_block",
            Self::SplitFin => "split_fin",
            Self::Ring => "ring",
            Self::Window => "window",
            Self::NodeId => "node_id",
            Self::Shards => "shards",
            Self::Cursor => "cursor",
            Self::KeyCollision => "key_collision",
        }
    }

    /// All variants (seed + tests).
    pub(crate) const ALL: [Self; 9] = [
        Self::Contig,
        Self::ColBlock,
        Self::SplitFin,
        Self::Ring,
        Self::Window,
        Self::NodeId,
        Self::Shards,
        Self::Cursor,
        Self::KeyCollision,
    ];
}

// ── metric handles ──────────────────────────────────────────────────────────

/// All storage §10.1 metric families (CC-4Ca).
///
/// Cheap to clone (each field is a handle into shared series storage).
/// Producers for each family land in the requirement that owns them — this
/// issue only declares and seeds.
#[derive(Debug, Clone)]
#[allow(dead_code)] // fields held for registration handles; producers land later
pub struct StorageMetrics {
    // Size and growth
    pub(crate) bytes_total: Family<ClassLabels, Gauge>,
    pub(crate) rows_total: Family<ClassLabels, Gauge>,
    pub(crate) disk_bytes: Gauge,
    pub(crate) live_set_bytes: Gauge,
    pub(crate) tables_total: Family<ClassLabels, Gauge>,
    // Pruning
    pub(crate) pruned_bytes: Family<ClassLabels, Counter>,
    pub(crate) pruned_rows: Family<ClassLabels, Counter>,
    pub(crate) prune_seconds: Family<PassLabels, Histogram>,
    pub(crate) prune_lag_epochs: Family<ClassLabels, Gauge>,
    pub(crate) prune_deadline_exceeded: Family<PassLabels, Counter>,
    pub(crate) shard_dropped: Family<ClassLabels, Counter>,
    // Write path
    pub(crate) commit_seconds: Family<ClassLabels, Histogram>,
    /// Wall time `commit_import` waits on the P0 writer, including a stall.
    pub(crate) commit_wait_seconds: Histogram,
    pub(crate) written_bytes: Family<ClassLabels, Counter>,
    pub(crate) write_behind_lag_slots: Histogram,
    pub(crate) stream_reconnect: Family<ReasonLabels, Counter>,
    pub(crate) writer_queue_depth: Family<ClassLabels, Gauge>,
    pub(crate) writer_chunk_dropped: Family<ClassLabels, Counter>,
    // Read path
    pub(crate) read_txn_seconds: Histogram,
    pub(crate) serve_seconds: Family<ProtocolLabels, Histogram>,
    pub(crate) serve_total: Family<ServeLabels, Counter>,
    pub(crate) serve_bytes: Family<ProtocolLabels, Counter>,
    pub(crate) serve_admission_wait_seconds: Histogram,
    // Restart
    pub(crate) restart_seconds: Family<RestartPhaseLabels, Histogram>,
    pub(crate) following_head: Gauge,
    pub(crate) replay_divergence: Counter,
    // Snapshots
    pub(crate) snapshot_seconds: Family<SnapshotPhaseLabels, Histogram>,
    pub(crate) snapshot_bytes: Gauge,
    pub(crate) snapshot_ring_depth: Gauge,
    // Windows
    pub(crate) earliest_available_slot: Gauge,
    pub(crate) window_branch: Gauge,
    pub(crate) window_hole_slots: Gauge,
    pub(crate) window_increase_rejected: Counter,
    pub(crate) backfill_oldest_slot: Family<ClassLabels, Gauge>,
    pub(crate) backfill_bytes: Family<ClassLabels, Counter>,
    pub(crate) split_slot: Gauge,
    // Health
    pub(crate) invariant_violation: Family<InvariantLabels, Counter>,
    pub(crate) key_collision: Counter,
}

impl StorageMetrics {
    /// Create and register every §10.1 storage family on `registry`.
    ///
    /// Call between [`cc_bootstrap::init`] and [`cc_bootstrap::serve`]. Seeds
    /// labelled series so exposition always emits HELP/TYPE.
    ///
    /// Histogram buckets are taken from [`cc_store::buckets`] by name — no
    /// literal boundary arrays live in this module.
    pub fn register(registry: &mut Registry) -> Self {
        // ── construct ───────────────────────────────────────────────────────
        let bytes_total = Family::<ClassLabels, Gauge>::default();
        let rows_total = Family::<ClassLabels, Gauge>::default();
        let disk_bytes = Gauge::default();
        let live_set_bytes = Gauge::default();
        let tables_total = Family::<ClassLabels, Gauge>::default();

        let pruned_bytes = Family::<ClassLabels, Counter>::default();
        let pruned_rows = Family::<ClassLabels, Counter>::default();
        let prune_seconds = Family::<PassLabels, Histogram>::new_with_constructor(|| {
            Histogram::new(buckets::PRUNE_SECONDS.iter().copied())
        });
        let prune_lag_epochs = Family::<ClassLabels, Gauge>::default();
        let prune_deadline_exceeded = Family::<PassLabels, Counter>::default();
        let shard_dropped = Family::<ClassLabels, Counter>::default();

        let commit_seconds = Family::<ClassLabels, Histogram>::new_with_constructor(|| {
            Histogram::new(buckets::COMMIT_SECONDS.iter().copied())
        });
        let commit_wait_seconds = Histogram::new(buckets::COMMIT_WAIT_SECONDS.iter().copied());
        let written_bytes = Family::<ClassLabels, Counter>::default();
        let write_behind_lag_slots =
            Histogram::new(buckets::WRITE_BEHIND_LAG_SLOTS.iter().copied());
        let stream_reconnect = Family::<ReasonLabels, Counter>::default();
        let writer_queue_depth = Family::<ClassLabels, Gauge>::default();
        let writer_chunk_dropped = Family::<ClassLabels, Counter>::default();

        let read_txn_seconds = Histogram::new(buckets::READ_TXN_SECONDS.iter().copied());
        let serve_seconds = Family::<ProtocolLabels, Histogram>::new_with_constructor(|| {
            Histogram::new(buckets::SERVE_SECONDS.iter().copied())
        });
        let serve_total = Family::<ServeLabels, Counter>::default();
        let serve_bytes = Family::<ProtocolLabels, Counter>::default();
        let serve_admission_wait_seconds =
            Histogram::new(buckets::SERVE_ADMISSION_WAIT_SECONDS.iter().copied());

        let restart_seconds = Family::<RestartPhaseLabels, Histogram>::new_with_constructor(|| {
            Histogram::new(buckets::RESTART_SECONDS.iter().copied())
        });
        let following_head = Gauge::default();
        let replay_divergence = Counter::default();

        let snapshot_seconds =
            Family::<SnapshotPhaseLabels, Histogram>::new_with_constructor(|| {
                Histogram::new(buckets::SNAPSHOT_SECONDS.iter().copied())
            });
        let snapshot_bytes = Gauge::default();
        let snapshot_ring_depth = Gauge::default();

        let earliest_available_slot = Gauge::default();
        let window_branch = Gauge::default();
        let window_hole_slots = Gauge::default();
        let window_increase_rejected = Counter::default();
        let backfill_oldest_slot = Family::<ClassLabels, Gauge>::default();
        let backfill_bytes = Family::<ClassLabels, Counter>::default();
        let split_slot = Gauge::default();

        let invariant_violation = Family::<InvariantLabels, Counter>::default();
        let key_collision = Counter::default();

        // ── register (OpenMetrics appends `_total` for counters) ────────────
        // Size and growth — gauges (current size; `_total` is part of the name).
        registry.register(
            "cc_storage_bytes_total",
            "Stored bytes by class (blocks|columns|snapshots|index|meta)",
            bytes_total.clone(),
        );
        registry.register(
            "cc_storage_rows_total",
            "Stored rows by class",
            rows_total.clone(),
        );
        registry.register(
            "cc_storage_disk_bytes",
            "On-disk store size in bytes (includes free pages)",
            disk_bytes.clone(),
        );
        registry.register(
            "cc_storage_live_set_bytes",
            "Live-set (reachable) store size in bytes",
            live_set_bytes.clone(),
        );
        registry.register(
            "cc_storage_tables_total",
            "Open tables by class",
            tables_total.clone(),
        );

        // Pruning
        registry.register(
            "cc_storage_pruned_bytes",
            "Bytes pruned by class (cumulative)",
            pruned_bytes.clone(),
        );
        registry.register(
            "cc_storage_pruned_rows",
            "Rows pruned by class (cumulative)",
            pruned_rows.clone(),
        );
        registry.register_with_unit(
            "cc_storage_prune",
            "Wall time of a prune pass (pass; boundary at 2.0 s)",
            Unit::Seconds,
            prune_seconds.clone(),
        );
        registry.register(
            "cc_storage_prune_lag_epochs",
            "Prune lag in epochs by class",
            prune_lag_epochs.clone(),
        );
        registry.register(
            "cc_storage_prune_deadline_exceeded",
            "Prune passes that exceeded the deadline (pass)",
            prune_deadline_exceeded.clone(),
        );
        registry.register(
            "cc_storage_shard_dropped",
            "Shards dropped by class (cumulative)",
            shard_dropped.clone(),
        );

        // Write path
        registry.register_with_unit(
            "cc_storage_commit",
            "Wall time of a store commit (class; boundary at 0.5 s)",
            Unit::Seconds,
            commit_seconds.clone(),
        );
        registry.register_with_unit(
            "cc_storage_commit_wait",
            "Wall time commit_import waits on the P0 writer. Expiry at 2 slots is a fail-closed abort, not backpressure (ADR-R-08)",
            Unit::Seconds,
            commit_wait_seconds.clone(),
        );
        registry.register(
            "cc_storage_written_bytes",
            "Bytes written by class (cumulative)",
            written_bytes.clone(),
        );
        registry.register(
            "cc_storage_write_behind_lag_slots",
            "Write-behind lag in slots (boundary at 1)",
            write_behind_lag_slots.clone(),
        );
        registry.register(
            "cc_storage_stream_reconnect",
            "Stream reconnects by reason (cursor_too_old|cursor_unknown_session|resource_exhausted|transport)",
            stream_reconnect.clone(),
        );
        registry.register(
            "cc_storage_writer_queue_depth",
            "Writer queue depth by class",
            writer_queue_depth.clone(),
        );
        registry.register(
            "cc_storage_writer_chunk_dropped",
            "Writer chunks dropped by class (cumulative)",
            writer_chunk_dropped.clone(),
        );

        // Read path
        registry.register_with_unit(
            "cc_storage_read_txn",
            "Wall time of a read transaction (boundary at 1.0 s)",
            Unit::Seconds,
            read_txn_seconds.clone(),
        );
        registry.register_with_unit(
            "cc_storage_serve",
            "Wall time of a serve request (protocol; boundary at 0.15 s)",
            Unit::Seconds,
            serve_seconds.clone(),
        );
        registry.register(
            "cc_storage_serve",
            "Serve outcomes (protocol, result=ok|resource_unavailable|rate_limited|error)",
            serve_total.clone(),
        );
        registry.register(
            "cc_storage_serve_bytes",
            "Bytes served by protocol (cumulative)",
            serve_bytes.clone(),
        );
        registry.register_with_unit(
            "cc_storage_serve_admission_wait",
            "Serve admission wait time (boundary at 2.0 s)",
            Unit::Seconds,
            serve_admission_wait_seconds.clone(),
        );

        // Restart
        registry.register_with_unit(
            "cc_storage_restart",
            "Wall time of restart phases (phase=open|schema_check|snapshot_load|restore_send|chain_replay|forkchoice_rebuild|resubscribe)",
            Unit::Seconds,
            restart_seconds.clone(),
        );
        registry.register(
            "cc_storage_following_head",
            "Storage is following chain head (0/1)",
            following_head.clone(),
        );
        registry.register(
            "cc_storage_replay_divergence",
            "Replay divergence detections (cumulative)",
            replay_divergence.clone(),
        );

        // Snapshots
        registry.register_with_unit(
            "cc_storage_snapshot",
            "Wall time of snapshot phases (phase=replay|serialize|write|load)",
            Unit::Seconds,
            snapshot_seconds.clone(),
        );
        registry.register(
            "cc_storage_snapshot_bytes",
            "Latest snapshot size in bytes",
            snapshot_bytes.clone(),
        );
        registry.register(
            "cc_storage_snapshot_ring_depth",
            "Snapshot ring depth (count of retained snapshots)",
            snapshot_ring_depth.clone(),
        );

        // Windows
        registry.register(
            "cc_storage_earliest_available_slot",
            "Earliest slot available from storage",
            earliest_available_slot.clone(),
        );
        registry.register(
            "cc_storage_window_branch",
            "Active window branch identifier",
            window_branch.clone(),
        );
        registry.register(
            "cc_storage_window_hole_slots",
            "Hole slots inside the retention window",
            window_hole_slots.clone(),
        );
        registry.register(
            "cc_storage_window_increase_rejected",
            "Window-size increases rejected (cumulative)",
            window_increase_rejected.clone(),
        );
        registry.register(
            "cc_storage_backfill_oldest_slot",
            "Oldest backfill slot by class",
            backfill_oldest_slot.clone(),
        );
        registry.register(
            "cc_storage_backfill_bytes",
            "Bytes backfilled by class (cumulative)",
            backfill_bytes.clone(),
        );
        registry.register(
            "cc_storage_split_slot",
            "Current split slot",
            split_slot.clone(),
        );

        // Health
        registry.register(
            "cc_storage_invariant_violation",
            "Invariant violations (invariant; closed domain §10.1)",
            invariant_violation.clone(),
        );
        registry.register(
            "cc_storage_key_collision",
            "Key collisions detected (cumulative)",
            key_collision.clone(),
        );

        let metrics = Self {
            bytes_total,
            rows_total,
            disk_bytes,
            live_set_bytes,
            tables_total,
            pruned_bytes,
            pruned_rows,
            prune_seconds,
            prune_lag_epochs,
            prune_deadline_exceeded,
            shard_dropped,
            commit_seconds,
            commit_wait_seconds,
            written_bytes,
            write_behind_lag_slots,
            stream_reconnect,
            writer_queue_depth,
            writer_chunk_dropped,
            read_txn_seconds,
            serve_seconds,
            serve_total,
            serve_bytes,
            serve_admission_wait_seconds,
            restart_seconds,
            following_head,
            replay_divergence,
            snapshot_seconds,
            snapshot_bytes,
            snapshot_ring_depth,
            earliest_available_slot,
            window_branch,
            window_hole_slots,
            window_increase_rejected,
            backfill_oldest_slot,
            backfill_bytes,
            split_slot,
            invariant_violation,
            key_collision,
        };
        metrics.seed_exposition();
        metrics
    }

    /// Ensure every labelled family has its fixed series so HELP/TYPE appear
    /// and soak queries return 0 rather than absent.
    fn seed_exposition(&self) {
        for class in StorageClass::ALL {
            let labels = ClassLabels {
                class: class.as_str().to_owned(),
            };
            self.bytes_total.get_or_create(&labels).set(0);
            self.rows_total.get_or_create(&labels).set(0);
            self.tables_total.get_or_create(&labels).set(0);
            let _ = self.pruned_bytes.get_or_create(&labels).get();
            let _ = self.pruned_rows.get_or_create(&labels).get();
            self.prune_lag_epochs.get_or_create(&labels).set(0);
            let _ = self.shard_dropped.get_or_create(&labels).get();
            self.commit_seconds.get_or_create(&labels).observe(0.0);
            let _ = self.written_bytes.get_or_create(&labels).get();
            // Chunk-drop also labels by storage class (prune/backfill drops).
            let _ = self.writer_chunk_dropped.get_or_create(&labels).get();
            self.backfill_oldest_slot.get_or_create(&labels).set(0);
            let _ = self.backfill_bytes.get_or_create(&labels).get();
        }
        self.commit_wait_seconds.observe(0.0);

        // Writer mailbox depth / drops by priority class (R-10: p0 must stay 0).
        for pri in WriterPriority::ALL {
            let labels = ClassLabels {
                class: pri.as_str().to_owned(),
            };
            self.writer_queue_depth.get_or_create(&labels).set(0);
            let _ = self.writer_chunk_dropped.get_or_create(&labels).get();
        }

        self.disk_bytes.set(0);
        self.live_set_bytes.set(0);

        for pass in PrunePass::ALL {
            let labels = PassLabels {
                pass: pass.as_str().to_owned(),
            };
            self.prune_seconds.get_or_create(&labels).observe(0.0);
            let _ = self.prune_deadline_exceeded.get_or_create(&labels).get();
        }

        self.write_behind_lag_slots.observe(0.0);
        for reason in ReconnectReason::ALL {
            let _ = self
                .stream_reconnect
                .get_or_create(&ReasonLabels {
                    reason: reason.as_str().to_owned(),
                })
                .get();
        }

        self.read_txn_seconds.observe(0.0);
        self.serve_admission_wait_seconds.observe(0.0);
        for protocol in ServeProtocol::ALL {
            let pl = ProtocolLabels {
                protocol: protocol.as_str().to_owned(),
            };
            self.serve_seconds.get_or_create(&pl).observe(0.0);
            let _ = self.serve_bytes.get_or_create(&pl).get();
            for result in ServeResult::ALL {
                let _ = self
                    .serve_total
                    .get_or_create(&ServeLabels {
                        protocol: protocol.as_str().to_owned(),
                        result: result.as_str().to_owned(),
                    })
                    .get();
            }
        }

        for phase in RestartPhase::ALL {
            self.restart_seconds
                .get_or_create(&RestartPhaseLabels {
                    phase: phase.as_str().to_owned(),
                })
                .observe(0.0);
        }
        self.following_head.set(0);
        let _ = self.replay_divergence.get();

        for phase in SnapshotPhase::ALL {
            self.snapshot_seconds
                .get_or_create(&SnapshotPhaseLabels {
                    phase: phase.as_str().to_owned(),
                })
                .observe(0.0);
        }
        self.snapshot_bytes.set(0);
        self.snapshot_ring_depth.set(0);

        self.earliest_available_slot.set(0);
        self.window_branch.set(0);
        self.window_hole_slots.set(0);
        let _ = self.window_increase_rejected.get();
        self.split_slot.set(0);

        for inv in Invariant::ALL {
            let _ = self
                .invariant_violation
                .get_or_create(&InvariantLabels {
                    invariant: inv.as_str().to_owned(),
                })
                .get();
        }
        let _ = self.key_collision.get();
    }

    /// Set `cc_storage_earliest_available_slot` from a derived [`cc_store::meta::ServeWindow`].
    pub(crate) fn set_serve_window(&self, window: &cc_store::meta::ServeWindow) {
        self.earliest_available_slot
            .set(window.earliest_available_slot.as_u64() as i64);
        self.window_branch.set(i64::from(window.branch));
        self.window_hole_slots
            .set(cc_store::hole_slots_total(window.holes.as_ref()) as i64);
    }

    /// Set `cc_storage_following_head` (0/1) — CC-45c restart bar endpoint.
    ///
    /// S2-A-09: write-behind no longer subscribes. Production leaves the
    /// register-time seed (0). Tests still toggle the handle.
    #[cfg(test)]
    pub(crate) fn set_following_head(&self, following: bool) {
        self.following_head.set(i64::from(following));
    }

    /// Current `cc_storage_following_head` gauge value (tests / diagnostics).
    #[cfg(test)]
    pub(crate) fn following_head_get(&self) -> i64 {
        self.following_head.get()
    }

    /// Increment `cc_storage_invariant_violation_total{invariant}` (CC-4H post-pass).
    ///
    /// Label must be one of [`Invariant::ALL`] (closed domain). Callers map
    /// `cc_store::StoreInvariant` via [`Invariant::from_store`] (or `key_collision`
    /// for the CC-44b path).
    ///
    /// Wired from migration/prune once those paths land; unit tests exercise now.
    #[allow(dead_code)]
    pub(crate) fn observe_invariant_violation(&self, inv: Invariant) {
        self.invariant_violation
            .get_or_create(&InvariantLabels {
                invariant: inv.as_str().to_owned(),
            })
            .inc();
    }
}

impl Invariant {
    /// Map a §2.7 store invariant to the metrics label enum (excludes `key_collision`).
    #[must_use]
    #[allow(dead_code)] // used by MetricsInvariantSink once post-pass is scheduled
    pub(crate) fn from_store(inv: cc_store::StoreInvariant) -> Self {
        match inv {
            cc_store::StoreInvariant::Contig => Self::Contig,
            cc_store::StoreInvariant::ColBlock => Self::ColBlock,
            cc_store::StoreInvariant::SplitFin => Self::SplitFin,
            cc_store::StoreInvariant::Ring => Self::Ring,
            cc_store::StoreInvariant::Window => Self::Window,
            cc_store::StoreInvariant::NodeId => Self::NodeId,
            cc_store::StoreInvariant::Shards => Self::Shards,
            cc_store::StoreInvariant::Cursor => Self::Cursor,
        }
    }
}

/// Metrics + tracing sink for post-pass invariant checks (CC-4H /3).
#[allow(dead_code)] // constructed by migration/prune pass sites when those land
pub(crate) struct MetricsInvariantSink<'a> {
    pub(crate) metrics: &'a StorageMetrics,
}

impl cc_store::InvariantSink for MetricsInvariantSink<'_> {
    fn on_violation(&self, violation: &cc_store::InvariantViolation) {
        tracing::error!(
            invariant = violation.invariant.as_str(),
            detail = %violation.detail,
            "store invariant violation"
        );
        self.metrics
            .observe_invariant_violation(Invariant::from_store(violation.invariant));
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use prometheus_client::encoding::text::encode;
    use std::collections::BTreeSet;

    /// Every §10.1 storage family name as it appears on OpenMetrics `# TYPE`
    /// lines after register+seed. Counters omit `_total` on TYPE; duration
    /// histograms keep the `_seconds` unit suffix.
    const EXPECTED_FAMILIES: &[&str] = &[
        // Size and growth
        "cc_storage_bytes_total",
        "cc_storage_rows_total",
        "cc_storage_disk_bytes",
        "cc_storage_live_set_bytes",
        "cc_storage_tables_total",
        // Pruning
        "cc_storage_pruned_bytes",
        "cc_storage_pruned_rows",
        "cc_storage_prune_seconds",
        "cc_storage_prune_lag_epochs",
        "cc_storage_prune_deadline_exceeded",
        "cc_storage_shard_dropped",
        // Write path
        "cc_storage_commit_seconds",
        "cc_storage_commit_wait_seconds",
        "cc_storage_written_bytes",
        "cc_storage_write_behind_lag_slots",
        "cc_storage_stream_reconnect",
        "cc_storage_writer_queue_depth",
        "cc_storage_writer_chunk_dropped",
        // Read path
        "cc_storage_read_txn_seconds",
        "cc_storage_serve_seconds",
        "cc_storage_serve",
        "cc_storage_serve_bytes",
        "cc_storage_serve_admission_wait_seconds",
        // Restart
        "cc_storage_restart_seconds",
        "cc_storage_following_head",
        "cc_storage_replay_divergence",
        // Snapshots
        "cc_storage_snapshot_seconds",
        "cc_storage_snapshot_bytes",
        "cc_storage_snapshot_ring_depth",
        // Windows
        "cc_storage_earliest_available_slot",
        "cc_storage_window_branch",
        "cc_storage_window_hole_slots",
        "cc_storage_window_increase_rejected",
        "cc_storage_backfill_oldest_slot",
        "cc_storage_backfill_bytes",
        "cc_storage_split_slot",
        // Health
        "cc_storage_invariant_violation",
        "cc_storage_key_collision",
    ];

    fn parse_cc_storage_family_names(buf: &str, kind: &str) -> BTreeSet<String> {
        let prefix = match kind {
            "TYPE" => "# TYPE ",
            "HELP" => "# HELP ",
            other => panic!("unknown openmetrics meta kind: {other}"),
        };
        let mut set = BTreeSet::new();
        for line in buf.lines() {
            let Some(rest) = line.strip_prefix(prefix) else {
                continue;
            };
            let Some(name) = rest.split_whitespace().next() else {
                continue;
            };
            if name.starts_with("cc_storage_") {
                set.insert(name.to_owned());
            }
        }
        set
    }

    fn extract_label_values(buf: &str, metric_prefix: &str, label: &str) -> BTreeSet<String> {
        let mut set = BTreeSet::new();
        let needle = format!("{label}=\"");
        for line in buf.lines() {
            if !line.starts_with(metric_prefix) {
                continue;
            }
            if let Some(start) = line.find(&needle) {
                let rest = &line[start + needle.len()..];
                if let Some(end) = rest.find('"') {
                    set.insert(rest[..end].to_owned());
                }
            }
        }
        set
    }

    #[test]
    fn family_name_fixture_matches_exposition() {
        let mut registry = Registry::default();
        let _m = StorageMetrics::register(&mut registry);
        let mut buf = String::new();
        encode(&mut buf, &registry).unwrap();

        let expected: BTreeSet<&str> = EXPECTED_FAMILIES.iter().copied().collect();
        assert_eq!(
            EXPECTED_FAMILIES.len(),
            expected.len(),
            "EXPECTED_FAMILIES must not contain duplicates"
        );
        let type_names = parse_cc_storage_family_names(&buf, "TYPE");
        let help_names = parse_cc_storage_family_names(&buf, "HELP");
        let type_as_str: BTreeSet<&str> = type_names.iter().map(String::as_str).collect();
        let help_as_str: BTreeSet<&str> = help_names.iter().map(String::as_str).collect();

        assert_eq!(
            type_as_str,
            expected,
            "TYPE family set must equal EXPECTED_FAMILIES\nmissing: {:?}\nextra: {:?}",
            expected.difference(&type_as_str).collect::<Vec<_>>(),
            type_as_str.difference(&expected).collect::<Vec<_>>(),
        );
        assert_eq!(
            help_as_str,
            expected,
            "HELP family set must equal EXPECTED_FAMILIES\nmissing: {:?}\nextra: {:?}",
            expected.difference(&help_as_str).collect::<Vec<_>>(),
            help_as_str.difference(&expected).collect::<Vec<_>>(),
        );
        // Family count for commit description.
        assert_eq!(EXPECTED_FAMILIES.len(), 38);
    }

    #[test]
    fn commit_and_read_txn_bucket_boundaries_in_exposition() {
        let mut registry = Registry::default();
        let _m = StorageMetrics::register(&mut registry);
        let mut buf = String::new();
        encode(&mut buf, &registry).unwrap();

        // AC: 0.5 s boundary exists as a boundary, not interpolated.
        // Line-coupled: both metric name and le must appear on the same line
        // (other ladders also emit le="0.5").
        assert!(
            buf.lines().any(|l| {
                l.contains("cc_storage_commit_seconds_bucket{") && l.contains("le=\"0.5\"")
            }),
            "commit_seconds must expose le=\"0.5\" bucket on the same line:\n{buf}"
        );
        // Substring form matches AC grep `le="1"` (prometheus-client emits le="1.0").
        assert!(
            buf.lines().any(|l| {
                l.contains("cc_storage_read_txn_seconds_bucket{") && l.contains("le=\"1")
            }),
            "read_txn_seconds must expose le=1 / le=1.0 bucket on the same line:\n{buf}"
        );
        // Serve 0.15 and restart 5.0 / 60.0 also clause-critical.
        assert!(
            buf.lines().any(|l| {
                l.contains("cc_storage_serve_seconds_bucket{") && l.contains("le=\"0.15\"")
            }),
            "serve_seconds must expose le=\"0.15\":\n{buf}"
        );
        assert!(
            buf.lines().any(|l| {
                l.contains("cc_storage_restart_seconds_bucket{") && l.contains("le=\"5.0\"")
            }),
            "restart_seconds must expose le=\"5.0\":\n{buf}"
        );
        assert!(
            buf.lines().any(|l| {
                l.contains("cc_storage_restart_seconds_bucket{") && l.contains("le=\"60.0\"")
            }),
            "restart_seconds must expose le=\"60.0\":\n{buf}"
        );
    }

    #[test]
    fn following_head_defaults_zero_and_toggles() {
        // CC-45c: gauge is the kill→resume bar endpoint; seed 0.
        // S2-A-09: no write-behind producer; tests still toggle the handle.
        let mut registry = Registry::default();
        let m = StorageMetrics::register(&mut registry);
        assert_eq!(m.following_head_get(), 0, "seed must be 0");
        m.set_following_head(true);
        assert_eq!(m.following_head_get(), 1);
        m.set_following_head(false);
        assert_eq!(m.following_head_get(), 0);

        let mut buf = String::new();
        encode(&mut buf, &registry).unwrap();
        assert!(
            buf.lines()
                .any(|l| l.starts_with("cc_storage_following_head ") && l.ends_with(" 0")),
            "exposition must show following_head 0 after clear:\n{buf}"
        );
        m.set_following_head(true);
        let mut buf2 = String::new();
        encode(&mut buf2, &registry).unwrap();
        assert!(
            buf2.lines()
                .any(|l| l.starts_with("cc_storage_following_head ") && l.ends_with(" 1")),
            "exposition must show following_head 1 when set:\n{buf2}"
        );
    }

    #[test]
    fn restart_phase_label_set_is_complete() {
        // CC-45/8: full closed domain including restore_send and schema_check.
        let phases: BTreeSet<&str> = RestartPhase::ALL.iter().map(|p| p.as_str()).collect();
        assert_eq!(
            phases,
            BTreeSet::from([
                "open",
                "schema_check",
                "snapshot_load",
                "restore_send",
                "chain_replay",
                "forkchoice_rebuild",
                "resubscribe",
            ])
        );
        assert_eq!(RestartPhase::ALL.len(), 7);

        let mut registry = Registry::default();
        let _m = StorageMetrics::register(&mut registry);
        let mut buf = String::new();
        encode(&mut buf, &registry).unwrap();

        let exposed = extract_label_values(&buf, "cc_storage_restart_seconds_bucket{", "phase");
        let expected: BTreeSet<String> = RestartPhase::ALL
            .iter()
            .map(|p| p.as_str().to_owned())
            .collect();
        assert_eq!(
            exposed, expected,
            "restart_seconds phase labels must match closed domain"
        );
    }

    #[test]
    fn label_domains_are_closed() {
        // §10.1 closed domains with documented cardinalities.
        assert_eq!(StorageClass::ALL.len(), 5);
        assert_eq!(PrunePass::ALL.len(), 5);
        assert_eq!(ServeProtocol::ALL.len(), 9);
        assert_eq!(ServeResult::ALL.len(), 4);
        assert_eq!(RestartPhase::ALL.len(), 7);
        assert_eq!(SnapshotPhase::ALL.len(), 4);
        assert_eq!(ReconnectReason::ALL.len(), 4);
        assert_eq!(Invariant::ALL.len(), 9);

        let classes: BTreeSet<&str> = StorageClass::ALL.iter().map(|c| c.as_str()).collect();
        assert_eq!(
            classes,
            BTreeSet::from(["blocks", "columns", "snapshots", "index", "meta"])
        );

        let passes: BTreeSet<&str> = PrunePass::ALL.iter().map(|p| p.as_str()).collect();
        assert_eq!(
            passes,
            BTreeSet::from([
                "columns",
                "blocks",
                "state_roots",
                "snapshots",
                "unfinalized"
            ])
        );

        let protocols: BTreeSet<&str> = ServeProtocol::ALL.iter().map(|p| p.as_str()).collect();
        assert_eq!(
            protocols,
            BTreeSet::from([
                "blocks_by_range",
                "blocks_by_root",
                "blocks_by_head",
                "columns_by_range",
                "columns_by_root",
                "historical_block_by_root",
                "historical_block_by_slot",
                "snapshot_state",
                "finalized_checkpoint_history",
            ])
        );

        let results: BTreeSet<&str> = ServeResult::ALL.iter().map(|r| r.as_str()).collect();
        assert_eq!(
            results,
            BTreeSet::from(["ok", "resource_unavailable", "rate_limited", "error"])
        );

        let snap_phases: BTreeSet<&str> = SnapshotPhase::ALL.iter().map(|p| p.as_str()).collect();
        assert_eq!(
            snap_phases,
            BTreeSet::from(["replay", "serialize", "write", "load"])
        );

        let reasons: BTreeSet<&str> = ReconnectReason::ALL.iter().map(|r| r.as_str()).collect();
        assert_eq!(
            reasons,
            BTreeSet::from([
                "cursor_too_old",
                "cursor_unknown_session",
                "resource_exhausted",
                "transport",
            ])
        );

        let invariants: BTreeSet<&str> = Invariant::ALL.iter().map(|i| i.as_str()).collect();
        assert_eq!(
            invariants,
            BTreeSet::from([
                "contig",
                "col_block",
                "split_fin",
                "ring",
                "window",
                "node_id",
                "shards",
                "cursor",
                "key_collision",
            ])
        );
    }

    #[test]
    fn seeded_labels_stay_inside_closed_domains() {
        // No metric is registered with a label value outside its domain:
        // seed only uses ALL variants; exposition labels ⊆ closed sets.
        let mut registry = Registry::default();
        let _m = StorageMetrics::register(&mut registry);
        let mut buf = String::new();
        encode(&mut buf, &registry).unwrap();

        let class_domain: BTreeSet<&str> = StorageClass::ALL.iter().map(|c| c.as_str()).collect();
        let class_labels = extract_label_values(&buf, "cc_storage_bytes_total{", "class");
        for v in &class_labels {
            assert!(
                class_domain.contains(v.as_str()),
                "class label outside domain: {v}"
            );
        }

        let pass_domain: BTreeSet<&str> = PrunePass::ALL.iter().map(|p| p.as_str()).collect();
        let pass_labels = extract_label_values(&buf, "cc_storage_prune_seconds_bucket{", "pass");
        for v in &pass_labels {
            assert!(
                pass_domain.contains(v.as_str()),
                "pass label outside domain: {v}"
            );
        }

        let protocol_domain: BTreeSet<&str> =
            ServeProtocol::ALL.iter().map(|p| p.as_str()).collect();
        let protocol_labels =
            extract_label_values(&buf, "cc_storage_serve_seconds_bucket{", "protocol");
        for v in &protocol_labels {
            assert!(
                protocol_domain.contains(v.as_str()),
                "protocol label outside domain: {v}"
            );
        }

        let result_domain: BTreeSet<&str> = ServeResult::ALL.iter().map(|r| r.as_str()).collect();
        let result_labels = extract_label_values(&buf, "cc_storage_serve_total{", "result");
        for v in &result_labels {
            assert!(
                result_domain.contains(v.as_str()),
                "result label outside domain: {v}"
            );
        }

        let reason_domain: BTreeSet<&str> =
            ReconnectReason::ALL.iter().map(|r| r.as_str()).collect();
        let reason_labels =
            extract_label_values(&buf, "cc_storage_stream_reconnect_total{", "reason");
        for v in &reason_labels {
            assert!(
                reason_domain.contains(v.as_str()),
                "reason label outside domain: {v}"
            );
        }

        let inv_domain: BTreeSet<&str> = Invariant::ALL.iter().map(|i| i.as_str()).collect();
        let inv_labels =
            extract_label_values(&buf, "cc_storage_invariant_violation_total{", "invariant");
        for v in &inv_labels {
            assert!(
                inv_domain.contains(v.as_str()),
                "invariant label outside domain: {v}"
            );
        }

        let snap_domain: BTreeSet<&str> = SnapshotPhase::ALL.iter().map(|p| p.as_str()).collect();
        let snap_labels =
            extract_label_values(&buf, "cc_storage_snapshot_seconds_bucket{", "phase");
        for v in &snap_labels {
            assert!(
                snap_domain.contains(v.as_str()),
                "snapshot phase label outside domain: {v}"
            );
        }
    }

    #[test]
    fn histograms_reference_cc_store_buckets_not_literals() {
        // Source-level: metrics.rs must not embed §10.2 literal arrays.
        let src = include_str!("metrics.rs");
        // Strip this test module so our own string literals do not trip the check.
        let production = src
            .split("#[cfg(test)]")
            .next()
            .expect("metrics.rs has a test module");
        for forbidden in [
            "0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5",
            "0.005, 0.01, 0.025, 0.05, 0.1, 0.15",
            "0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 30.0, 45.0, 60.0",
        ] {
            assert!(
                !production.contains(forbidden),
                "metrics.rs must not embed bucket literal containing {forbidden:?}"
            );
        }
        assert!(
            production.contains("buckets::COMMIT_SECONDS")
                && production.contains("buckets::READ_TXN_SECONDS")
                && production.contains("buckets::SERVE_SECONDS")
                && production.contains("buckets::PRUNE_SECONDS")
                && production.contains("buckets::RESTART_SECONDS")
                && production.contains("buckets::SNAPSHOT_SECONDS")
                && production.contains("buckets::WRITE_BEHIND_LAG_SLOTS")
                && production.contains("buckets::SERVE_ADMISSION_WAIT_SECONDS"),
            "all eight §10.2 ladders must be referenced by name from cc_store::buckets"
        );
    }

    #[test]
    fn chain_event_buffer_gauges_not_declared() {
        // CC-44a owns these; must be absent from this surface.
        let mut registry = Registry::default();
        let _m = StorageMetrics::register(&mut registry);
        let mut buf = String::new();
        encode(&mut buf, &registry).unwrap();
        assert!(
            !buf.contains("cc_chain_event_buffer"),
            "cc_chain_event_buffer_* must not be declared in storage metrics:\n{buf}"
        );
        let src = include_str!("metrics.rs");
        assert!(
            !src.contains("cc_chain_event_buffer_bytes") || src.contains("cc_chain_event_buffer_*"),
            "source must not register chain buffer gauges"
        );
        // Explicit absence of registration names.
        let production = src.split("#[cfg(test)]").next().unwrap();
        assert!(!production.contains("\"cc_chain_event_buffer_bytes\""));
        assert!(!production.contains("\"cc_chain_event_buffer_bytes_bound\""));
    }

    #[test]
    fn post_pass_split_fin_increments_metric_exactly_once() {
        // CC-4H /3 after a pass: MetricsInvariantSink increments
        // cc_storage_invariant_violation_total{invariant="split_fin"} by 1.
        // (Structural corrupt-store construction lives in `cc-store` unit tests.)
        use cc_store::{InvariantSink, InvariantViolation, StoreInvariant};

        let mut registry = Registry::default();
        let metrics = StorageMetrics::register(&mut registry);
        let sink = MetricsInvariantSink { metrics: &metrics };
        sink.on_violation(&InvariantViolation {
            invariant: StoreInvariant::SplitFin,
            detail: "blocks_hot row at slot 5 ≤ Split.slot 8".into(),
        });

        let mut buf = String::new();
        encode(&mut buf, &registry).unwrap();
        assert!(
            buf.contains("cc_storage_invariant_violation_total{invariant=\"split_fin\"} 1"),
            "expected split_fin count 1 in:\n{buf}"
        );
        for inv in StoreInvariant::ALL {
            if inv == StoreInvariant::SplitFin {
                continue;
            }
            let needle = format!(
                "cc_storage_invariant_violation_total{{invariant=\"{}\"}} 0",
                inv.as_str()
            );
            assert!(
                buf.contains(&needle),
                "expected zero for {} in:\n{buf}",
                inv.as_str()
            );
        }
        // Ninth domain value `key_collision` remains seeded at zero.
        assert!(
            buf.contains("cc_storage_key_collision_total 0")
                || buf.contains("cc_storage_key_collision 0")
                || buf.contains(
                    "cc_storage_invariant_violation_total{invariant=\"key_collision\"} 0"
                ),
            "key_collision series must remain present:\n{buf}"
        );
    }
}
