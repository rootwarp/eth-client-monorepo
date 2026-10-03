//! Persistent store library (Phase 4 store DAG).
//!
//! - [`engine`] — twelve-method concrete `Engine` (CC-40b / Architecture §1.1)
//! - [`keys`] — big-endian fixed-width codecs + shard arithmetic (§2.1 / §2.4)
//! - [`buckets`] — histogram boundaries shared with metrics and `bin/store-bench` (CC-4Ca)
//! - [`meta`] — ten SSZ singleton records (§2.5 / CC-40a)
//! - [`backfill_progress`] — resumable progress, per-index oldest, column completion (CC-47a)
//! - [`schema`] — schema version, config digest, table registry, open-or-refuse (CC-40a)
//! - [`window`] — computed block serve-window floor (CC-4A / Architecture §5.1)
//! - [`invariants`] — §2.7 eight named checks at open and after passes (CC-4H)
//! - [`blocks`] — hot/cold block tables, by-root index, state roots (CC-43a)
//! - [`canonical`] — slot → canonical root via parent-root walk at offset 116 (CC-43a)
//! - [`columns`] — hot/cold column tables, by-root index, DA status (CC-43b)
//! - [`split`] — hot/cold split record, lock order, migration staging (CC-41)
//! - [`snapshots`] — snapshot ring put/evict, uncompressed SSZ (CC-42)
//!
//! This crate depends only on `cc-types` among workspace members; consensus
//! containers must not appear here (opaque bytes under typed keys).

#![allow(missing_docs)]

/// Backfill progress helpers (CC-47a / Architecture §6.5).
pub mod backfill_progress;
/// Block store: hot + sharded cold, by-root index, state roots (CC-43a).
pub mod blocks;
/// Histogram bucket boundaries for the storage metric surface (§10.2 / CC-4Ca).
pub mod buckets;
/// Canonical chain index maintained by a parent-root walk (CC-43a).
pub mod canonical;
/// Column store: hot + 32-epoch cold shards, by-root index, DA status (CC-43b).
pub mod columns;
/// Concrete engine seam (CC-40/3: engine crate name only under `engine/`).
pub mod engine;
/// §2.7 store invariants (CC-4H).
pub mod invariants;
/// Key codecs and shard arithmetic.
pub mod keys;
/// SSZ meta singleton records (Architecture §2.5).
pub mod meta;
/// Schema version, config digest, table registry, [`schema::Store::open`].
pub mod schema;
/// Snapshot ring: full-state SSZ put + oldest-first eviction (CC-42).
pub mod snapshots;
/// Hot/cold split, lock order, migration staging (CC-41 / Architecture §3.1–3.2).
pub mod split;
/// Computed block serve-window floor (CC-4A) + derived ServeWindow (CC-48).
pub mod window;

pub use backfill_progress::{
    BACKFILL_BATCH_SLOT_LIMIT, COLUMN_BACKFILL_EPOCHS, COLUMN_INDEX_COUNT, ProgressError,
    advance_per_index_oldest, apply_block_batch_progress, apply_column_batch_progress,
    block_backfill_complete, block_backfill_target_slot, block_serve_window_slots,
    column_backfill_complete, column_backfill_target_slot, ensure_per_index_len,
    load_backfill_progress, load_backfill_progress_txn, oldest_custodied_column_slot,
    put_backfill_progress, resume_block_frontier, resume_block_parent, resume_column_frontier,
    resume_within_one_batch, serve_window_above_block_target, serve_window_above_column_target,
};
pub use blocks::{
    BlockClassStats, MAX_BLOCKS_BY_RANGE, PARENT_ROOT_SSZ_OFFSET, PutBlockOutcome, RangeBlock,
    SLOT_SSZ_OFFSET, STATE_ROOT_SSZ_OFFSET, TABLE_BLOCK_SLOT_BY_ROOT, TABLE_BLOCKS_HOT,
    TABLE_STATE_ROOTS, blocks_by_range, get_block_by_root, measure_class_stats,
    parent_root_at_offset, put_block, slot_at_offset, state_root_at_offset,
};
pub use canonical::{
    CanonicalWalkResult, MAX_CANONICAL_WALK_STEPS, TABLE_CANONICAL, get_canonical,
    put_block_and_update_head, rewrite_from_head,
};
pub use columns::{
    BYTES_PER_BLOB_IN_SIDECAR, COLUMN_HEADER_PARENT_ROOT_SSZ_OFFSET, COLUMN_HEADER_SLOT_SSZ_OFFSET,
    COLUMN_INDEX_SSZ_OFFSET, ColumnClassStats, ColumnsForBlock, DATA_COLUMN_SIDECAR_FIXED_BYTES,
    DaStatus, MAX_BLOBS_PER_COLUMN_SIDECAR, MAX_COLUMN_SIDECAR_BYTES,
    MAX_COLUMNS_BY_RANGE_SIDECARS, MAX_COLUMNS_BY_RANGE_SLOTS, PutColumnOutcome, RangeColumn,
    TABLE_COLUMN_SLOT_BY_ROOT, TABLE_COLUMNS_HOT, TABLE_DA_STATUS, column_index_at_offset,
    column_parent_root_at_offset, column_slot_at_offset, columns_by_range, columns_for_block,
    data_column_sidecar_size, get_column_by_root, get_da_status, measure_column_class_stats,
    put_column, put_da_status,
};
pub use engine::{
    Batch, Durability, Engine, EngineOptions, MAX_BATCH_OPS, MAX_INTERNED_TABLE_NAMES,
    MAX_RANGE_BYTES, MAX_RANGE_ENTRIES, RangeIter, ReadTxn, StoreError, db_file_path,
};
pub use invariants::{
    CountingSink, DEFAULT_MAX_OPEN_SCAN_ROWS, DEFAULT_SNAPSHOT_RING, FanoutSink,
    InvariantCheckMode, InvariantContext, InvariantSink, InvariantViolation, MAX_CONTIG_WALK_SLOTS,
    MAX_RING_SCAN_ROWS, StoreInvariant, TracingSink, check_invariants,
    run_invariant_checks_if_enabled,
};
pub use keys::{BlockRegion, shard_of, slots_in};
pub use schema::{
    BLOCK_SHARD_WIDTH_EPOCHS, COLUMN_SHARD_WIDTH_EPOCHS, ConfigDigestInput, DIGEST_VERSION,
    FIXED_TABLES, SCHEMA_VERSION, Store, StoreOpenOptions, compute_config_digest,
    compute_identity_digest, compute_schedule_digest, is_registered_table, iter_shard_tables,
    legacy_config_digest, parse_shard_table, refuse_populated_legacy_open,
    store_holds_chain_history,
};
pub use snapshots::{
    DEFAULT_SNAPSHOT_EPOCHS, MAX_SNAPSHOT_BYTES, SnapshotPlan, TABLE_SNAPSHOTS,
    apply_snapshot_plan, check_snapshot_len, encode_snapshot_key,
    epoch_of_slot as snapshot_epoch_of_slot, get_snapshot, list_snapshot_slots, newest_snapshot,
    oldest_snapshot_slot, plan_snapshot_put, put_snapshot, ring_depth, snapshot_due,
};
pub use split::{
    DEFAULT_EPOCHS_PER_MIGRATION, MAX_MIGRATION_SLOTS_PER_BATCH, MigrationPlan, MigrationStats,
    SLOTS_PER_EPOCH as SPLIT_SLOTS_PER_EPOCH, SPLIT_KEY, SplitLock, epoch_of_slot,
    epoch_start_slot, load_split, migrate, migrate_with_commit_fault, migration_needed,
    migration_window_end, plan_migration, put_split, should_migrate_on_finalization,
    stage_migration,
};
pub use window::{
    BlockServeWindowCfg, HoleAppendResult, MAX_SERVE_WINDOW_HOLES,
    MIN_EPOCHS_FOR_DATA_COLUMN_SIDECARS_REQUESTS, ServeWindowError, WindowBranch,
    WindowConfigError, append_hole, check_min_epochs_for_block_requests, column_slot_contiguous,
    compute_min_epochs_for_block_requests, derive_serve_window, derive_serve_window_from_meta,
    earliest_available_slot, extend_block_floor, hole_slots_total, load_serve_window,
    put_serve_window, raise_for_holes, shrink_holes_filled, sidecar_retention_floor,
    try_extend_column_floor, write_derived_serve_window, write_serve_window_from_meta,
};
// Re-export the SSZ Split record under the split module's natural name.
pub use meta::Split;

// Re-export key primitives so `bin/store-bench` (DAG: cc-store only) can build keys.
pub use cc_types::{Epoch, Root, Slot};

// Meta SSZ encode surface for integration tests / services writing singletons.
pub use ssz::{Decode as SszDecode, Encode as SszEncode};
