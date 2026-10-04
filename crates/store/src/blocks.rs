//! Block store: hot + cold sharded tables, by-root index, state roots (CC-43a).
//!
//! ## Tables (§2.2 / §2.3 / ADR P4-09)
//!
//! | Table | Key | Value |
//! |---|---|---|
//! | [`TABLE_BLOCKS_HOT`] | `(slot, root)` 40 B | block SSZ (opaque wire bytes) |
//! | `blocks_{shard}` | `slot` 8 B | block SSZ — **one row per slot** (root drops out) |
//! | [`TABLE_BLOCK_SLOT_BY_ROOT`] | `root` 32 B | `slot ‖ region` 9 B |
//! | [`TABLE_STATE_ROOTS`] | `slot` 8 B | `state_root` 32 B |
//!
//! Values are **SSZ wire bytes and nothing else**. This crate never re-serializes
//! and never fully decodes a block container — fixed-offset field peeks only
//! ([`PARENT_ROOT_SSZ_OFFSET`], [`SLOT_SSZ_OFFSET`], [`STATE_ROOT_SSZ_OFFSET`]).
//!
//! ## 256-epoch shards (Deviation 1 / ADR P4-10)
//!
//! Block shard width equals the block prune cadence (**256 epochs**), not 32:
//! retirement is one `drop_table` per tick. A 128-slot `ByRange` is 4 epochs and
//! spans at most two shards under either width (CC-43 /4).
//!
//! ## Measured Hoodi block sizes (CC-43 /1, A-P4-2)
//!
//! Spot sample of **384 contiguous Hoodi slots on 2026-08-07** (blobs are not
//! in the block body — 48 B of commitment each is):
//!
//! | Stat | Bytes |
//! |---|---|
//! | **p50** | **21 573** |
//! | mean | 24 827 |
//! | p95 | 47 110 |
//! | max | 75 957 |
//!
//! Slot fill **93.23 %**; mean **13.99 blobs/block**. **A-P4-2:** this is a spot
//! sample — the mean is a central estimate and the headroom row (p95 / max) is
//! the provisioning number. Do not re-derive the older CC-26a figures (wrong by
//! 5–10×).
//!
//! Grep anchor: `21573` / `21 573`.

use std::collections::HashMap;

use cc_types::{Root, Slot};

use crate::engine::{Batch, Engine, ReadTxn, StoreError};
use crate::keys::{
    BLOCK_SHARD_EPOCHS, BlockRegion, SLOTS_PER_EPOCH, block_shard_id, blocks_shard_table,
    decode_block_slot_by_root_value, decode_root_value, encode_block_slot_by_root_value,
    encode_cold_block_key, encode_hot_block_key, encode_root_key, encode_root_value,
};

// ---------------------------------------------------------------------------
// Table names
// ---------------------------------------------------------------------------

/// Hot block table (`slot ‖ root` → SSZ).
pub const TABLE_BLOCKS_HOT: &str = "blocks_hot";
/// `root` → `slot ‖ region` reverse index for `ByRoot` and the parent walk.
pub const TABLE_BLOCK_SLOT_BY_ROOT: &str = "block_slot_by_root";
/// Canonical-slot historical `state_root` (populated on block write; CC-42 /5 reads).
pub const TABLE_STATE_ROOTS: &str = "state_roots";

// ---------------------------------------------------------------------------
// Fixed SSZ offsets (no full container decode)
// ---------------------------------------------------------------------------
//
// Wire layout of a signed block SSZ (variable `message` + fixed 96 B signature):
//   bytes[0..4]   = little-endian offset of `message` (= 100)
//   bytes[4..100] = signature
//   message:
//     slot            @ +0   (u64 LE)
//     proposer_index  @ +8
//     parent_root     @ +16
//     state_root      @ +48
// Absolute: parent_root @ 116, slot @ 100, state_root @ 148.
// Asserted against a real Hoodi fixture in `tests/parent_root_offset.rs`.

/// Absolute byte offset of `parent_root` in a serialized signed block.
pub const PARENT_ROOT_SSZ_OFFSET: usize = 116;

/// Absolute byte offset of `slot` (SSZ little-endian u64) in a serialized signed block.
pub const SLOT_SSZ_OFFSET: usize = 100;

/// Absolute byte offset of `state_root` in a serialized signed block.
pub const STATE_ROOT_SSZ_OFFSET: usize = 148;

/// Minimum SSZ length that contains slot + parent_root + state_root.
pub const MIN_BLOCK_SSZ_LEN: usize = STATE_ROOT_SSZ_OFFSET + 32;

/// Hard cap on [`blocks_by_range`] `count` (SEC-43a-1).
///
/// Aligns with spec `MAX_REQUEST_BLOCKS_DENEB = 128`. Larger requests fail closed
/// with [`StoreError::Limit`] rather than materialising an unbounded walk.
pub const MAX_BLOCKS_BY_RANGE: u64 = 128;

/// Result of an idempotent block put (CC-43 /6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutBlockOutcome {
    /// Key was absent; row inserted.
    Inserted,
    /// Same key, same bytes; no-op (one row, no error).
    Idempotent,
}

/// In-memory accounting for `cc_storage_*{class="blocks"|"index"}` (CC-43 /7).
///
/// Production Prometheus export lives in `services/storage`; this struct is the
/// store-side measurement the gauges read after a synthetic write load.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BlockClassStats {
    /// Rows under the blocks class (hot + every cold shard).
    pub blocks_rows: u64,
    /// Value bytes under the blocks class.
    pub blocks_bytes: u64,
    /// Rows under the index class (`block_slot_by_root` + `canonical` + `state_roots`).
    pub index_rows: u64,
    /// Value bytes under the index class.
    pub index_bytes: u64,
}

/// Peek `parent_root` at [`PARENT_ROOT_SSZ_OFFSET`] without a full SSZ decode.
pub fn parent_root_at_offset(block_ssz: &[u8]) -> Result<Root, StoreError> {
    read_root_at(block_ssz, PARENT_ROOT_SSZ_OFFSET, "parent_root")
}

/// Peek `slot` at [`SLOT_SSZ_OFFSET`] (SSZ little-endian u64).
pub fn slot_at_offset(block_ssz: &[u8]) -> Result<Slot, StoreError> {
    if block_ssz.len() < SLOT_SSZ_OFFSET + 8 {
        return Err(StoreError::Codec(format!(
            "block SSZ too short for slot at {SLOT_SSZ_OFFSET}: len {}",
            block_ssz.len()
        )));
    }
    let mut le = [0u8; 8];
    le.copy_from_slice(&block_ssz[SLOT_SSZ_OFFSET..SLOT_SSZ_OFFSET + 8]);
    Ok(Slot::new(u64::from_le_bytes(le)))
}

/// Peek `state_root` at [`STATE_ROOT_SSZ_OFFSET`].
pub fn state_root_at_offset(block_ssz: &[u8]) -> Result<Root, StoreError> {
    read_root_at(block_ssz, STATE_ROOT_SSZ_OFFSET, "state_root")
}

fn read_root_at(block_ssz: &[u8], offset: usize, field: &str) -> Result<Root, StoreError> {
    if block_ssz.len() < offset + 32 {
        return Err(StoreError::Codec(format!(
            "block SSZ too short for {field} at {offset}: len {}",
            block_ssz.len()
        )));
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&block_ssz[offset..offset + 32]);
    Ok(Root::from_array(arr))
}

// ---------------------------------------------------------------------------
// Writes
// ---------------------------------------------------------------------------

/// Stage a block into `batch` with key-level idempotency (CC-43 /5, /6).
///
/// - **Absent key** → insert block body + `block_slot_by_root` (+ optional `state_roots`).
/// - **Same bytes** → [`PutBlockOutcome::Idempotent`], no error.
/// - **Different bytes** → [`StoreError::KeyCollision`] (fatal; never overwrite).
/// - **Caller `slot` ≠ SSZ slot at offset 100** → [`StoreError::Codec`] (SEC-43a-2).
///
/// Cold region keys by slot alone (ADR P4-09). Hot keys by `(slot, root)`.
pub fn put_block(
    rt: &ReadTxn,
    batch: &mut Batch,
    slot: Slot,
    root: &Root,
    ssz: &[u8],
    region: BlockRegion,
    write_state_root: bool,
) -> Result<PutBlockOutcome, StoreError> {
    // SEC-43a-2: refuse mis-keyed writes (caller slot must match body).
    let ssz_slot = slot_at_offset(ssz)?;
    if ssz_slot != slot {
        return Err(StoreError::Codec(format!(
            "put_block slot mismatch: caller {} != SSZ slot {} at offset {SLOT_SSZ_OFFSET}",
            slot.as_u64(),
            ssz_slot.as_u64()
        )));
    }

    let (table, key) = block_table_and_key(slot, root, region);
    match rt.get(&table, &key)? {
        Some(existing) if existing.as_slice() == ssz => {
            return Ok(PutBlockOutcome::Idempotent);
        }
        Some(_) => {
            return Err(StoreError::KeyCollision { table });
        }
        None => {}
    }

    // Reverse index collision: same root, different slot/region is also fatal.
    let idx_key = encode_root_key(root);
    let idx_val = encode_block_slot_by_root_value(slot, region);
    if let Some(existing) = rt.get(TABLE_BLOCK_SLOT_BY_ROOT, &idx_key)?
        && existing.as_slice() != idx_val.as_slice()
    {
        return Err(StoreError::KeyCollision {
            table: TABLE_BLOCK_SLOT_BY_ROOT.to_owned(),
        });
    }

    batch.put(&table, &key, ssz);
    batch.put(TABLE_BLOCK_SLOT_BY_ROOT, &idx_key, &idx_val);

    if write_state_root {
        let sr = state_root_at_offset(ssz)?;
        put_state_root(rt, batch, slot, &sr)?;
    }

    Ok(PutBlockOutcome::Inserted)
}

/// Stage `state_roots[slot] = state_root` with the same idempotency rule.
pub fn put_state_root(
    rt: &ReadTxn,
    batch: &mut Batch,
    slot: Slot,
    state_root: &Root,
) -> Result<PutBlockOutcome, StoreError> {
    let key = encode_cold_block_key(slot);
    let val = encode_root_value(state_root);
    match rt.get(TABLE_STATE_ROOTS, &key)? {
        Some(existing) if existing.as_slice() == val.as_slice() => Ok(PutBlockOutcome::Idempotent),
        Some(_) => Err(StoreError::KeyCollision {
            table: TABLE_STATE_ROOTS.to_owned(),
        }),
        None => {
            batch.put(TABLE_STATE_ROOTS, &key, &val);
            Ok(PutBlockOutcome::Inserted)
        }
    }
}

/// Stage `state_roots[slot]` for a body commit.
///
/// Absent → insert. Same root → no-op. A different root is replaced only when
/// `overwrite` is set (this body is the new canonical head). Otherwise the
/// existing row stays, so a losing sibling neither collides nor rewinds it.
pub fn stage_state_root(
    rt: &ReadTxn,
    batch: &mut Batch,
    slot: Slot,
    state_root: &Root,
    overwrite: bool,
) -> Result<(), StoreError> {
    let key = encode_cold_block_key(slot);
    let val = encode_root_value(state_root);
    match rt.get(TABLE_STATE_ROOTS, &key)? {
        Some(existing) if existing.as_slice() == val.as_slice() => Ok(()),
        Some(_) if overwrite => {
            batch.put(TABLE_STATE_ROOTS, &key, &val);
            Ok(())
        }
        Some(_) => Ok(()),
        None => {
            batch.put(TABLE_STATE_ROOTS, &key, &val);
            Ok(())
        }
    }
}

fn block_table_and_key(slot: Slot, root: &Root, region: BlockRegion) -> (String, Vec<u8>) {
    match region {
        BlockRegion::Hot => (
            TABLE_BLOCKS_HOT.to_owned(),
            encode_hot_block_key(slot, root).to_vec(),
        ),
        BlockRegion::Cold => {
            let shard = block_shard_id(slot);
            (
                blocks_shard_table(shard),
                encode_cold_block_key(slot).to_vec(),
            )
        }
    }
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

/// Look up slot + region for a block root.
pub fn slot_by_root(rt: &ReadTxn, root: &Root) -> Result<Option<(Slot, BlockRegion)>, StoreError> {
    let Some(v) = rt.get(TABLE_BLOCK_SLOT_BY_ROOT, &encode_root_key(root))? else {
        return Ok(None);
    };
    let decoded = decode_block_slot_by_root_value(&v).ok_or_else(|| {
        StoreError::Codec(format!("block_slot_by_root value len {} (want 9)", v.len()))
    })?;
    Ok(Some(decoded))
}

/// Load block SSZ by root (hot or cold via the reverse index).
pub fn get_block_by_root(rt: &ReadTxn, root: &Root) -> Result<Option<Vec<u8>>, StoreError> {
    let Some((slot, region)) = slot_by_root(rt, root)? else {
        return Ok(None);
    };
    get_block(rt, slot, root, region)
}

/// Load block SSZ given slot, root, and region.
pub fn get_block(
    rt: &ReadTxn,
    slot: Slot,
    root: &Root,
    region: BlockRegion,
) -> Result<Option<Vec<u8>>, StoreError> {
    let (table, key) = block_table_and_key(slot, root, region);
    rt.get(&table, &key)
}

/// Load a cold-region block by slot alone (root not in the key).
pub fn get_cold_block(rt: &ReadTxn, slot: Slot) -> Result<Option<Vec<u8>>, StoreError> {
    let table = blocks_shard_table(block_shard_id(slot));
    rt.get(&table, &encode_cold_block_key(slot))
}

/// Load `state_roots[slot]`.
pub fn get_state_root(rt: &ReadTxn, slot: Slot) -> Result<Option<Root>, StoreError> {
    let Some(v) = rt.get(TABLE_STATE_ROOTS, &encode_cold_block_key(slot))? else {
        return Ok(None);
    };
    decode_root_value(&v)
        .ok_or_else(|| StoreError::Codec(format!("state_roots value len {} (want 32)", v.len())))
        .map(Some)
}

/// One row returned by [`blocks_by_range`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeBlock {
    /// Slot of the block.
    pub slot: Slot,
    /// Canonical root at this slot (from the `canonical` index, or cold-derived).
    pub root: Root,
    /// Opaque block SSZ (byte-identical to what was stored).
    pub ssz: Vec<u8>,
}

/// `BeaconBlocksByRange`-shaped read: `count` slots from `start_slot`, contiguous
/// on the **canonical** chain, ordered by ascending slot (CC-43 /4).
///
/// - Slots `≤ split` (when `split` is `Some`) are read from cold shards (slot key).
/// - Slots above the split use `canonical[slot]` then `blocks_hot[(slot, root)]`.
/// - Missing canonical / body rows are skipped (sparse history / holes); the
///   returned sequence is still strictly ascending by slot.
///
/// `count` is hard-capped at [`MAX_BLOCKS_BY_RANGE`] (SEC-43a-1); larger values
/// return [`StoreError::Limit`].
pub fn blocks_by_range(
    rt: &ReadTxn,
    start_slot: Slot,
    count: u64,
    split: Option<Slot>,
) -> Result<Vec<RangeBlock>, StoreError> {
    if count == 0 {
        return Ok(Vec::new());
    }
    if count > MAX_BLOCKS_BY_RANGE {
        return Err(StoreError::limit(format!(
            "blocks_by_range count {count} exceeds MAX_BLOCKS_BY_RANGE ({MAX_BLOCKS_BY_RANGE})"
        )));
    }
    let mut out = Vec::with_capacity(count as usize);
    let start = start_slot.as_u64();
    let end = start.saturating_add(count); // exclusive
    for s in start..end {
        let slot = Slot::new(s);
        let in_cold = match split {
            Some(sp) => s <= sp.as_u64(),
            None => false,
        };
        if in_cold {
            if let Some(ssz) = get_cold_block(rt, slot)? {
                // Prefer canonical root when present; else ZERO (cold has no root in key).
                let root = crate::canonical::get_canonical(rt, slot)?.unwrap_or(Root::ZERO);
                out.push(RangeBlock { slot, root, ssz });
            }
        } else {
            let Some(root) = crate::canonical::get_canonical(rt, slot)? else {
                continue;
            };
            let Some(ssz) = get_block(rt, slot, &root, BlockRegion::Hot)? else {
                continue;
            };
            out.push(RangeBlock { slot, root, ssz });
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Stats (CC-43 /7)
// ---------------------------------------------------------------------------

/// Scan block + index tables and return row/byte totals for metrics classes.
pub fn measure_class_stats(engine: &Engine) -> Result<BlockClassStats, StoreError> {
    let rt = engine.read()?;
    let mut stats = BlockClassStats::default();

    // blocks_hot
    accumulate_table(
        &rt,
        TABLE_BLOCKS_HOT,
        &mut stats.blocks_rows,
        &mut stats.blocks_bytes,
    )?;

    // cold shards present on disk (O(active tables), not O(all shards ever))
    let names = engine.table_names()?;
    for (name, class, _) in crate::schema::iter_shard_tables(&names) {
        if class == "blocks" {
            accumulate_table(&rt, name, &mut stats.blocks_rows, &mut stats.blocks_bytes)?;
        }
    }

    // index class
    accumulate_table(
        &rt,
        TABLE_BLOCK_SLOT_BY_ROOT,
        &mut stats.index_rows,
        &mut stats.index_bytes,
    )?;
    accumulate_table(
        &rt,
        crate::canonical::TABLE_CANONICAL,
        &mut stats.index_rows,
        &mut stats.index_bytes,
    )?;
    accumulate_table(
        &rt,
        TABLE_STATE_ROOTS,
        &mut stats.index_rows,
        &mut stats.index_bytes,
    )?;

    Ok(stats)
}

fn accumulate_table(
    rt: &ReadTxn,
    table: &str,
    rows: &mut u64,
    bytes: &mut u64,
) -> Result<(), StoreError> {
    let lo = [0u8; 0];
    // Full table scan via max key prefix: use empty..0xff… range materialisation.
    // Engine range is half-open; use all-zero to all-0xff for fixed-width keys.
    let hi = [0xffu8; 64];
    for item in rt.range(table, &lo, &hi)? {
        let (k, v) = item?;
        *rows = rows.saturating_add(1);
        *bytes = bytes
            .saturating_add(k.len() as u64)
            .saturating_add(v.len() as u64);
    }
    Ok(())
}

/// Re-export shard helpers under the AC names (property tests).
pub use crate::keys::{shard_of as block_shard_of, slots_in as block_slots_in};

/// Pending block bodies staged for the same batch (parent-walk overlay).
pub type PendingBlocks = HashMap<Root, (Slot, BlockRegion, Vec<u8>)>;

/// Insert a staged body into a pending map (does not touch the engine).
pub fn stage_pending(
    pending: &mut PendingBlocks,
    slot: Slot,
    root: Root,
    region: BlockRegion,
    ssz: Vec<u8>,
) {
    pending.insert(root, (slot, region, ssz));
}

/// Resolve block SSZ for `root` from pending overlay then committed store.
pub fn resolve_block_ssz(
    rt: &ReadTxn,
    pending: &PendingBlocks,
    root: &Root,
) -> Result<Option<(Slot, BlockRegion, Vec<u8>)>, StoreError> {
    if let Some((slot, region, ssz)) = pending.get(root) {
        return Ok(Some((*slot, *region, ssz.clone())));
    }
    let Some((slot, region)) = slot_by_root(rt, root)? else {
        return Ok(None);
    };
    let Some(ssz) = get_block(rt, slot, root, region)? else {
        return Ok(None);
    };
    Ok(Some((slot, region, ssz)))
}

/// Documented constants for external greps / docs.
pub mod docs {
    /// Block shard width in epochs (must equal prune cadence).
    pub const BLOCK_SHARD_WIDTH_EPOCHS: u64 = super::BLOCK_SHARD_EPOCHS;
    /// Slots per epoch used by shard arithmetic.
    pub const SLOTS_PER_EPOCH: u64 = super::SLOTS_PER_EPOCH;
    /// Slots per block shard = 256 × 32 = 8192.
    pub const SLOTS_PER_BLOCK_SHARD: u64 = BLOCK_SHARD_WIDTH_EPOCHS * SLOTS_PER_EPOCH;
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::canonical;
    use crate::engine::{Durability, EngineOptions};
    use crate::keys::{block_shard_start_slot, shard_of, slots_in};
    use proptest::prelude::*;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("cc-store-blocks-{label}-{nanos}"));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn eng(label: &str) -> (PathBuf, Engine) {
        let dir = tmp_dir(label);
        let eng = Engine::open(
            &dir,
            EngineOptions::default().with_durability(Durability::None),
        )
        .unwrap();
        (dir, eng)
    }

    /// Minimal opaque body with slot / parent / state_root at the fixed offsets.
    fn synth_block(slot: u64, parent: &Root, state: &Root) -> Vec<u8> {
        let mut v = vec![0u8; MIN_BLOCK_SSZ_LEN];
        v[0..4].copy_from_slice(&100u32.to_le_bytes());
        v[SLOT_SSZ_OFFSET..SLOT_SSZ_OFFSET + 8].copy_from_slice(&slot.to_le_bytes());
        v[PARENT_ROOT_SSZ_OFFSET..PARENT_ROOT_SSZ_OFFSET + 32].copy_from_slice(parent.as_slice());
        v[STATE_ROOT_SSZ_OFFSET..STATE_ROOT_SSZ_OFFSET + 32].copy_from_slice(state.as_slice());
        v
    }

    fn root_n(n: u8) -> Root {
        Root::from_array([n; 32])
    }

    #[test]
    fn module_doc_carries_hoodi_p50_21573() {
        let src = include_str!("blocks.rs");
        assert!(
            src.contains("21 573") || src.contains("21573"),
            "module doc must carry measured Hoodi p50 21 573 B"
        );
        assert!(src.contains("24 827"), "mean");
        assert!(src.contains("47 110"), "p95");
        assert!(src.contains("75 957"), "max");
        assert!(src.contains("93.23"), "slot fill");
        assert!(src.contains("13.99"), "blobs/block");
        assert!(
            src.contains("A-P4-2") || src.contains("spot sample"),
            "caveat"
        );
        assert!(src.contains("384"), "sample size");
        assert!(src.contains("2026-08-07"), "sample date");
    }

    #[test]
    fn byte_identical_roundtrip_hot() {
        // CC-43 /5
        let (dir, eng) = eng("roundtrip");
        let slot = Slot::new(100);
        let root = root_n(0xAB);
        let ssz = synth_block(100, &root_n(0x01), &root_n(0x02));
        let mut b = eng.batch();
        {
            let rt = eng.read().unwrap();
            assert_eq!(
                put_block(&rt, &mut b, slot, &root, &ssz, BlockRegion::Hot, true).unwrap(),
                PutBlockOutcome::Inserted
            );
        }
        eng.commit(b).unwrap();
        let rt = eng.read().unwrap();
        let got = get_block_by_root(&rt, &root).unwrap().unwrap();
        assert_eq!(got, ssz, "stored bytes must be wire-identical");
        assert_eq!(get_state_root(&rt, slot).unwrap().unwrap(), root_n(0x02));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn idempotent_same_bytes_no_error() {
        // CC-43 /6 positive
        let (dir, eng) = eng("idem");
        let slot = Slot::new(5);
        let root = root_n(0x11);
        let ssz = synth_block(5, &Root::ZERO, &root_n(0x22));
        for _ in 0..2 {
            let mut b = eng.batch();
            let rt = eng.read().unwrap();
            put_block(&rt, &mut b, slot, &root, &ssz, BlockRegion::Hot, false).unwrap();
            eng.commit(b).unwrap();
        }
        let stats = measure_class_stats(&eng).unwrap();
        assert_eq!(stats.blocks_rows, 1, "one row after double write");
        assert_eq!(stats.index_rows, 1, "one by-root index row");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn stage_state_root_leaves_a_different_root_unless_overwrite() {
        let (dir, eng) = eng("state-root-stage");
        let slot = Slot::new(2);
        let first = root_n(0x11);
        let second = root_n(0x22);
        let mut b = eng.batch();
        {
            let rt = eng.read().unwrap();
            stage_state_root(&rt, &mut b, slot, &first, false).unwrap();
        }
        eng.commit(b).unwrap();

        let mut b = eng.batch();
        {
            let rt = eng.read().unwrap();
            stage_state_root(&rt, &mut b, slot, &second, false).unwrap();
        }
        eng.commit(b).unwrap();
        assert_eq!(
            get_state_root(&eng.read().unwrap(), slot).unwrap().unwrap(),
            first,
            "a non-head body must not replace the canonical slot root"
        );

        let mut b = eng.batch();
        {
            let rt = eng.read().unwrap();
            stage_state_root(&rt, &mut b, slot, &second, true).unwrap();
        }
        eng.commit(b).unwrap();
        assert_eq!(
            get_state_root(&eng.read().unwrap(), slot).unwrap().unwrap(),
            second
        );

        let mut b = eng.batch();
        {
            let rt = eng.read().unwrap();
            stage_state_root(&rt, &mut b, slot, &second, true).unwrap();
        }
        eng.commit(b).unwrap();
        assert_eq!(
            get_state_root(&eng.read().unwrap(), slot).unwrap().unwrap(),
            second,
            "same root is a no-op"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn different_bytes_same_key_is_key_collision() {
        // CC-43 /6 negative — recorded: different value under same key → KeyCollision.
        let (dir, eng) = eng("collision");
        let slot = Slot::new(5);
        let root = root_n(0x11);
        let ssz_a = synth_block(5, &Root::ZERO, &root_n(0x22));
        let mut ssz_b = ssz_a.clone();
        ssz_b[MIN_BLOCK_SSZ_LEN - 1] ^= 0xFF; // corrupt trailing byte
        let mut b = eng.batch();
        {
            let rt = eng.read().unwrap();
            put_block(&rt, &mut b, slot, &root, &ssz_a, BlockRegion::Hot, false).unwrap();
        }
        eng.commit(b).unwrap();

        let mut b = eng.batch();
        let rt = eng.read().unwrap();
        let err = put_block(&rt, &mut b, slot, &root, &ssz_b, BlockRegion::Hot, false).unwrap_err();
        assert!(
            matches!(err, StoreError::KeyCollision { ref table } if table == TABLE_BLOCKS_HOT),
            "err={err:?}"
        );
        // Original bytes preserved.
        let got = get_block_by_root(&eng.read().unwrap(), &root)
            .unwrap()
            .unwrap();
        assert_eq!(got, ssz_a);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn cold_region_keys_by_slot_only() {
        let (dir, eng) = eng("cold");
        let slot = Slot::new(42);
        let root = root_n(0x42);
        let ssz = synth_block(42, &Root::ZERO, &root_n(0x43));
        let mut b = eng.batch();
        {
            let rt = eng.read().unwrap();
            put_block(&rt, &mut b, slot, &root, &ssz, BlockRegion::Cold, false).unwrap();
        }
        eng.commit(b).unwrap();
        let table = blocks_shard_table(block_shard_id(slot));
        let rt = eng.read().unwrap();
        let by_slot = rt
            .get(&table, &encode_cold_block_key(slot))
            .unwrap()
            .unwrap();
        assert_eq!(by_slot, ssz);
        // Hot key must be absent.
        assert!(
            rt.get(TABLE_BLOCKS_HOT, &encode_hot_block_key(slot, &root))
                .unwrap()
                .is_none()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn range_128_straddling_shard_boundary_is_contiguous() {
        // CC-43 /4 — 128 slots across the 256-epoch (8192-slot) boundary.
        let (dir, eng) = eng("shard-boundary");
        let boundary = block_shard_start_slot(1).as_u64(); // 8192
        assert_eq!(boundary, 256 * 32);
        let start = boundary - 64;
        let count = 128u64;

        // Write all 128 as cold (below a high split) so both shards are exercised.
        let mut b = eng.batch();
        {
            let rt = eng.read().unwrap();
            for i in 0..count {
                let slot = Slot::new(start + i);
                let root = Root::from_array({
                    let mut a = [0u8; 32];
                    a[0..8].copy_from_slice(&(start + i).to_be_bytes());
                    a
                });
                let ssz = synth_block(start + i, &Root::ZERO, &root_n(1));
                put_block(&rt, &mut b, slot, &root, &ssz, BlockRegion::Cold, false).unwrap();
                // Canonical so range can name the root.
                canonical::put_canonical(&rt, &mut b, slot, &root).unwrap();
            }
        }
        eng.commit(b).unwrap();

        // Confirm we actually used two shards.
        let names = eng.table_names().unwrap();
        assert!(names.iter().any(|n| n == &blocks_shard_table(0)));
        assert!(names.iter().any(|n| n == &blocks_shard_table(1)));

        let rt = eng.read().unwrap();
        let rows =
            blocks_by_range(&rt, Slot::new(start), count, Some(Slot::new(u64::MAX))).unwrap();
        assert_eq!(rows.len(), 128);
        let slots: Vec<u64> = rows.iter().map(|r| r.slot.as_u64()).collect();
        let expected: Vec<u64> = (start..start + count).collect();
        assert_eq!(slots, expected, "contiguous ascending slot sequence");
        // First half shard 0, second half shard 1.
        assert_eq!(shard_of(rows[0].slot), 0);
        assert_eq!(shard_of(rows[127].slot), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn range_straddling_hot_cold_boundary() {
        // CC-43 /4 — hot/cold boundary.
        let (dir, eng) = eng("hot-cold");
        let split = Slot::new(50);
        let start = Slot::new(40);
        let count = 30u64; // 40..70 → cold 40..=50, hot 51..69

        let mut b = eng.batch();
        {
            let rt = eng.read().unwrap();
            for i in 0..count {
                let slot = Slot::new(start.as_u64() + i);
                let root = Root::from_array({
                    let mut a = [0u8; 32];
                    a[0] = (i as u8).wrapping_add(1);
                    a
                });
                let ssz = synth_block(slot.as_u64(), &Root::ZERO, &root_n(1));
                let region = if slot.as_u64() <= split.as_u64() {
                    BlockRegion::Cold
                } else {
                    BlockRegion::Hot
                };
                put_block(&rt, &mut b, slot, &root, &ssz, region, false).unwrap();
                canonical::put_canonical(&rt, &mut b, slot, &root).unwrap();
            }
        }
        eng.commit(b).unwrap();

        let rt = eng.read().unwrap();
        let rows = blocks_by_range(&rt, start, count, Some(split)).unwrap();
        assert_eq!(rows.len(), 30);
        let slots: Vec<u64> = rows.iter().map(|r| r.slot.as_u64()).collect();
        assert_eq!(slots, (40..70).collect::<Vec<_>>());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn class_stats_move_under_synthetic_write_load() {
        // CC-43 /7 — store-side gauges for class=blocks|index move.
        let (dir, eng) = eng("stats");
        let before = measure_class_stats(&eng).unwrap();
        assert_eq!(before.blocks_rows, 0);
        assert_eq!(before.index_rows, 0);

        let mut b = eng.batch();
        {
            let rt = eng.read().unwrap();
            for i in 0u8..10 {
                let slot = Slot::new(u64::from(i));
                let root = root_n(i.wrapping_add(1));
                let ssz = synth_block(u64::from(i), &Root::ZERO, &root_n(0xFE));
                put_block(&rt, &mut b, slot, &root, &ssz, BlockRegion::Hot, true).unwrap();
                canonical::put_canonical(&rt, &mut b, slot, &root).unwrap();
            }
        }
        eng.commit(b).unwrap();

        let after = measure_class_stats(&eng).unwrap();
        assert_eq!(after.blocks_rows, 10);
        assert!(after.blocks_bytes > 0);
        // by-root (10) + canonical (10) + state_roots (10) = 30
        assert_eq!(after.index_rows, 30);
        assert!(after.index_bytes > 0);
        let _ = std::fs::remove_dir_all(&dir);
        // Prometheus names the storage service exposes (documentation anchor):
        let _ = (
            "cc_storage_bytes_total",
            "cc_storage_rows_total",
            "blocks",
            "index",
        );
    }

    #[test]
    fn fixed_offsets_match_arithmetic() {
        assert_eq!(PARENT_ROOT_SSZ_OFFSET, 116);
        assert_eq!(SLOT_SSZ_OFFSET, 100);
        assert_eq!(STATE_ROOT_SSZ_OFFSET, 148);
        let parent = root_n(0xAA);
        let state = root_n(0xBB);
        let ssz = synth_block(99, &parent, &state);
        assert_eq!(slot_at_offset(&ssz).unwrap(), Slot::new(99));
        assert_eq!(parent_root_at_offset(&ssz).unwrap(), parent);
        assert_eq!(state_root_at_offset(&ssz).unwrap(), state);
    }

    #[test]
    fn blocks_by_range_count_over_cap_fails_closed() {
        // SEC-43a-1: count > MAX_BLOCKS_BY_RANGE → Limit, not unbounded walk.
        let (dir, eng) = eng("range-cap");
        let rt = eng.read().unwrap();
        let err = blocks_by_range(&rt, Slot::new(0), MAX_BLOCKS_BY_RANGE + 1, None).unwrap_err();
        assert!(
            matches!(err, StoreError::Limit(_)),
            "expected Limit, got {err:?}"
        );
        let msg = err.to_string();
        assert!(
            msg.contains("MAX_BLOCKS_BY_RANGE")
                && msg.contains(&(MAX_BLOCKS_BY_RANGE + 1).to_string()),
            "{msg}"
        );
        // Boundary: exactly at the cap is allowed (empty store → empty result).
        assert!(blocks_by_range(&rt, Slot::new(0), MAX_BLOCKS_BY_RANGE, None).is_ok());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn put_block_rejects_caller_slot_mismatch_with_ssz() {
        // SEC-43a-2: caller slot must equal SSZ slot at offset 100.
        let (dir, eng) = eng("slot-mismatch");
        let ssz = synth_block(10, &Root::ZERO, &root_n(0x01));
        let mut b = eng.batch();
        let rt = eng.read().unwrap();
        let err = put_block(
            &rt,
            &mut b,
            Slot::new(99), // deliberately wrong
            &root_n(0x10),
            &ssz,
            BlockRegion::Hot,
            false,
        )
        .unwrap_err();
        assert!(matches!(err, StoreError::Codec(_)), "err={err:?}");
        let msg = err.to_string();
        assert!(
            msg.contains("slot mismatch") && msg.contains("99") && msg.contains("10"),
            "{msg}"
        );
        // Nothing staged when validation fails before puts.
        assert!(b.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(2_000))]

        #[test]
        fn shard_of_slots_in_inverse(slot in 0u64..1_000_000u64) {
            let s = Slot::new(slot);
            let id = shard_of(s);
            let (lo, hi) = slots_in(id);
            prop_assert!(s.as_u64() >= lo.as_u64());
            prop_assert!(s.as_u64() < hi.as_u64());
            // Name round-trip for I-shards.
            let name = blocks_shard_table(id);
            let parsed = crate::schema::parse_shard_table(&name);
            prop_assert_eq!(parsed, Some(("blocks", id)));
            // Width is 256 epochs.
            prop_assert_eq!(hi.as_u64() - lo.as_u64(), BLOCK_SHARD_EPOCHS * SLOTS_PER_EPOCH);
        }
    }
}
