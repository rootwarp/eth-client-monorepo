//! Snapshot ring: full uncompressed state SSZ under `slot:u64be` (CC-42).
//!
//! Architecture §3.3–3.6 / ADR P4-14:
//! - Cadence (`storage.snapshot_epochs`, default **32**) and ring depth
//!   (`storage.snapshot_ring`, default **4**) are config in the service layer.
//! - Values are **opaque SSZ bytes** — this crate never decodes consensus state
//!   containers and never compresses (ADR P4-14).
//! - Ring eviction is staged as puts + deletes for the single-writer P2 path;
//!   production never calls [`Engine::commit`] from the replay task.

use ssz::{Decode, Encode};

use cc_types::Slot;

use crate::engine::{Batch, Engine, ReadTxn, StoreError};
use crate::keys::{decode_snapshot_key, encode_cold_block_key};
use crate::meta::{KEY_SNAPSHOT_COMPLETION, SnapshotCompletion, TABLE_META};

/// Snapshot table name (Architecture §2.2).
pub const TABLE_SNAPSHOTS: &str = "snapshots";

/// Default snapshot cadence in epochs (Grandine `DEFAULT_ARCHIVAL_EPOCH_INTERVAL`).
pub const DEFAULT_SNAPSHOT_EPOCHS: u64 = 32;

/// Default ring depth (re-export name aligned with invariants / schema).
pub use crate::invariants::DEFAULT_SNAPSHOT_RING;

/// Hard cap on a single uncompressed snapshot value (fail closed).
///
/// Aligns with [`crate::engine::MAX_RANGE_BYTES`] (512 MiB). Measured Hoodi
/// states are ~196 MiB; the 2 GiB ring-bytes compression revisit trigger is
/// separate (ADR P4-14).
pub const MAX_SNAPSHOT_BYTES: u64 = 512 * 1024 * 1024;

/// Encode a snapshot key (`slot:u64be`).
#[must_use]
pub fn encode_snapshot_key(slot: Slot) -> [u8; 8] {
    encode_cold_block_key(slot)
}

/// Plan returned by [`plan_snapshot_put`]: one put plus optional oldest-first deletes.
#[derive(Debug, Default, Clone)]
pub struct SnapshotPlan {
    /// `(table, key, value)` puts (exactly one snapshot put when non-empty ssz).
    pub puts: Vec<(String, Vec<u8>, Vec<u8>)>,
    /// `(table, key)` deletes for ring eviction (oldest first).
    pub deletes: Vec<(String, Vec<u8>)>,
    /// Ring depth after applying this plan (≤ configured ring).
    pub depth_after: u64,
    /// Slots that will be evicted by this plan.
    pub evicted: Vec<Slot>,
    /// Slot of the snapshot being written.
    pub slot: Slot,
    /// Byte length of the uncompressed SSZ payload (equals stored value length).
    pub bytes: u64,
}

/// Load snapshot SSZ at `slot`, if present.
pub fn get_snapshot(rt: &ReadTxn, slot: Slot) -> Result<Option<Vec<u8>>, StoreError> {
    rt.get(TABLE_SNAPSHOTS, &encode_snapshot_key(slot))
}

/// List all snapshot slots ascending (O(ring); ring is config-bounded).
pub fn list_snapshot_slots(rt: &ReadTxn) -> Result<Vec<Slot>, StoreError> {
    let lo = encode_snapshot_key(Slot::ZERO);
    let hi = encode_snapshot_key(Slot::new(u64::MAX));
    let mut slots = Vec::new();
    for item in rt.range(TABLE_SNAPSHOTS, &lo, &hi)? {
        let (k, _) = item?;
        if let Some(slot) = decode_snapshot_key(&k) {
            slots.push(slot);
        }
    }
    Ok(slots)
}

/// Current ring depth (`|snapshots|`).
pub fn ring_depth(rt: &ReadTxn) -> Result<u64, StoreError> {
    Ok(list_snapshot_slots(rt)?.len() as u64)
}

/// Newest (highest-slot) snapshot, if any.
pub fn newest_snapshot(rt: &ReadTxn) -> Result<Option<(Slot, Vec<u8>)>, StoreError> {
    let slots = list_snapshot_slots(rt)?;
    let Some(&slot) = slots.last() else {
        return Ok(None);
    };
    let ssz = get_snapshot(rt, slot)?.ok_or_else(|| {
        StoreError::Codec(format!(
            "snapshot slot {} listed but missing value",
            slot.as_u64()
        ))
    })?;
    Ok(Some((slot, ssz)))
}

/// Read [`SnapshotCompletion`], if the marker key is present.
pub fn load_snapshot_completion(rt: &ReadTxn) -> Result<Option<SnapshotCompletion>, StoreError> {
    let Some(bytes) = rt.get(TABLE_META, KEY_SNAPSHOT_COMPLETION.as_bytes())? else {
        return Ok(None);
    };
    SnapshotCompletion::from_ssz_bytes(&bytes)
        .map(Some)
        .map_err(|e| StoreError::Codec(format!("snap_complete: {e:?}")))
}

/// Snapshot named by the completion marker, when the stored length matches.
///
/// A ring row with no marker, or a value whose length differs from
/// [`SnapshotCompletion::bytes`], is not complete. This does not scan for
/// a higher `snapshots` key — a partial chunk above the marker stays invisible.
pub fn completed_snapshot(
    rt: &ReadTxn,
) -> Result<Option<(SnapshotCompletion, Vec<u8>)>, StoreError> {
    let Some(marker) = load_snapshot_completion(rt)? else {
        return Ok(None);
    };
    let Some(ssz) = get_snapshot(rt, marker.slot)? else {
        return Ok(None);
    };
    if ssz.len() as u64 != marker.bytes {
        return Ok(None);
    }
    Ok(Some((marker, ssz)))
}

/// Stage the completion marker. The caller commits it with the snapshot bytes.
pub fn put_snapshot_completion(batch: &mut Batch, marker: &SnapshotCompletion) {
    batch.put(
        TABLE_META,
        KEY_SNAPSHOT_COMPLETION.as_bytes(),
        &marker.as_ssz_bytes(),
    );
}

/// Oldest (lowest-slot) snapshot slot, if any.
pub fn oldest_snapshot_slot(rt: &ReadTxn) -> Result<Option<Slot>, StoreError> {
    Ok(list_snapshot_slots(rt)?.into_iter().next())
}

/// Stage a full-state snapshot put and ring-eviction deletes.
///
/// - Overwriting the same slot replaces the value (no depth change from that put).
/// - After the put, if depth would exceed `ring`, the **oldest** rows are deleted
///   until depth equals `ring` (CC-42 /1).
/// - `ring == 0` is treated as 1 so a configured ring always retains the newest.
///
/// Fail closed when `len` exceeds [`MAX_SNAPSHOT_BYTES`].
pub fn check_snapshot_len(slot: Slot, len: u64) -> Result<(), StoreError> {
    if len > MAX_SNAPSHOT_BYTES {
        return Err(StoreError::limit(format!(
            "snapshot at slot {} is {len} bytes; exceeds MAX_SNAPSHOT_BYTES ({MAX_SNAPSHOT_BYTES})",
            slot.as_u64()
        )));
    }
    Ok(())
}

/// **Uncompressed:** `ssz` is stored byte-identical — no compression.
///
/// Refuses payloads larger than [`MAX_SNAPSHOT_BYTES`] with [`StoreError::Limit`].
pub fn plan_snapshot_put(
    rt: &ReadTxn,
    slot: Slot,
    ssz: &[u8],
    ring: u64,
) -> Result<SnapshotPlan, StoreError> {
    check_snapshot_len(slot, ssz.len() as u64)?;
    let ring = ring.max(1);
    let mut slots = list_snapshot_slots(rt)?;
    let replacing = slots.iter().any(|s| s.as_u64() == slot.as_u64());
    if !replacing {
        slots.push(slot);
        slots.sort_by_key(|s| s.as_u64());
        // Dedup if any (defensive).
        slots.dedup_by_key(|s| s.as_u64());
    }

    let mut plan = SnapshotPlan {
        puts: vec![(
            TABLE_SNAPSHOTS.to_owned(),
            encode_snapshot_key(slot).to_vec(),
            ssz.to_vec(),
        )],
        deletes: Vec::new(),
        depth_after: slots.len() as u64,
        evicted: Vec::new(),
        slot,
        bytes: ssz.len() as u64,
    };

    while (slots.len() as u64) > ring {
        let oldest = slots.remove(0);
        // Never evict the row we are writing (ring=1 replace path already handled).
        if oldest.as_u64() == slot.as_u64() {
            // Pathological: only the new slot remains but count still > ring — break.
            break;
        }
        plan.deletes.push((
            TABLE_SNAPSHOTS.to_owned(),
            encode_snapshot_key(oldest).to_vec(),
        ));
        plan.evicted.push(oldest);
    }
    plan.depth_after = slots.len() as u64;
    Ok(plan)
}

/// Apply a [`SnapshotPlan`] into an existing batch (no commit).
pub fn apply_snapshot_plan(batch: &mut Batch, plan: &SnapshotPlan) {
    for (table, key, value) in &plan.puts {
        batch.put(table, key, value);
    }
    for (table, key) in &plan.deletes {
        batch.delete(table, key);
    }
}

/// Direct put+commit helper for unit tests (production uses the writer P2 path).
pub fn put_snapshot(
    engine: &Engine,
    slot: Slot,
    ssz: &[u8],
    ring: u64,
) -> Result<SnapshotPlan, StoreError> {
    let plan = {
        let rt = engine.read()?;
        plan_snapshot_put(&rt, slot, ssz, ring)?
    };
    let mut batch = engine.batch();
    apply_snapshot_plan(&mut batch, &plan);
    engine.commit(batch)?;
    Ok(plan)
}

/// Whether a finalized epoch should produce a snapshot given the last snap epoch.
///
/// First snapshot (`last_snapshot_epoch == None`) is always due when there is a
/// finalized checkpoint. Thereafter every `cadence` epochs of advance.
#[must_use]
pub fn snapshot_due(
    last_snapshot_epoch: Option<u64>,
    finalized_epoch: u64,
    cadence_epochs: u64,
) -> bool {
    let cadence = cadence_epochs.max(1);
    match last_snapshot_epoch {
        None => true,
        Some(last) => finalized_epoch.saturating_sub(last) >= cadence,
    }
}

/// Epoch of a slot (`slot // 32`).
#[must_use]
pub fn epoch_of_slot(slot: Slot) -> u64 {
    slot.as_u64() / crate::keys::SLOTS_PER_EPOCH
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use crate::engine::{Durability, Engine, EngineOptions};
    use cc_types::Root;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn open_engine(tag: &str) -> Engine {
        static N: AtomicU64 = AtomicU64::new(0);
        let id = N.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("cc-store-snap-{tag}-{id}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Engine::open(
            &dir,
            EngineOptions::default().with_durability(Durability::None),
        )
        .unwrap()
    }

    #[test]
    fn fifth_snapshot_evicts_oldest_ring_depth_stays_four() {
        // CC-42 /1: write a fifth snapshot; oldest evicted; depth stays 4.
        let engine = open_engine("ring4");
        let ring = 4u64;
        let payloads: Vec<Vec<u8>> = (0..5)
            .map(|i| format!("state-ssz-{i}").into_bytes())
            .collect();

        for (i, ssz) in payloads.iter().enumerate() {
            let slot = Slot::new((i as u64) * 32 * 32); // every 32 epochs
            let plan = put_snapshot(&engine, slot, ssz, ring).unwrap();
            assert!(
                plan.bytes == ssz.len() as u64,
                "uncompressed: stored length must equal SSZ length"
            );
            let rt = engine.read().unwrap();
            let depth = ring_depth(&rt).unwrap();
            assert!(
                depth <= ring,
                "depth {depth} exceeds ring {ring} after put #{i}"
            );
            if i < 4 {
                assert_eq!(depth, (i as u64) + 1);
                assert!(plan.evicted.is_empty());
            } else {
                assert_eq!(depth, 4, "fifth put must keep ring depth at 4");
                assert_eq!(plan.evicted.len(), 1);
                assert_eq!(plan.evicted[0].as_u64(), 0);
            }
        }

        let rt = engine.read().unwrap();
        let slots = list_snapshot_slots(&rt).unwrap();
        assert_eq!(slots.len(), 4);
        assert_eq!(slots[0].as_u64(), 32 * 32); // first evicted
        assert!(get_snapshot(&rt, Slot::new(0)).unwrap().is_none());
        assert_eq!(get_snapshot(&rt, slots[3]).unwrap().unwrap(), payloads[4]);
    }

    #[test]
    fn stored_bytes_equal_ssz_no_compression() {
        // CC-42: snapshots are uncompressed (ADR P4-14).
        let engine = open_engine("uncomp");
        let ssz = vec![0xABu8; 10_000];
        put_snapshot(&engine, Slot::new(96), &ssz, 4).unwrap();
        let rt = engine.read().unwrap();
        let loaded = get_snapshot(&rt, Slot::new(96)).unwrap().unwrap();
        assert_eq!(loaded.len(), ssz.len());
        assert_eq!(loaded, ssz);
    }

    #[test]
    fn snapshot_due_cadence() {
        assert!(snapshot_due(None, 0, 32));
        assert!(!snapshot_due(Some(0), 31, 32));
        assert!(snapshot_due(Some(0), 32, 32));
        assert!(snapshot_due(Some(16), 32, 16));
        assert!(!snapshot_due(Some(16), 31, 16));
    }

    #[test]
    fn source_has_no_compression_crate_names() {
        // AC: `grep -rn "flate\|zstd\|snap::" crates/store/src/snapshots.rs` returns
        // nothing in the production body (above `#[cfg(test)]`).
        let src = include_str!("snapshots.rs");
        let production = src
            .split("#[cfg(test)]")
            .next()
            .expect("test module marker");
        // Build needles without writing the banned tokens as contiguous literals
        // in a way that would fail a naive whole-file grep of this test itself —
        // the AC grep is on the production path; we scan only that slice.
        let a = ["fl", "ate"].concat();
        let b = ["zs", "td"].concat();
        let c = ["snap", "::"].concat();
        for needle in [a.as_str(), b.as_str(), c.as_str()] {
            assert!(
                !production.contains(needle),
                "snapshots.rs production body must not name compression crate {needle:?}"
            );
        }
    }

    #[test]
    fn newest_and_list_order() {
        let engine = open_engine("newest");
        put_snapshot(&engine, Slot::new(64), b"a", 4).unwrap();
        put_snapshot(&engine, Slot::new(32), b"b", 4).unwrap();
        put_snapshot(&engine, Slot::new(96), b"c", 4).unwrap();
        let rt = engine.read().unwrap();
        let slots = list_snapshot_slots(&rt).unwrap();
        assert_eq!(
            slots.iter().map(|s| s.as_u64()).collect::<Vec<_>>(),
            vec![32, 64, 96]
        );
        let (slot, ssz) = newest_snapshot(&rt).unwrap().unwrap();
        assert_eq!(slot.as_u64(), 96);
        assert_eq!(ssz, b"c");
    }

    #[test]
    fn refuse_oversized_snapshot_put() {
        let engine = open_engine("oversize");
        let rt = engine.read().unwrap();
        plan_snapshot_put(&rt, Slot::new(1), b"ok", 4).unwrap();
        let over = MAX_SNAPSHOT_BYTES.saturating_add(1);
        let err = check_snapshot_len(Slot::new(0), over).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("MAX_SNAPSHOT_BYTES") && msg.contains(&over.to_string()),
            "{msg}"
        );
        check_snapshot_len(Slot::new(0), MAX_SNAPSHOT_BYTES).unwrap();
    }

    #[test]
    fn completion_marker_hides_a_partial_ring_row() {
        let engine = open_engine("marker");
        let slot = Slot::new(32);
        put_snapshot(&engine, slot, b"partial-chunk", 4).unwrap();
        {
            let rt = engine.read().unwrap();
            assert!(load_snapshot_completion(&rt).unwrap().is_none());
            assert!(completed_snapshot(&rt).unwrap().is_none());
            assert!(newest_snapshot(&rt).unwrap().is_some());
        }

        let full = b"full-state-ssz-bytes".to_vec();
        put_snapshot(&engine, slot, &full, 4).unwrap();
        let marker = SnapshotCompletion {
            slot,
            state_root: Root::from_array([0xAB; 32]),
            bytes: full.len() as u64,
        };
        let mut batch = engine.batch();
        put_snapshot_completion(&mut batch, &marker);
        engine.commit(batch).unwrap();
        {
            let rt = engine.read().unwrap();
            let (got, ssz) = completed_snapshot(&rt).unwrap().unwrap();
            assert_eq!(got, marker);
            assert_eq!(ssz, full);
        }

        let short = SnapshotCompletion { bytes: 1, ..marker };
        let mut batch = engine.batch();
        put_snapshot_completion(&mut batch, &short);
        engine.commit(batch).unwrap();
        let rt = engine.read().unwrap();
        assert!(completed_snapshot(&rt).unwrap().is_none());
        assert_eq!(load_snapshot_completion(&rt).unwrap(), Some(short));
    }
}
