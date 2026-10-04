//! Singleton SSZ meta records (Architecture §2.5).
//!
//! Ten containers live in the `meta` table under short ASCII keys (≤ 16 B),
//! plus `node_id` for I-node-id identity (S2-J-01; not an origin floor),
//! plus [`SnapshotCompletion`] (`snap_complete`) — the one snapshot
//! completion marker. Chunked snapshot writes reuse this key; they do
//! not add a second marker.
//! Each is **defined** here and **populated** by its owning issue:
//! `Split` / CC-41, `ServeWindow` / CC-48, `WriteCursor` / CC-44b,
//! `ForkChoiceScalars` / CC-45b, `PruneMarks` / CC-46a, `BackfillProgress` / CC-47a.
//! Schema version + config digest are written by [`crate::schema::Store::open`].

use ssz_derive::{Decode, Encode};
use ssz_types::VariableList;
use typenum::{U64, U128};

use cc_types::{Checkpoint, Root, Slot};

/// Meta table name (Architecture §2.2).
pub const TABLE_META: &str = "meta";

// ---------------------------------------------------------------------------
// Meta key names (ASCII, ≤ 16 bytes)
// ---------------------------------------------------------------------------

/// Key for [`SchemaVersion`].
pub const KEY_SCHEMA_VERSION: &str = "schema_version";
/// Key for [`ConfigDigest`].
pub const KEY_CONFIG_DIGEST: &str = "config_digest";
/// Identity bucket (ADR-R-11). Sixteen bytes, which is the meta-key cap.
pub const KEY_CONFIG_DIGEST_V2: &str = "config_digest_v2";
/// Schedule bucket (ADR-R-11).
///
/// `config_schedule_digest` is 22 bytes and fails the cap. This spelling is 15.
pub const KEY_SCHEDULE_DIGEST: &str = "schedule_digest";
/// Key for [`Split`].
pub const KEY_SPLIT: &str = "split";
/// Key for [`AnchorInfo`].
pub const KEY_ANCHOR_INFO: &str = "anchor_info";
/// Key for [`ColumnInfo`].
pub const KEY_COLUMN_INFO: &str = "column_info";
/// Key for [`ServeWindow`].
pub const KEY_SERVE_WINDOW: &str = "serve_window";
/// Key for [`WriteCursor`].
pub const KEY_WRITE_CURSOR: &str = "write_cursor";
/// Key for [`ForkChoiceScalars`].
pub const KEY_FC_SCALARS: &str = "fc_scalars";
/// Key for [`PruneMarks`].
pub const KEY_PRUNE_MARKS: &str = "prune_marks";
/// Key for [`BackfillProgress`].
pub const KEY_BACKFILL_PROG: &str = "backfill_prog";
/// Key for the I-node-id identity `Root` (S2-J-01). Not an `AnchorInfo` origin.
pub const KEY_NODE_ID: &str = "node_id";
/// Scheme of [`KEY_NODE_ID`] (ADR-R-10). Absent is the legacy raw key.
///
/// The value is the single byte `1` for the public-key fingerprint. Fourteen
/// bytes, under the 16-byte cap. `cc-store` does not interpret it: pairing
/// stays a compare of 32 opaque bytes. Unknown keys are not a schema bump.
pub const KEY_NODE_ID_SCHEME: &str = "node_id_scheme";
/// Key for [`SnapshotCompletion`]. One marker for the anchor and for chunked snapshots.
pub const KEY_SNAPSHOT_COMPLETION: &str = "snap_complete";

/// All meta singleton keys (for inventory / docs).
pub const META_KEYS: &[&str] = &[
    KEY_SCHEMA_VERSION,
    KEY_CONFIG_DIGEST,
    KEY_CONFIG_DIGEST_V2,
    KEY_SCHEDULE_DIGEST,
    KEY_SPLIT,
    KEY_ANCHOR_INFO,
    KEY_COLUMN_INFO,
    KEY_SERVE_WINDOW,
    KEY_WRITE_CURSOR,
    KEY_FC_SCALARS,
    KEY_PRUNE_MARKS,
    KEY_BACKFILL_PROG,
    KEY_NODE_ID,
    KEY_NODE_ID_SCHEME,
    KEY_SNAPSHOT_COMPLETION,
];

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

/// Schema version singleton. Mismatch → refuse open (CC-40 /5).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode)]
pub struct SchemaVersion {
    /// Monotonic integer; Phase 4 has one value and no migration path.
    pub version: u32,
}

/// Network-config digest singleton. Mismatch → refuse open (CC-40 /6).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode)]
pub struct ConfigDigest {
    /// SHA-256 over the named §2.5 `ChainConfig` field list (see [`crate::schema`]).
    pub digest: Root,
}

/// Hot/cold split point (CC-41).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode)]
pub struct Split {
    /// Highest slot that has been migrated into the cold region.
    pub slot: Slot,
    /// State root at the split.
    pub state_root: Root,
    /// Block root at the split.
    pub block_root: Root,
}

/// Anchor / checkpoint-sync origin (Lighthouse shape + `node_id` for §1.7).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode)]
pub struct AnchorInfo {
    /// Slot of the anchor block.
    pub anchor_slot: Slot,
    /// Root of the anchor block.
    pub anchor_root: Root,
    /// State root of the anchor state.
    pub anchor_state_root: Root,
    /// Node id this store's columns were custodied for (`I-node-id`).
    pub node_id: Root,
    /// Oldest block slot retained (parent-linkage walk start).
    pub oldest_block_slot: Slot,
    /// Parent of the oldest retained block.
    pub oldest_block_parent: Root,
}

/// Newest snapshot whose bytes are complete.
///
/// A `snapshots` row is not the newest snapshot until this record names
/// its slot and byte length. The anchor writes it in the same transaction
/// as the first snapshot. A later chunked snapshot updates this same key
/// in the final chunk's transaction — not a second marker.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode)]
pub struct SnapshotCompletion {
    /// Slot of the completed snapshot.
    pub slot: Slot,
    /// Caller-supplied state root. Storage does not hash the snapshot SSZ.
    pub state_root: Root,
    /// Length of the completed value at `snapshots[slot]`.
    pub bytes: u64,
}

/// Column custody watermark info.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode)]
pub struct ColumnInfo {
    /// Custody group count at which columns were written.
    pub cgc: u64,
    /// Oldest slot with custodied columns.
    pub oldest_custodied_column_slot: Slot,
}

/// Inclusive-exclusive slot range used by [`ServeWindow::holes`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode)]
pub struct SlotRange {
    /// First slot of the hole (inclusive).
    pub start: Slot,
    /// First slot after the hole (exclusive).
    pub end: Slot,
}

/// Serve window: `earliest_available_slot` and `cgc` in **one** container (§2.5 / I3).
///
/// Stronger than "written in one transaction": a container cannot be split into
/// two independent puts by a well-meaning refactor.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct ServeWindow {
    /// Earliest slot this node advertises as available.
    pub earliest_available_slot: Slot,
    /// Custody group count advertised with the window.
    pub cgc: u64,
    /// Window branch tag (CC-49).
    pub branch: u8,
    /// Block floor used to derive the window.
    pub block_floor: Slot,
    /// Column floor used to derive the window.
    pub column_floor: Slot,
    /// Known gaps in the parent-linkage walk (≤ 64).
    pub holes: VariableList<SlotRange, U64>,
}

impl Default for ServeWindow {
    fn default() -> Self {
        Self {
            earliest_available_slot: Slot::ZERO,
            cgc: 0,
            branch: 0,
            block_floor: Slot::ZERO,
            column_floor: Slot::ZERO,
            holes: VariableList::default(),
        }
    }
}

/// Chain write-behind cursor (CC-44b); mirrors chain's `Cursor` fields.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode)]
pub struct WriteCursor {
    /// Session identifier for the write-behind producer.
    pub session_id: u64,
    /// Monotonic sequence within the session.
    pub seq: u64,
    /// Slot covered by this cursor position.
    pub slot: Slot,
    /// Block root at this cursor position.
    pub root: Root,
}

/// Persisted fork-choice scalars (~300 B; ADR P4-06 / CC-45b).
///
/// Vote tables are **not** stored in Phase 4 — only non-derivable scalars.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode)]
pub struct ForkChoiceScalars {
    /// Fork-choice store time.
    pub time: u64,
    /// Proposer-boost root (load-bearing across restart).
    pub proposer_boost_root: Root,
    /// Justified checkpoint.
    pub justified: Checkpoint,
    /// Finalized checkpoint.
    pub finalized: Checkpoint,
    /// Unrealized justified checkpoint.
    pub unrealized_justified: Checkpoint,
    /// Unrealized finalized checkpoint.
    pub unrealized_finalized: Checkpoint,
    /// Current head root.
    pub head_root: Root,
    /// Current head slot.
    pub head_slot: Slot,
}

/// Independent prune watermarks (CC-46a).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Encode, Decode)]
pub struct PruneMarks {
    /// Columns pruned up to this slot (exclusive upper bound of deleted range).
    pub columns_up_to: Slot,
    /// Blocks pruned up to this slot.
    pub blocks_up_to: Slot,
    /// Snapshots / states pruned up to this slot.
    pub states_up_to: Slot,
    /// State-root index pruned up to this slot.
    pub state_roots_up_to: Slot,
}

/// Backfill progress (CC-47a), including per-column-index oldest for CC-4G.
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct BackfillProgress {
    /// Oldest block slot still needed / reached by block backfill.
    pub blocks_oldest: Slot,
    /// Parent of `blocks_oldest` (linkage).
    pub blocks_oldest_parent: Root,
    /// Oldest column slot overall.
    pub columns_oldest: Slot,
    /// Per column-index oldest slot (`NUMBER_OF_COLUMNS = 128`).
    pub per_index_oldest: VariableList<Slot, U128>,
}

impl Default for BackfillProgress {
    fn default() -> Self {
        Self {
            blocks_oldest: Slot::ZERO,
            blocks_oldest_parent: Root::default(),
            columns_oldest: Slot::ZERO,
            per_index_oldest: VariableList::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use proptest::prelude::*;
    use ssz::{Decode, Encode};

    fn arb_root() -> impl Strategy<Value = Root> {
        any::<[u8; 32]>().prop_map(Root::from_array)
    }

    fn arb_slot() -> impl Strategy<Value = Slot> {
        any::<u64>().prop_map(Slot::new)
    }

    fn arb_checkpoint() -> impl Strategy<Value = Checkpoint> {
        (any::<u64>(), arb_root()).prop_map(|(e, r)| Checkpoint {
            epoch: cc_types::Epoch::new(e),
            root: r,
        })
    }

    fn arb_slot_range() -> impl Strategy<Value = SlotRange> {
        (arb_slot(), arb_slot()).prop_map(|(start, end)| SlotRange { start, end })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn schema_version_ssz_roundtrip(version in any::<u32>()) {
            let v = SchemaVersion { version };
            let bytes = v.as_ssz_bytes();
            let decoded = SchemaVersion::from_ssz_bytes(&bytes).unwrap();
            prop_assert_eq!(v, decoded);
        }

        #[test]
        fn config_digest_ssz_roundtrip(d in arb_root()) {
            let v = ConfigDigest { digest: d };
            let bytes = v.as_ssz_bytes();
            let decoded = ConfigDigest::from_ssz_bytes(&bytes).unwrap();
            prop_assert_eq!(v, decoded);
        }

        #[test]
        fn split_ssz_roundtrip(
            slot in arb_slot(),
            state_root in arb_root(),
            block_root in arb_root(),
        ) {
            let v = Split { slot, state_root, block_root };
            let decoded = Split::from_ssz_bytes(&v.as_ssz_bytes()).unwrap();
            prop_assert_eq!(v, decoded);
        }

        #[test]
        fn anchor_info_ssz_roundtrip(
            anchor_slot in arb_slot(),
            anchor_root in arb_root(),
            anchor_state_root in arb_root(),
            node_id in arb_root(),
            oldest_block_slot in arb_slot(),
            oldest_block_parent in arb_root(),
        ) {
            let v = AnchorInfo {
                anchor_slot,
                anchor_root,
                anchor_state_root,
                node_id,
                oldest_block_slot,
                oldest_block_parent,
            };
            let decoded = AnchorInfo::from_ssz_bytes(&v.as_ssz_bytes()).unwrap();
            prop_assert_eq!(v, decoded);
        }

        #[test]
        fn column_info_ssz_roundtrip(cgc in any::<u64>(), oldest in arb_slot()) {
            let v = ColumnInfo {
                cgc,
                oldest_custodied_column_slot: oldest,
            };
            let decoded = ColumnInfo::from_ssz_bytes(&v.as_ssz_bytes()).unwrap();
            prop_assert_eq!(v, decoded);
        }

        #[test]
        fn serve_window_ssz_roundtrip(
            earliest in arb_slot(),
            cgc in any::<u64>(),
            branch in any::<u8>(),
            block_floor in arb_slot(),
            column_floor in arb_slot(),
            holes in proptest::collection::vec(arb_slot_range(), 0..=64),
        ) {
            let holes = VariableList::new(holes).unwrap();
            let v = ServeWindow {
                earliest_available_slot: earliest,
                cgc,
                branch,
                block_floor,
                column_floor,
                holes,
            };
            let decoded = ServeWindow::from_ssz_bytes(&v.as_ssz_bytes()).unwrap();
            prop_assert_eq!(v, decoded);
        }

        #[test]
        fn write_cursor_ssz_roundtrip(
            session_id in any::<u64>(),
            seq in any::<u64>(),
            slot in arb_slot(),
            root in arb_root(),
        ) {
            let v = WriteCursor {
                session_id,
                seq,
                slot,
                root,
            };
            let decoded = WriteCursor::from_ssz_bytes(&v.as_ssz_bytes()).unwrap();
            prop_assert_eq!(v, decoded);
        }

        #[test]
        fn fork_choice_scalars_ssz_roundtrip(
            time in any::<u64>(),
            proposer_boost_root in arb_root(),
            justified in arb_checkpoint(),
            finalized in arb_checkpoint(),
            unrealized_justified in arb_checkpoint(),
            unrealized_finalized in arb_checkpoint(),
            head_root in arb_root(),
            head_slot in arb_slot(),
        ) {
            let v = ForkChoiceScalars {
                time,
                proposer_boost_root,
                justified,
                finalized,
                unrealized_justified,
                unrealized_finalized,
                head_root,
                head_slot,
            };
            let decoded = ForkChoiceScalars::from_ssz_bytes(&v.as_ssz_bytes()).unwrap();
            prop_assert_eq!(v, decoded);
        }

        #[test]
        fn prune_marks_ssz_roundtrip(
            columns_up_to in arb_slot(),
            blocks_up_to in arb_slot(),
            states_up_to in arb_slot(),
            state_roots_up_to in arb_slot(),
        ) {
            let v = PruneMarks {
                columns_up_to,
                blocks_up_to,
                states_up_to,
                state_roots_up_to,
            };
            let decoded = PruneMarks::from_ssz_bytes(&v.as_ssz_bytes()).unwrap();
            prop_assert_eq!(v, decoded);
        }

        #[test]
        fn backfill_progress_ssz_roundtrip(
            blocks_oldest in arb_slot(),
            blocks_oldest_parent in arb_root(),
            columns_oldest in arb_slot(),
            per_index in proptest::collection::vec(arb_slot(), 0..=128),
        ) {
            let per_index_oldest = VariableList::new(per_index).unwrap();
            let v = BackfillProgress {
                blocks_oldest,
                blocks_oldest_parent,
                columns_oldest,
                per_index_oldest,
            };
            let decoded = BackfillProgress::from_ssz_bytes(&v.as_ssz_bytes()).unwrap();
            prop_assert_eq!(v, decoded);
        }
    }

    #[test]
    fn meta_keys_fit_sixteen_bytes() {
        for k in META_KEYS {
            assert!(
                k.len() <= 16,
                "meta key {k:?} is {} bytes (max 16)",
                k.len()
            );
            assert!(!k.is_empty());
            assert!(k.is_ascii());
        }
    }

    /// ADR-R-11: only the schedule spelling is shortened. The cap is not raised.
    #[test]
    fn side_key_names_fit_the_cap_without_merging() {
        assert_eq!(KEY_CONFIG_DIGEST_V2, "config_digest_v2");
        assert_eq!(KEY_CONFIG_DIGEST_V2.len(), 16);
        assert_eq!(KEY_SCHEDULE_DIGEST, "schedule_digest");
        assert!(KEY_SCHEDULE_DIGEST.len() <= 16);
        assert!(
            "config_schedule_digest".len() > 16,
            "the spelled-out schedule key must stay illegal"
        );
        assert_ne!(KEY_SCHEDULE_DIGEST, KEY_CONFIG_DIGEST);
        assert_ne!(KEY_CONFIG_DIGEST_V2, KEY_CONFIG_DIGEST);
        assert_ne!(KEY_SCHEDULE_DIGEST, KEY_CONFIG_DIGEST_V2);
    }
}
