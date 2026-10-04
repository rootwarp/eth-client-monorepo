//! Head snapshot published by the core thread (Architecture §7.1, ADR-P1-09).
//!
//! `GetHead` is a pointer load on [`HeadSnapshotStore`] — it never queues behind
//! an epoch transition on the core thread.

use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use cc_types::containers::Checkpoint;
use cc_types::primitives::{Root, Slot};

/// Immutable head view published after every import (Architecture §7.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadSnapshot {
    pub head_root: Root,
    pub head_slot: Slot,
    pub head_state_root: Root,
    pub justified: Checkpoint,
    pub finalized: Checkpoint,
    pub unrealized_justified: Checkpoint,
    pub unrealized_finalized: Checkpoint,
    /// Phase 6 attestation target; zero until duties land.
    pub current_epoch_target_root: Root,
    /// Phase 6 duties dependent root; zero until duties land.
    pub dependent_root: Root,
    /// Node-level optimistic (CC-3B / CC-34c): derived from fork choice after
    /// fresh `get_head`, not from engine liveness.
    pub is_optimistic: bool,
    /// Monotonic publish sequence (core-thread producer only).
    pub sequence: u64,
}

impl Default for HeadSnapshot {
    fn default() -> Self {
        Self {
            head_root: Root::ZERO,
            head_slot: Slot::new(0),
            head_state_root: Root::ZERO,
            justified: Checkpoint::default(),
            finalized: Checkpoint::default(),
            unrealized_justified: Checkpoint::default(),
            unrealized_finalized: Checkpoint::default(),
            current_epoch_target_root: Root::ZERO,
            dependent_root: Root::ZERO,
            is_optimistic: false,
            sequence: 0,
        }
    }
}

/// Shared head snapshot: core thread writes, gRPC `GetHead` reads.
///
/// `durable_head` is the last root for which `set_head` or
/// `commit_import.head` returned `Ok` in this process. `load` does not take
/// that lock. Clones share both handles.
#[derive(Debug, Clone)]
pub struct HeadSnapshotStore {
    inner: Arc<ArcSwap<HeadSnapshot>>,
    durable_head: Arc<Mutex<Option<Root>>>,
}

impl HeadSnapshotStore {
    /// Empty snapshot (pre-bootstrap / before first publish).
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(HeadSnapshot::default())),
            durable_head: Arc::new(Mutex::new(None)),
        }
    }

    /// Seed from an initial snapshot (tests / post-bootstrap).
    ///
    /// The durable-head ack starts empty: a seeded snapshot is not a commit.
    pub fn with_snapshot(snapshot: HeadSnapshot) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(snapshot)),
            durable_head: Arc::new(Mutex::new(None)),
        }
    }

    /// Last head this process durably committed, if any.
    pub fn durable_head(&self) -> Option<Root> {
        *self
            .durable_head
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Record a head after `set_head` or `commit_import.head` returns `Ok`.
    pub fn set_durable_head(&self, root: Root) {
        *self
            .durable_head
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(root);
    }

    /// Clone of the `ArcSwap` handle (core thread + service share this).
    pub fn arc_swap(&self) -> Arc<ArcSwap<HeadSnapshot>> {
        Arc::clone(&self.inner)
    }

    /// Pointer load — no core-thread interaction.
    pub fn load(&self) -> Arc<HeadSnapshot> {
        self.inner.load_full()
    }

    /// Publish a new snapshot (core thread only).
    pub fn store(&self, snapshot: HeadSnapshot) {
        self.inner.store(Arc::new(snapshot));
    }
}

impl Default for HeadSnapshotStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    #[test]
    fn load_returns_published_snapshot() {
        let store = HeadSnapshotStore::new();
        assert_eq!(store.load().sequence, 0);
        store.store(HeadSnapshot {
            head_slot: Slot::new(42),
            sequence: 1,
            ..HeadSnapshot::default()
        });
        let snap = store.load();
        assert_eq!(snap.head_slot.as_u64(), 42);
        assert_eq!(snap.sequence, 1);
    }
}
