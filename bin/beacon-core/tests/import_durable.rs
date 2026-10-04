//! Durable rows after production `boot()` commits the anchor.
//!
//! The composer is the only anchor writer. This test calls
//! [`ArchiveWrite::commit_import`] for the anchor's first child and reads
//! the rows back, including after the store is reopened. It does not
//! hand-seed an anchor.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

#[path = "support/anchor_fixture.rs"]
#[allow(dead_code)]
mod anchor_fixture;

use std::time::Duration;

use anchor_fixture::{anchor_fixture, beacon_config, first_child, genesis_bytes, spawn_el_stub};
use cc_beacon_core::boot::boot;
use cc_chain_core::import::ForkChoiceScalarsPayload;
use cc_seam::{ArchiveWrite, Bytes, DaVerdict, DurableImport, HeadCause, HeadChange};
use cc_storage_core::DurableDaStatus;
use cc_types::containers::Checkpoint;
use cc_types::primitives::{Epoch, Root, Slot};
use ssz::Encode;

struct TempDir(std::path::PathBuf);

impl TempDir {
    fn new(prefix: &str) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let path = std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()));
        std::fs::create_dir_all(&path).expect("temp dir");
        Self(path)
    }

    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn child_scalars(head: Root, slot: Slot) -> Vec<u8> {
    let checkpoint = Checkpoint {
        epoch: Epoch::new(0),
        root: head,
    };
    ForkChoiceScalarsPayload {
        time: slot.as_u64().saturating_mul(6),
        proposer_boost_root: Root::ZERO,
        justified: checkpoint,
        finalized: checkpoint,
        unrealized_justified: checkpoint,
        unrealized_finalized: checkpoint,
        head_root: head,
        head_slot: slot,
    }
    .as_ssz_bytes()
}

#[tokio::test]
async fn import_on_block_then_commit_writes_durable_rows() {
    let fixture = anchor_fixture();
    let el = spawn_el_stub();
    let dir = TempDir::new("import-durable-boot");
    let mut cfg = beacon_config(dir.path(), el.endpoint().to_owned());
    cfg.checkpoint_providers.clear();
    cfg.checkpoint_provider = None;
    cfg.genesis_anchor = Some(genesis_bytes(&fixture));

    let node = boot(cfg).await.expect("boot returns before serve");
    let child = first_child(&fixture);
    let slot = child.signed.message.slot.as_u64();
    assert_eq!(slot, 1, "the anchor's first child is slot 1");
    let root = *child.root.as_array();
    let parent = *fixture.anchor_root.as_array();
    let state_root = *child.signed.message.state_root.as_array();
    let ssz = child.signed.as_ssz_bytes();
    let slots = [0u64, slot];

    let before = node
        .durable_frontier(&slots)
        .expect("frontier before import");
    let before_seq = before.cursor.expect("anchor advanced the cursor").seq;
    assert_eq!(
        before.canonical[0],
        Some(parent),
        "composer anchor is canonical"
    );
    assert_eq!(
        before.canonical[1], None,
        "child is not durable before import"
    );

    node.archive()
        .commit_import(DurableImport {
            block_root: root,
            parent_root: parent,
            slot,
            state_root,
            ssz: Bytes::from(ssz.clone()),
            da: DaVerdict::Available,
            scalars: Bytes::from(child_scalars(child.root, child.signed.message.slot)),
            head: Some(HeadChange {
                head_root: root,
                head_slot: slot,
                cause: HeadCause::Import,
            }),
        })
        .await
        .expect("commit_import");

    let after = node
        .durable_frontier(&slots)
        .expect("frontier after import");
    assert_eq!(after.canonical[0], Some(parent));
    assert_eq!(after.canonical[1], Some(root));
    assert_eq!(after.bodies[1].as_deref(), Some(ssz.as_slice()));
    assert_eq!(after.state_roots[1], Some(state_root));
    assert_eq!(after.da_status[1], Some(DurableDaStatus::Available));
    let after_cursor = after.cursor.expect("cursor after import");
    assert!(
        after_cursor.seq > before_seq,
        "cursor must advance: before={before_seq} after={}",
        after_cursor.seq
    );
    assert_eq!(after_cursor.slot, slot);
    assert_eq!(after_cursor.root, root);

    node.chain()
        .core_handle()
        .expect("core installed")
        .shutdown()
        .await;
    drop(node);

    let mut reopened = None;
    for _ in 0..50 {
        match cc_storage_core::reopen_durable_frontier(dir.path(), &slots) {
            Ok(view) => {
                reopened = Some(view);
                break;
            }
            Err(err) if err.to_string().contains("locked") => {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Err(err) => panic!("reopen: {err}"),
        }
    }
    let reopened = reopened.expect("committed rows stay readable after reopen");
    assert_eq!(reopened.canonical[0], Some(parent));
    assert_eq!(reopened.canonical[1], Some(root));
    assert_eq!(reopened.bodies[1].as_deref(), Some(ssz.as_slice()));
    assert_eq!(reopened.state_roots[1], Some(state_root));
    assert_eq!(reopened.da_status[1], Some(DurableDaStatus::Available));
    let cursor = reopened.cursor.expect("cursor survives reopen");
    assert_eq!(cursor.seq, after_cursor.seq);
    assert_eq!(cursor.slot, slot);
    assert_eq!(cursor.root, root);
}
