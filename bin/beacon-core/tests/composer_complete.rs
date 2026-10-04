//! A Complete store seeds, then `set_head` writes the recomputed head.
//!
//! One `boot` per process. The assertion is call order. Restart does not
//! preserve the head, so this test does not compare head roots. It also
//! does not claim that a Complete store skips checkpoint sync.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

#[path = "support/anchor_fixture.rs"]
#[allow(dead_code)]
mod anchor_fixture;

use anchor_fixture::{anchor_fixture, beacon_config, spawn_el_stub};
use cc_beacon_core::boot::{ComposerStep, boot, composer_steps};
use cc_chain_core::import::ForkChoiceScalarsPayload;
use cc_seam::{ArchiveWrite, Bytes, DaVerdict, TrustedAnchor};
use cc_storage_core::{
    NodeIdExpectation, OpenOpts, StorageMetrics, load_or_create_node_key, open, start_writer,
};
use cc_types::containers::Checkpoint;
use cc_types::preset::Minimal;
use cc_types::primitives::Epoch;
use cc_types::{ForkName, Root, SignedBeaconBlock};
use prometheus_client::registry::Registry;
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

fn root_hex(root: &Root) -> String {
    let mut hex = String::from("0x");
    for byte in root.as_slice() {
        hex.push_str(&format!("{byte:02x}"));
    }
    hex
}

fn scalars_ssz(head: Root, slot: u64) -> Vec<u8> {
    let checkpoint = Checkpoint {
        epoch: Epoch::new(slot / 32),
        root: head,
    };
    ForkChoiceScalarsPayload {
        time: slot,
        proposer_boost_root: Root::ZERO,
        justified: checkpoint,
        finalized: checkpoint,
        unrealized_justified: checkpoint,
        unrealized_finalized: checkpoint,
        head_root: head,
        head_slot: cc_types::primitives::Slot::new(slot),
    }
    .as_ssz_bytes()
}

#[tokio::test]
async fn complete_store_sets_head_after_seed() {
    let fixture = anchor_fixture();
    let el = spawn_el_stub();
    let dir = TempDir::new("composer-complete");
    let mut cfg = beacon_config(dir.path(), el.endpoint().to_owned());
    cfg.genesis_validators_root = Some(root_hex(&fixture.genesis.genesis_validators_root));
    cfg.checkpoint_providers.clear();
    cfg.checkpoint_provider = None;
    cfg.genesis_anchor = None;
    cfg.check_invariants = false;

    let loaded = load_or_create_node_key(&cfg.node_key_path).expect("node key");
    let node_id = NodeIdExpectation::Present(loaded.legacy());
    let mut pending = open(
        &cfg.data_dir,
        OpenOpts {
            durability: cfg.durability.clone(),
            check_invariants: false,
            snapshot_ring: cfg.snapshot_ring,
            genesis_validators_root: cfg.genesis_validators_root.clone(),
            node_id,
            chain: cfg.chain_config.clone(),
            ..OpenOpts::default()
        },
    )
    .expect("stamp side keys");
    let _peek = pending.peek_node_id().expect("peek");
    let opened = pending.pair(node_id).expect("pair");
    let mut registry = Registry::default();
    let metrics = StorageMetrics::register(&mut registry);
    let runtime = start_writer(opened, metrics, false);

    let signed =
        SignedBeaconBlock::<Minimal>::from_ssz_bytes_with(ForkName::Fulu, &fixture.block_ssz)
            .expect("anchor block");
    let anchor = TrustedAnchor {
        block_root: *fixture.anchor_root.as_array(),
        parent_root: *signed.message.parent_root.as_array(),
        slot: signed.message.slot.as_u64(),
        state_root: *signed.message.state_root.as_array(),
        block_ssz: Bytes::from(fixture.block_ssz.clone()),
        state_ssz: Bytes::from(fixture.state_ssz.clone()),
        scalars: Bytes::from(scalars_ssz(
            fixture.anchor_root,
            signed.message.slot.as_u64(),
        )),
        da: DaVerdict::Available,
    };
    runtime
        .archive()
        .commit_anchor(anchor)
        .await
        .expect("test anchor commit");
    runtime.drain_and_shutdown().await.expect("drain pre-seed");
    drop(runtime);

    let node = boot(cfg).await.expect("complete boot");
    assert_eq!(
        composer_steps(),
        vec![ComposerStep::SeedFromDurable, ComposerStep::SetHead],
        "Complete calls set_head after seed_from_durable"
    );
    drop(node);
    drop(el);
}
