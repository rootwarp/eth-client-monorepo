//! `Incomplete` names the failing durable item and does not start subsystems.
//!
//! One successful `boot` per process (`cc_bootstrap::init`). The refusal
//! happens first, so a later empty `boot` in this process still succeeds.
//! This file does not call `serve`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

#[path = "support/anchor_fixture.rs"]
#[allow(dead_code)]
mod anchor_fixture;

use anchor_fixture::{beacon_config, spawn_el_stub};
use cc_beacon_core::boot::boot;
use cc_storage_core::{NodeIdExpectation, OpenOpts, load_or_create_node_key, open};

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

#[tokio::test]
async fn incomplete_names_the_durable_item_and_leaves_init_free() {
    let el = spawn_el_stub();
    let dir = TempDir::new("composer-incomplete");
    let mut cfg = beacon_config(dir.path(), el.endpoint().to_owned());
    cfg.check_invariants = false;
    cfg.genesis_validators_root = Some(format!("0x{}", "ab".repeat(32)));
    cfg.checkpoint_providers.clear();
    cfg.checkpoint_provider = None;
    cfg.genesis_anchor = None;

    let loaded = load_or_create_node_key(&cfg.node_key_path).expect("node key");
    let node_id = NodeIdExpectation::Present(loaded.legacy());
    let mut pending = open(
        &cfg.data_dir,
        OpenOpts {
            durability: cfg.durability.clone(),
            check_invariants: cfg.check_invariants,
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
    let engine = opened.into_engine();
    let mut batch = engine.batch();
    batch.put("blocks_hot", b"body-only", b"not-an-anchor");
    engine.commit(batch).expect("body row");
    drop(engine);

    let err = match boot(cfg).await {
        Ok(_) => panic!("body without an anchor is Incomplete"),
        Err(err) => err,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("durable item") && msg.contains("anchor"),
        "error must name the failing durable item, got: {msg}"
    );

    let empty = TempDir::new("composer-after-incomplete");
    let el_empty = spawn_el_stub();
    let mut empty_cfg = beacon_config(empty.path(), el_empty.endpoint().to_owned());
    empty_cfg.checkpoint_providers.clear();
    empty_cfg.checkpoint_provider = None;
    let node = boot(empty_cfg)
        .await
        .expect("Incomplete must return before init, so a later boot still runs");
    assert!(
        cc_beacon_core::boot::composer_steps().is_empty(),
        "an uninitialized store does not record the Complete arm"
    );
    drop(node);
}
