//! M14c: clean restart and real-binary SIGKILL, plus the stamp-only gate.
//!
//! Clean: `boot` → import → `drain_and_shutdown` → `boot` again. Not `serve`.
//! Abrupt: the J-03 rig SIGKILLs `cc-beacon-core` mid-import. Dropping this
//! process's runtime is not that case. Both restarts configure zero
//! checkpoint providers. Resume is `SeedFromDurable` then `SetHead` with a
//! live core; a re-sync leaves the core absent and fails the test. Head
//! roots are not compared across either restart.
//!
//! Bodies without scalars stay in `composer_incomplete` (the error names the
//! durable item). This file does not remove that test, M14a, or M14b.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

#[path = "support/anchor_fixture.rs"]
#[allow(dead_code)]
mod anchor_fixture;

#[path = "support/m14c.rs"]
mod harness;

#[path = "support/sigkill_rig.rs"]
mod sigkill_rig;

use std::os::unix::process::ExitStatusExt;
use std::time::Duration;

use anchor_fixture::{anchor_fixture, beacon_config, provider_slot, spawn_el_stub};
use cc_beacon_core::boot::{BeaconCoreConfig, ComposerStep, boot, composer_steps};
use cc_proto::chain::GetHeadRequest;
use cc_proto::chain::chain_service_server::ChainService;
use cc_storage_core::{
    NodeIdExpectation, OpenOpts, StorageMetrics, load_or_create_node_key, open, start_writer,
};
use cc_types::config::ChainConfig;
use harness::{TempDir, gvr_hex, import_child, minimal_yaml, wait_store_unlocked};
use prometheus_client::registry::Registry;
use sigkill_rig::{RigConfig, SigkillRig};
use tonic::Request;

async fn boot_lock() -> tokio::sync::MutexGuard<'static, ()> {
    static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    LOCK.lock().await
}

fn with_gvr(dir: &std::path::Path, endpoint: String, gvr: &str) -> BeaconCoreConfig {
    let mut cfg = beacon_config(dir, endpoint);
    cfg.checkpoint_providers.clear();
    cfg.checkpoint_provider = None;
    cfg.genesis_anchor = None;
    cfg.genesis_validators_root = Some(gvr.to_owned());
    cfg
}

fn assert_resumed_without_checkpoint_sync(node: &cc_beacon_core::boot::BootedNode) {
    assert_eq!(
        composer_steps(),
        vec![ComposerStep::SeedFromDurable, ComposerStep::SetHead],
        "Complete resume calls set_head after seeding; zero providers must not re-sync"
    );
    assert!(
        node.chain().core_handle().is_some(),
        "a checkpoint re-sync with zero providers leaves the core absent"
    );
}

async fn assert_head_served(node: &cc_beacon_core::boot::BootedNode) {
    node.chain()
        .get_head(Request::new(GetHeadRequest {}))
        .await
        .expect("resumed core serves a head; roots are not compared across the restart");
}

/// Stamp rows only: schema, side keys, node id, and the write cursor.
#[tokio::test]
async fn open_and_stamp_only_store_is_uninitialized() {
    let _guard = boot_lock().await;
    let el = spawn_el_stub();
    let dir = TempDir::new("m14c-stamp");
    let gvr = format!("0x{}", "ab".repeat(32));
    let mut cfg = with_gvr(dir.path(), el.endpoint().to_owned(), &gvr);

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
    .expect("open_and_stamp");
    let _peek = pending.peek_node_id().expect("peek");
    let opened = pending.pair(node_id).expect("pair");
    let mut registry = Registry::default();
    let metrics = StorageMetrics::register(&mut registry);
    let runtime = start_writer(opened, metrics, false);
    runtime
        .drain_and_shutdown()
        .await
        .expect("stamp writer drains");
    drop(runtime);

    cfg.checkpoint_providers.clear();
    cfg.checkpoint_provider = None;
    let node = boot(cfg)
        .await
        .expect("stamp-only store is Uninitialized, not an incomplete durable item");
    assert!(
        composer_steps().is_empty(),
        "Uninitialized does not seed or set_head, steps={:?}",
        composer_steps()
    );
    assert!(
        node.chain().core_handle().is_none(),
        "zero providers on an uninitialized store leave the core absent"
    );
    drop(node);
    drop(el);
}

/// `boot` → import → `drain_and_shutdown` → `boot` with zero providers.
#[tokio::test]
async fn clean_restart_resumes_without_checkpoint_sync() {
    let _guard = boot_lock().await;
    let fixture = anchor_fixture();
    let el = spawn_el_stub();
    let dir = TempDir::new("m14c-clean");
    let gvr = gvr_hex(&fixture.genesis.genesis_validators_root);
    let mut cfg = with_gvr(dir.path(), el.endpoint().to_owned(), &gvr);
    cfg.checkpoint_provider = Some(provider_slot(&fixture));

    let node = boot(cfg).await.expect("first boot");
    import_child(&node, &fixture).await;
    node.drain_and_shutdown()
        .await
        .expect("clean drain_and_shutdown");

    let chain = fixture.chain.clone();
    wait_store_unlocked(dir.path(), &chain, &dir.path().join("node_key"), &gvr).await;

    let again = with_gvr(dir.path(), el.endpoint().to_owned(), &gvr);
    assert!(
        again.checkpoint_providers.is_empty() && again.checkpoint_provider.is_none(),
        "clean restart configures zero checkpoint providers"
    );
    let restarted = boot(again).await.unwrap_or_else(|err| {
        panic!("complete restart with zero checkpoint_providers failed: {err:#}")
    });
    assert_resumed_without_checkpoint_sync(&restarted);
    assert_head_served(&restarted).await;
    drop(restarted);
    drop(el);
}

/// Seed in-process, SIGKILL the real binary mid-replay, then resume.
#[tokio::test]
async fn abrupt_sigkill_restart_resumes_without_checkpoint_sync() {
    let _guard = boot_lock().await;
    let fixture = anchor_fixture();
    let yaml = minimal_yaml(&fixture.chain);
    let parsed = ChainConfig::from_yaml_str(&yaml).expect("minimal yaml");
    assert_eq!(
        parsed, fixture.chain,
        "child network yaml must match the seeder"
    );

    let el = spawn_el_stub();
    let dir = TempDir::new("m14c-abrupt");
    let gvr = gvr_hex(&fixture.genesis.genesis_validators_root);
    let mut cfg = with_gvr(dir.path(), el.endpoint().to_owned(), &gvr);
    cfg.checkpoint_provider = Some(provider_slot(&fixture));

    let node = boot(cfg).await.expect("in-process seed boot");
    import_child(&node, &fixture).await;
    if let Some(core) = node.chain().core_handle() {
        core.shutdown().await;
    }
    drop(node);

    let network_config = dir.path().join("minimal.yaml");
    std::fs::write(&network_config, yaml).expect("write network yaml");
    wait_store_unlocked(
        dir.path(),
        &fixture.chain,
        &dir.path().join("node_key"),
        &gvr,
    )
    .await;

    let mut rig = SigkillRig::spawn(RigConfig {
        data_dir: dir.path().to_path_buf(),
        node_key_path: dir.path().join("node_key"),
        jwt_secret_path: dir.path().join("jwt.hex"),
        network_config,
        genesis_validators_root: gvr.clone(),
        new_payload_status: "VALID".to_owned(),
        forkchoice_status: "VALID".to_owned(),
        hold_new_payload: true,
    })
    .unwrap_or_else(|err| panic!("spawn rig: {err}"));

    let toml = rig.config_toml();
    assert!(
        toml.lines()
            .any(|line| line.trim() == "checkpoint_providers = []"),
        "abrupt restart spawns with zero checkpoint_providers, toml:\n{toml}"
    );
    rig.wait_for_held_new_payload(Duration::from_secs(45))
        .unwrap_or_else(|err| panic!("newPayload was not held mid-import: {err}"));
    let status = rig.sigkill().unwrap_or_else(|err| panic!("sigkill: {err}"));
    assert_eq!(
        status.signal(),
        Some(9),
        "abrupt case is SIGKILL of the child, not a runtime drop; stderr:\n{}",
        rig.node_stderr()
    );
    assert!(
        !status.success(),
        "SIGKILL must not report a successful exit"
    );
    drop(rig);

    wait_store_unlocked(
        dir.path(),
        &fixture.chain,
        &dir.path().join("node_key"),
        &gvr,
    )
    .await;
    let again = with_gvr(dir.path(), el.endpoint().to_owned(), &gvr);
    assert!(
        again.checkpoint_providers.is_empty() && again.checkpoint_provider.is_none(),
        "post-kill resume configures zero checkpoint providers"
    );
    let restarted = boot(again).await.unwrap_or_else(|err| {
        panic!("resume after SIGKILL with zero checkpoint_providers failed: {err:#}")
    });
    assert_resumed_without_checkpoint_sync(&restarted);
    assert_head_served(&restarted).await;
    drop(restarted);
    drop(el);
}
