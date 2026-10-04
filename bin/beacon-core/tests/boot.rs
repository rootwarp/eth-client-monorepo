//! S2-J-01: one process opens redb before any subsystem; I-node-id on a
//! second opener; one writer.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use cc_beacon_core::boot::boot;
use cc_beacon_core::{BeaconCoreConfig, BootConfig, BootPhase, boot_in_process};
use cc_storage_core::StorageMetrics;
use cc_types::primitives::Root;
use prometheus_client::registry::Registry;

fn unique_temp_dir(prefix: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let seq = N.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!("{prefix}-{}-{seq}-{nanos}", std::process::id()))
}

fn metrics() -> StorageMetrics {
    let mut registry = Registry::default();
    StorageMetrics::register(&mut registry)
}

fn boot_cfg(dir: PathBuf, node_key: Option<PathBuf>) -> BootConfig {
    BootConfig {
        data_dir: dir,
        durability: "immediate".to_owned(),
        check_invariants: true,
        snapshot_ring: 4,
        genesis_validators_root: None,
        node_key_path: node_key,
        writer_process_fatal: false,
    }
}

#[tokio::test]
async fn open_completes_before_any_subsystem_starts() {
    let dir = unique_temp_dir("beacon-core-order");
    std::fs::create_dir_all(&dir).unwrap();
    let booted = boot_in_process(&boot_cfg(dir.clone(), None), metrics(), true).unwrap();
    assert_eq!(booted.phases.first().copied(), Some(BootPhase::Open));
    assert!(
        booted.phases.iter().position(|&p| p == BootPhase::Open)
            < booted.phases.iter().position(|&p| p == BootPhase::Writer),
        "writer must start after open: {:?}",
        booted.phases
    );
    assert_eq!(
        booted.storage.as_ref().map(|s| s.writer_count()),
        Some(1),
        "one writer"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn second_opener_of_same_data_dir_fails_inode_id() {
    let dir = unique_temp_dir("beacon-core-inode");
    std::fs::create_dir_all(&dir).unwrap();
    let key_a = dir.join("node_key");
    let id_a = Root::from_array([0x11u8; 32]);
    std::fs::write(&key_a, id_a.as_slice()).unwrap();

    // Production stamp: open_and_stamp persists meta.node_id from the key.
    let first = boot_in_process(&boot_cfg(dir.clone(), Some(key_a)), metrics(), false).unwrap();
    drop(first);

    let key_b = dir.join("node_key_b");
    std::fs::write(&key_b, [0x22u8; 32]).unwrap();
    let err = boot_in_process(&boot_cfg(dir.clone(), Some(key_b)), metrics(), false)
        .expect_err("I-node-id must refuse a second identity");
    let msg = err.to_string();
    assert!(
        msg.contains("node_id") || msg.contains("I-node-id"),
        "second opener must fail I-node-id, got: {msg}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// `boot` is the production sequence and returns. `serve` is what does not
/// return; this test must not call it.
#[tokio::test]
async fn boot_against_temp_dir_returns() {
    let dir = TempDir::new("beacon-core-boot");
    let node = boot(config_for_dir(dir.path()))
        .await
        .expect("boot returns before serve");
    assert!(
        dir.path().join("store.redb").is_file(),
        "boot opened redb under the temp dir"
    );
    drop(node);
}

struct TempDir(PathBuf);

impl TempDir {
    fn new(prefix: &str) -> Self {
        let path = unique_temp_dir(prefix);
        std::fs::create_dir_all(&path).unwrap();
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

fn config_for_dir(root: &std::path::Path) -> BeaconCoreConfig {
    let jwt = root.join("jwt.hex");
    // prepare refuses a missing [el_forks] and a non-0600 JWT (abort before bind).
    std::fs::write(
        &jwt,
        "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff",
    )
    .unwrap();
    let mut perms = std::fs::metadata(&jwt).unwrap().permissions();
    perms.set_mode(0o600);
    std::fs::set_permissions(&jwt, perms).unwrap();

    BeaconCoreConfig {
        service: cc_config::ServiceConfig {
            grpc_addr: "127.0.0.1:0".parse().unwrap(),
            metrics_addr: "127.0.0.1:0".parse().unwrap(),
            peers: std::collections::BTreeMap::new(),
            log_format: "json".to_owned(),
            log_filter: "info".to_owned(),
        },
        data_dir: root.to_path_buf(),
        durability: "immediate".to_owned(),
        check_invariants: true,
        snapshot_ring: 4,
        genesis_validators_root: None,
        node_key_path: root.join("node_key"),
        max_resident_states: 4,
        body_ring_capacity: 64,
        event_ring_events: 4096,
        event_ring_bytes: 67_108_864,
        subscriber_queue_capacity: 256,
        checkpoint_providers: Vec::new(),
        checkpoint_provider: None,
        genesis_anchor: None,
        chain_config: None,
        checkpoint_root: None,
        network_config: None,
        engine: cc_engine_api::config::EngineTransportConfig {
            jwt_secret_path: jwt,
            // Same Hoodi times as config/beacon-core.toml. Missing table is fatal.
            el_forks: Some(cc_engine_api::config::ElForksConfig {
                osaka_time: 1_761_677_592,
                bpo1_time: Some(1_762_365_720),
                bpo2_time: Some(1_762_955_544),
                amsterdam_time: None,
            }),
            ..cc_engine_api::config::EngineTransportConfig::default()
        },
        maximum_gossip_clock_disparity_ms: 500,
    }
}
