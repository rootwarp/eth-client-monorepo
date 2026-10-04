//! Real-binary SIGKILL rig: sibling `fake-el` plus a `beacon-core` child.
//!
//! One `boot` in this process seeds a durable child. The OS child replays it.
//! `cc_bootstrap::init` allows one boot here. This test must not treat dropping
//! that in-process runtime as the abrupt case — see the helper docs.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

#[path = "support/anchor_fixture.rs"]
#[allow(dead_code)]
mod anchor_fixture;

#[path = "support/sigkill_rig.rs"]
mod sigkill_rig;

use std::os::unix::process::ExitStatusExt;
use std::time::Duration;

use anchor_fixture::{anchor_fixture, beacon_config, first_child, provider_slot, spawn_el_stub};
use cc_beacon_core::boot::boot;
use cc_chain_core::import::encode_signed_block;
use cc_proto::chain::chain_service_server::ChainService;
use cc_proto::chain::{ImportBlockRequest, ImportBlockVerdict};
use cc_proto::error_info_from_status;
use cc_seam::ArchiveWrite;
use cc_storage_core::{NodeIdExpectation, OpenOpts, open};
use cc_types::config::ChainConfig;
use cc_types::primitives::Epoch;
use sigkill_rig::{RigConfig, SigkillRig, sibling_fake_el};
use tonic::Request;

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

fn minimal_yaml(chain: &ChainConfig) -> String {
    let mut blobs = String::new();
    for entry in chain.blob_schedule.entries() {
        blobs.push_str(&format!(
            "  - EPOCH: {}\n    MAX_BLOBS_PER_BLOCK: {}\n",
            entry.epoch, entry.max_blobs_per_block
        ));
    }
    format!(
        "\
PRESET_BASE: minimal
CONFIG_NAME: {name}
GENESIS_FORK_VERSION: {genesis_fork}
ALTAIR_FORK_VERSION: {altair_fork}
ALTAIR_FORK_EPOCH: {altair_epoch}
BELLATRIX_FORK_VERSION: {bellatrix_fork}
BELLATRIX_FORK_EPOCH: {bellatrix_epoch}
CAPELLA_FORK_VERSION: {capella_fork}
CAPELLA_FORK_EPOCH: {capella_epoch}
DENEB_FORK_VERSION: {deneb_fork}
DENEB_FORK_EPOCH: {deneb_epoch}
ELECTRA_FORK_VERSION: {electra_fork}
ELECTRA_FORK_EPOCH: {electra_epoch}
FULU_FORK_VERSION: {fulu_fork}
FULU_FORK_EPOCH: {fulu_epoch}
SECONDS_PER_SLOT: {seconds_per_slot}
DEPOSIT_CHAIN_ID: {deposit_chain_id}
DEPOSIT_CONTRACT_ADDRESS: {deposit_address}
CHURN_LIMIT_QUOTIENT: {churn}
MIN_PER_EPOCH_CHURN_LIMIT_ELECTRA: {min_churn}
MAX_PER_EPOCH_ACTIVATION_EXIT_CHURN_LIMIT: {max_churn}
SHARD_COMMITTEE_PERIOD: {shard_period}
MAX_BLOBS_PER_BLOCK_ELECTRA: {max_blobs}
BLOB_SCHEDULE:
{blobs}",
        name = chain.config_name,
        genesis_fork = chain.genesis_fork_version,
        altair_fork = chain.altair_fork_version,
        altair_epoch = chain.altair_fork_epoch,
        bellatrix_fork = chain.bellatrix_fork_version,
        bellatrix_epoch = chain.bellatrix_fork_epoch,
        capella_fork = chain.capella_fork_version,
        capella_epoch = chain.capella_fork_epoch,
        deneb_fork = chain.deneb_fork_version,
        deneb_epoch = chain.deneb_fork_epoch,
        electra_fork = chain.electra_fork_version,
        electra_epoch = chain.electra_fork_epoch,
        fulu_fork = chain.fulu_fork_version,
        fulu_epoch = chain.fulu_fork_epoch,
        seconds_per_slot = chain.seconds_per_slot,
        deposit_chain_id = chain.deposit_chain_id,
        deposit_address = chain.deposit_contract_address,
        churn = chain.churn_limit_quotient,
        min_churn = chain.min_per_epoch_churn_limit_electra,
        max_churn = chain.max_per_epoch_activation_exit_churn_limit,
        shard_period = chain.shard_committee_period,
        max_blobs = chain.max_blobs_per_block_electra,
        blobs = blobs,
    )
}

/// Parent missing, or the sibling binary missing, is an error string.
/// `Path::parent()` returning `None` must not panic.
#[test]
fn sibling_path_failure_is_a_message_not_a_panic() {
    let missing_parent =
        sibling_fake_el(std::path::Path::new("/")).expect_err("root has no parent");
    assert!(
        missing_parent.contains("parent") && missing_parent.contains("shared workspace"),
        "{missing_parent}"
    );
    let empty = sibling_fake_el(std::path::Path::new("")).expect_err("empty path");
    assert!(
        empty.contains("parent"),
        "empty path must not panic, got: {empty}"
    );

    let missing_bin = sibling_fake_el(std::path::Path::new(
        "/tmp/no-such-beacon-core-dir/cc-beacon-core",
    ))
    .expect_err("sibling binary absent");
    assert!(
        missing_bin.contains("fake-el") && missing_bin.contains("shared workspace"),
        "{missing_bin}"
    );
}

/// The helper text is what stops M14c from substituting an in-process drop.
#[test]
fn helper_rejects_in_process_runtime_drop_as_the_abrupt_case() {
    let src = include_str!("support/sigkill_rig.rs");
    assert!(
        src.contains("drop the runtime"),
        "helper must document that an in-process runtime drop is not the abrupt case"
    );
    assert!(
        src.contains("SIGKILL"),
        "helper must name SIGKILL as the abrupt stop"
    );
}

fn gvr_hex() -> String {
    format!("0x{}", "11".repeat(32))
}

async fn wait_store_unlocked(
    dir: &std::path::Path,
    chain: &ChainConfig,
    node_key: &std::path::Path,
) {
    let node_id = NodeIdExpectation::from_configured_path(Some(node_key)).expect("node key");
    let opts = OpenOpts {
        durability: "immediate".to_owned(),
        check_invariants: true,
        snapshot_ring: 4,
        genesis_validators_root: Some(gvr_hex()),
        node_id,
        chain: Some(chain.clone()),
        ..OpenOpts::default()
    };
    // `std::thread::sleep` would stall this runtime, so the writer task could
    // never observe shutdown and drop the redb lock.
    for _ in 0..80 {
        match open(dir, opts.clone()) {
            Ok(pending) => {
                // Dropping an unpaired pending store asserts in debug builds.
                let opened = pending.pair(node_id).expect("probe pair");
                drop(opened);
                return;
            }
            Err(err) => {
                let msg = err.to_string();
                if msg.contains("lock") || msg.contains("Database") {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
                panic!("store did not unlock cleanly: {msg}");
            }
        }
    }
    panic!("store stayed locked");
}

/// Seed one durable child in-process, then SIGKILL the real binary while
/// `fake-el` is holding that child's replayed `newPayload`.
#[tokio::test]
async fn sigkill_during_held_new_payload_is_signal_nine() {
    let fixture = anchor_fixture();
    let yaml = minimal_yaml(&fixture.chain);
    let parsed = ChainConfig::from_yaml_str(&yaml).expect("minimal yaml");
    assert_eq!(
        parsed, fixture.chain,
        "child network yaml must match the seeder"
    );

    let el = spawn_el_stub();
    let dir = TempDir::new("sigkill-rig");
    let mut cfg = beacon_config(dir.path(), el.endpoint().to_owned());
    cfg.checkpoint_providers.clear();
    cfg.checkpoint_provider = Some(provider_slot(&fixture));
    cfg.genesis_anchor = None;
    // Side keys stamp on the empty open only when a GVR is present. The child
    // reopens the populated store and must present the same value.
    cfg.genesis_validators_root = Some(gvr_hex());

    let node = boot(cfg).await.expect("in-process seed boot");
    let child = first_child(&fixture);
    let core = node.chain().core_handle().expect("core installed");
    core.notify_data_available(child.root, child.signed.message.slot.as_u64())
        .await
        .expect("mark child data available");

    let request = ImportBlockRequest {
        ssz: encode_signed_block(&child.signed),
        fork: fixture.chain.fork_name_at_epoch(Epoch::new(0)) as u32,
        root: child.root.as_slice().to_vec(),
        source: 0,
    };
    let mut imported = false;
    let mut last_reason = String::new();
    for _ in 0..40 {
        let response = node
            .chain()
            .import_block(Request::new(request.clone()))
            .await
            .unwrap_or_else(|status| {
                let reason = error_info_from_status(&status)
                    .ok()
                    .flatten()
                    .map(|info| info.reason);
                panic!("ImportBlock failed reason={reason:?} status={status}");
            })
            .into_inner();
        last_reason = response.reason.clone();
        let verdict = response.verdict;
        if verdict == ImportBlockVerdict::Imported as i32
            || verdict == ImportBlockVerdict::Duplicate as i32
        {
            imported = true;
            break;
        }
        if last_reason == "execution_engine_unavailable" || last_reason == "data_unavailable" {
            if last_reason == "data_unavailable" {
                core.notify_data_available(child.root, child.signed.message.slot.as_u64())
                    .await
                    .expect("re-mark data available");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        }
        panic!("child import verdict={verdict} reason={last_reason}");
    }
    assert!(
        imported,
        "child import did not succeed, last reason={last_reason}"
    );
    assert!(
        node.archive()
            .block_is_durable(*child.root.as_array())
            .expect("child durability"),
        "imported child must be durable before the child process replays it"
    );

    if let Some(core) = node.chain().core_handle() {
        core.shutdown().await;
    }
    drop(node);
    let network_config = dir.path().join("minimal.yaml");
    std::fs::write(&network_config, yaml).expect("write network yaml");
    wait_store_unlocked(dir.path(), &fixture.chain, &dir.path().join("node_key")).await;

    let mut rig = SigkillRig::spawn(RigConfig {
        data_dir: dir.path().to_path_buf(),
        node_key_path: dir.path().join("node_key"),
        jwt_secret_path: dir.path().join("jwt.hex"),
        network_config,
        genesis_validators_root: gvr_hex(),
        new_payload_status: "VALID".to_owned(),
        forkchoice_status: "VALID".to_owned(),
        hold_new_payload: true,
    })
    .unwrap_or_else(|err| panic!("spawn rig: {err}"));

    let toml = rig.config_toml();
    assert!(
        toml.lines()
            .any(|line| line.trim() == "checkpoint_providers = []"),
        "rig must spawn with zero checkpoint_providers, toml:\n{toml}"
    );

    rig.wait_for_held_new_payload(Duration::from_secs(45))
        .unwrap_or_else(|err| panic!("newPayload was not held mid-import: {err}"));
    let status = rig.sigkill().unwrap_or_else(|err| panic!("sigkill: {err}"));
    assert_eq!(
        status.signal(),
        Some(9),
        "abrupt case is SIGKILL of the child process, not a runtime drop; stderr:\n{}",
        rig.node_stderr()
    );
    assert!(
        !status.success(),
        "SIGKILL must not report a successful exit"
    );
}
