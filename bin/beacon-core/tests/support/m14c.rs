//! Shared M14c helpers. Not a harness by themselves.
//!
//! The clean half calls [`BootedNode::drain_and_shutdown`]. The abrupt half
//! uses the J-03 rig. Neither calls `serve`.

use std::path::{Path, PathBuf};
use std::time::Duration;

use cc_beacon_core::boot::BootedNode;
use cc_chain_core::import::encode_signed_block;
use cc_proto::chain::chain_service_server::ChainService;
use cc_proto::chain::{ImportBlockRequest, ImportBlockVerdict};
use cc_proto::error_info_from_status;
use cc_seam::ArchiveWrite;
use cc_storage_core::{NodeIdExpectation, OpenOpts, open};
use cc_types::config::ChainConfig;
use cc_types::primitives::{Epoch, Root};
use tonic::Request;

use super::anchor_fixture::AnchorFixture;

pub(crate) struct TempDir(PathBuf);

impl TempDir {
    pub(crate) fn new(prefix: &str) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let path = std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()));
        std::fs::create_dir_all(&path).expect("temp dir");
        Self(path)
    }

    pub(crate) fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub(crate) fn gvr_hex(root: &Root) -> String {
    let mut hex = String::from("0x");
    for byte in root.as_slice() {
        hex.push_str(&format!("{byte:02x}"));
    }
    hex
}

pub(crate) fn minimal_yaml(chain: &ChainConfig) -> String {
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

/// Import the anchor's first child. Panics with the verdict reason on failure.
pub(crate) async fn import_child(node: &BootedNode, fixture: &AnchorFixture) {
    let child = super::anchor_fixture::first_child(fixture);
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
        "imported child must be durable before restart"
    );
}

pub(crate) async fn wait_store_unlocked(
    dir: &Path,
    chain: &ChainConfig,
    node_key: &Path,
    genesis_validators_root: &str,
) {
    let node_id = NodeIdExpectation::from_configured_path(Some(node_key)).expect("node key");
    let opts = OpenOpts {
        durability: "immediate".to_owned(),
        check_invariants: true,
        snapshot_ring: 4,
        genesis_validators_root: Some(genesis_validators_root.to_owned()),
        node_id,
        chain: Some(chain.clone()),
        ..OpenOpts::default()
    };
    for _ in 0..80 {
        match open(dir, opts.clone()) {
            Ok(pending) => {
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
