//! Genesis `AnchorSource` reaches the same composer `commit_anchor` as checkpoint.
//!
//! Separate process from M14b: `cc_bootstrap::init` allows one `boot` here.
//! Does not call `serve`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

#[path = "support/anchor_fixture.rs"]
#[allow(dead_code)]
mod anchor_fixture;

use anchor_fixture::{anchor_fixture, beacon_config, genesis_bytes, spawn_el_stub};
use cc_beacon_core::boot::boot;
use cc_proto::chain::GetHeadRequest;
use cc_proto::chain::chain_service_server::ChainService;
use cc_proto::error_info_from_status;
use cc_seam::ArchiveWrite;
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

#[tokio::test]
async fn genesis_source_commits_through_the_same_boot() {
    let fixture = anchor_fixture();
    let el = spawn_el_stub();
    let dir = TempDir::new("m14b-genesis");
    let mut cfg = beacon_config(dir.path(), el.endpoint().to_owned());
    cfg.checkpoint_providers.clear();
    cfg.checkpoint_provider = None;
    cfg.genesis_anchor = Some(genesis_bytes(&fixture));

    let node = boot(cfg).await.expect("boot returns before serve");
    let head = match node.chain().get_head(Request::new(GetHeadRequest {})).await {
        Ok(response) => response.into_inner(),
        Err(status) => {
            let reason = error_info_from_status(&status)
                .ok()
                .flatten()
                .map(|info| info.reason);
            panic!("GetHead failed reason={reason:?} status={status}");
        }
    };
    assert_eq!(head.head_slot, 0);
    assert_eq!(head.head_root, fixture.anchor_root.as_slice());
    assert!(
        node.archive()
            .block_is_durable(*fixture.anchor_root.as_array())
            .expect("anchor durability"),
        "genesis anchor is durable"
    );
}
