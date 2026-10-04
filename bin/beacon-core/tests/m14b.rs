//! M14b: one `boot` on an empty store commits the anchor, then imports its child.
//!
//! One `boot` per process (`cc_bootstrap::init` installs a process-global
//! subscriber). This test does not call `serve`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

#[path = "support/anchor_fixture.rs"]
#[allow(dead_code)]
mod anchor_fixture;

use std::time::Duration;

use anchor_fixture::{
    ElStub, anchor_fixture, beacon_config, first_child, provider_slot, spawn_el_stub,
};
use cc_beacon_core::boot::boot;
use cc_chain_core::core::CoreHandle;
use cc_chain_core::import::encode_signed_block;
use cc_proto::chain::chain_service_server::ChainService;
use cc_proto::chain::{EventKind, GetHeadRequest, ImportBlockRequest, ImportBlockVerdict};
use cc_proto::error_info_from_status;
use cc_seam::{ArchiveWrite, Bytes, DaVerdict, FailedPreconditionReason, SeamError, TrustedAnchor};
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ResidencyRead {
    resident_states: i64,
    body_ring_len: i64,
}

fn read_residency(core: &CoreHandle) -> ResidencyRead {
    ResidencyRead {
        resident_states: core.metrics().resident_states_value(),
        body_ring_len: core.metrics().body_ring_len_value(),
    }
}

fn anchor_state_event(kind: i32) -> bool {
    kind == EventKind::BlockImported as i32
        || kind == EventKind::Head as i32
        || kind == EventKind::FinalizedCheckpoint as i32
}

/// The slot ticker sends a forkchoiceUpdated floor on each boundary. Park so
/// that floor is outside the refusal window; events are still filtered to
/// block-imported, head, and finalized.
async fn park_refusal_window(el: &ElStub, seconds_per_slot: u64) {
    let wait = cc_chain_core::tick::duration_until_next_slot_boundary(
        std::time::SystemTime::now(),
        0,
        seconds_per_slot,
    );
    if wait < Duration::from_secs(2) {
        tokio::time::sleep(wait + Duration::from_millis(250)).await;
    }
    let mut last = el.forkchoice_updated_calls();
    let mut stable = 0u32;
    for _ in 0..25 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let now = el.forkchoice_updated_calls();
        if now == last {
            stable += 1;
            if stable >= 3 {
                return;
            }
        } else {
            stable = 0;
            last = now;
        }
    }
}

fn head_or_panic(
    result: Result<tonic::Response<cc_proto::chain::GetHeadResponse>, tonic::Status>,
) -> cc_proto::chain::GetHeadResponse {
    match result {
        Ok(response) => response.into_inner(),
        Err(status) => {
            let reason = error_info_from_status(&status)
                .ok()
                .flatten()
                .map(|info| info.reason);
            panic!("GetHead failed reason={reason:?} status={status}");
        }
    }
}

#[tokio::test]
async fn m14b_empty_store_commits_anchor_and_imports_child() {
    let fixture = anchor_fixture();
    let el = spawn_el_stub();
    let dir = TempDir::new("m14b-anchor");
    let mut cfg = beacon_config(dir.path(), el.endpoint().to_owned());
    // Empty operator list plus the A-07 double. No API hand-seeds the store.
    cfg.checkpoint_providers.clear();
    cfg.checkpoint_provider = Some(provider_slot(&fixture));
    cfg.genesis_anchor = None;

    let node = boot(cfg).await.expect("boot returns before serve");

    let head = head_or_panic(node.chain().get_head(Request::new(GetHeadRequest {})).await);
    assert_eq!(head.head_slot, 0, "seeded head is the anchor");
    assert_eq!(head.head_root, fixture.anchor_root.as_slice());
    assert!(
        node.archive()
            .block_is_durable(*fixture.anchor_root.as_array())
            .expect("anchor durability"),
        "block_is_durable(anchor_root)"
    );

    let child = first_child(&fixture);
    let core = node.chain().core_handle().expect("core installed");
    core.notify_data_available(child.root, child.signed.message.slot.as_u64())
        .await
        .expect("mark child data available");

    let request = ImportBlockRequest {
        ssz: encode_signed_block(&child.signed),
        fork: fixture
            .chain
            .fork_name_at_epoch(cc_types::primitives::Epoch::new(0)) as u32,
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
        if verdict == ImportBlockVerdict::Imported as i32 {
            imported = true;
            break;
        }
        if verdict == ImportBlockVerdict::Duplicate as i32 {
            let durable = node
                .archive()
                .block_is_durable(*child.root.as_array())
                .expect("child durability");
            assert!(
                durable,
                "duplicate import only counts when the child is durable"
            );
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
            .expect("child durable"),
        "imported child is durable"
    );

    park_refusal_window(&el, fixture.chain.seconds_per_slot).await;
    let mut events = node
        .chain()
        .events()
        .subscribe(None)
        .await
        .expect("event subscribe");

    let head_before = head_or_panic(node.chain().get_head(Request::new(GetHeadRequest {})).await);
    assert_eq!(
        head_before.head_root,
        child.root.as_slice(),
        "snapshot is the imported head"
    );
    let residency_before = read_residency(&core);
    assert!(
        residency_before.resident_states > 0,
        "resident state count must be read, got {residency_before:?}"
    );
    assert!(
        residency_before.body_ring_len > 0,
        "body ring must be read, got {residency_before:?}"
    );
    let fcu_before = el.forkchoice_updated_calls();
    assert!(
        fcu_before > 0,
        "EL stub must have observed forkchoiceUpdated before the refusal"
    );

    let before = node
        .durable_frontier(&[0, child.signed.message.slot.as_u64()])
        .expect("frontier before refused commit");
    assert!(before.cursor.is_some(), "import advanced the write cursor");
    assert_eq!(
        before.canonical.first().copied().flatten(),
        Some(*fixture.anchor_root.as_array())
    );
    assert_eq!(
        before.canonical.get(1).copied().flatten(),
        Some(*child.root.as_array())
    );

    let refused = node
        .archive()
        .commit_anchor(TrustedAnchor {
            block_root: [0x22; 32],
            parent_root: [0; 32],
            slot: 0,
            state_root: [0; 32],
            block_ssz: Bytes::new(),
            state_ssz: Bytes::new(),
            scalars: Bytes::new(),
            da: DaVerdict::Available,
        })
        .await;
    match refused {
        Err(SeamError::FailedPrecondition { reason }) => {
            assert_eq!(reason, FailedPreconditionReason::StoreNotUninitialized);
            assert_eq!(reason.as_str(), "STORE_NOT_UNINITIALIZED");
        }
        other => panic!("second commit_anchor must be refused, got {other:?}"),
    }

    let head_after = head_or_panic(node.chain().get_head(Request::new(GetHeadRequest {})).await);
    assert_eq!(
        head_before, head_after,
        "refused commit must not move head or finalized"
    );
    assert_eq!(
        residency_before,
        read_residency(&core),
        "refused commit must not change residency"
    );
    assert_eq!(
        fcu_before,
        el.forkchoice_updated_calls(),
        "refused commit must not send forkchoiceUpdated"
    );

    let after = node
        .durable_frontier(&[0, child.signed.message.slot.as_u64()])
        .expect("frontier after refused commit");
    assert_eq!(
        before, after,
        "refused commit must not advance head, cursor, or canonical rows"
    );

    let mut state_events = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_millis(150);
    loop {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        if left.is_zero() {
            break;
        }
        tokio::select! {
            biased;
            got = events.recv() => {
                match got {
                    Ok(Some(ev)) if anchor_state_event(ev.kind) => {
                        state_events.push(format!(
                            "kind={} slot={} root={:02x?}",
                            ev.kind,
                            ev.slot,
                            ev.root.as_slice()
                        ));
                    }
                    Ok(Some(_)) => {}
                    Ok(None) => break,
                    Err(status) => panic!("event subscription ended: {status}"),
                }
            }
            _ = tokio::time::sleep(left) => break,
        }
    }
    assert!(
        state_events.is_empty(),
        "refused commit emitted block/head/finalized events: {state_events:?}"
    );
}
