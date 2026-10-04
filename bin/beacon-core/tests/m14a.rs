//! M14a: blocks are served from the canonical view at the selected head.
//!
//! One `boot` on one `TempDir`. The four cases are separate calls. Reads go
//! through by-range serve (canonical lookup, then the body), not a table scan.
//! A parent `boot`'s `commit_anchor` did not make durable is `PARENT_NOT_DURABLE`.
//!
//! Separate process from M14b: `cc_bootstrap::init` allows one `boot` here.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

#[path = "support/anchor_fixture.rs"]
#[allow(dead_code)]
mod anchor_fixture;

use std::sync::Arc;

use anchor_fixture::{
    ChainLink, anchor_fixture, anchor_state, beacon_config, extend_block, spawn_el_stub,
};
use cc_beacon_core::boot::boot;
use cc_chain_core::ArchiveWriteHandle;
use cc_chain_core::head::HeadSnapshotStore;
use cc_chain_core::import::ForkChoiceScalarsPayload;
use cc_chain_core::invalidation::commit_engine_invalidation_head;
use cc_fork_choice::{
    BlockImport, HarnessAvailability, get_forkchoice_store, get_head, on_block, on_tick,
};
use cc_seam::{
    ArchiveWrite, Bytes, DaVerdict, DurableImport, FailedPreconditionReason, HeadCause, HeadChange,
    SeamError,
};
use cc_state_transition::BlockSignatureStrategy;
use cc_storage_core::{ArchiveWriter, ServedCanonicalBlock};
use cc_types::containers::Checkpoint;
use cc_types::preset::Minimal;
use cc_types::primitives::{Epoch, Slot};
use cc_types::{BeaconState, ForkName, Root, SignedBeaconBlock};
use ssz::Encode;

fn scalars_ssz(head: Root, slot: Slot) -> Vec<u8> {
    let checkpoint = Checkpoint {
        epoch: Epoch::new(slot.as_u64() / 32),
        root: head,
    };
    ForkChoiceScalarsPayload {
        time: slot.as_u64(),
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

#[derive(Debug, Default, Clone, Copy)]
struct AcceptEngine;

impl<P: cc_types::preset::Preset> cc_state_transition::ExecutionEngine<P> for AcceptEngine {
    fn verify_and_notify_new_payload(
        &self,
        _request: cc_state_transition::NewPayloadRequest<'_, P>,
    ) -> Result<cc_state_transition::PayloadStatus, cc_state_transition::EngineError> {
        Ok(cc_state_transition::PayloadStatus::Valid)
    }
}

fn bytes_of(root: &Root) -> [u8; 32] {
    *root.as_array()
}

fn durable_import(link: &ChainLink, head: bool) -> DurableImport {
    let root = bytes_of(&link.root);
    DurableImport {
        block_root: root,
        parent_root: bytes_of(&link.signed.message.parent_root),
        slot: link.signed.message.slot.as_u64(),
        state_root: bytes_of(&link.signed.message.state_root),
        ssz: Bytes::from(link.signed.as_ssz_bytes()),
        da: DaVerdict::Available,
        scalars: Bytes::from(scalars_ssz(link.root, link.signed.message.slot)),
        head: head.then(|| HeadChange {
            head_root: root,
            head_slot: link.signed.message.slot.as_u64(),
            cause: HeadCause::Import,
        }),
    }
}

async fn commit(archive: &ArchiveWriter, link: &ChainLink, head: bool) {
    archive
        .commit_import(durable_import(link, head))
        .await
        .unwrap_or_else(|err| panic!("commit_import slot {}: {err}", link.signed.message.slot));
}

fn served(archive: &ArchiveWriter) -> Vec<ServedCanonicalBlock> {
    // Slot 0 through the side-branch tip. Missing canonical rows are omitted.
    archive.serve_blocks_by_range(0, 8).expect("by-range serve")
}

fn at(served: &[ServedCanonicalBlock], slot: u64) -> &ServedCanonicalBlock {
    served
        .iter()
        .find(|block| block.slot == slot)
        .unwrap_or_else(|| panic!("slot {slot} was not served from the canonical view"))
}

/// Signed-block prefix the durability bind peeks. Not a body the serve path returns.
fn refusal_ssz(slot: u64, parent: &[u8; 32], state: &[u8; 32]) -> Vec<u8> {
    let mut ssz = vec![0u8; 180];
    ssz[0..4].copy_from_slice(&100u32.to_le_bytes());
    ssz[100..108].copy_from_slice(&slot.to_le_bytes());
    ssz[116..148].copy_from_slice(parent);
    ssz[148..180].copy_from_slice(state);
    ssz
}

fn canonical_store(
    fixture: &anchor_fixture::AnchorFixture,
    links: &[&ChainLink],
) -> cc_fork_choice::Store<Minimal> {
    let state = BeaconState::<Minimal>::from_ssz_bytes_hydrated(ForkName::Fulu, &fixture.state_ssz)
        .expect("anchor state");
    let signed =
        SignedBeaconBlock::<Minimal>::from_ssz_bytes_with(ForkName::Fulu, &fixture.block_ssz)
            .expect("anchor block");
    let mut store = get_forkchoice_store(
        state,
        &signed.message,
        Arc::new(AcceptEngine),
        Arc::new(HarnessAvailability),
        fixture.chain.seconds_per_slot,
    )
    .expect("fork-choice anchor");
    // Past the canonical tip so `on_block` does not defer on future slot.
    on_tick(&mut store, fixture.chain.seconds_per_slot.saturating_mul(4)).expect("on_tick");
    for link in links {
        match on_block(
            &mut store,
            &link.signed,
            &fixture.chain,
            BlockSignatureStrategy::NoVerification,
        ) {
            Ok(BlockImport::Imported(imported)) => {
                assert_eq!(imported.root, link.root, "fork-choice root");
            }
            other => panic!("on_block slot {}: {other:?}", link.signed.message.slot),
        }
    }
    store
}

#[tokio::test]
async fn m14a_losing_branch_served_from_the_canonical_view() {
    let fixture = anchor_fixture();
    let el = spawn_el_stub();
    let dir = TempDir::new("m14a-losing-branch");
    let mut cfg = beacon_config(dir.path(), el.endpoint().to_owned());
    cfg.checkpoint_providers.clear();
    cfg.checkpoint_provider = Some(anchor_fixture::provider_slot(&fixture));
    cfg.genesis_anchor = None;

    let node = boot(cfg).await.expect("boot returns before serve");
    let archive = node.archive();
    assert!(
        archive
            .block_is_durable(bytes_of(&fixture.anchor_root))
            .expect("anchor durability"),
        "boot commit_anchor made the anchor durable"
    );

    // Until that anchor exists, a body whose parent is not durable is refused.
    let missing_parent = [0x44; 32];
    let refused = archive
        .commit_import(DurableImport {
            block_root: [0x45; 32],
            parent_root: missing_parent,
            slot: 1,
            state_root: [0x46; 32],
            ssz: Bytes::from(refusal_ssz(1, &missing_parent, &[0x46; 32])),
            da: DaVerdict::Available,
            scalars: Bytes::from(scalars_ssz(Root::from_array([0x45; 32]), Slot::new(1))),
            head: None,
        })
        .await;
    match refused {
        Err(SeamError::FailedPrecondition { reason }) => {
            assert_eq!(reason, FailedPreconditionReason::ParentNotDurable);
            assert_eq!(reason.as_str(), "PARENT_NOT_DURABLE");
        }
        other => panic!("fresh parent must be PARENT_NOT_DURABLE, got {other:?}"),
    }

    let parent = anchor_state(&fixture);
    let c1 = extend_block(&fixture, &parent, fixture.anchor_root, 0x11);
    let c2 = extend_block(&fixture, &c1.state, c1.root, 0x22);
    let c3 = extend_block(&fixture, &c2.state, c2.root, 0x33);
    commit(&archive, &c1, true).await;
    commit(&archive, &c2, true).await;
    commit(&archive, &c3, true).await;
    let canonical = served(&archive);
    assert_eq!(at(&canonical, 0).root, bytes_of(&fixture.anchor_root));
    assert_eq!(at(&canonical, 0).ssz, fixture.block_ssz);
    assert_eq!(at(&canonical, 1).root, bytes_of(&c1.root));
    assert_eq!(at(&canonical, 1).ssz, c1.signed.as_ssz_bytes());
    assert_eq!(at(&canonical, 2).root, bytes_of(&c2.root));
    assert_eq!(at(&canonical, 3).root, bytes_of(&c3.root));
    assert_eq!(at(&canonical, 3).ssz, c3.signed.as_ssz_bytes());

    // Case 1. Losing sibling at the head slot. `head: None` is its own call.
    let losing = extend_block(&fixture, &c2.state, c2.root, 0x44);
    assert_eq!(losing.signed.message.slot, c3.signed.message.slot);
    commit(&archive, &losing, false).await;
    assert!(
        archive
            .block_is_durable(bytes_of(&losing.root))
            .expect("losing sibling durability"),
        "the losing sibling body is durable"
    );
    let after_losing = served(&archive);
    assert_eq!(
        after_losing, canonical,
        "losing sibling leaves the answer served from the canonical view at the selected head"
    );
    assert!(
        after_losing
            .iter()
            .all(|block| block.root != bytes_of(&losing.root)),
        "the losing sibling is not the served block"
    );
    assert_eq!(at(&after_losing, 3).ssz, c3.signed.as_ssz_bytes());

    // Case 2. Shorter sibling. Rows above its slot stay when the head does not move.
    let shorter = extend_block(&fixture, &parent, fixture.anchor_root, 0x55);
    assert_eq!(shorter.signed.message.slot.as_u64(), 1);
    commit(&archive, &shorter, false).await;
    assert!(
        archive
            .block_is_durable(bytes_of(&shorter.root))
            .expect("shorter sibling durability")
    );
    let after_shorter = served(&archive);
    assert_eq!(
        after_shorter, canonical,
        "shorter sibling leaves answers above its slot served from the canonical view at the selected head"
    );
    assert_eq!(at(&after_shorter, 2).ssz, c2.signed.as_ssz_bytes());
    assert_eq!(at(&after_shorter, 3).ssz, c3.signed.as_ssz_bytes());

    // Side branch, already durable, forked at slot 1 so the later walk is not depth 1.
    let d2 = extend_block(&fixture, &c1.state, c1.root, 0x66);
    let d3 = extend_block(&fixture, &d2.state, d2.root, 0x77);
    let d4 = extend_block(&fixture, &d3.state, d3.root, 0x88);
    assert!(d4.signed.message.slot.as_u64() >= 4);
    commit(&archive, &d2, false).await;
    commit(&archive, &d3, false).await;
    assert_eq!(
        served(&archive),
        canonical,
        "durable side-branch bodies do not move the selected head"
    );

    // Case 3. Two calls: commit the body, then `set_head`. Not one unit.
    commit(&archive, &d4, false).await;
    assert!(
        archive
            .block_is_durable(bytes_of(&d4.root))
            .expect("reorg tip durability"),
        "set_head runs onto a body this commit already made durable"
    );
    assert_eq!(served(&archive), canonical);
    archive
        .set_head(
            HeadChange {
                head_root: bytes_of(&d4.root),
                head_slot: d4.signed.message.slot.as_u64(),
                cause: HeadCause::Attestation,
            },
            Bytes::from(scalars_ssz(d4.root, d4.signed.message.slot)),
        )
        .await
        .expect("attestation set_head");
    let reorged = served(&archive);
    assert_eq!(
        at(&reorged, 1).root,
        bytes_of(&c1.root),
        "fork point stays; a depth-1 sibling swap would not be this reorg"
    );
    assert_eq!(at(&reorged, 2).root, bytes_of(&d2.root));
    assert_eq!(at(&reorged, 2).ssz, d2.signed.as_ssz_bytes());
    assert_eq!(at(&reorged, 3).root, bytes_of(&d3.root));
    assert_eq!(at(&reorged, 3).ssz, d3.signed.as_ssz_bytes());
    assert_eq!(at(&reorged, 4).root, bytes_of(&d4.root));
    assert_eq!(at(&reorged, 4).ssz, d4.signed.as_ssz_bytes());
    assert_ne!(at(&reorged, 2).root, bytes_of(&c2.root));
    assert_ne!(at(&reorged, 3).root, bytes_of(&c3.root));

    // Case 4. Engine invalidation drives `set_head` from invalidation.rs.
    // The selected head is the already-durable canonical tip, two slots back
    // from the side branch, so the same walk rewrites more than the head slot.
    let mut store = canonical_store(&fixture, &[&c1, &c2, &c3]);
    let (selected, _) = get_head(&mut store).expect("get_head");
    assert_eq!(selected, c3.root, "invalidation selects the canonical tip");
    let handle: ArchiveWriteHandle = Arc::new(node.archive());
    let heads = HeadSnapshotStore::new();
    let previous = d4.root;
    let wrote = tokio::task::spawn_blocking(move || {
        commit_engine_invalidation_head(&mut store, &handle, &heads, previous)
    })
    .await
    .expect("invalidation thread")
    .expect("engine invalidation set_head");
    assert!(wrote, "engine invalidation drives set_head");
    let invalidated = served(&node.archive());
    assert_eq!(
        invalidated, canonical,
        "after invalidation the answer is the canonical view at the selected head"
    );
    assert!(
        invalidated.iter().all(|block| block.slot != 4),
        "a shorter selected head is not served above its slot"
    );
    assert_eq!(at(&invalidated, 2).ssz, c2.signed.as_ssz_bytes());
    assert_eq!(at(&invalidated, 3).ssz, c3.signed.as_ssz_bytes());
}
