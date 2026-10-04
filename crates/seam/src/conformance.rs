//! Overflow-policy conformance (`[ARCH]` §2.1–2.2).
//!
//! Eleven named cases. Each is a trait method or a §2.2 overflow row.
//! Do not collapse A–D.
//!
//! Cases 1–8 share one helper / one assertion on both [`InProcess`] and
//! [`Ipc`] / [`IpcEgress`]. Case 9 is an **impl split**, not one shared
//! test — see the two helpers below. Cases 10–11 are overflow rows that
//! are not seam methods; they stay in `cc-chain`.
//!
//! | # | Case | Trace |
//! |---|---|---|
//! | 1 | `backpressure_surfaces_after_deadline` | Policy A, `submit_gossip` |
//! | 2 | `da_backpressure_surfaces_after_deadline` | Policy A, `notify_data_available` |
//! | 3 | `publish_drop_is_observable` | Policy C, `publish` |
//! | 4 | `update_view_never_fails` | `update_view` |
//! | 5 | `oversize_column_is_invalid_argument` | `submit_column_sidecar` |
//! | 6 | `closed_ingress_is_unavailable` | `submit_gossip` / `notify_data_available` |
//! | 7 | `closed_publish_is_unavailable` | `publish` |
//! | 8 | `closed_column_is_unavailable` | `submit_column_sidecar` |
//! | 9 | impl split (not one assertion) | InProcess: HOL-free admit. Ipc: live admit, never Policy A |
//! | 10 | `events::slow_subscriber_is_terminated_not_stalled` | Policy B (`services/chain/tests/events.rs`) |
//! | 11 | `core::slot_tick_is_never_shed` | Policy D (`services/chain/src/core.rs`, `S0-A-14`) |
//!
//! Case 14 is not one of these eleven. `commit_deadline_is_fail_closed_not_backpressure`
//! lives in `cc-storage-core` (`archive_write` tests). A stalled P0 writer
//! aborts after `2 * seconds_per_slot`. Aborting is not backpressure
//! (ADR-R-08).
//!
//! Case 9 helpers (do not collapse these):
//!
//! - [`column_admits_when_import_full`] — InProcess 4096 ring still
//!   admits when import is full (HOL-free).
//! - [`column_admits_on_live_session`] — Ipc multiplexes gossip/DA/column
//!   on one `P2pToChain` stream; a live session admits a sidecar (`Ok`).
//!   Overflow is stall / [`SeamError::InvalidArgument`] /
//!   [`SeamError::Unavailable`] — never [`SeamError::Backpressure`]
//!   (ADR-R-01).
//!
//! Policy A wrappers:
//!
//! - [`InProcess`]: fill import lane [`IMPORT_LANE_DEPTH`], virtual time
//! - [`Ipc`]: dial and poll `run_ipc_loop`; far-side
//!   `RESOURCE_EXHAUSTED` maps to [`IMPORT_LANE_DEPTH`] (live edge)
//!
//! Named `*_ipc` wrappers always dial. Policy C fills [`PUBLISH_BOUND`]
//! on [`InProcess`] and `IpcEgress` after the session is up.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use tokio::time::Instant;

use crate::in_process::{IMPORT_LANE_DEPTH, InProcess, InProcessMailbox, PUBLISH_BOUND};
use crate::ipc::CHAIN_OUT_BOUND;
use crate::ipc::tests::LiveIpc;
use crate::{
    ChainIngress, ChainView, ColumnSidecar, GossipObject, IMPORT_SEND_TIMEOUT,
    MAX_EVENT_PAYLOAD_BYTES, ObjectKind, P2pEgress, PublishRequest, Published, SeamError,
};

fn gossip(root: crate::Root) -> GossipObject {
    GossipObject {
        ssz: Vec::new(),
        fork: 0,
        root,
        kind: ObjectKind::Block,
        subnet_id: 0,
    }
}

fn publish_req(topic: &str) -> PublishRequest {
    PublishRequest {
        ssz: Vec::new(),
        kind: ObjectKind::Block,
        topic: topic.to_owned(),
        subnet_id: 0,
    }
}

fn sidecar(ssz: Vec<u8>) -> ColumnSidecar {
    ColumnSidecar {
        ssz,
        fork: 0,
        root: [0; 32],
        column_index: 0,
        subnet_id: 0,
    }
}

fn sample_view() -> ChainView {
    ChainView {
        slot: 9,
        head_root: [9; 32],
        view_kind: 3,
        ..ChainView::default()
    }
}

fn assert_backpressure(err: SeamError, start: Instant, bound: usize) {
    assert!(
        start.elapsed() >= IMPORT_SEND_TIMEOUT,
        "must wait the deadline, elapsed {:?}",
        start.elapsed()
    );
    assert!(
        matches!(
            err,
            SeamError::Backpressure {
                bound: got,
                waited_ms
            } if got == bound && waited_ms >= IMPORT_SEND_TIMEOUT.as_millis() as u64
        ),
        "must surface Backpressure {{ bound: {bound}, waited_ms ≥ {} }}, got {err:?}",
        IMPORT_SEND_TIMEOUT.as_millis()
    );
}

/// Policy A: a full send path returns [`SeamError::Backpressure`] with the
/// impl's named `bound` only after blocking for [`IMPORT_SEND_TIMEOUT`].
async fn backpressure_surfaces_after_deadline(ingress: &dyn ChainIngress, bound: usize) {
    let start = Instant::now();
    let err = ingress.submit_gossip(gossip([0xff; 32])).await.unwrap_err();
    assert_backpressure(err, start, bound);
}

#[tokio::test(start_paused = true)]
async fn backpressure_surfaces_after_deadline_in_process() {
    let (seam, mailbox) = InProcess::pair();
    for slot in 0..IMPORT_LANE_DEPTH as u64 {
        seam.notify_data_available([0; 32], slot).await.unwrap();
    }
    assert_eq!(mailbox.import_rx.len(), IMPORT_LANE_DEPTH);
    backpressure_surfaces_after_deadline(&seam, IMPORT_LANE_DEPTH).await;
    drop(mailbox);
}

#[tokio::test]
async fn backpressure_surfaces_after_deadline_ipc() {
    let live = LiveIpc::exhaust_on_object().await;
    let err = live
        .ipc
        .submit_gossip(gossip([0xff; 32]))
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            SeamError::Backpressure {
                bound,
                waited_ms
            } if bound == IMPORT_LANE_DEPTH
                && waited_ms >= IMPORT_SEND_TIMEOUT.as_millis() as u64
        ),
        "live Ipc Policy A is RESOURCE_EXHAUSTED → bound IMPORT_LANE_DEPTH, got {err:?}"
    );
    live.shutdown().await;
}

/// Policy A on E2: `notify_data_available` uses the same lane and deadline.
async fn da_backpressure_surfaces_after_deadline(ingress: &dyn ChainIngress, bound: usize) {
    let start = Instant::now();
    let err = ingress
        .notify_data_available([0xfe; 32], 99)
        .await
        .unwrap_err();
    assert_backpressure(err, start, bound);
}

#[tokio::test(start_paused = true)]
async fn da_backpressure_surfaces_after_deadline_in_process() {
    let (seam, mailbox) = InProcess::pair();
    for slot in 0..IMPORT_LANE_DEPTH as u64 {
        seam.notify_data_available([0; 32], slot).await.unwrap();
    }
    assert_eq!(mailbox.import_rx.len(), IMPORT_LANE_DEPTH);
    da_backpressure_surfaces_after_deadline(&seam, IMPORT_LANE_DEPTH).await;
    drop(mailbox);
}

#[tokio::test]
async fn da_backpressure_surfaces_after_deadline_ipc() {
    let mut live = LiveIpc::exhaust_on_object().await;
    let killed = live
        .ipc
        .submit_gossip(gossip([0xaa; 32]))
        .await
        .unwrap_err();
    assert!(
        matches!(
            killed,
            SeamError::Backpressure {
                bound: IMPORT_LANE_DEPTH,
                ..
            }
        ),
        "session must be torn down by RESOURCE_EXHAUSTED first, got {killed:?}"
    );
    live.stop_server();
    da_backpressure_surfaces_after_deadline(&live.ipc, CHAIN_OUT_BOUND).await;
    live.shutdown().await;
}

/// Policy C: a full publish queue returns [`Published::Dropped`] as a
/// value, never [`SeamError::Backpressure`].
async fn publish_drop_is_observable(egress: &dyn P2pEgress) {
    let result = egress.publish(publish_req("overflow")).await;
    assert!(
        matches!(result, Ok(Published::Dropped)),
        "policy C must return Ok(Published::Dropped), got {result:?}"
    );
    let again = egress.publish(publish_req("overflow-2")).await;
    assert!(
        matches!(again, Ok(Published::Dropped)),
        "policy C drop must not become Backpressure, got {again:?}"
    );
}

async fn fill_publish_queue(egress: &dyn P2pEgress) {
    for i in 0..PUBLISH_BOUND {
        let out = egress.publish(publish_req(&format!("t{i}"))).await.unwrap();
        assert_eq!(
            out,
            Published::Queued,
            "slot {i} must enqueue before overflow"
        );
    }
}

#[tokio::test]
async fn publish_drop_is_observable_in_process() {
    let (seam, mailbox) = InProcess::pair();
    fill_publish_queue(&seam).await;
    assert_eq!(mailbox.publish_rx.len(), PUBLISH_BOUND);
    assert_eq!(mailbox.publish_rx.capacity(), 0);
    publish_drop_is_observable(&seam).await;
    assert_eq!(mailbox.publish_rx.len(), PUBLISH_BOUND);
    drop(mailbox);
}

#[tokio::test]
async fn publish_drop_is_observable_ipc() {
    let live = LiveIpc::echo().await;
    fill_publish_queue(&live.egress).await;
    assert_eq!(live.mailbox.publish_rx.max_capacity(), PUBLISH_BOUND);
    assert_eq!(live.mailbox.publish_rx.len(), PUBLISH_BOUND);
    assert_eq!(live.mailbox.publish_rx.capacity(), 0);
    publish_drop_is_observable(&live.egress).await;
    assert_eq!(live.mailbox.publish_rx.len(), PUBLISH_BOUND);
    live.shutdown().await;
}

/// `update_view` is an ArcSwap store — never blocks, never fails.
fn update_view_never_fails(egress: &dyn P2pEgress, before_drop: impl FnOnce(&ChainView)) {
    let view = sample_view();
    egress.update_view(view.clone());
    before_drop(&view);
    egress.update_view(ChainView {
        slot: 10,
        ..ChainView::default()
    });
}

#[tokio::test]
async fn update_view_never_fails_in_process() {
    let (seam, mailbox) = InProcess::pair();
    update_view_never_fails(&seam, |view| {
        assert_eq!(*mailbox.load_view(), *view);
        drop(mailbox);
    });
}

#[tokio::test]
async fn update_view_never_fails_ipc() {
    let live = LiveIpc::echo().await;
    update_view_never_fails(&live.egress, |view| {
        assert_eq!(*live.mailbox.load_view(), *view);
    });
    live.shutdown().await;
}

/// Oversize column sidecar is structurally rejected before any enqueue.
async fn oversize_column_is_invalid_argument(ingress: &dyn ChainIngress) {
    let err = ingress
        .submit_column_sidecar(sidecar(vec![0; MAX_EVENT_PAYLOAD_BYTES + 1]))
        .await
        .unwrap_err();
    assert!(
        matches!(err, SeamError::InvalidArgument(_)),
        "oversize column must be InvalidArgument, got {err:?}"
    );
}

#[tokio::test]
async fn oversize_column_is_invalid_argument_in_process() {
    let (seam, mailbox) = InProcess::pair();
    oversize_column_is_invalid_argument(&seam).await;
    assert!(mailbox.column_rx.is_empty());
    drop(mailbox);
}

#[tokio::test]
async fn oversize_column_is_invalid_argument_ipc() {
    let live = LiveIpc::echo().await;
    oversize_column_is_invalid_argument(&live.ipc).await;
    live.shutdown().await;
}

/// Closed import / out lane is [`SeamError::Unavailable`], not a drop.
async fn closed_ingress_is_unavailable(ingress: &dyn ChainIngress) {
    let err = ingress.submit_gossip(gossip([0x11; 32])).await.unwrap_err();
    assert!(
        matches!(err, SeamError::Unavailable(_)),
        "closed submit_gossip must be Unavailable, got {err:?}"
    );
    let err = ingress
        .notify_data_available([0x12; 32], 1)
        .await
        .unwrap_err();
    assert!(
        matches!(err, SeamError::Unavailable(_)),
        "closed notify_data_available must be Unavailable, got {err:?}"
    );
}

#[tokio::test]
async fn closed_ingress_is_unavailable_in_process() {
    let (seam, mailbox) = InProcess::pair();
    drop(mailbox);
    closed_ingress_is_unavailable(&seam).await;
}

#[tokio::test]
async fn closed_ingress_is_unavailable_ipc() {
    let live = LiveIpc::echo().await;
    let ipc = live.ipc.clone();
    live.shutdown().await;
    closed_ingress_is_unavailable(&ipc).await;
}

async fn closed_publish_is_unavailable(egress: &dyn P2pEgress) {
    let err = egress.publish(publish_req("closed")).await.unwrap_err();
    assert!(
        matches!(err, SeamError::Unavailable(_)),
        "closed publish must be Unavailable, got {err:?}"
    );
}

#[tokio::test]
async fn closed_publish_is_unavailable_in_process() {
    let (seam, mailbox) = InProcess::pair();
    drop(mailbox);
    closed_publish_is_unavailable(&seam).await;
}

#[tokio::test]
async fn closed_publish_is_unavailable_ipc() {
    let live = LiveIpc::echo().await;
    let egress = live.egress.clone();
    live.shutdown().await;
    closed_publish_is_unavailable(&egress).await;
}

async fn closed_column_is_unavailable(ingress: &dyn ChainIngress) {
    let err = ingress
        .submit_column_sidecar(sidecar(vec![1]))
        .await
        .unwrap_err();
    assert!(
        matches!(err, SeamError::Unavailable(_)),
        "closed column bus must be Unavailable, got {err:?}"
    );
}

#[tokio::test]
async fn closed_column_is_unavailable_in_process() {
    let (seam, mailbox) = InProcess::pair();
    drop(mailbox);
    closed_column_is_unavailable(&seam).await;
}

#[tokio::test]
async fn closed_column_is_unavailable_ipc() {
    let live = LiveIpc::echo().await;
    let ipc = live.ipc.clone();
    live.shutdown().await;
    closed_column_is_unavailable(&ipc).await;
}

/// Case 9 / InProcess — HOL-free admit. Not the Ipc assertion.
///
/// Columns ride the separate 4096 [`crate::DEFAULT_RING_CAPACITY`] ring.
/// A full import lane must still accept a sidecar (`Ok`), leave
/// `import_rx` at [`IMPORT_LANE_DEPTH`], and put one item on `column_rx`.
/// No overflow, no [`SeamError::Backpressure`].
async fn column_admits_when_import_full(ingress: &dyn ChainIngress, mailbox: &InProcessMailbox) {
    assert_eq!(mailbox.import_rx.len(), IMPORT_LANE_DEPTH);
    ingress
        .submit_column_sidecar(sidecar(vec![1, 2, 3]))
        .await
        .unwrap();
    assert_eq!(mailbox.import_rx.len(), IMPORT_LANE_DEPTH);
    assert_eq!(mailbox.column_rx.len(), 1);
}

/// Case 9 / Ipc — live admit on the multiplexed stream. Not Policy A.
///
/// Ipc still shares `P2pToChain` with gossip/DA (impl split). A live
/// session must admit a sidecar (`Ok`). ADR-R-01: column overflow is
/// stall / InvalidArgument / Unavailable — never
/// [`SeamError::Backpressure`].
async fn column_admits_on_live_session(ingress: &dyn ChainIngress) {
    let out = ingress.submit_column_sidecar(sidecar(vec![1, 2, 3])).await;
    assert!(
        matches!(out, Ok(())),
        "live Ipc column must admit, not Backpressure, got {out:?}"
    );
}

#[tokio::test]
async fn column_does_not_ride_import_lane_in_process() {
    let (seam, mailbox) = InProcess::pair();
    for slot in 0..IMPORT_LANE_DEPTH as u64 {
        seam.notify_data_available([0; 32], slot).await.unwrap();
    }
    column_admits_when_import_full(&seam, &mailbox).await;
    drop(mailbox);
}

#[tokio::test]
async fn column_admits_on_live_session_ipc() {
    let live = LiveIpc::echo().await;
    live.ipc.submit_gossip(gossip([1; 32])).await.unwrap();
    column_admits_on_live_session(&live.ipc).await;
    live.shutdown().await;
}
