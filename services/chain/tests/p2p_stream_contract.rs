//! CC-27a chain-side stream contract tests.
//!
//! - `StreamHello` → full `ChainView`
//! - `ChainView` cadence (slot / head / epoch payload)
//! - `ChainView` matches `GetHead` over many head publishes
//! - `EpochContext` ArcSwap lock-free while core blocked
//! - `GetValidatorRecords` bound + SSZ content
//! - `PublishRequest` unknown topic → structured error
//! - `ColumnSidecar` has no construction site in services/crates (grep-style)

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::time::Duration;

use cc_chain::core::{CoreConfig, MAX_VALIDATOR_RECORDS_PER_REQUEST, spawn_core_thread_with_epoch};
use cc_chain::epoch_context::{EpochContext, EpochContextStore};
use cc_chain::events::{EventsConfig, EventsHandle};
use cc_chain::head::{HeadSnapshot, HeadSnapshotStore};
use cc_chain::metrics::ChainMetrics;
use cc_chain::p2p_stream::{
    VIEW_KIND_EPOCH_TICK, VIEW_KIND_FULL, VIEW_KIND_HEAD_CHANGE, VIEW_KIND_SLOT_TICK, ViewTick,
    build_chain_view,
};
use cc_chain::service::ChainServiceImpl;
use cc_fork_choice::{HarnessAvailability, get_forkchoice_store};
use cc_proto::chain::chain_service_server::ChainService;
use cc_proto::chain::{GetHeadRequest, GetValidatorRecordsRequest};
use cc_proto::error_info_from_status;
use cc_proto::p2p::{
    ObjectKind, P2pToChain, PublishRequest, StreamHello, chain_to_p2p, p2p_to_chain,
};
use cc_state_transition::helpers::constants::{FAR_FUTURE_EPOCH, MAX_EFFECTIVE_BALANCE};
use cc_types::config::{BlobParameters, BlobSchedule, ChainConfig, PresetName};
use cc_types::containers::{Checkpoint, Validator};
use cc_types::preset::Minimal;
use cc_types::primitives::{
    BlsPublicKey, Epoch, ExecutionAddress, ForkVersion, Root, Slot, ValidatorIndex,
};
use cc_types::{BeaconBlock, BeaconState};
use futures::StreamExt;
use prometheus_client::registry::Registry;
use tonic::{Code, Request};

/// Private always-Valid test harness (CC-32b: production stub deleted; not exported).
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

const VALIDATORS: usize = 64;

fn minimal_config() -> ChainConfig {
    ChainConfig {
        preset_base: PresetName::Minimal,
        config_name: "minimal".into(),
        genesis_fork_version: ForkVersion::from_array([0x00, 0x00, 0x00, 0x01]),
        altair_fork_version: ForkVersion::from_array([0x01, 0x00, 0x00, 0x01]),
        altair_fork_epoch: Epoch::new(0),
        bellatrix_fork_version: ForkVersion::from_array([0x02, 0x00, 0x00, 0x01]),
        bellatrix_fork_epoch: Epoch::new(0),
        capella_fork_version: ForkVersion::from_array([0x03, 0x00, 0x00, 0x01]),
        capella_fork_epoch: Epoch::new(0),
        deneb_fork_version: ForkVersion::from_array([0x04, 0x00, 0x00, 0x01]),
        deneb_fork_epoch: Epoch::new(0),
        electra_fork_version: ForkVersion::from_array([0x05, 0x00, 0x00, 0x01]),
        electra_fork_epoch: Epoch::new(0),
        fulu_fork_version: ForkVersion::from_array([0x06, 0x00, 0x00, 0x01]),
        fulu_fork_epoch: Epoch::new(0),
        seconds_per_slot: 6,
        blob_schedule: BlobSchedule::try_from_entries(vec![BlobParameters {
            epoch: Epoch::new(0),
            max_blobs_per_block: 9,
        }])
        .unwrap(),
        deposit_chain_id: 0,
        deposit_contract_address: ExecutionAddress::ZERO,
        churn_limit_quotient: 32,
        min_per_epoch_churn_limit_electra: 64_000_000_000,
        max_per_epoch_activation_exit_churn_limit: 128_000_000_000,
        shard_committee_period: Epoch::new(64),
        max_blobs_per_block_electra: 9,
    }
}

fn active_validator(i: u64) -> Validator {
    Validator {
        pubkey: BlsPublicKey::from_array({
            let mut pk = [0u8; 48];
            pk[0..8].copy_from_slice(&i.to_le_bytes());
            pk[47] = 0x01;
            pk
        }),
        withdrawal_credentials: Root::from_array({
            let mut c = [0u8; 32];
            c[0] = 0x01;
            c
        }),
        effective_balance: MAX_EFFECTIVE_BALANCE,
        slashed: false,
        activation_eligibility_epoch: Epoch::new(0),
        activation_epoch: Epoch::new(0),
        exit_epoch: FAR_FUTURE_EPOCH,
        withdrawable_epoch: FAR_FUTURE_EPOCH,
    }
}

fn state_with_validators(n: usize, slot: Slot) -> BeaconState<Minimal> {
    let mut state = BeaconState::<Minimal>::default();
    state.set_genesis_time(1_700_000_000);
    state.set_genesis_validators_root(Root::from_array([0xee; 32]));
    state.set_slot(slot);
    for i in 0..n {
        state.validators_push(active_validator(i as u64)).unwrap();
        state.balances_push(MAX_EFFECTIVE_BALANCE).unwrap();
    }
    for i in 0..state.proposer_lookahead_len() {
        state
            .proposer_lookahead_set(i, ValidatorIndex::new((i as u64) % n as u64))
            .unwrap();
    }
    state
}

fn spawn_svc() -> (ChainServiceImpl, cc_chain::CoreThread, EventsHandle) {
    let state = state_with_validators(VALIDATORS, Slot::new(8));
    let config = minimal_config();
    let anchor_block = BeaconBlock {
        slot: state.slot(),
        proposer_index: ValidatorIndex::new(0),
        parent_root: Root::ZERO,
        state_root: Root::ZERO,
        body: Default::default(),
    };
    let store = get_forkchoice_store(
        state,
        &anchor_block,
        Arc::new(AcceptEngine),
        Arc::new(HarnessAvailability),
        config.seconds_per_slot,
    )
    .unwrap();

    let mut registry = Registry::default();
    let metrics = ChainMetrics::register(&mut registry);
    let events = EventsHandle::spawn(EventsConfig {
        ring_capacity: 32,
        subscriber_queue_capacity: 16,
        session_id: Some(0x27),
        ring_bytes: usize::MAX,
    });
    let head = HeadSnapshotStore::new();
    let epoch = EpochContextStore::new();
    let core = spawn_core_thread_with_epoch(
        store,
        config,
        head.clone(),
        epoch.clone(),
        events.event_sender(),
        metrics.clone(),
        CoreConfig::default(),
    );
    let svc = ChainServiceImpl::with_epoch(
        Some(core.handle.clone()),
        head,
        epoch,
        events.clone(),
        metrics,
    );
    (svc, core, events)
}

async fn shutdown(core: cc_chain::CoreThread, events: EventsHandle) {
    core.handle.shutdown().await;
    core.join();
    events.shutdown().await;
}

// ── StreamHello → full ChainView ────────────────────────────────────────────

#[tokio::test]
async fn stream_hello_receives_full_chain_view() {
    let (svc, core, events) = spawn_svc();

    let (in_tx, in_rx) = tokio::sync::mpsc::channel(8);
    let inbound = tokio_stream::wrappers::ReceiverStream::new(in_rx);
    let mut outbound = svc.open_p2p_stream(inbound).await.unwrap().into_inner();

    in_tx
        .send(Ok(P2pToChain {
            seq: 1,
            msg: Some(p2p_to_chain::Msg::Hello(StreamHello {
                session_id: 0xdead_beef,
                resume_seq: 0,
            })),
        }))
        .await
        .unwrap();

    let msg = tokio::time::timeout(Duration::from_secs(2), outbound.next())
        .await
        .expect("timeout waiting for ChainView")
        .expect("stream ended")
        .expect("status error");

    let view = match msg.msg {
        Some(chain_to_p2p::Msg::View(v)) => v,
        other => panic!("expected ChainView, got {other:?}"),
    };
    assert_eq!(view.view_kind, VIEW_KIND_FULL);
    assert!(!view.head_root.is_empty());
    assert!(
        !view.proposer_lookahead.is_empty(),
        "full view needs lookahead"
    );
    assert_eq!(
        view.proposer_pubkeys.len(),
        view.proposer_lookahead.len(),
        "pubkeys parallel to lookahead"
    );
    assert!(view.active_validator_count > 0);
    assert_eq!(view.genesis_time, 1_700_000_000);

    drop(in_tx);
    shutdown(core, events).await;
}

// ── Cadence: production drivers (no notify_tick for epoch/head) ─────────────

/// Collect the next `n` ChainView messages, ignoring any unexpected kinds.
async fn next_views(
    outbound: &mut (impl StreamExt<Item = Result<cc_proto::p2p::ChainToP2p, tonic::Status>> + Unpin),
    n: usize,
) -> Vec<cc_proto::p2p::ChainView> {
    let mut out = Vec::with_capacity(n);
    while out.len() < n {
        let msg = tokio::time::timeout(Duration::from_secs(3), outbound.next())
            .await
            .expect("timeout waiting for ChainView")
            .expect("stream ended")
            .expect("status error");
        if let Some(chain_to_p2p::Msg::View(v)) = msg.msg {
            out.push(v);
        }
    }
    out
}

#[tokio::test]
async fn chain_view_cadence_epoch_payload_three_times() {
    // genesis_time=0 disables the wall-clock slot driver so only explicit slot
    // notify_ticks (and production epoch/head watchers) fire.
    let head = HeadSnapshotStore::with_snapshot(HeadSnapshot {
        head_root: Root::from_array([0x11; 32]),
        head_slot: Slot::new(0),
        sequence: 1,
        finalized: Checkpoint {
            epoch: Epoch::new(0),
            root: Root::from_array([0x22; 32]),
        },
        justified: Checkpoint {
            epoch: Epoch::new(0),
            root: Root::from_array([0x33; 32]),
        },
        ..HeadSnapshot::default()
    });
    let epoch = EpochContextStore::with_context(EpochContext {
        epoch: Epoch::new(0),
        proposer_lookahead: vec![0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7],
        proposer_pubkeys: (0..16).map(|i| vec![i as u8; 48]).collect(),
        active_validator_count: 64,
        genesis_time: 0, // no wall-clock slot noise
        genesis_validators_root: Root::from_array([0xaa; 32]),
        seconds_per_slot: 6,
        slots_per_epoch: 8,
        sequence: 1,
    });

    let mut registry = Registry::default();
    let metrics = ChainMetrics::register(&mut registry);
    let events = EventsHandle::spawn(EventsConfig::default());
    let svc =
        ChainServiceImpl::with_epoch(None, head.clone(), epoch.clone(), events.clone(), metrics);

    let (in_tx, in_rx) = tokio::sync::mpsc::channel(8);
    let inbound = tokio_stream::wrappers::ReceiverStream::new(in_rx);
    let mut outbound = svc.open_p2p_stream(inbound).await.unwrap().into_inner();

    // Open session.
    in_tx
        .send(Ok(P2pToChain {
            seq: 1,
            msg: Some(p2p_to_chain::Msg::Hello(StreamHello {
                session_id: 1,
                resume_seq: 0,
            })),
        }))
        .await
        .unwrap();
    let _full = outbound.next().await.unwrap().unwrap();

    let deps = svc.stream_deps();
    let mut epoch_payloads = 0u32;
    let mut slot_or_head = 0u32;

    // Three epochs: production head watcher + epoch-sequence driver, slot via
    // notify_tick only (wall-clock disabled by genesis_time=0).
    for e in 0..3u64 {
        head.store(HeadSnapshot {
            head_root: Root::from_array([e as u8 + 1; 32]),
            head_slot: Slot::new(e * 8 + 1),
            sequence: e + 2,
            finalized: Checkpoint {
                epoch: Epoch::new(e),
                root: Root::from_array([0x22; 32]),
            },
            justified: Checkpoint {
                epoch: Epoch::new(e),
                root: Root::from_array([0x33; 32]),
            },
            ..HeadSnapshot::default()
        });
        // Production path: sequence advance alone must deliver EPOCH_TICK (F1).
        // Do NOT call notify_tick(Epoch).
        epoch.store(EpochContext {
            epoch: Epoch::new(e),
            proposer_lookahead: vec![e; 16],
            proposer_pubkeys: vec![vec![e as u8; 48]; 16],
            active_validator_count: 64,
            genesis_time: 0,
            genesis_validators_root: Root::from_array([0xaa; 32]),
            seconds_per_slot: 6,
            slots_per_epoch: 8,
            sequence: e + 2,
        });
        // Slot still via explicit bus (wall-clock path covered separately).
        deps.notify_tick(ViewTick::Slot);

        // Expect: HEAD_CHANGE (head.store), EPOCH_TICK (epoch.store), SLOT_TICK.
        let views = next_views(&mut outbound, 3).await;
        for view in views {
            match view.view_kind {
                VIEW_KIND_EPOCH_TICK => {
                    assert!(
                        !view.proposer_lookahead.is_empty(),
                        "epoch tick must carry lookahead"
                    );
                    assert_eq!(view.active_validator_count, 64);
                    epoch_payloads += 1;
                }
                VIEW_KIND_SLOT_TICK | VIEW_KIND_HEAD_CHANGE => {
                    assert!(
                        view.proposer_lookahead.is_empty(),
                        "slot/head must not carry lookahead"
                    );
                    assert_eq!(view.active_validator_count, 0);
                    slot_or_head += 1;
                }
                other => panic!("unexpected view_kind {other}"),
            }
        }
    }

    assert_eq!(epoch_payloads, 3, "lookahead payload exactly three times");
    assert_eq!(slot_or_head, 6, "two non-epoch views per epoch × 3");

    drop(in_tx);
    events.shutdown().await;
}

/// F4: head.store alone must produce VIEW_KIND_HEAD_CHANGE on the live stream.
#[tokio::test]
async fn head_store_alone_emits_head_change_view() {
    let head = HeadSnapshotStore::with_snapshot(HeadSnapshot {
        head_root: Root::from_array([0x01; 32]),
        head_slot: Slot::new(1),
        sequence: 1,
        ..HeadSnapshot::default()
    });
    let epoch = EpochContextStore::with_context(EpochContext {
        sequence: 1,
        slots_per_epoch: 8,
        // genesis_time=0: no wall-clock slot ticks.
        ..EpochContext::default()
    });
    let mut registry = Registry::default();
    let metrics = ChainMetrics::register(&mut registry);
    let events = EventsHandle::spawn(EventsConfig::default());
    let svc = ChainServiceImpl::with_epoch(None, head.clone(), epoch, events.clone(), metrics);

    let (in_tx, in_rx) = tokio::sync::mpsc::channel(4);
    let mut outbound = svc
        .open_p2p_stream(tokio_stream::wrappers::ReceiverStream::new(in_rx))
        .await
        .unwrap()
        .into_inner();
    in_tx
        .send(Ok(P2pToChain {
            seq: 1,
            msg: Some(p2p_to_chain::Msg::Hello(StreamHello {
                session_id: 2,
                resume_seq: 0,
            })),
        }))
        .await
        .unwrap();
    let _ = outbound.next().await.unwrap().unwrap();

    head.store(HeadSnapshot {
        head_root: Root::from_array([0xab; 32]),
        head_slot: Slot::new(42),
        sequence: 2,
        ..HeadSnapshot::default()
    });

    let views = next_views(&mut outbound, 1).await;
    assert_eq!(views[0].view_kind, VIEW_KIND_HEAD_CHANGE);
    assert_eq!(views[0].head_slot, 42);
    assert!(views[0].proposer_lookahead.is_empty());

    drop(in_tx);
    events.shutdown().await;
}

/// F1: epoch.store sequence advance alone must deliver EPOCH_TICK with lookahead.
#[tokio::test]
async fn epoch_store_alone_emits_epoch_tick_view() {
    let head = HeadSnapshotStore::with_snapshot(HeadSnapshot {
        head_root: Root::from_array([0x01; 32]),
        head_slot: Slot::new(8),
        sequence: 1,
        ..HeadSnapshot::default()
    });
    let epoch = EpochContextStore::with_context(EpochContext {
        sequence: 1,
        slots_per_epoch: 8,
        active_validator_count: 1,
        proposer_lookahead: vec![0],
        proposer_pubkeys: vec![vec![0u8; 48]],
        ..EpochContext::default()
    });
    let mut registry = Registry::default();
    let metrics = ChainMetrics::register(&mut registry);
    let events = EventsHandle::spawn(EventsConfig::default());
    let svc = ChainServiceImpl::with_epoch(None, head, epoch.clone(), events.clone(), metrics);

    let (in_tx, in_rx) = tokio::sync::mpsc::channel(4);
    let mut outbound = svc
        .open_p2p_stream(tokio_stream::wrappers::ReceiverStream::new(in_rx))
        .await
        .unwrap()
        .into_inner();
    in_tx
        .send(Ok(P2pToChain {
            seq: 1,
            msg: Some(p2p_to_chain::Msg::Hello(StreamHello {
                session_id: 3,
                resume_seq: 0,
            })),
        }))
        .await
        .unwrap();
    let _ = outbound.next().await.unwrap().unwrap();

    // No notify_tick — production epoch-sequence driver only.
    epoch.store(EpochContext {
        sequence: 2,
        slots_per_epoch: 8,
        active_validator_count: 64,
        proposer_lookahead: vec![1, 2, 3, 4],
        proposer_pubkeys: vec![vec![1u8; 48]; 4],
        ..EpochContext::default()
    });

    let views = next_views(&mut outbound, 1).await;
    assert_eq!(views[0].view_kind, VIEW_KIND_EPOCH_TICK);
    assert_eq!(views[0].proposer_lookahead, vec![1, 2, 3, 4]);
    assert_eq!(views[0].active_validator_count, 64);

    drop(in_tx);
    events.shutdown().await;
}

/// F2: install_core is visible to a live session (core re-read on gossip).
#[tokio::test]
async fn install_core_visible_to_live_session() {
    use cc_proto::p2p::{GossipObject, ObjectKind};

    let head = HeadSnapshotStore::new();
    let epoch = EpochContextStore::new();
    let mut registry = Registry::default();
    let metrics = ChainMetrics::register(&mut registry);
    let events = EventsHandle::spawn(EventsConfig::default());
    // Open service with no core.
    let svc = ChainServiceImpl::with_epoch(
        None,
        head.clone(),
        epoch.clone(),
        events.clone(),
        metrics.clone(),
    );

    let (in_tx, in_rx) = tokio::sync::mpsc::channel(8);
    let mut outbound = svc
        .open_p2p_stream(tokio_stream::wrappers::ReceiverStream::new(in_rx))
        .await
        .unwrap()
        .into_inner();
    in_tx
        .send(Ok(P2pToChain {
            seq: 1,
            msg: Some(p2p_to_chain::Msg::Hello(StreamHello {
                session_id: 4,
                resume_seq: 0,
            })),
        }))
        .await
        .unwrap();
    let _ = outbound.next().await.unwrap().unwrap();

    // Without core: BLOCK → IGNORE / Internal.
    in_tx
        .send(Ok(P2pToChain {
            seq: 2,
            msg: Some(p2p_to_chain::Msg::Object(GossipObject {
                ssz: vec![],
                fork: 0,
                root: vec![0u8; 32],
                source: 0,
                kind: ObjectKind::Block as i32,
                subnet_id: 0,
            })),
        }))
        .await
        .unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(2), outbound.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    match msg.msg {
        Some(chain_to_p2p::Msg::Verdict(v)) => {
            assert_eq!(v.reason, cc_proto::p2p::Reason::Internal as i32);
        }
        other => panic!("expected verdict, got {other:?}"),
    }

    // Install a real core sharing the same epoch store — live session must see it.
    let state = state_with_validators(VALIDATORS, Slot::new(8));
    let config = minimal_config();
    let anchor_block = BeaconBlock {
        slot: state.slot(),
        proposer_index: ValidatorIndex::new(0),
        parent_root: Root::ZERO,
        state_root: Root::ZERO,
        body: Default::default(),
    };
    let store = get_forkchoice_store(
        state,
        &anchor_block,
        Arc::new(AcceptEngine),
        Arc::new(HarnessAvailability),
        config.seconds_per_slot,
    )
    .unwrap();
    let core = spawn_core_thread_with_epoch(
        store,
        config,
        head,
        epoch,
        events.event_sender(),
        metrics,
        CoreConfig::default(),
    );
    svc.install_core(core.handle.clone());
    assert!(svc.is_bootstrapped());
    assert!(svc.stream_deps().core_handle().is_some());

    // With core: invalid SSZ still maps to Reject/Invalid (not Internal/no-core).
    in_tx
        .send(Ok(P2pToChain {
            seq: 3,
            msg: Some(p2p_to_chain::Msg::Object(GossipObject {
                ssz: vec![0xde, 0xad],
                fork: 0,
                root: vec![1u8; 32],
                source: 0,
                kind: ObjectKind::Block as i32,
                subnet_id: 0,
            })),
        }))
        .await
        .unwrap();
    let msg = tokio::time::timeout(Duration::from_secs(2), outbound.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    match msg.msg {
        Some(chain_to_p2p::Msg::Verdict(v)) => {
            // Core present → decode failure is InvalidArgument → Reject, not Internal.
            assert_ne!(
                v.reason,
                cc_proto::p2p::Reason::Internal as i32,
                "live session must see install_core"
            );
        }
        other => panic!("expected verdict, got {other:?}"),
    }

    drop(in_tx);
    shutdown(core, events).await;
}

// ── ChainView matches GetHead ───────────────────────────────────────────────

#[tokio::test]
async fn chain_view_matches_get_head_over_many_publishes() {
    let head = HeadSnapshotStore::new();
    let epoch = EpochContextStore::with_context(EpochContext {
        seconds_per_slot: 6,
        slots_per_epoch: 8,
        sequence: 1,
        ..EpochContext::default()
    });
    let mut registry = Registry::default();
    let metrics = ChainMetrics::register(&mut registry);
    let events = EventsHandle::spawn(EventsConfig::default());
    // Pre-seed a non-zero snapshot so GetHead is not NOT_BOOTSTRAPPED.
    head.store(HeadSnapshot {
        head_root: Root::from_array([0x01; 32]),
        head_slot: Slot::new(0),
        sequence: 1,
        ..HeadSnapshot::default()
    });
    let svc =
        ChainServiceImpl::with_epoch(None, head.clone(), epoch.clone(), events.clone(), metrics);

    for i in 1..=100u64 {
        let root = Root::from_array({
            let mut r = [0u8; 32];
            r[..8].copy_from_slice(&i.to_le_bytes());
            r
        });
        let snap = HeadSnapshot {
            head_root: root,
            head_slot: Slot::new(i),
            sequence: i,
            justified: Checkpoint {
                epoch: Epoch::new(i / 8),
                root: Root::from_array([0xb1; 32]),
            },
            finalized: Checkpoint {
                epoch: Epoch::new(i / 8),
                root: Root::from_array([0xb2; 32]),
            },
            ..HeadSnapshot::default()
        };
        head.store(snap);

        let get_head = svc
            .get_head(Request::new(GetHeadRequest {}))
            .await
            .unwrap()
            .into_inner();
        let view = build_chain_view(&head, &epoch, VIEW_KIND_HEAD_CHANGE, false);

        assert_eq!(view.head_root, get_head.head_root, "i={i}");
        assert_eq!(view.head_slot, get_head.head_slot, "i={i}");
        let fin = get_head.finalized.as_ref().unwrap();
        assert_eq!(view.finalized_root, fin.root, "i={i}");
        assert_eq!(view.finalized_epoch, fin.epoch, "i={i}");
    }

    events.shutdown().await;
}

// ── EpochContext lock-free under core block ─────────────────────────────────

#[tokio::test]
async fn epoch_context_readable_while_core_blocked() {
    let (svc, core, events) = spawn_svc();
    let epoch = svc.epoch_context().clone();

    // Ensure a non-zero sequence was published at spawn.
    assert!(epoch.load().sequence > 0 || epoch.load().active_validator_count > 0);

    let h = core.handle.clone();
    let blocker = tokio::spawn(async move {
        h.block_for(Duration::from_secs(2)).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let start = std::time::Instant::now();
    let ctx = epoch.load();
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(10),
        "EpochContext load took {elapsed:?}, expected < 10ms (Phase 1 §7.1)"
    );
    assert!(
        !ctx.proposer_lookahead.is_empty() || ctx.active_validator_count > 0,
        "expected published epoch context"
    );

    // ChainView producer path also stays lock-free.
    let start = std::time::Instant::now();
    let _view = build_chain_view(svc.head(), &epoch, VIEW_KIND_FULL, true);
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(10),
        "build_chain_view took {elapsed:?}"
    );

    blocker.await.unwrap();
    shutdown(core, events).await;
}

// ── GetValidatorRecords ─────────────────────────────────────────────────────

#[tokio::test]
async fn get_validator_records_ssz_and_bound() {
    let (svc, core, events) = spawn_svc();

    // Happy path: 4 indices.
    let resp = svc
        .get_validator_records(Request::new(GetValidatorRecordsRequest {
            indices: vec![0, 1, 2, 3],
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.ssz.len(), 4);
    assert_eq!(resp.slot, 8);
    // Round-trip SSZ.
    for (i, bytes) in resp.ssz.iter().enumerate() {
        let v: Validator = ssz::Decode::from_ssz_bytes(bytes).unwrap();
        assert_eq!(v.effective_balance, MAX_EFFECTIVE_BALANCE);
        let expected = active_validator(i as u64);
        assert_eq!(v.pubkey, expected.pubkey);
    }

    // Bound: 256 ok.
    let indices_256: Vec<u64> = (0..MAX_VALIDATOR_RECORDS_PER_REQUEST)
        .map(|i| i % VALIDATORS as u64)
        .collect();
    let start = std::time::Instant::now();
    let resp = svc
        .get_validator_records(Request::new(GetValidatorRecordsRequest {
            indices: indices_256,
        }))
        .await
        .unwrap()
        .into_inner();
    let elapsed = start.elapsed();
    assert_eq!(resp.ssz.len(), 256);
    // Query-path latency budget: well under a second for an in-process 256-record
    // read (Phase 1 §7.7 discipline; absolute wall-clock budget for CI).
    assert!(
        elapsed < Duration::from_millis(500),
        "256-record GetValidatorRecords took {elapsed:?}"
    );

    // 257 → INVALID_ARGUMENT, no truncated success.
    let err = svc
        .get_validator_records(Request::new(GetValidatorRecordsRequest {
            indices: (0..=MAX_VALIDATOR_RECORDS_PER_REQUEST).collect(),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    assert!(err.message().contains("256"));

    // Empty → INVALID_ARGUMENT.
    let err = svc
        .get_validator_records(Request::new(GetValidatorRecordsRequest { indices: vec![] }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);

    shutdown(core, events).await;
}

// ── PublishRequest unknown topic ────────────────────────────────────────────

#[tokio::test]
async fn publish_unknown_topic_is_structured_error() {
    let (svc, core, events) = spawn_svc();

    let err = svc
        .request_publish(PublishRequest {
            ssz: vec![0u8; 8],
            kind: ObjectKind::Block as i32,
            topic: "blob_sidecar_0".into(),
            subnet_id: 0,
        })
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    let info = error_info_from_status(&err).unwrap().unwrap();
    assert_eq!(info.reason, cc_chain::REASON_UNKNOWN_TOPIC);

    // Known topic succeeds (even with no live session).
    svc.request_publish(PublishRequest {
        ssz: vec![0u8; 8],
        kind: ObjectKind::Block as i32,
        topic: "beacon_block".into(),
        subnet_id: 0,
    })
    .unwrap();

    shutdown(core, events).await;
}

// ── ColumnSidecar: typed ingest (S2-A-05) ───────────────────────────────────

#[test]
fn column_sidecar_decode_lives_in_chain_core() {
    // S2-A-05: p2p_stream calls `decode_column_batch` / `ingest_columns`.
    // The consensus type is named in chain-core ingest, not on the stream.
    let stream_src = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../crates/chain-core/src/p2p_stream.rs"
    ));
    assert!(
        !stream_src.contains("DataColumnSidecar"),
        "p2p_stream must not name the consensus DataColumnSidecar type"
    );
    assert!(
        stream_src.contains("decode_column_batch"),
        "p2p_stream must decode via chain-core ingest"
    );
    assert!(
        stream_src.contains("ingest_columns"),
        "p2p_stream must call ArchiveWrite::ingest_columns"
    );
    assert!(
        !stream_src.contains("EventInput::data_column"),
        "column bytes must not enter the ring"
    );
    assert!(
        stream_src.contains("classify_column_ingest"),
        "p2p_stream must classify ingest errors (ADR-R-02)"
    );
    assert!(
        stream_src.contains("SeamError::Backpressure"),
        "p2p_stream must name SeamError::Backpressure (not map all errors to Ignore)"
    );
    let ingest_src = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../crates/chain-core/src/ingest.rs"
    ));
    assert!(
        ingest_src.contains("DataColumnSidecar"),
        "chain-core ingest names the sidecar to populate ColumnBatch"
    );
    // Live writer boundary is still ArchiveWrite::ingest_block.
    // S2R-A-02 names commit_import / set_head. S2R-A-05 switches this call.
    duplicate_anchor_reaches_ingest_block();
    let fanout_src = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../crates/chain-core/src/events/fanout.rs"
    ));
    assert!(
        fanout_src.contains("try_send"),
        "event fan-out stays Policy B (try_send, drop the subscriber)"
    );
}

/// A fork-choice duplicate whose body is not durable must reach the writer
/// the import path calls today: `ArchiveWrite::ingest_block`.
///
/// S2R-A-02 is the `commit_import` / `set_head` contract. S2R-A-05 is the
/// call-site switch. The recorder takes those ops only for calls this path
/// actually makes. It makes none, so those vecs stay empty. The zero-parent
/// remap (`parent_root == block_root`) stays until S2R-A-05 / S2R-A-08.
fn duplicate_anchor_reaches_ingest_block() {
    use async_trait::async_trait;
    use cc_chain::import::{ImportCounters, encode_signed_block, import_block_with_early};
    use cc_chain::residency::Residency;
    use cc_seam::{ArchiveWrite, Bytes, DurableImport, HeadChange, IngestBlock, SeamError};
    use cc_state_transition::BlockSignatureStrategy;
    use std::sync::Mutex;
    use tree_hash::TreeHash;

    struct Recording {
        blocks: Mutex<Vec<IngestBlock>>,
        imports: Mutex<Vec<DurableImport>>,
        heads: Mutex<Vec<(HeadChange, Bytes)>>,
    }

    #[async_trait]
    impl ArchiveWrite for Recording {
        async fn ingest_columns(&self, _batch: cc_seam::ColumnBatch) -> Result<(), SeamError> {
            Ok(())
        }

        async fn ingest_block(&self, block: IngestBlock) -> Result<(), SeamError> {
            self.ingest_block_blocking(block)
        }

        fn ingest_block_blocking(&self, block: IngestBlock) -> Result<(), SeamError> {
            self.blocks.lock().unwrap().push(block);
            Ok(())
        }

        async fn commit_import(&self, import: DurableImport) -> Result<(), SeamError> {
            self.imports.lock().unwrap().push(import);
            Ok(())
        }

        async fn set_head(&self, head: HeadChange, scalars: Bytes) -> Result<(), SeamError> {
            self.heads.lock().unwrap().push((head, scalars));
            Ok(())
        }
    }

    let mut state = state_with_validators(8, Slot::new(0));
    let state_root = state.canonical_root();
    let signed = cc_types::SignedBeaconBlock {
        message: BeaconBlock {
            slot: state.slot(),
            proposer_index: ValidatorIndex::new(0),
            parent_root: Root::ZERO,
            state_root,
            body: Default::default(),
        },
        signature: Default::default(),
    };
    let config = minimal_config();
    let mut store = get_forkchoice_store(
        state,
        &signed.message,
        Arc::new(AcceptEngine),
        Arc::new(HarnessAvailability),
        config.seconds_per_slot,
    )
    .unwrap();
    let root = Root::from_hash256(TreeHash::tree_hash_root(&signed.message));
    let ssz = encode_signed_block(&signed);
    let archive_impl = Arc::new(Recording {
        blocks: Mutex::new(Vec::new()),
        imports: Mutex::new(Vec::new()),
        heads: Mutex::new(Vec::new()),
    });
    let archive: cc_chain::ArchiveWriteHandle = archive_impl.clone();
    let mut registry = Registry::default();
    let metrics = ChainMetrics::register(&mut registry);
    let head = HeadSnapshotStore::new();
    let (event_tx, _event_rx) = tokio::sync::mpsc::channel(4);
    let counters = ImportCounters::default();
    let mut residency = Residency::<Minimal>::new(8, 4);
    let mut snap_seq = 0u64;

    let outcome = import_block_with_early(
        &mut store,
        &mut residency,
        &config,
        &head,
        &event_tx,
        &metrics,
        &counters,
        &mut snap_seq,
        cc_proto::chain::ImportBlockRequest {
            ssz,
            fork: 0,
            root: root.as_slice().to_vec(),
            source: 0,
        },
        BlockSignatureStrategy::NoVerification,
        None,
        None,
        None,
        None,
        None,
        None,
        Some(&archive),
    )
    .expect("duplicate anchor import");

    assert_eq!(
        outcome.response.verdict,
        cc_proto::chain::ImportBlockVerdict::Duplicate as i32
    );
    let got = archive_impl.blocks.lock().unwrap();
    assert_eq!(
        got.len(),
        1,
        "non-durable duplicate must reach the live writer"
    );
    let mut expected_root = [0u8; 32];
    expected_root.copy_from_slice(root.as_slice());
    assert_eq!(got[0].block_root, expected_root);
    // Genesis parent is the block itself at this boundary.
    assert_eq!(got[0].parent_root, expected_root);
    assert_eq!(got[0].slot, signed.message.slot.as_u64());
    drop(got);
    assert!(
        archive_impl.imports.lock().unwrap().is_empty(),
        "import still calls ingest_block, not commit_import"
    );
    assert!(
        archive_impl.heads.lock().unwrap().is_empty(),
        "import still calls ingest_block, not set_head"
    );
}
