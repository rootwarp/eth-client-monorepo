//! S2-A-14: after A-13 boot, import a block via `on_block` and persist
//! through [`ArchiveWrite::ingest_block`] (the same hook production import
//! calls). Then observe durable rows in the same redb.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use cc_beacon_inproc::{BootConfig, boot_in_process};
use cc_crypto::INFINITY_SIGNATURE;
use cc_fork_choice::{HarnessAvailability, get_forkchoice_store, on_block, on_tick};
use cc_seam::{ArchiveWrite, Bytes, IngestBlock};
use cc_state_transition::{
    BlockSignatureStrategy, ExecutionEngine, NewPayloadRequest, PayloadStatus, TransitionContext,
    get_beacon_proposer_index, get_current_epoch, get_expected_withdrawals, get_randao_mix,
    process_slots,
};
use cc_storage_core::{StorageMetrics, start_writer_from_store};
use cc_store::canonical::get_canonical;
use cc_store::get_block_by_root;
use cc_store::meta::{KEY_WRITE_CURSOR, TABLE_META, WriteCursor};
use cc_types::config::{BlobParameters, BlobSchedule, ChainConfig, PresetName};
use cc_types::containers::{BeaconBlockHeader, SyncAggregate, SyncCommittee, Validator};
use cc_types::execution::ExecutionPayload;
use cc_types::preset::{Minimal, Preset};
use cc_types::primitives::{
    BlsPublicKey, BlsSignature, Epoch, ExecutionAddress, ForkVersion, Gwei, Root, Slot,
    ValidatorIndex,
};
use cc_types::{BeaconBlock, BeaconBlockBody, BeaconState, SignedBeaconBlock};
use prometheus_client::registry::Registry;
use ssz::Encode;
use ssz_types::{FixedVector, VariableList};
use tree_hash::TreeHash;

#[derive(Debug, Default, Clone, Copy)]
struct AcceptEngine;

impl<P: Preset> ExecutionEngine<P> for AcceptEngine {
    fn verify_and_notify_new_payload(
        &self,
        _request: NewPayloadRequest<'_, P>,
    ) -> Result<PayloadStatus, cc_state_transition::EngineError> {
        Ok(PayloadStatus::Valid)
    }
}

fn unique_temp_dir(prefix: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let seq = N.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!("{prefix}-{}-{seq}-{nanos}", std::process::id()))
}

fn boot_cfg(dir: PathBuf) -> BootConfig {
    BootConfig {
        data_dir: dir,
        durability: "immediate".to_owned(),
        check_invariants: true,
        snapshot_ring: 4,
        genesis_validators_root: None,
        node_key_path: None,
    }
}

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

fn genesis_state() -> BeaconState<Minimal> {
    let mut state = BeaconState::<Minimal>::default();
    state.set_genesis_time(0);
    state.set_slot(Slot::new(0));
    for i in 0u8..3 {
        let mut raw = [0u8; 48];
        raw[0] = i.saturating_add(1);
        state
            .validators_push(Validator {
                pubkey: BlsPublicKey::from_array(raw),
                ..Validator::default()
            })
            .unwrap();
        state.balances_push(Gwei::new(32_000_000_000)).unwrap();
    }
    let committee_keys: Vec<BlsPublicKey> = (0..Minimal::SYNC_COMMITTEE_SIZE as usize)
        .map(|i| {
            let mut raw = [0u8; 48];
            raw[0] = (i % 3) as u8 + 1;
            BlsPublicKey::from_array(raw)
        })
        .collect();
    let committee = SyncCommittee {
        pubkeys: FixedVector::new(committee_keys.clone()).expect("sync committee size"),
        aggregate_pubkey: committee_keys[0],
    };
    state.set_current_sync_committee(committee.clone());
    state.set_next_sync_committee(committee);
    state
}

fn make_next_block(
    parent_state: &BeaconState<Minimal>,
    parent_root: Root,
    config: &ChainConfig,
) -> (SignedBeaconBlock<Minimal>, BeaconState<Minimal>) {
    let engine = AcceptEngine;
    let ctx = TransitionContext::new(config, &engine);
    ctx.top_up_pubkey_cache(parent_state);

    let mut st = parent_state.clone();
    let next_slot = Slot::new(st.slot().as_u64() + 1);
    let _pre_root = process_slots(&mut st, next_slot, config).expect("process_slots");
    let proposer = get_beacon_proposer_index(&st).expect("proposer");
    let (withdrawals, _) = get_expected_withdrawals(&st).expect("withdrawals");

    let epoch = get_current_epoch(&st);
    let prev_randao = get_randao_mix(&st, epoch).expect("randao");
    let timestamp = cc_state_transition::compute_time_at_slot(
        st.genesis_time(),
        next_slot,
        config.seconds_per_slot,
    );
    let parent_hash = st.latest_execution_payload_header().block_hash;

    let payload = ExecutionPayload::<Minimal> {
        parent_hash,
        prev_randao,
        timestamp,
        block_number: st.latest_execution_payload_header().block_number + 1,
        gas_limit: st.latest_execution_payload_header().gas_limit,
        withdrawals: VariableList::new(withdrawals).expect("withdrawals list"),
        ..Default::default()
    };

    let body = BeaconBlockBody::<Minimal> {
        execution_payload: payload,
        eth1_data: st.eth1_data(),
        sync_aggregate: SyncAggregate {
            sync_committee_bits: Default::default(),
            sync_committee_signature: BlsSignature::from_array(INFINITY_SIGNATURE),
        },
        ..Default::default()
    };

    let mut message = BeaconBlock {
        slot: next_slot,
        proposer_index: proposer,
        parent_root,
        state_root: Root::ZERO,
        body,
    };
    let trial_signed = SignedBeaconBlock {
        message: message.clone(),
        signature: Default::default(),
    };
    let mut trial = parent_state.clone();
    match cc_state_transition::state_transition(
        &mut trial,
        &trial_signed,
        &ctx,
        BlockSignatureStrategy::NoVerification,
    ) {
        Err(cc_state_transition::BlockError::StateRootMismatch { actual, .. }) => {
            message.state_root = actual;
        }
        Ok(()) => {
            message.state_root = trial.canonical_root();
        }
        Err(e) => panic!("state_transition for fixture: {e}"),
    }

    (
        SignedBeaconBlock {
            message,
            signature: Default::default(),
        },
        st,
    )
}

fn seam_root(root: Root) -> cc_seam::Root {
    let mut arr = [0u8; 32];
    arr.copy_from_slice(root.as_slice());
    arr
}

fn read_cursor(engine: &cc_store::engine::Engine) -> Option<WriteCursor> {
    use cc_store::SszDecode;
    let rt = engine.read().unwrap();
    rt.get(TABLE_META, KEY_WRITE_CURSOR.as_bytes())
        .unwrap()
        .map(|b| WriteCursor::from_ssz_bytes(&b).unwrap())
}

#[tokio::test]
async fn import_on_block_then_ingest_writes_durable_rows() {
    let dir = unique_temp_dir("s2-a-14-import");
    std::fs::create_dir_all(&dir).unwrap();

    let booted = boot_in_process(&boot_cfg(dir.clone())).expect("A-13 boot");
    {
        let rt = booted.store.engine().read().unwrap();
        assert!(
            get_canonical(&rt, Slot::new(1)).unwrap().is_none(),
            "fresh boot must not already have canonical[1]"
        );
    }

    let mut registry = Registry::default();
    let metrics = StorageMetrics::register(&mut registry);
    let runtime = start_writer_from_store(booted.store, metrics, false);
    let archive = runtime.archive();
    let engine = runtime.engine();

    let before_cursor = read_cursor(engine).expect("writer seeds a cursor");
    assert_eq!(before_cursor.seq, 0);

    let config = minimal_config();
    let mut state = genesis_state();
    let body = BeaconBlockBody::<Minimal>::default();
    let body_root = Root::from_hash256(TreeHash::tree_hash_root(&body));
    state.set_latest_block_header(BeaconBlockHeader {
        slot: Slot::new(0),
        proposer_index: ValidatorIndex::new(0),
        parent_root: Root::ZERO,
        state_root: Root::ZERO,
        body_root,
    });
    let state_root = state.canonical_root();
    let mut header = *state.latest_block_header();
    header.state_root = state_root;
    state.set_latest_block_header(header);
    let anchor_block = BeaconBlock {
        slot: Slot::new(0),
        proposer_index: ValidatorIndex::new(0),
        parent_root: Root::ZERO,
        state_root,
        body,
    };
    let mut fc = get_forkchoice_store(
        state.clone(),
        &anchor_block,
        Arc::new(AcceptEngine),
        Arc::new(HarnessAvailability),
        config.seconds_per_slot,
    )
    .unwrap();
    on_tick(&mut fc, config.seconds_per_slot.saturating_mul(2)).unwrap();
    let anchor_root = Root::from_hash256(TreeHash::tree_hash_root(&anchor_block));

    // Parent must be durable before the child can extend the frontier.
    let anchor_signed = SignedBeaconBlock {
        message: anchor_block,
        signature: Default::default(),
    };
    archive
        .ingest_block(IngestBlock {
            parent_root: seam_root(anchor_root),
            slot: 0,
            block_root: seam_root(anchor_root),
            ssz: Bytes::from(anchor_signed.as_ssz_bytes()),
        })
        .await
        .expect("persist genesis");

    let parent_state = fc.block_state(&anchor_root).unwrap().clone();
    let (child, _) = make_next_block(&parent_state, anchor_root, &config);
    let child_root = Root::from_hash256(TreeHash::tree_hash_root(&child.message));
    let child_ssz = child.as_ssz_bytes();

    let outcome = on_block(
        &mut fc,
        &child,
        &config,
        BlockSignatureStrategy::NoVerification,
    )
    .expect("on_block");
    assert!(
        matches!(
            outcome,
            cc_fork_choice::BlockImport::Imported(ref b) if b.root == child_root
        ),
        "real import must succeed, got {outcome:?}"
    );

    archive
        .ingest_block(IngestBlock {
            parent_root: seam_root(anchor_root),
            slot: child.message.slot.as_u64(),
            block_root: seam_root(child_root),
            ssz: Bytes::from(child_ssz.clone()),
        })
        .await
        .expect("persist imported child");

    {
        let rt = engine.read().unwrap();
        assert_eq!(
            get_canonical(&rt, child.message.slot).unwrap(),
            Some(child_root),
            "import persist must write canonical[slot] = root"
        );
        assert_eq!(
            get_block_by_root(&rt, &child_root).unwrap().as_deref(),
            Some(child_ssz.as_slice()),
            "import persist must write the block body"
        );
    }
    let after = read_cursor(engine).expect("cursor after import");
    assert!(
        after.seq > before_cursor.seq,
        "WriteCursor must advance: before={} after={}",
        before_cursor.seq,
        after.seq
    );
    assert_eq!(after.slot, child.message.slot);
    assert_eq!(after.root, child_root);

    drop(archive);
    runtime.shutdown();
    drop(runtime);
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // The host must not reopen a populated store on the legacy digest constant.
    // The bytes are still in the file; a direct engine open proves that.
    let err = boot_in_process(&boot_cfg(dir.clone()))
        .expect_err("populated store must not open on the legacy constant");
    let msg = err.to_string();
    assert!(
        msg.contains("genesis_validators_root") && msg.contains("not Root::ZERO"),
        "absent GVR must be refused, not substituted: {msg}"
    );
    let engine = cc_store::engine::Engine::open(&dir, cc_store::engine::EngineOptions::default())
        .expect("committed rows stay readable");
    {
        let rt = engine.read().unwrap();
        assert_eq!(
            get_canonical(&rt, child.message.slot).unwrap(),
            Some(child_root)
        );
        assert_eq!(
            get_block_by_root(&rt, &child_root).unwrap().as_deref(),
            Some(child_ssz.as_slice())
        );
    }
    let durable = read_cursor(&engine).expect("cursor survives reopen");
    assert_eq!(durable.seq, after.seq);
    assert_eq!(durable.root, child_root);

    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}

/// H3: import A then B (B is head); re-import A keeps durable head at B.
#[tokio::test]
async fn reimport_ancestor_keeps_durable_head() {
    let dir = unique_temp_dir("s2-a-14-h3");
    std::fs::create_dir_all(&dir).unwrap();

    let booted = boot_in_process(&boot_cfg(dir.clone())).expect("A-13 boot");
    let mut registry = Registry::default();
    let metrics = StorageMetrics::register(&mut registry);
    let runtime = start_writer_from_store(booted.store, metrics, false);
    let archive = runtime.archive();
    let engine = runtime.engine();

    let config = minimal_config();
    let mut state = genesis_state();
    let body = BeaconBlockBody::<Minimal>::default();
    let body_root = Root::from_hash256(TreeHash::tree_hash_root(&body));
    state.set_latest_block_header(BeaconBlockHeader {
        slot: Slot::new(0),
        proposer_index: ValidatorIndex::new(0),
        parent_root: Root::ZERO,
        state_root: Root::ZERO,
        body_root,
    });
    let state_root = state.canonical_root();
    let mut header = *state.latest_block_header();
    header.state_root = state_root;
    state.set_latest_block_header(header);
    let anchor_block = BeaconBlock {
        slot: Slot::new(0),
        proposer_index: ValidatorIndex::new(0),
        parent_root: Root::ZERO,
        state_root,
        body,
    };
    let mut fc = get_forkchoice_store(
        state.clone(),
        &anchor_block,
        Arc::new(AcceptEngine),
        Arc::new(HarnessAvailability),
        config.seconds_per_slot,
    )
    .unwrap();
    on_tick(&mut fc, config.seconds_per_slot.saturating_mul(2)).unwrap();
    let genesis_root = Root::from_hash256(TreeHash::tree_hash_root(&anchor_block));
    let genesis_signed = SignedBeaconBlock {
        message: anchor_block,
        signature: Default::default(),
    };
    archive
        .ingest_block(IngestBlock {
            parent_root: seam_root(genesis_root),
            slot: 0,
            block_root: seam_root(genesis_root),
            ssz: Bytes::from(genesis_signed.as_ssz_bytes()),
        })
        .await
        .expect("persist genesis");

    let parent_state = fc.block_state(&genesis_root).unwrap().clone();
    let (block_a, _) = make_next_block(&parent_state, genesis_root, &config);
    let root_a = Root::from_hash256(TreeHash::tree_hash_root(&block_a.message));
    let ssz_a = block_a.as_ssz_bytes();
    on_block(
        &mut fc,
        &block_a,
        &config,
        BlockSignatureStrategy::NoVerification,
    )
    .expect("on_block A");
    archive
        .ingest_block(IngestBlock {
            parent_root: seam_root(genesis_root),
            slot: block_a.message.slot.as_u64(),
            block_root: seam_root(root_a),
            ssz: Bytes::from(ssz_a.clone()),
        })
        .await
        .expect("persist A");

    on_tick(&mut fc, config.seconds_per_slot.saturating_mul(3)).unwrap();
    let state_a = fc.block_state(&root_a).unwrap().clone();
    let (block_b, _) = make_next_block(&state_a, root_a, &config);
    let root_b = Root::from_hash256(TreeHash::tree_hash_root(&block_b.message));
    let ssz_b = block_b.as_ssz_bytes();
    on_block(
        &mut fc,
        &block_b,
        &config,
        BlockSignatureStrategy::NoVerification,
    )
    .expect("on_block B");
    archive
        .ingest_block(IngestBlock {
            parent_root: seam_root(root_a),
            slot: block_b.message.slot.as_u64(),
            block_root: seam_root(root_b),
            ssz: Bytes::from(ssz_b.clone()),
        })
        .await
        .expect("persist B");

    archive
        .ingest_block(IngestBlock {
            parent_root: seam_root(genesis_root),
            slot: block_a.message.slot.as_u64(),
            block_root: seam_root(root_a),
            ssz: Bytes::from(ssz_a.clone()),
        })
        .await
        .expect("re-import A");

    {
        let rt = engine.read().unwrap();
        assert_eq!(
            get_canonical(&rt, block_a.message.slot).unwrap(),
            Some(root_a),
            "A's canonical row must remain"
        );
        assert_eq!(
            get_canonical(&rt, block_b.message.slot).unwrap(),
            Some(root_b),
            "durable head must stay B after re-import of A"
        );
        assert_eq!(
            get_block_by_root(&rt, &root_a).unwrap().as_deref(),
            Some(ssz_a.as_slice())
        );
        assert_eq!(
            get_block_by_root(&rt, &root_b).unwrap().as_deref(),
            Some(ssz_b.as_slice())
        );
    }

    drop(archive);
    runtime.shutdown();
    drop(runtime);
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let _ = std::fs::remove_dir_all(&dir);
}
