//! `cc-chain` library surface — thin host over [`cc_chain_core`] (S2-A-03).
//!
//! - **CC-18c**: resumable event bus (ring, cursor, fan-out)
//! - **CC-1C**: timing metrics (budgeted histograms, gauges, counters)
//! - **CC-18b**: dedicated core thread, import path, `ArcSwap<HeadSnapshot>`, residency
//! - **CC-1E**: batched `ApplyAttestations` (weight observed through `GetHead`)
//! - **CC-19a**: checkpoint fetch, provider fallback, verification
//! - **CC-1G**: `/eth/v1/config/spec` as a `BLOB_SCHEDULE` source
//! - **CC-27a**: `P2pStream` server, `ChainView` producer, `ArcSwap<EpochContext>`,
//!   `GetValidatorRecords`
//! - **CC-27c**: gossip-verify fast path — block acceptance before state transition
//! - **CC-24d**: DA seam substitution — [`da`] (`pending_da`) +
//!   [`cc_fork_choice::PeerDasAvailability`]; Phase-1 optimistic DA stub deleted
//! - **CC-36a**: third deferral outcome — [`pending_engine`] (64 / 8 slots),
//!   separate from `pending_da`
//! - **CC-38a**: block-branch fast-path trigger in [`da`] (template-sized
//!   only; no cell payload through chain)
//! - **S1-A-06**: E3 is a direct [`engine::DirectEngine`] call (no gRPC bridge)
//!
//! The binary (`main.rs`) is a thin shim that calls [`run`]. Bind first, then
//! fall back to checkpoint bootstrap when `checkpoint_providers` is configured
//! (CC-19 demoted; in-process seed lives on `bin/beacon-core`). Self health
//! SERVING while aggregate `""` stays NOT_SERVING until the core is installed.
//! Without providers the core is absent and fork-choice RPCs return
//! `NOT_BOOTSTRAPPED`. Tests construct a store and spawn the core via
//! [`core::spawn_core_thread`].
//!
//! `checkpoint_sync` stays in this crate (HTTP grandfather). E4 restore is
//! deleted (S2-J-02).

#![allow(missing_docs)]

pub use cc_chain_core::apply_attestations;
pub use cc_chain_core::core;
pub use cc_chain_core::da;
pub use cc_chain_core::engine;
pub use cc_chain_core::epoch_context;
pub use cc_chain_core::events;
pub use cc_chain_core::fcu_driver;
pub use cc_chain_core::head;
pub use cc_chain_core::import;
pub use cc_chain_core::invalidation;
pub use cc_chain_core::liveness;
pub use cc_chain_core::metrics;
pub use cc_chain_core::p2p_stream;
pub use cc_chain_core::pending_engine;
pub use cc_chain_core::residency;
pub use cc_chain_core::seed;
pub use cc_chain_core::service;
pub use cc_chain_core::tick;

pub mod checkpoint_sync;
pub mod run;

pub use apply_attestations::MAX_APPLY_ATTESTATIONS;
pub use checkpoint_sync::{
    AnchorKind, AnchorSource, BlobScheduleFromSpecError, BootstrapSummary,
    CheckpointBootstrapConfig, CheckpointClient, CheckpointError, CheckpointProvider,
    FetchedCheckpoint, GenesisInfo, InMemoryCheckpointProvider, MAX_BLOCK_BYTES, MAX_JSON_BYTES,
    MAX_STATE_BYTES, NETWORK_RETRIES, PROVIDER_CONNECT_TIMEOUT, PROVIDER_TOTAL_TIMEOUT,
    REQUIRED_CONSENSUS_VERSION, TRIPLE_ATTEMPTS, VerifiedAnchor, blob_schedule_from_spec,
    blob_schedule_from_spec_map, bootstrap_core_from_providers,
    bootstrap_core_from_providers_with_epoch, cross_check_spec, fetch_checkpoint,
    fetch_checkpoint_with_provider, parse_optional_root, spawn_core_from_checkpoint,
    spawn_core_from_checkpoint_with_epoch, validate_provider_base, verify_anchor,
    verify_checkpoint, warm_canonical_root,
};
pub use core::{
    AttestationEnqueue, AttestationSender, AttestationWork, CoreCommand, CoreConfig, CoreHandle,
    CoreThread, IMPORT_SEND_TIMEOUT, ImportWork, MAX_VALIDATOR_PUBKEYS_PER_REQUEST,
    MAX_VALIDATOR_RECORDS_PER_REQUEST, QueryP0Work, QueryP1Work, QueryReply, QueryRequest,
    SHUTDOWN_JOIN_TIMEOUT, WakingSender, spawn_core_thread, spawn_core_thread_with_epoch,
};
pub use da::{
    BlockBranchTrigger, CELL_PAYLOAD_SOFT_MIN, DEFAULT_DA_PENDING_TIMEOUT_SLOTS,
    DEFAULT_RECOVERY_MAX_ATTEMPTS, DEFAULT_RECOVERY_MAX_PEERS, DEFAULT_SECONDS_PER_SLOT,
    OutboundTriggerBytes, PENDING_DA_BOUND, PendingDa, PendingDaEntry, RESP_TIMEOUT_SECS,
    TEMPLATE_WIRE_SOFT_MAX, TTFB_TIMEOUT_SECS, assert_timeout_outlasts_recovery,
    block_branch_trigger_from_signed, chain_pending_timeout_secs, default_timeout_ordering_ok,
    kzg_commitment_to_versioned_hash, recovery_ladder_worst_case_secs,
    versioned_hashes_from_commitments,
};
pub use engine::{
    DEFAULT_ENGINE_FETCH_BLOBS_TIMEOUT, DEFAULT_ENGINE_FORKCHOICE_UPDATED_TIMEOUT,
    DEFAULT_ENGINE_GET_STATE_TIMEOUT, DEFAULT_ENGINE_NEW_PAYLOAD_TIMEOUT, DirectEngine,
    SharedEngine,
};
pub use epoch_context::{EpochContext, EpochContextStore};
pub use events::{
    ChainReorgPayload, DEFAULT_RING_BYTES, DEFAULT_RING_CAPACITY,
    DEFAULT_SUBSCRIBER_QUEUE_CAPACITY, ERROR_DOMAIN, EventInput, EventSubscription, EventsConfig,
    EventsHandle, FinalizedCheckpointPayload, MAX_EVENT_PAYLOAD_BYTES, Occupancy,
    REASON_CURSOR_TOO_OLD, REASON_CURSOR_UNKNOWN_SESSION, SESSION_ID_METADATA_KEY,
};
pub use fcu_driver::{
    FcuBuildError, FcuDriver, FcuSink, FcuSkip, ForkchoiceState, RecordingFcuSink,
    build_forkchoice_state, safe_is_ancestor_of_head,
};
pub use head::{HeadSnapshot, HeadSnapshotStore};
pub use import::{
    BLOCK_PAYLOAD_VERDICT_DEFERRED_DA, BLOCK_PAYLOAD_VERDICT_IMPORTED, BlockImportedPayloadVerdict,
    FORK_CHOICE_SCALARS_SSZ_LEN, ForkChoiceScalarsPayload, ImportCounters, ImportOutcome,
    ImportReason, block_imported_payload, common_ancestor_slot, decode_signed_block,
    encode_signed_block, fork_choice_scalars_ssz, import_block_with_early, late_import_flags,
    on_block_error_gossip_class, parse_root, split_block_imported_payload,
};
pub use invalidation::{ExitFn, handle_justified_checkpoint_invalidated, process_exit};
pub use liveness::{
    CONSECUTIVE_MISS_THRESHOLD, CONSECUTIVE_SUCCESS_THRESHOLD, ConsecutiveMissPolicy, CoreLiveness,
    DEFAULT_ATTESTATION_DUE_BPS, DEFAULT_SLOT_DURATION_MS, LivenessError, LivenessSink,
    SAMPLES_PER_SLOT, default_liveness_deadline, liveness_deadline, probe_core_liveness,
    run_core_liveness_loop, sample_interval, soft_deadline_ms,
};
pub use metrics::{
    AUX_DURATION_BUCKETS, BLOCK_BUDGET_SECS, BUFFER_RING, BUFFER_SUBSCRIBER, BootstrapResult,
    BudgetOp, CI_BLOCK_CEILING_SECS, CI_EPOCH_CEILING_SECS, ChainMetrics, EPOCH_BUDGET_SECS,
    HashPath, ImportResult, ImportStage, OptimisticDirection, PROCESS_BLOCK_BUCKETS,
    PROCESS_EPOCH_BUCKETS,
};
pub use p2p_stream::{
    MAX_P2P_STREAM_SESSIONS, P2pStreamDeps, REASON_STREAM_SESSION_LIMIT, REASON_UNKNOWN_TOPIC,
    STREAM_OUTBOUND_CAPACITY, VIEW_KIND_EPOCH_TICK, VIEW_KIND_FULL, VIEW_KIND_HEAD_CHANGE,
    VIEW_KIND_SLOT_TICK, ViewTick, build_chain_view, validate_publish_topic,
};
pub use pending_engine::{
    DEFAULT_ENGINE_PENDING_TIMEOUT_SLOTS, PENDING_ENGINE_BOUND, PendingEngine, PendingEngineEntry,
};
pub use residency::{
    BodyRingEntry, DEFAULT_BODY_RING_CAPACITY, DEFAULT_MAX_RESIDENT_STATES, Residency,
    ResidencyError, ResidentRole, StateProvider,
};
pub use run::run;
pub use seed::{
    DurableSeed, SeedApplyInput, SeedApplyResult, SeedBlock, SeedDaStatus, SeedInstall,
    reset_seed_da_gate_invocations, seed_da_gate_invocations, seed_from_durable,
    spawn_core_from_seed,
};
pub use service::{
    ChainServiceImpl, REASON_BELOW_FINALIZED_RETENTION, REASON_NOT_BOOTSTRAPPED,
    status_below_finalized,
};
pub use tick::{DEFAULT_MAXIMUM_GOSSIP_CLOCK_DISPARITY, GossipClock};

/// p2p → chain handle. Named here so this edge is not a transport type.
pub type ChainIngressHandle = std::sync::Arc<dyn cc_seam::ChainIngress>;
/// chain → p2p handle. Named here so this edge is not a transport type.
pub type P2pEgressHandle = std::sync::Arc<dyn cc_seam::P2pEgress>;
/// Archive ingest handle. Named in `cc-chain-core`; never a storage type.
pub use cc_chain_core::ArchiveWriteHandle;
pub use cc_chain_core::{decode_column_batch, ingest_column_ssz};
