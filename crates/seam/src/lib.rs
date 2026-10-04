//! Typed handles and overflow contracts for internal service edges
//! (`[ARCH]` §2.1).
//!
//! Methods mirror today's proto `oneof` arms (gossip / DA / column sidecar
//! / publish / view) without taking `cc-proto` types. [`ArchiveWrite`] is
//! the S2 archive ingest handle: a typed [`ColumnBatch`], not a proto arm.
//! `commit_anchor`, `commit_import`, `set_head`, and `commit_snapshot` are
//! signatures only (ADR-R-08). Their defaults do not write.
//!
//! [`InProcess`] is the Single Hull transport: its lanes **are** the live
//! import / column / publish queues. Do not wrap them in front of the
//! scheduler import lane, the event ring, or `publish_fwd` — that is a
//! second bound (`[ARCH]` §2.1). [`Ipc`] is the p2p-side tonic client;
//! [`IpcEgress`] is mailbox-only. Both stay buildable permanently
//! (`[ARCH]` §9.2).

#[cfg(all(test, feature = "ipc"))]
mod conformance;
mod event_payloads;
mod in_process;
#[cfg(feature = "ipc")]
mod ipc;

pub use event_payloads::{
    BlockImportedPayload, BlockImportedVerdict, ChainReorgPayload, FinalizedCheckpointPayload,
    HeadPayload,
};
pub use in_process::{
    DEFAULT_RING_CAPACITY, IMPORT_LANE_DEPTH, IMPORT_SEND_TIMEOUT, ImportMsg, InProcess,
    InProcessMailbox, MAX_EVENT_PAYLOAD_BYTES, PUBLISH_BOUND,
};
#[cfg(feature = "ipc")]
pub use ipc::{
    BACKOFF_CAP, BACKOFF_INITIAL, CHAIN_OUT_BOUND, DEFAULT_VERDICT_TIMEOUT, Ipc, IpcConfig,
    IpcEgress, IpcMailbox, IpcUpward, REASON_NOT_BOOTSTRAPPED, full_jitter, map_tonic_status,
    new_session_id, next_backoff, wait_reconnect_backoff,
};

use async_trait::async_trait;

/// Every internal edge fails in exactly these ways. Adding a variant is a
/// contract change and needs an ADR.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SeamError {
    /// The receiver's bounded queue was full for the whole `send_timeout`.
    /// MUST map to gRPC `RESOURCE_EXHAUSTED` and back. Caller-visible; the
    /// caller is expected to shed, retry or descore — never to ignore.
    #[error("seam queue full after {waited_ms}ms (bound {bound})")]
    Backpressure { bound: usize, waited_ms: u64 },
    /// Receiver is gone (process exited, task aborted, stream torn down).
    #[error("seam peer unavailable: {0}")]
    Unavailable(String),
    /// The request was structurally rejected before any work was done.
    #[error("seam invalid argument: {0}")]
    InvalidArgument(String),
    /// Precondition not met (e.g. [`FailedPreconditionReason::NotBootstrapped`]).
    #[error("seam failed precondition: {reason}")]
    FailedPrecondition { reason: FailedPreconditionReason },
}

/// Typed `FailedPrecondition` discriminant. Adding a variant is a contract change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FailedPreconditionReason {
    /// Called before checkpoint bootstrap completed.
    NotBootstrapped,
    /// `commit_anchor` on a store that is not `Uninitialized`.
    StoreNotUninitialized,
    /// Import or head change while the store is `Incomplete`.
    StoreIncomplete,
    /// `commit_import` whose parent body is not durable.
    ParentNotDurable,
    /// Store is `Complete` and the head root is not durable.
    ///
    /// `set_head`: `head_root` is not already durable. `commit_import`: `head`
    /// names a root other than the body this transaction writes. A store that
    /// is not `Complete` is [`Self::StoreIncomplete`], not this reason.
    HeadNotDurable,
}

impl FailedPreconditionReason {
    /// Stable token for logs / gRPC `ErrorInfo.reason`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotBootstrapped => "NOT_BOOTSTRAPPED",
            Self::StoreNotUninitialized => "STORE_NOT_UNINITIALIZED",
            Self::StoreIncomplete => "STORE_INCOMPLETE",
            Self::ParentNotDurable => "PARENT_NOT_DURABLE",
            Self::HeadNotDurable => "HEAD_NOT_DURABLE",
        }
    }
}

impl std::fmt::Display for FailedPreconditionReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 32-byte consensus identity. Not `cc-types::Root` and not a proto `bytes`.
pub type Root = [u8; 32];

/// Column identifier. Seam-owned; not `cc-types::ColumnIndex`.
pub type ColumnIndex = u64;

/// Opaque SSZ payload. Seam-owned; not a proto `bytes`.
pub type Bytes = bytes::Bytes;

/// Gossip object family. Seam-owned; not an `eth.p2p.v1.ObjectKind` integer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObjectKind {
    Block,
    Attestation,
    Aggregate,
    SyncCommittee,
    SyncContribution,
    VoluntaryExit,
    ProposerSlashing,
    AttesterSlashing,
    BlsToExecutionChange,
    ColumnSidecar,
}

/// GossipSub acceptance class. Seam-owned; not a proto discriminant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Acceptance {
    Accept,
    Reject,
    Ignore,
}

/// Gossip / import reason. Seam-owned; [`Self::Internal`] must never become
/// a network-facing penalty.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Reason {
    Valid,
    Invalid,
    InvalidSignature,
    NotDescendedFromFinalized,
    Duplicate,
    UnknownParent,
    FutureSlot,
    DeferredDa,
    AlreadyKnown,
    /// Our bug / shed — never a peer penalty.
    Internal,
}

/// Import-path outcome. Seam-owned; not a proto discriminant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ImportResult {
    Imported,
    Duplicate,
    DeferredDa,
    UnknownParent,
    Invalid,
    None,
}

/// `P2pToChain.object` — SSZ bytes + identity.
///
/// Provenance is **stamped by the impl**, not taken from the caller: there
/// is no `source` / `API` / `trusted_local` field to smuggle privilege.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GossipObject {
    pub ssz: Vec<u8>,
    pub fork: u32,
    pub root: Root,
    pub kind: ObjectKind,
    pub subnet_id: u64,
}

/// `P2pToChain.column` — column sidecar SSZ. Relayed, not decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSidecar {
    pub ssz: Vec<u8>,
    pub fork: u32,
    pub root: Root,
    pub column_index: u64,
    pub subnet_id: u64,
}

/// `ChainToP2p.publish` — outward gossip.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishRequest {
    pub ssz: Vec<u8>,
    pub kind: ObjectKind,
    pub topic: String,
    pub subnet_id: u64,
}

/// `ChainToP2p.view` — chain-owned consensus view (ADR-P2-05).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ChainView {
    pub slot: u64,
    pub epoch: u64,
    pub head_root: Root,
    pub head_slot: u64,
    pub finalized_root: Root,
    pub finalized_epoch: u64,
    pub justified_root: Root,
    pub justified_epoch: u64,
    pub genesis_time: u64,
    pub genesis_validators_root: Root,
    pub proposer_lookahead: Vec<u64>,
    pub proposer_pubkeys: Vec<Vec<u8>>,
    pub active_validator_count: u64,
    pub view_kind: u64,
}

/// Exactly-one-verdict reply to [`GossipObject`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verdict {
    pub correlation_id: Root,
    pub acceptance: Acceptance,
    pub reason: Reason,
    pub import: ImportResult,
}

/// Caller-visible resolution of [`ChainIngress::submit_gossip`].
///
/// A [`Verdict`] is the only success. Local IGNORE / timeout after 2 s is
/// **not** representable: stall-then-shed stays inside the Ipc impl.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerdictResolution {
    pub verdict: Verdict,
}

/// Outcome of [`P2pEgress::publish`]. Drop is a value, not a log line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Published {
    /// Enqueued on the existing publish queue (`PUBLISH_BOUND`).
    Queued,
    /// Queue was full — lossy by design (ADR-R-02).
    Dropped,
}

/// p2p → core. Gossip / DA / column-sidecar ingress (E1+E2).
///
/// # Overflow contract — `submit_gossip` / `notify_data_available`
///
/// Both MUST block up to `IMPORT_SEND_TIMEOUT` (2 s)
/// (`services/chain/src/core.rs`) and then return [`SeamError::Backpressure`].
/// They MUST NOT silently drop and MUST NOT return a success that means
/// IGNORE. Implementations that cannot block (e.g. a `try_send` fast path)
/// MUST still surface Backpressure.
///
/// The receiving bound is the live import lane:
/// `IMPORT_LANE_DEPTH = 64` (`crates/scheduler/src/config.rs`) and
/// `IMPORT_SEND_TIMEOUT = 2s` (`services/chain/src/core.rs`). This handle
/// does not introduce a second bound.
///
/// `notify_data_available` is a sampling-tracker signal, not a proof. Same
/// lane and deadline as `submit_gossip`; overflow MUST be
/// [`SeamError::Backpressure`], never swallowed as `Ok(())`.
///
/// # Overflow contract — `submit_column_sidecar`
///
/// Columns MUST NOT ride `IMPORT_LANE_DEPTH` (that HOL-blocks gossip).
/// The live path is the events producer `event_tx` (`p2p_stream.rs`):
/// depth `ring_capacity`, default `DEFAULT_RING_CAPACITY = 4096`
/// (`services/chain/src/events/mod.rs`); admit with `send().await` (no
/// import-lane deadline; F1: never silent `try_send`).
///
/// Oversize (`MAX_EVENT_PAYLOAD_BYTES` = 10 MiB, SEC-44a-2) MUST be
/// [`SeamError::InvalidArgument`]. A missing or closed bus MUST be
/// [`SeamError::Unavailable`]. Neither may be `Ok(())` after a drop.
#[async_trait]
pub trait ChainIngress: Send + Sync + 'static {
    async fn submit_gossip(&self, obj: GossipObject) -> Result<VerdictResolution, SeamError>;
    async fn notify_data_available(&self, root: Root, slot: u64) -> Result<(), SeamError>;
    async fn submit_column_sidecar(&self, sidecar: ColumnSidecar) -> Result<(), SeamError>;
}

/// core → p2p. Publish / view egress (E1).
///
/// # Overflow contract
///
/// `publish` is **lossy by design** and returns `Ok(Published::Dropped)`
/// rather than an error when the **publish** queue is full (policy C,
/// ADR-R-02). The bound this handle preserves is `PUBLISH_BOUND = 256`
/// (`services/p2p/src/channels.rs`), oldest-drop at
/// `services/p2p/src/chain_stream/publish.rs`. A full publish queue MUST
/// be `Ok(Published::Dropped)`, never [`SeamError::Backpressure`].
///
/// A later swarm hop may still drop on `CMD_BOUND` (`service.rs`:
/// "cmd queue full; dropping local publish"). That is not this handle's
/// overflow and is not a second bound here.
///
/// `update_view` is an `ArcSwap` store — never blocks, never fails. The
/// view is chain-owned (ADR-P2-05); this is not a p2p self-write.
#[async_trait]
pub trait P2pEgress: Send + Sync + 'static {
    async fn publish(&self, req: PublishRequest) -> Result<Published, SeamError>;
    fn update_view(&self, view: ChainView);
}

/// Signed-block ingest unit for the archive writer (S2-A-14).
///
/// Same continuity bind as [`ColumnBatch`]: a batch may only extend the
/// durable frontier. The old genesis rule (`parent_root == block_root`) is
/// struck: genesis is an anchor ([`TrustedAnchor`]). Callers that still
/// remap a zero parent have not moved off this type yet.
///
/// Removed in S2R-A-12, with the last caller. Replaced by [`DurableImport`]
/// and [`ArchiveWrite::commit_import`], not widened. `import.rs` calls this
/// until S2R-A-05; `import_durable.rs` until S2R-A-12. Do not add callers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestBlock {
    /// Parent the block attaches to.
    pub parent_root: Root,
    pub slot: u64,
    pub block_root: Root,
    pub ssz: Bytes,
}

/// Typed column ingest unit. **`index` is a field, not a byte-offset guess**
/// (`[ARCH]` §4.3 / S2-A-04).
///
/// Head is `(parent_root, slot)` — the block the batch's columns attach to
/// (S2-A-06). A batch may only extend the durable frontier, never jump it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnBatch {
    /// Parent the columns attach to. Required; there is no empty-progress bypass.
    pub parent_root: Root,
    pub slot: u64,
    pub block_root: Root,
    pub index: ColumnIndex,
    pub ssz: Bytes,
}

/// Data-availability verdict stored with a durable body.
///
/// No `None`: absent DA is not a legal durable state (ADR-R-08). `Deferred`
/// is durable and replay re-parks the block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DaVerdict {
    Available,
    Deferred,
}

/// Why the selected head changed. Canonical rows follow this, not a flag on
/// the body.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HeadCause {
    Import,
    Attestation,
    EngineInvalidation,
}

/// A change of selected head.
///
/// On `set_head`, a store that is not `Complete` is `STORE_INCOMPLETE`.
/// `HEAD_NOT_DURABLE` applies only when it is `Complete` and `head_root` is
/// not already durable. On `commit_import`, `head_root` must be the body
/// that call writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadChange {
    pub head_root: Root,
    pub head_slot: u64,
    pub cause: HeadCause,
}

/// One imported body. Canonical rewrite happens iff [`Self::head`] is
/// `Some` and `head.head_root` equals [`Self::block_root`] — this
/// transaction writes that body. Any other head root is `HEAD_NOT_DURABLE`.
/// Do not treat the inserted body as already durable. There is no
/// `update_canonical` field.
///
/// `ssz` is the opaque arrival bytes. `scalars` is fork-choice scalars SSZ,
/// chain-owned and opaque to storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableImport {
    pub block_root: Root,
    pub parent_root: Root,
    pub slot: u64,
    pub state_root: Root,
    pub ssz: Bytes,
    pub da: DaVerdict,
    pub scalars: Bytes,
    pub head: Option<HeadChange>,
}

/// Anchor verified in chain-core. Storage writes opaque bytes and metadata.
/// Legal only on an uninitialized store, once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrustedAnchor {
    pub block_root: Root,
    pub parent_root: Root,
    pub slot: u64,
    pub state_root: Root,
    pub block_ssz: Bytes,
    pub state_ssz: Bytes,
    pub scalars: Bytes,
    pub da: DaVerdict,
}

/// Epoch-boundary state snapshot. The ring entry is durable only when its
/// completion marker commits; this signature does not write that marker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub slot: u64,
    pub state_root: Root,
    pub state_ssz: Bytes,
}

/// chain-core → storage-core. Typed column ingest (E7 replacement, S2-A-04).
///
/// `chain-core` holds `Arc<dyn ArchiveWrite>`. It MUST NOT name a storage
/// type (`cc-store`, `cc-storage-core`, writer handle).
///
/// # Overflow contract — `ingest_columns`
///
/// Policy **A** (ADR-P4-04 ✓ `crates/storage-core/src/writer.rs:1` /
/// `services/storage/src/writer.rs:1`): overflow is the writer mailbox's
/// three-class priority admission, surfaced as [`SeamError::Backpressure`]
/// to the import path.
///
/// The receiving bound is the live P0 class: `WRITER_P0_BOUND = 32`, on
/// full **block** (never drop). P1 is bound 64 / block. P2 is bound 256 /
/// drop-newest. `ingest_columns` is P0. [`Self::commit_snapshot`] rides that
/// same P2 class with the ADR-R-08 exemption: a dropped chunk is not a
/// durable snapshot. This handle does not introduce a second bound and MUST
/// NOT re-derive those literals as a new mailbox.
///
/// A full P0 mailbox MUST return [`SeamError::Backpressure`]. It MUST NOT
/// silently drop, MUST NOT terminate the caller (policy B), and MUST NOT
/// return `Ok` after a shed (policy C). Implementations that cannot block
/// MUST still surface Backpressure.
///
/// S2-A-05: `chain-core` calls this with a typed [`ColumnBatch`]. The
/// impl is the live P0 mailbox. S2-A-06: the writer rejects a batch
/// unless its head `(parent_root, slot)` is already durable or is the
/// first row of the same batch. A batch may only extend the durable
/// frontier, never jump it.
#[async_trait]
pub trait ArchiveWrite: Send + Sync + 'static {
    async fn ingest_columns(&self, batch: ColumnBatch) -> Result<(), SeamError>;

    /// Persist an imported signed block through the live P0 writer.
    ///
    /// Default is a no-op so column-only test doubles stay valid. Production
    /// [`ArchiveWriter`] submits a `CommitUnit` (mailbox + one batch).
    ///
    /// Removed in S2R-A-12. Do not add callers; new work uses
    /// [`Self::commit_import`].
    async fn ingest_block(&self, block: IngestBlock) -> Result<(), SeamError> {
        let _ = block;
        Ok(())
    }

    /// Sync ingest for the core OS thread (not a tokio worker).
    ///
    /// Removed in S2R-A-12, with [`Self::ingest_block`].
    fn ingest_block_blocking(&self, block: IngestBlock) -> Result<(), SeamError> {
        let _ = block;
        Ok(())
    }

    /// Whether this block's durable rows (body) are already in the store.
    ///
    /// Default is `false` so test doubles still persist on a DUPLICATE retry
    /// (M2). Production [`ArchiveWriter`] returns the live body presence.
    /// When this is `true`, the live writer must not set `update_canonical:
    /// true` on the commit unit — that rewinds the durable tip (H3). The
    /// seam has no `update_canonical` argument. [`Self::commit_import`]
    /// carries `head: None` unless `head.head_root` is the body that call
    /// writes.
    fn block_is_durable(&self, root: Root) -> Result<bool, SeamError> {
        let _ = root;
        Ok(false)
    }

    /// Signature only. The default performs no store write and does not
    /// apply the precondition below.
    ///
    /// Legal when the store is `Uninitialized`, once. A second call is
    /// [`FailedPreconditionReason::StoreNotUninitialized`]. This is the only
    /// op that may write a body whose parent is not durable.
    async fn commit_anchor(&self, anchor: TrustedAnchor) -> Result<(), SeamError> {
        let _ = anchor;
        Ok(())
    }

    /// Signature only. The default performs no store write and does not
    /// apply the preconditions below.
    ///
    /// Legal when the store is `Complete`
    /// ([`FailedPreconditionReason::StoreIncomplete`] otherwise) and
    /// `parent_root` is durable
    /// ([`FailedPreconditionReason::ParentNotDurable`] otherwise).
    /// `head` is `None`, or `head.head_root` equals `block_root` because
    /// this transaction writes that body. Any other head root is
    /// [`FailedPreconditionReason::HeadNotDurable`], the same token as
    /// [`Self::set_head`]. Do not apply "already durable" to the body this
    /// call inserts. One transaction writes the body, reverse index,
    /// `state_roots[slot]`, `da_status`, scalars, and the cursor. The
    /// canonical table changes only when that head rule holds. There is no
    /// `update_canonical` field.
    ///
    /// Production `ArchiveWriter::commit_import` waits at most
    /// `2 * seconds_per_slot` on the P0 writer. The deadline is on this
    /// async call only. The core thread still persists through the
    /// blocking path with no deadline. Probe coverage of an archive stall
    /// starts when that thread blocks in `commit_import` (S2R-A-05). Past
    /// the deadline the process aborts with a named reason. Aborting is
    /// not backpressure (ADR-R-08). This default still performs no store
    /// write.
    async fn commit_import(&self, import: DurableImport) -> Result<(), SeamError> {
        let _ = import;
        Ok(())
    }

    /// Signature only. The default performs no store write and does not
    /// apply the precondition below.
    ///
    /// [`FailedPreconditionReason::StoreIncomplete`] when the store is not
    /// `Complete`. [`FailedPreconditionReason::HeadNotDurable`] only when
    /// the store is `Complete` and `head.head_root` is not already durable.
    /// Those are different reasons. Durability of that root is not relaxed,
    /// and this call does not insert the body. Writes the canonical rewrite
    /// from that root, scalars, and the cursor. `scalars` is fork-choice
    /// scalars SSZ, opaque to storage.
    async fn set_head(&self, head: HeadChange, scalars: Bytes) -> Result<(), SeamError> {
        let _ = (head, scalars);
        Ok(())
    }

    /// Signature only. The default performs no store write.
    ///
    /// Legal when the store is `Complete`. One call per epoch boundary.
    ///
    /// # Overflow contract
    ///
    /// Writer class **P2**, bound **256**, **drop-newest** (ADR-P4-04).
    /// This handle does not re-derive that bound.
    ///
    /// **Exemption (ADR-R-08):** a dropped chunk is not a durable snapshot.
    /// The ring entry becomes the newest snapshot only when a completion
    /// marker lands in the final chunk's transaction. Drop-newest itself
    /// is unchanged. This is not a move to P1 and not a fifth overflow policy.
    async fn commit_snapshot(&self, snapshot: Snapshot) -> Result<(), SeamError> {
        let _ = snapshot;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use std::sync::Arc;

    #[derive(Debug)]
    struct Unused;

    #[async_trait]
    impl ChainIngress for Unused {
        async fn submit_gossip(&self, _obj: GossipObject) -> Result<VerdictResolution, SeamError> {
            Err(SeamError::Unavailable("unused".into()))
        }

        async fn notify_data_available(&self, _root: Root, _slot: u64) -> Result<(), SeamError> {
            Ok(())
        }

        async fn submit_column_sidecar(&self, _sidecar: ColumnSidecar) -> Result<(), SeamError> {
            Ok(())
        }
    }

    #[async_trait]
    impl P2pEgress for Unused {
        async fn publish(&self, _req: PublishRequest) -> Result<Published, SeamError> {
            Ok(Published::Dropped)
        }

        fn update_view(&self, _view: ChainView) {}
    }

    #[async_trait]
    impl ArchiveWrite for Unused {
        async fn ingest_columns(&self, _batch: ColumnBatch) -> Result<(), SeamError> {
            Err(SeamError::Backpressure {
                bound: 32,
                waited_ms: 0,
            })
        }
    }

    #[test]
    fn chain_ingress_is_dyn() {
        let _: Arc<dyn ChainIngress> = Arc::new(Unused);
    }

    #[test]
    fn p2p_egress_is_dyn() {
        let _: Arc<dyn P2pEgress> = Arc::new(Unused);
    }

    #[test]
    fn archive_write_is_dyn() {
        let _: Arc<dyn ArchiveWrite> = Arc::new(Unused);
    }

    #[test]
    fn column_batch_index_is_a_field() {
        let batch = ColumnBatch {
            parent_root: [2; 32],
            slot: 7,
            block_root: [1; 32],
            index: 42,
            ssz: Bytes::from_static(b"ssz"),
        };
        assert_eq!(batch.parent_root, [2; 32]);
        assert_eq!(batch.slot, 7);
        assert_eq!(batch.block_root, [1; 32]);
        assert_eq!(batch.index, 42);
        assert_eq!(batch.ssz.as_ref(), b"ssz");
    }

    #[test]
    fn ipc_ingress_is_not_the_egress_write() {
        // p2p holds ChainIngress. P2pEgress write is IpcEgress, not Ipc.
        fn assert_ingress<T: ChainIngress>() {}
        fn assert_egress<T: P2pEgress>() {}
        assert_ingress::<Ipc>();
        assert_egress::<IpcEgress>();
    }

    #[test]
    fn seam_error_has_exactly_four_variants() {
        fn classify(err: SeamError) -> u8 {
            match err {
                SeamError::Backpressure { .. } => 0,
                SeamError::Unavailable(_) => 1,
                SeamError::InvalidArgument(_) => 2,
                SeamError::FailedPrecondition { .. } => 3,
            }
        }
        assert_eq!(
            classify(SeamError::Backpressure {
                bound: 64,
                waited_ms: 2000
            }),
            0
        );
        assert_eq!(classify(SeamError::Unavailable("gone".into())), 1);
        assert_eq!(classify(SeamError::InvalidArgument("bad".into())), 2);
        assert_eq!(
            classify(SeamError::FailedPrecondition {
                reason: FailedPreconditionReason::NotBootstrapped
            }),
            3
        );
        assert_eq!(
            FailedPreconditionReason::NotBootstrapped.as_str(),
            "NOT_BOOTSTRAPPED"
        );
    }

    #[test]
    fn submit_gossip_success_is_only_a_verdict() {
        let resolution = VerdictResolution {
            verdict: Verdict {
                correlation_id: [0; 32],
                acceptance: Acceptance::Ignore,
                reason: Reason::Internal,
                import: ImportResult::None,
            },
        };
        assert_eq!(resolution.verdict.reason, Reason::Internal);
    }

    #[test]
    fn gossip_object_has_no_caller_source() {
        let obj = GossipObject {
            ssz: Vec::new(),
            fork: 0,
            root: [0; 32],
            kind: ObjectKind::Block,
            subnet_id: 0,
        };
        let _ = obj;
    }

    #[test]
    fn object_kind_has_no_api_or_trusted_local() {
        fn classify(k: ObjectKind) -> u8 {
            match k {
                ObjectKind::Block => 0,
                ObjectKind::Attestation => 1,
                ObjectKind::Aggregate => 2,
                ObjectKind::SyncCommittee => 3,
                ObjectKind::SyncContribution => 4,
                ObjectKind::VoluntaryExit => 5,
                ObjectKind::ProposerSlashing => 6,
                ObjectKind::AttesterSlashing => 7,
                ObjectKind::BlsToExecutionChange => 8,
                ObjectKind::ColumnSidecar => 9,
            }
        }
        assert_eq!(classify(ObjectKind::Block), 0);
    }

    fn da_name(verdict: DaVerdict) -> &'static str {
        match verdict {
            DaVerdict::Available => "available",
            DaVerdict::Deferred => "deferred",
        }
    }

    fn head_cause_name(cause: HeadCause) -> &'static str {
        match cause {
            HeadCause::Import => "import",
            HeadCause::Attestation => "attestation",
            HeadCause::EngineInvalidation => "engine_invalidation",
        }
    }

    /// The durable-import surface is types and signatures. The default
    /// methods return `Ok` and do not open a store.
    #[tokio::test]
    async fn durable_import_ops_are_signatures_only() {
        let block_root = [1u8; 32];
        let parent_root = [2u8; 32];
        let state_root = [3u8; 32];
        let scalars = Bytes::from_static(b"scalars");
        let head = HeadChange {
            head_root: block_root,
            head_slot: 7,
            cause: HeadCause::Import,
        };
        let import = DurableImport {
            block_root,
            parent_root,
            slot: 7,
            state_root,
            ssz: Bytes::from_static(b"block"),
            da: DaVerdict::Deferred,
            scalars: scalars.clone(),
            head: Some(head.clone()),
        };
        // Canonical rewrite is `head`, not a caller-settable flag.
        let DurableImport {
            block_root: got_block,
            parent_root: got_parent,
            slot,
            state_root: got_state,
            ssz: _,
            da,
            scalars: got_scalars,
            head: got_head,
        } = import.clone();
        assert_eq!(got_block, block_root);
        assert_eq!(got_parent, parent_root);
        assert_ne!(got_parent, got_block);
        assert_eq!(slot, 7);
        assert_eq!(got_state, state_root);
        assert_eq!(da_name(da), "deferred");
        assert_eq!(got_scalars.as_ref(), b"scalars");
        let HeadChange {
            head_root: got_head_root,
            head_slot,
            cause,
        } = got_head.expect("head-changing import carries HeadChange");
        assert_eq!(got_head_root, block_root);
        assert_eq!(head_slot, 7);
        assert_eq!(head_cause_name(cause), "import");

        let anchor = TrustedAnchor {
            block_root,
            parent_root,
            slot: 7,
            state_root,
            block_ssz: Bytes::from_static(b"block"),
            state_ssz: Bytes::from_static(b"state"),
            scalars: scalars.clone(),
            da: DaVerdict::Available,
        };
        let TrustedAnchor {
            block_root: _,
            parent_root: _,
            slot: _,
            state_root: _,
            block_ssz: _,
            state_ssz: _,
            scalars: _,
            da: anchor_da,
        } = anchor.clone();
        assert_eq!(da_name(anchor_da), "available");

        let snapshot = Snapshot {
            slot: 8,
            state_root,
            state_ssz: Bytes::from_static(b"state"),
        };
        let Snapshot {
            slot: snap_slot,
            state_root: _,
            state_ssz: _,
        } = snapshot.clone();
        assert_eq!(snap_slot, 8);
        assert_eq!(head_cause_name(HeadCause::Attestation), "attestation");
        assert_eq!(
            head_cause_name(HeadCause::EngineInvalidation),
            "engine_invalidation"
        );

        let archive: Arc<dyn ArchiveWrite> = Arc::new(Unused);
        archive
            .commit_anchor(anchor)
            .await
            .expect("signature default does not write");
        archive
            .commit_import(import)
            .await
            .expect("signature default does not write");
        archive
            .set_head(
                HeadChange {
                    head_root: block_root,
                    head_slot: 7,
                    cause: HeadCause::Attestation,
                },
                scalars,
            )
            .await
            .expect("signature default does not write");
        archive
            .commit_snapshot(snapshot)
            .await
            .expect("signature default does not write");

        // Tokens, not a runtime check. The default bodies above still return
        // `Ok(())`. A collapsed mapping (one token for two reasons) fails.
        fn precondition_token(reason: FailedPreconditionReason) -> &'static str {
            match reason {
                FailedPreconditionReason::NotBootstrapped => "NOT_BOOTSTRAPPED",
                FailedPreconditionReason::StoreNotUninitialized => "STORE_NOT_UNINITIALIZED",
                FailedPreconditionReason::StoreIncomplete => "STORE_INCOMPLETE",
                FailedPreconditionReason::ParentNotDurable => "PARENT_NOT_DURABLE",
                FailedPreconditionReason::HeadNotDurable => "HEAD_NOT_DURABLE",
            }
        }
        let reasons = [
            FailedPreconditionReason::NotBootstrapped,
            FailedPreconditionReason::StoreNotUninitialized,
            FailedPreconditionReason::StoreIncomplete,
            FailedPreconditionReason::ParentNotDurable,
            FailedPreconditionReason::HeadNotDurable,
        ];
        for reason in reasons {
            assert_eq!(reason.as_str(), precondition_token(reason));
        }
        assert_ne!(
            FailedPreconditionReason::StoreIncomplete.as_str(),
            FailedPreconditionReason::HeadNotDurable.as_str(),
            "an incomplete store is not a non-durable head"
        );
    }
}
