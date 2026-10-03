# Block lifecycle end to end

> **As built at `7d8833d`** (`develop`, 2026-08-16). Status labels and diagram legend: [README](README.md#status-labels).
> Target design: [`plan/architecture.md`](../../plan/architecture.md) §2, §3, §4, §9.1.

This page follows one beacon block, and the data columns that make it available, from a libp2p
gossip message to the point where peers can fetch it from us. It walks the hops in the order they
were designed and gives each one's status at HEAD for each shape. Component internals live
elsewhere: [04 §5-§7](04-chain-core.md#5-import-pipeline-on-the-core-thread) (import pipeline, DA gate, `pending_engine`),
[07 §4.4](07-storage.md#44-archivewrite-the-import-persist-path-n1) (persist),
[08 §4](08-execution-engine.md#4-directengine-on-the-core-thread) (DirectEngine),
[10 §6](10-p2p-gossip-and-das.md#6-block-path-to-chain) (block path to chain) and
[11](11-p2p-reqresp-and-sync.md#serving-blocks-and-columns) (serving).

**IDLE (iff core)** means reachable only once a core has been installed, and then only by an
operator's hand-submitted `ImportBlock` ([README](README.md#status-labels)). At HEAD, installing a
core requires a non-empty `checkpoint_providers` (shape B's durable seed never has input). That
list defaults to `[]` in both shapes (`config/chain.toml:27`, `config/beacon-core.toml:23`), and
compose does not override it.

## What actually runs at HEAD

**No block completes the lifecycle at HEAD, in either shape.** Two independent facts block it:

1. **Gossip never delivers a block.** `SwarmCommand::Subscribe` has a handler
   (`services/p2p/src/host.rs:1253-1260`), but no production code sends it, and the pinned
   gossipsub drops messages on unsubscribed topics before it raises `Event::Message`. At HEAD p2p
   subscribes to no gossip topics; the plan schedules gossip wiring for a later stage.
2. **The DA gate never opens.** `on_block` checks `PeerDasAvailability` membership as the first
   check of the full-import path (`crates/fork-choice/src/on_block.rs:230-234`). The only
   production code that marks a root available is the P2pStream `DataAvailable` arm, and p2p never
   sends that message (E2 is DEAD-AS-WIRED). The gate is pure set membership with no zero-blob
   exemption (`crates/fork-choice/src/da_seam.rs:193-197`), so even a block with no blobs defers.

| Configuration | Entry that reaches the core thread | Hops that execute |
|---|---|---|
| Default: shape A compose, or shape B, with `checkpoint_providers = []` | None (core-absent). `ImportBlock` returns `FAILED_PRECONDITION` `NOT_BOOTSTRAPPED` (`crates/chain-core/src/service.rs:240-251`) | Shape A runs the P2pStream session and ChainView (E1 session + view, LIVE). Nothing reaches admission (hop 4) |
| Providers set (shape A, or shape B with no durable set, which at HEAD is every boot) | Unary `ImportBlock` only. No in-tree code calls it; an operator can call it with grpcurl | Hops 4, 5 and 6. Then: park in `pending_da`; `BLOCK_IMPORTED` with verdict byte `DEFERRED_DA`; FetchBlobs if the block carries blobs (output discarded); fcU re-sending the anchor head; reply `DEFERRED_DA`; expiry after 4 slots. Hops 7-11 never run for a new block. Only a direct child of the checkpoint anchor gets past hop 5; any other block returns `UNKNOWN_PARENT` there |
| Devnet (`cc-p2p --publish-fixture` / `--devnet-peer`) | None. The devnet swarm never talks to chain | Gossip receive and a blind ACCEPT. This is DEVNET-ONLY (`services/p2p/src/fault_mode.rs:1360-1381`) |

A block that fails a pre-STF check never reaches the DA gate. It gets a terminal verdict at hop 5
instead (unknown parent, reorg gap, future slot, too old, not descended from finalized, wrong
proposer, or bad signature); an SSZ decode failure or root mismatch returns `INVALID_ARGUMENT`.

## Swimlane overview

Each swimlane is a process or a thread. The last column holds the EL (an external process, reached
by `~~>`) and, in shape B only, the storage-core writer task inside cc-beacon-core (reached
in-process by `- ->`). The chain process is `cc-chain` in shape A and `cc-beacon-core` in shape B.
Read the rows top to bottom; hop numbers match the hop table.

```text
remote peers  | p2p process [A]        | chain process (tokio) | core thread          | EL; writer
--------------+------------------------+-----------------------+----------------------+-------------
 [1] gossip -x| 0 topics subscribed    |                       |                      |
              | [2] validation pool    |                       |                      |
              | [3] object - - - - - - +- -> P2pStream session |                      |
              | <======================+= ChainView, p2p dials |                      |
              |                        | ImportBlock (grpcurl) |                      |
              |                        | ....> [4] import      |                      |
              |                        |   scheduler lane - - -+-> [5] decode, root,  |
              |                        |                       |    cheap checks      |
              | <- - - - early ACCEPT -+- - - - - - - - - - - -+- - [5] gossip only   |
              |                        |                       | [6] DA gate (closed) |
              |                        |                       |  on miss: pending_da |
              |                        |                       | [7] STF ~~~~~~~~~~~~~+~> newPayload
              |                        |                       | [8] fork choice      |
              |                        |                       | [9] HeadSnapshot     |
              |                        |                       | [10] persist - - - - +- -> P0 [B]
              |                        | events task <- - - - -+- [11] events         |
              |                        |                       | [12] fcU ~~~~~~~~~~~~+~> fcU
              | [13] report <- - - - - +- - - - verdict - - - -+- reply               |
 [14] fwd <- -+- - mesh forward (IDLE) |                       |                      |
 [14] serve <-+- req/resp code 3 (STUB)|                       |                      |
--------------+------------------------+-----------------------+----------------------+-------------
```

The marks follow the [legend](README.md#diagram-legend):

- `x`: DEAD-AS-WIRED. `- - ->`: IDLE. `<====`: a LIVE stream. `....>`: served but never dialed
  in-tree. `[A]` / `[B]`: shape A only / shape B only.
- `~~~~>`: a call that leaves the process. It carries no status: [7] newPayload and [12] fcU are
  both IDLE (iff core).
- A `(STUB)` label means the answer is a placeholder.
- DELETED, not drawn: E3 (631994c, the chain -> engine-container gRPC hop), E4 (60c6200, the
  storage service's `RestoreFromStore` boot push to chain), E7 (78a90e1, the storage service's
  `SubscribeEvents` consumer).

draw.io version: [`diagrams/block-lifecycle.drawio`](diagrams/block-lifecycle.drawio) (hops 1-14 as
a swimlane, edges coloured by status; open in draw.io). The ASCII figure stays the reviewed source
([diagrams/README.md](diagrams/README.md)).

## Hop table

| # | Hop | Mechanism and code | Shape A | Shape B | See |
|---|---|---|---|---|---|
| 1 | libp2p gossip receive | `beacon_block` / `data_column_sidecar_{i}` topics; the swarm task routes `Event::Message` (`services/p2p/src/host.rs:462-548`) | DEAD-AS-WIRED (no `Subscribe` sender) | ABSENT | [gossip](#hops-1-3-and-13-gossip-in-verdict-out-designed-hop-1-dead-as-wired-the-rest-idle) |
| 2 | p2p validation pipeline | one serial `gossip-validate` loop (`services/p2p/src/gossip/validate/pipeline.rs:385-398`) | IDLE | ABSENT | [gossip](#hops-1-3-and-13-gossip-in-verdict-out-designed-hop-1-dead-as-wired-the-rest-idle) |
| 3 | P2pStream object (E1) | `ChainOutbound` -> `Ipc::submit_gossip`, 2 s budget (`crates/seam/src/ipc.rs:1146-1159`) | object/verdict arm IDLE; session + view LIVE | ABSENT | [gossip](#hops-1-3-and-13-gossip-in-verdict-out-designed-hop-1-dead-as-wired-the-rest-idle) |
| 4 | Admission to the import scheduler lane | `CoreHandle::import_block` / `import_block_for_gossip`, FIFO 64, 2 s push (`crates/chain-core/src/core.rs:938-999`) | gossip arm IDLE; `ImportBlock` IDLE (iff core) | `ImportBlock` IDLE (iff core) | [core](#hops-4-12-inside-chain-core) |
| 5 | Decode, root, cheap checks, early ACCEPT | `crates/chain-core/src/import.rs:369-441`; checks `:663-791` | IDLE (iff core; unary); early ACCEPT IDLE (gossip) | IDLE (iff core; unary) | [core](#hops-4-12-inside-chain-core) |
| 6 | DA gate | `PeerDasAvailability`, first check of the full-import path (`crates/fork-choice/src/on_block.rs:230-234`) | IDLE (iff core; never opens) | IDLE (iff core; never opens) | [core](#hops-4-12-inside-chain-core) |
| 6a | `pending_da` park and expiry | 64 entries, 4-slot expiry (`crates/chain-core/src/da.rs:43-49`) | IDLE (iff core) | IDLE (iff core) | [core](#hops-4-12-inside-chain-core) |
| 6b | DataAvailable re-drive (E2) | `handle_data_available` (`core.rs:2064-2136`) | DEAD-AS-WIRED | ABSENT | [columns](#data-columns-and-the-getblobs-fastpath) |
| 7 | STF + newPayload | `process_execution_payload` (`crates/state-transition/src/block/execution_payload.rs:82`) -> DirectEngine, 8 s | IDLE (iff core; DA gate never opens; E2 DEAD-AS-WIRED) | IDLE (iff core; DA gate never opens; no DataAvailable producer) | [core](#hops-4-12-inside-chain-core) |
| 8 | Fork choice | proto-array insert inside `on_block` (`on_block.rs:292`); then `get_head` and residency settle (`import.rs:928-950`) | IDLE (iff core) | IDLE (iff core) | [core](#hops-4-12-inside-chain-core) |
| 9 | HeadSnapshot | ArcSwap publish before events (`import.rs:952-978`) | IDLE (iff core) on import; anchor snapshot LIVE (iff core) | same as A | [core](#hops-4-12-inside-chain-core) |
| 10 | ArchiveWrite persist (N1 blocks) | `ingest_block_blocking` -> writer P0 (`import.rs:988-990`) | ABSENT (`archive` unset) | IDLE (iff core) | [core](#hops-4-12-inside-chain-core) |
| 11 | Events | `BLOCK_IMPORTED`, `HEAD`, `CHAIN_REORG`, `FINALIZED_CHECKPOINT` (`import.rs:1036-1098`) | IDLE (iff core); `DEFERRED_DA` event IDLE (iff core; no subscribers) | same as A | [core](#hops-4-12-inside-chain-core) |
| 12 | fcU | `emit_fcu_head` after every Ok import (`core.rs:2018-2037`) | IDLE (iff core; anchor head) | IDLE (iff core; anchor head) | [today](#the-run-that-executes-today-importblock-with-checkpoint-providers) |
| 13 | Verdict back to p2p | `emit_terminal_verdict` / `map_import_verdict` (`crates/chain-core/src/p2p_stream.rs:850-1057`) -> `report` (`pipeline.rs:823-857`) | IDLE | ABSENT | [gossip](#hops-1-3-and-13-gossip-in-verdict-out-designed-hop-1-dead-as-wired-the-rest-idle) |
| 14 | Serve to peers | gossipsub forward; req/resp by range / by root; E6 reads | forward IDLE; req/resp STUB; E6 reads DEAD-AS-WIRED | ABSENT | [serving](#hop-14-serving-the-block-to-peers) |

IDLE (iff core) is defined in the [introduction](#block-lifecycle-end-to-end). Shape B lists every
p2p hop as ABSENT because no shipped configuration pairs cc-p2p with cc-beacon-core; how p2p
attaches to shape B is not yet defined. beacon-core does serve `P2pStream`, but nothing dials it.

## Hops 1-3 and 13: gossip in, verdict out (designed; hop 1 DEAD-AS-WIRED, the rest IDLE)

```text
remote    |  cc-p2p process [A]                  |  cc-chain process
peer         swarm task        gossip-validate      P2pStream session        core thread
  |               |                   |                     |                     |
  |- - gossip - x>|                   |                     |                     |
  |               | - GossipWork  - ->|                     |                     |
  |               |                   | [2] local stage     |                     |
  |               |                   | - [3] Ipc (2 s) - ->|                     |
  |               |                   |                     | - [4] import  - - ->|
  |               |                   |                     |                     | [5] cheap checks
  |               |                   |                     |<- - early ACCEPT  - |
  |               |                   |<- - - ACCEPT  - - - |                     |
  |               |<- - [13] report - |                     |                     |
  |<- - [14] fwd -|                   |                     |                     |
  |               |                   |                     |                     | [6]-[12] go on
  |               |                   |                     |<- - - outcome - - - |
  |               |                   |<- - late REJECT - - |                     |
  |               |                   | import_invalid -25  |                     |
```

This is the shape A designed path (ABSENT in shape B); the top row marks the process boundary.
The first arrow is dead because no topic is subscribed, so every later arrow is IDLE. The late
REJECT is applied by the `chain-in-late-verdicts` task (`pipeline.rs:401-414`), not the validation
loop. p2p-side swimlanes (chain-stream client, Ipc): [10 §6](10-p2p-gossip-and-das.md#6-block-path-to-chain).

- **[1] Receive.** Topics look like `/eth2/{digest}/beacon_block/ssz_snappy`; `SnappyTransform`
  has already decompressed the payload. The swarm task (sole `Swarm` owner) sheds on a stall,
  answers IGNORE for an unknown topic and REJECT for an oversize payload, then `try_send`s a
  `GossipWork` onto an mpsc of 1024 (full: IGNORE).
- **[2] Validate.** The block local stage (`services/p2p/src/gossip/validate/block.rs:72-156`)
  checks size, Fulu decode, slot window, `(slot, proposer)` dedup and a local parent gate, then
  forwards. Only BLOCK is chain-authoritative ([ADR-P2-04](../adr/ADR-P2-04.md)).
- **[3] Forward.** `ChainOutbound` with a reply oneshot goes through `chain_out_tx.send().await`,
  which has no enqueue timeout (`pipeline.rs:605-659`). The chain-stream client calls
  `Ipc::submit_gossip`, whose 2 s budget starts at enqueue; a 12 s local timeout
  (`pipeline.rs:615`) is a backstop. On the wire: `P2pToChain.object(GossipObject)` on the bidi
  `ChainService.P2pStream`, which p2p dials.
  - The loop is serial, so every topic waits behind a forwarded block until chain answers:
    normally within 2 s, at worst 12 s. The local parent gate parks a block whose parent p2p has
    not recorded, with no head-root fallback (`block.rs:122-137`). Latent; unreachable at HEAD
    because no gossip topics are subscribed. Detail:
    [10 §13](10-p2p-gossip-and-das.md#13-concurrency-metrics-and-latent-defects).
- **On chain.** Each session handles inbound messages one at a time; chain allows at most 8
  sessions (`crates/chain-core/src/p2p_stream.rs:61`). `handle_gossip_object`
  (`p2p_stream.rs:738-848`) answers non-BLOCK kinds Ignore/AlreadyKnown, a core-absent chain
  Ignore/Internal, and otherwise calls `import_block_for_gossip` with an early-accept oneshot.
- **[13] Verdict.** Chain sends ACCEPT (Accept/Valid, import NONE) as soon as hop 5 passes,
  **before the DA gate and the STF**. As designed, a block that will defer at the DA gate has
  therefore already been ACCEPTed and propagated. After an early ACCEPT, chain sends a second
  message in two cases (`p2p_stream.rs:821-847`):
  - a late Reject-class import failure: Reject/Invalid;
  - an import that returns a gRPC error: Reject/Invalid for `INVALID_ARGUMENT`, else
    Ignore/Internal. In shape A no reachable post-ACCEPT error is `INVALID_ARGUMENT` (INTERNAL at
    `import.rs:477` and `:935`, or UNAVAILABLE if the core thread drops the reply); an `INVALID_ARGUMENT`
    persist bind failure needs an archive handle, which shape A does not set.

  The second case contradicts the doc comment (`p2p_stream.rs:733-737`), which says only a
  Reject-class failure sends a second message and that Internal sends none. p2p treats any second
  message as a stray verdict (`services/p2p/src/chain_stream/client.rs:331-339`): it applies a
  late REJECT as `import_invalid` (-25) and does not re-report.
- **Without an early ACCEPT**, `emit_terminal_verdict` (`p2p_stream.rs:850-883`) sends the one
  verdict. An Ok outcome goes through `map_import_verdict` (`:970-1057`;
  [ADR-P1-07](../adr/ADR-P1-07.md)): IMPORTED -> Accept; DUPLICATE -> Ignore/Duplicate;
  `DEFERRED_DA` -> Ignore/DeferredDa; UNKNOWN_PARENT -> Ignore/UnknownParent; INVALID ->
  Reject/Invalid, except future slot -> Ignore/FutureSlot, too old -> Ignore/AlreadyKnown, not
  descended from finalized -> Reject/NotDescendedFromFinalized, and an internal
  proposer-signature failure -> Ignore/Internal. A gRPC error becomes Reject/Invalid for
  `INVALID_ARGUMENT`, else Ignore/Internal, both with import INVALID. Gossipsub and app-score
  effects: [10 §4.1](10-p2p-gossip-and-das.md#41-verdict-mapping). Two consequences:
  - Ipc reads `(Ignore, Internal, Invalid)` as import backpressure (`crates/seam/src/ipc.rs:900-915`),
    and both the non-`INVALID_ARGUMENT` gRPC error and the internal proposer-signature failure
    take that shape. Some internal import failures surface to p2p as backpressure.
  - A reorg gap, our own residency limit, answers Reject/Invalid (`import.rs:686-693`), so p2p
    would REJECT it and penalise the forwarding peer (`gossip_invalid`, -10).
- **Reporting.** p2p `report` sends a penalty on REJECT, then `SwarmCommand::ReportValidation` to
  the one `report_message_validation_result` call site (`host.rs:439-449`). Forwarding to the
  mesh ([14]) follows an ACCEPT report.

The gossip-path effects above are traced from code. Latent; unreachable at HEAD because no gossip
topics are subscribed.

## Hops 4-12: inside chain-core

```text
caller       scheduler    core thread                    events task    writer [B]           EL
   |             |             |                              |              |                |
   |- - push 2s->|             |                              |              |                |
   |             |- - pop  - ->|                              |              |                |
   |             |             | [5] dedup probe, decode,     |              |                |
   |             |             |     root check, cheap checks |              |                |
   |<- - early ACCEPT, gossip -|                              |              |                |
   |             |             | [6] DA gate (PeerDAS set)    |              |                |
   |             |             | [7] STF on a parent clone    |              |                |
   |             |             |~~~~~~~~~~~~~~~~~~~~ [7] newPayload, 8 s ~~~~~~~~~~~~~~~~~~~~>|
   |             |             | [8] proto-array, get_head,   |              |                |
   |             |             |     residency settle         |              |                |
   |             |             | [9] HeadSnapshot (ArcSwap)   |              |                |
   |             |             | - - - - [10] ingest_block_blocking  - - - ->|                |
   |             |             |<- - - - - - commit, no deadline - - - - - - |                |
   |             |             |- - - [11] blocking_send  - ->|              |                |
   |             |             | [12] FetchBlobs, EpochContext|              |                |
   |             |             |~~~~~~~~~~~~~~~~~~~~~~~ [12] fcU, 8 s ~~~~~~~~~~~~~~~~~~~~~~~>|
   |<- - reply / verdict  - - -|                              |              |                |
```

The caller is the P2pStream session or the `ImportBlock` handler. Hops 4-6 and 12 are IDLE (iff
core) on the `ImportBlock` path; the early-ACCEPT arrow belongs to the gossip path, which is IDLE.
Hops 7-11 are IDLE (iff core): a DA-gate miss parks the block and stops the pipeline at hop 6.
Stage table with code: [04 §5](04-chain-core.md#5-import-pipeline-on-the-core-thread).

- **[4] Admission.** `CoreHandle` pushes onto the import scheduler lane (FIFO, depth 64, drop-new);
  a push not admitted within 2 s returns `RESOURCE_EXHAUSTED`. The core thread owns `Store<P>` by
  value ([ADR-P1-09](../adr/ADR-P1-09.md)).
- **[5] Cheap stage.** When `slot_tick_enabled` is set (both production hosts), `core_loop` first
  runs `advance_store_clock` (`core.rs:1711-1713`). `import_block_with_early`
  (`import.rs:349-632`) then runs: (1) a dedup probe on the supplied root, which returns DUPLICATE
  ([ADR-P1-10](../adr/ADR-P1-10.md)); (2) Fulu-only decode; (3) a root check, `INVALID_ARGUMENT`
  on mismatch; (4) the cheap checks (`import.rs:663-791`), each terminal; (5) the early ACCEPT,
  gossip path only (`import.rs:437-441`).
- **[6] DA gate.** On a miss (`import.rs:505-552`) the arrival SSZ parks in `pending_da`,
  `BLOCK_IMPORTED` is published with verdict byte `DEFERRED_DA` (`import.rs:528-534`), and a
  `BlockBranchTrigger` is built if the block carries blob commitments ([04 §6](04-chain-core.md#6-da-gate-and-pending_da)).
- **[7] STF and newPayload.** The STF runs on a clone of the parent state and makes the single
  `verify_and_notify_new_payload` call ([ADR-P3-03](../adr/ADR-P3-03.md)) through DirectEngine:
  `Handle::block_on(timeout(8 s))` on the ordered transport lane, never retried
  ([ADR-P3-09](../adr/ADR-P3-09.md)). INVALID or INVALID_BLOCK_HASH rejects the block; SYNCING or
  ACCEPTED imports it as optimistic ([ADR-P3-10](../adr/ADR-P3-10.md)). newPayload is refused
  without an HTTP call while the EL health machine is Offline (its start state;
  [08 §3.5](08-execution-engine.md#35-el-health-state-machine)). Every engine-api error parks the
  block in `pending_engine` (64 entries, 8-slot expiry; [ADR-P3-05](../adr/ADR-P3-05.md)) with wire
  verdict `DEFERRED_DA`, reason `execution_engine_unavailable`. Engine failures defer the block
  rather than reject it; re-drive depends on an EL health transition. Detail: [04 §7](04-chain-core.md#7-pending_engine-and-optimistic-import),
  [08 §4.3](08-execution-engine.md#43-pending_engine-and-re-drive).
- **[8]-[9] Fork choice and HeadSnapshot.** `on_block` inserts the block into proto-array;
  `finish_imported` (`import.rs:895-1023`) then records the body in the ring, runs `get_head`,
  settles residency and publishes the HeadSnapshot **before** any event (GetHead and ChainView
  kind 3 read it).
- **[10] Persist (shape B only).** `ArchiveWrite::ingest_block_blocking` into writer P0, an mpsc
  of 32: a `blocking_send`, then a wait for the commit with no deadline
  (`crates/storage-core/src/writer.rs:272-280`; [ADR-P4-04](../adr/ADR-P4-04.md),
  [ADR-R-02](../adr/ADR-R-02.md)). The trait documents Backpressure; the implementation blocks
  without a deadline.
  - Rows written: `blocks_hot`, `block_slot_by_root`, `canonical`, `write_cursor`
    ([storage-schema.md](../storage-schema.md)); not `da_status`, `state_roots`, columns or
    fork-choice scalars. In shape A, `CoreConfig.archive` is unset (`services/chain/src/run.rs:368-378`).
  - As wired, `canonical` follows the last persisted block, not the fork-choice head: every import
    persists with `update_canonical: true` (`crates/storage-core/src/archive_write.rs:321`), so a
    side-fork import rewrites it. [07 §4.4](07-storage.md#44-archivewrite-the-import-persist-path-n1).
- **[11] Events.** The core thread `blocking_send`s `BLOCK_IMPORTED(IMPORTED)`, `HEAD`,
  `CHAIN_REORG` (if any) and `FINALIZED_CHECKPOINT` (on a change). `SubscribeEvents` is served, but
  nothing in the tree subscribes. The storage service's consumer (E7) was DELETED in 78a90e1.
- **[12] Tail and reply.** Back in `core_loop` (`core.rs:1736-1754`, `:1784-1803`): FetchBlobs if a
  trigger exists, `maybe_publish_epoch_context`, `emit_fcu_head` (8 s), then the reply, so the RPC
  latency includes the FetchBlobs enqueue (up to 1 s) and the fcU round trip. Budget:
  [../engine-latency.md](../engine-latency.md) (its engine-container gRPC framing predates 631994c).

**Waits on the core thread.** newPayload, the persist commit, the event `blocking_send`s and the
fcU all run on the core thread, so a stalled EL, writer or events task parks it and the liveness
sampler flips aggregate health ([13 §5.2](13-concurrency-model.md#52-waits-with-no-deadline)).
Nothing in this repository restarts a process on that signal.

**What happens if persist fails (shape B).** Persist runs after fork choice, residency and the
HeadSnapshot have already changed. On an error no events are emitted, the epoch-context publish
and the fcU are skipped, and the unary caller receives `INVALID_ARGUMENT` or `UNAVAILABLE`
(`import.rs:846-852`). As wired, a gossip import would also send a second verdict (latent: no
shipped configuration has both an archive handle and a p2p peer). A retry takes the DUPLICATE
path, which persists the block if it is missing but never emits `BLOCK_IMPORTED`.

- **The first child of a checkpoint anchor.** Traced from code, not run: in shape B that child
  would, as wired, fail the continuity bind with `INVALID_ARGUMENT` (`archive_write.rs:131-154`),
  because nothing ever persists the anchor. This is moot while the DA gate never opens.
- **Test coverage.** No test drives the production persist path (core thread +
  `ingest_block_blocking` with a real writer).

## The run that executes today: ImportBlock with checkpoint providers

```text
operator       ChainService    core thread                   events task  fastpath             EL
   |                 |              |                             |           |                 |
   |- - ImportBlock->|              |                             |           |                 |
   |                 |- - push 2s ->|                             |           |                 |
   |                 |              | decode, root, cheap checks  |           |                 |
   |                 |              | DA gate: miss (always)      |           |                 |
   |                 |              | park in pending_da (64)     |           |                 |
   |                 |              | BLOCK_IMPORTED(DEFERRED_DA) |           |                 |
   |                 |              | - - - - - - - - - - - - - ->|           |                 |
   |                 |              |- - -  FetchBlobs if blobs, <= 1 s - - ->|                 |
   |                 |              |                             |           |~~ getBlobsV2 ~~>|
   |                 |              |                             |           | output dropped  |
   |                 |              | EpochContext (anchor epoch) |           |                 |
   |                 |              |~~~~~~~~~~~~~~~~ fcU: anchor head, <= 8 s ~~~~~~~~~~~~~~~~>|
   |                 |<- DEFERRED - |                             |           |                 |
   |<- - reply - - - |              |                             |           |                 |
   |                 |              | after 4 slots: expiry       |           |                 |
```

Every arrow is IDLE (iff core), drawn `- - ->` in process and `~~~~>` to the EL, per the legend.
An operator dials `ImportBlock` by hand; in-tree nothing does (the swimlane's `....>`). This is
the only path that brings a new block to the core thread at HEAD.
Because nothing past the anchor is ever imported, only a direct child of the checkpoint anchor
block can pass the parent check (`import.rs:677-684`) and reach the DA gate. Any other block ends
at hop 5 with UNKNOWN_PARENT, and re-submitting the anchor takes the DUPLICATE path.

The fcU carries the proto-array execution hashes of the checkpoint anchor (head, justified,
finalized), because the head never moves past the anchor through import. Once one fcU has been
sent, the per-slot floor re-sends the same triple (`crates/chain-core/src/fcu_driver.rs:256-286`);
no fcU is sent at core install. While the EL health machine is Offline, the fcU is refused locally
without an HTTP call (`crates/engine-api/src/api.rs:483-493`). The EpochContext step republishes
the anchor's epoch once (spawn publishes sequence 0, `core.rs:1273`), then is a no-op
(`core.rs:2222-2240`). The `BLOCK_IMPORTED` event has no in-tree subscriber, and
`cc_chain_da_pending_dropped_total` counts the expiry (and any capacity eviction).

## Other entry points

| Entry | Mechanism | Shape A | Shape B |
|---|---|---|---|
| `ImportBlock` RPC | `ChainService.ImportBlock` -> `CoreHandle::import_block`; unary path, no early ACCEPT, signature strategy from `CoreConfig.verify`; contract: [contracts.md](../contracts.md#importblock) | IDLE (iff core); no in-tree caller | IDLE (iff core); no in-tree caller |
| Checkpoint sync seed | `fetch_checkpoint` + `verify_checkpoint` -> `spawn_core_from_checkpoint_with_epoch` (`services/chain/src/checkpoint_sync.rs:868`, `:243`, `:1112-1170`); protocol in [12 §3.3](12-boot-health-and-shutdown.md#33-checkpoint-sync-protocol), and with `checkpoint_root` unset the first provider that returns a verifying triple is trusted (`:1054-1059`; [01 X4](01-deployment-and-processes.md#trust-boundaries)) | LIVE (opt-in; default `[]`) | LIVE (opt-in; runs whenever the durable set is None, which at HEAD is every boot) |
| Durable seed | `seed_from_durable` (`crates/chain-core/src/seed.rs:383`) | ABSENT | IDLE (durable set always None) |
| Gossip via P2pStream | hops 1-3 | IDLE | ABSENT |
| Req/resp sync (range sync, backfill) | `BackfillPlanner` (`services/p2p/src/backfill/planner.rs`) and `RequestScheduler` (`services/p2p/src/reqresp/client.rs:192`); nothing turns a downloaded batch into a `GossipObject` or a `PutBackfillBatch`; there is no parent-lookup code ([11](11-p2p-reqresp-and-sync.md#backfill)) | TEST-ONLY (no request is ever sent) | ABSENT |

**Checkpoint sync** fetches genesis, spec, the finalized block and its state from beacon-API
providers (HTTPS or loopback HTTP), then verifies the triple. It builds a store with
`PeerDasAvailability` and spawns the core thread, which publishes the anchor HeadSnapshot
(`publish_initial_snapshot`, `crates/chain-core/src/core.rs:1601`). The anchor enters fork choice
directly, never through `import_block_with_early`, so it is never persisted (see the first-child
note under [Hops 4-12](#hops-4-12-inside-chain-core)). In shape A, chain binds and marks itself
ready first, then syncs in a background task (a failure means `exit(1)`); shape B syncs before it
binds.

**Durable seed** replays stored blocks with `NoVerification` and applies the stored DA verdicts.
The STF still calls the EL during that replay, and an `ExecutionEngineUnavailable` deferral
during replay is fatal to boot. Shape B does not yet bound its store or order EL readiness before
seed replay. Boot order: [12 §3](12-boot-health-and-shutdown.md#3-boot-per-process).

## Data columns and the getBlobs fastpath

**Designed path.** Three sources make a block's columns available: gossip
`data_column_sidecar_{i}` (13-step validation, inline KZG); the EL getBlobs fastpath
([ADR-P3-12](../adr/ADR-P3-12.md), [ADR-P3-15](../adr/ADR-P3-15.md)); and by-root recovery.
Each source feeds the p2p `SamplingTracker`. Once `verified == required`, p2p sends
`DataAvailable{root, slot}` (E2). Chain then marks the root, re-drives `pending_da`
(`core.rs:2064-2136`) and runs fcU. Column bytes reach the store through `P2pToChain.column` ->
`ColumnBatch` -> `ArchiveWrite::ingest_columns` (`p2p_stream.rs:713`). As wired, the re-drive
passes no `pending_engine` (`core.rs:2129`), so an engine failure at that point drops the block.
Its outcome is not sent to p2p either: the verdict p2p already holds (early ACCEPT, or
Ignore/DeferredDa) stands even if the STF then fails. Both are latent: p2p never sends
`DataAvailable` at HEAD.

```text
p2p (NoopSamplingFeed)     core thread                 fastpath worker                     EL
      |                         |                             |                             |
      |                         | DA gate miss: pending_da    |                             |
      |                         |- - FetchBlobs trigger - - ->|                             |
      |                         |                             |~~~~~~ getBlobsV2, 1 s ~~~~~>|
      |                         |                             |<~~~ blobs + cell proofs ~~~~|
      |                         |                             | compute_cells, transpose    |
      |                         |                             | to 128 sidecars, filter     |
      |                         |                             | (empty SubscriptionSet)     |
      |<x- - - - - - - - - - - -+- inject_tx = None - - - - - |                             |
      | gossip column ACCEPT    |                             |                             |
      | -> NoopSamplingFeed     |                             |                             |
      |-- DataAvailable (E2) -x>|                             |                             |
      |                         | mark_available: never       |                             |
      |                         | pending_da expires          |                             |
```

This is shape A with providers set and an operator-submitted block that carries blobs; as wired,
the getBlobsV2 call would run (IDLE (iff core)). Every `x` arrow is DEAD-AS-WIRED, and gossip
column ACCEPT is IDLE (no topics). The core thread and the fastpath worker run in the chain
process; the p2p swimlane is cc-p2p. The E1 column arm (DEAD-AS-WIRED) is not drawn: it ends in
the P2pStream session, which fails closed with Ignore/Internal because `P2pStreamDeps.archive` is
None (`p2p_stream.rs:697-711`). It never reaches the core thread.

| Piece | Code | Shape A | Shape B |
|---|---|---|---|
| Gossip column validation | `services/p2p/src/gossip/validate/column.rs` | IDLE | ABSENT |
| KZG verify pool | `services/p2p/src/das/verify_pool.rs`; `kzg_tx: _` at `services/p2p/src/service.rs:705` | DEAD-AS-WIRED (self-terminating) | ABSENT |
| Sampling feed on gossip | `NoopSamplingFeed` (`pipeline.rs:375`) | STUB | ABSENT |
| Recovery ladder | `services/p2p/src/das/recovery.rs` | TEST-ONLY | ABSENT |
| E2 `DataAvailable` | no `notify_data_available` caller in `services/p2p` | DEAD-AS-WIRED | ABSENT |
| E1 column arm | `archive` is None in `P2pStreamDeps` (`crates/chain-core/src/service.rs:112-117`) | DEAD-AS-WIRED | ABSENT |
| N1 columns | `ingest_columns` call site, handle never injected | ABSENT | DEAD-AS-WIRED |
| getBlobsV2 call | `crates/chain-core/src/engine.rs:106-117` -> `crates/engine-api/src/api.rs:459-473` | IDLE (iff core) | IDLE (iff core) |
| Fastpath sidecar output | empty `SubscriptionSet` (`api.rs:186-193`); `inject_tx` None | DEAD-AS-WIRED (output discarded) | DEAD-AS-WIRED (output discarded) |
| E8 EngineStream inject | p2p server attached, client deleted in 631994c | DEAD-AS-WIRED | ABSENT |

Even a complete getBlobs hit cannot open the DA gate, because the fastpath never produces
`DataAvailable`. `ColumnSidecar` on P2pStream is contract-only ([ADR-P2-11](../adr/ADR-P2-11.md)).

## Hop 14: serving the block to peers

None of the ways a peer could get the block from us works at HEAD (detail: [11](11-p2p-reqresp-and-sync.md#serving-blocks-and-columns),
[07 §4.7](07-storage.md#47-serve-and-the-storage-serve-window-shape-a)):

- **Gossip re-propagation.** Once ACCEPT is reported, gossipsub forwards the message to mesh
  peers. It is IDLE, because nothing is received.
- **Req/resp is a STUB.** It covers `beacon_blocks_by_range/2`, `beacon_blocks_by_root/2`,
  `beacon_blocks_by_head/1`, `data_column_sidecars_by_range/1` and `data_column_sidecars_by_root/1`.
  All of them answer code 3 "handler not ready", because `block_serve` is never attached
  (`services/p2p/src/host.rs:745-752`, `:836-842`).
- **The storage service's serve reads (E6 reads) are DEAD-AS-WIRED.** `WatchServeWindow` (the
  storage serve window) is LIVE (content static) at `earliest_available_slot = u64::MAX`, so the
  advertised window (Status `earliest_available_slot`) is empty; shape B serves no `StorageService`.
  See [serve-windows.md](../serve-windows.md) and
  [11](11-p2p-reqresp-and-sync.md#three-serve-windows-and-earliest_available_slot).
- **Re-publication from chain (E1 publish arm) is DEAD-AS-WIRED.** `request_publish`
  (`p2p_stream.rs:228`) has no caller.
- **Reachability.** Shape A does not publish the libp2p port; inbound peer reachability is not
  configured.

## Stale statements elsewhere

- `docs/running.md:370` assumes head-following over gossip. At HEAD p2p subscribes to no gossip
  topics; the plan schedules gossip wiring for a later stage.
- `docs/contracts.md:134` defines `DEFERRED_DA` as DA-only; an engine-unavailable deferral also
  returns it (`import.rs:582-615`).

## Planned changes

Planned, per [`plan/architecture.md`](../../plan/architecture.md); none of these changes is live at
`7d8833d`, and code that already exists but is unwired is marked as such:

- **Before S3** (§3.4): reconnect `kzg_tx` so column KZG moves off the validation task (§3.5);
  shorten the 12 s block-forward wait; hoist the redrive walks off the validation hot path.
- **S3** (§9.1): select the E1/E2 transport implementation (§2.3, §2.5). Add the real DA feed,
  the reconnected `kzg_tx`, publication of the advertised window, the backfill write path,
  `cc-wire` and the Loop A queue taxonomy (§3.4). The S3 exit gate requires a foreign-peer block
  to import end to end on Hoodi while the node holds head: the first point at which this whole
  lifecycle is meant to run.
- **E8** (§2.3): becomes a second method on `P2pEgress` (chain-core -> p2p) rather than an
  engine-container->p2p stream. That would carry fastpath sidecars to p2p sampling.
- **Columns and persist** (§4.3): inject an archive handle into `P2pStreamDeps` so the existing
  `ingest_columns` path (`p2p_stream.rs:713`, DEAD-AS-WIRED at HEAD) and its top-of-batch
  continuity bind (`archive_write.rs:131-154`, already built) run. The plan intends a full writer
  mailbox to surface `Backpressure` to import (policy A: block up to 2 s, then `Backpressure`;
  [ADR-R-02](../adr/ADR-R-02.md)). At HEAD the writer blocks instead.
