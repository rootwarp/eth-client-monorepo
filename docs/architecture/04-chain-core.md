# Chain core

> **As built at `7d8833d`** (`develop`, 2026-08-16). Status labels and diagram legend: [README](README.md#status-labels).
> Target design: [`plan/architecture.md`](../../plan/architecture.md) §1.3, §3, §4, §7.

`cc-chain-core` (`crates/chain-core`) owns the fork-choice `Store<P>`, the block import
pipeline, the read models other tasks serve from, the event ring, the chain half of `P2pStream`
and the liveness probe. Two binaries host it: `cc-chain` (shape A, `services/chain`) and
`cc-beacon-core` (shape B, `bin/beacon-core`). A block's end-to-end journey is in
[05-block-lifecycle.md](05-block-lifecycle.md); every thread and task is in
[13-concurrency-model.md](13-concurrency-model.md).

**The one fact to hold first:** an installed core exists only after a checkpoint sync (both
shapes) or a durable seed (shape B). Both shipped configs set `checkpoint_providers = []`
(`config/chain.toml:27`, `config/beacon-core.toml:23`) and the durable set is always empty at
HEAD, so **by default both shapes run core-absent**: no core thread, scheduler, slot-tick thread
or liveness sampler. RPCs that need the core thread answer `FAILED_PRECONDITION` with reason
`NOT_BOOTSTRAPPED` (`crates/chain-core/src/service.rs:52`). How a core gets installed:
[12 §3.1-3.2](12-boot-health-and-shutdown.md#31-shape-a-cc-chain-binds-first-then-bootstraps)
and [05 Other entry points](05-block-lifecycle.md#other-entry-points).

## 1. Status at a glance

"Core installed" means an operator set `checkpoint_providers` (or, shape B only, a durable seed
ran). Short code paths are relative to `crates/chain-core/src/`; `run.rs` is
`services/chain/src/run.rs`; `boot.rs` is `bin/beacon-core/src/boot.rs`.

| Mechanism | Code | A, default | B, default | A, core installed | B, core installed |
|---|---|---|---|---|---|
| Core thread, scheduler (I1), slot-tick thread (I2) | `core.rs` | ABSENT | ABSENT | LIVE | LIVE |
| Checkpoint sync (installs a core) | `services/chain/src/checkpoint_sync.rs:868-911`, `:1112-1170` | ABSENT (providers = []) | ABSENT (providers = []) | LIVE (once, at boot) | LIVE (once, empty store only) |
| Durable seed | `seed.rs:383-443` | ABSENT | IDLE (durable set always None) | ABSENT | IDLE (durable set always None) |
| Import pipeline up to the DA gate | `import.rs` | ABSENT | ABSENT | IDLE (no block producer) | IDLE (no block producer) |
| Import past the DA gate (STF, head, persist, events) | `import.rs`, fork-choice `on_block` | ABSENT | ABSENT | IDLE (no `DataAvailable` sender) | IDLE (no `DataAvailable` sender) |
| `pending_da` / `pending_engine` re-drive | `da.rs`, `pending_engine.rs` | ABSENT | ABSENT | IDLE | IDLE |
| I5 core thread -> EL (newPayload, fcU, FetchBlobs enqueue); fcU driver | `engine.rs`, `fcu_driver.rs` | ABSENT | ABSENT | IDLE (no import; fcU needs a first emit) | IDLE (no import; fcU needs a first emit) |
| engine-api upcheck driver + fastpath worker | `crates/engine-api/src/api.rs:173-195` | LIVE (upcheck: `eth_syncing` every slot; fastpath worker IDLE) | LIVE (upcheck: `eth_syncing` every slot; fastpath worker IDLE) | LIVE (upcheck: `eth_syncing` every slot; fastpath worker IDLE) | LIVE (upcheck: `eth_syncing` every slot; fastpath worker IDLE) |
| `HeadSnapshot` / `EpochContext` stores | `head.rs`, `epoch_context.rs` | LIVE (content static: zero) | LIVE (content static: zero) | LIVE (content static: anchor) | LIVE (content static: anchor) |
| State residency | `residency.rs` | ABSENT | ABSENT | IDLE (holds the anchor) | IDLE (holds the anchor) |
| Events task + ring (I4) | `events/` | IDLE (task runs; no event input, no in-tree subscriber) | IDLE (task runs; no event input, no in-tree subscriber) | IDLE (task runs; no event input, no in-tree subscriber) | IDLE (task runs; no event input, no in-tree subscriber) |
| I3 core thread -> events task | `import.rs:1125-1152` | ABSENT | ABSENT | IDLE (no producer input) | IDLE (no producer input) |
| `ApplyAttestations` | `apply_attestations.rs` | ABSENT (RPC answers `NOT_BOOTSTRAPPED`) | ABSENT (RPC answers `NOT_BOOTSTRAPPED`) | IDLE (no in-tree caller) | IDLE (no in-tree caller) |
| E1 session + `ChainView` | `p2p_stream.rs` | LIVE (content static) | ABSENT (no p2p points at B) | LIVE | ABSENT |
| E1 object / verdict | `p2p_stream.rs:738-848` | IDLE (p2p subscribes 0 topics) | ABSENT | IDLE (p2p subscribes 0 topics) | ABSENT |
| E1 publish + column, E2 DataAvailable | `p2p_stream.rs` | DEAD-AS-WIRED | ABSENT | DEAD-AS-WIRED | ABSENT |
| N1 blocks: persist via `ArchiveWrite` | `import.rs:988-990` | ABSENT | ABSENT (no core thread) | ABSENT (`archive = None`) | IDLE |
| N1 columns: persist via `ArchiveWrite` | `p2p_stream.rs:697-713` | ABSENT | DEAD-AS-WIRED (handle never injected) | ABSENT | DEAD-AS-WIRED (handle never injected) |
| Liveness sampler | `liveness.rs` | ABSENT | ABSENT | LIVE | LIVE |

ABSENT means not constructed or not run in that configuration. The core-installed
columns describe a configuration no shipped config uses; shape B is not deployed, so LIVE there
means LIVE once deployed and configured. `ImportBlock` is served with no in-tree caller
(operators only), hence IDLE. I3 and I5 exist whenever a core is installed, but no producer
input reaches them at HEAD. No shipped configuration pairs cc-p2p with cc-beacon-core; how p2p
attaches to shape B is not yet defined.

## Module map

Under `crates/chain-core/src/`: `core.rs` scheduler, `CoreHandle`, `core_loop`, SlotTick,
queries (sections 2-4); `service.rs` `ChainServiceImpl`, `install_core` and `lib.rs:32`
`ArchiveWriteHandle` (4, 16); `import.rs` and `tick.rs` clock and gossip disparity (5); `da.rs`,
`pending_engine.rs` (6-7); `engine.rs` `DirectEngine`, `fcu_driver.rs`, `invalidation.rs`
DEAD-AS-WIRED exit (7-8); `head.rs`, `epoch_context.rs` (9); `residency.rs` (10);
`events/{mod,ring,cursor,fanout}.rs` (11); `apply_attestations.rs` (12); `p2p_stream.rs` and
`ingest.rs` column decode (13); `liveness.rs` (14); `metrics.rs` (15). Bootstrap: `seed.rs` and
`services/chain/src/checkpoint_sync.rs`, covered in
[12 §3](12-boot-health-and-shutdown.md#3-boot-per-process).

## 2. One thread owns the Store

`spawn_core_thread_with_epoch` moves the `Store<P>` into an OS thread named `chain-core`
(`crates/chain-core/src/core.rs:1301-1321`); a spawn failure aborts the process
([ADR-P1-09](../adr/ADR-P1-09.md): one `Store`, owned by value, no lock). `core_loop` also owns
`Residency`, `PendingDa`, `PendingEngine`, the `FcuDriver` and the `last_engine_online` bit
(`core.rs:1632-1674`). Before spawning it publishes an initial `HeadSnapshot` and
`EpochContext`, both with sequence 0 (`core.rs:1272-1273`), so readers work immediately.

Each iteration (`core.rs:1679-1889`) re-sizes the attestation scheduler lane from the current
`EpochContext`, expires `pending_da` and `pending_engine` against `store.get_current_slot()`,
then `select()`s one item, dispatches it and replies on its oneshot when it has one
(`DataAvailable` and `SlotTick` have none). An empty scheduler parks the thread until
`LaneWake::notify`; empty with every producer permit dropped exits the loop (`core.rs:1697-1706`),
and `close_consumer` fails every queued waiter with `UNAVAILABLE` (`core.rs:487-499`, `:1891`).

**One `SlotTick`** (`core.rs:1949-2015`), in order: `on_tick(store, now)`; expire `pending_da`
and `pending_engine`; `DirectEngine::is_online()` (a 1 s `block_on` of local engine-api state, no
EL call); on an Offline->Online edge with parked blocks, re-drive `pending_engine`, then fcU
(section 7); `FcuDriver::on_slot` re-sends the last triple (section 8). The `chain-slot-tick`
OS thread sleeps to each genesis-aligned boundary and `blocking_push`es it (`core.rs:1902-1929`).

```text
 PRODUCER (tokio task unless noted)         ADMISSION (I1: CoreHandle)              SCHEDULER LANE
 ImportBlock RPC .......................... push_timeout 2 s .....................> import
 P2pStream BLOCK object (p2p dials) - - - - push_timeout 2 s - - - - - - - - - - -> import
 P2pStream DataAvailable (E2) x no sender   push_timeout 2 s                        import
 IsOptimistic RPC ......................... push_timeout 2 s .....................> query_p0
 Shuffling, Pubkeys, CanonicalRoots RPCs .. push_timeout 2 s .....................> query_p1
 GetValidatorRecords N2 (p2p dials) - - - - push_timeout 2 s - - - - - - - - - - -> query_p1
 ApplyAttestations RPC .................... push; when full, evicts oldest .......> attestation
 liveness sampler: Ping, sleep slot/4 ----- push_wait in a 4 s probe timeout -----> tick
 pre-drain hook: Shutdown ----------------- push_wait, no deadline ---------------> tick
 I2 OS thread chain-slot-tick: SlotTick --- blocking_push, waits on Condvar ------> tick
                                              |
                                              v
 +----------------------------------------------------------------------------------------------+
 | SharedScheduler = Mutex<Manager<ChainLane, CoreWork>> + Condvar "space" + LaneWake           |
 | order: tick > import > query_p0 > attestation > query_p1 (depths, full policy: section 3)    |
 +----------------------------------------------------------------------------------------------+
                                              |  select(): first non-empty scheduler lane wins;
                                              v  empty -> park until LaneWake.notify
 +----------------------------------------------------------------------------------------------+
 | OS thread "chain-core": owns Store<P> BY VALUE + Residency, pending_da, pending_engine,      |
 | FcuDriver, PeerDasAvailability. One item at a time; replies on the item's oneshot.           |
 +----------------------------------------------------------------------------------------------+
   |
   +-- I5 DirectEngine, block_on(deadline) ~~~~> EL :8551, IDLE: newPayload 8 s, fcU 8 s;
   |                                             FetchBlobs 1 s only enqueues fastpath work
   +-- [B] N1 ingest_block_blocking - - - - - -> storage-core writer P0, waits for commit; IDLE
   +-- I3 blocking_send - - - - - - - - - - - -> events task: mpsc(4096) -> ring; IDLE
   +-- I6 ArcSwap store =======================> HeadSnapshot, EpochContext; LIVE (content static)
                                                 ^ pointer loads only: GetHead, ChainView
```

Legend: `....>` served, no in-tree caller; `- - ->` IDLE; `x` DEAD-AS-WIRED; `---->` LIVE control
call (README legend; these producers exist only with an installed core); `====>` LIVE data
flow; `~~~~>` leaves the process (status in the label); `[B]` shape B only; I1-I6, E2, N1, N2
are edge ids. Everything from the scheduler down exists only with an installed core.

## 3. Scheduler lanes

The scheduler lane table is data in `cc-scheduler` (`crates/scheduler/src/chain.rs:121-157`);
slice order is the policy, first match wins. The fixed depths (tick 4; import, query_p0,
query_p1 64) are compile-time constants. The attestation depth is
`sized_from_validators(active, SLOTS_PER_EPOCH)` and is re-set from `EpochContext` at the top of
every core-loop iteration (`core.rs:1680`). No depth has a config key
(`crates/scheduler/src/config.rs:65-98`). FIFO drops the **new** item; LIFO evicts the **oldest**.

| # | Scheduler lane | Work items | Queue | Depth | Why (from the `LaneSpec`) |
|---|---|---|---|---|---|
| 1 | tick | `SlotTick`, `Ping`, `Shutdown` | FIFO, never shed | 4 | a dropped tick makes valid blocks look `future_slot` |
| 2 | import | `ImportBlock`, `ImportBlockGossip`, `DataAvailable` | FIFO | 64 | blocks import in order; `DataAvailable` re-drives a parked block |
| 3 | query_p0 | `Query{Head, IsOptimistic, StoreClock}`, test `BlockFor` | FIFO | 64 | head probes must not wait behind imports |
| 4 | attestation | `ApplyAttestations` | LIFO | `max(active*110/100/SLOTS_PER_EPOCH, 128)` | a later attestation is better information |
| 5 | query_p1 | `Query{CommitteeShuffling, ValidatorPubkeys, ValidatorRecords, CanonicalRoots}` | FIFO | 64 | serving reads are not evicted |

Admission per producer is in the section 2 diagram; push variants, Condvar and park mechanics
and drain are in
[13 §4.3](13-concurrency-model.md#43-sharedscheduler-one-mutex-one-condvar-one-parkunpark).
Import and query pushes not admitted within 2 s answer `RESOURCE_EXHAUSTED` (`core.rs:72`).
Chain-core's own rule: `apply_attestations` never waits; on a full attestation scheduler lane the
**oldest** queued batch is evicted and **its** waiter gets `RESOURCE_EXHAUSTED`
(`core.rs:1005-1028`). `select()` calls `note_idle()` under the same lock (`core.rs:447-455`),
so the `WorkerIdle` / `max_workers = 1` gate is INERT; the `Deferred` inbound class and
`cc_scheduler::ChainWork` are DEAD-AS-WIRED (this section owns that list; the `Manager::select`
chain is in [13 §4.1](13-concurrency-model.md#41-the-substrate)).

## 4. CoreHandle, ChainServiceImpl and core-absent mode

`ChainServiceImpl` (`crates/chain-core/src/service.rs:69-80`) holds the installed-core handle as
`Arc<RwLock<Option<CoreHandle>>>`, shared with `P2pStreamDeps`, so `install_core`
(`service.rs:138-140`) reaches live `P2pStream` sessions for object import without a reconnect
(views: section 13). Hosts keep the `CoreThread` in a `CoreJoinOwner` that seals installs once
drain starts (`run.rs:47-114`). `GetInfo` always answers.

| RPC | Path | Scheduler lane | Core-absent answer |
|---|---|---|---|
| GetHead | `HeadSnapshot` pointer load | none | `NOT_BOOTSTRAPPED` while the snapshot is still zero |
| SubscribeEvents | events task | none | served (empty ring) |
| P2pStream | session task, see section 13 | import | served; BLOCK -> `Ignore/Internal` |
| ImportBlock | `core.import_block` | import | `NOT_BOOTSTRAPPED` |
| ApplyAttestations | bound 128 checked first, then the installed core | attestation | `NOT_BOOTSTRAPPED` |
| IsOptimistic | proto-array only, never the EL | query_p0 | `NOT_BOOTSTRAPPED` |
| GetCommitteeShuffling, GetValidatorPubkeys (<=256), GetValidatorRecords (<=256) | head state, read on the core thread | query_p1 | `NOT_BOOTSTRAPPED` |
| GetCanonicalRoots (span <=4096) | start below finalized -> `FAILED_PRECONDITION` `BELOW_FINALIZED_RETENTION`; else per slot `get_ancestor(head, s).unwrap_or(head)` (`core.rs:1515-1538`): a slot with no ancestor answers the head root (fabricated; Planned) | query_p1 | `NOT_BOOTSTRAPPED` |

`Query{Head}` and `StoreClock` have no production sender. Only `P2pStream` and
`GetValidatorRecords` have an in-tree client (both from cc-p2p).

<a id="5-import-pipeline-inside-the-core"></a>
## 5. Import pipeline on the core thread

Both import entry points run `import_block_with_early` on the core thread
(`crates/chain-core/src/import.rs:349-632`); the gossip variant also passes an `EpochContext`
snapshot (unused: `_epoch_ctx`, `import.rs:667`) and an `early_accept` oneshot
(`core.rs:1756-1804`).

```text
 import scheduler lane item: ImportBlock (unary) or ImportBlockGossip (P2pStream)
   v
 [0] on_tick(store, wall clock)                (slot_tick_enabled hosts)
 [1] probe supplied root -- known ----------> DUPLICATE; [B] persist if not durable; no event
 [2] decode SSZ (Fulu only) -- error -------> INVALID_ARGUMENT
 [3] hash_tree_root check -- mismatch ------> INVALID_ARGUMENT
 [4] cheap checks -- fail ------------------> terminal verdict, no STF, no early ACCEPT
       parent header, parent state (residency replay), future slot (+500 ms),
       too old, finalized descent, proposer index, proposer BLS
 [5] early ACCEPT oneshot, gossip path - - -> Verdict Accept/Valid to p2p
 [6] pubkey cache top-up on the parent state
 [7] on_block_with_context (crates/fork-choice/src/on_block.rs)
       idempotency: already imported -> Imported; partial import resumed
       DA gate (PeerDAS set) -- root not marked ----> park pending_da (64, age > 4 slots),
         |                                            BLOCK_IMPORTED(DEFERRED_DA), DEFERRED_DA
         |                                            NOTE: at HEAD every block stops here
       parent re-check -- missing ------------------> UNKNOWN_PARENT
       slot re-check -- ahead of store -------------> INVALID future_slot
       finality re-check -- not descended ----------> INVALID
       state_transition on a parent clone; newPayload inside (block_on, 8 s)
         |-- any engine-api error ------------------> park pending_engine (64, age > 8 slots),
         |                                            DEFERRED_DA execution_engine_unavailable
         |-- EL INVALID / INVALID_BLOCK_HASH -------> INVALID
         |-- any other STF error -------------------> INVALID (late Reject if Reject-class)
         v   VALID -> Valid; SYNCING / ACCEPTED -> Optimistic; integrate proto-array
 [8] finish_imported: body ring + Scratch -> get_head -> settle residency
       -> HeadSnapshot publish -> [B] N1 ArchiveWrite persist -> events (blocking_send);
       on a finality change: prune_on_finalized, then FINALIZED_CHECKPOINT
 [9] core_loop, any Ok outcome (any verdict, incl. DUPLICATE / DEFERRED_DA / INVALID):
       FetchBlobs if a trigger exists -> EpochContext -> fcU -> reply on the oneshot
```

Arrows are branches of one call on the core thread, not RPCs; `- - ->` is IDLE (gossip path);
`[B]` shape B only. Stages 0-6 sit inside hop [5] of
[05](05-block-lifecycle.md#hops-4-12-inside-chain-core) (admission is hop [4]); stage 7 = hops
[6]-[8]; stage 8 = hops [8]-[11]; stage 9 = hop [12].

| Stage | Code | Notes |
|---|---|---|
| 0 clock | `core.rs:1711-1713`, `tick.rs:124-126` | only when `slot_tick_enabled` (both production hosts set it) |
| 1 dedup | `import.rs:369-396`, `:879-892` | [ADR-P1-10](../adr/ADR-P1-10.md): probes the caller-supplied root before decoding |
| 2-3 decode, root | `import.rs:398-410`, `:1204-1211` | `ImportBlockRequest.fork` is ignored |
| 4 cheap checks | `import.rs:415-435` -> `:663-791` | gossip path always `VerifyIndividual`; unary uses `CoreConfig.verify` |
| 5 early ACCEPT | `import.rs:437-441` | before the STF, so p2p can forward within budget |
| 7 fork choice | `crates/fork-choice/src/on_block.rs:211-276` | DA gate is the first precondition after the idempotency checks (`:211-234`); an engine-api error -> deferral, store unmutated |
| 7 newPayload | `crates/state-transition/src/block/execution_payload.rs:82` | the one call site ([ADR-P3-03](../adr/ADR-P3-03.md)) |
| outcome map | `import.rs:488-631` | errors classify onto gossip verdicts ([ADR-P1-07](../adr/ADR-P1-07.md)) |
| 8 finish | `import.rs:895-1023`, `:1036-1098` | snapshot `:973` before persist `:989` before events `:992`; prune `:1085` |
| 9 tail | `core.rs:1733-1754` | RPC latency includes FetchBlobs and the fcU round trip |

Failure ordering: a persist error at stage 8 returns after fork choice, residency and the
snapshot moved; no events, no proto-array prune, no fcU. A retry hits DUPLICATE, which persists
if missing but never emits `BLOCK_IMPORTED`. In shape B the first child of a checkpoint anchor
would, as wired, fail that persist with `INVALID_ARGUMENT` (the anchor is never written); moot
while the DA gate is shut.

## 6. DA gate and `pending_da`

The gate is `PeerDasAvailability`: pure set membership over at most 256 roots, with no zero-blob
exemption (`crates/fork-choice/src/da_seam.rs:69`, `:193-197`). A root becomes available only
through `mark_available`, called by `CoreCommand::DataAvailable` (`core.rs:2064-2136`) or by
durable seed replay. `DataAvailable` arrives only on the `P2pStream` DataAvailable arm, and
cc-p2p never sends it. **The gate therefore never opens at HEAD**: every block that passes stage
4 parks with verdict `DEFERRED_DA` and is dropped once it is more than 4 slots old (`age > 4`,
`crates/chain-core/src/da.rs:184-204`), or earlier when 64 newer parked blocks evict it.

`pending_da` holds the arrival SSZ (bound 64, `da.rs:43-49`). Its re-drive passes no
`pending_engine`, so a block that clears DA and then meets an engine-api failure is not parked
(`core.rs:2129`). A DA-deferred block with blob commitments yields a `BlockBranchTrigger`; the
core thread then calls `DirectEngine::fetch_blobs`, a `block_on` of at most 1 s that only
enqueues fastpath work whose output is discarded (`engine.rs:105-117`). Across processes:
[10-p2p-gossip-and-das.md](10-p2p-gossip-and-das.md).

## 7. `pending_engine` and optimistic import

Any engine-api failure (timeout, transport, auth, decode) maps to `EngineError::Transport`
(`engine.rs:199-201`), which fork choice turns into `Deferred(ExecutionEngineUnavailable)`
without touching the store. The block parks in `pending_engine` (bound 64, dropped at `age > 8`
slots, `crates/chain-core/src/pending_engine.rs:26-29`, `:159-180`), a separate map from
`pending_da` by [ADR-P3-05](../adr/ADR-P3-05.md). On the wire it answers `DEFERRED_DA` with
reason `execution_engine_unavailable`; no event is published. Re-drive happens only at a
`SlotTick` that observes an Offline->Online edge of `DirectEngine::is_online()`
(`last_engine_online` starts false), oldest-first, then fcU (`core.rs:1980-2007`, `:2162-2219`).
Engine failures defer the block rather than reject it; re-drive depends on an EL health
transition. The exception is a residency replay (section 10), where an engine-api error rejects.

Optimistic status: `SYNCING` / `ACCEPTED` import as `ExecutionStatus::Optimistic`
(`crates/fork-choice/src/execution_status.rs:63-69`; [ADR-P3-10](../adr/ADR-P3-10.md)).
`HeadSnapshot.is_optimistic` comes from `is_optimistic_node` after each `get_head`;
`IsOptimistic` reads proto-array only. DEAD-AS-WIRED: `safe_slots_to_import_optimistically`
(logged only), the invalidation walk ([ADR-P3-11](../adr/ADR-P3-11.md)) and the
justified-INVALIDATED exit in `invalidation.rs`. An EL `INVALID` fails only that one import.
EL side: [08 §4](08-execution-engine.md#4-directengine-on-the-core-thread).

## 8. forkchoiceUpdated driver

`FcuDriver` (`crates/chain-core/src/fcu_driver.rs:117-321`) builds the triple from proto-array
`execution_block_hash` of head, justified (safe) and finalized, assigns a monotonic sequence, and
sends through `DirectEngine::emit` (8 s). Emission points: after any `Ok` import outcome, an `Ok`
`ApplyAttestations` batch, a `DataAvailable`, a `pending_engine` re-drive (`core.rs:1752`,
`:1801`, `:1817`, `:1854`, `:2004`), and the per-slot floor on `SlotTick`, which re-sends the
last delivered triple (`fcu_driver.rs:256-287`). `Ok` means any verdict, including DUPLICATE,
DEFERRED_DA and INVALID; only a gRPC error (decode, root mismatch, persist, `get_head`) skips
fcU. The floor re-sends only `last_state`, set by the first successful emit, and nothing emits
at spawn, so with a core installed but no import, attestation or `DataAvailable` input, **no
fcU is ever sent**.

## 9. Read models: `HeadSnapshot` and `EpochContext`

Two `ArcSwap` stores, written only by the core thread and read by pointer load
([ADR-P1-09](../adr/ADR-P1-09.md)):

| Model | Written when | Read by | Code |
|---|---|---|---|
| `HeadSnapshot` (head, checkpoints, `is_optimistic`, `sequence`) | spawn; every `finish_imported`; `ApplyAttestations` head recompute | `GetHead`; P2pStream head watcher (10 ms poll) | `crates/chain-core/src/head.rs:14-85` |
| `EpochContext` (proposer lookahead + pubkeys, active count, genesis, slot timing) | spawn; after an Ok import or re-drive when the head-state epoch changes or the sequence is still 0 | `ChainView` drivers; attestation scheduler lane sizing | `crates/chain-core/src/epoch_context.rs:18-90`; `core.rs:2222-2240` |

The snapshot is published **before** the events of the same import (`import.rs:952-978`);
`current_epoch_target_root` and `dependent_root` are always `Root::ZERO`. `EpochContext` only
advances with the head, so a head that never moves keeps serving the anchor epoch's lookahead.

## 10. State residency

`Residency` keeps at most four pinned post-states by role (Head, Anchor, EpochBoundary, Scratch)
plus a 64-block body ring ([ADR-P1-12](../adr/ADR-P1-12.md);
`crates/chain-core/src/residency.rs:32-44`). Config: `max_resident_states`, `body_ring_capacity`.

- `record_imported_body` rings the body and pins Scratch; `settle_after_head` then pins Head =
  fork-choice head, EpochBoundary on a boundary slot, Anchor = finalized root, clears Scratch and
  prunes `store.block_states` to the pinned set (`residency.rs:294-377`).
- `ensure_in_store` (import stage 4) replays a missing parent state from the nearest resident
  ancestor through the body ring; deeper than the ring is a **reorg gap** -> `INVALID`
  (`residency.rs:213-287`). Replay runs `state_transition` with the store's `DirectEngine`, so as
  wired a replay would re-issue newPayload for every replayed block. Any replay failure,
  including a `DirectEngine` transport error, is reported the same way: verdict `INVALID`, reason
  `reorg gap: ...` (`residency.rs:264-283`, `import.rs:686-693`), which the gossip path maps to
  `Reject/Invalid` (`p2p_stream.rs:1043-1048`). As wired, an EL outage during a replay would
  reject the block instead of deferring it. This is latent while no import passes the DA gate.

## 11. Event ring and subscriber contract

```text
 core thread (the only producer; IDLE at HEAD)
   | I3 blocking_send; the core thread waits while the channel is full
   v
 mpsc(ring_capacity = 4096)              SubscribeEvents{cursor} handler
   |                                       | command mpsc(64)
   v                                       v
 +----------------------------------------------------------------------------------------+
 | events task: one tokio task, select! biased (commands before events)                   |
 |                                                                                        |
 | event arm:     payload <= 10 MiB, else drop + log                                      |
 |                seq = next_seq++  (assigned here, in receive order)                     |
 |                push ring; evict oldest while len > 4096 or bytes > 64 MiB              |
 | subscribe arm: validate cursor -> copy replay from ring -> insert subscriber           |
 |                (one task turn: no gap, no duplicate between replay and live)           |
 +----------------------------------------------------------------------------------------+
   | I4 broadcast: try_send into each subscriber queue
   |
   +-- Ok:     subscriber mpsc(256) -> bridge task mpsc(16) ....> SubscribeEvents stream
   |           (one bridge task per subscriber; metadata x-cc-chain-session-id)
   +-- Full:   status RESOURCE_EXHAUSTED "slow consumer", subscriber removed
   +-- Closed: subscriber removed quietly
```

Legend: `....>` served, no in-tree subscriber (the E7 consumer was DELETED in `78a90e1`). Status:
the events task runs in both shapes and modes but is IDLE: I3 (fed only by an installed core's
import and ApplyAttestations paths) and I4 (no in-tree subscriber) are both IDLE.

`EventsHandle::spawn` (`crates/chain-core/src/events/mod.rs:402-435`) runs the task in both
modes; neither host shuts it down. Bounds (`events/mod.rs:90-110`) are configurable via
`event_ring_events`, `event_ring_bytes`, `subscriber_queue_capacity`. A slow subscriber is cut;
a slow events task instead blocks the core thread through `blocking_send` (`import.rs:1125-1152`).
Cursor contract ([ADR-P1-11](../adr/ADR-P1-11.md); `events/cursor.rs:19-67`), checks in order:

| Cursor condition | Result |
|---|---|
| none | live only, no replay |
| `session_id` differs from this process's random id | `FAILED_PRECONDITION` `CURSOR_UNKNOWN_SESSION` |
| `seq` ahead of the stream (or non-zero on an empty stream) | `INVALID_ARGUMENT` |
| `seq + 1` older than the ring front | `FAILED_PRECONDITION` `CURSOR_TOO_OLD` |
| retained entry at `seq` has a different slot/root | `INVALID_ARGUMENT` |
| otherwise | replay from `seq + 1`, then live |

Kinds produced: `BLOCK_IMPORTED` (first payload byte `IMPORTED` or `DEFERRED_DA`), `HEAD`,
`CHAIN_REORG`, `FINALIZED_CHECKPOINT` (with the 240-byte fork-choice scalars). `DATA_COLUMN`
has no producer. The session id is random per process, so a restart invalidates every cursor.

## 12. ApplyAttestations

Batches over 128 are rejected on the gRPC worker (`service.rs:253-275`); the rest ride the LIFO
attestation scheduler lane. Each item decodes as `IndexedAttestation` and goes through
`on_attestation(is_from_block = false)` with no BLS or committee checks. If any applied: one
`get_head`, a snapshot publish and `HEAD` / `CHAIN_REORG` events (`apply_attestations.rs:101-117`,
`:130-205`). `core_loop` then emits fcU after every `Ok` batch, even one where nothing applied
(`core.rs:1815-1818`). A failed trailing `get_head` is logged and per-item results are still
returned. No in-tree caller: IDLE.

## 13. P2pStream, server side

The chain serves the bidi stream cc-p2p dials (E1). Client half:
[11 Chain-stream client](11-p2p-reqresp-and-sync.md#chain-stream-client-e1-n2); message
contract: [03 P2pStream anatomy](03-internal-contracts.md#4-p2pstream-anatomy).

- Sessions: at most 8, else `RESOURCE_EXHAUSTED` `STREAM_SESSION_LIMIT`; outbound `mpsc(1024)`
  each (`crates/chain-core/src/p2p_stream.rs:58-61`, `:235-258`). The loop (`:460-542`) is a
  biased `select!`; inbound is **serial**, so one slow import stalls that session.
- `ChainView` ([ADR-P2-05](../adr/ADR-P2-05.md)): kind 4 Full on Hello, 1 per wall-clock slot,
  2 when `EpochContext.sequence` changes to a value above 0, 3 when `HeadSnapshot.sequence`
  changes; all `ArcSwap` loads (`p2p_stream.rs:276-329`, `:545-561`). The slot driver is silent
  while `genesis_time` is unset, so a core-absent chain sends only the Hello view (defaults).
- Latent (traced, not run): kind 1 and 3 views carry an empty `proposer_lookahead` /
  `proposer_pubkeys` (`p2p_stream.rs:395-430`), and p2p replaces its whole view on every message.
  The spawn-time `HeadSnapshot` and `EpochContext` carry sequence 0 (`core.rs:1272-1273`,
  `:1627`), which neither driver reports, so as wired a session opened before `install_core`
  would get no lookahead until an import advances the `EpochContext` or the session reconnects.
  Latent; unreachable at HEAD because no gossip topics are subscribed.
- Objects ([ADR-P2-04](../adr/ADR-P2-04.md)): non-BLOCK -> `Ignore/AlreadyKnown`; BLOCK with no
  installed core -> `Ignore/Internal`; else `import_block_for_gossip`, early ACCEPT preferred, a
  second `Reject/Invalid` on a late reject (`p2p_stream.rs:738-848`); an `Err` after early ACCEPT
  also sends a second verdict (`:846`). Some internal import failures surface to p2p as
  backpressure.
- Columns: `P2pStreamDeps.archive` is `None` in both hosts (`service.rs:112-117`), so every
  well-formed column answers `Ignore/Internal` (`p2p_stream.rs:697-711`); malformed SSZ answers
  `Ignore/Invalid` (`:680-695`). `request_publish` (`:228`) has no caller.

## 14. Liveness sampler

[ADR-R-04](../adr/ADR-R-04.md): liveness is a deadline-bounded no-op through the core thread
(`crates/chain-core/src/liveness.rs`). One tokio task, started only after a core is installed,
calls `CoreHandle::ping` and sleeps `slot/4` (3 s) after each sample; the `Ping` rides the
never-shed tick scheduler lane, its `push_wait` bounded only by the 3999.6 ms probe timeout
(`liveness.rs:86-96`). Three consecutive misses clear `local_ready`, three successes restore it
(`liveness.rs:112-115`). Both hosts call `mark_ready()` alongside the bind, as soon as
`serve_with_options` hands over the local-ready handle just before binding
(`crates/bootstrap/src/serve.rs:260-266`; `run.rs:421`, `boot.rs:540`;
[12 §3.1](12-boot-health-and-shutdown.md#31-shape-a-cc-chain-binds-first-then-bootstraps)), so
"chain healthy" means bound, not bootstrapped. Liveness flips aggregate health; nothing in this
repository restarts a process on that signal. Inputs, timeline and the N=3 rationale:
[12 §4.4](12-boot-health-and-shutdown.md#44-liveness-sampler-a-no-op-through-the-core).

## 15. Metrics

`ChainMetrics::register` (`crates/chain-core/src/metrics.rs:339`) exports on `:9101`. Names are
registration names; prometheus_client appends `_total` to counters and the unit to histograms
(for example `cc_chain_import_seconds{stage}`, `cc_chain_import_total{result}`).

| Area | Series |
|---|---|
| Scheduler | `cc_chain_lane_queue_depth{lane}`, `cc_chain_import_queue_depth`, `cc_chain_import_rejected_backpressure` |
| Import | `cc_chain_import{stage}` (seconds), `cc_chain_import{result}` (counter), `cc_chain_process_block`, `cc_chain_process_block_local`, `cc_chain_import_root_mismatch`, `cc_chain_budget_exceeded{op}` (op=block from `finish_imported`; op=epoch never fed), `cc_chain_pubkey_cache_len`, `cc_chain_validators_len` (M13, import stage 6) |
| Parked blocks | `cc_chain_da_pending_occupancy`, `cc_chain_da_pending_dropped`, `cc_chain_da_available_occupancy`, `cc_chain_pending_engine_occupancy`, `cc_chain_pending_engine_dropped` |
| Head | `cc_chain_head_slot`, `cc_chain_finalized_epoch`, `cc_chain_is_optimistic`, `cc_chain_optimistic_nodes`, `cc_chain_resident_states`, `cc_chain_body_ring` |
| Events | `cc_chain_event_publish_dropped`, `cc_chain_event_payload_rejected`, `cc_chain_event_buffer_bytes_bound` (cc-chain only) |
| Bootstrap | `cc_chain_bootstrap_attempts{provider,result}`, `cc_chain_state_hash_tree_root{path}` (checkpoint sync only, `services/chain/src/checkpoint_sync.rs:880-900`, `:1079`) |
| Liveness | `cc_core_liveness_rtt`, `cc_core_liveness_parked`, `cc_core_liveness_deadline` |
| DEAD-AS-WIRED (never fed) | `cc_chain_event_buffer_occupancy`, `cc_chain_event_buffer_bytes`, `cc_chain_subscribers` (only a test feeds them), `cc_chain_engine_call`, `cc_chain_process_epoch`, `cc_chain_process_slots`, `cc_chain_optimistic_transitions`, `cc_chain_valid_became_invalid`, `cc_chain_invalidated_nodes`, `cc_chain_justified_invalidated` (only `invalidation.rs:71`, no production caller); `cc_chain_head_lag_slots` is always 0 |

Latency budgets: `BLOCK_BUDGET_SECS` = 0.4 s and `EPOCH_BUDGET_SECS` = 1.0 s
(`metrics.rs:39-42`). `PROCESS_BLOCK_BUCKETS` has a bound at exactly 0.4 and
`PROCESS_EPOCH_BUCKETS` one at exactly 1.0 (`:53-60`); `AUX_DURATION_BUCKETS` reuses the block
list, so the histograms cannot drift (`:65`). Per [ADR-P1-15](../adr/ADR-P1-15.md) a latency SLO
is read as a bucket fraction, `fraction(le=L) >= 0.95`, never via `histogram_quantile`. At HEAD
`cc_chain_process_epoch` is DEAD-AS-WIRED (`observe_process_epoch`, `metrics.rs:888`, has no
production caller), so the epoch budget cannot be measured even with a core installed.

DirectEngine is built with `finish(None)`, so no
`cc_engine_*` series come from the process that drives the EL. The method label guard lists 7 of
11 RPCs in both hosts (`run.rs:293-301`, `boot.rs:49-57`); `P2pStream`, `GetCommitteeShuffling`,
`GetValidatorPubkeys` and `GetValidatorRecords` record as `unknown`.

## 16. The two hosts

| Aspect | `cc-chain` (shape A) | `cc-beacon-core` (shape B) |
|---|---|---|
| Host code; status | `services/chain/src/run.rs:307-536`; deployed (compose), TRANSITIONAL | `bin/beacon-core/src/boot.rs:397-577`; no compose service, TRANSITIONAL (target) |
| gRPC | `ChainService` :9001 (+ health, reflection) | same; no `StorageService` |
| Seeding order | bind first; boot task runs checkpoint sync, then `install_core` | before bind: durable set -> `seed_from_durable`, else providers -> checkpoint sync, else core-absent |
| `CoreConfig.archive` | `None`: never persists (`run.rs:368-378`) | `Some(ArchiveWriter)` (`boot.rs:428`, `:448`), N1 IDLE (iff core) |
| Other in-process units | events task, DirectEngine; liveness after install | same + storage-core writer (no StorageService, prune, replay) |

Both set `slot_tick_enabled: true`, build `DirectEngine` with `finish(None)`, and shut the core
thread down in the pre-drain hook with `CoreThread::shutdown_and_join`, one 2 s envelope
(`core.rs:1181-1224`; budgets:
[12 §5.2](12-boot-health-and-shutdown.md#52-budgets-against-stop_grace_period-5s)). The
`chain-slot-tick` thread is detached (its handle is dropped when the spawn function returns,
`core.rs:1286`) and exits on its next `blocking_push` after the core thread closes the scheduler.

Shutdown normally fits the 5 s grace period; with an installed core the worst case is
unbounded, because `begin_shutdown`'s `push_wait` has no deadline (`core.rs:416-427`;
[12 §5.2](12-boot-health-and-shutdown.md#52-budgets-against-stop_grace_period-5s)).

Shape B persist is synchronous on the core thread: `ingest_block_blocking` does a `blocking_send`
into writer P0 (bound 32) and waits for the commit. The trait documents Backpressure; the
implementation blocks without a deadline. No test drives the production persist path (core
thread + `ingest_block_blocking` with a real writer). See [07-storage.md](07-storage.md). Seed
replay runs the STF and so calls the EL ([ADR-R-06](../adr/ADR-R-06.md)). Shape B does not yet
bound its store or order EL readiness before seed replay.

## Failure handling

| Condition | Effect | Code |
|---|---|---|
| core or slot-tick thread spawn fails; core scheduler config invalid | `process::abort` | `core.rs:1317-1321`, `:1925-1928`, `:346-350` |
| checkpoint fetch or verify fails, or bad `checkpoint_root` (A) | `exit(1)` after bind | `run.rs:433-439`, `:490-493` |
| local-ready handle dropped (A) | `exit(1)` | `run.rs:409-415` |
| seed error (incl. head mismatch) or checkpoint error (B) | `run()` returns `Err` before bind | `boot.rs:473`, `:506` |
| engine-api error on newPayload | park in `pending_engine` (section 7) | `import.rs:582-615` |
| parent state not resident, or its replay fails | `INVALID` `reorg gap`, `reorg_gaps` counter | `residency.rs:213-283` |
| fcU error | warn log only | `core.rs:2033-2035` |
| events channel closed | `cc_chain_event_publish_dropped` + error log | `import.rs:1140-1150` |
| persist error (B) | state moved; no events, no prune, no fcU (section 5) | `import.rs:989` |
| ApplyAttestations trailing `get_head` fails | logged; per-item results returned | `apply_attestations.rs:101-114` |

## Where docs and comments disagree with the code

| Statement | Location | At HEAD |
|---|---|---|
| Event ring "1024 events" | [contracts.md](../contracts.md) line 226 | 4096 events + 64 MiB byte ceiling (`events/mod.rs:90`, `:95`) |
| ApplyAttestations `RESOURCE_EXHAUSTED` = "Core command channel full after `send_timeout`" | [contracts.md](../contracts.md) line 199 | no wait; LIFO evicts the oldest batch, whose waiter gets it (`core.rs:1005-1028`) |
| cc-beacon-core "Does not construct `ArchiveWriter`" | [s2-rollback.md](../s2-rollback.md) line 129 | constructs and injects it (`boot.rs:428`, `:448`, since `d3d7f60`) |
| "Restore is not deleted (S2-J-02)" | `services/chain/src/main.rs:6` | deleted in `60c6200` |
| DirectEngine "used by the core thread, restore, and checkpoint seed" | `engine.rs:41` | restore deleted in `60c6200` |
| formula duplicated "so `cc-chain` does not depend on `cc-engine-api`" | `liveness.rs:9-12` | both depend on it (`crates/chain-core/Cargo.toml:22`, `services/chain/Cargo.toml:30`) |

## Planned changes

From [`plan/architecture.md`](../../plan/architecture.md); none of these is built.

- Planned (§1.3): `checkpoint_sync.rs` moves from `services/chain` into chain-core; today
  cc-beacon-core imports it from `cc_chain`.
- Planned (§3.1): per-work-type metrics derived from one enum; today labels are per scheduler
  lane and `ChainWork` is unused. Not planned for chain-core: §3.2 and §3.7 keep
  `max_workers = 1` for the core scheduler, so the WorkerIdle -> deferred -> new chain in
  `Manager::select` (`crates/scheduler/src/manager.rs:249-280`) stays INERT here by design; the
  plan applies worker counts to Loop A (gossip, §3.4).
- Planned (§4.1, §4.3): `cc-beacon-core` replaces the shape A chain and storage containers (today
  shape B is not deployed); chain-core ingests columns through `ArchiveWrite` (today the handle
  is never injected); the `GetCanonicalRoots` `unwrap_or(head)` fabrication
  (`core.rs:1530-1535`) is triaged.
- Planned (§7.2, §7.3): a parked core thread gets restarted once aggregate health goes
  NOT_SERVING (nothing acts on that signal yet; the plan's one-slot / two-miss numbers are
  superseded by [ADR-R-04](../adr/ADR-R-04.md)); `cc_chain_event_buffer_*` are re-documented as
  API-consumer signals.
- Open: a `DataAvailable` producer in cc-p2p. At HEAD p2p subscribes to no gossip topics; the
  plan schedules gossip wiring for a later stage.
