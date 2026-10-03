# Concurrency model

> **As built at `7d8833d`** (`develop`, 2026-08-16). Status labels and diagram legend: [README](README.md#status-labels).
> Target design: [`plan/architecture.md`](../../plan/architecture.md) §2.1-§2.2, §3, §7.2.

This page lists every runtime, OS thread, blocking-pool site and long-lived task in each
process, the `cc-scheduler` mechanics behind the core thread, every bounded queue on the hot
paths, and the places where synchronous work runs inside async code. Component behaviour is in
the owning docs: [04-chain-core.md](04-chain-core.md) (core thread, scheduler lanes, events),
[09-p2p-host-and-discovery.md](09-p2p-host-and-discovery.md) (swarm, supervisor, p2p channels),
[07 §4.3](07-storage.md#43-the-single-writer-and-its-mailbox) (writer and its writer classes)
and [08-execution-engine.md](08-execution-engine.md) (EngineApi transport lanes). In-process
edge ids I1..I10 are defined in [15-glossary.md](15-glossary.md#in-process-edges-i1i10).

**Three facts to hold first.**

1. **By default no core is installed in either shape (core-absent).** Both shipped configs set
   `checkpoint_providers = []` (`config/chain.toml:27`, `config/beacon-core.toml:23`), and the
   durable set is always empty, so the `chain-core` and `chain-slot-tick` OS threads, the core
   thread's scheduler and the liveness sampler are never created. Rows marked **(iff core)**
   describe what runs once a checkpoint sync or a durable seed installs one
   ([README](README.md#status-labels) rule 2).
2. **Nothing enters the gossip hot path.** At HEAD p2p subscribes to no gossip topics; the plan
   schedules gossip wiring for a later stage. Nothing in-tree calls `ImportBlock` either, so the
   backpressure chain in section 5 is traced through code, not observed.
3. **An installed core makes no EL HTTP call at HEAD.** newPayload, fcU and getBlobs need an
   import, attestation or DataAvailable input (none has an in-tree producer); the fcU per-slot
   floor only re-sends a triple one of them primed
   (`crates/chain-core/src/fcu_driver.rs:143`, `:256-286`).

## 1. Status at a glance

| Unit | Edge | Shape A | Shape B |
|---|---|---|---|
| One multi-thread tokio runtime per binary | -- | LIVE | LIVE |
| OS threads `chain-core`, `chain-slot-tick` | I2 | LIVE (iff core) | LIVE (iff core) |
| Core thread's scheduler (`SharedScheduler`) and its tick scheduler lane | I1, I2 | LIVE (iff core) | LIVE (iff core) |
| import / query_p0 / attestation / query_p1 scheduler lanes | I1 | IDLE (iff core; producers: RPCs with no in-tree dialer, the IDLE E1 object arm, N2) | IDLE (iff core; producers: RPCs with no in-tree dialer) |
| `WorkerIdle` gate (`max_workers = 1`) | I1 | INERT | INERT |
| `Deferred` inbound class; `cc_scheduler::ChainWork` | -- | DEAD-AS-WIRED | DEAD-AS-WIRED |
| DirectEngine `block_on` from the core thread | I5 | `is_online` (local state read, no HTTP): LIVE (iff core); fcU, newPayload, getBlobs: IDLE (the fcU floor re-sends only a previous triple, and nothing primes it) | same |
| Core thread -> events task channel and ring | I3, I4 | IDLE (no event producer runs) | IDLE |
| `HeadSnapshot` / `EpochContext` read models | I6 | LIVE (content static: zero defaults core-absent, the anchor with a core; the I6 writer runs iff core) | same |
| p2p swarm task and its control channels | I8 | LIVE | ABSENT |
| p2p gossip ingress chain (`gossip` -> `chain_out` -> `Ipc`) | I8, I9 | IDLE | ABSENT |
| `cc-kzg-verify-{i}` OS threads | -- | DEAD-AS-WIRED (self-terminating) | ABSENT |
| storage-core writer task | I7 | LIVE (P2 chunks from prune) | IDLE (its only producer is N1) |
| Synchronous redb on tokio workers | -- | LIVE (writer, prune, boot) | IDLE (writer has no input; boot scans run once) |
| Storage serve admission (4 permits) | -- | DEAD-AS-WIRED (E6 reads have no client) | ABSENT |
| Migrator `block_in_place` | -- | DEAD-AS-WIRED | ABSENT |

## 2. Runtime ground rules

- **One runtime per process.** Every service binary enters through `#[tokio::main]`; cc-chain
  and cc-beacon-core say `flavor = "multi_thread"` explicitly (`services/chain/src/main.rs:9`,
  `bin/beacon-core/src/main.rs:3`), the others get it by default (`services/p2p/src/main.rs:463`,
  `services/storage/src/main.rs:6`, `services/engine/src/main.rs:89`,
  `services/attestation/src/main.rs:77`, `services/beacon-api/src/main.rs:75`). No code sets
  `worker_threads` or `max_blocking_threads` outside tests, so pool sizes are tokio library
  defaults. No crate uses rayon. The four pure-consensus crates spawn nothing.
- **Named OS threads: two families only.** `chain-core` and `chain-slot-tick`
  (`crates/chain-core/src/core.rs:1302`, `:1907`), and `cc-kzg-verify-{i}`
  (`services/p2p/src/das/verify_pool.rs:1008`). A spawn failure of either chain thread calls
  `process::abort()`; a KZG worker spawn failure is logged and retried once unnamed.
- **Task spawning.** Named tasks go through `cc_bootstrap::spawn`, which is `tokio::spawn` plus a
  root tracing span and **no panic capture** (`crates/bootstrap/src/lib.rs:201-212`). The only
  supervisor is p2p's, over four tasks ([ADR-P2-13](../adr/ADR-P2-13.md)). Where a panic lands:
  section 2.2.
- **Hand-off primitives.** Producers reach the core thread through a `std::sync::Mutex` +
  `Condvar` + `park`/`unpark` scheduler (section 4). The core thread publishes read models
  through `ArcSwap` (`HeadSnapshot`, `EpochContext`), so `GetHead` and `ChainView` never touch
  it. Replies are tokio `oneshot`s. Other hand-offs are tokio `mpsc`, `broadcast` and `watch`
  channels plus `Semaphore` in-flight caps; the KZG pool's own queue is a `Mutex<VecDeque>` +
  `Condvar`.

### 2.1 Blocking-pool sites

`spawn_blocking` / `block_in_place` sites in production code (`core.rs:3060` is test-only and
excluded):

| Site | Process | Work | Status |
|---|---|---|---|
| `crates/chain-core/src/core.rs:1206` | chain, beacon-core | OS join of `chain-core` at shutdown | LIVE (iff core) |
| `crates/chain-core/src/seed.rs:462` | beacon-core | `apply_durable_seed` replay, which itself `block_on`s newPayload | IDLE (durable set always empty) |
| `crates/engine-api/src/fastpath/cells.rs:211` | chain, beacon-core, engine container | `compute_cells` after a getBlobsV2 hit ([ADR-P3-12](../adr/ADR-P3-12.md)) | IDLE (no trigger) |
| `crates/storage-core/src/replay.rs:298`, `:649` | storage service | snapshot prepare; load measurement | IDLE (replay skips while the split is 0) |
| `crates/storage-core/src/migrate.rs:150` (`block_in_place`) | storage service | migration under the split write lock | DEAD-AS-WIRED (no driver) |

### 2.2 Panic and exit policy

**Build.** `[profile.release]` sets `panic = "unwind"` and `overflow-checks = true`
(`Cargo.toml:222-223`), so arithmetic overflow panics in production builds. No crate installs a
panic hook. `catch_unwind` appears only in tests (`#[cfg(test)]` modules such as
`crates/types/src/block.rs:223`, `operations.rs:502`, `execution.rs:183`, and `tests/` files) and
in the TEST-ONLY `RustEthKzgBackend` setup loader (`crates/crypto/src/kzg/rust_eth_kzg.rs:80`).

| A panic in | Lands as | Code |
|---|---|---|
| p2p supervised tasks (`swarm`, `peer_manager`, `discovery`, `idle_worker`) | that task's policy: respawn, or fatal -> `exit(1)` | [09 §2.3](09-p2p-host-and-discovery.md#23-supervisor-and-panic-policy) |
| p2p `p2p-runtime`, `grpc-serve` (a `supervisor` panic included) | `RuntimeError::SwarmPanic` -> `exit(1)` | `services/p2p/src/service.rs:669-679`, `:913-967`; `services/p2p/src/main.rs:527` |
| storage-core writer task | guard task -> `ProcessExit::Os` -> `exit(1)` | `crates/storage-core/src/writer.rs:399-429`, `:345` |
| core thread (`chain-core`) | no exit; queued callers never get a reply | section 4.5 |
| any other tokio task; the detached OS threads `chain-slot-tick` (`core.rs:1286`) and `cc-kzg-verify-{i}` (`services/p2p/src/das/verify_pool.rs:1117-1124`) | silent: tokio drops a task panic and the task ends; a detached thread just ends | [12 §5.3](12-boot-health-and-shutdown.md#53-exits-that-are-not-sigterm) |

Deliberate `abort()` and `exit()` sites in production crates:

| Call | Site | Trigger | Status |
|---|---|---|---|
| `abort()` | `crates/chain-core/src/core.rs:349` | scheduler manager config error | LIVE (iff core) |
| `abort()` | `core.rs:1320`, `:1927` | `chain-core` / `chain-slot-tick` spawn failure | LIVE (iff core) |
| `abort()` | `crates/storage-core/src/writer.rs:770` | KeyCollision: different bytes under an existing block or column key | A: DEAD-AS-WIRED (no P0 or block/column `commit_meta` producer runs); B: IDLE (N1) |
| `exit(1)` | `services/chain/src/run.rs:437`, `:492` | bad `checkpoint_root`; no checkpoint provider succeeds | LIVE (opt-in; never reached with the default `checkpoint_providers = []`) |
| `exit(1)` | `run.rs:413` | local-ready handle lost | DEAD-AS-WIRED: unreachable, the handle is sent before bind (`crates/bootstrap/src/serve.rs:262-266`) |
| `exit(1)` | `services/p2p/src/main.rs:527` | `SwarmPanic` (table above) | LIVE |
| `exit(1)` | `crates/storage-core/src/boot.rs:464`, `open.rs:355`, `replay.rs:600` | the bundled Hoodi chain-config YAML fails to parse (a shipping bug) | A: LIVE (all three, at boot); B: LIVE (`open.rs:355` only) |
| `exit(1)` | `crates/storage-core/src/replay.rs:136` | replay divergence (`DivergenceExit::Os`) | IDLE (replay skips while the split is 0) |
| `exit(code)` | `crates/chain-core/src/invalidation.rs:33` | justified checkpoint invalidated (`process_exit`) | DEAD-AS-WIRED (re-exported at `services/chain/src/lib.rs:105`; no production caller) |
| `exit(1)` | `crates/storage-core/src/resume.rs:55` | resume divergence (`ResumeExit::fire`) | TEST-ONLY: `fire` has no production caller; boot passes `ResumeExit::Os` (`boot.rs:532`) into the unused `_exit` parameter (`resume.rs:99`) |

What restarts a process after each exit: [12 §5.3](12-boot-health-and-shutdown.md#53-exits-that-are-not-sigterm).

## 3. Thread and task map

```text
 +----------------------------------------------+  +----------------------------------------------+
 | cc-chain [A]        tokio multi_thread       |  | cc-p2p [A]          tokio multi_thread       |
 | tasks: events; P2pStream epoch + slot        |  | supervised: swarm (ProcessFatal),            |
 |   drivers; per session (<= 8): session +     |  |   peer_manager, discovery (5 / 300 s),       |
 |   head watcher*; SubscribeEvents bridges;    |  |   idle_worker                                |
 |   boot task; engine-api upcheck + fastpath   |  | joined, a panic exits the process:           |
 | only if a core is installed (default: no):   |  |   p2p-runtime, grpc-serve, supervisor        |
 |   OS chain-core       owns Store<P>          |  | unsupervised, a panic is silent:             |
 |   OS chain-slot-tick  SlotTick, Condvar      |  |   gossip-validate (IDLE),                    |
 |   tokio liveness sampler: Ping, sleep slot/4 |  |   chain-stream-client + <= 1024 dispatch     |
 | blocking pool: compute_cells, core join      |  |   tasks, chain-publish-dispatch,             |
 +----------------------------------------------+  |   publish-bridge, chain-in-late-verdicts,    |
 +----------------------------------------------+  |   WatchServeWindow, status-epoch,            |
 | cc-storage [A]      tokio multi_thread       |  |   epoch-ticks, stub-reqresp,                 |
 | boot: redb open + resume scans inline        |  |   kzg-verify-pool bridge (exits at start),   |
 | writer task: redb commit + fsync inline      |  |   EngineStream sessions (none)               |
 |   + panic guard task (exit 1)                |  | library: libp2p handlers, discv5 tasks       |
 | prune task, 4 s interval        LIVE         |  | OS cc-kzg-verify-{i}, K = max(2, cores/2)    |
 | replay task, 2 s interval       IDLE         |  |   x no producer; as wired, exits at start    |
 | serve handlers: 4 permits, redb reads        |  +----------------------------------------------+
 |   inline; snapshot stream: 1 permit          |  +----------------------------------------------+
 | blocking pool: replay prepare, load (IDLE)   |  | cc-beacon-core [B]  tokio multi_thread       |
 | migrator: object only, block_in_place  x     |  | boot: redb open + scans inline, pre-bind     |
 +----------------------------------------------+  | events; P2pStream drivers; engine-api        |
 +----------------------------------------------+  |   upcheck + fastpath                         |
 | cc-engine [A]       tokio multi_thread       |  | storage-core writer + panic guard            |
 | 2nd engine-api: upcheck driver (LIVE),       |  | seed_from_durable, blocking pool (IDLE)      |
 |   fastpath worker (IDLE: no trigger)         |  | OS chain-core, chain-slot-tick only when     |
 +----------------------------------------------+  |   seeded or checkpoint-synced (default: no)  |
 +----------------------------------------------+  | local-ready task -> liveness sampler         |
 | cc-attestation, cc-beacon-api [A]            |  | not hosted: serve, prune, replay, migrator   |
 | common tasks only (GetInfo STUB)             |  +----------------------------------------------+
 +----------------------------------------------+
```

One box per OS process (cc-attestation and cc-beacon-api share one); every gRPC process also
runs the common tasks in the first row of section 3.1. `OS` = a named OS thread outside tokio's
pools; `x` = DEAD-AS-WIRED; `[A]` / `[B]` = shape; `*` = can outlive its session (section 3.1).
The engine container's EngineApi is a second, independent EL client.

### 3.1 Inventory, lifetimes and stop paths

| Process | Unit | Kind | Count / cadence | Stops on | Status |
|---|---|---|---|---|---|
| every gRPC process | common tasks (`cc_bootstrap::serve_with_options`): `shutdown-signal`, `metrics-http` (+1 per scrape connection), one `peer-prober` per `[peers]` entry, the tonic server | tokio | 1 + 1 + N + 1 | probers and metrics are aborted at exit (`crates/bootstrap/src/serve.rs:366-371`) | LIVE |
| chain, beacon-core | `chain-core` | OS thread | 1 | `Shutdown` on the tick scheduler lane (`core.rs:1884-1887`), or last `SchedulerPermit` dropped (`core.rs:1697-1701`) | LIVE (iff core) |
| chain, beacon-core | `chain-slot-tick` | OS thread | 1; genesis-aligned slot boundary | `blocking_push` returns `Err` once the consumer is gone; its handle is detached (`core.rs:1286`, `:1902-1929`) | LIVE (iff core) |
| chain, beacon-core | liveness sampler | tokio | one probe (<= ~4 s deadline), then a slot/4 (3 s) sleep (`crates/chain-core/src/liveness.rs:228-268`) | `watch` cancel at pre-drain | LIVE (iff core) |
| chain, beacon-core | events task | tokio | 1 | never shut down by either host | IDLE |
| chain, beacon-core | P2pStream epoch-sequence and slot-tick drivers | tokio | 1 + 1; the epoch driver polls every 10 ms, the slot driver sleeps to the next slot, or re-polls every 100 ms while genesis is unset (`crates/chain-core/src/p2p_stream.rs:276-329`) | never (no cancel path) | slot driver: IDLE core-absent (no genesis), LIVE (iff core); epoch driver: IDLE (it fires on a sequence > 0, and only import, DataAvailable or re-drive publish one; the spawn publish is sequence 0, `core.rs:1273`, `:2237-2239`) |
| chain | P2pStream session + head watcher | tokio | per session, <= 8 live sessions; watcher polls every 10 ms (`p2p_stream.rs:545-560`) | session: inbound stream ends. Watcher: only when `tx.send` fails at the first head-sequence change after its session ended. The sequence moves only on `finish_imported` (`import.rs:959`) or `ApplyAttestations` (`apply_attestations.rs:152`); install keeps 0 (`core.rs:1627`). So at HEAD, as wired, each ended session leaks one 10 ms poller | LIVE (one session from p2p) |
| chain, beacon-core | SubscribeEvents bridge | tokio | per subscriber, mpsc(16) (`crates/chain-core/src/service.rs:319-333`) | the next event fails to send; with no events, as wired a bridge whose client left would park until exit | IDLE (no in-tree subscriber) |
| chain | boot task | tokio | 1 (`services/chain/src/run.rs:408-493`) | ends after `mark_ready` and the optional checkpoint install; `exit(1)` on a checkpoint failure (`run.rs:492`) | LIVE |
| beacon-core | local-ready task | tokio | 1 (`bin/beacon-core/src/boot.rs:535-546`) | ends after `mark_ready` and the liveness spawn | LIVE |
| chain, beacon-core, engine container | engine-api upcheck driver | tokio | per slot, plus a nested spawn per probe and a 250 ms retry spawn (`crates/engine-api/src/state.rs:553-597`) | never | LIVE |
| chain, beacon-core, engine container | engine-api fastpath worker | tokio | 1 (`crates/engine-api/src/fastpath/mod.rs:270-273`) | never | IDLE |
| beacon-core, storage service | storage-core writer + panic guard | tokio (**not** an OS thread) | 1 + 1 (`crates/storage-core/src/writer.rs:399-429`) | storage service: shutdown watch wins the biased select, mailbox not drained; beacon-core: never signalled | A (storage service): LIVE (P2 from prune); B (beacon-core): IDLE (only producer is N1) |
| storage service | prune / replay | tokio | 4 s / 2 s interval | shutdown watch | LIVE / IDLE |
| p2p | swarm and the other p2p tasks | tokio | see [09 §2.2](09-p2p-host-and-discovery.md#22-task-and-channel-map) | supervisor abort (<= 2 s) or runtime end | LIVE / IDLE per task |
| p2p | `kzg-verify-pool` bridge | tokio | 1 (`services/p2p/src/service.rs:761-765`) | its `recv()` yields `None` at start (`services/p2p/src/das/verify_pool.rs:1130-1141`) | DEAD-AS-WIRED (self-terminating) |
| p2p | `cc-kzg-verify-{i}` | OS threads | `K = max(2, available_parallelism / 2)` (`verify_pool.rs:54-61`) | as wired, the queue closes when the pool drops, right after start | DEAD-AS-WIRED |

Shape B adds the storage-core writer and the `seed_from_durable` blocking-pool job to shape A's
chain set. It seeds or checkpoint-syncs **before** bind instead of in a background boot task, and
a local-ready task starts the liveness sampler. Its pre-drain joins the core thread only and
never calls `StorageRuntime::shutdown()`. Shutdown sequencing, the 2 s core-thread envelope and
the 5 s SIGTERM budget are in
[12 §5.2](12-boot-health-and-shutdown.md#52-budgets-against-stop_grace_period-5s). Shutdown
normally fits the 5 s grace period; with an installed core the worst case is unbounded, because
`begin_shutdown`'s `push_wait` has no deadline (section 4.4).

## 4. cc-scheduler and the core thread's scheduler

The plan calls the core thread's scheduler "Loop B" (`LOOP_B_LANES`) and p2p's single gossip
validation loop "Loop A" (`services/p2p/src/gossip/validate/pipeline.rs:385`).

### 4.1 The substrate

`cc-scheduler` (`crates/scheduler`) is a synchronous leaf crate with **zero dependencies**
(`crates/scheduler/Cargo.toml`); it starts no threads and wires no producers
(`crates/scheduler/src/lib.rs:1-8`). The overflow policy lives in the queue type.

| Part | Behaviour | Code |
|---|---|---|
| `FifoQueue<T>` | full: **returns the new item**, `dropped += 1`; pops oldest | `crates/scheduler/src/queue.rs:10-41` |
| `LifoQueue<T>` | full: **evicts the oldest**, pushes the new one at the front, `evicted += 1`; pops newest | `queue.rs:89-121` |
| `LaneSpec { id, queue, depth, never_shed, why }` | one scheduler lane; `Depth::Fixed(n)` or `Depth::FromValidators` = `max(active * 110 / 100 / slots_per_epoch, 128)` | `crates/scheduler/src/config.rs:87-111` |
| `Manager<K, T>` | owns the queues; `push` -> `Enqueue::{Accepted, DroppedNew, EvictedOldest, WouldShed, UnknownLane}`; a full `never_shed` scheduler lane returns `WouldShed` **without taking the item** | `crates/scheduler/src/manager.rs:62-72`, `:231-239` |
| `Manager::select` | walks `INBOUND_POLL_CHAIN = [WorkerIdle, Deferred, New]`: `WorkerIdle` returns `None` when `busy_workers >= max_workers`; `Deferred` pops a FIFO(128); `New` pops the first non-empty scheduler lane in slice order; a hit increments `busy_workers` | `manager.rs:249-280`; `config.rs:136-140` |

Queue length floors at 1, and `set_max_length` resizes in place (overflow applies on the next
push). `drain` resets the worker gate and pops everything; `snapshots` feeds the depth gauges.

### 4.2 The chain scheduler lane set

`LOOP_B_LANES` (`crates/scheduler/src/chain.rs:121-157`) is the only selection chain in the
tree; slice order is the policy. Order, depths, work items and producers:
[04 §3](04-chain-core.md#3-scheduler-lanes); this section covers the machinery behind them. The
same anchor lists what never takes effect: the `WorkerIdle` gate is INERT, and the `Deferred`
class and the 13-label `ChainWork` enum are DEAD-AS-WIRED. Depth gauges are labelled per
scheduler lane (`cc_chain_lane_queue_depth{lane}`, `core.rs:501-508`).

### 4.3 SharedScheduler: one Mutex, one Condvar, one park/unpark

chain-core wraps `Manager<ChainLane, CoreWork>` as `SharedScheduler { state:
Mutex<SchedulerState>, space: Condvar, wake: Arc<LaneWake>, senders: AtomicUsize }`
(`crates/chain-core/src/core.rs:330-335`). It is built inside `spawn_core_thread_with_epoch`, so
it exists only with an installed core; a manager config error aborts the process
(`core.rs:346-350`).

| Push variant | Caller context | On a full scheduler lane | Wakes the core thread via | Code |
|---|---|---|---|---|
| `push` | any | returns `Full` / `EvictedOldest(old)`; `apply_attestations` fails the evicted waiter with `RESOURCE_EXHAUSTED` (`core.rs:1013-1021`) | `LaneWake::notify` | `core.rs:369-390` |
| `blocking_push` | OS thread `chain-slot-tick` | `Condvar` `space.wait` until a `select`, `set_depth` or close notifies | `LaneWake::notify` | `core.rs:392-414` |
| `push_wait` | tokio (Ping, Shutdown) | `tokio::time::sleep(5 ms)` and retry, **no deadline** | same | `core.rs:416-427` |
| `push_timeout` | tokio (import, query, DataAvailable) | sleep `min(remaining, 5 ms)`, retry until the 2 s `IMPORT_SEND_TIMEOUT` (`core.rs:72`) | same | `core.rs:429-445` |

The 2 s "send_timeout" is a 5 ms async sleep-poll, not a tokio `send_timeout`; only the
slot-tick OS thread waits on `space`. `push_wait` and `push_timeout` drop an evicted item
silently, harmless because neither feeds the one LIFO scheduler lane. `LaneWake` is an
`AtomicBool` plus the parked `thread::Thread`; `park_current` skips parking when a notify
already landed, so a push between `select()` and `park` is not lost (`core.rs:279-313`).

```text
 PRODUCER (CoreHandle; tokio)   RETRY WHILE THE SCHEDULER LANE IS FULL
 import_block(_for_gossip),     push_timeout: sleep <= 5 ms, until 2 s - - - - - +
   query, notify_data_available                                                  |
 apply_attestations             push: never retries - - - - - - - - - - - - - +  |
 ping, begin_shutdown           push_wait: sleep 5 ms, no deadline ---------+ |  |
 OS thread chain-slot-tick      blocking_push: space.wait (Condvar) ------+ | |  |
                                     ^                                    v v v  v
                                     |              +------------------------------------------+
                                     |              | SharedScheduler::push, holds the Mutex   |
                                     |              |  consumer_gone      -> Closed            |
                                     |              |  never_shed, full   -> WouldShed: Full   |
                                     |              |  FIFO full          -> DroppedNew: Full  |
                                     |              |  LIFO full          -> EvictedOldest     |
                                     |              |  otherwise          -> Accepted          |
                                     |              +------------------------------------------+
                                     | space.notify_all() on      | Accepted, EvictedOldest:
                                     | select, set_depth, close   | unlock, LaneWake.notify()
                                     |                            v
 +----------------------------------------------------------------------------------------------+
 | OS thread chain-core: core_loop (crates/chain-core/src/core.rs:1632-1891)                    |
 |  1 set_depth(attestation, max(active * 110/100 / 32, 128))      [space.notify_all]           |
 |  2 expire pending_da (4 slots) and pending_engine (8 slots)                                  |
 |  3 select() under the Mutex, INBOUND_POLL_CHAIN, first match wins (a hit: note_idle(),       |
 |    space.notify_all(), unlock):                                                              |
 |      WorkerIdle  busy >= max_workers (1)? -> None    INERT: note_idle() at once              |
 |      Deferred    FIFO 128, pop                        DEAD-AS-WIRED: no push_deferred        |
 |      New         tick(4) > import(64) > query_p0(64) > attestation(LIFO) > query_p1(64)      |
 |  4 hit: dispatch, reply on the item's oneshot; Shutdown: send done, exit loop                |
 |    miss: exit loop if every SchedulerPermit dropped, else LaneWake.park_current()            |
 |  after the loop: close_consumer(): drain, reject queued work UNAVAILABLE                     |
 +----------------------------------------------------------------------------------------------+
```

Tick-lane producers (ping, begin_shutdown, chain-slot-tick) and the core loop: LIVE (iff core);
the import, query, notify_data_available and apply_attestations producers: IDLE (section 1),
drawn `- - -`. Producers sleep without the lock; only `SharedScheduler::push` and `select()` hold
the scheduler `Mutex`. The lower box runs on `chain-core`; statuses inside it are per branch.

### 4.4 Permits, closing and drained work

- **Producer count.** Each `CoreHandle` / sender holds a `SchedulerPermit`; the last drop sets
  `producers_closed` and wakes the core thread, which exits once `select()` comes back empty
  (`core.rs:511-534`, `:1697-1701`).
- **Consumer gone.** After the loop, `close_consumer` sets `consumer_gone`, drains the manager
  and rejects each item (`core.rs:487-499`, `:636-660`): replies get `UNAVAILABLE "chain core
  thread is shut down"`; a drained `Shutdown` fires its `done`; `SlotTick` and `DataAvailable`
  are dropped silently; a drained `Ping` drops its reply, so that probe fails.
- **Shutdown envelope.** `CoreThread::shutdown_and_join` shares one 2 s
  `SHUTDOWN_JOIN_TIMEOUT` between the `done` oneshot and the OS join, which runs on the blocking
  pool; when the budget runs out the join handle is detached (`core.rs:1181-1224`). The
  deadline is set first, but `begin_shutdown`'s `push_wait` is awaited without a timeout, so a
  full tick scheduler lane can hold shutdown past the 2 s envelope.

### 4.5 If the core thread panics

Release builds unwind and nothing catches the panic (section 2.2); `core_loop` has no guard
either. As wired, the thread would unwind past `sched.close_consumer()`
(`core.rs:1891`), so `consumer_gone` stays false. The item in hand drops its reply (that caller
gets `UNAVAILABLE`), but queued items keep their oneshots, so those callers' `rx.await`
(`core.rs:960`, `:997`, `:1026`, `:1056`) would never resolve. New pushes are accepted until
each scheduler lane fills, then fail with `RESOURCE_EXHAUSTED` after 2 s. Pings go unanswered,
so liveness flips aggregate health after ~18 s. Once the tick scheduler lane holds 4 items,
`chain-slot-tick` waits on `space` forever, and at SIGTERM `begin_shutdown`'s `push_wait` would
spin inside the pre-drain hook, itself awaited without a timeout
(`crates/bootstrap/src/serve.rs:398-400`), until SIGKILL. Not observed; traced only.

## 5. Backpressure

```text
 STAGE (process: unit)              BOUND, AND WHAT A FULL QUEUE DOES              STATUS
 p2p: swarm task                    gossip mpsc(1024), try_send: full ->           IDLE
   v                                  report IGNORE(Internal), count shed
 p2p: gossip-validate (one loop)    chain_out mpsc(1024), send().await: full ->    IDLE
   |                                  the whole validation loop WAITS, no deadline;
   |                                  then it awaits this block's verdict <= 12 s
   |  <-- swarm: while chain_out is full it waits <= 500 ms before EVERY swarm
   v      event, then handles it with gossip shed (commands still pass)
 p2p: chain-stream-client feed      Semaphore(1024) dispatch tasks: full -> stop   IDLE
   v                                  reading chain_out
 p2p: dispatch -> cc_seam::Ipc      out_tx mpsc(1024) send_timeout 2 s; the reply  IDLE
   |                                  waits out the rest of the same 2 s ->
   |                                  Backpressure{1024} -> IGNORE(Internal)
   v  gRPC P2pStream (p2p dials), HTTP/2 flow control
 chain: P2pStream session task      serial: awaits the WHOLE import (newPayload,   session LIVE;
   |                                  fcU) before reading the next message; views  objects IDLE
   v                                  stall meanwhile; out mpsc(1024); <= 8 sessions
 chain: CoreHandle                  import scheduler lane FIFO 64, push_timeout    IDLE
   |    ::import_block_for_gossip     2 s -> RESOURCE_EXHAUSTED -> Verdict(Ignore,
   v                                  Internal, Invalid) -> Ipc Backpressure{64}
 chain: OS thread chain-core        one item at a time; it parks in:               LIVE (iff core)
   |-- DirectEngine ~~~~> EL          block_on: newPayload, fcU 8 s; getBlobs 1 s  IDLE
   |-- events mpsc(4096)              blocking_send: full -> core thread WAITS     IDLE
   |      events task -> subscriber   mpsc(256) try_send: full -> subscriber killed
   |                                  with RESOURCE_EXHAUSTED (Policy B)
   |-- [B] ArchiveWrite P0 mpsc(32)   blocking_send: full -> core thread WAITS, no  IDLE
   v                                  deadline; then blocking_recv of the commit
 storage-core writer task (tokio)   biased: shutdown > P0 > P1 > P2; commit and    [B] N1 IDLE
                                      fsync run inline on a tokio worker
```

Read top to bottom: a full queue at one stage makes the stage above it wait, shed or fail.
The STATUS column gives each hop's status; `~~~~>` leaves the process; `[B]` = shape B only. In
shape A the installed core has no archive handle, so the chain ends at the core thread. Policy
letters follow [03 §5.3](03-internal-contracts.md#53-overflow-policies-a-d).

As wired, each P2pStream session handles inbound serially and awaits every import to
completion, even after an early ACCEPT (`crates/chain-core/src/p2p_stream.rs:485`, `:821`), so a
session holds at most one item in the import scheduler lane and, with <= 8 sessions, p2p alone
cannot fill it. A slow core thread would surface at p2p as the `Ipc` 2 s budget, counted from
enqueue, expiring (`crates/seam/src/ipc.rs:1132-1142`): an object queued behind a slow import
would be shed as `Backpressure{1024}` -> IGNORE(Internal), even if chain imports it later.
`Backpressure{64}` still arises from other import failures: chain maps every
non-InvalidArgument `Status` to `(Ignore, Internal, Invalid)` (`p2p_stream.rs:850-879`), which
`Ipc` converts (`ipc.rs:900-914`). Some internal import failures surface to p2p as backpressure.

### 5.1 Bounded queues on the hot paths

| Queue (edge, code) | Bound | When full | Who observes it | Status |
|---|---|---|---|---|
| p2p `gossip` swarm -> `gossip-validate` (I8, `services/p2p/src/channels.rs:31`) | 1024 | `try_send` fails: IGNORE(Internal), never a peer penalty | gossipsub peer; `cc_p2p_gossip_shed_total{topic}` | IDLE |
| p2p `chain_out` -> chain-stream client (I8, `channels.rs:39`) | 1024 | three producers: the block path `send().await` with no deadline, then a verdict wait <= 12 s (`services/p2p/src/gossip/validate/pipeline.rs:611-623`); sync AcceptForward `try_send`, fire-and-forget (`:502`; chain discards these); parent redrive `send().await` (`:763`). The swarm waits `stall_max` = 1 s heartbeat x 0.5 = 500 ms before **each** swarm event, then handles it with `shed = true` (`services/p2p/src/host.rs:254-311`) | `cc_p2p_swarm_stall_seconds` (stores **milliseconds**, `services/p2p/src/metrics.rs:1119-1123`); shed counter | IDLE |
| chain-stream in-flight `Semaphore` (I9, `services/p2p/src/chain_stream/client.rs:243`) | 1024 dispatch tasks | feed stops reading `chain_out` | via the row above | IDLE |
| `Ipc` `out_tx` (I9, `crates/seam/src/ipc.rs:226`, `:1121-1142`) | 1024; outstanding correlations <= 1024 (`ipc.rs:366`); request stream mpsc(1024) (`ipc.rs:590`) | `send_timeout(2 s)`, then the reply gets the rest of the same 2 s -> `Backpressure{1024}`; while disconnected buffers 1024, then `Backpressure{1024}` (`ipc.rs:997-1003`); outstanding at cap -> `Unavailable` | validation: IGNORE(Internal), "import backpressure; shedding block" | IDLE |
| P2pStream sessions (E1, `crates/chain-core/src/p2p_stream.rs:61`) | 8 | 9th: `RESOURCE_EXHAUSTED`, reason `STREAM_SESSION_LIMIT` | dialing p2p | LIVE |
| P2pStream outbound per session (E1, `p2p_stream.rs:58`, `:441`) | 1024 | session `send().await` waits; inbound is handled serially, so the session stops reading | p2p (views stall) | LIVE |
| ChainView tick and publish `broadcast` (E1, `p2p_stream.rs:165-166`) | 64 | a lagged session skips (`p2p_stream.rs:503`, `:519`) | nobody | ticks: IDLE (core-absent), LIVE (iff core; slot ticks); publish: DEAD-AS-WIRED (`request_publish` has no caller, E1 publish arm) |
| Tick scheduler lane (I1, I2) | 4 | never shed: slot tick waits on `space`; Ping, Shutdown retry every 5 ms | liveness per-sample deadline (~4 s) | LIVE (iff core) |
| import, query_p0, query_p1 scheduler lanes (I1) | 64 each | 2 s, then `RESOURCE_EXHAUSTED` (Policy A) | caller; `cc_chain_import_rejected_backpressure_total` (import only); `cc_chain_lane_queue_depth{lane}` | IDLE |
| Attestation scheduler lane (I1) | `max(active * 110/100 / 32, 128)` | evict oldest; that waiter gets `RESOURCE_EXHAUSTED` | evicted caller | IDLE |
| Core thread -> events task (I3, `crates/chain-core/src/events/mod.rs:412`) | `event_ring_events` = 4096 | `blocking_send` (`crates/chain-core/src/import.rs:1140`): the core thread waits | no metric covers the wait | IDLE |
| Event ring (I4, `crates/chain-core/src/events/ring.rs`) | 4096 events / 64 MiB | evict oldest | a resuming subscriber gets `CURSOR_TOO_OLD` | IDLE |
| Subscriber queue (I4, `crates/chain-core/src/events/fanout.rs:78-97`) | 256 | `try_send` fails: stream ends `RESOURCE_EXHAUSTED` (Policy B) | the subscriber | IDLE (no subscriber) |
| SubscribeEvents bridge (`crates/chain-core/src/service.rs:319`) | 16 | bridge `send().await` waits, so the 256 queue fills | -- | IDLE |
| `pending_da` / `pending_engine` (`crates/chain-core/src/da.rs:43`; `crates/chain-core/src/pending_engine.rs:26`) | 64 each | evict oldest; expire after 4 / 8 slots | `cc_chain_da_pending_dropped_total`, `cc_chain_pending_engine_dropped_total` | IDLE |
| EngineApi transport lanes (I5, `crates/engine-api/src/transport.rs:94-96`) | ordered `Mutex` 1 (held across the HTTP round trip), fastpath `Semaphore` 2, upcheck `Semaphore` 1 | caller waits for the permit inside its per-call deadline | `EngineError::Timeout` (newPayload: block deferred) | upcheck transport lane: LIVE; ordered (newPayload, fcU) and fastpath transport lanes: IDLE |
| Fastpath queue (`crates/engine-api/src/fastpath/mod.rs:55`, `:409-417`) | 32 | drop oldest; single flight per root | `cc_engine_fastpath_dropped_total`, **not exported** by chain or beacon-core (`finish(None)`) | IDLE |
| Writer class P0 (I7, `crates/storage-core/src/writer.rs:52`) | 32 | `blocking_send` / `send().await` wait, no deadline ([ADR-P4-04](../adr/ADR-P4-04.md)) | `cc_storage_writer_queue_depth{class}` | A: DEAD-AS-WIRED (ArchiveWriter dropped); B: IDLE |
| Writer class P1 (I7, `writer.rs:54`) | 64 | wait | same | A: DEAD-AS-WIRED (PutBackfillBatch, migrator); B: DEAD-AS-WIRED (migrator and StorageService not hosted) |
| Writer class P2 (I7, `writer.rs:56`, `:241-255`) | 256 | `try_send`: drop newest | `cc_storage_writer_chunk_dropped_total{class="p2"}` | A: LIVE (prune); B: DEAD-AS-WIRED (prune and replay not hosted) |
| Storage serve admission (`crates/storage-core/src/serve.rs:352-385`) | 4 permits (+1 snapshot permit) | wait `serve_queue_timeout_ms` (2000), then `RESOURCE_EXHAUSTED` | caller; `cc_storage_serve_admission_wait_seconds` | DEAD-AS-WIRED |

The remaining p2p channels (`cmd` 512, `conn` 256, `kzg` 256, `dial`, `penalty`, `reqresp_in`,
`chain_in`, `publish`, the `watch`es) are in
[09 §2.4](09-p2p-host-and-discovery.md#24-channel-map-bounds-and-overflow).

### 5.2 Waits with no deadline

Each of these can stall its owner indefinitely, as wired:

- `chain_out_tx.send().await` and `cmd_tx.send().await` in the one gossip validation loop, plus
  the N2 `GetValidatorRecords` fetch, which opens a fresh `Endpoint::connect()` with no timeout
  (`services/p2p/src/chain_stream/records.rs:104-115`). The block path then waits up to 12 s
  for its verdict, which serializes the whole of Loop A on chain. Latent; unreachable at HEAD
  because no gossip topics are subscribed. Details:
  [10 §13](10-p2p-gossip-and-das.md#13-concurrency-metrics-and-latent-defects).
- Every `CoreHandle` reply `rx.await` after an accepted push (`core.rs:960`, `:997`, `:1026`,
  `:1056`): the 2 s budget covers admission only, so the wait inherits the core thread's worst
  case (unbounded in shape B through N1; forever after a panic, section 4.5).
- The P2pStream session's await of each import, newPayload and fcU included, before its next
  read (`p2p_stream.rs:791-833`; section 5).
- The core thread's `event_tx.blocking_send` when the events task lags.
- The core thread's `ingest_block_blocking` -> `blocking_submit_p0_committed`: a `blocking_send`
  on P0 and then `blocking_recv` of the commit (`writer.rs:272-280`). The trait documents
  Backpressure; the implementation blocks without a deadline. chain-core's
  `Backpressure -> RESOURCE_EXHAUSTED` arm is unreachable.
- `begin_shutdown`'s `push_wait` (section 4.4), the pre-drain hook that awaits it
  (`crates/bootstrap/src/serve.rs:398-400`), and the peer manager's policy `cmd` sends.

### 5.3 Memory bounds

| Holder | Bound | Unit | Code | Status |
|---|---|---|---|---|
| Residency (core thread) | <= 4 pinned `BeaconState`s, plus 1 transient parent clone during each `state_transition` | states | `crates/chain-core/src/residency.rs:32-44`, `config/chain.toml:10`; clone `crates/fork-choice/src/on_block.rs:259-264` | pinned: IDLE (iff core; holds only the anchor); clone: IDLE (iff core; no import) |
| Body ring | 64 | blocks | `residency.rs:35`, `config/chain.toml:11` | IDLE (only import fills it, `crates/chain-core/src/import.rs:929`) |
| `pending_da` + `pending_engine` | 64 + 64 | SSZ blocks | `crates/chain-core/src/da.rs:43-49`, `crates/chain-core/src/pending_engine.rs:26-29` | IDLE |
| Checkpoint download (transient, at boot) | block 8 MiB, state 400 MiB, JSON 2 MiB per body | bytes | `services/chain/src/checkpoint_sync.rs:72-78` | LIVE (opt-in) in both shapes; nothing is downloaded with the default `checkpoint_providers = []` |
| Event ring; one payload; subscriber queue | 4096 events / 64 MiB; <= 10 MiB; 256 events | events, bytes | `crates/chain-core/src/events/mod.rs:90-107` | IDLE |
| Storage serve | 64 MiB buffer x 4 permits; snapshot: 1 permit, 1 MiB chunks | bytes | `crates/storage-core/src/serve.rs:120-129`, `:1326-1393`; `crates/storage-core/src/history.rs:39` | A: DEAD-AS-WIRED; B: ABSENT |
| EngineApi response body | 1 MiB; 16 MiB for getBlobsV2 | bytes | `crates/engine-api/src/transport.rs:29`, `:37` | upcheck: LIVE; getBlobsV2: IDLE |
| Engine fastpath queue | 32 | triggers | `crates/engine-api/src/fastpath/mod.rs:55` | IDLE |
| p2p `gossip`, `chain_out`, `Ipc` `out_tx` | 1024 each, bounded by count, not bytes; each item up to `GOSSIP_MAX_SIZE` = 10 MiB | items | `services/p2p/src/channels.rs:31`, `:39`; `crates/libp2p/src/snappy.rs:13`; `crates/seam/src/ipc.rs:226` | A: IDLE; B: ABSENT |
| `BackfillCache` | 1 GiB | bytes | `services/p2p/src/backfill/cache.rs:32` | TEST-ONLY |
| KZG trusted setup | parsed once per load. p2p loads it three times at start: validation loop, verify pool, EngineStream inject (`services/p2p/src/gossip/validate/pipeline.rs:374`, `services/p2p/src/service.rs:747`, `services/p2p/src/engine_stream/inject.rs:452`); each EngineApi loads one more when built (`crates/engine-api/src/api.rs:123`, `:146`) | settings | [10 §10](10-p2p-gossip-and-das.md#10-kzg-verification-inline-today-pool-disconnected) | A: LIVE (p2p x3, chain, engine container); B: LIVE (EngineApi only) |
| redb page cache | not configured: redb 4.1.0 `Builder` default, up to 1 GiB (the offline `open_existing`, `:282`, too) | bytes | `Database::create`, `crates/store/src/engine/redb.rs:253` | A (storage service): LIVE; B: LIVE |
| Containers | no `mem_limit` or `deploy.resources` on any service | -- | `docker-compose.yml` | ABSENT |

RSS is exported as the `process_resident_memory` gauge (`crates/bootstrap/src/process.rs:45-48`),
but no per-process memory envelope is stated or tested.

## 6. Sync-in-async hazards

### 6.1 `block_on` on the core thread for engine calls (I5)

`EngineApi` captures `Handle::try_current()` when it is finished on the host runtime
(`crates/engine-api/src/api.rs:160`). Its synchronous methods run
`runtime.block_on(tokio::time::timeout(d, fut))` (`api.rs:275-292`); `new_payload` and
`forkchoice_updated` first `block_on` the local `ensure_el_admitted` check (`api.rs:304`,
`:366-367`). DirectEngine calls them from `chain-core` with deadlines from `[timeouts]` x
`multiplier` (defaults: newPayload 8 s, fcU 8 s, getBlobs 1 s enqueue only, `is_online` 1 s via
`eth_syncing_ms`; `crates/engine-api/src/config.rs:24-28`, `:50-64`; wired at
`services/chain/src/run.rs:365`, `bin/beacon-core/src/boot.rs:439`;
`crates/chain-core/src/engine.rs:101-103`). Only `is_online` runs at HEAD, a local state read
with no HTTP (`api.rs:444-456`). Details:
[08 §4.2](08-execution-engine.md#42-deadlines-and-what-parks-the-core-thread).

- **Why this is safe.** `chain-core` is a plain OS thread, not a runtime worker, so
  `Handle::block_on` is legal there. The future is polled on `chain-core` itself, while the
  multi-thread runtime's workers drive the I/O and timer drivers and any task the future spawns
  (hyper's connection task). That is why cc-chain pins `flavor = "multi_thread"`
  (`services/chain/src/main.rs:8-9`). `Handle::block_on` panics inside an async context, so
  these sync methods must never be called from a tokio task; the engine container uses the
  async variants (`services/engine/src/main.rs:140`, `:167`).
- **What it costs.** While parked, the core thread serves nothing: one import can hold it for
  newPayload plus fcU (up to 16 s; plus the commit in shape B) before it replies. Ping waits on
  the tick scheduler lane behind it. The liveness policy is sized for this: N = 3 consecutive
  ~4 s misses, about an 18 s horizon, so one or two back-to-back 8 s calls cannot flip aggregate
  health (`crates/chain-core/src/liveness.rs:101-115`; [ADR-R-04](../adr/ADR-R-04.md)). Liveness
  flips aggregate health; nothing in this repository restarts a process on that signal.
- **Per-slot parking.** Every `SlotTick` calls `is_online` (`block_on`, 1 s deadline, local
  read) and the fcU per-slot floor on `chain-core` (`core.rs:1979-2014`). Today the floor
  returns without HTTP. Once an import or attestation primes it, as wired the floor would
  `block_on` an fcU of up to 8 s every slot, so an Online-but-slow EL could park the core thread
  for up to ~9 s of each 12 s slot. While the EL is externally Offline, `ensure_el_admitted`
  refuses without HTTP (`api.rs:483-493`).
- **Shape B seed.** `apply_durable_seed` replays blocks on a blocking-pool thread
  (`crates/chain-core/src/seed.rs:462`), so its newPayload `block_on` runs there; IDLE at HEAD.

### 6.2 Blocking sends from the core thread

`blocking_send` / `blocking_recv` on tokio channels are the sync side of the bridge; tokio
panics if they are called from an async context, which `chain-core` never is. The events
channel blocks the core thread when the events task lags (Policy B protects the task from slow
subscribers, not the core thread from the task). In shape B, a slow redb commit or a full P0
parks the core thread for the whole commit, fsync included.

### 6.3 Synchronous redb on tokio workers

redb calls are synchronous, and storage-core runs them inside async tasks without
`spawn_blocking`:

- **Boot.** redb open, I-node-id stamping, resume and invariant scans run inline in the async
  `run()` before bind (`crates/storage-core/src/boot.rs:528-549`;
  `bin/beacon-core/src/boot.rs:416-417`). Harmless before bind, but shape B scans grow with an
  unpruned store: Shape B does not yet bound its store or order EL readiness before seed replay.
- **Writer.** `commit_p0` / `commit_meta` / `commit_p2`, fsync included, run inline in the writer
  task's `select!` arms, followed by `yield_now()` (`crates/storage-core/src/writer.rs:468-522`).
  Each commit occupies one tokio worker for its duration. If `StorageRuntime` drops while the
  runtime still runs, the writer's biased `shutdown.changed()` arm may spin and starve P0..P2
  (`writer.rs:444-474`); both hosts keep the runtime alive until exit. Not observed; traced only.
- **Serve.** Handlers take a permit, then call `engine.read()` inline
  (`crates/storage-core/src/serve.rs:556-561`). The 4 permits cap how many workers can block on
  reads at once.
- **Prune.** `Engine::drop_table` runs from the prune task, bypassing the writer
  (`crates/storage-core/src/prune/shards.rs:151`, `:200`).

Whether this holds up under load is unmeasured; it is an open question, not a known defect.
`block_in_place` in the migrator is the one sync-in-async site that tells the runtime; it
requires the multi-thread flavor and is DEAD-AS-WIRED.

### 6.4 CPU work on the p2p reactor (latent)

The gossip validation loop runs BLS, Merkle and inline c-kzg batch verification on a tokio worker
while holding the `ValidationPoolState` `std::sync::Mutex`
(`services/p2p/src/gossip/validate/pipeline.rs:547-570`); the dedicated KZG pool that
[ADR-P2-02](../adr/ADR-P2-02.md) intended for this is disconnected (`kzg_tx: _`,
`services/p2p/src/service.rs:705`). The EngineStream session runs `inject()` KZG synchronously on
its task (`services/p2p/src/engine_stream/server.rs:244`); no client dials EngineStream, so this
is DEAD-AS-WIRED. The production swarm's `std::thread::sleep(first_byte_delay)`
(`services/p2p/src/host.rs:916-917`) is DEAD-AS-WIRED: column serving returns early while
`block_serve` is `None` (`host.rs:836-842`; nothing calls `with_block_serve`), and the host
hard-codes `ByRootFaultPolicy::Honest` (`host.rs:885`), so the delay is always zero. The devnet
stall-reqresp fault sleeps the devnet swarm for TTFB + 1 s, blocking a tokio worker
(`services/p2p/src/fault_mode.rs:1328-1332`; DEVNET-ONLY). The validation-loop CPU work above is
IDLE.

## 7. Where these invariants are tested

| Invariant | Test |
|---|---|
| The tick scheduler lane is never shed; Ping rides it | `crates/scheduler/src/manager.rs:459`, `crates/scheduler/src/chain.rs:209`; `crates/chain-core/src/core.rs:2446` (`slot_tick_is_never_shed`), `:2504` |
| Shutdown is served while the import scheduler lane is full | `core.rs:2984` (`shutdown_is_served_while_import_lane_is_full`) |
| Shutdown fails queued attestation waiters with `UNAVAILABLE` | `core.rs:3073` |
| The attestation scheduler lane evicts the oldest and serves the newest | `crates/scheduler/src/queue.rs:184`, `manager.rs:489`; `core.rs:3273` |
| Seed replay and `compute_cells` stay on the blocking pool (source scans) | `crates/chain-core/src/seed.rs:893`; `crates/engine-api/src/fastpath/cells.rs:408` |
| A timed-out EL defers the block instead of parking the core thread | `services/chain/tests/engine_rpc_deadlines.rs:167` |
| A black-holed EL flips aggregate health after N = 3 misses; an idle core thread stays SERVING | `services/chain/tests/engine_blackhole_liveness.rs:476`, `:497` |
| Liveness policy: 1-2 misses keep SERVING, 3 flip | `crates/chain-core/src/liveness.rs:397`, `:415`, `:427` |
| A writer panic is process-fatal | `crates/storage-core/src/writer.rs:1078` |
| P0 commit latency holds under snapshot and backfill P2 load | `crates/storage-core/src/replay.rs:1198`, `crates/storage-core/src/backfill.rs:1332`; run alone (`.config/nextest.toml:12-26`) |

Gaps: no test drives the production persist path (core thread + `ingest_block_blocking` with a
real writer). Nothing covers a core-thread panic (section 4.5) or the head-watcher leak (section
3.1). See [14-testing-and-enforcement.md](14-testing-and-enforcement.md).

## Planned changes

Target design: [`plan/architecture.md`](../../plan/architecture.md) §3 (§3.1-§3.8).

- **Planned: scheduler substrate (§3.1).** The plan has `cc-scheduler` depend on tokio, label
  metrics per work type from one enum, and invert poll priority (`WorkerIdle` -> deferred ->
  new). At HEAD it has zero dependencies and per-scheduler-lane metrics, and the inversion is
  data only (section 4.2).
- **Planned: Loop A before S3 (§3.4).** Reconnect `kzg_tx` so column KZG moves to the
  `cc-kzg-verify-*` threads, replace the 12 s block-verdict wait (`pipeline.rs:615`) with a
  slot-bounded one, and move the redrive walks off the validation hot path.
- **Planned: Loop A after S3 (§3.4).** Eight typed queues keyed by `ValidatorKind` (FIFO for
  blocks, columns, re-processing and operations; LIFO for aggregates, attestations and sync), a
  worker count, and sharding of `ValidationPoolState`.
- **Planned: one eviction implementation (§3.5).** Replace the VerifyPool's own oldest-drop
  queue with `cc_scheduler::LifoQueue`.
- **Planned: shedding layers (§3.6).** An untyped channel bound must not decide a
  consensus-relevant drop; shedding moves to typed queues and seam backpressure.
- **Open: the ArchiveWrite P0 deadline.** Either add a send deadline that returns
  `Backpressure`, or amend the trait doc, seam README and [ADR-R-02](../adr/ADR-R-02.md) to
  "block, no deadline". Which side changes is not decided.
