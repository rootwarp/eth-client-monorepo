# Glossary

> **As built at `7d8833d`** (`develop`, 2026-08-16). Status labels and diagram legend: [README](README.md#status-labels).
> Target design: [`plan/architecture.md`](../../plan/architecture.md) §0, §2.0, §2.2-§2.3 (edge ids, overflow policy letters), §3.2 (scheduler lanes), §3.4, §9.1.

This page fixes the vocabulary of the doc set. Read the **Name map** and **Collisions** first.
Those words are ambiguous in the code and in older docs, so every doc here writes one of their
qualified forms instead. Then come the edge ids (`E`, `N`, `I`, `X`), the two shapes and the
status labels, and then the A-Z terms. Each row gives a one-line meaning, its status at
`7d8833d` where it has one (closed labels, per shape where the shapes differ), where the code
is, and the doc that explains it. Paths are repo-relative. When a code comment or an older doc
disagrees with a row here, the row follows the code at `7d8833d`.

## Name map

```text
 BARE WORD                 QUALIFY AS                   WHAT IT NAMES
 "lane" ---------------+-- scheduler lane               tick/import/query_p0/attestation/query_p1
                       +-- transport lane               engine-api: ordered, fastpath, upcheck
                       +-- writer class                 storage-core writer mailbox: P0, P1, P2
 "serve window" -------+-- storage serve window         meta.serve_window, emitted on E6
                       +-- advertised window            p2p Arc<ServeWindow>, read by Status
                       +-- cache window                 BackfillCache's own; never constructed
                       +-- CC-4A block floor            spec serve obligation, 33024 epochs
 "core" ---------------+-- chain-core                   the crate crates/chain-core
                       +-- core thread                  OS thread "chain-core"; owns Store<P>
                       +-- installed core/core-absent   install_core ran / did not run
 "storage" ------------+-- storage service              container and process cc-storage
                       +-- storage-core                 the crate crates/storage-core
                       +-- cc-store                     the crate crates/store
                       +-- store                        the redb file store.redb
 "engine" -------------+-- engine container             cc-engine, a leftover shell
                       +-- engine-api                   the crate crates/engine-api
                       +-- DirectEngine                 chain-core adapter on the core thread
                       +-- EL                           geth, el:8551
 "health" -------------+-- aggregate health             grpc.health service name ""
                       +-- self-name                    eth.X.v1.XService, SERVING from bind
                       +-- local_ready                  readiness bit inside aggregate health
 "cursor" -------------+-- event cursor                 SubscribeEvents resume key
                       +-- write cursor                 meta.write_cursor; invariant `cursor`
 "session id" ---------+-- stream session id            StreamHello (E1) / EngineHello (E8)
                       +-- event session id             SubscribeEvents Cursor + metadata
                       +-- fcU session id               DirectEngine, stamped on every fcU
                       +-- writer session id            WriteCursor.session_id
 "verdict" ------------+-- gossip verdict               ACCEPT / REJECT / IGNORE + Reason
                       +-- import verdict               IMPORTED / DEFERRED_DA / INVALID ...
 "restore" ------------+-- seed or checkpoint sync      restore itself is DELETED (E4, 60c6200)
 "4-container topology"
                       +-- shape A                      six CC containers + el
 "Policy A/B/C/D" -----+-- letter + behaviour           e.g. "Policy C (drop and report)"
```

A `+--` branch means "write one of these instead". The branches are not data flow, so the
[diagram legend](README.md#diagram-legend) does not apply here.

## Collisions: qualify these words

Never write these words bare.

| Bare word | Write instead | Meaning | Code | Doc |
|---|---|---|---|---|
| "lane" | **scheduler lane** | one of five chain-core queues: tick (FIFO 4, never shed), import (FIFO 64, drop-new), query_p0 (FIFO 64), attestation (LIFO, evict oldest), query_p1 (FIFO 64). The first match wins in that order | `cc_scheduler::ChainLane`, `crates/scheduler/src/chain.rs:13`, `:121-157` | [04 §3](04-chain-core.md#3-scheduler-lanes) |
| | **transport lane** | engine-api HTTP lane: ordered (Mutex 1: newPayload, fcU), fastpath (Semaphore 2: getBlobs), upcheck (Semaphore 1: `eth_syncing`, exchangeCapabilities) | `transport::Lane`, `crates/engine-api/src/transport.rs:56` | [08 §3.2](08-execution-engine.md#32-transport-lanes) |
| | **writer class** | storage-core writer mailbox class: P0 32 (block), P1 64 (block), P2 256 (drop-newest) | `crates/storage-core/src/writer.rs:52-56` | [07 §4.3](07-storage.md#43-the-single-writer-and-its-mailbox) |
| "serve window" | **storage serve window** | the `meta.serve_window` record that `WatchServeWindow` emits. Nothing writes it (both publishers are `#[allow(dead_code)]`), so E6 carries `empty_window()` with `earliest_available_slot = u64::MAX`. cc-store comments call this record the "advertised window" (`crates/store/src/invariants.rs:95`); in this doc set that name means p2p's | `cc_store::meta::ServeWindow` (`crates/store/src/meta.rs:130`); wire `ServeWindow` (`proto/eth/storage/v1/storage.proto:157`); `crates/storage-core/src/serve.rs:281,300` (dead publishers), `:1196-1235` (`empty_window`, loader) | [11](11-p2p-reqresp-and-sync.md#three-serve-windows-and-earliest_available_slot), [07 §4.7](07-storage.md#47-serve-and-the-storage-serve-window-shape-a) |
| | **advertised window** | p2p `Arc<ServeWindow>`: one `AtomicU64` with one writer ([ADR-P2-14](../adr/ADR-P2-14.md)), which feeds `Status.earliest_available_slot`. LIVE (content static: `u64::MAX`) | `services/p2p/src/backfill/window.rs:88` | [11](11-p2p-reqresp-and-sync.md#three-serve-windows-and-earliest_available_slot) |
| | **cache window** | `BackfillCache`'s own `ServeWindow`. The cache is TEST-ONLY (never constructed) | `services/p2p/src/backfill/cache.rs:104,128` | [11](11-p2p-reqresp-and-sync.md#three-serve-windows-and-earliest_available_slot) |
| | **CC-4A block floor** | the spec serve obligation `MIN_EPOCHS_FOR_BLOCK_REQUESTS`, computed and never read from config: 256 + 65536 / 2 = 33024 epochs on Hoodi. Prune and the startup check use it. [serve-windows.md](../serve-windows.md) calls this the "serve window" | `crates/store/src/window.rs:124-171` | [serve-windows.md](../serve-windows.md), [07 §4.6](07-storage.md#46-prune-shape-a) |
| "core" | **chain-core** | the crate `cc-chain-core`, hosted by cc-chain (A) and cc-beacon-core (B) | `crates/chain-core` | [04](04-chain-core.md) |
| | **core thread** | the OS thread named `chain-core`, which owns `Store<P>` by value ([ADR-P1-09](../adr/ADR-P1-09.md)) | `crates/chain-core/src/core.rs:1301-1321` | [04 §2](04-chain-core.md#2-one-thread-owns-the-store) |
| | **installed core** / **core-absent** | whether `ChainServiceImpl::install_core` ran. While core-absent, the fork-choice RPCs (ImportBlock, GetHead, ApplyAttestations and the queries, GetValidatorRecords included) answer `FAILED_PRECONDITION` + `NOT_BOOTSTRAPPED`; GetInfo, P2pStream and SubscribeEvents still serve. Both shapes are core-absent by default (`checkpoint_providers = []`) | `crates/chain-core/src/service.rs:138,201-208`; `services/chain/src/run.rs:423-428`; `config/beacon-core.toml:23` | [04 §4](04-chain-core.md#4-corehandle-chainserviceimpl-and-core-absent-mode) |
| "storage" | **storage service** | the compose container and process `cc-storage`, a shim over storage-core serving StorageService (10 RPCs). TRANSITIONAL | `services/storage` | [07](07-storage.md), [01](01-deployment-and-processes.md#process-table) |
| | **storage-core** | the crate `cc-storage-core`: redb open, writer, ArchiveWriter, serve, prune, replay, migrator | `crates/storage-core` | [07 §4](07-storage.md#4-storage-core) |
| | **cc-store** | the crate `cc-store`: redb engine seam, key space, hot/cold layout, invariants | `crates/store` | [07 §3](07-storage.md#3-cc-store-engine-seam-and-on-disk-layout) |
| | **store** | the redb file `store.redb`, created inside the data directory: `/app/data` on volume `cc-store-data` in A; CWD-relative `data/storage/store.redb` in B | `crates/store/src/engine/mod.rs:193-195`; A: `docker-compose.yml:142,144`; B: `config/beacon-core.toml:10` | [07 §2](07-storage.md#2-who-opens-redb) |
| "engine" | **engine container** | `cc-engine`, a leftover shell. Its EngineService has no caller, and it runs a second EngineApi with the same JWT. TRANSITIONAL. "cc-engine remains in compose for A/B comparison and has no in-tree caller." | `services/engine/src/main.rs:89-112` | [08 §5](08-execution-engine.md#5-what-the-engine-container-still-does) |
| | **engine-api** | the crate `cc-engine-api`: transport lanes, JWT, EL health machine, fastpath | `crates/engine-api` | [08 §3](08-execution-engine.md#3-cc-engine-api) |
| | **DirectEngine** | the chain-core adapter over EngineApi, called on the core thread. It replaced E3 | `crates/chain-core/src/engine.rs:43` | [08 §4](08-execution-engine.md#4-directengine-on-the-core-thread) |
| | **EL** | the execution client: geth v1.17.5, container `el`, authrpc :8551 with an HS256 JWT | `docker-compose.yml:183-224` | [08](08-execution-engine.md) |
| "health" | **aggregate health** | gRPC health service `""`: SERVING iff every `[peers]` entry probes SERVING **and** `local_ready` is set; forced NOT_SERVING while draining | `crates/bootstrap/src/prober.rs:95-110` | [12 §4.1](12-boot-health-and-shutdown.md#41-two-health-names-and-the-local_ready-bit), [01](01-deployment-and-processes.md#startup-order-and-health-gating) |
| | **self-name** | the service's own health name `eth.X.v1.XService`, SERVING from bind | `crates/bootstrap/src/serve.rs:241` | [12 §4.1](12-boot-health-and-shutdown.md#41-two-health-names-and-the-local_ready-bit) |
| | **local_ready** | readiness bit inside aggregate health. chain and beacon-core set it alongside the bind (the handle is sent just before bind, `crates/bootstrap/src/serve.rs:260-266`), so "chain healthy" means bound, not bootstrapped | `crates/bootstrap/src/serve.rs:120`; `services/chain/src/run.rs:421` | [12 §4.1](12-boot-health-and-shutdown.md#41-two-health-names-and-the-local_ready-bit), [01](01-deployment-and-processes.md#startup-order-and-health-gating) |
| "cursor" | **event cursor** | the `SubscribeEvents` resume key `Cursor{session_id, seq, slot, root}`; `seq` is authoritative, and the event session id rejects an event cursor from an earlier incarnation ([ADR-P1-11](../adr/ADR-P1-11.md)). IDLE (no in-tree subscriber; served with an empty ring) | `proto/eth/chain/v1/chain.proto:106`; `crates/chain-core/src/events/cursor.rs:19-67` | [04 §11](04-chain-core.md#11-event-ring-and-subscriber-contract) |
| | **write cursor** | the `meta.write_cursor` record (`WriteCursor`); the store invariant named `cursor` checks it. A: DEAD-AS-WIRED (never created or written; the only P0 producer, ArchiveWriter, is built and dropped). B: created with seq 0 when the writer starts (`start_writer_on_engine`); each block ingest through `ArchiveWriter` then bumps the sequence by one in the same commit, which is IDLE, like N1 | `crates/storage-core/src/open.rs:298`; `crates/storage-core/src/archive_write.rs:192-206`; `crates/store/src/invariants.rs:110` | [07 §4.4](07-storage.md#44-archivewrite-the-import-persist-path-n1) |
| "session id" | **stream session id** | a per-process random u64 in `StreamHello` (E1, sent by p2p) and `EngineHello` (E8), so a reconnect cannot match an earlier incarnation | `proto/eth/p2p/v1/p2p.proto:131-134,219-222` | [03 §4](03-internal-contracts.md#4-p2pstream-anatomy) |
| | **event session id** | the `session_id` inside the event cursor, also sent as gRPC metadata `x-cc-chain-session-id` ([ADR-P1-11](../adr/ADR-P1-11.md)) | `proto/eth/chain/v1/chain.proto:104-107`; `crates/chain-core/src/events/mod.rs:84` | [04 §11](04-chain-core.md#11-event-ring-and-subscriber-contract) |
| | **fcU session id** | the per-process `DirectEngine.session_id`, stamped on every fcU | `crates/chain-core/src/engine.rs:46,87-91` | [08 §4.4](08-execution-engine.md#44-fcu-driver) |
| | **writer session id** | `WriteCursor.session_id` inside the write cursor: seeded as 1, carried unchanged by each ingest | `crates/store/src/meta.rs:162`; `crates/storage-core/src/archive_write.rs:53-57` | [07 §4.4](07-storage.md#44-archivewrite-the-import-persist-path-n1) |
| "verdict" | **gossip verdict** | `Acceptance` ACCEPT / REJECT / IGNORE plus a `Reason`; `REASON_INTERNAL` never penalises a peer. chain sends it up E1, as an early ACCEPT or after import. IDLE (the E1 object / verdict arm) | `proto/eth/p2p/v1/p2p.proto:54-80` | [05](05-block-lifecycle.md#hops-4-12-inside-chain-core), [10 §4.1](10-p2p-gossip-and-das.md#41-verdict-mapping) |
| | **import verdict** | `ImportBlockVerdict` IMPORTED / DUPLICATE / DEFERRED_DA / UNKNOWN_PARENT / INVALID (E1 mirror: `ImportResult`, which adds NONE). It is also the first payload byte of the `BLOCK_IMPORTED` event. Every block that reaches the DA gate gets DEFERRED_DA. IDLE (only a hand-submitted ImportBlock) | `proto/eth/chain/v1/chain.proto:71-78`; `proto/eth/p2p/v1/p2p.proto:83-91`; `crates/chain-core/src/import.rs:58-72` | [05](05-block-lifecycle.md) |
| "restore" | **seed** or **checkpoint sync** | `RestoreFromStore` (E4) was DELETED in 60c6200. Shape B seeds in-process; both shapes can checkpoint-sync | `crates/chain-core/src/seed.rs:383`; `services/chain/src/checkpoint_sync.rs` | [07 §4.5](07-storage.md#45-durable-set-resume-and-seed) |
| "4-container topology" | **shape A** (six CC containers + el) | deprecated term, still common in code comments, log lines and docs, e.g. `services/chain/src/run.rs:5`, `config/beacon-core.toml:2`, `crates/storage-core/src/boot.rs:537`, `docs/s2-rollback.md:21` | `docker-compose.yml` | [01](01-deployment-and-processes.md#shape-a-docker-compose) |
| "Policy A/B/C/D" | the letter **and** its behaviour | cc-seam overflow policies. **A**: block <= 2 s, then `Backpressure`. **B**: terminate the slow consumer. **C**: drop and report (`Ok(Published::Dropped)`). **D**: never shed | `crates/seam/src/conformance.rs:11-23` | [03 §5.3](03-internal-contracts.md#53-overflow-policies-a-d), [14](14-testing-and-enforcement.md#seam-conformance) |

The letters come from plan §2.2. In that table, row D is the silent `SlotTick` drop, which the
never-shed tick scheduler lane replaced. In the code, conformance case 11 uses "Policy D" for
never-shed. Name the behaviour every time, so the two readings cannot be confused.

Three types share the name `ServeWindow`: cc-store's record (the storage serve window), the E6
wire message that carries it, and p2p's `Arc<ServeWindow>` (the advertised window and the cache
window). Grep results mix all three.

## Where the names live

```text
 +-- cc-chain :9001 [A] / cc-beacon-core [B] ------+             +-- cc-p2p :9002 [A] ------------+
 | crate chain-core                                | E1 ChainView| advertised window:             |
 |   core thread "chain-core": owns Store<P>       |============>|   Arc<ServeWindow> (AtomicU64) |
 |   core scheduler: five scheduler lanes          | [A] (p2p    |   -> Status handshake          |
 |   events task: event ring, event cursor         |     dials)  | cache window: TEST-ONLY        |
 |   DirectEngine: fcU session id; calls           |             | stream session id: StreamHello |
 |     engine-api over its transport lanes         |~~~~> EL     +--------------------------------+
 | [B] storage-core writer: writer classes P0-P2   |  (geth, el:8551)            ^
 | [B]   -> store: data/storage/store.redb         |                             |
 +-------------------------------------------------+                             |
 +-- cc-storage :9006 [A]: storage service --------+                             |
 | crate storage-core over crate cc-store          |                             |
 |   storage-core writer: writer classes P0-P2     |   E6 WatchServeWindow       |
 |   storage serve window: meta.serve_window       |=============================+
 |   store: store.redb on volume cc-store-data     |   (p2p dials; u64::MAX)
 +-------------------------------------------------+
 +-- cc-engine :9004 [A]: engine container --------+
 |   engine-api: a second EL health machine        |~~~~> EL (same JWT)
 +-------------------------------------------------+
```

Legend per [README](README.md#diagram-legend): `====>` LIVE stream, arrowhead = data direction;
`~~~~>` in-process call that leaves the process (EL HTTP); `[A]`/`[B]` = only in that shape. The
core thread and its scheduler lanes exist only with an installed core; both shapes are
core-absent by default. E6 carries `u64::MAX` (LIVE, content static).

## Edge ids

### Cross-process and seam edges: E1..E8, N1..N3

`E1`..`E8` keep the plan's numbering (plan §2.0, §2.3). `N1`..`N3` are new since the plan's
baseline `4146791`. "Data flow" is the direction the payload moves. It differs from
"Dialer -> server" for the E1 session + view and publish arms, E6, E7 and N2. Draw arrows by
data flow and label the dialer. For E4 the storage service both dialed chain and pushed the
data. [03 §3](03-internal-contracts.md#3-edge-inventory) owns the payloads and evidence; the
statuses below repeat it. If a status here and there disagree, the code at `7d8833d` decides;
report the mismatch.

| Id | Arm | Data flow | Dialer -> server | Transport | Shape A | Shape B |
|---|---|---|---|---|---|---|
| E1 | session + view | chain -> p2p | p2p -> chain | gRPC bidi `P2pStream` | LIVE | ABSENT |
| E1 | object / verdict | p2p -> chain | p2p -> chain | same stream | IDLE | ABSENT |
| E1 | publish | chain -> p2p | p2p -> chain | same stream | DEAD-AS-WIRED | ABSENT |
| E1 | column | p2p -> chain | p2p -> chain | same stream | DEAD-AS-WIRED | ABSENT |
| E2 | data_available | p2p -> chain | p2p -> chain | same stream | DEAD-AS-WIRED | ABSENT |
| E3 | EL calls over gRPC | chain -> engine container | (none) | gRPC EngineService | DELETED (631994c) | DELETED |
| E4 | restore | storage service -> chain | storage service -> chain | gRPC `RestoreFromStore` | DELETED (60c6200) | DELETED |
| E5 | backfill | p2p -> storage service | p2p -> storage service | gRPC `PutBackfillBatch` | DEAD-AS-WIRED | ABSENT |
| E6 | window | storage service -> p2p | p2p -> storage service | gRPC `WatchServeWindow` | LIVE (content static) | ABSENT |
| E6 | reads | storage service -> p2p | p2p -> storage service | gRPC `Get{Blocks,Columns}By{Range,Root}` | DEAD-AS-WIRED | ABSENT |
| E7 | event feed | chain -> storage service | storage service -> chain | gRPC `SubscribeEvents` | DELETED (78a90e1) | DELETED |
| E8 | inject | engine container -> p2p | engine container -> p2p | gRPC bidi `EngineStream` | DEAD-AS-WIRED | ABSENT |
| N1 | blocks | chain-core -> storage-core | in-process | `ArchiveWrite` trait | ABSENT | IDLE (iff core) |
| N1 | columns | chain-core -> storage-core | in-process | `ArchiveWrite` trait | ABSENT | DEAD-AS-WIRED |
| N2 | op records | chain -> p2p | p2p -> chain | gRPC `GetValidatorRecords` | IDLE | ABSENT |
| N3 | health | peer status | each process -> each `[peers]` entry | `grpc.health.v1` Check | LIVE | ABSENT (`[peers]` empty) |

- **E3, E8**: the E3 client (`services/chain/src/engine_client.rs`) and the E8
  engine-container half (`services/engine/src/inject.rs`) were deleted in 631994c. p2p still
  attaches the E8 server (`services/p2p/src/main.rs:499-508`), and the engine container still
  serves EngineService.
- **E7**: only the storage service's consumer was deleted in 78a90e1. `SubscribeEvents` is still
  served, with no production subscriber.
- **E1, E2, N2 in shape B**: beacon-core serves `P2pStream` and `GetValidatorRecords`. No shipped
  configuration pairs cc-p2p with cc-beacon-core; how p2p attaches to shape B is not yet defined.
- **N1 blocks** (added in d3d7f60) is IDLE for two independent reasons. No block gets past the
  DA gate, and as wired the first persist after checkpoint sync would fail the continuity bind.
  No test drives the production persist path (core thread + `ingest_block_blocking` with a real
  writer); see [14](14-testing-and-enforcement.md#import---durable-and-its-gap).
- **Id look-alikes.** `I2` in [ADR-P4-08](../adr/ADR-P4-08.md) and
  [07 §4.6](07-storage.md#46-prune-shape-a) is a prune invariant, not the slot-tick edge. `E2.2` in
  [`plan/issues/s2-exit-note.md`](../../plan/issues/s2-exit-note.md) is an S2 exit gate, not
  edge E2. `X1`..`X5` in the PRD's §9, which plan §9.1 cites for S3, are acceptance criteria, not
  the external interfaces below. `I1`..`I10` on this page always name in-process edges. An `I`
  id above `I10`, or any `H` id, is not an edge id. If you find one in this doc set, it is a
  leftover review-note reference; report it.

### In-process edges: I1..I10

| Id | Edge | Mechanism and bound | Status at `7d8833d` | Doc |
|---|---|---|---|---|
| I1 | producers -> core thread | `CoreHandle` -> `SharedScheduler`, five scheduler lanes. Import and query pushes wait up to 2 s, then return `RESOURCE_EXHAUSTED`. Attestations evict the oldest batch, whose waiter gets `RESOURCE_EXHAUSTED`. Ping and Shutdown wait without a deadline | LIVE (iff core) | [04 §3](04-chain-core.md#3-scheduler-lanes) |
| I2 | slot-tick thread -> tick scheduler lane | `blocking_push`, waits on Condvar `space` | LIVE (iff core) | [04 §2](04-chain-core.md#2-one-thread-owns-the-store) |
| I3 | core thread -> events task | mpsc(4096) `blocking_send`; the core thread waits | IDLE (iff core; only a hand-submitted `ImportBlock` or `ApplyAttestations` feeds it) | [04 §11](04-chain-core.md#11-event-ring-and-subscriber-contract) |
| I4 | events task -> subscribers | ring of 4096 events / 64 MiB; per-subscriber mpsc(256) `try_send` (Policy B: terminate the slow consumer) | IDLE (no in-tree subscriber) | [04 §11](04-chain-core.md#11-event-ring-and-subscriber-contract) |
| I5 | core thread -> EL | DirectEngine `Handle::block_on(timeout(D))`: newPayload 8 s, fcU 8 s, getBlobs 1 s (enqueue only) | IDLE (iff core). The core thread reads the EL health machine every SlotTick, with no HTTP call. fcU fires only after a hand-submitted `ImportBlock` or `ApplyAttestations`. getBlobs (a 1 s enqueue to the fastpath worker) fires only when a hand-submitted block is DA-deferred and carries commitments. newPayload is never reached behind the DA gate | [08 §4.2](08-execution-engine.md#42-deadlines-and-what-parks-the-core-thread) |
| I6 | core thread -> read models | ArcSwap `HeadSnapshot` / `EpochContext` | LIVE (iff core; content static). The snapshot is published once at spawn and republished only after an import or attestation batch | [04 §9](04-chain-core.md#9-read-models-headsnapshot-and-epochcontext) |
| I7 | producers -> storage-core writer | writer classes P0 32 (block) / P1 64 (block) / P2 256 (drop-newest) | A: P2 LIVE (prune); P0 DEAD-AS-WIRED (ArchiveWriter built, then dropped before serve); P1 DEAD-AS-WIRED (no PutBackfillBatch client, no migrator driver). B: P0 IDLE (iff core; N1 via ArchiveWriter); P1 and P2 DEAD-AS-WIRED (mailboxes created, `crates/storage-core/src/writer.rs:387-389`; no producer hosted) | [07 §4.3](07-storage.md#43-the-single-writer-and-its-mailbox) |
| I8 | p2p swarm task <-> workers | bounded mpsc map (gossip 1024, conn 256, cmd 512, ...) | LIVE | [09 §2.4](09-p2p-host-and-discovery.md#24-channel-map-bounds-and-overflow) |
| I9 | p2p chain-stream client -> Ipc | out mpsc(1024), 2 s budget, <= 1024 outstanding | LIVE (session traffic; gossip objects IDLE) | [03 §5.4](03-internal-contracts.md#54-impls-and-capacities) |
| I10 | `cc_seam::InProcess` | import 64 / column 4096 / publish 256 | TEST-ONLY | [03 §5.4](03-internal-contracts.md#54-impls-and-capacities) |

I3 and I5 are IDLE even with an installed core: no in-tree producer submits blocks or
attestations, so they carry traffic only after an operator's grpcurl call. I4 is IDLE because
nothing in the tree subscribes. Event sends exist only in `crates/chain-core/src/import.rs` and
`crates/chain-core/src/apply_attestations.rs`; where another doc draws I3-I5 as LIVE, the code
supports IDLE. The fcU per-slot floor sends nothing until a first emit
(`crates/chain-core/src/fcu_driver.rs:256-287`). Threads, tasks and every bounded queue are
mapped in [13-concurrency-model.md](13-concurrency-model.md).

### External interfaces: X1..X4

| Id | Interface | Transport | Shape A | Shape B | Doc |
|---|---|---|---|---|---|
| X1 | CL -> EL Engine API | JSON-RPC over HTTP, HS256 JWT; authrpc `el:8551`, `cc` network only | LIVE (upcheck: `eth_syncing` every slot, exchangeCapabilities on the Synced edge); payload calls IDLE (see I5). chain and the engine container each dial it | same; `el_endpoint`, default `127.0.0.1:8551` | [08](08-execution-engine.md) |
| X2 | node <-> network | libp2p TCP + discv5 UDP on 9000, not published by compose | LIVE (discovery and Status handshake; 0 gossip topics) | ABSENT (no p2p) | [09](09-p2p-host-and-discovery.md) |
| X3 | operator -> metrics | HTTP `GET /metrics` on :910N; no auth, no TLS | LIVE | LIVE (:9101) | [12 §6](12-boot-health-and-shutdown.md#6-metrics-and-logging-surfaces) |
| X4 | checkpoint sync, outbound | HTTPS to `checkpoint_providers` (plain http only on loopback) | LIVE (opt-in; the default list is `[]`) | LIVE (opt-in; only when the store is empty) | [12 §3.1](12-boot-health-and-shutdown.md#31-shape-a-cc-chain-binds-first-then-bootstraps) |

Code: X1 `crates/engine-api/src/transport.rs`; X3 `crates/bootstrap/src/metrics_server.rs`; X4
`services/chain/src/checkpoint_sync.rs`.

## Shapes and status labels

| Term | Meaning | Where | Doc |
|---|---|---|---|
| **shape A** | `docker-compose.yml`: six CC containers built from one `Dockerfile` (`ARG SERVICE`) plus `el`. It is the only deployed shape | `docker-compose.yml`, `Dockerfile` | [01](01-deployment-and-processes.md#shape-a-docker-compose) |
| **shape B** | `bin/beacon-core` (`cc-beacon-core`): one process hosting chain-core and the storage-core writer. It is in the image (`Dockerfile:41`) but has no compose service, Make target or CI job. TRANSITIONAL (target, not deployed) | `bin/beacon-core/src/boot.rs:397-577` | [01](01-deployment-and-processes.md#shape-b-binbeacon-core) |

Status labels form a closed set: **LIVE**, **IDLE**, **DEAD-AS-WIRED**, **INERT**, **STUB**,
**DELETED**, **TRANSITIONAL**, **TEST-ONLY**, **DEVNET-ONLY**, **PARAMS-ONLY**, **ABSENT**.
Meanings, and the rules for operator-only paths, `(iff core)` and shape B columns:
[README](README.md#status-labels).

## Terms A-Z

Jump to: [A-C](#a-c) | [D-E](#d-e) | [F-H](#f-h) | [I-O](#i-o) | [P-R](#p-r) | [S](#s) |
[T-Z](#t-z). A name already defined under Collisions points back there.

### A-C

| Term | Meaning and status at `7d8833d` | Code | Doc |
|---|---|---|---|
| advertised window | see ["serve window"](#collisions-qualify-these-words) | | |
| aggregate health | see ["health"](#collisions-qualify-these-words) | | |
| anchor | the checkpoint block and state a core is installed from; residency keeps the state in its `Anchor` role. It is never persisted, so as wired the first child after checkpoint sync would fail the continuity bind (shape B). Not the `AnchorInfo` meta record, which nothing writes in production | `crates/chain-core/src/residency.rs:39` | [04 §10](04-chain-core.md#10-state-residency), [07 §4.4](07-storage.md#44-archivewrite-the-import-persist-path-n1) |
| app score | peer-manager score from -100 to +100: disconnect below -20, ban below -50 (in memory, permanent per process). LIVE | `services/p2p/src/peer_manager/score.rs` | [09 §6](09-p2p-host-and-discovery.md#6-peer-manager-scheduling-scoring-bans) |
| archive handle | `ArchiveWriteHandle = Arc<dyn ArchiveWrite>`, the only seam alias that production code holds; it keeps chain-core from naming a cc-store or storage-core type | `crates/chain-core/src/lib.rs:32` | [03 §5.1](03-internal-contracts.md#51-traits) |
| `ArchiveWrite` | cc-seam trait chain-core -> storage-core (N1): `ingest_columns`, `ingest_block`, `ingest_block_blocking`, `block_is_durable`; the event bus is not the data plane ([ADR-R-02](../adr/ADR-R-02.md)). A: ABSENT. B: IDLE (iff core; blocks), DEAD-AS-WIRED (columns). The trait documents Backpressure; the implementation blocks without a deadline | `crates/seam/src/lib.rs:337` | [07 §4.4](07-storage.md#44-archivewrite-the-import-persist-path-n1) |
| `ArchiveWriter` | the only production `ArchiveWrite` impl; it submits to the P0 writer class. A: DEAD-AS-WIRED (the storage service builds it into an unused `_archive` binding at `crates/storage-core/src/boot.rs:563`, which drops before serve). B: IDLE (injected as `CoreConfig.archive`) | `crates/storage-core/src/archive_write.rs:35` | [07 §4.4](07-storage.md#44-archivewrite-the-import-persist-path-n1) |
| BPO | blob-parameter-only fork: a `BLOB_SCHEDULE` entry that changes `MAX_BLOBS_PER_BLOCK` from an epoch on. It changes the fork digest but not the fork version. Hoodi has two (epoch 52480 -> 15 blobs, 54016 -> 21) | `crates/types/src/config.rs:219` (`get_blob_parameters`); `crates/types/tests/fixtures/hoodi-config.yaml:52-56` | [06 §4](06-consensus-primitives.md#4-eip-7892-fork-digest), [10 §2](10-p2p-gossip-and-das.md#2-topics-and-fork-digests) |
| cache window | see ["serve window"](#collisions-qualify-these-words) | | |
| `canonical` table | cc-store's slot -> root index. Each persisted block rewrites it back to the fork point (`rewrite_from_head`), so it tracks the ancestry of the **last persisted block**, not the fork-choice head. Its only production writer is ArchiveWriter. A: DEAD-AS-WIRED (ArchiveWriter dropped). B: IDLE (as N1) | `crates/store/src/canonical.rs:139`; `crates/storage-core/src/archive_write.rs:321` | [07 §3.2](07-storage.md#32-key-space-and-hotcold-layout) |
| CC-4A block floor | see ["serve window"](#collisions-qualify-these-words) | | |
| cc-seam handles | the traits `ChainIngress` (p2p -> chain-core), `P2pEgress` (chain-core -> p2p) and `ArchiveWrite` (chain-core -> storage-core). cc-chain and cc-p2p meet only through cc-seam ([ADR-R-01](../adr/ADR-R-01.md)) | `crates/seam/src/lib.rs:251,275,337` | [03 §5](03-internal-contracts.md#5-cc-seam-typed-handles) |
| cc-store | see ["storage"](#collisions-qualify-these-words) | | |
| cgc / custody | custody group count: how many column custody groups a node stores and serves. Local MetaData is static at `cgc=4` (`CUSTODY_REQUIREMENT`), and `SetCustodyGroupCount` is a STUB (always `FAILED_PRECONDITION`). By design p2p publishes only custody-sampled columns ([ADR-P3-07](../adr/ADR-P3-07.md)) | `crates/types/src/lib.rs:99`; `services/p2p/src/main.rs:509` | [10 §11](10-p2p-gossip-and-das.md#11-peerdas-custody-sampling-and-recovery) |
| chain-core | see ["core"](#collisions-qualify-these-words) | | |
| chain-stream client | p2p wrapper around `cc_seam::Ipc` (task `chain-stream-client`) that dials E1 when `[peers].chain` is set. LIVE (session + view) | `services/p2p/src/chain_stream/client.rs` | [11](11-p2p-reqresp-and-sync.md#chain-stream-client-e1-n2) |
| `ChainIngress` | cc-seam trait p2p -> chain-core: `submit_gossip`, `notify_data_available`, `submit_column_sidecar`. Production impl `Ipc`. A: IDLE (gossip), DEAD-AS-WIRED (DA, column). B: ABSENT | `crates/seam/src/lib.rs:251` | [03 §5.1](03-internal-contracts.md#51-traits) |
| `ChainView` | the view chain pushes to p2p on `P2pStream`; chain owns it ([ADR-P2-05](../adr/ADR-P2-05.md)). `view_kind`: 1 slot, 2 epoch, 3 head change, 4 full (sent on Hello). It feeds the p2p Status handshake. A: LIVE (content static without an installed core). B: ABSENT | `proto/eth/p2p/v1/p2p.proto:170-193` | [03 §4](03-internal-contracts.md#4-p2pstream-anatomy) |
| checkpoint sync | HTTPS bootstrap (X4) that yields an installed core, fetched from `checkpoint_providers` (genesis, spec, finalized block, state). Shape B uses it only when the store is empty. LIVE (opt-in); the default provider list is empty | `services/chain/src/checkpoint_sync.rs:868` | [12 §3.1](12-boot-health-and-shutdown.md#31-shape-a-cc-chain-binds-first-then-bootstraps), [04 §16](04-chain-core.md#16-the-two-hosts) |
| `checkpoint_root` | optional weak-subjectivity pin for checkpoint sync (X4), read by chain and beacon-core; commented out in `config/chain.toml:31`. When set, the fetched block's root must equal it (in A a bad value makes the boot task `exit(1)`, possibly after bind). When unset, the first provider that returns a self-consistent Fulu triple becomes the fork-choice root of trust, with only a `warn` log and no multi-provider quorum. LIVE (opt-in) in A and B | `services/chain/src/checkpoint_sync.rs:14-25,1054`; `services/chain/src/run.rs:170`; `bin/beacon-core/src/boot.rs:181` | [01](01-deployment-and-processes.md#trust-boundaries), [12 §3.3](12-boot-health-and-shutdown.md#33-checkpoint-sync-protocol) |
| config digest | the store's `config_digest` meta record: SHA-256 over SSZ of the six fork epochs, `SECONDS_PER_SLOT`, `BLOB_SCHEDULE`, GVR, `MIN_VALIDATOR_WITHDRAWABILITY_DELAY` and `CHURN_LIMIT_QUOTIENT`. storage-core always computes it from the bundled Hoodi YAML, with GVR defaulting to zero; a mismatch refuses open (`ConfigDigestMismatch`). LIVE in A and B. The store's config digest does not currently distinguish networks | `crates/store/src/schema.rs:142-160` (payload), `:224`, `:449`; `crates/storage-core/src/open.rs:213-215,341-358` | [07 §3.4](07-storage.md#34-open-gates) |
| continuity bind | a write batch's head `(parent_root, slot)` must already be durable or be the first row of the same batch. As wired, the first block after checkpoint sync would fail it with `INVALID_ARGUMENT`, because the anchor is never persisted | `crates/storage-core/src/archive_write.rs:131-154` | [07 §4.4](07-storage.md#44-archivewrite-the-import-persist-path-n1) |
| core scheduler | `SharedScheduler`: one `Mutex`, one Condvar and park/unpark over the five scheduler lanes; the plan (§3.2) and the code call it "Loop B". LIVE (iff core) | `crates/chain-core/src/core.rs:330-509` | [04 §3](04-chain-core.md#3-scheduler-lanes) |
| core thread | see ["core"](#collisions-qualify-these-words) | | |
| core-absent | see ["core"](#collisions-qualify-these-words) | | |
| `CoreHandle` | producer API onto the core scheduler; each request gets a oneshot reply. LIVE (iff core; the liveness sampler's Ping uses it every 3 s). While core-absent, the RPCs answer `NOT_BOOTSTRAPPED` instead | `crates/chain-core/src/core.rs:857-1153` | [04 §4](04-chain-core.md#4-corehandle-chainserviceimpl-and-core-absent-mode) |
| cursor | see ["cursor"](#collisions-qualify-these-words): event cursor or write cursor | | |

### D-E

| Term | Meaning and status at `7d8833d` | Code | Doc |
|---|---|---|---|
| DA gate | `PeerDasAvailability` set membership, the first check in `on_block`, with no zero-blob exemption. IDLE (iff core; only a hand-submitted ImportBlock reaches it). No production path marks data available (E2 is DEAD-AS-WIRED), so it never opens and defers every block that reaches it | `crates/fork-choice/src/da_seam.rs:80`; `crates/fork-choice/src/on_block.rs:230-234` | [04 §6](04-chain-core.md#6-da-gate-and-pending_da) |
| dangerous-knob guard | `check_dangerous_knobs`: three devnet-only knobs (`storage.retention_override`, `storage.debug.crash_point`, a `chain.event_ring_bytes` shrink) are refused unless the configured GVR is set and is neither Hoodi's nor mainnet's. A: LIVE (chain and the storage service run it before bind). B: ABSENT (beacon-core never calls it and reads `event_ring_bytes` unguarded) | `crates/config/src/devnet_guard.rs:92,142`; `services/chain/src/run.rs:311`; `crates/storage-core/src/boot.rs:475`; `bin/beacon-core/src/boot.rs:174` | [01](01-deployment-and-processes.md#dangerous-knob-guard) |
| DEFERRED_DA | see ["verdict"](#collisions-qualify-these-words): an import verdict | | |
| devnet mode | `cc-p2p --publish-fixture` / `--devnet-peer`: subscribes `beacon_block` and 128 column topics at the epoch-0 digest and ACCEPTs everything unvalidated. DEVNET-ONLY | `services/p2p/src/fault_mode.rs:907` | [09 §10](09-p2p-host-and-discovery.md#10-fault-modes-and-devnet-mode) |
| DirectEngine | see ["engine"](#collisions-qualify-these-words) | | |
| drain / pre-drain hook | the shutdown sequence of every gRPC process: SIGTERM -> aggregate health NOT_SERVING -> pre-drain hook (joins the core thread in chain and beacon-core) -> 150 ms pause -> drain <= 3 s (`DRAIN_TIMEOUT`) -> exit. LIVE. Shutdown normally fits the 5 s grace period; with an installed core the worst case is unbounded (`push_wait` has no deadline, [12 §5.2](12-boot-health-and-shutdown.md#52-budgets-against-stop_grace_period-5s)) | `crates/bootstrap/src/serve.rs:33,39,98` | [12 §5.1](12-boot-health-and-shutdown.md#51-sequence) |
| durable set | `DurableSet`, loaded when the store opens. A: DEAD-AS-WIRED (the storage service's resume sequence checks the store, which is always empty at HEAD, and only logs the outcome; its consumer, E4, was deleted in 60c6200; the resume schema check itself is LIVE ([07 §1](07-storage.md#1-status-at-a-glance))). B: LIVE (always `None` at HEAD), and it drives `seed_from_durable`. So every boot is checkpoint sync or core-absent | `crates/storage-core/src/open.rs:246-272`; `crates/storage-core/src/boot.rs:528-548` | [07 §4.5](07-storage.md#45-durable-set-resume-and-seed) |
| early ACCEPT | gossip verdict sent before the STF (import stage 5, [04 §5](04-chain-core.md#5-import-pipeline-on-the-core-thread)). IDLE (no gossip arrives) | `crates/chain-core/src/import.rs:437-441` | [05](05-block-lifecycle.md#hops-4-12-inside-chain-core) |
| EL | see ["engine"](#collisions-qualify-these-words) | | |
| EL health machine | Offline / Syncing / Synced / AuthFailed; it starts Offline, and AuthFailed is terminal. External Online = {Synced, Syncing}. In A, chain and the engine container each run their own. LIVE in chain and beacon-core; LIVE (TRANSITIONAL host) in the engine container | `crates/engine-api/src/state.rs:34` | [08 §3.5](08-execution-engine.md#35-el-health-state-machine) |
| engine container | see ["engine"](#collisions-qualify-these-words) | | |
| engine-api | see ["engine"](#collisions-qualify-these-words) | | |
| `EngineApi` | engine-api host: transport, EL health machine, upcheck driver, fastpath worker. chain and beacon-core build it with `finish(None)`, so the process that drives the EL exports no `cc_engine_*` series. LIVE in chain and beacon-core; LIVE (TRANSITIONAL host) in the engine container | `crates/engine-api/src/api.rs:86` | [08 §3](08-execution-engine.md#3-cc-engine-api) |
| engine-blackhole overlay | `docker-compose.engine-blackhole.yml` plus the `cc-engine-blackhole` binary replace `engine:9004`. chain no longer dials that address, so the overlay cannot park chain-core; `services/chain/tests/engine_blackhole_liveness.rs` is the real demonstration. TRANSITIONAL (opt-in overlay) | `services/engine/src/bin/blackhole.rs` | [01](01-deployment-and-processes.md#the-engine-blackhole-overlay) |
| EngineStream | the E8 bidi stream on P2pService. p2p attaches the server (`da_tx = None`); its only client, in the engine container, was deleted in 631994c. DEAD-AS-WIRED | `services/p2p/src/main.rs:499-508` | [11](11-p2p-reqresp-and-sync.md#enginestream-server-e8) |
| `EpochContext` | see [`HeadSnapshot` / `EpochContext`](#f-h) | | |
| event cursor | see ["cursor"](#collisions-qualify-these-words) | | |
| event ring / events task | ring of 4096 events / 64 MiB owned by one tokio task; subscribers use Policy B (terminate the slow consumer). IDLE | `crates/chain-core/src/events/mod.rs:90,95` | [04 §11](04-chain-core.md#11-event-ring-and-subscriber-contract) |
| event session id | see ["session id"](#collisions-qualify-these-words) | | |

### F-H

| Term | Meaning and status at `7d8833d` | Code | Doc |
|---|---|---|---|
| fastpath | engine-api `getBlobsV2` -> cells -> 128 sidecars on the fastpath transport lane; production skips `verify_cell_kzg_proof_batch` ([ADR-P3-15](../adr/ADR-P3-15.md)). The call runs for a DA-deferred block that carries commitments. Output DEAD-AS-WIRED (discarded) | `crates/engine-api/src/fastpath/` | [08 §3.7](08-execution-engine.md#37-getblobsv2-fastpath-and-who-consumes-it) |
| `FcuDriver` | builds the forkchoiceUpdatedV3 triple with a monotonic sequence, a per-slot floor and no dedupe. IDLE (iff core; needs a first emit). Nothing emits when the core thread spawns; the per-slot floor re-sends only after a first emit from an import, an attestation batch or `DataAvailable` | `crates/chain-core/src/fcu_driver.rs:117`, `:256-287` (`on_slot`) | [04 §8](04-chain-core.md#8-forkchoiceupdated-driver), [08 §4.4](08-execution-engine.md#44-fcu-driver) |
| fcU session id | see ["session id"](#collisions-qualify-these-words) | | |
| fork digest | EIP-7892 digest. `base = hash_tree_root(ForkData{version, genesis_validators_root})`; the 4-byte version is zero-padded to one 32-byte chunk in every branch. Before Fulu: `base[0..4]`. From Fulu on: `(base XOR sha256(u64le(bp.epoch) \|\| u64le(bp.max_blobs_per_block)))[0..4]`, where `bp` is the blob-schedule entry active at that epoch (the Electra tuple before the first BPO). It lives in p2p, not cc-types. LIVE for ENR `eth2` / `nfd`, the discv5 allowed-digest filter (`ForkContext::discovery_allowed_digests`, `services/p2p/src/discovery/task.rs:486,522,558`, through `allowed_digests` at `:626-627`) and Status; never used to build topic strings in production | `services/p2p/src/fork_digest.rs:78-100` | [06 §4](06-consensus-primitives.md#4-eip-7892-fork-digest) |
| fork-schedule authority | `ChainConfig::fork_schedule` and its accessors; the only allowed walk outside cc-types is `fork_digest.rs` | `crates/types/src/config.rs:227-307` | [06 §3.3](06-consensus-primitives.md#33-fork-schedule-authority) |
| Fulu-only decode | block and state SSZ decode accept only `ForkName::Fulu` | `crates/types/src/block.rs:135-142` | [06 §3.4](06-consensus-primitives.md#34-containers-ssz-and-the-fulu-only-chokepoints) |
| gossip verdict | see ["verdict"](#collisions-qualify-these-words) | | |
| `HeadSnapshot` / `EpochContext` | ArcSwap read models published by the core thread (I6). `current_epoch_target_root` and `dependent_root` are always ZERO. LIVE (content static; see I6). While core-absent they hold zero defaults, which `ChainView` still reads | `crates/chain-core/src/head.rs:14-85`; `epoch_context.rs` | [04 §9](04-chain-core.md#9-read-models-headsnapshot-and-epochcontext) |
| health DAG | the `[peers]` lists probed over N3; it roots at chain ([ADR-07](../adr/ADR-07.md)). p2p -> chain, storage service; storage service, engine container, attestation -> chain; beacon-api -> chain, attestation, storage service. A: LIVE. B: ABSENT (`[peers]` empty) | `[peers]` in `config/<svc>.toml`, e.g. `config/p2p.toml:42-45` | [12 §4.3](12-boot-health-and-shutdown.md#43-health-dag) |
| Hoodi | the Ethereum testnet this client targets: 9 discv5 bootnodes in `config/p2p.toml`, two BPO entries. The store's config digest always uses the bundled `hoodi-config.yaml`; it does not currently distinguish networks | `crates/types/tests/fixtures/hoodi-config.yaml`; `crates/storage-core/src/open.rs:341-358` | [09 §5.1](09-p2p-host-and-discovery.md#51-the-discovery-task) |
| hot / cold / split | `blocks_hot` versus the epoch-sharded cold `blocks_{ddddd}` tables (block shards 256 epochs, column shards 32, [ADR-P4-10](../adr/ADR-P4-10.md)), divided by the `split` meta record. Nothing writes `split`, so migration never runs (see migrator). A: IDLE (nothing writes blocks). B: IDLE (as N1) | `crates/store/src/split.rs` | [07 §3.2](07-storage.md#32-key-space-and-hotcold-layout) |

### I-O

| Term | Meaning and status at `7d8833d` | Code | Doc |
|---|---|---|---|
| I-node-id | the `node_id` store invariant: it compares the raw 32 node-key bytes, not a derived NodeId; a mismatch refuses open ([ADR-P4-13](../adr/ADR-P4-13.md)). A: INERT (no identity row is ever written). B: LIVE | `crates/store/src/invariants.rs:879-905` | [07 §3.5](07-storage.md#35-the-eight-invariants-and-i-node-id) |
| import pipeline | `import_block_with_early` on the core thread: pre-STF checks, early ACCEPT, DA gate, STF (newPayload inside `process_execution_payload`), fork choice, `finish_imported` (N1 persist, then events). IDLE (iff core; no block producer) | `crates/chain-core/src/import.rs:349-632`, `:988-993`; `crates/fork-choice/src/on_block.rs:258` | [04 §5](04-chain-core.md#5-import-pipeline-on-the-core-thread), [05](05-block-lifecycle.md) |
| import verdict | see ["verdict"](#collisions-qualify-these-words) | | |
| `InProcess` | cc-seam test impl of `ChainIngress` + `P2pEgress` (I10). TEST-ONLY | `crates/seam/src/in_process.rs:58` | [03 §5.4](03-internal-contracts.md#54-impls-and-capacities) |
| installed core | see ["core"](#collisions-qualify-these-words) | | |
| `Ipc` | production cc-seam impl: p2p's tonic client over `P2pStream`, plus the mailbox-only `IpcEgress`. LIVE in p2p | `crates/seam/src/ipc.rs:178` | [03 §5.4](03-internal-contracts.md#54-impls-and-capacities) |
| KZG verify pool | `cc-kzg-verify-{i}` OS threads (K = max(2, cores/2)) plus a bridge task. Its sender drops at startup. DEAD-AS-WIRED (self-terminating) | `services/p2p/src/das/verify_pool.rs:1009`; `services/p2p/src/service.rs:705` | [10 §10](10-p2p-gossip-and-das.md#10-kzg-verification-inline-today-pool-disconnected) |
| liveness sampler | Ping through the tick scheduler lane every slot/4 (3 s), with a per-sample deadline of ~4.0 s; 3 misses -> NOT_SERVING, 3 successes -> back ([ADR-R-04](../adr/ADR-R-04.md)). It owns `local_ready` after install. LIVE (iff core). Liveness flips aggregate health; nothing in this repository restarts a process on that signal | `crates/chain-core/src/liveness.rs` | [04 §14](04-chain-core.md#14-liveness-sampler), [12 §4.4](12-boot-health-and-shutdown.md#44-liveness-sampler-a-no-op-through-the-core-thread) |
| local_ready | see ["health"](#collisions-qualify-these-words) | | |
| Loop A | the plan's name (§3.4) for p2p gossip validation: today the one serial `gossip-validate` loop (see validation pool), IDLE; its 8-queue `ValidatorKind` taxonomy is Planned, not built ([Planned changes](#planned-changes)) | `services/p2p/src/gossip/validate/pipeline.rs:385` | [13 §4](13-concurrency-model.md#4-cc-scheduler-and-the-core-threads-scheduler), plan §3.4 |
| Loop B | the plan's and the code's name for the core scheduler | `cc_scheduler::LOOP_B_LANES`, `crates/scheduler/src/chain.rs:121` | [04 §3](04-chain-core.md#3-scheduler-lanes) |
| migrator | storage-core hot -> cold migration object, with no driver. A: DEAD-AS-WIRED. B: ABSENT | `crates/storage-core/src/migrate.rs` | [07 §4.9](07-storage.md#49-replay-migration-metrics-rehearsal) |
| node key | 32 raw secp256k1 bytes, mode exactly 0600, on volume `cc-p2p-identity`; the storage service mounts it read-only for I-node-id. A: LIVE. B: LIVE (beacon-core creates a missing node key without enforcing 0600) | `services/p2p/src/identity.rs` | [09 §4](09-p2p-host-and-discovery.md#4-identity-node-key-and-enr) |
| `NOT_BOOTSTRAPPED` | `ErrorInfo.reason` sent with `FAILED_PRECONDITION` while the process is core-absent. Ipc maps it to `SeamError::FailedPrecondition`. IDLE: both default shapes answer it, but only operator scripts and the IDLE N2 call the core-gated RPCs | `crates/chain-core/src/service.rs:52` | [04 §4](04-chain-core.md#4-corehandle-chainserviceimpl-and-core-absent-mode) |
| optimistic import | an EL SYNCING / ACCEPTED answer marks the block's node Optimistic in proto-array only ([ADR-P3-10](../adr/ADR-P3-10.md)); `IsOptimistic` reads that status. IDLE (no import reaches the EL) | `crates/fork-choice/src/execution_status.rs:30,77` | [04 §7](04-chain-core.md#7-pending_engine-and-optimistic-import), [06 §7.3](06-consensus-primitives.md#73-optimistic-bookkeeping-and-invalidation) |

### P-R

| Term | Meaning and status at `7d8833d` | Code | Doc |
|---|---|---|---|
| `P2pEgress` | cc-seam trait chain-core -> p2p: `publish` (Policy C: drop and report) and `update_view`. chain writes `ChainToP2p` directly instead, and p2p binds its `IpcEgress` to `_egress` and never uses it. DEAD-AS-WIRED | `crates/seam/src/lib.rs:275`; `services/p2p/src/chain_stream/client.rs:239` | [03 §5.1](03-internal-contracts.md#51-traits) |
| `P2pStream` session | one bidi stream from p2p to chain carrying E1 and E2. chain allows <= 8 sessions and handles inbound messages serially; the 30 s Endpoint timeout does not tear it down. A: LIVE (session + view). B: ABSENT | `crates/chain-core/src/p2p_stream.rs:61,436-542` | [03 §4](03-internal-contracts.md#4-p2pstream-anatomy) |
| peer prober | one task per `[peers]` entry: `Check{service:""}` every 3 s with a 1 s timeout; down after 2 failures, up after 1 success; never exits the process. LIVE (N3) | `crates/bootstrap/src/prober.rs:23-27` | [12 §4.2](12-boot-health-and-shutdown.md#42-peer-prober-timing) |
| peer scoring | GossipSub peer-scoring parameters ([p2p-scoring.md](../p2p-scoring.md)), never installed (no `with_peer_score`). PARAMS-ONLY. Not the app score | `services/p2p/src/gossip/scoring.rs` | [10 §12](10-p2p-gossip-and-das.md#12-gossip-scoring-parameters-params-only) |
| `pending_da` | DA-deferred blocks, 64 deep with a 4-slot expiry; re-driven on `DataAvailable`, which never arrives. IDLE (iff core): a hand-submitted block parks here and expires after 4 slots; its re-drive never fires | `crates/chain-core/src/da.rs:43-49` | [04 §6](04-chain-core.md#6-da-gate-and-pending_da) |
| `pending_engine` | blocks deferred on an EL failure, 64 deep with an 8-slot expiry, kept apart from `pending_da` ([ADR-P3-05](../adr/ADR-P3-05.md)); re-driven on an Offline -> Online EL edge seen at SlotTick. IDLE. Engine failures defer the block rather than reject it; re-drive depends on an EL health transition | `crates/chain-core/src/pending_engine.rs:26-29` | [04 §7](04-chain-core.md#7-pending_engine-and-optimistic-import), [08 §4.3](08-execution-engine.md#43-pending_engine-and-re-drive) |
| Policy A / B / C / D | see ["Policy A/B/C/D"](#collisions-qualify-these-words) | | |
| proto-array | cc-fork-choice's flat node array for LMD-GHOST head selection; each node also carries its execution status (Valid / Optimistic / Invalid) | `crates/fork-choice/src/proto_array.rs` | [06 §7.1](06-consensus-primitives.md#71-head-selection-lmd-ghost-over-proto-array) |
| prune task | storage-core tokio task on a 4 s interval that prunes by wall-clock epoch; `drop_table` bypasses the writer. A: LIVE. B: ABSENT | `crates/storage-core/src/prune/mod.rs` | [07 §4.6](07-storage.md#46-prune-shape-a) |
| re-drive | re-import of a parked block from `pending_da` (on `DataAvailable`) or `pending_engine` (on an Offline -> Online edge). IDLE: neither trigger fires | `crates/chain-core/src/core.rs:2064-2219` | [08 §4.3](08-execution-engine.md#43-pending_engine-and-re-drive) |
| reorg gap | the parent state is neither resident nor replayable, so the block is INVALID. IDLE (no import) | `crates/chain-core/src/residency.rs` | [04 §10](04-chain-core.md#10-state-residency) |
| replay task | storage-core tokio task on a 2 s interval; it skips while `split.slot == 0`, which is always. A: IDLE. B: ABSENT | `crates/storage-core/src/replay.rs` | [07 §4.9](07-storage.md#49-replay-migration-metrics-rehearsal) |
| residency | <= 4 pinned states (Head, Anchor, EpochBoundary, Scratch) plus a 64-body ring ([ADR-P1-12](../adr/ADR-P1-12.md)). IDLE (iff core; it holds the anchor, since no import arrives) | `crates/chain-core/src/residency.rs:39,77` | [04 §10](04-chain-core.md#10-state-residency) |

### S

| Term | Meaning and status at `7d8833d` | Code | Doc |
|---|---|---|---|
| scheduler lane | see ["lane"](#collisions-qualify-these-words) | | |
| `SeamError` | exactly four variants: `Backpressure{bound, waited_ms}`, `Unavailable`, `InvalidArgument`, `FailedPrecondition{reason}`; adding one needs an ADR | `crates/seam/src/lib.rs:42-57` | [03 §5.2](03-internal-contracts.md#52-seamerror) |
| seed | `seed_from_durable`: rebuilds an installed core from the durable set before bind, on `spawn_blocking`. It replaces E4 (`RestoreFromStore`, DELETED). A: ABSENT. B: IDLE (the durable set is always `None`) | `crates/chain-core/src/seed.rs:383` | [07 §4.5](07-storage.md#45-durable-set-resume-and-seed), [12 §3.2](12-boot-health-and-shutdown.md#32-shape-b-cc-beacon-core-seeds-before-bind) |
| self-name | see ["health"](#collisions-qualify-these-words) | | |
| session id | see ["session id"](#collisions-qualify-these-words): stream, event, fcU or writer session id | | |
| shape A / shape B | see [Shapes and status labels](#shapes-and-status-labels) | | |
| slot-tick thread | OS thread `chain-slot-tick`: genesis-aligned `SlotTick` via `blocking_push` (I2). It is not the `P2pStream` slot-tick driver task. LIVE (iff core) | `crates/chain-core/src/core.rs:1902-1929` | [04 §2](04-chain-core.md#2-one-thread-owns-the-store) |
| stage (S0..S5) | the migration stages of `plan/architecture.md` §9.1: S0 correctness floor; S1 fold the EL bridge; S2 fold storage; S3 wiring completion and first Hoodi soak (it selects the E1/E2/E8 transport); S4 the fork seam; S5 Phases 5-7 on standard boundaries. At `7d8833d`, S1's fold is built (E3 DELETED in 631994c). S2's is partly built: E4 and E7 are DELETED, E5/E6 survive on the TRANSITIONAL storage service, and its exit gate `E2.2` is open | `plan/issues/s2-exit-note.md` | [`plan/architecture.md`](../../plan/architecture.md) §9.1 |
| Status handshake | status/2 + ping/1 + metadata/3 + goodbye/1, run inside the swarm task. Local MetaData is static (`seq=0, cgc=4`), and `earliest_available_slot` comes from the advertised window. LIVE | `services/p2p/src/reqresp/handshake.rs` | [11](11-p2p-reqresp-and-sync.md#status-handshake) |
| storage serve window | see ["serve window"](#collisions-qualify-these-words) | | |
| storage service | see ["storage"](#collisions-qualify-these-words) | | |
| storage-core | see ["storage"](#collisions-qualify-these-words) | | |
| storage-core writer | one tokio task (not an OS thread): biased shutdown > P0 > P1 > P2, fsync inline, a panic guard that exits ([ADR-P4-04](../adr/ADR-P4-04.md)). A: LIVE (fed by prune via P2). B: IDLE (its only producer, N1, is IDLE) | `crates/storage-core/src/writer.rs:52-56` (bounds), `:434` (`run_writer`; biased select `:469`) | [07 §4.3](07-storage.md#43-the-single-writer-and-its-mailbox) |
| store | see ["storage"](#collisions-qualify-these-words) | | |
| store invariants | eight checks run when the store opens: contig, col_block, split_fin, ring, window, node_id, shards, cursor. They run at open in A and B (`check_invariants = true`), but at HEAD every check is INERT in A (nothing it checks exists yet: no meta row, column row or shard table); in B only `node_id` and `cursor` take effect ([07 §3.5](07-storage.md#35-the-eight-invariants-and-i-node-id)) | `crates/store/src/invariants.rs:94` | [07 §3.5](07-storage.md#35-the-eight-invariants-and-i-node-id) |
| stream session id | see ["session id"](#collisions-qualify-these-words) | | |
| supervisor | p2p per-task panic policy `TaskPolicy` ([ADR-P2-13](../adr/ADR-P2-13.md)): ProcessFatal (swarm), Respawn (default worker), RespawnBudget 5 per 300 s, then fatal (discovery). The `supervisor` task polls JoinHandles every 10 ms. LIVE | `services/p2p/src/supervisor.rs:26` | [09 §2.3](09-p2p-host-and-discovery.md#23-supervisor-and-panic-policy) |
| swarm task | the sole owner of `Swarm<CcBehaviour>`; req/resp and the Status handshake run inline in it ([ADR-P2-02](../adr/ADR-P2-02.md)). LIVE | `services/p2p/src/host.rs:86-325` | [09 §2.2](09-p2p-host-and-discovery.md#22-task-and-channel-map) |

### T-Z

| Term | Meaning and status at `7d8833d` | Code | Doc |
|---|---|---|---|
| transport lane | see ["lane"](#collisions-qualify-these-words) | | |
| upcheck | engine-api per-slot `eth_syncing` probe, plus one re-probe 250 ms later while not Synced (`spawn_upcheck_driver`, `crates/engine-api/src/state.rs:553-597`); `exchangeCapabilities` runs on the Synced edge. Runs on the upcheck transport lane, never the ordered one ([ADR-P3-08](../adr/ADR-P3-08.md)), with or without an installed core. LIVE in chain, beacon-core and the engine container | `crates/engine-api/src/api.rs:174` | [08 §3.3](08-execution-engine.md#33-methods-versions-and-timeouts) |
| validation pool | the `gossip-validate` task, one serial loop. IDLE. At HEAD p2p subscribes to no gossip topics; the plan schedules gossip wiring for a later stage | `services/p2p/src/gossip/validate/pipeline.rs:385` | [10 §4](10-p2p-gossip-and-das.md#4-the-validation-pipeline) |
| verdict | see ["verdict"](#collisions-qualify-these-words): gossip verdict or import verdict | | |
| WatchServeWindow client | p2p loop (a detached `tokio::spawn`, only if `[peers].storage` is set) that consumes E6 and writes the advertised window. LIVE (content static) | `services/p2p/src/storage_client.rs:389-538` | [11](11-p2p-reqresp-and-sync.md#storage-client-e6-e5) |
| write cursor | see ["cursor"](#collisions-qualify-these-words) | | |
| writer class | see ["lane"](#collisions-qualify-these-words) | | |
| writer session id | see ["session id"](#collisions-qualify-these-words) | | |

## Planned changes

From [`plan/architecture.md`](../../plan/architecture.md) §0, §2.3, §3.4 and §9.1. None of these
is built at `7d8833d`.

- **Planned: edges deleted or folded.** E5 and E6 leave as RPCs: backfill moves behind
  `ArchiveWrite`, and the storage serve window becomes one in-process `AtomicU64` read. Stage S3
  selects the E1/E2/E8 transport (plan §2.3). E8 folds into E1's egress half.
  ([`plan/issues/s2-exit-note.md`](../../plan/issues/s2-exit-note.md) counts E5/E6 as deleted
  from the S2 two-process inventory; in code, neither replacement exists yet, and both RPCs
  still run on the TRANSITIONAL storage service.)
- **Planned: process names.** The TRANSITIONAL cc-chain, storage service and engine container
  leave once shape B carries the load. The target is two processes, p2p and beacon-core (plan
  §0, §9.1).
- **Planned: Loop A.** Gossip validation in p2p gets its own 8 queues keyed by `ValidatorKind`
  (plan §3.4). The plan places the taxonomy after S3 in §3.4 and §3.8 but lists it under S3 in
  §9.1. These queues are not scheduler lanes; call them Loop A queues.
- **Planned (S5):** the cc-attestation and cc-beacon-api names disappear; see
  [README](README.md#planned-changes).
