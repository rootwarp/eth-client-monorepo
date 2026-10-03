# Internal contracts and seams

> **As built at `7d8833d`** (`develop`, 2026-08-16). Status labels and diagram legend: [README](README.md#status-labels).
> Target design: [`plan/architecture.md`](../../plan/architecture.md) §2.

Internal contracts come in two layers. The **wire layer** is seven proto3 packages compiled into
`cc-proto`, served over gRPC by the shape A containers (and, for ChainService, by shape B). The
**seam layer** is `cc-seam`: Rust traits with a stated overflow policy ([ADR-R-01](../adr/ADR-R-01.md),
[ADR-R-02](../adr/ADR-R-02.md)). This page inventories both, gives each edge's status at HEAD, and
lists where contract and code disagree. See also [../contracts.md](../contracts.md) (ChainService
semantics), [04](04-chain-core.md) (chain-core) and [11](11-p2p-reqresp-and-sync.md) (req/resp, sync).

## 1. Proto packages

| Package | File | Service | Imports | Notes |
|---|---|---|---|---|
| `eth.common.v1` | `proto/eth/common/v1/common.proto` | none | -- | `Source` {UNSPECIFIED, GOSSIP, REQRESP, API}; `BuildInfo` |
| `eth.chain.v1` | `proto/eth/chain/v1/chain.proto` | `ChainService` (11 RPCs) | common, p2p | events, `Cursor`, `Checkpoint` |
| `eth.p2p.v1` | `proto/eth/p2p/v1/p2p.proto` | `P2pService` (3) | common, engine | also holds the P2pStream enums and messages (`:39-193`) and the EngineStream messages (`:210-251`) |
| `eth.engine.v1` | `proto/eth/engine/v1/engine.proto` | `EngineService` (5) | common | payload status, fcU, `FetchBlobsRequest` |
| `eth.storage.v1` | `proto/eth/storage/v1/storage.proto` | `StorageService` (10) | common | own `FinalizedCheckpoint`, so this package does not import `eth.chain.v1` |
| `eth.attestation.v1` | `proto/eth/attestation/v1/attestation.proto` | `AttestationService` (1) | common | GetInfo only |
| `eth.beacon_api.v1` | `proto/eth/beacon_api/v1/beacon_api.proto` | `BeaconApiService` (1) | common | GetInfo only; no REST server exists |
| `google.rpc` (vendored) | `proto/third_party/google/rpc/` | none | -- | `Status`, `ErrorInfo`; see [ADR-P1-14](../adr/ADR-P1-14.md) |

The import graph is `chain -> p2p -> engine -> common` (chain imports p2p because P2pStream uses
`eth.p2p.v1` messages); storage, attestation and beacon_api import only common, so no cycle.

**Payload rule.** Large consensus containers (blocks, states, attestations, payloads, sidecars)
cross the wire only as `bytes ssz` plus scalars; only checkpoint-sized identity messages are
modelled (`Checkpoint`, `chain.proto:90`; `FinalizedCheckpoint`, `storage.proto:207`).
`scripts/check-no-remodelling.sh:19` (in `make proto`) fails on any `proto/` line holding
`message ` and one of six container names (`BeaconBlock`, `BeaconState`, ...). **Error details**
ride as a `google.rpc.ErrorInfo` in the `grpc-status-details-bin` trailer, written and read by
one helper pair (`crates/proto/src/lib.rs:134-181`).

| Reason | Domain | Raised by |
|---|---|---|
| `NOT_BOOTSTRAPPED` | `eth.chain.v1` | every core-gated ChainService RPC while core-absent (`crates/chain-core/src/service.rs:52`) |
| `BELOW_FINALIZED_RETENTION` | `eth.chain.v1` | `service.rs:55` |
| `CURSOR_TOO_OLD`, `CURSOR_UNKNOWN_SESSION` | `eth.chain.v1` | SubscribeEvents resume (`crates/chain-core/src/events/cursor.rs:10-13`) |
| `STREAM_SESSION_LIMIT` | `eth.chain.v1` | P2pStream open while 8 sessions are live, `RESOURCE_EXHAUSTED` (`crates/chain-core/src/p2p_stream.rs:235-258`) |
| `UNKNOWN_TOPIC` | `eth.chain.v1` | `INVALID_ARGUMENT` from the in-process `request_publish` topic check (`p2p_stream.rs:228-233,339-351`), never on the wire; DEAD-AS-WIRED (no caller) |
| `FCU_DROPPED_STALE` | `eth.engine.v1` | cc-engine ForkchoiceUpdated, `ABORTED` (`services/engine/src/main.rs:181-184`; enum `crates/proto/src/lib.rs:192-230`); no caller |
| `NOT_AVAILABLE` | `eth.storage.v1` | GetSnapshotState for a slot outside the snapshot ring, `FAILED_PRECONDITION` (`crates/storage-core/src/history.rs:30-33,76-89`); at HEAD every call, since nothing writes a snapshot |

## 2. gRPC services and RPCs

Every gRPC process also serves `grpc.health.v1.Health` and reflection v1 from
`cc_proto::FILE_DESCRIPTOR_SET` (`crates/bootstrap/src/serve.rs:304-312`). An RPC that no impl
overrides answers `UNIMPLEMENTED` (`generate_default_stubs(true)`). "Handler" below is the status
of the server code; "Dialer" is the in-tree production client at HEAD (section 3 depends on it).
Handler is the status of the server code under [README](README.md#status-labels) rule (1): a
ChainService RPC whose only input is an operator call or an IDLE dialer is IDLE, an EngineService
or StorageService RPC with no in-tree client is DEAD-AS-WIRED (served, never dialed), and
`(iff core)` marks a handler that answers `NOT_BOOTSTRAPPED` until a core is installed.
**AttestationService** (:9003) and **BeaconApiService** (:9005) are STUB services: GetInfo only.

**ChainService**: cc-chain :9001 (shape A, a TRANSITIONAL host, `services/chain/src/run.rs:525`)
and cc-beacon-core :9001 (shape B, `bin/beacon-core/src/boot.rs:567`); impl `crates/chain-core/src/service.rs:224-530`.

| RPC | Kind | Bound | Dialer at HEAD | Handler |
|---|---|---|---|---|
| GetInfo | unary | -- | none | LIVE |
| ImportBlock | unary | -- | none | IDLE (iff core): operator grpcurl only; core-absent answers NOT_BOOTSTRAPPED |
| GetHead | unary | -- | operator scripts only (`scripts/soak-sampler.sh`, `scripts/restart-trials.sh`) | IDLE (iff core): operator scripts only; core-absent answers NOT_BOOTSTRAPPED |
| SubscribeEvents | server-stream | subscriber mpsc(256), Policy B; cursor contract in [04 §11](04-chain-core.md#11-event-ring-and-subscriber-contract) | none: the cc-storage consumer (E7) was deleted in 78a90e1 | IDLE (no in-tree subscriber; served with an empty ring) |
| ApplyAttestations | unary | <= 128 per batch | none (cc-attestation is a stub) | IDLE (iff core); core-absent answers NOT_BOOTSTRAPPED |
| GetCommitteeShuffling | unary | current and next epoch | none | IDLE (iff core); core-absent answers NOT_BOOTSTRAPPED |
| GetValidatorPubkeys | unary | <= 256 indices | none | IDLE (iff core); core-absent answers NOT_BOOTSTRAPPED |
| P2pStream | bidi | <= 8 sessions | p2p, through `cc_seam::Ipc` (`crates/seam/src/ipc.rs:589-591`) | LIVE; per-arm status in section 3 |
| GetValidatorRecords | unary | <= 256 indices | p2p `RpcValidatorRecordSource` (`services/p2p/src/chain_stream/records.rs:87-128`) | IDLE (iff core; its dialer, N2, is IDLE); core-absent answers NOT_BOOTSTRAPPED |
| IsOptimistic | unary | -- | none | IDLE (iff core); core-absent answers NOT_BOOTSTRAPPED |
| GetCanonicalRoots | unary | span <= 4096 | none (its cc-storage gap-fill caller was deleted) | IDLE (iff core); core-absent answers NOT_BOOTSTRAPPED |

"(iff core)": the handler answers `FAILED_PRECONDITION` + `NOT_BOOTSTRAPPED` until `install_core`
runs (GetHead: while the head snapshot is zero, `service.rs:277-292`). Shipped `checkpoint_providers = []`
(`config/chain.toml:27`, `config/beacon-core.toml:23`) leaves both shapes core-absent, so only
GetInfo, SubscribeEvents and P2pStream answer. Size bounds are `INVALID_ARGUMENT`, never truncation.
GetCommitteeShuffling outside current/next epoch is `FAILED_PRECONDITION` without ErrorInfo
(`crates/chain-core/src/core.rs:1435-1439`); the session cap and a full subscriber queue answer `RESOURCE_EXHAUSTED`.

**P2pService**: cc-p2p :9002 (`services/p2p/src/main.rs:498-511`); impl `services/p2p/src/service.rs:1166-1233`.

| RPC | Kind | Dialer at HEAD | Handler |
|---|---|---|---|
| GetInfo | unary | none | LIVE |
| SetCustodyGroupCount | unary | none | STUB: always `FAILED_PRECONDITION` "cgc hook not attached" (`service.rs:1189-1206`) |
| EngineStream | bidi | none: the engine container's client was deleted in 631994c | DEAD-AS-WIRED (served, never dialed): `build_minimal_engine_stream` attached (`main.rs:503-508`; `services/p2p/src/engine_stream/server.rs:68-101`), inject publish is a `NoopPublisher` (`server.rs:96`); see [11](11-p2p-reqresp-and-sync.md#enginestream-server-e8) |

**EngineService**: cc-engine :9004 only, a TRANSITIONAL host (`services/engine/src/main.rs:108-110`)
with a second EL client. Unary RPCs, all DEAD-AS-WIRED (served, never dialed) except GetInfo
(LIVE): GetInfo, NewPayload, ForkchoiceUpdated (stale -> `ABORTED` + `FCU_DROPPED_STALE`),
GetEngineState, FetchBlobs. **No in-tree dialer**: chain uses DirectEngine
([08](08-execution-engine.md)). "cc-engine remains in compose for A/B comparison and
has no in-tree caller."

**StorageService**: cc-storage :9006 only, a TRANSITIONAL host (`crates/storage-core/src/boot.rs:643-645`);
shape B does **not** serve it. Impl `crates/storage-core/src/serve.rs`. Block, column,
historical-block and checkpoint-history reads take one of 4 serve permits, or answer
`RESOURCE_EXHAUSTED` after a 2 s wait (`serve.rs:351-382`); `UnaryPermitService` only moves that
permit onto the response body, which holds it until sent (`:1080-1129`). GetSnapshotState has its own
1-permit pool (`:390-425`); the other RPCs take none.

| RPC | Kind | Dialer at HEAD | Handler |
|---|---|---|---|
| GetInfo | unary | none | LIVE |
| GetBlocksByRange, GetBlocksByRoot | unary, <= 128 | none: p2p builds a `StorageClient` and drops it (`services/p2p/src/service.rs:389-391`) | DEAD-AS-WIRED (served, never dialed; window-gated: with the storage serve window at `u64::MAX`, ByRange answers `UNAVAILABLE` and ByRoot skips every block, `serve.rs:503-509,555-568`) |
| GetColumnsByRange, GetColumnsByRoot | unary, <= 128 | none (same) | DEAD-AS-WIRED (served, never dialed; window-gated, as above) |
| PutBackfillBatch | unary | none: `StorageClient` has no such method | DEAD-AS-WIRED (served, never dialed; server and admission wired, `serve.rs:757-883`) |
| WatchServeWindow | server-stream | p2p (`services/p2p/src/storage_client.rs:483`) | LIVE (content static: `u64::MAX`) |
| GetHistoricalBlock | unary | none | DEAD-AS-WIRED (served, never dialed; not window-gated; finds nothing HEAD wrote) |
| GetSnapshotState | server-stream of `StateChunk`; 1 permit, 1 MiB chunks | none | DEAD-AS-WIRED (served, never dialed; answers `NOT_AVAILABLE`) |
| GetFinalizedCheckpointHistory | unary | none | DEAD-AS-WIRED (served, never dialed; answers an empty list) |

In shape A nothing at HEAD writes blocks, snapshots or fork-choice scalars (`ArchiveWriter`
dropped, no E5 client, E7 deleted), so unless the volume holds an older build's data the last
three RPCs answer `NOT_FOUND`, `NOT_AVAILABLE` and an empty list (`history.rs:174-196`).

## 3. Edge inventory

Edge ids E1..E8 follow `plan/architecture.md` §2.3; N1..N3 are edges the plan's baseline did not
have. "Data flow" is the direction the payload moves; it differs from "Dialer -> server" for the
E1 session + view and publish arms, E6, E7 and N2 (for E4 the storage service both dialed chain
and pushed the data). The plan's own `from -> to` column is not consistently either (its E7
`storage -> chain` is the dialer), so the table gives both. An edge whose transport runs is split
into arms, each with a status; the bullets after the table say why.

| ID | Arm | Data flow | Dialer -> server | Transport | Shape A | Shape B |
|---|---|---|---|---|---|---|
| E1 | session + view | chain -> p2p | p2p -> chain | gRPC bidi P2pStream | LIVE | ABSENT |
| E1 | object / verdict | p2p -> chain | p2p -> chain | same stream | IDLE | ABSENT |
| E1 | publish | chain -> p2p | p2p -> chain | same stream | DEAD-AS-WIRED | ABSENT |
| E1 | column | p2p -> chain | p2p -> chain | same stream | DEAD-AS-WIRED | ABSENT |
| E2 | data_available | p2p -> chain | p2p -> chain | same stream | DEAD-AS-WIRED | ABSENT |
| E3 | EL calls over gRPC | chain -> engine container | (none) | gRPC EngineService | DELETED | DELETED |
| E4 | restore | storage service -> chain | storage service -> chain | gRPC RestoreFromStore ([ADR-P4-07](../adr/ADR-P4-07.md), history only) | DELETED | DELETED |
| E5 | backfill | p2p -> storage service | p2p -> storage service | gRPC PutBackfillBatch | DEAD-AS-WIRED | ABSENT |
| E6 | window | storage service -> p2p | p2p -> storage service | gRPC WatchServeWindow | LIVE (content static) | ABSENT |
| E6 | reads | storage service -> p2p | p2p -> storage service | gRPC Get{Blocks,Columns}* | DEAD-AS-WIRED | ABSENT |
| E7 | event feed | chain -> storage service | storage service -> chain | gRPC SubscribeEvents | DELETED | DELETED |
| E8 | inject | engine container -> p2p | engine container -> p2p | gRPC bidi EngineStream | DEAD-AS-WIRED | ABSENT |
| N1 | blocks | chain-core -> storage-core | in-process | ArchiveWrite trait | ABSENT | IDLE (iff core) |
| N1 | columns | chain-core -> storage-core | in-process | ArchiveWrite trait | ABSENT | DEAD-AS-WIRED |
| N2 | op records | chain -> p2p | p2p -> chain | gRPC GetValidatorRecords | IDLE | ABSENT |
| N3 | health | peer status | svc -> each peer | grpc.health.v1 Check | LIVE | ABSENT (`[peers]` empty) |

- **E1 session + view** is LIVE (content static while core-absent). p2p dials when
  `[peers].chain` is set (`services/p2p/src/service.rs:790-828`). As wired, the slot-tick driver
  needs `genesis_time`, and the epoch driver and per-session head watcher fire only when a core
  publishes (`crates/chain-core/src/p2p_stream.rs:276-330,545-561`), so a core-absent chain sends
  one `VIEW_KIND_FULL` view of default fields, in answer to Hello.
- **E1 object / verdict**: "At HEAD p2p subscribes to no gossip topics; the plan schedules
  gossip wiring for a later stage." `SwarmCommand::Subscribe` has no production sender
  (`services/p2p/src/host.rs:1253-1260`).
- **E1 publish**: `request_publish` (`crates/chain-core/src/service.rs:165`) has no caller.
  **E1 column**: there is no p2p producer, and `P2pStreamDeps.archive` is `None` in both hosts
  (`service.rs:112-117`). **E2**: nothing in `services/p2p` calls `Ipc::notify_data_available`,
  so the DA gate never opens ([05-block-lifecycle.md](05-block-lifecycle.md)).
- **E3 / E4 / E7**: deleted in 631994c (E3 client; replaced by DirectEngine), 60c6200 (E4 RPC
  and client; shape B seeds with `seed_from_durable`) and 78a90e1 (the E7 consumer). The E3 and
  E7 servers (EngineService, SubscribeEvents) are still up with no caller; E4 has no server. The
  E4 push design is recorded in [ADR-P4-07](../adr/ADR-P4-07.md), superseded by
  [ADR-R-02](../adr/ADR-R-02.md) and kept as history only (`docs/adr/ADR-P4-07.md:3`).
- **E5 / E6 reads**: no p2p client ([11](11-p2p-reqresp-and-sync.md#storage-client-e6-e5)).
  **E6 window**: the storage service only ever emits `empty_window()` (`u64::MAX`); its
  publishers are `#[allow(dead_code)]` (`crates/storage-core/src/serve.rs:281-338`).
- **E8**: p2p attaches the server ([11](11-p2p-reqresp-and-sync.md#enginestream-server-e8)).
  `trusted_local` is `reserved` (`p2p.proto:227-233`); `engine.p2p_uri` is parsed, never dialed.
- **N1 blocks** (added in d3d7f60) is IDLE for two independent reasons: no new block passes the
  DA gate, and, as wired, the never-persisted checkpoint anchor would make the first child fail the
  continuity bind (section 5.1). **N1 columns**: `ingest_columns` (`p2p_stream.rs:713`) gets no handle.
- **N2** is reached only from operation-gossip validation (fresh `Endpoint::connect()` per call,
  no timeout, 4096-entry LRU). **N3** is the health DAG ([12](12-boot-health-and-shutdown.md)).
- **Shape B ABSENT**: beacon-core serves P2pStream and GetValidatorRecords, but "No shipped configuration
  pairs cc-p2p with cc-beacon-core; how p2p attaches to shape B is not yet defined."

```text
 SHAPE A  (docker-compose: six CC containers + el; the deployed shape)
 +-----------------+                                    +----------------------+      +-----------+
 | p2p :9002       |<== E1 session + view (p2p dials) ==| chain :9001          |      | el :8551  |
 |                 |                                    | ChainService         |      | (geth)    |
 | chain-stream    |- - E1 object / verdict (IDLE) - -->| core-absent by       |      | authrpc,  |
 | client (Ipc)    |x E1 publish, x E1 column, x E2 DA  | default              |      | JWT       |
 |                 |- - N2 GetValidatorRecords (IDLE) ->| DirectEngine         |~~~~~>|           |
 |                 |                                    +----------------------+      |           |
 |                 |                                    +----------------------+      |           |
 | WatchServe-     |<== E6 window u64::MAX (p2p dials) =| storage :9006        |      |           |
 | Window client   |x E6 Get{Blocks,Columns}* reads,    | StorageService       |      |           |
 |                 |x E5 PutBackfillBatch: no client    | ArchiveWriter dropped|      |           |
 |                 |                                    +----------------------+      |           |
 |                 |                                    +----------------------+      |           |
 | EngineStream    |                                    | engine :9004         |      |           |
 | server attached |<.. E8 EngineStream (no client) ....| EngineService (no    |      |           |
 |                 |                                    | caller); EngineApi   |~~~~~>|           |
 +-----------------+                                    +----------------------+      +-----------+
 N3 health (not drawn): Check every 3 s, p2p -> chain, storage service; storage service, engine
 container, attestation -> chain; beacon-api -> chain, attestation, storage service; chain: no
 peers. attestation :9003 and beacon-api :9005 are GetInfo stubs (not drawn).
 DELETED: E3, E4, E7
```

[Legend](README.md#diagram-legend): `====` stream (head = data direction), `- - ->` IDLE, `x` a
DEAD-AS-WIRED arm of the live P2pStream; `x` also marks E5 and E6 reads (DEAD-AS-WIRED: no
client); `....>` served, never dialed (E8; head = server), `~~~~>` in-process client leaving the process (chain's carries the EL upcheck even
core-absent, `services/chain/src/run.rs:362`; newPayload and fcU need a core). Edges with an id
are drawn even with no dialer; other no-dialer RPCs appear only in section 2.

```text
 +-- cc-beacon-core :9001 (bin/beacon-core; shape B, not deployed) -----------------+
 |  +---------------------+                              +-----------------------+  |
 |  | core thread [only   |- N1 blocks (IDLE) - - - - -->| ArchiveWriter         |  |
 |  | when seeded or      |  ingest_block_blocking       | P0 mpsc(32): blocks,  |  |
 |  | ckpt-synced]        |                              | no deadline, waits    |  |
 |  | OS thread chain-core|                              | for the commit        |  |
 |  +---------------------+                              | writer task -> redb   |  |
 |             ~                                         +-----------------------+  |
 |             ~~ DirectEngine (I5, IDLE) ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~|~~~~> EL :8551
 |  EngineApi upcheck task (LIVE, also core-absent) ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~|~~~~> EL :8551
 |  +-----------------------------------------+                                     |
 |  | P2pStream session (none: no p2p dials)  |x N1 columns: ingest_columns         |
 |  +-----------------------------------------+  (archive handle None)              |
 |  not hosted: StorageService, serve pool, prune, replay, migrator, p2p            |
 +----------------------------------------------------------------------------------+
```

Same legend; I5 = core thread -> EL ([13 §6.1](13-concurrency-model.md#61-block_on-on-the-core-thread-for-engine-calls-i5)).
The core thread holds the archive handle (`bin/beacon-core/src/boot.rs:428,448`) and calls it
from `finish_imported` (`crates/chain-core/src/import.rs:988-990`). With the shipped config
beacon-core is core-absent: no core thread exists and N1 has no caller.

draw.io version: [`diagrams/topology-shapes.drawio`](diagrams/topology-shapes.drawio) (both shapes'
processes and internal edges, edges coloured by status; open in draw.io). The ASCII figure stays the
reviewed source ([diagrams/README.md](diagrams/README.md)).

## 4. P2pStream anatomy

One bidi stream per session: client p2p (the `cc_seam::Ipc` loop), server chain
(`crates/chain-core/src/p2p_stream.rs`); every message carries a per-session, per-direction
monotonic `seq`. Server: [04 §13](04-chain-core.md#13-p2pstream-server-side); client:
[11](11-p2p-reqresp-and-sync.md#chain-stream-client-e1-n2); p2p's verdict use: [10 §4.1](10-p2p-gossip-and-das.md#41-verdict-mapping).

| Direction | Message | `oneof msg` arm (field) | Producer at HEAD | Handling | Status (A) |
|---|---|---|---|---|---|
| down | `P2pToChain` | `object` GossipObject (2) | gossip validation (none subscribed) | BLOCK -> scheduler import lane; other kinds -> `(Ignore, AlreadyKnown, None)`, discarded | IDLE |
| down | `P2pToChain` | `data_available` DataAvailable (3) | none | `CoreHandle::notify_data_available` (2 s push); a failure is only logged | DEAD-AS-WIRED |
| down | `P2pToChain` | `column` ColumnSidecar (4) | none | decode, then `ArchiveWrite::ingest_columns`; with no handle -> `(Ignore, Internal, None)` | DEAD-AS-WIRED |
| down | `P2pToChain` | `hello` StreamHello (5) | Ipc, once per session | answered with a FULL `ChainView` | LIVE |
| up | `ChainToP2p` | `verdict` Verdict (2) | session, per object | Ipc `apply_verdict` -> waiter | IDLE |
| up | `ChainToP2p` | `publish` PublishRequest (3) | `request_publish` (no caller) | p2p publish queues (256, lossy) -> swarm `cmd_tx` (512, drops when full) | DEAD-AS-WIRED |
| up | `ChainToP2p` | `view` ChainView (4) | session drivers | ArcSwap store -> p2p `ChainViewStore` (Status, validation) | LIVE |

**StreamHello** carries `{session_id, resume_seq}`. Ipc draws a fresh random `session_id` on
every connect (`crates/seam/src/ipc.rs:603-612`); `resume_seq` is always 0 and chain only logs
both, so there is no resume. After a reconnect Ipc resends every outstanding object with new
seqs, then flushes its pending buffer (`ipc.rs:625-649`).

**Sessions.** Chain admits `MAX_P2P_STREAM_SESSIONS = 8` (`p2p_stream.rs:61`); the ninth gets
`RESOURCE_EXHAUSTED` + `STREAM_SESSION_LIMIT` (`:235-258`). On any stream `RESOURCE_EXHAUSTED`, that
refused open included, Ipc fails all outstanding objects with `Backpressure{bound: 64}` and
reconnects (`ipc.rs:593-598,669-676`). A session is one task: inbound in order (`:460-542`),
outbound mpsc(1024) via `send().await` (`:58`), so a slow p2p stalls its inbound side. Its head
watcher (`:545-561`) exits only when sending a changed head sequence fails, so as wired it outlives
the session until the next head change; core-absent, each ended session would leave a 10 ms poller
behind (traced, not observed).

**ChainView kinds** (`proto/eth/p2p/v1/p2p.proto:168-193`; constants `p2p_stream.rs:49-55`).
`slot` and `epoch` are filled from the head slot (`:395-407`; section 6, row 13).

| `view_kind` | Name | Trigger | Fields 11-13 (lookahead, pubkeys, active count) |
|---|---|---|---|
| 1 | SLOT_TICK | process-wide wall-clock driver; silent while `genesis_time == 0` | empty |
| 2 | EPOCH_TICK | process-wide driver polling the `EpochContext` sequence (10 ms) | filled |
| 3 | HEAD_CHANGE | per-session head watcher polling `HeadSnapshot` (10 ms) | empty |
| 4 | FULL | answer to `StreamHello` | filled |

p2p replaces its whole stored view on every message (`services/p2p/src/chain_stream/view.rs:56-58`),
so a slot-tick or head-change view wipes an epoch view's lookahead. Latent while gossip is off.

```text
 p2p: cc_seam::Ipc (dialer, one task)                         chain: p2p_stream.rs (server)
   |---- dial chain_uri (connect 5 s), open ChainService/P2pStream ----->| try_acquire_session
   |<--- RESOURCE_EXHAUSTED + STREAM_SESSION_LIMIT if 8 are already open-|
   |==== P2pToChain{seq 1, hello{session_id random, resume_seq 0}} =====>|
   |<=== ChainToP2p{view, kind 4 FULL, fields 1-14} =====================|
   |<- - view kind 1 SLOT_TICK   (needs a core: genesis_time) - - - - - -|
   |<- - view kind 2 EPOCH_TICK  (needs a core; adds fields 11-13)  - - -|
   |<- - view kind 3 HEAD_CHANGE (needs a core; 10 ms head watcher) - - -|
   |- - - object: GossipObject, source stamped GOSSIP  (IDLE)  - - - - ->|
   |<- - verdict: early ACCEPT, maybe a later REJECT   (IDLE) - - - - - -|
   |x data_available (E2)       x column (E1)                            |
   |x publish (chain -> p2p; request_publish has no caller)              |
 EOF or stream error: full-jitter sleep in [0, backoff], backoff 250 ms doubling to 10 s;
 the next session gets a new session_id and resends outstanding objects with new seqs.
```

`---->` unary or control, `====>` live stream data, `<- -` / `- - ->` IDLE (kinds 1-3 need an
installed core), `x` DEAD-AS-WIRED arm. The 30 s `Endpoint` timeout (`ipc.rs:574`) covers only the
response future, so it never tears down a session.

**Verdict mapping on the p2p side** (`ipc.rs:879-931`). Triples are `(acceptance, reason, import)`
(`Verdict`, `p2p.proto:149-156`; enums `:39-91`). The wire correlation id is the root; Ipc keys
outstanding entries by `(ObjectKind, root)` and resolves a verdict to the block entry first,
except that a column ACK completes only a non-block entry (`ipc.rs:399-416,879-893`).

| Chain sends | Source on chain | Ipc returns to the caller |
|---|---|---|
| `(Accept, Valid, None)` | early ACCEPT before the state transition (`p2p_stream.rs:795-815`) | `Ok(VerdictResolution)` |
| `(Reject, Invalid, Invalid)` | terminal reject (verdict `Invalid` or an `InvalidArgument` status), or a late second message after ACCEPT | `Ok`; a late one finds no waiter -> `on_stray_verdict` -> `chain-in-late-verdicts` |
| `(Ignore, AlreadyKnown, None)` | non-BLOCK kind discarded, or a column ACK | `Ok` for a non-block gossip waiter; columns are never tracked, so a column ACK is a stray |
| `(Ignore, Internal, None)` | no core installed; a column with a bad root, oversize, no archive handle, or an `Unavailable` ingest error; an `Unspecified` import verdict | `Ok(VerdictResolution)` |
| `(Ignore, Invalid, None)` | malformed column SSZ (`p2p_stream.rs:680-693`), or an `InvalidArgument` ingest error (`:900-903`) | `Ok` (resolved by root; see below) |
| `(Ignore, Internal, Invalid)` | an import `Status` other than `InvalidArgument`, or an `InternalProposerSig` outcome (`p2p_stream.rs:850-880,1002-1009`) | `Err(Backpressure{bound: 64})` if no early ACCEPT preceded it; after an ACCEPT it is a stray |
| any other terminal verdict | Imported, Duplicate, DeferredDa, UnknownParent, FutureSlot, TooOld, NotDescendedFromFinalized (`map_import_verdict`, `p2p_stream.rs:970-1056`) | `Ok(VerdictResolution)` |
| nothing within 2 s | -- | `Err(Backpressure{bound: 1024})` from the 100 ms timeout tick |

p2p's `dispatch_outbound` maps `Backpressure` to `VerdictResolution::Backpressure` and any other
error to `Timeout` (`services/p2p/src/chain_stream/client.rs:359-383`). As wired, Ipc runs
`on_verdict` before resolving the caller's oneshot (`ipc.rs:916-920`), and a column failure verdict
(block root) would resolve an outstanding BLOCK waiter with that root (`ipc.rs:879-893`). Latent;
unreachable at HEAD because no gossip topics are subscribed.

## 5. cc-seam: typed handles

`cc-seam` (`crates/seam`) depends only on `cc-proto`, via its default `ipc` feature
(`crates/seam/Cargo.toml:13-16`). `cc-storage-core` and `cc-beacon-import` take it with
`default-features = false`, so the writer and `ArchiveWrite` code stay proto-free; storage-core's
default `grpc` feature (boot, serve) adds `cc-proto` and `cc-seam/ipc` back
(`crates/storage-core/Cargo.toml:13-16`), so only cc-beacon-import's storage-core is proto-free.
`scripts/check-crate-dag.sh` forbids a `cc-chain <-> cc-p2p` crate dependency; at HEAD they meet
over the P2pStream wire contract (p2p uses `cc_seam::Ipc`; chain serves the proto stream itself).
The seam types (`Root`, `ObjectKind`, `Reason`, `ChainView`, ...) belong to cc-seam, not proto or
`cc-types` (`crates/seam/src/lib.rs:82-218`), as do the SubscribeEvents payload layouts
(`event_payloads.rs`: BLOCK_IMPORTED verdict byte + SSZ, HEAD 8 B, CHAIN_REORG 40 B,
FINALIZED_CHECKPOINT >= 40 B; a short or unknown payload decodes to `None`).

```text
 +------------------------------------------------------------------------------------------+
 | cc-seam  (crates/seam; default feature "ipc" = cc-proto + tonic)                         |
 | SeamError: Backpressure{bound, waited_ms}, Unavailable, InvalidArgument,                 |
 |            FailedPrecondition{reason: NotBootstrapped}            (exactly 4 variants)   |
 | trait ChainIngress   p2p -> chain-core           submit_gossip, notify_data_available,   |
 |                                                  submit_column_sidecar                   |
 | trait P2pEgress      chain-core -> p2p           publish -> Queued/Dropped; update_view  |
 | trait ArchiveWrite   chain-core -> storage-core  ingest_columns, ingest_block,           |
 |                                                  ingest_block_blocking, block_is_durable |
 +------------------------------------------------------------------------------------------+
              |                              |                              |
 +---------------------------+  +----------------------------+  +---------------------------+
 | InProcess   TEST-ONLY     |  | Ipc   LIVE (p2p only)      |  | ArchiveWriter             |
 | ChainIngress + P2pEgress  |  | ChainIngress over tonic    |  | (storage-core)            |
 | import mpsc(64), 2 s      |  | out mpsc(1024), 2 s budget |  | P0 mpsc(32): blocks with  |
 | column mpsc(4096)         |  | outstanding <= 1024        |  | no deadline, then waits   |
 | publish mpsc(256)         |  | IpcEgress: P2pEgress as a  |  | for the commit            |
 | view ArcSwap              |  | local mailbox (256);       |  | A: built and dropped      |
 | no production caller      |  | p2p never uses it          |  | B: injected, N1 IDLE      |
 +---------------------------+  +----------------------------+  +---------------------------+
```

Vertical lines: trait implemented by; no data edge is drawn.

### 5.1 Traits

| Trait (`lib.rs`) | Direction / edge | Methods | Production impl | Status at HEAD |
|---|---|---|---|---|
| `ChainIngress` (`:251`) | p2p -> chain-core, E1 + E2 | `submit_gossip -> VerdictResolution`, `notify_data_available(root, slot)`, `submit_column_sidecar` | `Ipc` (p2p) | A: IDLE (gossip) / DEAD-AS-WIRED (DA, column); B: ABSENT |
| `P2pEgress` (`:275`) | chain-core -> p2p, E1 | `publish -> Published{Queued, Dropped}`, `update_view` (sync, infallible) | none: chain writes `ChainToP2p` directly in `p2p_stream.rs`; p2p holds its `IpcEgress` unused (`services/p2p/src/chain_stream/client.rs:239`) | DEAD-AS-WIRED (the `InProcess` impl is TEST-ONLY) |
| `ArchiveWrite` (`:337`) | chain-core -> storage-core, N1 | `ingest_columns(ColumnBatch)`, plus `ingest_block` / `ingest_block_blocking` (default no-op `Ok(())`) and `block_is_durable` (default `Ok(false)`) | `ArchiveWriter` (`crates/storage-core/src/archive_write.rs:33-38,224-258`) | A: N1 ABSENT; the `ArchiveWriter` cc-storage builds is DEAD-AS-WIRED (dropped, `crates/storage-core/src/boot.rs:563`). B: IDLE (iff core; blocks), DEAD-AS-WIRED (columns) |

`ArchiveWriter` admits a batch only if its head `(parent_root, slot)` is durable or is the batch's
first row, else `InvalidArgument` (`archive_write.rs:131-154`). The core thread persists after
fork choice and the head snapshot are updated, so a persist error returns `Err` with fork choice
advanced and no events (`crates/chain-core/src/import.rs:973-992`); a DUPLICATE re-persists only
if `block_is_durable` is false (`import.rs:375-385,879-891`). See [07 §4.4](07-storage.md#44-archivewrite-the-import-persist-path-n1).
`cc_chain_core::ArchiveWriteHandle` (`crates/chain-core/src/lib.rs:31`) is the only handle alias
production holds; `ChainIngressHandle` and `P2pEgressHandle` (`services/chain/src/lib.rs:143-145`,
`services/p2p/src/lib.rs:62-64`) are DEAD-AS-WIRED.

### 5.2 SeamError

Exactly four variants (`lib.rs:42-57`); a unit test pins the count, and adding one needs an ADR.

| Variant | Meaning | Produced from |
|---|---|---|
| `Backpressure { bound, waited_ms }` | the receiving queue stayed full for the whole send deadline | gRPC `RESOURCE_EXHAUSTED` (`map_tonic_status`, `ipc.rs:124-127`), Ipc's `(Ignore, Internal, Invalid)` verdict (`ipc.rs:899-914`), and local send/reply timeouts; chain-core maps it back to `RESOURCE_EXHAUSTED` (`import.rs:846-852`) |
| `Unavailable(String)` | receiver gone, or shed inside Ipc | closed channels, shutdown, writer `ShutDown`, store errors, other gRPC codes |
| `InvalidArgument(String)` | structurally rejected before any work | gRPC `INVALID_ARGUMENT`; column > 10 MiB; writer `Codec` / `Limit` errors; continuity-bind failure (`archive_write.rs:131-154`) |
| `FailedPrecondition { reason }` | precondition unmet; `FailedPreconditionReason::NotBootstrapped` is the only reason | gRPC `FAILED_PRECONDITION` with `ErrorInfo.reason == NOT_BOOTSTRAPPED`; any other reason maps to `Unavailable` |

### 5.3 Overflow policies A-D

Policies A-C come from `plan/architecture.md` §2.2. There, D names the old silent `SlotTick` drop,
which S0 deleted by giving ticks a never-shed scheduler lane (case 11, `slot_tick_is_never_shed`);
this doc set uses **Policy D** for that replacement. Name the letter with its behaviour.

| Policy | Behaviour | Where it lives at HEAD | Test |
|---|---|---|---|
| **A**: block <= 2 s, then `Backpressure` | caller waits for the deadline, then gets a typed error | `ChainIngress::submit_gossip` / `notify_data_available`; chain's scheduler import lane (`IMPORT_LANE_DEPTH = 64`, 2 s `push_timeout`); documented but not implemented for `ArchiveWrite` (section 6) | conformance cases 1, 2 |
| **B**: terminate the slow consumer | `try_send`; when the queue is full, the subscriber's stream ends with `RESOURCE_EXHAUSTED` | SubscribeEvents subscriber mpsc(256) (`crates/chain-core/src/events/fanout.rs:81`, `events/mod.rs:98`) | `services/chain/tests/events.rs:243` |
| **C**: drop and report | full queue -> `Ok(Published::Dropped)`, never an error | `P2pEgress::publish`; p2p's real publish path drops the oldest item at 256, and drops again when `cmd_tx` is full | conformance case 3 |
| **D**: never shed | producer blocks rather than drop | scheduler tick lane (`TICK_LANE_DEPTH = 4`, `crates/scheduler/src/config.rs:71`) | case 11, `crates/chain-core/src/core.rs:2446` |

### 5.4 Impls and capacities

| Method | Documented contract (`lib.rs`) | `InProcess` (TEST-ONLY) | `Ipc` (LIVE in p2p) |
|---|---|---|---|
| `submit_gossip` | Policy A on the scheduler import lane: 64, 2 s | `send_timeout(2 s)` on import mpsc(64) -> `Backpressure{64, 2000}` (`crates/seam/src/in_process.rs:120-140`) | `out_tx` mpsc(1024) `send_timeout(2 s)`; the reply waits out the rest of the same 2 s -> `Backpressure{1024}`; chain's scheduler import lane full -> `Backpressure{64}`; duplicate or 1024 outstanding -> `Unavailable` (`ipc.rs:749-791,1121-1159`) |
| `notify_data_available` | same scheduler import lane and deadline; overflow never `Ok(())` | same as gossip | acked `Ok(())` as soon as it is written to the stream (`ipc.rs:792-818`); chain-side overflow is logged and lost |
| `submit_column_sidecar` | not on the scheduler import lane; `send().await` on the event ring, no deadline; > 10 MiB `InvalidArgument` | own column mpsc(4096), `send().await` | 10 MiB check, then the same 2 s path as gossip -> can return `Backpressure{1024}` (`ipc.rs:1177-1196`); once written to the stream, acked `Ok(())` with no verdict wait (`ipc.rs:819-838`) |
| `publish` | Policy C, bound 256 | `try_send` mpsc(256) | `IpcEgress` `try_send` mpsc(256), never used in production |
| `update_view` | ArcSwap store, never fails | ArcSwap | ArcSwap |

p2p drives `Ipc::connect_with` (`ipc.rs:221-240`) in its `chain-stream-client` task through
`IpcUpward` hooks and never drains `IpcMailbox.publish_rx`. Disconnected, the loop buffers 1024
items, then fails new ones with `Backpressure{1024}` (`ipc.rs:997-1003`); it never terminates p2p.

### 5.5 Conformance suite

`crates/seam/src/conformance.rs` names **11 cases** (`:1-23`). Cases 1-8 run one shared assertion
on `InProcess` and on `Ipc` / `IpcEgress` (against a live tonic server); case 9 is split by impl;
cases 10 (Policy B) and 11 (Policy D) live outside cc-seam: 18 `#[tokio::test]` functions, two
with `start_paused = true`, run by `make test-seam` (`Makefile:71-73`), `make ci` and CI
(`.github/workflows/ci.yml:152-154`). Not covered: **`ArchiveWrite`** (no case fills P0; "No test
drives the production persist path (core thread + `ingest_block_blocking` with a real writer).");
**Ipc's local Policy A bound** (case 1 forces a far-side `RESOURCE_EXHAUSTED`, `bound == 64`,
`:132-153`; case 2 stops the server, `CHAIN_OUT_BOUND` 1024, `:175-197`; neither fills `out_tx`);
**column overflow** (case 9 is happy-path only, `:424-430`; Ipc can return `Backpressure`); and
**the production adapter** (p2p's `run_chain_stream_client` / `P2pUpward`, chain's session code;
`services/chain/tests/p2p_stream_contract.rs` covers part).

### 5.6 Error mapping across layers

Every place an error changes type or wire code, grouped by layer. Details: section 4 (verdict
triples), section 5.2, [05](05-block-lifecycle.md) (hop 13), [06 §6](06-consensus-primitives.md#6-cc-state-transition),
[08 §3.4](08-execution-engine.md#34-error-taxonomy-and-the-collapse-at-the-seam),
[10 §4.1](10-p2p-gossip-and-das.md#41-verdict-mapping).

| # | From -> to | By | Code | Status at HEAD |
|---|---|---|---|---|
| 1 | `BlockError` -> `GossipClass` Reject / Ignore / Internal | `gossip_class()`, exhaustive (no catch-all); `OnBlockError` via `on_block_error_gossip_class` | `crates/state-transition/src/error.rs:119,280`; `crates/chain-core/src/import.rs:638` | IDLE (imports arrive only via ImportBlock, no in-tree dialer, or IDLE gossip) |
| 2 | `on_block` `Ok(Deferred)` / `Err` -> `ImportBlockVerdict` + `ImportReason` | DataUnavailable -> DEFERRED_DA; UnknownParent -> UNKNOWN_PARENT; FutureSlot -> INVALID + typed reason (row 6 makes it Ignore); ExecutionEngineUnavailable -> DEFERRED_DA (p2p sees `(Ignore, DeferredDa, DeferredDa)`); `Err` -> INVALID, late flags by class | `crates/chain-core/src/import.rs:488-631` | IDLE (as row 1) |
| 3 | persist `SeamError` -> gRPC | `map_archive_err`: Backpressure -> RESOURCE_EXHAUSTED, InvalidArgument -> INVALID_ARGUMENT, other -> UNAVAILABLE | `import.rs:846-852` | A: ABSENT (`archive` None); B: IDLE (iff core; N1) |
| 4 | core-absent -> `FAILED_PRECONDITION` + ErrorInfo `NOT_BOOTSTRAPPED` | `ChainServiceImpl::not_bootstrapped` | `crates/chain-core/src/service.rs:52,200-207`; helpers `crates/proto/src/lib.rs:134-181` | IDLE (handler answers it in the shipped config; callers: IDLE N2, operator scripts) |
| 5 | gRPC `Status` -> `SeamError` | `map_tonic_status`: RESOURCE_EXHAUSTED -> Backpressure{64}; INVALID_ARGUMENT -> InvalidArgument; FAILED_PRECONDITION + NOT_BOOTSTRAPPED -> FailedPrecondition; else Unavailable | `crates/seam/src/ipc.rs:124-140` | A: IDLE (fails outstanding objects; none while gossip is off); B: ABSENT |
| 6 | import verdict or `Status` -> `Verdict` triple | `map_import_verdict`; `emit_terminal_verdict` for an `Err` (INVALID_ARGUMENT -> Reject, else Ignore/Internal); table in section 4 | `crates/chain-core/src/p2p_stream.rs:850-880,970-1056` | A: IDLE; B: ABSENT |
| 7 | `Verdict` -> gossipsub `MessageAcceptance` | `to_message_acceptance` (Unspecified -> Ignore); `gossip_class_for_reason` keeps `Internal` out of every penalty | `services/p2p/src/verdict.rs:102-109,136` | A: IDLE; B: ABSENT |
| 8 | REJECT -> app-score penalty | reported REJECT -> GossipInvalid (-10); late REJECT after ACCEPT -> ImportInvalid (-25) | `services/p2p/src/gossip/validate/pipeline.rs:838-844,875-880`; deltas `services/p2p/src/peer_manager/score.rs:60-69` | A: IDLE; B: ABSENT |
| 9 | `cc_engine_api::EngineError` -> EL health, and -> import deferral | upcheck: `UpcheckOutcome::from_engine_error` (HTTP 401/403 -> AuthRejected, else Failure); DirectEngine `map_api_error` collapses every error to `EngineError::Transport`, which `on_block` turns into `Ok(Deferred(ExecutionEngineUnavailable))` | `crates/engine-api/src/state.rs:144-153`; `crates/chain-core/src/engine.rs:199-201`; `crates/fork-choice/src/on_block.rs:267-274` | upcheck LIVE; deferral IDLE (newPayload is behind the DA gate) |
| 10 | `StoreError` -> gRPC | `store_status`: Limit, Codec -> INVALID_ARGUMENT; KeyCollision -> ALREADY_EXISTS; other -> INTERNAL | `crates/storage-core/src/serve.rs:1249-1258` | A: IDLE (callers are no-dialer RPCs); B: ABSENT |
| 11 | `WriterError` -> gRPC | `writer_status`: InjectedFailure -> ABORTED, ShutDown -> UNAVAILABLE, Store -> row 10 | `serve.rs:1260-1264` | A: IDLE (PutBackfillBatch only; no in-tree dialer); B: ABSENT |
| 12 | `WriterError` -> `SeamError` | `map_writer_err`: Codec, Limit -> InvalidArgument; ShutDown, InjectedFailure, other store errors -> Unavailable | `crates/storage-core/src/archive_write.rs:99-108` | A: DEAD-AS-WIRED (writer dropped); B: IDLE (iff core) |
| 13 | `CheckpointError` -> retry decision | `is_verification_failure`: a verification failure is never retried against that provider (the loop advances to the next one); other errors retry up to `network_retries` times; a `StateRootMismatch` first re-fetches the triple | `services/chain/src/checkpoint_sync.rs:162-170,913-945,962-985` | LIVE (opt-in; shipped `checkpoint_providers = []`) |
| 14 | boot error -> process exit | service `main`s return `anyhow::Result<()>` from `run()`; the runtime prints the error and exits 1. Some paths call `std::process::exit(1)` directly (chain's boot task, `services/chain/src/run.rs:413,437,492`; storage-core open and replay, `crates/storage-core/src/open.rs:355`, `replay.rs:478`; p2p on a swarm panic, `services/p2p/src/main.rs:524-528`). Every exit is status 1; no structured exit codes | `services/chain/src/main.rs:10-12` | LIVE |

The KeyCollision arm of row 10 is reachable only from PutBackfillBatch staging
(`serve.rs:833-845`), which has no client (E5 DEAD-AS-WIRED); the writer itself aborts the
process on a collision (`crates/storage-core/src/writer.rs:752-786`). Row 9: "Engine failures
defer the block rather than reject it; re-drive depends on an EL health transition." Row 6's
`(Ignore, Internal, Invalid)` is what Ipc turns into `Backpressure{64}` (section 6, row 4):
"Some internal import failures surface to p2p as backpressure."

## 6. Documented vs implemented

| # | Documented | Implemented at HEAD | Evidence |
|---|---|---|---|
| 1 | `ArchiveWrite` trait doc, seam README and [ADR-R-02](../adr/ADR-R-02.md): a full P0 mailbox MUST return `Backpressure` (Policy A) | `ArchiveWriter` uses `blocking_send` / `send().await` with no deadline, then waits for the commit. It never returns `Backpressure`, so chain-core's `Backpressure -> RESOURCE_EXHAUSTED` arm is unreachable. The trait doc itself says both "block" and "MUST return Backpressure" (`lib.rs:321-329`). [ADR-P4-04](../adr/ADR-P4-04.md) ("P0 bound 32 on full: block") matches the code. "The trait documents Backpressure; the implementation blocks without a deadline." | `crates/storage-core/src/writer.rs:187-196,261-281`; `archive_write.rs:99-108` |
| 2 | [ADR-R-01](../adr/ADR-R-01.md) and the trait doc: a column is admitted with `send().await` on the **events ring** and never gets `Backpressure` | `InProcess` uses its own mpsc(4096). Chain sends columns to `ArchiveWrite`, not to the ring. `Ipc::submit_column_sidecar` shares the gossip 2 s path and can return `Backpressure{1024}`. Latent: there is no caller. | `in_process.rs:84`; `p2p_stream.rs:640-725`; `ipc.rs:1177-1196` |
| 3 | ADR-R-01 (`docs/adr/ADR-R-01.md:58`) and `plan/architecture.md` §2.2: Policy A is `Backpressure` after 2 s on `IMPORT_LANE_DEPTH` (64) | Ipc reports `bound: 1024` for local send and reply timeouts, and `bound: 64` only when chain reports overflow | `ipc.rs:146-159` |
| 4 | `ChainIngress`: `Backpressure` means the receiver's queue is full | Chain maps every import `Status` other than `InvalidArgument`, and `InternalProposerSig` outcomes, to `(Ignore, Internal, Invalid)`, which Ipc turns into `Backpressure{64}`; likewise a P2pStream open refused at the 8-session cap (`ipc.rs:593-598`). "Some internal import failures surface to p2p as backpressure." | `p2p_stream.rs:850-880,1002-1009`; `ipc.rs:899-914` |
| 5 | `notify_data_available`: overflow MUST be `Backpressure` and never swallowed as `Ok(())` | Ipc acks `Ok(())` once the message is written to the stream. An overflow of chain's scheduler import lane is only logged. | `ipc.rs:792-818`; `p2p_stream.rs:622-639` |
| 6 | `p2p.proto` `Verdict`: "Exactly one Verdict per GossipObject" | After an early ACCEPT, chain can send a second verdict with the same correlation id: a late `(Reject, Invalid, Invalid)`, or a terminal verdict when the import returns `Err` | `p2p_stream.rs:795-846` |
| 7 | `StreamHello`: `session_id` is "per-process"; `resume_seq` is the "last seq the client processed" | `session_id` is drawn per connect; `resume_seq` is always 0, and chain ignores both fields | `ipc.rs:603-612`; `p2p_stream.rs:600-618` |
| 8 | `plan/architecture.md` §2.1 and §2.5 (chain takes `Arc<dyn P2pEgress>`) and §2.3 (E8 becomes a second `P2pEgress` method) | No production code holds a `P2pEgress`, and the trait has only `publish` / `update_view` | `lib.rs:274-278` |
| 9 | cc-seam doc comments cite `services/chain/src/core.rs`, `services/chain/src/events/mod.rs` and `services/storage/src/writer.rs` | The first two now live under `crates/chain-core/src/`; the writer is `crates/storage-core/src/writer.rs` | `lib.rs:225,232,244,317`; `in_process.rs:27,30,33`; `ipc.rs:46,49`; `conformance.rs:23` |
| 10 | Proto comments: GetCanonicalRoots is "used by storage on CURSOR_TOO_OLD gap fill" (`chain.proto:47-50`); the EngineStream "client lives in services/engine" (`p2p.proto:20-23`); FetchBlobs is the "chain block-branch fast-path trigger" (`engine.proto:25-27`); `EVENT_KIND_DATA_COLUMN` is a column relay (`chain.proto:125-126`) | None of these callers or producers exist; chain reaches the EL through DirectEngine | 78a90e1; 631994c |
| 11 | [../contracts.md](../contracts.md): the ChainService table, an event ring of "1024 events", and the remodelling gate list | The table omits P2pStream, GetValidatorRecords and GetCanonicalRoots. The ring holds 4096 events / 64 MiB. The gate also rejects ExecutionPayload, ExecutionRequests and DataColumnSidecar. | `events/mod.rs:90,95`; `check-no-remodelling.sh:19` |
| 12 | "The ninth contract" | The phrase names two things: EngineStream (`p2p.proto:12`) and the p2p <-> cc-storage surface (`storage.proto:9`). `docs/supply-chain.md:136-138` uses it for the EngineStream client, whose engine-side code is gone; only the generated `cc-proto` stub remains | 631994c |
| 13 | `ChainView.slot` / `.epoch` are read by p2p as the wall-clock slot (`services/p2p/src/chain_stream/view.rs:3`; `gossip/validate/pipeline.rs:421-425`) | Chain sets both from the head slot; as wired, a lagging head would make p2p's clock lag. Latent; unreachable at HEAD because no gossip topics are subscribed. | `p2p_stream.rs:395-407` |

## 7. Codegen and breaking-change policy

Codegen ([ADR-04](../adr/ADR-04.md)): `crates/proto/build.rs:16-47` compiles every `.proto` with
pure-Rust `protox` and `generate_default_stubs(true)`; nothing generated is checked in. Lint and
breaking ([ADR-05](../adr/ADR-05.md)): `proto/buf.yaml` (lint `DEFAULT`, breaking `FILE`); `make proto`
= lint + format + breaking fixtures + remodelling gate. Rules: [../contracts.md](../contracts.md#file-category-evolution-rules-adr-05).

## Planned changes

None of this is built. Source: [`plan/architecture.md`](../../plan/architecture.md) §2.

- Planned: select and wire the E1/E2 transport at S3; every transport move must pass the
  conformance suite on both impls (§2.4, §2.5). E5 becomes `storage_core::backfill::admit()` behind
  `ArchiveWrite`; the storage serve window (E6) becomes one `AtomicU64` read; E8's inject becomes a second
  `P2pEgress` method (§2.3).
- Open: how p2p pairs with shape B (section 3); whether `ArchiveWrite` gains a P0 deadline that
  returns `Backpressure` or its docs change to "block, no deadline" (section 6, row 1).
