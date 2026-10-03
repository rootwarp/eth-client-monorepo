# Architecture overview

> **As built at `7d8833d`** (`develop`, 2026-08-16). Status labels and the diagram legend are defined on this page.
> Target design: [`plan/architecture.md`](../../plan/architecture.md) §0, §1, §2.5, §9.1.

## What the system is

A Rust Ethereum consensus client (a beacon node) for the Fulu / PeerDAS era, run against **Hoodi**:
mainnet preset, bundled Hoodi chain config, Fulu-only block and state SSZ decode
(`crates/types/src/block.rs:135-142`, `crates/types/src/state/mod.rs:239-246`), 9 Hoodi bootnodes
(`config/p2p.toml:69`), and geth `v1.17.5` over the Engine API (`docker-compose.yml:185`). The 28-crate
workspace builds two deployable **shapes**: shape A (`docker-compose.yml`, six CC containers plus `el`)
is the only one deployed; shape B (`bin/beacon-core`, one process) is the target host, not deployed.

**Bottom line at `7d8833d`:** the node boots, discovers and handshakes with Hoodi peers, upchecks
the EL and reports aggregate gRPC health. It does not follow the chain head, and it persists no
imported block ([why](#what-works-end-to-end-today)).

## Status labels

Each doc gives a mechanism as designed, then its status at `7d8833d` with these labels, per shape
where the shapes differ. The vocabulary is closed; qualifiers go in parentheses, e.g. `LIVE (content
static)`. A claim traced in code but never observed at runtime reads "as wired, ... would".

| Label | Meaning |
|---|---|
| LIVE | runs in a deployed process and does its job |
| IDLE | wired and reachable, but no input at HEAD (an upstream is DEAD-AS-WIRED) |
| DEAD-AS-WIRED | in a production binary; no production caller/producer, or output discarded |
| INERT | constructed and running; structurally cannot take effect (gate never blocks) |
| STUB | reached in production, answers with a placeholder by design |
| DELETED | removed from the tree; the commit is named |
| TRANSITIONAL | kept only for the A/B period: a shape A host or leftover that the plan removes; written `TRANSITIONAL (target)` for cc-beacon-core, which is built but not deployed while shape A runs (the plan keeps it) |
| TEST-ONLY | only constructed or called from `#[cfg(test)]` / `tests/` |
| DEVNET-ONLY | only reachable on the `cc-p2p --publish-fixture / --devnet-peer` path |
| PARAMS-ONLY | values computed and documented, never installed into the runtime |
| ABSENT | the edge or component does not exist in that shape |

Three rules apply across the set. (1) A path whose only input is an operator's hand-made call (for
example `ImportBlock` via grpcurl) is IDLE, and so is the ChainService handler that serves it. A
served RPC that no in-tree client dials and no operator procedure uses (EngineService, the
StorageService reads and history RPCs, `PutBackfillBatch`, `EngineStream`) is DEAD-AS-WIRED (served,
never dialed), whether or not it is drawn. `GetInfo`, health and reflection exist to answer
operators and probes and are LIVE. (2) The qualifier `(iff core)` marks a status that holds only
once a core is installed (checkpoint sync or durable seed). In a core-absent column such a
component is ABSENT (not constructed). (3) Shape B columns describe `bin/beacon-core` as wired. It
is not deployed, so LIVE there means it would run and do its job once started.

## Diagram legend

```text
 ====>   LIVE stream / data flow; arrowhead = data direction
 ---->   LIVE unary RPC or control call; arrowhead = server (request direction)
 - - ->  IDLE: wired, no input at HEAD (same arrowhead rules)
 x       DEAD-AS-WIRED arm or edge (write the arm name next to it); an x on any shaft wins
 ....>   served, never dialed (server exists, no client); arrowhead = server
 ~~~~>   in-process call that leaves the process (DirectEngine -> EL HTTP)
 [B]     element exists only in shape B; [A] only in shape A
 DELETED edges: list in a footer line "DELETED: E3, E4, E7"; do not draw them
 Label the dialer when it differs from data direction: "(p2p dials)"
```

Edge ids `E1`..`E8` keep the plan's numbering; `N1`..`N3` are new since its baseline (`4146791`).
Full inventory: [03-internal-contracts.md](03-internal-contracts.md).

## System context

```text
        +----------------------------------------+     +-------------------------------+
        | operator: compose, TOML, env, grpcurl  |     | metrics scraper: GET /metrics |
        +----------------------------------------+     +-------------------------------+
             | gRPC :900N (A: cc network only)              | 127.0.0.1:9101-9106 (A), :9101 (B)
             v                                              v
 +--------------------------------------------------------------+      +---------------------+
 | consensus client (this repo)                                 |~~~~~>| el: geth v1.17.5,   |
 | shape A: six CC containers, docker-compose.yml (deployed)    |      | authrpc :8551, JWT  |
 | shape B: cc-beacon-core, one process (not deployed)          |      +---------------------+
 |                                                              |~~~~~> checkpoint providers
 +--------------------------------------------------------------+      (HTTPS, opt-in, default off)
             |
             | libp2p TCP 9000 + discv5 UDP 9000 (cc-p2p dials out; shape A only)
             v   port not published: inbound peer reachability is not configured
        CL peers on Hoodi (handshake LIVE; 0 gossip topics; block/column serve STUB)
```

`|` + `v` is a `---->` call (arrowhead = server); a dialed peer link carries req/resp both ways.

draw.io version: [`diagrams/system-context.drawio`](diagrams/system-context.drawio) (system context,
edges coloured by status; open in draw.io). The ASCII figure stays the reviewed source
([diagrams/README.md](diagrams/README.md)).

| External party | Interface | Status at `7d8833d` |
|---|---|---|
| EL (geth) | Engine API JSON-RPC over HTTP with HS256 JWT. Clients: the EngineApi built in cc-chain (A) or cc-beacon-core (B), which DirectEngine calls from the core thread (`services/chain/src/run.rs:362-366`); plus a second, independent EngineApi in the engine container (A). `el` keeps authrpc 8551 on the cc network but publishes 8545, 6060 and 30303 on all host interfaces (`docker-compose.yml:183-224`); operations: [../el-runbook.md](../el-runbook.md). | `eth_syncing` upcheck: LIVE from both shape A EngineApis, spawned whether or not a core is installed (`crates/engine-api/src/api.rs:174`). `forkchoiceUpdatedV3`: IDLE (iff core) in both shapes; it needs an installed core plus an import or ApplyAttestations command, and none reaches the core thread at HEAD (`crates/chain-core/src/core.rs:1752,1817`). `newPayloadV4`: IDLE (iff core), behind the DA gate. |
| CL peers | libp2p TCP + discv5 UDP on 9000 | Discovery, dialing and the Status / ping / metadata / goodbye handshake: LIVE. Block and column serve: STUB, answering code 3 "handler not ready" (`services/p2p/src/host.rs:745-752`). Shape A does not publish the libp2p port; inbound peer reachability is not configured. |
| Checkpoint providers | HTTPS (or loopback HTTP) GET of genesis, spec, finalized block and state (`services/chain/src/checkpoint_sync.rs:722-728`) | LIVE (opt-in). The default is `checkpoint_providers = []` (`config/chain.toml:27`, `config/beacon-core.toml:23`). |
| Metrics scraper | `GET /metrics` on every process, OpenMetrics, no auth, no TLS (`crates/bootstrap/src/metrics_server.rs`) | LIVE. The process that drives the EL exports no `cc_engine_*`: it is built with `finish(None)` (`services/chain/src/run.rs:362`). `:9104` is the engine container's own client. |
| Operator | gRPC (grpcurl), TOML + `CC_<SVC>_*` env | LIVE. Compose never host-publishes gRPC. There is no REST Beacon API: `cc-beacon-api` is a GetInfo STUB. |

## Layered view

```text
 +------------------------------------------------------------------------------------------+
 | services / bins: services/{chain,p2p,storage,engine,attestation,beacon-api},             |
 |   bin/beacon-core; offline tools bin/{cc-store,devnet-gen,serve-probe,store-bench}       |
 +------------------------------------------------------------------------------------------+
       v                                              v
 +-------------------------------------------+  +-------------------------------------------+
 | host libs: cc-chain-core                  |  | storage: cc-storage-core, prune, replay   |
 | core thread, import, events, P2pStream    |  | storage-core writer, ArchiveWriter, serve |
 +-------------------------------------------+  +-------------------------------------------+
       v                                              v
 +--------------------------------------------+  +---------------------+  +-----------------+
 | host libs: cc-engine-api, cc-scheduler,    |  | seams / contracts:  |  | storage:        |
 | cc-libp2p, cc-bootstrap, cc-config         |  | cc-seam, cc-proto   |  | cc-store        |
 +--------------------------------------------+  +---------------------+  +-----------------+
       v                                                                       v
 +------------------------------------------------------------------------------------------+
 | pure consensus: cc-fork-choice -> cc-state-transition -> cc-crypto -> cc-types           |
 +------------------------------------------------------------------------------------------+
```

Arrows point from dependent to dependency (Cargo normal deps): a crate uses only lower bands and
siblings in its own band; cc-seam and cc-proto use nothing below theirs. Crate `cc-<name>` lives at
`crates/<name>`, `services/<name>` or `bin/<name>`, except `cc-store-tool` (`bin/cc-store`).
Pure-consensus crates have no tokio and spawn nothing; `bin/beacon-core` also uses `cc-chain` for
checkpoint sync. Test crates are not drawn.

cc-seam defines three traits (`crates/seam/src/lib.rs:251-337`): `ChainIngress` and `P2pEgress` for
p2p <-> chain (E1, E2), `ArchiveWrite` for N1. Production impls are `Ipc` (p2p, over P2pStream) and
`ArchiveWriter` (storage-core); `InProcess` is TEST-ONLY ([03](03-internal-contracts.md)).

`scripts/check-crate-dag.sh` encodes the rules: only cc-libp2p may declare `libp2p*` (`:642-654`);
JWT and HTTP-client crates are confined to an allowlist (`:45-67`); cc-chain and cc-p2p must not
depend on each other and talk through cc-seam (`:300-321`, [ADR-R-01](../adr/ADR-R-01.md)). At
`7d8833d` the script **exits 1** in its early scan (`:364-366`): the storage-core opaque-bytes rule
(`:345-360`) flags `crates/storage-core/src/archive_write.rs:26,332,335,395,803` (from d3d7f60), so
the `cargo metadata` phase (`:368` on: band allowlist, libp2p rule, pins) never runs. A manual
run of that phase passes every check except the allowlist, which fails on the dev edge
`cc-p2p -> cc-spec-tests` (`services/p2p/Cargo.toml:65`, row `:397`), so fixing the opaque-bytes
rule alone will not turn the guard green
([02](02-crate-map.md#the-dag-guard-scriptscheck-crate-dagsh)). `scripts/check-no-max-blobs-p2p.sh`
also exits 1 ([14](14-testing-and-enforcement.md)).

## Shape A and shape B at a glance

| | Shape A (`docker-compose.yml`) | Shape B (`bin/beacon-core`) |
|---|---|---|
| Deployed | yes: the only deployed shape | no. It is in the image (`Dockerfile:41`) but has no compose service, Make target or CI job. |
| Entry point | chain `services/chain/src/run.rs:307`; storage `crates/storage-core/src/boot.rs:471` (`services/storage` is a shim); p2p `services/p2p/src/service.rs:857`; engine `services/engine/src/main.rs:90` | `bin/beacon-core/src/boot.rs:397` |
| Hosts | chain-core in cc-chain, redb and the storage-core writer in cc-storage (both TRANSITIONAL); core-absent by default | chain-core plus an in-process storage-core writer over redb, opened before bind; core-absent by default |
| Persist imports | ABSENT. chain leaves `archive` at `CoreConfig::default()` = None (`services/chain/src/run.rs:368-378`, `crates/chain-core/src/core.rs:850`); the storage service drops its ArchiveWriter (`crates/storage-core/src/boot.rs:563`). | N1 blocks: IDLE (iff core) (`bin/beacon-core/src/boot.rs:428,448`). N1 columns: DEAD-AS-WIRED, the handle is never injected into P2pStreamDeps (`crates/chain-core/src/p2p_stream.rs:154`). |
| gRPC, p2p | six services; cc-p2p is the LIVE libp2p host. cc-engine remains in compose for A/B comparison and has no in-tree caller. | ChainService only (`bin/beacon-core/src/boot.rs:567`); p2p ABSENT. No shipped configuration pairs cc-p2p with cc-beacon-core; how p2p attaches to shape B is not yet defined. |

```text
 +---------------+   SHAPE A (deployed)                  +------------------------+
 | p2p :9002     |<=== E1 ChainView (p2p dials) =========| chain :9001            |
 | swarm, discv5 |- - E1 object/verdict (IDLE) - - - - ->| core-absent by default |
 |               |- - N2 GetValidatorRecords, IDLE - - ->| archive = None         |
 |               |x E1 publish, E1 column, E2 DA         | EngineApi/DirectEngine |~~~~> el :8551
 |               |                                       +------------------------+
 |               |                                       +------------------------+
 |               |<=== E6 WatchServeWindow (p2p dials) ==| storage :9006          |
 |               |x E6 reads, E5 PutBackfillBatch        | redb, prune LIVE;      |
 +---------------+                                       | ArchiveWriter dropped  |
         ^                                               +------------------------+
         : E8 EngineStream: DEAD-AS-WIRED (served, never dialed)
 +---------------+
 | engine :9004  |~~~~> el :8551 (second EngineApi; EngineService DEAD-AS-WIRED, never dialed)
 +---------------+
 attestation :9003, beacon-api :9005: GetInfo STUBs. N3 health probes (LIVE) not drawn.
 DELETED: E3 EngineService client (631994c); E4 RestoreFromStore (60c6200); E7 event feed (78a90e1)

 +---------------------------------------------------------------------------+
 | SHAPE B (not deployed): cc-beacon-core :9001 / :9101, ChainService only   |
 |  core thread - - N1 blocks, IDLE (iff core) - - -> storage-core writer    |
 |  x N1 columns: ingest_columns, handle never injected into P2pStreamDeps   |
 |  EngineApi, called through DirectEngine on the core thread                |~~~~> el :8551
 |  not hosted: StorageService, prune, replay, migrator, p2p; [peers] empty  |
 +---------------------------------------------------------------------------+
```

The `:` line ending in `^` is a `....>` arrow (served, never dialed). Processes:
[01](01-deployment-and-processes.md); boot and seeding order: [12](12-boot-health-and-shutdown.md).

draw.io version: [`diagrams/topology-shapes.drawio`](diagrams/topology-shapes.drawio) (both shapes'
processes and internal edges, edges coloured by status; open in draw.io). The ASCII figure stays the
reviewed source ([diagrams/README.md](diagrams/README.md)).

## What works end to end today

### LIVE in default shape A

- **Boot and aggregate health.** All six CC containers bind, probe their `[peers]` and report
  aggregate health (`scripts/prove-mutual-health.sh`). "chain healthy" means bound, not
  bootstrapped: `mark_ready()` runs alongside the bind: `serve_with_options` hands over the
  local-ready handle just before binding (`crates/bootstrap/src/serve.rs:260-266`,
  `services/chain/src/run.rs:408-421`;
  [12 §3.1](12-boot-health-and-shutdown.md#31-shape-a-cc-chain-binds-first-then-bootstraps)).
- **p2p.** discv5 discovery, dialing, Noise/yamux and the Status handshake run. Status is fed by
  E1 (session + ChainView) and the E6 window stream (LIVE, content static), which always carries
  `earliest_available_slot = u64::MAX` (`crates/storage-core/src/serve.rs:1196-1200`), so the
  advertised window is empty.
- **EL upcheck.** Chain's EngineApi and the engine container's own EngineApi each upcheck the EL
  every slot. Core-absent, chain's boot task drops DirectEngine, but the detached upcheck task keeps
  running (`crates/engine-api/src/state.rs:562-596`).
- **Storage service.** Opens redb behind fail-closed gates, runs the prune task on wall-clock epochs
  and answers StorageService RPCs; window-gated reads refuse while its storage serve window is empty.

### LIVE when opted in

With checkpoint providers set, checkpoint sync installs a core at the finalized anchor and the
liveness sampler owns readiness. No fcU is sent at install; as wired, a hand-submitted ImportBlock
would send one for the anchor head and the per-slot floor would re-send it every slot
(`crates/chain-core/src/core.rs:2009-2014`). Liveness flips aggregate health; nothing in this repository
restarts a process on that signal.

### Wired, but not doing its job

| Gap | At `7d8833d` |
|---|---|
| No block source | `SwarmCommand::Subscribe` has a handler (`services/p2p/src/host.rs:1253-1260`) but no production sender, and `ImportBlock` has no in-tree dialer; the validation pool, E1 object/verdict and N2 are IDLE. At HEAD p2p subscribes to no gossip topics; the plan schedules gossip wiring for a later stage. The DEVNET-ONLY `cc-p2p --publish-fixture` / `--devnet-peer` mode (`devnet/compose.yml`) does subscribe to beacon_block and the 128 column topics and accepts objects unvalidated; it is test tooling, not a shape. |
| No DA signal | E2 is DEAD-AS-WIRED (`services/p2p` never calls `notify_data_available`), so the DA gate, the first check in `on_block` (`crates/fork-choice/src/on_block.rs:230-234`), never opens. With a core installed, a hand-submitted block that passes the pre-STF checks parks in pending_da and expires after 4 slots (`crates/chain-core/src/da.rs:43-49`); newPayload is never reached. Core-absent, ImportBlock returns NOT_BOOTSTRAPPED (`crates/chain-core/src/service.rs:244-246`). |
| No persistence | ABSENT in shape A; in shape B, N1 blocks are IDLE (iff core) and N1 columns DEAD-AS-WIRED. The durable set is always `None` at HEAD: the loader (`crates/storage-core/src/open.rs:246-272`) treats a store without fork-choice scalars or a snapshot as empty (`crates/storage-core/src/resume.rs:176-188`), and no production path writes either. So every boot is checkpoint sync or core-absent. In shape B, as wired, the first persist after checkpoint sync would fail the continuity bind, because the anchor is never persisted. |
| Serving peers | Block and column req/resp are STUB. E6 reads are DEAD-AS-WIRED: the `StorageClient` is bound to `_client` and never used (`services/p2p/src/service.rs:391`). E5 is DEAD-AS-WIRED: the storage service admits PutBackfillBatch, but the p2p client has no such method (`services/p2p/src/storage_client.rs:273-371`). |
| DAS | KZG verify pool DEAD-AS-WIRED (self-terminating, `services/p2p/src/service.rs:705`); GossipSub peer scoring PARAMS-ONLY ([values](../p2p-scoring.md), not installed); `SetCustodyGroupCount` STUB. |
| Leftovers | E8 EngineStream (p2p server) and EngineService in the TRANSITIONAL engine container are DEAD-AS-WIRED (served, never dialed); engine-api fastpath output is DEAD-AS-WIRED (every sidecar built is dropped). cc-attestation and cc-beacon-api are STUBs. The block path is traced in [05-block-lifecycle.md](05-block-lifecycle.md). |

## Doc map

Reading order: this page, the [glossary collisions](15-glossary.md#collisions-qualify-these-words),
then [01](01-deployment-and-processes.md) -> [02](02-crate-map.md) -> [03](03-internal-contracts.md)
-> [05](05-block-lifecycle.md), then by area.

| File | What it answers |
|---|---|
| [01-deployment-and-processes.md](01-deployment-and-processes.md) | Which processes run in each shape, with which ports, volumes, config and env, and where are the trust boundaries? |
| [02-crate-map.md](02-crate-map.md) | Which crate owns what, and which dependency rules hold? |
| [03-internal-contracts.md](03-internal-contracts.md) | Which proto services and cc-seam handles join the parts, and what is each edge's status? |
| [04-chain-core.md](04-chain-core.md) | How does the core thread own fork choice, schedule work by scheduler lane, and publish read models and events? |
| [05-block-lifecycle.md](05-block-lifecycle.md) | What happens to a block from arrival through import, persist and events, and where does it stop today? |
| [06-consensus-primitives.md](06-consensus-primitives.md) | What do cc-types, cc-crypto, cc-state-transition and cc-fork-choice provide, how is the EIP-7892 fork digest derived, and which spec vectors cover them? |
| [07-storage.md](07-storage.md) | How are redb, the storage-core writer, ArchiveWrite, prune/replay and StorageService built? |
| [08-execution-engine.md](08-execution-engine.md) | How does chain-core call the EL: DirectEngine, transport lanes, JWT, EL health machine, fastpath? |
| [09-p2p-host-and-discovery.md](09-p2p-host-and-discovery.md) | How is the libp2p swarm built, supervised and filled with peers (discv5, peer manager, identity)? |
| [10-p2p-gossip-and-das.md](10-p2p-gossip-and-das.md) | What gossip, validation, KZG and custody code exists, and which of it is wired? |
| [11-p2p-reqresp-and-sync.md](11-p2p-reqresp-and-sync.md) | Which req/resp protocols run, what does Status advertise, what backfill code exists, and how do p2p's gRPC links behave (chain-stream client E1/N2, EngineStream server E8, storage client E6/E5)? |
| [12-boot-health-and-shutdown.md](12-boot-health-and-shutdown.md) | In what order does each process boot, how is aggregate health computed, and how does SIGTERM drain? |
| [13-concurrency-model.md](13-concurrency-model.md) | Which runtimes, OS threads, tasks and bounded channels exist, and with which overflow policy? |
| [14-testing-and-enforcement.md](14-testing-and-enforcement.md) | Which tests, conformance suites and guard scripts protect the design, and which are red? |
| [15-glossary.md](15-glossary.md) | What does a term mean, and which ambiguous words must always be qualified? |
| Cross-cutting concerns | Network identity, time sources and the fork/BPO change checklist: [06](06-consensus-primitives.md). Error taxonomy: [03](03-internal-contracts.md). Panic/exit policy and memory bounds: [13](13-concurrency-model.md). Metrics conventions and the checkpoint-sync protocol: [12](12-boot-health-and-shutdown.md). Storage metrics inventory and store compatibility gates: [07](07-storage.md). Dangerous-knob guard, config reference, restart-survival table and checkpoint root of trust: [01](01-deployment-and-processes.md). |
| [../running.md](../running.md), [../contracts.md](../contracts.md), [../s2-rollback.md](../s2-rollback.md) | Build, run and probe the compose stack; proto conventions; S2 store rollback. Parts predate HEAD: bootstrap gating and `CC_CHAIN_ENGINE_URI` (running), the ChainService table and event-ring size (contracts), d3d7f60 and 60c6200 (rollback). |
| [../storage-schema.md](../storage-schema.md), [../storage-engine.md](../storage-engine.md), [../serve-windows.md](../serve-windows.md), [../el-runbook.md](../el-runbook.md), [../../devnet/README.md](../../devnet/README.md) | Table layout, the redb engine seam, CC-4A block-floor serve obligations, EL operations, devnet topology. |
| [../adr/README.md](../adr/README.md) | Why was each decision taken? This is the ADR index. |

## Planned changes

From [`plan/architecture.md`](../../plan/architecture.md) §0, §1, §2.5, §9.1; none of these is
wired or deployed at `7d8833d`.

```text
 AS BUILT: shape A, six CC containers + el | TARGET after S2 (plan section 0)
 p2p <=== E1 session+view ======= chain    | +-------+ ChainIngress: E1, E2 +----------------+
 p2p - - E1 object/verdict - - -> chain    | | p2p   |<-------------------->| beacon-core:   |
 p2p x E1 publish, E1 column, E2  chain    | |       | P2pEgress: E1, E8    | chain, storage |
 p2p <=== E6 window (static) ==== storage  | +-------+                      | and engine;    |
 p2p x E6 reads, E5 backfill      storage  |   transport impl UNDECIDED     | one redb       |
 p2p <.... E8 (never dialed) .... engine   |                                +----------------+
 chain, engine ~~~~> el :8551              |                                   | JWT: the one
   (two independent EngineApis)            |                                   v external trust edge
 attestation, beacon-api: GetInfo STUBs    |                                 geth
                                           | E3, E4, E7 already DELETED; plan deletes E5, E6;
 DELETED: E3, E4, E7                       | E8 folds into E1's egress half (P2pEgress)
```

Right side: target design, none of it deployed; its arrows carry no status. The left side follows
the legend; p2p dials E1 and E6.

draw.io version: [`diagrams/target-after-s2.drawio`](diagrams/target-after-s2.drawio) (the
right-hand target only; no status). See [diagrams/README.md](diagrams/README.md).

- **Planned: two processes** (figure, right); the TRANSITIONAL cc-chain, cc-storage and cc-engine
  leave. S2 exit gate E2.2 is still open
  ([`plan/issues/s2-exit-note.md`](../../plan/issues/s2-exit-note.md)).
- **Planned (S3):** the Loop A queue taxonomy, the real DA feed, a reconnected `kzg_tx`, storage
  serve window publication, the backfill write path, and one `cc-wire` codec.
- **Planned (S5):** delete the cc-attestation and cc-beacon-api stubs; add a REST Beacon API and
  a separate slashing-protection store ([ADR-R-05](../adr/ADR-R-05.md)).
