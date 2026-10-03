# Deployment and processes

> **As built at `7d8833d`** (`develop`, 2026-08-16). Status labels and diagram legend: [README](README.md#status-labels).
> Target design: [`plan/architecture.md`](../../plan/architecture.md) §0, §4.2, §6, §7, §9.

This page covers what runs, where it runs, how it is configured and who can reach it. At HEAD
there are two deployable **shapes**:

- **Shape A** is `docker-compose.yml`: six consensus-client (CC) containers built from one
  `Dockerfile`, plus `el` (geth). It is the **only deployed shape**.
- **Shape B** is `bin/beacon-core` (binary `cc-beacon-core`): one process that hosts chain-core
  and the storage-core writer. The image builder compiles it, but no compose service, Make
  target or CI job runs it, and S2 exit criterion E2.2 (two processes on self-devnet) is open
  ([`s2-exit-note.md`](../../plan/issues/s2-exit-note.md#e22--two-processes-on-self-devnet-s2-j-01-s2-a-15)).

"4-container topology" (`services/chain/src/run.rs:5`, `services/engine/src/lib.rs:5`,
`config/beacon-core.toml:2`, `crates/storage-core/src/boot.rs:537`, `docs/s2-rollback.md:21`) is
a stale name for shape A.
See also: edges in [03-internal-contracts.md](03-internal-contracts.md); boot, health DAG and
SIGTERM in [12-boot-health-and-shutdown.md](12-boot-health-and-shutdown.md); threads in
[13-concurrency-model.md](13-concurrency-model.md); operator procedures in
[running.md](../running.md), [el-runbook.md](../el-runbook.md) and [s2-rollback.md](../s2-rollback.md).

## Shapes at a glance

| | Shape A (compose) | Shape B (`cc-beacon-core`) |
|---|---|---|
| Defined by | `docker-compose.yml`, `Dockerfile` | `bin/beacon-core/src/boot.rs:397-577`, `config/beacon-core.toml` |
| Processes | 6 CC containers + `el` | 1 CC process; the EL is whatever `el_endpoint` names |
| Deployed | yes | no |
| Network | Hoodi: el `--hoodi` (`docker-compose.yml:188`), bundled `hoodi-config.yaml` (`Dockerfile:78-79`) | Hoodi fixture unless `network_config` is set (`bin/beacon-core/src/boot.rs:265-287`) |
| Chain host | `cc-chain`, core-absent by default | in-process chain-core, core-absent by default |
| Store owner | `cc-storage` (redb, writer, serve, prune); no chain data written at HEAD | in-process storage-core writer; redb opened first |
| Imported blocks persisted | ABSENT (`archive = None`, `services/chain/src/run.rs:368-378`) | IDLE (iff core): wired through N1 `ArchiveWrite`; no block passes the DA gate, and as wired the first child of a checkpoint anchor would fail the continuity bind ([03](03-internal-contracts.md#3-edge-inventory)) |
| EL clients | two (chain's EngineApi, which DirectEngine drives only with an installed core; the engine container's leftover EngineApi) | one (beacon-core's EngineApi, driven by DirectEngine only with an installed core) |
| gRPC surface | six services on network `cc`; none host-published | `ChainService` only, default `127.0.0.1:9001` |
| p2p | `cc-p2p` container (LIVE) | ABSENT |

No shipped configuration pairs cc-p2p with cc-beacon-core; how p2p attaches to shape B is not
yet defined.

## Shape A: docker-compose

### Topology and ports

```text
 HOST (published ports)  COMPOSE NETWORK "cc" (bridge; DNS name = service name)
                       +----------------------------------------------------------------------+
 127.0.0.1:9101 ------>| chain        grpc :9001  metrics :9101   peers: none (DAG root)      |
 127.0.0.1:9102 ------>| p2p          grpc :9002  metrics :9102   peers: chain, storage       |
                       |              libp2p tcp/udp :9000 in the container, NOT published    |
 127.0.0.1:9103 ------>| attestation  grpc :9003  metrics :9103   peers: chain                |
 127.0.0.1:9104 ------>| engine       grpc :9004  metrics :9104   peers: chain                |
 127.0.0.1:9105 ------>| beacon-api   grpc :9005  metrics :9105   peers: chain, attestation,  |
                       |                                                  storage             |
 127.0.0.1:9106 ------>| storage      grpc :9006  metrics :9106   peers: chain                |
                       |                                                                      |
 all ifaces :8545 ---->| el (geth v1.17.5)   http :8545 eth,net,web3 (no auth, vhosts=*)      |
 all ifaces :6060 ---->|                     metrics :6060                                    |
 all ifaces :30303 --->|                     devp2p :30303 tcp+udp                            |
                       |                     authrpc :8551, JWT, cc only, NOT published       |
                       +----------------------------------------------------------------------+
 CC processes bind 0.0.0.0:900N and 0.0.0.0:910N inside their container; compose publishes
 only 910N, and only on host 127.0.0.1. gRPC 900N, authrpc 8551 and libp2p 9000 stay inside.
```

`---->` is a request toward its server, here a host-side connection to a published port.
`peers:` lists each process's health peers (its `[peers]` table).

### Process table

| Process (binary) | Crate; entry point | gRPC | Metrics | Shape A | Shape B |
|---|---|---|---|---|---|
| `cc-chain` | `services/chain` (shim over cc-chain-core); `services/chain/src/run.rs:307` | 9001 | 9101 | TRANSITIONAL (deployed; core-absent by default) | ABSENT |
| `cc-p2p` | `services/p2p`; `services/p2p/src/main.rs:463`, then `services/p2p/src/service.rs:857` | 9002 | 9102 | LIVE | ABSENT |
| `cc-attestation` | `services/attestation`; `services/attestation/src/main.rs:77` | 9003 | 9103 | STUB | ABSENT |
| `cc-engine` | `services/engine`; `services/engine/src/main.rs:89` | 9004 | 9104 | TRANSITIONAL (no gRPC caller; second EL client) | ABSENT |
| `cc-beacon-api` | `services/beacon-api`; `services/beacon-api/src/main.rs:75` | 9005 | 9105 | STUB | ABSENT |
| `cc-storage` | `services/storage` (shim over cc-storage-core); `crates/storage-core/src/boot.rs:471` | 9006 | 9106 | TRANSITIONAL (deployed) | ABSENT |
| `cc-beacon-core` | `bin/beacon-core`; `bin/beacon-core/src/boot.rs:397` | 9001 | 9101 | ABSENT | TRANSITIONAL (target host; not deployed) |
| `el` (geth v1.17.5) | image `ethereum/client-go:v1.17.5` | authrpc 8551 | 6060 | LIVE (external image) | ABSENT (operator supplies `el_endpoint`) |
| `cc-engine-blackhole` | `services/engine/src/bin/blackhole.rs:61-74` | 9004 | none | TRANSITIONAL (opt-in overlay; cannot park chain-core) | ABSENT |
| `cc-p2p` devnet mode | `services/p2p` (`--publish-fixture`, `--devnet-peer`); `services/p2p/src/main.rs:475-482` | none | 9102 | DEVNET-ONLY | ABSENT |

- On a deployed process (`cc-chain`, `cc-engine`, `cc-storage`), TRANSITIONAL means it runs in
  production today and the plan removes it. `cc-chain` and `cc-storage` are the production chain
  and storage hosts of shape A. `cc-beacon-core` is labelled TRANSITIONAL (target host): built
  and not deployed during the A/B period; the plan keeps it.
- Each CC host runs `cc_config::load`, its fail-before-bind checks and `cc_bootstrap::init`, then
  `cc_bootstrap::serve` or `serve_with_options` (`crates/bootstrap/src/serve.rs:204`; see
  [12 §2](12-boot-health-and-shutdown.md#2-the-bootstrap-host)). The chain, storage and
  beacon-core `main.rs` files are thin shims over the entry points above.
- `cc-beacon-core` and `cc-chain` have the same defaults, `127.0.0.1:9001` and `127.0.0.1:9101`
  (`config/beacon-core.toml:5-6`, `config/chain.toml:4-5`); one must be overridden to co-host.
- Devnet mode bypasses `cc-config` and `cc_bootstrap::serve`. `devnet/compose.yml`
  ([devnet/README.md](../../devnet/README.md)), a third topology outside this page, uses it.

#### What each process serves

Every gRPC process hosted by `cc_bootstrap` also serves `grpc.health.v1.Health` (aggregate `""`
plus its self-name) and reflection v1 (`crates/bootstrap/src/serve.rs:304-312`); the blackhole
overlay and p2p devnet mode do not.

| Process | gRPC services | Health peers | In-process subsystems |
|---|---|---|---|
| chain | `eth.chain.v1.ChainService` (11 RPCs) | none (DAG root) | events task and event ring; P2pStream server and drivers; DirectEngine over cc-engine-api (upcheck driver, fastpath worker); core thread, slot-tick thread and liveness sampler **only if** `checkpoint_providers` is non-empty |
| p2p | `eth.p2p.v1.P2pService`: GetInfo (LIVE), SetCustodyGroupCount (STUB), EngineStream (DEAD-AS-WIRED: served, never dialed; E8) | chain, storage | swarm task, supervisor, peer manager, discovery, validation pool (IDLE: at HEAD p2p subscribes to no gossip topics; the plan schedules gossip wiring for a later stage), KZG verify pool (DEAD-AS-WIRED), chain-stream client (LIVE: E1 session + view), WatchServeWindow client (LIVE (content static): E6 window) |
| attestation | `eth.attestation.v1.AttestationService`: GetInfo | chain | none |
| engine | `eth.engine.v1.EngineService` (5 RPCs; DEAD-AS-WIRED: served, no in-tree caller) | chain | a second, independent EngineApi with its own upcheck loop, EL health machine and fastpath worker; the only exporter of `cc_engine_*` |
| beacon-api | `eth.beacon_api.v1.BeaconApiService`: GetInfo; no REST server exists | chain, attestation, storage | none |
| storage | `eth.storage.v1.StorageService` (10 RPCs) behind `UnaryPermitService` | chain | redb, storage-core writer, prune task (LIVE), replay task (IDLE), migrator (DEAD-AS-WIRED), serve pool; an ArchiveWriter built and dropped (DEAD-AS-WIRED, `crates/storage-core/src/boot.rs:563`) |
| beacon-core (B) | `eth.chain.v1.ChainService` only (`bin/beacon-core/src/boot.rs:567`) | none | see [Shape B](#shape-b-binbeacon-core) |

- **Shape A chain is core-absent by default.** Compose sets no checkpoint providers and
  `config/chain.toml:27` has `checkpoint_providers = []`. ChainService fork-choice RPCs therefore
  answer `FAILED_PRECONDITION` with reason `NOT_BOOTSTRAPPED` (`crates/chain-core/src/service.rs:52`;
  the boot task stops at `services/chain/src/run.rs:423-428`).
- **Two EL clients.** Chain's EngineApi is built at boot (`run.rs:362`), so its upcheck driver
  polls `el:8551` every slot whether or not a core is installed. The engine container runs a
  second client against the same EL with the same JWT. cc-engine remains in compose for A/B
  comparison and has no in-tree caller. Chain builds its EngineApi with `finish(None)` and exports
  no `cc_engine_*` series, so `:9104` reflects only the engine container's own machine
  ([08-execution-engine.md](08-execution-engine.md)).

### Network and port-publish policy

- **One network.** Every service, `el` included, joins the user-defined network `cc`
  (`docker-compose.yml:226-227`) through the `x-cc-runtime` anchor (`:12-16`, `el` at `:184`).
  Service names are DNS names (`http://chain:9001`, `http://el:8551`).
- **CC ports.** `CC_<SVC>_GRPC_ADDR` / `CC_<SVC>_METRICS_ADDR` bind `0.0.0.0` inside each
  container. Compose publishes **only** metrics, and only as `127.0.0.1:910N:910N`
  (`docker-compose.yml:42,66,86,114,146,168`); gRPC 9001-9006 is never published (`:4-6`).
  TOML defaults bind `127.0.0.1` for local runs (e.g. `config/chain.toml:4-5`); metrics = gRPC + 100.
- **el ports.** 8545, 6060 and 30303 tcp+udp are published on **all** host interfaces
  (`:212-216`), outside the bind policy by design (`:6`). 8545 serves unauthenticated
  `eth,net,web3` with `--http.vhosts=*` (`:199-203`). authrpc 8551 is not published.
  `--authrpc.vhosts=el` rejects any *hostname* other than `el` in the `Host` header, while
  IP-literal hosts pass (`:192-197`); the JWT is the actual gate.
- **libp2p.** p2p listens on TCP and UDP 9000 inside its container (`config/p2p.toml:12,66-67`).
  Shape A does not publish the libp2p port; inbound peer reachability is not configured.
- **Enforcement.** `make check-offhost-policy` (`scripts/offhost-port-scan.sh --policy`, part of
  `make lint`) fails if a CC service publishes gRPC, or does not publish its metrics port exactly
  as `127.0.0.1:910N:910N` (`scripts/offhost-port-scan.sh:346-363`); `--live` is the off-host scan
  record ([running.md](../running.md#off-host-port-scan-e08--s0-b-20)). `make check-compose-uris`
  asserts the cross-service URI overrides and the identity mount.

### Volumes and the backup unit

| Volume or mount | Mounted into | Pointed at by | Contents |
|---|---|---|---|
| `cc-store-data` (named) | storage `/app/data`, rw | `CC_STORAGE_DATA_DIR=/app/data` | `store.redb` |
| `cc-p2p-identity` (named) | p2p `/app/data` rw; storage `/identity` ro | `CC_P2P_NODE_KEY_PATH`, `CC_STORAGE_NODE_KEY_PATH=/identity/node_key` | `node_key`: 32 raw bytes, mode 0600 |
| `elstore` (named) | el `/data` | `--datadir=/data` | geth datadir and `geth.ipc` |
| `./secrets` (bind, ro) | chain, engine and el at `/jwt` | `CC_CHAIN_JWT_SECRET_PATH`, `CC_ENGINE_JWT_SECRET_PATH`, `--authrpc.jwtsecret` | `jwt.hex` |

- **Prerequisite: `secrets/jwt.hex` before `up`** (hex for 32 bytes, mode exactly 0600;
  [running.md](../running.md#jwt-secret) names only `el` as its consumer). `cc-chain` and the
  engine container load it fail-before-bind (`services/chain/src/run.rs:316-318`,
  `services/engine/src/main.rs:94-95`); without it both exit and restart in a loop, so chain
  never turns healthy and its dependents never start. As wired, the images run as uid 10001
  (`Dockerfile:69-70,84`) and `JwtSecret::load` checks the mode, then reads
  (`crates/engine-api/src/jwt.rs:131-145,199-203`): on a Linux host with a native bind mount, a
  0600 file owned by another uid would fail the read (traced, not observed).
- **In shape A, `store.redb` receives no chain data at HEAD.** `cc-chain` has `archive = None`
  (`services/chain/src/run.rs:368-378`), `cc-storage` builds an ArchiveWriter and drops it
  (`crates/storage-core/src/boot.rs:563`), and `PutBackfillBatch` has no client (E5). Every boot
  is empty-store (`boot.rs:535-538`), then checkpoint sync or core-absent.
- **`cc-store-data` + `cc-p2p-identity` are one backup unit** (`docker-compose.yml:231-235`):
  back up and restore them together. As designed, open compares the store's identity row
  (`meta.node_id`, else `AnchorInfo.node_id`) with the 32 raw key bytes and refuses a mismatch,
  or a missing key beside an identity row (I-node-id, [ADR-P4-13](../adr/ADR-P4-13.md)).
  **Shape A: INERT.** cc-storage never stamps `meta.node_id` and nothing writes `AnchorInfo`, so
  both checks skip (`crates/store/src/invariants.rs:879-905`,
  `crates/storage-core/src/durable_set.rs:963-967`) and the
  [running.md](../running.md#the-two-volumes-are-one-backup-unit) negative exercise would not
  refuse. **Shape B: LIVE**: beacon-core stamps the id at every open
  (`bin/beacon-core/src/boot.rs:259-261`), and as wired either shape would then refuse that store
  beside a different key ([07-storage.md](07-storage.md#35-the-eight-invariants-and-i-node-id)).
  Moving the pair to shape B and back: [s2-rollback.md](../s2-rollback.md).
  **No online-backup path exists.** No RPC exports the database file (`GetSnapshotState` streams
  one stored state and at HEAD answers `NOT_AVAILABLE`), `cc-store` offers only `verify`,
  `compact` and `dump`, with no repair (`bin/cc-store/src/lib.rs:1-8`), and redb refuses a second
  opener while the node holds the file (`crates/store/src/engine/redb.rs:19-25`). Stop the
  storage service (or beacon-core) before copying `cc-store-data`, and copy `cc-p2p-identity`
  with it. The writer does not drain its mailbox on SIGTERM
  ([07 §4.3](07-storage.md#43-the-single-writer-and-its-mailbox)).
- In shape A only p2p writes `node_key`. It creates the key with mode 0600 and refuses any other
  mode, narrower ones included (`services/p2p/src/identity.rs:162,269`).
- The comments at `docker-compose.yml:62,235` and the volume table at `docs/running.md:548` say
  the identity volume also holds an ENR `.seq` file. None is written: production uses
  `EnrManager::from_key_and_listen` (`services/p2p/src/discovery/task.rs:270`), not the persisted variant.
- chain has no data volume: it keeps no durable state in shape A. Shape B resolves
  `data/storage/store.redb` and `./data/node_key` against its CWD (`config/beacon-core.toml:10,15`).

#### What survives a restart

| State | Survives? | Where, and why |
|---|---|---|
| `node_key` | yes | `cc-p2p-identity` (shape B: `./data/node_key`) |
| `store.redb` rows | yes | at HEAD shape A holds only `schema_version`, `config_digest` and `prune_marks`; shape B holds `schema_version`, `config_digest`, `node_id` and `write_cursor`, and no `prune_marks` ([07 §3.3](07-storage.md#33-meta-records-and-their-writers-at-head)) |
| fork-choice Store, residency, `pending_da` / `pending_engine`, `HeadSnapshot` / `EpochContext` | no | chain has no volume, and the durable set is always `None` ([07 §4.5](07-storage.md#45-durable-set-resume-and-seed)) |
| event ring and event session id | no | the id is random per process, so every saved cursor fails `CURSOR_UNKNOWN_SESSION` after a restart ([04 §11](04-chain-core.md#11-event-ring-and-subscriber-contract)) |
| ENR seq | no | restarts per process ([09 §4](09-p2p-host-and-discovery.md#4-identity-node-key-and-enr)) |
| peer bans, app scores, discv5 table | no | in memory; bans are a `HashSet` (`services/p2p/src/peer_manager/ban.rs:89-121`; [09 §6](09-p2p-host-and-discovery.md#6-peer-manager-scheduling-scoring-bans)) |
| advertised window | no | re-seeded to `u64::MAX` (`services/p2p/src/backfill/window.rs:24,102`) |
| EL health machine, `AuthFailed` included | no | starts Offline; a restart is the only `AuthFailed` reset ([08 §3.5](08-execution-engine.md#35-el-health-state-machine)) |

### Startup order and health gating

`depends_on` gates startup only:

| Service | Waits for | Condition |
|---|---|---|
| chain, el | nothing | -- |
| p2p, attestation, storage | chain | `service_healthy` |
| engine | chain; el | `service_healthy`; `service_started` ([ADR-P3-14](../adr/ADR-P3-14.md)) |
| beacon-api | chain, attestation, storage | `service_healthy` |

- **Healthchecks.** CC: `grpc-health-probe -addr=:900N` with no `-service`, i.e. the aggregate
  `""` status; 5 s interval, 2 s timeout, 12 retries, 10 s start period
  (`docker-compose.yml:18-22`). el: `geth attach --exec 'eth.syncing == false'` over IPC;
  15 s / 10 s / 40 / 120 s (`:217-224`).
- **"chain healthy" means bound, not bootstrapped.** chain calls `mark_ready()` alongside the
  bind: `serve_with_options` hands the boot task the local-ready handle just before binding
  (`crates/bootstrap/src/serve.rs:260-266`, `services/chain/src/run.rs:408-421`;
  [12 §3.1](12-boot-health-and-shutdown.md#31-shape-a-cc-chain-binds-first-then-bootstraps)), so
  dependents start whether or not a core is installed.
  `docs/running.md:191-192`, `services/chain/src/lib.rs:22-23` and
  `crates/bootstrap/src/serve.rs:87-92` say otherwise; the code wins.
- p2p does not wait for the storage service, but the storage service is its health peer, so
  p2p's aggregate health stays NOT_SERVING until the storage service serves. chain has no `depends_on el`; its EL health machine starts
  Offline and the upcheck loop retries.
- `make compose` builds, starts and runs `wait-healthy` (six CC services, `el` excluded);
  `make prove-health` runs the [mutual-health proof](../running.md#mutual-health-proof). Prober
  timing and the liveness sampler: [12-boot-health-and-shutdown.md](12-boot-health-and-shutdown.md).

### Restart and stop policy

All services, `el` included, inherit `restart: unless-stopped`, `stop_signal: SIGTERM` and
`stop_grace_period: 5s` (`docker-compose.yml:12-16`).

- `unless-stopped` acts on process exit only; Docker does not restart an unhealthy container,
  and the Docker healthcheck only gates `depends_on`. Liveness flips aggregate health; nothing in
  this repository restarts a process on that signal.
- Every non-zero exit in [12 §5.3](12-boot-health-and-shutdown.md#53-exits-that-are-not-sigterm)
  is restarted, for example a p2p swarm panic ([ADR-P2-13](../adr/ADR-P2-13.md)), a storage-core
  writer panic or a failed chain checkpoint sync. That includes fail-before-bind refusals (bad JWT
  file, store-open gate, missing storage `network_config`), which therefore crash-loop rather
  than stop. `depends_on` gates only the first start: a restarted chain does not re-gate its
  dependents.
- Shutdown normally fits the 5 s grace period. The worst case exceeds it: 5.15 s for p2p, and
  unbounded for chain with an installed core (`push_wait` has no deadline), so compose SIGKILLs
  at 5 s (budget in
  [12 §5.2](12-boot-health-and-shutdown.md#52-budgets-against-stop_grace_period-5s)). el gets the
  same 5 s; the comment at `docker-compose.yml:180-182` accepts that geth may be killed
  mid-flush.

### One Dockerfile, one binary per image

- **Builder** (`rust:1.97.1-bookworm`; `ARG RUST_VERSION` must equal `rust-toolchain.toml`,
  checked at `Dockerfile:29-31`): `cargo build --workspace --release --locked`, then stages
  **eight** binaries in `/out` (`:37-43`): the six services, `cc-engine-blackhole` and
  `cc-beacon-core` (`Dockerfile:2` still says "six").
- **Runtime** (`debian:bookworm-slim`, [ADR-09](../adr/ADR-09.md)): copies **only**
  `/out/${SERVICE}` to the `ENTRYPOINT` `/usr/local/bin/service` (`:64-85`). Each compose service
  sets `build.args.SERVICE`: six runtime images from one builder; none selects `cc-beacon-core`.
- **Also in the image:** `grpc-health-probe` v0.4.54, per-arch SHA-256 checked (`:46-59`);
  `config/` at `/app/config/`, which `cc-config` requires (`:74-75`); the Hoodi YAML (`:78-79`); a
  writable `/app/data`; uid/gid 10001 (`:83-84`). `crates/bootstrap/build.rs` embeds the
  `CC_GIT_SHA` build arg in `cc_build_info`.

## Shape B: `bin/beacon-core`

```text
 +------------------------------------------------------------------------+
 | cc-beacon-core: one process, one multi-thread tokio runtime            |
 | config/beacon-core.toml + CC_BEACON_CORE_*; data paths relative to CWD |
 |                                                                        |
 | tonic 127.0.0.1:9001  ChainService (+ grpc.health.v1, reflection)      |
 |   also P2pStream, GetValidatorRecords <.... (no p2p dials B; open)     |
 | hyper 127.0.0.1:9101  GET /metrics: chain, liveness, storage families  |
 |   |                                                                    |
 |   | I1 CoreHandle (core-absent: FAILED_PRECONDITION NOT_BOOTSTRAPPED)  |
 |   v                                                                    |
 | [chain-core thread]* <---- I2 SlotTick ---- [chain-slot-tick thread]*  |
 |   |    |    |                                                          |      +---------------+
 |   |    |    +~~ I5 DirectEngine, IDLE: newPayloadV4, fcUV3 ~~~~~~~~~~~~|~~~~~>| el (geth)     |
 |   |    |                                                               |      | authrpc :8551 |
 |   |    +- - I3 mpsc(4096), IDLE - -> events task (ring 4096 ev/64 MiB) |      | Bearer JWT    |
 |   |                                                                    |      | HS256 {iat}   |
 |   +- - N1 ingest_block_blocking (IDLE) - -> storage-core writer        |      | per request   |
 |        task (P0 32 / P1 64 / P2 256) - - -> data/storage/store.redb    |      |               |
 | engine-api tasks, spawned at boot by finish(None), even core-absent:   |      |               |
 |   upcheck driver: eth_syncing, exchangeCapabilities ~~~~~~~~~~~~~~~~~~~|~~~~~>|               |
 |   fastpath worker: getBlobsV2 (an installed core triggers it) ~~~~~~~~~|~~~~~>|               |
 |                                                                        |      +---------------+
 | liveness sampler* (Ping every slot/4) ----> local_ready, aggregate ""  |
 | SIGTERM pre-drain joins the core thread only; writer not shut down     |
 | NOT hosted: StorageService, serve pool, replay, prune, migrator, p2p   |
 +------------------------------------------------------------------------+
 * exists only once a core is installed (durable seed or checkpoint sync). With the default
   checkpoint_providers = [] and an always-empty durable set, B is core-absent and only the
   upcheck arrow carries traffic.
```

`---->` is a call, `- - ->` an idle edge, `<....` served but never dialed (note under
[Shapes at a glance](#shapes-at-a-glance)), `~~~~>` an in-process call that leaves the process
over HTTP to the EL (status in its label); a vertical `|`/`v` is a `---->` drawn downward. Edge
ids: E/N from [03-internal-contracts.md](03-internal-contracts.md), I1-I10 from the
[glossary](15-glossary.md#in-process-edges-i1i10).

Boot order: config, network YAML and `EngineApi::prepare_with_chain_config` (JWT, `[el_forks]`,
KZG); `ensure_node_key`; open redb (fail-closed gates, node-id stamp) and load the durable set;
tracing; writer; events and DirectEngine; seed decision; bind 9001/9101 with
`require_local_ready` (`bin/beacon-core/src/boot.rs:397-577`). Everything before the bind aborts
boot on failure; step by step in [12 §3.2](12-boot-health-and-shutdown.md#32-shape-b-cc-beacon-core-seeds-before-bind).
This matches `plan/architecture.md` §4.2, plus `ensure_node_key` before open. beacon-core creates
a missing node key without enforcing 0600 (`:290-316`). Writer bounds are the hard defaults
32/64/256 (`crates/storage-core/src/open.rs:304`).

Shape B has no `[peers]` (`config/beacon-core.toml:32`) and skips the
[dangerous-knob guard](#dangerous-knob-guard) that chain and the storage service run.
Shape B does not yet bound its store or order EL readiness before seed replay. On SIGTERM the
pre-drain hook joins the core thread only; `StorageRuntime::shutdown()` is never called, so the
writer is not stopped or drained (`bin/beacon-core/src/boot.rs:530,552-564`): read
[12-boot-health-and-shutdown.md](12-boot-health-and-shutdown.md) before relying on a "clean
shutdown" for rollback. To run it, start the binary from a CWD that holds
`config/beacon-core.toml`; the host procedure, including moving the compose volumes over, is in
[s2-rollback.md](../s2-rollback.md).

## Configuration layering

Every CC service process loads its configuration through `cc_config::load`
([ADR-12](../adr/ADR-12.md)). The two exceptions are listed after the table. Layers, where a
later layer wins (`crates/config/src/lib.rs:229-248`):

| # | Layer | Source | Notes |
|---|---|---|---|
| 1 | defaults | `TelemetryDefaults` | `log_filter = "info"`, `log_format = "json"` |
| 2 | TOML | `config/<svc>.toml`, CWD-relative, `Toml::file_exact` | must exist; a missing file is a load error |
| 3 | env | `CC_<SVC>_*`, nested keys split on `__` | `CC_BEACON_API_PEERS__CHAIN` -> `peers.chain` |
| 4 | bare env | `RUST_LOG`, `LOG_FORMAT` | override `log_filter` / `log_format` |

- **Slugs** match `^[a-z0-9-]+$` (`lib.rs:254-264`); a hyphen becomes `_` in the env prefix
  (`:269-271`): `chain`, `p2p`, `attestation`, `engine`, `beacon-api`, `storage`, `beacon-core`.
- **Validation** checks the shared `ServiceConfig` (`grpc_addr`, `metrics_addr`, `peers`,
  `log_format`, `log_filter`) first, then the service type. A missing-field error names the key,
  the TOML path and the env var (`lib.rs:166-195`). All of it happens before any bind.
- Compose always sets `RUST_LOG` and `LOG_FORMAT` (`docker-compose.yml:8-10`), so inside compose
  the TOML `log_filter` and `CC_<SVC>_LOG_FILTER` never take effect.
- `[peers]` is a map merged by key. As wired, env can override or add a peer but would not
  remove a TOML-declared one (inferred from figment's dictionary merge; untested in-repo).
- **Exceptions:** p2p parses clap `env =` variables (`CC_P2P_*`, `services/p2p/src/main.rs:66-125`)
  on every start (`:465`), outside cc-config and unseen by `scripts/check-no-env-reads.sh`; only
  devnet mode uses their values. Some names (`CC_P2P_NODE_KEY_PATH`, `_GRPC_ADDR`,
  `_METRICS_ADDR`, `_NETWORK_CONFIG`) are also cc-config keys, and compose sets the first three.
  `cc-engine-blackhole` takes its bind address from argv.

Compose environment per service. Each name below carries the `CC_<SVC>_` prefix, and every
service also gets `RUST_LOG` and `LOG_FORMAT`:

| Service | Set by `docker-compose.yml` |
|---|---|
| chain | `GRPC_ADDR`, `METRICS_ADDR`, `ENGINE_URI` (dead), `EL_ENDPOINT=http://el:8551`, `JWT_SECRET_PATH=/jwt/jwt.hex` |
| p2p | `GRPC_ADDR`, `METRICS_ADDR`, `PEERS__CHAIN`, `PEERS__STORAGE`, `NODE_KEY_PATH=/app/data/node_key` |
| attestation | `GRPC_ADDR`, `METRICS_ADDR`, `PEERS__CHAIN` |
| engine | `GRPC_ADDR`, `METRICS_ADDR`, `PEERS__CHAIN`, `P2P_URI` (dead), `EL_ENDPOINT`, `JWT_SECRET_PATH` |
| storage | `GRPC_ADDR`, `METRICS_ADDR`, `PEERS__CHAIN`, `NODE_KEY_PATH=/identity/node_key`, `DATA_DIR=/app/data` |
| beacon-api | `GRPC_ADDR`, `METRICS_ADDR`, `PEERS__CHAIN`, `PEERS__ATTESTATION`, `PEERS__STORAGE` |

### Dangerous-knob guard

`cc_config::check_dangerous_knobs` (`crates/config/src/devnet_guard.rs:142-161`) guards exactly
three knobs: `storage.retention_override`, `storage.debug.crash_point`, and
`chain.event_ring_bytes` below 64 MiB (`DEFAULT_EVENT_RING_BYTES`, `:32`). Each is accepted only
when the same process's `genesis_validators_root` config key is set and is neither Hoodi's
(`0x212f...cb5f`) nor mainnet's (`0x4b36...fe95`) (`:15-20`, `:81-127`). Otherwise the process
refuses before bind with `DangerousKnobError` naming the knob. Callers: chain
(`services/chain/src/run.rs:311`; its GVR key, `:200`, is read only by this guard) and the storage
service (`crates/storage-core/src/boot.rs:475`). **Shape A: LIVE. Shape B: ABSENT**: beacon-core
never calls it, yet reads `event_ring_bytes` (`bin/beacon-core/src/boot.rs:174-175`), so a shrunk
ring is accepted there unguarded. Not covered by the guard: p2p `[clock].slot_clock_offset_seconds`
(`config/p2p.toml:40`), which shifts p2p's wall clock (`services/p2p/src/clock.rs:207,231`; wired
at `services/p2p/src/main.rs:427`), and the clap `CC_P2P_*` devnet flags.

### Selected keys

Service keys outside the layering above that operators trip over, plus dead keys kept for gates.
Status is per shape (A / B).

| Key | Default | Consumer | Status (A / B) |
|---|---|---|---|
| p2p `enable_discovery` | `true` (`config/p2p.toml:17`) | `false` skips the discovery task factory (`services/p2p/src/service.rs:483`) | LIVE / ABSENT |
| p2p `[clock]` `genesis_time`, `seconds_per_slot`, `slots_per_epoch`, `slot_clock_offset_seconds` | TOML: Hoodi `1742213400`, 12, 32, 0 (`config/p2p.toml:32-40`) | `SlotClock` (`services/p2p/src/clock.rs:159-177`), never cross-checked against `network_config` | LIVE / ABSENT |
| storage `prune_margin_epochs`, `prune_chunk_keys`, `prune_deadline_ms` | 1, 512, 2000 (`crates/storage-core/src/boot.rs:107-115`) | prune task | LIVE / ABSENT (no prune) |
| storage `serve_permits`, `serve_queue_timeout_ms`, `serve_buffer_bytes` | 4, 2000, 64 MiB (`boot.rs:168-176`) | serve admission (`boot.rs:311-318`) | DEAD-AS-WIRED (admission wired; no in-tree client dials any permit-taking read) / ABSENT |
| `[el_forks]` `osaka_time` (required), `bpo1_time`, `bpo2_time`, `amsterdam_time` | Hoodi Osaka / BPO1 / BPO2, Amsterdam unset; three copies: `config/chain.toml:83-86`, `config/engine.toml:55-58`, `config/beacon-core.toml:42-45` | `ElForksConfig` (`crates/engine-api/src/config.rs:143-155`): a missing table is refused at boot; only `amsterdam_time` changes method selection (`crates/engine-api/src/version.rs:127-145`, [08 §3.3](08-execution-engine.md#33-methods-versions-and-timeouts)) | LIVE / LIVE |
| chain `engine_uri` | `http://127.0.0.1:9004` | debug-logged only (`services/chain/src/run.rs:358-361`) | DEAD-AS-WIRED / ABSENT |
| `p2p_uri` (engine; flattened into chain and beacon-core too) | `http://127.0.0.1:9002` | parsed, never dialed (`crates/engine-api/src/config.rs:194-199`) | DEAD-AS-WIRED / DEAD-AS-WIRED |
| storage `commit_slots`, `commit_max_events`, `commit_max_latency_ms` | 1, 64, 4000 | loaded and logged only (`crates/storage-core/src/boot.rs:606-609`) | DEAD-AS-WIRED / ABSENT |
| chain `safe_slots_to_import_optimistically` | 128 (`config/chain.toml:55`) | loaded and logged, not applied (`services/chain/src/run.rs:349`) | DEAD-AS-WIRED / ABSENT |

`engine_uri` and `p2p_uri` keep `scripts/check-compose-uri-overrides.sh` green; its header and
`docs/running.md:647-650` still call them live.

## Trust boundaries

```text
 OFF-HOST (LAN / internet)
   reaches el :8545 (unauthenticated eth namespace), :6060, :30303 tcp/udp
   reaches none of 9001-9006, 9101-9106, 8551, 9000 (none is published off-host)

 HOST LOOPBACK 127.0.0.1
   operator ---- GET /metrics, no auth, no TLS ----> 127.0.0.1:9101 .. 127.0.0.1:9106
   ./secrets/jwt.hex lives on the host disk; bind-mounted read-only, never COPY'd into an image

 +============================================================================================+
 | COMPOSE NETWORK "cc": no credential on any CC-to-CC edge                                   |
 |                                                                                            |
 | chain  p2p  attestation  engine  beacon-api  storage    gRPC h2c on 0.0.0.0:900N           |
 |   any container on cc (el included) can call any served RPC, e.g. ApplyAttestations        |
 |   health probes ----> each [peers] entry's grpc.health.v1 Check (N3), plaintext            |
 |   metrics on 0.0.0.0:910N are reachable from every container on cc                         |
 |                                                                                            |
 |                                                      +-------------------------------+     |
 |   chain  -- Bearer JWT HS256 {iat}, per request ---->| el authrpc :8551              |     |
 |   engine -- same secret, 2nd independent client ---->| --authrpc.vhosts=el           |     |
 |                                                      | jwtsecret /jwt/jwt.hex (ro)   |     |
 |                                                      +-------------------------------+     |
 +============================================================================================+
 OUTBOUND (Docker default egress; network "cc" is not declared internal)
   p2p ----> bootnodes and peers, libp2p TCP + discv5 UDP (Noise XX; not a credential)
   chain ----> checkpoint_providers over HTTPS (default list empty)
   el ----> devp2p peers
```

`---->` is a request toward its server. `====` borders enclose a trust zone, and `-` borders
enclose a process.

| Boundary | Authenticated by | Exposure at HEAD, shape A | Shape B |
|---|---|---|---|
| CL -> EL Engine API (X1) | HS256 JWT with only `{iat}`, signed per request | authrpc `el:8551`, reachable on `cc` only. Three processes load the secret: chain, engine and el | beacon-core loads it. The EL is at `el_endpoint`, default `127.0.0.1:8551` |
| CC <-> CC gRPC | nothing: plaintext h2c, no token | any container on `cc` can call any served RPC. Not host-published | no internal edges. ChainService on `127.0.0.1:9001` |
| operator -> metrics (X3) | nothing: no auth, no TLS | `127.0.0.1:910N` on the host; `0.0.0.0:910N` inside `cc` | `127.0.0.1:9101` |
| off-host -> el | nothing on 8545 | 8545, 6060 and 30303 on all host interfaces. Docker's port publish inserts DNAT rules that bypass host `ufw` (`plan/architecture.md` §6.1) | n/a |
| node <-> network (X2) | Noise XX handshake and peer limits; not a credential | libp2p and discv5 on 9000, not published | no p2p |
| checkpoint sync, outbound (X4) | TLS: `checkpoint_providers` must be https; plain http only on loopback. Root of trust: see below | default list is empty | same |

- **Checkpoint root of trust (X4).** `checkpoint_root` is optional in both hosts
  (`services/chain/src/run.rs:170,433-442`; `bin/beacon-core/src/boot.rs:181,486-490`) and set in
  neither shipped config (`config/chain.toml:31` is commented out; `config/beacon-core.toml` has
  no such key). Unset, the first provider whose triple is self-consistent and matches the local
  spec keys becomes fork choice's root of trust. There is no multi-provider quorum
  (`services/chain/src/checkpoint_sync.rs:14-25`), and the only signal is the warn log
  "checkpoint root not operator-supplied; trusting provider" (`:1058`). GVR is not among the
  cross-checked spec keys (`:286-294`, `:345-428`); it is compared only between the provider's
  own genesis response and its state (`:1030`), never with a local value. Treat
  `checkpoint_providers` as privileged config, like the network YAML. Protocol detail:
  [12-boot-health-and-shutdown.md](12-boot-health-and-shutdown.md).
- **JWT secret discipline** (`crates/engine-api/src/jwt.rs:179-232`): hex for exactly 32 bytes,
  a regular file of at most 4096 bytes, mode exactly 0600, no `..` in the path; loaded before
  bind, and a failure aborts before bind. The host file `./secrets/jwt.hex` is bind-mounted
  `:ro`, and `.dockerignore` keeps it out of every image. The shape A prerequisite and the
  uid 10001 readability hazard are under [Volumes](#volumes-and-the-backup-unit).
- **[el-runbook.md](../el-runbook.md) is out of date** in two places. Its crc32 check expects
  matching `el` and `engine` lines, but as wired no CC process would print one: `JwtSecret::load`
  logs the crc32 (`crates/engine-api/src/jwt.rs:226-230`) before `cc_bootstrap::init` installs
  tracing (`services/chain/src/run.rs:316-319`, `services/engine/src/main.rs:94-96`,
  `bin/beacon-core/src/boot.rs:400-419`), so only el's line would appear (traced, not observed).
  Its AuthFailed recovery restarts `engine`, but the live health machine is in chain (A) or
  beacon-core (B).
- **What an unauthenticated caller on `cc` can do.** `ApplyAttestations` applies votes with no
  signature or committee check (`crates/chain-core/src/service.rs:257`,
  [contracts.md](../contracts.md)), so once a core is installed any container on `cc`, the
  third-party `el` image included, can move fork-choice weight. p2p's `EngineStream` server runs
  with `AuthMode::Unauthenticated` (`services/p2p/src/engine_stream/inject.rs:431`). `RestoreFromStore`
  (E4), the state-install RPC, was deleted in 60c6200; the rest still carries no credential.
- **Build-time JWT invariant.** [ADR-R-03](../adr/ADR-R-03.md) moves JWT isolation from a process
  to an API surface: only `cc-engine-api` may declare an HTTP client or JWT signer. At HEAD
  `scripts/check-crate-dag.sh:45-67` still also allows the TRANSITIONAL `cc-engine`, which
  re-exposes the signer as `pub mod jwt` (`services/engine/src/lib.rs:26-27`); `cc-chain` and
  `cc-bootstrap` are grandfathered for HTTP only.

## The engine-blackhole overlay

`docker-compose.engine-blackhole.yml` is opt-in and is never merged automatically. Run it with:

```bash
docker compose -f docker-compose.yml -f docker-compose.engine-blackhole.yml \
  up -d --build --no-deps engine
```

- It rebuilds `engine` with `SERVICE=cc-engine-blackhole`, command `0.0.0.0:9004`,
  `restart: "no"`, healthcheck disabled and `depends_on: {}` (`:15-26`). The binary
  (`services/engine/src/bin/blackhole.rs:61-74`) is a bare tonic EngineService that never
  answers; it has no cc-config, health service or metrics.
- **Status: TRANSITIONAL, and it cannot park chain-core.** chain no longer dials `engine:9004`:
  E3 was deleted in 631994c and chain calls `http://el:8551` in-process. The overlay header
  (`:7-10`) and `blackhole.rs:3-5` still describe the old path. No service lists `engine` in
  `[peers]`, so no other aggregate changes either. The real demonstration is
  `services/chain/tests/engine_blackhole_liveness.rs`, which puts a sink in place of the EL HTTP
  endpoint ([14-testing-and-enforcement.md](14-testing-and-enforcement.md)).

## Offline tools

The builder compiles these (`--workspace`) but stages none in `/out`: `cc-store` (package
`cc-store-tool`, `bin/cc-store`; not the `cc-store` crate) to `verify`, `compact` and `dump` a
store the node is not running, with no repair command; `cc-devnet-gen` (self-devnet genesis);
`cc-serve-probe` ([ADR-P4-12](../adr/ADR-P4-12.md)); `cc-store-bench`; and `gen_hostile_corpus`
(in `cc-p2p`). Purposes and dependencies: [02-crate-map.md](02-crate-map.md).

## Planned changes

All of the following is target design from [`plan/architecture.md`](../../plan/architecture.md).
None of it is built at HEAD unless stated.

- **Two processes after S2** (§0, §9.1 S2). Planned: p2p plus `beacon-core` (chain, storage and
  the EL bridge, one redb handle), with E1/E2/E8 as typed handles whose transport S3 selects. At
  HEAD E2.2 is open and compose is unchanged; S1's "Ships 3 containers" (§9.1) is not reflected.
- **Shim removal** (§9.1 S2). Planned: delete the `services/chain` and `services/storage` shims
  at the end of S2; `services/engine` survives only as the A/B constructor.
- **S5** (§9.1). Planned: delete `services/attestation` and `services/beacon-api`; add a REST
  beacon API, the BN-VC line, a remote signer and `crates/slashing-protection`
  ([ADR-R-05](../adr/ADR-R-05.md)).
- **Trust boundaries** (§6.3, §6.4). Planned: the beacon-core <-> p2p boundary is decided at S3;
  if a unix socket, it is guarded by an `SO_PEERCRED` uid/gid check and mode 0600, not a token.
  Operator metrics stay on `127.0.0.1`. The §6.1 table (all bus and metrics ports on `0.0.0.0`)
  describes baseline `4146791`; at HEAD gRPC is unpublished and metrics are loopback-only.
- **JWT allowance** (§6.2, [ADR-R-03](../adr/ADR-R-03.md)). Planned: drop the transitional
  `cc-engine` HTTP/JWT allowance in `scripts/check-crate-dag.sh`.
- **Restart on liveness** (§7.2). Planned: compose restarts a beacon-core that reports
  NOT_SERVING. At HEAD beacon-core is not in compose and `unless-stopped` ignores the Docker healthcheck.
