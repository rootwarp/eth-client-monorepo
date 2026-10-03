# Boot, health and shutdown

> **As built at `7d8833d`** (`develop`, 2026-08-16). Status labels and diagram legend: [README](README.md#status-labels).
> Target design: [`plan/architecture.md`](../../plan/architecture.md) §4.2, §7.

This page covers the life of a process: what runs before the gRPC port is bound, what aggregate
health means and who probes whom, how a parked core thread turns aggregate health red, and what
SIGTERM does within the compose grace period. Ports, volumes, `depends_on` and the restart
policy are in [01-deployment-and-processes.md](01-deployment-and-processes.md). The core
thread and its scheduler lanes are in [04-chain-core.md](04-chain-core.md). Every thread and
task, with its stop path, is in
[13-concurrency-model.md](13-concurrency-model.md#31-inventory-lifetimes-and-stop-paths).
Operator procedures are in [running.md](../running.md) and [s2-rollback.md](../s2-rollback.md).

Every CC service process uses one two-phase host from `cc-bootstrap`: `init` (tracing and the
metrics registry), then `serve_with_options` (tonic gRPC + `grpc.health.v1` + reflection + peer
probers + `/metrics` + SIGTERM drain). Per-process code only decides what happens before bind,
what the local-ready bit means, and what the pre-drain hook does. The exception is cc-p2p devnet
mode (DEVNET-ONLY): it calls `init` and runs its own metrics server, but never
`serve_with_options` (`services/p2p/src/main.rs:480-481,533-543`).

## 1. Status at a glance

"Default" means `checkpoint_providers = []` in both shapes, so no core is installed. "iff core"
means an operator set providers and a core was installed.

| Mechanism | Code | Shape A | Shape B |
|---|---|---|---|
| Bootstrap host (`init`, `serve_with_options`) | `crates/bootstrap/src/{lib,serve}.rs` | LIVE (six processes) | LIVE |
| Fail-before-bind gates (config, JWT, KZG, identity, store open) | `crates/config/src/lib.rs:184-195`, `crates/engine-api/src/api.rs:111-157`, `services/p2p/src/identity.rs`, `crates/storage-core/src/open.rs:209` | LIVE | LIVE (no dangerous-knob guard) |
| Checkpoint sync at boot | `services/chain/src/checkpoint_sync.rs` | LIVE (opt-in; boot task, runs alongside the bind; default `checkpoint_providers = []`) | LIVE (opt-in, runs before bind, only on an empty durable set) |
| `seed_from_durable` | `crates/chain-core/src/seed.rs:383` | ABSENT | IDLE (the durable set is always `None`) |
| `BootPhase` recording (`boot_in_process`) | `bin/beacon-core/src/boot.rs:60-150` | ABSENT | TEST-ONLY |
| Peer probers (edge N3) | `crates/bootstrap/src/prober.rs` | LIVE | ABSENT (`[peers]` is empty) |
| Local-ready gate | `crates/bootstrap/src/serve.rs:86-99` | INERT by default (marked ready at once, `services/chain/src/run.rs:421`); LIVE (iff core; the sampler drives it) | same (`bin/beacon-core/src/boot.rs:540`) |
| Liveness sampler | `crates/chain-core/src/liveness.rs` | LIVE (iff core; not started core-absent, the default) | same |
| Docker healthcheck and `depends_on` | `docker-compose.yml` | LIVE | ABSENT (no compose service) |
| Restart on unhealthy | -- | ABSENT | ABSENT |
| SIGTERM drain | `crates/bootstrap/src/serve.rs:383-404` | LIVE | LIVE |
| Storage shutdown watch | `crates/storage-core/src/boot.rs:429-436`; shape B `crates/storage-core/src/open.rs:202-205` | LIVE (storage service pre-drain) | DEAD-AS-WIRED (`StorageRuntime::shutdown` is never called) |
| `/metrics` endpoint | `crates/bootstrap/src/metrics_server.rs` | LIVE | LIVE |

## 2. The bootstrap host

### 2.1 `init`

`cc_bootstrap::init(service, telemetry)` (`crates/bootstrap/src/lib.rs:167-170`):

- installs the global `tracing` subscriber: JSON or pretty, to stdout, filtered by `log_filter`.
  It is the only subscriber install in the service binaries (`crates/bootstrap/src/lib.rs:214-243`),
  so any event emitted before `init` is dropped (section 3);
- opens a root `service` span; `cc_bootstrap::spawn(name, fut)` is `tokio::spawn` with a child
  `task{name}` span and **no** panic capture (`crates/bootstrap/src/lib.rs:201-212`);
- builds the Prometheus registry with the shared families (section 6).

`init` never reads the environment. The config loader resolves the telemetry settings first
([ADR-12](../adr/ADR-12.md)). The crate pins are in [ADR-06](../adr/ADR-06.md).

### 2.2 `serve_with_options`

One call does all of the following (`crates/bootstrap/src/serve.rs:203-380`):

1. Sets the self-name (`eth.X.v1.XService`) to SERVING, and sets the aggregate `""` from the
   initial local-ready bit and the peer set (`:241-258`).
2. Sends the `LocalReadyHandle` to the caller, if it asked for one (`:262-266`).
3. Spawns the `shutdown-signal` task (`:274`). That task installs the SIGTERM/SIGINT handlers
   when it first runs (`:407-437`), so they are installed concurrently with the bind.
4. Adds health and reflection v1 to the user routes (`:312`), then spawns `metrics-http`
   (`:316`) and one `peer-prober` per `[peers]` entry (`:323`). A `/metrics` bind failure is not
   fatal: `metrics-http` logs `metrics server exited` at warn and the process keeps serving gRPC
   without `/metrics` (`:316-320`, `crates/bootstrap/src/metrics_server.rs:27-29`).
5. Binds tonic with `serve_with_shutdown` (`:338`). No gRPC port is bound before this step.
   `/metrics` binds in its own task from step 4 and may come up a moment earlier.

`ServeOptions` (`crates/bootstrap/src/serve.rs:86-99`) is how each host customizes this:

| Process | Entry | `require_local_ready` | Pre-drain hook | `[peers]` |
|---|---|---|---|---|
| chain | `serve_with_options` (`services/chain/src/run.rs:527`) | true | seal installs, stop the sampler, `shutdown_and_join` the core thread | none |
| beacon-core | `serve_with_options` (`bin/beacon-core/src/boot.rs:568`) | true | the same; the storage-core writer is not stopped | none |
| storage | `serve_with_options` (`crates/storage-core/src/boot.rs:650`) | false | send `true` on the storage watch | chain |
| p2p | `serve_with_options` inside the `grpc-serve` task (`services/p2p/src/service.rs:908-909`) | false | none | chain, storage |
| engine, attestation, beacon-api | `cc_bootstrap::serve` (`services/engine/src/main.rs:110`, `services/attestation/src/main.rs:83`, `services/beacon-api/src/main.rs:81`) | false | none | chain; beacon-api: chain, attestation, storage |

## 3. Boot, per process

Each process runs its refusals before the gRPC bind, so a bad config, JWT, key or store never
leaves the gRPC port open. There is one exception. Shape A chain checks `checkpoint_root` and the
checkpoint providers inside its boot task, which runs alongside `serve_with_options`, and calls
`exit(1)` on failure, possibly after the port is open (`services/chain/src/run.rs:433-439,490-493`).
A `/metrics` bind failure only logs a warning (section 2.2). A missing required key is refused
with the key, the file and its `CC_<SVC>_*` variable (`crates/config/src/lib.rs:184-195`); other
config errors surface figment's own message.

| Process | Before bind, in order | Alongside or after bind |
|---|---|---|
| chain | config, dangerous-knob guard, network YAML, `EngineApi::prepare_with_chain_config` (JWT, `[el_forks]`, KZG), `init`, metrics, events task, `finish(None)` (starts the EL upcheck and fastpath tasks), service with no core installed (`services/chain/src/run.rs:309-387`) | boot task: mark ready; optional checkpoint sync; install the core; start the sampler |
| storage | config, dangerous-knob guard, `init`, metrics; if `enable_write_path` (default true; else serve `StorageServer::stub`): `open` (lock, schema, digest, invariants, I-node-id, refuses a populated store whose key file is missing), resume (always "empty"), writer, ArchiveWriter built and dropped, migrator, replay task, prune config (refuses a missing `network_config` or a zero block floor unless `retention_override` is set), prune task; server (`crates/storage-core/src/boot.rs:471-648`, `crates/storage-core/src/open.rs:234-235`) | serve |
| p2p | CLI, config, `identity::load_or_create` (refuses a key whose mode is not exactly 0600, `services/p2p/src/identity.rs:268-270`), `init`, metrics, EngineStream attach (`services/p2p/src/main.rs:486-511`); then `p2p-runtime` starts and the host waits up to 10 s for its ready signal (an `IdentitySnapshot` sent once the swarm is built, `services/p2p/src/service.rs:625`; [09 §2.1](09-p2p-host-and-discovery.md#21-startup-order-servicesp2psrcmainrs463-531)) | serve |
| engine | config (`network_config` required), `EngineApi::prepare` (also loads and sandboxes `network_config`), `init`, `finish(Some(metrics))` (`services/engine/src/main.rs:93-101`) | serve |
| attestation, beacon-api | config, `init` (`services/attestation/src/main.rs:80-81`, `services/beacon-api/src/main.rs:78-79`) | serve the GetInfo STUB |
| beacon-core | see 3.2: config through seeding, all before bind | ready task: mark ready; start the sampler if a core is installed |

Two ordering details matter to operators:

- **Events before `init` are dropped.** The storage service calls `init` before it opens the store
  (`crates/storage-core/src/boot.rs:477,506`). beacon-core opens the store and loads the durable
  set before `init` (`bin/beacon-core/src/boot.rs:416-419`). chain, the engine container and
  beacon-core all run `EngineApi::prepare*` before `init`, so as wired the `Loaded JWT secret
  file ... crc32=` line (`crates/engine-api/src/jwt.rs:226-230`) that
  [el-runbook.md](../el-runbook.md) tells operators to grep is never emitted by our processes.
  The `load_network` fallback warning in chain and beacon-core is lost the same way. A refusal
  still ends `run()` with the `anyhow` error that `main` prints.
- **p2p binds gRPC only after its `p2p-runtime` task reports ready** (`services/p2p/src/service.rs:874-910`). Its startup and
  supervisor are in [09-p2p-host-and-discovery.md](09-p2p-host-and-discovery.md#2-process-layout).

### 3.1 Shape A: cc-chain binds first, then bootstraps

```text
 cc-chain, shape A (services/chain/src/run.rs:307-536)
 main task                                       boot task (tokio::spawn, run.rs:408)
 ---------------------------------------------   -------------------------------------------
 1 load chain.toml + CC_CHAIN_*          (:309)
 2 dangerous-knob guard; network YAML
                                     (:311-314)
 3 EngineApi::prepare_with_chain_config:
     JWT, [el_forks], KZG            (:316-318)
 4 init; ChainMetrics; events task   (:319-336)
 5 finish(None): EL upcheck + fastpath   (:362)
 6 ChainServiceImpl, no core installed   (:381)
     any error in 1-6: run() returns Err,
     no port bound
 7 spawn boot task --------------------------->  wait for the LocalReadyHandle
 8 serve_with_options(require_local_ready):
     self-name SERVING, aggregate NOT_SERVING
     send LocalReadyHandle (before bind) ----->  mark_ready(): aggregate SERVING      (:421)
     bind :9001; /metrics :9101 (own task)       (races the bind; first seen at bind)
                                                 no providers -> return; core-absent (default)
                                                 bad checkpoint_root -> exit(1)   (:433-439)
                                                 checkpoint sync over HTTPS; no provider
                                                   succeeds -> exit(1)            (:490-493)
                                                 install core, start liveness sampler;
                                                 the sampler owns local_ready from here
```

Here `---->` is an in-process hand-off between two tasks, not an RPC. The handle is sent before
the bind, so `mark_ready()` may run first, but health is observable only from the bind.

With the compose defaults there are no providers and no core is installed. RPCs that need an
installed core answer `FAILED_PRECONDITION` / `NOT_BOOTSTRAPPED`
(`crates/chain-core/src/service.rs:52`) while aggregate health is SERVING, so **"chain healthy"
means bound, not bootstrapped**. With providers set, sync runs while chain already reports
SERVING. A rejected provider URL is warned and skipped
(`services/chain/src/checkpoint_sync.rs:879-886`); only "no provider succeeds" exits, normally
after bind. The protocol is in section 3.3; what an installed core does next is in
[05-block-lifecycle.md](05-block-lifecycle.md#other-entry-points).

### 3.2 Shape B: cc-beacon-core seeds before bind

The plan's boot (`plan/architecture.md` §4.2) is open, then durable set, then
`seed_from_durable` or checkpoint sync. `bin/beacon-core` builds exactly that order, as plain
function calls in one process:

```text
 cc-beacon-core, shape B (bin/beacon-core/src/boot.rs:397-577); steps 1-9 run before bind
  1 load beacon-core.toml + CC_BEACON_CORE_*; network YAML                   (:398-399)
    (no dangerous-knob guard)
  2 prepare_with_chain_config: JWT, [el_forks], KZG                          (:400-402)
  3 ensure_node_key: read 32 B, or create a random key with umask perms      (:406)
  4 [Open]        storage_core::open: lock, schema, digest, invariants,      (:416)
                  I-node-id, missing-key refusal; then stamp meta.node_id
  5 [DurableSet]  durable_set() -> Option<DurableSet>; None at HEAD          (:417)
  6 init (tracing starts here); chain-core + storage-core metric families    (:419-421)
  7 [Writer]      start_writer (P0 32 / P1 64 / P2 256) -> archive handle    (:423-428)
  8 events task; finish(None): EL upcheck + fastpath; CoreConfig.archive     (:430-450)
  9 [Chain]       seed decision                                              (:461-528)
                    Some(durable)      -> seed_from_durable, blocking pool   IDLE
                    None + providers   -> checkpoint sync over HTTPS         LIVE (opt-in)
                    None, no providers -> no core installed                  core-absent (default)
                    any error          -> run() returns Err; nothing bound
 10 serve_with_options binds :9001 / :9101; a ready task calls mark_ready()  (:535-575)
    and starts the liveness sampler if a core was installed
```

`[Name]` marks the step that `BootPhase::Name` covers (`bin/beacon-core/src/boot.rs:60-70`); it
is not a legend `[A]`/`[B]` shape marker. With the timeouts and retries of section 3.3, shape B
can wait minutes before it binds.

- **Phase names.** Only `boot_in_process`, a TEST-ONLY entry point, records phases, and it
  stops at `Writer`; `BootPhase::Chain` is never recorded. The proto-free twin
  `crates/beacon-inproc/src/boot.rs` is in
  [14-testing-and-enforcement.md](14-testing-and-enforcement.md#in-process-boot-harness).
- **Seed.** `seed_from_durable` replays the stored blocks through the real STF (newPayload); a
  head other than the expected one is fatal (`crates/chain-core/src/seed.rs:404-411`). It is
  IDLE because `durable_set()` always returns `None`
  ([07-storage.md](07-storage.md#45-durable-set-resume-and-seed)), so every boot is checkpoint
  sync or core-absent. As wired, a replay before the first EL upcheck would abort boot
  ([08-execution-engine.md](08-execution-engine.md#45-shape-b-seed-replay)). Shape B does not
  yet bound its store or order EL readiness before seed replay.
- **Node key.** beacon-core creates a missing node key without enforcing 0600
  (`bin/beacon-core/src/boot.rs:290-316`). cc-p2p refuses a key whose mode is not exactly 0600
  (`services/p2p/src/identity.rs:268-270`), so a key that beacon-core creates under a default
  umask would be refused. I-node-id is [ADR-P4-13](../adr/ADR-P4-13.md).
- **No dangerous-knob guard.** Unlike chain (`services/chain/src/run.rs:311-312`) and the
  storage service (`crates/storage-core/src/boot.rs:475-476`), beacon-core never calls
  `check_dangerous_knobs`, so a shrunk `event_ring_bytes` is accepted without the devnet GVR
  check (`bin/beacon-core/src/boot.rs:430-435`).
- **Failure.** Every boot failure happens before bind and makes `run()` return `Err`. After
  bind, a writer panic exits 1 and a KeyCollision aborts (section 5.3).

With defaults (`config/beacon-core.toml:23`, `checkpoint_providers = []`), shape B binds with
no core, exactly like shape A chain. No shipped configuration pairs cc-p2p with
cc-beacon-core; how p2p attaches to shape B is not yet defined.

### 3.3 Checkpoint sync protocol

Both shapes call `bootstrap_core_from_providers_with_epoch`
(`services/chain/src/checkpoint_sync.rs:1210-1235`; beacon-core imports it from `cc_chain`):
fetch and verify an anchor, then build a core thread on it. Every request has a 5 s connect and
a 180 s total timeout (`:57-60`). Line references below are in `checkpoint_sync.rs` unless
another file is named.

```text
 fetch_checkpoint: for each checkpoint_providers entry, in config order               (:868-911)
   validate_provider_base: https, or http to a loopback host; else warn, skip         (:704-737)
   up to 3 tries of steps 1-5 on a transport, HTTP or decode error, backoff
   200 ms then 400 ms; a verification failure is never retried              (:913-945, :162-170)
     1 GET /eth/v1/beacon/genesis; JSON body <= 2 MiB                                (:547, :78)
     2 GET /eth/v1/config/spec -> cross_check_spec: 7 fork versions,
         FULU_FORK_EPOCH, SECONDS_PER_SLOT, BLOB_SCHEDULE
         any difference -> ConfigMismatch                                             (:345-428)
     3 GET the finalized block, SSZ <= 8 MiB; Eth-Consensus-Version other than
         fulu -> UnsupportedFork (a missing header is warned, fulu assumed)      (:72, :760-780)
     4 GET the state by block.state_root, SSZ <= 400 MiB; on HTTP 400, 404, 405
         or 500-503, GET the "finalized" alias instead               (:75, :739-758, :1007-1021)
         genesis endpoint vs the state's genesis_time / GVR: warn only              (:1029-1040)
     5 verify, in this order:
         StateRootMismatch: block and state re-fetched once (finalization race)
                                                                          (:962-972, :1042-1050)
         CheckpointRootMismatch: only if checkpoint_root is set                       (:243-283)
         AnchorSlotNotEpochAligned: block slot != state slot, or not epoch-aligned
         no checkpoint_root -> warn "... trusting provider"                              (:1058)
   the first provider to pass wins
 6 warm canonical_root; get_forkchoice_store; on_tick to wall clock;
   spawn the core thread                                                            (:1078-1170)
 every provider fails -> AllProvidersFailed; step 6 can fail with Store                   (:910)
 on either error:
   shape A: the boot task calls exit(1), normally after bind     (services/chain/src/run.rs:492)
   shape B: run() returns Err; nothing is bound
```

Each provider outcome, a rejected URL included, increments
`cc_chain_bootstrap_attempts_total{provider,result}` once (`:880-900`). **Root of trust.**
Without `checkpoint_root` (unset in both shipped configs, `config/chain.toml:31`), the anchor
is whatever the first passing provider serves: TLS authenticates the server, not the
checkpoint, and the other checks only show that block, state and spec agree with each other and
with the compiled network config. See X4 in
[01-deployment-and-processes.md](01-deployment-and-processes.md#trust-boundaries).

## 4. Health model

<a id="41-three-names-one-bit"></a>

### 4.1 Two health names and the local_ready bit

| Name | Meaning | Set by |
|---|---|---|
| self-name, e.g. `eth.chain.v1.ChainService` | "bound and my RPCs are servable". SERVING from just before bind; drain does not change it | `crates/bootstrap/src/serve.rs:241` |
| aggregate `""` | SERVING iff every `[peers]` entry is up **and** `local_ready` is true. Forced NOT_SERVING once draining starts, and never recomputed after that | `crates/bootstrap/src/prober.rs:91-120`, `crates/bootstrap/src/serve.rs:392-396` |
| `local_ready` (a bit, not a name) | readiness bit ANDed into the aggregate. Starts as `!require_local_ready` | `LocalReadyHandle` (`crates/bootstrap/src/serve.rs:108-129`); the liveness sampler |

Every process starts with aggregate NOT_SERVING. Peers start "down"
(`crates/bootstrap/src/prober.rs:51-54`), and chain and beacon-core start with
`local_ready = false`. The compose healthcheck, `grpc-health-probe -addr=:900N` with no
`-service`, reads the **aggregate** (`docker-compose.yml:46`). So does every peer prober
(`crates/bootstrap/src/prober.rs:172`). Only an operator running
`grpc-health-probe -service eth.X.v1.XService` sees the self-name.

### 4.2 Peer prober timing

One `peer-prober` task runs per `[peers]` entry, over a lazily connected channel, so startup
order cannot deadlock (`crates/bootstrap/src/prober.rs:161`). Constants are at
`crates/bootstrap/src/prober.rs:23-27`.

| Step | Value |
|---|---|
| Request | `grpc.health.v1.Health/Check{service: ""}`: the peer's aggregate, not its self-name |
| Cadence | probe, then sleep `PROBE_INTERVAL` = 3 s |
| Timeout | `PROBE_TIMEOUT` = 1 s per probe |
| Mark down | 2 consecutive failures (`FAIL_THRESHOLD`) |
| Mark up | 1 success |
| Effect | `cc_peer_health{service,peer}`, then recompute the aggregate. Never exits the process (`crates/bootstrap/src/prober.rs:1-5`) |

As wired, a peer that refuses connections or answers NOT_SERVING is marked down after 3-6 s.
A peer that hangs takes up to about 8 s, because each probe can also use its 1 s timeout.
Recovery takes one probe, so up to about 4 s. Recovery is cheaper here than in the liveness
sampler, which needs three good samples.

### 4.3 Health DAG

```text
 +--------------+       +---------------+       +-------------+
 | beacon-api   |------>| attestation   |------>| chain :9001 |
 | :9005        |       | :9003         |       |             |
 |              |       +---------------+       | DAG root:   |
 |              |------------------------------>| [peers] is  |
 |              |       +---------------+       | empty       |
 |              |------>| storage :9006 |------>|             |
 +--------------+       |               |       | aggregate = |
                        |               |       | local_ready |
 +--------------+       |               |       | only        |
 | p2p :9002    |------>|               |       |             |
 |              |       +---------------+       |             |
 |              |------------------------------>|             |
 +--------------+                               |             |
                                                |             |
 +--------------+                               |             |
 | engine :9004 |------------------------------>|             |
 +--------------+                               +-------------+
```

`---->` is edge N3, LIVE: the box at the tail probes the aggregate `""` of the box at the head.
No process probes `el`. Shape B has no peers, so it has no DAG.

- **Root.** The DAG roots at chain, which has no peers ([ADR-07](../adr/ADR-07.md)). chain's
  aggregate is its `local_ready` bit and nothing else.
- **Hops.** Every dependent lists chain directly, so all five see a chain flip in one hop. A
  flip also reaches p2p and beacon-api a second time through the storage service (beacon-api:
  and attestation).
- **Engine container and the EL.** The engine container is a leaf of the DAG: it probes chain,
  but no process lists it in `[peers]`, and nothing probes the EL.
  [ADR-P3-02](../adr/ADR-P3-02.md) kept the engine container from being any health peer. Its header
  marks it superseded by [ADR-R-04](../adr/ADR-R-04.md), but ADR-R-04 says it does not supersede
  that half and leaves it to issue S1-B-12. Whether chain (or beacon-core) should probe the
  engine container or the EL is open. cc-engine remains in compose for A/B comparison and has
  no in-tree caller. chain's EL health machine does not feed gRPC health
  ([08-execution-engine.md](08-execution-engine.md#35-el-health-state-machine)), so green says
  nothing about the EL: after a 401/403 the EL client is AuthFailed for the life of the process
  (no production reset) while aggregate health stays SERVING. Recovery is in
  [el-runbook.md](../el-runbook.md#authfailed-recovery).
- **Config edge.** As wired, an environment variable would not remove a `[peers]` entry that a
  TOML file sets (figment merges dicts; inferred, not tested), so that peer would stay an edge.
- **Proof.** `scripts/prove-mutual-health.sh` (`make prove-health`) exercises the DAG. It stops
  chain, expects all five dependents NOT_SERVING within 15 s, then restarts chain and expects
  all six SERVING within 30 s (`scripts/prove-mutual-health.sh:28-29`). See
  [running.md](../running.md#mutual-health-proof).

**Compose startup gating.** `depends_on` gates startup only and is separate from the probe edges
([table](01-deployment-and-processes.md#startup-order-and-health-gating)). Because chain marks
itself ready alongside the bind, its dependents start whether or not a core is installed.

<a id="44-liveness-sampler-a-no-op-through-the-core"></a>

### 4.4 Liveness sampler: a no-op through the core thread

The health service answers from a tokio task, but the core thread is a separate OS thread. A
core thread parked in a long `block_on` would otherwise leave aggregate health green.
[ADR-R-04](../adr/ADR-R-04.md) closes that gap with a deadline-bounded no-op that must pass
through the core thread.

| Property | As built | Code |
|---|---|---|
| Sample | `CoreHandle::ping` pushes `TickWork::Ping` on the never-shed tick scheduler lane; the core thread replies at once | `crates/chain-core/src/core.rs:1104-1122`, `:1856-1859` |
| Per-sample deadline | `3333 * slot_ms / 10000` = 3999.6 ms on 12 s slots ([ADR-P3-13](../adr/ADR-P3-13.md)) | `crates/chain-core/src/liveness.rs:66-74` |
| Cadence | sample, then sleep `slot_ms / 4` = 3 s | `crates/chain-core/src/liveness.rs:99,119-121` |
| Flip to NOT_SERVING | 3 consecutive misses. A deadline or `Unavailable` (core thread gone) counts as a miss | `crates/chain-core/src/liveness.rs:112`, `:183-205` |
| Back to SERVING | 3 consecutive successes | `crates/chain-core/src/liveness.rs:115` |
| Inputs | compiled `DEFAULT_ATTESTATION_DUE_BPS` and the network `seconds_per_slot`; the `attestation_due_bps` config key is not read by the sampler (only engine-api's soft-deadline counter reads it, `crates/engine-api/src/config.rs:224-225`) | `services/chain/src/run.rs:117-123`, `bin/beacon-core/src/boot.rs:541-543` |
| Lifetime | starts SERVING after a core is installed; cancelled by the pre-drain hook | `services/chain/src/run.rs:74-112` |

```text
 core thread parks (Ping waits behind the command that is running)
 t =  0 s   first sample after the park (up to 3 s after it); no reply within 4.0 s -> miss 1
 t =  7 s   sample 2 starts; no reply within 4.0 s -> miss 2; sleep 3 s
 t = 14 s   sample 3 starts; no reply within 4.0 s -> miss 3
 t ~ 18 s   local_ready = false; chain aggregate NOT_SERVING; cc_core_liveness_parked = 1
 + 3-6 s    each dependent's prober sees 2 failed Checks; its own aggregate goes NOT_SERVING
 recovery   3 on-time samples in a row (about 3 s apart) -> local_ready = true
 + <= 4 s   per hop: p2p and beacon-api also wait for the storage service
            (and attestation) to see chain up
```

Park to flip is therefore about 18-21 s. N = 3 is sized so that one 8 s newPayload or fcU on the
core thread, or two back to back, cannot flip aggregate health. When core-absent, nothing is
parked: the sampler never starts.

**What happens on red: nothing restarts.** Liveness flips aggregate health; nothing in this
repository restarts a process on that signal. `restart: unless-stopped`
(`docker-compose.yml:13`) acts on process exit only, and the repo has no autoheal or
orchestrator. A parked chain turns the whole DAG red and stays up until an operator acts. The
in-repo demonstration of a red sampler is `services/chain/tests/engine_blackhole_liveness.rs`;
the compose blackhole overlay replaces `engine:9004`, which chain no longer dials, so it cannot
park the core thread.

## 5. Shutdown

### 5.1 Sequence

```text
 SIGTERM (compose stop_signal), every CC process: crates/bootstrap/src/serve.rs:383-404
 t = 0          shutdown-signal task: mark_draining(); aggregate "" -> NOT_SERVING
                (self-name stays SERVING; probers keep probing without recomputing
                until cancel at h + 0.15 s, then exit)
 t = 0 .. h     await the pre-drain hook, if any:
                  chain, beacon-core: seal installs, stop sampler, shutdown_and_join  h <= 2 s *
                    queued and later RPCs to the core thread -> UNAVAILABLE
                  storage service: send true on the storage watch                   h ~ 0
                  p2p, engine, attestation, beacon-api: no hook                     h = 0
 t = h + 0.15   notify pause ends; release tonic graceful shutdown; cancel = true
 t <= h + 3.15  drain in-flight RPCs; at DRAIN_TIMEOUT (3 s) warn and drop the server future
                then abort probers and metrics-http, flush stdout, return
 p2p only       then stop p2p-runtime; wait <= 2 s for the supervisor
                (services/p2p/src/service.rs:645)
 compose        SIGKILL at t = 5 s (stop_grace_period)
 * the 2 s clock starts before begin_shutdown, but its push_wait onto the tick scheduler lane
   has no deadline (crates/chain-core/src/core.rs:1184-1185, :416-427). As wired, after a
   core-thread panic the tick scheduler lane fills and never drains, so this wait never
   ends (5.3)
```

- **chain and beacon-core.** `CoreJoinOwner::take_for_shutdown` seals installs and cancels the
  sampler. In chain only, a checkpoint sync that finishes during drain then joins its own core
  thread instead of installing it (`services/chain/src/run.rs:466-482`). When core-absent, the
  hook is a no-op.
- **`shutdown_and_join`.** It pushes `Shutdown` on the tick scheduler lane and waits for the done
  oneshot and the OS join under one 2 s envelope (`crates/chain-core/src/core.rs:1181-1224`). If
  the envelope is spent before the join starts, it drops the JoinHandle and detaches the thread
  (`:1196-1204`). If it expires during the join (the core thread acknowledged `Shutdown` but has
  not finished its teardown), the `spawn_blocking` join task keeps running, and as wired the
  runtime drop when `main` returns waits for it with no timeout (tokio's blocking pool). Not
  observed; traced only. `Shutdown` is selected ahead of queued imports and queries; on exit the
  core thread rejects every queued item, and every later RPC that needs it answers
  `UNAVAILABLE` "chain core thread is shut down" (`crates/chain-core/src/core.rs:487-499,1891`).
  So for chain, "drain in-flight RPCs" mostly drains RPCs that have already failed fast.
- **storage service.** The watch stops the writer, replay and prune tasks. The writer's biased
  select breaks without draining its mailbox (`crates/storage-core/src/writer.rs:444-474`).
  Operators idle the writer before signalling for that reason; the steps are in
  [s2-rollback.md](../s2-rollback.md#signal-path--compose-storage).
- **beacon-core storage-core writer.** The hook joins the core thread only.
  `StorageRuntime::shutdown` (`crates/storage-core/src/open.rs:203-205`) is never called. The
  writer is never told to stop and ends with the runtime when `run()` returns
  (`bin/beacon-core/src/boot.rs:530`). A possible writer spin after its watch sender drops is
  not observed; traced only. Operator steps:
  [host path](../s2-rollback.md#signal-path--host-cc-beacon-core).
- **Long-lived streams.** As wired, an open P2pStream session (chain) or WatchServeWindow
  stream (storage service) keeps tonic's graceful drain open until `DRAIN_TIMEOUT`. In compose,
  p2p holds both streams, so stopping chain or the storage service alone normally uses the full
  3 s drain. A full `docker compose stop` stops p2p before chain (p2p `depends_on` chain), but
  nothing orders p2p before the storage service. That does not change the worst case.
- **What dependents see.** The 150 ms pause is shorter than the 3 s probe cadence, so a
  dependent's next probe usually fails at the transport level rather than reading NOT_SERVING.
  Either way it marks the peer down within 3-6 s.

### 5.2 Budgets against `stop_grace_period: 5s`

All CC services inherit `stop_grace_period: 5s` (`docker-compose.yml:15`).

| Process | Pre-drain | Pause + drain | After serve | Worst case |
|---|---|---|---|---|
| chain, default (core-absent) | 0 | <= 3.15 s | -- | 3.15 s |
| chain, installed core | <= 2 s, plus an unbounded `push_wait` | <= 3.15 s | as wired, the runtime drop may wait on the join task (5.1) | 5.15 s or more |
| p2p | 0 | <= 3.15 s | <= 2 s supervisor wait | 5.15 s |
| storage | ~0 | <= 3.15 s | -- | 3.15 s |
| engine, attestation, beacon-api | 0 | <= 3.15 s | -- | 3.15 s |
| beacon-core | <= 2 s, plus `push_wait` | <= 3.15 s | same as chain, installed core | 5.15 s or more; no compose grace applies |

Shutdown normally fits the 5 s grace period. The worst case exceeds it: 5.15 s for p2p, and
unbounded for chain or beacon-core with an installed core, because `push_wait` has no deadline
(`crates/chain-core/src/core.rs:416-427`). Past 5 s, compose sends SIGKILL. Queued writer units
are lost on SIGTERM too, because the writer does not drain its mailbox; SIGKILL additionally
aborts the unit being committed.
[s2-rollback.md](../s2-rollback.md) treats that as crash recovery, not rollback.

### 5.3 Exits that are not SIGTERM

| Trigger | Process | Effect |
|---|---|---|
| Any fail-before-bind refusal | all | non-zero exit, no gRPC port bound |
| gRPC bind failure (port in use) | all | `serve` returns `Err` after every gate has passed; non-zero exit. chain and beacon-core share the `127.0.0.1:9001` / `:9101` defaults (`config/chain.toml:4-5`, `config/beacon-core.toml:5-6`) |
| `/metrics` bind or accept failure | all | no exit; warn `metrics server exited` only (`crates/bootstrap/src/serve.rs:316-320`) |
| p2p runtime-ready timeout (10 s) | p2p | returns with no gRPC bind; the exit status follows the runtime task's result (`services/p2p/src/service.rs:896-905,955-967`) |
| Bad `checkpoint_root`; no checkpoint provider succeeds | chain | `exit(1)` from the boot task, normally after bind (`services/chain/src/run.rs:433-439,490-493`). The lost-handle `exit(1)` (`:409-414`) is unreachable: the handle is sent before bind |
| Seed or checkpoint error | beacon-core | `run()` returns `Err` before bind |
| swarm panic, or discovery over its respawn budget | p2p | aggregate NOT_SERVING via drain, then `exit(1)` (`services/p2p/src/main.rs:524-528`); [ADR-P2-13](../adr/ADR-P2-13.md) |
| Writer panic; KeyCollision | storage, beacon-core | `exit(1)` (`crates/storage-core/src/writer.rs:413-427`); `abort()` (`:752-771`) |
| Core thread or slot-tick thread spawn failure | chain, beacon-core | `abort()` (`crates/chain-core/src/core.rs:1317-1321`, `:1925-1928`) |
| Core-thread panic | chain, beacon-core | As wired, no exit: `close_consumer` runs only on a clean loop exit (`crates/chain-core/src/core.rs:1891`; release builds unwind, `Cargo.toml:222`). Pings time out, so liveness turns aggregate NOT_SERVING after about 18 s; queued RPCs never get a reply. Once the tick scheduler lane holds 4 items (within about 20 s), the pre-drain `push_wait` never returns and SIGTERM ends only with compose SIGKILL at 5 s (beacon-core: a manual SIGKILL). Not observed; traced only |
| Panic in an unsupervised task | all | As wired, no exit: tokio drops the panic. chain boot task: core-absent while aggregate health stays SERVING. Liveness sampler: `local_ready` frozen at its last value. Peer prober: that peer's state frozen. `shutdown-signal`: the drain trigger drops, so tonic drains with no 3 s cap and ends when connections close or at SIGKILL. Only the storage-core writer and the p2p supervisor turn a panic into an exit |

In shape A, `restart: unless-stopped` restarts the container after each exit above. beacon-core
has no container, so nothing restarts it. Restart policy and its limits are in
[01-deployment-and-processes.md](01-deployment-and-processes.md#restart-and-stop-policy).

## 6. Metrics and logging surfaces

### 6.1 Conventions

- **Prefix per owner.** `cc_chain_*` and `cc_core_liveness_*` (chain-core: chain, beacon-core),
  `cc_storage_*` (storage-core: the storage service; beacon-core registers them too), `cc_p2p_*`
  (p2p), `cc_engine_*` (engine-api; only the engine container exports them, see 6.2). Every
  process also exports the shared `cc_grpc_*`, `cc_peer_health` and `cc_build_info`
  (`crates/bootstrap/src/metrics.rs:63-117`).
- **Suffixes.** Names in this doc set are as exposed: prometheus-client appends `_total` to
  counters and the unit suffix to families registered with a unit, so `cc_grpc_requests` is
  scraped as `cc_grpc_requests_total` (`crates/bootstrap/tests/serve_integration.rs:430`).
- **Latency SLOs** follow [ADR-P1-15](../adr/ADR-P1-15.md). A budget bound must be an exact
  bucket edge (0.4 s in `PROCESS_BLOCK_BUCKETS`, 1.0 s in `PROCESS_EPOCH_BUCKETS`), and pass
  means `fraction(le=L) >= 0.95` between two scrapes, never `histogram_quantile`
  (`crates/chain-core/src/metrics.rs:1-13,38-65`). `scripts/soak-report.sh` computes it that
  way; `bash scripts/soak-report.sh --self-test` runs it on synthetic series.
- **Per-component inventories.** chain-core [04 §15](04-chain-core.md#15-metrics),
  storage-core and the storage service [07](07-storage.md), engine-api
  [08 §3.8](08-execution-engine.md#38-metrics), p2p host
  [09 §9](09-p2p-host-and-discovery.md#9-metrics), gossip and DAS
  [10 §13](10-p2p-gossip-and-das.md#13-concurrency-metrics-and-latent-defects), req/resp and
  sync [11](11-p2p-reqresp-and-sync.md#metrics-in-scope).
- **Registered is not live.** Every component registers families that production never sets,
  e.g. `cc_chain_process_epoch_seconds` (only the 0.0 seed sample,
  `crates/chain-core/src/metrics.rs:658`), `cc_storage_following_head`,
  `cc_engine_inject_stream_state`, and the `cc_p2p_libp2p_*` sub-registry, whose recorder is
  dropped at once (`services/p2p/src/metrics.rs:650-651`). Dashboards must read each
  inventory's status column, not assume that a series exists or moves.

### 6.2 Surfaces

| Surface | Where | Notes |
|---|---|---|
| Logs | stdout, JSON by default or pretty | Compose always sets `RUST_LOG` and `LOG_FORMAT` (`docker-compose.yml:8-10`), which win over TOML and `CC_<SVC>_*`. chain's boot task, both hosts' liveness sampler and beacon-core's ready task use plain `tokio::spawn` (`services/chain/src/run.rs:102,408`; `bin/beacon-core/src/boot.rs:389,535`), so they carry no `task{name}` span |
| `/metrics` | every process, `metrics_addr` (`:910N`) | hyper http1, `GET /metrics` only, 404 otherwise, no auth or TLS (`crates/bootstrap/src/metrics_server.rs:72-98`). A bind or accept failure only logs `metrics server exited` (`crates/bootstrap/src/serve.rs:316-320`); the process keeps serving gRPC without `/metrics` |
| Shared families | every process (`crates/bootstrap/src/metrics.rs:63-117`) | `cc_build_info{service,version,git_sha,rustc}`, `cc_grpc_requests_total{service,method,code}`, `cc_grpc_request_duration_seconds`, `cc_peer_health{service,peer}`, and the Linux process collector |
| Method label guard | `GrpcMetricsLayer` | A path not in `known_methods` records as `method="unknown"`. chain and beacon-core list 7 of 11 RPCs (`services/chain/src/run.rs:293-301`, `bin/beacon-core/src/boot.rs:49-57`), so `GetCommitteeShuffling`, `GetValidatorPubkeys`, `P2pStream` and `GetValidatorRecords` record as `unknown`; p2p omits `EngineStream` (`services/p2p/src/main.rs:458`) |
| Liveness | chain, beacon-core | `cc_core_liveness_rtt_seconds`, `cc_core_liveness_parked`, `cc_core_liveness_deadline_seconds`. Set only inside the sampler loop (`crates/chain-core/src/liveness.rs:223-251`), so when core-absent all three stay at 0 |
| Peer health | every process with peers | `cc_peer_health == 0` is the per-edge view of 4.3 |
| Boot timing | storage | `cc_storage_restart_seconds{phase}`: `open` and `schema_check` are timed; the other five phases are observed as zero on the always-empty resume path (`crates/storage-core/src/resume.rs:104-113`). Every phase also gets one 0.0 seed sample at registration (`crates/storage-core/src/metrics.rs:852-858`); beacon-core registers the family too, with seed samples only |
| Task panics | p2p | `cc_p2p_worker_panics_total{task}` ([09-p2p-host-and-discovery.md](09-p2p-host-and-discovery.md#23-supervisor-and-panic-policy)) |

The process that drives the EL exports no `cc_engine_*` series, because its client is built with
`finish(None)`. `:9104` reflects only the engine container's own client
([08-execution-engine.md](08-execution-engine.md#38-metrics)).

## 7. Where docs and comments disagree with the code

| Source | Says | Code at HEAD |
|---|---|---|
| `docs/running.md:191-192`, `services/chain/src/lib.rs:22-23`, `crates/bootstrap/src/serve.rs:8-9`, `crates/bootstrap/src/prober.rs:40-41`, `ServeOptions::require_local_ready` (`crates/bootstrap/src/serve.rs:87-92`); `services/chain/src/run.rs:8-13` implies it by listing local-ready after checkpoint sync | "chain healthy" means bootstrapped | the boot task calls `mark_ready()` at once (`services/chain/src/run.rs:421`), so it means bound |
| `crates/chain-core/src/liveness.rs:139`, [ADR-R-04](../adr/ADR-R-04.md) | the sampler starts after the restore handshake marked ready | E4 was deleted in 60c6200; the sampler starts after checkpoint sync or seed |
| `plan/architecture.md` §7.2; ADR-R-04 (`:84`) | one-slot deadline, 2 misses, "compose restarts it"; ADR-R-04 also says "Compose restarts it" | about 4 s, 3 misses, 3 successes; nothing restarts the process, and beacon-core has no compose service. ADR-R-04 owns the miss budget |
| `plan/architecture.md` §7.2 (`:1482-1483`) | names the parked flag both `cc_core_liveness{state="parked"}` and `cc_core_liveness_parked` | the code registers `cc_core_liveness_parked` |
| `crates/chain-core/src/liveness.rs:61-64` | "Uses `SLOT_DURATION_MS`, never `SECONDS_PER_SLOT`" | both hosts pass the network `seconds_per_slot * 1000` (`services/chain/src/run.rs:117-123`, `bin/beacon-core/src/boot.rs:541`); the `slot_duration_ms` and `attestation_due_bps` keys (`config/beacon-core.toml:28-29`) are not read by the sampler |
| `services/chain/src/run.rs:16-17` | "total SIGTERM budget remains 5 s" | the worst case is 5.15 s or more (5.2) |
| `crates/bootstrap/src/serve.rs:134` | handlers are installed before binding | they are installed inside the spawned `shutdown-signal` task, concurrently with the bind (`:274`, `:411-419`). Traced only: a SIGTERM before that task first runs would get the default action |
| `services/chain/src/main.rs:6` | "Restore is not deleted" | E4 was deleted in 60c6200 |
| `bin/beacon-core/src/boot.rs:3-6` | the writer starts after the seed | `run()` starts it before the seed decision (`:423` vs `:461`) |
| `docs/el-runbook.md:141-150` | expect two `Loaded JWT secret file` lines (el and engine) | the engine container logs it before `init`, so as wired only el's line appears (section 3) |
| `docker-compose.engine-blackhole.yml:7-10` | implies the overlay can turn chain's `""` NOT_SERVING | chain no longer dials `engine:9004`, so the overlay cannot park the core thread |
| ADR-P3-02 header | "superseded-by: ADR-R-04" | ADR-R-04 says it does not supersede ADR-P3-02's engine half |
| `plan/architecture.md` §7.1 | compose healthchecks at `docker-compose.yml:35,59,79,110,134,160` | now `:46,72,92,125,152,178` |
| `docker-compose.yml:45` | once a core is installed the chain healthcheck reflects liveness | accurate, but no core is installed with the compose defaults |

## Planned changes

From [`plan/architecture.md`](../../plan/architecture.md) §4.2 and §7. None of these is in
effect in shape A at `7d8833d`.

- **Planned: in-process boot as the production path (§4.2).** Open, then durable set, then
  seed or checkpoint sync, all in one process. The order is built in shape B, but shape B is not
  deployed (S2 exit E2.2 is open, [`s2-exit-note.md`](../../plan/issues/s2-exit-note.md)), and
  the seed arm stays IDLE until something writes the durable set. The plan also notes that
  restart time then becomes bounded by `open()` and the invariant scan.
- **Planned: restart a parked core thread (§7.2).** The plan expects compose to restart
  beacon-core when it reports NOT_SERVING. This is not built: beacon-core has no compose service,
  and `unless-stopped` does not act on the Docker healthcheck.
- **Planned: the engine container's place in the health DAG.** ADR-R-04 leaves it to issue
  S1-B-12. Until then nothing probes the engine container or the EL; the engine container
  stays a leaf that probes chain. cc-engine remains in compose for A/B comparison and has no
  in-tree caller.
- **Planned: metrics reshape (§7.3).** The plan says `cc_storage_following_head` must be
  re-derived from the direct ingest path or deleted. At HEAD it is registered, seeded to 0 and
  never driven.
- **Planned: p2p beside beacon-core.** The target is two processes. No shipped configuration
  pairs cc-p2p with cc-beacon-core; how p2p attaches to shape B is not yet defined.
