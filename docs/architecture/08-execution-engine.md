# Execution engine bridge

> **As built at `7d8833d`** (`develop`, 2026-08-16). Status labels and diagram legend: [README](README.md#status-labels).
> Target design: [`plan/architecture.md`](../../plan/architecture.md) §2.3 (E3, E8), §6, §7.2, §9 (S1), §10.4.

How the consensus client talks to the execution layer (EL, geth) over the authenticated Engine API.
All Engine API logic lives in the library crate **engine-api** (`cc-engine-api`,
`crates/engine-api`); its live caller is **DirectEngine** (`crates/chain-core/src/engine.rs`),
called synchronously from the **core thread** (edge I5). The **engine container** (`cc-engine`,
`services/engine`) is a leftover shell. Shape B labels describe `bin/beacon-core` as wired; it is
not deployed. [engine-latency.md](../engine-latency.md) and [el-runbook.md](../el-runbook.md) are
partly stale ([section 7](#7-stale-statements-elsewhere)).

Related: `el` service in [01](01-deployment-and-processes.md); JWT guard in [02](02-crate-map.md#jwt--http-isolation);
E3/E8 in [03](03-internal-contracts.md); core thread, `pending_engine` and FcuDriver in [04](04-chain-core.md);
import hops, fcU timing and the one run that reaches the EL in [05](05-block-lifecycle.md#the-run-that-executes-today-importblock-with-checkpoint-providers);
optimistic status in [06](06-consensus-primitives.md#73-optimistic-bookkeeping-and-invalidation);
DA in [10](10-p2p-gossip-and-das.md); liveness and shutdown in [12](12-boot-health-and-shutdown.md#44-liveness-sampler-a-no-op-through-the-core);
`block_on` hazards in [13](13-concurrency-model.md#61-block_on-on-the-core-thread-for-engine-calls-i5); tests and guards in [14](14-testing-and-enforcement.md).

## 1. Status at a glance

| Mechanism | Shape A (compose) | Shape B (`cc-beacon-core`) |
|---|---|---|
| EngineApi in the chain host (upcheck transport lane, EL health machine) | LIVE | LIVE |
| Upcheck loop (`eth_syncing`, plus `exchangeCapabilities` on the Synced edge) | LIVE | LIVE |
| `engine_forkchoiceUpdatedV3` and the `engine_getBlobsV2` fastpath call (with cell computation) from the core thread | IDLE (iff core; needs an operator ImportBlock, or for fcU an ApplyAttestations; no in-tree producer) | IDLE (iff core; same) |
| `engine_newPayloadV4` from `on_block`; `pending_engine` and its re-drive | IDLE (iff core; behind the DA gate) | IDLE (iff core; same) |
| Fastpath output (128 sidecars per block) | DEAD-AS-WIRED (discarded) | DEAD-AS-WIRED (discarded) |
| `cc_engine_*` metrics from the process that drives the EL | ABSENT (`finish(None)`) | ABSENT |
| engine container (second EngineApi, `:9104` metrics) / its `eth.engine.v1.EngineService` gRPC on `:9004` | TRANSITIONAL / DEAD-AS-WIRED (served, never dialed) | ABSENT |
| E3 client and E8 `EngineStream` engine container half / E8 p2p server | DELETED (631994c) / DEAD-AS-WIRED | DELETED / ABSENT |
| `docker-compose.engine-blackhole.yml` overlay | TRANSITIONAL (cannot park the core thread) | ABSENT |

`config/chain.toml:27` (used by compose) and `config/beacon-core.toml:23` set
`checkpoint_providers = []`, so both shapes run **core-absent** by default. Even with a core
installed, the core thread sends nothing to the EL until an `ImportBlock` (or, for fcU only,
`ApplyAttestations`) arrives, which no in-tree code sends; the per-slot fcU floor does nothing
before a first successful fcU (`crates/chain-core/src/fcu_driver.rs:281-283`). At HEAD p2p
subscribes to no gossip topics; the plan schedules gossip wiring for a later stage. So in every
shipped configuration only the upcheck loops talk to the EL (two in shape A: chain's and the engine
container's). An operator `ImportBlock` with an installed core runs FetchBlobs (if the block carries
blobs) and an fcU for the anchor head (`crates/chain-core/src/core.rs:1736-1754`), which the floor
re-sends every slot. `newPayload` stays IDLE: `on_block` checks the DA gate first
(`crates/fork-choice/src/on_block.rs:230-233`) and no production path marks data available (E2 is
DEAD-AS-WIRED).

## 2. Component view

```text
 SHAPE A (docker-compose.yml)
 +--------------------------------------------------+
 | cc-chain :9001   metrics :9101 (no cc_engine_*)  |
 |  core thread "chain-core"   (core installed)     |
 |    on_block  - - -> DirectEngine  (newPayload)   |
 |    FcuDriver - - -> DirectEngine  (fcU)          |
 |    core loop - - -> DirectEngine  (fetch_blobs)  |
 |                       | I5: Handle::block_on(    |
 |                       |   timeout(D, fut))       |
 |                       v                          |
 |  tokio runtime: EngineApi #1, finish(None)       |
 |    transport lanes: ordered, fastpath, upcheck   |~~ JSON-RPC + JWT ~~~~> +-------------------+
 |    tasks: upcheck driver, fastpath worker        |                        | el (geth v1.17.5) |
 +--------------------------------------------------+                        | authrpc :8551     |
                                                                             | (cc network only) |
 +--------------------------------------------------+                        |                   |
 | cc-engine :9004  metrics :9104   TRANSITIONAL    |                        |                   |
 |  EngineApi #2, finish(Some(metrics))             |~~~ eth_syncing, ~~~~~> |                   |
 |  EngineService: served, never dialed             |    exchangeCaps        +-------------------+
 |  p2p_uri parsed, never dialed                    |
 +--------------------------------------------------+
 E8 EngineStream: cc-p2p :9002 still serves it, nothing dials it.
 SHAPE B: one cc-beacon-core process holds the cc-chain box above; no cc-engine, no p2p.
 DELETED: E3 (chain -> engine container gRPC) and the E8 engine-side client, both 631994c
```

`~~~~>` = in-process client calling out over HTTP (LIVE: `eth_syncing`, `exchangeCapabilities`;
IDLE: `newPayloadV4`, `forkchoiceUpdatedV3`, `getBlobsV2`). `- - ->` = IDLE. The `|` / `v` drop is
I5, a synchronous `block_on` on the host runtime; its `is_online` read runs every SlotTick.

| Unit | Path | Role |
|---|---|---|
| EngineApi host | `crates/engine-api/src/api.rs` | `prepare` (fail before bind); `finish` wires transport, EL health machine, fastpath, fcU gate; deadline-capped sync calls |
| transport | `crates/engine-api/src/transport.rs` | One `reqwest::Client` (<= 4 idle connections, `transport.rs:104-127`); three transport lanes; per-method timeouts; a fresh bearer token per request; bodies capped at 1 MiB, or 16 MiB for a successful getBlobsV2 (`transport.rs:29,37`); soft deadline |
| JWT signer | `crates/engine-api/src/jwt.rs` | Loads and validates the secret; signs HS256 `{iat}` per request. Private module |
| EL health machine | `crates/engine-api/src/state.rs` | Offline / Syncing / Synced / AuthFailed; admission gate; detached upcheck driver |
| methods, versions, errors | `crates/engine-api/src/methods/`, `version.rs`, `errors.rs`, `capabilities.rs` | One adapter per method; timestamp -> method; error taxonomy; advertised list |
| fastpath | `crates/engine-api/src/fastpath/`; `fastpath/fetch.rs` (`BlobBound`); `network_config.rs` (engine container only) | getBlobsV2 -> cells -> 128 sidecars -> subscription filter |
| DirectEngine, FcuDriver | `crates/chain-core/src/engine.rs`, `crates/chain-core/src/fcu_driver.rs` | `ExecutionEngine<P>` and `FcuSink` over EngineApi; FetchBlobs trigger; fcU triple, sequence, per-slot floor |
| engine container | `services/engine/src/{main.rs,lib.rs,bin/blackhole.rs}` | Section 5 |

Short paths: `api.rs`, `state.rs`, `transport.rs`, `config.rs`, `errors.rs`, `version.rs`, `jwt.rs`,
`capabilities.rs`, `methods/*`, `fastpath/*` -> `crates/engine-api/src/`; `core.rs`, `import.rs`,
`seed.rs`, `engine.rs`, `fcu_driver.rs` -> `crates/chain-core/src/`; `run.rs` ->
`services/chain/src/`; `main.rs` -> `services/engine/src/`; `on_block.rs` ->
`crates/fork-choice/src/`.

## 3. cc-engine-api

### 3.1 Construction: fail before bind

1. `EngineApi::prepare` (engine container) or `prepare_with_chain_config` (chain, beacon-core) runs
   before any bind (`api.rs:111-154`): it loads the JWT, requires `[el_forks]` (a missing table is
   refused, not read as Osaka-at-genesis), loads the production KZG setup and, in the engine
   container, a sandboxed `network_config`. Any failure aborts the process.
2. `PreparedEngine::finish(metrics)` captures `Handle::try_current()`, derives the blob bound and
   spawns two detached tasks with no production shutdown, the upcheck driver and the fastpath worker
   (`api.rs:159-207`). Chain (`run.rs:362`) and beacon-core (`bin/beacon-core/src/boot.rs:438`) pass
   `metrics = None`; only the engine container passes `Some` (`main.rs:99-101`).

### 3.2 Transport lanes

A **transport lane** (`transport::Lane`, `transport.rs:56-63`) is not a **scheduler lane**.

| Transport lane | Guard | Methods | Why separate |
|---|---|---|---|
| ordered | `tokio::sync::Mutex<()>`, held across the whole HTTP round trip (`transport.rs:237-241`); gated fcU also holds it across its 250 ms retry sleep (`methods/fcu.rs:201, 332-334`) | `newPayload`, `forkchoiceUpdated` (including the ungated Synced-edge fcU re-send) | Engine API ordering on one wire |
| fastpath | `Semaphore(2)` | `getBlobsV2` | blob fetches never queue behind payload calls; one worker task issues them one at a time, so the second permit is unused at HEAD |
| upcheck | `Semaphore(1)` | `eth_syncing`, `exchangeCapabilities` | a stalled probe must not block `newPayload` ([ADR-P3-08](../adr/ADR-P3-08.md)) |

- A call slower than the **soft deadline** (`attestation_due_bps * slot_duration_ms / 10000`,
  3999.6 ms by default) only logs a warning; only the engine container also counts it
  (`cc_engine_soft_deadline_exceeded_total`). fcU is exempt (`transport.rs:287-300`;
  [ADR-P3-13](../adr/ADR-P3-13.md)).

### 3.3 Methods, versions and timeouts

| Method | Transport lane | Timeout | Retry inside engine-api | Called by |
|---|---|---|---|---|
| `engine_newPayloadV4` | ordered | 8 s | never ([ADR-P3-09](../adr/ADR-P3-09.md)) | DirectEngine from `on_block` |
| `engine_forkchoiceUpdatedV3` (attributes `null`) | ordered | 8 s | once after 250 ms, only on -32603 / -32000 | FcuDriver; Synced-edge re-send |
| `engine_getBlobsV2` | fastpath | 1 s | none | fastpath worker |
| `engine_exchangeCapabilities` | upcheck | 1 s | none | upcheck task, on a not-Synced -> Synced edge only |
| `eth_syncing` | upcheck | 1 s | none | upcheck driver, every slot, plus one re-probe 250 ms later while not Synced |

- Defaults: `config.rs:24-28`. `[timeouts].multiplier` scales all five, not the soft deadline (bad
  values -> 1.0, `config.rs:45-65`). DirectEngine's caller-side caps are the same
  (`run.rs:363-366`).
- **Version selection.** newPayload reads only `executionPayload.timestamp` against `[el_forks]`
  (`version.rs:94-132`): Prague, Osaka, BPO1 and BPO2 select V4; at or after `amsterdam_time` (unset
  on Hoodi) it fails with `UnsupportedFork` before any HTTP. fcU sends null attributes and is always
  V3 (`methods/fcu.rs:259-266`). An EL `-38005` is logged, never retried (`version.rs:188-196`).
  Only the engine container counts either (`cc_engine_unsupported_fork_total`).
- **Capabilities.** The advertised list (`capabilities.rs:25-31`) includes `eth_chainId`, which is
  never dispatched. The EL's answer only logs and sets a gauge; it gates nothing. Not implemented:
  `getPayload*`, payload attributes, getBlobsV3 (discovered and logged only), newPayloadV5.

### 3.4 Error taxonomy and the collapse at the seam

`EngineError` (`errors.rs:29-78`) has one variant per JSON-RPC code (-32700..-32000,
-38001..-38006), plus `Http401`, `Http403` (vhost, never read as a token problem:
[el-runbook.md](../el-runbook.md#observed-auth-failures)), other 4xx, 5xx, `Timeout`, `Transport`,
`Decode` and `Other`. `retry_class` marks only -32603 and -32000 `Transient`, 401/403 `Auth`, the
rest `Fatal` (`errors.rs:112-118`); only fcU acts on `Transient`. Two mappings sit downstream:

| Where | Mapping | Effect |
|---|---|---|
| EL health machine | 401/403 -> `AuthFailed`; any other error -> `Offline` (`state.rs:144-153`) | upcheck outcomes; newPayload and gated fcU 401/403 via `note_ordered_lane_error` (`api.rs:495-502`) |
| DirectEngine | **every** error -> `EngineError::Transport` (`engine.rs:199-201`) | `on_block` returns `Deferred(ExecutionEngineUnavailable)`, store untouched (`on_block.rs:267-273`) |

So no `EngineError`, not even a deterministic one (-32602, `Decode`, `UnsupportedFork`), rejects a
block; only an `INVALID` or `INVALID_BLOCK_HASH` *status* does, and it fails only the one block
(`crates/state-transition/src/block/execution_payload.rs:86-89`). DirectEngine forwards
`latestValidHash` (`engine.rs:186-191`), but the CC-35 invalidation walk is DEAD-AS-WIRED, so
Optimistic descendants are never invalidated
([06 §7.3](06-consensus-primitives.md#73-optimistic-bookkeeping-and-invalidation);
[ADR-P3-10](../adr/ADR-P3-10.md), [ADR-P3-11](../adr/ADR-P3-11.md)). Statuses map one-to-one onto
`PayloadStatus` ([ADR-P3-04](../adr/ADR-P3-04.md)). Engine-api errors defer the block rather than
reject it; re-drive depends on an EL health transition.

### 3.5 EL health state machine

```text
          +------------------- eth_syncing = false [S] (direct) --------------------+
          |                                                                         v
        +---------+  eth_syncing != false  +---------+  eth_syncing = false [S]  +--------+
        | Offline | ---------------------> | Syncing | ------------------------> | Synced |
        | initial | <--------------------- |         | <------------------------ |        |
        +---------+      failure [C]       +---------+   eth_syncing != false    +--------+
             ^                                                                      |
             +------------------------------- failure [C] --------------------------+

                                                                      +------------+
 Offline, Syncing or Synced --- HTTP 401/403 [C] -------------------> | AuthFailed |
   (from the eth_syncing probe, newPayload or gated fcU)              | (terminal) |
                                                                      +------------+
```

Arrows are state transitions, not calls (`state.rs:425-495`). `failure` = any other EngineError
(timeout, transport, 5xx or other 4xx, decode, any JSON-RPC error). `[S]` = Synced edge: arm
`exchangeCapabilities` and a re-send of the cached fcU triple; a 401/403 from either is discarded
(`state.rs:296-319`). `[C]` = clear the capability cache. A newPayload or fcU timeout changes
nothing.

- **External view.** Synced and Syncing are **Online**; Offline and AuthFailed are **Offline**
  (`el_offline = true`, `state.rs:66-71`); the machine starts Offline (`state.rs:362`). While
  Offline, `newPayload` and fcU are refused without HTTP (`api.rs:483-493`). Consumers: the
  admission gate, `DirectEngine::is_online` (re-drive edge) and the engine container's
  `GetEngineState` (its own machine); `subscribe_external` has no production subscriber
  (DEAD-AS-WIRED).
- **AuthFailed is terminal.** `run_upcheck` stops probing, `apply` ignores every outcome, and
  `operator_reset_auth_failed` has no production caller (DEAD-AS-WIRED). Recovery means restarting
  chain (shape A) or beacon-core (shape B), **not** the engine container. So one 401 from clock skew
  beyond geth's +-60 s `iat` window, or a 401/403 after a secret rotation or vhost change, on the
  probe or on newPayload / gated fcU, stops every newPayload and fcU until that restart.
- **Driver.** `spawn_upcheck_driver` (`state.rs:553-597`) ticks every `slot_duration_ms` (first
  tick immediate, missed ticks skipped), runs each probe as a nested detached task, and re-probes
  once after 250 ms unless Synced or AuthFailed. Synced-edge side effects run on the same task.

### 3.6 JWT signer and the isolation rule

```text
 host ./secrets/jwt.hex, bind-mounted :ro at /jwt/jwt.hex (never COPY'd into an image)
     mount into                 mount into                                       mount into
          |                          |                                                     |
          v                          v                                                     v
 +----------------------+  +----------------------+                 +-----------------------------+
 | cc-chain (shape A)   |  | cc-engine            |                 | el (geth)                   |
 | JwtSecret::load      |  | JwtSecret::load      |                 | --authrpc.jwtsecret         |
 | before any bind      |  | before any bind      |                 | checks HS256, iat +-60 s    |
 +----------------------+  +----------------------+                 +-----------------------------+
    |                         |                                            ^
    +~~~~~~~~~~~~~~~~~~~~~~~~~+~~ Bearer token, HS256 {iat}, per request ~~+
 shape B: cc-beacon-core loads it the same way; leak: services/engine/src/lib.rs:26-27 pub mod jwt
```

`~` = HTTP request carrying a fresh bearer token. Chain's tokens ride I5 and its upcheck loop.

- **Rule** ([ADR-R-03](../adr/ADR-R-03.md), `plan/architecture.md` §6.2). The credential lives in
  the consensus process, so isolation is a property of the API surface: only `cc-engine-api` may
  declare a JWT signer or an HTTP client, and no exported type may carry or print secret material.
- **Signer.** `mod jwt` is private (`crates/engine-api/src/lib.rs:21-23`). `JwtSecret` holds 32
  bytes; its `Debug` prints `Jwt(<redacted>)`; `load()` wants hex, exactly 32 B, a regular file <=
  4096 B, mode 0600, no `..`, and logs `Loaded JWT secret file path=... crc32=0x...`
  (`jwt.rs:222-230`); `sign_now()` signs HS256 `{iat}`. `EngineApi::prepare*`,
  `EngineTransport::from_secret_path` and `from_(config_)secret_bytes` take raw `[u8; 32]` only as
  input; until `finish` the bytes sit in `PreparedEngine.jwt_bytes` (`api.rs:71`).
- **Enforcement** ([02](02-crate-map.md#jwt--http-isolation)). `http_or_jwt_allowed()`
  (`scripts/check-crate-dag.sh:45-67`) admits `cc-engine-api` **and** the TRANSITIONAL `cc-engine`;
  `cc-chain` and `cc-bootstrap` may declare HTTP but never JWT crates. At HEAD the early manifest
  scan is LIVE (passes); the metadata walk and the `pub jwt` / `JwtSecret` export grep (`:831-842`,
  scanning only `crates/engine-api/src/lib.rs`) are INERT (masked): the script exits 1 at `:365` on
  the storage-core opaque-bytes rule.
- **Known leak.** The `#[path]` re-include in `services/engine/src/lib.rs:26-27` makes `JwtSecret`
  public from the TRANSITIONAL crate; the export grep would miss it. It exists so that
  `services/engine/tests/auth_container.rs` (real-geth 401/403 and `iat` cases, `#[ignore]` by
  default) and `requests_32602.rs` can load a secret (`services/engine/src/lib.rs:24-25`).

### 3.7 getBlobsV2 fastpath, and who consumes it

| Step | Code | Notes |
|---|---|---|
| trigger | `core.rs:1736-1741, 1787-1792` | Only when import returns `Deferred(DataUnavailable)` and the block has KZG commitments |
| enqueue | `DirectEngine::fetch_blobs` -> `api.fetch_blobs(.., 1 s)` (`api.rs:459-473`) | The 1 s cap covers the **enqueue** only. No EL admission check. Results ignored |
| queue | `fastpath/mod.rs:380-425` | Bound gate (`max_blobs_per_block` at that epoch), single-flight per root, `VecDeque` of 32 dropping the oldest |
| fetch | `methods/get_blobs.rs:161-207` | Fastpath transport lane, 1 s. JSON `null` or a partial array is a miss, not an error |
| cells | `fastpath/cells.rs:201-232` | Binds each blob to its versioned hash and commitment; `compute_cells` on `spawn_blocking` ([ADR-P3-12](../adr/ADR-P3-12.md)) |
| sidecars, filter, inject | `fastpath/sidecars.rs:103-157`, `fastpath/filter.rs:118-153`, `fastpath/mod.rs:515-553` | 128-way transpose into `DataColumnSidecar`; keep subscribed column indices; `try_send` on `inject_tx` when set |

Cell proofs from the EL are **not** batch-verified ([ADR-P3-15](../adr/ADR-P3-15.md)). **Consumers:
none.** `finish` installs `SubscriptionSet::empty()` (`api.rs:192`); `set_subscription` and
`set_inject_tx` have no caller in the tree. Each completed fetch publishes 0 sidecars, drops 128,
and lands in a 32-entry ring that only tests read; nothing reaches p2p, chain's DA gate or
storage-core.

### 3.8 Metrics

| Family | Exported by | Status |
|---|---|---|
| The other 18 `cc_engine_*` families (`crates/engine-api/src/metrics.rs:511-616`) | engine container `:9104` only | LIVE (upcheck series only; they describe the second client) |
| `cc_engine_inject_stream_state`, `cc_engine_fastpath_to_available_seconds` | registered, never written | DEAD-AS-WIRED |
| any `cc_engine_*` from chain / beacon-core | not registered (`finish(None)`) | ABSENT |
| `cc_chain_pending_engine_occupancy`, `cc_chain_pending_engine_dropped_total`, `cc_chain_import_total{result="deferred_engine"}` | chain, beacon-core (`crates/chain-core/src/metrics.rs:431-435, 570-577`) | IDLE (nothing gets parked) |
| `cc_chain_engine_call_seconds` | chain, beacon-core | DEAD-AS-WIRED (`observe(0.0)` seed at `crates/chain-core/src/metrics.rs:755`; `observe_engine_call` has no caller) |

Live on `:9104`: `_upcheck_seconds`, `_state`, `_el_offline`, `_capability_missing`, and
`_request_seconds` / `_errors_total` / `_transport_timeout_total` for `eth_syncing` and
`exchangeCapabilities`. Every newPayload, fcU and fastpath series stays at 0 (nothing calls
`:9004`). In chain and beacon-core, soft deadline, unsupported fork and EL health are log lines only.

## 4. DirectEngine on the core thread

### 4.1 Call path

```text
 core thread: CoreCommand::ImportBlock / ImportBlockGossip   core.rs:1710-1803 (no in-tree sender)
 v
 import_block_with_early -> on_block                                    on_block.rs:230-283
   1. DA gate (first check) -- not available --> Ok(Deferred(DataUnavailable)) -> pending_da
   | available (never at HEAD: E2 is DEAD-AS-WIRED)
   v
   2. [IDLE at HEAD] state_transition -> process_execution_payload
      -> DirectEngine.verify_and_notify_new_payload  (I5, sole call site)     engine.rs:121
         -> EngineApi.new_payload(.., D = timeouts.new_payload = 8 s)         api.rs:296
            a. block_on(ensure_el_admitted) -- external Offline --> Err(Transport), no HTTP
            b. block_on(timeout(D, new_payload_v4))              methods/new_payload.rs:25
                 SSZ decode -> JSON hex -> version gate -> ordered transport lane  transport.rs:237
                 ~~~~> el :8551 engine_newPayloadV4   (Bearer JWT; 1 MiB body cap)
      <- VALID -> import as Valid;   SYNCING | ACCEPTED -> import as Optimistic
      <- INVALID | INVALID_BLOCK_HASH -> Err(InvalidPayload): block rejected, no LVH walk
      <- any EngineError -> Transport (engine.rs:199)
           -> Ok(Deferred(ExecutionEngineUnavailable)) -> pending_engine (64, 8 slots)
 v
 core loop, after any Ok outcome                                          core.rs:1736-1754
   a. if block_branch: DirectEngine.fetch_blobs (<= 1 s enqueue)    b. maybe_publish_epoch_context
   c. emit_fcu_head -> FcuDriver.on_head_update (new sequence; no same-head dedupe)
      -> try_emit (emit_mu; drop if sequence < latest) -> DirectEngine.emit (I5)
         -> EngineApi.forkchoice_updated(.., D = 8 s)                         api.rs:356
            block_on(admit); block_on(timeout(D, ordered lock -> FcuSequenceGate
               ~~~~> el :8551 engine_forkchoiceUpdatedV3 (attrs null)))   methods/fcu.rs:179
            returned payload status is discarded                       engine.rs:155-165
   d. reply.send(outcome), always    <- the RPC caller waits for a. and c.
 SlotTick: is_online() (1 s cap); on an Offline->Online edge re-drive, then fcU; then on_slot
```

`~~~~>` leaves the process over HTTP; the rest runs on the core thread, blocked in
`Handle::block_on` per call. At HEAD only an operator `ImportBlock` reaches step 1; step 2 never
runs.

### 4.2 Deadlines, and what parks the core thread

- `EngineApi::call_with_deadline` drives each future under `tokio::time::timeout(D)` on the captured
  `Handle` and returns `EngineError::Timeout` when it fires (`api.rs:275-293`); waiting for the
  ordered mutex counts against `D`. The admission check runs first in its own `block_on`, outside
  `D` (`api.rs:304, 366-368`); it only takes the EL health machine lock. `block_on` panics on a
  runtime worker, so durable seed replay runs on `spawn_blocking` (`seed.rs:462`).
- So a call parks the core thread for about `D` at most: 8 s for `newPayload` and fcU, 1 s for
  `is_online` and the getBlobs enqueue. fcU follows every import that returns a verdict (INVALID,
  UNKNOWN_PARENT, DUPLICATE and DA-deferred included; only errors returned as a gRPC status, such as
  decode or root mismatch, skip it), every successful `ApplyAttestations`, every `DataAvailable`,
  every re-drive and the per-slot floor. The RPC reply waits for the FetchBlobs enqueue and the fcU
  round trip (`core.rs:1752 -> 1754`, `1801 -> 1803`, `1817 -> 1819`), although `core.rs:1669, 1751`
  call fcU "off the attestation path".
- On SIGTERM, an in-flight call (up to 8 s) outlasts the 2 s core join envelope, and
  `shutdown_and_join` detaches the core thread (`core.rs:1181-1205`; [12 §5.1](12-boot-health-and-shutdown.md#51-sequence)).
- A sustained park is caught by the liveness sampler, not by the EL client
  ([ADR-R-04](../adr/ADR-R-04.md); `services/chain/tests/engine_blackhole_liveness.rs`). Liveness
  flips aggregate health; nothing in this repository restarts a process on that signal.
- Tests: `services/chain/tests/{engine_rpc_deadlines,import_engine_fork_choice,fcu_driver,engine_blackhole_liveness}.rs`;
  `services/engine/tests/*.rs`; in-crate tests in `crates/engine-api` ([14](14-testing-and-enforcement.md#service-tests)).

### 4.3 pending_engine and re-drive

Parking (64 entries, oldest evicted; an entry is dropped once older than 8 slots, `age > 8`) and
the re-drive loop are in [04 §7](04-chain-core.md#7-pending_engine-and-optimistic-import)
(`crates/chain-core/src/pending_engine.rs:26-29, 159-168`; `core.rs:1980-2007, 2162-2219`; the fcU
after a re-drive is `core.rs:2004`). A parked block answers
`DEFERRED_DA` with reason `execution_engine_unavailable` (`import.rs:582-612`), gossip class Ignore
(`crates/fork-choice/src/da_seam.rs:293`). The map is separate from `pending_da`
([ADR-P3-05](../adr/ADR-P3-05.md)). It is IDLE at HEAD (section 1). Two gaps follow from the wiring:

- The DA re-drive passes `None` for `pending_engine` (`core.rs:2129`): a block that clears DA and
  then hits an engine-api error is counted but not parked.
- Re-drive needs an Offline -> Online edge **as sampled at SlotTick**: a timeout or JSON-RPC error
  while Online produces no edge, so the block just expires; as wired, an Offline spell between two
  SlotTicks would also go unseen.

### 4.4 fcU driver

`FcuDriver` (`fcu_driver.rs:117-321`) builds the triple from the proto-array `execution_block_hash`
of the head, justified and finalized roots, with a new monotonic sequence on every call (no "same
head" dedupe); emission points are in [04 §8](04-chain-core.md#8-forkchoiceupdated-driver).
`try_emit` (`:219-235`) holds `emit_mu`, drops a stale sequence, and records `last_state` only on
success; `on_slot` (`:256-287`) re-sends `last_state` with a fresh sequence once per slot.

`FcuSequenceGate` in engine-api re-checks the sequence under the ordered lock and resets its
high-water mark when the random per-DirectEngine `session_id` changes (`methods/fcu.rs:179-231`); on
that path a stale fcU never reaches HTTP. The Synced-edge re-send (`state.rs:306-319`) calls the
ungated `forkchoice_updated_v3` (`methods/fcu.rs:146-172`), which takes only the
ordered transport lane: as wired, it could re-send the older cached triple after a newer gated fcU
(say, on a Syncing -> Synced edge while the core thread emits), and it discards its result,
401/403 included. Not observed; traced only. -38002 and -38006 are logged, never retried
([el-runbook.md](../el-runbook.md#-38002-and-fork-choice-ordering)); failures are `warn` only
(`core.rs:2033-2035`). `DirectEngine::emit` drops the returned payload status (`engine.rs:164`), so
an fcU answered INVALID or SYNCING counts as delivered: it becomes `last_state`, the floor re-sends
it, and it is cached for the Synced-edge re-send (`api.rs:418-431`). No code acts on an fcU INVALID.

### 4.5 Shape B seed replay

`seed_from_durable` replays durable blocks through the real DirectEngine
(`bin/beacon-core/src/boot.rs:461-473`), accepting only `Imported` or a DA deferral matching the
stored status; any other outcome, including `Deferred(ExecutionEngineUnavailable)`, fails boot,
except an `on_block` error for a block already in the store (`seed.rs:253-283`). The machine starts
Offline and the first probe is a detached spawn, so as wired the first replayed `newPayload` can
race the first upcheck and abort boot; moot at HEAD (the durable set is always `None`). Shape B does
not yet bound its store or order EL readiness before seed replay. Whether replay should call the EL
at all is open: [ADR-R-06](../adr/ADR-R-06.md) (proposed, revisit at S2).

## 5. What the engine container still does

`main.rs:89-112` loads `EngineConfig` (which requires `network_config`), calls `EngineApi::prepare`
and `finish(Some(metrics))`, and serves `eth.engine.v1.EngineService` (GetInfo, NewPayload,
ForkchoiceUpdated, GetEngineState, FetchBlobs). cc-engine remains in compose for A/B comparison and
has no in-tree caller.

| Behaviour | Status | Notes |
|---|---|---|
| second EngineApi: own upcheck loop, EL health machine and capability exchange against `el:8551`, same JWT | LIVE (TRANSITIONAL host) | The EL sees two independent clients |
| EngineService RPCs, including `GetEngineState` | DEAD-AS-WIRED | No caller-side deadline (`api.rs:327-352, 392-441, 476-481`); errors -> gRPC codes (`main.rs:250-263`); `GetEngineState` reports this container's machine only. Its fastpath worker is IDLE: nothing calls `FetchBlobs` |
| `:9104` metrics | LIVE (upcheck series only) | They describe this container's machine, not chain's |
| EngineStream client to p2p (E8) | DELETED (631994c) | `p2p_uri` parsed, never dialed (`config.rs:194-199`); p2p still attaches the server (`services/p2p/src/main.rs:499-508`) |
| health peer of chain | LIVE | Chain does not list the engine container. `depends_on`: chain `service_healthy`, el `service_started` ([ADR-P3-14](../adr/ADR-P3-14.md)). [ADR-P3-02](../adr/ADR-P3-02.md) marks its liveness implication superseded by [ADR-R-04](../adr/ADR-R-04.md), which says it does not supersede ADR-P3-02 |
| `cc-engine-blackhole` overlay ([01](01-deployment-and-processes.md#the-engine-blackhole-overlay)) | TRANSITIONAL | Replaces `engine:9004`, which chain no longer dials |

## 6. Configuration

The chain, engine container and beacon-core configs flatten `EngineTransportConfig` (`config.rs:172-200`).

| Key | Default | Compose (shape A) |
|---|---|---|
| `el_endpoint` | `http://127.0.0.1:8551` | `CC_CHAIN_EL_ENDPOINT` / `CC_ENGINE_EL_ENDPOINT` = `http://el:8551` |
| `jwt_secret_path` | `secrets/jwt.hex` | `/jwt/jwt.hex` (chain, engine container) |
| `[el_forks]` | none; refused at boot if missing (`config.rs:250-253`) | Hoodi `osaka_time`, `bpo1_time`, `bpo2_time`; no `amsterdam_time` (see below) |
| `p2p_uri` / chain `engine_uri` | `http://127.0.0.1:9002` / `:9004` | `CC_ENGINE_P2P_URI` / `CC_CHAIN_ENGINE_URI`: dead config kept for the URI-override gate; `engine_uri` is logged at debug only (`run.rs:188-191, 358-361`) |

**`[el_forks]`** (`config.rs:143-155`): `osaka_time` is required; `bpo1_time`, `bpo2_time` and
`amsterdam_time` are optional. `fork_at` maps a payload timestamp to Prague, Osaka, `Bpo(n)` or
Amsterdam (`version.rs:94-118`), but only `amsterdam_time` changes the call made: Amsterdam fails
with `UnsupportedFork` before any HTTP, every earlier arm selects V4 (`version.rs:127-131`;
[3.3](#33-methods-versions-and-timeouts)). So the BPO fields are informational, and a future BPO3
needs no `[el_forks]` field (the blob limit comes from `BLOB_SCHEDULE`, `fastpath/fetch.rs:94-117`).
The table is copied in `config/chain.toml:83`, `config/beacon-core.toml:42` and
`config/engine.toml:55`. Nothing checks the copies against each other or against the
`FULU_FORK_EPOCH` / `BLOB_SCHEDULE` of the `ChainConfig` that `PreparedEngine` holds beside the
schedule (`api.rs:111-154`), and no test compares the copies.
Network identity and adding a fork: [06](06-consensus-primitives.md) (identity table, fork checklist).

`slot_duration_ms` (12000), `attestation_due_bps` (3333) and `[timeouts]` (section 3.3) come from
TOML. Chain has no `depends_on: el`; it starts with the EL health machine Offline and lets the
upcheck loop retry. If chain or beacon-core has no `network_config` **and** no checkpoint providers,
the blob bound comes from the Hoodi test-fixture YAML (`run.rs:234-262`;
`bin/beacon-core/src/boot.rs:265-286`); with providers set, boot refuses. Compose sets no
`CC_CHAIN_NETWORK_CONFIG`, so compose chain always uses the fixture, although S1-B-02 says
production must not fall back to a compiled test fixture (`main.rs:49-53`).

## 7. Stale statements elsewhere

| Where | Says | At HEAD |
|---|---|---|
| [engine-latency.md](../engine-latency.md) OQ-P3-8; `EngineApiClient`; chain -> engine container gRPC hop | Keep the engine a separate process so the JWT stays out of consensus | Superseded by [ADR-R-03](../adr/ADR-R-03.md). The hop and the client are gone. `cc_chain_engine_call_seconds` is seed-only |
| [el-runbook.md](../el-runbook.md#crc32-pair-one-grep-both-sides) crc32 pair | Exactly two lines (el, engine) | Three processes load the secret: el, engine container and chain |
| [el-runbook.md](../el-runbook.md#authfailed-recovery); log text at `state.rs:443-445` | Restart `engine` to clear AuthFailed; watch `GetEngineState` | The live machine is in chain (A) or beacon-core (B), and its state shows only in logs: chain exports no `cc_engine_state`, and `GetEngineState` reports the container's machine |
| [running.md](../running.md) lines 450, 523, 647-650; [el-runbook.md](../el-runbook.md#container-auth-trap) lines 21-22 | JWT mounted into `el`; engine metrics on :9104; without `CC_CHAIN_ENGINE_URI` import is dead; `CC_ENGINE_P2P_URI` is EngineStream; only `engine` must abort before bind | Mounted into chain, engine container and el. :9104 is the leftover client. Neither URI is dialed. Chain and beacon-core also abort before bind |
| `docker-compose.engine-blackhole.yml`, `devnet/faults.sh`, `services/engine/src/bin/blackhole.rs:3-5` | The overlay makes newPayload/fcU hang and parks the core thread | It black-holes only the unused `:9004` |
| `scripts/check-no-http-import-path.sh:2` | No HTTP client reachable from block import | Its grep omits `crates/chain-core/src/engine.rs`. Import reaches reqwest through DirectEngine |
| `proto/eth/p2p/v1/p2p.proto:21-24`, `proto/eth/engine/v1/engine.proto` comments | EngineStream client and JSON encoding live in `services/engine` | The encoding moved to cc-engine-api at d5ada19; chain has called it in-process since 631994c, which also deleted the EngineStream client |
| `capabilities.rs:16-24`; `methods/capabilities.rs:29` | Never advertise an unimplemented method; exchange capabilities at startup | `eth_chainId` is advertised, never dispatched. The exchange runs only on a not-Synced -> Synced edge, so an EL that stays Syncing never gets it |

## Planned changes

From [`plan/architecture.md`](../../plan/architecture.md) (section per item) and the named ADRs; none is built at HEAD.

- Planned: delete the `services/engine` crate (with `cc-engine-blackhole`) as the last commit of a
  stage (§9). At HEAD it is already the thin A/B constructor that §9.1 S1 allows, and it still runs
  a second EL client. Deleting it also deletes the real-EL auth tests unless they move first.
- Planned: drop the TRANSITIONAL `cc-engine` HTTP/JWT allowance in `check-crate-dag.sh`
  ([ADR-R-03](../adr/ADR-R-03.md) refactor impact, S1-A-06).
- Planned: "engine-fastpath DA path works end to end" (§9.1 S1 "Ships"). At HEAD the output has no
  consumer; the S1 exit note records E1.5 as not discharged (`plan/issues/s1-exit-note.md`). A
  consumer makes the [ADR-P3-15](../adr/ADR-P3-15.md) skip of `verify_cell_kzg_proof_batch`
  load-bearing; the plan (§10.4, ADR-P3-15 row) asks for an explicit decision first.
- Planned: E8 becomes a second method on `P2pEgress`, not a separate contract (§2.3); its transport
  follows E1 at S3. At HEAD `P2pEgress` has only `publish` and `update_view`.
