# Crate map and layering

> **As built at `7d8833d`** (`develop`, 2026-08-16). Status labels and diagram legend: [README](README.md#status-labels).
> Target design: [`plan/architecture.md`](../../plan/architecture.md) §1, §6.2.

The workspace has **28 members** (`Cargo.toml:3-43`) and **13 binary targets**: the six
service binaries, `cc-beacon-core`, four offline tools, and two extra binaries inside service
crates (`cc-engine-blackhole` in `cc-engine`, `gen_hostile_corpus` in `cc-p2p`). This page
groups every member by layer, gives the dependency DAG, and lists the guards that hold it.

Everything here is compile-time structure. Which crates run in which process is in
[01](01-deployment-and-processes.md); CI wiring and the test pyramid are in
[14](14-testing-and-enforcement.md); dependency policy is in [../supply-chain.md](../supply-chain.md).

Key facts at HEAD:

- The DAG has **83 normal path edges** and is acyclic. Its transitive reduction has 41 edges.
- The four pure-consensus crates (`cc-types`, `cc-crypto`, `cc-state-transition`,
  `cc-fork-choice`) depend only on each other on normal edges (dev edges add `cc-spec-tests`
  and `cc-proto`) and declare no tokio, tonic, reqwest or redb.
- `cc-chain` and `cc-p2p` never depend on each other. Both depend on `cc-seam`.
- `bash scripts/check-crate-dag.sh` **exits 1 at HEAD**. An early rule fails, so its allowlist
  and lint checks, and, as wired, the guard steps after it in CI and `make lint`, are never
  reached. Fixing that rule would expose a second failure ([the DAG guard](#the-dag-guard-scriptscheck-crate-dagsh)).

## Layers at a glance

Seven layers. `cc-spec-tests` is test tooling here; [06](06-consensus-primitives.md) documents
it next to the consensus crates because it hosts their vector harness. The README's
[layered view](README.md#layered-view) draws the same assignment more coarsely (services and
bins merged, test tooling not drawn). Bands are stacked so that every dependency points at a
crate in a lower band, or later in the same band.

```text
 +== bins ========================================================================================+
 | cc-beacon-core [B] -> cc-chain, cc-storage-core                                                |
 | cc-store-tool -> cc-store                      cc-store-bench -> cc-store                      |
 | cc-devnet-gen -> cc-state-transition                                                           |
 | cc-serve-probe -> cc-config, cc-libp2p, cc-types                                               |
 +== test tooling ================================================================================+
 | cc-beacon-import -> cc-beacon-inproc, cc-fork-choice, cc-storage-core                          |
 | cc-beacon-inproc -> cc-store                   cc-spec-tests (leaf)                            |
 +== services ====================================================================================+
 | cc-chain [A] -> cc-chain-core                  cc-storage [A] -> cc-storage-core               |
 | cc-p2p [A] -> cc-bootstrap, cc-crypto, cc-libp2p, cc-seam                                      |
 | cc-engine [A] -> cc-bootstrap, cc-engine-api, cc-proto                                         |
 | cc-attestation [A] -> cc-bootstrap, cc-proto   cc-beacon-api [A] -> cc-bootstrap, cc-proto     |
 +== storage =====================================================================================+
 | cc-storage-core -> cc-bootstrap, cc-seam, cc-state-transition, cc-store                        |
 | cc-store -> cc-types                                                                           |
 +== host libs ===================================================================================+
 | cc-chain-core -> cc-bootstrap, cc-engine-api, cc-fork-choice, cc-scheduler, cc-seam            |
 | cc-engine-api -> cc-crypto                     cc-bootstrap -> cc-config                       |
 | cc-scheduler (leaf)                            cc-libp2p (leaf)                                |
 | cc-config (leaf)                                                                               |
 +== seams / contracts ===========================================================================+
 | cc-seam -> cc-proto                            cc-proto (leaf)                                 |
 +== pure consensus ==============================================================================+
 | cc-fork-choice -> cc-state-transition          cc-state-transition -> cc-crypto                |
 | cc-crypto -> cc-types                          cc-types (leaf)                                 |
 +================================================================================================+
```

Caption: `->` is a compile-time dependency, not a runtime edge. The figure is the transitive
reduction (41 of 83 edges; the [full matrix](#dependency-dag-normal-edges) lists all 83).
Within a band, read left to right, then down. `[A]` or `[B]` means the crate's binary runs
only in that shape. Unmarked crates are libraries (linked wherever a marked binary uses them)
or offline/test tools (in neither shape). No shipped configuration pairs cc-p2p with
cc-beacon-core; how p2p attaches to shape B is not yet defined.

draw.io version: [`diagrams/crate-map.drawio`](diagrams/crate-map.drawio), generated from `cargo
metadata` by `bash scripts/gen-crate-map.sh` (ELK layered layout, crates coloured by band). It draws
the same 41-edge transitive reduction; the other 42 edges sit on the hidden draw.io layer "implied
edges". [`diagrams/crate-map.edges`](diagrams/crate-map.edges) is the reviewable list, and `bash
scripts/gen-crate-map.sh --check` fails when it no longer matches the workspace.

The band order is a valid dependency order; the band names are responsibility groups.
`cc-storage-core` uses host libs (`cc-bootstrap`, `cc-config`), so storage sits above host libs,
where `cc-chain-core` is filed. Neither depends on the other, so the README's single tier for
the two respects every edge too.

## Workspace members

Size is the line count of `src/**/*.rs`, including in-file `#[cfg(test)]` modules (XS < 0.5k,
S < 2k, M < 8k, L < 20k, XL above). Status uses the closed labels, per shape where A and B
differ; for a library it describes the code the library contributes to deployed processes.
"As shipped" = the committed configs, which set `checkpoint_providers = []`
(`config/chain.toml:27`, `config/beacon-core.toml:23`), so neither shape installs a core.
Shape B columns describe `bin/beacon-core` as wired; it is not deployed ([README](README.md#status-labels)).

| Layer | Crate (path) | Responsibility | Size | Status at HEAD | Deep dive |
|---|---|---|---|---|---|
| pure consensus | `cc-types` (crates/types) | Preset, runtime `ChainConfig` and fork-schedule authority, SSZ containers, `BeaconState` with cached root, Fulu-only decode | M 7.5k | LIVE | [06](06-consensus-primitives.md) |
| pure consensus | `cc-crypto` (crates/crypto) | BLS over `blst`, signing domains, SHA-256, `CellKzg` trait with the c-kzg backend | S 1.99k | LIVE | [06](06-consensus-primitives.md) |
| pure consensus | `cc-state-transition` (crates/state-transition) | `state_transition`, slot/epoch/block processing, `ExecutionEngine` seam | M 7.4k | IDLE in A and B: as shipped its runtime callers are the IDLE storage-core replay task (A, `crates/storage-core/src/replay.rs:562`) and the IDLE `seed_from_durable` (B); chain-core import is a caller only with an installed core (IDLE (iff core)), where every new block that passes chain-core's pre-checks parks at the DA gate, which `on_block` runs before `state_transition` (`crates/fork-choice/src/on_block.rs:230-234`) | [06](06-consensus-primitives.md), [05](05-block-lifecycle.md) |
| pure consensus | `cc-fork-choice` (crates/fork-choice) | `Store<P>`, proto-array, `get_head`, DA seam, optimistic status | L 10.8k | ABSENT in A and B as shipped (core-absent: no `Store` is built; checkpoint sync builds it, `services/chain/src/checkpoint_sync.rs:1139`). With a core, the anchor store and `on_tick` are LIVE (iff core): the core thread runs `on_tick` per slot (`crates/chain-core/src/core.rs:1940`); `on_block` stops at the DA gate, so the post-import `get_head` (`crates/chain-core/src/import.rs:934`) is not reached | [06](06-consensus-primitives.md) |
| seams / contracts | `cc-proto` (crates/proto) | gRPC types generated at build time from `proto/` ([ADR-04](../adr/ADR-04.md)) | XS 0.4k + generated | LIVE | [03](03-internal-contracts.md) |
| seams / contracts | `cc-seam` (crates/seam) | Typed handles `ChainIngress`, `P2pEgress`, `ArchiveWrite`; `SeamError`; `Ipc` (tonic) and `InProcess` impls; conformance suite ([ADR-R-01](../adr/ADR-R-01.md)) | M 3.3k | `Ipc` LIVE in A (cc-p2p, E1 session), ABSENT in B. `ArchiveWrite` ABSENT in A, IDLE (iff core) in B (N1 blocks; columns DEAD-AS-WIRED). `InProcess` TEST-ONLY | [03](03-internal-contracts.md) |
| host libs | `cc-config` (crates/config) | figment loader: `config/<svc>.toml`, then `CC_<SVC>_*` env ([ADR-12](../adr/ADR-12.md)) | S 1.3k | LIVE | [01](01-deployment-and-processes.md#configuration-layering) |
| host libs | `cc-bootstrap` (crates/bootstrap) | Shared service runtime: tracing, metrics registry and `/metrics`, gRPC serve with `grpc.health.v1` and reflection, peer prober, SIGTERM drain | M 2.1k | LIVE | [12](12-boot-health-and-shutdown.md) |
| host libs | `cc-scheduler` (crates/scheduler) | Sync FIFO/LIFO queues, the scheduler-lane table and a first-match `Manager`. Zero dependencies; starts no threads | S 1.2k | ABSENT as shipped (scheduler lanes exist only inside an installed core); with a core, the tick scheduler lane is LIVE (iff core) and the other four are IDLE (iff core; their producers are RPCs with no in-tree dialer, the IDLE E1 object arm and IDLE N2, [13 §1](13-concurrency-model.md#1-status-at-a-glance)). Inside a core: `ChainWork` and the Deferred class DEAD-AS-WIRED, WorkerIdle gate INERT | [13](13-concurrency-model.md) |
| host libs | `cc-libp2p` (crates/libp2p) | Only crate that may declare the git-pinned libp2p stack: transport, `CcBehaviour`, `SnappyTransform` | S 1.96k | LIVE | [09](09-p2p-host-and-discovery.md) |
| host libs | `cc-engine-api` (crates/engine-api) | Engine API client: three transport lanes, module-private JWT signer, EL health machine, getBlobsV2 fastpath | L 12.7k | LIVE (upcheck transport lane and EL health machine: in A in cc-chain and again in cc-engine; in B in cc-beacon-core). Ordered transport lane IDLE (iff core; only DirectEngine on the core thread calls it). Fastpath output DEAD-AS-WIRED | [08](08-execution-engine.md) |
| host libs | `cc-chain-core` (crates/chain-core) | Core thread and scheduler lanes, import pipeline, pending_da / pending_engine, residency, event ring, P2pStream server, DirectEngine, seed | L 16.6k | A: P2pStream server (E1 session + ChainView) and the EngineApi EL upcheck LIVE; events task IDLE (runs; no event input, no in-tree subscriber); core thread and scheduler lanes ABSENT as shipped (core-absent), LIVE (iff core; per-scheduler-lane status in the `cc-scheduler` row). B: EngineApi EL upcheck LIVE; events task IDLE; core thread ABSENT as shipped, LIVE (iff core); `seed_from_durable` IDLE (durable set always None); N1 `ArchiveWrite` block persist IDLE (iff core) | [04](04-chain-core.md), [05](05-block-lifecycle.md) |
| storage | `cc-store` (crates/store) | The only crate that declares `redb`: concrete redb `Engine` (open / read / batch / commit / drop_table / compact, `crates/store/src/engine/redb.rs:239-470`), typed big-endian keys, meta records, 8 store invariants; values are opaque bytes ([ADR-P4-01](../adr/ADR-P4-01.md)) | L 11.7k | LIVE | [07](07-storage.md) |
| storage | `cc-storage-core` (crates/storage-core) | storage-core writer (P0/P1/P2 writer classes), serve, backfill admission, prune, replay, migrator, `ArchiveWriter`, storage-service boot | L 19.4k | A: StorageService, writer and prune task LIVE; replay IDLE; migrator DEAD-AS-WIRED; `ArchiveWriter` DEAD-AS-WIRED (built and dropped, `crates/storage-core/src/boot.rs:563`). B: writer (P0) + `ArchiveWriter` IDLE (iff core; N1 blocks; columns DEAD-AS-WIRED); P1 and P2 writer classes DEAD-AS-WIRED (mailboxes created, no producer hosted); serve, prune, replay, migrator ABSENT | [07](07-storage.md) |
| services | `cc-chain` (services/chain) | Shape A host over chain-core (`run.rs`) plus checkpoint sync (`checkpoint_sync.rs`, the only file in this crate that names `reqwest`) | M 3.0k | TRANSITIONAL (deployed in A) | [04](04-chain-core.md), [01](01-deployment-and-processes.md) |
| services | `cc-storage` (services/storage) | 16-line shim that calls `cc_storage_core::run` | XS 16 | TRANSITIONAL (deployed in A) | [07](07-storage.md) |
| services | `cc-p2p` (services/p2p) | libp2p host: swarm task, discovery, peer manager, gossip validation, req/resp, chain-stream client. Extra bin `gen_hostile_corpus` | XL 45k | LIVE (swarm, discovery, peer manager, Status handshake, chain-stream session). Gossip validation IDLE (0 topics). Block/column req/resp serve STUB. KZG verify pool DEAD-AS-WIRED | [09](09-p2p-host-and-discovery.md), [10](10-p2p-gossip-and-das.md), [11](11-p2p-reqresp-and-sync.md) |
| services | `cc-engine` (services/engine) | Leftover EngineService shell and a second EngineApi. Extra bin `cc-engine-blackhole` (opt-in compose overlay) | XS 0.4k | TRANSITIONAL (deployed in A) | [08](08-execution-engine.md) |
| services | `cc-attestation` (services/attestation) | `GetInfo`-only gRPC service | XS 85 | STUB | [01](01-deployment-and-processes.md) |
| services | `cc-beacon-api` (services/beacon-api) | `GetInfo`-only gRPC service; no REST server exists | XS 83 | STUB | [01](01-deployment-and-processes.md) |
| test tooling | `cc-spec-tests` (crates/spec-tests) | Consensus-spec vector harness: lockfile, cache root, skiplist, handler coverage | S 1.8k | TEST-ONLY | [06](06-consensus-primitives.md), [14](14-testing-and-enforcement.md) |
| test tooling | `cc-beacon-inproc` (crates/beacon-inproc) | Proto-free twin of beacon-core's open path (`open` -> `durable_set`) | XS 0.3k | TEST-ONLY | [14](14-testing-and-enforcement.md#in-process-boot-harness) |
| test tooling | `cc-beacon-import` (crates/beacon-import) | Proto-free import -> durable test crate; `src/` is a 7-line stub, the content is in `tests/` | XS 7 | TEST-ONLY | [14](14-testing-and-enforcement.md) |
| bins | `cc-beacon-core` (bin/beacon-core) | Shape B composer: opens redb first, then storage-core writer plus chain-core in one process ([ADR-R-02](../adr/ADR-R-02.md)) | S 0.6k | TRANSITIONAL (target; not deployed) | [01](01-deployment-and-processes.md#shape-b-binbeacon-core) |
| bins | `cc-store-tool` (bin/cc-store) | Offline verify / compact / dump of a stopped node's store. The binary is `cc-store`; it is not the `cc-store` crate in `crates/store` | XS 0.4k | ABSENT from A and B (offline tool) | [07](07-storage.md) |
| bins | `cc-store-bench` (bin/store-bench) | CC-40 prune-under-load falsifier | S 0.8k | ABSENT from A and B (offline tool) | [07](07-storage.md) |
| bins | `cc-devnet-gen` (bin/devnet-gen) | Self-devnet genesis, keys, signed chain and sidecars | S 1.5k | ABSENT from A and B (offline tool) | [14](14-testing-and-enforcement.md#tooling-binaries) |
| bins | `cc-serve-probe` (bin/serve-probe) | Wire-only prober of a remote peer's advertised window (Status `earliest_available_slot` plus by-range samples), with its own codec ([ADR-P4-12](../adr/ADR-P4-12.md)) | M 2.7k | ABSENT from A and B (offline tool) | [11](11-p2p-reqresp-and-sync.md) |

Notes:

- The image's builder stages eight binaries (the six services, `cc-engine-blackhole`,
  `cc-beacon-core`); each runtime image copies only the one named by `ARG SERVICE`
  (`Dockerfile:39-43,73`). Details: [01](01-deployment-and-processes.md#one-dockerfile-one-binary-per-image).
- `cc-engine`: "cc-engine remains in compose for A/B comparison and has no in-tree caller."
  It still `#[path]`-includes the JWT signer as `pub mod jwt` (`services/engine/src/lib.rs:24-27`).
- Crate docs still use the stale "4-container" term (`services/engine/src/lib.rs:5`
  "4-container topology", `services/chain/src/run.rs:5` "4-container chain"); say shape A.
- `cc-p2p`: At HEAD p2p subscribes to no gossip topics; the plan schedules gossip wiring for a
  later stage.
- `cc-beacon-import`: "No test drives the production persist path (core thread +
  `ingest_block_blocking` with a real writer)."

## Dependency DAG (normal edges)

All 83 normal path dependencies from `cargo metadata --no-deps` at `7d8833d`. Rows are
dependents and columns are dependencies, both in the band order of the figure above. Only the
16 crates that have a dependent get a column. The other 12 are roots: five of the six services
(all but `cc-chain`), the five bins, `cc-beacon-import` and `cc-spec-tests`. `cc-spec-tests`
has only dev-dependents, so on normal edges it is both a leaf and a root.

```text
                               typ cry stf fch pro sem cfg bst sch lp2 eap chc sto stc chn inp  n
 pure consensus
   typ cc-types                 \   .   .   .   .   .   .   .   .   .   .   .   .   .   .   .   0
   cry cc-crypto                *   \   .   .   .   .   .   .   .   .   .   .   .   .   .   .   1
   stf cc-state-transition      *   *   \   .   .   .   .   .   .   .   .   .   .   .   .   .   2
   fch cc-fork-choice           *   .   *   \   .   .   .   .   .   .   .   .   .   .   .   .   2
 seams / contracts
   pro cc-proto                 .   .   .   .   \   .   .   .   .   .   .   .   .   .   .   .   0
   sem cc-seam                  .   .   .   .   *   \   .   .   .   .   .   .   .   .   .   .   1
 host libs
   cfg cc-config                .   .   .   .   .   .   \   .   .   .   .   .   .   .   .   .   0
   bst cc-bootstrap             .   .   .   .   .   .   *   \   .   .   .   .   .   .   .   .   1
   sch cc-scheduler             .   .   .   .   .   .   .   .   \   .   .   .   .   .   .   .   0
   lp2 cc-libp2p                .   .   .   .   .   .   .   .   .   \   .   .   .   .   .   .   0
   eap cc-engine-api            *   *   .   .   .   .   .   .   .   .   \   .   .   .   .   .   2
   chc cc-chain-core            *   *   *   *   *   *   .   *   *   .   *   \   .   .   .   .   9
 storage
   sto cc-store                 *   .   .   .   .   .   .   .   .   .   .   .   \   .   .   .   1
   stc cc-storage-core          *   .   *   .   *   *   *   *   .   .   .   .   *   \   .   .   7
 services
   chn cc-chain                 *   *   *   *   *   *   *   *   *   .   *   *   .   .   \   .  11
       cc-storage               .   .   .   .   .   .   .   .   .   .   .   .   .   *   .   .   1
       cc-p2p                   *   *   .   .   *   *   *   *   .   *   .   .   .   .   .   .   7
       cc-engine                *   *   .   .   *   .   *   *   .   .   *   .   .   .   .   .   6
       cc-attestation           .   .   .   .   *   .   *   *   .   .   .   .   .   .   .   .   3
       cc-beacon-api            .   .   .   .   *   .   *   *   .   .   .   .   .   .   .   .   3
 test tooling
       cc-spec-tests            .   .   .   .   .   .   .   .   .   .   .   .   .   .   .   .   0
   inp cc-beacon-inproc         *   .   .   .   .   .   .   .   .   .   .   .   *   .   .   \   2
       cc-beacon-import         *   *   *   *   .   *   .   .   .   .   .   .   *   *   .   *   8
 bins
       cc-beacon-core           *   .   .   .   *   .   *   *   .   .   *   *   .   *   *   .   8
       cc-store-tool            .   .   .   .   .   .   .   .   .   .   .   .   *   .   .   .   1
       cc-store-bench           .   .   .   .   .   .   .   .   .   .   .   .   *   .   .   .   1
       cc-devnet-gen            *   *   *   .   .   .   .   .   .   .   .   .   .   .   .   .   3
       cc-serve-probe           *   .   .   .   .   .   *   .   .   *   .   .   .   .   .   .   3
                                -------------------------------------------------------------  83
```

Caption: `*` means the row crate has a direct normal dependency on the column crate. `\`
marks the diagonal. `n` is the row's out-degree. Rows and columns share one order, and every
row depends only on crates listed above it, so the graph has no cycle. Column codes are the
three letters before each crate name.

Both figures were generated from `cargo metadata` at `7d8833d`. To list the edges again after
a manifest change:

```bash
cargo metadata --no-deps --format-version 1 --offline \
  | jq -r '.packages[] | select(.source == null) | . as $p | .dependencies[]
           | select(.path != null and .kind == null) | "\($p.name) -> \(.name)"'
```

Reading the matrix:

- **Hubs.** `cc-types` has 15 dependents, `cc-proto` and `cc-config` 9 each, `cc-crypto` and
  `cc-bootstrap` 8 each.
- **`cc-chain` is a thin layer over `cc-chain-core`.** Its other 10 edges are all implied by
  `cc-chain-core`.
- **Shape B composes chain-core and storage-core.** `cc-beacon-core` depends on `cc-chain-core`
  and `cc-storage-core` directly. It uses `cc-chain` only for checkpoint sync:
  `cc_chain::checkpoint_sync` and four provider timeout constants
  (`bin/beacon-core/src/boot.rs:18,492-495`). `reqwest` reaches the shape B binary by two
  paths: through `cc-chain` (checkpoint sync) and through `cc-engine-api` (the Engine API
  transport, `crates/engine-api/src/transport.rs:88-139`), which `cc-chain-core` also depends
  on (`cargo tree -p cc-beacon-core -i reqwest`). Removing the `cc-chain` edge would not
  remove `reqwest` from shape B.
- **Optional edges.** `cc-seam -> cc-proto` sits behind `cc-seam`'s default feature `ipc`
  (`crates/seam/Cargo.toml:14-16`); `cc-storage-core -> {cc-proto, cc-bootstrap}` behind its
  default feature `grpc`, which also turns on `cc-seam/ipc` (`crates/storage-core/Cargo.toml:16`).
  `cc-beacon-import` takes both with `default-features = false`
  (`crates/beacon-import/Cargo.toml:19,21`), keeping its tree free of tonic and `cc-proto`.
- **`cc-crypto` features.** `signing` compiles BLS `SecretKey` and keygen
  (`crates/crypto/src/bls/mod.rs:339-350`). Two crates enable it on a normal edge: the offline
  tool `cc-devnet-gen` (`bin/devnet-gen/Cargo.toml:27`, its real user) and `cc-p2p`
  (`services/p2p/Cargo.toml:26`), whose only `SecretKey` uses are in `#[cfg(test)]` modules.
  The image runs `cargo build --workspace` (`Dockerfile:39`), which unifies features, so the
  six image binaries that link `cc-crypto` (all but `cc-attestation` and `cc-beacon-api`) get
  a `cc-crypto` built with `signing`. Dropping the feature from `cc-p2p` alone would not change
  that. The other features pick the KZG backend: default `kzg-c-kzg`; `kzg-rust-eth-kzg` is
  TEST-ONLY (CI vectors and bench) (`crates/crypto/Cargo.toml:13-20`).
- **What the narrow rows cost.** `cc-seam` may depend only on `cc-proto`, so it re-declares
  `IMPORT_LANE_DEPTH = 64` and `IMPORT_SEND_TIMEOUT = 2 s` (`crates/seam/src/in_process.rs:25,28`,
  `crates/seam/src/ipc.rs:44,47`) instead of importing `crates/scheduler/src/config.rs:74` and
  `crates/chain-core/src/core.rs:72`. `cc-beacon-inproc` re-implements storage-core's open,
  stamp and I-node-id checks to stay proto-free (`crates/beacon-inproc/src/boot.rs:89-256`).
  No test or guard checks that the copies agree.
- **Dev-only edges**, not in the matrix: `cc-bootstrap -> cc-proto`, `cc-fork-choice ->
  {cc-crypto, cc-proto}`, `cc-p2p -> cc-spec-tests`, `cc-storage -> {cc-store, cc-types}`,
  `cc-types -> cc-spec-tests`. `cc-chain`, `cc-chain-core` (`cc-crypto` with `signing`) and
  `cc-store` (`cc-types`) repeat a normal edge under `[dev-dependencies]`.

## Mechanical guards

Cargo enforces only acyclicity. The layering is held by shell and Python guards that run in
the CI `clippy`, `deps` and `proto` jobs and in `make lint` / `make deps` / `make proto`. Job
wiring and each guard's standalone result are in [14](14-testing-and-enforcement.md#guards).
Results here are local runs at `7d8833d`; no CI run was inspected.

As wired (static trace), `check-crate-dag.sh` runs right after `cargo clippy` in the `clippy`
job (`.github/workflows/ci.yml:83-84`) and after `cargo deny` in `deps` (`:403-404`). No step
has `if: always()` or `continue-on-error`, so while it exits 1 every later step would be
skipped: the env, HTTP, in-process gRPC and fork-schedule guards and the other clippy-job
checks (`:87-128`), and the max-blobs and libp2p-grep steps (`:407-433`; the grep also sits
behind max-blobs, which exits 1 too). `make lint` (`Makefile:153`) and `make deps` (`:247`)
stop at `check-dag` the same way. Only the `proto` job and `make proto` skip the DAG guard.

| Guard | What it forbids | Scope and mechanism | Status at HEAD |
|---|---|---|---|
| `check-crate-dag.sh` allowlist | Any workspace path edge (normal, dev or build) not listed in `allowed_deps()` (`scripts/check-crate-dag.sh:377-442`) | `cargo metadata --no-deps --locked` + jq | INERT (masked) |
| JWT / HTTP isolation (same script) | Declaring `reqwest`, `hyper`, `hyper-util`, `jsonwebtoken`, `hmac` or `sha2-jwt` anywhere except `cc-engine-api`. Exceptions: `cc-engine` (both kinds), `cc-chain` and `cc-bootstrap` (HTTP only) (`:45-67`) | Early manifest grep (`:266-280`) matches only a `<name> =` key or a `[dependencies.<name>]` table. The metadata walk, a `pub jwt` / `JwtSecret` export ban on engine-api, and a manifest backup scan (`:809-858`) also catch renamed keys | Early grep LIVE (passes); metadata half INERT (masked) |
| `cc-chain` <-> `cc-p2p` ban (same script) | Either crate depending on the other (`:95-115`) | Manifest grep (`:300-321`) for a `cc-p2p =` / `cc-chain =` key or a `[dependencies.<name>]` table. Package-name walk (`:617-640`) covers every dependency table and any rename, and an allowlist edit cannot silence it | Grep LIVE (passes); walk INERT (masked). At HEAD only the grep runs; it does not catch a renamed key or a `[dev-dependencies.cc-p2p]` table |
| Storage rules (same script) | `cc-storage` or `cc-storage-core` -> `cc-fork-choice` (`:282-298`); `cc-store` -> anything but `cc-types` (`:323-343`); naming `SignedBeaconBlock`, `BeaconState` or `DataColumnSidecar` in `crates/store/src` or `crates/storage-core/src` (`:345-362`) | Manifest and source grep; `replay.rs` and comment lines exempt | LIVE; the opaque-bytes grep **fails** |
| libp2p confinement (same script + CI step) | Any `libp2p*` declaration outside `cc-libp2p`; a pin other than one 40-hex git rev; a rev that differs from the one in the docs or `LIBP2P_GIT_REV` (`:642-807`) | Metadata walk and lockfile check; the CI `deps` job also greps manifests (`.github/workflows/ci.yml:416-433`). Declarations only: `discv5` (`services/p2p/Cargo.toml:33`) brings a second, crates.io `libp2p-identity` 0.2.14 into `cc-p2p`, accepted in [../p2p-dependencies.md](../p2p-dependencies.md) | Script half INERT (masked). CI grep not reached as wired; replayed locally it passes: only `crates/libp2p/Cargo.toml:16` and the root pin declare `libp2p` |
| `check-no-env-reads.sh` | `env::var`, `var_os`, `vars`, `vars_os` (with or without `std::`) outside `#[cfg(test)]` items | Lexer-aware scan of `services/` and `crates/bootstrap/` only, excluding `*/tests/*` (`scripts/check-no-env-reads.sh:280-283`) | LIVE (standalone); not reached as wired |
| `check-no-http-import-path.sh` | HTTP client names (`reqwest`, `ureq`, `::Client::builder()`, hyper clients) in 12 listed import-path entries; `reqwest` in `services/chain/src` outside `checkpoint_sync.rs`; `reqwest` or `ureq` anywhere in `cargo tree -p cc-p2p` | File list `:22-35`, grep `:72-83`, cargo tree `:88-95` | LIVE (standalone); not reached as wired |
| `check-no-grpc-beacon-inproc.sh` | `tonic*` or `cc-proto` in `cargo tree --edges normal,build,dev --locked` of `cc-beacon-inproc` or `cc-beacon-import` | `:58-78`, with no unlocked fallback; mirrored by `crates/beacon-{inproc,import}/tests/no_grpc.rs` | LIVE (standalone); not reached as wired |
| `check-fork-schedule-walks.sh` | Any `.rs` file outside `crates/types` that walks the fork schedule, other than `services/p2p/src/fork_digest.rs`; also fails if that file is no longer detected as a walk. A walk is 3 or more distinct `.<fork>_fork_epoch` field reads plus a comparison or an array | Python over `crates/`, `services/` and `bin/`, comments stripped (`scripts/check-fork-schedule-walks.sh:31-77`) | LIVE (standalone); not reached as wired |
| `check-no-max-blobs-p2p.sh` | `MAX_BLOBS_PER_BLOCK` or `ELECTRA_FORK_EPOCH` text in `services/p2p` or `crates/libp2p` `*.rs`, tests included | grep; CI `deps` job only (not in the Makefile) | LIVE (standalone, exit 1: `services/p2p/tests/bpo_transition.rs:589`); not reached as wired |
| `check-no-remodelling.sh` | A proto `message` re-modelling BeaconBlock, BeaconState, Attestation, ExecutionPayload, ExecutionRequests or DataColumnSidecar | grep over `proto/`; CI `proto` job and `make proto` | LIVE |

Caption: INERT (masked) = the rule is in the script but never executes at HEAD because an
earlier rule exits 1. "Not reached as wired" = its CI step and Make target sit behind a step
that exits 1 (traced above); standalone results: [14](14-testing-and-enforcement.md#guards).

### The DAG guard (`scripts/check-crate-dag.sh`)

One run walks three phases in order. The early phase greps files, collects every hit, and
exits before `cargo metadata --locked` runs, so a forbidden manifest edit is named even before
the lockfile is refreshed. The cost is that one early failure hides every later rule.

```text
 bash scripts/check-crate-dag.sh            CI: clippy and deps jobs; make check-dag
 +----------------------------------------------------------------------------------------+
 | 1  fixture self-tests                                              :117-259            |
 |    7 expect-fail + 2 expect-pass trees (JWT/HTTP, chain<->p2p)      HEAD: pass         |
 +----------------------------------------------------------------------------------------+
 | 2  EARLY rules: grep over files, no cargo, every hit collected     :266-362            |
 |    a  HTTP/JWT keys in services/, crates/, bin/ Cargo.toml          HEAD: pass         |
 |    b  cc-storage, cc-storage-core -//-> cc-fork-choice              HEAD: pass         |
 |    c  cc-chain -//-> cc-p2p and cc-p2p -//-> cc-chain               HEAD: pass         |
 |    d  cc-store -> cc-types only                                     HEAD: pass         |
 |    e  no SignedBeaconBlock / BeaconState / DataColumnSidecar        HEAD: FAIL         |
 |       names in crates/store/src, crates/storage-core/src                               |
 |       hits: crates/storage-core/src/archive_write.rs:26,332,335,395,803                |
 |    any hit -> exit 1 at :365                    *** the run ends here at HEAD ***      |
 +----------------------------------------------------------------------------------------+
 | 3  METADATA rules: cargo metadata --no-deps --locked (:368)         HEAD: masked       |
 |    --check-unused only: allowlist entries with no edge, exit       :485-509            |
 |      scratch: FAIL, unused row cc-devnet-gen -> cc-config (:392)                       |
 |    rust-toolchain.toml channel == workspace rust-version           :528-545            |
 |    each member: [lints] workspace = true, rust-version.workspace,                      |
 |                 every path edge listed in allowed_deps()           :549-615            |
 |      scratch: FAIL, cc-p2p -> cc-spec-tests (dev) not in row :397                      |
 |    cc-chain <-//-> cc-p2p by package name                          :617-640            |
 |    libp2p* only in cc-libp2p; one git rev, also in docs + const    :642-807            |
 |    HTTP/JWT walk; no pub jwt / JwtSecret; manifest backup          :809-858            |
 |    any failure -> exit 1                                           :860-862            |
 +----------------------------------------------------------------------------------------+
```

Caption: `-//->` is a dependency the rule forbids; `<-//->` forbids both directions. Numbers
are lines in `scripts/check-crate-dag.sh`. Lines marked `scratch:` come from an uncommitted
copy of the script with the early exit removed. They are not a run of the script at HEAD.

What the masking hides at HEAD:

- **Rule (e) fails since `d3d7f60`.** `ArchiveWriter` decodes the block SSZ to check its
  claimed root (`ssz_block_root_matches`, `crates/storage-core/src/archive_write.rs:329-340`).
  The rule's only file exemption is `replay.rs` (comment lines are also skipped), and it does
  not skip `#[cfg(test)]` code, so the test-module lines `:395,803` count too.
- **A second failure sits behind it.** The scratch copy ran the metadata phase. Its only error
  was `cc-p2p: forbidden workspace dependency on cc-spec-tests`. That is the dev-dependency at
  `services/p2p/Cargo.toml:65`, which the `cc-p2p` allowlist row omits
  (`scripts/check-crate-dag.sh:397`). Lints, rust-version, the chain/p2p walk, the libp2p rules
  and the JWT walk all passed in that run. Fixing rule (e) alone will not turn the guard green.
- **`--check-unused`** in the same scratch copy reports one unused entry,
  `cc-devnet-gen -> cc-config` (row `:392`). This matches the plan's "recorded, not deleted"
  note.
- Checked directly: all 28 manifests set `[lints] workspace = true` and
  `rust-version.workspace = true`; `rust-toolchain.toml` pins `1.97.1` = `Cargo.toml:48`.

The allowlist is a ceiling over normal, dev and build edges, not a mirror. Rows are
append-only by convention ("never re-sort"). The only minimality check is the opt-in
`--check-unused`, which neither CI nor the Makefile runs. As-built rows against plan §1.5:

| Row | plan §1.5 target | As built (`scripts/check-crate-dag.sh`) |
|---|---|---|
| `cc-engine`, `cc-storage` | deleted | kept (`:401`, `:409`) |
| `cc-seam` | `cc-types` | `cc-proto` (`:418`) |
| `cc-storage-core` | `cc-types cc-state-transition cc-store cc-seam` | adds `cc-proto cc-bootstrap cc-config`, the gRPC boot/serve host (`:429`) |
| `cc-beacon-core` | `cc-bootstrap cc-config cc-chain cc-storage-core cc-seam` | adds `cc-proto cc-types cc-chain-core cc-engine-api`; no `cc-seam` (`:432`) |
| `cc-chain` | S1 row, no `cc-chain-core` | appends `cc-chain-core` (`:395`) |
| `cc-chain-core`, `cc-beacon-inproc`, `cc-beacon-import` | not in plan | new rows (`:423`, `:434-436`) |

### JWT / HTTP isolation

[ADR-R-03](../adr/ADR-R-03.md) restates the old Phase-3 rule ("the JWT never enters the
consensus process") as a property of an API surface. That ADR supersedes the Phase-3 ADR.

- Only `cc-engine-api` may **declare** a JWT signer or an Engine API HTTP client. Its `jwt`
  module is private (`crates/engine-api/src/lib.rs:23`), and the guard fails on any
  `pub mod jwt` or `pub use ... JwtSecret` there (`scripts/check-crate-dag.sh:833-842`).
- The rule covers declarations, not linkage. `jsonwebtoken` links into `cc-chain-core`,
  `cc-chain` and `cc-beacon-core` through `cc-engine-api` (`cargo tree -p cc-chain-core -i
  jsonwebtoken`), which is the ADR's intended outcome.
- `cc-chain` (checkpoint sync; test-only hyper) and `cc-bootstrap` (metrics server) may
  declare HTTP crates but never JWT crates. `cc-engine` may declare both: TRANSITIONAL.
  ADR-R-03 ends that with S1-A-06, which has landed (`services/engine/src/lib.rs:3-5`), but the
  crate stayed a member for A/B, so the exemption (`scripts/check-crate-dag.sh:54-57`) and both
  declarations (`services/engine/Cargo.toml:44,47`) remain, serving the `#[path]` re-include.
- `cc-chain-core`, `cc-storage-core`, `cc-beacon-core`, `cc-beacon-inproc` and
  `cc-beacon-import` are deliberately on neither list. Self-tests (`:153-181`) and fixtures
  (`scripts/fixtures/check-crate-dag/expect-fail/{chain-core,storage-core,beacon-core}-jwt`)
  pin this.
- Stale text: [../supply-chain.md](../supply-chain.md) still names `cc-engine` as the only
  holder (`docs/supply-chain.md:121,134`).

### Smaller guards and their blind spots

- **No env reads.** The header says only `crates/config` may read the environment, but the
  scan covers only `services/` and `crates/bootstrap/`; every other `crates/*` member and
  `bin/` is unscanned. Outside tests and benches the reads there are
  `crates/spec-tests/src/cache.rs:13,20` (TEST-ONLY harness) and the build script
  `crates/proto/build.rs:20`; `crates/storage-core/src/boot.rs:763` is `#[cfg(test)]`. clap
  `env = "..."` attributes (`services/p2p/src/main.rs:68-124`) read 14 `CC_P2P_*` variables at
  parse time with no `env::var` text; only the DEVNET-ONLY `run_devnet_mode` uses them (`:481`).
- **No HTTP on the import path.** The header (`scripts/check-no-http-import-path.sh:19-21`)
  promises all of `services/chain/src` except `checkpoint_sync.rs` and `main.rs`. The list
  covers eleven `crates/chain-core/src` entries plus `services/chain/src/lib.rs`, leaving out
  `engine.rs`, `fcu_driver.rs`, `ingest.rs`, `invalidation.rs`, `lib.rs`, `liveness.rs`,
  `pending_engine.rs`, `seed.rs` and `tick.rs`; `services/chain/src/run.rs` and `main.rs` get
  only the `reqwest` grep (`:72-83`). Since S1 the import path does reach an HTTP client, by
  design: the core thread calls the EL through DirectEngine (`crates/chain-core/src/engine.rs`,
  not listed) and `cc-engine-api`'s `reqwest` transport ([ADR-R-03](../adr/ADR-R-03.md),
  [ADR-P3-03](../adr/ADR-P3-03.md); [08](08-execution-engine.md)), so `cc-chain-core` links
  `reqwest` without declaring it. The header's promise (`:2`) no longer holds: the guard only
  stops the listed files naming or building a client. Half 2 falls back to an unlocked
  `cargo tree` (`:88`).
- **No gRPC in the in-process test crates.** Both crates repeat the check as a test
  (`crates/beacon-{inproc,import}/tests/no_grpc.rs`), relying on the optional edges above.
- **One fork-schedule walk.** `ChainConfig::fork_schedule` (`crates/types/src/config.rs:227-307`)
  is the authority; the one allowed outside walk builds the EIP-7892 digest boundary
  ([06](06-consensus-primitives.md)). A walk over only two forks would pass the heuristic.
- **max-blobs.** Its header claims `make deps`, but `Makefile:247` is `deps: deny check-dag`.
  Its one hit at HEAD is a test that asserts the literal itself.
- **Not guarded at all:**
  - `redb` staying inside `cc-store` (holds at HEAD; no script checks it).
  - The pure-consensus crates staying free of tokio, tonic, reqwest and redb (holds at HEAD;
    the allowlist covers only workspace path edges, and `deny.toml` bans nothing).
  - `cc-scheduler` taking no third-party crates (only a manifest comment,
    `crates/scheduler/Cargo.toml:13-17`; its zero workspace edges are an allowlist row,
    `scripts/check-crate-dag.sh:413`, masked at HEAD).
  - Cross-crate `#[path]` includes, which no Cargo-level rule sees
    (`services/engine/src/lib.rs:26`; `crates/fork-choice/src/e05_p009.rs:9`, test-only).

## Workspace lints

Every member opts in with `[lints] workspace = true` and inherits `rust-version` 1.97.1. The
workspace table (`Cargo.toml:53-67`):

| Lint | Level | Effect in CI |
|---|---|---|
| `unsafe_code` | deny | error everywhere |
| `unreachable_pub`, `missing_debug_implementations` | warn | error under `-D warnings` |
| `rust_2018_idioms` (group, priority -1) | warn | error under `-D warnings` |
| `unexpected_cfgs`, with `check-cfg = ['cfg(coverage)']` | warn | admits the `--cfg coverage` that `make coverage` injects |
| `clippy::all` (priority -1) | warn | error under `-D warnings` |
| `clippy::unwrap_used`, `expect_used`, `panic`, `todo`, `dbg_macro` | warn | error under `-D warnings` |

CI and `make clippy` run `cargo clippy --workspace --all-targets --all-features --locked --
-D warnings` (`.github/workflows/ci.yml:80`, `Makefile:15,150`), promoting every warning, test
code included. There is no `clippy.toml`. The test-module allow convention is in
[../dev-conventions.md](../dev-conventions.md).

Exceptions in the tree:

- `#![allow(unsafe_code)]` appears only in test code, five times: two `#[cfg(test)]` modules
  that call edition-2024 `set_var` (`crates/config/src/lib.rs:336`,
  `crates/storage-core/src/boot.rs:687`), two `crates/bootstrap` integration tests that send
  SIGTERM (`crates/bootstrap/tests/shutdown.rs:8`, `crates/bootstrap/tests/serve_integration.rs:3`),
  and a counting `GlobalAlloc` (`services/p2p/tests/hostile_input.rs:23`).
- `services/p2p/src/fault_mode.rs:16` puts the unwrap/expect allow on the whole module
  (comment: "tests only below"). It therefore also covers the DEVNET-ONLY production code
  above the test module, including one `.expect` at `:664`.
- Several crates carry `#![allow(missing_docs)]`. `missing_docs` is not in the workspace
  table, so these attributes have no effect.

## Build profiles

Only the root manifest sets profiles (`Cargo.toml:217-233`); there is no `.cargo/config.toml`.

| Profile | Settings | Built by |
|---|---|---|
| `release` (`:217-223`) | `opt-level = 3`, `debug = 1`, `lto = "thin"`, `codegen-units = 16`, `panic = "unwind"`, `overflow-checks = true` | the image: `cargo build --workspace --release --locked` (`Dockerfile:39`); `make release` (`Makefile:57`) |
| `dev` (`:225-229`; `test` inherits it) | `opt-level = 1` for members; `[profile.dev.package."*"]` builds every dependency at `opt-level = 3`; the rest are Cargo defaults, so overflow checks are on | `cargo build` / `test` / `clippy`, nextest in CI and `make` |
| `bench` (`:231-233`) | inherits `release`, with `debug = 2` | `cargo bench` |

Consequence: shipped binaries keep overflow checks, so an arithmetic overflow **panics**, as in
tests, instead of wrapping. With `panic = "unwind"` the panic unwinds to its thread or task
boundary instead of aborting the process; where each one lands (silent end, restart, or process
exit): [13](13-concurrency-model.md) (panic policy). No workspace lint flags unchecked
arithmetic: `clippy::panic` covers only the `panic!` macro, and `clippy::arithmetic_side_effects`
is not in the lint table above.

## Planned changes

Planned: per [`plan/architecture.md`](../../plan/architecture.md) §1. None of this exists at
HEAD.

- **Fewer members.** `cc-engine` (S1) and `cc-storage` (S2) are to be deleted. Their code has
  moved, or is to move, into `cc-engine-api` and `cc-storage-core`, which `cc-beacon-core`
  composes (§1.3, §1.5). `cc-attestation` and `cc-beacon-api` are to be deleted at S5, not
  consolidated (§1.3). All four are still members.
- **One new crate.** `cc-wire` (S3) would be the single SSZ+snappy codec. Today three
  hand-rolled copies exist (`services/p2p/src/reqresp/codec.rs`,
  `crates/libp2p/src/ssz_snappy_codec.rs`, `bin/serve-probe/src/codec.rs`). `cc-serve-probe`
  must keep its own copy (§1.4, [ADR-P4-12](../adr/ADR-P4-12.md)).
- **Allowlist targets** (§1.5): see [the table above](#the-dag-guard-scriptscheck-crate-dagsh).
  The plan's rule is that the allowlist changes only in the commit that moves code, and that a
  widening names the edge in its commit message.
- **Thin composer.** `bin/beacon-core` targets under 400 lines (§1.1); it has 608 `src`
  lines today, 592 of them in `boot.rs`.
- **Not yet scheduled in plan §1:** making `check-crate-dag.sh` green (rule (e) versus
  `archive_write.rs`, and the `cc-p2p -> cc-spec-tests` dev edge), and deleting the unused
  `cc-devnet-gen -> cc-config` row. Plan §1.5 calls the storage-core opaque-bytes grep
  load-bearing, so clearing rule (e) is a design choice (move the decode out of storage-core,
  or amend the rule by ADR), not an exemption to add. The TRANSITIONAL `cc-engine` HTTP/JWT
  allowance goes when that crate is deleted.
