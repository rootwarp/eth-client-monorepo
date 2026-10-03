# Testing and architectural enforcement

> **As built at `7d8833d`** (`develop`, 2026-08-16). Status labels and diagram legend: [README](README.md#status-labels).
> Target design: [`plan/architecture.md`](../../plan/architecture.md) §8 (also §1.5, §2.2, §5.6).

This page covers what the tests prove at each level, which tools and scripts back them, and how CI
and the Makefile run them. It also covers the guard scripts that keep the architecture from eroding.
The per-rule internals of the crate-DAG guard and the workspace lints are in
[02-crate-map.md](02-crate-map.md#mechanical-guards). How to run the stack, fetch vectors and run a
soak is in [../running.md](../running.md). Third-party dependency policy is in
[../supply-chain.md](../supply-chain.md).

Key facts at HEAD:

- **The canonical local gate is `make test`**, which runs
  `cargo nextest run --workspace --locked --profile ci` (`Makefile:16,67-69`). Only nextest applies
  `.config/nextest.toml`, which gives the three wall-clock latency gates the machine to themselves.
  Plain `cargo test` (`make test-cargo`) does not.
- **A local `make test` needs the Hoodi fixtures.** Two test files panic instead of skipping when
  the fixture cache is empty ([drift list](#ci-and-the-makefile)).
- **Two guards exit 1 locally:** `check-crate-dag.sh` and `check-no-max-blobs-p2p.sh`. The other ten
  `check-*.sh` scripts and `offhost-port-scan.sh --policy` exit 0. No CI run was inspected; every
  exit code on this page comes from a local run at `7d8833d`.
- **No test drives the production persist path (core thread + `ingest_block_blocking` with a real
  writer).** See [import -> durable and its gap](#import---durable-and-its-gap).
- **The compose proof covers only the health DAG (N3), the port policy and the compose URI overrides
  of a core-absent shape A.** "Healthy" means bound, not bootstrapped. Shape B has no compose proof.

## Test pyramid

```text
                 /\
                /8 \                 soak / acceptance  operator scripts; never in CI
               /----\
              /  7   \               compose proofs     CI compose (path-filtered); shape A health
             /========\              === above: outside nextest; below: make test
            /    6     \             import -> durable  twin + async ingest; not core thread
           /------------\
          /      5       \           in-process boot    cc-beacon-inproc, bin/beacon-core/tests
         /----------------\
        /        4         \         service tests      services/*/tests, crates/*/tests
       /--------------------\
      /          3           \       seam conformance   InProcess + Ipc; also make test-seam
     /------------------------\
    /            2             \     spec vectors       CI test (minimal) + CI vectors job
   /----------------------------\
  /              1               \   unit + property    ~1,600 test attributes outside tests/
 /________________________________\
```

Caption: one band per layer. The `====` band marks the edge of what `make test` (nextest) runs. A
grep of `#[test]` / `#[tokio::test]` attribute lines finds about 1,603 outside `tests/` directories
and about 463 inside them; treat both numbers as approximate.

| # | Layer | Where | Runs in | What it does not prove |
|---|---|---|---|---|
| 1 | Unit + property | in-crate `#[cfg(test)]`; proptest in `cc-store` (5 files) and `crates/engine-api/src/transport.rs` | CI `test`; `make test` | any cross-crate wiring |
| 2 | Spec vectors | `crates/types/tests`, `crates/crypto/tests/kzg.rs`, `crates/state-transition/tests`, `crates/fork-choice/tests/fork_choice.rs`, `services/p2p/tests/custody_subset.rs` | CI `test` (unit + minimal) and `vectors` (4 shards) | anything beyond pure consensus |
| 3 | Seam conformance | `crates/seam/src/conformance.rs` | CI `test` (first step); `make test-seam` | the `ArchiveWrite` contract (no case) |
| 4 | Service tests | `services/{chain,p2p,engine,storage}/tests`, `crates/{bootstrap,store,libp2p,config}/tests`, `bin/{devnet-gen,serve-probe}/tests` | CI `test`; `make test` | multi-process behaviour |
| 5 | In-process boot | `crates/beacon-inproc`, `bin/beacon-core/tests/boot.rs` | CI `test`; `make test` | import or persistence |
| 6 | import -> durable | `crates/beacon-import/tests/import_durable.rs` | CI `test`; `make test` | the core-thread persist path (`ingest_block_blocking`) |
| 7 | Compose proofs | `scripts/wait-healthy.sh`, `prove-mutual-health.sh`, `offhost-port-scan.sh --live` | CI `compose` (path-filtered); `make compose-proof` | bootstrap, import, shape B |
| 8 | Soak / acceptance | `scripts/soak-*.sh`, `restart-trials.sh`, `devnet/*.sh`, ... | operator, by hand | (not a gate) |

### Tests no gate runs

Nine `#[ignore]` tests run only on request (`cargo nextest run --run-ignored all`). The only tests
against a real geth are among them: `services/engine/tests/auth_container.rs` (4) and
`requests_32602.rs` (3) need an EL on `127.0.0.1:8551`, so EL interop has no automated coverage at
any layer. The CC-1H early gate (`crates/state-transition/tests/cc1h_early_gate.rs:74`) and the
store open-scan timing (`crates/store/src/invariants.rs:1907`) are manual. Under `make coverage` the
CC-1H mid gate is skipped too (`services/chain/tests/offline_replay.rs:834-837`).

### Unit and property tests

Unit tests sit next to the code. Policy-heavy crates test their policies directly. For example,
`cc-scheduler` covers queue overflow and scheduler-lane order with no runtime
(`crates/scheduler/src/manager.rs:388-571`). The chain-core scheduler test `slot_tick_is_never_shed`
(`crates/chain-core/src/core.rs:2446`) doubles as seam conformance case 11. Property tests
(proptest) cover the `cc-store` key, block, column, meta and window codecs, and the engine-api
transport. Two benches use `harness = false`: the criterion `crates/crypto/benches/kzg.rs` and the
hand-timed `crates/fork-choice/benches/head.rs`. Because nextest cannot list them, the coverage
targets do not pass `--all-targets` (`Makefile:18-24`).

### Spec vectors

| Piece | As built |
|---|---|
| Pin | `spec-vectors.lock`: `ethereum/consensus-specs` tag `v1.7.0-alpha.13`, SHA-256 of four artifacts (general, mainnet, minimal, comptests) |
| Fetch | `make vectors` = `scripts/fetch-spec-vectors.sh`: download, verify against the lock, unpack outside the build tree ([ADR-11](../adr/ADR-11.md)). Never implicit. Details: [../running.md](../running.md#spec-vectors) |
| Harness | `cc-spec-tests` (`crates/spec-tests`), zero workspace deps. Resolves `${SPEC_VECTORS_CACHE:-$HOME/.cache/eth-consensus-spec-vectors}`, checks four `.complete-*` markers, never fetches (`crates/spec-tests/src/lib.rs:1-10`). Layout file `spec-vectors-layout.md` (regenerate with `make vectors-layout`, `Makefile:262-263`) |
| Missing cache | tests fail with `run scripts/fetch-spec-vectors.sh` (`crates/spec-tests/src/error.rs:8`); no silent skip |
| Consumers via harness | `crates/types/tests/{ssz_static,ssz_generic,custody}.rs`, `services/p2p/tests/custody_subset.rs` (dev-deps) |
| Self-resolving runners | state-transition (`operations`, `sanity`, `epoch_processing`, `finality`, `random`, `rewards`, `shuffling`), fork-choice, crypto `kzg`. Their DAG rows forbid `cc-spec-tests`, so they re-implement cache lookup and `include_str!` the lock, layout and skip list (`crates/state-transition/tests/operations.rs:40-42`) |
| Handler coverage | `assert_handler_coverage` requires set equality between handlers on disk and handlers the runner knows (`crates/types/tests/ssz_static.rs:172-185`) |
| Runner coverage | per runner only. Nothing checks that every runner on disk has an owner: `Vectors::runners` (`crates/spec-tests/src/case.rs:217`) has no caller outside its unit tests. A grep finds no consumer for the Fulu `fork`, `light_client`, `merkle_proof`, `sync`, `transition` or `fast_confirmation` runners, so an empty skip list does not mean total coverage. Planned: [`plan/architecture.md`](../../plan/architecture.md) §5.6 |
| Skip list | [../spec-vectors-skiplist.md](../spec-vectors-skiplist.md): one section per owning requirement (CC-10, CC-11, CC-12, CC-13, CC-15). **Every section is empty at HEAD.** A stale entry is a hard failure |
| Hoodi fixtures | `scripts/fetch-hoodi-fixtures.sh` into `HOODI_FIXTURES_CACHE`. Only manifests (expected roots and SHA-256 pins) and configs are committed; the SSZ bytes are fetched. Contract: [`crates/types/tests/fixtures/README.md`](../../crates/types/tests/fixtures/README.md) |

CI splits the suites by nextest filter. The `test` job runs everything except `ssz_generic`,
`custody` and mainnet `ssz_static`. The `vectors` job runs exactly those plus `kzg`, built with both
KZG backends (`cc-crypto/kzg-c-kzg,cc-crypto/kzg-rust-eth-kzg`), as `--partition count:i/4`
(`.github/workflows/ci.yml:214-219,370-377`). For spec vectors, both jobs cache only the downloaded
tarballs (`_dl/`), keyed on `hashFiles('spec-vectors.lock')`.

### Seam conformance

`crates/seam/src/conformance.rs` holds eleven named cases. The header table maps each case to a
trait method or an overflow policy ([ADR-R-01](../adr/ADR-R-01.md); Policy A blocks <= 2 s then
returns `Backpressure`, B terminates the slow consumer, C drops and reports, D never sheds). Cases
1-8 run one shared assertion on both impls. The status column is that of the production arm each
case stands for (shape A).

| Cases | Method | Asserted on | Status of the arm in production |
|---|---|---|---|
| 1, 6 | `submit_gossip` (Policy A; closed handle) | `InProcess` and `Ipc` | E1 object/verdict IDLE: one caller (`services/p2p/src/chain_stream/client.rs:366`), 0 gossip topics |
| 2, 6 | `notify_data_available` (Policy A; closed handle) | `InProcess` and `Ipc` | E2 DEAD-AS-WIRED: no caller in `services/p2p` |
| 3, 7 | `publish` (Policy C; closed handle) | `InProcess` and `IpcEgress` | DEAD-AS-WIRED: p2p drops the `IpcEgress` (`client.rs:239`, `_egress`) |
| 4 | `update_view` | `InProcess` and `IpcEgress` | DEAD-AS-WIRED (the same dropped `IpcEgress`). Real views reach p2p through `IpcUpward` (E1 session + view, LIVE), which no case covers |
| 5, 8 | `submit_column_sidecar` (oversize; closed handle) | `InProcess` and `Ipc` | E1 column DEAD-AS-WIRED: no p2p producer |
| 9 | column admit, split by impl | `InProcess` head-of-line-free; `Ipc` on a live session | E1 column DEAD-AS-WIRED |
| 10 | Policy B | `services/chain/tests/events.rs:242` | event ring IDLE (no in-tree subscriber) |
| 11 | Policy D | `crates/chain-core/src/core.rs:2446` (the header still says `services/chain/src/core.rs`) | tick scheduler lane LIVE (iff core) |

`InProcess` is TEST-ONLY. In shape B every E1/E2 arm is ABSENT. At HEAD p2p subscribes to no gossip
topics; the plan schedules gossip wiring for a later stage. The file has 18 test attributes, two of
them `start_paused = true` (virtual time).

- Case 9's header says `Ipc` column admit never returns `Backpressure`. The code can return
  `Backpressure{1024}`: `Ipc::submit_column_sidecar` uses the gossip 2 s send timeout
  (`crates/seam/src/ipc.rs:1177-1196`). This is latent because no production caller submits columns.
- `ArchiveWrite` has no conformance case. The trait documents Backpressure; the implementation
  blocks without a deadline. A conformance case is the mechanism that would have caught this.
- InProcess Policy A uses cc-seam's own copies of the import scheduler lane depth and timeout
  (`crates/seam/src/in_process.rs:24-28`), so the suite cannot catch drift in the scheduler's
  constants.
- Further gaps (Ipc's local `out_tx` bound is never filled, case 9 is happy-path only, the
  production adapters are outside the suite):
  [03-internal-contracts.md](03-internal-contracts.md#55-conformance-suite).

### Service tests

`crates/chain-core` has no `tests/` directory. Its integration tests live in `services/chain/tests`
and reach chain-core through the `cc_chain` re-exports (`services/chain/src/lib.rs:33-50`). Four of
its 18 files bind a real tonic server on loopback (`lifecycle.rs`, `offline_replay.rs`,
`is_optimistic.rs`, `engine_blackhole_liveness.rs`); the rest drive `spawn_core_thread`,
`ChainServiceImpl` or `CoreHandle` directly. The ones a maintainer is most likely to need:

| Test | Proves |
|---|---|
| `import_engine_fork_choice.rs` | plan §8.2 S1 shape: when `newPayload` times out, the core thread waits out the deadline (80 ms in the test, 8 s in production), then the block defers into pending_engine ([ADR-P3-05](../adr/ADR-P3-05.md)) and fork choice is left unmutated. Engine failures defer the block rather than reject it; re-drive depends on an EL health transition |
| `engine_blackhole_liveness.rs` | an EL sink that never answers holds the core thread for the full deadline on every import. With a hang longer than 8 s x N, liveness misses N=3 times and aggregate `""` goes NOT_SERVING ([ADR-R-04](../adr/ADR-R-04.md)). One deadlined RPC under production caps does not flip aggregate health (`:1-13`) |
| `lifecycle.rs` | the readiness gate: self-name SERVING while aggregate is NOT_SERVING until `mark_ready`. The test calls `mark_ready` itself. It does not exercise `services/chain/src/run.rs`, which marks ready alongside the bind (the handle is sent just before bind, `crates/bootstrap/src/serve.rs:260-266`; `run.rs:421`). `crates/bootstrap/tests/shutdown.rs` runs the real `cc-chain` binary (core-absent) and relies on that ready-alongside-bind to reach SERVING (`:167-188`) |
| `offline_replay.rs` | a 40-slot Hoodi-derived chain through the gRPC `ImportBlock` surface |
| `events.rs`, `p2p_stream_contract.rs`, `cc28_no_http_import_path.rs` | Policy B, the P2pStream contract, and a test-side copy of the HTTP import-path rule |
| `services/chain/src/checkpoint_sync.rs` (in-file `mod tests`, `:1239`; 21 tests) | checkpoint sync against a loopback hyper fixture server (`run_fixture_server`, `:1672`): https required except on loopback, fail-over past dead ports, oversize bodies fail closed, a block/state race re-fetches the whole triple, a verification failure is not network-retried, and the genesis validators root changes the signing domain (skips without Hoodi fixtures). No test completes a fetch with `checkpoint_root` unset; that trust-the-first-provider path has only a `warn` log (`:1054-1060`) |

The liveness test, like the compose proof below, stops at the aggregate-health flip.
Liveness flips aggregate health; nothing in this repository restarts a process on that signal.

`services/p2p/tests/hostile_input.rs` fuzzes every Fulu topic family with 10,000 random and 10,000
mutated seed-corpus inputs (the corpus comes from `gen_hostile_corpus`), asserting no panic and a
peak-heap bound; it also covers req/resp framing in both directions (`:1-19`).
`crates/bootstrap/tests/{shutdown,serve_integration}.rs` send real SIGTERMs to cover the drain
sequence.

### In-process boot harness

`cc-beacon-inproc` is TEST-ONLY: a proto-free twin of the shape B open path. `boot_in_process` runs
`Store::open` and then the durable-empty probe. It starts no writer and no server, and it
re-implements storage-core's I-node-id stamping and refusal (`crates/beacon-inproc/src/boot.rs`).

- `crates/beacon-inproc/tests/inproc_boot.rs:34-83`: one TempDir and one redb. The phases are
  exactly `[Open, DurableSet]`. A second live opener fails on the redb lock, and a second node key
  fails I-node-id.
- `bin/beacon-core/tests/boot.rs:42-84` drives `cc_beacon_core::boot_in_process`, a library function
  that only tests call. `run()` repeats its steps inline after `ensure_node_key` (`open_and_stamp`,
  `durable_set`, `start_writer`; `bin/beacon-core/src/boot.rs:405-423`). The tests prove that open
  completes before the writer starts, that there is exactly one writer, and that a second node key
  fails I-node-id. They do not repeat the redb-lock case.
- `crates/storage-core/src/rollback_rehearsal.rs` (TEST-ONLY) writes a store with the current writer
  and reopens it with the old open gates. The procedure is [../s2-rollback.md](../s2-rollback.md).

### import -> durable and its gap

`crates/beacon-import/tests/import_durable.rs` (TEST-ONLY crate `cc-beacon-import`) has two tests.
`import_on_block_then_ingest_writes_durable_rows` (`:231`) boots the twin and starts a real writer
(`process_fatal = false`). It persists the genesis anchor itself as a self-parent (`:286-299`),
applies one minimal-preset child with a direct `on_block`, and persists it with async
`ingest_block`. It asserts `canonical[slot] == root`, the stored body, an advanced `WriteCursor`,
and that the rows and cursor survive a reopen. `reimport_ancestor_keeps_durable_head` (`:381`)
persists A then B and re-ingests A. It asserts that both canonical rows and both bodies remain; it
checks neither the cursor nor a reopen.

The production path (edge N1 blocks) is IDLE in shape B and ABSENT in shape A, where chain runs with
`archive = None` (`services/chain/src/run.rs:368-378`). It is IDLE for two independent reasons: no
new block passes the DA gate, and the checkpoint anchor is never persisted. The test avoids both. It
persists the anchor itself, which production never does after checkpoint sync, and it builds fork
choice with `HarnessAvailability` (admits every root, so no DA gate), `AcceptEngine` (always VALID)
and `NoVerification` (`:40-49,278-279,309`). A green run therefore says nothing about either reason.

```text
 TEST  import_durable.rs (tokio test task)      PRODUCTION CODE  shape B, bin/beacon-core
 -----------------------------------------      -----------------------------------------
 cc_beacon_inproc::boot_in_process (twin)       run(): open_and_stamp, durable_set
 start_writer_from_store(.., false)             start_writer(opened, metrics, true)
 setup: ingest_block(anchor, self-parent)       (no path persists the checkpoint anchor)
                                                CoreHandle::import_block -> import scheduler lane
                                                core thread "chain-core":
 cc_fork_choice::on_block(.., NoVerification)     import_block_with_early -> on_block
   HarnessAvailability (DA always true)             PeerDasAvailability (gate never opens)
   AcceptEngine        (newPayload VALID)           DirectEngine -> EL, 8 s deadline
 ArchiveWrite::ingest_block(..).await             finish_imported -> ingest_block_blocking
   bind_ingest_block           (same code)          bind_ingest_block           (same code)
   submit_p0_committed: P0 send().await             blocking_submit_p0_committed: P0
                                                    blocking_send mpsc(32), no deadline
 asserts canonical, block, cursor, reopen       status: IDLE in B; ABSENT in A
 ======== shared: bind_ingest_block, storage-core writer task, store.redb ========
```

Caption: `->` marks a call sequence, not a legend edge. The columns share the ArchiveWriter bind
(continuity, root binding, dedup; `crates/storage-core/src/archive_write.rs:240-252`), the writer
task and the store. They differ in boot, DA availability, execution engine (`AcceptEngine` vs
`DirectEngine`), anchor persistence and the submit leg.

The chain-core import tests use doubles (`FailThenRecord`, `RecordDurable`,
`crates/chain-core/src/import.rs:1866,1975`). ArchiveWriter's own tests call only async
`ingest_block` (`crates/storage-core/src/archive_write.rs:375` onward). The only `archive: Some(..)`
in the tree is production `bin/beacon-core/src/boot.rs:448`. No integration test under `*/tests`
names `PeerDasAvailability`; the 15 files that name an availability use `HarnessAvailability`,
which admits every root (`crates/fork-choice/src/da_seam.rs:198-212`). The one test that gets the
production gate through `spawn_core_from_checkpoint` (`services/chain/tests/lifecycle.rs:698`)
imports only the anchor, which returns Duplicate. The PeerDAS gate and the pending_da re-drive are
covered only by chain-core unit tests (`crates/chain-core/src/da.rs:738,812`).

Because the anchor is never persisted, as wired the first child block after a checkpoint sync would
fail the continuity bind with INVALID_ARGUMENT after fork choice had already been mutated
(`crates/chain-core/src/import.rs:988-990`). This is traced, not observed. A real-writer test of the
production path is exactly what would catch it. See [05-block-lifecycle.md](05-block-lifecycle.md)
and [07-storage.md](07-storage.md).

### Compose proofs

The CI `compose` job (`.github/workflows/ci.yml:441-485`) always runs on push. On a PR it runs only
when `Dockerfile`, `docker-compose.yml`, `crates/bootstrap/` or one of the four health scripts
changed. Locally, `make compose-proof` runs the same sequence:

1. `docker compose build` and `up -d`, then `wait-healthy.sh`: poll until the six CC services report
   `healthy` (default 90 s).
2. `check-compose-uri-overrides.sh --runtime`: the URI overrides and the storage identity mount
   exist inside the running containers.
3. `prove-mutual-health.sh`: stop `chain`. Within 15 s all five dependents report aggregate
   NOT_SERVING (in-container `grpc-health-probe`) and `cc_peer_health{peer="chain"} == 0`. Start
   `chain`; within 30 s all six are SERVING and every `cc_peer_health` gauge is 1.
4. `offhost-port-scan.sh --live`: a scanner on an isolated docker network must reach none of
   9001-9006 and must reach 9101-9106 only on 127.0.0.1.

What this proves is edge N3 (the health DAG), the port policy and the compose URI overrides. Chain
marks itself ready alongside the bind, so with the default `checkpoint_providers = []` the proof
passes on a **core-absent** stack. It says nothing about bootstrap, import or the EL. The `el`
container is outside the six. Shape B has no compose service and no proof. Procedure:
[../running.md](../running.md#mutual-health-proof).

### Soak and acceptance tooling

These scripts are operator-run and never run in CI. They never run in a deployed process, so the
status labels do not apply to them; the compose overlay in the last row is TRANSITIONAL. Several
were written for earlier phases and assert signals that HEAD does not produce. Running one is not a
discharge: a soak clause is not discharged until its discharging evidence is recorded
([ADR-P4-11](../adr/ADR-P4-11.md)).

| Script | Measures | Note at HEAD |
|---|---|---|
| `scripts/soak-sampler.sh` | per-slot CSV (`--out`): local `GetHead` vs a reference provider; one recorded run is committed at `docs/soak/head-agreement.csv` | as wired, under default compose `GetHead` answers NOT_BOOTSTRAPPED |
| `scripts/soak-report.sh` | histogram deltas and clause tables over a soak window | its restart clause reads `cc_storage_following_head` (next row) |
| `scripts/restart-trials.sh` | 20 SIGKILL restarts; waits for `cc_storage_following_head == 1` | as wired, would fail: `set_following_head` is `#[cfg(test)]` (`crates/storage-core/src/metrics.rs:902-904`) |
| `scripts/el-restart-drills.sh`, `el-snapshot-restore.sh` | EL restart shapes; geth snapshot restore | local compose + EL |
| `scripts/storage-plateau.sh` | 24 h slope of `cc_storage_bytes_total` as a % of the plateau; prune/written ratio | as wired, would report a flat 0 % slope: `cc_storage_bytes_total` has no production writer and stays at its per-class seed of 0 (`crates/storage-core/src/metrics.rs:415,531-535,788`; no assignment outside `metrics.rs`), and the script floors the plateau at 1.0 (`scripts/storage-plateau.sh:158-160`) |
| `scripts/phase-3-acceptance.sh`, `phase2-soak-entry-checks.sh`, `s0-ab-baseline.sh`, `block-backfill-monitor.sh`, `probe-providers.sh` | phase acceptance skeleton; pre-soak checks; A/B baseline scrape; long backfill run; checkpoint provider probe | |
| `devnet/smoke.sh`, `devnet/faults.sh`, `devnet/scenarios/withheld-column.sh` | p2p-only self-devnet (publisher, node-a, node-b, nginx anchor) | exercises the DEVNET-ONLY p2p path; no chain, storage-service or EL containers |
| `docker-compose.engine-blackhole.yml` | replaces `engine:9004` with `cc-engine-blackhole` | TRANSITIONAL. It cannot park the core thread, because chain no longer dials the engine container. The real demonstration is `engine_blackhole_liveness.rs` |

## Tooling binaries

The offline tools are listed in [01 Offline tools](01-deployment-and-processes.md#offline-tools)
with their purposes in [02-crate-map.md](02-crate-map.md) (ABSENT from A and B). The black-hole
overlay is described in [01](01-deployment-and-processes.md#the-engine-blackhole-overlay).
Test-relevant facts:

| Tool | Role in testing |
|---|---|
| `cc-devnet-gen` (`bin/devnet-gen`) | genesis, keys and a pre-signed chain with column sidecars for `devnet/` (`devnet/devnet.toml`: minimal preset, 64 validators, 512 slots); its output feeds the DEVNET-ONLY p2p path |
| `gen_hostile_corpus` (`services/p2p/src/bin`) | writes the seed corpus under `services/p2p/tests/fixtures/corpus` that `hostile_input.rs` mutates; `scripts/corpus-from-capture.sh` turns a gossip capture into corpus entries |
| `cc-store-bench` (`bin/store-bench`) | CC-40 prune-under-load falsifier: p99 prune-commit latency and file/live-set ratio for `--layout {sharded,flat}` |
| `cc-serve-probe` (`bin/serve-probe`) | wire-only advertised-window prober (Status v2 + ByRange) with its own codec ([ADR-P4-12](../adr/ADR-P4-12.md)) |
| `scripts/bench-kzg.sh` | criterion KZG matrix; regenerates [../kzg-benchmark.md](../kzg-benchmark.md) |

## CI and the Makefile

`.github/workflows/ci.yml` has seven jobs. Their ids must equal `CI_JOBS` in `Makefile:40`.
`check-ci-job-list.sh` enforces this, but it compares only the ids, not the steps.

```text
 on: pull_request (opened, synchronize, reopened, labeled, unlabeled) | push: main, develop
 concurrency: one run per ref, cancel-in-progress
                                                             make target                exit
 |-- fmt      cargo fmt --all --check                        fmt-check [L]              0
 |-- clippy   cargo clippy --all-targets --all-features      clippy [L]                 -
 |   |-- check-crate-dag.sh                                  check-dag [L][D]           1
 |   |-- check-no-env-reads.sh                               check-env [L]              0
 |   |-- check-no-http-import-path.sh                        check-http [L]             0
 |   |-- check-no-grpc-beacon-inproc.sh                      check-inproc-grpc [L]      0
 |   |-- check-gha-sha-pins.sh                               check-gha-pins [L]         0
 |   |-- check-ci-job-list.sh                                check-ci-jobs [L]          0
 |   |-- check-compose-uri-overrides.sh                      check-compose-uris [L]     0
 |   |-- check-fork-schedule-walks.sh                        check-fork-schedule [L]    0
 |   |-- offhost-port-scan.sh --policy                       check-offhost-policy [L]   0
 |   `-- check-adr-resolver.sh                               check-adr-resolver [L]     0
 |-- test     cargo test -p cc-seam --locked                 test-seam [C]              -
 |   |-- fetch-spec-vectors.sh, fetch-hoodi-fixtures.sh      vectors (spec only)        -
 |   `-- nextest --profile ci, minus vectors-job suites      test [C] (no filter)       -
 |-- proto    proto-breaking-against.sh --self-test          proto-breaking-against [P] 0
 |   |-- proto-breaking-against.sh (resolve baseline)        none                       -
 |   |-- buf-action: lint, format, breaking                  proto-lint/-fmt-check [P]  -
 |   `-- check-no-remodelling.sh                             check-remodelling [P]      0
 |-- vectors  4 shards: nextest, both KZG backends           none                       -
 |-- deps     cargo deny check (4 check kinds)               deny [D]                   -
 |   |-- check-crate-dag.sh (second copy)                    check-dag [D]              1
 |   |-- check-no-max-blobs-p2p.sh                           none                       1
 |   `-- grep: libp2p* keys outside crates/libp2p            none                       -
 `-- compose  PRs: path filter; build, up, wait-healthy,     compose-proof              -
              uri --runtime, prove-mutual-health, offhost --live
```

Caption: tree lines mean "job runs step", not runtime edges. `[L]` = in `make lint`, `[P]` = in
`make proto`, `[D]` = in `make deps`, `[C]` = direct prerequisite of `make ci`
(`lint test-seam test proto deps`, `Makefile:311-312`). `exit` = standalone local exit code at
`7d8833d`; `-` = not run for this page; `none` = no Makefile target. `proto-breaking` (buf breaking
against `origin/develop`) exists but is in no aggregate target.

| Job | Trigger and shape | Intended gate (per file comments) |
|---|---|---|
| `fmt`, `clippy`, `test` | every run | "core CI gates" |
| `proto` | every run; `buf breaking` skipped on a PR labelled `buf skip breaking` ([ADR-05](../adr/ADR-05.md) FILE category) | required once CC-07b lands |
| `vectors` | matrix `vectors (1/4)` .. `(4/4)`, `fail-fast: false` | `ci.yml`: merge-blocking once branch protection lists all four. `Makefile`: "non-required" |
| `deps` | every run | required once branch protection includes it |
| `compose` | always on push; PRs only for the path filter | non-required |

Which checks are actually required lives in GitHub branch protection, outside the repository. Drift
between `ci.yml` and the Makefile, at HEAD:

- `make deps` is `deny check-dag` (`Makefile:246-247`). It lacks the max-blobs guard and the libp2p
  manifest grep that the CI `deps` job runs. The max-blobs script header claims it is wired into
  `make deps`.
- `make proto` omits the breaking check. `proto-breaking` exists but compares against
  `origin/develop`, while CI derives its baseline from the event.
- `make vectors` only fetches. No target reproduces the `vectors` job's dual-backend, 4-shard run.
- `make test` has no `-E` filter. It runs the CI `test` set plus the `vectors` suites, unsharded and
  with the default KZG backend only. It needs the vector cache either way (`make vectors`).
- No Make target fetches the Hoodi fixtures; CI runs `fetch-hoodi-fixtures.sh` in both `test` and
  `vectors`. Tests disagree on a missing cache, so a local `make test` fails until
  `bash scripts/fetch-hoodi-fixtures.sh` has run:
  - skip when `HOODI_FIXTURES_CACHE` is unset: the fixture tests in `crates/types/tests`
    (`crates/types/tests/fixtures/mod.rs:257-266`) and `services/chain/tests/timing.rs:115-121`;
  - try `$HOME/.cache/cc-hoodi-fixtures` and skip only if the file is missing:
    `crates/store/tests/parent_root_offset.rs:48-63` and `crates/crypto/tests/bls.rs:464`;
  - try the same default path and panic: `services/chain/tests/offline_replay.rs`
    (`:101-110,199-213`; three tests, including the `cc1h_mid_gate_with_fork_choice_clone` latency
    gate) and `services/engine/tests/encode_real_hoodi_payload.rs` (`:65-74`; two tests).
- `check-dag` is a prerequisite of `make lint`, which is a prerequisite of `make ci`. As wired, both
  fail no later than that guard on this commit. By the same reasoning the CI `clippy` and `deps`
  jobs would fail on this commit. No CI run was inspected.

### nextest profile

`.config/nextest.toml` defines profile `ci` with `retries = 0` and
`failure-output = "immediate-final"`. Three wall-clock gates are overridden with
`threads-required = 'num-test-threads'` and the `exclusive-perf-gates` group:
`cc1h_mid_gate_with_fork_choice_clone` (`services/chain/tests/offline_replay.rs:839`),
`commit_latency_during_snapshot_within_10_percent` (`crates/storage-core/src/replay.rs:1198`) and
`write_behind_p99_during_block_backfill_within_10_percent`
(`crates/storage-core/src/backfill.rs:1332`). The same override is repeated for `profile.default`
(`:23-26`), so a plain `cargo nextest run` isolates them too. The file records why: the two
commit-latency gates (about 200 us p99 against a 10 % / +2 ms budget) measured 4-18 ms under
peer-process load and failed about two runs in three.

`make test`, not `make test-cargo`, is the canonical gate, for two reasons. `cargo test` ignores
`.config/nextest.toml`, so the latency gates share the CPU with every other test. And
`crates/bootstrap/tests/serve_integration.rs` sends SIGTERM to its own pid; its safety comment
relies on nextest's one-process-per-test model (`:480-481`). Under `cargo test`, as wired, the
signal would reach a process that also runs sibling tests.

`make coverage` / `coverage-html` run `cargo llvm-cov nextest --profile ci` locally
(`Makefile:87-134`); no CI job runs coverage.

## Guards

Every `scripts/check-*.sh` guard, with where it runs. Rule details and blind spots are in
[02-crate-map.md](02-crate-map.md#the-dag-guard-scriptscheck-crate-dagsh) and
[Smaller guards and their blind spots](02-crate-map.md#smaller-guards-and-their-blind-spots).

| Script | Rule it enforces | CI job | Make target | Local exit at `7d8833d` |
|---|---|---|---|---|
| `check-crate-dag.sh` | crate-DAG allowlist, lints and toolchain pin, JWT/HTTP isolation, chain<->p2p ban, storage rules (`cc-storage` and `cc-storage-core` never depend on `cc-fork-choice`), libp2p confinement ([02](02-crate-map.md#the-dag-guard-scriptscheck-crate-dagsh)) | clippy, deps | `check-dag` | 1: opaque-bytes rule hits `crates/storage-core/src/archive_write.rs:26,332,335,395,803` (since `d3d7f60`). The metadata rules never run because the early rule exits first (`scripts/check-crate-dag.sh:364-366`); with it bypassed, they still fail on `cc-p2p -> cc-spec-tests` (dev, since `5bb1745`) |
| `check-no-env-reads.sh` | no `env::var` family outside `#[cfg(test)]`; scans only `services/` and `crates/bootstrap/` | clippy | `check-env` | 0 |
| `check-no-http-import-path.sh` | no HTTP client on the import path or in `cc-p2p`'s tree | clippy | `check-http` | 0 |
| `check-no-grpc-beacon-inproc.sh` | no `tonic*` / `cc-proto` under `cc-beacon-inproc` or `cc-beacon-import` (repeated as tests in `crates/beacon-{inproc,import}/tests/no_grpc.rs`) | clippy | `check-inproc-grpc` | 0 |
| `check-fork-schedule-walks.sh` | one fork-schedule walk outside `crates/types`: `services/p2p/src/fork_digest.rs` | clippy | `check-fork-schedule` | 0 |
| `check-gha-sha-pins.sh` | every `uses:` pinned to a 40-hex commit SHA | clippy | `check-gha-pins` | 0 |
| `check-ci-job-list.sh` | `CI_JOBS` == `ci.yml` job ids (ids only) | clippy | `check-ci-jobs` | 0 |
| `check-compose-uri-overrides.sh` | compose URI overrides and the storage identity mount; `--runtime` on a live stack | clippy; compose (`--runtime`) | `check-compose-uris`, `check-compose-uris-runtime` | 0 (static mode) |
| `check-adr-resolver.sh` | every ADR citation resolves to a file | clippy | `check-adr-resolver` | 0 |
| `check-no-remodelling.sh` | no proto `message` re-models a consensus container | proto | `check-remodelling` | 0 |
| `check-no-max-blobs-p2p.sh` | no `MAX_BLOBS_PER_BLOCK` / `ELECTRA_FORK_EPOCH` text in p2p or libp2p `*.rs`, tests included | deps | none | 1: `services/p2p/tests/bpo_transition.rs:589` (since `24a0d62`) |
| `offhost-port-scan.sh` | `--policy`: no published gRPC port, metrics on 127.0.0.1; `--live`: the same from an isolated scanner; `--scan`: compose-flag replay | clippy (`--policy`); compose (`--live`) | `check-offhost-policy`, `check-offhost-scan-live`, `check-offhost-scan` (`Makefile:200-201`) | 0 (`--policy`); `--live` and `--scan` not run |

Guards never run in a deployed process, so the status labels do not apply. Two of the keys
`check-compose-uri-overrides.sh` requires are dead config kept for this gate (`CC_CHAIN_ENGINE_URI`,
`CC_ENGINE_P2P_URI`).

The DAG guard fails on a design fact: `ArchiveWriter` decodes the block it persists to bind its root
(`crates/storage-core/src/archive_write.rs:330-335`). Fixing the early rule alone will not turn it
green (see `cc-p2p -> cc-spec-tests`). The max-blobs guard fails on a test that names the banned
literal in order to assert its absence (`services/p2p/tests/bpo_transition.rs:589`). Its grep
includes `tests/`.

### cargo-deny and workspace lints

`deny.toml` is checked by the CI `deps` job and `make deny` with
`cargo deny check advisories bans licenses sources` (not run for this page). Advisories: yanked =
deny, one ignore (`RUSTSEC-2024-0436`, paste, transitive). Licenses: permissive allowlist, private
workspace crates ignored. Bans: `wildcards = "deny"`, `multiple-versions = "warn"`. Sources:
crates.io only, plus `allow-git` for `https://github.com/libp2p/rust-libp2p`.

Policy, coverage gaps and the libp2p pin are in [../supply-chain.md](../supply-chain.md). That
page's "Gate" section predates the max-blobs and libp2p-grep steps in `deps`, and its JWT-holder
wording still names `cc-engine` rather than `cc-engine-api`. Workspace lints (`unsafe_code = deny`,
clippy `unwrap_used` / `expect_used` / `panic` / `todo` / `dbg_macro` promoted by `-D warnings`) are
in [02-crate-map.md](02-crate-map.md#workspace-lints).

## Planned changes

Planned: per [`plan/architecture.md`](../../plan/architecture.md) §8. None of the items below exists
at HEAD unless it is marked as built.

- **S1 `import -> engine -> fork-choice` in one process.** Built:
  `services/chain/tests/import_engine_fork_choice.rs`.
- **S2 `import -> durable`.** The plan's shape is one TempDir, one redb, one `beacon-core`, and no
  gRPC. As built, the test uses the `cc-beacon-inproc` twin, a direct `on_block` and async
  `ingest_block` (see [the gap above](#import---durable-and-its-gap)). Driving it through
  `cc-beacon-core` and the core thread would close that gap.
- **S3 `gossip receipt -> durable`.** `gossip_block_reaches_durable_storage` would boot a
  `BeaconCore` and inject at `ChainIngress::submit_gossip`. It does not exist (no hits in the tree).
  The plan's decision D-12 targets the seam, not the wire. A wire-level test through gossipsub stays
  a devnet acceptance clause.
- **Three properties to preserve (§8.3).** (1) Seam conformance on both impls in CI: holds for cases
  1-8. (2) `cc-scheduler` testable without a swarm or a gRPC server: holds. (3) The spec-vector
  harness does not regress, with the skip list empty and both preset suites green at every stage
  exit: the skip list is empty at HEAD; runner-level coverage is not enforced
  (see [Spec vectors](#spec-vectors); §5.6 plans a build failure on any unclaimed vector).
- **Test-infrastructure debt due at S3.** `services/p2p/src/fault_mode.rs` is 1,907 lines under a
  file-wide `#![allow(clippy::unwrap_used, clippy::expect_used)]` (`:16`). The plan wants it removed
  before the first decision-grade soak.
- The §8.1 picture of today's path predates the deletion of E7 (`78a90e1`). Use the edge inventory
  in [03-internal-contracts.md](03-internal-contracts.md) instead.
