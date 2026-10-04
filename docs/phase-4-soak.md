# Phase 4 soak record

This document is **append-only within named sections**. Owning issues fill only
their section; do not rewrite another issue's section. Skeleton opened by
**CC-4B** (Amendment 5) with the `OQ-1` foreign-peer probe section; **CC-4Cb**
lands the remaining named-section headers (empty) plus the D-12 gating record.
Later issues fill numbers only — the instrument measures the runs; it is not a
run (D-10).

| Section | Owner |
|---|---|
| `## OQ-1 — foreign-peer probe` | **CC-4B** |
| `## Engine falsifier — both layouts` | **CC-40b** |
| `## Snapshot terms` | **CC-42** |
| `## Phase 1 clause gating (D-12)` | **CC-4Cb** |
| `## Clause 1 — restart trials` | **CC-45c** |
| `## Clause 2 — cursor fallback` | **CC-44b** / **CC-48** / **CC-47a** |
| `## Clause 3 — compressed-retention plateau (discharging)` | **CC-4Cc** |
| `## Clause 3 — Hoodi confirmation (non-discharging)` | **CC-4Cd** |
| `## Clauses 4, 5, 6, 7 — Hoodi` | **CC-4Cd** |
| `## CC-4G — cgc rehearsal` | **CC-4G** |
| `## Machine and environment` | **CC-4Cd** |
| `## Run record` | **CC-4Cb** skeleton; numbers **CC-4Cd** |
| `## Clause table` | **CC-4Cb** skeleton; numbers **CC-4Cd** |

Report instrument: `bash scripts/soak-report.sh --phase 4` (CC-4Cb / §10.6).
Venues are the closed set `hoodi | self-devnet | self-devnet-compressed |
in-process-double | dev-machine`. Confirmation rows carry
`confirmation, non-discharging`.

## OQ-1 — foreign-peer probe

**Owner:** CC-4B  
**Date:** 2026-08-08  
**Tool:** `bin/serve-probe` (`cc-serve-probe`) — own varint/snappy/result-byte
codec against `cc-libp2p` transport (Noise + yamux). Does not link
`services/p2p` or `cc-store`.

### Status

**`OQ-1: NOT_RUN`** — real Hoodi peer multiaddrs were not reachable from this
agent environment as a one-shot dial list.

**Blocker (exact):**

1. Public Hoodi beacon HTTP endpoints reachable from this host
   (`beaconstate-hoodi.chainsafe.io`, similar) return placeholder / empty peer
   tables without dialable multiaddrs (no `/ip4/…/tcp/…/p2p/…` rows).
2. `bin/serve-probe` intentionally has **no** discv5 / ENR resolution path
   (allowed workspace edges are only `{cc-libp2p, cc-types, cc-config}`); it
   dials a multiaddr it is given. Bootnode ENRs in `config/p2p.toml` therefore
   cannot be turned into TCP multiaddrs inside this binary.
3. No pre-collected set of live Hoodi peer multiaddrs was available in-repo for
   Lighthouse / Prysm / Nimbus / Teku / Grandine.

**Do not invent peer rows.** The table below is the schema for a later operator
run; it is empty until a real dial list is available.

### Peer table (schema; empty while NOT_RUN)

One row per peer. Fill when OQ-1 is re-run from a network that can dial Hoodi
participants (5–10 peers × Lighthouse / Prysm / Nimbus / Teku / Grandine).

| Date | Implementation (`agent_version`) | Multiaddr | Advertised `earliest_available_slot` | Head slot | Blocks just above eas | Columns just above eas | Positive pass | Negative pass | Notes |
|---|---|---|---|---|---|---|---|---|---|
| — | — | — | — | — | — | — | — | — | **NOT_RUN** — see blocker above |

### Codec validation against five foreign implementations (ADR P4-12)

| Implementation | Handshake success count | Codec validated |
|---|---|---|
| Lighthouse | 0 / target 5–10 | **NOT_RUN** |
| Prysm | 0 / target 5–10 | **NOT_RUN** |
| Nimbus | 0 / target 5–10 | **NOT_RUN** |
| Teku | 0 / target 5–10 | **NOT_RUN** |
| Grandine | 0 / target 5–10 | **NOT_RUN** |

The probe binary and local dual-swarm tests exercise Status v2 + ByRange
framing end-to-end; foreign-implementation handshake counts remain debt until
OQ-1 is re-run.

### Local substitute (negative-side criterion proved)

While OQ-1 is NOT_RUN, CC-4B records a **local stub-peer** proof of the
negative-side criterion that must not be softened:

```text
cargo test -p cc-serve-probe --locked --test negative_stub \
  stub_empty_success_below_window_fails_naming_slot
```

| Check | Result |
|---|---|
| Stub peer returns **empty success** (zero chunks) below advertised eas | Probe exits non-zero |
| Failure reason **names the exact slot** | **PASS** (see test) |
| Cooperating stub (code 3 below eas; block above) | Both sides pass |
| Unreachable multiaddr | Non-zero exit; error names transport / dial |

### §5.6 outcome sentence

**Selected (pending foreign confirmation): (1) ship as designed — pending OQ-1.**

OQ-1 remains **`NOT_RUN`** (no foreign-peer multiaddrs; **do not invent peer
rows**). Under that residual, **CC-49** ships the two-branch flip with
`p2p.advertise_block_floor = true` (config default and `config/p2p.toml`). The
store holds the full window under every outcome; the boolean is the only switch
if a later operator run selects (2) ship behind `false` or (3) ship as designed
with the observed distribution. **Residual debt:** re-run
`bin/serve-probe` against 5–10 Hoodi peers × Lighthouse / Prysm / Nimbus / Teku
/ Grandine when dial access exists, fill the peer table above, and reaffirm or
revise this outcome from live data.

### Command shape for the later fill-in

```text
cc-serve-probe \
  --peer '/ip4/<host>/tcp/<port>/p2p/<peer_id>' \
  --fork-digest 0x<4-byte-hex> \
  --slots 1000 \
  --below 100 \
  --columns 0,1,2,3 \
  --json /tmp/serve-probe-<peer>.json
```

`--fork-digest` is required and never derived. Record `agent_version` from the
probe's identify observation (JSON / stderr Status line), advertised
`earliest_available_slot`, and block/column results just above it.

## Engine falsifier — both layouts

**Owner:** CC-40b  
**Status:** empty skeleton (Amendment 5 / CC-4Cb). Numbers land in this section
only.

## Snapshot terms

**Owner:** CC-42  
**Date:** 2026-08-08  
**Machine:** Apple M4 Pro, 24 GB RAM, macOS aarch64 (dev machine)  
**Metric families:** `cc_storage_snapshot_seconds{phase=replay|serialize|write|load}`,
`cc_storage_snapshot_bytes`, `cc_storage_snapshot_ring_depth`,
`cc_storage_replay_divergence_total`  
**Bucket boundary:** `cc_storage_snapshot_seconds` has an exact **5.0** boundary
(§10.2) so the cadence conditional is a count, not an interpolation.

### Measured BeaconState size (CC-42 /4)

| Field | Value |
|---|---|
| Source | Hoodi fixture cache `~/.cache/cc-hoodi-fixtures/3649472/beacon_state.ssz` (CC-10b pin) |
| Slot | **3649472** |
| **Byte count (uncompressed SSZ)** | **205 205 311** (~195.7 MiB) |
| **Validator-set size** | **1 455 439** |
| Compression | **none** (ADR P4-14) — stored length equals SSZ length |

The design does **not** depend on a 150–200 MB estimate; the numbers above are
what the cadence and disk model use from this measurement.

### Three terms measured separately (CC-42 /3 / OQ-P4-4)

| Term | Phase label | What | Measured | Notes |
|---|---|---|---|---|
| **(a)** | `replay` | Epoch-transition time during storage own-replay | **605.29 ms / epoch** (mean wall) | Reference: Phase 1 mid-gate mean wall. Own-replay now runs real `state_transition` / `process_slots` (`NoVerification` + always-Valid EL stub). Continuous-chain `phase=replay` samples accumulate on live write-behind. |
| **(b)** | `serialize` + `write` | SSZ serialize + P2 write | **serialize 0.053 s** (Hoodi state); write = one P2 put+evict | Unit criterion CC-42/2: P0 commit p99 during **8 MiB** P2 snapshot writes within 10 % of baseline (writer P0-priority). **Residual:** full ~200 MB Hoodi P2 write wall is soak-only (not the 10 % commit criterion). |
| **(c)** | `load` | SSZ deserialize + tree-hash-cache rebuild | **2.97 s** (warm) / **4.80 s** (cold first sample) | Both ≤ **5.0 s**. Command: `cargo test -p cc-storage --bins term_c_hoodi -- --nocapture` |

### Cadence conditional (executed in this issue)

| Check | Value |
|---|---|
| Term (c) vs 5.0 s boundary | **4.80 s max observed ≤ 5.0** |
| Decision | **Keep `storage.snapshot_epochs = 32`** |
| Fallback (if term (c) > 5.0) | Drop to **16** and re-derive CC-45 /6 — **not taken** |

`OQ-P4-4` is closed by the term-(c) measurement above.

### Ring and divergence (unit-level)

| Check | Result |
|---|---|
| Fifth snapshot evicts oldest; `cc_storage_snapshot_ring_depth` stays 4 | **PASS** (`cc-store` + `cc-storage` ring tests) |
| Divergence guard: corrupt expected root → fatal + both roots + counter +1 | **PASS** (`replay::tests::divergence_guard_fatal_logs_and_increments_exactly_one`) |
| Positive path counter at 0 | **PASS** (`replay::tests::positive_snapshot_zero_divergence`) |
| Commit p99 during snapshot within 10 % of baseline | **PASS** (micro-bench `commit_latency_during_snapshot_within_10_percent`) |
| Uncompressed (`grep` production body free of flate/zstd/snap) | **PASS** |

### Config

```text
storage.snapshot_epochs = 32
storage.snapshot_ring   = 4
```

## Phase 1 clause gating (D-12)

**Owner:** CC-4Cb  
**Date:** 2026-08-08  
**Milestone:** M4.3

### Decision (not an oversight)

**D-12's inversion is recorded here as a decision, not an oversight.**

| Phase 1 clause | Gates Phase 4? | Scope | Rationale |
|---|---|---|---|
| **Clause 3** (epoch p95 ≤ 1000 ms; `process_block` p95 ≤ 400 ms) | **Yes — hard gate** | **`CC-42` and `CC-46a` only** — and on **nothing else** | Both issues add work to the same slot budget; a snapshot cadence (or prune path) chosen against an unmeasured baseline is a guess |
| **Clause 2** (≥ 24 h Hoodi soak) | **No — not a gate at all** | — | A 24 h soak that a restart would void is precisely the thing Phase 4 removes the need for; requiring it first is **circular** |

### Phase 1 Clause 3 reading as of M4.3

| Source | Metric | Bar | Recorded | Status |
|---|---|---|---|---|
| `docs/phase-1-soak.md` mid-gate (V-10) | Epoch wall (stand-in for p95) | ≤ 1000 ms | **774.81 ms** max wall | **PASS** |
| mid-gate path | `process_block` p95 | ≤ 400 ms | See phase-1-soak / engine-latency — mid-gate path is epoch ST + FC clone, not a live `process_block` p95 series | **Recorded reference; not re-instrumented at M4.3** |
| CC-1H | Hierarchical state promoted? | — | **No** | mid gate closed without promoting hierarchical state |

**Commands used (V-10 re-read at M4.2 / M4.3 entry):**

```text
cargo test -p cc-chain --test offline_replay cc1h_mid_gate_with_fork_choice_clone -- --nocapture
```

**M4.2 entry (CC-42):** Clause 3 number below bar → CC-42 proceeded.  
**M4.3 entry (CC-46a):** same recorded mid-gate number → CC-46a proceeded.  
No fresh Clause 3 histogram was re-run in those agent sessions; the committed
`docs/phase-1-soak.md` mid-gate table is the authority. If a later operator
re-runs the mid-gate and sees a regression above 1000 ms, re-derive term 4 of
§3.4 and re-check the 60 s restart bar margin.

**What was used instead of a fresh Clause 3 `NOT_RUN`:** the committed mid-gate
table (max wall 774.81 ms) plus CC-42's term-(c) measurement closing OQ-P4-4.
Phase 1's **Clause 2** was **not** required at either entry.

### Implication for cheap diffing

`CC-1H` remains unpromoted → cheap diffing (`hdiff`) remains **unavailable**
(§3.3); cadence is the only lever. `CC-4K` stays unscheduled.

## Clause 1 — restart trials

**Owner:** CC-45c  
**Date:** 2026-08-08  
**Instrument:** `bash scripts/restart-trials.sh` + `bash scripts/soak-report.sh --phase 4 --clause 1`  
**Covers:** proof clause 1 + clause 7(b); CC-45 /6 + /7; §3.4; D-11, D-12; R-1  
**Bar:** kill → `cc_storage_following_head == 1` in **≤ 60 s** (CC-42 left
`storage.snapshot_epochs = 32`; term (c) ≤ 5.0 s — bar **not** re-derived)

### Status

**`NOT_RUN` — clause 1 is not discharged.**

A full exclusive **20/20** mid-slot `SIGKILL` set was **not** executed in this
session. No PASS row is invented. Until 20/20 pass on the correct branch with
per-term `cc_storage_restart_seconds{phase}` rows, identical pre/post `GetHead`
roots, window ≤ pre-crash, zero checkpoint-bootstrap delta, and both durability
settings (≥ 5 each), the clause remains **not discharged**. A 19/20 does not
discharge either — the failing run names its term and the full set is re-run.

### Branch detection (tree at record)

| Check | Result | Source |
|---|---|---|
| EL service in compose | **yes** (`el` present) | `docker compose config --services` / `docker-compose.yml` |
| Real optimistic-sync state machine in `services/chain` | **yes** (`is_optimistic_node` + Phase 3 engine path) | `services/chain/src/{core,invalidation,engine_client}.rs` |
| **Branch** | **A** (tree would allow discharge after 20/20) | both checks true |

Branch A means a later exclusive live set **may** discharge. It does **not**
mean this session discharged. Branch B would record the literal string
`partial — no EL in the restart set` and **not** discharge; re-run at Phase 3
exit. Only one branch is recorded; this row is **A / NOT_RUN**, not B.

### Tree facts recorded at open (not a run)

| Field | Value | Source |
|---|---|---|
| **Git SHA (tree at record)** | `c8ab6c30d595fad37c4c02dbb381b07548d81924` | `git rev-parse HEAD` on `feature/cc-45c-twenty-restart-trials` @ 2026-08-08 |
| **Engine** | redb **4.1.0** | `Cargo.lock` / `docs/storage-engine.md` |
| **Durability defaults** | production default **`immediate`**; trials must also exercise **`paranoid`** (≥ 5 each) | `config/storage.toml`; `CC_STORAGE_DURABILITY` |
| **Restart phases** | `open \| schema_check \| snapshot_load \| restore_send \| chain_replay \| forkchoice_rebuild \| resubscribe` | `cc_storage_restart_seconds{phase}` / CC-45b |
| **Mid-slot window** | random offset in **[4 s, 8 s)** of a 12 s slot | issue CC-45c |
| **Kill path** | `docker compose kill -s SIGKILL` then `up -d` — **never** `down`, **never** `down -v` | `scripts/restart-trials.sh`, `devnet/faults.sh`, `docs/running.md` |
| **Checkpoint fallback** | must be unreachable; assert **zero** Δ `cc_chain_bootstrap_attempts*` per trial | demoted CC-19 path; CC-45c |

### Procedure checklist (for the live 20/20 set)

Operator checklist. Every box must be true for a discharge write-up; this
session left them unchecked.

- [ ] **Exclusive machine (D-11):** Phase 4 stack only — no second stack, no
      builds, **no `bin/store-bench`**, no sleep / reboot / OS update for the set.
- [ ] **Branch re-confirmed** from live `docker compose config --services` paste
      into this section (not only the tree facts above).
- [ ] **Checkpoint host unreachable** for the whole set (empty providers and/or
      DNS-blackhole); per-trial bootstrap attempt delta **0** (count, not a claim).
- [ ] **20 trials**, each: mid-slot SIGKILL ∈ [4, 8); following_head==1 in ≤ 60 s;
      head advances within 2 slots; pre/post GetHead root identical; advertised
      `earliest_available_slot` ≤ pre-crash.
- [ ] **Both durability settings** ≥ 5 runs each; p50/p99 restart wall per setting
      recorded (`OQ-P4-3` measured).
- [ ] **Per-term breakdown** for all 20 rows (`cc_storage_restart_seconds{phase}`);
      a run over the bar **names its term**; phase sum ≉ wall is itself a finding.
- [ ] **Clause 7(b):** advertised window only moved downward across the whole set;
      7(c) `cc_storage_earliest_available_slot == cc_p2p_earliest_available_slot`
      at every scrape.
- [ ] **Deterministic repeat** on self-devnet recorded as its own venue row.
- [ ] **Harness + report:** `bash scripts/restart-trials.sh` →
      `.data/restart-trials.json`; `bash scripts/soak-report.sh --phase 4 --clause 1
      --harness-json .data/restart-trials.json` pasted below.
- [ ] **Docs-only discharge commit** touches only this section (and report paste).
      A code change in that commit voids the set.

### Trial table (schema; empty while NOT_RUN)

| # | Durability | Mid-slot offset (s) | Resume wall (s) | following_head | GetHead identical | eas ≤ pre | Bootstrap Δ | Phases (open…resubscribe) | Verdict |
|---|---|---|---|---|---|---|---|---|---|
| — | — | — | — | — | — | — | — | — | **NOT_RUN** |

### Key rotation (serve-window event; not a restart row)

A node-key rotation is a **serve-window event**, not a free action and not
one of the restart trials above. A new discv5 `NodeId` is a new custodied
column set, so the truth of `earliest_available_slot` changes. `S3b-W-05`
carries the row below beside those restart timings so a later clause-1
acceptance is not read against a silently changed custody set. Procedure:
`docs/key-rotation.md`. **Not reversible.** No automation. No rotation was
performed for this record.

The columns are the clause-1 / soak observation shape: the header that
includes advertised `earliest_available_slot` (the peer table near the top
of this file). The trial is **`NOT_RUN`**. Date, multiaddr, head slot, and
pass/fail are not filled.

| Date | Implementation (`agent_version`) | Multiaddr | Advertised `earliest_available_slot` | Head slot | Blocks just above eas | Columns just above eas | Positive pass | Negative pass | Notes |
|---|---|---|---|---|---|---|---|---|---|
| — | — | — | — | — | — | — | — | — | **NOT_RUN** — key rotation; see `docs/key-rotation.md` |

### Durability p50 / p99 (empty until measured)

| Durability | n | p50 wall (s) | p99 wall (s) | max wall (s) |
|---|---|---|---|---|
| `immediate` | **NOT_RUN** | **NOT_RUN** | **NOT_RUN** | **NOT_RUN** |
| `paranoid` | **NOT_RUN** | **NOT_RUN** | **NOT_RUN** | **NOT_RUN** |

### soak-report clause-1 row (instrument smoke; not a discharge)

```text
$ bash scripts/restart-trials.sh --self-test
# → self-test PASS (evaluate math, branch detect, never down -v, harness shape)

$ bash scripts/restart-trials.sh --emit-not-run --reason 'no exclusive live stack'
# → writes .data/restart-trials.json with overall_status NOT_RUN (exit 5)

$ bash scripts/soak-report.sh --phase 4 --clause 1 \
    --harness-json .data/restart-trials.json
| Clause | Venue | Measured | Threshold | Verdict |
| 1 · kill -9 resumes … | hoodi | NOT_RUN (…) | 20/20 ≤ 60 s; following_head=1; identical GetHead roots | **NOT_RUN** |
```

**Do not treat instrument smoke as 20/20 PASS.**

### Explicit non-discharge

**Clause 1 is not discharged** until branch A completes 20/20 on an exclusive
machine with the assertions above, or until a later Phase 3 exit re-run closes
a branch-B debt. This section currently holds **tree facts + procedure +
instrument smoke only**. No PASS. No fake 20/20.

### Residual blockers (named; do not invent PASS)

| Residual | Detail |
|---|---|
| **No exclusive live stack** | Agent session has empty `docker compose ps`; D-11 exclusive machine + Hoodi head-following stack not available for 20 kill cycles |
| **`cc_storage_following_head` producer** | **Wired** (this pass): write-behind sets **1** after successful `SubscribeEvents`, **0** on start / stream loss / panic-respawn / stop (`StorageMetrics::set_following_head`). Unit-tested. Still **not** a 20/20 discharge — needs live exclusive set |
| **No 20-row phase series** | Per-trial `cc_storage_restart_seconds{phase}` rows require a live set |
| **Self-devnet deterministic repeat** | Not run; venue row empty |
| **Clause 7(b)/(c) series** | Need live scrapes across the set |

Instrument only (this session):

```text
bash scripts/restart-trials.sh --self-test          # PASS
bash scripts/restart-trials.sh --detect-branch      # branch A
bash scripts/restart-trials.sh --emit-not-run \
  --reason 'no exclusive live stack in agent session'
bash scripts/soak-report.sh --phase 4 --clause 1 \
  --harness-json .data/restart-trials.json
# → branch A · NOT_RUN (clause1 harness present but empty) — not discharged
shellcheck scripts/restart-trials.sh                # clean
grep -c "not restartable" docs/running.md           # 0
```

## Clause 2 — cursor fallback

**Owner:** CC-44b / CC-48 / CC-47a  
**Status:** empty skeleton (Amendment 5 / CC-4Cb). Three stages (D-14); only the
third discharges.

### M4.2 attribution

**Owner:** CC-44b  
**Status:** empty.

### M4.4 hole recorded

**Owner:** CC-48 / CC-45b  
**Status:** empty.

### M4.5 hole closed

**Owner:** CC-47a  
**Status:** **`NOT_RUN`** — clause 2 is not discharged.

The fifth gap trigger (`GapTrigger::ServeWindowHoles`) and the below-anchor
`PutBackfillBatch` write path are **implemented and unit-tested** in this tree
(`services/p2p/src/backfill/`, `services/storage/src/backfill.rs`,
`crates/store/src/backfill_progress.rs`). A live self-devnet run that:

1. restarts storage with `T >` the ring's wall-clock depth so
   `CURSOR_TOO_OLD` fires,
2. fills the hole via **`PutBackfillBatch`** (not re-import through `chain`),
3. completes the parent-linkage walk so `ServeWindow.holes` shrinks to empty,
4. records `cc_storage_stream_reconnect_total{reason="cursor_too_old"} == 1`,

was **not** executed in this session. No bar numbers are invented. Until that
run is recorded here with both run (a) and run (b) rows, clause 2 remains
**not discharged** (D-14 / ADR P4-11).

## Clause 3 — compressed-retention plateau (discharging)

**Owner:** CC-4Cc  
**Date:** 2026-08-08  
**Venue:** `self-devnet-compressed`  
**Profile:** `devnet/retention-compressed.toml`  
**Instrument:** `bash scripts/storage-plateau.sh` + `bash scripts/soak-report.sh --phase 4`  
**Covers:** proof clause 3 — discharging; §10.4; **D-10**, **D-11**; `R-15`

### Status

**`NOT_RUN` — clause 3 is not discharged.**

A full ~38 h exclusive compressed-retention plateau run was **not** executed in
this session. No bar numbers are invented. Until **all four bars** pass after the
**block horizon is crossed**, with the horizon-crossing timestamp and hour-15
`R-15` check in this section, the clause remains **not discharged**. A run that
passes three of four is still **not discharged** (name the failing bar).

This row must **not** be merged with `## Clause 3 — Hoodi confirmation
(non-discharging)` (`CC-4Cd`, venue `hoodi`).

### Tree facts recorded at open (not a run)

| Field | Value | Source |
|---|---|---|
| **Git SHA (tree at record)** | `4aad621586d71df751a91e2d0b2cfe4cb6d92266` | `git rev-parse HEAD` on `feature/cc-4cc-plateau-run-record` @ 2026-08-08 |
| **Engine** | redb **4.1.0** | `Cargo.toml` workspace pin; `Cargo.lock` `name = "redb"` / `version = "4.1.0"`; `docs/storage-engine.md` |
| **Durability (production default)** | **`immediate`** (1PC+C) | `config/storage.toml` → `durability = "immediate"`; `EngineOptions` default `Durability::Immediate` |
| **Retention override (venue profile)** | columns **64** epochs; blocks **256** epochs | `devnet/retention-compressed.toml` `[retention_override]` |
| **Self-devnet GVR** | `0x4d04ab2dc363bf4d5e09d605f2872f49edf76c0bc09bcd14ad875f11742d11d0` | `devnet/retention-compressed.toml` / `crates/config` `DEVNET_GENESIS_VALIDATORS_ROOT` |
| **Expected horizons (12 s slots)** | columns ~**3.4 h**; blocks ~**13.7 h**; then **24 h** slope window → ~**38 h** wall | Architecture §10.4 / issue CC-4Cc |

**Run-time durability for a real discharge must be re-recorded from the live
config** (env override `CC_STORAGE_DURABILITY=paranoid` is legal but changes the
commit path). Default in-tree is **immediate**.

### CC-4D guard — legal here, illegal on Hoodi/mainnet

`storage.retention_override` is a **dangerous knob** gated by
`crates/config/src/devnet_guard.rs` (`require_devnet_gvr`). Startup **refuses**
the same compressed-retention profile when `genesis_validators_root` is Hoodi's
or mainnet's (or missing). The compressed profile is therefore legal **only** on
a non-production GVR (self-devnet). **Do not** overlay
`devnet/retention-compressed.toml` onto Hoodi or mainnet — the guard forbids it;
the Hoodi week (`CC-4Cd`) uses production retention and is confirmation-only.

### Procedure checklist (for the live ~38 h run)

Operator checklist. Every box must be true for a discharge write-up; this
session left them unchecked.

- [ ] **Retention profile:** overlay `devnet/retention-compressed.toml` onto the
      self-devnet storage (and ring) config: `columns_epochs = 64`,
      `blocks_epochs = 256`, self-devnet GVR set, `event_ring_bytes` as profiled.
      Confirm guard accepts start (non-Hoodi / non-mainnet GVR).
- [ ] **Exclusive machine (D-11 / R-8):** for the whole run the host runs the
      Phase 4 self-devnet stack and **nothing else**. No Hoodi stack, no builds,
      no container builds, **no `bin/store-bench`** (it writes ~28 GiB/layout and
      moves `cc_storage_commit_seconds` / `cc_storage_disk_bytes` — contamination
      appears in the clause, not the report).
- [ ] **No void conditions:** no `storage` writer panic; no
      `cc_storage_replay_divergence_total` or `key_collision` increment; no code
      change / rebuild / redeploy; no machine sleep, reboot, thermal throttle, or
      OS update; no `docker compose down -v`.
- [ ] **Samples series:** scrape `cc_storage_bytes_total`,
      `cc_storage_pruned_bytes*`, `cc_storage_written_bytes*` on a cadence that
      spans ≥ 24 h **after** block-horizon crossing; write CSV
      `ts_unix,bytes_total,pruned_bytes,written_bytes` for
      `bash scripts/storage-plateau.sh --samples PATH`.
- [ ] **Bar 1 — plateau slope:** `storage-plateau.sh` reports
      `status: OK` (not `HORIZON_NOT_CROSSED`); 24 h slope of
      `cc_storage_bytes_total` **&lt; 1 %** of plateau; horizon-crossing timestamp
      recorded here.
- [ ] **Bar 2 — prune ≈ ingest:** prune-bytes ÷ written-bytes over the same
      window **within 5 %** (script `prune_written_ratio`), both classes.
- [ ] **Bar 3 — deadlines:**
      `cc_storage_prune_deadline_exceeded_total` / pass count **&lt; 1 %** over the
      run. (A deadline exceed rate &gt; 1 % fails the bar; it is not a void.)
- [ ] **Bar 4 — hot path:** p99 ImportBlock-to-persisted latency **during prune
      passes** within **10 %** of the no-prune baseline; both numbers recorded.
- [ ] **R-15 hour-15 check:** at **hour 15** (block horizon ~13.7 h at this
      profile), `cc_storage_pruned_bytes_total` is **non-zero**. Zero at hour 15
      means the watermark is not advancing (`CC-46a` wall-clock rule). Record the
      hour-15 reading here. Do **not** judge the plateau solely at hour ~20
      before the post-horizon 24 h window is complete.
- [ ] **Report row:** `bash scripts/soak-report.sh --phase 4` emits venue
      **`self-devnet-compressed`**, the four measured numbers, thresholds, and
      `discharged` only when all four bars pass; paste into this section.
- [ ] **Docs-only discharge commit:** touches **only** `docs/phase-4-soak.md`
      (`git show --stat`). A code change in that commit voids the run.

### Four bars (required — empty until measured)

| Bar | Metric / expression | Threshold | Measured | Verdict |
|---|---|---|---|---|
| **1** Plateau + 24 h slope (after block horizon) | `storage-plateau.sh` → `slope_pct_of_plateau` | &lt; 1 % of plateau; no `HORIZON_NOT_CROSSED` | **NOT_RUN** | **NOT_RUN** |
| **2** Prune equals ingest | `prune_written_ratio` (pruned ÷ written over window) | within **5 %** of 1.0 | **NOT_RUN** | **NOT_RUN** |
| **3** Prune deadlines | `cc_storage_prune_deadline_exceeded_total` / pass count | **&lt; 1 %** | **NOT_RUN** | **NOT_RUN** |
| **4** Hot path during prune | p99 ImportBlock→persisted during prune vs no-prune baseline | within **10 %** | **NOT_RUN** | **NOT_RUN** |
| **R-15** Hour-15 early warning | `cc_storage_pruned_bytes_total` at t ≈ 15 h | **non-zero** | **NOT_RUN** | **NOT_RUN** |

**Clause 3 (compressed) overall:** **`NOT_RUN` — not discharged.**

### Horizon / window fields (fill after live run)

| Field | Value |
|---|---|
| Run start (UTC) | **NOT_RUN** |
| Column horizon crossed (~3.4 h) | **NOT_RUN** |
| Block horizon crossed (~13.7 h) | **NOT_RUN** |
| 24 h slope window end (~38 h) | **NOT_RUN** |
| Hour-15 pruned_bytes reading | **NOT_RUN** |
| Machine exclusive attestation | **NOT_RUN** (must affirm no second stack / build / `store-bench`) |
| Live durability used | **NOT_RUN** (tree default `immediate`; re-record from live config) |
| Live git SHA of binaries | **NOT_RUN** (record above is tree-at-open only) |

### Tooling smoke (instrument works; not a plateau)

These prove `storage-plateau.sh` and the phase-4 report path emit honest
`NOT_RUN` / `HORIZON_NOT_CROSSED` without a live series. **They do not discharge
clause 3.**

**`bash scripts/storage-plateau.sh --self-test`** → `self-test: PASS` (short
series → `HORIZON_NOT_CROSSED`; synthetic 25 h flat series → `status: OK`,
slope ~0, ratio 1.0).

**Live single-shot (no samples, no metrics listener):**

```text
$ bash scripts/storage-plateau.sh
==> live scrape http://127.0.0.1:9106/metrics (single shot cannot span horizon)
status: HORIZON_NOT_CROSSED
reason: metrics URL unreachable and no --samples file
slope_pct_of_plateau: HORIZON_NOT_CROSSED
prune_written_ratio: HORIZON_NOT_CROSSED
```

**Short samples file (span ≪ 24 h):**

```text
$ bash scripts/storage-plateau.sh --samples <short.csv>
status: HORIZON_NOT_CROSSED
reason: sample span 900s < horizon 86400s (24 h)
slope_pct_of_plateau: HORIZON_NOT_CROSSED
prune_written_ratio: HORIZON_NOT_CROSSED
sample_span_seconds: 900
sample_count: 2
```

**`bash scripts/soak-report.sh --phase 4 --venue self-devnet-compressed --clause 3`**
(no harness / no plateau samples):

```text
| Clause | Venue | Measured | Threshold | Verdict |
| 3 · disk bounded (compressed-retention, discharging) | self-devnet-compressed | NOT_RUN (no plateau samples / clause3.compressed harness — live discharge is CC-4Cc via storage-plateau.sh) | 24 h slope < 1% of plateau; prune/written within 5%; deadline exceeded ≤ 1% of passes; commit p99 delta ≤ 10% | **NOT_RUN** |
```

(With `--venue self-devnet-compressed` the script also **refuses** to emit the
Hoodi confirmation row at the wrong venue — correct behaviour.)

### Explicit non-discharge

**Clause 3 is not discharged** until bars 1–4 all pass after the block horizon,
`R-15` hour-15 is recorded non-zero, the machine was exclusive, and
`soak-report.sh` emits `self-devnet-compressed` with measured numbers. This
section currently holds **tree facts + procedure + instrument smoke only**. No
PASS. No fake discharge.

## Clause 3 — Hoodi confirmation (non-discharging)

**Owner:** CC-4Cd  
**Date:** 2026-08-08  
**Venue:** `hoodi`  
**Profile:** production retention (no `retention_override`; columns floor
`MIN_EPOCHS_FOR_DATA_COLUMN_SIDECARS_REQUESTS` = **4 096** epochs ≈ **18.2 d**;
blocks floor computed / vestigial `MIN_EPOCHS_FOR_BLOCK_REQUESTS` = **33 024**
epochs ≈ **146.8 d** — see `docs/serve-windows.md`)  
**Instrument:** `bash scripts/storage-plateau.sh` + metric scrapes over ≥ 7 days +
`bash scripts/soak-report.sh --phase 4`  
**Covers:** proof clause 3 — **confirmation, non-discharging** only; §10.4 Hoodi
half; **D-10**, **D-11**

### Status

**`NOT_RUN` — clause 3 Hoodi confirmation is not recorded as a live week.**

A ≥ 7-day exclusive Hoodi stack run was **not** executed in this session. No bar
numbers are invented. This row is **`confirmation, non-discharging`**: it must
**not** be merged with `## Clause 3 — compressed-retention plateau (discharging)`
(`CC-4Cc`, venue `self-devnet-compressed`). Clause 3 already has a discharging
path at the compressed venue; **a failure on this Hoodi confirmation row is still
a blocker** when the week is eventually run — confirmation debt is not optional
window-dressing.

**Statement in words (correct outcome at real retention):** over a seven-day
Hoodi window the production horizons (columns ≈ **18.2 d**, blocks ≈ **146.8 d**)
are far longer than the run. **A pruner deleting almost nothing over seven days
is the correct outcome** at real retention. Plateau slope and prune≈ingest bars
are expected to look quiet; that is not a pass-by-inactivity on the compressed
discharging row — it is the confirmation that production retention does not
mis-prune under real data volumes.

### Tree facts recorded at open (not a run)

| Field | Value | Source |
|---|---|---|
| **Git SHA (tree at record)** | `1f5f648401a92524964b14ebb77935c856d5d818` | `git rev-parse HEAD` on `feature/cc-4cd-hoodi-week-and-probes` @ 2026-08-08 |
| **Engine** | redb **4.1.0** | `Cargo.lock` `name = "redb"` / `version = "4.1.0"`; `docs/storage-engine.md` |
| **Durability (production default)** | **`immediate`** (1PC+C) | `config/storage.toml` → `durability = "immediate"` |
| **Retention** | production floors (no override) | columns ≈ 18.2 d; blocks ≈ 146.8 d (`docs/serve-windows.md`) |
| **Docker compose** | **empty** — no exclusive Hoodi stack | `docker compose ps` @ 2026-08-08 |

### Entry checks (offline / residual)

#### V-2 — fork / BPO boundary (recorded offline 2026-08-08)

| Check | Result |
|---|---|
| **Date (UTC)** | 2026-08-08 |
| **Sources** | (1) live `https://raw.githubusercontent.com/eth-clients/hoodi/main/metadata/config.yaml`; (2) in-tree `crates/types/tests/fixtures/hoodi-config.yaml` |
| **`GLOAS_FORK_EPOCH`** | **absent** on both sources — no Gloas fork scheduled in Hoodi metadata as of this read |
| **Scheduled forks present** | `ELECTRA_FORK_EPOCH=2048`, `FULU_FORK_EPOCH=50688` (both long past relative to ~epoch 114 555 on 2026-08-08) |
| **`BLOB_SCHEDULE` BPO steps** | `EPOCH: 52480 → MAX_BLOBS_PER_BLOCK 15`; `EPOCH: 54016 → MAX_BLOBS_PER_BLOCK 21` (both **already past**, ~2025-11; not inside a would-be run window started today) |
| **Would-be ≥ 7-day window if started today** | **No fork and no BPO boundary falls inside** a 7-day window opened on 2026-08-08 given the sources above |
| **Operator duty** | **Re-check V-2 at real entry** on the exclusive machine the day the week opens — metadata can change; this offline read does not freeze the network |

#### V-5 — Hoodi block-size / blob-density sample vs 13.039 blobs/slot

| Check | Result |
|---|---|
| **Status** | **`V-5: NOT_RUN`** |
| **Planning reference density** | **13.039 blobs/slot** (architecture / disk-budget planning figure — **not** re-measured here) |
| **Live sample** | **none** — no exclusive Hoodi stack; no live beacon RPC / blob-density scrape from this agent session |
| **Residual** | Operator must sample live density at real entry. **If density moved materially from 13.039**, state the disk-budget consequence in this section at fill-in. **Do not invent a density number.** |

### Procedure checklist (for the live ≥ 7-day Hoodi week)

Operator checklist. Every box must be true for a confirmation write-up; this
session left them unchecked.

- [ ] **Exclusive machine (D-11):** Phase 4 Hoodi stack only — no second stack, no
      builds, **no `bin/store-bench`**, no sleep / reboot / OS update for the week.
- [ ] **V-2 re-read same day** the week opens; paste fork / BPO table; confirm no
      boundary inside the planned window (or land CC-2A / move window).
- [ ] **V-5 live density sample** vs 13.039; record measured blobs/slot and any
      disk-budget delta.
- [ ] **Production retention** only — **never** `retention_override` / never overlay
      `devnet/retention-compressed.toml` on Hoodi (CC-4D guard).
- [ ] **≥ 7 continuous days** with head-following stack; no void conditions
      (writer panic, divergence/key_collision, code redeploy, `down -v`).
- [ ] **Four bars sampled** (same expressions as compressed row, at real volumes):
      plateau slope, prune≈ingest, deadline exceeded rate, hot-path p99 delta —
      **expect near-zero prune** (correct at 18.2 d / 146.8 d horizons).
- [ ] **Report row:** `bash scripts/soak-report.sh --phase 4` emits venue
      **`hoodi`**, clause 3 confirmation row with measured numbers and verdict
      **`confirmation, non-discharging`** (never merged with compressed row).
- [ ] **Docs-only confirmation commit:** touches **only** `docs/phase-4-soak.md`
      (`git show --stat`). A code change in that commit voids the week.

### Four bars (required — empty until measured)

| Bar | Metric / expression | Threshold (confirmation) | Measured | Verdict |
|---|---|---|---|---|
| **1** Plateau / slope over week | `storage-plateau.sh` / `cc_storage_bytes_total` slope | recorded; quiet growth expected at real horizons | **NOT_RUN** | **NOT_RUN** |
| **2** Prune vs ingest | pruned ÷ written over window | recorded; **near-zero prune is correct** | **NOT_RUN** | **NOT_RUN** |
| **3** Prune deadlines | `cc_storage_prune_deadline_exceeded_total` / pass count | **&lt; 1 %** (or justified quiet) | **NOT_RUN** | **NOT_RUN** |
| **4** Hot path | p99 ImportBlock→persisted during any prune vs baseline | within **10 %** | **NOT_RUN** | **NOT_RUN** |

**Clause 3 (Hoodi confirmation) overall:** **`NOT_RUN` — confirmation, non-discharging; not live-recorded.**

### Horizon / window fields (fill after live week)

| Field | Value |
|---|---|
| Run start (UTC) | **NOT_RUN** |
| Run end (UTC) | **NOT_RUN** |
| Continuous days | **NOT_RUN** (bar ≥ 7) |
| Column horizon (production ≈ 18.2 d) | **not crossed in a 7-day week** (expected) |
| Block horizon (production ≈ 146.8 d) | **not crossed in a 7-day week** (expected) |
| Machine exclusive attestation | **NOT_RUN** |
| Live durability used | **NOT_RUN** (tree default `immediate`; re-record from live config) |
| Live git SHA of binaries | **NOT_RUN** (record above is tree-at-open only) |

### Explicit non-discharge / non-confirmation

**This Hoodi confirmation row is not live-recorded.** It does **not** discharge
clause 3 (discharge is **CC-4Cc** at `self-devnet-compressed`). It also does
**not** yet confirm production retention under real Hoodi volumes. When the week
is run, a failing confirmation bar is a **blocker** even though the compressed
discharging path exists. This section currently holds **tree facts + offline V-2
+ V-5 residual + procedure only**. No PASS. No fake week.

### Residual blockers (named; do not invent PASS)

| Residual | Detail |
|---|---|
| **No exclusive Hoodi stack** | `docker compose ps` empty in this agent session; D-11 exclusive machine not available for ≥ 7 days |
| **No live metric series** | no `cc_storage_bytes_total` / prune / commit series spanning a week |
| **V-5 density** | no live beacon sample vs 13.039 blobs/slot |
| **Week wall-clock** | ≥ 7 continuous days not started |

## Clauses 4, 5, 6, 7 — Hoodi

**Owner:** CC-4Cd  
**Date:** 2026-08-08  
**Venue:** `hoodi` (clause 6 also has a self-devnet early-falsification half owned
by **CC-4F**; clause 7 restart half owned by **CC-45c**)  
**Instrument:** `bin/serve-probe` (`cc-serve-probe`) + metric scrapes +
`bash scripts/soak-report.sh --phase 4`  
**Covers:** proof clauses 4 (full-window blocks), 5 (full-window columns), 6
(negative side), 7 (advertisement = served)

### Status (session)

**All four live Hoodi halves are `NOT_RUN`.** This session has no exclusive Hoodi
stack, no dialable peer multiaddrs for full-window serve-probe, and no week-long
eas scrape series. Tree mechanisms and unit paths exist; **no PASS is invented.**

### Tree facts recorded at open (not a run)

| Field | Value | Source |
|---|---|---|
| **Git SHA (tree at record)** | `1f5f648401a92524964b14ebb77935c856d5d818` | `git rev-parse HEAD` on `feature/cc-4cd-hoodi-week-and-probes` @ 2026-08-08 |
| **Engine** | redb **4.1.0** | `Cargo.lock` |
| **Durability default** | **`immediate`** | `config/storage.toml` |
| **serve-probe binary** | **ready** (`bin/serve-probe`, CC-4B) — own codec; dials given multiaddr | package `cc-serve-probe` |
| **Docker compose** | **empty** | `docker compose ps` |

### CC-47b — block backfill mechanism + run (g) residual

**Owner:** CC-47b (mechanism / unit path); **CC-4Cd** (Hoodi proof clause 4 fill-in)  
**Status:** **`NOT_RUN`** — clause 4 is not discharged.

Block-target below-anchor path is **implemented and unit-tested** in this tree:

| Surface | Location |
|---|---|
| Block completion / target (CC-4A `min_epochs`) | `crates/store/src/backfill_progress.rs` |
| `blocks_oldest` + `blocks_oldest_parent` per batch | same + `services/storage/src/backfill.rs` |
| Outbound **128 blocks / 10 s** self-limit + counters | `services/p2p/src/backfill/rate.rs` + planner `schedule_at` |
| Resume / monotone / write-behind ≤ 10 % | `services/storage/src/backfill.rs` tests |
| Live residual instrument | `bash scripts/block-backfill-monitor.sh` |

A live exclusive ~985k-block / 5.3–21.4 h run that:

1. records **V-9** wall-clock extrapolation (Phase 2 soak or one-hour fallback),
2. asserts outbound ≤ 128 blocks / 10 s **per peer** over a 10-minute window
   against our own counters (per-peer distribution recorded),
3. `kill -9` mid-backfill and resumes within one batch
   (`cc_storage_backfill_oldest_slot{class="blocks"}` delta ≤ 64),
4. keeps the frontier **non-increasing** at every scrape (R-7 first hour),
5. discharges proof clause 4 via `bin/serve-probe --full-window` on Hoodi,

was **not** executed in this session. No bar numbers are invented. Until that
run is recorded here (machine exclusive / D-11, git SHA, durability), clause 4
remains **not discharged**. The clause is rescheduled never weakened if V-9 > 24 h
(`M4.5b`).

### Clause 4 — full-window block serve (Hoodi)

**Status:** **`NOT_RUN` — not discharged.**

| Field | Value |
|---|---|
| Bar | `serve-probe` positive **blocks** over full window: zero `ResourceUnavailable`; `eas ≤ start_slot(current_epoch − 33024)` (computed floor) |
| Mechanism residual | CC-47b unit path above — ready; live backfill + probe not run |
| Live peer / stack | **none** |
| Verdict | **NOT_RUN** |

**Explicit non-discharge:** clause 4 is not discharged until an exclusive Hoodi
run completes full-window block serve-probe with the bars above and the report
row shows measured numbers (not `NOT_RUN`).

### Clause 5 — full-window column serve (Hoodi)

**Status:** **`NOT_RUN` — not discharged.**

| Field | Value |
|---|---|
| Bar | `serve-probe` positive **columns** for 1000 slots (or `--full-window`): full requested ∩ held set per block; zero `ResourceUnavailable` |
| Instrument readiness | **`bin/serve-probe` ready from CC-4B** (`--slots 1000` / `--full-window`, `--columns …`, `--json`) |
| Live peer / stack | **none** — no exclusive Hoodi stack; no dialable multiaddr list in this session (see also `## OQ-1`) |
| Verdict | **NOT_RUN** |

**Explicit non-discharge:** clause 5 is not discharged until live full-window
column serve-probe JSON is recorded and `soak-report.sh` emits measured PASS
cells. Binary readiness alone does not discharge.

### Clause 6 — negative side (Hoodi half)

**Status:** **`NOT_RUN` — Hoodi half not discharged.**

| Field | Value |
|---|---|
| Bar | below `eas`: **100 slots**; every response code **3 `ResourceUnavailable`**; **zero empty successes**; blocks + columns × by-range + by-root |
| Hoodi half | **NOT_RUN** — no live peer / stack |
| Self-devnet half | residual stays **CC-4F** (`clause6.self_devnet` early falsification at M4.3) — see soak-report row |
| Local substitute | CC-4B `negative_stub` test names empty-success-below-window (codec criterion only — not Hoodi discharge) |
| Verdict | **NOT_RUN** |

**Explicit non-discharge:** clause 6 Hoodi half is not discharged until live
serve-probe negative results against a real Hoodi peer are pasted here. The
self-devnet early-falsification half is **not** claimed by this issue.

### Clause 7 — advertisement equals served

**Status:** **`NOT_RUN` — not discharged.**

| Field | Value |
|---|---|
| Continuous two-series bar | `cc_storage_earliest_available_slot == cc_p2p_earliest_available_slot` at **every** scrape over the Hoodi week; scrape count recorded; **mismatch count 0** |
| Restart half | **CC-45c residual** — clause 7(b)/(c) across the 20/20 SIGKILL set (`## Clause 1 — restart trials`) |
| Live scrape series | **none** |
| Verdict | **NOT_RUN** |

**Explicit non-discharge:** clause 7 is not discharged until (a) week-long
two-series agreement is recorded with scrape count / mismatch 0, and (b) the
restart half either lands under CC-45c or is explicitly cross-linked with its
own measured series. Neither half was run here.

### Procedure checklist (operator fill-in)

- [ ] Exclusive Hoodi stack up; head-following; production retention; D-11 clean.
- [ ] V-2 / V-5 re-recorded at entry (see Clause 3 Hoodi section).
- [ ] Clause 4/5: full-window (or 1000-slot) serve-probe against live peer(s);
      JSON under `.data/` or `/tmp`; zero ResourceUnavailable on positive side.
- [ ] Clause 6: `--below 100` (or equivalent); every code 3; zero empty success.
- [ ] Clause 7: scrape both eas gauges on a fixed cadence for the whole week;
      mismatch counter 0; paste scrape count.
- [ ] `bash scripts/soak-report.sh --phase 4 --serve-probe-json …` (and harness
      if used) pastes measured rows into `## Clause table`.
- [ ] Docs-only discharge/confirmation commit touches only this file.

### Command shapes for operator fill-in

```text
# Positive full-window / 1000-slot (clauses 4 + 5)
cc-serve-probe \
  --peer '/ip4/<host>/tcp/<port>/p2p/<peer_id>' \
  --fork-digest 0x<4-byte-hex> \
  --slots 1000 \
  --columns 0,1,2,3 \
  --json /tmp/serve-probe-hoodi-positive.json

# Or ceiling sample across [eas, head]:
cc-serve-probe \
  --peer '/ip4/<host>/tcp/<port>/p2p/<peer_id>' \
  --fork-digest 0x<4-byte-hex> \
  --full-window \
  --columns 0,1,2,3 \
  --json /tmp/serve-probe-hoodi-full-window.json

# Negative side (clause 6) — 100 slots below eas
cc-serve-probe \
  --peer '/ip4/<host>/tcp/<port>/p2p/<peer_id>' \
  --fork-digest 0x<4-byte-hex> \
  --slots 1000 \
  --below 100 \
  --columns 0,1,2,3 \
  --json /tmp/serve-probe-hoodi-negative.json

# Report (paste into ## Clause table)
bash scripts/soak-report.sh --phase 4 \
  --serve-probe-json /tmp/serve-probe-hoodi-positive.json \
  --out /tmp/phase4-clause-table.md
```

`--fork-digest` is required and never derived. Record `agent_version`, advertised
`earliest_available_slot`, and block/column results just above it (same discipline
as OQ-1).

### Residual blockers (named; do not invent PASS)

| Residual | Detail | Owner |
|---|---|---|
| **No exclusive Hoodi stack** | empty `docker compose ps`; D-11 not satisfied | CC-4Cd live |
| **No dialable multiaddrs** | OQ-1 still NOT_RUN; serve-probe has no discv5 path | CC-4B / operator |
| **Clause 4 live backfill + probe** | mechanism unit-ready; live run absent | CC-47b residual + CC-4Cd |
| **Clause 5 live columns probe** | binary ready; live absent | CC-4Cd |
| **Clause 6 Hoodi half** | live absent; self-devnet half CC-4F | CC-4Cd / CC-4F |
| **Clause 7 week series** | no scrapes; restart half CC-45c | CC-4Cd / CC-45c |
| **Week ≥ 7 days** | not started | CC-4Cd |

## CC-4G — cgc rehearsal

**Owner:** CC-4G  
**Date:** 2026-08-09  
**Status:** unit + docs discharged; **live four-effects run `NOT_RUN`**.

Phase 4 builds and rehearses the **cgc 4 → 8** transition. OQ-6 is resolved:
`get_custody_groups(node_id, count)` walks discovery order and only sorts at the
end (`BTreeSet`), so the cgc=4 set is always ⊆ the cgc=8 set. A raise never
invalidates stored columns for the old indices — only four new indices need
backfill. **No product code change** is required for the live raise path
(config + restart); this issue adds regression tests + this soak section.

### CC-4G /3 — narrowing is correct, not a regression

A **narrowing** advertised window on a cgc raise is **correct, not a
regression** (§5.4): the four new custody indices were never custodied, so the
column floor `C` for the extended set is head-ish; with `C > R` (sidecar
retention floor) the two-branch rule returns to **branch 2** and advertises
`max(B, C)` until the new indices backfill over the 4 096-epoch window.
Lighthouse's `DataColumnCustodyInfo.earliest_data_column_slot` is the same
model (earliest slot from which the node can serve its *current* custodied
column set).

### Unit path results

| Check | Path | Corpus / name | Result |
|---|---|---|---|
| **Subset property** | `services/p2p/tests/custody_subset.rs` | **11** mainnet Fulu `networking` / `get_custody_groups` vectors + **2 000** fixed-seed random node ids | unit |
| **Atomicity (abort)** | `services/storage/tests/cgc_raise.rs` | `cgc_raise_fail_commit_preserves_old_pair` | unit |
| **Atomicity (success)** | same | `cgc_raise_success_atomic_new_pair` | unit |
| **Four effects (synthetic)** | same | `effect1_*` … `effect4_*` | unit |

Commands:

```text
cargo test -p cc-p2p --test custody_subset --locked
cargo test -p cc-storage --test cgc_raise --locked
```

### Live four-effects run — `NOT_RUN`

**Status:** **`NOT_RUN`**. No exclusive Hoodi node with a complete column window
and 20 min–1.3 h wall clock was available in this agent session. **Do not invent
PASS numbers** for effects 1–4 live metrics.

| Residual blocker | Detail |
|---|---|
| **No exclusive Hoodi stack** | no D-11 exclusive machine / compose stack for a config-only cgc raise |
| **No complete column window at cgc=4** | live raise needs a node that already serves the four old indices over the sidecar retention period so effect 2 is a real branch-1 → branch-2 flip |
| **Wall clock** | 20 min–1.3 h continuous observation window not started |
| **Return to cgc=4** | post-run config restore is operator-owned; Phase 6 owns production raise |

### Procedure checklist (live raise — one continuous run)

One start timestamp and one end timestamp for the whole sequence. Config-only
cgc change + restart; no product binary swap required for the raise itself.

1. **Preflight** — node at `cgc = 4`, column window complete for the four
   custodied indices (`C ≤ R`, branch 1), metrics scrape healthy.
2. **Record t0** — start timestamp; scrape
   `cc_storage_window_branch`, `earliest_available_slot` / serve-window export,
   custodied index set, column row counts for the four old indices.
3. **Config-only raise** — set custody group count to **8** in config; **restart**
   the node (same binary).
4. **Effect 1** — new custody set = old ∪ four new indices; size 8; old four
   still present (`get_custody_groups` / ENR / MetaData).
5. **Effect 2** — branch → **2**; advertised eas **narrows** (sidecar floor
   until new indices backfill). Treat as correct (§5.4), not a regression.
6. **Effect 3** — row counts (or held set) for the **four old** indices
   unchanged across the raise.
7. **Effect 4** — after new indices backfill to `C ≤ R`, branch returns to **1**
   and the advertised window widens again (observe within the same run window
   or note still-backfilling at t1).
8. **Record t1** — end timestamp; final scrapes; write numbers into this
   section (replace `NOT_RUN` only with measured values).
9. **Restore** — return node to **`cgc = 4`** (Phase 6 owns production raise).

### Note on product surface

The live path is **config + restart**. This issue's deliverable is the subset
property test, the atomic cgc+window raise tests / four-effects unit model, and
this residual-honest soak record — not a production cgc=8 default.

## Machine and environment

**Owner:** CC-4Cd  
**Date:** 2026-08-08  
**Status:** tree-time host facts recorded; **no exclusive Hoodi stack**; **no
second machine**.

### Host at record (this agent / dev machine)

| Field | Value | Source |
|---|---|---|
| **Date (UTC)** | 2026-08-08T15:24:28Z | `date -u` |
| **Arch** | **arm64** (Apple Silicon) | `uname -m` |
| **CPU** | Apple M4 Pro | `sysctl machdep.cpu.brand_string` |
| **ncpu** | 14 | `sysctl hw.ncpu` |
| **memsize** | **24 GB** (`25769803776` bytes) | `sysctl hw.memsize` |
| **Workspace volume (`df -h`)** | `/dev/disk3s5` **926 Gi** total, **659 Gi** used, **230 Gi** avail, **75 %** — mounted `/System/Volumes/Data` | `df -h` on worktree (V-6) |
| **Docker compose** | **empty** (header only; no services) | `docker compose ps` |
| **Exclusive Hoodi stack** | **no** | same |
| **Second machine existed? (A-P4-7)** | **no** — single agent/dev machine; **M4.6 waits for exclusive second machine or after week** | operator / issue A-P4-7 |

### §10.6 additional rows (pointers + machine facts)

Nine named items, each traceable. This issue records what it can measure and
**points** at sibling sections for the rest — it does **not** re-measure them.

| # | §10.6 item | Status / pointer |
|---|---|---|
| 1 | **Machine spec** | **this section** — Apple M4 Pro, arm64, 14 ncpu, 24 GB RAM @ 2026-08-08 |
| 2 | **V-6 `df -h`** | **this section** — 926 Gi / 659 Gi used / 230 Gi avail (75 %) on workspace volume |
| 3 | **Three snapshot terms** | pointer → `## Snapshot terms` (**CC-42**) — replay / serialize+write / load measured |
| 4 | **Validator-set size** | pointer → `## Snapshot terms` — **1 455 439** validators; BeaconState **205 205 311** bytes @ slot 3649472 |
| 5 | **Both falsifier layouts** | pointer → `## Engine falsifier — both layouts` (**CC-40b** skeleton / residual) |
| 6 | **OQ-1 results** | pointer → `## OQ-1 — foreign-peer probe` (**NOT_RUN**) |
| 7 | **Durability per crash-resume** | pointer → `## Clause 1 — restart trials` (**NOT_RUN**; both `immediate` and `paranoid` required on live set) |
| 8 | **Measured BeaconState byte count** | pointer → `## Snapshot terms` — **205 205 311** bytes uncompressed SSZ |
| 9 | **Second machine (A-P4-7)** | **this section** — **no**; M4.6 waits for exclusive second machine or after week |

## Run record

**Owner:** CC-4Cb skeleton; numbers **CC-4Cd**  
**Status:** **`NOT_RUN` for live numbers** — instrument path recorded; owners
named. It measures the runs; it is not a run (D-10).

| Field | Value |
|---|---|
| Phase | 4 |
| Instrument | `bash scripts/soak-report.sh --phase 4` |
| Plateau run (discharging clause 3) | **CC-4Cc** — `## Clause 3 — compressed-retention plateau` — **NOT_RUN** |
| Hoodi week (confirmation clause 3 + probes) | **CC-4Cd** — this section's owners — **NOT_RUN** |
| Restart trials (clause 1 + 7 restart half) | **CC-45c** — `## Clause 1` — **NOT_RUN** |
| serve-probe / OQ-1 | **CC-4B** binary; OQ-1 **NOT_RUN**; full-window probes **CC-4Cd NOT_RUN** |
| **Git SHA (tree at record)** | `1f5f648401a92524964b14ebb77935c856d5d818` |
| **Start (UTC)** | **NOT_RUN** |
| **End (UTC)** | **NOT_RUN** |
| Continuous days | **NOT_RUN** (bar ≥ 7 for Hoodi week) |
| Venue(s) | closed set per clause row: `hoodi`, `self-devnet`, `self-devnet-compressed`, `in-process-double`, `dev-machine` |
| **Engine crate** | redb **4.1.0** (`Cargo.lock`) |
| **Durability (tree default)** | **`immediate`** (`config/storage.toml`) |
| Live durability used | **NOT_RUN** (re-record from live config / `CC_STORAGE_DURABILITY`) |
| Machine exclusive | **no** this session — see `## Machine and environment` |
| Second machine | **no** (A-P4-7) |

### Instrument smoke (not a week)

```text
$ bash scripts/soak-report.sh --phase 4 --out /tmp/phase4-clause-table-4cd.md
# → wrote clause table; every live cell NOT_RUN; clause 3 Hoodi row
#   verdict **confirmation, non-discharging** (correct without harness)
```

**Do not treat instrument smoke as a discharged week or probe PASS.**

## Clause table

**Owner:** CC-4Cb (script) / CC-4Cd (numbers)
**Generated by:** `scripts/soak-report.sh --phase 4`
**Storage metrics URL / file:** `http://127.0.0.1:9106/metrics`
**Plateau samples:** `(none)`
**Serve-probe JSON:** `(none)`
**Harness JSON:** `(none)`
**Venue filter:** (none — all venues)
**Clause filter:** (none — all clauses)
**Status at CC-4Cd record (2026-08-08):** verbatim instrument output with no live
harness — every discharge cell **NOT_RUN**; clause 3 Hoodi row
**`confirmation, non-discharging`**.

| Clause | Venue | Measured | Threshold | Verdict |
|---|---|---|---|---|
| 1 · kill -9 resumes | hoodi | NOT_RUN (no clause1 harness and no cc_storage_restart_seconds / cc_storage_following_head on scrape — live discharge is CC-45c) | 20/20 ≤ 60 s; following_head=1; identical GetHead roots (branch A) or `partial — no EL in the restart set` (branch B, not discharged) | **NOT_RUN** |
| 2 · cursor fallback · attribution | in-process-double | NOT_RUN (stage=attribution; no harness + no cc_storage_stream_reconnect_total — CC-44b) | exactly 1 of each reason on cc_storage_stream_reconnect_total; stage only (D-14) | **NOT_RUN** |
| 2 · cursor fallback · hole recorded | self-devnet | NOT_RUN (stage=hole recorded; no harness + no cc_storage_window_hole_slots — CC-48/CC-45b) | run (b) hole durably in ServeWindow.holes; parent-linkage walk; stage only (D-14) | **NOT_RUN** |
| 2 · cursor fallback · hole closed | self-devnet | NOT_RUN (stage=hole closed; harness clause2.hole_closed absent — CC-47a; clause discharged only when this stage is present — D-14) | store has no gap after parent-linkage walk; discharges clause 2 only when present (D-14) | **NOT_RUN** |
| 3 · disk bounded (compressed-retention, discharging) | self-devnet-compressed | NOT_RUN (no plateau samples / clause3.compressed harness — live discharge is CC-4Cc via storage-plateau.sh) | 24 h slope < 1% of plateau; prune/written within 5%; deadline exceeded ≤ 1% of passes; commit p99 delta ≤ 10% | **NOT_RUN** |
| 3 · disk bounded (Hoodi confirmation) | hoodi | NOT_RUN (clause3.hoodi / confirmation harness absent — live is CC-4Cd ≥ 7-day Hoodi week) | same four bars at real data volumes; ≥ 7 days; confirmation, non-discharging | **confirmation, non-discharging** |
| 4 · full-window block serve | hoodi | NOT_RUN (no serve-probe --full-window --json / clause4 harness — CC-4Cd) | serve-probe positive blocks: zero ResourceUnavailable; eas ≤ start_slot(current_epoch − 33024) | **NOT_RUN** |
| 5 · full-window column serve | hoodi | NOT_RUN (no clause5 harness / serve-probe columns result — CC-4Cd) | serve-probe positive columns: full requested∩held set per block; zero ResourceUnavailable | **NOT_RUN** |
| 6 · negative side (Hoodi) | hoodi | NOT_RUN (no clause6 / serve-probe.negative — CC-4Cd Hoodi half) | below eas: every response ResourceUnavailable (3); never empty success; blocks+columns × by-range+by-root | **NOT_RUN** |
| 6 · negative side (self-devnet early falsification) | self-devnet | NOT_RUN (clause6.self_devnet absent — early falsification at M4.3 / CC-4F) | below eas: ResourceUnavailable never empty success (early falsification) | **NOT_RUN** |
| 7 · advertisement equals served | self-devnet | NOT_RUN (no clause7 harness and no eas gauges — CC-49 / CC-45c / CC-4Cd) | cc_storage_earliest_available_slot == cc_p2p_earliest_available_slot at every scrape; migration step assertions hold | **NOT_RUN** |

### Method notes

- **Venue is machine-checked (closed set):** `hoodi`, `self-devnet`, `self-devnet-compressed`, `in-process-double`, `dev-machine`. `--venue` refuses non-matching clause rows — a clause at the wrong venue does not discharge.
- **Clause 1** emits branch A (EL in restart set → may discharge) or branch B carrying the literal string `partial — no EL in the restart set` with verdict **not discharged**.
- **Clause 2** emits three stages (`attribution` / `hole recorded` / `hole closed`); the clause is **discharged only when the third is present** (D-14).
- **Clause 3** has two rows that must not be merged: compressed-retention at `self-devnet-compressed` (discharging) and Hoodi marked **`confirmation, non-discharging`**.
- **§10.6 expressions:** restart histogram + following_head + GetHead roots; `cc_storage_stream_reconnect_total{reason}`; `storage-plateau.sh` + `cc_storage_prune_deadline_exceeded_total` + commit-latency delta; `serve-probe --full-window --json` + eas floor; negative side; storage eas vs p2p eas.
- Every measured cell is a **number or an explicit NOT_RUN** — no blank, no guess. A clause read by eye off a Grafana panel does not discharge it.
- **It measures the run; it is not the run** (D-10). Plateau run is CC-4Cc; Hoodi week is CC-4Cd; 20 restart trials are CC-45c.
