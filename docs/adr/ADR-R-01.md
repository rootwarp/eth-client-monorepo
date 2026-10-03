# ADR-R-01 — Internal contracts are typed handles with a stated overflow policy, before their transport moves

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16
- **Phase:** refactor (types at S1; E1/E2 transport selected at S3)
- **Issues:** S1-B-18, S1-A-07
- **Citations:** `plan/architecture.md` §2.1 / §2.2 / §9.1 S1 / §9.2 / §10.5; `plan/prd.md` R-1; `plan/project-plan.md` D7 / R-1; `plan/issues/s1-fold-el-bridge.md` S1-A-07 / S1-A-14 / S1-B-18; `crates/seam/src/lib.rs:22-40,174-232`; `crates/seam/src/in_process.rs:1-10,24-37`; `crates/seam/README.md`
- **Provenance:** new — records the R-1 discharge order; types landed at `S1-A-07`

This is **ADR-R-01**. It records the order that discharges [PRD] R-1: **types
land, transport later.** It does **not** change policies A–D. A change to a
row is its own ADR (`ADR-R-02` at S2 is the first of those). ADR-R-08 later
adds the `commit_snapshot` row in the table below: an exemption on the
existing P2 class, not a fifth policy and not a revision of A–D. The
column ring is **not** a §2.2 A–D row; this file freezes the landed stall
contract, it does not invent Policy A on that path.

## Context

Five of eight internal edges are deleted by S1–S2 (`[ARCH]` §2.3). Deleting a
transport rewrites its backpressure contract silently: a gRPC
`RESOURCE_EXHAUSTED` the caller handles becomes a `TrySendError` it does not
([PRD] R-1). The tree currently carries **four distinct overflow policies** on
internal edges (`[ARCH]` §2.2), one of which ([PRD] P0-12 / policy D) is already
a consensus bug caused by an untyped channel bound making a correctness
decision — drop a `SlotTick` and `store.time` lags, so valid blocks are
IGNOREd as `future_slot`.

S1's out-of-scope line is that order: E1/E2 are **typed only**; the transport
does not move until S3 (`s1-fold-el-bridge.md` header; `[ARCH]` §9.1).
`S1-A-07` landed `cc-seam` (`SeamError`, `ChainIngress`, `P2pEgress`) with
overflow contracts quoting the live bounds. `S1-A-08` landed `InProcess`.
`Ipc` and the conformance suite are later issues. This record is what forbids
shipping a transport move without those types, and what forbids waiting for a
field report that will never arrive.

## Decision

**Every internal edge gets a trait in `cc-seam` whose doc contract states its
overflow behaviour, whose error enum names `Backpressure` explicitly, and
which is covered by a conformance suite that every transport impl must pass.
The typed handle lands in the stage before the transport moves.**

`SeamError` has exactly four variants (`lib.rs:22-40`): `Backpressure
{ bound, waited_ms }`, `Unavailable`, `InvalidArgument`,
`FailedPrecondition`. Adding a variant is a contract change and needs an
ADR. `Backpressure` MUST map to gRPC `RESOURCE_EXHAUSTED` and back. The
caller is expected to shed, retry or descore — never to ignore.

The handle names the existing receiving bound and send timeout. It does
**not** introduce a second bound. Wrapping `InProcess` in front of the
scheduler import lane, the event ring, or `publish_fwd` is a second bound
(`in_process.rs:1-10`).

`[ARCH]` §2.2's four policies are preserved individually; collapsing them at
a fold is the silent change R-1 predicts. Column admit (`submit_column_sidecar`)
is **not** one of those four: it stalls, it does not return `Backpressure`.

| Policy | Today's caller-visible signal | Post-move signal |
|---|---|---|
| **A** — blocking with deadline | gRPC `RESOURCE_EXHAUSTED` + `cc_chain_import_rejected_backpressure` | `SeamError::Backpressure` after `IMPORT_SEND_TIMEOUT` (2 s) on `IMPORT_LANE_DEPTH` (64) |
| **B** — try_send, drop the *subscriber* | stream terminated `RESOURCE_EXHAUSTED`; consumer reconnects with cursor | unchanged for the API/observer bus; **N/A** for storage after S2 |
| **C** — try_send, drop the *message*, log | `error!(...)` on residual `CMD_BOUND` (512), no caller signal | **This file owns Policy C.** Handle bound is `PUBLISH_BOUND` (256) → `Ok(Published::Dropped)`. Residual `CMD_BOUND` hop stays log-only; it is **not** this handle and is **not** deleted. |
| **D** — try_send, drop *silently* | **none** | **deleted** — the tick has its own never-shed lane |
| **`commit_snapshot`** — P2 bound 256, drop-newest, **exempt** (ADR-R-08) | no snapshot writer | drop-newest still drops the chunk; the drop is not a durable snapshot. The newest snapshot advances only when a completion marker commits in the final chunk's transaction. Not a fifth A–D policy and not a move to P1. |

The `commit_snapshot` row was added by ADR-R-08 (2026-10-04). It does not
collapse A–D. Any change to a §2.2 row is a spec change requiring its own
ADR. Do not
change an overflow policy in the same PR that moves a transport (`[ARCH]`
§9.2). A new numeric literal in a moved file is a review-stopper: the diff
must show the bound moving, not being re-derived.

Neither `cc-chain` nor `cc-p2p` names a transport type. They take
`Arc<dyn ChainIngress>` / `Arc<dyn P2pEgress>`. This record does **not**
select `InProcess` versus `Ipc`. That is S3. Both impls stay buildable
permanently (`[ARCH]` §9.2).

Landed overflow contracts (`lib.rs:174-232`), quoted so a later PR cannot
quietly rewrite them:

- `submit_gossip` / `notify_data_available` MUST block up to
  `IMPORT_SEND_TIMEOUT` and then return `SeamError::Backpressure`. They MUST
  NOT silently drop and MUST NOT return a success that means IGNORE.
- `submit_column_sidecar` MUST NOT ride the import lane (that HOL-blocks
  gossip). Admit with `send().await` on the events ring
  (`DEFAULT_RING_CAPACITY` = 4096) — **no import-lane deadline**, never a
  silent `try_send`. A full ring stalls the producer; it does **not**
  return `SeamError::Backpressure`. Oversize (`MAX_EVENT_PAYLOAD_BYTES` =
  10 MiB) is `InvalidArgument`. A missing or closed bus is
  `Unavailable`. Neither may be `Ok(())` after a drop. Putting Policy A
  (deadline + shed) on this ring is a new §2.2-class row and needs its
  own ADR.
- `publish` is lossy by design. **This file owns Policy C.** The handle
  bound is `PUBLISH_BOUND` (256): a full publish queue is
  `Ok(Published::Dropped)`, never `SeamError::Backpressure`. A later
  swarm hop may still drop on `CMD_BOUND` (512) with
  `error!("cmd queue full; dropping local publish")`. That hop is **not**
  this handle's overflow, is **not** a second bound here, and is **not**
  deleted. S2 must not re-decide publish drop under `ADR-R-02`
  (`ADR-R-02` is archive Policy B→A only).
- `update_view` is an `ArcSwap` store — never blocks, never fails.

## Consequences

What this makes easy:

- "Did the semantics change?" is a CI answer (`cargo test -p cc-seam`
  against both impls) instead of a review opinion.
- Supplies the instrument for [PRD] §9 X2 on the **live** edge: count
  `SeamError::Backpressure` and `Published::Dropped` on **both** impls
  (`InProcess` and `Ipc`), and keep `cc_chain_import_rejected_backpressure`
  until the gRPC edge dies. X2's measurement sentence is that live
  counter, not an Ipc-only series.
- A later transport move is a review of bounds *moving*, not being
  re-derived.

What this makes hard:

- Costs one crate and a conformance suite that every impl must pass.
- Anyone who wants to collapse the four policies at a fold has to
  contradict this file, not just edit a doc comment.
- The losing impl is never deleted at S3; it is demoted to a test fixture
  so the suite stays honest.

What this forbids:

- Moving a transport and changing its overflow policy in one PR.
- Waiting for a report of changed backpressure semantics, then fixing it.
- A silent drop on `submit_gossip` / `notify_data_available`.
- `SeamError::Backpressure` (or a deadline) on `submit_column_sidecar`.
  The landed path is stall / `InvalidArgument` / `Unavailable`.
- `Ok(())` after dropping a column sidecar.
- A second bound in front of the live import / column / publish queues.
- `Result<T, Box<dyn Error>>` or `Option<T>` as the seam error type.
- Selecting the E1/E2 transport at S1.

## Alternatives considered

**Wait for a report of changed backpressure semantics, then fix.** Rejected.
The failure mode is silent, so there is no report. This is the alternative
[PLAN] D7 and `S1-A-14` name. A `TrySendError` the caller does not handle is
an in-process drop with no `RESOURCE_EXHAUSTED` and no counter the A/B gate
can see — until E1.1's overflow-counter family moves with zero
import/head-lag diff, which is a stage blocker *after* the fact, not a
report to design from.

**Type and move in the same PR.** Rejected. `[ARCH]` §9.2: changing an
overflow policy in the same PR that moves a transport makes the diff
unreviewable. The reviewer cannot tell a bound that moved from a bound that
was re-derived.

**Collapse the four §2.2 policies to one at the fold.** Rejected. That is
the silent change R-1 predicts. Policy D is already a consensus bug; the
fix is deleting D (never-shed tick lane), not averaging A–D.

**Defer the types until S3 selects a transport.** Rejected. R-1's discharge
order is types first. S1's out-of-scope line exists so E1/E2 can be typed
while the gRPC edge is still live. `S1-A-07` already landed the traits.

**Leave overflow in comments and keep `Box<dyn Error>` / `Option<T>`.**
Rejected. Comments are not a conformance suite. Policy D existed because
nothing in the type system named the drop.

## Refactor impact

**Created at S1. Types landed at `S1-A-07`. This file is the record.**

| Stage | What happens to this record |
|---|---|
| S1-A-07 | `cc-seam` traits + `SeamError` + overflow doc contracts. **Landed.** |
| S1-A-08 | `impl InProcess`. **Landed.** Lanes are the live queues. |
| S1-A-09 | `impl Ipc`. The jittered reconnect loop stays inside it. |
| S1-A-10..12 | Conformance suite against both impls (E1.3). |
| S1-A-14 | P1-D/11 S1 half — string discriminants. Same silent-failure reason. |
| S1-B-18 | This file. No production code change. |
| S2 | `ArchiveWrite` / typed column batch. Policy B→A on the archive path is `ADR-R-02`, not a silent amendment of this file. **Do not re-decide publish drop under `ADR-R-02`.** Policy C stays this record. |
| S3 | Select E1/E2/E8 transport. Both impls stay buildable. Isolation of overflow policy from transport choice stays this record. |
