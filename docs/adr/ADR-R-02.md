# ADR-R-02 — `beacon-core` owns redb; the event bus is not a data plane

- **Status:** accepted · `IngestBlock` / `update_canonical` half superseded-by: ADR-R-08 · backpressure consequence amended-by: ADR-R-08 · **Date:** 2026-08-16
- **Phase:** refactor (archive ownership at S2; `bin/beacon-core` lands at `S2-J-01`)
- **Issues:** S2-A-07, S2-A-04, S2-A-05, S2-A-06, S2-A-09, S2-J-01, S2-J-02
- **Citations:** `plan/architecture.md` §4.2 / §4.3 / §9.2 / §10.5; `plan/prd.md` R-1; `plan/project-plan.md` R-1 / R-17; `plan/issues/s2-fold-storage.md` S2-A-07; `crates/seam/src/lib.rs:298-322`; `crates/chain-core/src/p2p_stream.rs`; `crates/chain-core/src/events/fanout.rs`; `docs/adr/ADR-R-01.md`; `docs/adr/ADR-P2-11.md`; `docs/adr/ADR-P4-03.md`; `docs/adr/ADR-P4-04.md`; `docs/adr/ADR-P4-07.md`; `docs/adr/ADR-07.md`
- **Provenance:** new — records the Policy B→A archive overflow change; types and typed ingest landed at `S2-A-04`…`S2-A-06`. `S2-J-01` lands the binary; this file is the decision.

This is **ADR-R-02**. It is the first change to an `[ARCH]` §2.2 overflow row
(`[PRD]` R-1): the node's own archive ingest flips from Policy **B** to Policy
**A**. It also records that `beacon-core` owns redb and that the event bus
ceases to be a data plane. It supersedes `ADR-P2-11` and `ADR-P4-07`. It may
note the *direction half* of `ADR-07`; it does **not** mark `ADR-07` fully
superseded — the S3 transport re-decision remains.

It does **not** rewrite the column-admit stall (`submit_column_sidecar`,
`ADR-R-01`) as `SeamError::Backpressure`. That ring still stalls the producer
and never returns Backpressure. Archive writer mailbox overflow **is** Policy
A / `SeamError::Backpressure`. It does **not** re-decide publish drop (Policy
C stays `ADR-R-01`). It does **not** delete `write_behind.rs` (`S2-A-09`).

## Context

Column sidecars reached durable storage through a bounded, evicting event ring
in another process (`[ARCH]` §4.3, three links verified). A slow archive was a
`SubscribeEvents` subscriber: per-subscriber `try_send` dropped it and the
stream ended `RESOURCE_EXHAUSTED`; it reconnected with a cursor (Policy **B**).
Ring eviction was therefore a durability event. Recovery
(`CURSOR_TOO_OLD` → `GetCanonicalRoots`) could invent canonical history, and
write-behind recovered `column_index` at a fixed SSZ offset with
`unwrap_or(0)`.

S2 folds storage into the consensus process. `S2-A-04`…`S2-A-06` already
landed `ArchiveWrite` + typed `ColumnBatch` + the top-of-batch continuity
bind. Columns no longer enter the ring. The remaining defect is the *caller*:
`p2p_stream.rs` mapped every `ingest_columns` error — including
`SeamError::Backpressure` — to `Acceptance::Ignore` / `Reason::Internal`.
That is still Policy B (ACK Ignore and drop). R-1 forbids treating that as
an implementation detail.

Separately, two processes sharing redb forced `RestoreFromStore` (push over
the storage→chain edge, `ADR-P4-07`) and left the event bus as a bulk data
plane (`ADR-P2-11` reserved `P2pToChain.column`; `ADR-P4-03` relayed SSZ
without decode). One process that opens the store does not need a restore
RPC, and an owner that persists columns it cannot name is the wrong owner.

`ADR-R-01` already forbids collapsing §2.2 rows at a fold and forbids
rewriting the column-admit stall as Backpressure. This file is the row
change that record predicted.

## Decision

**On the node's own archive ingest path, overflow is Policy A.** A full
writer mailbox (ADR-P4-04: P0 bound 32, block, never drop) surfaces
`SeamError::Backpressure` to the import path. The live caller
(`ArchiveWrite::ingest_columns` in `p2p_stream.rs`) MUST honour that
variant: stall / apply backpressure — the same shape as S1-A-10
(`VerdictResolution::Backpressure` / gRPC `RESOURCE_EXHAUSTED`). It MUST
NOT ACK `Acceptance::Ignore` and drop.

Other ingest errors (`InvalidArgument`, `Unavailable` / Internal) stay
fail-closed. They MUST NOT become silent `AlreadyKnown`.

**One process opens redb.** `bin/beacon-core` opens the store before any
subsystem starts (`[ARCH]` §4.2). `chain-core` holds
`Arc<dyn ArchiveWrite>` and never names a storage type. This file is that
decision; `S2-J-01` lands the binary. `RestoreFromStore` does not survive
the merge (`ADR-P4-07`); deletion of the RPC is `S2-J-02`.

**The event bus is not a data plane.** Column bytes never enter the ring.
The ring survives for `SubscribeEvents` (external API/observer consumers,
later REST SSE). Its bounds and cursor semantics are unchanged. **External
fan-out stays Policy B**: a slow *external* consumer is dropped
(`events/fanout.rs` `try_send`). That is correct for an observer and is
not this row. `S2-A-09` deletes `write_behind.rs` and gap-fill; this PR
does not.

**Column admit is not this row.** `submit_column_sidecar` still admits with
`send().await` on the events ring (`ADR-R-01`). A full ring stalls the
producer; it does not return `SeamError::Backpressure`. Do not rewrite
that stall as Policy A.

**Publish drop is not this row.** Policy C stays `ADR-R-01`.

**`ADR-07` is not fully superseded.** This file may note the direction half
(p2p dials chain) as part of folding storage. The S3 re-decision of E1's
transport remains on `ADR-07` (`Status: proposed · revisit at S3`).

## Consequences

What this makes easy:

- A slow archive blocks import instead of silently losing the only copy of
  the sidecar. The trade is correct for a node whose archive is its own.
- "Did the overflow policy change?" is this file, not a review of a
  `match` arm that swallowed `SeamError::Backpressure`.
- `beacon-core` owning redb has a named decision before the binary exists.
- `ADR-P2-11` / `ADR-P4-07` (and the no-decode relay in `ADR-P4-03`) have a
  successor instead of an unwritten id.

What this makes hard:

- Import stalls when the writer mailbox is full. That is intended
  (`ADR-P4-04`: a slow P0 consumer blocks slot commits). Past the commit
  deadline the stall is a fail-closed abort; that amendment is ADR-R-08
  (aborting is not backpressure), not a silent edit of this trade.
- Anyone who wants Policy B on the archive path — drop the ingest and
  reconnect with a cursor — has to contradict this file, not restore a
  `SubscribeEvents` subscriber.
- `write_behind.rs` must stay until `S2-A-09`. Deleting it here would mix
  a policy ADR with a transport deletion (`[ARCH]` §9.2).

What this forbids:

- Mapping `SeamError::Backpressure` from `ingest_columns` to
  `Acceptance::Ignore` (ACK and drop).
- Silent `AlreadyKnown` after a failed ingest.
- Applying Policy A to external `SubscribeEvents` fan-out.
- Rewriting `submit_column_sidecar`'s stall as `SeamError::Backpressure`.
- Re-deciding Policy C publish drop under this id.
- Changing this overflow policy in the same PR that moves a transport.
- Marking `ADR-07` `accepted` or fully `superseded` because this file
  landed.
- Two processes opening the same redb, or putting column SSZ back on the
  event ring as a durability path.

## Alternatives considered

**Keep the event bus and raise the ring bound.** Rejected. It moves the
eviction threshold without removing eviction-as-durability-event
(`[ARCH]` §10.5).

**Wait for a report of changed backpressure semantics, then fix.**
Rejected. The failure mode is silent (`[PRD]` R-1 / `ADR-R-01`). An Ignore
ACK after `Backpressure` is an in-process drop with no
`RESOURCE_EXHAUSTED` and no `VerdictResolution::Backpressure`.

**Change the overflow policy in the same PR that moves ingest / deletes
write-behind.** Rejected. `[ARCH]` §9.2: the diff is unreviewable. This
issue is a separate PR from `S2-A-05`, `S2-A-06`, and `S2-A-09`.

**Rewrite the column-admit stall as Policy A / `SeamError::Backpressure`.**
Rejected. `ADR-R-01` owns that path: stall, never Backpressure. Collapsing
it into this row is the silent change R-1 predicts.

**Apply Policy A to external `SubscribeEvents`.** Rejected. A slow
observer being dropped is correct. Policy B stays on the fan-out
`try_send`.

**Keep `RestoreFromStore` after the process merge.** Rejected. Push vs
pull is a two-process boot question. `beacon-core` opens redb in-process;
`chain_core::seed_from_durable` replaces `apply_restore_set`.

## Refactor impact

**Created at S2. Policy A caller lands at `S2-A-07`. This file is the
record.** `S2-J-01` lands the binary that opens redb; `S2-A-09` deletes
the write-behind data plane; `S2-J-02` deletes `RestoreFromStore`.

| Stage | What happens to this record |
|---|---|
| S2-A-04 | `ArchiveWrite` + typed `ColumnBatch`. Overflow contract is Policy A on the trait. **Landed.** |
| S2-A-05 | Direct `ingest_columns`. Column bytes never enter the ring. **Landed.** |
| S2-A-06 | Top-of-batch continuity bind. **Landed.** |
| S2-A-07 | This file. Live `p2p_stream` caller honours `SeamError::Backpressure`. `ADR-P2-11` / `ADR-P4-07` superseded-by this id. `write_behind.rs` still exists. |
| S2-A-09 | Delete `write_behind.rs` and gap-fill. Ring stays Policy B for external consumers. Not this PR. |
| S2-J-01 | `bin/beacon-core` opens redb before any subsystem. Decision is this file. |
| S2-J-02 | Delete `RestoreFromStore` / `restore.rs`. `ADR-P4-07` citations go with the code. |
| S3 | `ADR-07` is revisited for E1 transport. This file is not that revisit. |

## Amendment — durable-import contract (ADR-R-08, 2026-10-04)

**Superseded half.** The block-ingest contract is no longer `IngestBlock`,
and canonical rewrite is no longer a caller-settable `update_canonical`
flag. ADR-R-08
replaces that half with `commit_anchor`, `commit_import`, `set_head`, and
`commit_snapshot`. `IngestBlock` / `ingest_block` keep compiling until their
last caller is removed; they are not this contract, and they are not widened.

**What stays this record.** Column ingest (`ingest_columns`), Policy A on
that path, one process opening redb, the event bus not being a data plane,
Policy B on external fan-out, and the refusal to rewrite column-admit or
publish drop.

**Backpressure consequence, amended.** A slow archive still applies
backpressure and blocks import (Policy A). Past the commit deadline of
2 slots the outcome is a fail-closed abort with a named reason. Aborting
is not backpressure. The deadline and its conformance case cite ADR-R-08;
they do not re-decide this paragraph. Policy A is not a new §2.2 row.
