# ADR-R-08 — The durable-import commit contract: four seam operations, canonical on head change only

- **Status:** accepted · superseded-by: — · **Date:** 2026-10-04
- **Phase:** refactor (S2R durable-import contract; storage behaviour is later)
- **Issues:** S2R-A-02
- **Citations:** `plan/architecture.md` §4.5.1 / §4.5.2 / §4.5.6 / §2.2 / §10.5a; `docs/adr/ADR-R-01.md`; `docs/adr/ADR-R-02.md`; `docs/adr/ADR-P4-04.md`; `docs/adr/ADR-P4-06.md`; `crates/seam/src/lib.rs`
- **Provenance:** new — the typed surface and this record land before any storage behaviour

This is **ADR-R-08**. It supersedes the `IngestBlock` / `update_canonical`
half of ADR-R-02. It amends that ADR's backpressure consequence (§4.5.6)
and ADR-P4-06's scalar cadence. It adds one `[ARCH]` §2.2 overflow row
for `commit_snapshot`. It does not implement storage.

## Context

Block ingest on `ArchiveWrite` is `IngestBlock` plus `ingest_block`.
Production always rewrites canonical (`update_canonical: true` inside the
writer). That flag is caller-settable in form and constant in practice,
and a losing sibling still rewinds the hot canonical table. Scalars ride
finalization, not every body, so a post-finalization body can be
unreplayable. There is no snapshot writer, so a store is `Complete` only
immediately after an anchor and the replay window is unbounded.

`DaVerdict` must not grow a `None`. Absent DA is fatal for every hot
replay block; `Deferred` is a legal durable state.

A mainnet `BeaconState` cannot sit in one transaction in front of a P0
import commit. `commit_snapshot` therefore uses the existing P2 class
(bound 256, drop-newest). A dropped chunk that became a readable snapshot
would silently truncate history. Choosing P1, or blocking P2, or hoping
the implementation notices, would be an overflow-policy change made in
code. `[PRD]` R-1 / ADR-R-01 require that choice to be a §2.2 row.

`IngestBlock` still has callers (`import.rs` until the chain half moves,
`import_durable.rs` until the last caller is removed). Deleting it in the
same change as the new signatures would leave those callers dangling.
Widening it would keep `update_canonical` as a field.

## Decision

**Chain-core owns every root and drives four named seam operations.**
Storage decodes nothing. The operations are signatures on `ArchiveWrite`
until their storage behaviour lands. The defaults perform no store write
and do not apply the preconditions; those preconditions are the contract
the storage impl must meet, not a choice it may reopen.

| Op | Legal when | Writes |
|---|---|---|
| `commit_anchor(TrustedAnchor)` | store `Uninitialized`, once | one transaction: body, canonical slot, reverse index, `state_roots`, `da_status`, snapshot, anchor metadata, split, scalars, cursor. The only op that may write a body whose parent is not durable. |
| `commit_import(DurableImport)` | store `Complete` (`STORE_INCOMPLETE` otherwise); parent durable (`PARENT_NOT_DURABLE` otherwise). `head` is `None`, or `head.head_root == block_root` because this transaction writes that body. Any other head root is `HEAD_NOT_DURABLE`. Do not apply "already durable" to the body this call inserts. | one transaction: body, reverse index, `state_roots[slot]`, `da_status`, scalars, cursor. Canonical rewrite only when that head rule holds. |
| `set_head(HeadChange, scalars)` | `STORE_INCOMPLETE` when the store is not `Complete`. `HEAD_NOT_DURABLE` only when the store is `Complete` and `head_root` is not already durable. Durability is not relaxed. This call does not insert the body. | canonical rewrite from `head_root`, scalars, cursor. |
| `commit_snapshot(Snapshot)` | store `Complete`; epoch boundary | snapshot ring entry, chunked. See the overflow row. |

`finalize` / `migrate` / `prune` are not these ops.

**`update_canonical` is not a field.** On `commit_import`, canonical
rewrite happens only when `head` is `Some` and `head.head_root` equals
`block_root` (this transaction writes that body). `set_head` rewrites
from a root that is already durable. A second commit of an already-durable
body is an upgrade (`da_status`, scalars, and the canonical rewrite only
under that same head rule), not a duplicate skip. That upgrade is storage
behaviour; this record only forbids a fifth op for it.

**`DaVerdict` is `Available` or `Deferred`.** There is no `None`.

**Genesis is an anchor.** The doc rule that genesis uses
`parent_root == block_root` on `IngestBlock` is struck. `IngestBlock` /
`ingest_block` / `ingest_block_blocking` keep compiling and are marked
for deletion with their last caller. They are not widened. `ingest_columns`
is untouched.

**Scalars, `da_status`, and `state_roots` ride every body** (`commit_import`)
and scalars also ride every `set_head`. That amends ADR-P4-06's cadence
from per-finalization to per-import. The persisted blob is still
`ForkChoiceScalars`, not a vote table.

**A commit failure past `on_block` is fail-closed.** Parent-not-durable
and store-classification refusals are ordinary errors, knowable before
`on_block`. An I/O failure after `on_block` is process-fatal with a named
reason. This record states that split; it does not install the checks.

### §2.2 overflow row — `commit_snapshot`

Not a fifth A–D policy. An exemption on the existing P2 class.

| Op | Class | Exemption |
|---|---|---|
| `commit_snapshot` | writer **P2**, bound **256**, **drop-newest** (ADR-P4-04; the literal is not re-derived) | Snapshot chunks stay on that class. A dropped chunk must not become a durable snapshot. The ring entry is the newest snapshot only when a **completion marker** lands in the final chunk's transaction. Drop-newest itself is unchanged. This is not a move to P1 and not an implementation choice. |

The same row is in ADR-R-01's §2.2 table and in
`docs/architecture/03-internal-contracts.md`.

### Backpressure consequence, amended (ADR-R-02)

`commit_import` on the core OS thread is policy A's first caller with no
shed available. A slow archive still applies backpressure and blocks
import. Past a deadline of **2 slots** — longer than the core-liveness
probe's one-slot threshold — the outcome is a **fail-closed abort** with
a named reason. **Aborting is not backpressure.** The deadline, the
`cc_storage_commit_wait_seconds` histogram, and conformance case 14 cite
this sentence. They do not re-decide it, and they are not this record's
code.

## Consequences

What this makes easy:

- The storage change reviews a transaction against a named op, not a
  widened `IngestBlock`.
- "Did canonical change?" is `commit_import` with `head.head_root == block_root`, or `set_head` of a root that was already durable. A head root that is neither the body this transaction writes nor already durable is `HEAD_NOT_DURABLE`.
- A dropped snapshot chunk cannot be read as a truncated state, because
  visibility is the completion marker. That rule is fixed before the
  writer exists.
- Callers of `ingest_block` still compile.

What this makes hard:

- Four signatures return `Ok(())` by default. `Ok` from the default is
  not a durable commit. Callers must not treat it as one before the
  storage impl lands.
- Import blocks on the archive, and past the deadline the process aborts.
  That is the trade for a node whose archive is its own.
- `IngestBlock` and the new ops coexist until the last caller moves.
  New code must not call `ingest_block`.

What this forbids:

- A `None` variant on `DaVerdict`.
- An `update_canonical` field on `DurableImport` or `TrustedAnchor`.
- Rewriting canonical on a body commit whose `head` is `None`.
- `set_head` while the store is not `Complete` (`STORE_INCOMPLETE`), or of a root that is not already durable when it is (`HEAD_NOT_DURABLE`).
- `commit_import` whose `head.head_root` is neither absent nor `block_root`. The body this call inserts is not already durable, so that equality is the only head that may rewrite canonical in the same transaction.
- A second `commit_anchor`, or `commit_anchor` on a store that is not
  `Uninitialized`.
- Putting `commit_snapshot` on P1, blocking P2, or re-deriving bound 256,
  to avoid stating this exemption.
- Treating a dropped P2 snapshot chunk as the newest snapshot.
- Calling the post-deadline abort "backpressure".
- Deleting `IngestBlock` / `ingest_block` in this change.
- Implementing the storage transactions under this id.

## Alternatives considered

**Widen `IngestBlock` with the new fields, including `update_canonical`.**
Rejected. The op is replaced, not widened. A caller-settable canonical
flag is how every body rewinds the hot table today.

**Move snapshot chunks to P1 so they cannot be dropped.** Rejected. P1
blocks when full, and a mainnet state in front of a P0 import commit is
the stall P2 exists to avoid. The completion marker is the exemption;
the class stays P2.

**Leave the drop-newest choice to the storage implementation.** Rejected.
A dropped chunk that truncates a snapshot is an overflow-policy change.
R-1 requires the row here.

**Treat the post-deadline abort as a longer backpressure wait.** Rejected.
Aborting is not backpressure. Recording it as policy A-with-shed would
hide a process-fatal path.

**Delete `IngestBlock` now.** Rejected. `import.rs` and `import_durable.rs`
still call it. The deletion belongs with the last caller.

## Refactor impact

**Accepted at S2R. Types and signatures land with this file. Storage
behaviour does not.**

| Later change | What it must not re-decide |
|---|---|
| `commit_import` storage | canonical iff `head` is `Some` and `head.head_root == block_root` (this transaction writes that body). Any other head root is `HEAD_NOT_DURABLE`. Do not treat the inserted body as already durable. An already-durable body is still an upgrade (`da_status`, scalars, canonical only under that head rule). |
| `set_head` storage | `STORE_INCOMPLETE` when the store is not `Complete`. `HEAD_NOT_DURABLE` only when it is `Complete` and `head_root` is not already durable. |
| commit deadline | aborting is not backpressure; 2-slot deadline; cite this file |
| `commit_anchor` storage | one shot on `Uninitialized`; genesis is an anchor; no self-parent bypass |
| restart classification | `Complete` reads the snapshot completion marker, not a partial ring entry |
| `commit_snapshot` storage | P2 bound 256, drop-newest, completion-marker exemption. Not P1. |
| last `ingest_block` caller removed | delete `IngestBlock`, `ingest_block`, and `ingest_block_blocking` |
