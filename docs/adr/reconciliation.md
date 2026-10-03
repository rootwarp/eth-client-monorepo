# ADR reconciliation table

**M11 baseline: 748 occurrences** (541 `Architecture §` + 207 ADR). Unit:
*occurrences*, not lines (`[ARCH]` §10.1, `[PLAN]` X-5).

**58 cited ids** from the enumerated `[ARCH]` §10.4 table: **(a) 43 · (b) 12 ·
(c) 3**. (`[ARCH]` §10.4's totals line and ⟡ D-13 say 42/12/4; `[PLAN]` X-1 —
this file follows the enumerated rows, which is the artifact the work is done
against.)

This is a file, not a section of `plan/architecture.md`, so `S1-B-06` can
resolve against it. Classification: **(a)** re-derivable from the citing code ·
**(b)** needs a decision recorded · **(c)** stale — delete the citation.

The eight never-cited numbering gaps (`P1-01`, `P1-02`, `P1-03`, `P1-06`,
`P2-01`, `P2-03`, `P2-12`, `P4-02`) are **not rows here**. They need no ADR;
see [`README.md`](README.md). Their resolving token `uncited` is README-only.

## Parse contract

The GFM table whose header is `| id | bucket | status | resolving |` is the
data. A later ratchet (S2 / ⟡ D-13, not this file's S1-B-06 gate) should:

1. Take body rows of that table (skip the header and the `---` separator).
2. Split on `|` and strip cells. No cell contains `|`.
3. `id` is the hyphenated canonical form (`ADR-P3-02`).
4. `bucket` is `a`, `b`, or `c`. Every cited id has exactly one. No
   unclassified rows.
5. `status` is a token from the vocabulary below. R-17 is represented by
   appending `; revisit at Sn` (example: `proposed; revisit at S2`).
6. `resolving` is a repo-relative path or `-` (no document yet). The token
   `uncited` is README-only (never-cited numbering gaps); it does not appear
   in this table.

`scripts/check-adr-resolver.sh` (S1-B-06) reads the `id` column as an
allowlist (plus `docs/adr/ADR-*.md` filenames). It does not check bucket,
status, or resolving, and an unwritten row still resolves.

### Status vocabulary

| status | meaning |
|---|---|
| `unwritten (a)` | (a) row; ADR body not written (`S1-B-07`…`S1-B-10`) |
| `unwritten (b)` | (b) row; decision not recorded (`S1-B-11`…`S1-B-16`) |
| `stale (c)` | (c) row; citation to delete (`S1-B-17`) |
| `deleted` | (c) row; code citation removed; no ADR (subject gone or premise deleted) |
| `accepted` | document exists; `Status: accepted` |
| `proposed` | document exists; `Status: proposed` |
| `proposed; revisit at Sn` | R-17 form; `Sn` is the owning stage |
| `superseded` | document exists; superseded (optionally `; superseded-by ADR-R-NN`) |

Empty stub rows (`unwritten (a)` / `unwritten (b)` / `stale (c)`, `resolving: -`)
are correct until the owning `S1-B-07`…`S1-B-17` issue lands the document or
deletes the citation.

## Table

| id | bucket | status | resolving |
|---|---|---|---|
| ADR-04 | a | accepted | docs/adr/ADR-04.md |
| ADR-05 | a | accepted | docs/adr/ADR-05.md |
| ADR-06 | a | accepted | docs/adr/ADR-06.md |
| ADR-07 | b | proposed; revisit at S3 | docs/adr/ADR-07.md |
| ADR-09 | b | proposed; revisit at S3 | docs/adr/ADR-09.md |
| ADR-11 | a | accepted | docs/adr/ADR-11.md |
| ADR-12 | a | accepted | docs/adr/ADR-12.md |
| ADR-P1-04 | b | proposed; revisit at S4a | docs/adr/ADR-P1-04.md |
| ADR-P1-05 | a | accepted | docs/adr/ADR-P1-05.md |
| ADR-P1-07 | a | accepted | docs/adr/ADR-P1-07.md |
| ADR-P1-08 | a | accepted | docs/adr/ADR-P1-08.md |
| ADR-P1-09 | a | accepted | docs/adr/ADR-P1-09.md |
| ADR-P1-10 | a | accepted | docs/adr/ADR-P1-10.md |
| ADR-P1-11 | b | proposed; revisit at S2 | docs/adr/ADR-P1-11.md |
| ADR-P1-12 | a | accepted | docs/adr/ADR-P1-12.md |
| ADR-P1-13 | c | deleted | - |
| ADR-P1-14 | a | accepted | docs/adr/ADR-P1-14.md |
| ADR-P1-15 | a | accepted | docs/adr/ADR-P1-15.md |
| ADR-P2-02 | a | accepted | docs/adr/ADR-P2-02.md |
| ADR-P2-04 | a | accepted | docs/adr/ADR-P2-04.md |
| ADR-P2-05 | a | accepted | docs/adr/ADR-P2-05.md |
| ADR-P2-06 | a | accepted | docs/adr/ADR-P2-06.md |
| ADR-P2-07 | a | accepted | docs/adr/ADR-P2-07.md |
| ADR-P2-08 | a | accepted | docs/adr/ADR-P2-08.md |
| ADR-P2-09 | a | accepted | docs/adr/ADR-P2-09.md |
| ADR-P2-10 | b | proposed; revisit at S3 | docs/adr/ADR-P2-10.md |
| ADR-P2-11 | b | superseded; superseded-by ADR-R-02 | docs/adr/ADR-P2-11.md |
| ADR-P2-13 | b | accepted | docs/adr/ADR-P2-13.md |
| ADR-P2-14 | a | accepted | docs/adr/ADR-P2-14.md |
| ADR-P3-01 | a | accepted | docs/adr/ADR-P3-01.md |
| ADR-P3-02 | b | superseded; superseded-by ADR-R-04 | docs/adr/ADR-P3-02.md |
| ADR-P3-03 | a | accepted | docs/adr/ADR-P3-03.md |
| ADR-P3-04 | a | accepted | docs/adr/ADR-P3-04.md |
| ADR-P3-05 | a | accepted | docs/adr/ADR-P3-05.md |
| ADR-P3-06 | c | deleted | - |
| ADR-P3-07 | a | accepted | docs/adr/ADR-P3-07.md |
| ADR-P3-08 | a | accepted | docs/adr/ADR-P3-08.md |
| ADR-P3-09 | a | accepted | docs/adr/ADR-P3-09.md |
| ADR-P3-10 | a | accepted | docs/adr/ADR-P3-10.md |
| ADR-P3-11 | a | accepted | docs/adr/ADR-P3-11.md |
| ADR-P3-12 | a | accepted | docs/adr/ADR-P3-12.md |
| ADR-P3-13 | a | accepted | docs/adr/ADR-P3-13.md |
| ADR-P3-14 | b | proposed; revisit at S2 | docs/adr/ADR-P3-14.md |
| ADR-P3-15 | b | accepted | docs/adr/ADR-P3-15.md |
| ADR-P3-16 | b | superseded; superseded-by ADR-R-03 | docs/adr/ADR-R-03.md |
| ADR-P4-01 | a | accepted | docs/adr/ADR-P4-01.md |
| ADR-P4-03 | b | superseded; superseded-by ADR-R-02 | docs/adr/ADR-P4-03.md |
| ADR-P4-04 | a | accepted | docs/adr/ADR-P4-04.md |
| ADR-P4-05 | a | accepted | docs/adr/ADR-P4-05.md |
| ADR-P4-06 | a | accepted | docs/adr/ADR-P4-06.md |
| ADR-P4-07 | c | superseded; superseded-by ADR-R-02 | docs/adr/ADR-P4-07.md |
| ADR-P4-08 | a | accepted | docs/adr/ADR-P4-08.md |
| ADR-P4-09 | a | accepted | docs/adr/ADR-P4-09.md |
| ADR-P4-10 | a | accepted | docs/adr/ADR-P4-10.md |
| ADR-P4-11 | a | accepted | docs/adr/ADR-P4-11.md |
| ADR-P4-12 | a | accepted | docs/adr/ADR-P4-12.md |
| ADR-P4-13 | a | accepted | docs/adr/ADR-P4-13.md |
| ADR-P4-14 | a | accepted | docs/adr/ADR-P4-14.md |

## Refactor records (not in the 58)

`ADR-R-*` is out of the S1-B-06 extractor (`[ARCH]` §10.3). These files
resolve by filename. They are not census rows: do not add them to the table
above (that table is the 58 cited ids). This section is not a second
`| id | bucket | status | resolving |` table.

S1-B-18 lands [`ADR-R-01.md`](ADR-R-01.md) — Status: accepted. Records the
R-1 discharge order (types land, transport later). Does not supersede a
§10.4 row.

S1-B-13 names **ADR-R-07** as the future successor of `ADR-P3-15` (X-6:
`[ARCH]` §10.4's "ADR-R-05" is the slashing-protection record, `S0-B-14`).
The file is not written — this issue keeps the live skip. Do not add
`ADR-R-07` to the 58-row table.

S1-A-16 lands [`ADR-R-04.md`](ADR-R-04.md) — Status: accepted. Records that
liveness is proved by a deadline-bounded no-op through the consensus core
(N=3 consecutive `probe_core_liveness` misses → aggregate NOT_SERVING; the
same N successes restore). `S1-B-12` records that this supersedes
`ADR-P3-02` (parked-core-green is no longer acceptable). `[ARCH]` named
ADR-R-03 as that successor; R-03 is JWT isolation (`S1-B-11`).

S2-A-07 lands [`ADR-R-02.md`](ADR-R-02.md) — Status: accepted. Records
Policy B→A on the node's own archive ingest (`SeamError::Backpressure`
stalls import; not an Ignore ACK). `beacon-core` owning redb and the
event bus ceasing to be a data plane are part of the record. Supersedes
`ADR-P2-11` / `ADR-P4-07` / `ADR-P4-03`. Does not fully supersede
`ADR-07` (S3 transport re-decision remains). Not a census row.
ADR-R-08 supersedes the `IngestBlock` / `update_canonical` half and amends
the backpressure consequence (aborting is not backpressure).

S2R lands [`ADR-R-08.md`](ADR-R-08.md) — Status: accepted. Four seam
operations (`commit_anchor`, `commit_import`, `set_head`,
`commit_snapshot`); canonical rewrite only on head change;
`commit_snapshot` is P2 bound 256 drop-newest with a completion-marker
exemption. Amends ADR-P4-06's scalar cadence (per-import, not
per-finalization). Not a census row.

S2R lands [`ADR-R-09.md`](ADR-R-09.md) — Status: accepted. The trusted
anchor is verified in `cc-chain` from `AnchorSource::{Checkpoint, Genesis}`
and returned as `VerifiedAnchor` without installing a core. Completes
ADR-P4-07 (still superseded-by ADR-R-02: that record deletes
`RestoreFromStore`; this one is the replacement's one-shot rule) and
discharges R-15 as two properties: after `commit_anchor` the ordinary
continuity check admits the first child with no bypass, and a second
`commit_anchor` is `STORE_NOT_UNINITIALIZED`. The storage transaction is
not this record. Not a census row.
