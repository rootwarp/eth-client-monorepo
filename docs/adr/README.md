# Architecture Decision Records

**M11 baseline: 748 occurrences** (541 `Architecture §` + 207 ADR). The unit is
*occurrences*, not lines — `grep -c` counts matching lines, and some lines carry
two citations (`[ARCH]` §10.1, `[PLAN]` X-5).

S2 entry (D-6 / ⟡ D-13) is: every cited id resolves to a committed document and
the [reconciliation table](reconciliation.md) has no unclassified rows — not
"write 58 ADRs".

Landed records (`ADR-R-01.md`, `ADR-R-02.md`, `ADR-R-03.md`, `ADR-R-04.md`,
`ADR-R-05.md`, `ADR-R-06.md`, `ADR-R-08.md`, `ADR-R-09.md`, `ADR-R-10.md`,
`ADR-R-11.md`) already use this house format.
Do not rewrite them. New records match them.

## Never-cited ids — no ADR

These eight ids exist in the numbering and have **zero** citation sites
(`[ARCH]` §10.4). An uncited id is not an unresolvable citation. **They need no
ADR.** The resolving token `uncited` is README-only — it is not a value in the
[`reconciliation.md`](reconciliation.md) data table.

Canonical hyphenated form is the `ADR-` prefix plus the short id below. **Do
not write that concatenation** (not even in backticks): a §10.1 / S1-B-06
extractor (`ADR[ -]P?[0-9]+(-[0-9]+)?`) would treat it as a citation of an id
that must not get a file.

| short | form | resolving |
|---|---|---|
| P1-01 (never cited; no ADR) | `ADR-` prefix + P1-01 | uncited |
| P1-02 (never cited; no ADR) | `ADR-` prefix + P1-02 | uncited |
| P1-03 (never cited; no ADR) | `ADR-` prefix + P1-03 | uncited |
| P1-06 (never cited; no ADR) | `ADR-` prefix + P1-06 | uncited |
| P2-01 (never cited; no ADR) | `ADR-` prefix + P2-01 | uncited |
| P2-03 (never cited; no ADR) | `ADR-` prefix + P2-03 | uncited |
| P2-12 (never cited; no ADR) | `ADR-` prefix + P2-12 | uncited |
| P4-02 (never cited; no ADR) | `ADR-` prefix + P4-02 | uncited |

## Id scheme (`[ARCH]` §10.3)

**Keep cited ids exactly as cited — do not renumber.** 207 citations point at
them.

| Form | Meaning | Canonical |
|---|---|---|
| `ADR-NN` | pre-phase / workspace-wide (`ADR-04`…`ADR-12`) | `ADR-04` |
| `ADR-PN-MM` | phase `N`, decision `MM` | `ADR-P3-02` |
| `ADR-R-NN` | decisions this refactor program creates | `ADR-R-01` |

New citations use the hyphenated form. Existing space-separated citations
(`ADR P3-02`) stay until that file is touched for another reason. A resolver
accepts both spellings. That resolver is `scripts/check-adr-resolver.sh`
(S1-B-06; `make lint` / clippy job). It fails an ADR id that is in neither
the reconciliation `id` column nor a `docs/adr/ADR-*.md` file. It does not
enforce the table's bucket / status / resolving columns.
`Architecture §<n>` is extracted for the census only — those hits do not
resolve under `docs/adr/`. `ADR-R-*` is out of the extractor: that series
is the refactor records; `plan/` already cites unwritten `ADR-R-07`,
and extracting them would go red before those bodies exist. Landed
files (`ADR-R-01.md`, `ADR-R-02.md`, `ADR-R-03.md`, `ADR-R-04.md`,
`ADR-R-05.md`, `ADR-R-06.md`, `ADR-R-08.md`, `ADR-R-09.md`, `ADR-R-10.md`,
`ADR-R-11.md`) still count as resolvable by
filename.

## Filename

House files are `docs/adr/<id>.md`, matching `ADR-R-05.md` and `ADR-R-06.md`.
`[ARCH]` §10.2 also allows `docs/adr/<id>-<kebab-slug>.md`. A resolver should
accept either. Do not rename the two landed files to add a slug.

## Format (`[ARCH]` §10.2, matched to landed files)

One file per decision. Provenance and Refactor impact are additions to standard
MADR: provenance stops a re-derived ADR from being read as contemporaneous
authority; refactor impact is what a reviewer needs when the citing code is
about to move.

```markdown
# ADR-P3-02 — Engine dials p2p; engine is not a health peer

- **Status:** accepted · superseded-by: — · **Date:** 2026-03-xx (reconstructed)
- **Phase:** 3
- **Issues:** CC-32b, CC-38a
- **Citations:** 11 sites — `proto/eth/p2p/v1/p2p.proto:21`, …
- **Provenance:** re-derived from code (2026-08-xx) | imported from <source> | new

## Context
What forced a decision. One paragraph.

## Decision
The decision, in the imperative. One paragraph.

## Consequences
What this makes easy, what it makes hard, and what it forbids.

## Alternatives considered
Only if they were genuinely considered. "None recorded" is acceptable for a
re-derived ADR — inventing alternatives is worse than admitting they were not
written down.

## Refactor impact
Survives / modified at Sn / deleted at Sn. Required.
```

**R-17.** A resolving document may carry `Status: proposed` plus an explicit
`revisit at Sn` (see `ADR-R-06.md`). That satisfies the S2 entry gate for
decisions owned by a later stage. Do not stall S2 waiting for them to become
`accepted`.

## Reconciliation table

[`reconciliation.md`](reconciliation.md) is the file `S1-B-06` resolves against.
It enumerates the **58 cited ids**: **(a) 43 · (b) 12 · (c) 3**, from the
enumerated `[ARCH]` §10.4 table (not that section's 42/12/4 totals line;
`[PLAN]` X-1). Stub rows say `unwritten (a)` / `unwritten (b)` / `stale (c)`
until `S1-B-07`…`S1-B-17` write or delete them. The eight never-cited ids above
are not rows in that table.
