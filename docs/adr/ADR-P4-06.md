# ADR-P4-06 — Persist fork-choice scalars (~300 B), not a vote table

- **Status:** accepted · amended-by: ADR-R-08 (scalar cadence) · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 4
- **Issues:** S1-B-10, CC-45b
- **Citations:** 5 sites — `crates/store/src/meta.rs:167`; `docs/storage-schema.md:64`; `proto/eth/chain/v1/chain.proto:358,360,384`
- **Provenance:** re-derived from code (2026-08-16)

## Context

A Lighthouse-shaped vote table is tens of megabytes (~75 MB is the
number the proto comments refuse). Phase 4 has no live `ApplyAttestations`
producer; votes that matter sit inside replayed blocks. Persisting the
table would couple the archive to a Phase-5 shape and would make every
restore payload huge. What is **not** derivable from the replay set is a
handful of scalars, including `proposer_boost_root` (load-bearing across
restart, CC-45 /3).

## Decision

Persist **`ForkChoiceScalars` only** — time, proposer-boost root,
justified / finalized / unrealized checkpoints, head root and slot
(`crates/store/src/meta.rs:167-188`). Fixed SSZ layout ~240 B, ~300 B on
the wire comment. Do **not** persist a vote table in Phase 4. Carry the
same blob on `RestoreHeader.fork_choice_scalars_ssz`
(`chain.proto:357-385`). **Cadence, amended by ADR-R-08 (2026-10-04):**
the blob is unchanged, and it is still not a vote table. It is written on
every `commit_import` and every `set_head`, not only at finalization.
`da_status` and `state_roots` ride every body with it. Two named expiry
triggers, so they are not rediscovered:

- **A.** Block-embedded attestations are not yet wired into
  `on_attestation(..., is_from_block = true)`. Steady-state votes still
  reconstruct from the replay set; the caveat is a validator whose latest
  attestation predates the snapshot.
- **B.** Phase 5's attestation service becomes a live
  `ApplyAttestations` producer. Then the record must grow a vote table
  or a bounded delta log — those messages exist nowhere on disk.

## Consequences

What this makes easy:

- Restore / S2 `seed_from_durable` boots from a ~300 B row plus the
  snapshot and the replay set (`[ARCH]` §4.2).
- `proposer_boost_root` survives a restart without a 75 MB table.

What this makes hard:

- Head stability after restore is empirical (CC-45 /7), not proven by a
  stored vote set.
- Phase 5 cannot pretend the archive already has attestations.

What this forbids:

- Writing a 75 MB (or any) vote table into `meta` / `fork_choice` in
  Phase 4.
- Treating missing block-embedded `on_attestation` as a silent "votes
  are complete" claim.
- Dropping `proposer_boost_root` from the persisted set.

## Alternatives considered

**Persist the full vote table now.** Rejected in the proto comments:
Phase 4 has no free-floating attestation producer; the cost is paid for
a table nothing reconstructs from.

**Persist nothing; re-derive all FC state from the snapshot.** Rejected:
`proposer_boost_root` and the unrealized checkpoints are not in the
`BeaconState` the way the live store needs them across restart.

## Refactor impact

**Survives; becomes the boot seed at S2** (`[ARCH]` §4.2).
`RestoreFromStore` goes away; `seed_from_durable` still reads this row.
Trigger B remains a Phase 5 entry condition, not a Phase 4 gap.

**Cadence amended by ADR-R-08.** Per-finalization writes (the
`FINALIZED_CHECKPOINT` tail) are not the contract. Every `commit_import`
and every `set_head` carries scalars. The ~300 B blob and the refusal to
persist a vote table are unchanged.
