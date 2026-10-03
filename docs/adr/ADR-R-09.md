# ADR-R-09 — The trusted anchor is a one-shot operation on an uninitialized store

- **Status:** accepted · superseded-by: — · **Date:** 2026-10-04
- **Phase:** refactor (S2R anchor verification; the storage transaction is later)
- **Issues:** S2R-A-07
- **Citations:** `plan/architecture.md` §4.5.4 / §10.5a; `plan/prd.md` R-15; `docs/adr/ADR-P4-07.md`; `docs/adr/ADR-R-02.md`; `docs/adr/ADR-R-08.md`; `services/chain/src/checkpoint_sync.rs`; `crates/storage-core/src/archive_write.rs`; `bin/beacon-core/src/boot.rs`; `devnet/compose.yml`; `devnet/compose.mesh.yml`
- **Provenance:** new — verification and this record land before `commit_anchor`'s storage transaction

This is **ADR-R-09**. It completes ADR-P4-07. ADR-R-02 already superseded
that record by deleting `RestoreFromStore`. ADR-R-08 named `commit_anchor`
as the replacement signature and said genesis is an anchor. This record is
the rule those two left open, and it discharges R-15 as two properties
rather than review discipline.

## Context

`RestoreFromStore` installed consensus state from storage with BLS off and
caller-chosen DA verdicts. Deleting it did not make the anchor durable.
`spawn_core_from_checkpoint_with_epoch` still builds the fork-choice store
in memory and takes no archive handle, so the anchor is not a body the
archive can see. The archive admits a self-parent only when the SSZ
`parent_root` is zero and the caller claims the block is its own parent.
A non-genesis anchor cannot be seeded that way.

The composer has no genesis arm. Boot is durable seed, checkpoint sync, or
no core. The self-devnet is genesis-based: `devnet/compose.yml` runs an
nginx anchor that serves genesis, and `devnet/compose.mesh.yml` passes
`--genesis-time` (set by `up.sh` after the image build). A `Genesis` arm is
required by that devnet, not only by fixture harnesses.

R-15 is that a trusted-anchor write widens storage's trust boundary into
the shape S2 deleted. Built loosely, `commit_anchor` is a second
`RestoreFromStore`: an unauthenticated state-takeover path. "One caller,
reviewed carefully" is not a property a test can fail.

The checkpoint double cannot be a `wiremock`-style dev-dependency of
`cc-beacon-core`. `check-crate-dag.sh` resolves dev-dependencies and
forbids that crate `reqwest` and `hyper`. The provider has to be a trait
in `cc-chain`, which is already allowed an HTTP client.

## Decision

**Verify the anchor in `cc-chain`. Commit it once, later, from the
composer. Do not install a core on this path.**

```
AnchorSource::{Checkpoint | Genesis}  →  verify_anchor (cc-chain)  →  VerifiedAnchor
                                                                     →  commit_anchor (composer, not this change)
                                                                     →  spawn_core_from_seed (chain-core, not called here)
```

`AnchorSource` is an enum because rule 5 allows only one anchor path.
`Checkpoint` fetches through an injected `CheckpointProvider`, then checks
the state root and the optional expected root. `Genesis` takes a local
block and state and runs the same two checks. A genesis parent of zero
stays zero. It is not rewritten to the block root, and this path does not
use the storage self-parent bypass.

Neither arm builds a fork-choice store or spawns a core. `verify_anchor`
does not call `commit_anchor`. `spawn_core_from_seed` stays the only
core-install path: one non-test caller, the composer. The legacy
`spawn_core_from_checkpoint*` functions remain for the 4-container host
until that host moves. They are not a second anchor commit.

`commit_anchor` is legal only on an uninitialized store, and only once.
Its non-test call site is the composer, fed by either arm of
`AnchorSource`. A second caller is an explicit decision, not an
implementation detail. This record does not add that call and does not
write the redb transaction.

Verification stays in chain. Storage writes opaque bytes and anchor
metadata. It does not decode the state to decide trust. The checks are
the state root, the expected root, and — still chain-owned, not
implemented in `verify_anchor` — the weak-subjectivity period and the
fork schedule. Putting any of those in storage is a reversal of this
record.

**R-15 is these two properties, not a review rule.** They are the storage
op's tests. This record fixes them so that op cannot reopen them:

1. After `commit_anchor`, `block_is_durable(anchor_root)` is true, so the
   ordinary continuity check admits the anchor's first child unchanged
   and needs no bypass.
2. A second `commit_anchor` is refused `STORE_NOT_UNINITIALIZED`.

The zero-parent self-parent bypass in the archive stays until that
storage op deletes it. The chain anchor path does not feed it. Deleting
it here would make today's only genesis ingest illegal before a
replacement write exists.

`CheckpointProvider` lives in `cc-chain`. `CheckpointClient` is the HTTP
implementor. `InMemoryCheckpointProvider` is the injectable double. No
`reqwest`, `hyper`, or `wiremock` dev-dependency is added to
`cc-beacon-core`.

## Consequences

What this makes easy:

- Checkpoint and genesis produce one `VerifiedAnchor` for one later
  `commit_anchor` call.
- A test double implements the trait. The composer crate does not take
  an HTTP stack to fake a provider.
- A zero parent on genesis is representable without claiming the block
  is its own parent.

What this makes hard:

- `VerifiedAnchor` is not durable. A successful verify is not a committed
  anchor. Callers must not treat it as one before the storage op lands.
- The legacy in-memory spawn still installs a core on the 4-container
  host and on today's composer checkpoint arm. That is not the anchor
  commit, and it is not a second `spawn_core_from_seed` caller.
- Weak-subjectivity period and fork-schedule checks are assigned to
  chain and are not in `verify_anchor` yet. Their absence is not
  permission to do them in storage.
- Ordinary import still remaps a zero parent to the block root before
  `ingest_block`. That remap is not the anchor path. It keeps feeding
  the bypass until the bypass is deleted with the storage op.

What this forbids:

- A second non-test `commit_anchor` caller.
- Building a core, or calling `commit_anchor`, inside `verify_anchor`
  or either `AnchorSource` arm.
- Remapping a zero parent to the block root on the anchor path.
- A second anchor write for genesis that is not `commit_anchor`.
- Decoding the anchor in storage to decide whether it is trusted.
- A `reqwest`, `hyper`, or `wiremock` dev-dependency on `cc-beacon-core`
  to stand in for a checkpoint provider.
- Treating "reviewers will notice a second caller" as the discharge of
  R-15.
- Deleting the archive's zero-parent bypass in this change.
- Implementing the redb transaction under this id.

## Alternatives considered

**Keep `spawn_core_from_checkpoint` as the anchor install.** Rejected.
It takes no archive handle, so the anchor is not durable and the first
child has no durable parent. That is why the anchor commit cannot hold.

**Seed genesis by remapping its parent to itself, and checkpoint by a
different write.** Rejected. Two anchor paths are the whole of R-15.
Genesis is an anchor like any other.

**Fake the provider with `wiremock` in `cc-beacon-core`.** Rejected.
The DAG gate forbids that crate an HTTP client, and it resolves
dev-dependencies. The trait is the injection point.

**Let storage validate the anchor by decoding the state.** Rejected.
Storage decodes nothing. Trust is established above the archive, which
is the Teku split: the store method is one-shot, and the checks live
in the caller.

**Delete the zero-parent bypass in this change.** Rejected. That
deletion belongs to the storage transaction. Removing the only current
genesis admission before `commit_anchor` writes would leave no legal
genesis ingest and no replacement.

## Refactor impact

**Accepted at S2R. The source enum, `VerifiedAnchor`, and the provider
trait land with this file. The storage transaction does not.**

| Later change | What it must not re-decide |
|---|---|
| `commit_anchor` storage | One shot on an uninitialized store. Afterwards the ordinary continuity check admits the first child with no bypass. A second call is `STORE_NOT_UNINITIALIZED`. |
| Composer call site | Exactly one non-test `commit_anchor`, fed by `AnchorSource::{Checkpoint, Genesis}`. Core install is `spawn_core_from_seed`. |
| Bypass deletion | Delete the zero-parent self-parent admission with the storage op. Do not add a second genesis rule. |
| Weak subjectivity and fork schedule | Chain-owned checks on the verified anchor. Not storage. |
| Provider double | `CheckpointProvider` in `cc-chain`. No `reqwest` / `hyper` dev-dependency on `cc-beacon-core`. |
