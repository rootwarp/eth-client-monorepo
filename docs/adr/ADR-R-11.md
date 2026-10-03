# ADR-R-11 — The store fingerprint digests the running configuration in two buckets

- **Status:** accepted · superseded-by: — · **Date:** 2026-10-04
- **Phase:** refactor (ratified at S2R; the input fix and the side-key comparison follow)
- **Issues:** S2R-B-06
- **Citations:** `plan/architecture.md` §6.6 / §10.5a / ⟡ D-23 / ⟡ D-28 / Q-13; `plan/prd.md` P1-G/4 / R-14; `plan/project-plan.md` D15; `plan/research/q7-identity-and-config-integrity.md` §3.1 / §3.2; `crates/store/src/schema.rs:3-16,36,139-160,196-204,250-267,419-422,440-453`; `crates/store/src/meta.rs:24-26,92-105,182-183,419-429`; `crates/types/src/config.rs:145-181`; `crates/types/src/containers.rs:17-22`; `crates/types/src/state/mod.rs:72-74`; `crates/types/src/state/accessors.rs:81-82`; `crates/types/src/fork.rs:111-119`; `crates/storage-core/src/open.rs:211-217,325-328,344-361`; `crates/storage-core/src/boot.rs:126-135,382-388,413-426,438-468`; `bin/beacon-core/src/boot.rs:248-259,405,422`; `docs/storage-schema.md:57,62`; `docs/s2-rollback.md:93-100,243`
- **Provenance:** new — Q-13, ratified at the architecture gate on 2026-10-04. Records ⟡ D-28 and discharges [PRD] R-14 by clause (ii). This file changes no code.

This is **ADR-R-11**. The policy exists before the input fix, not after.
Correcting the digest's input is what arms [PRD] R-14. There is no later
step to attach this decision to. Writing it afterwards means shipping the
outage first.

## Context

The fingerprint is supposed to stop a store being opened against the wrong
network. It does not do that today, and the reason is the input, not the
field list.

`compute_config_digest` already hashes the fork epochs (`altair` … `fulu`),
`BLOB_SCHEDULE`, `seconds_per_slot`, `genesis_validators_root`, and two
scalars (`schema.rs:142-160,250-267`). `Store::bind` refuses on exact
inequality of that one digest (`schema.rs:448-453`) and on a
`SCHEMA_VERSION` mismatch (`:440-445`). `SCHEMA_VERSION` is `1` (`:36`).
`docs/s2-rollback.md:243` says do not bump it, and
`docs/s2-rollback.md:93-100` says a pre-`78a90e1` storage binary performs
that same digest check.

The live payload does **not** contain `genesis_fork_version`,
`deposit_contract_address`, or `deposit_chain_id`. Those three are already
on `ChainConfig` (`config.rs:147,179,181`). They are not yet digested.

Both hosts feed the function a constant. `open` always digests
`digest_chain_config()` (`open.rs:216,344-361`) — the committed Hoodi
fixture, from the source tree or from `include_str!` — and an absent
`genesis_validators_root` becomes `Root::ZERO` (`open.rs:325-328`).
beacon-core loads a real network at `boot.rs:405` and then calls `open`
without it (`boot.rs:248-259,422`). Compose storage reaches the same `open`
(`crates/storage-core/src/boot.rs:413-426`). Its `network_config` is
already required for the retention floor (`:134-135,382-388`) and is not a
digest input. `ChainConfig::mainnet_like_for_digest` (`:438-468`) is a
second fixture helper. No compose service sets a GVR override. The rollback
target is equally blind ([q7] §3.1).

That constant is why a network switch is not caught. The moment the real
config is passed in, the fork epochs already in the payload make every
scheduled hardfork and every devnet schedule bump refuse a populated store.
That is [PRD] R-14. P1-G/4's older acceptance text ("changing a digested
consensus field refuses") and R-14's older remedy ("a migration step or an
acknowledged-override allowlist") cannot both hold with this payload. Q-13
ratified the replacement on 2026-10-04.

`canonical` and `snapshots` are keyed by slot
(`docs/storage-schema.md:57,62`). A change to `seconds_per_slot` rewrites
the wall-clock meaning of every slot-keyed row the store already holds. It
has no future-only form. ⟡ D-23 puts it in the fatal bucket, against
[q7] §3.2, which filed it under schedule. Teku does not persist it. Teku
does not key its hot data this way.

## Decision

**Digest the running configuration, in both hosts, as two buckets under
side keys. Do not bump `SCHEMA_VERSION`. Do not take either bucket's input
from a source-tree fixture.**

The running configuration is the network the host actually loaded.
`config_name` is not part of either bucket — an operator-chosen label must
not refuse a valid store (`config.rs:145`). The two lists below are closed.

| Bucket | Fields | On mismatch |
|---|---|---|
| **Identity — fatal** | `genesis_fork_version`, `deposit_contract_address`, `deposit_chain_id`, `genesis_validators_root`, `seconds_per_slot` | Refuse a populated store **before any subsystem starts**. |
| **Schedule — advisory** | Fork epochs, `BLOB_SCHEDULE` | Own meta key. Refuse **only** a move at or below the store's finalized epoch. A move above it logs at WARN and re-stamps this digest. |

The identity list is Teku's three persisted values, plus
`genesis_validators_root`, plus `seconds_per_slot` (⟡ D-23).
`seconds_per_slot` is fatal because this tree keys `canonical` and
`snapshots` by slot. It is not a schedule entry.

The store's finalized epoch is `ForkChoiceScalars.finalized.epoch`
(`meta.rs:182-183`, `containers.rs:19-20`). The schedule bucket refuses
only when a fork epoch or a `BLOB_SCHEDULE` entry moves at or below that
epoch. That move is a retroactive rewrite of history the store already
holds. A move strictly above it is not a refusal.

**A future fork-epoch change is not caught, and that is accepted.** The
digest exists to stop a store being opened against the wrong *network*. A
fork-epoch move above the finalized epoch is the same network revising its
future. History already in the store was applied under the old boundary and
is not rewritten by a later one. Catching that move would refuse every
scheduled hardfork. Independent support, not convenience: no client checked
digests fork epochs at all. Teku persists three identity values. Lighthouse
persists none ([q7] §3.2). The same above-finalized path is how a forward
`BLOB_SCHEDULE` update lands. The at-or-below-finalized guard is the only
schedule move that can corrupt bytes the store already holds.

The two scalars the live payload still hashes,
`min_validator_withdrawability_delay` and `churn_limit_quotient`
(`schema.rs:156-159,263-266`), are in neither bucket. A change to them is
not an identity refusal and not a schedule refusal.

Each new bucket payload carries an explicit `digest_version: u16`. Adding
a field later bumps that integer. It does not bump `SCHEMA_VERSION`. The
comment at `schema.rs:139-140` ("do not add fields without a schema version
bump") describes the single payload this policy splits. It is not a reason
to bump `schema_version`. The legacy payload does not gain
`digest_version`. That would change the bytes the rollback binary compares.

Both new buckets live under **side keys**. `meta.config_digest`
(`meta.rs:26`) keeps the legacy value. New code reads two other meta keys,
and only new code reads them. The identity key is `config_digest_v2`.
That spelling is 16 bytes and passes the `META_KEYS` cap (`k.len() <= 16`,
`meta.rs:419-429`). [ARCH] §6.6 spells the schedule key
`config_schedule_digest`. That spelling is 22 bytes and fails the cap.
Only the schedule key is shortened. This record does not raise the cap,
does not merge the two keys, and does not write either new digest under
`config_digest`. Unknown meta keys are safe for the rollback binary.
Open's registry check inspects table names, not keys
(`schema.rs:419-422`). A new digest *value* under the old key is not safe.
The pre-`78a90e1` binary's equality check would refuse the store.

`meta.config_digest` continues to hold the legacy constant, including on a
store this code creates, so that binary's check still passes. The constant
is what today's opener writes: the committed Hoodi fixture, `Root::ZERO`,
and the two scalars `with_mainnet_scalars` hardcodes (`schema.rs:196-204`).
It is recognisable by equality. It is not permission to stamp a new
identity onto a store that already holds history. The three Teku identity
fields and `digest_version` are added on the identity side key, not by
widening this legacy payload.

**Write both side keys from the running configuration only when the store
has no canonical rows, no blocks, and no snapshots.** An empty store has
no history to contradict, so it may take the running config. Those rows
live in `canonical`, `blocks_hot` and `blocks_{shard}`, and `snapshots`
(`schema.rs:43-51`, `docs/storage-schema.md:54,57,62`).

**If any of those rows exist and either side key is absent, refuse.** Do
not adopt the caller's identity. Do not re-stamp. Once the side keys have
been written, the absence of either is fatal even when `config_digest`
still equals the Hoodi constant. Deleting a side key does not open a
second stamp. Until both side keys are present and compared, a populated
store is refused rather than opened on the legacy constant. That
comparison is `S2R-B-08`. Today's equality check against the fixture is
not the success path.

On a populated store, do not copy a fatal field that cannot be read back
from data the store already holds. The only such witness is
`genesis_validators_root`, via `BeaconState::genesis_validators_root`
(`state/mod.rs:72`, `accessors.rs:81-82`). Matching that root is not
permission to copy `genesis_fork_version`, `deposit_contract_address`,
`deposit_chain_id`, or `seconds_per_slot`, and it is not permission to
write side keys. Genesis fork version is not recoverable from `Fork`.
After the second fork, `previous_version` is not genesis
(`fork.rs:111-119`, `state/mod.rs:74`). The deposit fields and
`seconds_per_slot` are not on the anchor state. An unverifiable value is
a refusal, not a copy from the supplied config. A populated store that
lacks either side key is refused, full stop. There is no successful
re-stamp of a populated store.

Genesis fork version is enforced by the identity bucket against the
running config. An empty store may be stamped with it, because there is
no stored copy to disagree. A populated store is not. Do not add
`AnchorInfo` fields to manufacture a witness (`meta.rs:92-105`).

The forward schedule update below is a different write. It re-stamps the
schedule side key only when that key is already present. It does not
create side keys on a populated store, and it does not rewrite the
identity bucket.

An absent `genesis_validators_root` is refused, not substituted with
`Root::ZERO`, whether the store is populated or being stamped empty. The
substitution is how the identity bucket became a constant.

No digest input comes from a source-tree fixture, in either host. The Hoodi
yaml, `digest_chain_config`, and `mainnet_like_for_digest` are not inputs
to either bucket. Computing the legacy constant, so the stored value can be
recognised, is not that input.

**R-14 is discharged by clause (ii).** The sanctioned way through a
schedule change is the ordinary forward update: above the finalized epoch,
log at WARN and re-stamp the schedule digest that is already present; at
or below it, refuse. Not a migration flag. Not an operator override. Not
an allowlist of fields an operator may change with an acknowledged flag.
That re-stamp is not a first stamp of a populated store.

An override flag is strictly worse than nothing. The fatal bucket exists
so two networks cannot share one store. A flag that opens the store anyway
is a mechanism for opening a store against the wrong network. Once it
exists, the identity refusal is optional, and the failure the digest exists
to prevent becomes a logged choice. A migration step for a forward fork
epoch is the same mistake in another costume: it treats a future schedule
edit as a schema change. The guard already is the way through. Nothing
else is adopted.

## Consequences

What this makes easy:

- A Hoodi store and a mainnet store cannot open as each other. The refusal
  happens in `open`, before any subsystem starts, on both hosts.
- A scheduled hardfork whose epoch is still above the store's finalized
  epoch does not refuse a populated store whose schedule side key is
  already present. The key's absence is a refusal, not a stamp.
- Rollback to a pre-`78a90e1` binary still opens the store.
  `SCHEMA_VERSION` stays `1`. `config_digest` stays the legacy constant.
  The new keys are invisible to that binary.
- "Which policy?" is this file. The input fix and the comparison cite it.
  They do not re-decide it.

What this makes hard:

- A retroactive schedule edit — a fork epoch or a `BLOB_SCHEDULE` entry
  moved at or below the finalized epoch — will not open. There is no flag
  to force it.
- A future fork-epoch change is not detected. That residual is accepted
  (Q-13). A later epoch published by the same network does not refuse.
- A store that already holds a canonical row, a block, or a snapshot, and
  lacks either side key, does not migrate. The new code refuses it. The
  pre-`78a90e1` binary can still open it, because `config_digest` is
  untouched. There is no path that stamps the caller's identity onto
  that history.
- The legacy constant is a value the old binary trusts and the new code
  knows is not the running network. It stays under `config_digest` until
  the compose storage host, the rollback target, is deleted. Contract
  that key then. Not in this fold. Equality with the constant does not
  open a populated store and does not stamp one.
- `config_digest_v2` is 16 bytes and is the identity key.
  `config_schedule_digest` is 22 bytes and fails the cap, so only that
  spelling is shortened. The cap is not raised, the two keys are not
  merged, and neither new digest is written under `config_digest`.

What this forbids:

- Merging the input fix (`S2R-B-07`) or the side-key comparison
  (`S2R-B-08`) before this record. The orchestrator enforces that order.
  A code change that lands first arms R-14: every scheduled hardfork
  refuses a populated store, and there is no policy to point at. This
  record does not start either change. The beacon-core `boot.rs` half of
  the input is under the same rule and is not this diff.
- A `SCHEMA_VERSION` bump for this split.
- `digest_version` on the legacy payload.
- A migration flag, an acknowledged-override flag, or an operator
  allowlist, on either bucket.
- Digest input from a source-tree fixture, in either host. That includes
  leaving `digest_chain_config` or `mainnet_like_for_digest` as the
  running configuration.
- `Root::ZERO` as the genesis validators root of a populated store, or
  as the value stamped onto an empty store when the running config does
  not supply one.
- A refusal because `config_name` changed.
- Putting `seconds_per_slot` in the schedule bucket.
- Treating a change to either live-payload scalar as an identity mismatch
  or a schedule mismatch.
- Adding `AnchorInfo` fields. Genesis fork version is not given a schema
  home here.
- Re-stamping either side key onto a store that has a canonical row, a
  block, or a snapshot. A matching legacy constant is not that permission.
  A matching anchor `genesis_validators_root` is not that permission.
- Copying `genesis_fork_version`, `deposit_contract_address`,
  `deposit_chain_id`, or `seconds_per_slot` from the supplied config onto
  a populated store. Those four cannot be read back from the store.
- Opening a populated store because `config_digest` equals the legacy
  constant. Until both side keys are present and compared, refuse.
  `S2R-B-07` must not leave that equality check as the success path.
  `S2R-B-08` is the comparison. Neither issue starts in this record.

## Alternatives considered

**Keep one exact-equality digest and correct only the input.** Rejected.
The payload already contains the fork epochs, so the corrected input
refuses every future hardfork. That is R-14's outage, and it is what
writing this record after the code would ship.

**Discharge R-14 with a migration step or an acknowledged-override
allowlist.** Rejected at Q-13. The at-or-below-finalized guard is the way
through a forward schedule edit. An override flag is strictly worse than
nothing: it is how an operator opens the wrong network. P1-G/4's phrase
"a digested consensus field" is what swept the schedule into the fatal
set. That phrase is no longer the acceptance text.

**File `seconds_per_slot` under the schedule bucket** ([q7] §3.2).
Rejected (⟡ D-23). It is not a fork epoch and it has no future-only form.
Changing it rewrites slot-keyed `canonical` and `snapshots` rows the store
already holds. Teku's choice not to persist it does not transfer to a
store keyed that way.

**Bump `SCHEMA_VERSION` and rewrite `config_digest` in place.** Rejected.
`docs/s2-rollback.md` forbids the bump, and the rollback binary compares
the old key. A new value there is a store the rollback target will not
open. Side keys are what keep this change off that one-way door. P1-G/4
is not in that bind.

**Catch every fork-epoch change, including future ones.** Rejected. That
asks the digest a question it is not for. It answers "is this the same
network?", not "has this network edited its future?". No reference client
digests fork epochs. The residual is stated above, in the open.

## Refactor impact

**Created at S2R. This file is the record. It does not land the code.**

| Stage | What happens to this record |
|---|---|
| S2R-B-06 | This file. `Status: accepted`, dated 2026-10-04 (Q-13). No production code. |
| S2R-B-07 | Stops taking digest input from a source-tree fixture, in both hosts. `digest_version` is carried on the new payloads. Does not implement the two-bucket comparison. Must not leave today's `config_digest` equality — the fixture constant — as the success path for a populated store. Until both side keys are present and compared, a populated store is refused. Merges only after this file. The orchestrator enforces that order. |
| S2R-B-08 | The two side keys and the comparison this decision states. A populated open succeeds only once both side keys are present and compared. An empty store may be stamped from the running config. A populated store that lacks either side key is refused, not re-stamped. Cites this file and makes no policy of its own. Merges only after this file and after the input fix. |
| Later beacon-core boot edit | The beacon-core host's half of "the running network is the input". The same prohibition on fixture input applies. Not started here. |
| Compose-host deletion | Contract the legacy `config_digest` key. Until then it holds the constant. |
