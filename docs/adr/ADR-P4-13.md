# ADR-P4-13 — `I-node-id`: a mismatched node key refuses `open`

- **Status:** accepted · superseded-by: — · **Date:** 2026-08-16 (reconstructed)
- **Phase:** 4
- **Issues:** S1-B-10, CC-20b, P1-A/27
- **Citations:** 4 sites — `crates/store/src/invariants.rs:129`; `services/storage/src/main.rs:181`; `docker-compose.yml:134,228` (also `docs/running.md:569,656`)
- **Provenance:** re-derived from code (2026-08-16)

This record is **load-bearing.** `[ARCH]` §4.2's boot policy **is**
`open()`'s fail-closed gates plus this check. They run before anything
binds a port. After S2 they run before any subsystem starts.

## Context

`I-node-id` compares two `Root` values: the **32 raw bytes** of the
configured `node_key` file (`durable_set.rs:867-898`; `main.rs:443` —
the file is the secp256k1 secret, not a derived id) and, when present,
`AnchorInfo.node_id` (`meta.rs:95-96`). `Root`'s `Display` is `0x` plus
64 hex digits (`crates/types/src/primitives.rs:240-247`). If those two
values ever match, the stored field **is** that secret; printing
`AnchorInfo.node_id` is then printing the key. Live mismatch errors
`Display` the stored `Root` and omit the *configured* file bytes
(`invariants.rs:696-703`; `main.rs:1285-1292`). That is not a redaction
of the pairing value.

> **Dated note (2026-10-04).** Struck: the citation of
> `services/storage/src/main.rs:1285-1292` (and `main.rs:443` in the
> same paragraph) as a `node_id` site. Confirmed by grep on this date:
> `services/storage/src` does not reference `node_id`.
> `services/storage/src/main.rs` is a 9-line shim over
> `cc_storage_core::run()` and does not name `node_id`. The mismatch
> display in `crates/store/src/invariants.rs` is the literal
> `<redacted>` (**P1-G/1**); it does not print the stored value.
> On-disk `meta.node_id` is still the raw key until **S2R-J-07**.
> Superseded by **P1-G/2 / S2R-J-07**. The sentences above are the
> original text.

That compare is **not** a discv5 `NodeId` and is **not**
`get_custody_groups(node_id, cgc)`. `docs/running.md:563-569` tells the
custody-group story; the opener does not call that function and does not
derive a discv5 id from the file.

No production path writes `AnchorInfo.node_id` today. The row is put in
tests and `bin/cc-store` fixtures. p2p writes the `node_key` **file**
(identity volume, RW); storage mounts that volume `:ro` and sets
`CC_STORAGE_NODE_KEY_PATH=/identity/node_key`
(`docker-compose.yml:134-140,226-230`). The check still refuses when
the row is already on disk and disagrees, or when the path is set, the
file is missing, and the row is present. `expected_node_id: None` skips
the invariant (`invariants.rs:128-131`) — bootstrap, offline tools, and
tests, not a configured production open of a populated store.

> **Dated note (2026-10-04).** Struck: "No production path writes
> `AnchorInfo.node_id` today." False since the beacon-core boot path.
> `run` calls `open_and_stamp`, which calls `persist_anchor_node_id`
> (`bin/beacon-core/src/boot.rs`; `crates/storage-core/src/open.rs`).
> That writes `meta.node_id` and, when an `AnchorInfo` row is already
> present, updates `AnchorInfo.node_id`. It does not create a missing
> `AnchorInfo`. The same claim remains, unedited, in Decision ("Do not
> claim a production writer of `AnchorInfo.node_id` until one exists")
> and in Consequences ("Until a production writer lands"); both are
> struck by this note. Pairs with **P1-G/1**: that writer still stores
> the raw key in `meta.node_id` until **S2R-J-07**, and the mismatch
> display is the literal `<redacted>`, not the key bytes. Superseded
> by **P1-G/2 / S2R-J-07**. The paragraph above is the original text.

## Decision

**Refuse `open` when a configured node key does not match a stored
`AnchorInfo.node_id`.** Callers that enforce the pairing **must** supply
the expected `Root` (`invariants.rs:128-131`). Production `open_store`
loads the 32 file bytes and passes
`StoreOpenOptions::with_expected_node_id`. Then:

- Path set, file present, `AnchorInfo` present, bytes ≠ `node_id` →
  `StoreError::InvariantViolation { invariant: "node_id", … }`. Refuse.
- Path set, file missing, `AnchorInfo` present → refuse
  (`refuse_missing_key_if_anchor_present`). No skip.
- Path set, file missing, no `AnchorInfo` (first boot) → skip.
- Path unset → skip. Compose must not leave it unset.

Do not silently re-backfill. Do not document the error string as
secret-safe. Do not claim a production writer of `AnchorInfo.node_id`
until one exists.

## Consequences

What this makes easy:

- A configured key that disagrees with a stored `AnchorInfo` is a named
  boot failure, not a continue-and-serve.
- S2 can delete the 30 s restore grace: this check plus the other
  fail-closed gates *are* the boot policy (`[ARCH]` §4.2).

What this makes hard:

- Restoring `cc-store-data` without `cc-p2p-identity` (or the reverse)
  will not start once an `AnchorInfo` row exists. That is the operator
  contract (`docs/running.md:561-569`).
- Offline tools that omit the key skip `I-node-id`; they must not be
  copied into the production opener.
- Until a production writer lands, a store that never grew `AnchorInfo`
  will not fire the mismatch arm (missing-file + no row still skips).

What this forbids:

- Opening a store that already has `AnchorInfo` when the configured key
  mismatches or the file is missing.
- Skipping the check because "we will just re-backfill."
- Equating this compare with discv5 `NodeId` /
  `get_custody_groups(node_id, cgc)`.
- Treating exclusive-open (`DatabaseLocked`) as this invariant — that
  is a different lock (`ADR-R-05` / Q-2).
- Starting any subsystem (after S2, anything in `beacon-core`) before
  this check has passed.

## Alternatives considered

**Skip when the key file is missing, even if `AnchorInfo` exists.**
Rejected: that is the restore-beside-a-new-key hole. First boot (no
anchor) may skip; a populated store may not.

**Derive a discv5 `NodeId` and compare that.** Not what the live pairing
does. The stored `Root` is compared to the 32 raw key-file bytes.
Changing the surface is a new record.

**Fail-open and re-backfill.** Rejected in `main.rs:183-186` and
`docs/running.md:567-569`.

## Refactor impact

**Survives and is load-bearing** — `[ARCH]` §4.2. S2 moves `open()`
into `bin/beacon-core` / `storage_core::open`; the gates stay
fail-closed and still run first. Citation line numbers in
`services/storage/src/main.rs` will shift when the file becomes a thin
shim; the invariant lives in `crates/store/src/invariants.rs`.
