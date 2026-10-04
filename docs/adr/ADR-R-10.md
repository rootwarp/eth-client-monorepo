# ADR-R-10 — The paired node id is the public-key fingerprint

- **Status:** accepted · superseded-by: — · **Date:** 2026-10-04
- **Phase:** refactor (the only rollback-breaking change in the S2R fold)
- **Issues:** S2R-J-07
- **Citations:** `plan/architecture.md` §6.5 ⟡ D-22a / ⟡ D-22c; `plan/prd.md` P1-G/2 / R-16; `plan/research/q7-identity-and-config-integrity.md` §1.4; `docs/adr/ADR-P4-13.md`; `docs/s2-rollback.md`; `crates/node-key/src/lib.rs` (`node_id_fingerprint`); `crates/store/src/meta.rs` (`KEY_NODE_ID`, `KEY_NODE_ID_SCHEME`); `crates/store/src/invariants.rs` (`check_node_id`, `stored_node_id`); `crates/store/src/schema.rs` (`SCHEMA_VERSION`); `crates/storage-core/src/open.rs` (`open`, `bind_node_id`, `rewrite_node_id_fingerprint`, `persist_anchor_node_id`); `crates/storage-core/src/boot.rs` (`open_store`); `bin/beacon-core/src/boot.rs` (`boot_with_preset`); `services/storage/src/main.rs`
- **Provenance:** new — Q-11, accepted at the architecture gate on 2026-10-04. Supersedes ADR-P4-13, including that record's stale `services/storage/src/main.rs` citation. This record does not revise ADR-R-11.

This is **ADR-R-10**. It is the one-way door. Every earlier commit in the
fold stays a legal rollback target. This one does not, without a backup
taken before the rewrite.

## Context

ADR-P4-13 pairs the store with the **32 raw bytes** of the configured
`node_key` file. That file is the secp256k1 secret. `AnchorInfo.node_id`
and `meta.node_id` are the same `Root`. A match means the stored field
**is** the secret. ADR-P4-13's header still cites
`services/storage/src/main.rs`, and its body cited `main.rs:1285-1292`
and `main.rs:443` as the mismatch display. That file is a 9-line shim
over `cc_storage_core::run()` and does not name `node_id`. Those
citations are stale. This record supersedes them with the decision
below; it does not keep the raw key as the paired value.

The replacement value already exists and was unused.
`node_id_fingerprint` is `SHA256("cc-node-id-v1" ‖ uncompressed pubkey)`
(⟡ D-22a). The uncompressed key is 65 bytes. The digest is 32 bytes.
The legacy secret is also 32 bytes. Length cannot tell them apart
([Q7] §1.4). A reader that branches on `node_id.len()` will treat a
fingerprint as a secret, or a secret as a fingerprint.

`cc-store` compares 32 opaque bytes. `Store::open` in production is
called with `expected_node_id: None`. The store does not hash, does not
know the scheme, and does not recover a secret from the stored root.
`stored_node_id` prefers `meta.node_id` and falls back to
`AnchorInfo.node_id`. `persist_anchor_node_id` writes both rows and
refuses to overwrite a stored id that is neither `Root::ZERO` nor the
new value. Using that function for the secret→fingerprint rewrite would
refuse the migration. The rewrite is a different commit.

`SCHEMA_VERSION` is `1`. Unknown meta keys are safe for an older binary:
open's registry checks table names, not keys. A new value under
`meta.node_id` is not safe. The older binary compares those bytes to
the raw secret and refuses. That refusal is the one-way door. Bumping
`SCHEMA_VERSION` is not required and is not done. ADR-R-11's digest
buckets, its refusal of a populated re-stamp, and its refusal to
substitute `Root::ZERO` for an absent genesis validators root are
untouched.

Two hosts open this store. `bin/beacon-core` `boot` and the compose
storage host (`services/storage` → `cc_storage_core::run` →
`open_store`). A host that still pairs the raw secret will refuse a
store the other host has migrated, or will keep writing the secret.
Both migrate. `PendingStore::pair` still stamps the root it is given
and does not write the scheme. Tests and the pre-migration rollback
rehearsal keep calling it with the raw secret. Production hosts call
`bind_node_id`.

## Decision

**The paired node id is the fingerprint. Discriminate by
`meta.node_id_scheme`, never by length. Migrate a matching legacy id
in one batch, then pair the fingerprint. `cc-store` stays compare-only.**

| Stored scheme | Test | Action |
|---|---|---|
| absent | stored node id equals the configured raw secret | one batch: rewrite `meta.node_id` and `AnchorInfo.node_id` to the fingerprint, set scheme = `1`, then pair the fingerprint |
| absent | any other **present** stored id | refuse with a named reason; print neither value |
| `1` | — | pair the fingerprint; ordinary equality |

The scheme value is the single byte `1`, not SSZ and not a `Root`. The
key is `node_id_scheme` (14 bytes, under the 16-byte meta cap). Absent
means legacy. Any other byte is refused by name, before side-key
writes, and the byte is not printed.

**No stored id is not "anything else".** There is no stored value to
compare and none to leak. First boot, and a scheme-`1` store whose id
row is missing, stamps the fingerprint and scheme `1` in that same
batch. `Root::ZERO` stored on the anchor with no `meta.node_id` is a
present value. It is not the secret, so it is refused. The migration
does not treat `ZERO` as empty. `persist_anchor_node_id` still does,
for the stamp path; that function is not the rewrite.

The rewrite reads `AnchorInfo` if the row exists, then puts three
things in one `Engine::commit`: `meta.node_id`, `AnchorInfo.node_id`
when that row exists (no new `AnchorInfo` is created), and scheme
byte `1`. A crash before that commit leaves the previous scheme and
both rows. A crash after it leaves scheme `1` and both rows on the
fingerprint. There is no committed state in which one row is the
secret and the other is the fingerprint.

The composer holds the secret. `open` is still called with that secret
as `NodeIdExpectation::Present`, `StoreOpenOptions::with_expected_node_id`
stays `None`, the missing-key refusal still runs, and the legacy
mismatch still runs **before** `reconcile_config_side_keys`. Scheme `1`
skips the secret compare there: the stored root is not the secret, and
comparing it would refuse every reopen. Equality against the
fingerprint is `bind_node_id`, after `open` returns. A scheme-`1`
mismatch refuses without printing either root. A legacy mismatch keeps
the existing redacted `node_id` error and still runs before side keys.

The fingerprint is not invertible to the secret. Logs and errors on
the refusal path do not include the secret, the fingerprint, or the
stored id. The literal `node_id` stays in the message.

Both production hosts call `bind_node_id(legacy, fingerprint)`:
beacon-core `boot_with_preset`, and `open_store` on the compose host.
The resume check on that host compares the fingerprint, not the raw
secret. `crates/beacon-inproc` is not a production host and is not
changed; it still stamps the raw secret and will refuse a migrated
store.

This does not re-stamp a populated config digest, does not write
`Root::ZERO` for an absent genesis validators root, and does not bump
`SCHEMA_VERSION`.

## Consequences

What this makes easy:

- A copied `node_id` is not a copied key. The store can be compared
  without holding the secret.
- One batch means a crash cannot publish a mixed scheme.
- An older binary's refusal is deterministic: the stored bytes are no
  longer the secret it expects.

What this makes hard:

- past this commit, no rollback without a pre-migration backup. The
  backup has to be taken before the first open that rewrites the id.
  Open will not convert the fingerprint back. The previous binary
  refuses the rewritten store. That line is also in `docs/s2-rollback.md`.
- A store stamped with some other 32 bytes, including another node's
  secret, is refused rather than overwritten. The operator needs the
  key that produced the stored id, or a backup. The error does not
  reveal which.
- `pair` of the raw secret against a migrated store fails the overwrite
  refusal inside `persist_anchor_node_id`. Callers that reopen after
  `boot` use `bind_node_id`.

What a reader can check:

- `rewrite_node_id_fingerprint` has one `engine.commit` covering both
  rows and the scheme byte.
- `bind_node_id` is the production entry. `open.rs` does not grow a
  `fn migrate` on the runtime; hot/cold migration stays in `migrate.rs`.
- `cc-store` has no call to `node_id_fingerprint`.
- ADR-P4-13's status is `superseded-by ADR-R-10`.
