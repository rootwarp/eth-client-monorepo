# Node-key rotation (Q-12)

The operator owns rotation. This page is the procedure and the custody
advisory. It ships no tool, no flag, and no in-process rotation. No rotation
was performed to write it. Rotation is not reversible.

The secret is the 32 raw secp256k1 bytes at `node_key_path`. It is a custody
input (`services/p2p/src/identity.rs`): one file is both the libp2p identity
and the discv5 identity. The helper that loads or creates that file is
`cc_node_key::load_or_create` in `crates/node-key/src/lib.rs` (crate
`cc-node-key`). `services/p2p` calls it through `identity::load_or_create`
before anything binds.

## Advisory

Treat every existing `store.redb` backup, including every pre-migration
backup, as containing the secret. redb is copy-on-write, and freed pages are
not documented as zeroed, so deleting or rewriting a row does not make an old
file safe. Treat every captured stderr line the same way: as containing the
secret. Rotation does not scrub either. The guidance is to rotate the node
key, not to edit the backup.

On-disk `meta.node_id` is still the raw key where a writer has stored it.
`load_or_create` does not call `node_id_fingerprint`, and this procedure does
not either. That function is `SHA256("cc-node-id-v1" ‖ uncompressed pubkey)`
(`NODE_ID_FINGERPRINT_DOMAIN`, `b"cc-node-id-v1"`). It is housed for a later
migration. This page does not run that migration and does not claim
`meta.node_id` changed.

## Custody

A new secret is a new discv5 `NodeId`. `get_custody_groups(node_id, …)` is a
deterministic function of that `NodeId`, so the custodied column set changes.
The truth of `earliest_available_slot` changes with it: the advertisement is
about the columns this identity holds, not the columns the previous identity
held. Rotation is a **serve-window event**, not a free action. Record it in
the key-rotation sibling in `docs/phase-4-soak.md` (clause 1, same columns as
the table whose header includes advertised `earliest_available_slot`). That
row is **`NOT_RUN`**. Do not invent the date, multiaddr, head slot, or
pass/fail. This page does not claim the advertised window has already moved.

Putting the old file back is not an undo. It does not restore a serve window
advertised under the new `NodeId`, and it does not remove the secret from
backups or captured logs.

`I-node-id` refuses `open` only when a stored identity row is already present
(`meta.node_id`, otherwise `AnchorInfo.node_id`). `check_node_id` skips when
no identity row exists (`crates/store/src/invariants.rs`). Compose storage
does not write one. A missing refusal is not permission to serve the old
store under the new key.

A beacon-core store that already paired the old secret fails the next `open`.
That failure ends the procedure. Do not clear `meta.node_id`.

## Procedure

The new key is created with `create_new` on the final path. There is no
temporary file and no rename onto that path. Do not copy a secret into a
`0644` file. Do not `chmod` a file that was group- or world-readable and
then keep it as the key.

1. Stop every process that has the key file or the store open, including
   `p2p` and `beacon-core`. Nothing here rotates a live file.
   `load_or_create` does not replace a file that exists.
2. The final path is the configured `node_key_path`. When config omits it,
   `DEFAULT_NODE_KEY_PATH` is `./data/node_key` (`config/p2p.toml`,
   `config/beacon-core.toml`). Compose keeps the file on `cc-p2p-identity`:
   `CC_P2P_NODE_KEY_PATH=/app/data/node_key`, read-only at
   `/identity/node_key` for storage. The path must be non-empty and must not
   contain `..`. Non-unix load and create are refused.
3. Retain the old key by renaming that existing file off the final path.
   Rename keeps the inode, so the mode is preserved. Never copy it (`cp` or
   a redirect follows the umask and is often `0644`). Never place it on a
   world-readable path. The destination must not be the configured path.
   The final path is absent when this step ends.
4. Do not pre-create the replacement. A shell redirect follows the umask and
   is often `0644`, which the helper refuses. Do not write a temporary file
   and rename it onto the final path.
5. With the final path still absent, start `p2p` only.
   `identity::load_or_create` calls `cc_node_key::load_or_create`. A missing
   path takes the create arm: `create_new`, `OpenOptions` with
   `create_new(true)` (`O_EXCL`) and mode `NODE_KEY_MODE` (`0o600`) on that
   final path, then `sync_all`, then an fsync of the parent directory. The
   scalar is drawn by the helper. Length is checked at 32 before
   `SecretKey::from_slice`.
   An existing path, or `EEXIST`, is failure. The helper would load the
   other file and not clobber it; do not adopt that file. The
   `node identity loaded` line is the same for a load and a create, so it
   does not prove this start created the key. Continue only when this start
   created the final path and the mode is `0600` or stricter owner-only
   (group and other bits clear). Otherwise stop. Do not `chmod` the
   unexpected file into service. To try again the final path must be absent;
   rename an unexpected file aside under step 3 (mode preserved, never
   copy) and do not serve with it.
6. Do not mint the file by starting `beacon-core`. `ensure_node_key` in
   `bin/beacon-core/src/boot.rs` is not `cc_node_key::load_or_create`. When
   the path is absent it writes with `std::fs::write` at the umask, commonly
   `0644`. It does not use `create_new`. `load_or_create` refuses that mode
   and does not unlink the file. A beacon-core create in the window after
   the old file was renamed aside must be deleted, not `chmod`'d into
   service. Those bytes are an exposed secret. Do not keep that file as the
   rotated key. The final path has to be absent again before another create.
7. This step is the custody consequence, not a claim that the advertised
   window has already moved. The `node identity loaded` line
   (`peer_id`, `node_id`, path — not the secret) in
   `services/p2p/src/service.rs` is the new discv5 `NodeId` only if step 5
   created the file. That `NodeId` would custody a different column set, so
   the truth of `earliest_available_slot` changes if the node serves. Do not
   serve the old store under the new key, whether or not `open` refused.
   Do not record the secret, and do not record `node_id_fingerprint` as if
   this step had written it. Do not fill the phase-4 sibling from this page.
   It stays **`NOT_RUN`**.

The mismatch drill in `docs/running.md` is not this procedure. Do not use
its overwrite (`or overwrite with a fresh key`). Do not expect the refusal
to print the stored id: the mismatch text is `stored node_id <redacted>`
(`check_node_id`). Do not paste the refusal anywhere.
