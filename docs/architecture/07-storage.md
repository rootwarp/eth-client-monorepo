# Storage

> **As built at `7d8833d`** (`develop`, 2026-08-16). Status labels and diagram legend: [README](README.md#status-labels).
> Target design: [`plan/architecture.md`](../../plan/architecture.md) §4 (and §2.3 for E5/E6, §9.1 for removing shape A).

Two crates own persistence. **cc-store** (`crates/store`) is a typed-key, opaque-value layer over
redb behind an engine seam. **storage-core** (`crates/storage-core`) opens the store, runs the
single writer task, implements the `ArchiveWrite` seam and, in shape A only, hosts prune, replay,
migration and the gRPC serve pool. The store is one file, `<data_dir>/store.redb`. In short:

- The only block ingress wired to a producer is shape B's import path (N1 `ingest_block_blocking`);
  shape B is not deployed, and N1 is IDLE: no new block passes the DA gate ([04](04-chain-core.md)).
- Shape A prunes a store nothing feeds and serves an empty storage serve window (`u64::MAX`).
- Nothing writes fork-choice scalars or snapshots, so the durable set is always empty and every boot
  is checkpoint sync or core-absent.

## 1. Status at a glance

| Component | Code (bare names: `crates/storage-core/src/`; `schema.rs`, `invariants.rs`, `blocks.rs`, `canonical.rs`, `keys.rs`, `engine/` are cc-store, `crates/store/src/`) | Shape A (`cc-storage` :9006) | Shape B (`cc-beacon-core` :9001) |
|---|---|---|---|
| open + fail-closed gates | `open.rs:209` | LIVE | LIVE |
| I-node-id check | `crates/store/src/invariants.rs:879` | INERT (A writes no identity row; a store moved from B carries one, see [s2-rollback](../s2-rollback.md)) | LIVE (`meta.node_id` stamped at every open) |
| resume schema check | `resume.rs:95`, `:162` | LIVE (re-checks schema + digest after open) | ABSENT (B never calls `run_resume_sequence`; `Store::open` checks the same rows) |
| durable set | `open.rs:246`, `resume.rs:176` | DEAD-AS-WIRED (outcome always "empty", only logged, `boot.rs:534-548`; its consumer E4 DELETED in 60c6200) | LIVE (always `None`) |
| writer task | `writer.rs:434` | LIVE (fed by prune P2) | IDLE (its only producer, N1, is IDLE) |
| `ArchiveWriter` object | `archive_write.rs:35` | DEAD-AS-WIRED (built, dropped: `boot.rs:563`) | IDLE (injected as `CoreConfig.archive`) |
| N1 blocks, `ingest_block_blocking` | `archive_write.rs:247` | ABSENT | IDLE (iff core) |
| N1 columns, `ingest_columns` | `archive_write.rs:226` | ABSENT | DEAD-AS-WIRED (no handle injected) |
| prune task | `prune/mod.rs:1097` | LIVE (wall-clock ticks) | ABSENT |
| replay task | `replay.rs:707` | IDLE (`split.slot == 0`) | ABSENT |
| Migrator | `migrate.rs:101` | DEAD-AS-WIRED (no driver) | ABSENT |
| E6 `WatchServeWindow` | `serve.rs:885` | LIVE (content static: `u64::MAX`) | ABSENT |
| E6 `GetBlocks*` / `GetColumns*` | `serve.rs:481-716` | DEAD-AS-WIRED (no client; window refuses) | ABSENT |
| E5 `PutBackfillBatch` | `serve.rs:757` | DEAD-AS-WIRED (no client) | ABSENT |
| history RPCs (3) | `history.rs` | DEAD-AS-WIRED (served, no in-tree client) | ABSENT |
| `StorageServer::stub` | `boot.rs:641` | DEAD-AS-WIRED (reached only with `enable_write_path = false`; the default is `true` and no shipped config sets it) | ABSENT |
| rollback rehearsal | `rollback_rehearsal.rs` | TEST-ONLY | TEST-ONLY |
| host `services/storage` | `services/storage/src/main.rs` | TRANSITIONAL (deployed) | ABSENT |
| host `bin/beacon-core` | `bin/beacon-core/src/boot.rs` | ABSENT | TRANSITIONAL (target, not deployed) |

## 2. Who opens redb

While a node holds a store, any other opener, even a read-only one, fails at once with
`StoreError::DatabaseLocked`: redb's lock is try-lock and never waits
(`crates/store/src/engine/redb.rs:20-25`; `crates/store/tests/q2_redb_exclusive_open.rs`). Numbers
give the boot order; shape B numbers match
[12 §3.2](12-boot-health-and-shutdown.md#32-shape-b-cc-beacon-core-seeds-before-bind), which has
line refs.

```text
 +----------------------------------------------+   +----------------------------------------------+
 | SHAPE A  cc-storage :9006, storage_core::run |   | SHAPE B  cc-beacon-core :9001 (not deployed) |
 | 1 config + dangerous-knob guard              |   | 1-2 config, network YAML, EngineApi (JWT)    |
 | 2 cc_bootstrap::init (telemetry)             |   | 3 ensure_node_key ./data/node_key            |
 | 3 open /app/data/store.redb                  |   | 4 open data/storage/store.redb,              |
 |   key /identity/node_key (ro)                |   |   then stamp meta.node_id                    |
 | 4 run_resume_sequence -> "empty"             |   | 5 durable_set() -> None (always)             |
 | 5 spawn_writer (no write_cursor seed)        |   | 6 cc_bootstrap::init  7 start_writer         |
 | 6 ArchiveWriter built, dropped; replay,      |   | 8 CoreConfig.archive = ArchiveWriter         |
 |   prune (CC-4A floor check), Migrator, serve |   | 9 seed / checkpoint sync / core-absent       |
 | 7 bind :9006 (StorageService)                |   | 10 bind :9001 (ChainService only)            |
 +----------------------------------------------+   +----------------------------------------------+
```

Paths: A `docker-compose.yml:140-145` (the key is the p2p identity volume, mounted `:ro`); B
`config/beacon-core.toml:10,15`. The format is identical; [`s2-rollback.md`](../s2-rollback.md)
moves a store between shapes. TEST-ONLY cc-beacon-inproc opens a `Store` directly.

> "beacon-core creates a missing node key without enforcing 0600." (`std::fs::write`, `bin/beacon-core/src/boot.rs:290-316`.)
> "No shipped configuration pairs cc-p2p with cc-beacon-core; how p2p attaches to shape B is not yet defined."

## 3. cc-store: engine seam and on-disk layout

### 3.1 Engine seam

- `Engine` (`crates/store/src/engine/redb.rs:232`) is a concrete struct; `engine/redb.rs` alone
  names `redb` (4.1.0), so a fjall swap replaces one file ([ADR-P4-01](../adr/ADR-P4-01.md)).
- Surface: `open` / `open_existing` / `open_read_only`; `read()` -> `ReadTxn` (`get`, `range`,
  `range_max`); `batch()` + `commit(Batch)`, which takes the mutex only for `begin_write` and
  applies all ops in one write transaction (`redb.rs:369`); `drop_table`; `compact`.
- Caps (`engine/mod.rs:19-28`): 512 live interned names, 65_536 ops per batch, 1_048_576 entries or
  512 MiB per range; `drop_table` un-interns (`redb.rs:430-449`), so rollover cannot fill the pool.
- Durability ([ADR-P4-05](../adr/ADR-P4-05.md)): `immediate` (default) = redb `Immediate`;
  `paranoid` adds two-phase commit; `none` is rejected by `Durability::parse` (`engine/mod.rs:55`).

### 3.2 Key space and hot/cold layout

Keys are fixed-width big-endian, so byte order is slot order. Block, column and snapshot values
are SSZ wire bytes that cc-store never decodes (meta records it does decode); it peeks only at
block `slot@100`, `parent_root@116`, `state_root@148` and column `index@0`, `slot@20`,
`parent_root@36` (`crates/store/src/blocks.rs:79-85`, `crates/store/src/columns.rs:80-91`).

```text
 slot ->  0 ........................... Split.slot | Split.slot+1 ..................... head
          COLD: finalized, canonical only          | HOT: unfinalized, forks coexist
 blocks   blocks_NNNNN  [slot u64be]               | blocks_hot  [slot u64be, root 32]
 columns  columns_NNNNN [slot u64be, idx u16be]    | columns_hot [slot u64be, root 32, idx u16be]
          -----------------------------------------+---------------------------------------------
 indices  block_slot_by_root   [root 32]            -> [slot u64be, region u8: 0 hot, 1 cold]
          column_slot_by_root  [root 32, idx u16be] -> [slot u64be]
          canonical            [slot u64be]         -> [root 32]
          state_roots          [slot u64be]         -> [root 32]
 other    da_status            [root 32]            -> [status u8, slot u64be]
          snapshots            [slot u64be]         -> BeaconState SSZ, uncompressed (ring, depth 4)
          meta                 [ascii key <= 16 B]  -> SSZ singleton (11 keys)
          fork_choice          registered in FIXED_TABLES, never used
 AT HEAD  Only blocks_hot, block_slot_by_root, canonical and meta have a production writer.
          meta.split is never written, so Split.slot = 0 and every row is HOT. Cold shards are
          written only by backfill (no client) and migration (no driver).
```

Key `->` value; `[a, b]` = fixed-width concatenation, integers big-endian; NNNNN = shard id. Full
inventory: [`storage-schema.md`](../storage-schema.md).

- Hot keys carry the root so forks coexist; cold keys are by slot, as cold rows are canonical
  ([ADR-P4-09](../adr/ADR-P4-09.md)). A cold shard spans 256 epochs (blocks) or 32 (columns); suffix
  `{:05}` widens past 99999 (`crates/store/src/keys.rs:11-14,59`; [ADR-P4-10](../adr/ADR-P4-10.md)).
  Registry: `FIXED_TABLES` (the 10 above) plus two shard patterns (`schema.rs:43`).
- With no Split row, `blocks_by_range` reads `canonical[slot]`, then `blocks_hot` (`blocks.rs:328`);
  backfilled cold rows are reachable only by root. Scalars live in `meta.fc_scalars`
  ([ADR-P4-06](../adr/ADR-P4-06.md)); snapshots are uncompressed ([ADR-P4-14](../adr/ADR-P4-14.md)).
- `canonical` is rewritten from each persisted block back to its fork point, deleting rows above
  that slot (`canonical.rs:139`). Import persists every block, so `canonical` is the ancestry of the
  **last persisted block**, not the fork-choice head; a non-head sibling rewinds it.

### 3.3 Meta records and their writers at HEAD

`meta` holds 11 SSZ singletons (`crates/store/src/meta.rs:24-44`):

| Key | Writer, shape A | Writer, shape B |
|---|---|---|
| `schema_version`, `config_digest` | `Store::bind` on a fresh store (direct commit, `schema.rs:508`) | same |
| `node_id` | never | `persist_anchor_node_id` at every open (direct commit, `open.rs:92`) |
| `write_cursor` | never (shape A calls `spawn_writer`, skipping the seed) | seeded `{1,0,0,ZERO}` by `ensure_write_cursor`; P0 restamps it |
| `prune_marks` | prune task, P2 | never (no prune) |
| `backfill_prog` | `PutBackfillBatch` only (no client) | never |
| `fc_scalars`, `split`, `anchor_info`, `column_info`, `serve_window` | never | never |

### 3.4 Open gates

`Store::open` (`crates/store/src/schema.rs:409`) refuses; it never repairs:

1. `Engine::open` creates the directory, refuses symlinks, maps a held lock to `DatabaseLocked`;
   `Store::bind` refuses unregistered tables (`UnregisteredTable`, `schema.rs:419-422`).
2. `schema_version`, `config_digest`: absent on a table-less store -> written; else `MissingMeta`,
   `SchemaVersionMismatch` (`SCHEMA_VERSION = 1`) or `ConfigDigestMismatch`, naming both values.
3. With `check_invariants = true` (default in both shapes) the eight invariants run (§3.5).
4. storage-core adds `refuse_missing_key_if_anchor_present` (`open.rs:234`): a configured key
   path with no file, on a store that has an identity row, refuses open. INERT in both shapes:
   A writes no identity row (a store moved from B carries one and could trip it), and B's
   `ensure_node_key` creates a missing key file before `open` (`bin/beacon-core/src/boot.rs:406`).

The digest is SHA-256 over SSZ of the six fork epochs, `SECONDS_PER_SLOT`, `BLOB_SCHEDULE`, GVR,
`MIN_VALIDATOR_WITHDRAWABILITY_DELAY` and `CHURN_LIMIT_QUOTIENT` (`schema.rs:142-160,224`).
storage-core always feeds it the bundled Hoodi config, a GVR defaulting to zero and those two
scalars hard-coded to 256 / 65_536 (`open.rs:214-215,322-358`; `schema.rs:196-205`); prune timing
uses the same config (§4.6). "The store's config digest does not currently distinguish networks."

What trips each gate (none has a repair path: no migration code, and `cc-store`, §6, cannot
repair or re-stamp). Rollback rules: [s2-rollback (a)](../s2-rollback.md#a-on-disk-format-is-unchanged)
(`docs/s2-rollback.md:99-100`); adding a fork: [06](06-consensus-primitives.md#33-fork-schedule-authority).

| Gate | Tripped by | Recovery |
|---|---|---|
| `UnregisteredTable` (`schema.rs:419-422`) | any on-disk table outside this binary's registry. A store last written by a newer build that added a table refuses in an older build: a rollback trap | none in code; run a build that registers the table |
| `SchemaVersionMismatch` | a store stamped with another `SCHEMA_VERSION` (`= 1`, `schema.rs:36`). `ConfigDigestPayload` may not gain a field without a bump (`schema.rs:139-140`) | none in code (no migration) |
| `ConfigDigestMismatch` | any digested input: a new `BLOB_SCHEDULE` entry, a changed fork epoch (Altair..Fulu) or `SECONDS_PER_SLOT` in the bundled Hoodi YAML (`open.rs:341-358`; `schema.rs:142-160`); setting or changing `genesis_validators_root` (storage: commented out at `config/storage.toml:134`; beacon-core's config, `bin/beacon-core/src/boot.rs:165`; unset = ZERO, `open.rs:322-339`). A dev host reads the source-tree YAML via `CARGO_MANIFEST_DIR` first (`open.rs:342-345`), so editing it changes the digest without a rebuild | none in code; restore the previous inputs |
| I-node-id (§3.5) | a node-key change beside a store with an identity row (B; A only for a store moved from B) | restore the paired key: store and key are one backup unit ([ADR-P4-13](../adr/ADR-P4-13.md)) |
| invariant scan `Limit` (§3.5) | one check scanning more than `max_open_scan_rows` rows | A: raise `max_open_scan_rows` (`config/storage.toml:35`); B has no key (`OpenOpts::default()`, `bin/beacon-core/src/boot.rs:256`); both: `check_invariants = false` skips the scans |

### 3.5 The eight invariants and I-node-id

Open mode is fatal on the first violation. Each check gets its own budget of `max_open_scan_rows`
rows (default 1_056_800, sized to the CC-4A block window) and fails with `StoreError::Limit` past it
(`invariants.rs:305`). PostPass mode (log and count) has no production caller. Most checks return
early when their meta row is missing (`crates/store/src/invariants.rs:480,728,775,844,883-889,958`):

| Label | Check | At HEAD |
|---|---|---|
| `contig` | every slot in `[AnchorInfo.oldest_block_slot, head]` is canonical or a recorded hole | INERT (no `anchor_info`) |
| `col_block` | every column row has a block at its slot | INERT (no column rows) |
| `split_fin` | `Split.slot <= finalized slot`; no `blocks_hot` row at or below the split | INERT (no `split`) |
| `ring` | if a Split exists: ring non-empty, depth within bound, newest <= split | INERT (no `split`) |
| `window` | `ServeWindow.earliest_available_slot` >= `AnchorInfo.oldest_block_slot` and >= `PruneMarks.blocks_up_to` | INERT (no `serve_window`) |
| `node_id` | stored node id equals the configured node key | A INERT, B LIVE |
| `shards` | every table registered; no shard wholly below the prune marks | INERT at open (`Store::bind` refuses an unregistered table first; no shard tables exist); `cc-store verify` runs it |
| `cursor` | `WriteCursor.slot` <= newest stored block slot (`canonical` + `blocks_hot`) | A INERT (no cursor), B LIVE |

**I-node-id** ([ADR-P4-13](../adr/ADR-P4-13.md)) compares the raw 32 node-key bytes with
`meta.node_id` (else `AnchorInfo.node_id`), derives no discv5 NodeId, and is skipped without a key
or identity row (`invariants.rs:879-905`). Shape B stamps `meta.node_id` per open, so a changed key
fails reopen; with `check_invariants = false` the stamp refuses (`open.rs:117`).

> "Shape B does not yet bound its store or order EL readiness before seed replay." With no prune
> or migration, `blocks_hot` grows until the `cursor` check's scan of `canonical` + `blocks_hot`
> (`invariants.rs:987-1003`) may hit the row cap. Not measured.

## 4. storage-core

### 4.1 Component view

```text
 [B] chain-core              [A] cc-p2p (p2p dials)                             [A] operators
     |                           ^                x                                 :
     | N1 blocks (core thread):  | E6 window      x E6 reads, E5 backfill           : history RPCs
     | IDLE                      | LIVE (static)  x (no caller)                     : (no client)
     | N1 cols (P2pStream task): |                x                                 :
     | x, no handle injected     |                x                                 :
 +---|-- cc-storage-core --------|----------------x---------------------------------:------------+
 | +-v--------------------+   +--|----------------v---------------------------------v----------+ |
 | | archive_write.rs     |   | serve.rs, backfill.rs, history.rs [A]   StorageService         | |
 | +----------------------+   +----------------------------------------------------------------+ |
 |   | P0: [B] IDLE, [A] x          | P1 backfill commit: x (no client)           reads |        |
 | +-v------------------------------v---------------+                                   |        |
 | | writer.rs: one tokio task                      |<--- P2 --- prune/ [A] LIVE        |        |
 | | writer classes P0 > P1 > P2 (biased select)    |<- - P2 - - replay.rs [A] IDLE     |        |
 | | 1 job -> 1 Batch -> 1 Engine.commit            |x    P1     migrate.rs [A]         |        |
 | +------------------------------------------------+                                   |        |
 |   |   prune/shards.rs drop_table (bypasses writer) --------------------------------->+        |
 |   |   open.rs  resume.rs  durable_set.rs  boot.rs [A]  metrics.rs                    |        |
 +---v----------------------------------------------------------------------------------v--------+
     | commit (one Batch per job)                                    reads, drop_table  |
 +---v----------------------------------------------------------------------------------v--------+
 | cc-store: Engine (only module naming redb) | schema + open gates | keys | meta                |
 | blocks | canonical | columns | split | snapshots | window | backfill_progress | invariants    |
 +-----------------------------------------------------------------------------------------------+
```

Edge status is in its label: `x` DEAD-AS-WIRED, `:` served, never dialed, `- -` IDLE; [A] / [B] =
that shape only. E6 reads and E5: no caller; p2p builds and drops a `StorageClient`
(`services/p2p/src/service.rs:391`), which has no E5 method. cc-store boxes name `crates/store/src/`
files. DELETED: E4 (60c6200), E7 (78a90e1). Edge ids:
[03](03-internal-contracts.md#3-edge-inventory). Feature `grpc` gates `boot`, `serve`, `history`
(`lib.rs:20-38`).

### 4.2 Public boot surface (`open.rs`)

| Function | Does | Used by |
|---|---|---|
| `open(data_dir, OpenOpts)` | durability, GVR, node key, `Store::open`, missing-key refusal | A (`boot.rs:413`), B (`bin/beacon-core/src/boot.rs:247`) |
| `persist_anchor_node_id(id)` (`open.rs:92`) | writes `meta.node_id`; refuses a different non-zero id | B (`bin/beacon-core/src/boot.rs:260`) |
| `durable_set(&OpenedStore)` | `None` if the store is empty, else the seed payload | B |
| `start_writer(OpenedStore, ..)` | `ensure_write_cursor`, then `spawn_writer` with `WriterBounds::default()` (32/64/256) | B |
| `start_writer_from_store(Store, ..)` (`open.rs:284`) | same as `start_writer` | TEST-ONLY (`crates/beacon-import/tests/import_durable.rs:246`) |
| `spawn_writer` (crate-private, `writer.rs:358`) | writer task + panic guard | A, with configured bounds |
| `StorageRuntime` (`open.rs:176`) | holds engine, writer handle, `ArchiveWriter`, shutdown `watch::Sender`; `writer_count()` is always 1 | B |

### 4.3 The single writer and its mailbox

[ADR-P4-04](../adr/ADR-P4-04.md): one tokio task owns every ordinary commit and reads three bounded
`mpsc` channels, one per writer class (`writer.rs:434-525`); bounds are `writer_p*_bound` in shape
A, hard defaults in B. Queue depth per class is set every loop.

```text
 PRODUCER (status)                           WRITER CLASS                WRITER TASK (tokio)
                                           +----------------------+     +------------------------+
 [B] ingest_block_blocking - - N1 - - - -->| P0  bound 32         |- - >| select! { biased;      |
 x [B] ingest_columns (no handle injected) | full: block          |     |   shutdown -> break    |
                                           +----------------------+     |   P0 -> commit_p0      |
 x [A] PutBackfillBatch (no client)        | P1  bound 64         |x    |   P1 -> commit_meta    |
 x [A] Migrator (no driver)                | full: block          |     |   P2 -> commit_p2 }    |
                                           +----------------------+     | 1 job -> 1 Batch ->    |
 [A] prune chunks, prune_marks ----------->| P2  bound 256        |---->|   1 engine.commit      |
 [A] replay snapshot - - IDLE - - - - - -->| full: drop newest    |     |   (sync fsync) -> redb |
                                           +----------------------+     | reply oneshot, yield   |
                                                                        | no drain on shutdown   |
                                                                        +------------------------+
```

`x` = DEAD-AS-WIRED; `- - ->` = IDLE; `---->` = LIVE; [A] / [B] = shape-only producer. Class ->
task edges: P0 A DEAD-AS-WIRED (ArchiveWriter dropped), B IDLE; P1 A DEAD-AS-WIRED, B DEAD-AS-WIRED
(mailbox exists; no producer hosted); P2 A LIVE, B DEAD-AS-WIRED (prune and replay not hosted).
Both shapes create all three mailboxes (`writer.rs:387-389`; B via `open.rs:301`). At HEAD only P2
carries jobs, and only in shape A.

- P0 carries blocks, columns, optional fc scalars and the `WriteCursor` in one batch (`commit_p0`,
  `writer.rs:528`); P1 opaque puts and deletes (`commit_meta`, `:664`); P2 prune and snapshot chunks
  (`commit_p2`, `:711`). A full P0 or P1 blocks the sender (`send().await` / `blocking_send`); a
  full P2 drops the newest (`try_send`), counted in
  `cc_storage_writer_chunk_dropped_total{class="p2"}`.
- The commit and its fsync run synchronously inside the async task (no `spawn_blocking`);
  unmeasured under load, see [13-concurrency-model.md](13-concurrency-model.md).
- Before each put the commit reads the key: identical bytes are skipped; different bytes under a
  `blocks_*` / `columns_*` key abort as a key collision (§5.1). Index and meta rows may change.
- Shutdown wins the biased select: queued jobs are **not** drained. If `StorageRuntime` drops while
  the runtime keeps running, the `shutdown.changed()` arm may spin (in shape B only once `run()`
  returns). "Not observed; traced only."
- Writes that bypass the writer: `write_bootstrap`, `ensure_write_cursor`, `persist_anchor_node_id`
  (before the writer starts); prune's `Engine::drop_table` (`prune/shards.rs:151,200`).

### 4.4 ArchiveWrite: the import persist path (N1)

`ArchiveWriter` (`archive_write.rs:35`) is the only production `cc_seam::ArchiveWrite`. Shape B
passes it to chain-core as `CoreConfig.archive` (`bin/beacon-core/src/boot.rs:428,448`); the core
thread calls `persist_imported_block` -> `ingest_block_blocking` from `finish_imported`, after fork
choice applied the block; a DUPLICATE import persists only if `block_is_durable` is false
(`crates/chain-core/src/import.rs:855,879,988`; [05](05-block-lifecycle.md)).

1. `bind_ingest_block` (`archive_write.rs:265`): the `slot@100` and `parent_root@116` peeks must
   match the claims (genesis self-parent remap allowed; a self-parent only with no durable head),
   and `block_root` must equal `hash_tree_root` under a Fulu decode. An already-durable body
   returns `Ok` without submitting, so it cannot rewind `canonical`. That decode trips the
   storage-core opaque-bytes rule of `scripts/check-crate-dag.sh` (exit 1 at HEAD since d3d7f60;
   `archive_write.rs:26,332,335,395,803`; [14](14-testing-and-enforcement.md)).
2. `block_unit`: cursor `{session_id, seq + 1, slot, root}` from the stored cursor; a missing
   cursor is `Unavailable`, never invented. Continuity bind (`archive_write.rs:131`): the parent
   must be durable or the first row of the same batch, else `InvalidArgument`.
3. `blocking_submit_p0_committed`: `blocking_send` into P0, then `blocking_recv` of the commit,
   with no deadline. `commit_p0` writes `blocks_hot`, `block_slot_by_root`, the `canonical`
   rewrite and `meta.write_cursor` in one transaction; not `state_roots`, `da_status` or fc scalars.

- Errors (`archive_write.rs:99`): `ShutDown`, store errors -> `Unavailable`; `Codec` / `Limit` ->
  `InvalidArgument`; key collision aborts.
- Static traces, moot while N1 is IDLE: the first child after a checkpoint sync would fail the
  continuity bind (`INVALID_ARGUMENT`) after fork choice applied it, as the anchor is never
  ingested; with two or more stored blocks the self-parent probe hits `Limit` (-> `Unavailable`).
- `ingest_columns` validates the index and the `parent_root@36` peek and restamps the cursor with
  `seq` unchanged, but no host injects the handle into `P2pStreamDeps`
  (`crates/chain-core/src/p2p_stream.rs:154,697`), so columns fail closed (Ignore/Internal).
- "The trait documents Backpressure; the implementation blocks without a deadline."
  (`crates/seam/src/lib.rs:321-329` says both; chain-core's `Backpressure` arm is unreachable.) "No
  test drives the production persist path (core thread + `ingest_block_blocking` with a real
  writer)."

### 4.5 Durable set, resume and seed

The store is empty with neither `meta.fc_scalars` nor a snapshot (`resume.rs:176`); nothing writes
either. Shape A `run_resume_sequence` (`resume.rs:95`) checks the schema and reports "empty", which
`boot.rs` only logs. Shape
B `durable_set()` returns `None`: with `checkpoint_providers` set the composer checkpoint-syncs,
else the process stays core-absent (`NOT_BOOTSTRAPPED`). A non-empty set (`resume.rs:190-389`) would
carry the newest snapshot, anchor block, every hot block in `(snapshot, head]` with siblings (each
**requiring** a `da_status` row) and raw fc scalars for `seed_from_durable`.

### 4.6 Prune (shape A)

The prune task wakes every 4 s (`prune/mod.rs:1097`) and derives its epoch from wall clock and
`genesis_time`, not finality. `FULU_FORK_EPOCH` and `SECONDS_PER_SLOT` come from the bundled Hoodi
config (`boot.rs:586-587`); only the CC-4A floor reads `network_config` (`boot.rs:382-406`). An
unset `genesis_time` falls back to the Hoodi fixture (`prune/mod.rs:1191`);
`config/storage.toml:111` sets it.

| Pass | Runs when | Mark (exclusive) |
|---|---|---|
| columns | `epoch % 32 == 0` (`prune_columns_epochs`) | start of `max(epoch - 4096, FULU_FORK_EPOCH)` minus margin |
| blocks + state_roots | `epoch % 256 == 0` (`prune_blocks_epochs`) | start of `epoch - floor` minus margin; floor = CC-4A, 33024 epochs on Hoodi |
| snapshot ring | after a snapshot write | newest `snapshot_ring` kept; IDLE (no snapshot is written) |
| unfinalized | migration (`migrate.rs`), not the prune task | `Split.slot`; DEAD-AS-WIRED (no driver) |

- I2 ([ADR-P4-08](../adr/ADR-P4-08.md)): a blocks mark above the floor is refused, not clamped,
  and counted (`cc_storage_window_increase_rejected_total`, `prune/blocks.rs:85`). INERT at HEAD:
  the pass derives its mark as floor minus margin from the same floor (`prune/blocks.rs:74-77`,
  `prune/mod.rs:580-589`), so only the test-only `run_blocks_pass_with_mark` can trip it.
- Deletes go out as 512-key P2 chunks, each awaiting its commit. A 2 s deadline or a full P2 queue
  abandons the pass with the cadence marker held; it retries on the next 4 s tick while the epoch is
  still a cadence multiple, else at the next cadence epoch (`prune/chunk.rs:69`,
  `prune/mod.rs:452-475`). At most one covered shard is dropped per tick; then marks advance
  (`meta.prune_marks`, via P2).
- `da_status` is never pruned; `disk_alarm_bytes` is an alarm, never a trigger. A bad CC-4A floor
  refuses start (§5.1). Floor arithmetic: [`serve-windows.md`](../serve-windows.md).

### 4.7 Serve and the storage serve window (shape A)

`StorageServer` serves `eth.storage.v1.StorageService`, 10 RPCs behind `UnaryPermitService`
(`boot.rs:643`; method list `boot.rs:52`). Admission: 4 serve permits, 2 s queue timeout, then
`RESOURCE_EXHAUSTED`, plus one permit for `GetSnapshotState` (`serve.rs:120-129`). Ranges cap at 128
slots, by-root at 128 ids. A read takes one `ReadTxn`, loads slot by slot and stops before the next
whole block (columns: whole slot) would push the response past `serve_buffer_bytes` (64 MiB), so
peak memory is <= 64 MiB x 4 permits (`serve.rs:1326-1393`). It drops the transaction before sending
and never takes `SplitLock`. A range with `start_slot < earliest_available_slot` is `UNAVAILABLE`;
by-root skips rows below it (`serve.rs:503`).

The **storage serve window** is the `meta.serve_window` row. Nothing writes it, and `publish_window`
/ `derive_and_publish_window` are `#[allow(dead_code)]` (`serve.rs:281,300`), so `WatchServeWindow`
emits only `empty_window()` with `earliest_available_slot = u64::MAX` (`serve.rs:1196`) and every
gated read is refused. p2p copies the stream into its **advertised window**
([ADR-P2-14](../adr/ADR-P2-14.md); [11](11-p2p-reqresp-and-sync.md)) and issues no reads (§4.1). The
three history RPCs (no in-tree client) are admitted but not window-gated (`history.rs:114-196`);
outside the ring `GetSnapshotState` returns `FAILED_PRECONDITION` (`NOT_AVAILABLE`), never replays.

### 4.8 Backfill write path (E5, shape A)

DEAD-AS-WIRED (no client). `PutBackfillBatch` (`serve.rs:757`) takes no permit. Admission
(`backfill.rs:122-396`): blocks sorted descending, no duplicate slots, slot-contiguous; slot peek
equals claim; `higher.parent_root == lower.root`; progress required, monotone and bound to the
batch; the first row extends the durable frontier. Cold blocks, `canonical`, cold columns and
`meta.backfill_prog` go to the writer as one P1 job, awaited (`commit_backfill`, `serve.rs:1483`).

### 4.9 Replay, migration, metrics, rehearsal

- **Replay task** [A] (`replay.rs:707`), IDLE: polls every 2 s, skips while `split.slot == 0`
  (always). It would replay from the newest ring snapshot (`NoVerification`, always-Valid engine),
  treat a state-root divergence as fatal, and submit the snapshot plus evictions on P2.
- **Migrator** [A] (`migrate.rs:101`), DEAD-AS-WIRED: kept alive (`boot.rs:604`); nothing calls
  `on_finalized_checkpoint`. Under the split write lock it would move <= 2048 canonical hot slots to
  cold, write `state_roots`, drop non-canonical hot rows and put `Split`, as one P1 job.
- **Split lock vs MVCC.** `crates/store/src/split.rs:3-11` sets a lock order: `split` first and
  released last; every read that can race migration takes `read_recursive()` (a plain `read()`
  could deadlock behind a waiting writer) before its `ReadTxn`. The Migrator (`migrate.rs:111`;
  write lock `:161`, held through the P1 commit) and replay (`replay.rs:442`) follow it. Serve does
  not: it reads the Split row inside its own `ReadTxn` (`serve.rs:1279-1283`, columns `:645`) and
  relies on redb MVCC plus each migration window committing `Split` in the same batch as the moved
  rows (`split.rs:13-23`). Moot at HEAD (no Split row exists). Not yet decided: which rule is
  authoritative; it needs settling before the Migrator gets a driver.
- **Rollback rehearsal** (`rollback_rehearsal.rs`, TEST-ONLY, f1d035a): writes a store with the P0
  writer, shuts down cleanly, reopens it with the `e854b1d` open gates.

**Metrics.** `StorageMetrics::register` (`metrics.rs:469`) registers 37 families
(`metrics.rs:528-735`) in both hosts (B: `bin/beacon-core/src/boot.rs:421`) and seeds their series
(`metrics.rs:783`). Names are the struct fields; each is exposed as `cc_storage_<field>`, and
prometheus-client appends `_total` to a counter whose field lacks it. Shape A status:

| Shape A | Families | Written at |
|---|---|---|
| LIVE (prune task) | `disk_bytes` (file length, every 4 s tick), `pruned_bytes`, `pruned_rows`, `prune_seconds`, `prune_lag_epochs`, `prune_deadline_exceeded`, `shard_dropped` | `prune/mod.rs:367-398` (called `:1118`), `prune/chunk.rs:142`, `prune/shards.rs:154,203` |
| LIVE (P2 commits only) | `commit_seconds`, `written_bytes` (storage-class label), `writer_queue_depth{class}`, `writer_chunk_dropped{class}` | `writer.rs:246,450-466,711` |
| LIVE (once, at boot) | `restart_seconds{phase}` | `resume.rs:155` (`open` phase: `boot.rs:510`) |
| INERT | `window_increase_rejected` (I2 cannot trip, §4.6); `invariant_violation{invariant}` and `key_collision`, counted only on a key collision just before `abort()`, so no scrape sees them (no open-time check counts) | `prune/blocks.rs:109`; `writer.rs:752-771` |
| DEAD-AS-WIRED (no client) | `read_txn_seconds`, `serve_seconds`, `serve_total`, `serve_bytes`, `serve_admission_wait_seconds`, `backfill_oldest_slot`, `backfill_bytes` | `serve.rs:352-460`, `backfill.rs:404` |
| IDLE (replay skips) | `snapshot_seconds`, `snapshot_bytes`, `snapshot_ring_depth` (also set once at construction), `replay_divergence` | `replay.rs:199,470,633` |
| DEAD-AS-WIRED (no Migrator driver) | `split_slot` (set to 0 once at construction, `migrate.rs:79-82`) | `migrate.rs:200` |
| DEAD-AS-WIRED (only `#[allow(dead_code)]` publishers) | `earliest_available_slot`, `window_branch`, `window_hole_slots` | `serve.rs:281-338` |
| DEAD-AS-WIRED (no production writer; seed 0 only) | `bytes_total`, `rows_total`, `live_set_bytes`, `tables_total` (`metrics.rs:415-419`); `following_head` (setter `#[cfg(test)]`, `:903`; scripts wait on it, [14](14-testing-and-enforcement.md)); `write_behind_lag_slots`; `stream_reconnect` (`observe_reconnect`, `writer.rs:789-797`, has no caller) | -- |

Shape B hosts only the writer: `commit_seconds`, `written_bytes` and `writer_queue_depth` are IDLE
(N1 is IDLE), the key-collision pair is INERT, and every other family, `restart_seconds` included,
is DEAD-AS-WIRED. Effects: the growth ratio `disk_bytes / live_set_bytes` named at
`prune/mod.rs:43-44` has a denominator stuck at 0; `scripts/storage-plateau.sh:5,267` measures the
slope of `cc_storage_bytes_total`, which stays 0; and the plan's window of record,
`cc_storage_earliest_available_slot` ([11](11-p2p-reqresp-and-sync.md#planned-changes)), reads 0
while E6 emits `u64::MAX`.

## 5. The two hosts compared

| Aspect | `services/storage` -> `cc_storage_core::run` (A) | `bin/beacon-core` (B) |
|---|---|---|
| Config | `config/storage.toml`, `CC_STORAGE_*` | `config/beacon-core.toml`, `CC_BEACON_CORE_*`; no writer, prune or serve keys |
| gRPC | StorageService :9006, health peer `chain` | ChainService only (:9001) |
| Shutdown | pre-drain sends `true` on the storage watch (`boot.rs:429`); writer, replay, prune stop | pre-drain joins the core thread only; `StorageRuntime::shutdown()` never called |

Open order: §2; tasks: §1; writer: §4.2; threads: [13](13-concurrency-model.md); no storage-service
liveness signal ([12](12-boot-health-and-shutdown.md)). DEAD-AS-WIRED shape A config:
`commit_slots`, `commit_max_events`, `commit_max_latency_ms` (loaded and logged only);
`epochs_per_migration`, `snapshot_epochs` (feed only the undriven Migrator and IDLE replay task);
`debug.crash_point` (gated by the dangerous-knob guard and logged; no fault injected,
`boot.rs:200-207,281-287`).

### 5.1 Failure modes

| Failure | Shape | Effect | Code |
|---|---|---|---|
| open gate refusal (lock, registry, schema, digest, invariant, missing key) or (A only) resume schema check | A, B | `run()` returns Err before bind | `crates/store/src/schema.rs:409-473`, `open.rs:209-240`, `boot.rs:547-549` |
| CC-4A floor missing, unreadable or zero (no `retention_override`) | A | Err before bind (the writer is already spawned) | `boot.rs:382-406,587` |
| writer panic | A, B | panic guard calls `exit(1)` | `writer.rs:413-427` |
| key collision (different bytes under a `blocks_*` / `columns_*` key) | A, B | `abort()` after counting `cc_storage_invariant_violation_total{invariant="key_collision"}` | `writer.rs:752-771` |
| replay state-root divergence | A (IDLE) | `exit(1)` | `replay.rs:469-484` |
| `ensure_write_cursor` fails | B | warn only; every later ingest -> `Unavailable` | `open.rs:298-299`, `archive_write.rs:157-165` |

## 6. Offline tools

- **`cc-store`** (package `cc-store-tool`): `verify` (read-only, shared lock) runs the eight
  invariants (`expected_node_id: None`, no schema or digest check; `bin/cc-store/src/lib.rs:51`);
  `dump` prints value length and a 64-byte hex prefix; `compact` opens read-write. No repair.
- **`cc-store-bench`** (`bin/store-bench`): CC-40 prune-under-load falsifier; results in
  [`storage-engine.md`](../storage-engine.md).

## 7. Where the linked docs have drifted

| Doc | Says | Code at HEAD |
|---|---|---|
| [`storage-schema.md`](../storage-schema.md) | `fork_choice` table holds `"current"` scalars; 10 meta keys; names interned to `'static` | scalars in `meta.fc_scalars`; 11 keys (adds `node_id`); live-set intern pool |
| [`storage-engine.md`](../storage-engine.md) | `Box::leak` once per name; "twelve methods" | no leak; larger surface (`open_existing`, `open_read_only`, `range_max`, ...) |
| [`s2-rollback.md`](../s2-rollback.md) | beacon-core builds no `ArchiveWriter`; no ingest restamps `WriteCursor`; shape A still drives `RestoreFromStore` | built and injected (d3d7f60); block ingest writes `seq + 1`; E4 DELETED (60c6200) |
| [`serve-windows.md`](../serve-windows.md) | four consumers of the CC-4A floor | only I2 / prune and the startup check |
| [`running.md`](../running.md) | the store records the discv5 NodeId; a replaced key makes the storage service refuse, naming the derived id | raw 32 key bytes, no derived id; shape A writes no identity row, so the negative exercise starts normally |
| [`running.md`](../running.md) | restart trials resume from the durable surface; poll `cc_storage_following_head == 1` | durable set always empty; `set_following_head` is `#[cfg(test)]` |
| [`running.md`](../running.md) | `CC_P2P_PEERS__STORAGE` enables the `WatchServeWindow` / serve path | window stream only, `u64::MAX`; p2p issues no reads |

## Planned changes

Target: [`plan/architecture.md`](../../plan/architecture.md) §4, §2.3 (E5/E6), §9.1 (shape A).

- Planned (open question): either a full P0 returns `SeamError::Backpressure` to import (policy A:
  block <= 2 s, then `Backpressure`), per [ADR-R-02](../adr/ADR-R-02.md) and the seam trait doc, or
  those docs are amended to "block, no deadline" ([ADR-P4-04](../adr/ADR-P4-04.md) says block).
  "The trait documents Backpressure; the implementation blocks without a deadline."
- Planned: chain-core persists columns via `ArchiveWrite::ingest_columns`; today no host injects it.
- Not yet specified: plan §4.2 seeds from a durable set but names no post-S2 writer for
  `meta.fc_scalars` or snapshots and no migration trigger; until then `durable_set()` stays `None`.
- Planned: E5 `PutBackfillBatch` is deleted as an RPC (admission moves behind `ArchiveWrite`), and
  E6 becomes one in-process `AtomicU64` read in shape B, which today has no serve path.
- Planned: only storage-core names cc-store (today cc-beacon-inproc, cc-beacon-import,
  `bin/cc-store`, `bin/store-bench` also do); shape B already opens one handle in `bin/beacon-core`.
- Planned at S2, when `open()` is on every restart's critical path: the remaining P0-18 fix,
  multi-GB invariant scans (a table above `max_open_scan_rows` fails open with `Limit`); the
  contig-cap and intern-pool fixes landed (`crates/store/src/invariants.rs:55-62`,
  `engine/redb.rs:57-64`). `services/storage` (shape A) is deleted at the end of S2.
