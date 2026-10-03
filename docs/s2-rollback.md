# S2 rollback — operator procedure

`S2-B-13`. `[ARCH]` §9.1 — the only stage with a **data-shape** consequence.
The redb schema does not change. The writer's **input** did (at `S2-A-09`).

**This file is the written procedure. It is not the rehearsal.**
`S2-B-14` / E2.4 is a clean shutdown, a redeploy of the previous topology
against the same data directory, and a successful open — **not done here**.
No soak numbers. No restart-trial table.

---

## What this HEAD actually starts

Two **hosts** on this tree can open a schema-1 store. They are not two
writer-input worlds.

| Host | Binary | Who opens `store.redb` |
|---|---|---|
| **S2 (host process)** | `cc-beacon-core` (`bin/beacon-core`) | This process, before any subsystem (`S2-J-01`). |
| **4-container (compose)** | `cc-storage` (`services/storage` → `cc_storage_core::run`) | Compose `storage`. `chain` is a separate process. |

`docker compose up` from **this HEAD** builds those images
(`CC_GIT_SHA="$(git rev-parse --short HEAD)"`). That 4-container stack is
**not** the pre-S2 writer:

- `write_behind.rs` is deleted (`78a90e1`). `run()` has no
  `SubscribeEvents` consumer (`boot.rs`).
- `ArchiveWriter` is constructed and immediately discarded
  (`let _archive = ArchiveWriter::new(...)` in `run()`). Nothing
  injects it into chain-core.
- 4-container `chain` builds `P2pStreamDeps` with `archive: None`.
  Column ingest fail-closes:
  `"column ingest unavailable: archive handle not injected"`.

The last commit on this line that still contains
`crates/storage-core/src/write_behind.rs` is **`e854b1d`** (`78a90e1^`).
This repo does not pin a published image tag for that SHA. Do not treat
`compose up` from HEAD as restoring stream-seq write-behind.

**(a)** is about the **data directory**: a previous topology (a binary
that still has that writer) can *open the same redb*. It is not “start
today’s compose and write-behind comes back.”

---

## Physical files and compose mounts

The file is always `<data_dir>/store.redb` (`cc_store::db_file_path`).
“Same data directory” means that inode, not a matching config key.

### Host S2 defaults

| Knob | Default |
|---|---|
| Binary | `cc-beacon-core` |
| `data_dir` | `data/storage` → `data/storage/store.redb` |
| `node_key_path` | `./data/node_key` |
| Metrics | `127.0.0.1:9101` (`config/beacon-core.toml`) |

### Compose mounts **as written** in `docker-compose.yml`

These are **named volumes**. They are not host `./data/storage` or
`./data/node_key`.

| Service | Env | Container path | Volume |
|---|---|---|---|
| `storage` | `CC_STORAGE_DATA_DIR=/app/data` | `/app/data` (`store.redb` here) | **`cc-store-data`** |
| `storage` | `CC_STORAGE_NODE_KEY_PATH=/identity/node_key` | `/identity/node_key` | **`cc-p2p-identity`** (`:ro`) |
| `p2p` | `CC_P2P_NODE_KEY_PATH=/app/data/node_key` | `/app/data/node_key` | **`cc-p2p-identity`** (rw) |
| `storage` metrics | `CC_STORAGE_METRICS_ADDR=0.0.0.0:9106` | published `127.0.0.1:9106:9106` | — |

`cc-store-data` and `cc-p2p-identity` are **one backup unit**
(`docs/running.md`, ADR-P4-13). A store beside a different `node_key`
refuses `I-node-id`.

`docker compose up -d` from this HEAD therefore opens
`cc-store-data`/`store.redb` and `cc-p2p-identity`/`node_key`. That is
a **different** pair from host `data/storage/store.redb` +
`./data/node_key` unless those host paths **are** the volume mounts
below. `wait-healthy.sh` can go green on an empty named volume. That
is not a successful open of the live store.

A second compose file (`-f a -f b`) **appends** `volumes:` lists. Do
not append a bind-mount on top of the named volume and hope the same
file wins. Each container dest (`/app/data`, `/identity`) must appear
**once**.

---

## (a) On-disk format is unchanged

A binary of either host — and a pre-`78a90e1` storage binary — can
`Store::open` the same schema-1 directory. Open refuses; it does not
convert.

| Surface | Live value | Rollback effect |
|---|---|---|
| `SchemaVersion` | `SCHEMA_VERSION = 1` (`crates/store/src/schema.rs`) | Same integer. Mismatch: `schema version mismatch: found …, expected …`. |
| Config digest | SHA-256 of the named field list | Same check. Use the same network config. |
| Table inventory / key codecs | `docs/storage-schema.md` | Unchanged. |
| `WriteCursor` SSZ | `session_id: u64`, `seq: u64`, `slot`, `root` (`crates/store/src/meta.rs`) | Same container, same meta key `write_cursor`. |
| Cursor+data atomicity | `WriteCursor` is put in the **same** redb `Batch` as the rows it describes (P0 commit) | Unchanged **when a P0 unit commits**. |

---

## (b) Clean shutdown first

Required because of the exclusive redb lock and an undrained writer
mailbox — **not** because this HEAD’s compose will resume a stream
cursor.

### Live `WriteCursor` (composed processes)

`write_behind.rs` is gone. Stream-seq is **already gone as the writer's
input** on **both** hosts this HEAD starts.

What the **types** do (`archive_write.rs`):

- `ArchiveWriter::submit_writer_batch` loads `KEY_WRITE_CURSOR` and
  restamps that value in the same `CommitUnit` as the rows. It refuses
  a missing cursor (does not invent `session_id=0` / `seq=0`). Tests
  assert `seq` unchanged.

What the **composed processes** do:

| Process | Cursor write |
|---|---|
| `cc-beacon-core` | Starts the P0 writer (`start_writer`). Does **not** construct `ArchiveWriter`. Does **not** call `P2pStreamDeps::with_archive`. |
| compose `storage` | Spawns the writer. Builds `ArchiveWriter` and **drops** it. No `SubscribeEvents` consumer. |
| compose `chain` | `archive: None`. Columns fail-close. |

> **Dated note (2026-10-04).** Struck: the `cc-beacon-core` cell
> "Does **not** construct `ArchiveWriter`." False since `d3d7f60`.
> Confirmed on the current boot path: `bin/beacon-core/src/boot.rs`
> `run` calls `cc_storage_core::start_writer`, and
> `start_writer_on_engine` builds `ArchiveWriter::new`
> (`crates/storage-core/src/open.rs`). `run` then installs
> `storage.archive()` as `CoreConfig.archive`. The other clause of
> that cell still stands: this host does **not** call
> `P2pStreamDeps::with_archive`. The sentence below that a leftover
> cursor stays frozen unless some other P0 submitter writes it assumed
> beacon-core was not such a submitter; a block ingest through the
> injected handle can restamp `WriteCursor`. Superseded by that boot
> path (`d3d7f60`). The table above is the original text.

So leftover `write_cursor` on disk is **frozen** unless some other P0
submitter writes it. Production replay / prune / migrate in this tree
do not call `ArchiveWrite`. Leftover `CommitUnit` comments still say
“seq of the last event included” — write-behind-era; not what these
hosts run.

Missing `WriteCursor` is **not** an open failure. Durable-set item 5
degrades (`session_id=0` or absent). The assessment text still says
“resubscribe live from tip”; this HEAD has no write-behind consumer
that will.

A **pre-`e854b1d` / `e854b1d` storage binary** would treat
`WriteCursor` as a stream resume cursor again. That is a different
artifact than `compose up` from HEAD. Name that SHA (or its image) if
you deploy it. This file does not.

### Why the mailbox still matters

`run_writer` is biased `shutdown.changed()` → `break` with **no**
mailbox drain. An in-flight P0 that has not committed is dropped. Idle
the writer, then signal.

### Signal path — compose `storage`

`docker compose stop storage` (or `stop` of the stack):

1. Compose sends **`SIGTERM`** (`x-cc-runtime.stop_signal`).
2. `cc-bootstrap` `SignalTrigger::UnixSignals` → `begin_shutdown`.
3. Aggregate health goes `NOT_SERVING`.
4. `on_pre_drain` is `pre_drain_fire_shutdown` →
   `shutdown_tx.send(true)`.
5. Writer watch fires → `break` (mailbox not drained).
6. If the process is still running after **`stop_grace_period: 5s`**,
   compose **SIGKILL**s it.

That 5 s grace is a **compose** property.

### Signal path — host `cc-beacon-core`

`kill -TERM` on the host pid:

1. Same bootstrap `UnixSignals` → `begin_shutdown`.
2. `on_pre_drain` only `core.shutdown_and_join()`. It does **not**
   call `StorageRuntime::shutdown()`.
3. The writer watch is **not** fired by that hook. The writer stops
   when the process tears down (`StorageRuntime` / engine drop after
   `serve` returns).
4. There is **no** compose 5 s grace on the host unless the operator
   supplies one.

Idle-then-TERM is still the rule. Do **not** `kill -9` mid-P0. Do
**not** use `docker compose kill` as the rollback path (that is the
Phase 4 crash-recovery clause in `docs/running.md`).

### Idle the live opener

Scrape **that opener’s** metrics port. Do not scrape the other
topology.

```bash
# Compose storage (published 127.0.0.1:9106):
curl -s localhost:9106/metrics | grep cc_storage_writer_queue_depth

# Host cc-beacon-core (default 127.0.0.1:9101):
curl -s localhost:9101/metrics | grep cc_storage_writer_queue_depth

# expect class="p0" at 0 and staying 0
```

If metrics are down: stop ingest (`docker compose stop p2p`, or stop
feeding `ImportBlock`) and wait until the pid is idle. Do **not** wait
on `commit_max_latency_ms`. That key is still loaded so old toml
parses; `S2-A-09` deleted the SubscribeEvents consumer it configured.
It is not a live flush timer.

Then TERM and wait for exit. No `down -v`.

```bash
# Host S2:
kill -TERM "$BEACON_CORE_PID"
# wait until the pid is gone

# Compose opener:
docker compose stop storage
# confirm exited:
docker compose ps -a storage
```

Forbidden on this path:

```bash
docker compose kill -s SIGKILL storage     # crash-recovery clause, not rollback
docker compose down -v                     # deletes cc-store-data + cc-p2p-identity
```

`docker compose down` *without* `-v` keeps named volumes. Prefer
`stop` so the service definitions remain.

Confirm the lock is free. A second opener of a live store fails:

```text
store database locked: another process holds the file lock
```

(`StoreError::DatabaseLocked` ← redb `DatabaseAlreadyOpen`).

---

## (c) No migration in either direction

Do not rewrite `write_cursor`. Do not bump `schema_version`. Do not
run a schema migrator. Do not copy rows into a new directory and call
that the same store.

| Direction | Action |
|---|---|
| Host S2 → 4-container | Clean shutdown. Point compose `storage` at the **same** `store.redb` and `node_key` (mounts below). |
| 4-container → host S2 | Clean shutdown. Point `cc-beacon-core` at those **same** files. |
| Either host → pre-`78a90e1` storage | Same files. Deploy **that** binary (`e854b1d` still has `write_behind.rs`). Not `compose up` from HEAD. |

Both directions are `Store::open` on schema **1**. Open refuses; it
does not convert.

---

## Redeploy — same files

### Host S2 store → compose `storage` (same inodes)

Default named volumes will **not** see `data/storage` or
`./data/node_key`. Replace the two services’ `volumes:` so each dest
appears once:

```yaml
# storage — replace the named-volume pair, do not append
volumes:
  - ./data/storage:/app/data
  - ./data:/identity:ro

# p2p — same identity directory p2p already uses for node_key
volumes:
  - ./data:/app/data
```

Keep the env already in `docker-compose.yml`:

- `CC_STORAGE_DATA_DIR=/app/data`
- `CC_STORAGE_NODE_KEY_PATH=/identity/node_key`
- `CC_P2P_NODE_KEY_PATH=/app/data/node_key`

Then:

```bash
export CC_GIT_SHA="$(git rev-parse --short HEAD)"
docker compose up -d
bash scripts/wait-healthy.sh
```

`wait-healthy.sh` expects the six compose services. After it returns,
confirm `storage` opened **this** file, not a new empty volume (logs
must not show the refuse strings below; `data_dir` in the ready line
should be `/app/data`).

This starts **this HEAD’s** 4-container host (no write-behind). It is
a host swap onto the same redb.

### Compose named volumes → host `cc-beacon-core` (same inodes)

```bash
docker compose config --volumes
# expect: cc-store-data, cc-p2p-identity (and elstore when el is present)
docker volume inspect "$(docker compose ls -q | head -1)_cc-store-data"
```

On Linux the volume `Mountpoint` is a host path you can pass as
`CC_BEACON_CORE_DATA_DIR` (and the identity volume as
`CC_BEACON_CORE_NODE_KEY_PATH=…/node_key`).

On Docker Desktop (macOS) that `Mountpoint` is **inside the VM**. Host
`cc-beacon-core` cannot open it as `./data/storage`. Same-file means
run the binary in a container that mounts the **named volumes**:

```text
cc-store-data      →  data_dir          (store.redb)
cc-p2p-identity    →  node_key_path     (/…/node_key)
```

Copying the volume out to `./data/storage` is a **copy**, not the same
files. Do not record that as E2.4.

`cc-beacon-core` binds `127.0.0.1:9001` by default. Leftover compose
`p2p` / `beacon-api` still dial `chain:9001` and `storage:9006` on the
`cc` network. That mesh is **not** this process. Stop or re-point those
clients; this procedure only covers `Store::open`.

### Pre-A-09 writer (stream-seq), if you have that artifact

Deploy the image / tree that still contains `write_behind.rs` (last
commit here: `e854b1d`). Point it at the same `store.redb` +
`node_key` as above. Clean shutdown first. This HEAD does not build
that writer.

---

## Successful open

Success is a completed `Store::open` of the **intended** file, not
“the container is running.”

| Gate | Failure `Display` (do not treat as success) |
|---|---|
| Exclusive lock | `store database locked: another process holds the file lock` |
| Schema | `schema version mismatch: found N, expected 1` |
| Config digest | `config digest mismatch: found …, expected …` |
| `I-node-id` | `store invariant node_id violated: …` |
| `I-cursor` | `store invariant cursor violated: …` (`WriteCursor.slot` past newest block, or slot > 0 with no blocks) |
| Missing meta on a non-empty store | `missing required meta record …` |

Compose: `storage` healthcheck is `grpc-health-probe` on `:9006`.
Host `cc-beacon-core`: `open` is the first boot phase; no subsystem
starts if it fails.

---

## R-10 — do not invent a re-sync budget

This HEAD’s 4-container `storage` still drives **RestoreFromStore**
into `chain` (`resume.rs`; `S2-J-02` has not deleted it). A chain
restart on that topology can still fall through to checkpoint-sync.
Budget that into `S2-B-14`. `S3b-W-05` stays after S2.

This is **not** “write-behind comes back and holes the archive.”
Write-behind is gone on this HEAD.

This file does not record a re-sync duration. None was measured here.

---

## What this file does not claim

| Claim | Status |
|---|---|
| `S2-B-14` rehearsal ran | **no** |
| E2.4 discharged | **no** — earned by `S2-B-14` |
| Soak / restart-trial / re-sync numbers | **none** |
| `compose up` from this HEAD reintroduces write-behind | **no** |
| Stream-seq still feeds either host on this HEAD | **no** — deleted at `S2-A-09` |
| Composed ingest restamps `WriteCursor` | **no** — `ArchiveWriter` restamp is unused by these hosts |
| This HEAD writes batch-seq into `WriteCursor.seq` | **no** |
| `commit_max_latency_ms` is a live flush bound | **no** — leftover toml key |
| Clean shutdown optional | **no** — lock + undrained mailbox |

> **Dated note (2026-10-04).** Struck, for `cc-beacon-core` block
> ingest: the parenthetical on "Composed ingest restamps
> `WriteCursor`" that "`ArchiveWriter` restamp is unused by these
> hosts." False for that host since `d3d7f60`. `run` calls
> `start_writer`, `start_writer_on_engine` builds `ArchiveWriter::new`,
> and `block_unit` commits a new `WriteCursor`
> (`bin/beacon-core/src/boot.rs`; `crates/storage-core/src/open.rs`;
> `crates/storage-core/src/archive_write.rs`). The row above is the
> original wording.

---

## Checklist (operator)

1. Name the live opener (`cc-beacon-core` or compose `storage`) and
   the real `<data_dir>/store.redb` + `node_key`.
2. Stop ingest. Scrape **that** process’s metrics port
   (`:9101` host / `:9106` compose). `cc_storage_writer_queue_depth{class="p0"}` = 0.
3. SIGTERM on **that** opener. Compose: `docker compose stop storage`
   (watch-fired). Host: `kill -TERM` (core pre-drain only; writer dies
   with the process). No `kill -9`. No `down -v`.
4. Confirm no leftover pid / no `DatabaseLocked`.
5. Start the other **host** with volumes that resolve to **those**
   files (table above). Do not `up -d` onto empty named volumes and
   call it the same store.
6. `Store::open` succeeds (table above). Compose: `wait-healthy.sh`,
   then confirm it was this `store.redb`.
7. Record the drill under `S2-B-14`, not here.
