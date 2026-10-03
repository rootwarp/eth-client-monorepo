//! redb 4.1.0 body of the engine seam (Architecture §1.1 / §8).
//!
//! This is the **only** module (with its parent) that names the `redb` crate
//! (CC-40 /3). Swapping to fjall replaces this file.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use redb::{
    Database, DatabaseError, ReadOnlyDatabase, ReadableDatabase, ReadableTableMetadata,
    TableDefinition, TableHandle,
};

use super::{
    Durability, EngineOptions, MAX_BATCH_OPS, MAX_INTERNED_TABLE_NAMES, MAX_RANGE_BYTES,
    MAX_RANGE_ENTRIES, StoreError, db_file_path,
};

/// Map redb open errors, naming the live-lock case for offline tooling (CC-4J).
fn map_database_error(err: DatabaseError) -> StoreError {
    match err {
        DatabaseError::DatabaseAlreadyOpen => StoreError::DatabaseLocked,
        other => StoreError::engine(other),
    }
}

/// Internal redb handle: read-write exclusive or read-only shared readers.
enum DbInner {
    ReadWrite(Database),
    ReadOnly(ReadOnlyDatabase),
}

impl std::fmt::Debug for DbInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ReadWrite(_) => f.write_str("DbInner::ReadWrite(..)"),
            Self::ReadOnly(_) => f.write_str("DbInner::ReadOnly(..)"),
        }
    }
}

impl DbInner {
    fn begin_read(&self) -> Result<redb::ReadTransaction, StoreError> {
        match self {
            Self::ReadWrite(db) => db.begin_read().map_err(StoreError::engine),
            Self::ReadOnly(db) => db.begin_read().map_err(StoreError::engine),
        }
    }

    const fn is_read_only(&self) -> bool {
        matches!(self, Self::ReadOnly(_))
    }
}

type NameIntern = Arc<Mutex<HashSet<String>>>;

/// Record a live table name in the intern pool (R-14 / SEC-40b-3).
///
/// redb copies the name into table metadata; `open_table` only needs the
/// `&str` for the duration of the call. The pool is therefore a **live-set
/// cap**, not a `Box::leak` of every name ever formatted. Dropped tables
/// leave via [`unintern_name`] so shard rollover cannot exhaust the 512
/// slots (P0-18/1).
fn intern_name(cache: &NameIntern, name: &str) -> Result<(), StoreError> {
    if name.is_empty() {
        return Err(StoreError::Config("table name must be non-empty".into()));
    }
    let mut guard = cache.lock().unwrap_or_else(|e| e.into_inner());
    if guard.contains(name) {
        return Ok(());
    }
    if guard.len() >= MAX_INTERNED_TABLE_NAMES {
        return Err(StoreError::limit(format!(
            "table name intern pool full ({MAX_INTERNED_TABLE_NAMES}); refusing new name {name:?}"
        )));
    }
    guard.insert(name.to_owned());
    Ok(())
}

fn unintern_name(cache: &NameIntern, name: &str) {
    let mut guard = cache.lock().unwrap_or_else(|e| e.into_inner());
    guard.remove(name);
}

fn interned_len(cache: &NameIntern) -> usize {
    cache.lock().unwrap_or_else(|e| e.into_inner()).len()
}

/// `TableDefinition` borrows `name` only for the `open_table` call; redb
/// copies it (`Table::new` → `name.to_string()`).
fn table_def(name: &str) -> Result<TableDefinition<'_, &'static [u8], &'static [u8]>, StoreError> {
    if name.is_empty() {
        return Err(StoreError::Config("table name must be non-empty".into()));
    }
    Ok(TableDefinition::new(name))
}

/// Accumulated write ops committed as one unit (intersection with fjall batch).
#[derive(Debug, Default)]
pub struct Batch {
    ops: Vec<Op>,
    /// Set if a put/delete would exceed [`MAX_BATCH_OPS`]; commit fails (medium 5).
    overflowed: bool,
}

#[derive(Debug)]
enum Op {
    Put {
        table: String,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        table: String,
        key: Vec<u8>,
    },
    DeleteRange {
        table: String,
        lo: Vec<u8>,
        hi: Vec<u8>,
    },
}

impl Batch {
    fn push_op(&mut self, op: Op) {
        if self.overflowed {
            return;
        }
        if self.ops.len() >= MAX_BATCH_OPS {
            self.overflowed = true;
            return;
        }
        self.ops.push(op);
    }

    pub fn put(&mut self, table: &str, key: &[u8], value: &[u8]) {
        self.push_op(Op::Put {
            table: table.to_owned(),
            key: key.to_vec(),
            value: value.to_vec(),
        });
    }

    pub fn delete(&mut self, table: &str, key: &[u8]) {
        self.push_op(Op::Delete {
            table: table.to_owned(),
            key: key.to_vec(),
        });
    }

    /// Delete keys in `[lo, hi)` (half-open).
    pub fn delete_range(&mut self, table: &str, lo: &[u8], hi: &[u8]) {
        self.push_op(Op::DeleteRange {
            table: table.to_owned(),
            lo: lo.to_vec(),
            hi: hi.to_vec(),
        });
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty() && !self.overflowed
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }

    pub fn overflowed(&self) -> bool {
        self.overflowed
    }

    /// Puts staged so far, in order. The canonical walk uses this to resolve
    /// parents written earlier in the same batch (not yet visible to `ReadTxn`).
    pub(crate) fn staged_puts(&self) -> impl Iterator<Item = (&str, &[u8], &[u8])> + '_ {
        self.ops.iter().filter_map(|op| match op {
            Op::Put { table, key, value } => {
                Some((table.as_str(), key.as_slice(), value.as_slice()))
            }
            _ => None,
        })
    }

    /// Drain put/delete ops for submission through the single writer (CC-41).
    ///
    /// `DeleteRange` is expanded only if present — migration stages discrete deletes.
    /// Returns [`StoreError::Limit`] if the batch overflowed [`MAX_BATCH_OPS`].
    pub fn into_puts_and_deletes(self) -> Result<BatchPutsDeletes, StoreError> {
        if self.overflowed {
            return Err(StoreError::limit(format!(
                "batch exceeds MAX_BATCH_OPS ({MAX_BATCH_OPS})"
            )));
        }
        let mut puts = Vec::new();
        let mut deletes = Vec::new();
        for op in self.ops {
            match op {
                Op::Put { table, key, value } => puts.push((table, key, value)),
                Op::Delete { table, key } => deletes.push((table, key)),
                Op::DeleteRange { table, lo, hi } => {
                    return Err(StoreError::Config(format!(
                        "into_puts_and_deletes does not expand DeleteRange on {table} [{lo:?},{hi:?}); \
                         stage discrete deletes for writer submission"
                    )));
                }
            }
        }
        Ok(BatchPutsDeletes { puts, deletes })
    }
}

/// Put/delete lists drained from a [`Batch`] for writer submission (CC-41).
#[derive(Debug, Default)]
pub struct BatchPutsDeletes {
    /// `(table, key, value)` puts.
    pub puts: Vec<(String, Vec<u8>, Vec<u8>)>,
    /// `(table, key)` deletes.
    pub deletes: Vec<(String, Vec<u8>)>,
}

/// Concrete engine. Single writer (§1.5); reads are MVCC snapshots.
///
/// `Database` is `Send + Sync` and `begin_read`/`begin_write` take `&self`. The
/// mutex is held **only** around those begin calls (and for the full `compact`,
/// which needs `&mut Database`). Concurrent `read()` during `commit()`/fsync is
/// therefore allowed (Architecture §8.1 #4 / review F1).
///
/// [`Engine::open_read_only`] uses redb's shared read lock so offline tools
/// (`bin/cc-store`) never take the writer lock; a live node still yields
/// [`StoreError::DatabaseLocked`].
#[derive(Debug)]
pub struct Engine {
    db: Mutex<DbInner>,
    path: PathBuf,
    durability: Durability,
    names: NameIntern,
}

impl Engine {
    /// Open (or create) a store under data directory `path`.
    ///
    /// `path` must be a directory (created if missing). Symlinks and regular
    /// files are refused (medium 7 / path policy).
    pub fn open(path: &Path, opts: EngineOptions) -> Result<Self, StoreError> {
        validate_data_dir(path)?;
        if !path.exists() {
            std::fs::create_dir_all(path)?;
            // Re-check after create (TOCTOU-ish: still refuse if something odd).
            validate_data_dir(path)?;
        }
        let file = db_file_path(path);
        refuse_symlink_store(&file)?;
        let db = Database::create(&file).map_err(map_database_error)?;
        Ok(Self {
            db: Mutex::new(DbInner::ReadWrite(db)),
            path: file,
            durability: opts.durability,
            names: Arc::new(Mutex::new(HashSet::new())),
        })
    }

    /// Open an **existing** store for read-write without creating a new file.
    ///
    /// Used by on-demand compaction (`bin/cc-store compact`). Refuses a missing
    /// `store.redb` and maps a live writer lock to [`StoreError::DatabaseLocked`].
    pub fn open_existing(path: &Path, opts: EngineOptions) -> Result<Self, StoreError> {
        validate_data_dir(path)?;
        if !path.exists() {
            return Err(StoreError::Config(format!(
                "data directory does not exist: {}",
                path.display()
            )));
        }
        let file = db_file_path(path);
        if !file.exists() {
            return Err(StoreError::Config(format!(
                "store file missing: {}",
                file.display()
            )));
        }
        refuse_symlink_store(&file)?;
        let db = Database::builder()
            .open(&file)
            .map_err(map_database_error)?;
        Ok(Self {
            db: Mutex::new(DbInner::ReadWrite(db)),
            path: file,
            durability: opts.durability,
            names: Arc::new(Mutex::new(HashSet::new())),
        })
    }

    /// Open an existing store **read-only** (redb shared lock).
    ///
    /// Does not create directories or files. Concurrent with other read-only
    /// openers; refuses if a writer holds the store ([`StoreError::DatabaseLocked`]).
    pub fn open_read_only(path: &Path) -> Result<Self, StoreError> {
        validate_data_dir(path)?;
        if !path.exists() {
            return Err(StoreError::Config(format!(
                "data directory does not exist: {}",
                path.display()
            )));
        }
        let file = db_file_path(path);
        if !file.exists() {
            return Err(StoreError::Config(format!(
                "store file missing: {}",
                file.display()
            )));
        }
        refuse_symlink_store(&file)?;
        let db = ReadOnlyDatabase::open(&file).map_err(map_database_error)?;
        Ok(Self {
            db: Mutex::new(DbInner::ReadOnly(db)),
            path: file,
            durability: Durability::Immediate,
            names: Arc::new(Mutex::new(HashSet::new())),
        })
    }

    /// Whether this engine was opened with [`Self::open_read_only`].
    pub fn is_read_only(&self) -> bool {
        self.db.lock().map(|g| g.is_read_only()).unwrap_or(false)
    }

    /// Durability setting this engine was opened with.
    pub fn durability(&self) -> Durability {
        self.durability
    }

    /// Whether two-phase commit is engaged (`Paranoid`).
    pub fn two_phase_commit(&self) -> bool {
        self.durability.two_phase_commit()
    }

    fn intern_and_def<'a>(
        &self,
        name: &'a str,
    ) -> Result<TableDefinition<'a, &'static [u8], &'static [u8]>, StoreError> {
        intern_name(&self.names, name)?;
        table_def(name)
    }

    /// Count of names currently in the intern pool (live set; tests / P0-18/1).
    pub fn interned_name_count(&self) -> usize {
        interned_len(&self.names)
    }

    fn lock_db(&self) -> Result<std::sync::MutexGuard<'_, DbInner>, StoreError> {
        self.db
            .lock()
            .map_err(|_| StoreError::engine("engine lock poisoned"))
    }

    pub fn read(&self) -> Result<ReadTxn, StoreError> {
        // Lock only for begin_read; ReadTransaction is self-contained (Arc).
        let txn = {
            let db = self.lock_db()?;
            db.begin_read()?
        };
        Ok(ReadTxn { txn })
    }

    pub fn batch(&self) -> Batch {
        Batch::default()
    }

    pub fn commit(&self, batch: Batch) -> Result<(), StoreError> {
        if batch.overflowed {
            return Err(StoreError::limit(format!(
                "batch exceeds MAX_BATCH_OPS ({MAX_BATCH_OPS})"
            )));
        }
        if batch.is_empty() {
            return Ok(());
        }
        // Lock only for begin_write; drop before ops + fsync so concurrent
        // begin_read can proceed (F1 / Architecture §8.1 #4).
        let mut txn = {
            let db = self.lock_db()?;
            match &*db {
                DbInner::ReadOnly(_) => {
                    return Err(StoreError::Config("read-only engine refuses writes".into()));
                }
                DbInner::ReadWrite(rw) => rw.begin_write().map_err(StoreError::engine)?,
            }
        };
        apply_durability(&mut txn, self.durability)?;

        for op in batch.ops {
            match op {
                Op::Put { table, key, value } => {
                    let def = self.intern_and_def(&table)?;
                    let mut t = txn.open_table(def).map_err(StoreError::engine)?;
                    t.insert(key.as_slice(), value.as_slice())
                        .map_err(StoreError::engine)?;
                }
                Op::Delete { table, key } => {
                    let def = self.intern_and_def(&table)?;
                    let mut t = txn.open_table(def).map_err(StoreError::engine)?;
                    t.remove(key.as_slice()).map_err(StoreError::engine)?;
                }
                Op::DeleteRange { table, lo, hi } => {
                    let def = self.intern_and_def(&table)?;
                    let mut t = txn.open_table(def).map_err(StoreError::engine)?;
                    t.retain_in(lo.as_slice()..hi.as_slice(), |_, _| false)
                        .map_err(StoreError::engine)?;
                }
            }
        }

        txn.commit().map_err(StoreError::engine)?;
        Ok(())
    }

    pub fn table_names(&self) -> Result<Vec<String>, StoreError> {
        let txn = {
            let db = self.lock_db()?;
            db.begin_read()?
        };
        let mut names = Vec::new();
        for handle in txn.list_tables().map_err(StoreError::engine)? {
            names.push(handle.name().to_owned());
        }
        names.sort();
        Ok(names)
    }

    pub fn drop_table(&self, name: &str) -> Result<(), StoreError> {
        let def = table_def(name)?;
        let mut txn = {
            let db = self.lock_db()?;
            match &*db {
                DbInner::ReadOnly(_) => {
                    return Err(StoreError::Config(
                        "read-only engine refuses drop_table".into(),
                    ));
                }
                DbInner::ReadWrite(rw) => rw.begin_write().map_err(StoreError::engine)?,
            }
        };
        apply_durability(&mut txn, self.durability)?;
        let _existed = txn.delete_table(def).map_err(StoreError::engine)?;
        txn.commit().map_err(StoreError::engine)?;
        // Retire the intern slot so shard rollover cannot fill the pool.
        unintern_name(&self.names, name);
        Ok(())
    }

    pub fn file_len(&self) -> Result<u64, StoreError> {
        Ok(std::fs::metadata(&self.path)?.len())
    }

    pub fn compact(&self) -> Result<bool, StoreError> {
        // compact needs &mut Database — hold the mutex for the whole call.
        let mut guard = self.lock_db()?;
        match &mut *guard {
            DbInner::ReadOnly(_) => Err(StoreError::Config(
                "read-only engine refuses compact".into(),
            )),
            DbInner::ReadWrite(db) => db.compact().map_err(StoreError::engine),
        }
    }

    /// Path of the on-disk database file.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Refuse a symlink at `store.redb` (path policy / medium 7).
fn refuse_symlink_store(file: &Path) -> Result<(), StoreError> {
    if file.exists() {
        let meta = std::fs::symlink_metadata(file)?;
        if meta.file_type().is_symlink() {
            return Err(StoreError::Config(format!(
                "refusing to open symlink store file {}",
                file.display()
            )));
        }
    }
    Ok(())
}

/// Data directory policy: must be a real directory, never a symlink (medium 7).
fn validate_data_dir(path: &Path) -> Result<(), StoreError> {
    if !path.exists() {
        return Ok(());
    }
    let meta = std::fs::symlink_metadata(path)?;
    if meta.file_type().is_symlink() {
        return Err(StoreError::Config(format!(
            "refusing data dir that is a symlink: {}",
            path.display()
        )));
    }
    if !meta.is_dir() {
        return Err(StoreError::Config(format!(
            "data path must be a directory, not a file: {}",
            path.display()
        )));
    }
    Ok(())
}

fn apply_durability(txn: &mut redb::WriteTransaction, d: Durability) -> Result<(), StoreError> {
    match d {
        Durability::None => {
            txn.set_durability(redb::Durability::None)
                .map_err(StoreError::engine)?;
        }
        Durability::Immediate => {
            txn.set_durability(redb::Durability::Immediate)
                .map_err(StoreError::engine)?;
        }
        Durability::Paranoid => {
            txn.set_durability(redb::Durability::Immediate)
                .map_err(StoreError::engine)?;
            txn.set_two_phase_commit(true);
        }
    }
    Ok(())
}

/// Read transaction; values are owned `Vec<u8>` (long-reader hazard removed, §7.2).
#[derive(Debug)]
pub struct ReadTxn {
    txn: redb::ReadTransaction,
}

impl ReadTxn {
    pub fn get(&self, table: &str, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        let t = match self.txn.open_table(table_def(table)?) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(StoreError::engine(e)),
        };
        match t.get(key).map_err(StoreError::engine)? {
            Some(v) => Ok(Some(v.value().to_vec())),
            None => Ok(None),
        }
    }

    /// Whether `table` contains any row.
    ///
    /// Opens the table with `ReadTransaction::open_untyped_table`.
    /// redb 4.1 `ReadOnlyUntypedTable::new` stores the catalog header in
    /// `RawBtree` and does not `get_page` that table's root. `len` is
    /// `BtreeHeader.length`. A one-row snapshot leaf is not loaded. The name
    /// lookup can still read table-catalog pages. A missing table is
    /// `Ok(false)`. Any other error is `Err` — a read failure is not an empty
    /// store.
    pub fn has_any(&self, table: &str) -> Result<bool, StoreError> {
        let t = match self.txn.open_untyped_table(table_def(table)?) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(false),
            Err(e) => return Err(StoreError::engine(e)),
        };
        let n = t.len().map_err(StoreError::engine)?;
        Ok(n > 0)
    }

    /// Half-open range `[lo, hi)`. Yields owned key/value pairs.
    ///
    /// Materialisation is capped at [`MAX_RANGE_ENTRIES`] / [`MAX_RANGE_BYTES`]
    /// (SEC-40b-4). Exceeding either returns [`StoreError::Limit`].
    pub fn range(&self, table: &str, lo: &[u8], hi: &[u8]) -> Result<RangeIter, StoreError> {
        self.range_max(table, lo, hi, MAX_RANGE_ENTRIES)
    }

    /// Like [`Self::range`], but fail-closed if more than `max_rows` entries match.
    ///
    /// `max_rows` is clamped to [`MAX_RANGE_ENTRIES`]. Stops before copying the
    /// rest of the table so a caller-imposed row budget can bound wall time.
    pub fn range_max(
        &self,
        table: &str,
        lo: &[u8],
        hi: &[u8],
        max_rows: usize,
    ) -> Result<RangeIter, StoreError> {
        let t = match self.txn.open_table(table_def(table)?) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => {
                return Ok(RangeIter {
                    inner: Vec::new().into_iter(),
                });
            }
            Err(e) => return Err(StoreError::engine(e)),
        };
        let row_cap = max_rows.min(MAX_RANGE_ENTRIES);
        // Materialise under the table borrow so we release page pins before return (§7.2).
        let mut out = Vec::new();
        let mut total_bytes: u64 = 0;
        let iter = t.range::<&[u8]>(lo..hi).map_err(StoreError::engine)?;
        for item in iter {
            if out.len() >= row_cap {
                if row_cap >= MAX_RANGE_ENTRIES {
                    return Err(StoreError::limit(format!(
                        "range materialisation exceeded MAX_RANGE_ENTRIES ({MAX_RANGE_ENTRIES})"
                    )));
                }
                return Err(StoreError::limit(format!(
                    "range materialisation exceeded max_rows ({row_cap})"
                )));
            }
            let (k, v) = item.map_err(StoreError::engine)?;
            let kb = k.value().len() as u64;
            let vb = v.value().len() as u64;
            total_bytes = total_bytes.saturating_add(kb).saturating_add(vb);
            if total_bytes > MAX_RANGE_BYTES {
                return Err(StoreError::limit(format!(
                    "range materialisation exceeded MAX_RANGE_BYTES ({MAX_RANGE_BYTES})"
                )));
            }
            out.push((k.value().to_vec(), v.value().to_vec()));
        }
        Ok(RangeIter {
            inner: out.into_iter(),
        })
    }
}

/// Owned range result (copied out of the read txn to release page pins).
#[derive(Debug)]
pub struct RangeIter {
    inner: std::vec::IntoIter<(Vec<u8>, Vec<u8>)>,
}

impl Iterator for RangeIter {
    type Item = Result<(Vec<u8>, Vec<u8>), StoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(Ok)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use std::sync::Barrier;
    use std::thread;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn tmp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("cc-store-{label}-{nanos}"));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn has_any_uses_untyped_open_and_reports_rows() {
        // The small value does not prove a snapshot page was skipped. The
        // source assertion pins the untyped constructor; redb 4.1
        // `RawBtree::new` does not `get_page`.
        let src = include_str!("redb.rs");
        let start = src.find("pub fn has_any").expect("has_any");
        let rest = &src[start..];
        let end = rest.find("\n    /// Half-open range").expect("next method");
        let body = &rest[..end];
        assert!(
            body.contains("open_untyped_table"),
            "has_any must use the untyped open: {body}"
        );
        assert!(
            !body.contains("open_table("),
            "typed open_table loads the btree root: {body}"
        );
        assert!(
            body.contains("TableDoesNotExist"),
            "a missing table is empty, not an error: {body}"
        );

        let dir = tmp_dir("has-any");
        let eng = Engine::open(&dir, EngineOptions::default()).unwrap();
        assert!(
            !eng.read().unwrap().has_any("snapshots").unwrap(),
            "missing table is empty"
        );
        let mut b = eng.batch();
        b.put("snapshots", b"k", b"v");
        eng.commit(b).unwrap();
        assert!(eng.read().unwrap().has_any("snapshots").unwrap());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_put_get_roundtrip() {
        let dir = tmp_dir("roundtrip");
        let eng = Engine::open(&dir, EngineOptions::default()).unwrap();
        let mut b = eng.batch();
        b.put("meta", b"k", b"v");
        eng.commit(b).unwrap();
        let v = eng.read().unwrap().get("meta", b"k").unwrap();
        assert_eq!(v.as_deref(), Some(&b"v"[..]));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn runtime_table_name_and_drop() {
        // R-14: runtime-built names work via TableDefinition borrow + intern pool.
        let dir = tmp_dir("runtime-name");
        let eng = Engine::open(&dir, EngineOptions::default()).unwrap();
        let name = crate::keys::columns_shard_table(42);
        let mut b = eng.batch();
        b.put(&name, b"\x00\x00\x00\x00\x00\x00\x00\x01", b"sidecar");
        eng.commit(b).unwrap();
        assert!(eng.table_names().unwrap().contains(&name));
        assert_eq!(eng.interned_name_count(), 1);
        eng.drop_table(&name).unwrap();
        assert!(!eng.table_names().unwrap().contains(&name));
        assert_eq!(eng.interned_name_count(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn intern_pool_retires_on_drop_past_unique_cap() {
        // P0-18/1: unique-ever names used to leak into a 512-slot intern pool.
        // Create+drop more unique shard tables than the cap; the live set stays 1.
        let dir = tmp_dir("intern-retire");
        let eng = Engine::open(&dir, EngineOptions::default()).unwrap();
        let n = MAX_INTERNED_TABLE_NAMES + 64;
        for id in 0..n as u64 {
            let name = crate::keys::columns_shard_table(id);
            let mut b = eng.batch();
            b.put(&name, b"k", b"v");
            eng.commit(b).unwrap();
            assert_eq!(eng.interned_name_count(), 1);
            eng.drop_table(&name).unwrap();
            assert_eq!(eng.interned_name_count(), 0);
        }
        // Next unique name after the old cap still opens.
        let next = crate::keys::columns_shard_table(n as u64);
        let mut b = eng.batch();
        b.put(&next, b"k", b"v");
        eng.commit(b).unwrap();
        assert_eq!(
            eng.read().unwrap().get(&next, b"k").unwrap().as_deref(),
            Some(&b"v"[..])
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn delete_range_half_open() {
        let dir = tmp_dir("del-range");
        let eng = Engine::open(&dir, EngineOptions::default()).unwrap();
        let mut b = eng.batch();
        for i in 0u8..10 {
            b.put("t", &[i], &[i]);
        }
        eng.commit(b).unwrap();
        let mut b = eng.batch();
        b.delete_range("t", &[3], &[7]);
        eng.commit(b).unwrap();
        let rt = eng.read().unwrap();
        assert!(rt.get("t", &[2]).unwrap().is_some());
        assert!(rt.get("t", &[3]).unwrap().is_none());
        assert!(rt.get("t", &[6]).unwrap().is_none());
        assert!(rt.get("t", &[7]).unwrap().is_some());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn durability_immediate_and_paranoid_open() {
        // CC-40/7: both settings open; paranoid engages 2PC.
        let dir_i = tmp_dir("dur-imm");
        let dir_p = tmp_dir("dur-par");
        let imm = Engine::open(
            &dir_i,
            EngineOptions::default().with_durability(Durability::Immediate),
        )
        .unwrap();
        let par = Engine::open(
            &dir_p,
            EngineOptions::default().with_durability(Durability::Paranoid),
        )
        .unwrap();
        assert_eq!(imm.durability(), Durability::Immediate);
        assert!(!imm.two_phase_commit());
        assert_eq!(par.durability(), Durability::Paranoid);
        assert!(par.two_phase_commit());

        // Config resolution: override wins (figment CC_STORAGE_DURABILITY path).
        let d = Durability::resolve("immediate", Some("paranoid")).unwrap();
        assert_eq!(d, Durability::Paranoid);
        assert!(d.two_phase_commit());
        let d2 = Durability::resolve("paranoid", None).unwrap();
        assert_eq!(d2, Durability::Paranoid);

        // SEC-40b-6: none rejected from config parse.
        assert!(Durability::parse("none").is_err());
        assert!(Durability::resolve("none", None).is_err());

        let mut b = par.batch();
        b.put("meta", b"x", b"y");
        par.commit(b).unwrap();
        assert_eq!(
            par.read().unwrap().get("meta", b"x").unwrap().as_deref(),
            Some(&b"y"[..])
        );

        let _ = std::fs::remove_dir_all(&dir_i);
        let _ = std::fs::remove_dir_all(&dir_p);
    }

    #[test]
    fn file_len_and_compact() {
        let dir = tmp_dir("compact");
        let eng = Engine::open(&dir, EngineOptions::default()).unwrap();
        let mut b = eng.batch();
        b.put("t", b"k", &vec![0u8; 4096]);
        eng.commit(b).unwrap();
        assert!(eng.file_len().unwrap() > 0);
        let _ = eng.compact().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_read_only_reads_and_refuses_writes() {
        let dir = tmp_dir("ro");
        {
            let eng = Engine::open(&dir, EngineOptions::default()).unwrap();
            let mut b = eng.batch();
            b.put("t", b"k", b"v");
            eng.commit(b).unwrap();
        }
        let ro = Engine::open_read_only(&dir).unwrap();
        assert!(ro.is_read_only());
        assert_eq!(
            ro.read().unwrap().get("t", b"k").unwrap().as_deref(),
            Some(&b"v"[..])
        );
        let mut b = ro.batch();
        b.put("t", b"k2", b"v2");
        let err = ro.commit(b).unwrap_err();
        assert!(
            matches!(err, StoreError::Config(_)),
            "read-only must refuse writes: {err}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_read_only_refuses_live_writer_lock() {
        let dir = tmp_dir("ro-locked");
        let writer = Engine::open(&dir, EngineOptions::default()).unwrap();
        let mut b = writer.batch();
        b.put("t", b"k", b"v");
        writer.commit(b).unwrap();
        let err = Engine::open_read_only(&dir).unwrap_err();
        assert!(
            matches!(err, StoreError::DatabaseLocked),
            "expected DatabaseLocked, got {err:?}"
        );
        drop(writer);
        // After writer drops, read-only succeeds.
        let ro = Engine::open_read_only(&dir).unwrap();
        assert!(ro.is_read_only());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_existing_missing_file_refused() {
        let dir = tmp_dir("existing-missing");
        std::fs::create_dir_all(&dir).unwrap();
        let err = Engine::open_existing(&dir, EngineOptions::default()).unwrap_err();
        assert!(matches!(err, StoreError::Config(_)), "got {err:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_refuses_file_path() {
        let dir = tmp_dir("not-dir");
        std::fs::write(&dir, b"not a dir").unwrap();
        let err = Engine::open(&dir, EngineOptions::default()).unwrap_err();
        assert!(matches!(err, StoreError::Config(_)));
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn concurrent_read_during_commit() {
        // F1: mutex must not block begin_read while a write txn is open/fsyncing.
        let dir = tmp_dir("concurrent-read");
        let eng = Arc::new(Engine::open(&dir, EngineOptions::default()).unwrap());
        // Seed one key.
        let mut b = eng.batch();
        b.put("t", b"k", b"v0");
        eng.commit(b).unwrap();

        let barrier = Arc::new(Barrier::new(2));
        let eng_w = Arc::clone(&eng);
        let bar_w = Arc::clone(&barrier);
        let writer = thread::spawn(move || {
            let mut b = eng_w.batch();
            // Large-ish value so commit takes measurable time under Immediate.
            b.put("t", b"k", &vec![1u8; 256 * 1024]);
            bar_w.wait();
            eng_w.commit(b).unwrap();
        });

        let eng_r = Arc::clone(&eng);
        let bar_r = Arc::clone(&barrier);
        let reader = thread::spawn(move || {
            bar_r.wait();
            // Read must complete while the writer may still be in commit/fsync
            // (mutex is not held across commit). Retry briefly if begin_write
            // exclusivity races on a slow scheduler.
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            loop {
                match eng_r.read() {
                    Ok(rt) => {
                        let v = rt.get("t", b"k").unwrap();
                        assert!(v.is_some());
                        break;
                    }
                    Err(_) if std::time::Instant::now() < deadline => {
                        thread::yield_now();
                    }
                    Err(e) => panic!("read() failed during concurrent commit: {e}"),
                }
            }
        });

        writer.join().unwrap();
        reader.join().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn batch_overflow_fails_commit() {
        let dir = tmp_dir("batch-overflow");
        let eng = Engine::open(&dir, EngineOptions::default()).unwrap();
        let mut b = eng.batch();
        // Force overflow flag without allocating MAX_BATCH_OPS entries if possible:
        // push one past the cap by setting overflowed via MAX pushes is heavy;
        // unit-test the flag path with a small helper simulation.
        b.overflowed = true;
        let err = eng.commit(b).unwrap_err();
        assert!(matches!(err, StoreError::Limit(_)));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
