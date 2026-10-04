//! S2-B-14 rollback rehearsal (E2.4) and the E2R.7 pre-migration backup.
//! Procedure: `docs/s2-rollback.md`.
//!
//! Last SHA that still has `write_behind.rs` is `e854b1d`. This HEAD's
//! compose is not that writer. This module writes a schema-1 store with the
//! current P0 writer, clean-shuts it, and opens the same `store.redb` with
//! the `e854b1d` `open_store` gates (`Store::open` + missing-key refuse).
//! E2R.7 takes the backup before `bind_node_id` and does not check out `e854b1d`.

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use cc_seam::{ArchiveWrite, Bytes, DaVerdict, TrustedAnchor};
    use cc_store::blocks::{
        MIN_BLOCK_SSZ_LEN, PARENT_ROOT_SSZ_OFFSET, SLOT_SSZ_OFFSET, STATE_ROOT_SSZ_OFFSET,
    };
    use cc_store::engine::{Durability, EngineOptions};
    use cc_store::meta::{
        AnchorInfo, ForkChoiceScalars, KEY_ANCHOR_INFO, KEY_NODE_ID, KEY_NODE_ID_SCHEME,
        KEY_SCHEMA_VERSION, KEY_WRITE_CURSOR, SchemaVersion, TABLE_META, WriteCursor,
    };
    use cc_store::{
        ConfigDigestInput, Root, Slot, SszDecode, SszEncode, Store, StoreOpenOptions, db_file_path,
        get_block_by_root,
    };
    use cc_types::{ChainConfig, Checkpoint, Epoch};
    use prometheus_client::registry::Registry;
    use tokio::sync::watch;

    use crate::durable_set::refuse_missing_key_if_anchor_present;
    use crate::metrics::StorageMetrics;
    use crate::node_id::{NodeIdExpectation, NodeIdScheme};
    use crate::open::{OpenOpts, OpenedStore, open, start_writer};
    use crate::test_tmpdir::unique_temp_dir;
    use crate::writer::{
        CommitUnit, StagedBlock, WriterBounds, WriterFaults, load_write_cursor, spawn_writer,
    };

    const CURSOR_SLOT: u64 = 19;
    const CURSOR_SEQ: u64 = 11;
    const CURSOR_SESSION: u64 = 7;

    fn hoodi_digest_input() -> ConfigDigestInput {
        let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../crates/types/tests/fixtures/hoodi-config.yaml");
        let chain = ChainConfig::from_yaml_file(&fixture).unwrap();
        ConfigDigestInput::with_mainnet_scalars(chain, Root::ZERO)
    }

    fn current_writer_opts(node_key: &Path) -> OpenOpts {
        let node_id =
            NodeIdExpectation::from_configured_path(Some(node_key)).expect("test node key");
        OpenOpts {
            durability: "immediate".to_owned(),
            check_invariants: true,
            snapshot_ring: 4,
            max_open_scan_rows: cc_store::DEFAULT_MAX_OPEN_SCAN_ROWS,
            genesis_validators_root: None,
            node_id,
            chain: None,
        }
    }

    fn open_paired(dir: &Path, opts: OpenOpts) -> anyhow::Result<OpenedStore> {
        let expectation = opts.node_id;
        let mut pending = open(dir, opts)?;
        let _ = pending.peek_node_id()?;
        pending.pair(expectation)
    }

    /// Raw 32-byte read used by the `e854b1d` opener. Not the current API.
    fn legacy_bytes(node_key: &Path) -> Result<Option<Root>, String> {
        if !node_key.exists() {
            return Ok(None);
        }
        let bytes = std::fs::read(node_key).map_err(|e| e.to_string())?;
        if bytes.len() != 32 {
            return Err(format!(
                "node key at {} has length {}, expected 32",
                node_key.display(),
                bytes.len()
            ));
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Ok(Some(Root::from_array(arr)))
    }

    /// `e854b1d` `open_store` options: expected id is the raw 32-byte secret.
    fn previous_topology_options(node_key: &Path) -> Result<StoreOpenOptions, String> {
        let durability = Durability::parse("immediate").map_err(|e| e.to_string())?;
        let digest_input = hoodi_digest_input();
        let expected_node_id = legacy_bytes(node_key)?;
        Ok(StoreOpenOptions::from_config(
            EngineOptions::default().with_durability(durability),
            &digest_input,
        )
        .map_err(|e| e.to_string())?
        .with_check_invariants(true)
        .with_snapshot_ring(4)
        .with_max_open_scan_rows(cc_store::DEFAULT_MAX_OPEN_SCAN_ROWS)
        .with_expected_node_id(expected_node_id))
    }

    /// `e854b1d` `crates/storage-core/src/boot.rs` `open_store`:
    /// `Store::open` + `refuse_missing_key_if_anchor_present`.
    fn previous_topology_open(data_dir: &Path, node_key: &Path) -> Result<Store, String> {
        let expected_node_id = legacy_bytes(node_key)?;
        let opts = previous_topology_options(node_key)?;
        let store = Store::open(data_dir, opts).map_err(|e| e.to_string())?;
        let expectation = if node_key.exists() {
            match expected_node_id {
                Some(root) => NodeIdExpectation::Present(root),
                None => NodeIdExpectation::Unset,
            }
        } else {
            NodeIdExpectation::ConfiguredButMissing
        };
        refuse_missing_key_if_anchor_present(store.engine(), expectation)?;
        Ok(store)
    }

    fn synth_block(slot: u64, parent: &Root) -> Vec<u8> {
        let mut v = vec![0u8; MIN_BLOCK_SSZ_LEN];
        v[0..4].copy_from_slice(&100u32.to_le_bytes());
        v[SLOT_SSZ_OFFSET..SLOT_SSZ_OFFSET + 8].copy_from_slice(&slot.to_le_bytes());
        v[PARENT_ROOT_SSZ_OFFSET..PARENT_ROOT_SSZ_OFFSET + 32].copy_from_slice(parent.as_slice());
        v
    }

    fn refuse_strings() -> &'static [&'static str] {
        &[
            "store database locked: another process holds the file lock",
            "schema version mismatch:",
            "config digest mismatch:",
            "store invariant node_id violated:",
            "store invariant cursor violated:",
            "missing required meta record",
        ]
    }

    fn assert_not_refuse(context: &str, msg: &str) {
        for needle in refuse_strings() {
            assert!(
                !msg.contains(needle),
                "{context}: open refuse string {needle:?} in {msg}"
            );
        }
    }

    #[cfg(unix)]
    fn file_ino(path: &Path) -> u64 {
        use std::os::unix::fs::MetadataExt;
        std::fs::metadata(path).unwrap().ino()
    }

    async fn wait_exclusive_lock_free(data_dir: &Path) {
        let db = db_file_path(data_dir);
        let start = Instant::now();
        loop {
            match Store::open(
                data_dir,
                StoreOpenOptions::from_config(EngineOptions::default(), &hoodi_digest_input())
                    .unwrap()
                    .with_check_invariants(false),
            ) {
                Ok(store) => {
                    drop(store);
                    return;
                }
                Err(e) => {
                    let msg = e.to_string();
                    assert!(
                        msg.contains("locked") || msg.contains("Database"),
                        "unexpected error while waiting for exclusive lock on {}: {msg}",
                        db.display()
                    );
                    assert!(
                        start.elapsed() < Duration::from_secs(5),
                        "writer did not release the exclusive lock on {}",
                        db.display()
                    );
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn s2_b_14_current_writer_files_open_via_previous_topology_gates() {
        let dir = unique_temp_dir("s2-b-14-rollback");
        std::fs::create_dir_all(&dir).unwrap();
        let key_path = dir.join("node_key");
        let node_id = Root::from_array([0x51u8; 32]);
        std::fs::write(&key_path, node_id.as_slice()).unwrap();

        let opened =
            open_paired(&dir, current_writer_opts(&key_path)).expect("current Store::open");
        opened.persist_anchor_node_id(node_id).unwrap();

        let engine = Arc::new(opened.into_engine());
        let mut registry = Registry::default();
        let metrics = StorageMetrics::register(&mut registry);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = spawn_writer(
            Arc::clone(&engine),
            metrics,
            WriterBounds::default(),
            WriterFaults::default(),
            shutdown_rx,
            false,
        );

        let parent = Root::from_array([0x11; 32]);
        let root = Root::from_array([0xAB; 32]);
        handle
            .submit_p0_committed(CommitUnit {
                blocks: vec![StagedBlock {
                    slot: Slot::new(CURSOR_SLOT),
                    root,
                    ssz: synth_block(CURSOR_SLOT, &parent),
                    update_canonical: false,
                    write_state_root: false,
                    da_status: None,
                }],
                columns: Vec::new(),
                fork_choice: None,
                canonical_from: None,
                anchor: None,
                cursor: WriteCursor {
                    session_id: CURSOR_SESSION,
                    seq: CURSOR_SEQ,
                    slot: Slot::new(CURSOR_SLOT),
                    root,
                },
                done: None,
            })
            .await
            .unwrap();

        let written = load_write_cursor(&engine).unwrap().expect("writer cursor");
        assert_eq!(written.session_id, CURSOR_SESSION);
        assert_eq!(written.seq, CURSOR_SEQ);
        assert_eq!(written.slot, Slot::new(CURSOR_SLOT));
        assert_eq!(written.root, root);

        let db = db_file_path(&dir);
        assert!(db.is_file(), "current writer must leave {}", db.display());
        let ino_before = file_ino(&db);
        let len_before = std::fs::metadata(&db).unwrap().len();
        assert!(len_before > 0);

        // Idle (P0 committed) then shutdown watch — compose `stop` fires this;
        // mailbox is not drained.
        let _ = shutdown_tx.send(true);
        drop(handle);
        drop(engine);
        wait_exclusive_lock_free(&dir).await;

        assert_eq!(
            file_ino(&db),
            ino_before,
            "same store.redb inode after shutdown"
        );

        let prev = previous_topology_open(&dir, &key_path).unwrap_or_else(|msg| {
            assert_not_refuse("e854b1d open_store gates", &msg);
            panic!("previous-topology Store::open failed: {msg}");
        });
        {
            let rt = prev.engine().read().unwrap();
            let sv = SchemaVersion::from_ssz_bytes(
                &rt.get(TABLE_META, KEY_SCHEMA_VERSION.as_bytes())
                    .unwrap()
                    .expect("schema_version"),
            )
            .unwrap();
            assert_eq!(sv.version, 1);
            let cursor = WriteCursor::from_ssz_bytes(
                &rt.get(TABLE_META, KEY_WRITE_CURSOR.as_bytes())
                    .unwrap()
                    .expect("write_cursor"),
            )
            .unwrap();
            assert_eq!(cursor.session_id, CURSOR_SESSION);
            assert_eq!(cursor.seq, CURSOR_SEQ);
            assert_eq!(cursor.slot, Slot::new(CURSOR_SLOT));
            assert!(
                get_block_by_root(&rt, &root).unwrap().is_some(),
                "previous topology must see the row the current writer committed"
            );
        }
        drop(prev);

        // Same inode; redb may checkpoint/compact on close so the length can change.
        assert_eq!(file_ino(&db), ino_before);
        assert!(std::fs::metadata(&db).unwrap().len() > 0);

        // The rollback binary opened above. This binary must not: the store holds
        // a block and has no side keys, so legacy `config_digest` equality is not
        // an open. No GVR was configured, and that is not `Root::ZERO`.
        let err = open_paired(&dir, current_writer_opts(&key_path))
            .expect_err("populated store must not open on the legacy constant");
        let msg = err.to_string();
        assert!(
            msg.contains("genesis_validators_root") && msg.contains("not Root::ZERO"),
            "absent GVR on a store that holds a block must refuse: {msg}"
        );
        assert!(
            !msg.contains("config digest mismatch"),
            "refusal must not be a mismatch against a substituted root: {msg}"
        );

        assert_eq!(file_ino(&db), ino_before, "same inode after both opens");
        eprintln!(
            "S2-B-14 observed: current P0 writer → clean shutdown → \
             e854b1d Store::open + storage-core::open on {} inode={ino_before} bytes={len_before}",
            db.display()
        );
    }

    /// Same gates as [`s2_b_14_current_writer_files_open_via_previous_topology_gates`].
    /// Side keys are unknown to that opener. `SCHEMA_VERSION` stays 1 and
    /// `meta.config_digest` stays the legacy constant, so it still opens.
    #[test]
    fn side_keys_still_open_under_previous_topology_gates() {
        use cc_store::meta::{KEY_CONFIG_DIGEST_V2, KEY_SCHEDULE_DIGEST, TABLE_META};
        use cc_store::{SszDecode, legacy_config_digest};

        let dir = unique_temp_dir("s2r-b08-rollback");
        std::fs::create_dir_all(&dir).unwrap();
        let key_path = dir.join("node_key");
        let node_id = Root::from_array([0x51u8; 32]);
        std::fs::write(&key_path, node_id.as_slice()).unwrap();

        let mut opts = current_writer_opts(&key_path);
        let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../crates/types/tests/fixtures/hoodi-config.yaml");
        opts.chain = Some(ChainConfig::from_yaml_file(&fixture).unwrap());
        opts.genesis_validators_root = Some(format!("0x{}", "ab".repeat(32)));
        let opened = open_paired(&dir, opts).expect("current open stamps side keys");
        opened.persist_anchor_node_id(node_id).unwrap();
        {
            let rt = opened.engine().read().unwrap();
            assert!(
                rt.get(TABLE_META, KEY_CONFIG_DIGEST_V2.as_bytes())
                    .unwrap()
                    .is_some(),
                "identity side key must be written before the rollback open"
            );
            assert!(
                rt.get(TABLE_META, KEY_SCHEDULE_DIGEST.as_bytes())
                    .unwrap()
                    .is_some()
            );
        }
        drop(opened);

        let prev = previous_topology_open(&dir, &key_path).unwrap_or_else(|msg| {
            assert_not_refuse("e854b1d open_store gates with side keys", &msg);
            panic!("previous-topology Store::open failed: {msg}");
        });
        let rt = prev.engine().read().unwrap();
        let sv = SchemaVersion::from_ssz_bytes(
            &rt.get(TABLE_META, KEY_SCHEMA_VERSION.as_bytes())
                .unwrap()
                .expect("schema_version"),
        )
        .unwrap();
        assert_eq!(sv.version, 1, "SCHEMA_VERSION is not bumped");
        let digest = cc_store::meta::ConfigDigest::from_ssz_bytes(
            &rt.get(TABLE_META, cc_store::meta::KEY_CONFIG_DIGEST.as_bytes())
                .unwrap()
                .expect("config_digest"),
        )
        .unwrap();
        assert_eq!(digest.digest, legacy_config_digest());
        assert!(
            rt.get(TABLE_META, KEY_CONFIG_DIGEST_V2.as_bytes())
                .unwrap()
                .is_some(),
            "the rollback open must leave the identity side key in place"
        );
        assert!(
            rt.get(TABLE_META, KEY_SCHEDULE_DIGEST.as_bytes())
                .unwrap()
                .is_some()
        );
    }

    /// Display of the `e854b1d` gate when the stored id is no longer the secret.
    /// Quoted by `docs/s2-rollback.md`. Not a guess: this is `StoreError`'s
    /// `InvariantViolation` text for `I-node-id`.
    const MIGRATED_STORE_REFUSAL: &str = "store invariant node_id violated: \
        stored node_id <redacted> does not match the configured node key";

    fn hex64(root: &Root) -> String {
        let text = root.to_string();
        text.strip_prefix("0x").unwrap_or(&text).to_owned()
    }

    fn anchor_scalars(head: &Root, slot: u64) -> Vec<u8> {
        let checkpoint = Checkpoint {
            epoch: Epoch::new(slot / 32),
            root: *head,
        };
        ForkChoiceScalars {
            time: slot,
            proposer_boost_root: Root::ZERO,
            justified: checkpoint,
            finalized: checkpoint,
            unrealized_justified: checkpoint,
            unrealized_finalized: checkpoint,
            head_root: *head,
            head_slot: Slot::new(slot),
        }
        .as_ssz_bytes()
    }

    fn read_root(rt: &cc_store::ReadTxn, key: &str) -> Root {
        Root::from_ssz_bytes(&rt.get(TABLE_META, key.as_bytes()).unwrap().expect(key)).unwrap()
    }

    /// E2R.7. Secret-stamped store, `drain_and_shutdown`, same-inode `Store::open`,
    /// then a backup taken before `bind_node_id`. The `e854b1d` gate refuses the
    /// migrated file and opens the backup. The backup is a different inode.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn e2r7_drain_then_backup_opens_and_migrated_store_is_refused() {
        let dir = unique_temp_dir("s2r-j08-rollback");
        std::fs::create_dir_all(&dir).unwrap();
        let key_path = dir.join("node_key");
        // Last byte 1 is a valid secp256k1 scalar (`cc-node-key` fingerprint test).
        let mut secret_bytes = [0u8; 32];
        secret_bytes[31] = 1;
        std::fs::write(&key_path, secret_bytes).unwrap();
        let secret = Root::from_array(secret_bytes);
        let fingerprint = Root::from_hash256(
            cc_node_key::node_id_fingerprint(secret.as_array())
                .expect("test secret is a valid secp256k1 scalar"),
        );
        assert!(
            secret != fingerprint,
            "fingerprint must not equal the raw secret"
        );

        let mut opts = current_writer_opts(&key_path);
        let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../crates/types/tests/fixtures/hoodi-config.yaml");
        opts.chain = Some(ChainConfig::from_yaml_file(&fixture).unwrap());
        opts.genesis_validators_root = Some(format!("0x{}", "ab".repeat(32)));
        let opened = open_paired(&dir, opts.clone()).expect("pair stamps the raw secret");

        let parent = Root::from_array([0x11; 32]);
        let root = Root::from_array([0xAB; 32]);
        let state = Root::from_array([0xF0; 32]);
        // Slot 0 keeps Split.slot == finalized slot so I-split-fin stays quiet.
        let slot = 0u64;
        let block = {
            let mut v = synth_block(slot, &parent);
            v[STATE_ROOT_SSZ_OFFSET..STATE_ROOT_SSZ_OFFSET + 32].copy_from_slice(state.as_slice());
            v
        };
        let state_ssz = b"anchor-state".to_vec();

        let mut registry = Registry::default();
        let metrics = StorageMetrics::register(&mut registry);
        let rt = start_writer(opened, metrics, false);
        rt.archive()
            .commit_anchor(TrustedAnchor {
                block_root: root.into_array(),
                parent_root: parent.into_array(),
                slot,
                state_root: state.into_array(),
                block_ssz: Bytes::from(block.clone()),
                state_ssz: Bytes::from(state_ssz),
                scalars: Bytes::from(anchor_scalars(&root, slot)),
                da: DaVerdict::Available,
            })
            .await
            .expect("durable anchor row");

        let db = db_file_path(&dir);
        assert!(db.is_file(), "writer must leave {}", db.display());
        let ino_before = file_ino(&db);
        assert!(std::fs::metadata(&db).unwrap().len() > 0);

        rt.drain_and_shutdown()
            .await
            .expect("Ok only after WriterStop::Drained");
        drop(rt);
        wait_exclusive_lock_free(&dir).await;
        assert_eq!(
            file_ino(&db),
            ino_before,
            "same store.redb inode after drain_and_shutdown"
        );

        let same = Store::open(
            &dir,
            previous_topology_options(&key_path).expect("rollback open options"),
        )
        .expect("Store::open of the same store.redb after drain_and_shutdown");
        {
            let read = same.engine().read().unwrap();
            assert!(
                get_block_by_root(&read, &root).unwrap().is_some(),
                "the drained store must still hold the durable row"
            );
            assert!(
                read_root(&read, KEY_NODE_ID) == secret,
                "meta.node_id before migration is the legacy secret"
            );
            let anchor = AnchorInfo::from_ssz_bytes(
                &read
                    .get(TABLE_META, KEY_ANCHOR_INFO.as_bytes())
                    .unwrap()
                    .expect("anchor"),
            )
            .unwrap();
            assert!(
                anchor.node_id == secret,
                "AnchorInfo.node_id before migration is the legacy secret"
            );
            assert!(
                read.get(TABLE_META, KEY_NODE_ID_SCHEME.as_bytes())
                    .unwrap()
                    .is_none(),
                "scheme row is absent until bind_node_id"
            );
        }
        drop(same);
        assert_eq!(file_ino(&db), ino_before, "same inode after Store::open");

        let backup = unique_temp_dir("s2r-j08-backup");
        std::fs::create_dir_all(&backup).unwrap();
        let backup_db = db_file_path(&backup);
        let backup_key = backup.join("node_key");
        std::fs::copy(&db, &backup_db).expect("copy store.redb before bind_node_id");
        std::fs::copy(&key_path, &backup_key).expect("copy node key with the store");
        assert_ne!(
            file_ino(&backup_db),
            ino_before,
            "the backup is a copy, not the same file"
        );
        assert!(
            std::fs::read(&backup_key).unwrap() == std::fs::read(&key_path).unwrap(),
            "backup node key is the same bytes as the live key"
        );

        let mut pending = open(&dir, opts).expect("reopen the live directory");
        let (found, scheme) = pending.peek_node_id().expect("peek").expect("identity row");
        assert_eq!(scheme, NodeIdScheme::Legacy);
        assert!(
            found == secret,
            "live store is still the legacy secret before bind_node_id"
        );
        let migrated = pending
            .bind_node_id(secret, fingerprint)
            .expect("one commit rewrites both id rows");
        {
            let read = migrated.engine().read().unwrap();
            assert!(
                read_root(&read, KEY_NODE_ID) == fingerprint,
                "meta.node_id is the fingerprint"
            );
            let anchor = AnchorInfo::from_ssz_bytes(
                &read
                    .get(TABLE_META, KEY_ANCHOR_INFO.as_bytes())
                    .unwrap()
                    .expect("anchor"),
            )
            .unwrap();
            assert!(
                anchor.node_id == fingerprint,
                "AnchorInfo.node_id is the fingerprint"
            );
            let scheme_row = read
                .get(TABLE_META, KEY_NODE_ID_SCHEME.as_bytes())
                .unwrap()
                .expect("scheme");
            assert_eq!(scheme_row.as_slice(), &[1u8]);
        }
        drop(migrated);

        let err = previous_topology_open(&dir, &key_path)
            .expect_err("migrated store is refused by the e854b1d gate");
        let secret_hex = hex64(&secret);
        let fingerprint_hex = hex64(&fingerprint);
        assert_eq!(secret_hex.len(), 64);
        assert_eq!(fingerprint_hex.len(), 64);
        assert!(
            !err.contains(&secret_hex) && !err.contains(&fingerprint_hex),
            "refusal leaked a 64-hex id"
        );
        assert!(err.contains("node_id"), "refusal must name node_id");
        assert_eq!(err, MIGRATED_STORE_REFUSAL);

        let prev = previous_topology_open(&backup, &backup_key)
            .expect("pre-migration backup opens under the same gate");
        {
            let read = prev.engine().read().unwrap();
            let sv = SchemaVersion::from_ssz_bytes(
                &read
                    .get(TABLE_META, KEY_SCHEMA_VERSION.as_bytes())
                    .unwrap()
                    .expect("schema_version"),
            )
            .unwrap();
            assert_eq!(sv.version, 1, "SCHEMA_VERSION stays 1");
            assert!(
                read_root(&read, KEY_NODE_ID) == secret,
                "backup meta.node_id is still the legacy secret"
            );
            let anchor = AnchorInfo::from_ssz_bytes(
                &read
                    .get(TABLE_META, KEY_ANCHOR_INFO.as_bytes())
                    .unwrap()
                    .expect("anchor"),
            )
            .unwrap();
            assert!(
                anchor.node_id == secret,
                "backup AnchorInfo.node_id is still the legacy secret"
            );
            assert!(
                get_block_by_root(&read, &root).unwrap().is_some(),
                "backup must hold the pre-migration row"
            );
        }
        drop(prev);
    }
}
