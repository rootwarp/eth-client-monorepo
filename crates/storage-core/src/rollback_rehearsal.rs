//! S2-B-14 rollback rehearsal (E2.4). Procedure: `docs/s2-rollback.md`.
//!
//! Last SHA that still has `write_behind.rs` is `e854b1d`. This HEAD's
//! compose is not that writer. This module writes a schema-1 store with the
//! current P0 writer, clean-shuts it, and opens the same `store.redb` with
//! the `e854b1d` `open_store` gates (`Store::open` + missing-key refuse).

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use cc_store::blocks::{MIN_BLOCK_SSZ_LEN, PARENT_ROOT_SSZ_OFFSET, SLOT_SSZ_OFFSET};
    use cc_store::engine::{Durability, EngineOptions};
    use cc_store::meta::{
        KEY_SCHEMA_VERSION, KEY_WRITE_CURSOR, SchemaVersion, TABLE_META, WriteCursor,
    };
    use cc_store::{
        ConfigDigestInput, Root, Slot, SszDecode, Store, StoreOpenOptions, db_file_path,
        get_block_by_root,
    };
    use cc_types::ChainConfig;
    use prometheus_client::registry::Registry;
    use tokio::sync::watch;

    use crate::durable_set::{
        load_expected_node_id_from_key_path, refuse_missing_key_if_anchor_present,
    };
    use crate::metrics::StorageMetrics;
    use crate::open::{OpenOpts, open};
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

    fn current_writer_opts(node_key: PathBuf) -> OpenOpts {
        OpenOpts {
            durability: "immediate".to_owned(),
            check_invariants: true,
            snapshot_ring: 4,
            max_open_scan_rows: cc_store::DEFAULT_MAX_OPEN_SCAN_ROWS,
            genesis_validators_root: None,
            node_key_path: Some(node_key),
            chain: None,
        }
    }

    /// `e854b1d` `crates/storage-core/src/boot.rs` `open_store`:
    /// `Store::open` + `refuse_missing_key_if_anchor_present`.
    fn previous_topology_open(data_dir: &Path, node_key: &Path) -> Result<Store, String> {
        let durability = Durability::parse("immediate").map_err(|e| e.to_string())?;
        let digest_input = hoodi_digest_input();
        let expected_node_id = load_expected_node_id_from_key_path(Some(node_key))?;
        let opts = StoreOpenOptions::from_config(
            EngineOptions::default().with_durability(durability),
            &digest_input,
        )
        .map_err(|e| e.to_string())?
        .with_check_invariants(true)
        .with_snapshot_ring(4)
        .with_max_open_scan_rows(cc_store::DEFAULT_MAX_OPEN_SCAN_ROWS)
        .with_expected_node_id(expected_node_id);
        let store = Store::open(data_dir, opts).map_err(|e| e.to_string())?;
        refuse_missing_key_if_anchor_present(store.engine(), Some(node_key))?;
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
            open(&dir, current_writer_opts(key_path.clone())).expect("current Store::open");
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
        let err = open(&dir, current_writer_opts(key_path))
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

        let mut opts = current_writer_opts(key_path.clone());
        let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../crates/types/tests/fixtures/hoodi-config.yaml");
        opts.chain = Some(ChainConfig::from_yaml_file(&fixture).unwrap());
        opts.genesis_validators_root = Some(format!("0x{}", "ab".repeat(32)));
        let opened = open(&dir, opts).expect("current open stamps side keys");
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
}
