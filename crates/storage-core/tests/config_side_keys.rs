//! Populated legacy stores are not re-stamped. The anchor witness is
//! `BeaconState::genesis_validators_root` (ADR-R-11) and a match is not
//! permission to write side keys. This file lives outside `src/` so the
//! consensus-type grep stays on `replay.rs`.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use cc_storage_core::{OpenOpts, open};
use cc_store::engine::{Engine, EngineOptions};
use cc_store::meta::{KEY_CONFIG_DIGEST, KEY_CONFIG_DIGEST_V2, KEY_SCHEDULE_DIGEST, TABLE_META};
use cc_store::{
    ConfigDigestInput, Root, Slot, SszDecode, Store, StoreOpenOptions, legacy_config_digest,
    put_snapshot,
};
use cc_types::preset::Minimal;
use cc_types::{BeaconState, ChainConfig, PresetName};
use ssz::Encode;

fn unique_temp_dir(prefix: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let seq = N.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!("{prefix}-{}-{seq}-{nanos}", std::process::id()))
}

fn hoodi_chain() -> ChainConfig {
    let mut chain = ChainConfig::from_yaml_file(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../crates/types/tests/fixtures/hoodi-config.yaml"),
    )
    .unwrap();
    // The witness is a minimal state. The preset selects the decoder only;
    // it is not a digest field.
    chain.preset_base = PresetName::Minimal;
    chain
}

fn gvr_hex(byte: u8) -> String {
    format!("0x{}", format!("{byte:02x}").repeat(32))
}

fn opts(chain: ChainConfig, gvr: &str) -> OpenOpts {
    OpenOpts {
        durability: "immediate".to_owned(),
        check_invariants: false,
        snapshot_ring: 4,
        max_open_scan_rows: cc_store::DEFAULT_MAX_OPEN_SCAN_ROWS,
        genesis_validators_root: Some(gvr.to_owned()),
        node_key_path: None,
        chain: Some(chain),
    }
}

fn snapshot_with_gvr(gvr: Root) -> Vec<u8> {
    let mut state = BeaconState::<Minimal>::default();
    state.set_genesis_validators_root(gvr);
    state.as_ssz_bytes()
}

fn meta(dir: &std::path::Path, key: &str) -> Option<Vec<u8>> {
    let engine = Engine::open(dir, EngineOptions::default()).unwrap();
    let rt = engine.read().unwrap();
    rt.get(TABLE_META, key.as_bytes()).unwrap()
}

/// The pre-`78a90e1` digest gate: `Store::open` against Hoodi + `Root::ZERO`.
fn previous_digest_gate(dir: &std::path::Path) {
    let chain = ChainConfig::from_yaml_file(
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../crates/types/tests/fixtures/hoodi-config.yaml"),
    )
    .unwrap();
    let input = ConfigDigestInput::with_mainnet_scalars(chain, Root::ZERO);
    assert_eq!(
        cc_store::compute_config_digest(&input).unwrap(),
        legacy_config_digest()
    );
    Store::open(
        dir,
        StoreOpenOptions::from_config(EngineOptions::default(), &input)
            .unwrap()
            .with_check_invariants(false),
    )
    .expect("pre-78a90e1 digest equality must still open");
}

#[test]
fn legacy_constant_restamps_when_anchor_gvr_matches() {
    let dir = unique_temp_dir("legacy-match");
    std::fs::create_dir_all(&dir).unwrap();
    let gvr = Root::from_array([0x44; 32]);
    let bare = OpenOpts {
        check_invariants: false,
        ..OpenOpts::default()
    };
    let opened = open(&dir, bare).expect("empty legacy store");
    put_snapshot(opened.engine(), Slot::new(8), &snapshot_with_gvr(gvr), 4).unwrap();
    drop(opened);

    let mut chain = hoodi_chain();
    let gvr_text = gvr_hex(0x44);
    let err = open(&dir, opts(chain.clone(), &gvr_text))
        .expect_err("a matching anchor GVR must not stamp a populated store");
    let msg = err.to_string();
    assert!(
        msg.contains("populated store refused") && msg.contains("not permission"),
        "{msg}"
    );
    assert!(meta(&dir, KEY_CONFIG_DIGEST_V2).is_none());
    assert!(meta(&dir, KEY_SCHEDULE_DIGEST).is_none());
    let digest =
        cc_store::meta::ConfigDigest::from_ssz_bytes(&meta(&dir, KEY_CONFIG_DIGEST).unwrap())
            .unwrap();
    assert_eq!(digest.digest, legacy_config_digest());
    let version = cc_store::meta::SchemaVersion::from_ssz_bytes(
        &meta(&dir, cc_store::meta::KEY_SCHEMA_VERSION).unwrap(),
    )
    .unwrap();
    assert_eq!(version.version, 1);
    previous_digest_gate(&dir);

    // A different seconds_per_slot is still not copied onto the populated store.
    chain.seconds_per_slot = 6;
    let err = open(&dir, opts(chain, &gvr_text)).expect_err("identity fields stay unstamped");
    assert!(
        !err.to_string().contains("identity digest mismatch"),
        "{err}"
    );
    assert!(meta(&dir, KEY_CONFIG_DIGEST_V2).is_none());
    assert!(meta(&dir, KEY_SCHEDULE_DIGEST).is_none());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn legacy_constant_refuses_when_anchor_gvr_differs() {
    let dir = unique_temp_dir("legacy-mismatch");
    std::fs::create_dir_all(&dir).unwrap();
    let opened = open(
        &dir,
        OpenOpts {
            check_invariants: false,
            ..OpenOpts::default()
        },
    )
    .unwrap();
    put_snapshot(
        opened.engine(),
        Slot::new(8),
        &snapshot_with_gvr(Root::from_array([0x44; 32])),
        4,
    )
    .unwrap();
    drop(opened);

    let err = open(&dir, opts(hoodi_chain(), &gvr_hex(0x45)))
        .expect_err("a disagreeing anchor GVR must not re-stamp");
    let msg = err.to_string();
    assert!(
        msg.contains("populated store refused") && msg.contains("does not match"),
        "{msg}"
    );
    assert!(meta(&dir, KEY_CONFIG_DIGEST_V2).is_none());
    assert!(meta(&dir, KEY_SCHEDULE_DIGEST).is_none());
    let digest =
        cc_store::meta::ConfigDigest::from_ssz_bytes(&meta(&dir, KEY_CONFIG_DIGEST).unwrap())
            .unwrap();
    assert_eq!(digest.digest, legacy_config_digest());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn anchor_witness_is_beacon_state_genesis_validators_root() {
    let gvr = Root::from_array([0x7A; 32]);
    let state = {
        let mut state = BeaconState::<Minimal>::default();
        state.set_genesis_validators_root(gvr);
        state
    };
    assert_eq!(
        state.genesis_validators_root(),
        gvr,
        "the stored cross-check is BeaconState::genesis_validators_root"
    );
    // Four fatal fields (genesis fork version, deposit contract, deposit
    // chain id, seconds per slot) are not on this state. A GVR match does
    // not prove them. ADR-R-11 refuses to invent that witness.
}
