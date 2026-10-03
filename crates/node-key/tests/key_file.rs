//! File contract for the shared node key.
//!
//! beacon-core still has its own `ensure_node_key` until J-02, so this test
//! does not call that binary. The crate is the implementation both hosts will
//! use. p2p calls it now. A `0644` key is refused; a stricter `0400` key is
//! accepted; a key this crate creates is mode `0600`.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use cc_node_key::{NODE_KEY_MODE, NodeKeyError, load_or_create};

fn tmp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir =
        std::env::temp_dir().join(format!("cc-node-key-{name}-{nanos}-{}", std::process::id()));
    fs::create_dir_all(&dir).unwrap();
    dir
}

#[cfg(unix)]
fn write_mode(path: &Path, bytes: &[u8], mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    fs::write(path, bytes).unwrap();
    let mut perms = fs::metadata(path).unwrap().permissions();
    perms.set_mode(mode);
    fs::set_permissions(path, perms).unwrap();
}

fn secret_one() -> [u8; 32] {
    let mut secret = [0u8; 32];
    secret[31] = 1;
    secret
}

#[test]
#[cfg(unix)]
fn created_key_is_0600_and_reloads() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tmp_dir("create");
    let path = dir.join("node_key");
    let created = load_or_create(&path).unwrap();
    assert!(path.is_file());
    let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, NODE_KEY_MODE);
    assert_eq!(mode, 0o600);
    let names: Vec<_> = fs::read_dir(&dir)
        .unwrap()
        .map(|ent| ent.unwrap().file_name())
        .collect();
    assert_eq!(names, vec![std::ffi::OsString::from("node_key")]);
    let loaded = load_or_create(&path).unwrap();
    assert_eq!(loaded.to_bytes(), created.to_bytes());
    assert_eq!(loaded.path(), path.as_path());
    let _ = fs::remove_dir_all(&dir);
}

#[test]
#[cfg(unix)]
fn mode_0644_is_refused_and_not_rewritten() {
    let dir = tmp_dir("broad");
    let path = dir.join("node_key");
    let bytes = secret_one();
    write_mode(&path, &bytes, 0o644);
    let err = load_or_create(&path).unwrap_err();
    assert!(
        matches!(err, NodeKeyError::PermissionsTooBroad { mode: 0o644, .. }),
        "got {err:?}"
    );
    assert_eq!(fs::read(&path).unwrap(), bytes);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
#[cfg(unix)]
fn mode_0400_is_accepted() {
    let dir = tmp_dir("strict");
    let path = dir.join("node_key");
    let bytes = secret_one();
    write_mode(&path, &bytes, 0o400);
    let loaded = load_or_create(&path).expect("0400 is stricter than 0600, not broader");
    assert_eq!(loaded.to_bytes(), bytes);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
#[cfg(unix)]
fn short_file_is_refused_not_replaced() {
    let dir = tmp_dir("short");
    let path = dir.join("node_key");
    // 24 bytes is a length `k256::SecretKey::from_slice` accepts by left-padding.
    let bytes = vec![1u8; 24];
    write_mode(&path, &bytes, 0o600);
    let err = load_or_create(&path).unwrap_err();
    assert!(
        matches!(err, NodeKeyError::InvalidLength { got: 24, .. }),
        "got {err:?}"
    );
    assert_eq!(fs::read(&path).unwrap(), bytes);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
#[cfg(unix)]
fn invalid_scalar_is_refused() {
    let dir = tmp_dir("scalar");
    let path = dir.join("node_key");
    let bytes = [0u8; 32];
    write_mode(&path, &bytes, 0o600);
    let err = load_or_create(&path).unwrap_err();
    assert!(
        matches!(err, NodeKeyError::InvalidSecret { .. }),
        "got {err:?}"
    );
    assert_eq!(fs::read(&path).unwrap(), bytes);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn empty_path_and_parent_dir_refused() {
    assert!(matches!(load_or_create(""), Err(NodeKeyError::EmptyPath)));
    assert!(matches!(
        load_or_create("../secrets/node_key"),
        Err(NodeKeyError::PathEscape { .. })
    ));
}

#[test]
#[cfg(unix)]
fn concurrent_first_boot_yields_one_key() {
    let dir = tmp_dir("race");
    let path = dir.join("node_key");
    let n = 8;
    let barrier = Arc::new(Barrier::new(n));
    let mut handles = Vec::with_capacity(n);
    for _ in 0..n {
        let barrier = Arc::clone(&barrier);
        let path = path.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            load_or_create(&path).expect("racer").to_bytes()
        }));
    }
    let mut secrets = Vec::with_capacity(n);
    for handle in handles {
        secrets.push(handle.join().expect("join"));
    }
    for secret in &secrets[1..] {
        assert_eq!(secret, &secrets[0]);
    }
    assert_eq!(fs::read(&path).unwrap(), secrets[0]);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
#[cfg(not(unix))]
fn non_unix_is_explicit_refusal() {
    let dir = tmp_dir("non-unix");
    let err = load_or_create(dir.join("node_key")).unwrap_err();
    assert!(
        matches!(err, NodeKeyError::NonUnix),
        "non-unix must refuse, not skip mode checks; got {err:?}"
    );
    let _ = fs::remove_dir_all(&dir);
}
