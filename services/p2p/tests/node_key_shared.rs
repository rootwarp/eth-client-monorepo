//! p2p and the shared node-key crate agree on one key file.
//!
//! The crate and p2p both refuse `0644`, both accept `0400`, and a key the
//! crate creates is `0600` and loads through p2p.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use cc_node_key::{NodeKeyError, load_or_create as load_shared};
use cc_p2p::identity::{self, IdentityError};

fn tmp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "cc-p2p-node-key-{name}-{nanos}-{}",
        std::process::id()
    ));
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
fn crate_and_p2p_refuse_0644_and_accept_0400() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tmp_dir("agree");
    let path = dir.join("node_key");
    let bytes = secret_one();

    write_mode(&path, &bytes, 0o644);
    let shared = load_shared(&path).unwrap_err();
    assert!(
        matches!(
            shared,
            NodeKeyError::PermissionsTooBroad { mode: 0o644, .. }
        ),
        "crate refused the wrong way: {shared:?}"
    );
    let via_p2p = identity::load_or_create(&path).unwrap_err();
    assert!(
        matches!(via_p2p, IdentityError::PermissionsTooBroad { .. }),
        "p2p refused the wrong way: {via_p2p:?}"
    );
    assert_eq!(
        fs::read(&path).unwrap(),
        bytes,
        "0644 must not be rewritten"
    );

    write_mode(&path, &bytes, 0o400);
    let shared = load_shared(&path).expect("crate accepts 0400");
    assert_eq!(shared.to_bytes(), bytes);
    let via_p2p = identity::load_or_create(&path).expect("p2p accepts the same 0400 key");
    assert_eq!(via_p2p.path(), path.as_path());

    let created = dir.join("created");
    let key = load_shared(&created).unwrap();
    let mode = fs::metadata(&created).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600);
    let via_p2p = identity::load_or_create(&created).unwrap();
    assert_eq!(key.to_bytes().len(), 32);
    let _ = via_p2p;
    let _ = fs::remove_dir_all(&dir);
}
