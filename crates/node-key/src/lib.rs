//! Persisted secp256k1 node key shared by hosts that load `node_key`.
//!
//! The on-disk secret is 32 raw bytes, mode `0600` when this crate creates it.
//! Group or other permission bits are refused. A stricter owner-only mode such
//! as `0400` is accepted. Non-unix platforms are refused: mode bits are not
//! skipped.
//!
//! Create opens the final path with `create_new` (`O_EXCL`). There is no
//! temporary file and no rename — rename would replace an existing key and
//! defeat no-clobber. Two first boots of the same path yield one key: the
//! loser sees `EEXIST` and loads the winner, retrying while the file is still
//! short. The file is `sync_all`'d, then the parent directory is fsynced.
//!
//! [`node_id_fingerprint`] is the D-22a id `SHA256("cc-node-id-v1" ‖
//! uncompressed pubkey)`. Nothing in the load/create path calls it.

use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};
use std::thread;
use std::time::Duration;

use thiserror::Error;

/// Default relative path when config omits `node_key_path`.
pub const DEFAULT_NODE_KEY_PATH: &str = "./data/node_key";

/// Mode bits used when this crate creates a key file (owner read/write).
pub const NODE_KEY_MODE: u32 = 0o600;

/// On-disk secret length. Checked before `k256::SecretKey::from_slice`, which
/// accepts slices from 24 bytes and left-pads them.
pub const NODE_KEY_LEN: usize = 32;

/// Domain string mixed into [`node_id_fingerprint`].
pub const NODE_ID_FINGERPRINT_DOMAIN: &[u8] = b"cc-node-id-v1";

/// Uncompressed SEC1 public key length (`0x04 ‖ x ‖ y`).
const UNCOMPRESSED_PUBKEY_LEN: usize = 65;

/// How many times the `EEXIST` loser re-reads a still-short winner file.
const WINNER_READ_ATTEMPTS: u32 = 32;

#[cfg(all(test, unix))]
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(all(test, unix))]
static PARENT_FSYNCS: AtomicUsize = AtomicUsize::new(0);

#[cfg(all(test, unix))]
thread_local! {
    static FAIL_PARENT_FSYNC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// 32-byte secp256k1 secret loaded from or written to `path`.
#[derive(Clone)]
pub struct NodeKey {
    secret: [u8; NODE_KEY_LEN],
    path: PathBuf,
}

impl std::fmt::Debug for NodeKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeKey")
            .field("path", &self.path)
            .field("secret", &"<redacted>")
            .finish()
    }
}

impl NodeKey {
    /// Copy of the raw secret. Callers that build a libp2p identity consume this.
    #[must_use]
    pub fn to_bytes(&self) -> [u8; NODE_KEY_LEN] {
        self.secret
    }

    /// Path the secret was loaded from or written to.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Errors from [`load_or_create`], [`validate_node_key_path`], and the fingerprint.
#[derive(Debug, Error)]
pub enum NodeKeyError {
    /// Group or other permission bits are set.
    #[error(
        "node key permissions too broad at {}: mode {mode:#o} (group/other bits must be clear; create mode {required:#o})",
        path.display()
    )]
    PermissionsTooBroad {
        /// Key path.
        path: PathBuf,
        /// Mode bits (`0o777`).
        mode: u32,
        /// Mode used when this crate creates a file.
        required: u32,
    },
    /// File length is not exactly 32 bytes.
    #[error("node key at {} has length {got}, expected 32", path.display())]
    InvalidLength {
        /// Key path.
        path: PathBuf,
        /// Observed length.
        got: usize,
    },
    /// Bytes are not a valid non-zero secp256k1 scalar.
    #[error(
        "node key at {} is not a valid secp256k1 secret: {source}",
        path.display()
    )]
    InvalidSecret {
        /// Key path, or `<secret>` for an in-memory scalar.
        path: PathBuf,
        /// Curve rejection.
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// Filesystem I/O, including a failed parent-directory fsync.
    #[error("node key I/O at {}: {source}", path.display())]
    Io {
        /// Path of the failed operation.
        path: PathBuf,
        /// OS error.
        #[source]
        source: io::Error,
    },
    /// Path is empty.
    #[error("node key path is empty")]
    EmptyPath,
    /// Path contains `..`.
    #[error("node key path must not contain '..' components: {}", path.display())]
    PathEscape {
        /// Rejected path.
        path: PathBuf,
    },
    /// Mode bits cannot be enforced, so load and create are refused.
    #[error("node key load/create is refused on non-unix platforms (mode bits cannot be enforced)")]
    NonUnix,
    /// OS CSPRNG failed while drawing a new scalar.
    #[error("node key generation failed at {}: {message}", path.display())]
    Rng {
        /// Key path being created.
        path: PathBuf,
        /// RNG error text.
        message: String,
    },
}

/// Validate and normalise a configured `node_key_path` before open or create.
///
/// Refuses an empty path and any `..` component. `.` components are dropped.
pub fn validate_node_key_path(path: impl AsRef<Path>) -> Result<PathBuf, NodeKeyError> {
    let path = path.as_ref();
    if path.as_os_str().is_empty() {
        return Err(NodeKeyError::EmptyPath);
    }
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => out.push(prefix.as_os_str()),
            Component::RootDir => out.push(Component::RootDir.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                return Err(NodeKeyError::PathEscape {
                    path: path.to_path_buf(),
                });
            }
            Component::Normal(segment) => out.push(segment),
        }
    }
    if out.as_os_str().is_empty() {
        return Err(NodeKeyError::EmptyPath);
    }
    Ok(out)
}

/// Load a 32-byte secp256k1 secret from `path`, or create one at mode [`NODE_KEY_MODE`].
///
/// Refuses group/other permission bits, a non-32-byte file, and an invalid
/// scalar. Does not replace an existing file.
pub fn load_or_create(path: impl AsRef<Path>) -> Result<NodeKey, NodeKeyError> {
    ensure_platform()?;
    let path = validate_node_key_path(path)?;
    // `exists` is not the no-clobber decision: O_EXCL publishes a 0-byte file
    // before the 32-byte write. A still-empty file is the other host's create,
    // not a finished key.
    if path.exists() {
        match load_existing(&path) {
            Err(NodeKeyError::InvalidLength { got: 0, .. }) => load_winner(&path),
            other => other,
        }
    } else {
        create_new(&path)
    }
}

/// Uncompressed SEC1 public key (`0x04 ‖ x ‖ y`) for `secret`.
pub fn uncompressed_pubkey(secret: &[u8; NODE_KEY_LEN]) -> Result<[u8; 65], NodeKeyError> {
    use k256::elliptic_curve::sec1::ToSec1Point;
    let sk = parse_scalar(secret)?;
    let encoded = sk.public_key().to_sec1_point(false);
    let raw = encoded.as_bytes();
    if raw.len() != UNCOMPRESSED_PUBKEY_LEN || raw[0] != 0x04 {
        return Err(invalid_secret(
            Path::new("<secret>"),
            io::Error::other("uncompressed secp256k1 public key was not 65 bytes"),
        ));
    }
    let mut out = [0u8; UNCOMPRESSED_PUBKEY_LEN];
    out.copy_from_slice(raw);
    Ok(out)
}

/// D-22a fingerprint: `SHA256("cc-node-id-v1" ‖ uncompressed pubkey)`.
///
/// Housed here for a later migration. The load/create path does not call it,
/// and production hosts must not either until that migration.
pub fn node_id_fingerprint(secret: &[u8; NODE_KEY_LEN]) -> Result<cc_types::Hash256, NodeKeyError> {
    let pubkey = uncompressed_pubkey(secret)?;
    let mut preimage = Vec::with_capacity(NODE_ID_FINGERPRINT_DOMAIN.len() + pubkey.len());
    preimage.extend_from_slice(NODE_ID_FINGERPRINT_DOMAIN);
    preimage.extend_from_slice(&pubkey);
    Ok(cc_types::Hash256::from(ethereum_hashing::hash_fixed(
        &preimage,
    )))
}

enum ModeFault {
    NonUnix,
    TooBroad { mode: u32 },
}

/// `unix == false` is an explicit refusal. Otherwise group/other bits (`0o077`)
/// must be clear. File-type bits above `0o777` are ignored.
fn mode_policy(unix: bool, mode: u32) -> Result<(), ModeFault> {
    if !unix {
        return Err(ModeFault::NonUnix);
    }
    let mode = mode & 0o777;
    if (mode & 0o077) != 0 {
        return Err(ModeFault::TooBroad { mode });
    }
    Ok(())
}

fn ensure_platform() -> Result<(), NodeKeyError> {
    match mode_policy(cfg!(unix), 0) {
        Ok(()) => Ok(()),
        Err(ModeFault::NonUnix) => Err(NodeKeyError::NonUnix),
        Err(ModeFault::TooBroad { mode }) => Err(NodeKeyError::PermissionsTooBroad {
            path: PathBuf::new(),
            mode,
            required: NODE_KEY_MODE,
        }),
    }
}

enum SecretDecodeError {
    Length(usize),
    Scalar(Box<dyn std::error::Error + Send + Sync>),
}

/// Reject any length other than 32 before `SecretKey::from_slice`.
fn decode_file_secret(bytes: &[u8]) -> Result<[u8; NODE_KEY_LEN], SecretDecodeError> {
    if bytes.len() != NODE_KEY_LEN {
        return Err(SecretDecodeError::Length(bytes.len()));
    }
    let mut secret = [0u8; NODE_KEY_LEN];
    secret.copy_from_slice(bytes);
    k256::SecretKey::from_slice(&secret).map_err(|err| SecretDecodeError::Scalar(Box::new(err)))?;
    Ok(secret)
}

fn parse_scalar(secret: &[u8; NODE_KEY_LEN]) -> Result<k256::SecretKey, NodeKeyError> {
    k256::SecretKey::from_slice(secret).map_err(|err| invalid_secret(Path::new("<secret>"), err))
}

fn invalid_secret(
    path: &Path,
    source: impl std::error::Error + Send + Sync + 'static,
) -> NodeKeyError {
    NodeKeyError::InvalidSecret {
        path: path.to_path_buf(),
        source: Box::new(source),
    }
}

fn io_at(path: &Path, source: io::Error) -> NodeKeyError {
    NodeKeyError::Io {
        path: path.to_path_buf(),
        source,
    }
}

fn load_existing(path: &Path) -> Result<NodeKey, NodeKeyError> {
    check_permissions(path)?;
    let bytes = fs::read(path).map_err(|source| io_at(path, source))?;
    let secret = match decode_file_secret(&bytes) {
        Ok(secret) => secret,
        Err(SecretDecodeError::Length(got)) => {
            return Err(NodeKeyError::InvalidLength {
                path: path.to_path_buf(),
                got,
            });
        }
        Err(SecretDecodeError::Scalar(source)) => {
            return Err(NodeKeyError::InvalidSecret {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    Ok(NodeKey {
        secret,
        path: path.to_path_buf(),
    })
}

fn create_new(path: &Path) -> Result<NodeKey, NodeKeyError> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).map_err(|source| io_at(path, source))?;
    }
    let secret = generate_secret(path)?;
    match write_new(path, &secret)? {
        WriteNew::Created => {
            check_permissions(path)?;
            Ok(NodeKey {
                secret,
                path: path.to_path_buf(),
            })
        }
        WriteNew::Exists => load_winner(path),
    }
}

/// Loser of `O_EXCL`: the winner may still be writing the 32 bytes.
///
/// Only a still-empty file is treated as in progress. Any other length is
/// final (a short key is refused, not overwritten).
fn load_winner(path: &Path) -> Result<NodeKey, NodeKeyError> {
    let mut wait = Duration::from_millis(1);
    for _ in 0..WINNER_READ_ATTEMPTS {
        match load_existing(path) {
            Err(NodeKeyError::InvalidLength { got: 0, .. }) => {
                thread::sleep(wait);
                wait = (wait * 2).min(Duration::from_millis(25));
            }
            other => return other,
        }
    }
    load_existing(path)
}

fn generate_secret(path: &Path) -> Result<[u8; NODE_KEY_LEN], NodeKeyError> {
    use k256::elliptic_curve::Generate;
    for _ in 0..4 {
        let sk = k256::SecretKey::try_generate().map_err(|err| NodeKeyError::Rng {
            path: path.to_path_buf(),
            message: err.to_string(),
        })?;
        let raw = sk.to_bytes();
        let slice: &[u8] = raw.as_ref();
        if slice.len() != NODE_KEY_LEN {
            continue;
        }
        let mut secret = [0u8; NODE_KEY_LEN];
        secret.copy_from_slice(slice);
        if k256::SecretKey::from_slice(&secret).is_ok() {
            return Ok(secret);
        }
    }
    Err(NodeKeyError::Rng {
        path: path.to_path_buf(),
        message: "could not draw a non-zero secp256k1 scalar".to_owned(),
    })
}

enum WriteNew {
    Created,
    Exists,
}

fn write_new(path: &Path, secret: &[u8; NODE_KEY_LEN]) -> Result<WriteNew, NodeKeyError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        let mut file = match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(NODE_KEY_MODE)
            .open(path)
        {
            Ok(file) => file,
            Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {
                return Ok(WriteNew::Exists);
            }
            Err(source) => return Err(io_at(path, source)),
        };
        let mut perms = file
            .metadata()
            .map_err(|source| io_at(path, source))?
            .permissions();
        perms.set_mode(NODE_KEY_MODE);
        if let Err(source) = fs::set_permissions(path, perms) {
            drop(file);
            let _ = fs::remove_file(path);
            return Err(io_at(path, source));
        }
        if let Err(source) = file.write_all(secret) {
            drop(file);
            let _ = fs::remove_file(path);
            return Err(io_at(path, source));
        }
        if let Err(source) = file.sync_all() {
            return Err(io_at(path, source));
        }
        fsync_parent(path)?;
        Ok(WriteNew::Created)
    }
    #[cfg(not(unix))]
    {
        let _ = (path, secret);
        Err(NodeKeyError::NonUnix)
    }
}

#[cfg(unix)]
fn fsync_parent(path: &Path) -> Result<(), NodeKeyError> {
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    #[cfg(test)]
    if FAIL_PARENT_FSYNC.with(std::cell::Cell::get) {
        return Err(io_at(
            parent,
            io::Error::other("injected parent fsync failure"),
        ));
    }
    let dir = File::open(parent).map_err(|source| io_at(parent, source))?;
    dir.sync_all().map_err(|source| io_at(parent, source))?;
    #[cfg(test)]
    {
        PARENT_FSYNCS.fetch_add(1, Ordering::Relaxed);
    }
    Ok(())
}

fn check_permissions(path: &Path) -> Result<(), NodeKeyError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let meta = fs::metadata(path).map_err(|source| io_at(path, source))?;
        let mode = meta.permissions().mode();
        match mode_policy(true, mode) {
            Ok(()) => Ok(()),
            Err(ModeFault::NonUnix) => Err(NodeKeyError::NonUnix),
            Err(ModeFault::TooBroad { mode }) => Err(NodeKeyError::PermissionsTooBroad {
                path: path.to_path_buf(),
                mode,
                required: NODE_KEY_MODE,
            }),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Err(NodeKeyError::NonUnix)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn mode_policy_group_other_bits_and_non_unix() {
        assert!(matches!(mode_policy(false, 0o600), Err(ModeFault::NonUnix)));
        assert!(mode_policy(true, 0o600).is_ok());
        assert!(mode_policy(true, 0o400).is_ok());
        assert!(mode_policy(true, 0o700).is_ok());
        assert!(matches!(
            mode_policy(true, 0o644),
            Err(ModeFault::TooBroad { mode: 0o644 })
        ));
        assert!(matches!(
            mode_policy(true, 0o640),
            Err(ModeFault::TooBroad { mode: 0o640 })
        ));
        // File-type bits (for example `S_IFREG`) are masked off before the check.
        assert!(matches!(
            mode_policy(true, 0o100644),
            Err(ModeFault::TooBroad { mode: 0o644 })
        ));
    }

    #[test]
    fn twenty_four_byte_slice_is_not_passed_to_from_slice() {
        let short = vec![1u8; 24];
        assert!(
            k256::SecretKey::from_slice(&short).is_ok(),
            "precondition: k256 left-pads a 24-byte scalar"
        );
        let err = decode_file_secret(&short).unwrap_err();
        assert!(matches!(err, SecretDecodeError::Length(24)));
        assert!(matches!(
            decode_file_secret(&[0u8; 31]),
            Err(SecretDecodeError::Length(31))
        ));
        assert!(matches!(
            decode_file_secret(&[0u8; 33]),
            Err(SecretDecodeError::Length(33))
        ));
        assert!(matches!(
            decode_file_secret(&[0xff; 32]),
            Err(SecretDecodeError::Scalar(_))
        ));
        assert!(matches!(
            decode_file_secret(&[0u8; 32]),
            Err(SecretDecodeError::Scalar(_))
        ));
    }

    #[test]
    fn fingerprint_is_sha256_of_domain_and_uncompressed_pubkey() {
        let mut secret = [0u8; 32];
        secret[31] = 1;
        let pubkey = uncompressed_pubkey(&secret).unwrap();
        assert_eq!(pubkey.len(), 65);
        assert_eq!(pubkey[0], 0x04);
        let mut preimage = b"cc-node-id-v1".to_vec();
        preimage.extend_from_slice(&pubkey);
        let expect = cc_types::Hash256::from(ethereum_hashing::hash_fixed(&preimage));
        assert_eq!(NODE_ID_FINGERPRINT_DOMAIN, b"cc-node-id-v1");
        assert_eq!(node_id_fingerprint(&secret).unwrap(), expect);

        let mut other = secret;
        other[31] = 2;
        assert_ne!(
            node_id_fingerprint(&other).unwrap(),
            node_id_fingerprint(&secret).unwrap()
        );
    }

    #[cfg(unix)]
    fn tmp_dir(name: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "cc-node-key-unit-{name}-{nanos}-{}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    #[cfg(unix)]
    fn create_fsyncs_parent_directory() {
        let before = PARENT_FSYNCS.load(Ordering::Relaxed);
        let dir = tmp_dir("fsync");
        let path = dir.join("node_key");
        load_or_create(&path).unwrap();
        let after = PARENT_FSYNCS.load(Ordering::Relaxed);
        assert!(
            after > before,
            "parent directory was not fsynced ({before} -> {after})"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    #[cfg(unix)]
    fn parent_fsync_failure_fails_create() {
        let _guard = ParentFsyncGuard::arm();
        let dir = tmp_dir("fsync-fail");
        let path = dir.join("node_key");
        let err = load_or_create(&path).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("injected parent fsync failure"),
            "parent fsync failure must surface, got {msg}"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    struct ParentFsyncGuard;

    #[cfg(unix)]
    impl ParentFsyncGuard {
        fn arm() -> Self {
            FAIL_PARENT_FSYNC.with(|flag| flag.set(true));
            Self
        }
    }

    #[cfg(unix)]
    impl Drop for ParentFsyncGuard {
        fn drop(&mut self) {
            FAIL_PARENT_FSYNC.with(|flag| flag.set(false));
        }
    }
}
