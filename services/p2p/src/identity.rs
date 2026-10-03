//! Persisted node identity — Architecture §3.5, CC-20/2.
//!
//! The secp256k1 secret at `node_key_path` is a **custody input**, not key-management
//! hygiene: `get_custody_groups(node_id, …)` is a deterministic function of the discv5
//! node id. Regenerating the key on every start changes the custodied column set and
//! makes `earliest_available_slot` a claim about columns we no longer hold.
//!
//! The same secret is the libp2p identity **and** the discv5 identity so `PeerId` and
//! `NodeId` stay coupled. The file (mode, exclusive create, parent fsync) is
//! `cc_node_key`. This module only turns those 32 bytes into libp2p and discv5 ids.

use std::io;
use std::path::{Path, PathBuf};

use cc_libp2p::PeerId;
use cc_libp2p::reexport::identity::{self, Keypair};
use cc_node_key::NodeKeyError;
use discv5::enr::{CombinedKey, Enr, NodeId};
use thiserror::Error;

pub use cc_node_key::{DEFAULT_NODE_KEY_PATH, NODE_KEY_MODE};

/// Loaded (or newly created) node identity.
///
/// Holds the libp2p [`Keypair`] and the discv5 [`NodeId`] derived from the **same**
/// 32-byte secret. Consumers that need a discv5 signer call [`NodeIdentity::combined_key`].
#[derive(Clone)]
pub struct NodeIdentity {
    keypair: Keypair,
    peer_id: PeerId,
    node_id: NodeId,
    /// Raw 32-byte secp256k1 secret — retained so discv5 `CombinedKey` can be rebuilt
    /// (CombinedKey is not `Clone`).
    secret: [u8; 32],
    path: PathBuf,
}

impl std::fmt::Debug for NodeIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeIdentity")
            .field("peer_id", &self.peer_id)
            .field("node_id", &self.node_id)
            .field("path", &self.path)
            .field("secret", &"<redacted>")
            .finish()
    }
}

/// Errors from [`load_or_create`].
#[derive(Debug, Error)]
pub enum IdentityError {
    /// Group or other permission bits are set (refuse before bind). `0400` is accepted.
    #[error(
        "node key permissions too broad at {}: mode {mode:#o} (require {required:#o})",
        path.display()
    )]
    PermissionsTooBroad {
        path: PathBuf,
        mode: u32,
        required: u32,
    },
    /// Key file length is not exactly 32 bytes.
    #[error("node key at {} has length {got}, expected 32", path.display())]
    InvalidLength { path: PathBuf, got: usize },
    /// Bytes are not a valid secp256k1 secret (libp2p).
    #[error("node key at {} is not a valid secp256k1 secret: {source}", path.display())]
    InvalidSecret {
        path: PathBuf,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// discv5 / ENR side rejected the same secret.
    #[error("discv5 identity from node key at {}: {message}", path.display())]
    Discv5 { path: PathBuf, message: String },
    /// Filesystem I/O.
    #[error("node key I/O at {}: {source}", path.display())]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    /// Path is empty or otherwise unusable before any open.
    #[error("node key path is empty")]
    EmptyPath,
    /// Path contains `..` (refused as path-escape under config control).
    #[error("node key path must not contain '..' components: {}", path.display())]
    PathEscape { path: PathBuf },
    /// `cc-node-key` refused a non-unix host instead of skipping the mode check.
    #[error("node key load/create is refused on non-unix platforms (mode bits cannot be enforced)")]
    NonUnix,
}

impl NodeIdentity {
    /// libp2p keypair (same secret as discv5).
    #[must_use]
    pub fn keypair(&self) -> &Keypair {
        &self.keypair
    }

    /// libp2p peer id.
    #[must_use]
    pub const fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    /// discv5 node id (custody input). Taken **by value** by later custody construction.
    #[must_use]
    pub const fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Path the secret was loaded from / written to.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Rebuild a discv5 [`CombinedKey`] from the persisted secret.
    pub fn combined_key(&self) -> Result<CombinedKey, IdentityError> {
        let mut bytes = self.secret;
        CombinedKey::secp256k1_from_bytes(&mut bytes).map_err(|e| IdentityError::Discv5 {
            path: self.path.clone(),
            message: format!("CombinedKey::secp256k1_from_bytes: {e}"),
        })
    }
}

/// Validate and normalise a configured `node_key_path` before open/create.
///
/// - refuses empty paths
/// - refuses `..` components (config-controlled path escape)
/// - normalises away `.` components
pub fn validate_node_key_path(path: impl AsRef<Path>) -> Result<PathBuf, IdentityError> {
    cc_node_key::validate_node_key_path(path).map_err(identity_error)
}

/// Load a 32-byte secp256k1 secret from `path`, or create one at mode [`NODE_KEY_MODE`].
///
/// **Refuses to start** if the file has group or other permission bits. Mode `0400`
/// is accepted. Loaded **before** any bind in `main` (CC-20/2 structural load order).
/// The bytes come from [`cc_node_key::load_or_create`].
pub fn load_or_create(path: impl AsRef<Path>) -> Result<NodeIdentity, IdentityError> {
    let key = cc_node_key::load_or_create(path).map_err(identity_error)?;
    identity_from_secret(key.path(), key.to_bytes())
}

fn identity_error(err: NodeKeyError) -> IdentityError {
    match err {
        NodeKeyError::PermissionsTooBroad {
            path,
            mode,
            required,
        } => IdentityError::PermissionsTooBroad {
            path,
            mode,
            required,
        },
        NodeKeyError::InvalidLength { path, got } => IdentityError::InvalidLength { path, got },
        NodeKeyError::InvalidSecret { path, source } => {
            IdentityError::InvalidSecret { path, source }
        }
        NodeKeyError::Io { path, source } => IdentityError::Io { path, source },
        NodeKeyError::EmptyPath => IdentityError::EmptyPath,
        NodeKeyError::PathEscape { path } => IdentityError::PathEscape { path },
        NodeKeyError::NonUnix => IdentityError::NonUnix,
        NodeKeyError::Rng { path, message } => IdentityError::Io {
            path,
            source: io::Error::other(message),
        },
    }
}

fn identity_from_secret(path: &Path, secret: [u8; 32]) -> Result<NodeIdentity, IdentityError> {
    let keypair = keypair_from_secret(path, secret)?;
    let peer_id = PeerId::from_public_key(&keypair.public());
    let node_id = node_id_from_secret(path, secret)?;
    Ok(NodeIdentity {
        keypair,
        peer_id,
        node_id,
        secret,
        path: path.to_path_buf(),
    })
}

fn keypair_from_secret(path: &Path, secret: [u8; 32]) -> Result<Keypair, IdentityError> {
    let mut bytes = secret;
    let sk = identity::secp256k1::SecretKey::try_from_bytes(&mut bytes).map_err(|e| {
        IdentityError::InvalidSecret {
            path: path.to_path_buf(),
            source: Box::new(e),
        }
    })?;
    let secp_kp = identity::secp256k1::Keypair::from(sk);
    Ok(Keypair::from(secp_kp))
}

fn node_id_from_secret(path: &Path, secret: [u8; 32]) -> Result<NodeId, IdentityError> {
    let mut bytes = secret;
    let combined =
        CombinedKey::secp256k1_from_bytes(&mut bytes).map_err(|e| IdentityError::Discv5 {
            path: path.to_path_buf(),
            message: format!("CombinedKey::secp256k1_from_bytes: {e}"),
        })?;
    let enr = Enr::empty(&combined).map_err(|e| IdentityError::Discv5 {
        path: path.to_path_buf(),
        message: format!("Enr::empty: {e}"),
    })?;
    Ok(enr.node_id())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("cc-p2p-id-{name}-{nanos}"))
    }

    #[test]
    fn missing_path_creates_0600_file_not_ephemeral() {
        let path = tmp_path("create");
        let _ = fs::remove_file(&path);
        let id = load_or_create(&path).expect("create");
        assert!(path.is_file(), "key must be persisted to disk");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }
        assert_eq!(id.path(), path.as_path());
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn peer_id_stable_across_two_loads() {
        let path = tmp_path("stable");
        let _ = fs::remove_file(&path);
        let a = load_or_create(&path).expect("first");
        let b = load_or_create(&path).expect("second");
        assert_eq!(a.peer_id(), b.peer_id());
        assert_eq!(a.node_id(), b.node_id());
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn changing_key_file_changes_peer_and_node_id() {
        let path = tmp_path("change");
        let _ = fs::remove_file(&path);
        let a = load_or_create(&path).expect("first");
        let peer_a = a.peer_id();
        let node_a = a.node_id();
        drop(a);
        fs::remove_file(&path).unwrap();
        let b = load_or_create(&path).expect("second");
        assert_ne!(peer_a, b.peer_id());
        assert_ne!(node_a.raw(), b.node_id().raw());
        let _ = fs::remove_file(&path);
    }

    #[test]
    #[cfg(unix)]
    fn broad_permissions_fail_before_bind() {
        use std::os::unix::fs::PermissionsExt;
        let path = tmp_path("broad");
        let _ = fs::remove_file(&path);
        fs::write(&path, [1u8; 32]).unwrap();
        let mut perms = fs::metadata(&path).unwrap().permissions();
        perms.set_mode(0o644);
        fs::set_permissions(&path, perms).unwrap();
        let err = load_or_create(&path).expect_err("must refuse");
        assert!(
            matches!(err, IdentityError::PermissionsTooBroad { .. }),
            "got {err:?}"
        );
        let _ = fs::remove_file(&path);
    }

    #[test]
    #[cfg(unix)]
    fn stricter_0400_is_accepted() {
        use std::os::unix::fs::PermissionsExt;
        let path = tmp_path("strict");
        let _ = fs::remove_file(&path);
        let mut bytes = [0u8; 32];
        bytes[31] = 1;
        fs::write(&path, bytes).unwrap();
        let mut perms = fs::metadata(&path).unwrap().permissions();
        perms.set_mode(0o400);
        fs::set_permissions(&path, perms).unwrap();
        let id = load_or_create(&path).expect("0400 is owner-read, not broader");
        assert_eq!(id.path(), path.as_path());
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn empty_path_refused() {
        let err = load_or_create("").expect_err("empty");
        assert!(matches!(err, IdentityError::EmptyPath));
    }

    #[test]
    fn parent_dir_component_refused() {
        let err = load_or_create("../secrets/node_key").expect_err("escape");
        assert!(matches!(err, IdentityError::PathEscape { .. }));
    }
}
