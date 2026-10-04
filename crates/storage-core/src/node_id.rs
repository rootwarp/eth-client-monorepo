//! Node-id expectation supplied by the host. The open path does not read a
//! key file unless the caller asks [`NodeIdExpectation::from_configured_path`].

use std::fmt;
use std::path::Path;

use cc_types::Root;

/// What the caller knows about the node key before [`crate::open`].
///
/// [`Present`]'s root is the legacy raw key bytes. The fingerprint is a
/// different value and is not written here.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub enum NodeIdExpectation {
    /// No key configured. `I-node-id` is not armed.
    #[default]
    Unset,
    /// A path was configured and the file is absent.
    ConfiguredButMissing,
    /// Legacy pairing root: the raw 32 key bytes.
    Present(Root),
}

impl NodeIdExpectation {
    /// Legacy file read. Does not create a key and does not check mode.
    ///
    /// Missing or empty path → [`Unset`](Self::Unset). Missing file →
    /// [`ConfiguredButMissing`](Self::ConfiguredButMissing). Exactly 32 bytes
    /// → [`Present`](Self::Present).
    pub fn from_configured_path(path: Option<&Path>) -> Result<Self, String> {
        let Some(path) = path else {
            return Ok(Self::Unset);
        };
        if path.as_os_str().is_empty() {
            return Ok(Self::Unset);
        }
        if !path.exists() {
            return Ok(Self::ConfiguredButMissing);
        }
        let bytes = std::fs::read(path)
            .map_err(|e| format!("node key read failed at {}: {e}", path.display()))?;
        if bytes.len() != 32 {
            return Err(format!(
                "node key at {} has length {}, expected 32",
                path.display(),
                bytes.len()
            ));
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Ok(Self::Present(Root::from_array(arr)))
    }

    /// Legacy root when this is [`Present`](Self::Present).
    #[must_use]
    pub fn legacy_root(self) -> Option<Root> {
        match self {
            Self::Present(root) => Some(root),
            Self::Unset | Self::ConfiguredButMissing => None,
        }
    }
}

impl fmt::Debug for NodeIdExpectation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unset => f.write_str("Unset"),
            Self::ConfiguredButMissing => f.write_str("ConfiguredButMissing"),
            Self::Present(_) => f.write_str("Present(<redacted>)"),
        }
    }
}

/// Scheme of a stored node id. No scheme row means the legacy raw key bytes.
/// The fingerprint scheme is not written by this fold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeIdScheme {
    /// Stored root is the raw 32 key bytes.
    Legacy,
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    fn dir(label: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "node-id-exp-{label}-{nanos}-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn from_configured_path_classifies_legacy_files() {
        assert_eq!(
            NodeIdExpectation::from_configured_path(None).unwrap(),
            NodeIdExpectation::Unset
        );
        assert_eq!(
            NodeIdExpectation::from_configured_path(Some(Path::new(""))).unwrap(),
            NodeIdExpectation::Unset
        );
        assert_eq!(NodeIdExpectation::default(), NodeIdExpectation::Unset);
        assert_eq!(NodeIdExpectation::Unset.legacy_root(), None);

        let root = dir("class");
        let missing = root.join("missing");
        assert_eq!(
            NodeIdExpectation::from_configured_path(Some(&missing)).unwrap(),
            NodeIdExpectation::ConfiguredButMissing
        );
        assert_eq!(
            format!("{:?}", NodeIdExpectation::ConfiguredButMissing),
            "ConfiguredButMissing"
        );

        let short = root.join("short");
        std::fs::write(&short, [1, 2, 3]).unwrap();
        let err = NodeIdExpectation::from_configured_path(Some(&short)).unwrap_err();
        assert!(err.contains("length"), "{err}");

        let key = root.join("node_key");
        let bytes = [0x11u8; 32];
        std::fs::write(&key, bytes).unwrap();
        let present = NodeIdExpectation::from_configured_path(Some(&key)).unwrap();
        assert_eq!(present.legacy_root(), Some(Root::from_array(bytes)));
        assert_eq!(format!("{present:?}"), "Present(<redacted>)");
        assert_eq!(format!("{:?}", NodeIdScheme::Legacy), "Legacy");
        let _ = std::fs::remove_dir_all(&root);
    }
}
