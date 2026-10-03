//! Request/response SSZ bodies and protocol IDs for the five probe protocols.
//!
//! Independent of `services/p2p` — field layouts match the eth2 p2p-interface.

use std::io;

use cc_types::{Epoch, ForkDigest, Root, Slot};

use crate::codec::SszLimits;

/// Fixed SSZ length of `Status v2` (4+32+8+32+8+8).
pub const STATUS_V2_SSZ_LEN: usize = 92;

/// SSZ length of `BeaconBlocksByRange` request: three `uint64`
/// (`start_slot`, `count`, `step`). `step` is deprecated but required.
pub const BY_RANGE_BLOCKS_SSZ_LEN: usize = 24;

/// Fixed prefix of a column by-range SSZ container before the columns list body.
pub const COLUMNS_BY_RANGE_FIXED_PREFIX: usize = 20;

/// The five protocols this probe speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Protocol {
    /// `/eth2/beacon_chain/req/status/2/`
    StatusV2,
    /// `/eth2/beacon_chain/req/beacon_blocks_by_range/2/`
    BeaconBlocksByRangeV2,
    /// `/eth2/beacon_chain/req/beacon_blocks_by_root/2/`
    BeaconBlocksByRootV2,
    /// `/eth2/beacon_chain/req/data_column_sidecars_by_range/1/`
    DataColumnSidecarsByRangeV1,
    /// `/eth2/beacon_chain/req/data_column_sidecars_by_root/1/`
    DataColumnSidecarsByRootV1,
}

impl Protocol {
    /// Full libp2p protocol ID including the `ssz_snappy` encoding suffix.
    #[must_use]
    pub const fn protocol_id(self) -> &'static str {
        match self {
            Self::StatusV2 => "/eth2/beacon_chain/req/status/2/ssz_snappy",
            Self::BeaconBlocksByRangeV2 => {
                "/eth2/beacon_chain/req/beacon_blocks_by_range/2/ssz_snappy"
            }
            Self::BeaconBlocksByRootV2 => {
                "/eth2/beacon_chain/req/beacon_blocks_by_root/2/ssz_snappy"
            }
            Self::DataColumnSidecarsByRangeV1 => {
                "/eth2/beacon_chain/req/data_column_sidecars_by_range/1/ssz_snappy"
            }
            Self::DataColumnSidecarsByRootV1 => {
                "/eth2/beacon_chain/req/data_column_sidecars_by_root/1/ssz_snappy"
            }
        }
    }

    /// Whether successful response chunks carry a 4-byte `ForkDigest` context.
    #[must_use]
    pub const fn has_context_bytes(self) -> bool {
        matches!(
            self,
            Self::BeaconBlocksByRangeV2
                | Self::BeaconBlocksByRootV2
                | Self::DataColumnSidecarsByRangeV1
                | Self::DataColumnSidecarsByRootV1
        )
    }

    /// Protocol-declared SSZ size bounds for requests.
    ///
    /// Mirrored in `cc_p2p::reqresp::Protocol::request_limits` and
    /// `cc_libp2p::request_limits` (S0-B-06). Values are copied, not shared —
    /// the encoder below stays independent (S0-B-07 / ADR-P4-12).
    #[must_use]
    pub const fn request_limits(self) -> SszLimits {
        match self {
            Self::StatusV2 => SszLimits { min: 92, max: 92 },
            // (start_slot, count, step) — three uint64s.
            Self::BeaconBlocksByRangeV2 => SszLimits { min: 24, max: 24 },
            // `BeaconBlockRoots` = `List[Root, MAX_REQUEST_BLOCKS]`. Roots are
            // fixed-size, so the body is `32 * n` with no offset table.
            Self::BeaconBlocksByRootV2 => SszLimits {
                min: 0,
                max: 1024 * 32,
            },
            // (start_slot, count, columns offset) + up to 128×u64 column indices.
            Self::DataColumnSidecarsByRangeV1 => SszLimits {
                min: 20,
                max: 20 + 128 * 8,
            },
            // List[DataColumnsByRootIdentifier, 1024] framing max; semantic bound is 128.
            Self::DataColumnSidecarsByRootV1 => SszLimits {
                min: 0,
                max: crate::codec::MAX_PAYLOAD_SIZE,
            },
        }
    }

    /// Protocol-declared SSZ size bounds for a single success response chunk.
    #[must_use]
    pub const fn response_limits(self) -> SszLimits {
        match self {
            Self::StatusV2 => SszLimits { min: 92, max: 92 },
            Self::BeaconBlocksByRangeV2
            | Self::BeaconBlocksByRootV2
            | Self::DataColumnSidecarsByRangeV1
            | Self::DataColumnSidecarsByRootV1 => SszLimits {
                min: 0,
                max: crate::codec::MAX_PAYLOAD_SIZE,
            },
        }
    }

    /// All five probe protocols.
    pub const ALL: [Self; 5] = [
        Self::StatusV2,
        Self::BeaconBlocksByRangeV2,
        Self::BeaconBlocksByRootV2,
        Self::DataColumnSidecarsByRangeV1,
        Self::DataColumnSidecarsByRootV1,
    ];
}

/// Ethereum `Status v2` (Fulu) request **and** response body.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StatusV2 {
    /// Current fork digest.
    pub fork_digest: ForkDigest,
    /// Finalized checkpoint root.
    pub finalized_root: Root,
    /// Finalized checkpoint epoch.
    pub finalized_epoch: Epoch,
    /// Head block root.
    pub head_root: Root,
    /// Head slot.
    pub head_slot: Slot,
    /// Earliest slot this node can honestly serve.
    pub earliest_available_slot: Slot,
}

impl StatusV2 {
    /// SSZ-encode the six fields (fixed 92 bytes, little-endian integers).
    #[must_use]
    pub fn to_ssz_bytes(self) -> [u8; STATUS_V2_SSZ_LEN] {
        let mut out = [0u8; STATUS_V2_SSZ_LEN];
        out[0..4].copy_from_slice(self.fork_digest.as_slice());
        out[4..36].copy_from_slice(self.finalized_root.as_slice());
        out[36..44].copy_from_slice(&self.finalized_epoch.as_u64().to_le_bytes());
        out[44..76].copy_from_slice(self.head_root.as_slice());
        out[76..84].copy_from_slice(&self.head_slot.as_u64().to_le_bytes());
        out[84..92].copy_from_slice(&self.earliest_available_slot.as_u64().to_le_bytes());
        out
    }

    /// SSZ-decode; rejects anything but exactly 92 bytes.
    pub fn from_ssz_bytes(bytes: &[u8]) -> Result<Self, io::Error> {
        if bytes.len() != STATUS_V2_SSZ_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Status v2 SSZ length {} != {STATUS_V2_SSZ_LEN} (must be six fields)",
                    bytes.len()
                ),
            ));
        }
        let mut fd = [0u8; 4];
        fd.copy_from_slice(&bytes[0..4]);
        let mut fr = [0u8; 32];
        fr.copy_from_slice(&bytes[4..36]);
        let mut hr = [0u8; 32];
        hr.copy_from_slice(&bytes[44..76]);
        Ok(Self {
            fork_digest: ForkDigest::from_array(fd),
            finalized_root: Root::from_array(fr),
            finalized_epoch: Epoch::new(u64::from_le_bytes(
                bytes[36..44]
                    .try_into()
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "finalized_epoch"))?,
            )),
            head_root: Root::from_array(hr),
            head_slot: Slot::new(u64::from_le_bytes(
                bytes[76..84]
                    .try_into()
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "head_slot"))?,
            )),
            earliest_available_slot: Slot::new(u64::from_le_bytes(
                bytes[84..92].try_into().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "earliest_available_slot")
                })?,
            )),
        })
    }

    /// Build a probe-side Status with the CLI fork digest and zero chain fields.
    #[must_use]
    pub fn for_probe(fork_digest: ForkDigest) -> Self {
        Self {
            fork_digest,
            finalized_root: Root::ZERO,
            finalized_epoch: Epoch::ZERO,
            head_root: Root::ZERO,
            head_slot: Slot::ZERO,
            earliest_available_slot: Slot::ZERO,
        }
    }
}

/// `BeaconBlocksByRange v2` request body.
///
/// `step` is not a field: the encoder always writes 1 and the decoder
/// rejects any other value (phase0 p2p-interface: deprecated, must be 1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlocksByRangeRequest {
    /// First slot (inclusive).
    pub start_slot: Slot,
    /// Number of slots to cover.
    pub count: u64,
}

impl BlocksByRangeRequest {
    /// Spec-deprecated `step`. The p2p-interface requires this value.
    const STEP: u64 = 1;

    /// SSZ-encode `(start_slot, count, step = 1)`.
    #[must_use]
    pub fn to_ssz_bytes(self) -> [u8; BY_RANGE_BLOCKS_SSZ_LEN] {
        let mut out = [0u8; BY_RANGE_BLOCKS_SSZ_LEN];
        out[0..8].copy_from_slice(&self.start_slot.as_u64().to_le_bytes());
        out[8..16].copy_from_slice(&self.count.to_le_bytes());
        out[16..24].copy_from_slice(&Self::STEP.to_le_bytes());
        out
    }

    /// SSZ-decode; length must be exactly 24 and `step` must be 1.
    pub fn from_ssz_bytes(bytes: &[u8]) -> Result<Self, io::Error> {
        if bytes.len() != BY_RANGE_BLOCKS_SSZ_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "by_range SSZ length {} != {BY_RANGE_BLOCKS_SSZ_LEN}",
                    bytes.len()
                ),
            ));
        }
        let start = u64::from_le_bytes(
            bytes[0..8]
                .try_into()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "start_slot"))?,
        );
        let count = u64::from_le_bytes(
            bytes[8..16]
                .try_into()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "count"))?,
        );
        let step = u64::from_le_bytes(
            bytes[16..24]
                .try_into()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "step"))?,
        );
        if step != Self::STEP {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("by_range step {step} != {}", Self::STEP),
            ));
        }
        Ok(Self {
            start_slot: Slot::new(start),
            count,
        })
    }
}

/// `BeaconBlocksByRoot v2` request body: ordered list of roots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlocksByRootRequest {
    /// Roots in request order.
    pub roots: Vec<Root>,
}

impl BlocksByRootRequest {
    /// SSZ-encode `BeaconBlockRoots` (`List[Root, MAX_REQUEST_BLOCKS]`).
    ///
    /// consensus-specs SSZ: for a list of fixed-size elements, `serialize` is
    /// the concatenation of `serialize(element)`. `Root` is `Bytes32`, so the
    /// body is exactly `32 * n` bytes. Offset tables apply only when an
    /// *element* is variable-size; a top-level list is not wrapped in one.
    #[must_use]
    pub fn to_ssz_bytes(&self) -> Vec<u8> {
        let mut out = vec![0u8; self.roots.len() * 32];
        for (i, root) in self.roots.iter().enumerate() {
            let off = i * 32;
            out[off..off + 32].copy_from_slice(root.as_slice());
        }
        out
    }

    /// SSZ-decode `BeaconBlockRoots`. Length must be a multiple of 32
    /// (including the empty list: 0 bytes).
    pub fn from_ssz_bytes(bytes: &[u8]) -> Result<Self, io::Error> {
        if !bytes.len().is_multiple_of(32) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "BeaconBlockRoots SSZ length {} is not a multiple of 32",
                    bytes.len()
                ),
            ));
        }
        let n = bytes.len() / 32;
        if n > 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "BeaconBlockRoots exceeds List limit 1024",
            ));
        }
        let mut roots = Vec::with_capacity(n);
        for chunk in bytes.chunks_exact(32) {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(chunk);
            roots.push(Root::from_array(arr));
        }
        Ok(Self { roots })
    }
}

/// `DataColumnSidecarsByRange v1` request body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnsByRangeRequest {
    /// First slot (inclusive).
    pub start_slot: Slot,
    /// Number of slots to cover.
    pub count: u64,
    /// Requested column indices.
    pub columns: Vec<u64>,
}

impl ColumnsByRangeRequest {
    /// SSZ-encode `(start_slot, count, columns)` as a container.
    #[must_use]
    pub fn to_ssz_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(COLUMNS_BY_RANGE_FIXED_PREFIX + self.columns.len() * 8);
        out.extend_from_slice(&self.start_slot.as_u64().to_le_bytes());
        out.extend_from_slice(&self.count.to_le_bytes());
        out.extend_from_slice(&(COLUMNS_BY_RANGE_FIXED_PREFIX as u32).to_le_bytes());
        for c in &self.columns {
            out.extend_from_slice(&c.to_le_bytes());
        }
        out
    }

    /// SSZ-decode.
    pub fn from_ssz_bytes(bytes: &[u8]) -> Result<Self, io::Error> {
        if bytes.len() < COLUMNS_BY_RANGE_FIXED_PREFIX {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "by_range column SSZ length {} < {COLUMNS_BY_RANGE_FIXED_PREFIX}",
                    bytes.len()
                ),
            ));
        }
        let start = u64::from_le_bytes(
            bytes[0..8]
                .try_into()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "start_slot"))?,
        );
        let count = u64::from_le_bytes(
            bytes[8..16]
                .try_into()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "count"))?,
        );
        let offset = u32::from_le_bytes(
            bytes[16..20]
                .try_into()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "columns offset"))?,
        ) as usize;
        if offset != COLUMNS_BY_RANGE_FIXED_PREFIX {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("by_range unexpected columns offset {offset}"),
            ));
        }
        let rest = &bytes[COLUMNS_BY_RANGE_FIXED_PREFIX..];
        if !rest.len().is_multiple_of(8) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "by_range columns not a multiple of 8",
            ));
        }
        let n = rest.len() / 8;
        if n > 128 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "by_range columns list exceeds 128",
            ));
        }
        let mut columns = Vec::with_capacity(n);
        for i in 0..n {
            let c = u64::from_le_bytes(
                rest[i * 8..(i + 1) * 8]
                    .try_into()
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "column index"))?,
            );
            columns.push(c);
        }
        Ok(Self {
            start_slot: Slot::new(start),
            count,
            columns,
        })
    }
}

/// `DataColumnSidecarsByRoot v1` request body: ordered list of identifiers.
///
/// Each identifier is encoded as `Root (32) + offset (4) + column indices (8 each)`.
/// For the probe we only need encode of a single-identifier list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnsByRootRequest {
    /// `(block_root, column_indices)` pairs.
    pub identifiers: Vec<(Root, Vec<u64>)>,
}

impl ColumnsByRootRequest {
    /// SSZ-encode `DataColumnsByRootIdentifiers`
    /// (`List[DataColumnsByRootIdentifier, N]`).
    ///
    /// Each identifier is a variable-size container (`Root` + `List[uint64]`),
    /// so the outer list is `[offset_0 … offset_{n-1} ‖ body_0 … body_{n-1}]`
    /// with `offset_0 = 4*n`. An empty list serializes to the empty byte
    /// string — not a lone 4-byte offset.
    #[must_use]
    pub fn to_ssz_bytes(&self) -> Vec<u8> {
        let n = self.identifiers.len();
        if n == 0 {
            return Vec::new();
        }

        let mut elements = Vec::with_capacity(n);
        for (root, cols) in &self.identifiers {
            // Container: root(32) + offset-to-columns(4) + uint64 indices.
            let mut el = Vec::with_capacity(36 + cols.len() * 8);
            el.extend_from_slice(root.as_slice());
            el.extend_from_slice(&36u32.to_le_bytes());
            for c in cols {
                el.extend_from_slice(&c.to_le_bytes());
            }
            elements.push(el);
        }

        let mut bodies = Vec::new();
        let mut offsets = Vec::with_capacity(n);
        let mut cursor = (4 * n) as u32;
        for el in &elements {
            offsets.push(cursor);
            cursor = cursor.saturating_add(el.len() as u32);
            bodies.extend_from_slice(el);
        }
        let mut out = Vec::with_capacity(4 * n + bodies.len());
        for o in offsets {
            out.extend_from_slice(&o.to_le_bytes());
        }
        out.extend_from_slice(&bodies);
        out
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn status_ssz_roundtrip() {
        let s = StatusV2 {
            fork_digest: ForkDigest::from_array([1, 2, 3, 4]),
            finalized_root: Root::from_array([0x11; 32]),
            finalized_epoch: Epoch::new(10),
            head_root: Root::from_array([0x22; 32]),
            head_slot: Slot::new(320),
            earliest_available_slot: Slot::new(300),
        };
        let bytes = s.to_ssz_bytes();
        assert_eq!(bytes.len(), 92);
        assert_eq!(StatusV2::from_ssz_bytes(&bytes).unwrap(), s);
    }

    #[test]
    fn blocks_by_range_ssz_roundtrip() {
        let r = BlocksByRangeRequest {
            start_slot: Slot::new(99),
            count: 1,
        };
        let bytes = r.to_ssz_bytes();
        assert_eq!(bytes.len(), 24);
        assert_eq!(&bytes[16..24], &1u64.to_le_bytes());
        assert_eq!(BlocksByRangeRequest::from_ssz_bytes(&bytes).unwrap(), r);
    }

    /// consensus-specs `v1.7.0-alpha.13` `specs/phase0/p2p-interface.md`
    /// defines the `BeaconBlocksByRange` request (unchanged in Altair; Fulu
    /// does not touch it) as the fixed tuple
    /// `(start_slot: Slot, count: uint64, step: uint64)` with step
    /// "Deprecated, must be set to 1".
    ///
    /// A fixed-size container serializes as the concatenation of its fields:
    /// three little-endian `uint64`s, no offset table. The definition sits in
    /// a bare fence, not `class BeaconBlocksByRangeRequest(Container)`, so
    /// `consensus-spec-tests` ships no `ssz_static/BeaconBlocksByRangeRequest`.
    /// Bytes below are hand-pinned from that derivation. Agreement of copied
    /// `request_limits` tables (min = max = 24) is not this check.
    ///
    /// `start_slot = 1, count = 2, step = 1` →
    /// `01 00 00 00 00 00 00 00 | 02 00 00 00 00 00 00 00 | 01 00 00 00 00 00 00 00`
    #[test]
    fn blocks_by_range_matches_hand_pinned_bytes() {
        const FIXTURE: [u8; 24] = [
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // start_slot = 1
            0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // count = 2
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // step = 1
        ];
        let req = BlocksByRangeRequest {
            start_slot: Slot::new(1),
            count: 2,
        };
        assert_eq!(BY_RANGE_BLOCKS_SSZ_LEN, FIXTURE.len());
        assert_eq!(req.to_ssz_bytes(), FIXTURE);
        assert_eq!(BlocksByRangeRequest::from_ssz_bytes(&FIXTURE).unwrap(), req);
    }

    /// Spec: `step` is deprecated and must be 1. A 16-byte `(start_slot, count)`
    /// body is the pre-fix shape and is not the v2 request.
    #[test]
    fn blocks_by_range_rejects_step_not_one() {
        let bad_step: [u8; 24] = [
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // start_slot = 1
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // count = 1
            0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // step = 2
        ];
        let err = BlocksByRangeRequest::from_ssz_bytes(&bad_step).expect_err("step != 1");
        let msg = err.to_string();
        assert!(msg.contains("step"), "expected step rejection, got {msg}");

        let zero_step: [u8; 24] = [
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // start_slot = 1
            0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // count = 1
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // step = 0
        ];
        let err0 = BlocksByRangeRequest::from_ssz_bytes(&zero_step).expect_err("step == 0");
        assert!(
            err0.to_string().contains("step"),
            "expected step rejection, got {err0}"
        );

        let err16 = BlocksByRangeRequest::from_ssz_bytes(&[0u8; 16]).expect_err("16-byte body");
        assert!(
            err16.to_string().contains("16"),
            "expected length rejection, got {err16}"
        );
    }

    #[test]
    fn columns_by_range_ssz_roundtrip() {
        let r = ColumnsByRangeRequest {
            start_slot: Slot::new(50),
            count: 1,
            columns: vec![0, 1, 2, 3],
        };
        let bytes = r.to_ssz_bytes();
        assert_eq!(ColumnsByRangeRequest::from_ssz_bytes(&bytes).unwrap(), r);
    }

    #[test]
    fn protocol_ids_end_with_ssz_snappy() {
        for p in Protocol::ALL {
            assert!(
                p.protocol_id().ends_with("/ssz_snappy"),
                "{}",
                p.protocol_id()
            );
        }
    }

    /// S0-B-06: probe request-limit table equals the live libp2p codec table
    /// on the five overlapping protocol IDs.
    #[test]
    fn request_limits_match_libp2p_codec() {
        const GLOBAL: usize = cc_libp2p::REQRESP_MAX_PAYLOAD_SIZE;
        for p in Protocol::ALL {
            let proto = p.request_limits();
            let codec = cc_libp2p::request_limits(p.protocol_id(), GLOBAL);
            assert_eq!(
                (proto.min, proto.max),
                codec,
                "{}: probe Protocol::request_limits != cc_libp2p::request_limits",
                p.protocol_id()
            );
        }
    }

    /// consensus-specs: request type is `BeaconBlockRoots` =
    /// `List[Root, MAX_REQUEST_BLOCKS]`. Fixed-size elements → packed roots.
    /// Bytes are assembled here by hand so this crate is checked against the
    /// schema, not against `services/p2p`.
    #[test]
    fn blocks_by_root_matches_handwritten_three_root_fixture() {
        let r0 = Root::from_array({
            let mut a = [0u8; 32];
            a[0] = 0xaa;
            a
        });
        let r1 = Root::from_array({
            let mut a = [0u8; 32];
            a[0] = 0xbb;
            a
        });
        let r2 = Root::from_array({
            let mut a = [0u8; 32];
            a[0] = 0xcc;
            a
        });
        let mut fixture = [0u8; 96];
        fixture[0..32].copy_from_slice(r0.as_slice());
        fixture[32..64].copy_from_slice(r1.as_slice());
        fixture[64..96].copy_from_slice(r2.as_slice());

        let req = BlocksByRootRequest {
            roots: vec![r0, r1, r2],
        };
        assert_eq!(req.to_ssz_bytes(), fixture);
        assert_eq!(BlocksByRootRequest::from_ssz_bytes(&fixture).unwrap(), req);
    }

    #[test]
    fn blocks_by_root_empty_list_is_zero_bytes() {
        let req = BlocksByRootRequest { roots: Vec::new() };
        assert!(req.to_ssz_bytes().is_empty());
        assert_eq!(BlocksByRootRequest::from_ssz_bytes(&[]).unwrap(), req);
    }

    #[test]
    fn blocks_by_root_rejects_length_not_multiple_of_32() {
        assert!(BlocksByRootRequest::from_ssz_bytes(&[0u8; 1]).is_err());
        assert!(BlocksByRootRequest::from_ssz_bytes(&[0u8; 4]).is_err());
        assert!(BlocksByRootRequest::from_ssz_bytes(&[0u8; 31]).is_err());
        assert!(BlocksByRootRequest::from_ssz_bytes(&[0u8; 33]).is_err());
        // Pre-fix shape: 4-byte offset + one root = 36, not 32·n.
        let mut prefixed = vec![4, 0, 0, 0];
        prefixed.extend_from_slice(&[0u8; 32]);
        assert!(BlocksByRootRequest::from_ssz_bytes(&prefixed).is_err());
    }

    #[test]
    fn blocks_by_root_request_limits_are_bare_32n() {
        let lim = Protocol::BeaconBlocksByRootV2.request_limits();
        assert_eq!(lim.min, 0);
        assert_eq!(lim.max, 1024 * 32);
    }

    #[test]
    fn columns_by_root_empty_list_is_zero_bytes() {
        let req = ColumnsByRootRequest {
            identifiers: Vec::new(),
        };
        assert!(req.to_ssz_bytes().is_empty());
    }

    #[test]
    fn columns_by_root_one_identifier_matches_ssz_list_layout() {
        let root = Root::from_array([0xab; 32]);
        let req = ColumnsByRootRequest {
            identifiers: vec![(root, vec![7])],
        };
        let mut expected = Vec::new();
        expected.extend_from_slice(&4u32.to_le_bytes());
        expected.extend_from_slice(root.as_slice());
        expected.extend_from_slice(&36u32.to_le_bytes());
        expected.extend_from_slice(&7u64.to_le_bytes());
        assert_eq!(req.to_ssz_bytes(), expected);
    }
}
