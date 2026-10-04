//! S2-A-14: proto-free import surface.
//!
//! The durable-row discharge enters through `bin/beacon-core`'s `boot()`
//! and [`cc_seam::ArchiveWrite::commit_import`]. This crate stays free of
//! `tonic` and `cc-proto`.

#![allow(missing_docs)]
