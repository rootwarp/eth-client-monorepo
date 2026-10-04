//! Storage core: single-writer mailbox, serve, backfill, prune, resume, and boot.
//!
//! S2-B-01: [`writer.rs`](writer.rs) and [`serve.rs`](serve.rs).
//! S2-B-02: [`backfill.rs`](backfill.rs), [`prune/`](prune/mod.rs),
//! [`durable_set.rs`](durable_set.rs), and [`resume.rs`](resume.rs).
//! S2-B-03: remaining companions and the process host ([`boot.rs`](boot.rs))
//! are production items here. `services/storage` is a thin shim
//! (`[ARCH]` §9.1).
//! S2-A-09: the events ring is API/observer only. Write-behind and
//! `CURSOR_TOO_OLD` gap-fill are gone; columns enter via [`ArchiveWrite`].
//!
//! Replay is the one decoder (CC-42); writer/serve/backfill/prune stay
//! opaque-bytes (`[ARCH]` §1.5). Not JWT/HTTP-grandfathered.
//!
//! S2-J-01: [`open`] / [`durable_set`] / [`start_writer`] are the in-process
//! boot surface for `bin/beacon-core`. The 4-container host remains [`run`].
//! S2-J-02: E4 RestoreFromStore client is gone.

#![cfg_attr(test, allow(dead_code, unreachable_pub))]
#![cfg_attr(not(feature = "grpc"), allow(dead_code))]

mod archive_write;
mod backfill;
#[cfg(feature = "grpc")]
mod boot;
mod durable_set;
#[cfg(feature = "grpc")]
mod history;
mod metrics;
mod migrate;
mod node_id;
mod open;
mod prune;
mod replay;
mod resume;
#[cfg(test)]
mod rollback_rehearsal;
#[cfg(feature = "grpc")]
mod serve;
#[cfg(test)]
mod test_tmpdir;
mod writer;

pub use archive_write::{ArchiveWriter, ServedCanonicalBlock};
#[cfg(feature = "grpc")]
pub use boot::{LoadedNodeKey, load_or_create_node_key, run};
pub use durable_set::{DurableBlock, DurableDaStatus};
pub use metrics::StorageMetrics;
pub use node_id::{NodeIdExpectation, NodeIdScheme};
pub use open::{
    CursorSnap, DurableFrontier, DurableSet, OpenOpts, OpenedStore, PendingStore, StorageRuntime,
    durable_frontier, durable_set, open, reopen_durable_frontier, start_writer,
    start_writer_from_store,
};

#[cfg(test)]
mod s2_a_09_tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::path::Path;

    /// S2-A-09: write-behind is gone; writer paths must not invent history
    /// from a lost ring cursor via GetCanonicalRoots.
    #[test]
    fn write_behind_module_gone_and_no_writer_gap_fill() {
        let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        assert!(
            !src.join("write_behind.rs").exists(),
            "crates/storage-core/src/write_behind.rs must be deleted"
        );
        let lib = include_str!("lib.rs");
        let prod_lib = lib.split("mod s2_a_09_tests").next().unwrap();
        assert!(
            !prod_lib.contains("mod write_behind"),
            "lib.rs must not declare the write_behind module"
        );

        let writer_paths = [
            ("boot.rs", include_str!("boot.rs")),
            ("writer.rs", include_str!("writer.rs")),
            ("archive_write.rs", include_str!("archive_write.rs")),
            ("resume.rs", include_str!("resume.rs")),
            ("migrate.rs", include_str!("migrate.rs")),
            ("replay.rs", include_str!("replay.rs")),
        ];
        for (name, src) in writer_paths {
            let prod = src.split("#[cfg(test)]").next().unwrap();
            let mentions_too_old = prod.contains("CURSOR_TOO_OLD");
            let mentions_gap = prod.contains("GetCanonicalRoots")
                || prod.contains("get_canonical_roots")
                || prod.contains("plan_canonical_fallback");
            assert!(
                !(mentions_too_old && mentions_gap),
                "{name}: writer path must not gap-fill via CURSOR_TOO_OLD → GetCanonicalRoots"
            );
        }
    }
}
