//! Histogram bucket boundaries for the storage metric surface (§10.2).
//!
//! Shared by `services/storage` and `bin/store-bench` so falsifiers and production
//! report against one list (CC-4Ca / Amendment 7). Do not duplicate these arrays.

/// `cc_storage_commit_seconds` — exact boundary at **0.5** (CC-40 falsifier #1).
pub const COMMIT_SECONDS: &[f64] = &[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5];

/// `cc_storage_read_txn_seconds` — exact boundary at **1.0** (CC-46/5; zero above).
pub const READ_TXN_SECONDS: &[f64] = &[0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0];

/// `cc_storage_serve_seconds` — exact boundary at **0.15** (CC-4F/5 p99 budget).
pub const SERVE_SECONDS: &[f64] = &[0.005, 0.01, 0.025, 0.05, 0.1, 0.15, 0.25, 0.5, 1.0];

/// `cc_storage_prune_seconds` — exact boundary at **2.0** (prune deadline §7.4).
pub const PRUNE_SECONDS: &[f64] = &[0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 4.0, 8.0];

/// `cc_storage_restart_seconds` — exact at **5.0** (term (c)) and **60.0** (the bar).
pub const RESTART_SECONDS: &[f64] = &[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 30.0, 45.0, 60.0, 120.0];

/// `cc_storage_snapshot_seconds` — exact boundary at **5.0**.
pub const SNAPSHOT_SECONDS: &[f64] = &[0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 40.0];

/// `cc_storage_write_behind_lag_slots` — exact boundary at **1.0**.
pub const WRITE_BEHIND_LAG_SLOTS: &[f64] = &[0.0, 1.0, 2.0, 3.0, 5.0, 8.0, 16.0, 32.0, 64.0];

/// `cc_storage_serve_admission_wait_seconds` — exact boundary at **2.0** (timeout).
pub const SERVE_ADMISSION_WAIT_SECONDS: &[f64] = &[0.001, 0.01, 0.05, 0.1, 0.5, 1.0, 2.0];

/// `cc_storage_commit_wait_seconds` — `commit_import` wait.
///
/// Not a §10.2 ladder. Boundaries span a sub-second commit through several
/// mainnet slots so a soak can see a 2-slot deadline without a fixed 24 s
/// wait in the caller. The deadline itself is `2 * seconds_per_slot`.
pub const COMMIT_WAIT_SECONDS: &[f64] =
    &[0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0, 12.0, 24.0, 48.0, 120.0];

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    fn assert_strictly_ascending(name: &str, buckets: &[f64]) {
        assert!(!buckets.is_empty(), "{name}: bucket list must be non-empty");
        for w in buckets.windows(2) {
            assert!(
                w[0] < w[1],
                "{name}: not strictly ascending: {} >= {}",
                w[0],
                w[1]
            );
        }
    }

    #[test]
    fn all_bucket_consts_are_strictly_ascending() {
        assert_strictly_ascending("COMMIT_SECONDS", COMMIT_SECONDS);
        assert_strictly_ascending("READ_TXN_SECONDS", READ_TXN_SECONDS);
        assert_strictly_ascending("SERVE_SECONDS", SERVE_SECONDS);
        assert_strictly_ascending("PRUNE_SECONDS", PRUNE_SECONDS);
        assert_strictly_ascending("RESTART_SECONDS", RESTART_SECONDS);
        assert_strictly_ascending("SNAPSHOT_SECONDS", SNAPSHOT_SECONDS);
        assert_strictly_ascending("WRITE_BEHIND_LAG_SLOTS", WRITE_BEHIND_LAG_SLOTS);
        assert_strictly_ascending("SERVE_ADMISSION_WAIT_SECONDS", SERVE_ADMISSION_WAIT_SECONDS);
        assert_strictly_ascending("COMMIT_WAIT_SECONDS", COMMIT_WAIT_SECONDS);
    }

    #[test]
    fn named_clause_boundaries_present() {
        // AC: named boundaries that make each clause a counting question.
        assert!(COMMIT_SECONDS.contains(&0.5), "0.5 ∈ COMMIT_SECONDS");
        assert!(READ_TXN_SECONDS.contains(&1.0), "1.0 ∈ READ_TXN_SECONDS");
        assert!(SERVE_SECONDS.contains(&0.15), "0.15 ∈ SERVE_SECONDS");
        assert!(PRUNE_SECONDS.contains(&2.0), "2.0 ∈ PRUNE_SECONDS");
        assert!(RESTART_SECONDS.contains(&5.0), "5.0 ∈ RESTART_SECONDS");
        assert!(RESTART_SECONDS.contains(&60.0), "60.0 ∈ RESTART_SECONDS");
        assert!(SNAPSHOT_SECONDS.contains(&5.0), "5.0 ∈ SNAPSHOT_SECONDS");
        assert!(
            WRITE_BEHIND_LAG_SLOTS.contains(&1.0),
            "1.0 ∈ WRITE_BEHIND_LAG_SLOTS"
        );
        assert!(
            SERVE_ADMISSION_WAIT_SECONDS.contains(&2.0),
            "2.0 ∈ SERVE_ADMISSION_WAIT_SECONDS"
        );
    }

    #[test]
    fn ladders_match_section_10_2_verbatim() {
        assert_eq!(
            COMMIT_SECONDS,
            &[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5]
        );
        assert_eq!(
            READ_TXN_SECONDS,
            &[0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0]
        );
        assert_eq!(
            SERVE_SECONDS,
            &[0.005, 0.01, 0.025, 0.05, 0.1, 0.15, 0.25, 0.5, 1.0]
        );
        assert_eq!(PRUNE_SECONDS, &[0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 4.0, 8.0]);
        assert_eq!(
            RESTART_SECONDS,
            &[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 30.0, 45.0, 60.0, 120.0]
        );
        assert_eq!(
            SNAPSHOT_SECONDS,
            &[0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 40.0]
        );
        assert_eq!(
            WRITE_BEHIND_LAG_SLOTS,
            &[0.0, 1.0, 2.0, 3.0, 5.0, 8.0, 16.0, 32.0, 64.0]
        );
        assert_eq!(
            SERVE_ADMISSION_WAIT_SECONDS,
            &[0.001, 0.01, 0.05, 0.1, 0.5, 1.0, 2.0]
        );
    }
}
