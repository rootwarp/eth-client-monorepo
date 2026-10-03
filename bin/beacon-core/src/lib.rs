//! `bin/beacon-core` — thin composer ([ARCH] §4.2 / S2-J-01).
//!
//! One process opens redb **before** any subsystem starts, then starts
//! chain-core + storage-core in-process. One redb handle, one writer.
//! Not JWT- or HTTP-grandfathered. `services/chain` and `services/storage`
//! stay workspace members for A/B (`[ARCH]` §9.1).

pub mod boot;

pub use boot::{BeaconCoreConfig, BootConfig, BootPhase, Booted, BootedNode, boot_in_process, run};
