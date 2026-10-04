//! SIGTERM reaches the writer drain, and `serve` awaits that drain on exit.
//!
//! This binary is the only test in the process. `kill -TERM` is delivered
//! to this process after the listener accepts. Nextest and a filtered
//! `cargo test --test composer_serve` keep that signal off sibling binaries.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

#[path = "support/anchor_fixture.rs"]
#[allow(dead_code)]
mod anchor_fixture;

use std::net::SocketAddr;
use std::process::Command;
use std::time::Duration;

use anchor_fixture::{beacon_config, spawn_el_stub};
use cc_beacon_core::boot::{boot, serve_exit_drained, serve_pre_drain_drained};

struct TempDir(std::path::PathBuf);

impl TempDir {
    fn new(prefix: &str) -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let path = std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()));
        std::fs::create_dir_all(&path).expect("temp dir");
        Self(path)
    }

    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn free_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    listener.local_addr().expect("addr")
}

async fn wait_tcp(addr: SocketAddr) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("listener did not accept on {addr}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sigterm_drains_before_serve_returns() {
    let el = spawn_el_stub();
    let dir = TempDir::new("composer-serve");
    let grpc = free_addr();
    let metrics = free_addr();
    let mut cfg = beacon_config(dir.path(), el.endpoint().to_owned());
    cfg.service.grpc_addr = grpc;
    cfg.service.metrics_addr = metrics;
    cfg.checkpoint_providers.clear();
    cfg.checkpoint_provider = None;

    let node = boot(cfg).await.expect("empty boot");
    let serve = tokio::spawn(async move { node.serve().await });
    wait_tcp(grpc).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let status = Command::new("kill")
        .args(["-TERM", &std::process::id().to_string()])
        .status()
        .expect("kill");
    assert!(status.success(), "SIGTERM was not delivered");

    let result = tokio::time::timeout(Duration::from_secs(10), serve)
        .await
        .expect("serve did not return after SIGTERM")
        .expect("serve task");
    assert!(result.is_ok(), "serve must return Ok: {result:?}");
    assert!(
        serve_pre_drain_drained(),
        "SIGTERM pre-drain must drain the mailbox"
    );
    assert!(
        serve_exit_drained(),
        "serve's exit path must await the drain"
    );
}
