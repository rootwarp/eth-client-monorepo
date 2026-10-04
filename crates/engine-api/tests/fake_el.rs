//! `fake-el` speaks `newPayload` / `forkchoiceUpdated` with the configured verdict.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

fn fake_el_exe() -> PathBuf {
    let path = option_env!("CARGO_BIN_EXE_fake_el")
        .or(option_env!("CARGO_BIN_EXE_fake-el"))
        .unwrap_or_else(|| {
            panic!(
                "neither CARGO_BIN_EXE_fake_el nor CARGO_BIN_EXE_fake-el is set; \
                 run `cargo test -p cc-engine-api --test fake_el`"
            )
        });
    PathBuf::from(path)
}

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or(0);
        let path = std::env::temp_dir().join(format!("fake-el-{}-{nanos}", std::process::id()));
        std::fs::create_dir_all(&path).expect("temp dir");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Fake {
    child: Child,
    addr: String,
    events: PathBuf,
    stderr: PathBuf,
    dir: TempDir,
}

impl Fake {
    fn spawn(extra: &[&str]) -> Self {
        let dir = TempDir::new();
        let addr_file = dir.path().join("addr");
        let events = dir.path().join("events");
        let stderr_path = dir.path().join("stderr");
        let stderr = std::fs::File::create(&stderr_path).expect("stderr");
        let mut args = vec![
            "--listen".to_owned(),
            "127.0.0.1:0".to_owned(),
            "--addr-file".to_owned(),
            addr_file.display().to_string(),
            "--events".to_owned(),
            events.display().to_string(),
        ];
        args.extend(extra.iter().map(|arg| (*arg).to_owned()));
        let child = Command::new(fake_el_exe())
            .args(&args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(stderr)
            .spawn()
            .expect("spawn fake-el");
        let mut fake = Self {
            child,
            addr: String::new(),
            events,
            stderr: stderr_path,
            dir,
        };
        fake.addr = wait_addr(&mut fake).expect("addr");
        fake
    }

    fn stderr_text(&self) -> String {
        std::fs::read_to_string(&self.stderr).unwrap_or_default()
    }
}

impl Drop for Fake {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn wait_addr(fake: &mut Fake) -> Result<String, String> {
    let path = fake.dir.path().join("addr");
    let start = Instant::now();
    loop {
        if let Some(status) = fake.child.try_wait().expect("try_wait") {
            return Err(format!(
                "fake-el exited {status} before the addr file.\n{}",
                fake.stderr_text()
            ));
        }
        if let Ok(text) = std::fs::read_to_string(&path) {
            let line = text.lines().next().unwrap_or("").trim();
            if line.starts_with("http://") && line.len() > "http://".len() {
                return Ok(line.to_owned());
            }
        }
        if start.elapsed() > Duration::from_secs(5) {
            return Err(format!("addr file missing.\n{}", fake.stderr_text()));
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn post(addr: &str, body: &str) -> String {
    let host = addr.strip_prefix("http://").expect("http addr").to_owned();
    let mut stream = TcpStream::connect(&host).expect("connect");
    // macOS reports `SO_RCVTIMEO` as WouldBlock. Keep-alive has no EOF, so
    // read one Content-Length body and retry until it is complete.
    stream
        .set_read_timeout(Some(Duration::from_millis(200)))
        .expect("read timeout");
    let request = format!(
        "POST / HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).expect("write");
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut buf = Vec::new();
    let mut tmp = [0_u8; 4096];
    while !response_complete(&buf) {
        assert!(
            Instant::now() < deadline,
            "response timeout, got {}",
            String::from_utf8_lossy(&buf)
        );
        match stream.read(&mut tmp) {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
            Err(err)
                if err.kind() == std::io::ErrorKind::WouldBlock
                    || err.kind() == std::io::ErrorKind::TimedOut =>
            {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(err) => panic!("read: {err}"),
        }
    }
    String::from_utf8_lossy(&buf).into_owned()
}

fn response_complete(buf: &[u8]) -> bool {
    let Some(header_end) = buf.windows(4).position(|window| window == b"\r\n\r\n") else {
        return false;
    };
    let header = String::from_utf8_lossy(&buf[..header_end]);
    let mut len: Option<usize> = None;
    for line in header.lines() {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("content-length") {
            len = value.trim().parse().ok();
        }
    }
    let Some(len) = len else {
        return false;
    };
    buf.len() >= header_end + 4 + len
}

#[test]
fn verdicts_are_configurable() {
    let fake = Fake::spawn(&["--new-payload", "INVALID", "--forkchoice", "SYNCING"]);
    let new_payload = post(
        &fake.addr,
        r#"{"jsonrpc":"2.0","id":7,"method":"engine_newPayloadV4","params":[]}"#,
    );
    assert!(
        new_payload.contains(r#""id":7"#) && new_payload.contains(r#""status":"INVALID""#),
        "{new_payload}"
    );
    let forkchoice = post(
        &fake.addr,
        r#"{"jsonrpc":"2.0","id":1,"method":"engine_forkchoiceUpdatedV3","params":[]}"#,
    );
    assert!(
        forkchoice.contains(r#""status":"SYNCING""#) && forkchoice.contains("payloadStatus"),
        "{forkchoice}"
    );
    let syncing = post(
        &fake.addr,
        r#"{"jsonrpc":"2.0","id":1,"method":"eth_syncing","params":[]}"#,
    );
    assert!(syncing.contains("false"), "{syncing}");
    let caps = post(
        &fake.addr,
        r#"{"jsonrpc":"2.0","id":1,"method":"engine_exchangeCapabilities","params":[]}"#,
    );
    assert!(caps.contains("engine_newPayloadV4"), "{caps}");
}

#[test]
fn hold_new_payload_until_release_file() {
    let dir = TempDir::new();
    let release = dir.path().join("release");
    let fake = Fake::spawn(&[
        "--new-payload",
        "VALID",
        "--hold-new-payload",
        "--release-file",
        &release.display().to_string(),
    ]);
    let (tx, rx) = mpsc::channel();
    let addr = fake.addr.clone();
    std::thread::spawn(move || {
        let response = post(
            &addr,
            r#"{"jsonrpc":"2.0","id":3,"method":"engine_newPayloadV4","params":[]}"#,
        );
        let _ = tx.send(response);
    });

    let start = Instant::now();
    loop {
        let events = std::fs::read_to_string(&fake.events).unwrap_or_default();
        if events.contains("newPayload") && events.contains("hold") {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "no hold line in {events:?}\n{}",
            fake.stderr_text()
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    std::thread::sleep(Duration::from_millis(150));
    assert!(
        rx.try_recv().is_err(),
        "newPayload answered while the release file was absent"
    );
    std::fs::write(&release, b"go").expect("release");
    let response = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("response after release");
    assert!(response.contains(r#""status":"VALID""#), "{response}");
}

#[test]
fn rejected_forkchoice_status_exits_before_serving() {
    let child = Command::new(fake_el_exe())
        .args(["--forkchoice", "ACCEPTED"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn");
    let mut child = child;
    let start = Instant::now();
    loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            assert!(!status.success(), "{status}");
            return;
        }
        if start.elapsed() > Duration::from_secs(2) {
            let _ = child.kill();
            let _ = child.wait();
            panic!("fake-el kept listening after --forkchoice ACCEPTED");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}
