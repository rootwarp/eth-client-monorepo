//! Loopback Engine API stand-in for process-level tests.
//!
//! JWT is not checked. A 401 would trip `AuthFailed` and the consensus client
//! would never put `newPayload` on the wire. `forkchoiceUpdated` is never held:
//! the upcheck and on-tick fcU must complete so a held `newPayload` is the
//! mid-import point, not a stuck health probe.

use std::fs::File;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::Duration;

const BODY_CAP: usize = 8 * 1024 * 1024;
const NEW_PAYLOAD_STATUSES: &[&str] = &[
    "VALID",
    "INVALID",
    "SYNCING",
    "ACCEPTED",
    "INVALID_BLOCK_HASH",
];
const FORKCHOICE_STATUSES: &[&str] = &["VALID", "INVALID", "SYNCING"];

fn main() {
    if let Err(err) = run() {
        eprintln!("fake-el: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let args = Args::parse()?;
    let listener =
        TcpListener::bind(&args.listen).map_err(|err| format!("bind {}: {err}", args.listen))?;
    let local = listener
        .local_addr()
        .map_err(|err| format!("local addr: {err}"))?;
    if let Some(path) = &args.addr_file {
        write_addr_file(path, local)?;
    }
    let shared = Arc::new(Shared::new(args)?);
    for conn in listener.incoming() {
        match conn {
            Ok(stream) => {
                let shared = Arc::clone(&shared);
                thread::spawn(move || serve_conn(stream, shared));
            }
            Err(err) => eprintln!("fake-el: accept: {err}"),
        }
    }
    Ok(())
}

struct Args {
    listen: String,
    addr_file: Option<PathBuf>,
    events: Option<PathBuf>,
    new_payload: String,
    forkchoice: String,
    hold_new_payload: bool,
    hold_after: u64,
    release_file: Option<PathBuf>,
}

impl Args {
    fn parse() -> Result<Self, String> {
        let mut args = Self {
            listen: "127.0.0.1:0".to_owned(),
            addr_file: None,
            events: None,
            new_payload: "VALID".to_owned(),
            forkchoice: "VALID".to_owned(),
            hold_new_payload: false,
            hold_after: 0,
            release_file: None,
        };
        let mut rest = std::env::args().skip(1);
        while let Some(arg) = rest.next() {
            match arg.as_str() {
                "--help" | "-h" => {
                    println!("{HELP}");
                    std::process::exit(0);
                }
                "--listen" => args.listen = required(&mut rest, "--listen")?,
                "--addr-file" => {
                    args.addr_file = Some(PathBuf::from(required(&mut rest, "--addr-file")?));
                }
                "--events" => args.events = Some(PathBuf::from(required(&mut rest, "--events")?)),
                "--new-payload" => args.new_payload = required(&mut rest, "--new-payload")?,
                "--forkchoice" => args.forkchoice = required(&mut rest, "--forkchoice")?,
                "--hold-new-payload" => args.hold_new_payload = true,
                "--hold-after" => {
                    let raw = required(&mut rest, "--hold-after")?;
                    args.hold_after = raw
                        .parse()
                        .map_err(|_| format!("--hold-after is not an integer: {raw}"))?;
                }
                "--release-file" => {
                    args.release_file = Some(PathBuf::from(required(&mut rest, "--release-file")?));
                }
                other => return Err(format!("unknown flag {other}")),
            }
        }
        require_status("--new-payload", &args.new_payload, NEW_PAYLOAD_STATUSES)?;
        require_status("--forkchoice", &args.forkchoice, FORKCHOICE_STATUSES)?;
        Ok(args)
    }
}

const HELP: &str = "\
fake-el — loopback engine_newPayloadV4 / engine_forkchoiceUpdatedV3

  --listen ADDR          default 127.0.0.1:0
  --addr-file PATH       write http://ADDR once bound
  --events PATH          append one line per method
  --new-payload STATUS   VALID|INVALID|SYNCING|ACCEPTED|INVALID_BLOCK_HASH
  --forkchoice STATUS    VALID|INVALID|SYNCING
  --hold-new-payload     hold newPayload (not forkchoiceUpdated)
  --hold-after N         hold once the 1-based count exceeds N (default 0)
  --release-file PATH    unanswered hold ends when this path is a file;
                         omitted, the hold lasts until the process is killed
";

fn required(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next()
        .filter(|value| !value.starts_with("--"))
        .ok_or_else(|| format!("{flag} needs a value"))
}

fn require_status(flag: &str, status: &str, allowed: &[&str]) -> Result<(), String> {
    if allowed.contains(&status) {
        Ok(())
    } else {
        Err(format!(
            "{flag} status {status:?} is not one of {allowed:?}"
        ))
    }
}

fn write_addr_file(path: &Path, addr: SocketAddr) -> Result<(), String> {
    let mut file =
        File::create(path).map_err(|err| format!("addr file {}: {err}", path.display()))?;
    writeln!(file, "http://{addr}").map_err(|err| format!("addr file: {err}"))?;
    file.sync_all()
        .map_err(|err| format!("addr file sync: {err}"))?;
    Ok(())
}

struct Shared {
    new_payload: String,
    forkchoice: String,
    hold_new_payload: bool,
    hold_after: u64,
    release_file: Option<PathBuf>,
    events: Mutex<Option<File>>,
    new_payload_seen: Mutex<u64>,
}

impl Shared {
    fn new(args: Args) -> Result<Self, String> {
        let events = match args.events {
            Some(path) => {
                let file = File::create(&path)
                    .map_err(|err| format!("events {}: {err}", path.display()))?;
                Some(file)
            }
            None => None,
        };
        Ok(Self {
            new_payload: args.new_payload,
            forkchoice: args.forkchoice,
            hold_new_payload: args.hold_new_payload,
            hold_after: args.hold_after,
            release_file: args.release_file,
            events: Mutex::new(events),
            new_payload_seen: Mutex::new(0),
        })
    }

    fn record(&self, line: &str) {
        let mut guard = lock(&self.events);
        let Some(file) = guard.as_mut() else {
            return;
        };
        if writeln!(file, "{line}").is_err() {
            return;
        }
        let _ = file.sync_all();
    }

    fn note_new_payload(&self) -> u64 {
        let mut seen = lock(&self.new_payload_seen);
        *seen = seen.saturating_add(1);
        *seen
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn serve_conn(stream: TcpStream, shared: Arc<Shared>) {
    let _ = stream.set_nodelay(true);
    let mut conn = Conn {
        stream,
        buf: Vec::new(),
    };
    loop {
        let request = match conn.read_request() {
            Ok(Some(value)) => value,
            Ok(None) => break,
            Err(err) => {
                eprintln!("fake-el: {err}");
                let _ = conn.write_bytes(
                    b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                );
                break;
            }
        };
        if conn
            .write_response(&response_body(&request, &shared))
            .is_err()
        {
            break;
        }
    }
}

fn response_body(request: &serde_json::Value, shared: &Shared) -> String {
    let id = request.get("id").cloned().unwrap_or(serde_json::json!(1));
    let method = request
        .get("method")
        .and_then(|value| value.as_str())
        .unwrap_or("");
    let result = dispatch(method, shared);
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result,
    })
    .to_string()
}

fn dispatch(method: &str, shared: &Shared) -> serde_json::Value {
    if method.contains("newPayload") {
        let seen = shared.note_new_payload();
        // `>` so `--hold-after 0` holds the first call and N holds after N answers.
        if shared.hold_new_payload && seen > shared.hold_after {
            shared.record(&format!("{method} hold"));
            // No release file: stay unanswered until SIGKILL reaps the client.
            // Answering here would let the import finish and hide the abrupt case.
            wait_for_release(shared.release_file.as_deref());
        } else {
            shared.record(method);
        }
        return payload_status(&shared.new_payload);
    }
    shared.record(method);
    if method.contains("forkchoiceUpdated") {
        serde_json::json!({
            "payloadStatus": {
                "status": shared.forkchoice,
                "latestValidHash": null,
                "validationError": null
            },
            "payloadId": null
        })
    } else if method == "eth_syncing" {
        serde_json::json!(false)
    } else if method == "eth_chainId" {
        serde_json::json!("0x1")
    } else if method == "engine_exchangeCapabilities" {
        serde_json::json!([
            "engine_newPayloadV4",
            "engine_forkchoiceUpdatedV3",
            "engine_getBlobsV2",
            "eth_syncing",
            "eth_chainId"
        ])
    } else if method == "engine_getBlobsV2" {
        serde_json::json!([])
    } else {
        serde_json::json!(null)
    }
}

fn payload_status(status: &str) -> serde_json::Value {
    serde_json::json!({
        "status": status,
        "latestValidHash": null,
        "validationError": null
    })
}

fn wait_for_release(path: Option<&Path>) {
    loop {
        if path.is_some_and(|path| path.is_file()) {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
}

struct Conn {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl Conn {
    fn read_request(&mut self) -> Result<Option<serde_json::Value>, String> {
        let mut tmp = [0_u8; 8192];
        loop {
            if let Some(header_end) = find_header_end(&self.buf) {
                return self.take_body(header_end);
            }
            if self.buf.len() > BODY_CAP {
                return Err("headers exceed 8MiB".to_owned());
            }
            match self.stream.read(&mut tmp) {
                Ok(0) => return Ok(None),
                Ok(n) => self.buf.extend_from_slice(&tmp[..n]),
                Err(err) if err.kind() == std::io::ErrorKind::Interrupted => {}
                Err(err) => return Err(err.to_string()),
            }
        }
    }

    fn take_body(&mut self, header_end: usize) -> Result<Option<serde_json::Value>, String> {
        let header = std::str::from_utf8(&self.buf[..header_end])
            .map_err(|_| "request headers are not utf-8".to_owned())?;
        let len = content_length(header)?;
        let body_at = header_end + 4;
        let need = body_at.saturating_add(len);
        if need > BODY_CAP {
            return Err("body exceeds 8MiB".to_owned());
        }
        let mut tmp = [0_u8; 8192];
        while self.buf.len() < need {
            match self.stream.read(&mut tmp) {
                Ok(0) => return Ok(None),
                Ok(n) => self.buf.extend_from_slice(&tmp[..n]),
                Err(err) if err.kind() == std::io::ErrorKind::Interrupted => {}
                Err(err) => return Err(err.to_string()),
            }
        }
        let body = self.buf[body_at..need].to_vec();
        // Keep bytes past this request. Dropping them breaks keep-alive when
        // the next request arrived in the same read.
        self.buf.drain(..need);
        serde_json::from_slice(&body)
            .map(Some)
            .map_err(|err| err.to_string())
    }

    fn write_response(&mut self, body: &str) -> Result<(), String> {
        let header = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: keep-alive\r\n\r\n",
            body.len()
        );
        self.write_bytes(header.as_bytes())?;
        self.write_bytes(body.as_bytes())
    }

    fn write_bytes(&mut self, bytes: &[u8]) -> Result<(), String> {
        self.stream.write_all(bytes).map_err(|err| err.to_string())
    }
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|window| window == b"\r\n\r\n")
}

fn content_length(header: &str) -> Result<usize, String> {
    for line in header.lines() {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("content-length") {
            return value
                .trim()
                .parse()
                .map_err(|_| format!("bad content-length {value:?}"));
        }
    }
    Err("content-length required".to_owned())
}
