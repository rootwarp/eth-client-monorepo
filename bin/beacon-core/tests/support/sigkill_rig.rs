//! OS-child rig: sibling `fake-el` plus the real `cc-beacon-core` binary.
//!
//! The abrupt case is SIGKILL (`Child::kill`) of that child while `fake-el`
//! is holding `engine_newPayloadV4` inside durable replay. An in-process
//! "drop the runtime" shutdown is not the abrupt case: dropping a tokio
//! runtime, aborting a task, returning from `boot` without `serve`, or
//! dropping `BootedNode` all unwind inside one process. M14c must call
//! [`SigkillRig::sigkill`] while the payload is held. `Drop` only reaps a
//! leftover child; it is cleanup, not the abrupt stop.
//!
//! `CARGO_BIN_EXE_*` is set only for binaries of this test's package, so
//! `fake-el` is `parent(node).join("fake-el")`. That assumes one shared
//! workspace target directory, which is Cargo's default. A missing parent
//! is an error string, not a panic on `None`.

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

/// Node binary with no parent, or a missing sibling, is a message.
pub(crate) fn sibling_fake_el(node_exe: &Path) -> Result<PathBuf, String> {
    let parent = node_exe
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty());
    let Some(parent) = parent else {
        return Err(format!(
            "beacon-core binary {} has no parent directory; sibling fake-el \
             resolution needs a shared workspace target directory",
            node_exe.display()
        ));
    };
    let fake = parent.join("fake-el");
    if !fake.is_file() {
        return Err(format!(
            "sibling fake-el not found at {}. This rig assumes a shared workspace \
             target directory (Cargo's default). Build it with \
             `cargo build -p cc-engine-api --bin fake-el` into that same target dir.",
            fake.display()
        ));
    }
    Ok(fake)
}

#[derive(Debug)]
pub(crate) struct RigConfig {
    pub(crate) data_dir: PathBuf,
    pub(crate) node_key_path: PathBuf,
    pub(crate) jwt_secret_path: PathBuf,
    pub(crate) network_config: PathBuf,
    pub(crate) genesis_validators_root: String,
    pub(crate) new_payload_status: String,
    pub(crate) forkchoice_status: String,
    pub(crate) hold_new_payload: bool,
}

pub(crate) struct SigkillRig {
    node: Child,
    node_reaped: bool,
    node_stderr_path: PathBuf,
    fake: Child,
    events_path: PathBuf,
    fake_stderr_path: PathBuf,
    config_dir: PathBuf,
    config_toml: String,
}

impl std::fmt::Debug for SigkillRig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SigkillRig")
            .field("node_id", &self.node.id())
            .field("node_reaped", &self.node_reaped)
            .finish()
    }
}

impl SigkillRig {
    pub(crate) fn spawn(cfg: RigConfig) -> Result<Self, String> {
        require_file("data_dir", &cfg.data_dir)?;
        require_file("node_key_path", &cfg.node_key_path)?;
        require_file("jwt_secret_path", &cfg.jwt_secret_path)?;
        require_file("network_config", &cfg.network_config)?;
        let node_exe = node_exe()?;
        let fake_exe = sibling_fake_el(&node_exe)?;
        let config_dir = fresh_dir("sigkill-rig-config")?;
        let fake = match start_fake(&cfg, &fake_exe, &config_dir) {
            Ok(fake) => fake,
            Err(err) => {
                let _ = fs::remove_dir_all(&config_dir);
                return Err(err);
            }
        };
        let config_toml = match write_beacon_toml(&cfg, &config_dir, &fake.addr) {
            Ok(toml) => toml,
            Err(err) => {
                reap(&mut FakeChild(fake.child));
                let _ = fs::remove_dir_all(&config_dir);
                return Err(err);
            }
        };
        let node = match start_node(&node_exe, &config_dir) {
            Ok(node) => node,
            Err(err) => {
                reap(&mut FakeChild(fake.child));
                let _ = fs::remove_dir_all(&config_dir);
                return Err(err);
            }
        };
        Ok(Self {
            node,
            node_reaped: false,
            node_stderr_path: config_dir.join("node.stderr"),
            fake: fake.child,
            events_path: fake.events,
            fake_stderr_path: fake.stderr,
            config_dir,
            config_toml,
        })
    }

    pub(crate) fn config_toml(&self) -> String {
        self.config_toml.clone()
    }

    pub(crate) fn node_stderr(&self) -> String {
        tail(&self.node_stderr_path, 8 * 1024)
    }

    /// Block until `fake-el` has logged a held `newPayload` and not answered it.
    pub(crate) fn wait_for_held_new_payload(&mut self, timeout: Duration) -> Result<(), String> {
        let start = Instant::now();
        loop {
            if let Some(status) = self.node.try_wait().map_err(|err| err.to_string())? {
                self.node_reaped = true;
                return Err(format!(
                    "beacon-core exited before newPayload was held ({status}).\n\
                     node stderr:\n{}\nevents:\n{}\nfake-el stderr:\n{}",
                    self.node_stderr(),
                    self.events(),
                    tail(&self.fake_stderr_path, 8 * 1024),
                ));
            }
            let events = self.events();
            if events.contains("newPayload") && events.contains("hold") {
                return Ok(());
            }
            if start.elapsed() > timeout {
                return Err(format!(
                    "timed out waiting for a held newPayload.\n\
                     node stderr:\n{}\nevents:\n{}\nfake-el stderr:\n{}",
                    self.node_stderr(),
                    events,
                    tail(&self.fake_stderr_path, 8 * 1024),
                ));
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// SIGKILL the node. Not a runtime drop: the kernel reaps the process.
    pub(crate) fn sigkill(&mut self) -> Result<ExitStatus, String> {
        self.node.kill().map_err(|err| {
            format!(
                "SIGKILL failed (child already gone is not mid-import): {err}\nstderr:\n{}",
                self.node_stderr()
            )
        })?;
        let status = self.node.wait().map_err(|err| err.to_string())?;
        self.node_reaped = true;
        Ok(status)
    }

    fn events(&self) -> String {
        fs::read_to_string(&self.events_path).unwrap_or_default()
    }
}

impl Drop for SigkillRig {
    fn drop(&mut self) {
        // Cleanup only. Killing here is not the abrupt case the test asserts.
        if !self.node_reaped {
            let _ = self.node.kill();
            let _ = self.node.wait();
        }
        let _ = self.fake.kill();
        let _ = self.fake.wait();
        let _ = fs::remove_dir_all(&self.config_dir);
    }
}

struct StartedFake {
    child: Child,
    addr: String,
    events: PathBuf,
    stderr: PathBuf,
}

struct FakeChild(Child);

fn reap(child: &mut FakeChild) {
    let _ = child.0.kill();
    let _ = child.0.wait();
}

fn start_fake(cfg: &RigConfig, exe: &Path, config_dir: &Path) -> Result<StartedFake, String> {
    let addr_file = config_dir.join("fake-el.addr");
    let events = config_dir.join("fake-el.events");
    let stderr_path = config_dir.join("fake-el.stderr");
    let stderr = File::create(&stderr_path).map_err(|err| err.to_string())?;
    let mut cmd = Command::new(exe);
    cmd.args([
        "--listen",
        "127.0.0.1:0",
        "--addr-file",
        &addr_file.display().to_string(),
        "--events",
        &events.display().to_string(),
        "--new-payload",
        &cfg.new_payload_status,
        "--forkchoice",
        &cfg.forkchoice_status,
    ])
    .stdin(Stdio::null())
    .stdout(Stdio::null())
    .stderr(stderr);
    if cfg.hold_new_payload {
        cmd.arg("--hold-new-payload");
    }
    let mut child = cmd.spawn().map_err(|err| format!("spawn fake-el: {err}"))?;
    match wait_http_addr(&mut child, &addr_file, &stderr_path) {
        Ok(addr) => Ok(StartedFake {
            child,
            addr,
            events,
            stderr: stderr_path,
        }),
        Err(err) => {
            let _ = child.kill();
            let _ = child.wait();
            Err(err)
        }
    }
}

fn wait_http_addr(child: &mut Child, addr_file: &Path, stderr: &Path) -> Result<String, String> {
    let start = Instant::now();
    loop {
        if let Some(status) = child.try_wait().map_err(|err| err.to_string())? {
            return Err(format!(
                "fake-el exited before publishing an address ({status}).\n{}",
                tail(stderr, 8 * 1024)
            ));
        }
        if let Ok(text) = fs::read_to_string(addr_file) {
            let line = text.lines().next().unwrap_or("").trim();
            if let Some(rest) = line.strip_prefix("http://")
                && !rest.is_empty()
            {
                return Ok(line.to_owned());
            }
        }
        if start.elapsed() > Duration::from_secs(5) {
            return Err(format!(
                "timed out waiting for fake-el addr file.\n{}",
                tail(stderr, 8 * 1024)
            ));
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn start_node(exe: &Path, config_dir: &Path) -> Result<Child, String> {
    let stdout = File::create(config_dir.join("node.stdout")).map_err(|err| err.to_string())?;
    let stderr = File::create(config_dir.join("node.stderr")).map_err(|err| err.to_string())?;
    let mut cmd = Command::new(exe);
    cmd.current_dir(config_dir)
        .stdin(Stdio::null())
        .stdout(stdout)
        .stderr(stderr)
        .env("RUST_LOG", "info")
        .env("LOG_FORMAT", "json");
    for (key, _) in std::env::vars() {
        if key.starts_with("CC_BEACON_CORE_") {
            cmd.env_remove(key);
        }
    }
    cmd.spawn()
        .map_err(|err| format!("spawn beacon-core: {err}"))
}

fn write_beacon_toml(
    cfg: &RigConfig,
    config_dir: &Path,
    el_endpoint: &str,
) -> Result<String, String> {
    let dir = config_dir.join("config");
    fs::create_dir_all(&dir).map_err(|err| err.to_string())?;
    let toml = format!(
        "\
grpc_addr = \"127.0.0.1:0\"
metrics_addr = \"127.0.0.1:0\"
log_format = \"json\"
log_filter = \"info\"
data_dir = {data_dir}
durability = \"immediate\"
check_invariants = true
snapshot_ring = 4
genesis_validators_root = {gvr}
node_key_path = {node_key}
network_config = {network}
checkpoint_providers = []
el_endpoint = {el}
jwt_secret_path = {jwt}
slot_duration_ms = 50
attestation_due_bps = 3333
maximum_gossip_clock_disparity_ms = 500

[peers]

[timeouts]
new_payload_ms = 20000
forkchoice_updated_ms = 2000
get_blobs_ms = 1000
exchange_capabilities_ms = 1000
eth_syncing_ms = 1000
multiplier = 1.0

[el_forks]
osaka_time = 0
",
        data_dir = toml_string(&cfg.data_dir)?,
        gvr = toml_raw(&cfg.genesis_validators_root)?,
        node_key = toml_string(&cfg.node_key_path)?,
        network = toml_string(&cfg.network_config)?,
        el = toml_raw(el_endpoint)?,
        jwt = toml_string(&cfg.jwt_secret_path)?,
    );
    fs::write(dir.join("beacon-core.toml"), &toml).map_err(|err| err.to_string())?;
    Ok(toml)
}

fn toml_string(path: &Path) -> Result<String, String> {
    let text = path
        .to_str()
        .ok_or_else(|| format!("path is not utf-8: {}", path.display()))?;
    toml_raw(text)
}

fn toml_raw(text: &str) -> Result<String, String> {
    if text.contains('\n') || text.contains('\r') {
        return Err(format!("toml value contains a newline: {text:?}"));
    }
    Ok(format!(
        "\"{}\"",
        text.replace('\\', "\\\\").replace('"', "\\\"")
    ))
}

fn node_exe() -> Result<PathBuf, String> {
    let found = [
        option_env!("CARGO_BIN_EXE_cc_beacon_core"),
        option_env!("CARGO_BIN_EXE_cc-beacon-core"),
        option_env!("CARGO_BIN_EXE_beacon_core"),
        option_env!("CARGO_BIN_EXE_beacon-core"),
    ]
    .into_iter()
    .flatten()
    .find(|path| !path.is_empty());
    match found {
        Some(path) => Ok(PathBuf::from(path)),
        None => Err(
            "none of CARGO_BIN_EXE_cc_beacon_core, CARGO_BIN_EXE_cc-beacon-core, \
             CARGO_BIN_EXE_beacon_core, CARGO_BIN_EXE_beacon-core is set"
                .to_owned(),
        ),
    }
}

fn require_file(label: &str, path: &Path) -> Result<(), String> {
    if path.is_file() || path.is_dir() {
        Ok(())
    } else {
        Err(format!("{label} does not exist: {}", path.display()))
    }
}

fn fresh_dir(prefix: &str) -> Result<PathBuf, String> {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or(0);
    let path = std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()));
    fs::create_dir_all(&path).map_err(|err| err.to_string())?;
    Ok(path)
}

fn tail(path: &Path, max: usize) -> String {
    let Ok(text) = fs::read_to_string(path) else {
        return String::new();
    };
    if text.len() <= max {
        text
    } else {
        text[text.len() - max..].to_owned()
    }
}
