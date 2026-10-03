#!/usr/bin/env bash
# devnet/up.sh — bring up the CC-2Jd self-devnet (publisher, node-a, node-b, anchor).
#
# Steps:
#   1. Ensure generated artifacts (cc-devnet-gen) exist under devnet/out/
#   2. Write deterministic node keys + bootnodes.txt (same peer ids every run)
#   3. Materialise anchor static files
#   4. docker compose *build* (slow path)
#   5. Set genesis wall-clock *after* build (H1) with optional grace
#   6. docker compose up -d
#
# Env:
#   CC_DEVNET_COMPOSE          compose file (default: devnet/compose.yml).
#                              devnet/compose.beacon-core.yml is cc-p2p + cc-beacon-core
#                              on the same anchor, publisher, fixtures, and EL volume.
#   CC_DEVNET_GEN=1            force regenerate fixture (default: only if missing)
#   CC_DEVNET_SLOT_COUNT=N     override generator slot count for a short smoke fixture
#   CC_DEVNET_GENESIS_TIME     wall-clock genesis (default: now + grace after build)
#   CC_DEVNET_GENESIS_GRACE    seconds after build before genesis (default: 15)
#   CC_DEVNET_MAX_SLOTS        publisher stop after N slots (default: 64)
#   CC_NODE_A_PEERS=publisher  node-a dials only the publisher (still dials)
#   CC_NODE_A_DISABLE_DIAL     true → node-a does not dial (needs inbound from publisher)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEVNET="${ROOT}/devnet"
OUT="${DEVNET}/out"
export COMPOSE_FILE="${CC_DEVNET_COMPOSE:-${DEVNET}/compose.yml}"
cd "${ROOT}"

export CC_GIT_SHA="${CC_GIT_SHA:-$(git rev-parse --short HEAD 2>/dev/null || echo unknown)}"

need() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "error: $1 is required" >&2
    exit 1
  }
}
need docker
need cargo
need python3

# ── 1. fixture ───────────────────────────────────────────────────────────────
if [[ "${CC_DEVNET_GEN:-0}" == "1" || ! -f "${OUT}/manifest.json" || ! -d "${OUT}/chain" ]]; then
  echo "==> generating fixture with cc-devnet-gen"
  if [[ -n "${CC_DEVNET_SLOT_COUNT:-}" ]]; then
    # SEC-H2: validate integer and pass via env into a *quoted* heredoc (no shell inject).
    if ! [[ "${CC_DEVNET_SLOT_COUNT}" =~ ^[0-9]+$ ]]; then
      echo "error: CC_DEVNET_SLOT_COUNT must be a non-negative integer, got: ${CC_DEVNET_SLOT_COUNT}" >&2
      exit 2
    fi
    tmp_toml="$(mktemp)"
    export CC_DEVNET_SLOT_COUNT
    export DEVNET_TOML="${DEVNET}/devnet.toml"
    export TMP_TOML="${tmp_toml}"
    python3 - <<'PY'
import os
from pathlib import Path

slot = os.environ["CC_DEVNET_SLOT_COUNT"]
if not slot.isdigit():
    raise SystemExit(f"CC_DEVNET_SLOT_COUNT must be integer, got {slot!r}")
src = Path(os.environ["DEVNET_TOML"]).read_text()
lines = []
for line in src.splitlines():
    if line.startswith("slot_count"):
        lines.append(f"slot_count = {slot}")
    else:
        lines.append(line)
Path(os.environ["TMP_TOML"]).write_text("\n".join(lines) + "\n")
PY
    cargo run -p cc-devnet-gen --release --locked -- --config "${tmp_toml}" --out "${OUT}"
    rm -f "${tmp_toml}"
  else
    cargo run -p cc-devnet-gen --release --locked -- --config "${DEVNET}/devnet.toml" --out "${OUT}"
  fi
else
  echo "==> reusing existing fixture at ${OUT}"
fi

# ── 2. deterministic keys + bootnodes ────────────────────────────────────────
echo "==> writing deterministic keys + bootnodes.txt"
if [[ -x "${ROOT}/target/release/cc-p2p" ]]; then
  "${ROOT}/target/release/cc-p2p" --emit-bootnodes "${OUT}"
else
  cargo build -p cc-p2p --release --locked
  "${ROOT}/target/release/cc-p2p" --emit-bootnodes "${OUT}"
fi

# Docker image runs as uid 10001; relax mode so mounted keys are readable
# (these are deterministic devnet secrets, not production custody keys).
if [[ -d "${OUT}/node_keys" ]]; then
  chmod -R a+rX "${OUT}/node_keys" || true
fi

# Per-node multiaddr files (everyone dials everyone else by default).
python3 - <<'PY'
import os
from pathlib import Path
out = Path("devnet/out")
roles = [
    ("publisher", "publisher", 9000),
    ("node-a", "node-a", 9000),
    ("node-b", "node-b", 9000),
]
peer_ids = {}
pid_path = out / "peer_ids.txt"
if pid_path.exists():
    for line in pid_path.read_text().splitlines():
        parts = line.split()
        if len(parts) >= 2:
            peer_ids[parts[0]] = parts[1]

def write_peers(for_role: str, peers, path: Path, publisher_only: bool = False):
    lines = []
    for role, host, port in peers:
        if role == for_role:
            continue
        if publisher_only and role != "publisher":
            continue
        pid = peer_ids.get(role, "")
        lines.append(f"/dns/{host}/tcp/{port} # {role} peer_id={pid}")
    path.write_text("\n".join(lines) + ("\n" if lines else ""))

# Beacon-core topology has no node-a/node-b. cc-p2p still dials the publisher
# via CC_P2P_STATIC_PEERS; the publisher must not dial names that do not exist.
compose = os.environ.get("COMPOSE_FILE", "")
if compose.endswith("compose.beacon-core.yml"):
    (out / "multiaddrs.publisher.txt").write_text("")
    print("wrote publisher multiaddrs: empty (beacon-core topology)")
else:
    write_peers("publisher", roles, out / "multiaddrs.publisher.txt")
write_peers("node-a", roles, out / "multiaddrs.node-a.txt")
write_peers("node-b", roles, out / "multiaddrs.node-b.txt")
# Single-peer scenario (CC-2Jb): node-a dials only the publisher.
write_peers("node-a", roles, out / "multiaddrs.node-a.publisher-only.txt", publisher_only=True)
print("wrote per-node multiaddr files")
PY

# GVR + publisher peer id are deterministic for a given fixture. The two-process
# file dials the publisher with them; the four-container file ignores them.
if [[ -f "${OUT}/manifest.json" ]]; then
  CC_DEVNET_GVR="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("genesis_validators_root",""))' "${OUT}/manifest.json")"
  if [[ -n "${CC_DEVNET_GVR}" ]]; then
    export CC_DEVNET_GVR
  fi
fi
if [[ -f "${OUT}/peer_ids.txt" ]]; then
  pub_pid="$(awk '$1=="publisher" {print $2}' "${OUT}/peer_ids.txt")"
  if [[ -n "${pub_pid}" ]]; then
    export CC_P2P_STATIC_PEERS="[{peer_id=\"${pub_pid}\",multiaddr=\"/dns/publisher/tcp/9000\"}]"
  fi
fi

# Scenario peer set for node-a.
if [[ "${CC_NODE_A_PEERS:-}" == "publisher" ]]; then
  cp -f "${OUT}/multiaddrs.node-a.publisher-only.txt" "${OUT}/multiaddrs.node-a.txt"
  # AC: single-peer mode restricts to publisher; node-a still dials (publisher also dials mesh).
  # Only force disable-dial when explicitly requested.
  echo "==> node-a peer set: publisher only (dial=${CC_NODE_A_DISABLE_DIAL:-false})"
fi
if [[ "${CC_NODE_A_DISABLE_DIAL:-false}" == "true" ]]; then
  # With dialling disabled, publisher must dial node-a (multiaddrs.publisher includes node-a).
  echo "==> node-a dialling disabled; publisher will dial inbound"
fi

# ── 3. anchor static files ───────────────────────────────────────────────────
echo "==> materialising anchor payloads"
ANCHOR="${OUT}/anchor"
mkdir -p "${ANCHOR}"
cp -f "${OUT}/genesis.ssz" "${ANCHOR}/genesis.ssz"

python3 - <<'PY'
import json, pathlib, re
out = pathlib.Path("devnet/out")
yaml_text = (out / "config.yaml").read_text()
data = {}
blob_schedule = []
in_blob = False
current = None
for raw in yaml_text.splitlines():
    line = raw.split("#", 1)[0].rstrip()
    if not line.strip():
        continue
    if line.startswith("BLOB_SCHEDULE"):
        in_blob = True
        continue
    if in_blob:
        m = re.match(r"\s*-\s*EPOCH:\s*(\d+)", line)
        if m:
            current = {"EPOCH": m.group(1)}
            blob_schedule.append(current)
            continue
        m = re.match(r"\s*MAX_BLOBS_PER_BLOCK:\s*(\d+)", line)
        if m and current is not None:
            current["MAX_BLOBS_PER_BLOCK"] = m.group(1)
            continue
        if re.match(r"^[A-Z_]", line):
            in_blob = False
        else:
            continue
    if in_blob:
        continue
    if ":" not in line:
        continue
    k, v = line.split(":", 1)
    k = k.strip()
    v = v.strip().strip('"').strip("'")
    if k:
        data[k] = v
if blob_schedule:
    data["BLOB_SCHEDULE"] = json.dumps(blob_schedule)
spec = {"data": data}
(out / "anchor" / "spec.json").write_text(json.dumps(spec, indent=2) + "\n")

manifest = json.loads((out / "manifest.json").read_text())
gvr = manifest.get("genesis_validators_root", "0x" + "00" * 32)
genesis_time = data.get("MIN_GENESIS_TIME", "1700000000")
genesis = {
    "data": {
        "genesis_time": str(genesis_time),
        "genesis_validators_root": gvr,
        "genesis_fork_version": data.get("GENESIS_FORK_VERSION", "0x00000064"),
    }
}
(out / "anchor" / "genesis.json").write_text(json.dumps(genesis, indent=2) + "\n")
print("anchor ready")
PY

# ── 4. build images first (H1: genesis stamped after this) ───────────────────
# Clock knobs come from the generated fixture, not config/p2p.toml (32 / 500 ms).
# Committed devnet/devnet.toml (preset=minimal, seconds_per_slot=3) emits
# SLOTS_PER_EPOCH 8 and MAXIMUM_GOSSIP_CLOCK_DISPARITY 125ms
# (bin/devnet-gen: 500 * seconds_per_slot / 12).
clock_line="$(OUT_CONFIG="${OUT}/config.yaml" python3 - <<'PY'
import os, re
from pathlib import Path
text = Path(os.environ["OUT_CONFIG"]).read_text()

def grab(key: str) -> str:
    m = re.search(rf"^{key}:\s*(\S+)", text, re.M)
    if not m:
        raise SystemExit(f"error: {key} missing in {os.environ['OUT_CONFIG']}")
    return m.group(1)

slots = grab("SLOTS_PER_EPOCH")
secs = grab("SECONDS_PER_SLOT")
disp = grab("MAXIMUM_GOSSIP_CLOCK_DISPARITY")
if disp.endswith("ms"):
    disp = disp[:-2]
if not (slots.isdigit() and secs.isdigit() and disp.isdigit()):
    raise SystemExit(
        f"error: clock fields not integers: slots={slots} secs={secs} disparity={disp}"
    )
print(f"{slots} {secs} {disp}")
PY
)"
read -r yaml_slots yaml_secs yaml_disp <<<"${clock_line}"
export CC_DEVNET_SLOTS_PER_EPOCH="${yaml_slots}"
export CC_DEVNET_GOSSIP_DISPARITY_MS="${yaml_disp}"
export CC_DEVNET_SECONDS_PER_SLOT="${CC_DEVNET_SECONDS_PER_SLOT:-${yaml_secs}}"
export CC_DEVNET_SLOT_DURATION_MS="${CC_DEVNET_SLOT_DURATION_MS:-$((CC_DEVNET_SECONDS_PER_SLOT * 1000))}"
export CC_DEVNET_MAX_SLOTS="${CC_DEVNET_MAX_SLOTS:-64}"
export CC_NODE_A_DISABLE_DIAL="${CC_NODE_A_DISABLE_DIAL:-false}"
echo "==> fixture clock: slots_per_epoch=${CC_DEVNET_SLOTS_PER_EPOCH} disparity_ms=${CC_DEVNET_GOSSIP_DISPARITY_MS} seconds_per_slot=${CC_DEVNET_SECONDS_PER_SLOT}"

# el and cc-beacon-core both load this file (mode 0600). Do not rotate an
# existing secret — both topologies must keep the same bytes. Create at 0600
# before the first write so a crash cannot leave a umask mode.
if [[ ! -f "${ROOT}/secrets/jwt.hex" ]]; then
  echo "==> writing secrets/jwt.hex (0600)"
  python3 - <<'PY'
import os
import secrets
path = "secrets/jwt.hex"
fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
with os.fdopen(fd, "w") as fh:
    fh.write(secrets.token_hex(32) + "\n")
PY
fi

# Capture any operator-supplied genesis before we overwrite for compose parse.
OPERATOR_GENESIS="${CC_DEVNET_GENESIS_TIME:-}"
# Placeholder so compose file interpolation succeeds during build.
export CC_DEVNET_GENESIS_TIME=1

echo "==> docker compose build"
docker compose -f "${COMPOSE_FILE}" build

# ── 5. genesis wall-clock AFTER build (H1) ───────────────────────────────────
# Prefer operator pin if provided; otherwise now+grace so cold builds cannot
# exhaust catch-up before the mesh forms.
if [[ -n "${OPERATOR_GENESIS}" ]]; then
  export CC_DEVNET_GENESIS_TIME="${OPERATOR_GENESIS}"
  echo "==> genesis_time=${CC_DEVNET_GENESIS_TIME} (operator-supplied)"
else
  grace="${CC_DEVNET_GENESIS_GRACE:-15}"
  if ! [[ "${grace}" =~ ^[0-9]+$ ]]; then
    echo "error: CC_DEVNET_GENESIS_GRACE must be integer seconds" >&2
    exit 2
  fi
  export CC_DEVNET_GENESIS_TIME="$(( $(date +%s) + grace ))"
  echo "==> genesis_time=${CC_DEVNET_GENESIS_TIME} (now+${grace}s grace after build)"
fi

# ── 6. compose up (no rebuild; force-recreate so publisher gets real genesis) ─
echo "==> docker compose up -d --force-recreate"
docker compose -f "${COMPOSE_FILE}" up -d --remove-orphans --no-build --force-recreate

echo "==> waiting for metrics endpoints"
deadline=$((SECONDS + 180))
case "${COMPOSE_FILE}" in
  *compose.beacon-core.yml)
    # publisher + anchor stay; the host is cc-beacon-core :9101 and cc-p2p :9102.
    wait_ports=(19102 18080 9101 9102)
    ;;
  *)
    wait_ports=(19102 19112 19122 18080)
    ;;
esac
for port in "${wait_ports[@]}"; do
  ok=0
  while (( SECONDS < deadline )); do
    if curl -fsS "http://127.0.0.1:${port}/metrics" >/dev/null 2>&1 \
      || curl -fsS "http://127.0.0.1:${port}/healthz" >/dev/null 2>&1 \
      || curl -fsS "http://127.0.0.1:${port}/eth/v1/config/spec" >/dev/null 2>&1; then
      ok=1
      break
    fi
    sleep 2
  done
  if [[ "${ok}" -ne 1 ]]; then
    echo "error: endpoint on port ${port} did not become ready" >&2
    docker compose -f "${COMPOSE_FILE}" ps >&2 || true
    docker compose -f "${COMPOSE_FILE}" logs --tail=80 >&2 || true
    exit 1
  fi
  echo "  port ${port}: ready"
done

case "${COMPOSE_FILE}" in
  *compose.beacon-core.yml)
    echo "devnet up: anchor + publisher + el + cc-p2p + cc-beacon-core (liveness, not import)"
    echo "  publisher metrics:    http://127.0.0.1:19102/metrics"
    echo "  cc-p2p metrics:       http://127.0.0.1:9102/metrics"
    echo "  cc-beacon-core metrics: http://127.0.0.1:9101/metrics"
    ;;
  *)
    echo "devnet up: publisher + node-a + node-b + anchor + el"
    echo "  publisher metrics: http://127.0.0.1:19102/metrics"
    echo "  node-a metrics:    http://127.0.0.1:19112/metrics"
    echo "  node-b metrics:    http://127.0.0.1:19122/metrics"
    ;;
esac
echo "  anchor:            http://127.0.0.1:18080/"
echo "  el snapshot:       volume cc-devnet-el"
echo "  bootnodes:         ${OUT}/bootnodes.txt"
echo "  genesis_time:      ${CC_DEVNET_GENESIS_TIME}"
