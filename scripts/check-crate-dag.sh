#!/usr/bin/env bash
# Enforce Architecture §2.2 crate dependency DAG + workspace lint/rust-version opt-in (R-6).
# Exit non-zero and name the offending crate/edge on failure.
# Portable: no bash-4 associative arrays (macOS /bin/bash is 3.2).
#
# Requires: bash, cargo, jq (and rust-toolchain.toml / Cargo.lock present).
# `--self-test` needs only bash (JWT/HTTP + chain↔p2p fixtures under scripts/fixtures/).
#
# Usage:
#   bash scripts/check-crate-dag.sh                # JWT/HTTP + chain↔p2p fixtures, then live DAG
#   bash scripts/check-crate-dag.sh --self-test    # JWT/HTTP + chain↔p2p fixtures only
#   bash scripts/check-crate-dag.sh --check-unused # fixtures + metadata; fail on unused allowlist
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ARG="${1:-}"
if [[ "$ARG" == "-h" || "$ARG" == "--help" ]]; then
  echo "Usage: bash scripts/check-crate-dag.sh [--self-test|--check-unused]"
  echo "Enforce the workspace crate DAG, JWT/HTTP isolation (ADR P3-16 / [ARCH] §6.2),"
  echo "and the named cc-chain ↛ cc-p2p / cc-p2p ↛ cc-chain bans ([ARCH] §2.5)."
  echo "  --self-test      JWT/HTTP + chain↔p2p fixtures only"
  echo "  --check-unused   fail if allowed_deps lists an edge cargo metadata does not have (Q-1)"
  exit 0
fi
if [[ -n "$ARG" && "$ARG" != "--self-test" && "$ARG" != "--check-unused" ]]; then
  echo "error: unknown argument: $ARG (want --self-test or --check-unused)" >&2
  exit 1
fi

# --- ADR P3-16 / [ARCH] §6.2: Engine API HTTP client + JWT signer isolation ---
# Decision recorded as ADR-R-03 (supersedes ADR-P3-16).
# Same shape as the libp2p rule: the project's first real credential and its
# Engine API transport live in exactly one crate, so a JWT signer appearing
# outside that crate is a build failure, not a review comment.
#
# S1-A-01 re-points the named crate to cc-engine-api. S1-A-03 moves jwt.rs
# here (module private). Transitional cc-engine still #[path]s the signer
# until S1-A-06; both may declare HTTP+JWT.
# Grandfathered HTTP (never JWT): cc-chain, cc-bootstrap. Workspace root pins.
# `sha2` is deliberately not listed — cc-crypto legitimately uses hashing.
# The rule is about *declaring* the dependency; transitive hyper under tonic
# is unaffected (metadata walk selects direct edges).
FORBIDDEN_OUTSIDE_ENGINE='^(reqwest|hyper|hyper-util|jsonwebtoken|hmac|sha2-jwt)$'
FORBIDDEN_JWT_ONLY='^(jsonwebtoken|hmac|sha2-jwt)$'

http_or_jwt_allowed() {
  # $1 = package name, $2 = dependency name
  local pkg="$1" dep="$2"
  if [[ "$pkg" == "cc-engine-api" ]]; then
    return 0
  fi
  # Transitional host binary may still declare HTTP/JWT for container ITs.
  if [[ "$pkg" == "cc-engine" ]]; then
    return 0
  fi
  if [[ "$dep" =~ $FORBIDDEN_JWT_ONLY ]]; then
    return 1
  fi
  # HTTP client crates: grandfather chain + bootstrap. Never JWT.
  # S2-A-01: cc-chain-core is not on either list.
  case "$pkg" in
    cc-chain|cc-bootstrap) return 0 ;;
    *) return 1 ;;
  esac
}

# $1 = path to Cargo.toml to read, $2 = logical workspace-relative path.
# Prints grep hits (line:text). Empty = allowed for this rel.
http_jwt_manifest_hits() {
  local manifest="$1" rel="$2"
  case "$rel" in
    services/engine/Cargo.toml|crates/engine-api/Cargo.toml|Cargo.toml)
      return 0
      ;;
  esac
  local pat='reqwest|hyper|hyper-util|jsonwebtoken|hmac|sha2-jwt'
  case "$rel" in
    services/chain/Cargo.toml|crates/bootstrap/Cargo.toml)
      pat='jsonwebtoken|hmac|sha2-jwt'
      ;;
  esac
  grep -nE \
    "^[[:space:]]*(${pat})[[:space:]]*=|^[[:space:]]*\[dependencies\.(${pat})\]" \
    "$manifest" 2>/dev/null || true
}

# S1-A-13 / [ARCH] §2.5: cc-chain ↛ cc-p2p and cc-p2p ↛ cc-chain.
# Calls go through cc-seam traits. Named policy (not allowed_deps): return 0
# unless this is one of the two banned package-name pairs. cargo metadata
# `.dependencies[].name` is the package name, so rename / workspace-dot /
# [dev-dependencies.*] / [build-dependencies.*] / [target.*.dependencies.*]
# all resolve here (same shape as http_or_jwt_allowed).
chain_p2p_allowed() {
  # $1 = depender package name, $2 = dependee package name
  case "$1 $2" in
    "cc-chain cc-p2p"|"cc-p2p cc-chain") return 1 ;;
    *) return 0 ;;
  esac
}

# Filesystem backup (storage ↛ fork-choice style). $1 = Cargo.toml, $2 = logical rel.
# Prints grep hits (line:text). Empty = no house-style key / [dependencies.*] hit.
chain_p2p_manifest_hits() {
  local manifest="$1" rel="$2" pat=""
  case "$rel" in
    services/chain/Cargo.toml) pat='cc-p2p' ;;
    services/p2p/Cargo.toml)   pat='cc-chain' ;;
    *) return 0 ;;
  esac
  grep -nE \
    "^[[:space:]]*${pat}[[:space:]]*=|^[[:space:]]*\[dependencies\.${pat}\]" \
    "$manifest" 2>/dev/null || true
}

# ── Fixture self-test (S1-A-01 E1.4) ────────────────────────────────────────
# Fixtures live outside services/crates/bin so they cannot weaken the live scan.
FIXTURE_ROOT="${ROOT}/scripts/fixtures/check-crate-dag"
FAIL_DIR="${FIXTURE_ROOT}/expect-fail"
PASS_DIR="${FIXTURE_ROOT}/expect-pass"

if [[ ! -d "${FAIL_DIR}" || ! -d "${PASS_DIR}" ]]; then
  echo "error: missing fixture dirs under ${FIXTURE_ROOT#"$ROOT"/}" >&2
  exit 1
fi

selftest_failed=0

for required in third-crate-reqwest chain-jwt chain-core-jwt chain-depends-p2p p2p-depends-chain storage-core-jwt beacon-core-jwt; do
  if [[ ! -f "${FAIL_DIR}/${required}/Cargo.toml" || ! -f "${FAIL_DIR}/${required}/rel" ]]; then
    echo "error: self-test: missing negative fixture ${FAIL_DIR#"$ROOT"/}/${required}" >&2
    selftest_failed=1
  fi
done
for required_pass in engine-api-reqwest chain-http; do
  if [[ ! -f "${PASS_DIR}/${required_pass}/Cargo.toml" || ! -f "${PASS_DIR}/${required_pass}/rel" ]]; then
    echo "error: self-test: missing positive fixture ${PASS_DIR#"$ROOT"/}/${required_pass}" >&2
    selftest_failed=1
  fi
done

# Function-level: the rule names cc-engine-api; cc-chain is not JWT-grandfathered.
if ! http_or_jwt_allowed cc-engine-api reqwest \
  || ! http_or_jwt_allowed cc-engine-api jsonwebtoken; then
  echo "error: self-test: http_or_jwt_allowed must allow cc-engine-api (E1.4)" >&2
  selftest_failed=1
fi
if http_or_jwt_allowed cc-chain jsonwebtoken; then
  echo "error: self-test: cc-chain must not be on the JWT grandfather list (E1.4)" >&2
  selftest_failed=1
fi
if http_or_jwt_allowed cc-chain-core jsonwebtoken \
  || http_or_jwt_allowed cc-chain-core reqwest; then
  echo "error: self-test: cc-chain-core must not be JWT- or HTTP-grandfathered (S2-A-01)" >&2
  selftest_failed=1
fi
if http_or_jwt_allowed cc-scheduler reqwest; then
  echo "error: self-test: a third crate must not be allowed an HTTP client" >&2
  selftest_failed=1
fi
if http_or_jwt_allowed cc-storage-core jsonwebtoken \
  || http_or_jwt_allowed cc-storage-core reqwest; then
  echo "error: self-test: cc-storage-core must not be on the JWT/HTTP grandfather list (S2-B-01)" >&2
  selftest_failed=1
fi
if http_or_jwt_allowed cc-beacon-core jsonwebtoken \
  || http_or_jwt_allowed cc-beacon-core reqwest; then
  echo "error: self-test: cc-beacon-core must not be JWT- or HTTP-grandfathered (S2-J-01)" >&2
  selftest_failed=1
fi
if http_or_jwt_allowed cc-beacon-inproc jsonwebtoken \
  || http_or_jwt_allowed cc-beacon-inproc reqwest; then
  echo "error: self-test: cc-beacon-inproc must not be JWT- or HTTP-grandfathered (S2-A-13)" >&2
  selftest_failed=1
fi
if http_or_jwt_allowed cc-beacon-import jsonwebtoken \
  || http_or_jwt_allowed cc-beacon-import reqwest; then
  echo "error: self-test: cc-beacon-import must not be JWT- or HTTP-grandfathered (S2-A-14)" >&2
  selftest_failed=1
fi

# Function-level: both directions forbidden; seam (and any other edge) is not this rule.
if chain_p2p_allowed cc-chain cc-p2p; then
  echo "error: self-test: cc-chain must not be allowed to depend on cc-p2p (S1-A-13)" >&2
  selftest_failed=1
fi
if chain_p2p_allowed cc-p2p cc-chain; then
  echo "error: self-test: cc-p2p must not be allowed to depend on cc-chain (S1-A-13)" >&2
  selftest_failed=1
fi
if ! chain_p2p_allowed cc-chain cc-seam \
  || ! chain_p2p_allowed cc-p2p cc-seam; then
  echo "error: self-test: cc-seam must remain allowed on chain and p2p (S1-A-13)" >&2
  selftest_failed=1
fi

n_fail=0
for dir in "${FAIL_DIR}"/*/; do
  [[ -d "$dir" ]] || continue
  n_fail=$((n_fail + 1))
  if [[ ! -f "${dir}rel" || ! -f "${dir}Cargo.toml" ]]; then
    echo "error: self-test: fixture ${dir#"$ROOT"/} needs Cargo.toml and rel" >&2
    selftest_failed=1
    continue
  fi
  rel="$(tr -d '[:space:]' < "${dir}rel")"
  hits="$(http_jwt_manifest_hits "${dir}Cargo.toml" "$rel")"
  if [[ -z "${hits}" ]]; then
    hits="$(chain_p2p_manifest_hits "${dir}Cargo.toml" "$rel")"
  fi
  if [[ -z "${hits}" ]]; then
    echo "error: self-test: expected HTTP/JWT or chain↔p2p hit in ${dir#"$ROOT"/} (rel=${rel})" >&2
    selftest_failed=1
  else
    echo "ok: self-test ${dir#"$ROOT"/} is red (${rel})"
  fi
done

n_pass=0
for dir in "${PASS_DIR}"/*/; do
  [[ -d "$dir" ]] || continue
  n_pass=$((n_pass + 1))
  if [[ ! -f "${dir}rel" || ! -f "${dir}Cargo.toml" ]]; then
    echo "error: self-test: fixture ${dir#"$ROOT"/} needs Cargo.toml and rel" >&2
    selftest_failed=1
    continue
  fi
  rel="$(tr -d '[:space:]' < "${dir}rel")"
  hits="$(http_jwt_manifest_hits "${dir}Cargo.toml" "$rel")"
  if [[ -z "${hits}" ]]; then
    hits="$(chain_p2p_manifest_hits "${dir}Cargo.toml" "$rel")"
  fi
  if [[ -n "${hits}" ]]; then
    echo "error: self-test: unexpected HTTP/JWT or chain↔p2p hit in ${dir#"$ROOT"/} (rel=${rel}):" >&2
    echo "${hits}" >&2
    selftest_failed=1
  else
    echo "ok: self-test ${dir#"$ROOT"/} is green (${rel})"
  fi
done

if [[ "${n_fail}" -lt 2 ]]; then
  echo "error: self-test: need >=2 negative fixtures in ${FAIL_DIR#"$ROOT"/} (found ${n_fail})" >&2
  selftest_failed=1
fi
if [[ "${n_pass}" -lt 2 ]]; then
  echo "error: self-test: need >=2 positive fixtures in ${PASS_DIR#"$ROOT"/} (found ${n_pass})" >&2
  selftest_failed=1
fi

if [[ "${selftest_failed}" -ne 0 ]]; then
  exit 1
fi

if [[ "$ARG" == "--self-test" ]]; then
  echo "ok: check-crate-dag JWT/HTTP and chain↔p2p fixtures"
  exit 0
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "error: jq is required" >&2
  exit 1
fi

# --- ADR P3-16 early manifest scan (before cargo metadata --locked) ----------
# Pure filesystem grep so adding a forbidden dep names the offending
# Cargo.toml even when the lockfile is not yet updated (negative-test shape).
# cc-engine-api / cc-engine are exempt; chain/bootstrap may declare HTTP
# clients but never JWT signers; workspace root may pin.
EARLY_FAILED=0
while IFS= read -r manifest; do
  [[ -z "$manifest" || ! -f "$manifest" ]] && continue
  rel="${manifest#"$ROOT"/}"
  while IFS= read -r hit; do
    [[ -z "$hit" ]] && continue
    echo "error: $rel: only cc-engine-api may declare an HTTP client or JWT signer (ADR P3-16; $hit)" >&2
    EARLY_FAILED=1
  done < <(http_jwt_manifest_hits "$manifest" "$rel")
done < <(find "$ROOT/services" "$ROOT/crates" "$ROOT/bin" -name Cargo.toml 2>/dev/null | sort)

# --- Phase 4: storage may never depend on cc-fork-choice (D-P4-3) ---
# Explicit named prohibition (not merely an unlisted allowed_deps edge). Filesystem
# first so the negative-test shape works without a lockfile refresh.
# S2-B-01: the same ban applies to crates/storage-core (the extraction target).
for STORAGE_MANIFEST in \
  "$ROOT/services/storage/Cargo.toml" \
  "$ROOT/crates/storage-core/Cargo.toml"
do
  if [[ -f "$STORAGE_MANIFEST" ]]; then
    if grep -qE \
      '^[[:space:]]*cc-fork-choice[[:space:]]*=|^[[:space:]]*\[dependencies\.cc-fork-choice\]' \
      "$STORAGE_MANIFEST"; then
      echo "error: ${STORAGE_MANIFEST#"$ROOT"/} may never depend on cc-fork-choice" >&2
      EARLY_FAILED=1
    fi
  fi
done

# --- S1-A-13: cc-chain ↛ cc-p2p and cc-p2p ↛ cc-chain ([ARCH] §2.5) ---
# Explicit named prohibition (not merely an unlisted allowed_deps edge). Filesystem
# first so the negative-test shape works without a lockfile refresh.
# Calls must go through cc-seam traits.
CHAIN_MANIFEST="$ROOT/services/chain/Cargo.toml"
if [[ -f "$CHAIN_MANIFEST" ]]; then
  if grep -qE \
    '^[[:space:]]*cc-p2p[[:space:]]*=|^[[:space:]]*\[dependencies\.cc-p2p\]' \
    "$CHAIN_MANIFEST"; then
    echo "error: cc-chain may never depend on cc-p2p" >&2
    EARLY_FAILED=1
  fi
fi
P2P_MANIFEST="$ROOT/services/p2p/Cargo.toml"
if [[ -f "$P2P_MANIFEST" ]]; then
  if grep -qE \
    '^[[:space:]]*cc-chain[[:space:]]*=|^[[:space:]]*\[dependencies\.cc-chain\]' \
    "$P2P_MANIFEST"; then
    echo "error: cc-p2p may never depend on cc-chain" >&2
    EARLY_FAILED=1
  fi
fi

# --- Phase 4: cc-store may depend on cc-types and nothing else permanently (§1.1) ---
# Filesystem scan: key form `cc-foo =` and table form `[dependencies.cc-foo]`
# (aligned with storage→fork-choice early rule; negative-test friendly).
STORE_MANIFEST="$ROOT/crates/store/Cargo.toml"
if [[ -f "$STORE_MANIFEST" ]]; then
  while IFS= read -r hit; do
    [[ -z "$hit" ]] && continue
    dep=""
    if [[ "$hit" =~ ^[[:space:]]*(cc-[a-z0-9-]+)[[:space:]]*= ]]; then
      dep="${BASH_REMATCH[1]}"
    elif [[ "$hit" =~ ^[[:space:]]*\[dependencies\.(cc-[a-z0-9-]+)\] ]]; then
      dep="${BASH_REMATCH[1]}"
    fi
    if [[ -n "$dep" && "$dep" != "cc-types" ]]; then
      echo "error: cc-store: forbidden workspace dependency on $dep" >&2
      EARLY_FAILED=1
    fi
  done < <(grep -E \
    '^[[:space:]]*cc-[a-z0-9-]+[[:space:]]*=|^[[:space:]]*\[dependencies\.cc-[a-z0-9-]+\]' \
    "$STORE_MANIFEST" 2>/dev/null || true)
fi

# --- Phase 4: crates/store must not name consensus containers (§1.1) ----------
# Opaque bytes under typed keys only; cc-types is for Slot/Root/Epoch + meta SSZ.
# S2-B-01: extend the same grep to crates/storage-core/src ([ARCH] §1.5).
# S2-B-02: skip comment/doc lines — moved resume/prune document SSZ peeks by
# name; the ban is on naming the consensus *types*, not on those comments.
# S2-B-03: replay.rs is the one decoder (CC-42 / D-P4-4); it names BeaconState
# and SignedBeaconBlock. Writer/serve/backfill/prune stay opaque-bytes.
for STORE_SRC in "$ROOT/crates/store/src" "$ROOT/crates/storage-core/src"; do
  if [[ -d "$STORE_SRC" ]]; then
    while IFS= read -r hit; do
      [[ -z "$hit" ]] && continue
      echo "error: ${STORE_SRC#"$ROOT"/} must not reference consensus types SignedBeaconBlock|BeaconState|DataColumnSidecar ($hit)" >&2
      EARLY_FAILED=1
    done < <(grep -rn "SignedBeaconBlock\|BeaconState\|DataColumnSidecar" "$STORE_SRC" 2>/dev/null \
      | grep -vE ':[0-9]+:[[:space:]]*//' \
      | grep -v 'storage-core/src/replay.rs:' || true)
  fi
done

# Node-key leaf: a stranger crate must not name the rust crate. services/p2p
# loads the file. crates/storage-core/src/boot.rs is the only storage-core
# file allowed to name it: the composer calls `load_or_create_node_key` there
# so bin/beacon-core does not name the crate. The leaf crate may name itself.
# Comment and doc lines are skipped, same shape as the consensus-type grep above.
while IFS= read -r hit; do
  [[ -z "$hit" ]] && continue
  case "$hit" in
    */services/p2p/*) continue ;;
    */crates/storage-core/src/boot.rs:*) continue ;;
    */crates/node-key/*) continue ;;
  esac
  echo "error: only services/p2p and crates/storage-core/src/boot.rs may name cc_node_key ($hit)" >&2
  EARLY_FAILED=1
done < <(grep -rn "cc_node_key" "$ROOT/crates" "$ROOT/services" "$ROOT/bin" 2>/dev/null \
  | grep -vE ':[0-9]+:[[:space:]]*//' || true)

if [[ "$EARLY_FAILED" -ne 0 ]]; then
  exit 1
fi

METADATA="$(cargo metadata --no-deps --format-version 1 --locked)"

# Allowed intra-workspace edges (Architecture §2.2 / Phase 1 §1.2).
# Empty list = root with no workspace deps.
# Services may take cc-types/cc-crypto for chain, p2p, and engine (Phase 1
# allowance + Phase 3 CC-32b: engine decodes ExecutionPayload SSZ via cc-types).
# cc-driver edge rule removed with the crate at CC-28 (allowed workspace deps were cc-proto, cc-config).
# cc-spec-tests has no workspace edges (zero outgoing); cc-types may take it as a
# test-only harness edge for ssz_static / vector runners (dev-dependency).
allowed_deps() {
  case "$1" in
    cc-types)             echo "cc-spec-tests" ;;
    cc-config)            echo "" ;;
    cc-proto)             echo "" ;;
    # Phase 2: zero workspace deps permanently (Architecture §1.2 / CC-2K).
    cc-libp2p)            echo "" ;;
    cc-crypto)            echo "cc-types" ;;
    cc-bootstrap)         echo "cc-proto cc-config" ;;
    cc-state-transition)  echo "cc-types cc-crypto" ;;
    # CC-34 / invalidation-walk tests: dev-dep on cc-proto for PayloadStatus fixtures
    # (admitted at CC-3Kb audit — edge landed with ea3c079 without a row append).
    cc-fork-choice)       echo "cc-state-transition cc-types cc-crypto cc-proto" ;;
    cc-spec-tests)        echo "" ;;
    # Self-devnet generator (CC-2K member; content is CC-2Ja).
    cc-devnet-gen)        echo "cc-types cc-crypto cc-state-transition cc-config" ;;
    # S1-A-06: append cc-engine-api (direct E3 call; never JWT).
    # S2-A-01: append cc-chain-core (never re-sort).
    cc-chain)             echo "cc-bootstrap cc-config cc-proto cc-types cc-crypto cc-state-transition cc-fork-choice cc-scheduler cc-seam cc-engine-api cc-chain-core" ;;
    # Phase 2: services/p2p may take cc-libp2p (CC-2K / Architecture §1.2).
    # Custody-vector dev-dep (services/p2p/tests/custody_subset.rs). Append-only.
    # Node-key leaf appended (p2p loads the shared secret file). Not re-sorted.
    cc-p2p)               echo "cc-bootstrap cc-config cc-proto cc-types cc-crypto cc-libp2p cc-seam cc-spec-tests cc-node-key" ;;
    cc-attestation)       echo "cc-bootstrap cc-config cc-proto" ;;
    # CC-32b: append cc-types (never re-sort). CC-37b: append cc-crypto (never re-sort).
    # S1-A-02: append cc-engine-api (transport + config move).
    cc-engine)            echo "cc-bootstrap cc-config cc-proto cc-types cc-crypto cc-engine-api" ;;
    cc-beacon-api)        echo "cc-bootstrap cc-config cc-proto" ;;
    # Phase 4 store DAG: permanent cc-types-only rule; storage gains cc-store + ST.
    cc-store)             echo "cc-types" ;;
    cc-store-bench)       echo "cc-store" ;;
    cc-serve-probe)       echo "cc-libp2p cc-types cc-config" ;;
    # S2-B-01: append cc-storage-core (writer + serve read paths).
    # S2-B-03: thin shim — remaining workspace edges moved onto cc-storage-core.
    cc-storage)           echo "cc-types cc-store cc-storage-core" ;;
    # CC-4J: offline tool; append-only (Amendment 8) — edge set is cc-store only.
    cc-store-tool)        echo "cc-store" ;;
    # S0-A-13: leaf crate, no workspace deps ([ARCH] §1.5 / §3.1).
    cc-scheduler)         echo "" ;;
    # S1-A-01: skeleton, no workspace deps. JWT/HTTP isolation re-points here.
    # S1-A-05: append cc-types cc-crypto (methods + fastpath).
    cc-engine-api)        echo "cc-types cc-crypto" ;;
    # S1-A-09: Ipc wraps tonic; cc-proto is the live wire schema ([ARCH] §2.5).
    cc-seam)              echo "cc-proto" ;;
    # S2-A-01: moved core/import/apply_attestations.
    # S2-A-02: append cc-engine-api (DirectEngine; never JWT).
    # S2-A-03: remaining siblings compile here (not via #[path] from cc-chain).
    # S2-A-04: append cc-seam (`ArchiveWrite` handle; never a storage type).
    cc-chain-core)        echo "cc-types cc-crypto cc-state-transition cc-fork-choice cc-scheduler cc-proto cc-bootstrap cc-engine-api cc-seam" ;;
    # S2-B-01: writer + serve tests need store/proto/bootstrap/types.
    # S2-B-02: backfill/prune/durable_set/resume join the same crate.
    # S2-B-03: append cc-config (boot) + cc-state-transition (replay); never re-sort.
    # S2-A-05: append cc-seam (ArchiveWrite ingest; never re-sort).
    # Not JWT/HTTP-grandfathered.
    cc-storage-core)      echo "cc-store cc-proto cc-bootstrap cc-types cc-config cc-state-transition cc-seam cc-node-key" ;;
    # S2-J-01: thin composer. Not JWT/HTTP-grandfathered. cc-chain is checkpoint
    # sync only (HTTP stays in that crate).
    # Composer ArchiveWrite call. Dev-deps: signing + state transition for the
    # anchor fixture (append-only; never re-sort).
    cc-beacon-core)       echo "cc-bootstrap cc-config cc-proto cc-types cc-chain-core cc-storage-core cc-engine-api cc-chain cc-seam cc-crypto cc-state-transition cc-fork-choice" ;;
    # S2-A-13: proto-free in-process boot harness. Never proto / tonic / JWT.
    cc-beacon-inproc)     echo "cc-store cc-types" ;;
    # S2-A-14: proto-free import → durable test crate. Never proto / tonic / JWT.
    cc-beacon-import)     echo "cc-beacon-inproc cc-store cc-types cc-fork-choice cc-state-transition cc-storage-core cc-seam cc-crypto" ;;
    # Node-key leaf. Echoes cc-types (fingerprint hash newtype). Not cc-crypto.
    cc-node-key)          echo "cc-types" ;;
    *)
      echo "error: unknown workspace member: $1" >&2
      return 1
      ;;
  esac
}

is_allowed() {
  # $1 = depender, $2 = dependee
  local allow a
  allow="$(allowed_deps "$1")" || return 1
  for a in $allow; do
    if [[ "$a" == "$2" ]]; then
      return 0
    fi
  done
  return 1
}

# Direct path deps (normal + dev + build). Same surface as the DAG walk, so
# --check-unused cannot disagree with the ceiling check about what an edge is.
path_deps_for() {
  echo "$METADATA" | jq -r --arg n "$1" '
    .packages[]
    | select(.name == $n and .source == null)
    | .dependencies[]?
    | select(.path != null)
    | .name
  ' | sort -u
}

# Workspace member package names (path packages under this workspace only).
MEMBERS=()
while IFS= read -r name; do
  MEMBERS+=("$name")
done < <(echo "$METADATA" | jq -r '
  .packages[]
  | select(.source == null)
  | .name
' | sort)

if [[ ${#MEMBERS[@]} -eq 0 ]]; then
  echo "error: no workspace members found" >&2
  exit 1
fi

# Opt-in: an allowlist entry can land and never be consumed (Q-1 / S1-B-22).
# Default CI stays ceiling-only so unused edges do not block.
if [[ "$ARG" == "--check-unused" ]]; then
  unused_n=0
  unused_failed=0
  for pkg in "${MEMBERS[@]}"; do
    if ! allow="$(allowed_deps "$pkg")"; then
      unused_failed=1
      continue
    fi
    deps="$(path_deps_for "$pkg")"
    for a in $allow; do
      if ! printf '%s\n' "$deps" | grep -qxF "$a"; then
        echo "error: $pkg: unused allowed_deps entry $a (no cargo metadata edge)" >&2
        unused_n=$((unused_n + 1))
        unused_failed=1
      fi
    done
  done
  if [[ "$unused_n" -ne 0 ]]; then
    echo "error: allowed_deps is not minimal (${unused_n} unused entries; not deleted)" >&2
  fi
  if [[ "$unused_failed" -ne 0 ]]; then
    exit 1
  fi
  echo "check-crate-dag: allowlist is minimal (${#MEMBERS[@]} members)"
  exit 0
fi

member_list() {
  printf '%s\n' "${MEMBERS[@]}"
}

is_member() {
  member_list | grep -qxF "$1"
}

manifest_for() {
  echo "$METADATA" | jq -r --arg n "$1" '
    .packages[]
    | select(.name == $n and .source == null)
    | .manifest_path
  '
}

# --- (c) rust-version: workspace pin equals rust-toolchain.toml channel ---
TOOLCHAIN_FILE="$ROOT/rust-toolchain.toml"
if [[ ! -f "$TOOLCHAIN_FILE" ]]; then
  echo "error: missing rust-toolchain.toml" >&2
  exit 1
fi
CHANNEL="$(awk -F'"' '/^[[:space:]]*channel[[:space:]]*=/ { print $2; exit }' "$TOOLCHAIN_FILE")"
# Exact 1.XX.Y only — never stable/beta/nightly or partial pins (Architecture §2.4, R-6).
if [[ -z "$CHANNEL" ]] || ! [[ "$CHANNEL" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "error: rust-toolchain.toml channel must be an exact 1.XX.Y (got: ${CHANNEL:-empty})" >&2
  exit 1
fi

WS_RUST_VERSION="$(awk -F'"' '/^[[:space:]]*rust-version[[:space:]]*=/ { print $2; exit }' "$ROOT/Cargo.toml")"
if [[ "$WS_RUST_VERSION" != "$CHANNEL" ]]; then
  echo "error: [workspace.package] rust-version ($WS_RUST_VERSION) != rust-toolchain.toml channel ($CHANNEL)" >&2
  exit 1
fi

FAILED=0

for pkg in "${MEMBERS[@]}"; do
  # Reject unknown members early via allowed_deps.
  if ! allowed_deps "$pkg" >/dev/null; then
    FAILED=1
    continue
  fi

  manifest="$(manifest_for "$pkg")"
  if [[ -z "$manifest" || ! -f "$manifest" ]]; then
    echo "error: no manifest for $pkg" >&2
    FAILED=1
    continue
  fi

  # --- (b) [lints] workspace = true ---
  if ! grep -qE '^[[:space:]]*\[lints\]' "$manifest"; then
    echo "error: $pkg: missing [lints] table (need workspace = true)" >&2
    FAILED=1
  elif ! awk '
    /^[[:space:]]*\[lints\]/ { in_lints=1; next }
    /^[[:space:]]*\[/ { in_lints=0 }
    in_lints && /^[[:space:]]*workspace[[:space:]]*=[[:space:]]*true([[:space:]]|#|$)/ { found=1 }
    END { exit !found }
  ' "$manifest"; then
    echo "error: $pkg: [lints] must set workspace = true" >&2
    FAILED=1
  fi

  # --- (c) package rust-version inherits from workspace ---
  pkg_rv="$(echo "$METADATA" | jq -r --arg n "$pkg" '
    .packages[] | select(.name == $n and .source == null) | .rust_version // empty
  ')"
  if [[ -z "$pkg_rv" ]]; then
    echo "error: $pkg: rust-version not set (must inherit workspace rust-version = $CHANNEL)" >&2
    FAILED=1
  elif [[ "$pkg_rv" != "$CHANNEL" ]]; then
    echo "error: $pkg: rust-version ($pkg_rv) != workspace/toolchain ($CHANNEL)" >&2
    FAILED=1
  fi
  if grep -qE '^[[:space:]]*rust-version\.workspace[[:space:]]*=[[:space:]]*true([[:space:]]|#|$)' "$manifest"; then
    : # inherits workspace — required form
  elif grep -qE '^[[:space:]]*rust-version[[:space:]]*=' "$manifest"; then
    lit="$(awk -F'"' '/^[[:space:]]*rust-version[[:space:]]*=/ { print $2; exit }' "$manifest")"
    if [[ "$lit" != "$CHANNEL" ]]; then
      echo "error: $pkg: literal rust-version ($lit) != $CHANNEL; use rust-version.workspace = true" >&2
      FAILED=1
    fi
  else
    echo "error: $pkg: missing rust-version.workspace = true in [package]" >&2
    FAILED=1
  fi

  # --- (a) intra-workspace edges must be in the allowed table ---
  deps="$(path_deps_for "$pkg")"

  while IFS= read -r dep; do
    [[ -z "$dep" ]] && continue
    # Only enforce edges to other workspace members.
    if ! is_member "$dep"; then
      continue
    fi
    if ! is_allowed "$pkg" "$dep"; then
      echo "error: $pkg: forbidden workspace dependency on $dep" >&2
      FAILED=1
    fi
  done <<< "$deps"
done

# --- S1-A-13: named cc-chain ↛ cc-p2p / cc-p2p ↛ cc-chain (metadata) ----------
# Independent of allowed_deps so an allowlist append cannot silence the ban.
# Uses --no-deps metadata (package .dependencies[].name): `package = "cc-p2p"`,
# workspace-dot, [dev-dependencies.cc-p2p], build-dep and target-specific
# tables all resolve here (JWT walk shape). Filesystem grep above is the
# house-style backup only. Runs before the full-graph --locked walk so a
# lockfile refresh cannot skip the named error.
while IFS= read -r line; do
  [[ -z "$line" ]] && continue
  pkg="${line%%$'\t'*}"
  dep="${line#*$'\t'}"
  if chain_p2p_allowed "$pkg" "$dep"; then
    continue
  fi
  echo "error: $pkg may never depend on $dep" >&2
  FAILED=1
done < <(echo "$METADATA" | jq -r '
  .packages[]
  | select(.source == null)
  | . as $p
  | .dependencies[]?
  | select(.name == "cc-chain" or .name == "cc-p2p")
  | "\($p.name)\t\(.name)"
')

# --- CC-20/1: only cc-libp2p may declare a libp2p* dependency ---
# Workspace root may pin libp2p in [workspace.dependencies]; every path package
# other than cc-libp2p is forbidden from naming libp2p* in any dependency table.
# Uses cargo metadata (normal + dev + build) so feature-gated / renamed edges count.
while IFS= read -r line; do
  [[ -z "$line" ]] && continue
  pkg="${line%%$'\t'*}"
  dep="${line#*$'\t'}"
  if [[ "$pkg" == "cc-libp2p" ]]; then
    continue
  fi
  echo "error: $pkg: only cc-libp2p may declare a libp2p* dependency (found $dep)" >&2
  FAILED=1
done < <(echo "$METADATA" | jq -r '
  .packages[]
  | select(.source == null)
  | . as $p
  | .dependencies[]?
  | select(.name | test("^libp2p"))
  | "\($p.name)\t\(.name)"
')

# Manifest scan backup: any libp2p* key or [dependencies.libp2p*] table outside
# crates/libp2p and the workspace root pin. Process substitution — no /tmp file.
while IFS= read -r manifest; do
  [[ -z "$manifest" ]] && continue
  case "$manifest" in
    */crates/libp2p/Cargo.toml) continue ;;
    "$ROOT/Cargo.toml") continue ;;
  esac
  while IFS= read -r hit; do
    [[ -z "$hit" ]] && continue
    echo "error: ${manifest#"$ROOT"/}: only cc-libp2p may declare a libp2p* dependency ($hit)" >&2
    FAILED=1
  done < <(grep -nE \
    '^[[:space:]]*libp2p[a-zA-Z0-9_-]*[[:space:]]*=|^[[:space:]]*\[dependencies\.libp2p[a-zA-Z0-9_-]*\]' \
    "$manifest" 2>/dev/null || true)
done < <(echo "$METADATA" | jq -r '
  .packages[]
  | select(.source == null)
  | .manifest_path
' | sort -u)

# --- CC-20/1 pin integrity: direct libp2p* deps must resolve to the workspace git rev ---
# Parse 40-hex rev from root [workspace.dependencies] libp2p = { git = "...", rev = "..." }.
EXPECTED_LIBP2P_REV="$(
  awk '
    /^[[:space:]]*libp2p[[:space:]]*=[[:space:]]*\{/ {
      line = $0
      # Multi-line table: keep reading until closing brace line if needed.
      while (line !~ /\}/ && (getline nxt) > 0) { line = line " " nxt }
      if (match(line, /rev[[:space:]]*=[[:space:]]*"[0-9a-fA-F]+"/)) {
        s = substr(line, RSTART, RLENGTH)
        sub(/^rev[[:space:]]*=[[:space:]]*"/, "", s)
        sub(/"$/, "", s)
        print s
        exit
      }
    }
  ' "$ROOT/Cargo.toml"
)"
if [[ -z "$EXPECTED_LIBP2P_REV" ]]; then
  echo "error: root Cargo.toml: could not parse libp2p workspace pin rev= (need git pin with 40-hex rev)" >&2
  FAILED=1
elif ! [[ "$EXPECTED_LIBP2P_REV" =~ ^[0-9a-f]{40}$ ]]; then
  echo "error: root Cargo.toml: libp2p rev must be lowercase 40-hex (got: $EXPECTED_LIBP2P_REV)" >&2
  FAILED=1
else
  # Reject branch=/tag= form on the workspace pin (rev-only).
  if awk '
    /^[[:space:]]*libp2p[[:space:]]*=[[:space:]]*\{/ {
      line = $0
      while (line !~ /\}/ && (getline nxt) > 0) { line = line " " nxt }
      if (line ~ /branch[[:space:]]*=/ || line ~ /tag[[:space:]]*=/) { found=1 }
    }
    END { exit !found }
  ' "$ROOT/Cargo.toml"; then
    echo "error: root Cargo.toml: libp2p pin must use rev= only (no branch=/tag=)" >&2
    FAILED=1
  fi
  if ! grep -qE 'libp2p[[:space:]]*=[[:space:]]*\{[^}]*git[[:space:]]*=[[:space:]]*"https://github.com/libp2p/rust-libp2p"' "$ROOT/Cargo.toml" \
    && ! awk '
      /^[[:space:]]*libp2p[[:space:]]*=[[:space:]]*\{/ {
        line = $0
        while (line !~ /\}/ && (getline nxt) > 0) { line = line " " nxt }
        if (line ~ /git[[:space:]]*=[[:space:]]*"https:\/\/github.com\/libp2p\/rust-libp2p"/) { found=1 }
      }
      END { exit !found }
    ' "$ROOT/Cargo.toml"; then
    echo "error: root Cargo.toml: libp2p must be git-pinned to https://github.com/libp2p/rust-libp2p" >&2
    FAILED=1
  fi

  EXPECTED_LIBP2P_SOURCE="git+https://github.com/libp2p/rust-libp2p?rev=${EXPECTED_LIBP2P_REV}"
  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    pkg="${line%%$'\t'*}"
    rest="${line#*$'\t'}"
    dep="${rest%%$'\t'*}"
    src="${rest#*$'\t'}"
    if [[ "$src" != "$EXPECTED_LIBP2P_SOURCE" && "$src" != "${EXPECTED_LIBP2P_SOURCE}#${EXPECTED_LIBP2P_REV}" ]]; then
      # cargo metadata may omit #fragment; accept either form. Reject crates.io / other revs.
      if [[ "$src" != "$EXPECTED_LIBP2P_SOURCE"* ]]; then
        echo "error: $pkg: direct dependency $dep must resolve to workspace git pin" >&2
        echo "error:   expected source prefix: $EXPECTED_LIBP2P_SOURCE" >&2
        echo "error:   got: ${src:-<empty/registry>}" >&2
        FAILED=1
      fi
    fi
  done < <(echo "$METADATA" | jq -r '
    .packages[]
    | select(.source == null)
    | . as $p
    | .dependencies[]?
    | select(.name | test("^libp2p"))
    | "\($p.name)\t\(.name)\t\(.source // "")"
  ')

  # Resolved graph: any package named exactly "libp2p" in the lock must be the pin
  # (catches lockfile drift vs declaration). Hybrid crates.io libp2p-identity is OK.
  FULL_META="$(cargo metadata --format-version 1 --locked)"
  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    name="${line%%$'\t'*}"
    src="${line#*$'\t'}"
    if [[ "$name" == "libp2p" ]]; then
      if [[ "$src" != "$EXPECTED_LIBP2P_SOURCE"* ]]; then
        echo "error: resolved package libp2p is not the workspace git pin" >&2
        echo "error:   expected source prefix: $EXPECTED_LIBP2P_SOURCE" >&2
        echo "error:   got: $src" >&2
        FAILED=1
      fi
    elif [[ "$name" =~ ^libp2p- ]] && [[ "$src" == git+https://github.com/libp2p/rust-libp2p* ]]; then
      # Protocol crates from the monorepo must share the same rev.
      if [[ "$src" != "$EXPECTED_LIBP2P_SOURCE"* ]]; then
        echo "error: resolved $name is from rust-libp2p but not the pinned rev" >&2
        echo "error:   expected source prefix: $EXPECTED_LIBP2P_SOURCE" >&2
        echo "error:   got: $src" >&2
        FAILED=1
      fi
    fi
  done < <(echo "$FULL_META" | jq -r '
    .packages[]
    | select(.name | test("^libp2p"))
    | "\(.name)\t\(.source // "")"
  ')

  # Docs + greppable constant must match Cargo.toml rev (OQ-7 compensating control).
  for doc in "$ROOT/docs/p2p-dependencies.md" "$ROOT/docs/supply-chain.md"; do
    if [[ ! -f "$doc" ]]; then
      echo "error: missing $doc (must record libp2p rev)" >&2
      FAILED=1
    elif ! grep -qF "$EXPECTED_LIBP2P_REV" "$doc"; then
      echo "error: ${doc#"$ROOT"/}: does not contain libp2p rev $EXPECTED_LIBP2P_REV (OQ-7 / docs drift)" >&2
      FAILED=1
    fi
  done
  LIBRS="$ROOT/crates/libp2p/src/lib.rs"
  if [[ ! -f "$LIBRS" ]]; then
    echo "error: missing crates/libp2p/src/lib.rs (need pub const LIBP2P_GIT_REV)" >&2
    FAILED=1
  elif ! grep -qE "pub const LIBP2P_GIT_REV: &str = \"${EXPECTED_LIBP2P_REV}\"" "$LIBRS"; then
    echo "error: crates/libp2p/src/lib.rs: LIBP2P_GIT_REV must equal Cargo.toml rev $EXPECTED_LIBP2P_REV" >&2
    FAILED=1
  fi
fi

# --- ADR P3-16 / [ARCH] §6.2: metadata walk + manifest-scan backup ------------
# http_or_jwt_allowed / http_jwt_manifest_hits defined above (S1-A-01 re-point).
# Grandfathered HTTP (never JWT): cc-chain checkpoint-sync + test hyper
# (CC-15 / CC-28); cc-bootstrap metrics/health server (Phase 0).
while IFS= read -r line; do
  [[ -z "$line" ]] && continue
  pkg="${line%%$'\t'*}"
  dep="${line#*$'\t'}"
  if http_or_jwt_allowed "$pkg" "$dep"; then
    continue
  fi
  echo "error: $pkg: only cc-engine-api may declare an HTTP client or JWT signer (ADR P3-16; found $dep)" >&2
  FAILED=1
done < <(echo "$METADATA" | jq -r --arg re "$FORBIDDEN_OUTSIDE_ENGINE" '
  .packages[]
  | select(.source == null)
  | . as $p
  | .dependencies[]?
  | select(.name | test($re))
  | "\($p.name)\t\(.name)"
')

# ADR-R-03 / S1-A-03: no crate-public JwtSecret. jwt.rs keeps `pub struct`
# (verbatim move; module is private). lib.rs must not re-export it.
ENGINE_API_LIB="$ROOT/crates/engine-api/src/lib.rs"
if [[ ! -f "$ENGINE_API_LIB" ]]; then
  echo "error: missing crates/engine-api/src/lib.rs (JwtSecret export grep)" >&2
  FAILED=1
elif grep -nE \
  '^[[:space:]]*pub[[:space:]]+(mod[[:space:]]+jwt\b|use[[:space:]].*\bjwt\b|use[[:space:]].*\bJwtSecret\b)' \
  "$ENGINE_API_LIB"; then
  echo "error: cc-engine-api must not publicly export jwt / JwtSecret (ADR-R-03)" >&2
  FAILED=1
fi

# Manifest-scan backup (renamed keys / direct tables). Workspace root pin OK;
# cc-engine-api + transitional cc-engine OK; chain/bootstrap HTTP-only.
while IFS= read -r manifest; do
  [[ -z "$manifest" ]] && continue
  rel="${manifest#"$ROOT"/}"
  while IFS= read -r hit; do
    [[ -z "$hit" ]] && continue
    echo "error: ${rel}: only cc-engine-api may declare an HTTP client or JWT signer (ADR P3-16; $hit)" >&2
    FAILED=1
  done < <(http_jwt_manifest_hits "$manifest" "$rel")
done < <(echo "$METADATA" | jq -r '
  .packages[]
  | select(.source == null)
  | .manifest_path
' | sort -u)

if [[ "$FAILED" -ne 0 ]]; then
  exit 1
fi

echo "check-crate-dag: ok (${#MEMBERS[@]} members, channel $CHANNEL, libp2p rev ${EXPECTED_LIBP2P_REV:-n/a})"
