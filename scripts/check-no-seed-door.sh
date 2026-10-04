#!/usr/bin/env bash
# Tests must not name the writer seed door.
#
# Fails if any Rust file under bin/beacon-core/tests or
# crates/beacon-import/tests names `submit_p0_committed`,
# `blocking_submit_p0_committed`, or `StagedBlock`.
# Production sources are not scanned: those names stay crate-private on the
# writer. A comment or a string counts — the identifier is the door.
#
# Fixtures under scripts/fixtures/check-no-seed-door/ run first. They live
# outside the production find so they cannot weaken it.
# Exit non-zero and print every hit on failure. `make ci` runs this via `lint`.
set -euo pipefail

export LC_ALL=C

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Print `path:line:text` hits. Exit 0 only when none of the door names appear.
check_tree() {
  local root="$1"
  local -a files=()
  local rel dir
  for rel in bin/beacon-core/tests crates/beacon-import/tests; do
    dir="${root}/${rel}"
    if [[ -d "${dir}" ]]; then
      while IFS= read -r -d '' f; do
        files+=("$f")
      done < <(find "${dir}" -name '*.rs' -print0)
    fi
  done

  if [[ ${#files[@]} -eq 0 ]]; then
    return 0
  fi

  local hits
  hits="$(grep -n -E '(^|[^A-Za-z0-9_])(blocking_submit_p0_committed|submit_p0_committed|StagedBlock)([^A-Za-z0-9_]|$)' "${files[@]}" || true)"
  if [[ -z "${hits}" ]]; then
    return 0
  fi
  echo "error: test names the writer seed door (blocking_submit_p0_committed, submit_p0_committed, or StagedBlock):" >&2
  echo "${hits}" >&2
  return 1
}

# ── Fixture self-test ───────────────────────────────────────────────────────
FIXTURE_ROOT="${ROOT}/scripts/fixtures/check-no-seed-door"
FAIL_DIR="${FIXTURE_ROOT}/expect-fail"
PASS_DIR="${FIXTURE_ROOT}/expect-pass"

if [[ ! -d "${FAIL_DIR}" || ! -d "${PASS_DIR}" ]]; then
  echo "error: missing fixture dirs under ${FIXTURE_ROOT#"$ROOT"/}" >&2
  exit 1
fi

selftest_failed=0
n_fail=0
for tree in "${FAIL_DIR}"/*; do
  [[ -d "${tree}" ]] || continue
  n_fail=$((n_fail + 1))
  if check_tree "${tree}" >/dev/null 2>&1; then
    echo "error: self-test: naming the seed door must fail: ${tree#"$ROOT"/}" >&2
    selftest_failed=1
  fi
done

n_pass=0
for tree in "${PASS_DIR}"/*; do
  [[ -d "${tree}" ]] || continue
  n_pass=$((n_pass + 1))
  if ! check_tree "${tree}"; then
    echo "error: self-test: clean tests must pass: ${tree#"$ROOT"/}" >&2
    selftest_failed=1
  fi
done

if [[ "${n_fail}" -lt 1 ]]; then
  echo "error: self-test: need a negative fixture under ${FAIL_DIR#"$ROOT"/}" >&2
  selftest_failed=1
fi
if [[ "${n_pass}" -lt 1 ]]; then
  echo "error: self-test: need a positive fixture under ${PASS_DIR#"$ROOT"/}" >&2
  selftest_failed=1
fi

if [[ "${selftest_failed}" -ne 0 ]]; then
  exit 1
fi

# ── Production scan ────────────────────────────────────────────────────────
if ! check_tree "${ROOT}"; then
  echo >&2
  echo "hint: import tests enter through boot() and commit_import." >&2
  echo "      blocking_submit_p0_committed, submit_p0_committed, and StagedBlock stay inside cc-storage-core." >&2
  exit 1
fi

echo "ok: beacon-core and beacon-import tests do not name the writer seed door"
