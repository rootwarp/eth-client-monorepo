#!/usr/bin/env bash
# Exactly one non-test commit_anchor( call, and that call is under
# bin/beacon-core/src/. A second caller is an explicit decision (R-15).
#
# Counts call expressions, not `fn commit_anchor` definitions:
#   .commit_anchor(   ::commit_anchor(   with optional space before '('
#   Whitespace or a newline may sit between `.` / `::` and the name.
#   A name that ends the line counts (the '(' is on the next line, or missing).
# Each non-overlapping match is one hit, so two calls on one line count as two.
# A one-line #[cfg(test)] item does not hide the rest of that line.
#
# Exempt: the item #[cfg(test)] annotates (same brace walker as
# check-no-env-reads.sh) and integration-test roots only
# (bin/*/tests, crates/*/tests, services/*/tests). A call under src/tests/
# is production and counts.
#
# Fixtures under scripts/fixtures/check-one-commit-anchor/ run first. They
# live outside the production find so they cannot weaken it.
# Exit non-zero and print every hit on failure. `make ci` runs this via `lint`.
set -euo pipefail

# Byte-wise scan. A UTF-8 locale makes BSD awk warn on non-ASCII source bytes
# (for example an em dash) and can split a character across substr calls.
export LC_ALL=C

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# R-5 / CC-45b: exempt only the item that #[cfg(test)] annotates, then resume.
# The previous `skip=1` never reset, so one test attribute silently exempted
# the rest of the file (mod foo; / use x; / the body after `}`).
# A one-line `{…}` body (rustfmt `mod tests {}`) is net-zero braces and has no
# `;` — treat that as end-of-item so the next production item is scanned.
# A `;` inside `//` / `///` between the attribute and the item is not the item.
# When the item ends mid-line, the suffix is scanned in the same pass.
#
# Brace depth ignores `{` / `}` inside strings, char/byte lits (`'{'`, `b'{'`),
# line comments, block comments, and raw strings. A live witness was
# services/p2p/src/reqresp/status.rs (`split('{')`) leaving skip_depth=1 to EOF.
#
# Call sites are `.commit_anchor` / `::commit_anchor`. Whitespace, including a
# newline, may sit between the operator and the name. Optional whitespace then
# `(`, or the end of the line, finishes the call. A definition
# (`fn commit_anchor`) matches neither. Full-line `//` comments and comment
# tails are not calls, so a note cannot look like a second caller.
# A trailing `.` or `::` in code is kept as the tail across the newline.
#
# Match #[cfg(test)] only while the lexer is in code. A continued block
# comment or raw string must not start an exemption. An attribute line that
# is only the attribute does not end the item.
# shellcheck disable=SC2016  # awk program is a literal; $0 etc. are awk, not bash
AWK_SCAN='
function reset_lex() {
  lx = 0
  raw_n = 0
  prev = ""
}

# i points at the opening quote. Char lits: quote + char + quote, or a backslash escape.
# Lifetimes (quote + ident, no closer) must not consume the rest of the line.
function skip_char_or_lifetime(s, i,    n1, n2) {
  n1 = substr(s, i + 1, 1)
  n2 = substr(s, i + 2, 1)
  if (n1 == "\\") {
    i += 2
    if (i <= length(s)) i++
    while (i <= length(s)) {
      if (substr(s, i, 1) == SQ) { i++; break }
      i++
    }
    return i
  }
  if (n2 == SQ) return i + 3
  i++
  while (i <= length(s) && substr(s, i, 1) ~ /[[:alnum:]_]/) i++
  return i
}

# One lexer step. lx/raw_n/prev persist. Sets step to cont|punct|brk|eot and ch for punct.
function step_lex(s, i,    c, nxt, j, hashes, ok) {
  if (i > length(s)) { step = "eot"; return i }
  c = substr(s, i, 1)
  nxt = substr(s, i + 1, 1)
  if (lx == 3) {
    if (c == "*" && nxt == "/") { lx = 0; prev = "/"; step = "cont"; return i + 2 }
    prev = c
    step = "cont"
    return i + 1
  }
  if (lx == 1) {
    if (c == "\\") { prev = ""; step = "cont"; return i + 2 }
    if (c == "\"") { lx = 0; prev = "\""; step = "cont"; return i + 1 }
    prev = c
    step = "cont"
    return i + 1
  }
  if (lx == 2) {
    if (c == "\"") {
      ok = 1
      for (j = 1; j <= raw_n; j++) {
        if (substr(s, i + j, 1) != "#") { ok = 0; break }
      }
      if (ok) { lx = 0; prev = "#"; step = "cont"; return i + 1 + raw_n }
    }
    prev = c
    step = "cont"
    return i + 1
  }
  if (c == "/" && nxt == "/") { step = "brk"; return i }
  if (c == "/" && nxt == "*") { lx = 3; prev = "*"; step = "cont"; return i + 2 }
  if (prev !~ /[[:alnum:]_]/) {
    if (c == "b" && nxt == SQ) {
      i = skip_char_or_lifetime(s, i + 1)
      prev = SQ
      step = "cont"
      return i
    }
    if (c == "b" && nxt == "\"") { lx = 1; prev = "\""; step = "cont"; return i + 2 }
    if (c == "b" && nxt == "r") {
      hashes = 0
      j = i + 2
      while (substr(s, j, 1) == "#") { hashes++; j++ }
      if (substr(s, j, 1) == "\"") {
        lx = 2
        raw_n = hashes
        prev = "\""
        step = "cont"
        return j + 1
      }
    }
    if (c == "r") {
      hashes = 0
      j = i + 1
      while (substr(s, j, 1) == "#") { hashes++; j++ }
      if (substr(s, j, 1) == "\"") {
        lx = 2
        raw_n = hashes
        prev = "\""
        step = "cont"
        return j + 1
      }
    }
    if (c == "\"") { lx = 1; prev = "\""; step = "cont"; return i + 1 }
    if (c == SQ) {
      i = skip_char_or_lifetime(s, i)
      prev = SQ
      step = "cont"
      return i
    }
  }
  prev = c
  ch = c
  step = "punct"
  return i + 1
}

function remember(c) {
  tail = tail c
  if (length(tail) > 80) tail = substr(tail, length(tail) - 79)
}

# `.` or `::`, then optional whitespace, then the name `commit_anchor`.
function call_name_ended(t,    n, i, name) {
  name = "commit_anchor"
  n = length(name)
  if (length(t) < n) return 0
  if (substr(t, length(t) - n + 1) != name) return 0
  i = length(t) - n
  while (i > 0 && is_ws(substr(t, i, 1))) i--
  if (i <= 0) return 0
  if (substr(t, i, 1) == ".") return 1
  if (i >= 2 && substr(t, i - 1, 2) == "::") return 1
  return 0
}

# Operator still open for a name on a later line (optional trailing whitespace).
function dangling_op(t,    n) {
  n = length(t)
  while (n > 0 && is_ws(substr(t, n, 1))) n--
  if (n <= 0) return 0
  if (substr(t, n, 1) == ".") return 1
  if (n >= 2 && substr(t, n - 1, 2) == "::") return 1
  return 0
}

function is_ident(c) {
  return c ~ /[[:alnum:]_]/
}

function is_ws(c) {
  return c == " " || c == "\t" || c == "\r"
}

# True when optional whitespace, then "(" or end-of-line or a comment.
function accept_call(s, i,    j, c, n) {
  j = i
  while (j <= length(s) && is_ws(substr(s, j, 1))) j++
  if (j > length(s)) return 1
  c = substr(s, j, 1)
  n = substr(s, j + 1, 1)
  if (c == "/" && (n == "/" || n == "*")) return 1
  if (c == "(") return 1
  return 0
}

function next_is_ident(s, i) {
  if (i > length(s)) return 0
  return is_ident(substr(s, i, 1))
}

function at_cfg_test(s, i) {
  return substr(s, i) ~ /^[[:space:]]*#\[cfg\(test\)\]/
}

# Walk an open #[cfg(test)] item starting at i. Returns the index after the
# item when it ends on this line, or 0 when the item continues past this line.
function consume_exempt(s, i,    ni, depth, seen_brace) {
  depth = skip_depth
  seen_brace = (skip_depth > 0) ? 1 : 0
  while (i <= length(s)) {
    ni = step_lex(s, i)
    if (step == "brk" || step == "eot") break
    if (step == "cont") { i = ni; continue }
    if (ch == "{") {
      depth++
      seen_brace = 1
    } else if (ch == "}") {
      if (depth > 0) depth--
      if (seen_brace && depth == 0) {
        skip_depth = 0
        pending = 0
        reset_lex()
        return ni
      }
    } else if (ch == ";" && depth == 0 && seen_brace == 0) {
      skip_depth = 0
      pending = 0
      reset_lex()
      return ni
    }
    i = ni
  }
  if (depth > 0) {
    skip_depth = depth
    pending = 0
  } else {
    pending = 1
    skip_depth = 0
  }
  return 0
}

# Scan production code from i. Returns 0 at end of line, or the index of a
# later #[cfg(test)] so the caller can exempt that item and keep going.
function scan_production(s, i,    ni) {
  while (i <= length(s)) {
    ni = step_lex(s, i)
    if (step == "brk" || step == "eot") return 0
    if (step == "cont") { i = ni; continue }
    if (ch == "#" && substr(s, i) ~ /^#\[cfg\(test\)\]/) return i
    remember(ch)
    if (call_name_ended(tail) && !next_is_ident(s, ni) && accept_call(s, ni)) {
      print FILENAME ":" FNR ":" $0
      tail = ""
    }
    i = ni
  }
  return 0
}

function process_line(s,    i, ni) {
  i = 1
  # Keep a trailing `.` or `::` across a newline only while still in code.
  # A continued string or comment must not glue that operator to later text.
  if (lx != 0 || !dangling_op(tail)) tail = ""
  if (lx == 0) prev = ""
  while (i <= length(s)) {
    if (pending || skip_depth > 0) {
      ni = consume_exempt(s, i)
      if (ni == 0) return
      i = ni
      tail = ""
      continue
    }
    if (lx == 0 && at_cfg_test(s, i)) {
      pending = 1
      reset_lex()
      continue
    }
    ni = scan_production(s, i)
    if (ni == 0) return
    i = ni
    pending = 1
    reset_lex()
    tail = ""
  }
}

FNR == 1 { skip_depth = 0; pending = 0; reset_lex(); SQ = sprintf("%c", 39); tail = "" }

{ process_line($0) }
'

scan_files() {
  if [[ $# -eq 0 ]]; then
    return 0
  fi
  awk "${AWK_SCAN}" "$@"
}

# Integration-test roots only. src/tests/ is not one of these.
is_integration_test() {
  [[ "$1" =~ /(bin|crates|services)/[^/]+/tests/ ]]
}

# Print hits. Exit 0 only when there is exactly one, under bin/beacon-core/src/.
check_tree() {
  local root="$1"
  local -a search=()
  local d
  for d in bin crates services; do
    if [[ -d "${root}/${d}" ]]; then
      search+=("${root}/${d}")
    fi
  done

  local -a files=()
  if [[ ${#search[@]} -gt 0 ]]; then
    while IFS= read -r -d '' f; do
      if is_integration_test "${f}"; then
        continue
      fi
      files+=("$f")
    done < <(find "${search[@]}" -name '*.rs' -print0)
  fi

  local hits=""
  if [[ ${#files[@]} -gt 0 ]]; then
    hits="$(scan_files "${files[@]}")"
  fi

  local count=0
  local bad=0
  local line path rel
  if [[ -n "${hits}" ]]; then
    while IFS= read -r line; do
      [[ -z "${line}" ]] && continue
      count=$((count + 1))
      path="${line%%:*}"
      rel="${path#"${root}/"}"
      case "${rel}" in
        bin/beacon-core/src/*) ;;
        *) bad=1 ;;
      esac
    done <<< "${hits}"
  fi

  if [[ "${count}" -eq 1 && "${bad}" -eq 0 ]]; then
    return 0
  fi
  echo "error: want exactly one non-test commit_anchor( call under bin/beacon-core/src/ (found ${count}):" >&2
  if [[ -n "${hits}" ]]; then
    echo "${hits}" >&2
  else
    echo "(no calls)" >&2
  fi
  return 1
}

# ── Fixture self-test ───────────────────────────────────────────────────────
FIXTURE_ROOT="${ROOT}/scripts/fixtures/check-one-commit-anchor"
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
    echo "error: self-test: second caller must fail: ${tree#"$ROOT"/}" >&2
    selftest_failed=1
  fi
done

n_pass=0
for tree in "${PASS_DIR}"/*; do
  [[ -d "${tree}" ]] || continue
  n_pass=$((n_pass + 1))
  if ! check_tree "${tree}"; then
    echo "error: self-test: single beacon-core call must pass: ${tree#"$ROOT"/}" >&2
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
  echo "hint: commit_anchor is the beacon-core composer's one call." >&2
  echo "      A second non-test caller is an explicit decision (R-15)." >&2
  exit 1
fi

echo "ok: one non-test commit_anchor( call, under bin/beacon-core/src/"
