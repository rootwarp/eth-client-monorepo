#!/usr/bin/env bash
# S0-B-18 / E0.9 — M3 `Discharged by` column on [PRD] §5.1 and §5.2.
#
# M3's target is "0 open, with the discharging commit or stage recorded per
# row". This gate asserts the artifact: every table in §5.1/§5.2 that has a
# Disposition column also has Discharged by, and every row this stage claimed
# has a non-blank cell.
#
# A claimed patch/write/wire cell is a 12-hex SHA (optionally `SHA → Sn`) or
# an owning issue id that exists as a `### \`S0-…\`` (or `##`) heading in the
# issues directory. `pending` and free-form ids do not discharge. A claimed
# deletion cell is a stage id.
#
# git cat-file is used when the object is present. A missing object (shallow
# actions/checkout fetch-depth 1) is not a failure: a well-formed 12-hex SHA
# is accepted without lookup so lint stays cheap.
#
# CLAIM_STAGES is S0a, S0, S1, S2R, S2. S2R is before S2 so `patch @ S2R` is
# captured as S2R and is not consumed as S2. Later stage-exit issues
# (S3a-B-27, S4c-06) only append a stage.
#
# The planning corpus is not in this tree. Commit 3b02a4c removed plan/prd.md
# and plan/issues/; they now live outside the repo. This script does not
# hardcode that external path.
#
# Usage:
#   bash scripts/check-m3-discharged-by.sh
#       Self-test. If plan/prd.md exists, also scan it against plan/issues/
#       (the layout from before 3b02a4c). If plan/prd.md is absent, print one
#       line that the live ledger is NOT_RUN because the corpus is not in
#       this tree, and exit 0 when the self-test passes. `make lint` and CI
#       call this form. A GitHub runner has no corpus and must not be pointed
#       at a machine-local vault.
#   bash scripts/check-m3-discharged-by.sh --self-test
#       Fixtures only.
#   bash scripts/check-m3-discharged-by.sh --prd <file> --issues <dir>
#       Self-test, then parse <file> and load `### \`S…\`` headings from
#       <dir>. The files do not need to sit under plan/. Paths may be
#       absolute or relative to the repository root.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! command -v python3 >/dev/null 2>&1; then
  echo "error: python3 is required" >&2
  exit 1
fi

exec python3 - "$ROOT" "$@" <<'PY'
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(sys.argv[1])
ARGS = sys.argv[2:]
LIVE = ROOT / "plan" / "prd.md"
ISSUES_DIR = ROOT / "plan" / "issues"
FIXTURE_ROOT = ROOT / "scripts" / "fixtures" / "check-m3-discharged-by"

# Stages whose claimed rows must have a Discharged-by cell. Later stage
# exits append here (S3a-B-27 → S3/S3a, …). S2R is before S2 so
# `patch @ S2R` is captured as S2R, not by backtracking past S2.
CLAIM_STAGES = ("S0a", "S0", "S1", "S2R", "S2")

# Disposition still names a later stage, but S0 already discharged the row
# ([PLAN] C-11). Keep the extra so a reader tracking @ S4 cannot drop it.
EXTRA_CLAIMED = frozenset({"P1-B/8"})

STAGE_ALT = "S0a|S3a|S3b|S4a|S4b|S4c|S0|S1|S2|S3|S4|S5"
# §5.0 verbs. Optional spaces around @ so `patch@S0` / `patch @S0` stay claimed.
CLAIM_RE = re.compile(
    r"(?<![A-Za-z])(patch|write|wire|deleted|delete)\s*@\s*("
    + "|".join(CLAIM_STAGES)
    + r")\b",
    re.I,
)
SHA12_RE = re.compile(r"\b([0-9a-f]{12})\b", re.I)
# One optional stage letter: S2R-A-01 as well as S0a / S3b / S4c.
ISSUE_RE = re.compile(r"\b(S\d+[A-Za-z]?-[A-Z]+-\d+)\b")
STAGE_RE = re.compile(r"\b(" + STAGE_ALT + r")(?!-[A-Z])\b")
PENDING_RE = re.compile(r"\bpending(?:\s+this\s+stage)?\b", re.I)
BLANK_RE = re.compile(r"^(?:[-—–]|n/?a|tbd|todo|none)?$", re.I)
HEADER_DISPOSITION = re.compile(r"^disposition$", re.I)
HEADER_DISCHARGED = re.compile(r"^discharged\s+by$", re.I)
SECTION_END = re.compile(r"^#{1,3} (?!5\.[12]\b)")
H_51 = re.compile(r"^### 5\.1\b")
H_52 = re.compile(r"^### 5\.2\b")
H_512 = re.compile(r"^#### 5\.1\.2\b")
H_511 = re.compile(r"^#### 5\.1\.1\b")
# Single-letter P1 registers. A-F stopped before P1-G, so a blank P1-G
# row was reported as the previous register (P1-F/n).
H_P1 = re.compile(r"^#### (P1-[A-Z])\b")
ISSUE_HEADING_RE = re.compile(r"^#{2,4}\s+`([^`]+)`")
DELETE_VERBS = frozenset({"deleted", "delete"})


@dataclass
class Row:
    row_id: str
    disposition: str
    discharged: str
    claimed: bool
    line_no: int


@dataclass
class Table:
    label: str
    register: str | None
    has_disposition: bool
    has_discharged: bool
    rows: list[Row]
    header_line: int


def die(msg: str) -> None:
    print(f"error: {msg}", file=sys.stderr)
    raise SystemExit(1)


def rel(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def cells(line: str) -> list[str] | None:
    raw = line.rstrip("\n")
    if not raw.lstrip().startswith("|"):
        return None
    body = raw.strip()
    if not body.startswith("|"):
        return None
    return [c.strip() for c in body.strip("|").split("|")]


def is_separator(parts: list[str]) -> bool:
    return bool(parts) and all(re.fullmatch(r":?-{3,}:?", p.replace(" ", "")) for p in parts)


def strip_md(text: str) -> str:
    text = text.replace("`", "")
    text = re.sub(r"\*\*([^*]+)\*\*", r"\1", text)
    return text.strip()


def row_id(register: str | None, first: str) -> str:
    token = strip_md(first).split()[0] if first.strip() else ""
    token = token.rstrip("—-").strip()
    if register == "P0":
        return token or "P0-?"
    if register == "P0-19":
        return f"P0-19/{token}" if token else "P0-19/?"
    if register:
        return f"{register}/{token}" if token else f"{register}/?"
    return token or "?"


def claim_matches(disposition: str) -> list[tuple[str, str]]:
    return [(m.group(1).lower(), m.group(2)) for m in CLAIM_RE.finditer(strip_md(disposition))]


def is_claimed(rid: str, disposition: str) -> bool:
    if rid in EXTRA_CLAIMED:
        return True
    return bool(claim_matches(disposition))


def is_delete_claim(row: Row) -> bool:
    if row.row_id in EXTRA_CLAIMED:
        return False
    matches = claim_matches(row.disposition)
    return bool(matches) and all(verb in DELETE_VERBS for verb, _ in matches)


def parse_ledger(text: str, *, label: str) -> list[Table]:
    lines = text.splitlines()
    tables: list[Table] = []
    section: str | None = None
    register: str | None = None
    i = 0
    n = len(lines)
    while i < n:
        line = lines[i]
        if H_51.match(line):
            section, register = "5.1", "P0"
            i += 1
            continue
        if H_52.match(line):
            section, register = "5.2", None
            i += 1
            continue
        if section and H_512.match(line):
            register = "P0-19"
            i += 1
            continue
        if section and H_511.match(line):
            register = None
            i += 1
            continue
        if section == "5.2":
            m = H_P1.match(line)
            if m:
                register = m.group(1)
                i += 1
                continue
        if section and (line.startswith("### ") or line.startswith("## ")):
            if SECTION_END.match(line) and not H_51.match(line) and not H_52.match(line):
                section = None
                register = None
        if section is None:
            i += 1
            continue
        parts = cells(line)
        if parts is None:
            i += 1
            continue
        headers = parts
        header_line = i + 1
        i += 1
        if i < n:
            sep = cells(lines[i])
            if sep is None or not is_separator(sep):
                continue
            i += 1
        else:
            continue
        disp_idx = next((k for k, h in enumerate(headers) if HEADER_DISPOSITION.match(h)), None)
        disc_idx = next((k for k, h in enumerate(headers) if HEADER_DISCHARGED.match(h)), None)
        rows: list[Row] = []
        while i < n:
            row_parts = cells(lines[i])
            if row_parts is None:
                break
            if is_separator(row_parts):
                i += 1
                continue
            first = row_parts[0] if row_parts else ""
            rid = row_id(register, first)
            disposition = row_parts[disp_idx] if disp_idx is not None and disp_idx < len(row_parts) else ""
            discharged = row_parts[disc_idx] if disc_idx is not None and disc_idx < len(row_parts) else ""
            rows.append(
                Row(
                    row_id=rid,
                    disposition=disposition,
                    discharged=discharged,
                    claimed=is_claimed(rid, disposition),
                    line_no=i + 1,
                )
            )
            i += 1
        if disp_idx is None:
            continue
        tables.append(
            Table(
                label=f"{label}:{header_line}",
                register=register,
                has_disposition=True,
                has_discharged=disc_idx is not None,
                rows=rows,
                header_line=header_line,
            )
        )
    return tables


def cell_is_blank(raw: str) -> bool:
    return bool(BLANK_RE.match(strip_md(raw)))


def load_issue_headings(issues_dir: Path) -> frozenset[str]:
    """Issue ids that exist as `##` / `###` `` `S0-B-13` `` headings."""
    found: set[str] = set()
    if not issues_dir.is_dir():
        return frozenset()
    for path in sorted(issues_dir.glob("*.md")):
        try:
            text = path.read_text(encoding="utf-8")
        except OSError:
            continue
        for line in text.splitlines():
            m = ISSUE_HEADING_RE.match(line)
            if not m:
                continue
            token = m.group(1).split()[0]
            if ISSUE_RE.fullmatch(token):
                found.add(token)
    return frozenset(found)


def git_object_exists(sha: str) -> bool:
    try:
        subprocess.run(
            ["git", "-C", str(ROOT), "cat-file", "-t", sha],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return True
    except (OSError, subprocess.CalledProcessError):
        return False


def sha12_ok(sha: str, *, verify_shas: bool) -> bool:
    if not SHA12_RE.fullmatch(sha):
        return False
    if not verify_shas:
        return True
    # Object present → confirmed. Object missing (depth-1 checkout) → still
    # accept the 12-hex form; do not fail CI for history the clone omitted.
    git_object_exists(sha)
    return True


def check_tables(
    tables: list[Table],
    *,
    source: str,
    verify_shas: bool,
    issue_ids: frozenset[str],
    issues_label: str = "plan/issues/",
) -> list[str]:
    errors: list[str] = []
    if not tables:
        errors.append(f"{source}: no Disposition tables found under §5.1/§5.2")
        return errors
    claimed_n = 0
    for table in tables:
        if not table.has_discharged:
            errors.append(
                f"{table.label}: Disposition table is missing a `Discharged by` column"
            )
            continue
        for row in table.rows:
            if not row.claimed:
                continue
            claimed_n += 1
            if cell_is_blank(row.discharged):
                errors.append(
                    f"{source}:{row.line_no}: {row.row_id} is claimed by "
                    f"{'/'.join(CLAIM_STAGES)} but `Discharged by` is blank"
                )
                continue
            text = strip_md(row.discharged)
            shas = SHA12_RE.findall(text)
            issues = ISSUE_RE.findall(text)
            stages = STAGE_RE.findall(text)
            pending = bool(PENDING_RE.search(text))
            if is_delete_claim(row):
                if stages or shas:
                    continue
                errors.append(
                    f"{source}:{row.line_no}: {row.row_id} `Discharged by` must "
                    f"hold a stage id (deletion) or a commit SHA; got {row.discharged!r}"
                )
                continue
            # patch / write / wire (and EXTRA_CLAIMED): 12-hex SHA or a real
            # still-open issue heading. Not `pending`, not a free-form id.
            if pending and not shas and not issues:
                errors.append(
                    f"{source}:{row.line_no}: {row.row_id} `pending` does not "
                    f"discharge a claimed patch; use a 12-hex SHA or an issue "
                    f"heading under {issues_label}"
                )
                continue
            if shas:
                bad = [s for s in shas if not sha12_ok(s, verify_shas=verify_shas)]
                if bad:
                    errors.append(
                        f"{source}:{row.line_no}: {row.row_id} `Discharged by` "
                        f"SHA {', '.join(bad)} is not a 12-hex commit id"
                    )
                continue
            if issues:
                unknown = [i for i in issues if i not in issue_ids]
                if unknown:
                    errors.append(
                        f"{source}:{row.line_no}: {row.row_id} `Discharged by` "
                        f"issue id {', '.join(unknown)} is not a "
                        f"`### \\`…\\`` heading under {issues_label}"
                    )
                continue
            errors.append(
                f"{source}:{row.line_no}: {row.row_id} `Discharged by` must "
                f"hold a 12-hex commit SHA (optionally `SHA → Sn`) or a "
                f"still-open issue id with a heading under {issues_label}; got "
                f"{row.discharged!r}"
            )
    if claimed_n == 0:
        errors.append(
            f"{source}: parsed §5.1/§5.2 but found no rows claimed by "
            f"{'/'.join(CLAIM_STAGES)} — the table parser is wrong or the "
            f"Disposition vocabulary changed"
        )
    return errors


def load_prd(path: Path) -> str:
    if not path.is_file():
        die(f"missing {rel(path)}")
    return path.read_text(encoding="utf-8")


def report(errors: list[str], *, source: str) -> int:
    if not errors:
        return 0
    print(f"error: M3 Discharged-by ledger failed ({source}, S0-B-18 / E0.9):", file=sys.stderr)
    for err in errors:
        print(f"  {err}", file=sys.stderr)
    print(file=sys.stderr)
    print(
        "hint: fill `Discharged by` with a 12-hex commit SHA (patch) or a "
        "stage id (deletion). Still-open rows may use an issue id that exists "
        "as a heading in the issues directory (plan/issues/, or --issues). "
        "See [PRD] §5.0.",
        file=sys.stderr,
    )
    return 1


def self_test() -> int:
    failed = 0
    fail_dir = FIXTURE_ROOT / "expect-fail"
    pass_dir = FIXTURE_ROOT / "expect-pass"
    required_fail = (
        "missing-column",
        "blank-s0-row",
        "blank-s1-row",
        "blank-s2-row",
        "blank-s2r-row",
        "blank-p1b8",
        "blank-p1g-row",
        "pending-s0-patch",
        "fake-issue-id",
    )
    required_pass = ("filled-s0",)
    # Hermetic: the live corpus is not in this tree (removed in 3b02a4c).
    issues_dir = FIXTURE_ROOT / "issues"
    issue_ids = load_issue_headings(issues_dir)
    issues_label = rel(issues_dir)

    if not fail_dir.is_dir() or not pass_dir.is_dir():
        die(f"missing fixture dirs under {rel(FIXTURE_ROOT)}")

    for name in required_fail:
        if not (fail_dir / name / "prd.md").is_file():
            print(f"error: self-test: missing negative fixture {rel(fail_dir / name)}", file=sys.stderr)
            failed = 1
    for name in required_pass:
        if not (pass_dir / name / "prd.md").is_file():
            print(f"error: self-test: missing positive fixture {rel(pass_dir / name)}", file=sys.stderr)
            failed = 1

    def expect_fail(name: str, *needles: str, absent: tuple[str, ...] = ()) -> None:
        nonlocal failed
        path = fail_dir / name / "prd.md"
        if not path.is_file():
            return
        tables = parse_ledger(path.read_text(encoding="utf-8"), label=rel(path))
        errors = check_tables(
            tables,
            source=rel(path),
            verify_shas=False,
            issue_ids=issue_ids,
            issues_label=issues_label,
        )
        if not errors:
            print(f"error: self-test: expected failure in {rel(path)}", file=sys.stderr)
            failed = 1
            return
        blob = "\n".join(errors)
        missing = [n for n in needles if n not in blob]
        present = [n for n in absent if n in blob]
        if missing or present:
            print(
                f"error: self-test: {name} missing {missing!r} unexpected {present!r} in errors:",
                file=sys.stderr,
            )
            for err in errors:
                print(f"  {err}", file=sys.stderr)
            failed = 1
            return
        print(f"ok: self-test {name} is red")

    def expect_pass(name: str) -> None:
        nonlocal failed
        path = pass_dir / name / "prd.md"
        if not path.is_file():
            return
        tables = parse_ledger(path.read_text(encoding="utf-8"), label=rel(path))
        errors = check_tables(
            tables,
            source=rel(path),
            verify_shas=False,
            issue_ids=issue_ids,
            issues_label=issues_label,
        )
        if errors:
            print(f"error: self-test: expected pass in {rel(path)}", file=sys.stderr)
            for err in errors:
                print(f"  {err}", file=sys.stderr)
            failed = 1
            return
        print(f"ok: self-test {name} is green")

    for token in ("S0-B-10", "S2-B-07", "S2R-A-01", "S0a-B-06", "S3b-B-01", "S4c-B-01"):
        if ISSUE_RE.fullmatch(token) is None:
            print(f"error: self-test: ISSUE_RE does not match {token}", file=sys.stderr)
            failed = 1
    if "S2R-A-01" not in issue_ids:
        print("error: self-test: fixture headings did not load S2R-A-01", file=sys.stderr)
        failed = 1
    s2r_match = claim_matches("patch @ S2R")
    s2_match = claim_matches("patch @ S2")
    if s2r_match != [("patch", "S2R")] or s2_match != [("patch", "S2")]:
        print(
            f"error: self-test: stage capture S2R={s2r_match!r} S2={s2_match!r}",
            file=sys.stderr,
        )
        failed = 1

    expect_fail("missing-column", "missing a `Discharged by` column")
    expect_fail("blank-s0-row", "P0-01", "blank")
    expect_fail("blank-s1-row", "P0-01", "blank")
    expect_fail("blank-s2-row", "P0-01", "blank")
    expect_fail("blank-s2r-row", "P0-01", "blank")
    expect_fail("blank-p1b8", "P1-B/8")
    expect_fail("blank-p1g-row", "P1-G/1", "blank", absent=("P1-F/1",))
    expect_fail("pending-s0-patch", "P0-01", "pending")
    expect_fail("fake-issue-id", "P0-01", "S0-ZZZ-1")
    expect_pass("filled-s0")

    n_fail = sum(1 for p in fail_dir.iterdir() if p.is_dir() and (p / "prd.md").is_file())
    n_pass = sum(1 for p in pass_dir.iterdir() if p.is_dir() and (p / "prd.md").is_file())
    if n_fail < 5:
        print(
            f"error: self-test: need >=5 negative fixtures in {rel(fail_dir)} (found {n_fail})",
            file=sys.stderr,
        )
        failed = 1
    if n_pass < 1:
        print(
            f"error: self-test: need >=1 positive fixture in {rel(pass_dir)} (found {n_pass})",
            file=sys.stderr,
        )
        failed = 1

    if failed:
        die("M3 Discharged-by fixture self-test failed")
    print("ok: M3 Discharged-by fixtures")
    return 0


def compare_live(prd: Path | None = None, issues_dir: Path | None = None) -> int:
    prd = LIVE if prd is None else prd
    issues_dir = ISSUES_DIR if issues_dir is None else issues_dir
    text = load_prd(prd)
    tables = parse_ledger(text, label=rel(prd))
    errors = check_tables(
        tables,
        source=rel(prd),
        verify_shas=True,
        issue_ids=load_issue_headings(issues_dir),
        issues_label=rel(issues_dir),
    )
    if errors:
        return report(errors, source=rel(prd))
    claimed = sum(1 for t in tables for r in t.rows if r.claimed)
    print(
        f"ok: M3 Discharged by — {claimed} claimed rows filled "
        f"({len(tables)} Disposition tables in §5.1/§5.2)"
    )
    return 0


def user_path(raw: str) -> Path:
    path = Path(raw)
    if path.is_absolute():
        return path
    return ROOT / path


def parse_args(args: list[str]) -> tuple[str, Path | None, Path | None]:
    if not args:
        return "default", None, None
    if args in (["-h"], ["--help"]):
        return "help", None, None
    if args == ["--self-test"]:
        return "self-test", None, None
    prd: Path | None = None
    issues: Path | None = None
    i = 0
    while i < len(args):
        flag = args[i]
        if flag not in ("--prd", "--issues"):
            die(
                f"unknown argument: {flag} "
                "(want --self-test, or --prd <file> --issues <dir>)"
            )
        if i + 1 >= len(args) or args[i + 1].startswith("--"):
            die(f"{flag} requires a path")
        value = user_path(args[i + 1])
        if flag == "--prd":
            if prd is not None:
                die("--prd given more than once")
            prd = value
        else:
            if issues is not None:
                die("--issues given more than once")
            issues = value
        i += 2
    if prd is None or issues is None:
        die("--prd and --issues must be given together")
    return "live", prd, issues


def main() -> int:
    mode, prd, issues = parse_args(ARGS)
    if mode == "help":
        print(
            "Usage: bash scripts/check-m3-discharged-by.sh [--self-test]\n"
            "       bash scripts/check-m3-discharged-by.sh --prd <file> --issues <dir>\n"
            "Assert [PRD] §5.1/§5.2 Discharged-by cells for rows this stage claimed.\n"
            "No args: self-test, then plan/prd.md when it exists; otherwise the\n"
            "live ledger is NOT_RUN (corpus removed in 3b02a4c) and the exit\n"
            "status is the self-test's. --prd/--issues scans those paths and\n"
            "does not require them to sit under plan/."
        )
        return 0
    rc = self_test()
    if mode == "self-test":
        return rc
    if mode == "live":
        if prd is None or issues is None:
            die("internal: live mode without --prd and --issues")
        if not issues.is_dir():
            die(f"missing issues directory {issues}")
        live = compare_live(prd, issues)
        return live if live else rc
    if not LIVE.is_file():
        print(
            "ok: M3 Discharged by — live ledger NOT_RUN because the corpus "
            "is not in this tree (removed in 3b02a4c)"
        )
        return rc
    live = compare_live()
    return live if live else rc


if __name__ == "__main__":
    raise SystemExit(main())
PY
