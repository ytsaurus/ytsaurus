#!/usr/bin/env python3

"""Offline validator for agent-produced YQL documentation check plans."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from typing import Any, Callable, Iterable, Mapping, Sequence


SCHEMA_VERSION = 1
CLUSTER = "freud"
DOCS_PREFIX = "yt/docs/ru/yql/"
WORKSPACE_ROOT = "//home/dev/docs-team"
MAX_PLAN_BYTES = 256 * 1024
MAX_CHANGES = 100
MAX_QUERY_CHECKS = 20
MAX_QUERY_BYTES = 32 * 1024
MAX_EXPECTED_ROWS = 100
MAX_TEXT_LENGTH = 4000

ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,79}$")
VALIDATION_BASE_RE = re.compile(
    r"(?m)^YQL_DOC_SYNC_VALIDATION_BASE: (?P<revision>[0-9a-f]{40})$"
)
FENCED_JSON_RE = re.compile(r"```json\s*(?P<json>.*?)\s*```", re.IGNORECASE | re.DOTALL)
ABSOLUTE_YT_PATH_RE = re.compile(r"(?<![:/])//[A-Za-z0-9_@%+=:,./-]+")
BACKTICK_RE = re.compile(r"`([^`]*)`")
CLUSTER_TABLE_RE = re.compile(
    r"\b([A-Za-z][A-Za-z0-9_-]*)\s*\.\s*`([^`]*)`",
    re.IGNORECASE,
)
TABLE_REFERENCE_RE = re.compile(
    r"\b(?:FROM|JOIN|PROCESS|REDUCE)\s+"
    r"(?:[A-Za-z][A-Za-z0-9_-]*\s*\.\s*)?`([^`]*)`",
    re.IGNORECASE,
)
SOURCE_CLAUSE_RE = re.compile(r"\b(?:FROM|JOIN|PROCESS|REDUCE)\b", re.IGNORECASE)
UNQUOTED_CLUSTER_TABLE_RE = re.compile(
    r"\b(?:FROM|JOIN|PROCESS|REDUCE)\s+"
    r"([A-Za-z][A-Za-z0-9_-]*)\s*\.\s*([A-Za-z][A-Za-z0-9_-]*)",
    re.IGNORECASE,
)
USE_RE = re.compile(r"\bUSE\s+[`\"']?([A-Za-z][A-Za-z0-9_-]*)", re.IGNORECASE)
FORBIDDEN_STATEMENT_RE = re.compile(
    r"\b(?:ALTER|COMMIT|COPY|CREATE|DELETE|DROP|ERASE|GRANT|INSERT|MOVE|"
    r"REMOVE|REPLACE|REVOKE|TRUNCATE|UPDATE|UPSERT)\b",
    re.IGNORECASE,
)


class ValidationError(Exception):
    """A deterministic validation failure with a stable machine code."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


def _require_exact_keys(value: Mapping[str, Any], keys: set[str], where: str) -> None:
    actual = set(value)
    if actual != keys:
        missing = sorted(keys - actual)
        extra = sorted(actual - keys)
        raise ValidationError(
            "INVALID_SCHEMA",
            f"{where} has invalid keys; missing={missing}, extra={extra}",
        )


def _require_text(value: Any, where: str, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str):
        raise ValidationError("INVALID_SCHEMA", f"{where} must be a string")
    if not allow_empty and not value.strip():
        raise ValidationError("INVALID_SCHEMA", f"{where} must not be empty")
    if len(value) > MAX_TEXT_LENGTH:
        raise ValidationError(
            "INVALID_SCHEMA",
            f"{where} exceeds {MAX_TEXT_LENGTH} characters",
        )
    return value


def parse_plan(raw_plan: str) -> dict[str, Any]:
    if not raw_plan or not raw_plan.strip():
        raise ValidationError("EMPTY_PLAN", "agent returned an empty validation plan")
    if len(raw_plan.encode("utf-8")) > MAX_PLAN_BYTES:
        raise ValidationError(
            "PLAN_TOO_LARGE",
            f"agent plan exceeds {MAX_PLAN_BYTES} bytes",
        )
    candidate = raw_plan.strip()
    try:
        value = json.loads(candidate)
    except json.JSONDecodeError as bare_error:
        fenced_blocks = FENCED_JSON_RE.findall(raw_plan)
        if len(fenced_blocks) != 1:
            raise ValidationError(
                "INVALID_JSON",
                "agent output must contain exactly one JSON object, either bare or "
                f"inside one json fence: {bare_error.msg}",
            ) from bare_error
        try:
            value = json.loads(fenced_blocks[0].strip())
        except json.JSONDecodeError as fenced_error:
            raise ValidationError(
                "INVALID_JSON",
                f"agent json fence contains invalid JSON: {fenced_error.msg}",
            ) from fenced_error
    if not isinstance(value, dict):
        raise ValidationError("INVALID_SCHEMA", "plan root must be an object")
    return value


def _validate_expected(value: Any, where: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValidationError("INVALID_SCHEMA", f"{where} must be an object")
    mode = value.get("mode")
    if mode == "success":
        _require_exact_keys(value, {"mode"}, where)
    elif mode == "rows":
        _require_exact_keys(value, {"mode", "rows"}, where)
        rows = value["rows"]
        if not isinstance(rows, list):
            raise ValidationError("INVALID_SCHEMA", f"{where}.rows must be an array")
        if len(rows) > MAX_EXPECTED_ROWS:
            raise ValidationError(
                "INVALID_SCHEMA",
                f"{where}.rows exceeds {MAX_EXPECTED_ROWS} rows",
            )
        for index, row in enumerate(rows):
            if not isinstance(row, dict):
                raise ValidationError(
                    "INVALID_SCHEMA",
                    f"{where}.rows[{index}] must be an object",
                )
    else:
        raise ValidationError(
            "INVALID_SCHEMA",
            f"{where}.mode must be 'success' or 'rows'",
        )
    try:
        encoded = json.dumps(value, ensure_ascii=False)
    except (TypeError, ValueError) as error:
        raise ValidationError(
            "INVALID_SCHEMA",
            f"{where} must contain JSON values only",
        ) from error
    if len(encoded.encode("utf-8")) > MAX_PLAN_BYTES // 2:
        raise ValidationError("INVALID_SCHEMA", f"{where} is too large")
    return value


def _validate_doc_path(value: Any, where: str) -> str:
    path = _require_text(value, where)
    if not path.startswith(DOCS_PREFIX) or path.endswith("/"):
        raise ValidationError(
            "INVALID_SCHEMA",
            f"{where} must name a file below {DOCS_PREFIX}",
        )
    parts = Path(path).parts
    if path.startswith("/") or ".." in parts or "." in parts:
        raise ValidationError("INVALID_SCHEMA", f"{where} is not normalized")
    return path


def validate_plan(plan: Mapping[str, Any], changed_paths: Iterable[str]) -> dict[str, Any]:
    _require_exact_keys(plan, {"schema_version", "summary", "changes"}, "plan")
    if plan["schema_version"] != SCHEMA_VERSION or isinstance(plan["schema_version"], bool):
        raise ValidationError(
            "UNSUPPORTED_SCHEMA_VERSION",
            f"schema_version must be {SCHEMA_VERSION}",
        )
    summary = _require_text(plan["summary"], "plan.summary")
    raw_changes = plan["changes"]
    if not isinstance(raw_changes, list):
        raise ValidationError("INVALID_SCHEMA", "plan.changes must be an array")
    if len(raw_changes) > MAX_CHANGES:
        raise ValidationError(
            "INVALID_SCHEMA",
            f"plan.changes exceeds {MAX_CHANGES} entries",
        )

    normalized_changes: list[dict[str, Any]] = []
    ids: set[str] = set()
    query_count = 0
    for index, raw_change in enumerate(raw_changes):
        where = f"plan.changes[{index}]"
        if not isinstance(raw_change, dict):
            raise ValidationError("INVALID_SCHEMA", f"{where} must be an object")
        kind = raw_change.get("kind")
        if kind == "query":
            _require_exact_keys(
                raw_change,
                {"id", "path", "kind", "description", "query", "expected"},
                where,
            )
        elif kind == "na":
            _require_exact_keys(
                raw_change,
                {"id", "path", "kind", "description", "reason"},
                where,
            )
        else:
            raise ValidationError(
                "INVALID_SCHEMA",
                f"{where}.kind must be 'query' or 'na'",
            )

        change_id = _require_text(raw_change["id"], f"{where}.id")
        if not ID_RE.fullmatch(change_id):
            raise ValidationError(
                "INVALID_SCHEMA",
                f"{where}.id must match {ID_RE.pattern}",
            )
        if change_id in ids:
            raise ValidationError("INVALID_SCHEMA", f"duplicate change id {change_id!r}")
        ids.add(change_id)

        normalized: dict[str, Any] = {
            "id": change_id,
            "path": _validate_doc_path(raw_change["path"], f"{where}.path"),
            "kind": kind,
            "description": _require_text(
                raw_change["description"],
                f"{where}.description",
            ),
        }
        if kind == "query":
            query = _require_text(raw_change["query"], f"{where}.query")
            if len(query.encode("utf-8")) > MAX_QUERY_BYTES:
                raise ValidationError(
                    "INVALID_SCHEMA",
                    f"{where}.query exceeds {MAX_QUERY_BYTES} bytes",
                )
            normalized["query"] = query
            normalized["expected"] = _validate_expected(
                raw_change["expected"],
                f"{where}.expected",
            )
            query_count += 1
        else:
            normalized["reason"] = _require_text(
                raw_change["reason"],
                f"{where}.reason",
            )
        normalized_changes.append(normalized)

    if query_count > MAX_QUERY_CHECKS:
        raise ValidationError(
            "INVALID_SCHEMA",
            f"plan contains {query_count} queries; maximum is {MAX_QUERY_CHECKS}",
        )

    expected_paths = {path for path in changed_paths if path}
    actual_paths = {change["path"] for change in normalized_changes}
    if actual_paths != expected_paths:
        raise ValidationError(
            "INCOMPLETE_COVERAGE",
            "plan paths do not exactly cover the pull-request YQL documentation files; "
            f"missing={sorted(expected_paths - actual_paths)}, "
            f"unexpected={sorted(actual_paths - expected_paths)}",
        )

    return {
        "schema_version": SCHEMA_VERSION,
        "summary": summary,
        "changes": normalized_changes,
    }


def _without_quoted_strings(query: str) -> str:
    result: list[str] = []
    quote: str | None = None
    escaped = False
    for character in query:
        if quote is None:
            if character in {"'", '"'}:
                quote = character
                result.append(" ")
            else:
                result.append(character)
            continue
        result.append(" ")
        if escaped:
            escaped = False
        elif character == "\\":
            escaped = True
        elif character == quote:
            quote = None
    if quote is not None:
        raise ValidationError("UNSAFE_QUERY", "query contains an unterminated string")
    return "".join(result)


def _validate_workspace_path(path: str) -> None:
    normalized = path.rstrip(".,;")
    if normalized != WORKSPACE_ROOT and not normalized.startswith(WORKSPACE_ROOT + "/"):
        raise ValidationError(
            "UNSAFE_QUERY_PATH",
            f"YT path {normalized!r} is outside {WORKSPACE_ROOT}",
        )


def validate_query(query: str) -> None:
    if "\x00" in query:
        raise ValidationError("UNSAFE_QUERY", "query contains a NUL byte")
    code = _without_quoted_strings(query)
    forbidden = FORBIDDEN_STATEMENT_RE.search(code)
    if forbidden:
        raise ValidationError(
            "UNSAFE_QUERY_STATEMENT",
            f"read-only pilot rejects statement keyword {forbidden.group(0).upper()}",
        )

    for match in USE_RE.finditer(code):
        if match.group(1).lower() != CLUSTER:
            raise ValidationError(
                "UNSAFE_QUERY_CLUSTER",
                f"query selects cluster {match.group(1)!r}; only {CLUSTER!r} is allowed",
            )

    for match in SOURCE_CLAUSE_RE.finditer(code):
        source = code[match.end() :].lstrip()
        explicit_path = re.match(
            r"(?:(?P<cluster>[A-Za-z][A-Za-z0-9_-]*)\s*\.\s*)?`(?P<path>[^`]*)`",
            source,
        )
        if explicit_path:
            cluster = explicit_path.group("cluster")
            if cluster is not None and cluster.lower() != CLUSTER:
                raise ValidationError(
                    "UNSAFE_QUERY_CLUSTER",
                    f"table reference selects cluster {cluster!r}; only {CLUSTER!r} is allowed",
                )
            _validate_workspace_path(explicit_path.group("path"))
            continue
        if source.startswith("(") or re.match(r"AS_TABLE\s*\(", source, re.IGNORECASE):
            continue
        raise ValidationError(
            "UNSAFE_QUERY_SOURCE",
            "every FROM/JOIN/PROCESS/REDUCE source must be an explicit path below "
            f"{WORKSPACE_ROOT}, a subquery, or AS_TABLE(...)",
        )

    for match in ABSOLUTE_YT_PATH_RE.finditer(query):
        _validate_workspace_path(match.group(0))

    for match in CLUSTER_TABLE_RE.finditer(query):
        cluster, table = match.groups()
        if cluster.lower() != CLUSTER:
            raise ValidationError(
                "UNSAFE_QUERY_CLUSTER",
                f"table reference selects cluster {cluster!r}; only {CLUSTER!r} is allowed",
            )
        _validate_workspace_path(table)

    for match in UNQUOTED_CLUSTER_TABLE_RE.finditer(code):
        cluster, table = match.groups()
        raise ValidationError(
            "UNSAFE_QUERY_PATH",
            f"unquoted table reference {cluster}.{table} is not an absolute path below "
            f"{WORKSPACE_ROOT}",
        )

    for match in TABLE_REFERENCE_RE.finditer(query):
        _validate_workspace_path(match.group(1))

    for match in BACKTICK_RE.finditer(query):
        value = match.group(1)
        if "/" in value:
            _validate_workspace_path(value)


CommandRunner = Callable[..., subprocess.CompletedProcess[str]]


def _run_command(
    args: Sequence[str],
    *,
    cwd: Path,
    timeout: float,
    runner: CommandRunner = subprocess.run,
) -> subprocess.CompletedProcess[str]:
    try:
        completed = runner(
            list(args),
            cwd=str(cwd),
            check=False,
            capture_output=True,
            text=True,
            timeout=max(1.0, timeout),
        )
    except subprocess.TimeoutExpired as error:
        raise ValidationError(
            "COMMAND_TIMEOUT",
            f"command {Path(args[0]).name} {args[1]} exceeded {timeout:.0f}s",
        ) from error
    if completed.returncode != 0:
        details = (completed.stderr or completed.stdout or "").strip()[-4000:]
        raise ValidationError(
            "COMMAND_FAILED",
            f"command {Path(args[0]).name} {args[1]} failed: {details}",
        )
    return completed


def get_validation_base(
    repo_root: Path,
    runner: CommandRunner = subprocess.run,
) -> str:
    trunk_base = _run_command(
        ["arc", "merge-base", "--leftmost", "HEAD", "arcadia/trunk"],
        cwd=repo_root,
        timeout=120,
        runner=runner,
    ).stdout.strip()
    if not re.fullmatch(r"[0-9a-f]{40}", trunk_base):
        raise ValidationError(
            "INVALID_VALIDATION_BASE",
            "arc merge-base did not return a 40-character revision",
        )

    log_output = _run_command(
        [
            "arc",
            "log",
            "--first-parent",
            "-n",
            "100",
            "--json",
            f"{trunk_base}..HEAD",
        ],
        cwd=repo_root,
        timeout=120,
        runner=runner,
    ).stdout
    try:
        commits = json.loads(log_output)
    except json.JSONDecodeError as error:
        raise ValidationError("INVALID_ARC_LOG", "arc log returned non-JSON output") from error
    if not isinstance(commits, list):
        raise ValidationError("INVALID_ARC_LOG", "arc log JSON root must be an array")

    validation_base: str | None = None
    for commit in commits:
        if not isinstance(commit, dict):
            continue
        message = commit.get("message")
        if not isinstance(message, str):
            continue
        match = VALIDATION_BASE_RE.search(message)
        if match:
            validation_base = match.group("revision")
            break

    if validation_base is None:
        validation_base = trunk_base

    common_ancestor = _run_command(
        ["arc", "merge-base", "--leftmost", validation_base, "HEAD"],
        cwd=repo_root,
        timeout=120,
        runner=runner,
    ).stdout.strip()
    if common_ancestor != validation_base:
        raise ValidationError(
            "INVALID_VALIDATION_BASE",
            f"validation base {validation_base} is not an ancestor of HEAD",
        )
    return validation_base


def get_changed_paths(
    repo_root: Path,
    validation_base: str,
    runner: CommandRunner = subprocess.run,
) -> list[str]:
    completed = _run_command(
        [
            "arc",
            "diff",
            "--name-only",
            validation_base,
            "HEAD",
            "yt/docs/ru/yql",
        ],
        cwd=repo_root,
        timeout=120,
        runner=runner,
    )
    paths = sorted({line.strip() for line in completed.stdout.splitlines() if line.strip()})
    invalid = [path for path in paths if not path.startswith(DOCS_PREFIX)]
    if invalid:
        raise ValidationError(
            "INVALID_DIFF",
            f"arc diff returned paths outside {DOCS_PREFIX}: {invalid}",
        )
    return paths


def get_validation_diff(
    repo_root: Path,
    validation_base: str,
    runner: CommandRunner = subprocess.run,
) -> str:
    return _run_command(
        ["arc", "diff", validation_base, "HEAD", "yt/docs/ru/yql"],
        cwd=repo_root,
        timeout=120,
        runner=runner,
    ).stdout


def build_offline_report(
    plan: Mapping[str, Any],
    changed_paths: Sequence[str],
    validation_base: str,
) -> dict[str, Any]:
    for change in plan["changes"]:
        if change["kind"] == "query":
            validate_query(change["query"])

    checks: list[dict[str, Any]] = []
    for change in plan["changes"]:
        base = {
            "id": change["id"],
            "path": change["path"],
            "kind": change["kind"],
            "description": change["description"],
        }
        if change["kind"] == "na":
            checks.append({**base, "status": "na", "reason": change["reason"]})
            continue
        checks.append(
            {
                **base,
                "status": "not_executed",
                "reason": "Remote Query Tracker validation is disabled",
                "expected": change["expected"],
            }
        )

    return {
        "schema_version": SCHEMA_VERSION,
        "status": "no_changes" if not changed_paths else "plan_validated",
        "execution_mode": "offline",
        "remote_execution": False,
        "proposed_cluster": CLUSTER,
        "proposed_workspace_root": WORKSPACE_ROOT,
        "validation_base": validation_base,
        "summary": plan["summary"],
        "changed_paths": list(changed_paths),
        "checks": checks,
    }


def _write_outputs(report: Mapping[str, Any], report_out: Path, status_out: Path) -> None:
    report_out.parent.mkdir(parents=True, exist_ok=True)
    status_out.parent.mkdir(parents=True, exist_ok=True)
    report_out.write_text(
        json.dumps(report, ensure_ascii=False, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    status_out.write_text(str(report.get("status", "failed")) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


def _failed_report(
    error: ValidationError,
    changed_paths: Sequence[str],
    validation_base: str | None,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "failed",
        "execution_mode": "offline",
        "remote_execution": False,
        "proposed_cluster": CLUSTER,
        "proposed_workspace_root": WORKSPACE_ROOT,
        "validation_base": validation_base,
        "changed_paths": list(changed_paths),
        "checks": [],
        "error": {"code": error.code, "message": error.message},
    }


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan-env", default="YQL_DOCS_VALIDATION_PLAN")
    parser.add_argument("--repo-root", type=Path, default=None)
    parser.add_argument("--report-out", type=Path)
    parser.add_argument("--status-out", type=Path)
    parser.add_argument("--print-diff", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_argument_parser().parse_args(argv)
    repo_root_value: str | Path | None = args.repo_root or os.environ.get("ARCADIA_PATH")
    changed_paths: list[str] = []
    validation_base: str | None = None
    try:
        if not repo_root_value:
            raise ValidationError("MISSING_REPO_ROOT", "ARCADIA_PATH is not set")
        repo_root = Path(repo_root_value).resolve()
        validation_base = get_validation_base(repo_root)
        changed_paths = get_changed_paths(repo_root, validation_base)
        if args.print_diff:
            print(
                json.dumps(
                    {
                        "validation_base": validation_base,
                        "changed_paths": changed_paths,
                    },
                    ensure_ascii=False,
                )
            )
            print(get_validation_diff(repo_root, validation_base), end="")
            return 0
        if args.report_out is None or args.status_out is None:
            raise ValidationError(
                "MISSING_OUTPUT_PATH",
                "--report-out and --status-out are required unless --print-diff is used",
            )
        plan = validate_plan(parse_plan(os.environ.get(args.plan_env, "")), changed_paths)
        report = build_offline_report(plan, changed_paths, validation_base)
        _write_outputs(report, args.report_out, args.status_out)
        return 0
    except ValidationError as error:
        report = _failed_report(error, changed_paths, validation_base)
        if args.report_out is None or args.status_out is None:
            print(json.dumps(report, ensure_ascii=False, indent=2))
            return 1
        _write_outputs(report, args.report_out, args.status_out)
        return 1
    except Exception as error:  # Fail closed without leaking the agent plan.
        report = _failed_report(
            ValidationError("INTERNAL_ERROR", f"{type(error).__name__}: {error}"),
            changed_paths,
            validation_base,
        )
        if args.report_out is None or args.status_out is None:
            print(json.dumps(report, ensure_ascii=False, indent=2))
            return 1
        _write_outputs(report, args.report_out, args.status_out)
        return 1


if __name__ == "__main__":
    sys.exit(main())
