#!/usr/bin/env python3
"""Validate the YTsaurus codebase map and its structural inventory."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

BACKTICK_FENCE_OPEN_RE = re.compile(r"^[ ]{0,3}(?P<fence>`{3,})(?P<info>[^`]*)$")
TILDE_FENCE_OPEN_RE = re.compile(r"^[ ]{0,3}(?P<fence>~{3,})(?P<info>.*)$")
SCOPED_AST_VERB_RE = re.compile(
    r"(?:^|[ \t`])(?:\.\./)*ya[ \t]+tool[ \t]+ast-index[ \t]+" r"(?:outline|file(?:[ \t]+--exact)?)(?=$|[ \t`])",
    re.MULTILINE,
)
STRICT_ISO_DATE_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}\Z")
PATTERN_GROUPS = {
    "<": (">", re.compile(r"[A-Za-z0-9_.-]+\Z")),
    "[": ("]", re.compile(r"[!^]?[A-Za-z0-9_.-]+\Z")),
    "{": ("}", re.compile(r"[A-Za-z0-9_.-]+(?:,[A-Za-z0-9_.-]+)*\Z")),
}
PATTERN_CLOSERS = frozenset(">]}")


def nonnegative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be an integer") from error
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be greater than or equal to 0")
    return parsed


def parse_strict_iso_date(value: str) -> dt.date:
    if not STRICT_ISO_DATE_RE.fullmatch(value):
        raise ValueError("must be an ISO date in YYYY-MM-DD format")
    try:
        parsed = dt.date.fromisoformat(value)
    except ValueError as error:
        raise ValueError("must be a valid calendar date in YYYY-MM-DD format") from error
    if parsed.isoformat() != value:
        raise ValueError("must be an ISO date in YYYY-MM-DD format")
    return parsed


def iso_date(value: str) -> dt.date:
    try:
        parsed = parse_strict_iso_date(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from error
    if parsed > dt.date.today():
        raise argparse.ArgumentTypeError("must not be in the future")
    return parsed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, required=True)
    parser.add_argument(
        "--map-path",
        "--map",
        dest="map_path",
        type=Path,
        default=Path("yt/docs/ai/artifacts/kb/codebase-map.md"),
    )
    parser.add_argument(
        "--inventory-path",
        "--inventory",
        dest="inventory_path",
        type=Path,
        default=Path("yt/docs/ai/artifacts/kb/codebase-map-inventory.json"),
    )
    parser.add_argument(
        "--max-review-age-days",
        type=nonnegative_int,
        default=0,
        help="Fail when reviewed_on is older than this many days; 0 disables only the age limit.",
    )
    parser.add_argument(
        "--refresh-inventory",
        action="store_true",
        help="Replace the tracked top-level directory snapshot from HEAD; preserve reviewed_on.",
    )
    parser.add_argument(
        "--reviewed-on",
        type=iso_date,
        help="Manually attest that the map received a semantic review on YYYY-MM-DD.",
    )
    return parser.parse_args()


def resolve(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def tracked_top_level_directories(repo_root: Path) -> list[str]:
    try:
        result = subprocess.run(
            ["arc", "ls-tree", "-d", "--name-only", "HEAD", "yt"],
            cwd=repo_root,
            check=True,
            capture_output=True,
            text=True,
            timeout=60,
        )
    except FileNotFoundError as error:
        raise ValueError("cannot build inventory: arc is not available") from error
    except subprocess.TimeoutExpired as error:
        raise ValueError("cannot build inventory: arc ls-tree timed out") from error
    except subprocess.CalledProcessError as error:
        details = (error.stderr or error.stdout or "").strip()
        suffix = f": {details}" if details else ""
        raise ValueError(f"cannot build inventory: arc ls-tree failed{suffix}") from error

    directories: list[str] = []
    for raw_line in result.stdout.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        components = line.split("/")
        if len(components) != 2 or components[0] != "yt" or not components[1]:
            raise ValueError(f"cannot build inventory: unexpected arc ls-tree entry {line!r}")
        directories.append(components[1])
    if not directories:
        raise ValueError("cannot build inventory: arc ls-tree returned no top-level yt directories")
    return sorted(set(directories))


def load_inventory(path: Path) -> dict[str, Any]:
    try:
        inventory = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise ValueError(f"inventory does not exist: {path}") from error
    except json.JSONDecodeError as error:
        raise ValueError(f"inventory is not valid JSON: {error}") from error

    if not isinstance(inventory, dict):
        raise ValueError("inventory root must be a JSON object")
    schema_version = inventory.get("schema_version")
    if type(schema_version) is not int or schema_version != 1:
        raise ValueError("inventory schema_version must be the integer 1")

    directories = inventory.get("top_level_directories")
    if not isinstance(directories, list):
        raise ValueError("inventory top_level_directories must be a JSON array")
    if any(
        not isinstance(directory, str)
        or not directory
        or directory.strip() != directory
        or "/" in directory
        or directory in {".", ".."}
        for directory in directories
    ):
        raise ValueError("inventory top_level_directories must contain only non-empty top-level directory names")
    if directories != sorted(set(directories)):
        raise ValueError("inventory top_level_directories must be sorted and unique")
    return inventory


def parse_reviewed_on(inventory: dict[str, Any], today: dt.date) -> tuple[dt.date | None, list[str]]:
    value = inventory.get("reviewed_on")
    if not isinstance(value, str):
        return None, ["inventory reviewed_on must be an ISO date string in YYYY-MM-DD format"]
    try:
        reviewed_on = parse_strict_iso_date(value)
    except ValueError:
        return None, [f"inventory reviewed_on is not a valid YYYY-MM-DD date: {value!r}"]
    if reviewed_on > today:
        return None, [f"inventory reviewed_on is in the future: {reviewed_on.isoformat()}"]
    return reviewed_on, []


def check_review_age(
    inventory: dict[str, Any], max_age_days: int, today: dt.date | None = None
) -> tuple[list[str], int | None]:
    today = today or dt.date.today()
    reviewed_on, errors = parse_reviewed_on(inventory, today)
    if errors or reviewed_on is None:
        return errors, None
    age = (today - reviewed_on).days
    if max_age_days and age > max_age_days:
        errors.append(f"map review is {age} days old; maximum allowed age is {max_age_days} days")
    return errors, age


def _backtick_run_length(text: str, start: int) -> int:
    end = start
    while end < len(text) and text[end] == "`":
        end += 1
    return end - start


def extract_inline_code_spans(line: str) -> tuple[list[str], list[str]]:
    spans = []
    errors = []
    cursor = 0
    while cursor < len(line):
        start = line.find("`", cursor)
        if start == -1:
            break
        delimiter_length = _backtick_run_length(line, start)
        content_start = start + delimiter_length
        candidate = content_start
        closing_start = None
        while candidate < len(line):
            candidate = line.find("`", candidate)
            if candidate == -1:
                break
            run_length = _backtick_run_length(line, candidate)
            if run_length == delimiter_length:
                closing_start = candidate
                break
            candidate += run_length

        if closing_start is None:
            errors.append("multiline or unterminated Markdown code span is not supported")
            break

        spans.append(line[content_start:closing_start])
        cursor = closing_start + delimiter_length
    return spans, errors


def extract_markdown_inline_code(markdown: str) -> tuple[list[str], list[str]]:
    inline_spans = []
    errors = []
    fence_character = None
    fence_length = None

    for line in markdown.splitlines():
        if fence_length is not None:
            closing = re.fullmatch(
                rf"[ ]{{0,3}}{re.escape(fence_character)}{{{fence_length},}}[ \t]*",
                line,
            )
            if closing:
                fence_character = None
                fence_length = None
            continue

        opening = BACKTICK_FENCE_OPEN_RE.fullmatch(line) or TILDE_FENCE_OPEN_RE.fullmatch(line)
        if opening:
            fence = opening.group("fence")
            fence_character = fence[0]
            fence_length = len(fence)
            continue

        if re.search(r"~{3,}", line):
            errors.append("nested or indented tilde fenced code block is not supported")
            continue

        spans, line_errors = extract_inline_code_spans(line)
        inline_spans.extend(spans)
        errors.extend(line_errors)

    if fence_length is not None:
        errors.append("unterminated Markdown fenced code block")
    return inline_spans, errors


def _is_bare_arcadia_root(path: str) -> bool:
    return path.strip() == "yt/"


def extract_arcadia_path_candidates(markdown: str) -> tuple[list[str], list[str]]:
    inline_spans, errors = extract_markdown_inline_code(markdown)
    paths = {
        content for content in inline_spans if content.lstrip().startswith("yt/") and not _is_bare_arcadia_root(content)
    }
    return sorted(paths), errors


def extract_arcadia_paths(markdown: str) -> list[str]:
    paths, _ = extract_arcadia_path_candidates(markdown)
    return paths


def extract_scoped_ast_paths(markdown: str) -> tuple[list[str], list[str]]:
    paths = []
    errors = []
    for match in SCOPED_AST_VERB_RE.finditer(markdown):
        line_end = markdown.find("\n", match.end())
        if line_end == -1:
            line_end = len(markdown)
        start = match.end()
        while start < line_end and markdown[start] in " \t":
            start += 1
        if start >= line_end or markdown[start] == "`":
            errors.append("scoped ast-index command has no path argument")
            continue

        quote = markdown[start] if markdown[start] in {"'", '"'} else None
        if quote is None:
            end = start
            while end < line_end and markdown[end] not in " \t`":
                end += 1
            path = markdown[start:end]
        else:
            end = markdown.find(quote, start + 1, line_end)
            code_span_end = markdown.find("`", start + 1, line_end)
            if end == -1 or (code_span_end != -1 and end > code_span_end):
                errors.append("scoped ast-index command has an unterminated quoted path argument")
                continue
            path = markdown[start + 1 : end]
            after = end + 1
            if after < line_end and markdown[after] not in " \t`":
                errors.append("scoped ast-index command has characters after its quoted path argument")
                continue

        if not path:
            errors.append("scoped ast-index command has an empty path argument")
        else:
            paths.append(path)
    return sorted(set(paths)), errors


def validate_pattern_syntax(path: str) -> tuple[bool, str | None]:
    has_pattern = False
    cursor = 0
    while cursor < len(path):
        character = path[cursor]
        if character in PATTERN_GROUPS:
            closer, content_re = PATTERN_GROUPS[character]
            end = path.find(closer, cursor + 1)
            if end == -1:
                return False, f"path has an unterminated {character}{closer} pattern: {path}"
            content = path[cursor + 1 : end]
            if any(token in content for token in "<>[]{}") or not content_re.fullmatch(content):
                return False, f"path has an invalid {character}{closer} pattern: {path}"
            has_pattern = True
            cursor = end + 1
            continue
        if character in PATTERN_CLOSERS:
            return False, f"path has an unmatched pattern closer {character}: {path}"
        if character in "*?":
            has_pattern = True
        cursor += 1
    if has_pattern and any(character.isspace() for character in path):
        return False, f"path pattern must not contain whitespace: {path}"
    return has_pattern, None


def validate_mapped_path(
    path: str,
    base_root: Path,
    scope: str,
    *,
    required_first_component: str | None = None,
) -> tuple[str | None, bool]:
    if path != path.strip():
        return f"{scope} path has leading or trailing whitespace: {path!r}", False
    if not path:
        return f"{scope} path is empty", False
    if "\\" in path or any(ord(character) < 32 for character in path):
        return f"{scope} path contains unsupported control or separator characters: {path!r}", False

    has_trailing_slash = path.endswith("/")
    path_without_trailing_slash = path[:-1] if has_trailing_slash else path
    components = path_without_trailing_slash.split("/")
    if any(component == "" for component in components):
        return f"{scope} path contains an empty component: {path}", False
    if any(component in {".", ".."} for component in components):
        return f"{scope} path contains a forbidden . or .. component: {path}", False
    if required_first_component is not None and components[0] != required_first_component:
        return f"{scope} path must start with {required_first_component}/: {path}", False

    has_pattern, pattern_error = validate_pattern_syntax(path)
    if pattern_error:
        return f"{scope} {pattern_error}", False
    if has_pattern:
        return None, False

    candidate = base_root.joinpath(*components)
    try:
        resolved_base = base_root.resolve()
        resolved_candidate = candidate.resolve(strict=False)
        resolved_candidate.relative_to(resolved_base)
    except ValueError:
        return f"{scope} path escapes its allowed root: {path}", False
    except (OSError, RuntimeError) as error:
        return f"{scope} path cannot be resolved safely: {path}: {error}", False

    try:
        if not candidate.exists():
            return f"{scope} path does not exist: {path}", False
        if has_trailing_slash and not candidate.is_dir():
            return f"{scope} path has a trailing slash but is not a directory: {path}", False
    except (OSError, RuntimeError) as error:
        return f"{scope} path cannot be inspected safely: {path}: {error}", False
    return None, True


def check_paths(markdown: str, repo_root: Path) -> tuple[list[str], int]:
    paths, errors = extract_arcadia_path_candidates(markdown)
    checked = 0
    for path in paths:
        error, was_checked = validate_mapped_path(
            path,
            repo_root,
            "Arcadia",
            required_first_component="yt",
        )
        if error:
            errors.append(error)
        elif was_checked:
            checked += 1
    return errors, checked


def check_scoped_ast_paths(markdown: str, repo_root: Path) -> list[str]:
    paths, errors = extract_scoped_ast_paths(markdown)
    for path in paths:
        if path.startswith("yt/yt/"):
            errors.append(
                "ast-index command uses an Arcadia-root path; from yt/ use the scoped form without "
                f"the first yt/: {path}"
            )
            continue
        error, _ = validate_mapped_path(path, repo_root / "yt", "scoped ast-index")
        if error:
            errors.append(error)
    return errors


def compare_inventory(inventory: dict[str, Any], actual: list[str]) -> list[str]:
    expected = inventory["top_level_directories"]
    added = sorted(set(actual) - set(expected))
    removed = sorted(set(expected) - set(actual))
    errors = []
    if added:
        errors.append("new tracked top-level yt/ directories: " + ", ".join(added))
    if removed:
        errors.append("tracked top-level yt/ directories disappeared: " + ", ".join(removed))
    return errors


def write_inventory(path: Path, inventory: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(inventory, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    map_path = resolve(repo_root, args.map_path)
    inventory_path = resolve(repo_root, args.inventory_path)

    try:
        markdown = map_path.read_text(encoding="utf-8")
        inventory = load_inventory(inventory_path)
        actual_directories = tracked_top_level_directories(repo_root)
    except (OSError, ValueError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1

    errors, checked_paths = check_paths(markdown, repo_root)
    errors.extend(check_scoped_ast_paths(markdown, repo_root))

    # An explicit attestation may renew an expired date, but the old value must
    # still have a valid schema and must not point into the future.
    age_limit = 0 if args.reviewed_on is not None else args.max_review_age_days
    age_errors, review_age = check_review_age(inventory, age_limit)
    errors.extend(age_errors)
    if not args.refresh_inventory:
        errors.extend(compare_inventory(inventory, actual_directories))

    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1

    if args.refresh_inventory or args.reviewed_on is not None:
        updated = dict(inventory)
        if args.refresh_inventory:
            updated["top_level_directories"] = actual_directories
        if args.reviewed_on is not None:
            updated["reviewed_on"] = args.reviewed_on.isoformat()
        write_inventory(inventory_path, updated)

        changes = []
        if args.refresh_inventory:
            changes.append(f"{len(actual_directories)} tracked directories from HEAD")
        if args.reviewed_on is not None:
            changes.append(f"manual review attestation {args.reviewed_on.isoformat()}")
        print(f"Updated {inventory_path}: " + "; ".join(changes))
        return 0

    print(
        f"Checked {checked_paths} exact Arcadia paths, "
        f"{len(actual_directories)} tracked top-level yt/ directories, "
        f"review age {review_age} days"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
