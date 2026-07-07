from __future__ import annotations

import logging
from typing import Any, Iterable, Optional, cast


class MessageBuilder():
    """Create logging message with context fields."""

    def __init__(self, initial_message: str) -> None:
        self._underlying = initial_message
        self._fields: list[str] = []

    def with_fields(
        self,
        items: dict[str, Any] | Iterable[tuple[str, Any]],
    ) -> MessageBuilder:
        if isinstance(items, dict):
            items = cast(dict, items).items()
        for key, value in items:
            self._fields.append(f"{key}: {value}")
        return self

    def with_field(self, key: str, value: Any) -> MessageBuilder:
        return self.with_fields([(key, value)])

    def build(self) -> str:
        if not self._fields:
            return self._underlying
        return f'{self._underlying} ({", ".join(self._fields)})'


def log_dry_run(msg: str, logger: logging.Logger) -> None:
    """Log message specific to the dry-run mode."""
    logger.info(f"[DRY-RUN] {msg}")


def format_attributes(attributes: dict[str, Any]) -> str:
    return ", ".join(f"{k.capitalize()}: {v}" for k, v in attributes.items())


def compare_values(
    a: Any,
    b: Any,
    patch_mode: bool = False,
    path: str = "",
    mismatches: Optional[list[str]] = None,
) -> list[str]:
    """Compare two structures recursively with configurable strictness.

    If `patch_mode` is True, ignores fields missing in `b`.
    """
    if mismatches is None:
        mismatches = []

    if isinstance(a, dict) and isinstance(b, dict):
        for k in (set(b) if patch_mode else set(a) | set(b)):
            new_path = f"{path}/{k}" if path else k
            if k not in a:
                mismatches.append(f"Missing in current: {new_path}")
            elif k not in b:
                mismatches.append(f"Missing in expected: {new_path}")
            else:
                compare_values(a[k], b[k], patch_mode, new_path, mismatches)
    elif isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            mismatches.append(
                "List length mismatch{}".format(f" at {path}" if path else ""))
        for i, (x, y) in enumerate(zip(a, b)):
            compare_values(x, y, patch_mode, f"{path}[{i}]", mismatches)
    elif a != b:
        prefix = "Value mismatch{}".format(f" at {path}" if path else "")
        mismatches.append(f"{prefix}: {a!r} != {b!r}")

    return mismatches
