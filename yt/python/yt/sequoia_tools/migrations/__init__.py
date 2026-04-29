"""Ground cluster state migrations on ground reign promotion."""

from typing import Any, Callable

from .. import actions, app

from . import (
    m0002,
    m0003,
    m0004,
)


BASE_GROUND_REIGN = 1

MIGRATION_PLANNERS: dict[int, Callable[[app.SequoiaTool], actions.ActionPlan]] = {
    2: m0002.alter_child_node_table,
    3: m0003.alter_chunk_replicas_table,
    4: m0004.alter_location_replicas_table,
}

assert BASE_GROUND_REIGN not in MIGRATION_PLANNERS


KNOWN_GROUND_REIGNS = [BASE_GROUND_REIGN] + list(MIGRATION_PLANNERS.keys())

MIN_GROUND_REIGN = BASE_GROUND_REIGN
MAX_GROUND_REIGN = max(KNOWN_GROUND_REIGNS)

assert MAX_GROUND_REIGN - MIN_GROUND_REIGN + 1 == len(KNOWN_GROUND_REIGNS), (
    "Known ground reigns must be continious")


def validate_ground_reign(value: Any) -> None:
    if not isinstance(value, int):
        raise ValueError(f"Ground reign must be an integer, got {type(value)}")

    if value > MAX_GROUND_REIGN:
        raise ValueError(f"Reign must be not greater than {MAX_GROUND_REIGN}")

    if value < MIN_GROUND_REIGN:
        raise ValueError(f"Reign must be not less than {MIN_GROUND_REIGN}")
