from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


ALL_AGGREGATE_FUNCTIONS = [
    "sum",
    "max",
    "min",
    # "first",
    # NB: `first()` is a special function similar to `random()`.
    # TODO(dtorilov): Support such special functions.
]


STRING_AGGREGATE_FUNCTIONS = [
    "max",
    "min",
    # "first",
]


@dataclass
class ProjectionAggregate:
    function: str
    column: str


class TotalsMode(Enum):
    NONE = 0
    BEFORE_HAVING = 1
    AFTER_HAVING = 2


@dataclass
class Query:
    group_keys: list[str] = field(default_factory=list)
    order_keys: list[str] = field(default_factory=list)
    projection_columns: list[str] = field(default_factory=list)
    projection_aggregates: list[ProjectionAggregate] = field(default_factory=list)
    having_predicates: list = field(default_factory=list)
    filter_predicates: list = field(default_factory=list)
    totals_mode: TotalsMode = TotalsMode.NONE
    limit: Optional[int] = None
