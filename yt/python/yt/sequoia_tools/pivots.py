from __future__ import annotations

import enum
from typing import Any, Callable, Sequence

from yt.yson import YsonUint64


PivotKey = list
PivotFunction = Callable[..., list[PivotKey]]


class Type(enum.Enum):
    DISCRETE = "discrete"
    CONTINUOUS = "continuous"


Dimension = tuple[Type, Any]


_SMALL_HASH_CARDINALITY = 2 ** 4
_HASH_CARDINALITY = 2 ** 64


def _build_pivot_keys(
    dimensions: Sequence[Dimension],
    shard_count: int,
) -> list[PivotKey]:
    """Build shard_count pivot keys over the lexicographic product of dimensions."""
    if shard_count < 0:
        raise ValueError(f"Shard count must be non-negative, got {shard_count}")
    if shard_count <= 1 or not dimensions:
        return [[]]

    cardinalities: list[int] = []
    for kind, payload in dimensions:
        if kind is Type.DISCRETE:
            cardinalities.append(len(payload))
        elif kind is Type.CONTINUOUS:
            cardinalities.append(int(payload))

    N = 1
    for c in cardinalities:
        N *= c
    if N == 0:
        return [[]]

    if shard_count > N:
        shard_count = N

    m = len(dimensions)
    strides = [1] * m
    for i in range(m - 2, -1, -1):
        strides[i] = strides[i + 1] * cardinalities[i + 1]

    pivots: list[PivotKey] = [[]]
    for j in range(1, shard_count):
        idx = (j * N) // shard_count
        pivot: PivotKey = []
        for i, (kind, payload) in enumerate(dimensions):
            digit = idx // strides[i]
            idx %= strides[i]
            if kind is Type.DISCRETE:
                pivot.append(payload[digit])
            else:
                pivot.append(YsonUint64(digit))
        pivots.append(pivot)

    assert len(pivots) == shard_count
    return pivots


def _normalize_cell_tags(cell_tags: Sequence[Any]) -> list[YsonUint64]:
    return sorted(YsonUint64(int(tag)) for tag in cell_tags)


def _default_pivots(shard_count: int, cell_tags: list[str], version: int) -> list[PivotKey]:
    return _build_pivot_keys([(Type.CONTINUOUS, _HASH_CARDINALITY)], shard_count)


def _chunk_replicas(shard_count: int, cell_tags: list[str], version: int) -> list[PivotKey]:
    cell_tags = _normalize_cell_tags(cell_tags)

    if version >= 5:
        return _build_pivot_keys([
            (Type.CONTINUOUS, _SMALL_HASH_CARDINALITY),
            (Type.DISCRETE, cell_tags),
            (Type.DISCRETE, list(range(60))),
            (Type.CONTINUOUS, _HASH_CARDINALITY),
        ], shard_count)
    elif version >= 3:
        return _build_pivot_keys([
            (Type.DISCRETE, cell_tags),
            (Type.DISCRETE, list(range(60))),
            (Type.CONTINUOUS, _HASH_CARDINALITY),
        ], shard_count)
    else:
        return _build_pivot_keys([(Type.CONTINUOUS, _HASH_CARDINALITY)], shard_count)


def _location_replicas(shard_count: int, cell_tags: list[str], version: int) -> list[PivotKey]:
    cell_tags = _normalize_cell_tags(cell_tags)

    if version >= 6:
        return _build_pivot_keys([
            (Type.CONTINUOUS, _SMALL_HASH_CARDINALITY),
            (Type.DISCRETE, cell_tags),
            (Type.CONTINUOUS, _HASH_CARDINALITY),
        ], shard_count)
    elif version >= 4:
        return _build_pivot_keys([
            (Type.CONTINUOUS, _SMALL_HASH_CARDINALITY),
            (Type.DISCRETE, cell_tags),
        ], shard_count)
    else:
        return _build_pivot_keys([(Type.DISCRETE, cell_tags)], shard_count)


_REGISTRY: dict[str, PivotFunction] = {
    "acls":                      _default_pivots,
    "child_forks":               _default_pivots,
    "child_nodes":               _default_pivots,
    "chunk_replicas":            _chunk_replicas,
    "dependent_transactions":    _default_pivots,
    "doomed_transactions":       _default_pivots,
    "location_replicas":         _location_replicas,
    "node_forks":                _default_pivots,
    "node_snapshots":            _default_pivots,
    "path_forks":                _default_pivots,
    "transaction_descendants":   _default_pivots,
    "transaction_replicas":      _default_pivots,
    "transactions":              _default_pivots,
    "unapproved_chunk_replicas": _default_pivots,
}


def get_pivot_function(logical_name: str) -> PivotFunction | None:
    return _REGISTRY.get(logical_name)
