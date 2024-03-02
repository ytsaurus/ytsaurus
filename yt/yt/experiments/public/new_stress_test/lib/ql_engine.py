from .ql_base import Query, ProjectionAggregate
from .ql_util import compare_rows_like_in_ql

from functools import cmp_to_key
from dataclasses import dataclass


def aggregate_init(
    aggregate: ProjectionAggregate,
    row: list,
    column_name_to_index: dict[str, int],
):
    return row[column_name_to_index[aggregate.column]]


def aggregate_update(
    aggregate: ProjectionAggregate,
    row: list,
    state,
    column_name_to_index: dict[str, int],
):
    value = row[column_name_to_index[aggregate.column]]

    if value is None:
        return state

    if state is None and aggregate.function != "first":
        return aggregate_init(aggregate, row, column_name_to_index)

    if aggregate.function == "sum":
        return state + value
    if aggregate.function == "max":
        return max(state, value)
    if aggregate.function == "min":
        return min(state, value)
    if aggregate.function == "first":
        return state

    assert False


def aggregate_finalize(
    aggregate: ProjectionAggregate,
    column_name_to_type: dict[str, str],
    state,
):
    if state is None:
        return state

    if column_name_to_type[aggregate.column] == "int64":
        return state % (2**63)

    if column_name_to_type[aggregate.column] == "uint64":
        return state % (2**64)

    return state


@dataclass
class QueryResult:
    rows: list
    truncated: bool
    truncated_rows: list


def run_query(
    query: Query,
    table_data: list,
    column_name_to_index: dict[str, int],
    column_name_to_type: dict[str, str],
) -> QueryResult:
    hash_map = {}

    for row in table_data:
        key = tuple(row[column_name_to_index[name]] for name in query.group_keys)

        if key not in hash_map:
            projections = [row[column_name_to_index[name]] for name in query.projection_columns]
            states = [aggregate_init(aggregate, row, column_name_to_index) for aggregate in query.projection_aggregates]
            order_keys = [row[column_name_to_index[name]] for name in query.order_keys]
            hash_map[key] = (projections, states, order_keys)
        else:
            projections, states, order_keys = hash_map[key]
            states = [
                aggregate_update(query.projection_aggregates[i], row, states[i], column_name_to_index)
                for i in range(len(states))
            ]
            hash_map[key] = (projections, states, order_keys)

    unsorted = []
    for key, value in hash_map.items():
        projections, states, order_keys = value
        finalized = [
            aggregate_finalize(query.projection_aggregates[i], column_name_to_type, states[i])
            for i in range(len(states))
        ]
        unsorted.append((projections + finalized, order_keys))

    unsorted.sort(key=cmp_to_key(lambda lhs, rhs: compare_rows_like_in_ql(lhs[1], rhs[1])))

    result = [i[0] for i in unsorted]

    return QueryResult(
        rows=result,
        truncated=query.limit is not None and len(result) > query.limit,
        truncated_rows=result[: query.limit] if query.limit is not None else result,
    )
