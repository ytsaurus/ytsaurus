from .ql_base import ProjectionAggregate, STRING_AGGREGATE_FUNCTIONS, ALL_AGGREGATE_FUNCTIONS, Query, TotalsMode

import random


def make_group_keys(
    column_names: list[str],
    column_name_to_type: dict[str, str],
    group_keys_length: int,
) -> list[str]:
    assert not all(map(lambda x: column_name_to_type[x] == "yson", column_names))

    result = []
    while len(result) < group_keys_length:
        column = random.choice(column_names)
        if column_name_to_type[column] != "yson":
            result.append(column)
    return result


def make_aggregates(
    not_grouped_columns: list[str],
    projection_aggregates_length: int,
    column_name_to_type: dict[str, str],
) -> list[ProjectionAggregate]:
    if len(not_grouped_columns) == 0:
        return []

    result = []
    attempt = 0

    while len(result) < projection_aggregates_length and attempt < 1024:
        attempt += 1

        column = random.choice(not_grouped_columns)
        column_type = column_name_to_type[column]
        if column_type == "yson":
            continue
        if column_type == "bool":
            # TODO(dtorilov): Some aggregate functions can take boolean values as input.
            continue
        elif column_type == "string":
            result.append(ProjectionAggregate(random.choice(STRING_AGGREGATE_FUNCTIONS), column))
        else:
            result.append(ProjectionAggregate(random.choice(ALL_AGGREGATE_FUNCTIONS), column))

    return result


def make_query(
    column_names: list[str],
    column_name_to_type: dict[str, str],
    group_keys_length: int,
    projection_columns_length: int,
    projection_aggregates_length: int,
    order_keys_length: int,
) -> Query:
    query = Query()

    query.group_keys = make_group_keys(column_names, column_name_to_type, group_keys_length)

    query.projection_columns = [random.choice(query.group_keys) for _ in range(projection_columns_length)]

    not_grouped_columns = [column for column in column_names if column not in query.group_keys]

    query.projection_aggregates = make_aggregates(
        not_grouped_columns, projection_aggregates_length, column_name_to_type
    )

    # TODO(dtorilov): Support filter predicates.
    # TODO(dtorilov): Support having predicates.

    # TODO(dtorilov): Support totals clause.
    query.totals_mode = random.choice([TotalsMode.NONE])

    # TODO(dtorilov): Support both ascending and descending sorting.
    query.order_keys = [random.choice(query.group_keys) for _ in range(order_keys_length)]

    if query.order_keys or random.randint(0, 100) > 50:
        query.limit = random.randint(0, 100)

    return query
