from .ql_base import Query, ProjectionAggregate
from .ql_engine import run_query
from .ql_generator import make_query
from .ql_printer import to_ql_notation
from .ql_util import compare_rows_like_in_ql

from pprint import pformat


class IExecutor:
    def run_query(self, query: str) -> list:
        pass


def query_result_is_deterministic(query: Query) -> bool:
    if query.limit is None:
        return False  # Query is not ordered.

    for key in query.projection_columns:
        if key not in query.order_keys:
            return False  # There are many correct ways to sort the answer.

    if query.projection_aggregates:
        return False  # TODO(dtorilov): This should be similar to projection_columns.

    return True


def unsorted_lists_have_identical_items(expected, actual) -> (bool, list):
    expected_map = {}
    for row in expected:
        row = tuple(row)
        if row not in expected_map:
            expected_map[row] = 1
        else:
            expected_map[row] += 1

    for row in actual:
        row = tuple(row)
        if row not in expected_map:
            return (False, row)
        expected_map[row] -= 1
        if expected_map[row] == 0:
            expected_map.pop(row)

    return (True, [])


def rowset_looks_sorted(
    rowset: list,
    order_keys: list[str],
    projection_columns: list[str],
    projection_aggregates: list[ProjectionAggregate],
) -> bool:
    # We find the longest prefix of the sort key, the values of which are in the projections.
    indices = []
    for order_key in order_keys:
        found = False
        for index, projection in enumerate(projection_columns + projection_aggregates):
            if order_key == projection:
                indices.append(index)
                found = True
        if not found:
            break

    for i in range(len(rowset) - 1):
        lhs = rowset[i]
        rhs = rowset[i + 1]

        lhs_known_order_key = [lhs[i] for i in indices]
        rhs_known_order_key = [rhs[i] for i in indices]

        compare = compare_rows_like_in_ql(lhs_known_order_key, rhs_known_order_key)

        if compare > 0:
            return False
    return True


def check_one_query_with_group_by(
    query: Query,
    table_path: str,
    table_data: list,
    column_name_to_index: dict[str, int],
    column_name_to_type: dict[str, str],
    actual_executor: IExecutor,
    verbose: bool,
) -> None:
    expected = run_query(query, table_data, column_name_to_index, column_name_to_type)
    actual = actual_executor.run_query(to_ql_notation(query, table_path, False))

    error_message = ""

    if query_result_is_deterministic(query):
        if expected.truncated_rows != actual:
            error_message += "Query result is deterministic, but expected != actual\n\n"
    else:
        looks_sorted = rowset_looks_sorted(
            rowset=actual,
            order_keys=query.order_keys,
            projection_columns=query.projection_columns,
            projection_aggregates=query.projection_aggregates,
        )

        if not looks_sorted:
            error_message += "Query result does not look sorted\n\n"

        if len(expected.truncated_rows) != len(actual):
            error_message += (
                f"Query result has wrong length: expected {len(expected.truncated_rows)}, but got {len(actual)}\n\n"
            )

        identical, counterexample = unsorted_lists_have_identical_items(expected.rows, actual)
        if not identical:
            error_message += "Query result has wrong entry: "
            error_message += pformat(counterexample)
            error_message += "\n"

    if error_message:
        if verbose:
            error_message += "expected:\n"
            error_message += pformat(expected)
            error_message += "\n"
            error_message += "\n"
            error_message += "actual:\n"
            error_message += pformat(actual)
            error_message += "\n"
            error_message += "\n"
            error_message += pformat(query)
            error_message += "\n"
            error_message += "\n"
            error_message += to_ql_notation(query, table_path, True)
            error_message += "\n"
            error_message += "\n"
        error_message += to_ql_notation(query, table_path, False)
        raise Exception(error_message)


def check_random_query_with_group_by(
    column_names: list[str],
    column_name_to_type: dict[str, str],
    group_keys_length: int,
    projection_columns_length: int,
    projection_aggregates_length: int,
    order_keys_length: int,
    table_path: str,
    table_data: list,
    column_name_to_index: dict[str, int],
    where_clause: str,
    verbose: bool,
    actual_executor: IExecutor,
) -> None:
    query = make_query(
        column_names,
        column_name_to_type,
        group_keys_length,
        projection_columns_length,
        projection_aggregates_length,
        order_keys_length,
    )

    if where_clause:
        query.filter_predicates.append(where_clause)

    check_one_query_with_group_by(
        query, table_path, table_data, column_name_to_index, column_name_to_type, actual_executor, verbose
    )
