from .helpers import create_client, run_operation_and_wrap_error
from .schema import Schema
from .spec import Spec
from .job_base import JobBase
from .process_runner import process_runner

from .ql_checks import check_random_query_with_group_by, IExecutor
from .ql_util import from_yt_to_list

import yt.wrapper as yt
from yt.wrapper.client import YtClient
from yt.wrapper.http_helpers import get_proxy_url

from itertools import product
import sys


class YtCLientExecutor(IExecutor):
    def __init__(self, client: YtClient):
        self.client = client

    def run_query(self, query: str) -> list:
        return from_yt_to_list(list(self.client.select_rows(query)))


def check_group_by_script(fast: bool):
    if fast:
        return range(1, 4), range(1, 4), range(1, 3), range(0, 2), range(15)
    return range(1, 6), range(1, 6), range(1, 6), range(0, 6), range(100)


def rewrite_type(t: str) -> str:
    mapping = {
        "int64": "int64",
        "uint64": "uint64",
        "boolean": "bool",
        "double": "double",
        "string": "string",
        "any": "yson",
    }
    return mapping[t]


def check_group_by(
    column_names: list[str],
    column_name_to_type: dict[str, str],
    table_path: str,
    table_data: list,
    column_name_to_index: dict[str, int],
    where_clause: str,
    executor: IExecutor,
) -> None:
    a, b, c, d, e = check_group_by_script(fast=True)

    for (
        group_keys_length,
        projection_columns_length,
        projection_aggregates_length,
        order_keys_length,
        iteration,
    ) in product(a, b, c, d, e):
        check_random_query_with_group_by(
            column_names,
            column_name_to_type,
            group_keys_length,
            projection_columns_length,
            projection_aggregates_length,
            order_keys_length,
            table_path,
            table_data,
            column_name_to_index,
            where_clause,
            False,
            executor,
        )


def get_current_part_data(iterator, column_names) -> list:
    table_data = []

    for key, rows in iterator:
        table_row = []
        row_as_dict = list(rows)[0]
        for column_name in column_names:
            table_row.append(row_as_dict[column_name])
        table_data.append(table_row)

    return table_data


def row_to_ql_tuple(
    row: list,
    column_names: list[str],
    column_name_to_type: dict[str, str],
) -> str:
    assert len(row) == len(column_names)

    result = "("
    for name, value in zip(column_names, row):
        if value is None:
            result += "null"
        elif column_name_to_type[name] == 'int64':
            result += str(value)
        elif column_name_to_type[name] == 'uint64':
            result += str(value) + 'u'
        elif column_name_to_type[name] == 'bool':
            if value:
                result += "true"
            else:
                result += "false"
        elif column_name_to_type[name] == 'double':
            result += str(value)
        elif column_name_to_type[name] == 'string':
            result += '"' + value + '"'
        elif column_name_to_type[name] == 'yson':
            result += str(value)

        result += ", "

    if result != "(":
        result = result[:-2] + ')'

    return result


@yt.reduce_aggregator
class GroupByReducer(JobBase):
    def __init__(
        self,
        column_names: list[str],
        key_columns: list[str],
        column_name_to_type: dict[str, str],
        table_path: str,
        column_name_to_index: dict[str, int],
        spec: Spec,
    ):
        super(GroupByReducer, self).__init__(spec)

        self.column_names = column_names
        self.key_columns = key_columns
        self.column_name_to_type = column_name_to_type
        self.table_path = table_path
        self.column_name_to_index = column_name_to_index

    def __call__(self, input_row_iterator):
        table_data = get_current_part_data(input_row_iterator, self.column_names)

        first_key = table_data[0][:len(self.key_columns)]
        last_key = table_data[-1][:len(self.key_columns)]

        first = row_to_ql_tuple(first_key, self.key_columns, self.column_name_to_type)
        middle = f"({', '.join(self.key_columns)})"
        last = row_to_ql_tuple(last_key, self.key_columns, self.column_name_to_type)

        where_clause = f"{first} <= {middle} and {middle} <= {last}"

        client = self.create_client()
        executor: IExecutor = YtCLientExecutor(client)

        try:
            check_group_by(
                self.column_names,
                self.column_name_to_type,
                self.table_path,
                table_data,
                self.column_name_to_index,
                where_clause,
                executor,
            )
        except Exception as ex:
            yield {"error": 'error ' + str(ex)}


@process_runner.run_in_process()
def verify_group_by(schema: Schema, key_table: str, data_table: str, output_table: str, spec: Spec):
    client = create_client(get_proxy_url(), yt.config.config)

    column_names: list[str] = schema.get_column_names()
    key_columns: list[str] = schema.get_key_column_names()
    column_name_to_type: dict[str, str] = {
        column.name: rewrite_type(column.type.str()) for column in schema.get_columns()
    }
    table_path: str = data_table
    column_name_to_index: dict[str, int] = {name: index for index, name in enumerate(column_names)}

    op_spec = {
        "job_count": spec.size.job_count,
        "title": "Verify group by",
    }
    if spec.get_read_user_slot_count() is not None:
        op_spec["scheduling_options_per_pool_tree"] = {
            "physical": {"resource_limits": {"user_slots": spec.get_read_user_slot_count()}}
        }

    op = client.run_reduce(
        GroupByReducer(
            column_names,
            key_columns,
            column_name_to_type,
            table_path,
            column_name_to_index,
            spec,
        ),
        key_table,
        output_table,
        reduce_by=schema.get_key_column_names(),
        spec=op_spec,
        sync=False,
    )
    run_operation_and_wrap_error(op, "GroupBy")

    if client.get(output_table + "/@row_count") != 0:
        raise Exception(f"GROUP_BY failed: check {output_table}")
