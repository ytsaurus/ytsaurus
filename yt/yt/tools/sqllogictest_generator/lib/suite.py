import sqlglot
import typing
import yt.yson as yson
from functools import cmp_to_key

from .enum import StatementType, SQLLogicSortMode
from .parser import BaseStatement, Statement, Query
from .data import TableColumnInfo
from .executor import Executor


class Suite:
    def __init__(self):
        self._schema_by_table_name = {}
        self._inserted_rows_by_table_name = {}
        self._query_by_name = {}
        self._expected_rows_by_query_name = {}
        self._skip_reason_by_test_name = {}
        self._sorted_result_queries = set()

    def _to_yson_table_name(self, name: str) -> typing.Any:
        return name

    def _to_yson_table_schema(self, schema: list[TableColumnInfo]) -> typing.Any:
        return [(c.name, c.type) for c in schema]

    def _to_yson_source_row(self, schema: list[TableColumnInfo], inserted_row) -> typing.Any:
        return inserted_row

    def _to_yson_query(self, name: str, query: str) -> typing.Any:
        query = {"name": name, "query": query, "expected_rows": self._expected_rows_by_query_name[name]}
        if name in self._skip_reason_by_test_name:
            query["skip_reason"] = self._skip_reason_by_test_name[name]
        return query

    def to_yson(self):
        data = {
            "table_schema": {
                self._to_yson_table_name(name): self._to_yson_table_schema(schema)
                for name, schema in self._schema_by_table_name.items()
            },
            "source_rows": {
                self._to_yson_table_name(name): [
                    self._to_yson_source_row(self._schema_by_table_name[name], inserted_row)
                    for inserted_row in inserted_rows
                ]
                for name, inserted_rows in self._inserted_rows_by_table_name.items()
            },
            "queries": [self._to_yson_query(name, query) for name, query in self._query_by_name.items()],
        }

        return yson.dumps(data, yson_format="pretty", indent=4)

    def add_skip_reason_data(self, skip_reason_data: dict[str, list[str]]) -> None:
        for reason, test_names in skip_reason_data.items():
            for test_name in test_names:
                self._skip_reason_by_test_name[test_name] = reason

    def add_table_schema(self, table_name: str, table_schema: list[TableColumnInfo]):
        self._schema_by_table_name[table_name] = table_schema

    def add_inserted_row(self, table_name: str, inserted_row: tuple[typing.Any]):
        if table_name not in self._inserted_rows_by_table_name:
            self._inserted_rows_by_table_name[table_name] = []

        self._inserted_rows_by_table_name[table_name].append(inserted_row)

    def add_query(self, query_name: str, query: str, expected_rows: list[tuple[typing.Any]], sorted: bool):
        self._query_by_name[query_name] = query
        self._expected_rows_by_query_name[query_name] = expected_rows
        if sorted:
            self._sorted_result_queries.add(query_name)


class SuiteCollector:
    def __init__(self, executor: Executor, suite_type: typing.Type):
        self.executor = executor
        self._suite = suite_type()

    def process_statement(self, statement: BaseStatement):
        ast = sqlglot.parse_one(statement.statement)

        if statement.sql_statement_type == StatementType.CREATE:
            assert isinstance(statement, Statement)

            try:
                self.executor.create(statement.statement)
            except Exception as e:
                if not statement.with_error:
                    raise e

            table_name = ast.this.this.name
            assert table_name
            table_schema = self.executor.get_schema(table_name)

            self._suite.add_table_schema(table_name, table_schema)
        elif statement.sql_statement_type == StatementType.INSERT:
            assert isinstance(statement, Statement)

            try:
                inserted_rows = self.executor.insert(ast.returning("*").sql())
            except Exception as e:
                if not statement.with_error:
                    raise e

            table_name = ast.this.this.name
            assert table_name

            for inserted_row in inserted_rows:
                self._suite.add_inserted_row(table_name, inserted_row)
        elif statement.sql_statement_type in (
            StatementType.SELECT,
            StatementType.UNION,
            StatementType.EXCEPT,
            StatementType.INTERSECT,
        ):
            assert isinstance(statement, Query)

            expected_rows = self.executor.select(statement.statement)

            if statement.sort_mode != SQLLogicSortMode.NOSORT:

                def order_rows(row_a, row_b):
                    assert len(row_a) == len(row_b)
                    columns_count = len(row_a)
                    for i in range(0, columns_count):
                        if row_a[i] is None and row_b[i] is None:
                            continue
                        if row_a[i] is None:
                            return -1
                        elif row_b[i] is None:
                            return 1
                        elif row_a[i] == row_b[i]:
                            continue
                        elif row_a[i] < row_b[i]:
                            return -1
                        else:
                            return 1
                    return 0

                sorted_rows = sorted(expected_rows, key=cmp_to_key(order_rows))
                expected_rows = sorted_rows

            query_name = statement.name

            self._suite.add_query(
                query_name, statement.statement, expected_rows, sorted=statement.sort_mode != SQLLogicSortMode.NOSORT
            )
        else:
            raise RuntimeError(f"Unsupported statement type {statement.sql_statement_type}")

    def set_skip_reason_data(self, skip_reason_data: dict[str, list[str]]) -> None:
        self._suite.add_skip_reason_data(skip_reason_data)

    def get_suite(self) -> Suite:
        return self._suite
