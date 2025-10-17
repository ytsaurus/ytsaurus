import re
import sqlglot
import typing
import yt.yson as yson


from .constants import (
    SQL_TYPE_TO_YT_TYPE,
    DEFAULT_YT_ROWS_COUNT_LIMIT,
)
from .data import TableColumnInfo
from .suite import Suite


class YTSuiteAdapter(Suite):
    def _transform_statement_table_name(self, ast: sqlglot.Expression) -> sqlglot.Expression:
        updated_table_references = set()

        def transform_table_name(node: sqlglot.Expression) -> sqlglot.Expression:
            if isinstance(node, sqlglot.exp.Table):
                updated_name_indentifier = self._get_table_indentifier(node.name)
                updated_table_references.add(node.name)
                return sqlglot.exp.Table(this=updated_name_indentifier, alias=node.alias)
            elif isinstance(node, sqlglot.exp.Identifier):
                if node.name in updated_table_references:
                    updated_name_indentifier = self._get_table_indentifier(node.name)
                    return sqlglot.exp.Identifier(this=updated_name_indentifier)
            return node

        return ast.transform(transform_table_name)

    def _transform_statement_append_limit(self, ast: sqlglot.Expression) -> sqlglot.Expression:
        def append_limit(node: sqlglot.Expression) -> sqlglot.Expression:
            if (
                isinstance(node, sqlglot.exp.Select)
                and node.find(sqlglot.exp.Order)
                and not node.find(sqlglot.exp.Limit)
            ):
                return node.limit(DEFAULT_YT_ROWS_COUNT_LIMIT)
            return node

        return ast.transform(append_limit)

    def _get_statement(self, statement: str) -> str:
        ast = sqlglot.parse_one(statement)

        ast = self._transform_statement_table_name(ast)
        ast = self._transform_statement_append_limit(ast)

        query_string = ast.sql()
        query_string = query_string.replace("COUNT(*)", "sum(1)")

        return query_string.lower()

    def _get_table_indentifier(self, table_name: str) -> sqlglot.exp.Identifier:
        updated_name = self._to_yson_table_name(table_name)
        indentifier = sqlglot.to_identifier(f"`{updated_name}`", quoted=False)
        return indentifier

    def _get_column_type(self, column_type: str) -> str:
        if column_type in SQL_TYPE_TO_YT_TYPE:
            return SQL_TYPE_TO_YT_TYPE[column_type]
        else:
            match = re.match(r"\w+\(\d+\)", column_type)
            if match:
                return self._get_column_type(re.sub("\\d+", "*", column_type))

            raise RuntimeError(f"Unknown column type {column_type}")

    def _get_expected_row(self, expected_row: tuple[typing.Any]) -> str:
        row_with_attributes = [
            yson.to_yson_type(value, attributes={"id": index}) for index, value in enumerate(expected_row)
        ]

        return yson.dumps(row_with_attributes, yson_type="list_fragment")

    @typing.override
    def _to_yson_table_name(self, name: str) -> typing.Any:
        return f"//{name}"

    @typing.override
    def _to_yson_table_schema(self, schema: list[TableColumnInfo]) -> typing.Any:
        schema = [{"name": column.name, "type": self._get_column_type(column.type)} for column in schema]
        return yson.dumps(schema)

    @typing.override
    def _to_yson_source_row(self, schema: list[TableColumnInfo], inserted_row) -> typing.Any:
        row = {info.name: inserted_row[index] for index, info in enumerate(schema)}
        return yson.dumps(row, yson_type="map_fragment")

    @typing.override
    def _to_yson_query(self, name: str, query: str) -> typing.Any:
        query = {
            "name": name,
            "query": self._get_statement(query),
            "expected_rows": [
                self._get_expected_row(expected_row) for expected_row in self._expected_rows_by_query_name[name]
            ],
        }
        if name in self._skip_reason_by_test_name:
            query["skip_reason"] = self._skip_reason_by_test_name[name]
        if name in self._sorted_result_queries:
            query["sort_result"] = True
        return query
