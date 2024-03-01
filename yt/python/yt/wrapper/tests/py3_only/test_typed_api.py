from yt.common import YtError
from yt.testlib import authors

from yt.wrapper.schema import (
    yt_dataclass,
    TableSchema,
    OutputRow,
    Int8,
    Int16,
    Int32,
    Int64,
    Uint8,
    Uint16,
    Uint32,
    Uint64,
    Float,
    Double,
    Date,
    Datetime,
    Timestamp,
    Interval,
    YsonBytes,
    OtherColumns,
    RowIterator,
    Context,
    Variant,
    FormattedPyDatetime,
)

from yt.wrapper.schema.internal_schema import _get_annotation

from yt.wrapper.prepare_operation import TypedJob
from yt.testlib.helpers import set_config_option, check_rows_equality
from yt.wrapper.testlib.helpers import sync_create_cell, TEST_DIR

import yt.yson as yson
import yt.wrapper as yt
import yt.type_info as ti

from dataclasses import dataclass

import copy
import pytest
import typing
import datetime


@yt_dataclass
class Struct:
    str_field: str
    uint8_field: typing.Optional[Uint64]


@yt_dataclass
class TheRow:
    int32_field: Int64
    str_field: str
    struct_field: typing.Optional[Struct] = None


@yt_dataclass
class RowWithContext(TheRow):
    row_index: int = -1
    table_index: int = -1


@yt_dataclass
class Row1:
    str_field: str
    int32_field: Int64


@yt_dataclass
class AnotherRow:
    int32_field: Int64  # ??, done to match `TheRow` field's type
    uint64_field: Uint64


@yt_dataclass
class SumRow:
    key: str
    sum: Int64


ROW_DICTS = [
    {
        "int32_field": -(2 ** 31),
        "str_field": "Привет, мир",
        "struct_field": {
            "str_field": "И тебе привет",
            "uint8_field": yson.YsonUint64(255),
        },
    },
    {
        "int32_field": -123,
        "str_field": "spam",
        "struct_field": {
            "str_field": "foobar",
            "uint8_field": yson.YsonUint64(123),
        },
    },
]

ROWS = [
    TheRow(
        int32_field=-(2 ** 31),
        str_field="Привет, мир",
        struct_field=Struct(
            str_field="И тебе привет",
            uint8_field=255,
        ),
    ),
    TheRow(
        int32_field=-123,
        str_field="spam",
        struct_field=Struct(
            str_field="foobar",
            uint8_field=123,
        ),
    ),
]


@yt.with_context
class IdentityMapper(TypedJob):
    def __init__(self, row_types=None):
        if row_types is None:
            row_types = [TheRow]
        self._row_types = row_types

    def prepare_operation(self, context, preparer):
        for i, row_type in enumerate(self._row_types):
            preparer.input(i, type=row_type).output(i, type=row_type)

    def __call__(self, row, context):
        yield OutputRow(row, table_index=context.get_table_index())

    def get_intermediate_stream_count(self):
        return len(self._row_types)


@yt.with_context
class MapperWithContext(TypedJob):
    def prepare_operation(self, context, preparer):
        preparer.inputs([0, 1], type=TheRow).output(0, type=RowWithContext)

    def __call__(self, row, context):
        yield RowWithContext(
            int32_field=row.int32_field,
            str_field=row.str_field,
            struct_field=row.struct_field,
            row_index=context.get_row_index(),
            table_index=context.get_table_index(),
        )


class IdentityReducer(TypedJob):
    def prepare_operation(self, context, preparer):
        preparer.input(0, type=TheRow).output(0, type=TheRow)

    def __call__(self, rows):
        for row in rows:
            yield row


class EmptyInputReducer(TypedJob):
    def prepare_operation(self, context, preparer):
        preparer.input(0, type=TheRow).output(0, type=TheRow)

    def __call__(self, rows):
        assert False, "The method should have never been called"

    def finish(self):
        # Check that the job has actually been run.
        yield ROWS[0]


@yt.with_context
class TwoOutputMapper(TypedJob):
    def __init__(self, for_map_reduce):
        self._for_map_reduce = for_map_reduce

    def prepare_operation(self, context, preparer):
        (preparer.input(0, type=TheRow).output(0, type=TheRow).output(1, type=Row1))

    def __call__(self, row, context):
        assert context.get_table_index() == 0
        yield OutputRow(row, table_index=0)
        yield OutputRow(
            Row1(int32_field=row.int32_field + 1, str_field="Mapped: " + row.str_field),
            table_index=1,
        )

    def get_intermediate_stream_count(self):
        return 2 if self._for_map_reduce else 0


@yt.aggregator
class AggregatorTwoOutputMapper(TypedJob):
    def prepare_operation(self, context, preparer):
        (preparer.input(0, type=TheRow).output(0, type=TheRow).output(1, type=Row1))

    def __call__(self, rows):
        for row, context in rows.with_context():
            assert context.get_table_index() == 0
            yield OutputRow(row, table_index=0)
            yield OutputRow(
                Row1(int32_field=row.int32_field + 1, str_field="Mapped: " + row.str_field),
                table_index=1,
            )


@yt.with_context
class MapperWithBigPrepareOperation(TypedJob):
    def __init__(self, first_output_schema):
        self._first_output_schema = first_output_schema

    def prepare_operation(self, context, preparer):
        (
            preparer.input(
                0,
                type=TheRow,
                column_filter=["int32_field", "str_field", "struct_field"],
                column_renaming={"int32_field_original": "int32_field"},
            )
            .inputs([1, 2], type=TheRow)
            .output(0, type=TheRow, schema=self._first_output_schema)
            .outputs(range(1, 6), type=TheRow)
        )

    def __call__(self, row, context):
        yield OutputRow(row, table_index=context.get_table_index())


class TwoInputReducer(TypedJob):
    def prepare_operation(self, context, preparer):
        preparer.input(0, type=TheRow).input(1, type=Row1).output(0, type=TheRow)

    def __call__(self, rows):
        for row, ctx in rows.with_context():
            if ctx.get_table_index() == 0:
                assert isinstance(row, TheRow)
                yield row
            else:
                assert ctx.get_table_index() == 1
                assert isinstance(row, Row1)
                yield TheRow(
                    int32_field=row.int32_field,
                    str_field=row.str_field,
                    struct_field=Struct(str_field="foo", uint8_field=12),
                )


class SummingReducer(TypedJob):
    def prepare_operation(self, context, preparer):
        preparer.input(0, type=TheRow).output(0, type=SumRow)

    def __call__(self, rows):
        s = 0
        key = None
        for row in rows:
            key = row.str_field
            s += row.int32_field
        assert key is not None
        yield SumRow(key=key, sum=s)


class TwoInputSummingReducer(TypedJob):
    def prepare_operation(self, context, preparer):
        preparer.input(0, type=TheRow).input(1, type=Row1).output(0, type=SumRow)

    def __call__(self, rows):
        s = 0
        key = None
        for row, ctx in rows.with_context():
            if ctx.get_table_index() == 0:
                assert isinstance(row, TheRow)
            else:
                assert ctx.get_table_index() == 1
                assert isinstance(row, Row1)
            key = row.str_field
            s += row.int32_field
        assert key is not None
        yield SumRow(key=key, sum=s)


TWO_OUTPUT_MAPPER_ROWS_SECOND_TABLE = [
    {
        "int32_field": -(2 ** 31) + 1,
        "str_field": "Mapped: Привет, мир",
    },
    {
        "int32_field": -123 + 1,
        "str_field": "Mapped: spam",
    },
]


TWO_INPUT_REDUCER_OUTPUT_ROWS = ROW_DICTS + [
    {
        "int32_field": -(2 ** 31) + 1,
        "str_field": "Mapped: Привет, мир",
        "struct_field": {
            "str_field": "foo",
            "uint8_field": 12,
        },
    },
    {
        "int32_field": -123 + 1,
        "str_field": "Mapped: spam",
        "struct_field": {
            "str_field": "foo",
            "uint8_field": 12,
        },
    },
]


def write_and_read_primitive(py_type, ti_type, value, mode):
    @yt_dataclass
    class Row1:
        field: py_type

    schema = TableSchema().add_column("field", ti_type)
    path = "//tmp/table"
    yt.create("table", path, force=True, attributes={"schema": schema})
    if mode == "write_unstructured_read_structured":
        yt.write_table(path, [{"field": value}])
        rows = list(yt.read_table_structured(path, Row1))
        assert len(rows) == 1
        return rows[0].field
    elif mode == "write_structured_read_unstructured":
        yt.write_table_structured(path, Row1, [Row1(field=value)])
        rows = list(yt.read_table(path))
        assert len(rows) == 1
        return rows[0]["field"]
    else:
        assert False, "Unsupported mode {}".format(mode)


@pytest.mark.usefixtures("yt_env_v4")
class TestTypedApi(object):
    @authors("levysotsky")
    def test_basic_read_and_write(self):
        table = "//tmp/table"
        yt.remove(table, force=True)
        yt.create("table", table, attributes={"schema": TableSchema.from_row_type(TheRow)})
        yt.write_table(table, ROW_DICTS)
        iterator = yt.read_table_structured(table, TheRow)
        row = next(iterator)
        assert iterator.get_row_index() == 0
        assert iterator.get_table_index() == 0
        assert iterator.get_key_switch() is False
        assert row == ROWS[0]

        row = next(iterator)
        assert iterator.get_row_index() == 1
        assert iterator.get_table_index() == 0
        assert iterator.get_key_switch() is False
        assert row == ROWS[1]

        with pytest.raises(StopIteration):
            next(iterator)

        yt.write_table_structured(table, TheRow, ROWS)
        read_rows = list(yt.read_table(table))
        assert read_rows == ROW_DICTS
        yt.write_table_structured(table, TheRow, [])
        read_rows = list(yt.read_table(table))
        assert read_rows == []

    @authors("denvr")
    def test_write_with_append(self):
        table = "//tmp/table"
        yt.remove(table, force=True)
        assert not yt.exists(table)
        yt.write_table_structured(yt.TablePath(table, append=True), TheRow, ROWS)
        assert ROW_DICTS == list(yt.read_table(table))

    @authors("levysotsky")
    @pytest.mark.parametrize("create", [True, False])
    def test_write_schema_inference(self, create):
        table = "//tmp/table"
        yt.remove(table, force=True)

        if create:
            yt.create("table", table)
            assert TableSchema.from_yson_type(yt.get(table + "/@schema")) == TableSchema(strict=False)

        yt.write_table_structured(table, TheRow, ROWS)

        expected_schema = TableSchema() \
            .add_column("int32_field", ti.Int64) \
            .add_column("str_field", ti.Utf8) \
            .add_column("struct_field", ti.Optional[ti.Struct[
                "str_field": ti.Utf8,
                "uint8_field": ti.Optional[ti.Uint64],
            ]])

        assert TableSchema.from_row_type(TheRow) == expected_schema
        assert TableSchema.from_yson_type(yt.get(table + "/@schema")) == expected_schema

    @authors("aleexfi")
    def test_optional_schema_inference(self):
        @yt_dataclass
        class OptionalRow:
            a: int | None
            a: None | int
            b: typing.Optional[str]

        expected_schema = (
            TableSchema()
            .add_column("a", ti.Optional[ti.Int64])
            .add_column("b", ti.Optional[ti.Utf8])
        )

        assert TableSchema.from_row_type(OptionalRow) == expected_schema

    @authors("aleexfi")
    def test_invalid_optional(self):
        @yt_dataclass
        class InvalidOptionalRow:
            a: str | int

        with pytest.raises(YtError):
            TableSchema.from_row_type(InvalidOptionalRow)

    @authors("levysotsky")
    def test_read_ranges(self):
        table = "//tmp/table"
        yt.remove(table, force=True)
        yt.create("table", table, attributes={"schema": TableSchema.from_row_type(TheRow)})
        row_count = 100
        generator = ({"int32_field": i, "str_field": str(i)} for i in range(row_count))
        yt.write_table(table, generator)
        ranges = [(1, 4), (4, 5), (10, 20), (7, 10), (90, 100)]
        ranges_dicts = [
            {
                "lower_limit": {"row_index": begin},
                "upper_limit": {"row_index": end},
            }
            for begin, end in ranges
        ]
        path_with_ranges = yt.TablePath(table, ranges=ranges_dicts)
        iterator = yt.read_table_structured(path_with_ranges, TheRow)
        for range_index, (begin, end) in enumerate(ranges):
            for row_index in range(begin, end):
                row = next(iterator)
                assert iterator.get_row_index() == row_index
                assert iterator.get_table_index() == 0
                assert iterator.get_key_switch() is False
                assert iterator.get_range_index() == range_index
                assert row == TheRow(int32_field=row_index, str_field=str(row_index))
        with pytest.raises(StopIteration):
            next(iterator)

        iterator = yt.read_table_structured(path_with_ranges, TheRow).with_context()
        for range_index, (begin, end) in enumerate(ranges):
            for row_index in range(begin, end):
                row, context = next(iterator)
                assert context.get_row_index() == row_index
                assert context.get_table_index() == 0
                assert context.get_key_switch() is False
                assert context.get_range_index() == range_index
                assert row == TheRow(int32_field=row_index, str_field=str(row_index))
        with pytest.raises(StopIteration):
            next(iterator)

    @authors("levysotsky")
    def test_map_with_context(self):
        input_tables = ["//tmp/input1", "//tmp/input2"]
        output_table = "//tmp/output"
        schema = TableSchema.from_row_type(TheRow)
        for input_table in input_tables:
            yt.remove(input_table, force=True)
            yt.create("table", input_table, attributes={"schema": schema})
            yt.write_table(input_table, ROW_DICTS)

        yt.run_map(
            MapperWithContext(),
            input_tables,
            output_table,
            spec={"max_failed_job_count": 1},
        )

        expected_rows = []
        for table_index in range(2):
            for row_index, row in enumerate(ROW_DICTS):
                row_copy = copy.deepcopy(row)
                row_copy.update({
                    "row_index": row_index,
                    "table_index": table_index,
                })
                expected_rows.append(row_copy)
        read_rows = sorted(
            yt.read_table(output_table),
            key=lambda row: (row["table_index"], row["row_index"]),
        )
        assert read_rows == expected_rows

    @authors("levysotsky")
    @pytest.mark.parametrize("aggregator", [True, False])
    def test_map_two_outputs(self, aggregator):
        input_table = "//tmp/input"
        output_table1 = "//tmp/output1"
        output_table2 = "//tmp/output2"
        yt.remove(input_table, force=True)
        schema = TableSchema.from_row_type(TheRow)
        yt.create("table", input_table, attributes={"schema": schema})
        yt.write_table(input_table, ROW_DICTS)

        if aggregator:
            mapper = AggregatorTwoOutputMapper()
        else:
            mapper = TwoOutputMapper(for_map_reduce=False)

        yt.run_map(
            mapper,
            input_table,
            [output_table1, output_table2],
            spec={"max_failed_job_count": 1},
        )
        read_rows1 = list(yt.read_table(output_table1))
        assert read_rows1 == ROW_DICTS
        read_rows2 = list(yt.read_table(output_table2))
        assert read_rows2 == TWO_OUTPUT_MAPPER_ROWS_SECOND_TABLE

    @authors("levysotsky")
    @pytest.mark.parametrize("trivial_mapper", [True, False])
    @pytest.mark.parametrize("with_reduce_combiner", [True, False])
    def test_basic_map_reduce(self, trivial_mapper, with_reduce_combiner):
        input_table = "//tmp/input"
        output_table = "//tmp/output"
        yt.remove(input_table, force=True)
        schema = TableSchema.from_row_type(TheRow)
        yt.create("table", input_table, attributes={"schema": schema})
        yt.write_table(input_table, ROW_DICTS)
        yt.run_map_reduce(
            mapper=IdentityMapper() if not trivial_mapper else None,
            reducer=IdentityReducer(),
            reduce_combiner=IdentityReducer() if with_reduce_combiner else None,
            source_table=input_table,
            destination_table=output_table,
            reduce_by=["int32_field"],
            spec={
                "force_reduce_combiners": with_reduce_combiner,
                "max_failed_job_count": 1,
            },
        )
        read_rows = list(yt.read_table(output_table))
        assert read_rows == ROW_DICTS

    @authors("levysotsky")
    def test_map_reduce_several_intermediate_streams(self):
        input_table = "//tmp/input"
        output_table = "//tmp/output"
        yt.remove(input_table, force=True)
        schema = TableSchema.from_row_type(TheRow)
        yt.create("table", input_table, attributes={"schema": schema})
        yt.write_table(input_table, ROW_DICTS)
        yt.run_map_reduce(
            mapper=TwoOutputMapper(for_map_reduce=True),
            reducer=TwoInputReducer(),
            source_table=input_table,
            destination_table=output_table,
            reduce_by=["int32_field"],
            spec={"max_failed_job_count": 1},
        )
        read_rows = list(yt.read_table(output_table))

        def sort_key(d):
            return (d["int32_field"], "struct_field" not in d)

        assert sorted(read_rows, key=sort_key) == sorted(TWO_INPUT_REDUCER_OUTPUT_ROWS, key=sort_key)

    @authors("levysotsky")
    def test_map_reduce_trivial_mapper_several_intermediate_streams(self):
        input_table1 = "//tmp/input1"
        input_table2 = "//tmp/input2"
        output_table = "//tmp/output"

        yt.remove(input_table1, force=True)
        yt.create_table(input_table1, attributes={"schema": TableSchema.from_row_type(TheRow)})
        yt.write_table(input_table1, ROW_DICTS)

        yt.remove(input_table2, force=True)
        yt.create_table(input_table2, attributes={"schema": TableSchema.from_row_type(Row1)})
        yt.write_table(input_table2, TWO_OUTPUT_MAPPER_ROWS_SECOND_TABLE)

        yt.run_map_reduce(
            mapper=None,
            reducer=TwoInputReducer(),
            source_table=[input_table1, input_table2],
            destination_table=output_table,
            reduce_by=["int32_field"],
            spec={"max_failed_job_count": 1},
        )
        read_rows = list(yt.read_table(output_table))

        def sort_key(d):
            return (d["int32_field"], "struct_field" not in d)

        assert sorted(read_rows, key=sort_key) == sorted(TWO_INPUT_REDUCER_OUTPUT_ROWS, key=sort_key)

    def _do_test_grouping(self, input_rows, row_types, expected_output_rows, reducer):
        table_count = len(input_rows)
        input_tables = ["//tmp/input_{}".format(i) for i in range(table_count)]
        for input_table, row_type, rows in zip(input_tables, row_types, input_rows):
            yt.remove(input_table, force=True)
            yt.create_table(input_table, attributes={"schema": TableSchema.from_row_type(row_type)})
            yt.write_table(input_table, rows)
        output_table = "//tmp/output"

        def sort_key(d):
            return d["key"]

        yt.run_map_reduce(
            mapper=IdentityMapper(row_types),
            reducer=reducer,
            source_table=input_tables,
            destination_table=output_table,
            reduce_by=["str_field"],
            spec={"max_failed_job_count": 1},
        )

        assert sorted(yt.read_table(output_table), key=sort_key) == expected_output_rows

        yt.run_map_reduce(
            mapper=None,
            reducer=reducer,
            source_table=input_tables,
            destination_table=output_table,
            reduce_by=["str_field"],
            spec={"max_failed_job_count": 1},
        )

        assert sorted(yt.read_table(output_table), key=sort_key) == expected_output_rows

        for input_table in input_tables:
            yt.run_sort(input_table, sort_by="str_field")

        yt.run_reduce(
            reducer,
            source_table=input_tables,
            destination_table=output_table,
            reduce_by=["str_field"],
            spec={"max_failed_job_count": 1},
        )

        assert sorted(yt.read_table(output_table), key=sort_key) == expected_output_rows

    @authors("levysotsky")
    def test_grouping_single_input(self):
        rows = [
            [
                {"int32_field": 10, "str_field": "b"},
                {"int32_field": 10, "str_field": "b"},
                {"int32_field": 10, "str_field": "c"},
                {"int32_field": 10, "str_field": "a"},
                {"int32_field": 10, "str_field": "a"},
                {"int32_field": 10, "str_field": "a"},
            ],
        ]

        expected_rows = [
            {"key": "a", "sum": 30},
            {"key": "b", "sum": 20},
            {"key": "c", "sum": 10},
        ]

        self._do_test_grouping(rows, [TheRow], expected_rows, SummingReducer())

    @authors("levysotsky")
    def test_grouping_two_inputs(self):
        rows = [
            [
                {"int32_field": 10, "str_field": "b"},
                {"int32_field": 10, "str_field": "b"},
                {"int32_field": 10, "str_field": "c"},
                {"int32_field": 10, "str_field": "a"},
                {"int32_field": 10, "str_field": "a"},
                {"int32_field": 10, "str_field": "a"},
            ],
            [
                {"int32_field": 100, "str_field": "c"},
                {"int32_field": 100, "str_field": "c"},
                {"int32_field": 100, "str_field": "a"},
                {"int32_field": 100, "str_field": "a"},
            ],
        ]

        expected_rows = [
            {"key": "a", "sum": 230},
            {"key": "b", "sum": 20},
            {"key": "c", "sum": 210},
        ]

        self._do_test_grouping(rows, [TheRow, Row1], expected_rows, TwoInputSummingReducer())

    @authors("levysotsky")
    def test_grouping_empty_input(self):
        rows = [
            {"int32_field": 100, "str_field": "a"},
            {"int32_field": 100, "str_field": "a"},
            {"int32_field": 100, "str_field": "z"},
            {"int32_field": 100, "str_field": "z"},
        ]

        input_table = "//tmp/input"
        schema = TableSchema.from_row_type(TheRow).build_schema_sorted_by(["str_field"])
        yt.create_table(input_table, attributes={"schema": schema})
        yt.write_table(input_table, rows)
        output_table = "//tmp/output"

        # The range is crafted intentionally to result in empty set of rows.
        range_ = {
            "lower_limit": {"key": ["p"]},
            "upper_limit": {"key": ["q"]},
        }
        yt.run_reduce(
            EmptyInputReducer(),
            source_table=yt.TablePath(input_table, ranges=[range_]),
            destination_table=output_table,
            spec={"max_failed_job_count": 1},
            reduce_by=["str_field"],
        )

        expected_rows = [
            ROW_DICTS[0],
        ]
        assert list(yt.read_table(output_table)) == expected_rows

    @authors("levysotsky")
    @pytest.mark.parametrize("other_columns_as_bytes", [True, False])
    def test_other_columns(self, other_columns_as_bytes):
        dict_row = {
            "x": 10,
            "y": {"a": "foo", "b": b"bar"},
            "z": -10,
        }
        table = "//tmp/table"
        schema = (
            TableSchema()
            .add_column("x", ti.Int64)
            .add_column("y", ti.Struct["a": ti.Utf8, "b": ti.String])
            .add_column("z", ti.Int64)
        )
        yt.create("table", table, attributes={"schema": schema})
        yt.write_table(table, [dict_row])

        other_columns_dict = {
            "y": {"a": "foo", "b": b"bar"},
        }
        if other_columns_as_bytes:
            other_columns = OtherColumns(yson.dumps(other_columns_dict))
        else:
            other_columns = OtherColumns(other_columns_dict)
        assert len(other_columns) == 1
        assert other_columns["y"]["a"] == "foo"
        if other_columns_as_bytes:
            assert other_columns["y"]["b"] == "bar"
        else:
            assert other_columns["y"]["b"] == b"bar"
        other_columns["z"] = -10
        assert len(other_columns) == 2
        assert other_columns["z"] == -10

        other_columns_dict["z"] = -10
        if other_columns_as_bytes:
            other_columns = OtherColumns(yson.dumps(other_columns_dict))

        @yt_dataclass
        class TheRow:
            x: int
            other: OtherColumns

        structured_row = TheRow(
            x=10,
            other=other_columns,
        )

        read_structured_rows = list(yt.read_table_structured(table, TheRow))
        assert len(read_structured_rows) == 1
        read_structured_row = read_structured_rows[0]
        assert read_structured_row.x == 10
        # All strings are automatically decoded from UTF-8 in yson parser.
        dict_row_with_str = copy.deepcopy(dict_row)
        dict_row_with_str["y"]["b"] = dict_row_with_str["y"]["b"].decode()
        assert read_structured_row.other["y"] == dict_row_with_str["y"]
        assert read_structured_row.other["z"] == dict_row_with_str["z"]

        yt.write_table_structured(table, TheRow, [structured_row])
        read_dict_rows = list(yt.read_table(table))
        assert read_dict_rows == [dict_row_with_str]

    @authors("levysotsky")
    def test_schema_matching_read(self):
        @yt_dataclass
        class Struct:
            a1: typing.Optional[Int64]
            b1: Uint64
            c1: bytes
            d1: typing.Optional[str]
            e1: str

        @yt_dataclass
        class Row1:
            a: typing.Optional[Int64]
            b: typing.Optional[Struct]

        schema = (
            TableSchema()
            .add_column("a", ti.Int64)
            .add_column(
                "b",
                ti.Struct[
                    "c1": ti.String,
                    "b1": ti.Uint64,
                    "a1": ti.Int64,
                    "e1": ti.String,
                ],
            )
            .add_column("c", ti.Uint64)
        )

        table = "//tmp/table"
        yt.create("table", table, attributes={"schema": schema})
        row = {
            "a": 100,
            "b": {
                "a1": -100,
                "b1": yson.YsonUint64(255),
                "c1": b"\xFF",
                "e1": "Приветосы".encode(),
            },
            "c": yson.YsonUint64(1000),
        }
        yt.write_table(table, [row])
        read_rows = list(yt.read_table_structured(table, Row1))
        assert read_rows == [
            Row1(
                a=100,
                b=Struct(
                    a1=-100,
                    b1=255,
                    c1=b"\xFF",
                    d1=None,
                    e1="Приветосы",
                ),
            ),
        ]

        @yt_dataclass
        class Row2:
            a: Int64
            b: Int64

        schema = TableSchema().add_column("b", ti.Int64)
        yt.remove(table, force=True)
        yt.create_table(table, attributes={"schema": schema})
        with pytest.raises(yt.YtError, match=r'struct schema is missing non-nullable field ".*\.Row2\.a"'):
            yt.read_table_structured(table, Row2)

        schema = TableSchema().add_column("b", ti.Int64).add_column("a", ti.Optional[ti.Int64])
        yt.remove(table, force=True)
        yt.create_table(table, attributes={"schema": schema})
        with pytest.raises(yt.YtError, match=r'field ".*\.Row2\.a" is non-nullable in yt_dataclass and optional'):
            yt.read_table_structured(table, Row2)

    @authors("levysotsky")
    def test_schema_matching_write(self):
        @yt_dataclass
        class Struct:
            a1: typing.Optional[Int64]
            b1: Uint64
            c1: bytes
            d1: str
            e1: str

        @yt_dataclass
        class Row2:
            a: Int64
            b: Struct
            c: int

        schema = (
            TableSchema()
            .add_column("a", ti.Optional[ti.Int64])
            .add_column(
                "b",
                ti.Struct[
                    "d1": ti.Optional[ti.Utf8],
                    "c1": ti.String,
                    "b1": ti.Uint64,
                    "a1": ti.Optional[ti.Int64],
                    "e1": ti.String,
                ],
            )
            .add_column("c", ti.Optional[ti.Int64])
        )

        table = "//tmp/table"
        yt.create("table", table, attributes={"schema": schema})
        row = Row2(
            a=100,
            b=Struct(
                a1=-100,
                b1=255,
                c1=b"\xFF",
                d1="Енот",
                e1="Луна",
            ),
            c=1000,
        )
        yt.write_table_structured(table, Row2, [row])
        read_rows = list(yt.read_table(table))
        assert read_rows == [
            {
                "a": 100,
                "b": {
                    "a1": -100,
                    "b1": yson.YsonUint64(255),
                    "c1": b"\xFF",
                    "d1": "Енот",
                    "e1": "Луна",
                },
                "c": 1000,
            },
        ]

        schema_missing_b = (
            TableSchema()
            .add_column("a", ti.Optional[ti.Int64])
            .add_column("c", ti.Optional[ti.Int64])
        )

        yt.remove(table)
        yt.create("table", table, attributes={"schema": schema_missing_b})
        with pytest.raises(yt.YtError, match=r'struct schema is missing field ".*\.Row2\.b"'):
            yt.write_table_structured(table, Row2, [row])

        schema_with_required_extra = copy.deepcopy(schema)
        schema_with_required_extra.add_column("extra", ti.Int64)

        yt.remove(table)
        yt.create("table", table, attributes={"schema": schema_with_required_extra})
        with pytest.raises(yt.YtError, match=r'yt_dataclass is missing non-nullable field ".*\.Row2\.extra"'):
            yt.write_table_structured(table, Row2, [row])

        @yt_dataclass
        class Row3:
            a: typing.Optional[Int64]

        schema_with_required_a = TableSchema().add_column("a", ti.Int64)
        yt.remove(table)
        yt.create("table", table, attributes={"schema": schema_with_required_a})
        with pytest.raises(yt.YtError, match=r'field ".*\.Row3\.a" is optional in yt_dataclass and required'):
            yt.write_table_structured(table, Row3, [Row3(a=1)])

    @authors("aleexfi")
    def test_schema_matching_datetime(self):
        def check_datetime(py_type, ti_type, datetime_object, raw_object, expected_object=None):
            if not expected_object:
                expected_object = datetime_object
            assert expected_object == write_and_read_primitive(py_type, ti_type, raw_object, mode="write_unstructured_read_structured")
            assert raw_object == write_and_read_primitive(py_type, ti_type, datetime_object, mode="write_structured_read_unstructured")

        def microseconds_timestamp(timedelta):
            return int(timedelta.total_seconds()) * 10 ** 6 + int(timedelta.microseconds)

        MSK_TIMEZONE = datetime.timezone(datetime.timedelta(hours=+3))
        yt_timestamp_as_datetime = datetime.datetime(year=2010, month=10, day=29, hour=17, minute=54, microsecond=42, tzinfo=MSK_TIMEZONE)
        yt_timestamp_as_int = microseconds_timestamp(yt_timestamp_as_datetime - datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc))
        check_datetime(datetime.datetime, ti.Timestamp, yt_timestamp_as_datetime, yt_timestamp_as_int)

        yt_datetime_as_datetime = yt_timestamp_as_datetime.replace(microsecond=0)
        yt_datetime_as_int = yt_timestamp_as_int // 10 ** 6
        check_datetime(datetime.datetime, ti.Datetime, yt_datetime_as_datetime, yt_datetime_as_int)
        # Microseconds lost since ti.Datetime's resolution is seconds
        check_datetime(datetime.datetime, ti.Datetime, yt_timestamp_as_datetime, yt_datetime_as_int, expected_object=yt_datetime_as_datetime)

        yt_date_as_date = yt_timestamp_as_datetime.date()
        yt_date_as_int = int((yt_date_as_date - datetime.date.fromtimestamp(0)).days)
        check_datetime(datetime.date, ti.Date, yt_date_as_date, yt_date_as_int)

        yt_interval_as_timedelta = datetime.timedelta(days=1, hours=2, minutes=3, seconds=4, milliseconds=5, microseconds=6, weeks=7)
        yt_interval_as_int = (((((7 * 7 + 1) * 24 + 2) * 60 + 3) * 60 + 4) * 1000 + 5) * 1000 + 6
        assert yt_interval_as_int == microseconds_timestamp(yt_interval_as_timedelta)
        check_datetime(datetime.timedelta, ti.Interval, yt_interval_as_timedelta, yt_interval_as_int)

    @authors("aleexfi")
    def test_datetime_representations(self):
        @yt_dataclass
        class Row:
            py_datetime: datetime.datetime
            formatted_datetime: FormattedPyDatetime["%Y-%m-%dT%H:%M%z"]  # noqa
            no_time_zone: FormattedPyDatetime["%Y-%m-%dT%H:%M"]  # noqa

        schema = (
            TableSchema()
            .add_column("py_datetime", ti.Datetime)
            .add_column("formatted_datetime", ti.String)
            .add_column("no_time_zone", ti.String)
        )

        table = "//tmp/table"
        yt.create("table", table, attributes={"schema": schema})

        datetime_ = datetime.datetime(2011, 12, 13, 14, 15, tzinfo=datetime.timezone.utc)
        row = Row(
            py_datetime=datetime_,
            formatted_datetime=datetime_,
            no_time_zone=datetime_.replace(tzinfo=None)
        )
        raw_row = {
            "py_datetime": int(datetime_.timestamp()),
            "formatted_datetime": "2011-12-13T14:15+0000",
            "no_time_zone": "2011-12-13T14:15",
        }

        yt.write_table(table, [raw_row])
        assert list(yt.read_table_structured(table, Row)) == [Row(datetime_, datetime_, datetime_.replace(tzinfo=None))]

        yt.write_table_structured(table, Row, [row])
        assert list(yt.read_table(table)) == [raw_row]

    @authors("levysotsky")
    def test_schema_matching_primitive(self):
        for py_type, ti_type in [
            (Int8, ti.Int8),
            (Int16, ti.Int16),
            (Int32, ti.Int32),
            (Int64, ti.Int64),
            (Interval, ti.Interval),
        ]:
            assert -10 == write_and_read_primitive(py_type, ti_type, -10, mode="write_unstructured_read_structured")
            assert -10 == write_and_read_primitive(py_type, ti_type, -10, mode="write_structured_read_unstructured")

        for py_type, ti_type in [
            (Int8, ti.Int8),
            (Int16, ti.Int16),
            (Int32, ti.Int32),
            (Int64, ti.Int64),
            (Interval, ti.Interval),

            (Uint8, ti.Uint8),
            (Uint16, ti.Uint16),
            (Uint32, ti.Uint32),
            (Uint64, ti.Uint64),
            (Date, ti.Date),
            (Datetime, ti.Datetime),
            (Timestamp, ti.Timestamp),
        ]:
            assert 10 == write_and_read_primitive(py_type, ti_type, 10, mode="write_unstructured_read_structured")
            assert 10 == write_and_read_primitive(py_type, ti_type, 10, mode="write_structured_read_unstructured")

        for py_type, ti_type in [
            (Float, ti.Float),
            (Double, ti.Double),
        ]:
            assert 1.25 == write_and_read_primitive(py_type, ti_type, 1.25, mode="write_unstructured_read_structured")
            assert 1.25 == write_and_read_primitive(py_type, ti_type, 1.25, mode="write_structured_read_unstructured")

        with pytest.raises(yt.YtError, match="signedness"):
            write_and_read_primitive(Int64, ti.Uint64, 10, mode="write_unstructured_read_structured")
        with pytest.raises(yt.YtError, match="signedness"):
            write_and_read_primitive(Uint64, ti.Int64, 10, mode="write_unstructured_read_structured")
        with pytest.raises(yt.YtError, match="signedness"):
            write_and_read_primitive(Int64, ti.Uint64, 10, mode="write_structured_read_unstructured")
        with pytest.raises(yt.YtError, match="signedness"):
            write_and_read_primitive(Uint64, ti.Int64, 10, mode="write_structured_read_unstructured")

        assert -10 == write_and_read_primitive(Int64, ti.Int32, -10, mode="write_unstructured_read_structured")
        with pytest.raises(yt.YtError, match="larger than destination"):
            write_and_read_primitive(Int64, ti.Int32, -10, mode="write_structured_read_unstructured")

        assert -10 == write_and_read_primitive(Int32, ti.Int16, -10, mode="write_unstructured_read_structured")
        with pytest.raises(yt.YtError, match="larger than destination"):
            write_and_read_primitive(Int32, ti.Int16, -10, mode="write_structured_read_unstructured")

        assert 10 == write_and_read_primitive(Uint64, ti.Uint32, 10, mode="write_unstructured_read_structured")
        with pytest.raises(yt.YtError, match="larger than destination"):
            write_and_read_primitive(Uint64, ti.Uint32, 10, mode="write_structured_read_unstructured")

        assert 10 == write_and_read_primitive(Uint32, ti.Uint16, 10, mode="write_unstructured_read_structured")
        with pytest.raises(yt.YtError, match="larger than destination"):
            write_and_read_primitive(Uint32, ti.Uint16, 10, mode="write_structured_read_unstructured")

        for py_type, ti_type in [
            (Date, ti.Uint64),
            (Datetime, ti.Uint64),
            (Timestamp, ti.Uint64),
            (Interval, ti.Int64),
        ]:
            for mode in ["write_structured_read_unstructured", "write_unstructured_read_structured"]:
                with pytest.raises(yt.YtError, match="source type .* is incompatible with destination type"):
                    write_and_read_primitive(py_type, ti_type, 10, mode=mode)

        # Check all time types are incompatible with each other
        checks = 0
        for py_type in [Date, Datetime, Timestamp, Interval]:
            for ti_type in [ti.Date, ti.Datetime, ti.Timestamp, ti.Interval]:
                if _get_annotation(py_type)._ti_type == ti_type:
                    continue
                with pytest.raises(yt.YtError, match="source type .* is incompatible with destination type"):
                    write_and_read_primitive(Interval, ti.Int64, 10, mode="write_structured_read_unstructured")
                with pytest.raises(yt.YtError, match="source type .* is incompatible with destination type"):
                    write_and_read_primitive(Interval, ti.Int64, 10, mode="write_unstructured_read_structured")
                checks += 1
        # Double check that tests in the loop above have run
        assert checks == 12

        string = "Привет"
        string_utf8 = string.encode("utf-8")

        assert string == write_and_read_primitive(str, ti.Utf8, string, mode="write_unstructured_read_structured")
        assert string == write_and_read_primitive(str, ti.Utf8, string, mode="write_structured_read_unstructured")

        assert string == write_and_read_primitive(str, ti.String, string_utf8, mode="write_unstructured_read_structured")
        assert string == write_and_read_primitive(str, ti.String, string, mode="write_structured_read_unstructured")

        assert string_utf8 == write_and_read_primitive(bytes, ti.String, string_utf8, mode="write_unstructured_read_structured")
        assert string == write_and_read_primitive(bytes, ti.String, string_utf8, mode="write_structured_read_unstructured")

        assert string_utf8 == write_and_read_primitive(bytes, ti.Utf8, string_utf8, mode="write_unstructured_read_structured")
        assert string == write_and_read_primitive(bytes, ti.Utf8, string_utf8, mode="write_structured_read_unstructured")

    @authors("levysotsky")
    def test_prepare_operation(self):
        first_input_schema = (
            TableSchema()
            .add_column("int32_field_original", ti.Int32)
            .add_column("str_field", ti.Utf8)
            .add_column("struct_field", ti.Struct["str_field" : ti.Utf8, "uint8_field" : ti.Optional[ti.Uint64]])
            .add_column("extra_field", ti.Int64)
        )
        other_input_schema = TableSchema.from_row_type(TheRow)

        first_output_schema = TableSchema.from_row_type(TheRow)
        first_output_schema.add_column("extra_field", ti.Optional[ti.Int64])
        other_output_schema = TableSchema.from_row_type(TheRow)

        input_paths = []
        for i in range(3):
            table = "//tmp/in_{}".format(i)
            input_paths.append(table)
            if i == 0:
                schema = first_input_schema
            else:
                schema = other_input_schema
            yt.create("table", table, attributes={"schema": schema})
            row = {
                "str_field": "Хы",
                "struct_field": {
                    "str_field": "Зы",
                },
            }
            if i == 0:
                row["int32_field_original"] = 42
                row["extra_field"] = -42
            else:
                row["int32_field"] = 42
            yt.write_table(table, [row])

        output_paths = ["//tmp/out_{}".format(i) for i in range(6)]

        mapper = MapperWithBigPrepareOperation(first_output_schema)
        yt.run_map(mapper, input_paths, output_paths)

        actual_output_schemas = [TableSchema.from_yson_type(yt.get(output_paths[i] + "/@schema")) for i in range(6)]
        assert actual_output_schemas[0] == first_output_schema
        for i in range(1, 6):
            assert actual_output_schemas[i] == other_output_schema

        for i in range(3):
            rows = list(yt.read_table(output_paths[i]))
            expected_row = {
                "int32_field": 42,
                "str_field": "Хы",
                "struct_field": {
                    "str_field": "Зы",
                    "uint8_field": None,
                },
            }
            if i == 0:
                expected_row["extra_field"] = None
            assert rows == [expected_row]
        for i in range(3, 6):
            assert list(yt.read_table(output_paths[i])) == []

    @authors("levysotsky")
    def test_prepare_operation_errors(self):
        input_tables, output_tables = [], []
        for i in range(3):
            input_table = "//tmp/in_{}".format(i)
            input_tables.append(input_table)
            output_table = "//tmp/out_{}".format(i)
            output_tables.append(output_table)
            yt.remove(input_table, force=True)
            schema = TableSchema.from_row_type(TheRow)
            yt.create("table", input_table, attributes={"schema": schema})
            yt.write_table(input_table, ROW_DICTS)

        class TypedJobNotToBeCalled(TypedJob):
            pass

        class MapperWithMissingInput(TypedJobNotToBeCalled):
            def prepare_operation(self, context, preparer):
                preparer.inputs([0, 2], type=TheRow).output(0, type=TheRow)

        with pytest.raises(ValueError, match="Missing type for input table no. 1 \\(//tmp/in_1\\)"):
            yt.run_map(MapperWithMissingInput(), input_tables, output_tables)

        class MapperWithMissingOutput(TypedJobNotToBeCalled):
            def prepare_operation(self, context, preparer):
                preparer.inputs(range(3), type=TheRow).outputs([0, 1], type=TheRow)

        with pytest.raises(ValueError, match="Missing type for output table no. 2 \\(//tmp/out_2\\)"):
            yt.run_map(MapperWithMissingOutput(), input_tables, output_tables)

        class MapperWithOutOfRange(TypedJobNotToBeCalled):
            def prepare_operation(self, context, preparer):
                preparer.inputs(range(100), type=TheRow).output(range(100), type=TheRow)

        with pytest.raises(ValueError, match="Input type index 3 out of range \\[0, 3\\)"):
            yt.run_map(MapperWithOutOfRange(), input_tables, output_tables)

        class MapperWithTypes(TypedJobNotToBeCalled):
            def __init__(self, input_type, output_type):
                self._input_type = input_type
                self._output_type = output_type

            def prepare_operation(self, context, preparer):
                preparer.inputs(range(3), type=self._input_type).outputs(range(3), type=self._output_type)

        with pytest.raises(TypeError, match="Input type must be a class marked with"):
            yt.run_map(MapperWithTypes(int, TheRow), input_tables, output_tables)
        with pytest.raises(TypeError, match="Output type must be a class marked with"):
            yt.run_map(MapperWithTypes(TheRow, int), input_tables, output_tables)

        @yt_dataclass
        class Row2:
            list_field: typing.List[typing.List[int]]

        with pytest.raises(yt.YtError, match=r'struct schema is missing non-nullable field ".*\.Row2\.list_field"'):
            yt.run_map(MapperWithTypes(Row2, TheRow), input_tables, output_tables)

        with pytest.raises(yt.YtError, match=r'incompatible with "input_query"'):
            yt.run_map(MapperWithTypes(Row2, TheRow), input_tables, output_tables, spec={"input_query": "*"})

    @authors("ignat")
    def test_yson_bytes(self):
        @yt_dataclass
        class RowWithYson:
            pure_yson: typing.Optional[YsonBytes]
            list_of_ysons: typing.List[typing.Optional[YsonBytes]]

        schema = TableSchema.from_row_type(RowWithYson)

        table = "//tmp/table"
        yt.create("table", table, attributes={"schema": schema})
        yt.write_table_structured(
            table,
            RowWithYson,
            [
                RowWithYson(pure_yson=b"1",
                            list_of_ysons=[
                                yson.dumps({"my_data": 10}),
                                yson.dumps(["item1", yson.loads(b"<attr=10>item2")]),
                            ]),
            ])

        typed_rows = list(yt.read_table_structured(table, RowWithYson))
        assert len(typed_rows) == 1
        row = typed_rows[0]
        assert yson.loads(row.pure_yson) == 1
        assert len(row.list_of_ysons) == 2
        assert yson.loads(row.list_of_ysons[0]) == {"my_data": 10}
        assert yson.loads(row.list_of_ysons[1]) == ["item1", yson.loads(b"<attr=10>item2")]

        rows = list(yt.read_table(table))
        assert len(rows) == 1
        row = rows[0]
        assert row["pure_yson"] == 1
        assert len(row["list_of_ysons"]) == 2
        assert row["list_of_ysons"][0] == {"my_data": 10}
        assert row["list_of_ysons"][1] == ["item1", yson.loads(b"<attr=10>item2")]

    @authors("ignat")
    def test_dict(self):
        @yt_dataclass
        class RowWithDicts:
            dict_str_to_int: typing.Optional[typing.Dict[str, int]]
            dict_int_to_bytes: typing.Optional[typing.Dict[int, bytes]]

        schema = TableSchema.from_row_type(RowWithDicts)

        table = "//tmp/table"
        yt.create("table", table, attributes={"schema": schema})
        yt.write_table_structured(
            table,
            RowWithDicts,
            [
                RowWithDicts(
                    dict_str_to_int={"a": 10, "b": 20},
                    dict_int_to_bytes={1: b"\x01", 2: b"\x02"},
                )
            ])

        typed_rows = list(yt.read_table_structured(table, RowWithDicts))
        assert len(typed_rows) == 1
        row = typed_rows[0]
        assert row.dict_str_to_int == {"a": 10, "b": 20}
        assert row.dict_int_to_bytes == {1: b"\x01", 2: b"\x02"}

    @authors("denvr")
    def test_tuple(self):
        @yt_dataclass
        class RowWithTuple:
            tuple_str_int: typing.Optional[typing.Tuple[str, int]]
            tuple_int_bytes: typing.Optional[typing.Tuple[int, bytes]]

        schema = TableSchema.from_row_type(RowWithTuple)

        table = "//tmp/table"
        yt.create("table", table, attributes={"schema": schema})
        yt.write_table_structured(
            table,
            RowWithTuple,
            [
                RowWithTuple(
                    tuple_str_int=tuple(["a", 10]),
                    tuple_int_bytes=tuple([1, b"\x01"]),
                )
            ])

        typed_rows = list(yt.read_table_structured(table, RowWithTuple))
        assert len(typed_rows) == 1
        row = typed_rows[0]
        assert row.tuple_str_int == ("a", 10,)
        assert row.tuple_int_bytes == (1, b"\x01",)

    class SimpleIdentityMapper(TypedJob):
        def __call__(self, input_row: TheRow) -> typing.Iterable[TheRow]:
            yield input_row

    @yt.with_context
    class ContextedIdentityMapper(TypedJob):
        def __call__(self, input_row: TheRow, context: Context) -> typing.Iterator[TheRow]:
            yield input_row

    class ReturnOutputRowIdentityMapper(TypedJob):
        def __call__(self, input_row: TheRow) -> typing.Generator[OutputRow[Variant[TheRow]], None, None]:
            yield OutputRow(input_row)

    @authors("aleexfi")
    @pytest.mark.parametrize("mapper", [SimpleIdentityMapper, ContextedIdentityMapper, ReturnOutputRowIdentityMapper])
    def test_simple_schema_inferring_from_type_hints(self, mapper):
        schema = TableSchema.from_row_type(TheRow)

        src_table = "//tmp/src_table"
        dest_table = "//tmp/dest_table"
        yt.create_table(src_table, attributes={"schema": schema})
        yt.create_table(dest_table, attributes={"schema": schema})
        yt.write_table_structured(src_table, TheRow, ROWS)

        yt.run_map(
            mapper(),
            source_table=src_table,
            destination_table=dest_table,
            spec={"max_failed_job_count": 1},
        )

        assert list(yt.read_table_structured(dest_table, TheRow)) == ROWS

    class NotAnnotatedIdentityMapper(TypedJob):
        def __call__(self, input_row):
            yield input_row

    class NoReturnTypeIdentityMapper(TypedJob):
        def __call__(self, input_row: TheRow):
            yield input_row

    class NoIterableIdentityMapper(TypedJob):
        def __call__(self, input_row: TheRow) -> OutputRow[TheRow]:
            yield OutputRow(input_row)

    class TooMuchInputTypesIdentityMapper(TypedJob):
        def __call__(self, input_row: Variant[TheRow, TheRow]) -> typing.Iterable[OutputRow[TheRow]]:
            yield OutputRow(input_row)

    class TooMuchOutputTypesIdentityMapper(TypedJob):
        def __call__(self, input_row: TheRow) -> typing.Iterable[OutputRow[Variant[TheRow, TheRow]]]:
            yield OutputRow(input_row)

    @authors("aleexfi")
    @pytest.mark.parametrize("mapper", [
        NotAnnotatedIdentityMapper, NoReturnTypeIdentityMapper,
        NoIterableIdentityMapper, TooMuchInputTypesIdentityMapper,
        TooMuchOutputTypesIdentityMapper,
    ])
    def test_bad_schema_inferring_from_type_hints(self, mapper):
        schema = TableSchema.from_row_type(TheRow)

        src_table = "//tmp/src_table"
        dest_table = "//tmp/dest_table"
        yt.create_table(src_table, attributes={"schema": schema})
        yt.create_table(dest_table, attributes={"schema": schema})

        with pytest.raises(YtError):
            yt.run_map(
                mapper(),
                source_table=src_table,
                destination_table=dest_table,
            )

    @dataclass
    class TestConfig:
        __test__ = False
        reducer: typing.Type[TypedJob]
        inputs: list[yt.schema.types.YtDataclassType]
        outputs: list[yt.schema.types.YtDataclassType]

    class SameInputTypesIdentityReducer(TypedJob):
        def __call__(self, input_row_iterator: RowIterator[Variant[TheRow, TheRow]]) \
                -> typing.Iterable[OutputRow[Variant[AnotherRow, AnotherRow, AnotherRow]]]:
            yield OutputRow(AnotherRow())  # never called

    class MultipleTypesIdentityReducer(TypedJob):
        def __call__(self, input_row_iterator: RowIterator[Variant[TheRow, AnotherRow]]) \
                -> typing.Iterable[OutputRow[Variant[AnotherRow, TheRow]]]:
            yield OutputRow(TheRow())

    @yt.reduce_aggregator
    class ReduceAggregator(TypedJob):
        def __call__(self, row_groups: typing.Iterable[RowIterator[TheRow]]) \
                -> typing.Iterable[AnotherRow]:
            sum = 0
            for rows in row_groups:
                for row in rows:
                    sum += row.int32_field
            yield AnotherRow(int32_field=0, uint64_field=sum)

    @authors("aleexfi")
    @pytest.mark.parametrize("config", [
        TestConfig(SimpleIdentityMapper, inputs=[TheRow, TheRow], outputs=[TheRow, TheRow, TheRow]),
        TestConfig(SameInputTypesIdentityReducer, inputs=[TheRow, TheRow], outputs=[AnotherRow, AnotherRow, AnotherRow]),
        TestConfig(MultipleTypesIdentityReducer, inputs=[TheRow, AnotherRow], outputs=[AnotherRow, TheRow]),
        TestConfig(ReduceAggregator, inputs=[TheRow], outputs=[AnotherRow]),
    ])
    def test_multiple_tables_schema_inferring_from_type_hints(self, config):
        def create_tables(table_path_prefix, tables_row_type_list):
            paths = []
            for i, row_type in enumerate(tables_row_type_list):
                table_path = "{}_{}".format(table_path_prefix, i)
                schema = TableSchema.from_row_type(row_type)
                yt.create_table(table_path, attributes={"schema": schema})
                paths.append(table_path)
            return paths

        src_tables = create_tables("//tmp/src_table", config.inputs)
        dest_tables = create_tables("//tmp/dest_table", config.outputs)

        for table in src_tables:
            yt.run_sort(table, sort_by=["int32_field"])

        yt.run_reduce(
            config.reducer(),
            source_table=src_tables,
            destination_table=dest_tables,
            reduce_by=["int32_field"],
        )

    @authors("aleexfi")
    @pytest.mark.parametrize("config", [
        TestConfig(SimpleIdentityMapper, inputs=[TheRow, AnotherRow], outputs=[TheRow]),
        TestConfig(SameInputTypesIdentityReducer, inputs=[TheRow, AnotherRow], outputs=[AnotherRow, AnotherRow, AnotherRow]),
        TestConfig(SameInputTypesIdentityReducer, inputs=[TheRow, TheRow], outputs=[TheRow, AnotherRow, AnotherRow]),
        TestConfig(SameInputTypesIdentityReducer, inputs=[TheRow], outputs=[AnotherRow, AnotherRow, AnotherRow]),
        TestConfig(SameInputTypesIdentityReducer, inputs=[TheRow, TheRow], outputs=[AnotherRow, AnotherRow]),
        TestConfig(MultipleTypesIdentityReducer, inputs=[AnotherRow, TheRow], outputs=[AnotherRow, TheRow]),
    ])
    def test_bad_multiple_tables_schema_inferring_from_type_hints(self, config):
        with pytest.raises(YtError):
            self.test_multiple_tables_schema_inferring_from_type_hints(config)

    @authors("aleexfi")
    def test_schema_inference_with_postponed_type_hints_evaluation(self):
        @yt_dataclass
        class Struct:
            str_field: "str"
            uint8_field: "typing.Optional[Uint64]"

        expected_schema = TableSchema() \
            .add_column("str_field", ti.Utf8) \
            .add_column("uint8_field", ti.Optional[ti.Uint64])

        assert TableSchema.from_row_type(Struct) == expected_schema

        @yt_dataclass
        class TheRow:
            int32_field: "Int64"
            str_field: "str"
            struct_field: "typing.Optional[Struct]" = None

        expected_schema = TableSchema() \
            .add_column("int32_field", ti.Int64) \
            .add_column("str_field", ti.Utf8) \
            .add_column("struct_field", ti.Optional[ti.Struct[
                "str_field": ti.Utf8,
                "uint8_field": ti.Optional[ti.Uint64],
            ]])

        assert TableSchema.from_row_type(TheRow) == expected_schema

    @authors("aleexfi")
    def test_read_skiff_with_retries(self):
        old_value = yt.config["read_retries"]["enable"]
        yt.config["read_retries"]["enable"] = True
        yt.config._ENABLE_READ_TABLE_CHAOS_MONKEY = True
        yt.config._ENABLE_HTTP_CHAOS_MONKEY = True

        try:
            table = "//tmp/table"

            with pytest.raises(yt.YtError):
                yt.read_table(table)

            yt.create("table", table)
            with pytest.raises(StopIteration):
                next(yt.read_table_structured(table, TheRow))

            yt.write_table_structured(table, TheRow, ROWS)
            all_rows = list(yt.read_table_structured(table, TheRow))
            assert len(all_rows) == 2 and all_rows == ROWS

            rsp = yt.read_table_structured(table, TheRow)
            assert next(rsp) == ROWS[0]
            new_row = copy.deepcopy(ROWS[1])
            new_row.struct_field.uint8_field = 42
            yt.write_table_structured(table, TheRow, [ROWS[0], new_row])
            assert next(rsp) == ROWS[1]

            with pytest.raises(StopIteration):
                next(rsp)

            all_rows = list(yt.read_table_structured(table, TheRow))
            assert len(all_rows) == 2 and all_rows == [ROWS[0], new_row]

            response_parameters = {}
            rsp = yt.read_table(table, response_parameters=response_parameters, format=yt.JsonFormat(), raw=True)
            assert response_parameters["start_row_index"] == 0
            assert response_parameters["approximate_row_count"] == 2
            rsp.close()
        finally:
            yt.config._ENABLE_READ_TABLE_CHAOS_MONKEY = False
            yt.config._ENABLE_HTTP_CHAOS_MONKEY = False
            yt.config["read_retries"]["enable"] = old_value

    @authors("aleexfi")
    def test_read_skiff_ranges_with_retries(self):
        @yt_dataclass
        class SimpleRow:
            x: typing.Optional[Int64]

        yt.config._ENABLE_READ_TABLE_CHAOS_MONKEY = True
        try:
            table = "//tmp/table"

            yt.create("table", table)
            with pytest.raises(StopIteration):
                next(yt.read_table_structured(table, SimpleRow))

            x1, x2, x3 = SimpleRow(1), SimpleRow(2), SimpleRow(3)
            yt.write_table_structured("<sorted_by=[x]>" + table, SimpleRow, [x1, x2, x3])

            assert [x1, x2, x3] == list(yt.read_table_structured(table, SimpleRow))
            assert [x1] == list(yt.read_table_structured(table + "[#0]", SimpleRow))

            table_path = yt.TablePath(table, exact_key=[2])
            for i in range(10):
                assert [x2] == list(yt.read_table_structured(table_path, SimpleRow))

            assert [x1, x3, x2, x1, x2] == list(yt.read_table_structured(table + '[#0,"2":"1",#2,#1,1:3]', SimpleRow))

            expected_rows = [x1, x3]
            expectated_attributes = [
                {"range_index": 0, "row_index": 0},
                {"range_index": 1, "row_index": 2},
            ]
            for i, (row, ctx) in enumerate(yt.read_table_structured(table + "[#0,#2]", SimpleRow).with_context()):
                assert expectated_attributes[i] == {"range_index": ctx.get_range_index(), "row_index": ctx.get_row_index()}
                assert expected_rows[i] == row

            if not yt.config["read_parallel"]["enable"]:
                two_exact_ranges = yt.TablePath(table, attributes={"ranges": [{"exact": {"key": [1]}}, {"exact": {"key": [3]}}]})
                assert [x1, x3] == list(yt.read_table_structured(two_exact_ranges, SimpleRow))

                yt.write_table_structured("<sorted_by=[x]>" + table, SimpleRow, [x1, x1, x2, x3, x3, x3])
                two_exact_ranges = yt.TablePath(table, attributes={"ranges": [{"exact": {"key": [1]}}, {"exact": {"key": [3]}}]})
                assert [x1, x1, x3, x3, x3] == list(yt.read_table_structured(two_exact_ranges, SimpleRow))

        finally:
            yt.config._ENABLE_READ_TABLE_CHAOS_MONKEY = False

    @authors("aleexfi")
    def test_read_skiff_parallel_with_retries(self):
        with set_config_option("read_parallel/enable", True):
            self.test_read_skiff_with_retries()

    @authors("aleexfi")
    def test_read_skiff_ranges_parallel_with_retries(self):
        with set_config_option("read_parallel/enable", True):
            self.test_read_skiff_ranges_with_retries()

    @authors("aleexfi")
    def test_dynamic_table_map(self):
        table = TEST_DIR + "/dyntable"
        result_table = table + "-out"
        schema = TableSchema.from_row_type(TheRow)

        sync_create_cell()

        yt.create("table", table, attributes={"dynamic": True, "schema": schema})

        yt.mount_table(table, sync=True)
        yt.insert_rows(table, ROW_DICTS, raw=False)
        yt.unmount_table(table, sync=True)

        yt.run_map(TestTypedApi.SimpleIdentityMapper(), table, result_table)

        check_rows_equality(
            yt.read_table(result_table),
            ROW_DICTS,
            ordered=False
        )

    @authors("ermolovd")
    def test_optional_column_with_custom_converter(self):
        @yt_dataclass
        class Row:
            formatted_datetime: typing.Optional[FormattedPyDatetime["%Y-%m-%dT%H:%M%z"]]  # noqa

        table = "//tmp/table"

        yt.create("table", table)

        rows = [
            Row(formatted_datetime=datetime.datetime(2021, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)),
            Row(formatted_datetime=None),
        ]
        yt.write_table_structured(table, Row, rows)

        read_rows = list(yt.read_table_structured(table, Row))
        assert read_rows == rows

    @authors("ermolovd")
    def test_yt_dataclass_post_init(self):
        @yt_dataclass
        class NestedStruct:
            field: str

            def __post_init__(self):
                self.post_init_field = self.field + "_nested_post_init"

        @yt_dataclass
        class Row:
            field: str
            nested: NestedStruct

            def __post_init__(self):
                self.post_init_field = self.field + "_row_post_init"
                self.nested_post_init_field = self.nested.post_init_field + "_row_post_init"

        @yt_dataclass
        class NestedStructWithBadPostInit:
            field: str

            def __post_init__(self):
                raise RuntimeError("bad post init: {field}".format(field=self.field))

        @yt_dataclass
        class RowWithBadPostInit:
            field: str
            nested: NestedStruct

            def __post_init__(self):
                raise RuntimeError("bad post init: {field}".format(field=self.field))

        @yt_dataclass
        class RowWithBadNestedPostInit:
            field: str
            nested: NestedStructWithBadPostInit

        table = "//tmp/table"
        yt.create("table", table)

        rows = [
            Row(field="field1", nested=NestedStruct(field="field2")),
            Row(field="field3", nested=NestedStruct(field="field4")),
        ]
        yt.write_table_structured(table, Row, rows)

        read_rows = list(yt.read_table_structured(table, Row))
        # Just sanity check.
        assert read_rows == rows

        assert read_rows[0].post_init_field == "field1_row_post_init"
        assert read_rows[0].nested_post_init_field == "field2_nested_post_init_row_post_init"
        assert read_rows[0].nested.post_init_field == "field2_nested_post_init"
        assert read_rows[1].post_init_field == "field3_row_post_init"
        assert read_rows[1].nested_post_init_field == "field4_nested_post_init_row_post_init"
        assert read_rows[1].nested.post_init_field == "field4_nested_post_init"

        with pytest.raises(YtError, match="bad post init: field1"):
            list(yt.read_table_structured(table, RowWithBadPostInit))

        with pytest.raises(YtError, match="bad post init: field2"):
            list(yt.read_table_structured(table, RowWithBadNestedPostInit))

    @authors("ermolovd")
    def test_manual_mapper_output_streams_in_map_reduce(self):
        table1 = "//tmp/table1"
        table2 = "//tmp/table2"
        result_table = "//tmp/result_table"

        yt.write_table_structured(table1, TheRow, [
            TheRow(int32_field=10, str_field="key1"),
            TheRow(int32_field=20, str_field="key1"),
            TheRow(int32_field=30, str_field="key2"),
            TheRow(int32_field=40, str_field="key2"),
        ])
        yt.write_table_structured(table2, Row1, [
            Row1(int32_field=100, str_field="key2"),
            Row1(int32_field=200, str_field="key2"),
            Row1(int32_field=300, str_field="key3"),
            Row1(int32_field=400, str_field="key3"),
        ])

        yt.run_map_reduce(
            "cat",
            TwoInputSummingReducer(),
            [table1, table2],
            result_table,
            map_input_format="yson",
            map_output_format="yson",
            reduce_by=["str_field"],
            spec={
                "mapper": {
                    "output_streams": [
                        {
                            "schema": TableSchema.from_row_type(row_type).build_schema_sorted_by(["str_field"]),
                        }
                        for row_type in [TheRow, Row1]
                    ],
                },
            },
        )

        result = sorted(yt.read_table_structured(result_table, SumRow), key=lambda row: row.key)
        assert result == [
            SumRow(key="key1", sum=30),
            SumRow(key="key2", sum=370),
            SumRow(key="key3", sum=700),
        ]
