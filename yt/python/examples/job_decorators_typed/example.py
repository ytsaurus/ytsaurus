# -*- coding: utf-8 -*-

import getpass
import os
import sys
import json
import typing

import yt.wrapper
from yt.wrapper.schema import RowIterator, Context


@yt.wrapper.yt_dataclass
class Row:
    x: int


# Inheritance of dataclasses can be used.
@yt.wrapper.yt_dataclass
class IndexRow(Row):
    table_index: int
    row_index: int


@yt.wrapper.yt_dataclass
class SumRow:
    sum: int


# `@with_context` allows you to request a context for a function.
# This variable will contain control attributes
# (for example, the index of the current row and input table).
@yt.wrapper.with_context
class MapperWithContext(yt.wrapper.TypedJob):
    def __call__(self, row: Row, context: Context) -> typing.Iterable[IndexRow]:
        table_index = context.get_table_index()
        row_index = context.get_row_index()
        yield IndexRow(x=row.x, table_index=table_index, row_index=row_index)


# `@aggregator` helps to mark if that mapper is aggregator.
# This means mapper-aggregator take the string iterator as input (instead of single row).
@yt.wrapper.aggregator
class MapperAggregator(yt.wrapper.TypedJob):
    def __call__(self, rows: RowIterator[Row]) -> typing.Iterable[SumRow]:
        sum = 0
        for row in rows:
            sum += row.x
        yield SumRow(sum=sum)


# You can use a similar decorator for the reducer.
@yt.wrapper.reduce_aggregator
class ReduceAggregator(yt.wrapper.TypedJob):
    def __call__(self, row_groups: typing.Iterable[RowIterator[Row]]) -> typing.Iterable[SumRow]:
        sum = 0
        for rows in row_groups:
            for row in rows:
                sum += row.x
        yield SumRow(sum=sum)


# `@raw_io` helps to mark the function taking records(rows) from `stdin` and writing to `stdout`.
@yt.wrapper.raw_io
def sum_x_raw():
    sum = 0
    for line in sys.stdin:
        sum += int(line.strip())
        # NB: stdout takes strings not bytes in Python 3.
        sys.stdout.write(json.dumps({"sum": sum}) + "\n")


# `@raw` marks the function taking raw data stream as input instead of parsed records.
@yt.wrapper.raw
def change_field_raw(line):
    row = json.loads(line)
    row["x"] += 10
    # Mapper like this gets and passes byte strings.
    yield json.dumps(row).encode() + b"\n"


def main():
    # You should set up cluster address in YT_PROXY environment variable.
    cluster = os.getenv("YT_PROXY")
    if cluster is None or cluster == "":
        raise RuntimeError("Environment variable YT_PROXY is empty")
    client = yt.wrapper.YtClient(cluster)

    path = "//tmp/{}-pytutorial-job-decorators".format(getpass.getuser())
    client.create("map_node", path, force=True)
    input1, input2 = yt.wrapper.ypath_join(path, "input1"), yt.wrapper.ypath_join(path, "input2")
    output = yt.wrapper.ypath_join(path, "output")

    client.write_table_structured(
        yt.wrapper.TablePath(input1, sorted_by=["x"]),
        Row,
        [Row(x=2), Row(x=4)],
    )
    client.write_table_structured(
        yt.wrapper.TablePath(input2, sorted_by=["x"]),
        Row,
        [Row(x=1), Row(x=100)],
    )

    client.run_map(
        MapperWithContext(),
        [input1, input2],
        output,
    )
    output_rows = sorted(
        client.read_table_structured(output, IndexRow),
        key=lambda row: row.x,
    )
    assert output_rows == [
        IndexRow(row_index=0, table_index=1, x=1),
        IndexRow(row_index=0, table_index=0, x=2),
        IndexRow(row_index=1, table_index=0, x=4),
        IndexRow(row_index=1, table_index=1, x=100),
    ]

    client.remove(output)
    client.run_map(
        MapperAggregator(),
        input1,
        output,
    )
    assert list(client.read_table_structured(output, SumRow)) == [SumRow(sum=6)]

    client.run_reduce(
        ReduceAggregator(),
        input1,
        output,
        reduce_by=["x"],
    )
    assert list(client.read_table_structured(output, SumRow)) == [SumRow(sum=6)]

    client.remove(output)
    client.run_map(
        sum_x_raw,
        input1,
        output,
        # Input and output formats can be different.
        # We will get format schemaful dsv as input in this case.
        # https://yt.yandex-team.ru/docs/description/storage/formats#schemaful_dsv
        input_format=yt.wrapper.SchemafulDsvFormat(columns=["x"]),
        # We expect some variant of json lines as output.
        # https://yt.yandex-team.ru/docs/description/storage/formats#json
        output_format=yt.wrapper.JsonFormat(),
    )
    assert list(client.read_table_structured(output, SumRow)) == [SumRow(sum=2), SumRow(sum=6)]

    client.remove(output)
    client.run_map(
        change_field_raw,
        input1,
        output,
        format=yt.wrapper.JsonFormat(),
    )
    assert list(client.read_table_structured(output, Row)) == [Row(x=12), Row(x=14)]


if __name__ == "__main__":
    main()
