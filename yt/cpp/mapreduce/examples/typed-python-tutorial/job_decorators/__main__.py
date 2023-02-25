# -*- coding: utf-8 -*-

import getpass
import sys
import json
import typing

import yt.wrapper
from yt.wrapper.schema import RowIterator, Context


@yt.wrapper.yt_dataclass
class Row:
    x: int


# Можно использовать наследование датаклассов.
@yt.wrapper.yt_dataclass
class IndexRow(Row):
    table_index: int
    row_index: int


@yt.wrapper.yt_dataclass
class SumRow:
    sum: int


# @with_context позволяет заказать контекст для функции.
# В этой переменной будут лежать контрольные атрибуты
# (например, индекс текущей строки и входной таблицы)
@yt.wrapper.with_context
class MapperWithContext(yt.wrapper.TypedJob):
    def __call__(self, row: Row, context: Context) -> typing.Iterable[IndexRow]:
        table_index = context.get_table_index()
        row_index = context.get_row_index()
        yield IndexRow(x=row.x, table_index=table_index, row_index=row_index)


# @aggregator позволяет отметить, что данный маппер является агрегатором,
# то есть принимает на вход итератор строк, а не одну строку.
@yt.wrapper.aggregator
class MapperAggregator(yt.wrapper.TypedJob):
    def __call__(self, rows: RowIterator[Row]) -> typing.Iterable[SumRow]:
        sum = 0
        for row in rows:
            sum += row.x
        yield SumRow(sum=sum)


# Для редьюсера есть аналогичный декоратор.
@yt.wrapper.reduce_aggregator
class ReduceAggregator(yt.wrapper.TypedJob):
    def __call__(self, row_groups: typing.Iterable[RowIterator[Row]]) -> typing.Iterable[SumRow]:
        sum = 0
        for rows in row_groups:
            for row in rows:
                sum += row.x
        yield SumRow(sum=sum)


# @raw_io позволяет отметить, что функция будет брать записи (строки) из stdin и писать в stdout.
@yt.wrapper.raw_io
def sum_x_raw():
    sum = 0
    for line in sys.stdin:
        sum += int(line.strip())
        # Обратите внимание: в Python 3 в stdout надо писать строки, а не байты.
        sys.stdout.write(json.dumps({"sum": sum}) + "\n")


# @raw позволяет отметить, что функция принимает на вход поток сырых данных, а не распарсенные записи.
@yt.wrapper.raw
def change_field_raw(line):
    row = json.loads(line)
    row["x"] += 10
    # А вот такой маппер получает и отдаёт **байтовые** строки.
    yield json.dumps(row).encode() + b"\n"


def main():
    client = yt.wrapper.YtClient(proxy="freud")

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
        # Входной и выходной форматы могут различаться.
        # В данном случае вход будет приходить в формате schemaful dsv
        # https://yt.yandex-team.ru/docs/description/storage/formats#schemaful_dsv
        input_format=yt.wrapper.SchemafulDsvFormat(columns=["x"]),
        # Выход ожидается в виде некоторой вариации json lines:
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
