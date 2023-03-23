# -*- coding: utf-8 -*-

import getpass
import typing

import yt.wrapper
from yt.wrapper.schema import RowIterator
from yt.wrapper.spec_builders import (
    MapperSpecBuilder,
    MapSpecBuilder,
    MapReduceSpecBuilder,
)


@yt.wrapper.yt_dataclass
class ValueRow:
    key: str
    value: int


@yt.wrapper.yt_dataclass
class SumRow:
    key: str
    sum: int


class Reducer(yt.wrapper.TypedJob):
    def __call__(self, rows: RowIterator[ValueRow]) -> typing.Iterable[SumRow]:
        key = None
        s = 0
        for row in rows:
            key = row.key
            s += row.value
        yield SumRow(key=key, sum=s)


if __name__ == "__main__":
    client = yt.wrapper.YtClient(proxy="freud")

    input_table = "//tmp/{}-spec-builder-input".format(getpass.getuser())
    client.remove(input_table, force=True)
    map_output_table = "//tmp/{}-spec-builder-map-output".format(getpass.getuser())
    map_reduce_output_table = "//tmp/{}-spec-builder-map-reduce-output".format(getpass.getuser())

    rows = [
        ValueRow(key="a", value=1),
        ValueRow(key="b", value=2),
        ValueRow(key="a", value=3),
    ]
    # Запишем несколько строк в таблицу.
    client.write_table_structured(input_table, ValueRow, rows)

    # Можно создать отдельный билдер для пользовательского джоба.
    mapper_spec_builder = (MapperSpecBuilder()
                           .command("cat")
                           .memory_limit(yt.wrapper.common.GB)
                           .format(yt.wrapper.YsonFormat()))

    # И для операции.
    spec_builder = (MapSpecBuilder()
                    .input_table_paths(input_table)
                    .output_table_paths(map_output_table)
                    .mapper(mapper_spec_builder))

    # Билдер можно передавать прямо в функию run_operation.
    client.run_operation(spec_builder)

    print("*** After map operation ***")
    for row in client.read_table_structured(map_output_table, ValueRow):
        print("  {}".format(row))

    # Спеку пользовательского джоба можно строить fluently
    # внутри билдера для операции, используя функции
    # begin_reducer()/end_reducer().
    spec_builder = (MapReduceSpecBuilder()
                    .input_table_paths(input_table)
                    .output_table_paths(map_reduce_output_table)
                    .mapper(mapper_spec_builder)
                    .begin_reducer()
                    .command(Reducer())
                    .end_reducer()
                    .reduce_by("key"))

    client.run_operation(spec_builder)

    print("\n*** After map_reduce operation ***")
    for row in client.read_table_structured(map_reduce_output_table, SumRow):
        print("  {}".format(row))
