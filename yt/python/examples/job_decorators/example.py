# -*- coding: utf-8 -*-

import getpass
import os
import sys

import yt.wrapper


# @with_context позволяет заказать контекст для функции.
# В этой переменной будут лежать контрольные атрибуты, заказанные при запуске операции.
@yt.wrapper.with_context
def reducer_with_context(key, rows, context):
    for row in rows:
        table_index = context.table_index
        row_index = context.row_index
        row.update({"table_index": table_index, "row_index": row_index})
        yield row


# @with_context позволяет заказать контекст для функции.
# В этой переменной будут лежать контрольные атрибуты, заказанные при запуске операции.
@yt.wrapper.with_context
class ReducerWithContext(object):
    def __init__(self, value):
        self.value = value

    def __call__(self, key, rows, context):
        for row in rows:
            table_index = context.table_index
            row_index = context.row_index
            row.update({"table_index": table_index, "row_index": row_index, "value": self.value})
            yield row


# @aggregator позволяет отметить, что данный маппер является агрегатором,
# то есть принимает на вход генератор рекордов, а не один рекорд.
@yt.wrapper.aggregator
def mapper_aggregator(recs):
    sum = 0
    for rec in recs:
        sum += rec.get("x", 0)
    yield {"sum": sum}


# @reduce_aggregator позволяет отметить, что reducer является агрегатором,
# то есть принимает на вход не одну пару (ключ, записи), а генератор из пар, где каждая пара — (ключ, записи с этим ключом)
@yt.wrapper.reduce_aggregator
def reducer_aggregator(row_groups):
    sum = 0
    for key, rows in row_groups:
        for row in rows:
            sum += row["x"]
    yield {"sum": sum}


# @raw_io позволяет отметить, что функция будет брать записи (строки) из stdin и писать в stdout.
@yt.wrapper.raw_io
def sum_x_raw():
    sum = 0
    for line in sys.stdin:
        sum += int(line.strip())
        sys.stdout.write("{0}\n".format(sum))


# @raw позволяет отметить, что функция принимает на вход поток сырых данных, а не распарсенные записи.
@yt.wrapper.raw
def change_field_raw(line):
    yield "{}\n".format(int(line.strip()) + 10).encode()


def main():
    # You need to set up cluster address in YT_PROXY environment variable.
    cluster = os.getenv("YT_PROXY")
    if cluster is None or cluster == "":
        raise RuntimeError("Environment variable YT_PROXY is empty")
    client = yt.wrapper.YtClient(cluster)

    path = "//tmp/{}-pytutorial-job-decorators".format(getpass.getuser())
    client.create("map_node", path, force=True)

    client.write_table("<sorted_by=[x]>{}/input1".format(path), [{"x": 2}, {"x": 4}])
    client.write_table("<sorted_by=[x]>{}/input2".format(path), [{"x": 1}, {"x": 100}])

    # Чтобы получать в контексте непустой row_index, его необходимо заказать в спеке.
    client.run_reduce(
        reducer_with_context,
        [path + "/input1", path + "/input2"],
        path + "/output",
        reduce_by=["x"],
        spec={"job_io": {"control_attributes": {"enable_row_index": True}}},
    )
    assert list(client.read_table(path + "/output")) == [
        {"row_index": 0, "table_index": 1, "x": 1},
        {"row_index": 0, "table_index": 0, "x": 2},
        {"row_index": 1, "table_index": 0, "x": 4},
        {"row_index": 1, "table_index": 1, "x": 100},
    ]

    # Чтобы получать в контексте непустой row_index, его необходимо заказать в спеке.
    client.run_reduce(
        ReducerWithContext(12),
        [path + "/input1", path + "/input2"],
        path + "/output",
        reduce_by=["x"],
        spec={"job_io": {"control_attributes": {"enable_row_index": True}}},
    )
    assert list(client.read_table(path + "/output")) == [
        {"row_index": 0, "table_index": 1, "x": 1, "value": 12},
        {"row_index": 0, "table_index": 0, "x": 2, "value": 12},
        {"row_index": 1, "table_index": 0, "x": 4, "value": 12},
        {"row_index": 1, "table_index": 1, "x": 100, "value": 12},
    ]

    client.run_map(
        mapper_aggregator,
        path + "/input1",
        path + "/output",
    )
    assert list(client.read_table(path + "/output")) == [{"sum": 6}]

    client.run_reduce(
        reducer_aggregator,
        path + "/input1",
        path + "/output",
        reduce_by=["x"],
    )
    assert list(client.read_table(path + "/output")) == [{"sum": 6}]

    client.run_map(
        sum_x_raw,
        path + "/input1",
        path + "/output",
        input_format=yt.wrapper.SchemafulDsvFormat(columns=["x"]),
        output_format=yt.wrapper.SchemafulDsvFormat(columns=["sum"]),
    )
    assert list(client.read_table(path + "/output")) == [{"sum": "2"}, {"sum": "6"}]

    client.run_map(
        change_field_raw,
        path + "/input1",
        path + "/output",
        format=yt.wrapper.SchemafulDsvFormat(columns=["x"]),
    )
    assert list(client.read_table(path + "/output")) == [{"x": "12"}, {"x": "14"}]


if __name__ == "__main__":
    main()
