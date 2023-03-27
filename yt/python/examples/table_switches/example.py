# -*- coding: utf-8 -*-

import getpass
import os

import yt.wrapper
import yt.yson


@yt.wrapper.aggregator
@yt.wrapper.with_context
def mapper_with_iterator(rows, context):
    sum = 0
    for row in rows:
        # Такой способ узнавать индекс входной таблицы соответствует control_attributes_mode="iterator".
        input_table_index = context.table_index
        if input_table_index == 0:
            sum += int(row["value"])
        else:
            sum -= int(row["value"])

        output_table_index = sum % 2

        # Такой способ переключать таблицы соответствует control_attributes_mode="iterator".
        yield yt.wrapper.create_table_switch(output_table_index)
        yield {"sum": sum}


@yt.wrapper.aggregator
def mapper_with_row_fields(rows):
    sum = 0
    for row in rows:
        # Такой способ узнавать индекс входной таблицы соответствует control_attributes_mode="row_fields".
        input_table_index = row["@table_index"]
        if input_table_index == 0:
            sum += int(row["value"])
        else:
            sum -= int(row["value"])

        output_table_index = sum % 2
        # Такой способ переключать таблицы соответствует control_attributes_mode="row_fields".
        yield {"sum": sum, "@table_index": output_table_index}


# Пример получения row_index в reducer с помощью context.
@yt.wrapper.with_context
def reducer(key, rows, context):
    for row in rows:
        yield {"row_index": context.row_index}


def main():
    # You need to set up cluster address in YT_PROXY environment variable.
    cluster = os.getenv("YT_PROXY")
    if cluster is None or cluster == "":
        raise RuntimeError("Environment variable YT_PROXY is empty")
    client = yt.wrapper.YtClient(cluster)

    path = "//tmp/{}-table_switches".format(getpass.getuser())
    client.create("map_node", path, ignore_existing=True)

    input1, input2, input3 = inputs = ["{}/input{}".format(path, i) for i in range(1, 4)]
    client.write_table(input1, [{"value": 7}])
    client.write_table(input2, [{"value": 3}])
    client.write_table(input3, [{"value": 4}])

    output1, output2 = outputs = ["{}/output{}".format(path, i) for i in range(1, 3)]

    # Пример запуска маппера, который будет использовать функцию yt.wrapper.create_table_switch
    # для переключения выходных таблиц.
    client.run_map(
        mapper_with_iterator,
        inputs,
        outputs,
        format=yt.wrapper.YsonFormat(control_attributes_mode="iterator"),
    )
    # В первую таблицу попадают чётные суммы.
    assert list(client.read_table(output1)) == [{"sum": 4}, {"sum": 0}]
    # Во вторую таблицу попадают нечётные суммы.
    assert list(client.read_table(output2)) == [{"sum": 7}]

    # Пример запуска маппера, который будет использовать поле @table_index для переключения выходных таблиц.
    client.run_map(
        mapper_with_row_fields,
        inputs,
        outputs,
        format=yt.wrapper.YsonFormat(control_attributes_mode="row_fields"),
    )
    # В первую таблицу попадают чётные суммы.
    assert list(client.read_table(output1)) == [{"sum": 4}, {"sum": 0}]
    # Во вторую таблицу попадают нечётные суммы.
    assert list(client.read_table(output2)) == [{"sum": 7}]

    client.remove(input1)
    client.write_table("<sorted_by=[x]>" + input1, [{"x": 1}, {"x": 3}, {"x": 4}])

    # Пример запуска редьюсера, который получает индексы строк из контекста.
    client.run_reduce(
        reducer,
        input1,
        output1,
        reduce_by=["x"],
        format=yt.wrapper.YsonFormat(),
        spec={"job_io": {"control_attributes": {"enable_row_index": True}}},
    )
    assert list(client.read_table(output1)) == [{"row_index": 0}, {"row_index": 1}, {"row_index": 2}]


if __name__ == "__main__":
    main()
