# -*- coding: utf-8 -*-

import getpass
import os

import yt.wrapper


@yt.wrapper.with_context
def filter_robots_reducer(key, input_row_iterator, context):
    login_row = None
    is_robot = False
    for input_row in input_row_iterator:
        if context.table_index == 0:
            login_row = input_row
        elif context.table_index == 1:
            is_robot = input_row["is_robot"]
        else:
            raise RuntimeError("Unknown table index")

    assert login_row is not None

    # Функция create_table_switch переключает запись последующих строк в таблицу с указанным индексом.
    if is_robot:
        yield yt.wrapper.create_table_switch(0)
    else:
        yield yt.wrapper.create_table_switch(1)

    yield login_row


if __name__ == "__main__":
    # You need to set up cluster address in YT_PROXY environment variable.
    cluster = os.getenv("YT_PROXY")
    if cluster is None or cluster == "":
        raise RuntimeError("Environment variable YT_PROXY is empty")
    client = yt.wrapper.YtClient(cluster)

    sorted_staff_table = "//tmp/{}-pytutorial-staff-sorted".format(getpass.getuser())
    sorted_is_robot_table = "//tmp/{}-pytutorial-is_robot-sorted".format(getpass.getuser())
    human_table = "//tmp/{}-pytutorial-humans".format(getpass.getuser())
    robot_table = "//tmp/{}-pytutorial-robots".format(getpass.getuser())

    client.run_sort(
        source_table="//home/tutorial/staff_unsorted", destination_table=sorted_staff_table, sort_by=["uid"]
    )

    client.run_sort(
        source_table="//home/tutorial/is_robot_unsorted",
        destination_table=sorted_is_robot_table,
        sort_by=["uid"],
    )

    client.run_reduce(
        filter_robots_reducer,
        source_table=[sorted_staff_table, sorted_is_robot_table],
        # Индексы выходных таблиц так же определяются тем порядком в котором они указываются в параметре destination_table
        destination_table=[robot_table, human_table],
        reduce_by=["uid"],
    )

    ui_url = os.getenv("YT_UI_URL")
    print(f"Robot table: {ui_url}/#page=navigation&offsetMode=row&path={robot_table}")
    print(f"Human table: {ui_url}/#page=navigation&offsetMode=row&path={human_table}")
