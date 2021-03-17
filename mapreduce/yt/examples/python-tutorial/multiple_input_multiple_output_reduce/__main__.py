# -*- coding: utf-8 -*-

import getpass

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
    yt.wrapper.config.set_proxy("freud")

    sorted_staff_table = "//tmp/" + getpass.getuser() + "-pytutorial-staff-sorted"
    sorted_is_robot_table = "//tmp/" + getpass.getuser() + "-pytutorial-is_robot-sorted"
    human_table = "//tmp/" + getpass.getuser() + "-pytutorial-humans"
    robot_table = "//tmp/" + getpass.getuser() + "-pytutorial-robots"

    yt.wrapper.run_sort(
        source_table="//home/ermolovd/yt-tutorial/staff_unsorted", destination_table=sorted_staff_table, sort_by=["uid"]
    )

    yt.wrapper.run_sort(
        source_table="//home/ermolovd/yt-tutorial/is_robot_unsorted",
        destination_table=sorted_is_robot_table,
        sort_by=["uid"],
    )

    yt.wrapper.run_reduce(
        filter_robots_reducer,
        source_table=[sorted_staff_table, sorted_is_robot_table],
        # Индексы выходных таблиц так же определяются тем порядком в котором они указываются в параметре destination_table
        destination_table=[robot_table, human_table],
        reduce_by=["uid"],
    )

    print("Robot table: https://yt.yandex-team.ru/freud/#page=navigation&offsetMode=row&path={0}".format(robot_table))
    print("Human table: https://yt.yandex-team.ru/freud/#page=navigation&offsetMode=row&path={0}".format(human_table))
