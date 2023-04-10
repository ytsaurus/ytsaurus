# -*- coding: utf-8 -*-

import getpass
import os

import yt.wrapper


# Для того чтобы обращаться к свойствам строк таблиц (например, из какой таблицы пришла строка)
# нужно воспользоваться декоратором yt.wraper.with_context и добавить к аргументам функции `context'
# Этот новый аргумент как раз и поможет нам со свойствами строк.
@yt.wrapper.with_context
def filter_robots_reducer(key, input_row_iterator, context):

    login_row = None
    is_robot = False
    for input_row in input_row_iterator:
        # Свойство `table_index' вернёт нам индекс таблицы откуда была прочитана текущая строка.
        if context.table_index == 0:
            # Таблица с логинами.
            login_row = input_row
        elif context.table_index == 1:
            # Таблица про роботов.
            is_robot = input_row["is_robot"]
        else:
            # Какая-то фигня, такого индекса быть не может.
            raise RuntimeError("Unknown table index")

    assert login_row is not None

    if is_robot:
        yield login_row


def main():
    # You need to set up cluster address in YT_PROXY environment variable.
    cluster = os.getenv("YT_PROXY")
    if cluster is None or cluster == "":
        raise RuntimeError("Environment variable YT_PROXY is empty")
    client = yt.wrapper.YtClient(cluster)

    sorted_staff_table = "//tmp/{}-pytutorial-staff-sorted".format(getpass.getuser())
    sorted_is_robot_table = "//tmp/{}-pytutorial-is_robot-sorted".format(getpass.getuser())
    output_table = "//tmp/{}-pytutorial-robots".format(getpass.getuser())

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
        # В source_table мы указываем список из двух таблиц.
        # Внутри редьюсера table_index для записи будет равен индексу соответсвующей таблицы внутри этого списка.
        # 0 для записей из sorted_staff_table, 1 для записей из sorted_is_robot_table.
        source_table=[sorted_staff_table, sorted_is_robot_table],
        destination_table=output_table,
        reduce_by=["uid"],
    )

    ui_url = os.getenv("YT_UI_URL")
    print(f"Output table: {ui_url}/#page=navigation&offsetMode=row&path={output_table}")


if __name__ == "__main__":
    main()
