# -*- coding: utf-8 -*-

import getpass

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


if __name__ == "__main__":
    yt.wrapper.config.set_proxy("freud")

    sorted_staff_table = "//tmp/" + getpass.getuser() + "-pytutorial-staff-sorted"
    sorted_is_robot_table = "//tmp/" + getpass.getuser() + "-pytutorial-is_robot-sorted"
    output_table = "//tmp/" + getpass.getuser() + "-pytutorial-robots"

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
        # В source_table мы указываем список из двух таблиц.
        # Внутри редьюсера table_index для записи будет равен индексу соответсвующей таблицы внутри этого списка.
        # 0 для записей из sorted_staff_table, 1 для записей из sorted_is_robot_table.
        source_table=[sorted_staff_table, sorted_is_robot_table],
        destination_table=output_table,
        reduce_by=["uid"],
    )

    print("Output table: https://yt.yandex-team.ru/freud/#page=navigation&offsetMode=row&path={0}".format(output_table))
