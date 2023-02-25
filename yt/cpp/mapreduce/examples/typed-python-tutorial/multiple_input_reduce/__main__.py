# -*- coding: utf-8 -*-

import getpass
import typing

import yt.wrapper
import yt.wrapper.schema as schema
from yt.wrapper.schema import RowIterator, Variant


@yt.wrapper.yt_dataclass
class StaffRow:
    name: str
    login: str
    uid: schema.Int64


@yt.wrapper.yt_dataclass
class IsRobotRow:
    uid: schema.Int64
    is_robot: bool


class FilterRobotsReducer(yt.wrapper.TypedJob):
    # Для указания типов нескольких строк используется специальный тип yt.wrapper.schema.Variant,
    # параметризуемый списком типов строк таблиц.
    # Порядок типов внутри варианта должен совпадать с порядком соответсвующих входных (или выходных) таблиц.
    # Если все таблицы имеют один и тот же тип строк, можно обойтись без варианта.
    def __call__(self, input_row_iterator: RowIterator[Variant[StaffRow, IsRobotRow]]) -> typing.Iterable[StaffRow]:
        login_row = None
        is_robot = False
        # Для того чтобы обращаться к свойствам строк таблиц (например, из какой таблицы пришла строка),
        # нужно воспользоваться методом .with_context() у итератора.
        # Новый итератор возвращает пары (строка, контекст).
        # Из контекста можно доставать интересующие атрибуты.
        for input_row, context in input_row_iterator.with_context():
            # Метод `get_table_index' вернёт нам индекс таблицы, откуда была прочитана текущая строка.
            if context.get_table_index() == 0:
                # Таблица с логинами.
                login_row = input_row
            elif context.get_table_index() == 1:
                # Таблица про роботов.
                is_robot = input_row.is_robot
            else:
                # Какая-то фигня, такого индекса быть не может.
                raise RuntimeError("Unknown table index")

        assert login_row is not None

        if is_robot:
            yield login_row


if __name__ == "__main__":
    client = yt.wrapper.YtClient(proxy="freud")

    sorted_staff_table = "//tmp/{}-pytutorial-staff-sorted".format(getpass.getuser())
    sorted_is_robot_table = "//tmp/{}-pytutorial-is_robot-sorted".format(getpass.getuser())
    output_table = "//tmp/{}-pytutorial-robots".format(getpass.getuser())

    client.run_sort(
        source_table="//home/dev/typed-py-tutorial/staff_unsorted",
        destination_table=sorted_staff_table,
        sort_by=["uid"],
    )

    client.run_sort(
        source_table="//home/dev/typed-py-tutorial/is_robot_unsorted",
        destination_table=sorted_is_robot_table,
        sort_by=["uid"],
    )

    client.run_reduce(
        FilterRobotsReducer(),
        # В source_table мы указываем список из двух таблиц.
        # Внутри редьюсера table_index для записи будет равен индексу соответсвующей таблицы внутри этого списка:
        # 0 -- для записей из sorted_staff_table, 1 -- для записей из sorted_is_robot_table.
        source_table=[sorted_staff_table, sorted_is_robot_table],
        destination_table=output_table,
        reduce_by=["uid"],
    )

    print("Output table: https://yt.yandex-team.ru/freud/#page=navigation&offsetMode=row&path={0}".format(output_table))
