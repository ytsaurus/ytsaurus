# -*- coding: utf-8 -*-

import getpass

import yt.wrapper


#
# Для того чтобы запустить операцию mapreduce, нам нужны обычные маппер и редьюсер
# (их даже можно использовать в других местах в отдельных операциях map/reduce).
#


@yt.wrapper.yt_dataclass
class StaffRow:
    name: str
    login: str
    uid: int


@yt.wrapper.yt_dataclass
class CountRow:
    name: str
    count: int


class NormalizeNameMapper(yt.wrapper.TypedJob):
    def prepare_operation(self, context, preparer):
        preparer.input(0, StaffRow).output(0, StaffRow)

    def __call__(self, row):
        normalized_name = row.name.lower()
        row.name = normalized_name
        yield row


class CountNamesReducer(yt.wrapper.TypedJob):
    def prepare_operation(self, context, preparer):
        preparer.input(0, StaffRow).output(0, CountRow)

    def __call__(self, rows):
        count = 0
        for input_row in rows:
            name = input_row.name
            count += 1

        yield CountRow(name=name, count=count)


if __name__ == "__main__":
    client = yt.wrapper.YtClient(proxy="freud")

    output_table = "//tmp/{}-pytutorial-name-stat".format(getpass.getuser())

    # Запуск операции MapReduce несильно отличается от запуска других операций.
    # Нам надо указать список ключей, по которым мы будем редьюсить,
    # а так же маппер и редьюсер.
    client.run_map_reduce(
        NormalizeNameMapper(),
        CountNamesReducer(),
        source_table="//home/dev/tutorial/staff_unsorted",
        destination_table=output_table,
        reduce_by=["name"],
    )

    print("Output table: https://yt.yandex-team.ru/freud/#page=navigation&offsetMode=row&path={0}".format(output_table))
