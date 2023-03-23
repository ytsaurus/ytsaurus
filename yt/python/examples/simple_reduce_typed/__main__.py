# -*- coding: utf-8 -*-

import getpass
import typing

import yt.wrapper
from yt.wrapper.schema import RowIterator


@yt.wrapper.yt_dataclass
class StaffRow:
    name: str
    login: str


@yt.wrapper.yt_dataclass
class CountRow:
    name: str
    count: int
    longest_login: str


class CountNamesReducer(yt.wrapper.TypedJob):
    # Метод __call__ Reducer-а принимает на вход
    # итератор по всем записям входной таблицы с данным ключом.
    # На выходе он должна вернуть (как и __call__ у Mapper-а) все записи,
    # которые мы хотим записать в выходные таблицы.
    def __call__(self, input_row_iterator: RowIterator[StaffRow]) -> typing.Iterable[CountRow]:
        count = 0
        longest_login = ""
        for input_row in input_row_iterator:
            name = input_row.name
            count += 1
            if len(longest_login) < len(input_row.login):
                longest_login = input_row.login
        yield CountRow(name=name, count=count, longest_login=longest_login)


if __name__ == "__main__":
    client = yt.wrapper.YtClient(proxy="freud")

    sorted_tmp_table = "//tmp/{}-pytutorial-tmp".format(getpass.getuser())
    output_table = "//tmp/{}-pytutorial-name-stat".format(getpass.getuser())

    # Отсортируем таблицу по возрастанию поля name.
    client.run_sort(
        source_table="//home/dev/typed-py-tutorial/staff_unsorted",
        destination_table=sorted_tmp_table,
        sort_by=["name"],
    )

    # Теперь можно запустить reducer.
    client.run_reduce(
        CountNamesReducer(),
        source_table=sorted_tmp_table,
        destination_table=output_table,
        reduce_by=["name"],
        spec={"max_failed_job_count": 1},
    )

    print("Output table: https://yt.yandex-team.ru/freud/#page=navigation&offsetMode=row&path={0}".format(output_table))
