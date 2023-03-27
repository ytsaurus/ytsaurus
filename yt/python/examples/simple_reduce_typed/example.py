# -*- coding: utf-8 -*-

import getpass
import os
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
    # You need to set up cluster address in YT_PROXY environment variable.
    cluster = os.getenv("YT_PROXY")
    if cluster is None or cluster == "":
        raise RuntimeError("Environment variable YT_PROXY is empty")
    client = yt.wrapper.YtClient(cluster)

    sorted_tmp_table = "//tmp/{}-pytutorial-tmp".format(getpass.getuser())
    output_table = "//tmp/{}-pytutorial-name-stat".format(getpass.getuser())

    # Отсортируем таблицу по возрастанию поля name.
    client.run_sort(
        source_table="//home/tutorial/staff_unsorted_schematized",
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

    ui_url = os.getenv("YT_UI_URL")
    print(f"Output table: {ui_url}/#page=navigation&offsetMode=row&path={output_table}")
