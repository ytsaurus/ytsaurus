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
    # The `__call__` method of a reducer gets
    # an iterator over all records in the input table with the given key as input.
    # The output should be like all the records that we want to write to the output tables (like the `__call__` method of a mapper).
    def __call__(self, input_row_iterator: RowIterator[StaffRow]) -> typing.Iterable[CountRow]:
        count = 0
        longest_login = ""
        for input_row in input_row_iterator:
            name = input_row.name
            count += 1
            if len(longest_login) < len(input_row.login):
                longest_login = input_row.login
        yield CountRow(name=name, count=count, longest_login=longest_login)


def main():
    # You need to set up cluster address in YT_PROXY environment variable.
    cluster = os.getenv("YT_PROXY")
    if cluster is None or cluster == "":
        raise RuntimeError("Environment variable YT_PROXY is empty")
    client = yt.wrapper.YtClient(cluster)

    sorted_tmp_table = "//tmp/{}-pytutorial-tmp".format(getpass.getuser())
    output_table = "//tmp/{}-pytutorial-simple-reduce-typed-name-stat".format(getpass.getuser())

    # Sorting the table by the field "name" in ascending order.
    client.run_sort(
        source_table="//home/tutorial/staff_unsorted_schematized",
        destination_table=sorted_tmp_table,
        sort_by=["name"],
    )

    # Running the reducer.
    client.run_reduce(
        CountNamesReducer(),
        source_table=sorted_tmp_table,
        destination_table=output_table,
        reduce_by=["name"],
        spec={"max_failed_job_count": 1},
    )

    ui_url = os.getenv("YT_UI_URL")
    print(f"Output table: {ui_url}/#page=navigation&offsetMode=row&path={output_table}")


if __name__ == "__main__":
    main()
