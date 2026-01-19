# -*- coding: utf-8 -*-

import getpass
import os
import typing

import yt.wrapper
from yt.wrapper.schema import RowIterator

# Use standard mapper and reducer to run mapreduce operation.
# You can use them in other parts for standalone operations map/reduce also.


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
    def __call__(self, row: StaffRow) -> typing.Iterable[StaffRow]:
        normalized_name = row.name.lower()
        row.name = normalized_name
        yield row


class CountNamesReducer(yt.wrapper.TypedJob):
    def __call__(self, rows: RowIterator[StaffRow]) -> typing.Iterable[CountRow]:
        count = 0
        for input_row in rows:
            name = input_row.name
            count += 1

        yield CountRow(name=name, count=count)


def main():
    # You need to set up cluster address in YT_PROXY environment variable.
    cluster = os.getenv("YT_PROXY")
    if cluster is None or cluster == "":
        raise RuntimeError("Environment variable YT_PROXY is empty")
    client = yt.wrapper.YtClient(cluster)

    output_table = "//tmp/{}-pytutorial-name-stat".format(getpass.getuser())

    # Running MapReduce operation looks like running other operations.
    # Specify keys list for reducing and mapper and reducer also.
    client.run_map_reduce(
        NormalizeNameMapper(),
        CountNamesReducer(),
        source_table="//home/tutorial/staff_unsorted",
        destination_table=output_table,
        reduce_by=["name"],
    )

    ui_url = os.getenv("YT_UI_URL")
    print(f"Output table: {ui_url}/#page=navigation&offsetMode=row&path={output_table}")


if __name__ == "__main__":
    main()
