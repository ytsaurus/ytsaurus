# -*- coding: utf-8 -*-

import getpass
import os
import typing

import yt.wrapper
import yt.wrapper.schema as schema
from yt.wrapper.schema import RowIterator, OutputRow, Variant


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
    # Чтобы не запутаться, можно завести именованные константы для индексов таблиц.
    ROBOT_TABLE_INDEX = 0
    HUMAN_TABLE_INDEX = 1

    def __call__(self, input_row_iterator: RowIterator[Variant[StaffRow, IsRobotRow]]) \
            -> typing.Iterable[OutputRow[Variant[StaffRow, StaffRow]]]:

        login_row = None
        is_robot = False
        for input_row, context in input_row_iterator.with_context():
            if context.get_table_index() == 0:
                login_row = input_row
            elif context.get_table_index() == 1:
                is_robot = input_row.is_robot
            else:
                raise RuntimeError("Unknown table index")

        assert login_row is not None

        if is_robot:
            output_table_index = self.ROBOT_TABLE_INDEX
        else:
            output_table_index = self.HUMAN_TABLE_INDEX

        yield yt.wrapper.OutputRow(login_row, table_index=output_table_index)


def main():
    # You need to set up cluster address in YT_PROXY environment variable.
    cluster = os.getenv("YT_PROXY")
    if cluster is None or cluster == "":
        raise RuntimeError("Environment variable YT_PROXY is empty")
    client = yt.wrapper.YtClient(cluster)

    sorted_staff_table = "//tmp/{}-pytutorial-mio-typed-staff-sorted".format(getpass.getuser())
    sorted_is_robot_table = "//tmp/{}-pytutorial-mio-type-is_robot-sorted".format(getpass.getuser())
    human_table = "//tmp/{}-pytutorial-mio-typed-humans".format(getpass.getuser())
    robot_table = "//tmp/{}-pytutorial-mio-typed-robots".format(getpass.getuser())

    client.run_sort(
        source_table="//home/tutorial/staff_unsorted_schematized",
        destination_table=sorted_staff_table,
        sort_by=["uid"],
    )

    client.create("table", sorted_is_robot_table, ignore_existing=True, attributes={
        "schema": schema.TableSchema()
        .add_column("uid", yt.type_info.Int64)
        .add_column("is_robot", yt.type_info.Bool)
    })

    client.run_sort(
        source_table="//home/tutorial/is_robot_unsorted",
        destination_table=sorted_is_robot_table,
        sort_by=["uid"],
    )

    client.run_reduce(
        FilterRobotsReducer(),
        # Индексы входных и выходных таблиц так же определяются тем порядком,
        # в котором они указываются в соответствующих параметрах.
        source_table=[sorted_staff_table, sorted_is_robot_table],
        destination_table=[robot_table, human_table],
        reduce_by=["uid"],
    )

    ui_url = os.getenv("YT_UI_URL")
    print(f"Robot table: {ui_url}/#page=navigation&offsetMode=row&path={robot_table}")
    print(f"Human table: {ui_url}/#page=navigation&offsetMode=row&path={human_table}")


if __name__ == "__main__":
    main()
