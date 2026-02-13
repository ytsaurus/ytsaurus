# -*- coding: utf-8 -*-

import getpass
import os
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
    # Use the special type `yt.wrapper.schema.Variant` to specify types of the several rows.
    # This type is parameterized by the list of tables rows types.
    # The types order inside the variant should match an order of corresponding input or output tables.
    # You should not use the variant if all the tables have the same string type.
    def __call__(self, input_row_iterator: RowIterator[Variant[StaffRow, IsRobotRow]]) -> typing.Iterable[StaffRow]:
        login_row = None
        is_robot = False
        # Use the method `.with_context()` of iterator in order to access tables rows properties.
        # E.G. you can know the table which the row came from.
        # The new iterator returnes pairs of row and context.
        # You can get attributes from the context.
        for input_row, context in input_row_iterator.with_context():
            # The `get_table_index` method will return the index of the table which the current row was read from.
            if context.get_table_index() == 0:
                # Table with logins.
                login_row = input_row
            elif context.get_table_index() == 1:
                # Table about robots.
                is_robot = input_row.is_robot
            else:
                # An unexpected error, such an index could not exist.
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

    sorted_staff_table = "//tmp/{}-pytutorial-mi-typed-staff-sorted".format(getpass.getuser())
    sorted_is_robot_table = "//tmp/{}-pytutorial-mi-typed-is_robot-sorted".format(getpass.getuser())
    output_table = "//tmp/{}-pytutorial-mi-typed-robots".format(getpass.getuser())

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
        # We specify the list with two tables into source_table.
        # `Table_index` for writing will be equal to index of the corresponding table inside this list in the reducer.
        # 0 for the records from `sorted_staff_table`, 1 for the records from `sorted_is_robot_table`.
        source_table=[sorted_staff_table, sorted_is_robot_table],
        destination_table=output_table,
        reduce_by=["uid"],
    )

    ui_url = os.getenv("YT_UI_URL")
    print(f"Output table: {ui_url}/#page=navigation&offsetMode=row&path={output_table}")


if __name__ == "__main__":
    main()
