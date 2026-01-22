# -*- coding: utf-8 -*-

import getpass
import os

import yt.wrapper


# In order to call parameters of tables rows you should use decorator `yt.wrapper.with_context`
# for example if you need to know the table from which the row comes
# A new argument will help with rows properties.
@yt.wrapper.with_context
def filter_robots_reducer(key, input_row_iterator, context):

    login_row = None
    is_robot = False
    for input_row in input_row_iterator:
        # The `table_index` property will return the table index from which the current row was read.
        if context.table_index == 0:
            # Table with logins.
            login_row = input_row
        elif context.table_index == 1:
            # Table about robots.
            is_robot = input_row["is_robot"]
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
        # We specify the list with two tables into source_table.
        # Table_index for writing wil be equal to index of the corresponding table inside this list in the reducer.
        # 0 for the records from `sorted_staff_table`, 1 for the records from `sorted_is_robot_table`.
        source_table=[sorted_staff_table, sorted_is_robot_table],
        destination_table=output_table,
        reduce_by=["uid"],
    )

    ui_url = os.getenv("YT_UI_URL")
    print(f"Output table: {ui_url}/#page=navigation&offsetMode=row&path={output_table}")


if __name__ == "__main__":
    main()
