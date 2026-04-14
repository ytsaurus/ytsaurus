# -*- coding: utf-8 -*-

import getpass
import os

import yt.wrapper


def main():
    # You need to set up cluster address in YT_PROXY environment variable.
    cluster = os.getenv("YT_PROXY")
    if cluster is None or cluster == "":
        raise RuntimeError("Environment variable YT_PROXY is empty")
    client = yt.wrapper.YtClient(cluster)

    table = "//tmp/{}-read-write".format(getpass.getuser())

    # Writing data to the table. If the table exists already than it will be overwritten.
    client.write_table(
        table,
        [
            {"english": "one", "russian": "один"},
            {"english": "two", "russian": "два"},
        ],
    )

    # Writing data to the end of the table. Let`s use a trick.
    # Use the `TablePath` class and its append option.
    client.write_table(
        yt.wrapper.TablePath(table, append=True),
        [
            {"english": "three", "russian": "три"},
        ],
    )

    # Reading the entire table.
    print("*** ALL TABLE ***")
    for row in client.read_table(table):
        print("english:", row["english"], "; russian:", row["russian"])
    print("*****************")
    print("")

    # Reading the first 2 rows of the table.
    print("*** FIRST TWO ROWS ***")
    for row in client.read_table(
        yt.wrapper.TablePath(table, start_index=0, end_index=2)  # reading from zero to second row. The 2nd row is no need to read.
    ):
        print("english:", row["english"], "; russian:", row["russian"])
    print("*****************")
    print("")

    # We can read records using keys if we sort the table.
    client.run_sort(table, sort_by=["english"])

    # Reading the record using one key.
    print("*** EXACT KEY ***")
    for row in client.read_table(
        yt.wrapper.TablePath(
            table,
            exact_key=["three"]  # passing a list of key columns values as a key
            # (that columns by which the table is sorted).
            # Here we have a simple case with one key column, but there can be more of them.
        )
    ):
        print("english:", row["english"], "; russian:", row["russian"])
    print("*****************")
    print("")


if __name__ == "__main__":
    main()
