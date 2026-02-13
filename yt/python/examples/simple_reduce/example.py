# -*- coding: utf-8 -*-

import getpass
import os

import yt.wrapper


# A reducer is a regular generator function. For input it gets:
#   - the current key
#   - an iterator over all the records of the input table with given key
# The output should be like that: all the records we want to write to the output tables (the same as the mapper function).
def count_names_reducer(key, input_row_iterator):

    # In this case the key consists of only one column "name". But generally it could be several columns.
    # You can read specific fields of the key like a dictionary.
    name = key["name"]

    count = 0
    longest_login = ""
    for input_row in input_row_iterator:
        count += 1
        if len(input_row["login"]) > len(longest_login):
            longest_login = input_row["login"]

    yield {"name": name, "count": count, "longest_login": longest_login}


def main():
    # You need to set up cluster address in YT_PROXY environment variable.
    cluster = os.getenv("YT_PROXY")
    if cluster is None or cluster == "":
        raise RuntimeError("Environment variable YT_PROXY is empty")
    client = yt.wrapper.YtClient(cluster)

    sorted_tmp_table = "//tmp/{}-pytutorial-tmp".format(getpass.getuser())
    output_table = "//tmp/{}-pytutorial-simple-reduce-name-stat".format(getpass.getuser())

    client.run_sort(
        source_table="//home/tutorial/staff_unsorted", destination_table=sorted_tmp_table, sort_by=["name"]
    )

    client.run_reduce(
        count_names_reducer, source_table=sorted_tmp_table, destination_table=output_table, reduce_by=["name"]
    )

    ui_url = os.getenv("YT_UI_URL")
    print(f"Output table: {ui_url}/#page=navigation&offsetMode=row&path={output_table}")


if __name__ == "__main__":
    main()
