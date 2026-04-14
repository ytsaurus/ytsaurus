# -*- coding: utf-8 -*-

import getpass
import os

import yt.wrapper

# Use standard mapper and reducer to run mapreduce operation.
# You can use them in other parts for standalone operations map/reduce also.


def normalize_name_mapper(row):
    normalized_name = row["name"].lower()
    yield {"name": normalized_name}


def count_names_reducer(key, input_row_iterator):
    name = key["name"]

    count = 0
    for input_row in input_row_iterator:
        count += 1

    yield {"name": name, "count": count}


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
        normalize_name_mapper,
        count_names_reducer,
        source_table="//home/tutorial/staff_unsorted",
        destination_table=output_table,
        reduce_by=["name"],
    )

    ui_url = os.getenv("YT_UI_URL")
    print(f"Output table: {ui_url}/#page=navigation&offsetMode=row&path={output_table}")


if __name__ == "__main__":
    main()
