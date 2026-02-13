# -*- coding: utf-8 -*-

import yt.wrapper

import getpass
import os


def main():
    # Set up cluster address in YT_PROXY environment variable.
    cluster = os.getenv("YT_PROXY")
    if cluster is None or cluster == "":
        raise RuntimeError("Environment variable YT_PROXY is empty")
    # Create an RPC-client.
    client = yt.wrapper.YtClient(cluster, config={"backend": "rpc"})

    schema = [
        {"name": "x", "type": "int64", "sort_order": "ascending"},
        {"name": "y", "type": "int64"},
        {"name": "z", "type": "int64"},
    ]
    table = "//tmp/{}-pytutorial-dynamic-tables-rpc".format(getpass.getuser())
    client.create("table", table, force=True, attributes={"schema": schema, "dynamic": True})

    # Mounting new dynamic table and adding row into it.
    client.mount_table(table, sync=True)
    client.insert_rows(table, [{"x": 0, "y": 99}])

    # Create a tablet transaction for transactional work with the table.
    with client.Transaction(type="tablet"):
        # Get the row with the given key
        rows = list(client.lookup_rows(table, [{"x": 0}]))
        if len(rows) == 1 and rows[0]["y"] <= 100:
            rows[0]["y"] += 1
            # Update the row.
            client.insert_rows(table, rows)

    # Select all rows from the table and print them.
    print(list(client.select_rows("* from [{}]".format(table))))


if __name__ == "__main__":
    main()
