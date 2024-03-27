#!/usr/bin/python3

import argparse

import yt.yson as yson
from yt.wrapper import select_rows, delete_rows
from sys import stderr


def main():
    parser = argparse.ArgumentParser(
        description="Script for faulty query removal from query tracker state"
    )
    parser.add_argument("query_id", help="Query id to remove")
    parser.add_argument(
        "--dry-run", help="Do not modify the state", action="store_true"
    )
    args = parser.parse_args()

    tables = {
        "active_queries": {
            "path": "//sys/query_tracker/active_queries",
            "key_columns": ["query_id"],
        },
        "finished_queries": {
            "path": "//sys/query_tracker/finished_queries",
            "key_columns": ["query_id"],
        },
        "finished_queries_by_start_time": {
            "path": "//sys/query_tracker/finished_queries_by_start_time",
            "key_columns": ["start_time", "query_id"],
        },
        "finished_query_results": {
            "path": "//sys/query_tracker/finished_query_results",
            "key_columns": ["query_id", "result_index"],
        },
    }
    ommitted_columns = ["rowset", "schema"]

    # TODO(max): formally speaking, this is racy as the query could be archived concurrently with the script.
    # But making a tablet transaction requires a lot of boilerplate and utilizing rpc proxy bindings, so I'd
    # not bother with that for now.

    found_rows = {}

    for table_name, table in tables.items():
        path = table["path"]
        rows = list(select_rows(f"* from [{path}] where query_id = '{args.query_id}'"))
        for row in rows:
            for column in ommitted_columns:
                if column in row:
                    row[column] = "<omitted>"
        found_rows[table_name] = rows

    print("Found following rows in query tracker state:", file=stderr)
    print(yson.dumps(found_rows, yson_format="pretty").decode(), file=stderr)

    if any(found_rows.values()):
        print("Deleting rows from query tracker state", file=stderr)

        for table_name, rows in found_rows.items():
            if rows:
                path = tables[table_name]["path"]
                key_columns = tables[table_name]["key_columns"]
                truncated_rows = [
                    {column: row[column] for column in key_columns} for row in rows
                ]
                print(f"Deleting {truncated_rows} from table {path}", file=stderr)
                if not args.dry_run:
                    delete_rows(path, truncated_rows)


if __name__ == "__main__":
    main()
