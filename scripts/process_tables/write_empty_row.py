#!/usr/bin/env python

from yt.tools.atomic import process_tasks_from_list

import yt.wrapper as yt

def write_empty_row(table):
    row_count = yt.records_count(table)
    with yt.Transaction():
        yt.write_table(yt.TablePath(table, append=True), [], format=yt.DsvFormat())
        if  yt.records_count(table) != row_count:
            raise Exception("Row count differs: expected=%d, read=%d" % (row_count, yt.records_count(table)))

if __name__ == "__main__":
    process_tasks_from_list(
        "//home/ignat/bad_balanced_tables",
        write_empty_row
    )

