#!/usr/bin/env python

from atomic import process_tasks_from_list

import yt.wrapper as yt

def merge(table):
    try:
        yt.merge_tables(table, table, "unordered",
                        table_writer={"codec":"gzip_best_compression"},
                        spec={"combine_chunks":"true"})
    except yt.YtError as e:
        print "Failed to merge table %s with error %s" % (table, repr(e))

if __name__ == "__main__":
    process_tasks_from_list("//home/ignat/tables_to_merge", merge)

