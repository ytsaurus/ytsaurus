#!/usr/bin/env python

from atomic import process_tasks_from_list

import yt.wrapper as yt

import sys

def merge(table):
    try:
        if not yt.exists(table) or yt.get_attribute(table, "row_count") == 0:
            return

        locks = yt.get_attribute(table, "locks")
        if any(map(lambda l: l["mode"] in ["exclusive", "shared"], locks)):
            return -1

        revision = yt.get_attribute(table, "revision")

        compression_ratio = yt.get_attribute(table, "compression_ratio")
        data_size_per_job = max(1,  int(512 * 1024 ** 2 / compression_ratio))

        temp_table = yt.create_temp_table(prefix="merge")
        
        mode = "sorted" if yt.is_sorted(table) else "unordered"
        yt.run_merge(table, temp_table, mode,
                     spec={"combine_chunks":"true",
                           "data_size_per_job": data_size_per_job,
                           "unavailable_chunk_strategy": "fail",
                           "unavailable_chunk_tactics": "fail"})

        if yt.exists(table) and yt.get_attribute(table, "revision") == revision:
            yt.run_merge(temp_table, table, mode=mode)
        yt.remove(temp_table)

    except yt.YtError as e:
        print "Failed to merge table %s with error %s" % (table, repr(e))

if __name__ == "__main__":
    process_tasks_from_list(sys.argv[1], merge)

