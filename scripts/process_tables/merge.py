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

        compression_ratio = yt.get_attribute(table, "compression_ratio")
        data_size_per_job = max(1, int(min(16 * 1024 ** 3, 512 * 1024 ** 2 / compression_ratio)))
        
        mode = "sorted" if yt.is_sorted(table) else "unordered"
        yt.run_merge(table, table, mode,
                     spec={"combine_chunks":"true",
                           "data_size_per_job": data_size_per_job,
                           "job_io": {"table_reader": {"prefetch_window": 100}},
                           "strategy": "fail"})
    except yt.YtError as e:
        print "Failed to merge table %s with error %s" % (table, repr(e))

if __name__ == "__main__":
    process_tasks_from_list(sys.argv[1], merge)

