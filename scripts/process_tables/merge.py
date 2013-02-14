#!/usr/bin/env python

from atomic import process_tasks_from_list

import yt.wrapper as yt

def merge(table):
    try:
        if not yt.exists(table):
            return
        compression_ratio = yt.get_attribute(table, "compression_ratio")
        data_size_per_job = min(16 * 1024 ** 3, 512 ** 3 / compression_ratio)
        
        yt.run_merge(table, table, "unordered",
                     table_writer={"codec":"gzip_best_compression"},
                     spec={"combine_chunks":"true", "max_data_size_per_job": data_size_per_job})
    except yt.YtError as e:
        print "Failed to merge table %s with error %s" % (table, repr(e))

if __name__ == "__main__":
    process_tasks_from_list("//home/ignat/tables_to_merge", merge)

