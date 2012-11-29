#!/usr/bin/env python

from atomic import process_tasks_from_list

import yt.wrapper as yt

if __name__ == "__main__":
    process_tasks_from_list(
        "//home/ignat/tables_to_merge", 
        lambda table: \
            yt.merge_tables(table, table, "unordered",
                            table_writer={"codec_id":"gzip_best_compression"},
                            spec={"combine_chunks":"true"})
    )

