#!/usr/bin/env python

from yt.tools.atomic import process_tasks_from_list

import yt.logger as logger
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
        erasure_codec = yt.get_attribute(table, "erasure_codec")

        if yt.check_permission("cron", "write", table)["action"] != "allow":
            logger.warning("Have no permission to write table %s", table)
            return

        desired_chunk_size = 512 * 1024 ** 2
        if erasure_codec != "none":
            desired_chunk_size = 2 * 1024 ** 3

        data_size_per_job = max(1,  int(desired_chunk_size / compression_ratio))

        logger.info("Merging table %s (erasure codec: %s, compression_ratio: %f)", table, erasure_codec, compression_ratio)

        temp_table = yt.create_temp_table(prefix="merge")
        try:
            # To copy all attributes of node
            yt.remove(temp_table)
            yt.copy(table, temp_table, preserve_account=True)
            yt.run_erase(temp_table)
            #for attr in ["account", "compression_codec", "erasure_codec", "replication_factor"]:
            #    yt.set("{}/@{}".format(temp_table, attr), yt.get("{}/@{}".format(table, attr)))

            mode = "sorted" if yt.is_sorted(table) else "unordered"
            yt.run_merge(table, temp_table, mode,
                         spec={"combine_chunks":"true",
                               "data_size_per_job": data_size_per_job,
                               "unavailable_chunk_strategy": "fail",
                               "unavailable_chunk_tactics": "fail",
                               "job_io": {
                                   "table_writer": {
                                       "desired_chunk_size": desired_chunk_size
                                   }
                               }})

            if yt.exists(table):
                with yt.Transaction():
                    yt.lock(table)
                    if yt.get_attribute(table, "revision") == revision:
                        yt.run_merge(temp_table, table, mode=mode)
        finally:
            yt.remove(temp_table, force=True)

    except yt.YtError as e:
        logger.error("Failed to merge table %s with error %s", table, repr(e))

if __name__ == "__main__":
    process_tasks_from_list(sys.argv[1], merge)

