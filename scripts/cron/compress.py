#!/usr/bin/env python

from yt.tools.atomic import process_tasks_from_list
from yt.tools.conversion_tools import convert_to_erasure

from yt.wrapper.cli_helpers import die
from yt.wrapper.common import parse_bool

import yt.logger as logger
import yt.wrapper as yt

from argparse import ArgumentParser

def compress(table):
    try:
        if not yt.exists(table):
            return
        
        try:
            if not parse_bool(yt.get(table + "/@force_nightly_compress")):
                return
        except yt.YtResponseError as error:
            if error.is_resolve_error():
                return

        if yt.check_permission("cron", "write", table)["action"] != "allow":
            logger.warning("Have no permission to write table %s", table)
            return

        logger.info("Compressing table %s", table)
        convert_to_erasure(table, erasure_codec="lrc_12_2_2", compression_codec="gzip_best_compression")
        yt.remove(table + "/@force_nightly_compress")

    except yt.YtError as e:
        logger.error("Failed to merge table %s with error %s", table, repr(e))

def main():
    parser = ArgumentParser(description="Find tables to compress and run compression")
    parser.add_argument("action", help="Action should be 'find' or 'run'")
    parser.add_argument('--queue', required=True, help="Path to the queue with tables")
    args = parser.parse_args()

    if args.action == "find":
        tables = []
        for table in yt.search("//tmp", node_type="table", attributes=["force_nightly_compress"]):
            if parse_bool(table.attributes.get("force_nightly_compress", "false")):
                tables.append(table)
        yt.set(args.queue, tables)
    elif args.action == "run":
        process_tasks_from_list(args.queue, compress)
    else:
        die("Incorrect action: " + args.action)

if __name__ == "__main__":
    main()


