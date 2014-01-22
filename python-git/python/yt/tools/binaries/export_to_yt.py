#!/usr/bin/env python

from yt.tools.atomic import process_tasks_from_list
from yt.tools.common import update_args
from yt.wrapper.common import die

from yt.wrapper.table_commands import get_sorted_by

import yt.logger as logger
import yt.wrapper as yt

import os
import sys
import copy
import traceback

from argparse import ArgumentParser

def export_table(object, args):
    object = copy.deepcopy(object)
    if isinstance(object, dict):
        src = object["src"]
        del object["src"]
        dst = object["dst"]
        del object["dst"]
        params = update_args(args, object)
    else:
        src = object
        dst = os.path.join(params.destination_dir, src.strip("/"))
        params = args

    logger.info("Exporting '%s' to '%s'", src, dst)

    if not yt.exists(src):
        logger.warning("Export table '%s' is empty", src)
        return -1

    if params.yt_proxy is None:
        logger.error("You should specify yt proxy")
        return -1
    
    old_proxy = yt.config.http.PROXY
    try: 
        yt.config.set_proxy(params.yt_proxy)
        if yt.exists(dst) and yt.records_count(dst) != 0:
            if params.force:
                yt.remove(dst)
            else:
                logger.error("Destination table '%s' is not empty" % dst)
                return -1
        yt.create_table(dst, recursive=True, ignore_existing=True)
    finally:
        yt.config.set_proxy(old_proxy)
    
    record_count = yt.records_count(src)

    is_sorted = yt.is_sorted(src)
    if is_sorted:
        sorted_by = get_sorted_by(src)

    hosts = "hosts/fb" if params.fastbone else "hosts"

    command = "YT_TOKEN={} YT_HOSTS={} yt2 write --proxy {} --format '<format=binary>yson' '<append=true>{}'"\
            .format(params.yt_token, hosts, params.yt_proxy, dst)
    
    logger.info("Running map '%s'", command)
    yt.run_map(command, src, yt.create_temp_table(),
               format=yt.YsonFormat(format="binary"),
               memory_limit=3500 * yt.config.MB,
               spec={"pool": params.yt_pool,
                     "data_size_per_job": 2 * 1024 * yt.config.MB})

    try: 
        yt.config.set_proxy(params.yt_proxy)
        result_record_count = yt.records_count(dst)
        if record_count != result_record_count:
            logger.error("Incorrect record count (expected: %d, actual: %d)", record_count, result_record_count)
            yt.remove(dst)
            return -1

        if is_sorted:
            logger.info("Running sort")
            yt.run_sort(dst, sort_by=sorted_by)

    finally:
        yt.config.set_proxy(old_proxy)
    


def main():
    parser = ArgumentParser()
    parser.add_argument("--tables-queue")
    parser.add_argument("--destination-dir")

    parser.add_argument("--src")
    parser.add_argument("--dst")

    parser.add_argument("--yt-proxy", help="Proxy of destination cluster. Source cluster should be specified through YT_PROXY")
    parser.add_argument("--yt-token")
    parser.add_argument("--yt-pool", default="export_restricted")

    parser.add_argument("--force", action="store_true", default=False)
    parser.add_argument("--fastbone", action="store_true", default=False)

    args = parser.parse_args()

    if args.tables_queue is not None:
        assert args.src is None and args.dst is None
        process_tasks_from_list(
            args.tables_queue,
            lambda obj: export_table(obj, args)
        )
    else:
        assert args.src is not None and args.dst is not None
        export_table({"src": args.src, "dst": args.dst}, args)


if __name__ == "__main__":
    try:
        main()
    except yt.YtError as error:
        die(str(error))
    except Exception:
        traceback.print_exc(file=sys.stderr)
        die()

