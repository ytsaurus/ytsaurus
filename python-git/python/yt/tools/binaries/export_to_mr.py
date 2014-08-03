#!/usr/bin/env python

from yt.tools.atomic import process_tasks_from_list, REPEAT, CANCEL
from yt.tools.common import update_args
from yt.tools.yamr import Yamr
from yt.wrapper.common import die

import yt.logger as logger
import yt.wrapper as yt

import os
import copy
import sys
import traceback
import subprocess

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
        params = args
        src = object
        dst = os.path.join(params.destination_dir, src.strip("/"))

    mr = Yamr(binary=params.mapreduce_binary,
              server=params.mr_server,
              server_port=params.mr_server_port,
              http_port=params.mr_http_port,
              proxies=params.mr_proxy,
              proxy_port=params.mr_proxy_port,
              fetch_info_from_http=params.fetch_info_from_http,
              cache=False,
              mr_user=params.mr_user)

    try:
        logger.info("Exporting '%s' to '%s'", src, dst)

        if not yt.exists(src):
            logger.warning("Export table '%s' is empty", src)
            return CANCEL

        if not mr.is_empty(dst):
            if params.force:
                mr.drop(dst)
            else:
                logger.error("Destination table '%s' is not empty" % dst)
                return CANCEL

        record_count = yt.records_count(src)

        user_slots_path = "//sys/pools/{0}/@resource_limits/user_slots".format(params.yt_pool)
        if not yt.exists(user_slots_path):
            logger.error("Pool must have user slots limit")
            return CANCEL
        else:
            limit = params.speed_limit / yt.get(user_slots_path)

        use_fastbone = "-opt net_table=fastbone" if params.fastbone else ""

        command = "pv -q -L {0} | "\
            "{1} USER=tmp MR_USER={2} ./mapreduce -server {3} {4} -append -lenval -subkey -write {5}"\
                .format(limit,
                        params.opts,
                        params.mr_user,
                        mr.server,
                        use_fastbone,
                        dst)
        logger.info("Running map '%s'", command)
        yt.run_map(command, src, yt.create_temp_table(),
                   files=mr.binary,
                   format=yt.YamrFormat(has_subkey=True, lenval=True),
                   memory_limit=2500 * yt.config.MB,
                   spec={"pool": params.yt_pool,
                         "data_size_per_job": 2 * 1024 * yt.config.MB})

        result_record_count = mr.records_count(dst)
        if record_count != result_record_count:
            logger.error("Incorrect record count (expected: %d, actual: %d)", record_count, result_record_count)
            mr.drop(dst)
            return REPEAT
    
    except subprocess.CalledProcessError:
        logger.exception("Mapreduce binary failed")
        return CANCEL
    except yt.YtOperationFailedError:
        logger.exception("Operation failed")
        return CANCEL


def main():
    yt.config.IGNORE_STDERR_IF_DOWNLOAD_FAILED = True

    parser = ArgumentParser()
    parser.add_argument("--tables-queue")
    parser.add_argument("--destination-dir")

    parser.add_argument("--src")
    parser.add_argument("--dst")

    parser.add_argument("--mr-server")
    parser.add_argument("--mr-server-port", default="8013")
    parser.add_argument("--mr-http-port", default="13013")
    parser.add_argument("--mr-proxy", action="append")
    parser.add_argument("--mr-proxy-port", default="13013")
    parser.add_argument("--mr-user", default="tmp")
    parser.add_argument("--mapreduce-binary", default="./mapreduce")
    parser.add_argument("--fetch-info-from-http", action="store_true", default=False)

    parser.add_argument("--speed-limit", type=int, default=500 * yt.config.MB)
    parser.add_argument("--force", action="store_true", default=False)
    parser.add_argument("--skip-empty-tables", action="store_true", default=False,
                        help="do not return empty source tables back to queue")
    parser.add_argument("--fastbone", action="store_true", default=False)

    parser.add_argument("--yt-proxy")
    parser.add_argument("--yt-pool", default="export_restricted")

    parser.add_argument("--opts", default="")

    args = parser.parse_args()

    if args.yt_proxy is not None:
        yt.config.set_proxy(args.yt_proxy)

    if args.tables_queue is not None:
        assert args.src is None and args.dst is None
        process_tasks_from_list(
            args.tables_queue,
            lambda obj: export_table(obj, args))
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
