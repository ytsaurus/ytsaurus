#!/usr/bin/env python

from yt.wrapper.client import Yt
from yt.tools.atomic import process_tasks_from_list, CANCEL
from yt.tools.common import update_args
from yt.tools.yamr import Yamr
from yt.tools.remote_copy_tools import copy_yt_to_yamr_pull, copy_yt_to_yamr_push
from yt.wrapper.cli_helpers import die

import yt.logger as logger
import yt.wrapper as yt

import os
import copy
import sys
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
        params = args
        src = object
        dst = os.path.join(params.destination_dir, src.strip("/"))

    logger.info("Exporting '%s' to '%s'", src, dst)

    yt_client = Yt(params.yt_proxy, token=params.yt_token)
    yamr_client = Yamr(binary=params.mapreduce_binary,
                       server=params.mr_server,
                       server_port=params.mr_server_port,
                       http_port=params.mr_http_port,
                       proxies=params.mr_proxy,
                       proxy_port=params.mr_proxy_port,
                       fetch_info_from_http=params.fetch_info_from_http,
                       mr_user=params.mr_user)

    if not yt_client.exists(src):
        raise yt.YtError("Export table '{0}' is empty".format(src))

    if not yamr_client.is_empty(dst):
        if params.force:
            yamr_client.drop(dst)
        else:
            raise yt.YtError("Destination table '{0}' is not empty".format(dst))


    if params.copy_type == "pull":
        copy_yt_to_yamr_pull(yt_client, yamr_client, src, dst, fastbone=params.fastbone)
    else:
        user_slots_path = "//sys/pools/{0}/@resource_limits/user_slots".format(params.yt_pool)
        if not yt.exists(user_slots_path):
            raise yt.YtError("Pool must have user slots limit")

        spec = {
            "title": "Remote copy from YT to Yamr",
            "pool": params.yt_pool}

        copy_yt_to_yamr_push(yt_client, yamr_client, src, dst, fastbone=params.fastbone, spec_template=spec)

def export_table_wrapper(object, args):
    try:
        export_table(object, args)
    except Exception as error:
        logger.exception(error.message)
        return CANCEL

def main():
    yt.config.IGNORE_STDERR_IF_DOWNLOAD_FAILED = True

    parser = ArgumentParser()
    parser.add_argument("--tables-queue")
    parser.add_argument("--destination-dir")

    parser.add_argument("--src")
    parser.add_argument("--dst")

    parser.add_argument("--copy-type", default="push")

    parser.add_argument("--mr-server")
    parser.add_argument("--mr-server-port", default="8013")
    parser.add_argument("--mr-http-port", default="13013")
    parser.add_argument("--mr-proxy", action="append")
    parser.add_argument("--mr-proxy-port", default="13013")
    parser.add_argument("--mr-user", default="tmp")
    parser.add_argument("--mapreduce-binary", default="./mapreduce")
    parser.add_argument("--fetch-info-from-http", action="store_true", default=False)
    parser.add_argument("--fastbone", action="store_true", default=False)

    parser.add_argument("--force", action="store_true", default=False)
    parser.add_argument("--skip-empty-tables", action="store_true", default=False,
                        help="do not return empty source tables back to queue")

    parser.add_argument("--yt-proxy", default=yt.config.http.PROXY)
    parser.add_argument("--yt-token")
    parser.add_argument("--yt-pool", default="export_restricted")

    parser.add_argument("--opts", default="")

    args = parser.parse_args()

    if args.tables_queue is not None:
        assert args.src is None and args.dst is None
        process_tasks_from_list(
            args.tables_queue,
            lambda obj: export_table_wrapper(obj, args))
    else:
        assert args.src is not None and args.dst is not None
        export_table({"src": args.src, "dst": args.dst}, args)

if __name__ == "__main__":
    try:
        main()
    except yt.YtError as error:
        die(yt.errors.format_error(error))
    except Exception:
        traceback.print_exc(file=sys.stderr)
        die()
