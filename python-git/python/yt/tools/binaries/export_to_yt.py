#!/usr/bin/env python

from yt.tools.atomic import process_tasks_from_list, CANCEL
from yt.tools.common import update_args
from yt.tools.remote_copy_tools import copy_yt_to_yt_through_proxy
from yt.wrapper.cli_helpers import die
from yt.wrapper.client import Yt
from yt.wrapper.http import get_token

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
        return CANCEL

    if params.yt_proxy is None:
        logger.error("You should specify yt proxy")
        return CANCEL

    if params.yt_proxy == yt.config.http.PROXY:
        logger.error("Source and destination proxies should be different")
        return CANCEL

    source_client = Yt(yt.config.http.PROXY, get_token())
    destination_client = Yt(params.yt_proxy, params.yt_token)

    yt.config.http.PROXY = None

    if destination_client.exists(dst) and destination_client.records_count(dst) != 0:
        if params.force:
            destination_client.remove(dst)
        else:
            logger.error("Destination table '%s' is not empty" % dst)
            return CANCEL
    destination_client.create_table(dst, recursive=True, ignore_existing=True)

    copy_yt_to_yt_through_proxy(source_client, destination_client, src, dst, params.fastbone)


def main():
    yt.config.IGNORE_STDERR_IF_DOWNLOAD_FAILED = True

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

    parser.add_argument("--compression-codec")
    parser.add_argument("--erasure-codec")

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

