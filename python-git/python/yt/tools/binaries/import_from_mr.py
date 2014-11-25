#!/usr/bin/env python

from yt.tools.atomic import process_tasks_from_list, CANCEL, REPEAT
from yt.tools.common import update_args
from yt.tools.yamr import Yamr, YamrError
from yt.tools.remote_copy_tools import copy_yamr_to_yt_pull, IncorrectRowCount
from yt.wrapper.cli_helpers import die
from yt.wrapper.client import Yt

import yt.logger as logger
import yt.wrapper as yt

import os
import copy
import sys
import traceback
from argparse import ArgumentParser
from subprocess32 import TimeoutExpired, CalledProcessError

def import_table(object, args):
    object = copy.deepcopy(object)
    if isinstance(object, dict):
        src = object["src"]
        del object["src"]
        dst = object["dst"]
        del object["dst"]
        params = update_args(args, object)
    else:
        src = object
        dst = os.path.join(args.destination_dir, src)
        params = args

    try:
        yamr = Yamr(binary=params.mapreduce_binary,
                    server=params.mr_server,
                    server_port=params.mr_server_port,
                    http_port=params.mr_http_port,
                    proxies=params.mr_proxy,
                    proxy_port=params.mr_proxy_port,
                    fetch_info_from_http=params.fetch_info_from_http,
                    mr_user=params.mr_user,
                    timeout=300.0)

        yt_client = Yt(yt.config.http.PROXY, token=yt.config.http.TOKEN)

        if yamr.is_empty(src):
            logger.info("Source table '%s' is empty", src)
            return CANCEL

        record_count = yamr.records_count(src, allow_cache=True)
        sorted = yamr.is_sorted(src, allow_cache=True)

        if not params.force and yt_client.exists(dst) and (yt_client.get_type(dst) != "table" or not yt_client.is_empty(dst)):
            logger.warning("Destination table '%s' is not empty", dst)
            return CANCEL

        yt_client.mkdir(os.path.dirname(dst), recursive=True)

        logger.info("Destination table '%s' created", dst)

        spec = {
            "pool": params.yt_pool,
            "job_io": {"table_writer": {"max_row_weight": 32 * 1024 * 1024}}
        }
        copy_yamr_to_yt_pull(yamr, yt_client, src, dst, fastbone=params.fastbone, spec_template=spec)

        if params.erasure_codec is not None and params.erasure_codec == "none":
            params.erasure_codec = None

        if params.compression_codec is not None or params.erasure_codec is not None:
            mode = "sorted" if sorted else "unordered"
            spec = {"combine_chunks": "true",
                    "force_transform": "true"}

            if params.compression_codec is not None:
                yt_client.set_attribute(dst, "compression_codec", params.compression_codec)
            if params.erasure_codec is not None:
                yt_client.set_attribute(dst, "erasure_codec", params.erasure_codec)
                spec["job_io"] = {"table_writer": {"desired_chunk_size": 2 * 1024 ** 3}}
                spec["data_size_per_job"] = max(1, int(4 * (1024 ** 3) / yt_client.get(dst + "/@compression_ratio")))

            logger.info("Merging '%s' with spec '%s'", dst, repr(spec))
            yt_client.run_merge(dst, dst, mode=mode, spec=spec)

        # Additional final check
        if yt_client.records_count(dst) != record_count:
            logger.error("Incorrect record count (expected: %d, actual: %d)", record_count, yt_client.records_count(dst))
            return REPEAT

    except (CalledProcessError, TimeoutExpired, IncorrectRowCount) as error:
        logger.exception(error.message)
        return REPEAT
    except YamrError as error:
        logger.exception("Yamr failed: " + yt.errors.format_error(error))
        return REPEAT
    except:
        logger.exception("Unknown error occurred while import")
        return CANCEL

def main():
    yt.config.IGNORE_STDERR_IF_DOWNLOAD_FAILED = True

    parser = ArgumentParser()
    parser.add_argument("--tables-queue", help="YT path to list with tables")
    parser.add_argument("--destination-dir")

    parser.add_argument("--src")
    parser.add_argument("--dst")

    parser.add_argument("--mr-server")
    parser.add_argument("--mr-server-port", default="8013")
    parser.add_argument("--mr-http-port", default="13013")
    parser.add_argument("--mr-proxy", action="append")
    parser.add_argument("--mr-user", default="tmp")
    parser.add_argument("--mr-proxy-port", default="13013")
    parser.add_argument("--mapreduce-binary", default="./mapreduce")
    parser.add_argument("--fetch-info-from-http", action="store_true", default=False,
                        help="parse table meta information from http server")

    parser.add_argument("--compression-codec")
    parser.add_argument("--erasure-codec")

    parser.add_argument("--force", action="store_true", default=False,
                        help="always drop destination table")
    parser.add_argument("--fastbone", action="store_true", default=False)

    parser.add_argument("--yt-token")
    parser.add_argument("--yt-pool")
    parser.add_argument("--yt-proxy")

    args = parser.parse_args()

    if args.yt_proxy is not None:
        yt.config.set_proxy(args.yt_proxy)

    if args.tables_queue is not None:
        assert args.src is None and args.dst is None
        process_tasks_from_list(
            args.tables_queue,
            lambda obj: import_table(obj, args))
    else:
        assert args.src is not None and args.dst is not None
        import_table({"src": args.src, "dst": args.dst}, args)


if __name__ == "__main__":
    try:
        main()
    except yt.YtError as error:
        die(str(error))
    except Exception:
        traceback.print_exc(file=sys.stderr)
        die()
