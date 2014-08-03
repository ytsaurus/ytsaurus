#!/usr/bin/env python

from yt.tools.atomic import process_tasks_from_list, CANCEL, REPEAT
from yt.tools.common import update_args
from yt.tools.yamr import Yamr
from yt.wrapper.common import die

import yt.logger as logger
import yt.wrapper as yt

import os
import copy
import uuid
import subprocess
import sys
import traceback
from argparse import ArgumentParser
from urllib import quote_plus

def pull_table(source, destination, record_count, mr, fastbone, portion_size, job_count, pool):
    proxies = mr.proxies
    if not proxies:
        proxies = [mr.server]

    ranges = []
    record_threshold = max(1, record_count * portion_size / mr.data_size(source))
    for i in xrange((record_count - 1) / record_threshold + 1):
        server = proxies[i % len(proxies)]
        start = i * record_threshold
        end = min(record_count, (i + 1) * record_threshold)
        ranges.append((server, start, end))

    temp_table = yt.create_temp_table(prefix=os.path.basename(source))
    yt.write_table(temp_table,
                   ["\t".join(map(str, range)) + "\n" for range in ranges],
                   format=yt.YamrFormat(lenval=False, has_subkey=True))

    if pool is None:
        pool = "restricted"
    spec = {"data_size_per_job": 1, "pool": pool, "job_io": {"table_writer": {"max_row_weight": 32 * 1024 * 1024}}}
    if job_count is not None:
        spec["job_count"] = job_count

    temp_yamr_table = "tmp/yt/" + str(uuid.uuid4())
    mr.copy(source, temp_yamr_table)
    source = temp_yamr_table

    if mr.proxies:
        command = 'curl "http://${{server}}/table/{0}?subkey=1&lenval=1&startindex=${{start}}&endindex=${{end}}"'.format(quote_plus(source))
    else:
        use_fastbone = "-opt net_table=fastbone" if fastbone else ""
        shared_tx = "-sharedtransactionid yt" if mr.supports_shared_transactions else ""
        command = 'USER=yt MR_USER=tmp ./mapreduce -server $server {0} -read {1}:[$start,$end] -lenval -subkey {2}'\
                    .format(use_fastbone, source, shared_tx)

    debug_str = 'echo "{0}" 1>&2; '.format(command.replace('"', "'"))
    command = 'while true; do '\
                  'IFS="\t" read -r server start end; '\
                  'if [ "$?" != "0" ]; then break; fi; '\
                  'set -e; '\
                  '{0} {1}; '\
                  'set +e; '\
              'done;'\
                  .format(debug_str, command)
    logger.info("Pull import: run map '%s' with spec '%s'", command, repr(spec))
    try:
        yt.run_map(
            command,
            temp_table,
            destination,
            input_format=yt.YamrFormat(lenval=False, has_subkey=True),
            output_format=yt.YamrFormat(lenval=True, has_subkey=True),
            files=mr.binary,
            memory_limit = 2500 * yt.config.MB,
            spec=spec)
    finally:
        mr.drop(temp_yamr_table)

def push_table(source, destination, record_count, mr, yt_binary, token, fastbone, job_size, job_count, speed_limit, push_mapper_opts, push_mapper_spec, use_default_mapreduce_yt):
    if "'" in push_mapper_spec:
        raise yt.YtError("push_mapper_spec can't contain single quote symbol")

    record_threshold = max(1, record_count * job_size / mr.data_size(source))
    new_job_count = (record_count - 1) / record_threshold + 1
    if job_count is None or new_job_count < job_count:
        job_count = new_job_count

    if speed_limit is not None:
        limit = speed_limit / job_count
        speed_limit = "pv -q -L {0} | ".format(limit)
    else:
        speed_limit = ""

    if yt_binary is None and not use_default_mapreduce_yt:
        with open("./mapreduce-yt", "w") as f:
            for block in yt.download_file("//home/files/mapreduce-yt", response_type="iter_content"):
                f.write(block)
        yt_binary = "./mapreduce-yt"

    if fastbone:
        hosts = "hosts/fb"
    else:
        hosts = "hosts"

    if token is None:
        token = yt.http.get_token()

    # TODO(ignat): do not show token
    if use_default_mapreduce_yt:
        files = ""
        binary = "mapreduce-yt"
    else:
        files = "-file " + yt_binary
        binary = os.path.basename(yt_binary)

    command = \
        "{0} -server {1} "\
            "-map '{2} {3} YT_TOKEN={4} YT_HOSTS={5} {6} -server {7} -ytspec '\"'\"'{8}'\"'\"' -append -lenval -subkey -write {9}' "\
            "-src {10} "\
            "-dst {11} "\
            "-jobcount {12} "\
            "-lenval "\
            "-subkey "\
            "{13} "\
                .format(
                    mr.binary,
                    mr.server,
                    speed_limit,
                    push_mapper_opts,
                    token,
                    hosts,
                    binary,
                    yt.config.http.PROXY,
                    push_mapper_spec,
                    destination,
                    source,
                    os.path.join("tmp", os.path.basename(source)),
                    job_count,
                    files)

    logger.info("Push import: run command %s", command)

    subprocess.check_call(command, shell=True)

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

    mr = Yamr(binary=params.mapreduce_binary,
              server=params.mr_server,
              server_port=params.mr_server_port,
              http_port=params.mr_http_port,
              proxies=params.mr_proxy,
              proxy_port=params.mr_proxy_port,
              fetch_info_from_http=params.fetch_info_from_http,
              mr_user=params.mr_user)

    if mr.is_empty(src):
        logger.info("Source table '%s' is empty", src)
        return CANCEL

    record_count = mr.records_count(src)
    sorted = mr.is_sorted(src)

    logger.info("Importing table '%s' (row count: %d, sorted: %d, method: %s)", src, record_count, sorted, params.import_type)

    if params.force:
        yt.remove(dst, recursive=True, force=True)
    else:
        if yt.exists(dst):
            if not (yt.get_type(dst) == "table" and yt.is_empty(dst)):
                logger.warning("Destination table '%s' is not empty", dst)
                return CANCEL
    yt.create_table(dst, recursive=True, ignore_existing=True)

    logger.info("Destination table '%s' created", dst)

    try:
        if params.import_type == "pull":
            pull_table(src, dst, record_count, mr,
                       fastbone=params.fastbone,
                       portion_size=params.portion_size,
                       job_count=params.job_count,
                       pool=params.yt_pool)
        elif params.import_type == "push":
            push_table(src, dst, record_count, mr,
                       yt_binary=params.yt_binary,
                       token=params.yt_token,
                       fastbone=params.fastbone,
                       job_size=params.job_size,
                       job_count=params.job_count,
                       speed_limit=params.speed_limit,
                       push_mapper_opts=params.push_mapper_opts,
                       push_mapper_spec=params.push_mapper_spec,
                       use_default_mapreduce_yt=params.use_default_mapreduce_yt)
        else:
            raise RuntimeError("Incorrect import type: " + params.import_type)

        # TODO: add checksum checking
        if yt.records_count(dst) != record_count:
            logger.error("Incorrect record count (expected: %d, actual: %d)", record_count, yt.records_count(dst))
            return REPEAT

        if sorted:
            logger.info("Sorting '%s'", dst)
            yt.run_sort(dst, sort_by=["key", "subkey"])

        if params.erasure_codec is not None and params.erasure_codec == "none":
            params.erasure_codec = None

        if params.compression_codec is not None or params.erasure_codec is not None:
            mode = "sorted" if sorted else "unordered"
            spec = {"combine_chunks": "true",
                    "force_transform": "true"}

            if params.compression_codec is not None:
                yt.set_attribute(dst, "compression_codec", params.compression_codec)
            if params.erasure_codec is not None:
                yt.set_attribute(dst, "erasure_codec", params.erasure_codec)
                spec["job_io"] = {"table_writer": {"desired_chunk_size": 2 * 1024 ** 3}}
                spec["data_size_per_job"] = max(1, int(4 * (1024 ** 3) / yt.get(dst + "/@compression_ratio")))

            logger.info("Merging '%s' with spec '%s'", dst, repr(spec))
            yt.run_merge(dst, dst, mode=mode, spec=spec)

        # Additional final check
        if yt.records_count(dst) != record_count:
            logger.error("Incorrect record count (expected: %d, actual: %d)", record_count, yt.records_count(dst))
            return REPEAT

    except yt.YtError:
        logger.exception("Error occurred while import")
        yt.remove(dst, force=True)
        return CANCEL

def main():
    yt.config.IGNORE_STDERR_IF_DOWNLOAD_FAILED = True

    parser = ArgumentParser()
    parser.add_argument("--tables-queue", help="YT path to list with tables")
    parser.add_argument("--destination-dir")

    parser.add_argument("--src")
    parser.add_argument("--dst")

    parser.add_argument("--import-type", default="pull",
                        help="push (run operation in Yamr that writes to YT) or pull (run operation in YT that reads from Yamr)")

    parser.add_argument("--mr-server")
    parser.add_argument("--mr-server-port", default="8013")
    parser.add_argument("--mr-http-port", default="13013")
    parser.add_argument("--mr-proxy", action="append")
    parser.add_argument("--mr-user", default="tmp")
    parser.add_argument("--mr-proxy-port", default="13013")
    parser.add_argument("--mapreduce-binary", default="./mapreduce")
    parser.add_argument("--fetch-info-from-http", action="store_true", default=False,
                        help="parse table meta information from http server")

    parser.add_argument("--job-size", type=int, default=1024 ** 3,
                        help="size of job in push import")
    parser.add_argument("--portion-size", type=int, default=1024 ** 3,
                        help="portion of data for single read in pull import")
    parser.add_argument("--job-count", type=int)
    parser.add_argument("--speed-limit", type=int)

    parser.add_argument("--compression-codec")
    parser.add_argument("--erasure-codec")

    parser.add_argument("--force", action="store_true", default=False,
                        help="always drop destination table")
    parser.add_argument("--fastbone", action="store_true", default=False)

    parser.add_argument("--yt-binary")
    parser.add_argument("--yt-token")
    parser.add_argument("--yt-pool")
    parser.add_argument("--yt-proxy")

    parser.add_argument("--push-mapper-opts", default="")
    parser.add_argument("--push-mapper-spec", default="{}")
    parser.add_argument("--use-default-mapreduce-yt", action="store_true", default=False)

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
