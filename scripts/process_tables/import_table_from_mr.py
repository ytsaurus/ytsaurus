#!/usr/bin/env python

from atomic import process_tasks_from_list

import yt.wrapper as yt

import os
import sh
import sys
import uuid
import traceback
import subprocess
import simplejson as json
from argparse import ArgumentParser
from urllib import quote_plus

yt.config.MEMORY_LIMIT = 2500 * yt.config.MB

def main():
    parser = ArgumentParser()
    parser.add_argument("--tables")
    parser.add_argument("--destination")
    parser.add_argument("--server")
    parser.add_argument("--import-type", default="pull")
    parser.add_argument("--proxy", action="append")
    parser.add_argument("--server-port", default="8013")
    parser.add_argument("--http-port", default="13013")
    parser.add_argument("--job-size", type=int, default=2 * 1024 ** 3)
    parser.add_argument("--job-count", type=int)
    parser.add_argument("--speed", type=int)
    parser.add_argument("--compression-codec")
    parser.add_argument("--erasure-codec")
    parser.add_argument("--force", action="store_true", default=False)
    parser.add_argument("--ignore", action="store_true", default=False)
    parser.add_argument("--fastbone", action="store_true", default=False)
    parser.add_argument("--debug", action="store_true", default=False)
    parser.add_argument("--pool")
    parser.add_argument("--fetch-info-from-http", action="store_true", default=False)
    parser.add_argument("--mapreduce-binary", default="./mapreduce")
    parser.add_argument("--yt-binary")

    args = parser.parse_args()

    def field_from_page(table, field):
        """ Extract value of given field from http page of the table """
        http_content = sh.curl("{}:{}/debug?info=table&table={}".format(args.server, args.http_port, table)).stdout
        records_line = filter(lambda line: line.find(field) != -1,  http_content.split("\n"))[0]
        records_line = records_line.replace("</b>", "").replace("<br>", "").replace("<b>", "").replace(",", "")
        return records_line.split(field + ":")[1].split()[0]

    def fetch_from_server(table, field):
        if not hasattr(fetch_from_server, "cache"):
            fetch_from_server.cache = {}
        if table not in fetch_from_server.cache:
            output = subprocess.check_output(
                "{} -server {}:{} -list -prefix {} -jsonoutput"\
                    .format(args.mapreduce_binary, args.server, args.server_port, table),
                shell=True)
            fetch_from_server.cache[table] = filter(lambda obj: obj["name"] == table, json.loads(output))[0]
        return fetch_from_server.cache[table].get(field, None)

    def records_count(table):
        if args.fetch_info_from_http:
            return int(field_from_page(table, "Records"))
        else:
            return fetch_from_server(table, "records")
    
    def data_size(table):
        if args.fetch_info_from_http:
            return int(field_from_page(table, "Size"))
        else:
            return fetch_from_server(table, "size")

    def is_sorted(table):
        if args.fetch_info_from_http:
            return field_from_page(table, "Sorted").lower() == "yes"
        else:
            return fetch_from_server(table, "sorted") == 1

    def copy_table(src, dst):
        subprocess.check_call("USER=yt MR_USER=tmp {} -server {}:{} -copy -src {} -dst {}".format(args.mapreduce_binary, args.server, args.server_port, src, dst), shell=True)
    
    def drop_table(table):
        subprocess.check_call("USER=yt MR_USER=tmp {} -server {}:{} -drop {}".format(args.mapreduce_binary, args.server, args.server_port, table), shell=True)

    def is_empty(table):
        if not args.fetch_info_from_http:
            return fetch_from_server(table, "records") == 0
        """ Parse whether table is empty from html """
        http_content = sh.curl("{}:{}/debug?info=table&table={}".format(args.server, args.http_port, table)).stdout
        empty_lines = filter(lambda line: line.find("is empty") != -1,  http_content.split("\n"))
        return empty_lines and empty_lines[0].startswith("Table is empty")

    def pull_table(source, destination, count):
        has_proxy = args.proxy is not None
        if has_proxy:
            servers = ["%s:%s" % (proxy, args.http_port) for proxy in args.proxy]
        else:
            servers = ["%s:%s" % (args.server, args.server_port)]
        ranges = []
    
        record_threshold = max(1, count * args.job_size / data_size(source))
        for i in xrange((count - 1) / record_threshold + 1):
            server = servers[i % len(servers)]
            start = i * record_threshold
            end = min(count, (i + 1) * record_threshold)
            ranges.append((server, start, end))

        temp_table = yt.create_temp_table(prefix=os.path.basename(source))
        yt.write_table(temp_table,
                       ["\t".join(map(str, range)) + "\n" for range in ranges],
                       format=yt.YamrFormat(lenval=False, has_subkey=True))

        pool = args.pool
        if pool is None:
            pool = "restricted"
        spec = {"data_size_per_job": 1, "pool": pool}
        if args.job_count is not None:
            spec["job_count"] = args.job_count

        yt.set_attribute(destination, "compression_codec", "gzip_best_compression")
        
        temp_yamr_table = "tmp/yt/" + str(uuid.uuid4())
        copy_table(source, temp_yamr_table)
        source = temp_yamr_table

        if has_proxy:
            command = 'curl "http://${{server}}/table/{}?subkey=1&lenval=1&startindex=${{start}}&endindex=${{end}}"'.format(quote_plus(source))
        else:
            use_fastbone = "-opt net_table=fastbone" if args.fastbone else ""
            command = 'USER=yt MR_USER=tmp ./mapreduce -server $server {} -read {}:[$start,$end] -lenval -subkey'.format(use_fastbone, source)

        debug_str = 'echo "{}" 1>&2; '.format(command.replace('"', "'")) if args.debug else ''

        try:
            yt.run_map(
                    'while true; do '
                        'IFS="\t" read -r server start end; '
                        'if [ "$?" != "0" ]; then break; fi; '
                        'set -e; '
                        '{0}'
                        '{1}; '
                        'set +e; '
                    'done;'.format(debug_str, command),
                    temp_table,
                    destination,
                    input_format=yt.YamrFormat(lenval=False, has_subkey=True),
                    output_format=yt.YamrFormat(lenval=True, has_subkey=True),
                    files=args.mapreduce_binary,
                    spec=spec)
        finally:
            drop_table(temp_yamr_table)

    def push_table(source, destination, count):
        if args.job_count is None:
            record_threshold = max(1, count * args.job_size / data_size(source))
            # Number of data pieces
            args.job_count = (count - 1) / record_threshold + 1

        if args.speed is not None:
            limit = args.speed * yt.config.MB / args.job_count
            speed_limit = "pv -q -L {} | ".format(limit)
        else:
            speed_limit = ""

        if args.yt_binary is None:
            with open("./mapreduce-yt", "w") as f:
                for block in yt.download_file("//home/files/mapreduce-yt", response_type="iter_content"):
                    f.write(block)
            args.yt_binary = "./mapreduce-yt"

        if args.fastbone:
            yt.config.HOSTS = "hosts/fb"

        subprocess.check_call(
            "{} -server {}:{} "
                "-map '{} YT_USE_TOKEN=0 YT_USE_HOSTS=1 ./{} -server {} -append -lenval -subkey -write {}' "
                "-src {} "
                "-dst {} "
                "-jobcount {} "
                "-lenval "
                "-subkey "
                "-file {} "\
                    .format(
                        args.mapreduce_binary,
                        args.server,
                        args.server_port,
                        speed_limit,
                        os.path.basename(args.yt_binary),
                        yt.config.http.PROXY,
                        destination,
                        source,
                        os.path.join("tmp", os.path.basename(source)),
                        args.job_count,
                        args.yt_binary),
            shell=True)
        

    def import_table(obj):
        destination = None
        if isinstance(obj, dict):
            table = obj["src"]
            destination = obj["dst"]
            args.server = obj.get("mr_server", args.server)
            args.proxy = obj.get("mr_proxies", args.proxy)
            args.server_port = obj.get("server_port", args.server_port)
            args.http = obj.get("http_port", args.http_port)
        else:
            table = obj
            destination = os.path.join(args.destination, table)

        if is_empty(table):
            print >>sys.stderr, "Table is empty"
            return -1

        count = records_count(table)
        sorted = is_sorted(table)

        if yt.exists(destination):
            if args.force or (yt.get_type(destination) == "table" and yt.is_empty(destination)):
                yt.remove(destination)
        if yt.exists(destination) and args.ignore:
            return
        yt.create_table(destination, recursive=True)

        try:
            if args.import_type == "pull":
                pull_table(table, destination, count)
            elif args.import_type == "push":
                push_table(table, destination, count)
            else:
                raise yt.YtError("Incorrect import type: " + args.import_type)

            # TODO: add checksum checking
            if yt.records_count(destination) != count:
                raise yt.YtError("Incorrect record count: expected=%d, actual=%d" % (count, yt.records_count(destination)))

            if sorted:
                yt.run_sort(destination, sort_by=["key", "subkey"], spec={"parition_count": yt.get_attribute(destination, "chunk_count")})
        except yt.YtError:
            _, _, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, file=sys.stdout)
            yt.remove(destination, force=True)
        
        if args.erasure_codec is not None and args.erasure_codec == "none":
            args.erasure_codec = None

        if args.compression_codec is not None or args.erasure_codec is not None:
            mode = "sorted" if sorted else "unordered"
            spec = {"combine_chunks": "true",
                    "force_transform": "true"}

            if args.compression_codec is not None:
                yt.set_attribute(destination, "compression_codec", args.compression_codec)
            if args.erasure_codec is not None:
                yt.set_attribute(destination, "erasure_codec", args.erasure_codec)
                spec["job_io"] = {"table_writer": {"desired_chunk_size": 2 * 1024 ** 3}}
                spec["data_size_per_job"] = 4 * (1024 ** 3) / yt.get(destination + "/@compression_ratio")

            yt.run_merge(destination, destination, mode=mode, spec=spec)


    process_tasks_from_list(
        args.tables,
        import_table
    )

if __name__ == "__main__":
    main()
