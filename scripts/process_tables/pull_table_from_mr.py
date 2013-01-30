#!/usr/bin/env python

from atomic import process_tasks_from_list

import yt.wrapper as yt

import os
import sh
import sys
from argparse import ArgumentParser
from urllib import quote_plus

yt.config.MEMORY_LIMIT = 2000 * yt.config.MB

def main():
    parser = ArgumentParser()
    parser.add_argument("--tables")
    parser.add_argument("--destination")
    parser.add_argument("--server")
    parser.add_argument("--proxy", action="append")
    parser.add_argument("--server-port", default="8013")
    parser.add_argument("--http-port", default="13013")
    parser.add_argument("--record-threshold", type=int, default=5 * 10 ** 6)
    parser.add_argument("--job-count", type=int)
    parser.add_argument("--codec")
    parser.add_argument("--force", action="store_true", default=False)
    parser.add_argument("--fastbone", action="store_true", default=False)

    args = parser.parse_args()

    use_fastbone = "-opt net_table=fastbone" if args.fastbone else ""

    def records_count(table):
        """ Parse record count from the html """
        http_content = sh.curl("{}:{}/debug?info=table&table={}".format(args.server, args.http_port, table)).stdout
        records_line = filter(lambda line: line.find("Records") != -1,  http_content.split("\n"))[0]
        records_line = records_line.replace("</b>", "").replace(",", "")
        return int(records_line.split("Records:")[1].split()[0])

    def is_sorted(table):
        """ Parse sorted from the html """
        http_content = sh.curl("{}:{}/debug?info=table&table={}".format(args.server, args.http_port, table)).stdout
        sorted_line = filter(lambda line: line.find("Sorted") != -1,  http_content.split("\n"))[0]
        sorted_line = sorted_line.replace("</b>", "")
        return sorted_line.split("Sorted:")[1].strip().lower() == "yes"

    def is_empty(table):
        """ Parse whether table is empty from html """
        http_content = sh.curl("{}:{}/debug?info=table&table={}".format(args.server, args.http_port, table)).stdout
        empty_lines = filter(lambda line: line.find("is empty") != -1,  http_content.split("\n"))
        return empty_lines and empty_lines[0].startswith("Table is empty")

    def import_table(table):
        if is_empty(table):
            print >>sys.stderr, "Table {} is empty".format(table)
            return

        temp_table = yt.create_temp_table(prefix=os.path.basename(table))
        count = records_count(table)
        sorted = is_sorted(table)

        has_proxy = args.proxy is not None
        if has_proxy:
            servers = ["%s:%s" % (proxy, args.http_port) for proxy in args.proxy]
        else:
            servers = ["%s:%s" % (args.server, args.server_port)]
        ranges = []
        for i in xrange((count - 1) / args.record_threshold + 1):
            server = servers[i % len(servers)]
            start = i * args.record_threshold
            end = min(count, (i + 1) * args.record_threshold)
            ranges.append((server, start, end))

        yt.write_table(temp_table,
                       ["\t".join(map(str, range)) + "\n" for range in ranges],
                       format=yt.YamrFormat(lenval=False, has_subkey=True))

        destination = os.path.join(args.destination, table)
        if args.force and yt.exists(destination):
            yt.remove(destination)
        yt.create_table(destination, recursive=True)

        if args.job_count is None:
            args.job_count = len(ranges)
        spec = {"min_data_size_per_job": 1, "job_count": args.job_count, "pool": "restricted"}

        table_writer = None
        if args.codec is not None:
            table_writer["codec"] = args.codec

        if has_proxy:
            command = 'curl "http://${{server}}/table/{}?subkey=1&lenval=1&startindex=${{start}}&endindex=${{end}}"'.format(quote_plus(table))
        else:
            command = 'USER=yt MR_USER=tmp ./mapreduce -server $server {} -read {}:[$start,$end] -lenval -subkey'.format(use_fastbone, table)
        yt.run_map(
                'while true; do '
                    'IFS="\t" read -r server start end; '
                    'if [ "$?" != "0" ]; then break; fi; '
                    'set -e; '
                    'echo "{0}" 1>&2; '
                    '{0}; '
                    'set +e; '
                'done;'.format(command),
                temp_table,
                destination,
                input_format=yt.YamrFormat(lenval=False, has_subkey=True),
                output_format=yt.YamrFormat(lenval=True, has_subkey=True),
                files="mapreduce",
                table_writer=table_writer,
                spec=spec)

        # TODO: add checksum checking
        if yt.records_count(destination) != count:
            raise yt.YtError("Incorrect record count: expected=%d, actual=%d" % (count, yt.records_count(destination)))

        if sorted:
            yt.run_sort(destination, sort_by=["key", "subkey"])

    process_tasks_from_list(
        args.tables,
        import_table
    )

if __name__ == "__main__":
    main()
