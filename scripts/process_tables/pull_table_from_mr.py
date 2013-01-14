#!/usr/bin/env python

from atomic import process_tasks_from_list

import yt.wrapper as yt

import os
import sh
from argparse import ArgumentParser

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
        records_line = filter(lambda line: line.find("Sorted") != -1,  http_content.split("\n"))[0]
        records_line = records_line.replace("</b>", "")
        return records_line.split("Sorted:")[1].strip().lower() == "yes"

    def import_table(table):
        temp_table = yt.create_temp_table(prefix=os.path.basename(table))
        count = records_count(table)
        sorted = is_sorted(table)

        servers = args.proxy if args.proxy is not None else ["%s:%s" % (args.server, args.server_port)]
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
        if args.force:
            yt.remove(destination)
        yt.create_table(destination, recursive=True)

        spec = {"min_data_size_per_job": 1}
        if args.job_count is not None:
            spec["job_count"] = args.job_count

        table_writer = None
        if args.codec is not None:
            table_writer["codec"] = args.codec

        yt.run_map(
                'while true; do '
                    'IFS="\t" read -r server start end; '
                    'if [ "$?" != "0" ]; then break; fi; '
                    './mapreduce -server $server {} -read {}:[$start,$end] -lenval -subkey; '
                'done;'.format(use_fastbone, table),
                temp_table,
                destination,
                input_format=yt.YamrFormat(lenval=False, has_subkey=False),
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
