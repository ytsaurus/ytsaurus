#!/usr/bin/env python

from atomic import process_tasks_from_list

import yt.wrapper as yt

import os
import sh
from argparse import ArgumentParser

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--destination", default="//home/copy_test")
    parser.add_argument("--server", default="redwood.yandex.ru")
    parser.add_argument("--server-port", default="8013")
    parser.add_argument("--http-port", default="13013")
    parser.add_argument("--record-threshold", type=int, default=10 ** 7)
    parser.add_argument("--force", action="store_true", default=False)
    parser.add_argument("--fastbone", action="store_true", default=False)
    
    args = parser.parse_args()

    use_fastbone = "-opt net_table=fastbone" if args.fastbone else ""

    def records_count(table):
        """ Parse record count from the url """ 
        http_content = sh.curl("{}:{}/debug?info=table&table={}".format(args.server, args.http_port, table))
        records_line = filter(lambda line: line.find("Records") != -1,  http_content.split("\n"))[0]
        records_line = records_line.replace("</b>", "").replace(",", "")
        return int(records_line.split("Records:")[1].split()[0])
    
    def import_table(table):
        temp_table = yt.create_temp_table(prefix=os.path.basename(table))
        count = records_count(table)
        ranges = [(i * args.record_threshold, min(count, (i + 1) * args.record_threshold))
                  for i in xrange((count - 1) / args.record_threshold + 1)]
        yt.write_table(temp_table,
                       ["\t".join(map(str, range)) + "\n" for range in ranges],
                       format=yt.YamrFormat(lenval=False, has_subkey=False))

        destination = os.path.join(args.destination, table)
        if args.force:
            yt.remove(destination)
        yt.create_table(destination, recursive=True)
        yt.run_map(
                'while true; do '
                    'IFS="\t" read -r start end; '
                    'if [ "$?" != "0" ]; then break; fi; '
                    './mapreduce -server {}:{} {} -read {}:[$start,$end] -lenval -subkey; '
                'done;'.\
                    format(args.server, args.server_port, use_fastbone, table),
                temp_table,
                destination,
                input_format=yt.YamrFormat(lenval=False, has_subkey=False),
                output_format=yt.YamrFormat(lenval=True, has_subkey=True),
                files="mapreduce",
                spec={"job_count": len(ranges),
                      # To force job count
                      "min_data_size_per_job": 1})

        # TODO: add checksum checking 
        if yt.records_count(destination) != count:
            raise yt.YtError("Incorrect record count: expected=%d, actual=%d" % (count, yt.records_count(destination)))

    process_tasks_from_list(
        "//home/ignat/tables_to_import", 
        import_table
    )
    
