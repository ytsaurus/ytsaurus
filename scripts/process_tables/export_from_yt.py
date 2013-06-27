#!/usr/bin/env python

from atomic import process_tasks_from_list

import yt.wrapper as yt

import sh
import sys
import subprocess

from argparse import ArgumentParser

yt.config.MEMORY_LIMIT = 2500 * yt.config.MB
yt.config.set_proxy("proxy.yt.yandex.net")

POOL_RESTRICTION = 10

def main():
    parser = ArgumentParser()
    parser.add_argument("--tables")
    parser.add_argument("--server")
    parser.add_argument("--import-type", default="pull")
    parser.add_argument("--proxy", action="append")
    parser.add_argument("--server-port", default="8013")
    parser.add_argument("--http-port", default="13013")
    parser.add_argument("--speed", type=int, default=500)
    parser.add_argument("--force", action="store_true", default=False)
    parser.add_argument("--fastbone", action="store_true", default=False)
    parser.add_argument("--mapreduce-binary", default="./mapreduce")
    parser.add_argument("--mr-user", default="tmp")

    args = parser.parse_args()
    
    server = args.server
    def records_count(table):
        """ Parse record count from the html """
        http_content = sh.curl("{}:{}/debug?info=table&table={}".format(server, args.http_port, table)).stdout
        records_line = filter(lambda line: line.find("Records") != -1,  http_content.split("\n"))[0]
        records_line = records_line.replace("</b>", "").replace(",", "")
        return int(records_line.split("Records:")[1].split()[0])

    def is_sorted(table):
        """ Parse sorted from the html """
        http_content = sh.curl("{}:{}/debug?info=table&table={}".format(server, args.http_port, table)).stdout
        sorted_line = filter(lambda line: line.find("Sorted") != -1,  http_content.split("\n"))[0]
        sorted_line = sorted_line.replace("</b>", "")
        return sorted_line.split("Sorted:")[1].strip().lower() == "yes"

    def is_empty(table):
        """ Parse whether table is empty from html """
        http_content = sh.curl("{}:{}/debug?info=table&table={}".format(server, args.http_port, table)).stdout
        empty_lines = filter(lambda line: line.find("is empty") != -1,  http_content.split("\n"))
        return empty_lines and empty_lines[0].startswith("Table is empty")
    

    def export_table(obj):
        global server
        mr_table = None
        server = args.server
        if isinstance(obj, dict):
            yt_table = obj["src"]
            mr_table = obj["dst"]
            args.server = obj.get("mr_server", args.server)
            args.proxy = obj.get("mr_proxies", args.proxy)
            args.server_port = obj.get("server_port", args.server_port)
            args.http = obj.get("http_port", args.http_port)
        else:
            yt_table = obj
            mr_table = obj.strip("/")
        
        if not yt.exists(yt_table):
            print >>sys.stderr, "Table %s is absent" % yt_table
            return

        if not is_empty(mr_table):
            if not args.force:
                raise yt.YtError("Destination table %s is not empty" % mr_table)
            else:
                subprocess.check_call("{} -server {}:{} -drop {}".format(
                        args.mapreduce_binary,
                        server,
                        args.server_port,
                        mr_table),
                   shell=True) 
        
        count = yt.records_count(yt_table)
        limit = args.speed * yt.config.MB / POOL_RESTRICTION
        yt.run_map(
            "pv -q -L {} | USER=tmp MR_USER={} "
            "{} -server {}:{} -append -lenval -subkey "
            "-write {}".format(
                limit,
                args.mr_user,
                args.mapreduce_binary,
                server,
                args.server_port,
                mr_table),
            yt_table,
            "//tmp/null",
            files=args.mapreduce_binary,
            format=yt.YamrFormat(has_subkey=True, lenval=True),
            spec={
                "pool":"export_restricted",
                "data_size_per_job": 2 * 1024 * yt.config.MB})
        
        res_count = records_count(mr_table)
        if count != res_count:
            raise yt.YtError("Incorrect record count: expected=%d, actual=%d" % (count, res_count))


    process_tasks_from_list(
        args.tables,
        export_table
    )

if __name__ == "__main__":
    main()
