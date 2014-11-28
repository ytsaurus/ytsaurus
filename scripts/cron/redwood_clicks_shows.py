#!/usr/bin/env python

from yt.tools.yamr import Yamr
import yt.wrapper as yt

import argparse
from datetime import datetime

def assign_list(old, new):
    old[:] = []
    for elem in new:
        old.append(elem)

def process_logs(import_list, remove_list, destination_pattern, yamr, prefix, suffix, table_count):
    def get_dst(elem):
        if isinstance(elem, dict):
            return elem["dst"]
        return elem

    def is_data_range(name):
        def is_date(date_str):
            try:
                datetime.strptime(date_str, "%Y%m%d")
                return True
            except Exception:
                return False

        name = name[len(prefix):]
        if not is_date(name[:8]):
            return False
        if name[8] != "_":
            return False
        if not is_date(name[9:17]):
            return False
        return True

    data_range_length = 17

    tables = [table["name"] for table in yamr.list(prefix)]
    tables = filter(lambda table: is_data_range(table), tables)
    tables = filter(lambda table: len(table) == len(prefix) + data_range_length + len(suffix),  tables)

    tables.sort(reverse=True)

    for i, table in enumerate(tables):
        src = table
        dst = destination_pattern.format(table[len(prefix):len(prefix) + data_range_length])
        if i < table_count: # Add case
            if dst in map(get_dst, import_list):
                continue
            if not yt.exists(dst):
                import_list.append({"src": src, "dst": dst})
        else: # Remove case
            if yt.exists(dst) and dst not in remove_list:
                remove_list.append(dst)
            assign_list(import_list, [elem for elem in import_list if get_dst(elem) != dst])

def main():
    parser = argparse.ArgumentParser(description='Prepare tables to merge')
    parser.add_argument('--import-queue', required=True)
    parser.add_argument('--remove-queue', required=True)
    parser.add_argument('--table-count', default=3)
    args = parser.parse_args()

    tables_to_import = yt.get(args.import_queue)
    tables_to_remove = yt.get(args.remove_queue)

    yamr = Yamr("/Berkanavt/bin/mapreduce", server="redwood00.search.yandex.net", server_port=8013, http_port=13013)

    for prefix in ["", "com.tr."]:
        process_logs(tables_to_import, tables_to_remove, "//userdata/clicks_shows/{}/filtered/web",
                     yamr, "clicks_shows/" + prefix, "/filtered/web", args.table_count)

    yt.set(args.import_queue, list(tables_to_import))
    yt.set(args.remove_queue, list(tables_to_remove))

if __name__ == "__main__":
    main()

