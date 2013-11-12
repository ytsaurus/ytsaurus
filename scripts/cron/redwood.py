#!/usr/bin/env python

import yt.wrapper as yt

import os
import argparse
from datetime import date, timedelta

def process_logs(import_list, remove_list, link_queue, log_path, log_name, subnames, days, make_link):
    if subnames is None:
        subnames = [None]

    if days is None:
        count = 7
    else:
        count = days + 7

    for i in xrange(count):
        for subname in subnames:
            table_date = date.today() - timedelta(days=i)

            table_name = os.path.join(log_name, table_date.strftime("%Y%m%d"))
            link_table_name = os.path.join(log_name, table_date.strftime("%Y-%m-%d"))
            if subname is not None:
                table_name = os.path.join(table_name, subname)
                link_table_name = os.path.join(link_table_name, subname)
            table_path = os.path.join(log_path, table_name)
            link_table_path = os.path.join(log_path, link_table_name)
            if days is not None and i >= days: # Remove case
                if table_name in remove_list:
                    continue
                elif yt.exists(table_path):
                    if make_link:
                        remove_list.append(link_table_path)
                    remove_list.append(table_path)
            else: # Import case
                if table_name in import_list:
                    continue
                elif not yt.exists(table_path):
                    import_list.append(table_name)
                    if make_link:
                        link_queue.append({"src": table_path, "dst": link_table_path})

def main():
    parser = argparse.ArgumentParser(description='Prepare tables to merge')
    parser.add_argument('--path', required=True)
    parser.add_argument('--import-queue', required=True)
    parser.add_argument('--remove-queue', required=True)
    parser.add_argument('--link-queue', required=True)
    args = parser.parse_args()

    tables_to_import = yt.get(args.import_queue)
    tables_to_remove = yt.get(args.remove_queue)
    link_queue = yt.get(args.link_queue)
    process_logs(tables_to_import, tables_to_remove, link_queue, args.path, "user_sessions",      None,           60,   True)
    process_logs(tables_to_import, tables_to_remove, link_queue, args.path, "user_intents",       None,           None, False)
    process_logs(tables_to_import, tables_to_remove, link_queue, args.path, "reqregscdata",       ["www", "xml"], None, False)

    for i in xrange(9, 16):
        date_str = date(2013, 9, i).strftime("%Y%m%d")
        date_str_dash = date(2013, 9, i).strftime("%Y-%m-%d")
        for name in [date_str, date_str_dash]:
            key = os.path.join(args.path, "user_sessions", name)
            if key in tables_to_remove:
                tables_to_remove.remove(key)

    yt.set(args.import_queue, list(tables_to_import))
    yt.set(args.remove_queue, list(tables_to_remove))
    yt.set(args.link_queue, link_queue)

if __name__ == "__main__":
    main()
