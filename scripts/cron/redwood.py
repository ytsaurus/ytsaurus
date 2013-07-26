#!/usr/bin/env python

import yt.wrapper as yt

import os
import argparse
from datetime import date, timedelta

def process_logs(import_set, remove_set, log_path, log_name, subnames, days):
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
            if subname is not None:
                table_name = os.path.join(table_name, subname)
            table_path = os.path.join(log_path, table_name)
            if days is not None and i >= days: # Remove case
                if table_name in remove_set:
                    continue
                elif yt.exists(table_path):
                    remove_set.add(table_path)
            else: # Import case
                if table_name in import_set:
                    continue
                elif not yt.exists(table_path):
                    import_set.add(table_name)

def main():
    parser = argparse.ArgumentParser(description='Prepare tables to merge')
    parser.add_argument('--path')
    parser.add_argument('--import-queue')
    parser.add_argument('--remove-queue')
    args = parser.parse_args()
    
    tables_to_import = set(yt.get(args.import_queue))
    tables_to_remove = set(yt.get(args.remove_queue))
    process_logs(tables_to_import, tables_to_remove, args.path, "user_sessions",      None,           90)
    process_logs(tables_to_import, tables_to_remove, args.path, "user_intents",       None,           None)
    process_logs(tables_to_import, tables_to_remove, args.path, "reqregscdata",       ["www", "xml"], None)
    process_logs(tables_to_import, tables_to_remove, args.path, "mapsreqans_log_new", None,           None)

    yt.set(args.import_queue, list(tables_to_import))
    yt.set(args.remove_queue, list(tables_to_remove))

if __name__ == "__main__":
    main()
