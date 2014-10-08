#!/usr/bin/env python

import yt.wrapper as yt

import os
import argparse
import calendar
from datetime import timedelta, datetime

def assign_list(old, new):
    old[:] = []
    for elem in new:
        old.append(elem)

def process_logs(import_list, remove_list, destination_dir, source_pattern, destination_pattern, hours):
    def get_dst(elem):
        if isinstance(elem, dict):
            return elem["dst"]
        return elem

    if destination_pattern is None:
        destination_pattern = source_pattern

    for i in xrange(2 * hours):
        for minutes in xrange(0, 60, 30):
            now = datetime.utcnow()
            rounded_now = datetime(now.year, now.month, now.day, now.hour) - timedelta(hours=i, minutes=minutes)
            name = calendar.timegm(rounded_now.timetuple())
            
            src = source_pattern.format(name)
            dst = os.path.join(destination_dir, destination_pattern).format(name)
            if i >= hours: # Remove case
                if yt.exists(dst) and dst not in remove_list:
                    remove_list.append(dst)
                assign_list(import_list, [elem for elem in import_list if get_dst(elem) != dst])
            else: # Import case
                if dst in map(get_dst, import_list):
                    continue
                if not yt.exists(dst):
                    import_list.append({"src": src, "dst": dst, "mr_user": "userdata"})

def main():
    parser = argparse.ArgumentParser(description='Prepare tables to merge')
    parser.add_argument('--path', required=True)
    parser.add_argument('--import-queue', required=True)
    parser.add_argument('--remove-queue', required=True)
    parser.add_argument('--user-sessions-period', default=48, help='How many last hours of user sessions would be imported')
    args = parser.parse_args()

    tables_to_import = yt.get(args.import_queue)
    tables_to_remove = yt.get(args.remove_queue)

    def process(source, destination, days):
        process_logs(tables_to_import, tables_to_remove, args.path, source, destination, days)

    process("fast_logs/{}", None, args.user_sessions_period)

    print "\n".join(map(str, tables_to_import))
    print "\n".join(map(str, tables_to_remove))
    #yt.set(args.import_queue, list(tables_to_import))
    #yt.set(args.remove_queue, list(tables_to_remove))

if __name__ == "__main__":
    main()

