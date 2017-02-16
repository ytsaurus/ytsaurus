#!/usr/bin/python2

import yt.wrapper as yt

import argparse
import time
import sys

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tasks-path", required=True)
    parser.add_argument("--id", required=True)
    args = parser.parse_args()

    while True:
        print >>sys.stderr, "Work work work, id", args.id

        try:
            table = yt.get(args.tasks_path)[0]
        except IndexError:
            continue

        yt.write_table(yt.TablePath(table, append=True), [{"wid": args.id}])
        break

    while True:
        time.sleep(0.5)

if __name__ == "__main__":
    main()
