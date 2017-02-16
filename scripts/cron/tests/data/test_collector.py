#!/usr/bin/python2

import yt.wrapper as yt

import argparse
import sys

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--tasks-root", required=True)
    args = parser.parse_args()

    alive_worker_ids = yt.get_attribute(args.tasks_root, "alive_workers")
    for wid in alive_worker_ids:
        yt.set(yt.ypath_join(args.tasks_root, wid), ["//tmp/wtable"])
        print >>sys.stderr, "Set tasks for worker", wid

    print >>sys.stderr, "Test collector finished"
