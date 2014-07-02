#!/usr/bin/env python

from yt.tools.atomic import process_tasks_from_list

import yt.wrapper as yt

import sys

def remove(table):
    if yt.exists(table):
        yt.remove(table, force=True)
    elif yt.exists(table + "&"):
        yt.remove(table + "&", force=True)
    else:
        return -1

if __name__ == "__main__":
    process_tasks_from_list(sys.argv[1], remove)


