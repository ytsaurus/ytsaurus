#!/usr/bin/env python

from yt.tools.atomic import process_tasks_from_list

import yt.wrapper as yt

import sys

def link(obj):
    src = obj["src"]
    dst = obj["dst"]
    if not yt.exists(src):
        return -1
    yt.link(src, dst)

if __name__ == "__main__":
    process_tasks_from_list(sys.argv[1], link)


