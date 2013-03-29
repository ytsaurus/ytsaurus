#!/usr/bin/env python

from atomic import process_tasks_from_list

import yt.wrapper as yt

def remove(table):
    if yt.exists(table):
        yt.remove(table, force=True)
    else:
        return -1

if __name__ == "__main__":
    process_tasks_from_list("//home/ignat/to_remove", remove)


