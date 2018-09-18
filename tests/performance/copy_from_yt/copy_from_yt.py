#!/usr/bin/env python

import yt
import config
from common import flatten

import sys
import subprocess

config.DEFAULT_FORMAT=yt.DsvFormat()

def merge_tables(tables, destination):
    # It is impossible to merge in one command because of the large number of the tables
    cur = 0
    first = True
    while True:
        next = min(cur + 100, len(tables))
        if cur == next:
            break
        cur_tables = tables[cur:next]

        dst_option = "-dst" if first else "-dstappend"
        command = '../mapreduce -server "n01-0449g.yt.yandex.net:8013" -copy %s %s ' % \
                    (dst_option, destination) + \
                  ' '.join(flatten(zip(["-src"] * len(cur_tables), cur_tables)))
        print command

        proc = subprocess.Popen(command, shell=True)
        proc.communicate()

        first = False
        cur = next

source = sys.argv[1]
destination = sys.argv[2]

# Store names of tables in mapreduce
stat_tables = "//home/ignat/stat_tables"
yt.run_map('./prepare.py | ./writer.py', source, stat_tables,
           files=["../mapreduce", "writer.py", "prepare.py"])

merge_tables([rec["table"] for rec in map(yt.line_to_record, yt.read_table(stat_tables))],
             destination)

