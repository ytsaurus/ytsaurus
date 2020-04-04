#!/usr/bin/env python

import yt.wrapper as yt

from itertools import imap

import sys

def parse(stream):
    for line in stream:
        if not line.strip():
            continue
        time, log_level, system, value = line.split("\t", 3)
        yield {"system": "scheduler", "time": time, "log_level": log_level, "subsystem": system, "value": value}

if __name__ == "__main__":
    yt.config.DEFAULT_FORMAT = yt.DsvFormat()

    table_name = sys.argv[1]
    records = parse(sys.stdin)
    lines = imap(yt.dumps_row, records)
    yt.write_table(table_name, lines)
