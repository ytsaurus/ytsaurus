#!/usr/bin/env python

from helpers import record_to_line, line_to_record, python_map, Record

from time import sleep
import sys

try:
    from itertools import imap
except ImportError:  # Python 3
    imap = map

def func(rec):
    for i in [0, 1]:
        yield Record(rec.key + str(i), rec.subkey, rec.value + str(10 + i))

if __name__ == "__main__":
    if len(sys.argv) > 1:
        sleep(float(sys.argv[1]))

    for rec in python_map(func, imap(line_to_record, sys.stdin)):
        sys.stdout.write(record_to_line(rec))

