#!/usr/bin/env python
from helpers import record_to_line, line_to_record, python_map, Record

from itertools import imap, chain
from time import sleep
import sys

def func(rec):
    for i in xrange(2):
        yield Record(rec.key + str(i), rec.subkey, rec.value + str(10 + i))

if __name__ == "__main__":
    if len(sys.argv) > 1:
        sleep(float(sys.argv[1]))

    for rec in python_map(func, imap(line_to_record, sys.stdin.readlines())):
        sys.stdout.write(record_to_line(rec))

