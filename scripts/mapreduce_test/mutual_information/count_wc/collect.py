#!/usr/bin/env python

import sys
from itertools import imap, groupby

def parse_line(line):
    key, num = line.strip().split("\t")
    num = int(num)
    return (key, num)

def extract_key(record):
    return record[0]

def extract_value(record):
    return record[1]

if __name__ == "__main__":
    for key, records in groupby(imap(parse_line, sys.stdin), extract_key):
        sys.stdout.write("%s\t%d\n" % (key, sum(imap(extract_value, records))))

