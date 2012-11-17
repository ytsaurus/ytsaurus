#!/usr/bin/env python

import sys
from itertools import imap
from functools import partial

def capitilizeB(rec):
    if "b" in rec: rec["b"] = rec["b"].upper()
    return rec

""" Methods for records convertion """
def record_to_line(rec, eoln=True):
    body = "\t".join("=".join(map(str, item)) for item in rec.iteritems())
    return "%s%s" % (body, "\n" if eoln else "")

def line_to_record(line):
    return dict(field.split("=", 1) for field in line.strip("\n").split("\t"))

if __name__ == "__main__":
    lines = sys.stdin.readlines()
    print >>sys.stderr, lines
    recs = map(partial(line_to_record), lines)
    print >>sys.stderr, recs
    sys.stdout.writelines(imap(partial(record_to_line), imap(capitilizeB, recs)))
    
