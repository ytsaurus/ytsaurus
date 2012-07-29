#!/usr/bin/env python

from record import line_to_record, record_to_line
from format import DsvFormat
from common import partial

import sys
from itertools import imap

def capitilizeB(rec):
    if "b" in rec: rec["b"] = rec["b"].upper()
    return rec


if __name__ == "__main__":
    lines = sys.stdin.readlines()
    print >>sys.stderr, lines
    recs = map(partial(line_to_record, format=DsvFormat()), lines)
    print >>sys.stderr, recs
    sys.stdout.writelines(imap(partial(record_to_line, format=DsvFormat()), imap(capitilizeB, recs)))
    
