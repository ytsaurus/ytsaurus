#!/usr/bin/env python

import sys
from itertools import izip

if __name__ == "__main__":
    for line in sys.stdin:
        words = line.split()
        sys.stdout.write("".join("%s\t1\n" % word for word in words))
        sys.stdout.write("".join("%s %s\t1\n" % pair for pair in izip(words, words[1:])))

