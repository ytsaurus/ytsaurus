#!/usr/bin/env python

import sys

if __name__ == "__main__":
    number = {1: 0, 2: 0}
    for line in sys.stdin:
        count = 1 if line.find(" ") == -1 else 2
        number[count] += 1
    print "\t%d %d" %(number[1], number[2])


