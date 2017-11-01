#!/usr/bin/env python

import sys

if __name__ == "__main__":
    sum = [0, 0]
    for line in sys.stdin:
        current = map(int, line.split())
        for i in xrage(2):
            sum[i] += current[i]
    print "%d %d" % (sum[0], sum[1])


