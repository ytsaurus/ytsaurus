#!/usr/bin/env python

import sys

if __name__ == "__main__":
    for line in sys.stdin:
        if line.startswith("\1"):
            sys.stdout.write(line[1:])
        else:
            for word in line.split():
                sys.stdout.write("%s\tx\t1\n" % word)

