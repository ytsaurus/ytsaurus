#!/usr/bin/env python

import sys

if __name__ == "__main__":
    for line in sys.stdin:
        key, value = line.strip().split("\t")
        words = key.split()
        second = "" if len(words) == 1 else " " + words[1]
        sys.stdout.write("%s\t%s\t%s\n" % (words[0], second, value))

