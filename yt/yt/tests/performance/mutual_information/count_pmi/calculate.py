#!/usr/bin/env python

import sys
import math

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print >>sys.stderr, "Usage: %s <words number> <couples number> <get_logarithm>"
        exit(1)

    words_number = int(sys.argv[1])
    couples_number = int(sys.argv[2])

    for line in sys.stdin:
        key, subkey, value = line.strip().split("\t")
        value = float(value)
        if subkey == "":
            pword = math.log(value) - math.log(words_number)
        else:
            if sys.argv[3] == "1":
                value = math.log(value)
            value -= math.log(couples_number ** 0.5) - pword
        if subkey != "":
            key, subkey = subkey, key
        sys.stdout.write("%s\t%s\t%.8g\n" % (key, subkey, value))

