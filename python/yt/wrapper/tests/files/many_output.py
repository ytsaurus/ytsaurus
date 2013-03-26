#!/usr/bin/env python

import os
import sys

if __name__ == "__main__":
    for line in sys.stdin:
        pass

    for i in xrange(15 + 1):
        value = str(i)
        rec = '\t'.join([value] * 3) + '\n'
        os.write(i * 3 + 1, rec)

