#!/usr/bin/env python

import os
import sys

if __name__ == "__main__":
    is_yamr_mode = sys.argv[1] == "yamr"

    for line in sys.stdin:
        pass

    for i in xrange(10 + 1):
        if is_yamr_mode:
            value = str(i)
            rec = "\t".join([value] * 3) + '\n'
            os.write(i + 3, rec)
        else:
            rec = "x={0}\ty={0}\n".format(i)
            os.write(i * 3 + 1, rec)

