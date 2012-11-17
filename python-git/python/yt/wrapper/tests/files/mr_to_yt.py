#!/usr/bin/env python
import sys

from helpers import line_to_yt

if __name__ == "__main__":
    for line in sys.stdin:
        print line_to_yt(line)

