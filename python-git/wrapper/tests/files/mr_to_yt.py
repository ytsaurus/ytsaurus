#!/usr/bin/env python

from __future__ import print_function

from helpers import line_to_yt

import sys

if __name__ == "__main__":
    for line in sys.stdin:
        print(line_to_yt(line))
