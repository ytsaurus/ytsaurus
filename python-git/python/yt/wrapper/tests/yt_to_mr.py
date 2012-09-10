#!/usr/bin/env python
import sys

from helpers import yt_to_line

if __name__ == "__main__":
    for line in sys.stdin:
        sys.stdout.write(yt_to_line(line))

