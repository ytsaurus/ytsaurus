#!/usr/bin/env python

import sys

parts = sys.stdin.read().split("YT_TOKEN=")

for i in xrange(1, len(parts)):
    token, other = parts[i].split(" ", 1)
    parts[i] = " ".join(["x" * len(token), other])

sys.stdout.write("YT_TOKEN=".join(parts))
