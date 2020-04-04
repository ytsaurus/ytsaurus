#!/usr/bin/env python

import yt.wrapper as yt

import sys
import random
from time import sleep

if __name__ == "__main__":
    retries_count=10
    delay=5.0
    path = sys.argv[1]
    with yt.Transaction():
        for i in xrange(retries_count):
            try:
                print >>sys.stderr, "Trying to take lock, %d-th attempt..." % (i + 1)
                yt.lock(path)
                yt.set(path, sorted(list(set(yt.get(path)))))
                break
            except yt.YtResponseError as e:
                print >>sys.stderr, "Error", e
                print >>sys.stderr, "Cannot take lock, waiting for %f second..." % delay
                sleep(random.uniform(0.1, delay))

