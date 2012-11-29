import yt.wrapper as yt

import sys
import random
from time import sleep

def atomic_pop(list, retries_count=10, delay=5.0):
    with yt.Transaction():
        for i in xrange(retries_count):
            print >>sys.stderr, "Trying to take lock, %d-th attempt..." % (i + 1)
            try:
                yt.lock(list)
                count = int(yt.get(list + "/@count"))
                if not count:
                    return
                value = yt.get(list + "/-1")
                yt.remove(list + "/-1", recursive=True)
                return value
            except yt.YtError as e:
                print >>sys.stderr, e
                print >>sys.stderr, "Cannot take lock, waiting for %f second..." % delay
                sleep(random.uniform(0.1, delay))


def atomic_push(list, value):
    yt.set(list + "/begin", value)

def process_tasks_from_list(list, action):
    while True:
        value = atomic_pop(list)
        if value is None:
            break
        try:
            print >>sys.stderr, "Processing value", value
            action(value)
        except yt.YtError:
            atomic_push(list, value)

