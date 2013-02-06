import yt.wrapper as yt

import sys
import random
import traceback
from time import sleep

def atomic_pop(list, retries_count=10, delay=5.0):
    with yt.Transaction():
        for i in xrange(retries_count):
            print >>sys.stderr, "Trying to take lock, %d-th attempt..." % (i + 1)
            try:
                count = int(yt.get(list + "/@count"))
                if not count:
                    return
                yt.lock(list + "/-1")
                value = yt.get(list + "/-1")
                yt.remove(list + "/-1", recursive=True)
                return value
            # We hope that it is cannot take lock error
            except yt.YtResponseError as e:
                print >>sys.stderr, "Error", e
                print >>sys.stderr, "Cannot take lock, waiting for %f second..." % delay
                sleep(random.uniform(0.1, delay))


def atomic_push(list, value):
    yt.set(list + "/begin", value)

def process_tasks_from_list(list, action):
    while True:
        value = None
        try:
            value = atomic_pop(list)
            if value is None:
                break
            print >>sys.stderr, "Processing value", value
            action(value)
        except (Exception, KeyboardInterrupt) as e:
            print >>sys.stderr, "Crashed with error", e
            _, _, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, file=sys.stdout)
            if value is not None:
                atomic_push(list, value)
            break

