import yt.wrapper as yt

import __builtin__

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

def is_hashable(obj):
    try:
        hash(obj)
        return True
    except:
        return False

def process_tasks_from_list(list, action):
    processed_values = set()
    while True:
        value = None
        try:
            value = atomic_pop(list)

            if value is None:
                print >>sys.stderr, "List %s is empty, processing stopped" % list
                break

            hashable_value = None
            if isinstance(value, __builtin__.list):
                hashable_value = tuple(value)
            elif isinstance(value, __builtin__.dict):
                hashable_value = tuple(value.items())

            if hashable_value is not None and is_hashable(hashable_value):
                if hashable_value in processed_values:
                    print >>sys.stderr, "We have already prosessed value %r, processing stopped." %value
                    print >>sys.stderr, "Put value %s back to the queue" % str(value)
                    atomic_push(list, value)
                    break
                processed_values.add(value)

            print >>sys.stderr, "Processing value", value
            result = action(value)
            if result == -1:
                print >>sys.stderr, "Action can not be done."
                print >>sys.stderr, "Put value %s back to the queue" % str(value)
                atomic_push(list, value)

        except (Exception, KeyboardInterrupt) as e:
            print >>sys.stderr, "Crashed with error", e
            _, _, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, file=sys.stdout)
            if value is not None:
                print >>sys.stderr, "Put value %s back to the queue" % str(value)
                atomic_push(list, value)
            break

