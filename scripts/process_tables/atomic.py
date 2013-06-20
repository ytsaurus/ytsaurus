import yt.wrapper as yt

import __builtin__

import sys
import random
import logging
import traceback
from time import sleep

logger = logging.getLogger("Cron")
logger.setLevel(level="INFO")

formatter = logging.Formatter('%(asctime)-15s: %(message)s')
logger.addHandler(logging.StreamHandler())
logger.handlers[0].setFormatter(formatter)

def atomic_pop(list, retries_count=10, delay=5.0):
    with yt.Transaction():
        for i in xrange(retries_count):
            logger.info("Trying to take lock, %d-th attempt...", i + 1)
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
                logger.error("Error %s", str(e))
                logger.info("Cannot take lock, waiting for %f second...", delay)
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
                logger.info("List %s is empty, processing stopped", list)
                break

            hashable_value = None
            if isinstance(value, __builtin__.list):
                hashable_value = tuple(value)
            elif isinstance(value, __builtin__.dict):
                hashable_value = tuple(value.items())

            if hashable_value is not None and is_hashable(hashable_value):
                if hashable_value in processed_values:
                    logger.info("We have already prosessed value {0}, "
                                "it put back to queue and processed is stopped".format(str(value)))
                    atomic_push(list, value)
                    break
                processed_values.add(value)

            logger.info("Processing value %s", str(value))
            result = action(value)
            if result == -1:
                logger.warning("Action can not be done. "
                               "Put value %s back to the queue", str(value))
                atomic_push(list, value)

        except (Exception, KeyboardInterrupt) as e:
            logger.error("Crashed with error %s", str(e))
            _, _, exc_traceback = sys.exc_info()
            traceback.print_tb(exc_traceback, file=sys.stdout)
            if value is not None:
                logger.info("Put value %s back to the queue", str(value))
                atomic_push(list, value)
            break

