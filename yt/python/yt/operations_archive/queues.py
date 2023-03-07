import yt.logger as logger

from datetime import datetime, timedelta
from threading import Thread, Lock

from time import sleep
from collections import deque

class Timer(object):
    def __init__(self):
        self.elapsed = timedelta(0)
        self.start = None

    def __str__(self):
        return "{0:.3f}s".format(self.elapsed.total_seconds())

    def value(self):
        return self.elapsed.total_seconds()

    def __enter__(self):
        if self.start is not None:
            raise Exception("Recursive usage of class Timer")
        else:
            self.start = datetime.utcnow()

    def __exit__(self, type, value, traceback):
        self.elapsed += datetime.utcnow() - self.start
        self.start = None

class ThreadSafeCounter(object):
    def __init__(self, counter):
        self.counter = counter
        self.mutex = Lock()

    def add(self, name, count):
        with self.mutex:
            self.counter[name] += count

# Standard python Queues have thread blocking methods such as join.
class NonBlockingQueue(object):
    def __init__(self, iterable=None):
        self.data = deque(iterable) if iterable is not None else deque()
        self.executing_count = 0
        self.closed = False
        self.active = False
        self.mutex = Lock()

    def put(self, value):
        self.put_many([value])

    def put_many(self, values):
        with self.mutex:
            self.data.extend(values)

    def get(self, default=None):
        with self.mutex:
            if not self.data:
                return default
            else:
                self.executing_count += 1
                return self.data.popleft()

    def task_done(self, count=1):
        with self.mutex:
            if self.executing_count >= count:
                self.executing_count -= count
            else:
                raise ValueError("Extra task_done called")

    def __len__(self):
        return len(self.data)

    def close(self):
        self.closed = True

    def activate(self):
        self.active = True

    def is_finished(self):
        with self.mutex:
            return self.executing_count == 0 and len(self.data) == 0 and self.active

    def is_done(self):
        with self.mutex:
            return self.executing_count == 0 and len(self.data) == 0 and self.active or self.closed

    def clear(self):
        result = []
        with self.mutex:
            while len(self.data) > 0:
                result.append(self.data.pop())
        return result

def queue_worker(queue, worker_cls, args=()):
    worker = worker_cls(*args)
    while not queue.is_done():
        value = queue.get()
        if value is None:
            sleep(0.1)
        else:
            try:
                worker(value)
            except:
                logger.exception("Exception caught")
            finally:
                queue.task_done()

def batching_queue_worker(queue, worker_cls, args=(), batch_size=32, failed_items=None):
    worker = worker_cls(*args)
    while not queue.is_done():
        values = []
        while len(values) < batch_size:
            value = queue.get()
            if value is None:
                break
            else:
                values.append(value)
        if not values:
            sleep(0.1)
        else:
            try:
                worker(values[:])
            except:
                logger.exception("Exception caught")
                if failed_items is not None:
                    failed_items.extend(values[:])
            finally:
                queue.task_done(len(values))

def run_workers(worker, args, thread_count):
    if thread_count == 0:
        args[0].activate()
        worker(*args)
    else:
        for _ in range(thread_count):
            thread = Thread(target=worker, args=args)
            thread.daemon = True
            thread.start()

def run_queue_workers(queue, worker_cls, thread_count, args=()):
    run_workers(queue_worker, (queue, worker_cls, args), thread_count)

def run_batching_queue_workers(queue, worker_cls, thread_count, args=(), batch_size=32, failed_items=None):
    run_workers(batching_queue_worker, (queue, worker_cls, args, batch_size, failed_items), thread_count)

def wait_for_queue(queue, name, end_time=None, sleep_timeout=0.2):
    queue.activate()
    counter = 0
    result = []
    while True:
        if queue.is_finished():
            break
        else:
            sleep(sleep_timeout)
            if counter % max(int(1.0 / sleep_timeout), 1) == 0:
                logger.info(
                    "Waiting for processing items in queue '%s' (left items: %d, items in progress: %d, time left: %s)",
                    name,
                    len(queue),
                    queue.executing_count,
                    str(end_time - datetime.utcnow()) if end_time is not None else "inf")
        counter += 1
        if end_time is not None and datetime.utcnow() > end_time:
            logger.info("Waiting timeout is expired for queue '%s'", name)
            queue.close()
            result.extend(queue.clear())
    logger.info("Joined queue '%s'", name)
    result.extend(queue.clear())
    return result

