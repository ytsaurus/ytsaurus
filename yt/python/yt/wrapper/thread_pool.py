from collections.abc import Iterable
from queue import Queue, Empty

import threading


class _ImapQueue(object):
    def __init__(self, maxsize=0):
        self._data = {}
        self._get_item_queue = set()
        self._maxsize = maxsize

        self._lock = threading.Lock()
        self._put_allowed = threading.Condition(self._lock)
        self._pop_allowed = threading.Condition(self._lock)

    def put(self, key, item):
        with self._put_allowed:
            if self._maxsize > 0:
                while len(self._data) >= self._maxsize and key not in self._get_item_queue:
                    self._put_allowed.wait()

            self._data[key] = item
            self._pop_allowed.notify_all()

    def get(self, block=True):
        with self._pop_allowed:
            if not block and not self._data:
                raise Empty()

            while not self._data:
                self._pop_allowed.wait()

            result = self._data.popitem()[1]
            self._put_allowed.notify()
            return result

    def get_item(self, key):
        with self._pop_allowed:
            try:
                self._get_item_queue.add(key)
                self._put_allowed.notify_all()

                while key not in self._data:
                    self._pop_allowed.wait()

                result = self._data.pop(key)

                self._put_allowed.notify()
                return result

            finally:
                self._get_item_queue.remove(key)

    def get_nowait(self):
        return self.get(block=False)

    def empty(self):
        with self._lock:
            result = not self._data
        return result


class _Worker(threading.Thread):
    RUNNING = 0
    TERMINATING = 1
    TERMINATED = 2

    def __init__(self, task_queue, result_queue, initfunc, initargs):
        super(_Worker, self).__init__()

        self._task_queue = task_queue
        self._result_queue = result_queue
        self._initfunc = initfunc
        self._initargs = initargs
        self._func = None

        self.state = self.RUNNING
        self.daemon = True
        self.start()

    def set_func(self, func):
        self._func = func

    def run(self):
        if self._initfunc:
            self._initfunc(*self._initargs)

        while True:
            if self.state == self.TERMINATING:
                break

            index, item = self._task_queue.get()

            if self.state == self.TERMINATING:
                break

            try:
                result = self._func(item), None
            except Exception as error:
                result = None, error

            self._result_queue.put(index, result)

        self.state = self.TERMINATED


class _ImapIterator(Iterable):
    FREE = 0
    BUSY = 1

    def __init__(self, result_queue, task_queue, task_iterator, ordered, pool):
        self._result_queue = result_queue
        self._task_queue = task_queue
        self._ordered = ordered
        self._state = self.BUSY
        self._pool = pool
        self._task_iterator = task_iterator

        self._task_count = 0
        self._generated_item_count = 0

        self._is_task_iterator_stopped = False

        for _ in range(self._pool.get_thread_count() - self._task_queue.qsize()):
            self.try_to_put_next_task()

    def try_to_put_next_task(self):
        if self._is_task_iterator_stopped:
            return

        try:
            task = next(self._task_iterator)
        except StopIteration:
            self._is_task_iterator_stopped = True
            return

        self._task_queue.put((self._task_count, task))
        self._task_count += 1

    def close(self):
        self._state = self.FREE
        if self._pool:
            self._pool._terminate_iterator()
            self._pool = None
            self._result_queue = None

    def __next__(self):
        is_pool_running = (self._state == self.BUSY and self._pool._state == self._pool.RUNNING)

        if self._generated_item_count >= self._task_count:
            self.close()
            raise StopIteration

        if not is_pool_running:
            raise RuntimeError("Pool is not running")

        if self._ordered:
            result, exception = self._result_queue.get_item(self._generated_item_count)
        else:
            result, exception = self._result_queue.get()

        self._generated_item_count += 1

        if exception is not None:
            raise exception

        if is_pool_running and not self._is_task_iterator_stopped:
            self.try_to_put_next_task()

        return result

    def __iter__(self):
        return self

    def __del__(self):
        self.close()


class ThreadPool(object):
    """Class representing a thread pool.

    This class provides two methods for parallel computing:
    imap(func, iterable) - an equivalent of itertools.imap().
    imap_unordered(func, iterable) - the same as imap but the ordering of results from
                                     the returned iterator of imap_unordered is arbitrary

    The result cache size is determined by the parameter max_queue_size.
    At the same time can not be running more than one imap.

    """
    RUNNING = 0
    TERMINATING = 1
    TERMINATED = 2

    def __init__(self, thread_count, initfunc=None, initargs=(), max_queue_size=0):
        if thread_count < 1:
            self._state = self.TERMINATED
            raise ValueError("Number of threads must be at least 1")

        self._task_queue = Queue()
        self._result_queue = _ImapQueue(max_queue_size)
        self._thread_count = thread_count

        self._workers = [_Worker(self._task_queue, self._result_queue, initfunc, initargs)
                         for _ in range(thread_count)]

        self._state = self.RUNNING
        self._iterable_state = _ImapIterator.FREE

    def _clear_queue(self, queue):
        while True:
            try:
                queue.get_nowait()
            except Empty:
                break

    def _clear_queues(self):
        self._clear_queue(self._task_queue)
        self._clear_queue(self._result_queue)

    def close(self):
        self._state = self.TERMINATING
        for worker in self._workers:
            worker.state = worker.TERMINATING

        main_thread = getattr(threading, "_main_thread", None)
        while True:
            # Main thread can be dead if __del__ called from Py_Finalize -> PyGC_Collect.
            # In this case all threads are 'blocked' and wait will be infinite.
            # More information in ticket YT-10041.
            if main_thread is not None and not main_thread.is_alive():
                break

            self._clear_queue(self._result_queue)
            self._task_queue.put((None, None))

            running_workers = 0
            for worker in self._workers:
                if worker.state != worker.TERMINATED:
                    running_workers += 1

            if not running_workers:
                break

        self._clear_queues()
        self._state = self.TERMINATED
        self._iterable_state = _ImapIterator.FREE

    def get_thread_count(self):
        return self._thread_count

    def __del__(self):
        if self._state == self.RUNNING:
            self.close()

    def join(self):
        for worker in self._workers:
            worker.join()

    def _terminate_iterator(self):
        self._iterable_state = _ImapIterator.FREE
        self._clear_queues()

    def _imap(self, func, iterable, ordered):
        if self._state != self.RUNNING:
            raise RuntimeError("Pool is closed")
        if self._iterable_state != _ImapIterator.FREE:
            raise RuntimeError("Cannot run more than one imap simultaneously")

        for worker in self._workers:
            worker.set_func(func)

        iterator = _ImapIterator(self._result_queue, self._task_queue, iter(iterable), ordered, self)
        self._iterable_state = _ImapIterator.BUSY
        return iterator

    def imap(self, func, iterable):
        return self._imap(func, iterable, True)

    def imap_unordered(self, func, iterable):
        return self._imap(func, iterable, False)
