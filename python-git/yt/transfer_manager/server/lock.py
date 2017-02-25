from threading import RLock
from contextlib import contextmanager

try:
    xrange
except NameError:  # Python 3
    xrange = range

class CountedRLock(object):
    def __init__(self):
        self._lock = RLock()
        self._counter = 0

    def is_acquired(self):
        return self._counter > 0

    def _acquire(self, blocking):
        if blocking:
            self._lock.acquire(blocking=1)
            self._counter += 1
        else:
            if self._lock.acquire(blocking=0):
                self._counter += 1
                return True
            else:
                return False

    def acquire(self, blocking=1, count=1):
        for i in xrange(count):
            self._acquire(blocking=blocking)

    def _release(self):
        self._counter -= 1
        self._lock.release()

    def release(self, count=1):
        for i in xrange(count):
            self._release()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, type, value, tb):
        self.release()

    def get_counter(self):
        return self._counter

    @contextmanager
    def discharge(self):
        try:
            acquired_count = self.get_counter()
            self.release(count=acquired_count)
            yield
        finally:
            self.acquire(count=acquired_count)
