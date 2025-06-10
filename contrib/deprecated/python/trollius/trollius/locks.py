"""Synchronization primitives."""

__all__ = ['Lock', 'Event', 'Condition', 'Semaphore', 'BoundedSemaphore']

import collections

from . import compat
from . import events
from . import futures
from .coroutines import coroutine, From, Return


class _ContextManager:
    """Context manager.

    This enables the following idiom for acquiring and releasing a
    lock around a block:

        with (yield From(lock)):
            <block>

    while failing loudly when accidentally using:

        with lock:
            <block>
    """

    def __init__(self, lock):
        self._lock = lock

    def __enter__(self):
        # We have no use for the "as ..."  clause in the with
        # statement for locks.
        return None

    def __exit__(self, *args):
        try:
            self._lock.release()
        finally:
            self._lock = None  # Crudely prevent reuse.


class _ContextManagerMixin(object):
    def __enter__(self):
        raise RuntimeError(
            '"yield From" should be used as context manager expression')

    def __exit__(self, *args):
        # This must exist because __enter__ exists, even though that
        # always raises; that's how the with-statement works.
        pass

    # FIXME: support PEP 492?
    # if compat.PY35:

    #     def __await__(self):
    #         # To make "with await lock" work.
    #         yield from self.acquire()
    #         return _ContextManager(self)

    #     @coroutine
    #     def __aenter__(self):
    #         yield from self.acquire()
    #         # We have no use for the "as ..."  clause in the with
    #         # statement for locks.
    #         return None

    #     @coroutine
    #     def __aexit__(self, exc_type, exc, tb):
    #         self.release()


class Lock(_ContextManagerMixin):
    """Primitive lock objects.

    A primitive lock is a synchronization primitive that is not owned
    by a particular coroutine when locked.  A primitive lock is in one
    of two states, 'locked' or 'unlocked'.

    It is created in the unlocked state.  It has two basic methods,
    acquire() and release().  When the state is unlocked, acquire()
    changes the state to locked and returns immediately.  When the
    state is locked, acquire() blocks until a call to release() in
    another coroutine changes it to unlocked, then the acquire() call
    resets it to locked and returns.  The release() method should only
    be called in the locked state; it changes the state to unlocked
    and returns immediately.  If an attempt is made to release an
    unlocked lock, a RuntimeError will be raised.

    When more than one coroutine is blocked in acquire() waiting for
    the state to turn to unlocked, only one coroutine proceeds when a
    release() call resets the state to unlocked; first coroutine which
    is blocked in acquire() is being processed.

    acquire() is a coroutine and should be called with 'yield From'.

    Locks also support the context management protocol.  '(yield From(lock))'
    should be used as context manager expression.

    Usage:

        lock = Lock()
        ...
        yield From(lock)
        try:
            ...
        finally:
            lock.release()

    Context manager usage:

        lock = Lock()
        ...
        with (yield From(lock)):
             ...

    Lock objects can be tested for locking state:

        if not lock.locked():
           yield From(lock)
        else:
           # lock is acquired
           ...

    """

    def __init__(self, loop=None):
        self._waiters = collections.deque()
        self._locked = False
        if loop is not None:
            self._loop = loop
        else:
            self._loop = events.get_event_loop()

    def __repr__(self):
        res = super(Lock, self).__repr__()
        extra = 'locked' if self._locked else 'unlocked'
        if self._waiters:
            extra = '{0},waiters:{1}'.format(extra, len(self._waiters))
        return '<{0} [{1}]>'.format(res[1:-1], extra)

    def locked(self):
        """Return True if lock is acquired."""
        return self._locked

    @coroutine
    def acquire(self):
        """Acquire a lock.

        This method blocks until the lock is unlocked, then sets it to
        locked and returns True.
        """
        if not self._waiters and not self._locked:
            self._locked = True
            raise Return(True)

        fut = futures.Future(loop=self._loop)
        self._waiters.append(fut)
        try:
            yield From(fut)
            self._locked = True
            raise Return(True)
        finally:
            self._waiters.remove(fut)

    def release(self):
        """Release a lock.

        When the lock is locked, reset it to unlocked, and return.
        If any other coroutines are blocked waiting for the lock to become
        unlocked, allow exactly one of them to proceed.

        When invoked on an unlocked lock, a RuntimeError is raised.

        There is no return value.
        """
        if self._locked:
            self._locked = False
            # Wake up the first waiter who isn't cancelled.
            for fut in self._waiters:
                if not fut.done():
                    fut.set_result(True)
                    break
        else:
            raise RuntimeError('Lock is not acquired.')


class Event(object):
    """Asynchronous equivalent to threading.Event.

    Class implementing event objects. An event manages a flag that can be set
    to true with the set() method and reset to false with the clear() method.
    The wait() method blocks until the flag is true. The flag is initially
    false.
    """

    def __init__(self, loop=None):
        self._waiters = collections.deque()
        self._value = False
        if loop is not None:
            self._loop = loop
        else:
            self._loop = events.get_event_loop()

    def __repr__(self):
        res = super(Event, self).__repr__()
        extra = 'set' if self._value else 'unset'
        if self._waiters:
            extra = '{0},waiters:{1}'.format(extra, len(self._waiters))
        return '<{0} [{1}]>'.format(res[1:-1], extra)

    def is_set(self):
        """Return True if and only if the internal flag is true."""
        return self._value

    def set(self):
        """Set the internal flag to true. All coroutines waiting for it to
        become true are awakened. Coroutine that call wait() once the flag is
        true will not block at all.
        """
        if not self._value:
            self._value = True

            for fut in self._waiters:
                if not fut.done():
                    fut.set_result(True)

    def clear(self):
        """Reset the internal flag to false. Subsequently, coroutines calling
        wait() will block until set() is called to set the internal flag
        to true again."""
        self._value = False

    @coroutine
    def wait(self):
        """Block until the internal flag is true.

        If the internal flag is true on entry, return True
        immediately.  Otherwise, block until another coroutine calls
        set() to set the flag to true, then return True.
        """
        if self._value:
            raise Return(True)

        fut = futures.Future(loop=self._loop)
        self._waiters.append(fut)
        try:
            yield From(fut)
            raise Return(True)
        finally:
            self._waiters.remove(fut)


class Condition(_ContextManagerMixin):
    """Asynchronous equivalent to threading.Condition.

    This class implements condition variable objects. A condition variable
    allows one or more coroutines to wait until they are notified by another
    coroutine.

    A new Lock object is created and used as the underlying lock.
    """

    def __init__(self, lock=None, loop=None):
        if loop is not None:
            self._loop = loop
        else:
            self._loop = events.get_event_loop()

        if lock is None:
            lock = Lock(loop=self._loop)
        elif lock._loop is not self._loop:
            raise ValueError("loop argument must agree with lock")

        self._lock = lock
        # Export the lock's locked(), acquire() and release() methods.
        self.locked = lock.locked
        self.acquire = lock.acquire
        self.release = lock.release

        self._waiters = collections.deque()

    def __repr__(self):
        res = super(Condition, self).__repr__()
        extra = 'locked' if self.locked() else 'unlocked'
        if self._waiters:
            extra = '{0},waiters:{1}'.format(extra, len(self._waiters))
        return '<{0} [{1}]>'.format(res[1:-1], extra)

    @coroutine
    def wait(self):
        """Wait until notified.

        If the calling coroutine has not acquired the lock when this
        method is called, a RuntimeError is raised.

        This method releases the underlying lock, and then blocks
        until it is awakened by a notify() or notify_all() call for
        the same condition variable in another coroutine.  Once
        awakened, it re-acquires the lock and returns True.
        """
        if not self.locked():
            raise RuntimeError('cannot wait on un-acquired lock')

        self.release()
        try:
            fut = futures.Future(loop=self._loop)
            self._waiters.append(fut)
            try:
                yield From(fut)
                raise Return(True)
            finally:
                self._waiters.remove(fut)

        except Exception as exc:
            # Workaround CPython bug #23353: using yield/yield-from in an
            # except block of a generator doesn't clear properly
            # sys.exc_info()
            err = exc
        else:
            err = None

        if err is not None:
            yield From(self.acquire())
            raise err

        yield From(self.acquire())

    @coroutine
    def wait_for(self, predicate):
        """Wait until a predicate becomes true.

        The predicate should be a callable which result will be
        interpreted as a boolean value.  The final predicate value is
        the return value.
        """
        result = predicate()
        while not result:
            yield From(self.wait())
            result = predicate()
        raise Return(result)

    def notify(self, n=1):
        """By default, wake up one coroutine waiting on this condition, if any.
        If the calling coroutine has not acquired the lock when this method
        is called, a RuntimeError is raised.

        This method wakes up at most n of the coroutines waiting for the
        condition variable; it is a no-op if no coroutines are waiting.

        Note: an awakened coroutine does not actually return from its
        wait() call until it can reacquire the lock. Since notify() does
        not release the lock, its caller should.
        """
        if not self.locked():
            raise RuntimeError('cannot notify on un-acquired lock')

        idx = 0
        for fut in self._waiters:
            if idx >= n:
                break

            if not fut.done():
                idx += 1
                fut.set_result(False)

    def notify_all(self):
        """Wake up all threads waiting on this condition. This method acts
        like notify(), but wakes up all waiting threads instead of one. If the
        calling thread has not acquired the lock when this method is called,
        a RuntimeError is raised.
        """
        self.notify(len(self._waiters))


class Semaphore(_ContextManagerMixin):
    """A Semaphore implementation.

    A semaphore manages an internal counter which is decremented by each
    acquire() call and incremented by each release() call. The counter
    can never go below zero; when acquire() finds that it is zero, it blocks,
    waiting until some other thread calls release().

    Semaphores also support the context management protocol.

    The optional argument gives the initial value for the internal
    counter; it defaults to 1. If the value given is less than 0,
    ValueError is raised.
    """

    def __init__(self, value=1, loop=None):
        if value < 0:
            raise ValueError("Semaphore initial value must be >= 0")
        self._value = value
        self._waiters = collections.deque()
        if loop is not None:
            self._loop = loop
        else:
            self._loop = events.get_event_loop()

    def __repr__(self):
        res = super(Semaphore, self).__repr__()
        extra = 'locked' if self.locked() else 'unlocked,value:{0}'.format(
            self._value)
        if self._waiters:
            extra = '{0},waiters:{1}'.format(extra, len(self._waiters))
        return '<{0} [{1}]>'.format(res[1:-1], extra)

    def locked(self):
        """Returns True if semaphore can not be acquired immediately."""
        return self._value == 0

    @coroutine
    def acquire(self):
        """Acquire a semaphore.

        If the internal counter is larger than zero on entry,
        decrement it by one and return True immediately.  If it is
        zero on entry, block, waiting until some other coroutine has
        called release() to make it larger than 0, and then return
        True.
        """
        if not self._waiters and self._value > 0:
            self._value -= 1
            raise Return(True)

        fut = futures.Future(loop=self._loop)
        self._waiters.append(fut)
        try:
            yield From(fut)
            self._value -= 1
            raise Return(True)
        finally:
            self._waiters.remove(fut)

    def release(self):
        """Release a semaphore, incrementing the internal counter by one.
        When it was zero on entry and another coroutine is waiting for it to
        become larger than zero again, wake up that coroutine.
        """
        self._value += 1
        for waiter in self._waiters:
            if not waiter.done():
                waiter.set_result(True)
                break


class BoundedSemaphore(Semaphore):
    """A bounded semaphore implementation.

    This raises ValueError in release() if it would increase the value
    above the initial value.
    """

    def __init__(self, value=1, loop=None):
        self._bound_value = value
        super(BoundedSemaphore, self).__init__(value, loop=loop)

    def release(self):
        if self._value >= self._bound_value:
            raise ValueError('BoundedSemaphore released too many times')
        super(BoundedSemaphore, self).release()
