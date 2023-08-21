from yt.testlib import authors

from yt.wrapper.thread_pool import ThreadPool

try:
    from yt.packages.six.moves import xrange
except ImportError:
    from six.moves import xrange

import pytest
import threading
import time


@authors("ostyakov")
def test_thread_pool():
    n = threading.active_count()

    pool = ThreadPool(5)
    time.sleep(0.5)
    assert threading.active_count() == n + 5
    del pool

    time.sleep(0.5)
    assert threading.active_count() == n

    pool = ThreadPool(5)

    def func(x):
        if x == 0:
            raise ValueError
        return x

    result = pool.imap(func, [2, 0, 1])
    assert next(result) == 2

    with pytest.raises(ValueError):
        next(result)

    assert next(result) == 1
    with pytest.raises(StopIteration):
        next(result)

    result = pool.imap(lambda x: x, [2, 0, 1])
    next(result)
    pool.close()
    time.sleep(0.5)
    assert threading.active_count() == n

    with pytest.raises(RuntimeError):
        next(result)

    pool = ThreadPool(5)
    result = pool.imap(lambda x: x, [2, 0, 1])

    with pytest.raises(RuntimeError):
        result = pool.imap(lambda x: x, [2, 0, 1])

    result = None
    result = pool.imap(lambda x: x, [2, 0, 1])
    assert list(result) == [2, 0, 1]
    pool.close()
    time.sleep(0.5)
    assert threading.active_count() == n

    pool = ThreadPool(5)
    result = pool.imap(lambda x: x, [2, 0, 1])
    del pool
    del result

    pool = ThreadPool(5)
    result = pool.imap(lambda x: x, [2, 0, 1])
    del result
    del pool

    time.sleep(0.5)
    assert threading.active_count() == n

    with pytest.raises(ValueError):
        ThreadPool(0)


@authors("ostyakov")
def test_imap_unordered():
    pool = ThreadPool(5)

    result = pool.imap_unordered(lambda item: item ** 2, [0, 1, 5, 2, 3])
    assert set(result) == {0, 1, 25, 4, 9}

    result = pool.imap_unordered(lambda item: item ** 2, [6])
    assert set(result) == {36}

    result = pool.imap_unordered(lambda item: item ** 2, [])
    assert set(result) == set()

    pool = ThreadPool(1)

    result = pool.imap_unordered(lambda item: item * 2, [0, 5, 3])
    assert set(result) == {0, 10, 6}

    pool = ThreadPool(30)
    result = pool.imap_unordered(lambda item: -item, xrange(10 ** 5))
    assert set(result) == set(-item for item in xrange(10 ** 5))


@authors("ostyakov")
def test_imap():
    pool = ThreadPool(5)

    result = pool.imap(lambda item: item ** 2, [0, 1, 5, 2, 3])
    assert list(result) == [0, 1, 25, 4, 9]

    result = pool.imap(lambda item: item ** 2, [6])
    assert list(result) == [36]

    result = pool.imap(lambda item: item ** 2, [])
    assert list(result) == []

    with pytest.raises(ValueError):
        pool = ThreadPool(0)

    pool = ThreadPool(1)

    result = pool.imap(lambda item: item * 2, [0, 5, 3])
    assert list(result) == [0, 10, 6]

    pool = ThreadPool(30)
    result = pool.imap(lambda item: -item, xrange(10 ** 5))
    assert list(result) == [-item for item in xrange(10 ** 5)]
