import time
import inspect

try:
    xrange
except NameError:  # Python 3
    xrange = range

try:
    from unittest.util import unorderable_list_difference
except ImportError:
    def unorderable_list_difference(expected, actual, ignore_duplicate=False):
        """Same behavior as sorted_list_difference but
        for lists of unorderable items (like dicts).

        As it does a linear search per item (remove) it
        has O(n*n) performance.
        """
        missing = []
        unexpected = []
        while expected:
            item = expected.pop()
            try:
                actual.remove(item)
            except ValueError:
                missing.append(item)
            if ignore_duplicate:
                for lst in expected, actual:
                    try:
                        while True:
                            lst.remove(item)
                    except ValueError:
                        pass
        if ignore_duplicate:
            while actual:
                item = actual.pop()
                unexpected.append(item)
                try:
                    while True:
                        actual.remove(item)
                except ValueError:
                    pass
            return missing, unexpected

        # anything left in actual is unexpected
        return missing, actual

try:
    from collections import Counter
except ImportError:
    def Counter(iterable):
        result = {}
        for item in iterable:
            result[item] = result.get(item, 0) + 1
        return result

def assert_items_equal(actual_seq, expected_seq):
    # It is simplified version of the same method of unittest.TestCase
    try:
        actual = Counter(iter(actual_seq))
        expected = Counter(iter(expected_seq))
    except TypeError:
        # Unhashable items (example: set(), list(), ...)
        actual = list(actual_seq)
        expected = list(expected_seq)
        missing, unexpected = unorderable_list_difference(expected, actual)
    else:
        if actual == expected:
            return
        missing = list(expected - actual)
        unexpected = list(actual - expected)

    assert not missing, "Expected, but missing:\n    %s" % repr(missing)
    assert not unexpected, "Unexpected, but present:\n    %s" % repr(unexpected)

def are_almost_equal(lhs, rhs, decimal_places=4):
    eps = 10**(-decimal_places)
    return abs(lhs - rhs) < eps

class WaitFailed(Exception):
    pass

def wait(predicate, error_message=None, iter=100, sleep_backoff=0.3, ignore_exceptions=False):
    for _ in xrange(iter):
        try:
            if predicate():
                return
        except:
            if ignore_exceptions:
                time.sleep(sleep_backoff)
                continue
            raise
        time.sleep(sleep_backoff)

    if inspect.isfunction(error_message):
        error_message = error_message()
    if error_message is None:
        error_message = "Wait failed"
    error_message += " (timeout = {0})".format(iter * sleep_backoff)
    raise WaitFailed(error_message)
