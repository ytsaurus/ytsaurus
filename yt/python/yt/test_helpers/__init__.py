try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

import os

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

from yt.common import wait, WaitFailed

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
    if lhs is None or rhs is None:
        return False
    eps = 10**(-decimal_places)
    return abs(lhs - rhs) < eps

# TODO(ignat): move it to arcadia_interop.
def get_tmpfs_path():
    if yatest_common is not None and yatest_common.get_param("ram_drive_path") is not None:
        path = yatest_common.output_ram_drive_path()
        if not os.path.exists(path):
            os.makedirs(path)
        return path
    return None

def get_tests_sandbox(non_arcadia_path):
    path = os.environ.get("TESTS_SANDBOX")
    if path is None:
        if yatest_common is not None:
            if os.environ.get("YT_OUTPUT") is not None:
                path = os.environ.get("YT_OUTPUT")
            else:
                tmpfs_path = get_tmpfs_path()
                if tmpfs_path is None:
                    path = os.path.join(yatest_common.output_path(), "sandbox")
                else:
                    path = os.path.join(tmpfs_path, "sandbox")
        else:
            path = non_arcadia_path
    if not os.path.exists(path):
        try:
            os.mkdir(path)
        except OSError:  # Already exists.
            pass
    return path
