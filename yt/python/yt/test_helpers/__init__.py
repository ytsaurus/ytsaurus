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

from yt.common import wait, WaitFailed  # noqa


# A simplified version of the same method of unittest.TestCase
def _compute_items_difference(actual_seq, expected_seq):
    actual_list = list(actual_seq)
    expected_list = list(expected_seq)
    try:
        actual = Counter(actual_list)
        expected = Counter(expected_list)
        missing = list(expected - actual)
        unexpected = list(actual - expected)
    except TypeError:
        # Unhashable items (example: set(), list(), ...)
        missing, unexpected = unorderable_list_difference(expected_list, actual_list)
    return [missing, unexpected]


# A simplified version of the same method of unittest.TestCase
def assert_items_equal(actual_seq, expected_seq):
    missing, unexpected = _compute_items_difference(actual_seq, expected_seq)
    assert not missing and not unexpected, \
        "Expected, but missing:\n    %s\nUnexpected, but present:\n    %s" % (repr(missing), repr(unexpected))


# A checking counterpart of assert_items_equal.
def are_items_equal(actual_seq, expected_seq):
    missing, unexpected = _compute_items_difference(actual_seq, expected_seq)
    return missing == [] and unexpected == []


def are_almost_equal(lhs, rhs, absolute_error=None, relative_error=None):
    if lhs is None or rhs is None:
        return False
    if absolute_error is not None and relative_error is not None:
        assert False, "absolute_error and relative_error cannot be specified simultaneously"
    if relative_error is not None:
        diff = abs(lhs - rhs)
        magnitude = max(abs(lhs), abs(rhs))
        return diff < relative_error * magnitude
    else:
        if absolute_error is None:
            absolute_error = 10**(-4)
        return abs(lhs - rhs) < absolute_error


# TODO(ignat): move it to arcadia_interop.
def get_tmpfs_path():
    if yatest_common is not None and yatest_common.get_param("ram_drive_path") is not None:
        path = yatest_common.output_ram_drive_path()
        if not os.path.exists(path):
            os.makedirs(path)
        return path
    return None


def get_tests_sandbox(non_arcadia_path=None, arcadia_suffix="sandbox"):
    path = os.environ.get("YT_TESTS_SANDBOX")
    if path is None:
        if yatest_common is not None:
            if os.environ.get("YT_OUTPUT") is not None:
                path = os.environ.get("YT_OUTPUT")
            else:
                tmpfs_path = get_tmpfs_path()
                if tmpfs_path is None:
                    path = yatest_common.output_path()
                else:
                    path = tmpfs_path
                if arcadia_suffix is not None:
                    path = os.path.join(path, arcadia_suffix)
        else:
            assert non_arcadia_path is not None
            path = non_arcadia_path
    if not os.path.exists(path):
        try:
            os.mkdir(path)
        except OSError:  # Already exists.
            pass
    return path


def get_source_root():
    if yatest_common is not None:
        return yatest_common.source_path()
    else:
        return os.environ["SOURCE_ROOT"]


def get_build_root():
    if yatest_common is not None:
        return yatest_common.build_path()
    else:
        return os.environ["YT_BUILD_ROOT"]


def get_work_path():
    if yatest_common is not None:
        if yatest_common.get_param("ram_drive_path") is not None:
            return os.path.join(yatest_common.ram_drive_path())
        else:
            return os.path.join(yatest_common.work_path())
    else:
        return os.environ["YT_TESTS_SANDBOX"]


def prepare_yt_environment():
    from yt.environment import arcadia_interop

    if arcadia_interop.yatest_common is not None:
        arcadia_interop.configure_logging()

    destination = os.path.join(get_tests_sandbox(), "build")
    if not os.path.exists(destination):
        os.makedirs(destination)
    path = arcadia_interop.prepare_yt_environment(destination, binary_root=get_build_root())
    environment_paths = os.environ.get("PATH", "").split(os.pathsep)
    if path not in environment_paths:
        os.environ["PATH"] = os.pathsep.join([path, os.environ.get("PATH", "")])
