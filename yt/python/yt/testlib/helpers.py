from __future__ import print_function

from yt.test_helpers import get_tmpfs_path, wait, get_tests_sandbox as get_tests_sandbox_impl

from yt.packages.six import iteritems, text_type
from yt.packages.six.moves import map as imap
import yt.wrapper as yt

import os
import sys
import pytest
from contextlib import contextmanager

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

TEST_DIR = "//home/wrapper_tests"

# These variables are to be set by particular conftest.py by calling set_testsuite_details().
LOCAL_ABSPATH = None
ARCADIA_PATH = None

# These variables are intialized lazily by calling get_tests_location() or get_tests_sandbox().
# They require two variables above to be set beforehand by calling set_testsuite_details().
# Note that we can't initialize them too early because it may lead to 
# "NotImplementedError: yatest.common.* is only available from the testing runtime"
# error being thrown from yatest_common.
TESTS_LOCATION = None
TESTS_SANDBOX = None

def set_testsuite_details(local_abspath, arcadia_path): 
    global LOCAL_ABSPATH, ARCADIA_PATH

    assert LOCAL_ABSPATH is None and ARCADIA_PATH is None

    LOCAL_ABSPATH = local_abspath
    ARCADIA_PATH = arcadia_path

def init_tests_location():
    global TESTS_LOCATION, TESTS_SANDBOX, LOCAL_ABSPATH, ARCADIA_PATH

    assert TESTS_LOCATION is None and TESTS_SANDBOX is None

    print("Initializing tests location from testsuite details: local_abspath = {}, arcadia_path = {}".format(LOCAL_ABSPATH, ARCADIA_PATH), file=sys.stderr)

    try:
        TESTS_LOCATION = os.path.dirname(LOCAL_ABSPATH) if yatest_common is None else yatest_common.source_path(ARCADIA_PATH)
    except NotImplementedError:
        # In some cases calling yatest_common.source_path is illegal,
        # for example, this may happen during ya make -L. In this case
        # we will not need any related getter, so just skip the initialization.
        print("Tests location is not set; probably not running in test mode", file=sys.stderr)
        return

    TESTS_SANDBOX = os.environ.get("TESTS_SANDBOX", TESTS_LOCATION + ".sandbox")

    print("Tests location is set: tests_location = {}, tests_sandbox = {}" \
          .format(TESTS_LOCATION, TESTS_SANDBOX), file=sys.stderr)

def get_tests_location():
    if TESTS_LOCATION is None:
        init_tests_location()

    return TESTS_LOCATION

def get_tests_sandbox():
    if TESTS_SANDBOX is None:
        init_tests_location()

    return get_tests_sandbox_impl(TESTS_SANDBOX)

def get_port_locks_path():
    path = get_tmpfs_path()
    if path is None:
        path = get_tests_sandbox()
    return os.path.join(path, "ports")

def get_test_files_dir_path():
    return os.path.join(get_tests_location(), "files")

def authors(*the_authors):
    return pytest.mark.authors(the_authors)

# Check equality of records
def check(rowsA, rowsB, ordered=True):
    def prepare(rows):
        def fix_unicode(obj):
            if isinstance(obj, text_type):
                return str(obj)
            return obj
        def process_row(row):
            if isinstance(row, dict):
                return dict([(fix_unicode(key), fix_unicode(value)) for key, value in iteritems(row)])
            return row

        rows = list(imap(process_row, rows))
        if not ordered:
            rows = tuple(sorted(imap(lambda obj: tuple(sorted(iteritems(obj))), rows)))

        return rows

    lhs, rhs = prepare(rowsA), prepare(rowsB)
    assert lhs == rhs

@contextmanager
def set_config_option(name, value, final_action=None):
    old_value = yt.config._get(name)
    try:
        yt.config._set(name, value)
        yield
    finally:
        if final_action is not None:
            final_action()
        yt.config._set(name, old_value)

@contextmanager
def set_config_options(options_dict):
    old_values = {}
    for key in options_dict:
        old_values[key] = yt.config._get(key)
    try:
        for key, value in iteritems(options_dict):
            yt.config._set(key, value)
        yield
    finally:
        for key, value in iteritems(old_values):
            yt.config._set(key, value)
