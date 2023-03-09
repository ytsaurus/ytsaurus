from __future__ import print_function

import warnings

import yt.wrapper as yt

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

try:
    from yt.packages.six import iteritems, text_type
    from yt.packages.six.moves import map as imap
except ImportError:
    from six import iteritems, text_type
    from six.moves import map as imap

import pytest
from contextlib import contextmanager


def authors(*the_authors):
    # pytest perform test collection before processing all pytest_configure calls.
    warnings.filterwarnings("ignore", category=pytest.PytestUnknownMarkWarning)
    return pytest.mark.authors(the_authors)


def check_rows_equality(rowsA, rowsB, ordered=True):
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
