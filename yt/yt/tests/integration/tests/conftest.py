try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

from yt.test_helpers.authors import pytest_configure, pytest_collection_modifyitems, pytest_itemcollected  # noqa

if yatest_common is None:
    # You should have prepared python repo.
    pytest_plugins = "yt.test_runner.plugin"
