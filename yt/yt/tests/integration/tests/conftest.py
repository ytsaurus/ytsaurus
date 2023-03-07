import sys
import os

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

if yatest_common is None:
    sys.path.insert(0, os.path.abspath('../../../python'))
    sys.path.append(os.path.abspath('.'))

    pytest_plugins = "yt.test_runner.plugin"

def pytest_configure(config):
    for line in [
        "authors(*authors): mark explicating test authors (owners)",
        "skip_if(condition)",
        "timeout(timeout)",
    ]:
        config.addinivalue_line("markers", line)

def _get_first_marker(item, name):
    marker = None
    if hasattr(item, "get_closest_marker"):
        marker = item.get_closest_marker(name=name)
    else:
        marker = item.get_marker(name)
    return marker.args[0] if marker is not None else None

def pytest_runtest_makereport(item, call, __multicall__):
    rep = __multicall__.execute()
    if hasattr(item, "cls") and hasattr(item.cls, "Env") and item.cls.Env is not None:
        rep.environment_path = item.cls.Env.path
    return rep

def pytest_collection_modifyitems(items, config):
    for item in items:
        authors = _get_first_marker(item, name="authors")
        if authors is not None:
            item._nodeid += " ({})".format(", ".join(authors))

def pytest_itemcollected(item):
    authors = _get_first_marker(item, name="authors")
    if authors is None:
        raise RuntimeError("Test {} is not marked with @authors".format(item._nodeid))
