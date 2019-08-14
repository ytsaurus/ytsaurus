import sys
import os

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
    if hasattr(item, "cls") and hasattr(item.cls, "Env"):
        rep.environment_path = item.cls.Env.path
    authors = _get_first_marker(item, name="authors")
    if authors is not None:
        rep.nodeid += " ({})".format(", ".join(authors))
    return rep

def pytest_itemcollected(item):
    authors = _get_first_marker(item, name="authors")
    if authors is None:
        raise RuntimeError("Test {} is not marked with @authors".format(item.nodeid))
