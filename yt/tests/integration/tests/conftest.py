import sys
import os

sys.path.insert(0, os.path.abspath('../../../python'))
sys.path.append(os.path.abspath('.'))

pytest_plugins = "yt.test_runner.plugin"

def pytest_runtest_makereport(item, call, __multicall__):
    rep = __multicall__.execute()
    if hasattr(item, "cls") and hasattr(item.cls, "Env"):
        rep.environment_path = item.cls.Env.path
    return rep

def _get_closest_marker(item, name):
    if hasattr(item, "get_closest_marker"):
        return item.get_closest_marker(name=name)
    for item in reversed(item.listchain()):
        if item.get_marker(name) is not None:
            return item.get_marker
    return None

def pytest_itemcollected(item):
    authors = _get_closest_marker(item, name="authors")
    if authors is None:
        raise RuntimeError("Test {} is not marked with @authors".format(item.nodeid))
