import pytest


def get_timeout_mark(item):
    for mark in item.iter_markers():
        if mark.name == "timeout":
            return mark
    return None


def pytest_collection_modifyitems(items, config):
    for item in items:
        if get_timeout_mark(item) is None:
            item.add_marker(pytest.mark.timeout(90))
