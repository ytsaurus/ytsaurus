
def pytest_configure(config):
    for line in [
        "authors(*authors): mark explicating test authors (owners)",
        "skip_if(condition)",
        "timeout(timeout)",
        "opensource",
    ]:
        config.addinivalue_line("markers", line)


def _get_first_marker(item, name):
    marker = None
    if hasattr(item, "get_closest_marker"):
        marker = item.get_closest_marker(name=name)
    else:
        marker = item.get_marker(name)
    return marker.args[0] if marker is not None else None


def pytest_collection_modifyitems(items, config):
    for item in items:
        authors = _get_first_marker(item, name="authors")
        if authors is not None:
            item._nodeid += " ({})".format(", ".join(authors))


def pytest_itemcollected(item):
    authors = _get_first_marker(item, name="authors")
    if authors is None:
        raise RuntimeError("Test {} is not marked with @authors".format(item._nodeid))
