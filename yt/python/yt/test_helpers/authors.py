def pytest_configure(config):
    for line in [
        "authors(*authors): mark explicating test authors (owners)",
        "skip_if(condition)",
        "timeout(timeout)",
        "opensource",
        "ignore_in_opensource_ci",
        "enabled_multidaemon",
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
            if isinstance(authors, (list, tuple)):
                item._nodeid += " ({})".format(", ".join(authors))
            else:
                raise ValueError(
                    "Authors must be a list/tuple of strings, got {}({}). Node id is {}".format(
                        type(authors), repr(authors), item._nodeid
                    )
                )


def pytest_itemcollected(item):
    authors = _get_first_marker(item, name="authors")
    if authors is None:
        raise RuntimeError("Test {} is not marked with @authors".format(item._nodeid))
