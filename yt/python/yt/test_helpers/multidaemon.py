def pytest_configure(config):
    config.addinivalue_line("markers", "enabled_multidaemon")


def pytest_collection_modifyitems(items, config):
    for item in items:
        cls = getattr(item, "cls", None)
        if cls is not None and getattr(cls, "ENABLE_MULTIDAEMON", False):
            item.add_marker("enabled_multidaemon")
