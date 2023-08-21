def pytest_collection_modifyitems(items, config):
    items_by_class = {}

    for item in items:
        if hasattr(item, "cls") and hasattr(item.cls, "partition_items"):
            items_by_class.setdefault(item.cls, []).append(item)

    for cls, items in items_by_class.items():
        partitions = cls.partition_items(items)
        if len(partitions) == 1:
            continue

        for i, partition_items in enumerate(partitions):
            for item in partition_items:
                file, class_name, rest = item.nodeid.split("::", 3)
                item._nodeid = file + "::" + class_name + "[{}]".format(i) + "::" + rest
