import yt.wrapper as yt

from .log import get_logger
logger = get_logger()


def make_schema(
    sorted=True,
    with_hash=False,
    with_hunks=False
):
    schema = [
        {"name": "key", "type": "int64"},
        {"name": "value", "type": "string"},
    ]
    if sorted:
        schema[0]["sort_order"] = "ascending"
    if with_hunks:
        schema[1]["max_inline_hunk_size"] = 16
    if with_hash:
        assert sorted
        schema[:0] = [
            {
                "name": "hash",
                "type": "uint64",
                "sort_order": "ascending",
                "expression": "farm_hash(key)"
            }
        ]
    return schema


def create_sorted_table(
    path, ignore_existing=False, force=False, attributes={}, schema_attributes={},
    reshard_args=None, mount=False,
    client=None
):
    schema_attributes["sorted"] = True
    return create_table(
        path, ignore_existing, force, attributes, schema_attributes,
        reshard_args, mount, client=client)


def create_ordered_table(
    path, ignore_existing=False, force=False, attributes={}, schema_attributes={},
    reshard_args=None, mount=False,
    client=None
):
    schema_attributes["sorted"] = False
    schema_attributes["with_hash"] = False
    return create_table(
        path, ignore_existing, force, attributes, schema_attributes,
        reshard_args, mount, client=client)


def create_table(
    path, ignore_existing=False, force=False, attributes={}, schema_attributes={},
    reshard_args=None, mount=False,
    client=None
):
    client = client or yt
    if not force:
        if client.exists(path):
            if not ignore_existing:
                raise Exception("Table already exists")
            return False

    attributes.setdefault("schema", make_schema(**schema_attributes))
    attributes["dynamic"] = True

    attributes["enable_dynamic_store_read"] = False
    #  attributes["in_memory_mode"] = "compressed"
    attributes["tablet_balancer_config"] = {
        "enable_auto_reshard": False,
    }

    mount_config = attributes.setdefault("mount_config", {})
    mount_config |= {
        "dynamic_store_auto_flush_period": 20000,
        "dynamic_store_flush_period_splay": 0,
        "auto_compaction_period": 1,
        "backing_store_retention_time": 5000,
        "lookup_cache_rows_ratio": 1.0,
        "enable_lookup_cache_by_default": True,
        "testing": {
            "opaque_stores_in_orchid": False,
        },
    }

    client.create("table", path, attributes=attributes, force=force)
    logger.info(f"Created table {path}")

    if reshard_args is not None:
        client.reshard_table(path, **reshard_args)
        logger.info(f"Resharded table {path}: {reshard_args}")

    if mount:
        client.mount_table(path, sync=True)
        logger.info(f"Mounted table {path}")

    return True
