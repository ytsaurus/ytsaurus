import yt.logger as logger

from yt.wrapper.dynamic_table_commands import mount_table, unmount_table, reshard_table
from yt.packages.six import iteritems
from yt.wrapper.cypress_commands import exists, create, set
from yt.yson import YsonUint64

import os.path


def _is_fresher_than(version, min_version):
    if version is None:
        return False
    return version >= min_version


# Here and below table_kind is in ("ordered_normally", "ordered_by_trace_id")


def set_log_tailer_table_attributes(table_kind, table_path, ttl, log_tailer_version=None, client=None):
    # COMPAT(max42)
    atomicity = "none" if _is_fresher_than(log_tailer_version, "19.8.34144") else "full"

    attributes = {
        "min_data_versions": 0,
        "max_data_versions": 1,
        "max_dynamic_store_pool_size": 268435456,
        "min_data_ttl": ttl,
        "max_data_ttl": ttl,
        "primary_medium": "ssd_blobs",
        "optimize_for": "scan",
        "backing_store_retention_time": 0,
        "auto_compaction_period": 86400000,
        "dynamic_store_overflow_threshold": 0.5,
        "merge_rows_on_flush": True,
        "atomicity": atomicity,
    }

    if log_tailer_version is not None:
        attributes["log_tailer_version"] = log_tailer_version

    for attribute, value in iteritems(attributes):
        attribute_path = table_path + "/@" + attribute
        logger.debug("Setting %s to %s", attribute_path, value)
        set(attribute_path, value, client=client)


def set_log_tailer_table_dynamic_attributes(table_kind, table_path, client=None):
    attributes = {
        "tablet_balancer_config/min_tablet_size": 0,
        "tablet_balancer_config/desired_tablet_size": 10 * 1024**3,
        "tablet_balancer_config/max_tablet_size": 20 * 1024**3,
    }
    for attribute, value in iteritems(attributes):
        attribute_path = table_path + "/@" + attribute
        logger.debug("Setting %s to %s", attribute_path, value)
        set(attribute_path, value, client=client)


def reshard_log_tailer_table(table_kind, table_path, sync=True, client=None):
    if table_kind == "ordered_normally":
        logger.debug("Resharding %s", table_path)
    pivot_keys = [[]] + [[YsonUint64(i), None, None, None] for i in xrange(1, 100)]
    reshard_table(table_path, pivot_keys=pivot_keys, sync=sync, client=client)


def create_log_tailer_table(table_kind, table_path, client=None):
    ORDERED_NORMALLY_SCHEMA = [
        {"name": "job_id_shard", "type": "uint64", "sort_order": "ascending", "expression": "farm_hash(job_id) % 100"},
        {"name": "timestamp", "type": "string", "sort_order": "ascending"},
        {"name": "job_id", "type": "string", "sort_order": "ascending"},
        {"name": "increment", "type": "uint64", "sort_order": "ascending"},
        {"name": "category", "type": "string"},
        {"name": "message", "type": "string"},
        {"name": "log_level", "type": "string"},
        {"name": "thread_id", "type": "string"},
        {"name": "fiber_id", "type": "string"},
        {"name": "trace_id", "type": "string"},
        {"name": "operation_id", "type": "string"}
    ]
    ORDERED_BY_TRACE_ID_SCHEMA = [
        {"name": "trace_id_hash", "type": "uint64", "sort_order": "ascending", "expression": "farm_hash(trace_id)"},
        {"name": "trace_id", "type": "string", "sort_order": "ascending"},
        {"name": "timestamp", "type": "string", "sort_order": "ascending"},
        {"name": "job_id", "type": "string", "sort_order": "ascending"},
        {"name": "increment", "type": "uint64", "sort_order": "ascending"},
        {"name": "category", "type": "string"},
        {"name": "message", "type": "string"},
        {"name": "log_level", "type": "string"},
        {"name": "thread_id", "type": "string"},
        {"name": "fiber_id", "type": "string"},
        {"name": "operation_id", "type": "string"}
    ]

    table_kind_to_schema = {"ordered_normally": ORDERED_NORMALLY_SCHEMA, "ordered_by_trace_id": ORDERED_BY_TRACE_ID_SCHEMA}
    schema = table_kind_to_schema[table_kind]
    attributes = {
        "dynamic": True,
        "schema": schema,
    }
    logger.info("Creating log tailer table %s of kind %s", table_path, table_kind)
    create("table", table_path, attributes=attributes, client=client, force=True)


def prepare_log_tailer_tables(log_file,
                              artifact_path,
                              log_tailer_version=None,
                              client=None):

    assert len(log_file.get("tables", [])) == 0

    base_path = os.path.basename(log_file["path"])

    ordered_normally_path = artifact_path + "/" + base_path
    ordered_by_trace_id_path = artifact_path + "/" + base_path + ".ordered_by_trace_id"

    log_file["tables"] = [
        {"path": ordered_normally_path},
        {"path": ordered_by_trace_id_path, "require_trace_id": True},
    ]

    ttl = log_file["ttl"]

    for kind, path in [("ordered_normally", ordered_normally_path), ("ordered_by_trace_id", ordered_by_trace_id_path)]:
        logger.info("Preparing log table %s", path)
        if not exists(path, client=client):
            create_log_tailer_table(kind, path, client=client)
        else:
            unmount_table(path, sync=True)
        set_log_tailer_table_attributes(kind, path, ttl, log_tailer_version=log_tailer_version, client=client)
        set_log_tailer_table_dynamic_attributes(kind, path, client=client)
        reshard_log_tailer_table(kind, path, client=client)
        mount_table(path, sync=True)
