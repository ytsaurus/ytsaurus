import yt.logger as logger

try:
    from yt.packages.six import iteritems
except ImportError:
    from six import iteritems

from yt.wrapper.dynamic_table_commands import mount_table, unmount_table, reshard_table
from yt.wrapper.cypress_commands import exists, create, set, get
from yt.yson import YsonUint64

from yt.wrapper.common import update_inplace

try:
    from yt.packages.six.moves import xrange
except ImportError:
    from six.moves import xrange

from yt.wrapper import YtClient
import yt.wrapper.config

import os.path


def _is_fresher_than(version, min_version):
    if version is None:
        return False
    return version >= min_version


# Here and below table_kind is in ("ordered_normally", "ordered_by_trace_id")


def set_log_tailer_table_attributes(table_kind, table_path, ttl, log_tailer_version=None, client=None,
                                    attribute_patch=None):
    attribute_patch = attribute_patch or {}

    non_batch_client = YtClient(config=yt.wrapper.config.get_config(client))

    if "ssd_blobs" in non_batch_client.get("//sys/media"):
        medium = "ssd_blobs"
    else:
        medium = "default"

    attributes = {
        "min_data_versions": 0,
        "max_data_versions": 1,
        "max_dynamic_store_pool_size": 268435456,
        "min_data_ttl": ttl,
        "max_data_ttl": ttl,
        "primary_medium": medium,
        "optimize_for": "scan",
        "backing_store_retention_time": 0,
        "auto_compaction_period": 86400000,
        "dynamic_store_overflow_threshold": 0.5,
        "merge_rows_on_flush": True,
        "atomicity": "none",
    }

    update_inplace(attributes, attribute_patch)

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


def reshard_log_tailer_table(table_kind, table_path, tablet_count=None, sync=True, client=None):
    tablet_count = tablet_count or 100
    if table_kind == "ordered_normally":
        logger.debug("Resharding %s", table_path)
        pivot_keys = [[]] + [[YsonUint64(i), None, None, None] for i in xrange(1, tablet_count)]
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
    logger.debug("Creating log tailer table %s of kind %s", table_path, table_kind)
    create("table", table_path, attributes=attributes, client=client, force=True)


def prepare_log_tailer_tables(log_file,
                              artifact_path,
                              log_tailer_version=None,
                              attribute_patch=None,
                              tablet_count=None,
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

    if get(artifact_path + "/@", attributes=["disable_logging"], client=client).get("disable_logging", False):
        logger.warning("Logging disabled due to 'disable_logging' attribute")
        return

    for kind, path in [("ordered_normally", ordered_normally_path), ("ordered_by_trace_id", ordered_by_trace_id_path)]:
        logger.debug("Preparing log table %s", path)
        if not exists(path, client=client):
            create_log_tailer_table(kind, path, client=client)
        else:
            unmount_table(path, sync=True, client=client)
        set_log_tailer_table_attributes(kind, path, ttl, log_tailer_version=log_tailer_version, client=client,
                                        attribute_patch=attribute_patch)
        set_log_tailer_table_dynamic_attributes(kind, path, client=client)
        reshard_log_tailer_table(kind, path, tablet_count=tablet_count, client=client)
        mount_table(path, sync=True, client=client)
