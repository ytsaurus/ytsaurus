from .helpers import mount_table
from .logger import logger

import yt.wrapper as yt

import copy

################################################################################


def set_dynamic_table_attributes(table, spec):
    tablet_balancer_config = {
        "enable_auto_reshard": spec.enable_tablet_balancer,
        "enable_auto_tablet_move": spec.enable_tablet_balancer,
    }
    yt.set(table + "/@tablet_balancer_config", tablet_balancer_config)


def create_dynamic_table(
        table, schema, attributes, tablet_count=None, sorted=True, dynamic=True,
        skip_mount=False, object_type="table", spec=None):
    logger.info("Create dynamic table %s" % table)
    attributes["dynamic"] = dynamic
    attributes["schema"] = schema.yson_with_unique() if sorted else schema.yson()
    yt.create(object_type, table, attributes=attributes, force=True)

    if not dynamic:
        return

    set_dynamic_table_attributes(table, spec)

    if sorted:
        yt.reshard_table(table, schema.get_pivot_keys(), sync=True)
    else:
        yt.reshard_table(table, tablet_count=tablet_count, sync=True)

    if not skip_mount:
        logger.info("Mounting...")
        mount_table(table)


def create_table_if_not_exists(path, schema, force, create_callback, check_callback=None):
    """Ensure that table exists and has expected settings, creates it otherwise.

    create_callback: called when table is actually created.
    check_callback: checks additional table settings, returns False on mismatch.
    """

    if yt.exists(path):
        existing_schema = yt.get(path + "/@schema")

        def _filter_column(column):
            column = copy.deepcopy(column)
            for attr in list(column):
                if attr not in ("name", "type", "sort_order", "aggregate"):
                    del column[attr]
            return column

        def _check_schema_match():
            if len(existing_schema) != len(schema.yson()):
                return "Schema has invalid length: expected {}, got {}".format(
                    len(schema.yson()), len(existing_schema))
            for expected, actual in zip(schema.yson(), existing_schema):
                if _filter_column(expected) != _filter_column(actual):
                    return "Column schema mismatch: expected {}, got {}".format(
                        expected, actual)
            return None

        msg = _check_schema_match()
        if not msg and check_callback and not check_callback(path):
            msg = "Custom check failed"
        if msg:
            if force:
                yt.remove(path)
            else:
                raise RuntimeError(msg + ". Use --force to replace existing table")
        else:
            logger.info("Using existing table")
            return

    logger.info("Creating new table")
    create_callback()
