import yt.yson as yson

from yt.packages.six.moves import xrange

import yt.wrapper as yt

import calendar
import datetime
from yt.tools.dynamic_tables import get_dynamic_table_attributes

OPERATIONS_ARCHIVE_PATH = "//sys/operations_archive"
BY_ID_ARCHIVE = "{}/ordered_by_id".format(OPERATIONS_ARCHIVE_PATH)
BY_START_TIME_ARCHIVE = "{}/ordered_by_start_time".format(OPERATIONS_ARCHIVE_PATH)
SHARD_COUNT = 100

def create_ordered_by_id_table(path, force=False):
    if force and yt.exists(path):
        yt.remove(path, force=True)

    schema = [
        {"name": "id_hash",  "type": "uint64", "expression": "farm_hash(id_hi, id_lo)"},
        {"name": "id_hi",  "type": "uint64"},
        {"name": "id_lo",  "type": "uint64"},
        {"name": "state", "type": "string"},
        {"name": "authenticated_user", "type": "string"},
        {"name": "operation_type", "type": "string"},
        {"name": "progress", "type": "any"},
        {"name": "spec", "type": "any"},
        {"name": "brief_progress", "type": "any"},
        {"name": "brief_spec", "type": "any"},
        {"name": "start_time", "type": "int64"},
        {"name": "finish_time", "type": "int64"},
        {"name": "filter_factors", "type": "string"},
        {"name": "result", "type": "any"}]

    key_columns = ["id_hash", "id_hi", "id_lo"]

    yt.create("table", path, recursive=True, attributes=get_dynamic_table_attributes(yt, schema, key_columns))

    pivot_keys = [[]] + [[yson.YsonUint64((i * 2 ** 64) // SHARD_COUNT)] for i in xrange(1, SHARD_COUNT)]
    yt.reshard_table(path, pivot_keys)
    yt.mount_table(path)

def create_ordered_by_start_time_table(path, force=False):
    if force and yt.exists(path):
        yt.remove(path, force=True)

    schema = [
        {"name": "start_time", "type": "int64"},
        {"name": "id_hi",  "type": "uint64"},
        {"name": "id_lo",  "type": "uint64"},
        {"name": "dummy", "type": "int64"}]

    key_columns = ["start_time", "id_hi", "id_lo"]

    yt.create("table", path, recursive=True, attributes=get_dynamic_table_attributes(yt, schema, key_columns))

    yt.mount_table(path)

def create_ordered_by_start_time_table_v1(path, force=False):
    if force and yt.exists(path):
        yt.remove(path, force=True)

    schema = [
        {"name": "start_time", "type": "int64"},
        {"name": "id_hi",  "type": "uint64"},
        {"name": "id_lo",  "type": "uint64"},
        {"name": "operation_type", "type": "string"},
        {"name": "state", "type": "string"},
        {"name": "authenticated_user", "type": "string"},
        {"name": "filter_factors", "type": "string"}]

    key_columns = ["start_time", "id_hi", "id_lo"]

    yt.create("table", path, recursive=True, attributes=get_dynamic_table_attributes(yt, schema, key_columns))

    yt.mount_table(path)

def datestr_to_timestamp(time_str):
    dt = datetime.datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    return int(calendar.timegm(dt.timetuple()) * 1000000 + dt.microsecond)

