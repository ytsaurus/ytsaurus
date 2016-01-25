import yt.yson as yson
import yt.wrapper as yt

import time
import datetime
from yt.tools.dynamic_tables import convert_to_new_schema

BY_ID_ARCHIVE = "//sys/operations_archive/ordered_by_id"
BY_START_TIME_ARCHIVE = "//sys/operations_archive/ordered_by_start_time"
SHARD_COUNT = 100

def get_cluster_version():
    sys_keys = yt.list("//sys")
    if "primary_masters" in sys_keys:
        masters_key = "primary_masters"
    else:
        masters_key = "masters"

    master_name = yt.list("//sys/" + masters_key)[0]
    version = yt.get("//sys/{0}/{1}/orchid/service/version".format(masters_key, master_name)).split(".")[:2]
    return version

def create_ordered_by_id_table(path, force=False):
    if force and yt.exists(path):
        yt.remove(path, force=True)

    yt.create("table", path, recursive=True)

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

    if get_cluster_version()[0] == "18":
        yt.set(path + "/@external", False)
        yt.set(path + "/@schema", convert_to_new_schema(schema, key_columns))
    else:
        yt.set(path + "/@schema", schema)
        yt.set_attribute(path, "key_columns", key_columns)

    pivot_keys = [[]] + [[yson.YsonUint64((i * 2 ** 64) / SHARD_COUNT)] for i in xrange(1, SHARD_COUNT)]
    yt.reshard_table(path, pivot_keys)
    yt.mount_table(path)

def create_ordered_by_start_time_table(path, force=False):
    if force and yt.exists(path):
        yt.remove(path, force=True)

    yt.create("table", path, recursive=True)

    schema = [
        {"name": "start_time", "type": "int64"},
        {"name": "id_hi",  "type": "uint64"},
        {"name": "id_lo",  "type": "uint64"},
        {"name": "dummy", "type": "int64"}]

    key_columns = ["start_time", "id_hi", "id_lo"]

    if get_cluster_version()[0] == "18":
        yt.set(path + "/@external", False)
        yt.set(path + "/@schema", convert_to_new_schema(schema, key_columns))
    else:
        yt.set(path + "/@schema", schema)
        yt.set_attribute(path, "key_columns", key_columns)

    yt.mount_table(path)

def datestr_to_timestamp(time_str):
    dt = datetime.datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S.%fZ")
    return int(time.mktime(dt.timetuple()) * 1000000 + dt.microsecond)

