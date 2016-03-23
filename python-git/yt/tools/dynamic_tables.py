import yt.wrapper as yt
import yt.yson as yson
import sys
import time
import inspect
import logging
import itertools as it

from yt.common import YtError, update, set_pdeathsig
from yt.wrapper.common import run_with_retries
from yt.wrapper.client import Yt
from random import randint, shuffle
from time import sleep

yt.config["pickling"]["module_filter"] = lambda module: not hasattr(module, "__file__") or "yt_driver_bindings" not in module.__file__

DEFAULT_CLIENT_CONFIG = {
    "driver_config_path": "/etc/ytdriver.conf",
    "api_version": "v3"
}

# Mapper job options.
JOB_COUNT = 100
# Maximum number of simultaneously running jobs.
USER_SLOTS = 100
# Maximum amount of memory allowed for a job
JOB_MEMORY_LIMIT = 4*1024*1024*1024
# Maximum number of failed jobs which doesn't imply operation failure.
MAX_FAILDED_JOB_COUNT = 20
# Maximum number of output rows
OUTPUT_ROW_LIMIT = 100000000
# Maximum number of input rows
INPUT_ROW_LIMIT = 100000000

# Nice yson format
YSON_FORMAT = yt.YsonFormat(boolean_as_string=False, process_table_index=False)

def call(function, *args, **kwargs):
    argspec = inspect.getargspec(function)
    namedargs = argspec.args
    has_keywords = argspec.keywords is not None
    valid_kwargs = kwargs if has_keywords else {key: value for key, value in kwargs.iteritems() if key in namedargs}
    return function(*args, **valid_kwargs)

def log_exception(ex):
    sys.stderr.write("Execution failed with error: \n")
    sys.stderr.write(str(ex) + "\n")
    sys.stderr.write("Retrying...")
    sys.stderr.flush()

# Build map operation spec from command line args.
def build_spec_from_options(
    job_count=JOB_COUNT,
    max_failed_job_count=MAX_FAILDED_JOB_COUNT,
    memory_limit=JOB_MEMORY_LIMIT,
    user_slots=USER_SLOTS,
    pool=None):
    spec = {
        "pool": pool,
        "enable_job_proxy_memory_control": False,
        "job_count": job_count,
        "max_failed_job_count": max_failed_job_count,
        "job_proxy_memory_control": False,
        "mapper": {"memory_limit": memory_limit},
        "resource_limits": {"user_slots": user_slots}}
    return spec

# Mapper - get tablet partition pivot keys.
def _collect_pivot_keys_mapper(tablet):
    client = Yt(config=DEFAULT_CLIENT_CONFIG)
    for pivot_key in get_pivot_keys(tablet, client):
        yield {"pivot_key": pivot_key}

def get_pivot_keys(tablet, client=None):
    pivot_keys = [tablet["pivot_key"]]
    tablet_id = tablet["tablet_id"]
    cell_id = tablet["cell_id"]
    node = yt.get("#{}/@peers/0/address".format(cell_id), client=client)
    for partition in yt.get("//sys/nodes/{}/orchid/tablet_cells/{}/tablets/{}/partitions".format(node, cell_id, tablet_id), client=client):
        pivot_keys.append(partition["pivot_key"])
    return pivot_keys

def wait_for_state(table, state):
    while not all(tablet["state"] == state for tablet in yt.get(table + "/@tablets")):
        logging.info("Waiting for table {} tablets to become {}".format(table, state))
        sleep(1)

def unmount_table(table):
    yt.unmount_table(table)
    wait_for_state(table, "unmounted")

def mount_table(table):
    yt.mount_table(table)
    wait_for_state(table, "mounted")

# Write source table partition bounds into partition_bounds_table
def extract_partition_bounds(table, partition_bounds_table, pool=None):
    # Get pivot keys. For a large number of tablets use map-reduce version.
    # Tablet pivots are merged with partition pivots

    tablets = yt.get(table + "/@tablets")

    logging.info("Prepare partition keys for {} tablets".format(len(tablets)))
    partition_keys = []

    if len(tablets) < 10:
        logging.info("Via get")
        tablet_idx = 0
        for tablet in tablets:
            tablet_idx += 1
            logging.info("Tablet {} of {}".format(tablet_idx, len(tablets)))
            partition_keys.extend(get_pivot_keys(tablet))
    else:
        logging.info("Via map")
        with yt.TempTable() as tablets_table, yt.TempTable() as partitions_table:
            yt.write_table(tablets_table, tablets, YSON_FORMAT, raw=False)
            yt.run_map(
                _collect_pivot_keys_mapper,
                tablets_table,
                partitions_table,
                spec={"pool": pool, "job_count": 100, "max_failed_job_count": 10, "resource_limits": {"user_slots": 50}},
                format=YSON_FORMAT)
            yt.run_merge(partitions_table, partitions_table)
            partition_keys = yt.read_table(partitions_table, format=YSON_FORMAT, raw=False)
            partition_keys = [p["pivot_key"] for p in partition_keys]

    partition_keys = [list(it.takewhile(lambda x : x is not None, key)) for key in partition_keys]
    partition_keys = [key for key in partition_keys if len(key) > 0]
    partition_keys = sorted(partition_keys)
    logging.info("Total {} partitions".format(len(partition_keys) + 1))

    # Write partition bounds into partition_bounds_table.
    regions = zip([None] + partition_keys, partition_keys + [None])
    regions = [{"left": r[0], "right": r[1]} for r in regions]
    shuffle(regions)
    yt.write_table(
        partition_bounds_table,
        regions,
        format=YSON_FORMAT,
        raw=False)

def run_map_over_dynamic(mapper, src_table, dst_table, columns=None, predicate=None, pool=None, workload_descriptor=None, input_row_limit=INPUT_ROW_LIMIT, output_row_limit=OUTPUT_ROW_LIMIT, **kwargs):
    schema = yt.get(src_table + "/@schema")
    key_columns = yt.get(src_table + "/@key_columns")

    select_columns = [x["name"] for x in schema if "expression" not in x.keys()]
    if columns is not None:
        select_columns = [x for x in select_columns if x in columns]

    # Get something like ((key1, key2, key3), (bound1, bound2, bound3)) from a bound.
    get_bound_value = lambda bound : ",".join([yson.dumps(x, yson_format="text") for x in bound])
    get_bound_key = lambda width : ",".join([str(x) for x in key_columns[:width]])
    expand_bound = lambda bound : (get_bound_key(len(bound)), get_bound_value(bound))

    # Get records from source table.
    def query(left, right):
        left = "({}) >= ({})".format(*expand_bound(left)) if left != None else None
        right = "({}) < ({})".format(*expand_bound(right)) if right != None else None
        bounds = [x for x in [left, right, predicate] if x is not None]
        where = (" where " + " and ".join(bounds)) if len(bounds) > 0 else ""
        query = "{} from [{}] {}".format(",".join(select_columns), src_table, where)

        client = Yt(config=DEFAULT_CLIENT_CONFIG)
        def do_select():
            return client.select_rows(query, input_row_limit=input_row_limit, output_row_limit=output_row_limit, workload_descriptor=workload_descriptor, raw=False)
        return run_with_retries(do_select, except_action=log_exception)

    def dump_mapper(bound):
        rows = query(bound["left"], bound["right"])

        for res in mapper(rows):
            yield res

    map_spec = call(build_spec_from_options, pool=pool, **kwargs);

    mount_table(src_table)

    with yt.TempTable() as partition_bounds_table:
        extract_partition_bounds(src_table, partition_bounds_table, pool)

        yt.run_map(
            dump_mapper,
            partition_bounds_table,
            dst_table,
            spec=map_spec,
            format=YSON_FORMAT)

def split_in_groups(rows, count=10000):
    result = []
    for row in rows:
        if len(result) >= count:
            yield result
            result = []
        result.append(row)
    yield result

def run_map_dynamic(mapper, src_table, dst_table, batch_size=50000, **kwargs):
    def insert_mapper(rows):
        client = Yt(config=DEFAULT_CLIENT_CONFIG)

        def mapped_iterator():
            for row in rows:
                for res in mapper(row):
                    yield res

        for rowset in split_in_groups(mapped_iterator(), batch_size):
            def do_insert():
                client.insert_rows(dst_table, rowset, raw=False)
            run_with_retries(do_insert, except_action=log_exception)

        if False:
            yield

    mount_table(dst_table)

    with yt.TempTable() as out_table:
        run_map_over_dynamic(insert_mapper, src_table, out_table, **kwargs)

def convert_to_new_schema(schema, key_columns):
    result = []
    for column in schema:
        result_column = dict(column)
        if column["name"] in key_columns:
            result_column["sort_order"] = "ascending"
        result.append(result_column)
    return result

def run_convert(mapper, target_table, tmp_table):
    mount_table(target_table)

    run_map_dynamic(mapper, target_table, tmp_table)

    unmount_table(tmp_table)
    yt.set(tmp_table + "/@forced_compaction_revision", yt.get(tmp_table + "/@revision"))
    mount_table(tmp_table)

    unmount_table(target_table)
    unmount_table(tmp_table)

    yt.move(target_table, tmp_table + ".src")
    yt.move(tmp_table, target_table)

    mount_table(target_table)
