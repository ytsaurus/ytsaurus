#!/usr/bin/python

import yt.wrapper as yt
from yt.wrapper.client import Yt
from yt.wrapper.common import run_with_retries
from yt.tools.dynamic_tables import call, mount_table, split_in_groups, log_exception, build_spec_from_options, run_map_over_dynamic, DEFAULT_CLIENT_CONFIG, YSON_FORMAT

import argparse

yt.config.VERSION = "v3"
yt.config.http.HEADER_FORMAT="yson"
#yt.config.http.TOKEN = "your token"

# Maximum number or rows passed to yt insert.
BATCH_SIZE = 50000
# Maximum number of output rows
OUTPUT_ROW_LIMIT = 100000000
# Maximum number of input rows
INPUT_ROW_LIMIT = 100000000
# Mapper job options.
JOB_COUNT = 100
# Maximum number of simultaneously running jobs.
USER_SLOTS = 100
# Maximum amount of memory allowed for a job
JOB_MEMORY_LIMIT = "4GB"
# Maximum number of failed jobs which doensn't imply operation failure.
MAX_FAILDED_JOB_COUNT = 10
# Attribute prefix
ATTRIBUTE_PREFIX = "_yt_dump_restore_"

# Parse human-writable size.
def parse_size(size):
    scale = {"kb": 2**10, "mb": 2**20, "gb": 2**30}
    try:
        return int(float(size[:-2]) * scale[size[-2:].lower()]) if size[-2:].lower() in scale.keys() else int(size)
    except:
        raise ValueError("Invalid size: '%s'. Valid suffixes are: %s." %
            (size, ", ".join(["'%s'" % key for key in scale.keys()])))

# Save dynamic table attributes.
def save_attributes(dst, schema, key_columns, pivot_keys):
    yt.set(dst + "/@" + ATTRIBUTE_PREFIX + "schema", schema)
    yt.set(dst + "/@" + ATTRIBUTE_PREFIX + "key_columns", key_columns)
    yt.set(dst + "/@" + ATTRIBUTE_PREFIX + "pivot_keys", pivot_keys)

# Restore dynamic table attributes.
def restore_attributes(src):
    schema = yt.get(src + "/@" + ATTRIBUTE_PREFIX + "schema")
    key_columns = yt.get(src + "/@" + ATTRIBUTE_PREFIX + "key_columns")
    pivot_keys = yt.get(src + "/@" + ATTRIBUTE_PREFIX + "pivot_keys")
    return schema, key_columns, pivot_keys

# Create destination table.
def create_destination(dst, force=False):
    if yt.exists(dst):
        if force:
            yt.remove(dst)
        else:
            raise Exception("Destination table exists. Use --force")
    yt.create_table(dst)

# Dump dynamic table rows into a static table
def dump_table(src_table, dst_table, **kwargs):
    schema = yt.get(src_table+ "/@schema")
    key_columns = yt.get(src_table + "/@key_columns")
    tablets = yt.get(src_table + "/@tablets")
    pivot_keys = sorted([tablet["pivot_key"] for tablet in tablets])

    call(create_destination, dst_table, **kwargs)
    save_attributes(dst_table, schema, key_columns, pivot_keys)

    def dump_mapper(rows):
        for row in rows:
            yield row

    run_map_over_dynamic(dump_mapper, src_table, dst_table, **kwargs)

# Insert static table rows into a dynamic table
def restore_table(src_table, dst_table, batch_size, **kwargs):
    schema, key_columns, pivot_keys = restore_attributes(src_table)

    call(create_destination, dst_table, **kwargs)
    yt.set(dst_table + "/@schema", schema)
    yt.set(dst_table + "/@key_columns", key_columns)
    yt.reshard_table(dst_table, pivot_keys)

    mount_table(dst_table)

    @yt.aggregator
    def restore_mapper(rows):
        client = Yt(config=DEFAULT_CLIENT_CONFIG)

        for rowset in split_in_groups(rows, batch_size):
            run_with_retries(
                lambda: client.insert_rows(dst_table, rowset, raw=False),
                except_action=log_exception)

        if False:
            yield

    with yt.TempTable() as out_table:
        yt.run_map(
            restore_mapper,
            src_table,
            out_table,
            spec=call(build_spec_from_options, **kwargs),
            format=YSON_FORMAT)

# Dump dynamic table rows into a static table
def erase_table(table, batch_size, **kwargs):
    key_columns = yt.get(table + "/@key_columns")

    def erase_mapper(rows):
        client = Yt(config=DEFAULT_CLIENT_CONFIG)

        for rowset in split_in_groups(rows, batch_size):
            run_with_retries(
                lambda: client.delete_rows(table, rowset, raw=False),
                except_action=log_exception)

        if False:
            yield

    with yt.TempTable() as out_table:
        run_map_over_dynamic(erase_mapper, table, out_table, columns=key_columns, **kwargs)

def main():
    parser = argparse.ArgumentParser(description="Map-Reduce table manipulator.")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dump", nargs=2, metavar=('SOURCE', 'DESTINATION'), help="Dump dynamic table to static")
    mode.add_argument("--restore", nargs=2, metavar=('SOURCE', 'DESTINATION'), help="Restore dynamic table from static")
    mode.add_argument("--erase", nargs=1, metavar=('TABLE'), help="Erase rows in table")
    parser.add_argument("--force", action="store_true", help="Overwrite destination table if it exists")
    parser.add_argument("--proxy", type=yt.config.set_proxy, help="YT proxy")
    parser.add_argument("--job_count", type=int, default=JOB_COUNT, help="Numbser of jobs in copy task")
    parser.add_argument("--user_slots", type=int, default=USER_SLOTS, help="Maximum number of simultaneous jobs running")
    parser.add_argument("--max_failed_job_count", type=int, default=MAX_FAILDED_JOB_COUNT, help="Maximum number of failed jobs")
    parser.add_argument("--memory_limit", type=parse_size, default=JOB_MEMORY_LIMIT, help="Memory limit for a copy task")
    parser.add_argument("--batch_size", type=int, default=BATCH_SIZE, help="Number of rows passed to 'yt insert/delete' call")
    parser.add_argument("--input_row_limit", type=int, default=INPUT_ROW_LIMIT, help="Limit the input of 'yt select' call")
    parser.add_argument("--output_row_limit", type=int, default=OUTPUT_ROW_LIMIT, help="Limit the output of 'yt select' call")
    parser.add_argument("--predicate", type=str, help="Additional predicate for 'yt select'")
    args = parser.parse_args()
    args_dict = vars(args)

    if args.dump:
        dump_table(*args.dump, **args_dict)
    elif args.restore:
        restore_table(*args.restore, **args_dict)
    elif args.erase:
        erase_table(*args.erase, **args_dict)

if __name__ == "__main__":
    main()

