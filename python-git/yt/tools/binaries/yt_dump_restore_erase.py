#!/usr/bin/python

import yt.wrapper as yt_module
import yt.yson as yson
from yt.wrapper.client import Yt
from yt.wrapper.common import run_with_retries
from yt.tools.dynamic_tables import DynamicTablesClient

import os
import argparse


# # Defaults

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


def parse_size(size):
    """ Parse human-writable size """
    scale = {"kb": 2**10, "mb": 2**20, "gb": 2**30}
    try:
        if size[-2:].lower() in scale.keys():
            value, scale_name = size[:-2], size[-2:].lower()
            return int(float(size[:-2]) * scale[scale_name])
        else:
            return int(size)
    except Exception:
        raise ValueError(
            "Invalid size: '%s'. Valid suffixes are: %s." %
            (size, ", ".join(["'%s'" % key for key in scale.keys()])))


def save_attributes(dst, schema, key_columns, pivot_keys, yt):
    """ Save dynamic table properties to simple attributes """
    yt.set(dst + "/@" + ATTRIBUTE_PREFIX + "schema", schema)
    yt.set(dst + "/@" + ATTRIBUTE_PREFIX + "key_columns", key_columns)
    yt.set(dst + "/@" + ATTRIBUTE_PREFIX + "pivot_keys", pivot_keys)


def restore_attributes(src, yt):
    """ Reverse of the `save_attributes`: load dynamic table
    properties from simple attributes """
    schema = yt.get(src + "/@" + ATTRIBUTE_PREFIX + "schema")
    key_columns = yt.get(src + "/@" + ATTRIBUTE_PREFIX + "key_columns")
    pivot_keys = yt.get(src + "/@" + ATTRIBUTE_PREFIX + "pivot_keys")
    return schema, key_columns, pivot_keys


def create_destination(dst, yt, force=False):
    """ Create the destination table (force-overwrite logic) """
    if yt.exists(dst):
        if force:
            yt.remove(dst)
        else:
            raise Exception("Destination table exists. Use --force")
    yt.create_table(dst)


def common_preprocess(options):
    """ options -> dynamic_tables_client.

    Because some options are utils-dynamic_tables_client-specific and some are local.
    """
    # An inclusive list of things to remove.
    local_options = (
        "force", "batch_size", "dump", "restore", "erase",
        "predicate")
    extra_options = {
        name: options.pop(name, None)
        for name in local_options}

    options["job_memory_limit"] = (  # compat
        options.pop("memory_limit", None) or
        options.get("job_memory_limit"))
    dynamic_tables_client = DynamicTablesClient(**options)
    return extra_options, dynamic_tables_client


def dump_table(src_table, dst_table, force=False, predicate=None, **options):
    """ Dump dynamic table rows into a static table """
    _, dynamic_tables_client = common_preprocess(options)
    yt = dynamic_tables_client.yt

    schema = yt.get(src_table + "/@schema")
    key_columns = yt.get(src_table + "/@key_columns")
    tablets = yt.get(src_table + "/@tablets")
    pivot_keys = sorted([tablet["pivot_key"] for tablet in tablets])

    create_destination(dst_table, yt=yt, force=force)
    save_attributes(dst_table, schema, key_columns, pivot_keys, yt=yt)

    def dump_mapper(rows):
        for row in rows:
            yield row

    dynamic_tables_client.run_map_over_dynamic(
        dump_mapper, src_table, dst_table,
        predicate=predicate)


def restore_table(src_table, dst_table, force=False, batch_size=BATCH_SIZE, **options):
    """ Insert static table rows into a dynamic table """
    _, dynamic_tables_client = common_preprocess(options)
    yt = dynamic_tables_client.yt
    schema, key_columns, pivot_keys = restore_attributes(src_table, yt=yt)

    create_destination(dst_table, yt=yt, force=force)
    yt.set(dst_table + "/@schema", schema)
    yt.set(dst_table + "/@key_columns", key_columns)
    yt.reshard_table(dst_table, pivot_keys)

    dynamic_tables_client.mount_table(dst_table)

    @yt_module.aggregator
    def restore_mapper(rows):
        client = dynamic_tables_client.make_driver_yt_client()

        def make_inserter(rowset):
            def do_insert():
                client.insert_rows(dst_table, rowset, raw=False)

            return do_insert

        for rowset in dynamic_tables_client.split_in_groups(rows, batch_size):
            inserter = make_inserter(rowset)
            run_with_retries(inserter, except_action=dynamic_tables_client.log_exception)

        if False:  # make this function into a generator
            yield

    with yt.TempTable() as out_table:
        yt.run_map(
            restore_mapper,
            src_table,
            out_table,
            spec=dynamic_tables_client.build_spec_from_options(),
            format=dynamic_tables_client.yson_format)


def erase_table(table, batch_size=BATCH_SIZE, force=False, **options):
    """ Delete all rows from a dynamic table """
    _, dynamic_tables_client = common_preprocess(options)

    yt = dynamic_tables_client.yt

    key_columns = yt.get(table + "/@key_columns")

    def erase_mapper(rows):
        client = dynamic_tables_client.make_driver_yt_client()

        def make_deleter(rowset):
            def do_delete():
                client.delete_rows(table, rowset, raw=False)

            return do_delete

        for rowset in dynamic_tables_client.split_in_groups(rows, batch_size):
            deleter = make_deleter(rowset)
            run_with_retries(deleter, except_action=dynamic_tables_client.log_exception)

        if False:  # make this function into a generator
            yield

    with yt.TempTable() as out_table:
        dynamic_tables_client.run_map_over_dynamic(
            erase_mapper, table, out_table,
            columns=key_columns)


def make_parser():
    parser = argparse.ArgumentParser(description="Map-Reduce table manipulator.")

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dump", nargs=2, metavar=("SOURCE", "DESTINATION"), help="Dump dynamic table to static")
    mode.add_argument("--restore", nargs=2, metavar=("SOURCE", "DESTINATION"), help="Restore dynamic table from static")
    mode.add_argument("--erase", nargs=1, metavar=("TABLE"), help="Erase rows in table")

    parser.add_argument("--force", action="store_true", help="Overwrite destination table if it exists")
    parser.add_argument("--proxy", type=str, help="YT proxy")
    parser.add_argument("--job_count", type=int, default=JOB_COUNT, help="Numbser of jobs in copy task")
    parser.add_argument("--user_slots", type=int, default=USER_SLOTS, help="Maximum number of simultaneous jobs running")
    parser.add_argument("--max_failed_job_count", type=int, default=MAX_FAILDED_JOB_COUNT, help="Maximum number of failed jobs")
    parser.add_argument("--job_memory_limit", type=parse_size, default=JOB_MEMORY_LIMIT, help="Memory limit for a copy task")
    parser.add_argument("--memory_limit", type=parse_size, default=None, help="Memory limit for a copy task (deprecated backwards-compatibility option)")
    parser.add_argument("--batch_size", type=int, default=BATCH_SIZE, help="Number of rows passed to the 'yt insert/delete' call")
    parser.add_argument("--input_row_limit", type=int, default=INPUT_ROW_LIMIT, help="Limit the input of the 'yt select' call")
    parser.add_argument("--output_row_limit", type=int, default=OUTPUT_ROW_LIMIT, help="Limit the output of the 'yt select' call")
    parser.add_argument("--predicate", type=str, help="Additional predicate for 'yt select'")
    parser.add_argument("--workload_descriptor", type=yson.loads, help="Workload descriptor (in yson format) for 'yt select'")
    parser.add_argument("--pool", type=str, help="Pool for scheduler operations")

    return parser


def main():
    parser = make_parser()
    args = parser.parse_args()
    args_dict = vars(args)

    if os.environ.get('DEBUG'):
        import logging
        logging.basicConfig(level=1)
        logging.getLogger('yt.packages.requests.packages.urllib3.connectionpool').setLevel('ERROR')

    if args.dump:
        dump_table(*args.dump, **args_dict)
    elif args.restore:
        restore_table(*args.restore, **args_dict)
    elif args.erase:
        erase_table(*args.erase, **args_dict)

if __name__ == "__main__":
    main()

