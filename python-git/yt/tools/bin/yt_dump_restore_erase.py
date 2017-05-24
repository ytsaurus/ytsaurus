#!/usr/bin/python

import yt.yson as yson
from yt.tools.dump_restore_client import DumpRestoreClient
from yt.wrapper.client import YtClient
from yt.wrapper.config import get_config

import os
import copy
import argparse


# # Defaults

# Maximum number or rows passed to yt insert.
BATCH_SIZE = 50000
# Maximum number of output rows
OUTPUT_ROW_LIMIT = 100000000
# Maximum number of input rows
INPUT_ROW_LIMIT = 100000000
# Maximum number of simultaneously running jobs.
USER_SLOTS = 100
# Maximum amount of memory allowed for a job
JOB_MEMORY_LIMIT = "4GB"


def parse_size(size):
    """ Parse human-writable size """
    scale = {"kb": 2**10, "mb": 2**20, "gb": 2**30}
    try:
        if size[-2:].lower() in list(scale):
            value, scale_name = size[:-2], size[-2:].lower()
            return int(float(value) * scale[scale_name])
        else:
            return int(size)
    except Exception:
        raise ValueError(
            "Invalid size: '%s'. Valid suffixes are: %s." %
            (size, ", ".join(["'%s'" % key for key in list(scale)])))


def common_preprocess(options):
    """ options -> DumpRestoreClient.

    Because some options are utils-DynamicTablesClient-specific and some are local.
    """
    options = copy.deepcopy(options)

    # An inclusive list of things to remove.
    local_options = ("dump", "restore", "erase", "force", "predicate")
    extra_options = {
        name: options.pop(name, None)
        for name in local_options}

    options["job_memory_limit"] = (  # compat
        options.pop("memory_limit", None) or
        options.get("job_memory_limit"))
    proxy = options.get("proxy", None)
    options.pop("proxy")
    dump_restore_client = DumpRestoreClient(YtClient(proxy=proxy, config=get_config(None)), **options)
    return extra_options, dump_restore_client


def make_parser():
    parser = argparse.ArgumentParser(description="Map-Reduce table manipulator.")

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dump", nargs=2, metavar=("SOURCE", "DESTINATION"), help="Dump dynamic table to static")
    mode.add_argument("--restore", nargs=2, metavar=("SOURCE", "DESTINATION"), help="Restore dynamic table from static")
    mode.add_argument("--erase", nargs=1, metavar=("TABLE"), help="Erase rows in table")

    parser.add_argument("--force", action="store_true", help="Overwrite destination table if it exists")
    parser.add_argument("--proxy", type=str, help="YT proxy")
    parser.add_argument("--job_count", type=int, help="Numbser of jobs in copy task")
    parser.add_argument("--user_slots", type=int, default=USER_SLOTS, help="Maximum number of simultaneous jobs running")
    parser.add_argument("--max_failed_job_count", type=int, help="Maximum number of failed jobs")
    parser.add_argument("--job_memory_limit", type=parse_size, default=JOB_MEMORY_LIMIT, help="Memory limit for a copy task")
    parser.add_argument("--memory_limit", type=parse_size, help="Memory limit for a copy task (deprecated backwards-compatibility option)")
    parser.add_argument("--data_size_per_job", type=parse_size, help="Memory limit for a copy task (deprecated backwards-compatibility option)")
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

    if os.environ.get('DEBUG'):
        import logging
        logging.basicConfig(level=1)
        logging.getLogger('yt.packages.requests.packages.urllib3.connectionpool').setLevel('ERROR')

    extra_options, dump_restore_client = common_preprocess(vars(args))

    if args.dump:
        dump_restore_client.dump_table(
            *args.dump,
            force=extra_options.get('force'),
            predicate=extra_options.get('predicate'))
    elif args.restore:
        dump_restore_client.restore_table(
            *args.restore,
            force=extra_options.get('force'))
    elif args.erase:
        dump_restore_client.erase_table(
            *args.erase,
            predicate=extra_options.get('predicate'))

if __name__ == "__main__":
    main()
