import argparse
import logging
import sys

from collections import defaultdict

import yt.wrapper as yt
from yt.wrapper import yson
from yt.wrapper.default_config import get_config_from_env

from yt.ypath.rich import RichYPath

EPILOG = """Example:

{} --proxy zeno \\
    --pipeline-path //path/on/zeno
""".format(sys.argv[0])


def get_args():
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, epilog=EPILOG)

    parser.add_argument("--proxy", type=str, required=False, default=None, help="YT proxy")
    parser.add_argument("--pipeline-path", type=str, required=True, help="path to the flow pipeline")
    parser.add_argument("--tablet-count", type=int, required=False, default=10, help="tablets per computation")
    parser.add_argument(
        "--table",
        choices=[
            "input_messages",
            "compact_input_messages",
            "compact_output_messages",
            "compact_partition_output_messages",
            "timers",
            "states",
            "partition_states",
            "partition_transactions",
        ],
        default=None,
        help="table to reshard",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="enable debug output")

    args = parser.parse_args()

    args.pipeline_path, path_attributes = RichYPath().parse(args.pipeline_path)
    if cluster := path_attributes.get("cluster"):
        assert args.proxy is None
        args.proxy = cluster

    return args


def build_compact_input_message_pivot_key(computation_id, key_hash=None):
    result = computation_id.encode("utf-8") + b"\0"
    if key_hash is not None:
        result += key_hash.to_bytes(8, "big")
    return [yson.YsonString(result)]


def key_sort_value(key):
    """Sort value ordering key tuples the way YT orders composite key values (see
    composite_compare.cpp): lexicographically, where a shorter prefix sorts first and columns of
    different types compare by the EValueType order - Int64 < Uint64 < Double < Boolean < String.
    Plain sorted() fails on this: a pipeline mid-release holds keys of both the old and the new
    layout at once, and int meets str at the same position."""

    def column_sort_value(column):
        if isinstance(column, bool):
            return (3, column)
        if isinstance(column, yson.YsonUint64):
            return (1, column)
        if isinstance(column, int):
            return (0, column)
        if isinstance(column, float):
            return (2, column)
        if isinstance(column, str):
            return (4, column.encode("utf-8"))
        if isinstance(column, bytes):
            return (4, column)
        raise TypeError(f"Unsupported key column type: {type(column)}")

    return [column_sort_value(column) for column in key]


def reshard_computation_key_table(client, computations, source_keys, table, tablet_count, compact_key=False):
    if len(computations) == 0:
        logging.info(f"Skip {table} because there is no computations")
        return

    hash_step = 2**64 // tablet_count

    logging.info(f"Resharding {table}...")
    client.unmount_table(table, sync=True)
    pivot_keys = []
    for computation_id in sorted(computations):
        if pivot_keys:
            if compact_key:
                pivot_keys.append(build_compact_input_message_pivot_key(computation_id))
            else:
                pivot_keys.append([computation_id])
        else:
            pivot_keys.append([])
        if computation_id in source_keys:
            keys = [[computation_id, key] for key in source_keys[computation_id]]
            source_step = max(1, len(keys) // tablet_count)
            keys.sort(key=lambda item: (item[0], key_sort_value(item[1])))
            pivot_keys.extend(keys[source_step::source_step])
        else:
            for i in range(hash_step, 2**64, hash_step):
                if compact_key:
                    pivot_keys.append(build_compact_input_message_pivot_key(computation_id, i))
                else:
                    pivot_keys.append([computation_id, yson.YsonList([yson.YsonUint64(i)])])
    client.reshard_table(table, pivot_keys=pivot_keys, sync=True)
    client.mount_table(table, sync=True)
    logging.info(f"Finished resharding {table}")


def reshard_partition_table(client, computations, table, tablet_count):
    logging.info(f"Resharding {table}...")
    client.unmount_table(table, sync=True)
    client.reshard_table(table, tablet_count=tablet_count * len(computations), uniform=True, sync=True)
    client.mount_table(table, sync=True)
    logging.info(f"Finished resharding {table}")


def reshard_input_table(client, computations, path, tablet_count):
    table = f"{path}/input_messages"
    reshard_computation_key_table(client, computations, {}, table, tablet_count)


def reshard_compact_input_table(client, computations, path, tablet_count):
    table = f"{path}/compact_input_messages"
    reshard_computation_key_table(client, computations, {}, table, tablet_count, compact_key=True)


def reshard_timer_table(client, computations, path, tablet_count):
    table = f"{path}/timers"
    reshard_computation_key_table(client, computations, {}, table, tablet_count)


def reshard_compact_partition_output_table(client, computations, path, tablet_count):
    table = f"{path}/compact_partition_output_messages"
    reshard_partition_table(client, computations, table, tablet_count)


def reshard_compact_output_table(client, computations, source_keys, path, tablet_count):
    table = f"{path}/compact_output_messages"
    reshard_computation_key_table(client, computations, source_keys, table, tablet_count)


def reshard_state_table(client, computations, source_keys, path, tablet_count):
    table = f"{path}/states"
    reshard_computation_key_table(client, computations, source_keys, table, tablet_count)


def reshard_partition_state_table(client, computations, path, tablet_count):
    table = f"{path}/partition_states"
    reshard_partition_table(client, computations, table, tablet_count)


def reshard_partition_transactions_table(client, computations, path, tablet_count):
    table = f"{path}/partition_transactions"
    reshard_partition_table(client, computations, table, tablet_count)


def reshard_tables(args):
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s", level=logging.DEBUG if args.verbose else logging.INFO
    )

    client = yt.YtClient(proxy=args.proxy, config=get_config_from_env())

    spec = client.get_pipeline_spec(args.pipeline_path)
    partitions = client.get_flow_view(args.pipeline_path, "/state/execution_spec/layout/partitions", cache=True)

    inputs = []
    outputs = []
    timers = []
    sources = []
    computations = []

    for computation_id, computation in spec["spec"]["computations"].items():
        if computation["input_stream_ids"]:
            assert computation[
                "group_by_schema"
            ], "Computation with input_stream_ids should have non-empty group_by_schema"
            assert (
                computation["group_by_schema"][0]["type"] == "uint64"
            ), "First column in group_by_schema should have type equal to 'uint64' with hash value"
            inputs.append(computation_id)
        if computation["output_stream_ids"]:
            outputs.append(computation_id)
        if computation["timer_streams"]:
            timers.append(computation_id)
        if computation["source_streams"]:
            sources.append(computation_id)
        computations.append(computation_id)

    source_keys = defaultdict(list)
    for partition in partitions.values():
        if "source_key" in partition:
            source_keys[partition["computation_id"]].append(partition["source_key"])

    logging.info(args.table)
    if args.table is None or args.table == "input_messages":
        reshard_input_table(client, inputs, args.pipeline_path, args.tablet_count)
    if args.table is None or args.table == "compact_input_messages":
        reshard_compact_input_table(client, inputs, args.pipeline_path, args.tablet_count)
    if args.table is None or args.table == "compact_output_messages":
        reshard_compact_output_table(client, sources, source_keys, args.pipeline_path, args.tablet_count)
    if args.table is None or args.table == "compact_partition_output_messages":
        reshard_compact_partition_output_table(client, outputs, args.pipeline_path, args.tablet_count)
    if args.table is None or args.table == "timers":
        reshard_timer_table(client, timers, args.pipeline_path, args.tablet_count)
    if args.table is None or args.table == "states":
        reshard_state_table(client, computations, source_keys, args.pipeline_path, args.tablet_count)
    if args.table is None or args.table == "partition_states":
        reshard_partition_state_table(client, computations, args.pipeline_path, args.tablet_count)
    if args.table is None or args.table == "partition_transactions":
        reshard_partition_transactions_table(client, computations, args.pipeline_path, args.tablet_count)


if __name__ == "__main__":
    args = get_args()
    reshard_tables(args)
