from yt.wrapper.schema import (
    yt_dataclass,
    OtherColumns,
)

from yt.wrapper.default_config import get_config_from_env
from yt.wrapper.common import date_string_to_timestamp
from yt.wrapper.schema import YsonBytes, TableSchema
from yt.wrapper.prepare_operation import TypedJob
import yt.yson as yson
import yt.wrapper as yt

import argparse
import collections
import copy
import dataclasses
import json
import itertools
import re
import sys
import typing

import os
import psutil
import time
import tracemalloc


ROOT_POOL_NAME = "<Root>"
NUM_TABLES_TO_GET_TAGS = 5
TAGS_REDUCE_KEY = ["cluster", "pool_tree", "pool_path"]


def build_pool_path(pools):
    return "/" + "/".join(pools)


def get_value(value, default):
    if value is None:
        return default
    else:
        return value


def extract_job_statistics_for_tree(job_statistics, pool_tree):
    def filter_tree(tree):
        if isinstance(tree, dict):
            return {key: filter_tree(value) for key, value in tree.items()}
        else:
            assert isinstance(tree, list)
            return [item for item in tree if item["tags"]["pool_tree"] == pool_tree]
    return filter_tree(job_statistics)


def extract_statistic(job_statistics, path, aggr="sum", default=None):
    if job_statistics is None:
        return default

    statistics_by_path = job_statistics
    for part in path.split("/"):
        if default is not None and part not in statistics_by_path:
            return default
        statistics_by_path = statistics_by_path[part]

    return sum([item["summary"][aggr] for item in statistics_by_path])


def extract_cumulative_max_memory(job_statistics):
    job_proxy_memory_reserve = extract_statistic(job_statistics, "job_proxy/cumulative_max_memory", default=0)
    user_job_memory_reserve = extract_statistic(job_statistics, "user_job/cumulative_max_memory", default=0)
    return job_proxy_memory_reserve + user_job_memory_reserve


def extract_cumulative_used_cpu(job_statistics):
    statistic_paths = [
        "job_proxy/cpu/user",
        "job_proxy/cpu/system",
        "user_job/cpu/user",
        "user_job/cpu/system",
    ]
    return sum([extract_statistic(job_statistics, statistic_path, default=0) for statistic_path in statistic_paths]) / 1000.0


def extract_cumulative_gpu_utilization(job_statistics):
    return extract_statistic(job_statistics, "user_job/gpu/cumulative_utilization_gpu", default=0) / 1000.0


class YsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, yson.YsonEntity):
            return None
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


def get_item(other_columns, key, default_value=None):
    if other_columns is None:
        return default_value
    if key not in other_columns:
        return default_value
    return other_columns[key]


@yt_dataclass
class InputRow:
    other: OtherColumns


@yt_dataclass
class OperationInfo:
    timestamp: int
    cluster: typing.Optional[str]
    pool_tree: typing.Optional[str]
    pool_path: typing.Optional[str]
    operation_id: typing.Optional[str]
    operation_type: typing.Optional[str]
    operation_state: typing.Optional[str]
    user: typing.Optional[str]
    # TODO(ignat): Tuple?
    pools: typing.List[str]
    annotations: typing.Optional[YsonBytes]
    accumulated_resource_usage_cpu: typing.Optional[float]
    accumulated_resource_usage_memory: typing.Optional[float]
    accumulated_resource_usage_gpu: typing.Optional[float]
    cumulative_max_memory: typing.Optional[float]
    cumulative_used_cpu: typing.Optional[float]
    cumulative_gpu_utilization: typing.Optional[float]
    start_time: typing.Optional[int]
    finish_time: typing.Optional[int]
    job_statistics: typing.Optional[YsonBytes]
    other: OtherColumns


@yt_dataclass
class PoolsMapping:
    cluster: typing.Optional[str]
    pool_tree: typing.Optional[str]
    pool_mapping: typing.Optional[typing.Dict[str, typing.Optional[YsonBytes]]]


@yt_dataclass
class TempTagsRow:
    cluster: str
    pool_tree: str
    pool_path: str
    tags: typing.List[str]


@yt_dataclass
class TagsRow:
    cluster: str
    pool_tree: str
    pool_path: str
    tags_json: str


class AggregateTags(TypedJob):
    def prepare_operation(self, context, preparer):
        for index in range(NUM_TABLES_TO_GET_TAGS):
            preparer.input(index, type=OperationInfo)
        preparer.output(0, type=TempTagsRow, infer_strict_schema=False)

    def extract_tags(self, annotations_yson):
        return annotations_yson.keys() if annotations_yson else ()

    def __call__(self, rows):
        tags_to_keep = 100

        first_row = None
        counter = collections.Counter()
        for row in rows:
            first_row = first_row or row
            if row.annotations:
                counter.update(self.extract_tags(yson.loads(row.annotations)))

        if counter:
            yield TempTagsRow(
                tags=[key for key, _ in counter.most_common(tags_to_keep)],
                cluster=first_row.cluster,
                pool_tree=first_row.pool_tree,
                pool_path=first_row.pool_path
            )


def collect_tags_on_yt(input_directory, destination_table, client):
    spec = yt.ReduceSpecBuilder()\
        .input_table_paths(
            [str(node) for node in client.list(input_directory, attributes=["type"], absolute=True)
             if node.attributes["type"] == "table"][-NUM_TABLES_TO_GET_TAGS:]
        ) \
        .output_table_paths([destination_table]) \
        .begin_reducer() \
            .command(AggregateTags()) \
        .end_reducer() \
        .reduce_by(TAGS_REDUCE_KEY)\
        .spec({"max_failed_job_count": 1})  # noqa
    client.run_operation(spec)


def reduce_key(item):
    return tuple(getattr(item, field) for field in TAGS_REDUCE_KEY)


def collect_tags_locally(rows):
    sorted_rows = sorted(rows, key=reduce_key)
    grouped_rows = (items for group, items in itertools.groupby(sorted_rows, key=reduce_key))
    return list(itertools.chain.from_iterable(map(AggregateTags(), grouped_rows)))


def aggregate_tags_for_ancestor_pools(rows):
    pool_key_to_tags = collections.defaultdict(set)

    for row in rows:
        intermediate_pools = row.pool_path.split("/")
        for i in range(1, len(intermediate_pools)):
            pool_path = "/".join(intermediate_pools[:i + 1])
            pool_key_to_tags[(row.cluster, row.pool_tree, pool_path)] |= set(row.tags)

    return (
        dataclasses.asdict(
            TagsRow(
                cluster=cluster,
                pool_tree=pool_tree,
                pool_path=pool_path,
                tags_json=json.dumps(list(tag_set))
            )
        )
        for (cluster, pool_tree, pool_path), tag_set in pool_key_to_tags.items()
    )


def merge_info(info_base, info_update):
    # TODO(ignat): what if pools don't match?
    assert info_update.timestamp >= info_base.timestamp
    info_base.accumulated_resource_usage_cpu += info_update.accumulated_resource_usage_cpu
    info_base.accumulated_resource_usage_memory += info_update.accumulated_resource_usage_memory
    info_base.accumulated_resource_usage_gpu += info_update.accumulated_resource_usage_gpu

    info_base.cumulative_max_memory += info_update.cumulative_max_memory
    info_base.cumulative_used_cpu += info_update.cumulative_used_cpu
    info_base.cumulative_gpu_utilization += info_update.cumulative_gpu_utilization

    info_base.operation_state = info_update.operation_state
    info_base.start_time = info_update.start_time
    info_base.finish_time = info_update.finish_time
    info_base.job_statistics = info_update.job_statistics

    if len(info_update.other):
        info_base.other = info_update.other


def build_pool_mapping(pools_info):
    def build_pool_mapping_recursive(self, pool, mapping_output):
        if pool == ROOT_POOL_NAME:
            mapping_output[pool] = copy.deepcopy(pools_info[pool])
            mapping_output[pool]["ancestor_pools"] = []
        else:
            parent_pool = pools_info[pool]["parent"]
            if parent_pool not in mapping_output:
                build_pool_mapping_recursive(pools_info, parent_pool, mapping_output)
            mapping_output[pool] = copy.deepcopy(pools_info[pool])
            mapping_output[pool]["ancestor_pools"] = mapping_output[parent_pool]["ancestor_pools"] + [pool]

    mapping_output = {}
    for pool in pools_info:
        if pool not in mapping_output:
            build_pool_mapping_recursive(pools_info, pool, mapping_output)
    assert len(mapping_output) == len(pools_info)
    return mapping_output


def convert_pool_mapping_to_pool_paths_info(cluster_and_tree_to_pool_mapping):
    result = collections.defaultdict(lambda: collections.defaultdict(dict))
    for cluster_and_tree, pool_mapping in cluster_and_tree_to_pool_mapping.items():
        cluster, pool_tree = cluster_and_tree
        for pool_info in pool_mapping.values():
            result[cluster][pool_tree][build_pool_path(pool_info["ancestor_pools"])] = pool_info
    for cluster in result:
        for pool_tree in result[cluster]:
            result[cluster][pool_tree] = list(result[cluster][pool_tree].items())
    return result


@yt.aggregator
class ExtractPoolsMapping(TypedJob):
    def prepare_operation(self, context, preparer):
        preparer.input(0, type=InputRow).output(0, type=PoolsMapping)

    def __call__(self, rows):
        cluster_and_tree_to_pool_mapping = {}
        for row in rows:
            if row.other["event_type"] == "accumulated_usage_info":
                key = (row.other["cluster"], row.other["tree_id"])
                # TODO(ignat): how is it possible?
                if "pools" in row.other:
                    pool_mapping = build_pool_mapping(row.other["pools"])
                    cluster_and_tree_to_pool_mapping[key] = {key: yson.dumps(value) for key, value in pool_mapping.items()}
                else:
                    print("XXX", row.other, file=sys.stderr)

        for key in cluster_and_tree_to_pool_mapping:
            cluster, pool_tree = key
            yield PoolsMapping(
                cluster=cluster,
                pool_tree=pool_tree,
                pool_mapping=cluster_and_tree_to_pool_mapping[key])


class FilterAndNormalizeEvents(TypedJob):
    def __init__(self, cluster_and_tree_to_pool_mapping):
        self._known_events = ("operation_completed", "operation_aborted", "operation_failed", "accumulated_usage_info")
        self._cluster_and_tree_to_pool_mapping = cluster_and_tree_to_pool_mapping

    def prepare_operation(self, context, preparer):
        preparer.input(0, type=InputRow).output(0, type=OperationInfo, infer_strict_schema=False)

    def _process_accumulated_usage_info(self, input_row):
        pool_mapping = build_pool_mapping(input_row["pools"])
        for operation_id, info in input_row["operations"].items():
            pools = pool_mapping[info["pool"]]["ancestor_pools"]
            yield OperationInfo(
                timestamp=date_string_to_timestamp(input_row["timestamp"]),
                cluster=input_row["cluster"],
                pool_tree=input_row["tree_id"],
                pool_path=build_pool_path(pools),
                operation_id=operation_id,
                operation_type=info["operation_type"],
                operation_state="running",
                user=info["user"],
                pools=pools,
                annotations=yson.dumps(info.get("trimmed_annotations")),
                accumulated_resource_usage_cpu=info["accumulated_resource_usage"]["cpu"],
                accumulated_resource_usage_memory=info["accumulated_resource_usage"]["user_memory"],
                accumulated_resource_usage_gpu=info["accumulated_resource_usage"]["gpu"],
                cumulative_used_cpu=0.0,
                cumulative_gpu_utilization=0.0,
                cumulative_max_memory=0.0,
                job_statistics=None,
                start_time=None,
                finish_time=None,
                other=OtherColumns(dict()),
            )

    def _process_operation_finished(self, input_row):
        accumulated_resource_usage_per_tree = input_row["accumulated_resource_usage_per_tree"]
        if accumulated_resource_usage_per_tree is None:
            return

        if "scheduling_info_per_tree" in input_row:
            scheduling_info_per_tree = input_row["scheduling_info_per_tree"]
        else:
            scheduling_info_per_tree = input_row["_rest"].get("scheduling_info_per_tree")

        if "runtime_parameters" in input_row:
            runtime_parameters = input_row["runtime_parameters"]
        else:
            runtime_parameters = input_row["_rest"]["runtime_parameters"]

        all_job_statistics = get_item(get_item(input_row, "progress"), "job_statistics_v2")
        for pool_tree in accumulated_resource_usage_per_tree:
            if scheduling_info_per_tree is not None:
                pools = scheduling_info_per_tree[pool_tree]["ancestor_pools"]
            else:
                # COMPAT(ignat)
                pool = runtime_parameters["scheduling_options_per_pool_tree"][pool_tree]["pool"]
                pool_info = self._cluster_and_tree_to_pool_mapping[(input_row["cluster"], pool_tree)].get(pool)
                pools = pool_info["ancestor_pools"] if pool_info else [pool]

            job_statistics = None \
                if all_job_statistics is None \
                else extract_job_statistics_for_tree(all_job_statistics, pool_tree)
            annotations_yson = yson.dumps(get_item(input_row, "trimmed_annotations"))
            usage = input_row["accumulated_resource_usage_per_tree"][pool_tree]
            yield OperationInfo(
                timestamp=date_string_to_timestamp(input_row["timestamp"]),
                cluster=input_row["cluster"],
                pool_tree=pool_tree,
                pool_path=build_pool_path(pools),
                operation_id=input_row["operation_id"],
                operation_type=input_row["operation_type"],
                operation_state=input_row["event_type"][len("operation_"):],
                user=input_row["authenticated_user"],
                pools=pools,
                annotations=annotations_yson,
                accumulated_resource_usage_cpu=usage["cpu"],
                accumulated_resource_usage_memory=usage["user_memory"],
                accumulated_resource_usage_gpu=usage["gpu"],
                cumulative_gpu_utilization=extract_cumulative_gpu_utilization(job_statistics),
                cumulative_used_cpu=extract_cumulative_used_cpu(job_statistics),
                cumulative_max_memory=extract_cumulative_max_memory(job_statistics),
                job_statistics=yson.dumps(job_statistics),
                start_time=date_string_to_timestamp(input_row["start_time"]),
                finish_time=date_string_to_timestamp(input_row["finish_time"]),
                other=OtherColumns(dict()),
            )

    def start(self):
        self._process = psutil.Process(os.getpid())
        self._iter_count = 0
        tracemalloc.start()

    def __call__(self, input_row_raw):
        self._iter_count += 1
        if self._iter_count % 1000 == 0 and self._process.memory_full_info().rss > 2 * 1024 ** 3:
            print("Memory usage exceeded 2 GB", file=sys.stderr)
            snapshot = tracemalloc.take_snapshot()
            with open("tracemalloc.out", "w") as fout:
                for stat in snapshot.statistics("lineno"):
                    print(stat, file=fout)
            time.sleep(3600)
        input_row = input_row_raw.other
        event_type = input_row["event_type"]
        if event_type not in self._known_events:
            return

        if event_type == "accumulated_usage_info":
            yield from self._process_accumulated_usage_info(input_row)
        else:
            yield from self._process_operation_finished(input_row)


class AggregateEvents(TypedJob):
    def prepare_operation(self, context, preparer):
        preparer.input(0, type=OperationInfo).output(0, type=OperationInfo, infer_strict_schema=False)

    def __call__(self, rows):
        aggregated_row = None
        for row in rows:
            if aggregated_row is None:
                aggregated_row = row
            else:
                merge_info(aggregated_row, row)
        yield aggregated_row


def process_scheduler_log_locally(input_path, output_path):
    reduce_key = lambda item: (item.cluster, item.pool_tree, item.pool_path, item.operation_id)  # noqa
    sort_key = lambda item: (item.timestamp,)  # noqa

    input_file = open(input_path, "rb")
    output_file = open(output_path, "wb")
    output_pools_file = open(output_path + ".pools", "wb")
    output_tags_file = open(output_path + ".tags", "wb")

    rows = yson.load(input_file, yson_type="list_fragment")
    input_rows = list(map(lambda row: InputRow(other=OtherColumns(yson.dumps(row))), rows))

    # TODO: Temporary hack, do not forget to remove list() above
    cluster_and_tree_to_pool_mapping = {}
    for row in input_rows:
        if row.other["event_type"] == "accumulated_usage_info":
            key = (row.other["cluster"], row.other["tree_id"])
            cluster_and_tree_to_pool_mapping[key] = build_pool_mapping(row.other["pools"])
    pool_paths_info = convert_pool_mapping_to_pool_paths_info(cluster_and_tree_to_pool_mapping)

    mapper = FilterAndNormalizeEvents(cluster_and_tree_to_pool_mapping)
    mapper.start()
    mapped_rows = itertools.chain.from_iterable(map(mapper, input_rows))

    sorted_rows = sorted(mapped_rows, key=lambda item: tuple(list(reduce_key(item)) + list(sort_key(item))))
    grouped_rows = (items for key, items in itertools.groupby(sorted_rows, key=reduce_key))

    reducer = AggregateEvents()
    reduced_rows = list(itertools.chain.from_iterable(map(reducer, grouped_rows)))

    yson.dump(map(dataclasses.asdict, reduced_rows), output_file, yson_type="list_fragment")

    yson.dump(pool_paths_info, output_pools_file)

    list_pool_tags = collect_tags_locally(reduced_rows)
    yson.dump(aggregate_tags_for_ancestor_pools(list_pool_tags), output_tags_file, yson_type="list_fragment")


def extract_pool_mapping(client, input_table):
    temp_table = client.create_temp_table()
    client.alter_table(temp_table, schema=TableSchema.from_row_type(PoolsMapping))

    spec = yt.MapSpecBuilder()\
        .begin_mapper()\
            .command(ExtractPoolsMapping())\
            .memory_limit(4 * 1024 ** 3)\
        .end_mapper()\
        .input_table_paths([input_table])\
        .output_table_paths([temp_table])\
        .spec({"max_failed_job_count": 1})  # noqa
    client.run_operation(spec)

    cluster_and_tree_to_pool_mapping = {}
    for row in client.read_table_structured(temp_table, PoolsMapping):
        pool_mapping = {key: yson.loads(value) for key, value in row.pool_mapping.items()}
        if (row.cluster, row.pool_tree) not in cluster_and_tree_to_pool_mapping:
            cluster_and_tree_to_pool_mapping[(row.cluster, row.pool_tree)] = pool_mapping
        else:
            for pool in row.pool_mapping:
                if pool not in cluster_and_tree_to_pool_mapping[(row.cluster, row.pool_tree)]:
                    cluster_and_tree_to_pool_mapping[(row.cluster, row.pool_tree)][pool] = pool_mapping[pool]
    return cluster_and_tree_to_pool_mapping


def add_expiration_timeout_to_attributes(attributes, expiration_timeout):
    if expiration_timeout is not None and expiration_timeout > 0:
        attributes["expiration_timeout"] = expiration_timeout
    return attributes


def do_process_scheduler_log_on_yt(client, input_table, output_table, expiration_timeout):
    reduce_by = ["cluster", "pool_tree", "pool_path", "operation_id"]
    sort_by = reduce_by + ["timestamp"]

    client.create(
        "table",
        output_table,
        attributes=add_expiration_timeout_to_attributes({
            "schema": TableSchema.from_row_type(OperationInfo),
            "optimize_for": "scan",
        }, expiration_timeout)
    )

    # TODO: Temporary hack
    cluster_and_tree_to_pool_mapping = extract_pool_mapping(client, input_table)

    spec = yt.MapReduceSpecBuilder()\
        .begin_mapper()\
            .command(FilterAndNormalizeEvents(cluster_and_tree_to_pool_mapping))\
            .memory_limit(4 * 1024 ** 3)\
        .end_mapper()\
        .begin_reducer()\
            .command(AggregateEvents())\
            .memory_limit(4 * 1024 ** 3)\
        .end_reducer()\
        .input_table_paths([input_table])\
        .output_table_paths([output_table])\
        .reduce_by(reduce_by)\
        .sort_by(sort_by)\
        .spec({"max_failed_job_count": 1})  # noqa
    client.run_operation(spec)

    spec = yt.SortSpecBuilder()\
        .input_table_paths([output_table])\
        .output_table_path(output_table)\
        .sort_by(sort_by)  # noqa
    client.run_operation(spec)

    dir_name, table_name = yt.ypath_split(output_table)
    pools_output_table = yt.ypath_join(dir_name, "pools", table_name)
    client.remove(pools_output_table, force=True)
    client.create(
        "table",
        pools_output_table,
        recursive=True,
        attributes=add_expiration_timeout_to_attributes(
            {"schema": [{"name": "pools", "type": "string"}]}, expiration_timeout))

    pool_paths_info = convert_pool_mapping_to_pool_paths_info(cluster_and_tree_to_pool_mapping)
    client.write_table(
        pools_output_table,
        [{"pools": json.dumps(pool_paths_info, cls=YsonEncoder)}],
        table_writer={"max_row_weight": 64 * 1024 * 1024},
    )

    tags_output_table = yt.ypath_join(dir_name, "tags", table_name)
    tags_latest_link = yt.ypath_join(dir_name, "tags", "latest")
    client.remove(tags_output_table, force=True)
    client.create(
        "table",
        tags_output_table,
        recursive=True,
        attributes=add_expiration_timeout_to_attributes({
            "schema": TableSchema.from_row_type(TagsRow),
            "optimize_for": "scan",
        }, expiration_timeout)
    )

    with client.TempTable() as temp_table:
        collect_tags_on_yt(
            input_directory=dir_name,
            destination_table=temp_table,
            client=client
        )

        client.write_table(
            tags_output_table,
            aggregate_tags_for_ancestor_pools(client.read_table_structured(temp_table, TempTagsRow)),
            table_writer={"max_row_weight": 64 * 1024 * 1024},
        )
        client.link(tags_output_table, tags_latest_link, force=True)


def process_scheduler_log_on_yt(client, input_table, output_table, expiration_timeout):
    client.remove(output_table, force=True)
    with client.Transaction():
        do_process_scheduler_log_on_yt(client, input_table, output_table, expiration_timeout)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path")
    parser.add_argument("--output-path")
    parser.add_argument("--temp-dir")
    parser.add_argument("--cluster",
                        help="Cluster to perform processing")
    parser.add_argument("--pool",
                        help="Pool")
    parser.add_argument("--filter",
                        help="Regexp to match tables to process")
    parser.add_argument("--mode", choices=["local", "table", "dir"], default="table",
                        help="Read data from stdin and process locally (for testing purposes)")
    parser.add_argument("--set-expiration-timeout", type=int,
                        help="Set expiration timeout for output table in ms")
    args = parser.parse_args()

    if args.mode == "local":
        process_scheduler_log_locally(args.input_path, args.output_path)
    else:
        assert args.input_path
        assert args.output_path

        config = get_config_from_env()
        config["pool"] = args.pool
        client = yt.YtClient(args.cluster, config=config)

        if args.mode == "table":
            if client.exists(args.output_path) and client.get(args.output_path + "/@type") == "map_node":
                input_dir_name, input_base_name = yt.ypath_split(args.input_path)
                output_path = yt.ypath_join(args.output_path, input_base_name)
            else:
                output_path = args.output_path
            process_scheduler_log_on_yt(client, args.input_path, output_path, args.set_expiration_timeout)
        else:  # "dir"
            for name in client.list(args.input_path):
                if args.filter is not None and not re.match(args.filter, name):
                    continue
                input_table = yt.ypath_join(args.input_path, name)
                output_table = yt.ypath_join(args.output_path, name)
                process_scheduler_log_on_yt(client, input_table, output_table, args.set_expiration_timeout)


if __name__ == "__main__":
    main()
