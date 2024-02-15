#!/usr/bin/env python3

from yt.wrapper.schema import (
    yt_dataclass,
    OtherColumns,
)

from yt.wrapper.default_config import get_config_from_env
from yt.wrapper.common import date_string_to_timestamp
from yt.wrapper.schema import YsonBytes, TableSchema
from yt.wrapper.operation_commands import OperationState
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


def extract_cumulative_memory(job_statistics):
    MB = 10 ** 6
    job_proxy_memory = extract_statistic(job_statistics, "job_proxy/cumulative_memory_mb_sec", default=0) * MB
    user_job_memory = extract_statistic(job_statistics, "user_job/cumulative_memory_mb_sec", default=0) * MB
    return job_proxy_memory + user_job_memory


def extract_cumulative_max_memory(job_statistics):
    job_proxy_max_memory = extract_statistic(job_statistics, "job_proxy/cumulative_max_memory", default=0)
    user_job_max_memory = extract_statistic(job_statistics, "user_job/cumulative_max_memory", default=0)
    return job_proxy_max_memory + user_job_max_memory


def extract_cumulative_used_cpu(job_statistics):
    statistic_paths = [
        "job_proxy/cpu/user",
        "job_proxy/cpu/system",
        "user_job/cpu/user",
        "user_job/cpu/system",
    ]
    return sum([extract_statistic(job_statistics, path, default=0) for path in statistic_paths]) / 1000.0


def extract_data_output_stat(job_statistics, stat):
    output_count = len((job_statistics or {}).get("data", {}).get("output", {}))
    statistic_paths = ["data/output/{}/{}".format(index, stat) for index in range(output_count)]
    return sum([extract_statistic(job_statistics, path, default=0) for path in statistic_paths])


class YsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, yson.YsonEntity):
            return None
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


class TableInfo:
    def __init__(self, row_type=None, output_path=None, link_path=None):
        self.row_type = row_type
        self.output_path = output_path
        self.link_path = link_path


def get_item(other_columns, key, default_value=None):
    if other_columns is None:
        return default_value
    if key not in other_columns:
        return default_value
    return other_columns[key]


def get_sdk(spec):
    if get_item(spec, "annotations", {}).get("nv_block_id"):
        return "Nirvana"
    elif get_item(spec, "started_by", {}).get("python_version"):
        return "Python"
    elif (get_item(spec, "started_by", {}).get("user") == "yqlworker"
          or get_item(spec, "description", {}).get("yql_runner")):
        return "YQL"
    elif get_item(spec, "started_by", {}).get("wrapper_version", "").startswith("YT C++"):
        return "C++"
    elif get_item(spec, "started_by", {}).get("wrapper_version", "").startswith("yt/java/ytclient"):
        return "Java"
    elif get_item(spec, "started_by", {}).get("wrapper_version", "").startswith("iceberg/inside-yt"):
        return "Java Iceberg"
    elif get_item(spec, "started_by", {}).get("wrapper_version", "").startswith("JavaScript Wrapper"):
        return "JavaScript"
    elif not get_item(spec, "started_by"):
        return "Go"
    else:
        return "Unknown"


def optional_date_string_to_timestamp(s):
    if s:
        return date_string_to_timestamp(s)


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
    sdk: typing.Optional[str]
    # TODO(ignat): Tuple?
    pools: typing.List[str]
    title: typing.Optional[str]
    annotations: typing.Optional[YsonBytes]
    accumulated_resource_usage_cpu: typing.Optional[float]
    accumulated_resource_usage_memory: typing.Optional[float]
    accumulated_resource_usage_gpu: typing.Optional[float]
    cumulative_memory: typing.Optional[float]
    cumulative_max_memory: typing.Optional[float]
    cumulative_used_cpu: typing.Optional[float]
    cumulative_gpu_utilization: typing.Optional[float]
    tmpfs_max_usage: typing.Optional[float]
    tmpfs_limit: typing.Optional[float]
    cumulative_sm_utilization: typing.Optional[float]
    time_total: typing.Optional[float]
    time_prepare: typing.Optional[float]
    data_input_chunk_count: typing.Optional[float]
    data_input_data_weight: typing.Optional[float]
    data_output_chunk_count: typing.Optional[float]
    data_output_data_weight: typing.Optional[float]
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


@yt_dataclass
class PoolsRow:
    cluster: str
    pool_tree: str
    pool_path: str
    pool_info: str


class AggregateTags(TypedJob):
    def prepare_operation(self, context, preparer):
        for index in range(context.get_input_count()):
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
    input_directory_content = client.list(input_directory, attributes=["type"], absolute=True)
    input_directory_tables = [str(node) for node in input_directory_content if node.attributes["type"] == "table"]
    input_table_paths = sorted(input_directory_tables)[-NUM_TABLES_TO_GET_TAGS:]
    spec = yt.ReduceSpecBuilder()\
        .input_table_paths(input_table_paths) \
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

    info_base.cumulative_memory += info_update.cumulative_memory
    info_base.cumulative_max_memory += info_update.cumulative_max_memory
    info_base.cumulative_used_cpu += info_update.cumulative_used_cpu
    info_base.cumulative_gpu_utilization += info_update.cumulative_gpu_utilization
    info_base.tmpfs_max_usage += info_update.tmpfs_max_usage
    info_base.tmpfs_limit += info_update.tmpfs_limit
    info_base.cumulative_sm_utilization += info_update.cumulative_sm_utilization
    info_base.time_total += info_update.time_total
    info_base.time_prepare += info_update.time_prepare
    info_base.data_input_chunk_count += info_update.data_input_chunk_count
    info_base.data_input_data_weight += info_update.data_input_data_weight
    info_base.data_output_chunk_count += info_update.data_output_chunk_count
    info_base.data_output_data_weight += info_update.data_output_data_weight

    if not OperationState(info_base.operation_state).is_finished():
        info_base.operation_state = info_update.operation_state
    info_base.start_time = info_update.start_time
    info_base.finish_time = info_update.finish_time
    info_base.job_statistics = info_update.job_statistics

    info_base.title = info_base.title or info_update.title
    info_base.sdk = info_base.sdk or info_update.sdk

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
            if (
                "id" not in (mapping_output[pool].get("abc") or {}) and
                mapping_output[parent_pool].get("abc") is not None
            ):
                mapping_output[pool]["abc"] = mapping_output[parent_pool]["abc"]

    mapping_output = {}
    for pool in pools_info:
        if pool not in mapping_output:
            build_pool_mapping_recursive(pools_info, pool, mapping_output)
    assert len(mapping_output) == len(pools_info)
    return mapping_output


def convert_pool_mapping_to_pool_paths_info(cluster_and_tree_to_pool_mapping):
    result = list()
    for cluster_and_tree, pool_mapping in cluster_and_tree_to_pool_mapping.items():
        cluster, pool_tree = cluster_and_tree
        for pool_info in pool_mapping.values():
            result.append(
                {
                    "cluster": cluster,
                    "pool_tree": pool_tree,
                    "pool_path": build_pool_path(pool_info["ancestor_pools"]),
                    "pool_info": json.dumps(pool_info, cls=YsonEncoder)
                }
            )
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
                    cluster_and_tree_to_pool_mapping[key] = {
                        key: yson.dumps(value) for key, value in pool_mapping.items()
                    }
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
        self._known_events = ("operation_completed", "operation_aborted", "operation_failed", "operation_started",
                              "accumulated_usage_info")
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
                sdk=None,
                pools=pools,
                title=None,
                annotations=yson.dumps(info.get("trimmed_annotations")),
                accumulated_resource_usage_cpu=info["accumulated_resource_usage"]["cpu"],
                accumulated_resource_usage_memory=info["accumulated_resource_usage"]["user_memory"],
                accumulated_resource_usage_gpu=info["accumulated_resource_usage"]["gpu"],
                cumulative_used_cpu=0.0,
                cumulative_gpu_utilization=0.0,
                cumulative_sm_utilization=0,
                cumulative_max_memory=0.0,
                cumulative_memory=0.0,
                tmpfs_max_usage=0.0,
                tmpfs_limit=0.0,
                time_total=0.0,
                time_prepare=0.0,
                data_input_chunk_count=0.0,
                data_input_data_weight=0.0,
                data_output_chunk_count=0.0,
                data_output_data_weight=0.0,
                job_statistics=None,
                start_time=None,
                finish_time=None,
                other=OtherColumns(dict()),
            )

    def _process_operation_finished_or_started(self, input_row):
        accumulated_resource_usage_per_tree = input_row["accumulated_resource_usage_per_tree"]
        if accumulated_resource_usage_per_tree is None:
            return

        if "scheduling_info_per_tree" in input_row:
            scheduling_info_per_tree = input_row["scheduling_info_per_tree"]
        else:
            scheduling_info_per_tree = input_row["_rest"].get("scheduling_info_per_tree")

        if input_row["event_type"] != "operation_started":
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
            ms_multiplier = 1.0 / 1000
            operation_state = "running" \
                if input_row["event_type"] == "operation_started" \
                else input_row["event_type"][len("operation_"):]
            yield OperationInfo(
                timestamp=date_string_to_timestamp(input_row["timestamp"]),
                cluster=input_row["cluster"],
                pool_tree=pool_tree,
                pool_path=build_pool_path(pools),
                operation_id=input_row["operation_id"],
                operation_type=input_row["operation_type"],
                operation_state=operation_state,
                user=input_row["authenticated_user"],
                sdk=get_sdk(get_item(input_row, "spec")),
                pools=pools,
                title=get_item(input_row, "spec", {}).get("title"),
                annotations=annotations_yson,
                accumulated_resource_usage_cpu=usage["cpu"],
                accumulated_resource_usage_memory=usage["user_memory"],
                accumulated_resource_usage_gpu=usage["gpu"],
                cumulative_gpu_utilization=extract_statistic(
                    job_statistics,
                    "user_job/gpu/cumulative_utilization_gpu",
                    default=0
                ) * ms_multiplier,
                cumulative_sm_utilization=extract_statistic(
                    job_statistics,
                    "user_job/gpu/cumulative_sm_utilization",
                    default=0
                ) * ms_multiplier,
                cumulative_used_cpu=extract_cumulative_used_cpu(job_statistics),
                cumulative_max_memory=extract_cumulative_max_memory(job_statistics),
                cumulative_memory=extract_cumulative_memory(job_statistics),
                tmpfs_max_usage=extract_statistic(job_statistics, "user_job/tmpfs_max_usage", default=extract_statistic(
                    job_statistics, "user_job/max_tmpfs_size", default=0)),
                tmpfs_limit=extract_statistic(job_statistics, "user_job/tmpfs_limit", default=0),
                time_total=extract_statistic(job_statistics, "time/total", default=0) * ms_multiplier,
                time_prepare=extract_statistic(job_statistics, "time/prepare", default=0) * ms_multiplier,
                data_input_chunk_count=extract_statistic(job_statistics, "data/input/chunk_count", default=0),
                data_input_data_weight=extract_statistic(job_statistics, "data/input/data_weight", default=0),
                data_output_chunk_count=extract_data_output_stat(job_statistics, "chunk_count"),
                data_output_data_weight=extract_data_output_stat(job_statistics, "data_weight"),
                job_statistics=yson.dumps(job_statistics),
                start_time=optional_date_string_to_timestamp(get_item(input_row, "start_time")),
                finish_time=optional_date_string_to_timestamp(get_item(input_row, "finish_time")),
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
            yield from self._process_operation_finished_or_started(input_row)


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
        key_tuple = (row.cluster, row.pool_tree)
        if key_tuple not in cluster_and_tree_to_pool_mapping:
            cluster_and_tree_to_pool_mapping[key_tuple] = {
                key: yson.loads(value) for key, value in row.pool_mapping.items()
            }
        else:
            for pool in row.pool_mapping:
                if pool not in cluster_and_tree_to_pool_mapping[key_tuple]:
                    cluster_and_tree_to_pool_mapping[key_tuple][pool] = yson.loads(row.pool_mapping[pool])
    return cluster_and_tree_to_pool_mapping


def add_expiration_timeout_to_attributes(attributes, expiration_timeout):
    if expiration_timeout is not None and expiration_timeout > 0:
        attributes["expiration_timeout"] = expiration_timeout
    return attributes


def do_process_scheduler_log_on_yt(client, input_table, output_table, expiration_timeout, table_info_per_category):
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

    for table_category, table_info in table_info_per_category.items():
        output_path = yt.ypath_join(dir_name, table_category, table_name)
        table_info.output_path = output_path
        table_info.link_path = yt.ypath_join(dir_name, table_category, "latest")
        client.remove(output_path, force=True)
        client.create(
            "table",
            output_path,
            recursive=True,
            attributes=add_expiration_timeout_to_attributes({
                "schema": TableSchema.from_row_type(table_info.row_type),
                "optimize_for": "scan",
            }, expiration_timeout)
        )

    pool_paths_info = convert_pool_mapping_to_pool_paths_info(cluster_and_tree_to_pool_mapping)
    client.write_table(
        table_info_per_category["pools_tables"].output_path,
        pool_paths_info,
        table_writer={"max_row_weight": 64 * 1024 * 1024},
    )

    with client.TempTable() as temp_table:
        collect_tags_on_yt(
            input_directory=dir_name,
            destination_table=temp_table,
            client=client
        )

        client.write_table(
            table_info_per_category["tags"].output_path,
            aggregate_tags_for_ancestor_pools(client.read_table_structured(temp_table, TempTagsRow)),
            table_writer={"max_row_weight": 64 * 1024 * 1024},
        )


def update_latest_links(client, table_info_per_category):
    for table_category, table_info in table_info_per_category.items():
        category_dir, table_name = yt.ypath_split(table_info.output_path)
        names_list = client.list(category_dir)
        if "latest" in names_list:
            names_list.remove("latest")
        if table_name >= max(names_list):
            client.link(table_info.output_path, table_info.link_path, force=True)


def process_scheduler_log_on_yt(client, input_table, output_table, expiration_timeout):
    table_info_per_category = {"pools_tables": TableInfo(row_type=PoolsRow), "tags": TableInfo(row_type=TagsRow)}
    client.remove(output_table, force=True)
    with client.Transaction():
        do_process_scheduler_log_on_yt(client, input_table, output_table, expiration_timeout, table_info_per_category)
    update_latest_links(client, table_info_per_category)


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
