from yt_env_setup import YTEnvSetup, is_asan_build, search_binary_path

from yt_commands import authors, create_pool, run_test_vanilla, ls, create, write_table, read_table, exists, get

from yt_helpers import read_structured_log, write_log_barrier

import yt.wrapper as yt
import yt.yson as yson
from yt.wrapper.schema import yt_dataclass, OtherColumns
from yt.wrapper.schema import YsonBytes, TableSchema

import pytest

from collections import defaultdict
import json
import os
import sys
import subprocess
import time
import typing


PREPARE_SCHEDULING_USAGE_BINARY = search_binary_path("prepare_scheduling_usage")


@yt_dataclass
class YtStructuredSchedulerLog:
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


@authors("ignat")
class TestPrepareSchedulingUsage(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    ENABLE_HTTP_PROXY = True

    USE_PORTO = True

    LOG_WRITE_WAIT_TIME = 0.5

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "accumulated_usage_log_period": 1000,
            "accumulated_resource_usage_update_period": 100,
        }
    }

    def _check_base_pools_tags(self, rows):
        for row in rows:
            assert row
            assert row["cluster"] == "local_cluster"
            assert row["pool_tree"] == "default"
        assert "/parent_pool" in (row["pool_path"] for row in rows)
        assert "/parent_pool/test_pool" in (row["pool_path"] for row in rows)

    def _check_operation_info(self, rows, op_id):
        stat = defaultdict(float)
        columns = ["accumulated_resource_usage_cpu", "accumulated_resource_usage_memory", "cumulative_memory",
                   "cumulative_max_memory", "cumulative_used_cpu", "tmpfs_max_usage", "tmpfs_limit", "time_total",
                   "time_prepare", "data_input_chunk_count", "data_input_data_weight",  "data_output_chunk_count",
                   "data_output_data_weight", "accumulated_resource_usage_gpu", "cumulative_gpu_utilization",
                   "cumulative_sm_utilization"]

        for index, row in enumerate(rows):
            for key in columns:
                assert key in row
                stat[key] += row[key]
            assert row["operation_id"] == op_id
            assert row["cluster"] == "local_cluster"
            assert row["pool_path"] == "/parent_pool/test_pool"
            assert row["title"] == "test_title"
            if isinstance(row["annotations"], str):
                row["annotations"] = yson.loads(row["annotations"].encode("ascii"))
            assert row["annotations"] == {"my_key": "my_value"}
            if index + 1 < len(rows):
                assert row["operation_state"] == "running"
            else:
                assert row["operation_state"] == "completed"

        assert 5.0 <= stat["accumulated_resource_usage_cpu"]

        assert stat["cumulative_memory"] > 0
        assert stat["cumulative_memory"] <= stat["accumulated_resource_usage_memory"]

        assert stat["cumulative_max_memory"] > 0
        assert stat["cumulative_max_memory"] <= stat["accumulated_resource_usage_memory"]

        assert stat["cumulative_used_cpu"] > 0
        assert stat["cumulative_used_cpu"] <= stat["accumulated_resource_usage_cpu"]

    def _create_pools(self):
        parent_attributes = {"strong_guarantee_resources": {"cpu": 1.0}, "abc": {"id": 1, "slug": "abc_slug"}}
        create_pool("parent_pool", pool_tree="default", attributes=parent_attributes)
        create_pool("test_pool", pool_tree="default", parent_name="parent_pool")

    def _prepare_test_environment(self):
        scheduler_address = ls("//sys/scheduler/instances")[0]
        from_barrier = write_log_barrier(scheduler_address)

        op = run_test_vanilla(
            "sleep 5.2",
            pool="test_pool",
            spec={
                "annotations": {"my_key": "my_value"},
                "title": "test_title"
            },
            track=True)

        scheduler_log_file = self.path_to_run + "/logs/scheduler-0.json.log"

        time.sleep(self.LOG_WRITE_WAIT_TIME)

        to_barrier = write_log_barrier(scheduler_address)

        structured_log = read_structured_log(scheduler_log_file, from_barrier=from_barrier, to_barrier=to_barrier,
                                             row_filter=lambda e: "event_type" in e)

        for row in structured_log:
            row["cluster"] = "local_cluster"

        operation_event_indexes = []
        accumulated_usage_event_indexes = []
        for index, row in enumerate(structured_log):
            if row["event_type"].startswith("operation_"):
                operation_event_indexes.append(index)
            if row["event_type"] == "accumulated_usage_info":
                accumulated_usage_event_indexes.append(index)

        assert len(operation_event_indexes) >= 2
        min_operation_event_index = operation_event_indexes[0]
        max_operation_event_index = operation_event_indexes[-1]

        accumulated_usage_event_indexes = [
            index
            for index in accumulated_usage_event_indexes
            if index >= min_operation_event_index and index <= max_operation_event_index
        ]
        assert len(accumulated_usage_event_indexes) >= 5

        mid_index = (min_operation_event_index + max_operation_event_index) // 2
        structured_log_part1 = structured_log[min_operation_event_index:mid_index]
        structured_log_part2 = structured_log[mid_index:max_operation_event_index + 1]

        return structured_log_part1, structured_log_part2, op.id

    def _run_script_local_mode(self, rows):
        prepare_scheduling_usage_dir = os.path.join(self.Env.path, "prepare_scheduling_usage")
        if not os.path.exists(prepare_scheduling_usage_dir):
            os.mkdir(prepare_scheduling_usage_dir)

        input_filepath = os.path.join(prepare_scheduling_usage_dir, "input.yson")
        output_filepath = os.path.join(prepare_scheduling_usage_dir, "output.yson")

        with open(input_filepath, "wb") as fout:
            yson.dump(rows, fout, yson_type="list_fragment")

        subprocess.check_call(
            [
                PREPARE_SCHEDULING_USAGE_BINARY,
                "--cluster", self.Env.get_proxy_address(),
                "--mode", "local",
                "--input-path", input_filepath,
                "--output-path", output_filepath
            ],
            stderr=sys.stderr)

        with open(output_filepath, "rb") as fin:
            output = list(yson.load(fin, yson_type="list_fragment"))

        with open(output_filepath + ".pools", "rb") as fin:
            pools = yson.load(fin)

        with open(output_filepath + ".tags", "rb") as fin:
            tags = list(yson.load(fin, yson_type="list_fragment"))

        return output, pools, tags

    @pytest.mark.skipif(is_asan_build(), reason="Memory consumption is unpredictable under ASAN")
    def test_simple(self):
        self._create_pools()
        structured_log_part1, structured_log_part2, op_id = self._prepare_test_environment()

        rows1, pools1, tags1 = self._run_script_local_mode(structured_log_part1)
        rows2, pools2, tags2 = self._run_script_local_mode(structured_log_part2)

        assert pools1 == pools2
        assert len(pools1) == 3
        assert "/" in (row["pool_path"] for row in pools1)
        self._check_base_pools_tags(pools1)
        for row in pools1:
            pool_info = json.loads(row["pool_info"])
            assert pool_info
            if row["pool_path"] == "/parent_pool":
                assert pool_info["strong_guarantee_resources"]["cpu"] == 1.0
                assert pool_info["abc"]["id"] == 1
            if row["pool_path"] == "/parent_pool/test_pool":
                assert pool_info["parent"] == "parent_pool"
                assert pool_info["abc"]["id"] == 1

        assert tags1 == tags2
        assert len(tags1) == 2
        self._check_base_pools_tags(tags1)
        for row in tags1:
            assert json.loads(row["tags_json"]) == ["my_key"]

        assert len(rows1) >= 1
        assert len(rows2) >= 1
        assert rows1 != rows2
        rows = rows1 + rows2
        self._check_operation_info(rows, op_id)

    def _run_script_table_mode(self, input_data, target, expiration_timeout):
        prepare_scheduling_usage_dir = "//prepare_scheduling_usage"

        if not os.path.exists(os.path.join(self.Env.path, "prepare_scheduling_usage")):
            os.mkdir(os.path.join(self.Env.path, "prepare_scheduling_usage"))

        input_dir = yt.ypath_join(prepare_scheduling_usage_dir, "input")
        output_dir = yt.ypath_join(prepare_scheduling_usage_dir, "output")

        create("map_node", prepare_scheduling_usage_dir, force=True)
        create("map_node", input_dir, force=True)
        create("map_node", output_dir, force=True)

        input_filepath = yt.ypath_join(input_dir, target)
        output_filepath = yt.ypath_join(output_dir, target)

        create(
            "table",
            input_filepath,
            attributes={
                "schema": TableSchema.from_row_type(YtStructuredSchedulerLog),
            },
            force=True
        )
        write_table(
            input_filepath,
            input_data
        )

        for date in ["2020-01-01", "2020-01-02", "2020-01-03", "2020-01-04", "2020-01-05"]:
            create(
                "table",
                yt.ypath_join(output_dir, date),
                attributes={
                    "schema": TableSchema.from_row_type(OperationInfo).build_schema_sorted_by([
                        "cluster", "pool_tree", "pool_path"
                    ]),
                },
                force=True
            )

        with open(os.path.join(self.Env.path, "prepare_scheduling_usage", "test_link_bin.log"), "w") as fout:
            subprocess.check_call(
                [
                    PREPARE_SCHEDULING_USAGE_BINARY,
                    "--cluster", self.Env.get_proxy_address(),
                    "--mode", "table",
                    "--input-path", input_filepath,
                    "--output-path", output_filepath,
                ] + ([] if expiration_timeout is None else [
                    "--set-expiration-timeout", str(expiration_timeout)
                ]),
                stderr=fout)

        return input_filepath, output_filepath, input_dir, output_dir

    @pytest.mark.parametrize(
        "target,expiration_timeout",
        [("2020-01-06", 604800000), ("2020-01-07", None)]
    )
    @pytest.mark.timeout(300)
    def test_link(self, target, expiration_timeout):
        target_schemas = {
            "pools_tables": {
                "cluster": "utf8",
                "pool_tree": "utf8",
                "pool_path": "utf8",
                "pool_info": "utf8",
            },
            "tags": {
                "cluster": "utf8",
                "pool_tree": "utf8",
                "pool_path": "utf8",
                "tags_json": "utf8",
            },
        }
        self._create_pools()

        structured_log_part1, structured_log_part2, op_id = self._prepare_test_environment()
        structured_log = structured_log_part1 + structured_log_part2
        _, output_filepath, _, output_dir = self._run_script_table_mode(structured_log, target, expiration_timeout)

        for table_category in ("pools_tables", "tags"):
            link_target_path = yt.ypath_join(output_dir, table_category, target)
            assert exists(link_target_path)

            link_target_schema = {field["name"]: field["type"] for field in get(link_target_path + "/@schema")}
            assert link_target_schema == target_schemas[table_category]

            link_path = yt.ypath_join(output_dir, table_category, "latest")
            assert exists(link_path)
            assert get(link_path + "&/@target_path") == link_target_path

            if expiration_timeout:
                assert get(output_filepath + "/@expiration_timeout") == expiration_timeout
                assert get(link_target_path + "/@expiration_timeout") == expiration_timeout
            else:
                assert not exists(output_filepath + "/@expiration_timeout")
                assert not exists(link_target_path + "/@expiration_timeout")

            self._check_base_pools_tags(read_table(link_target_path))

            if table_category == "pools_tables":
                assert get(link_target_path + "/@row_count") == 3
                assert "/" in (row["pool_path"] for row in read_table(link_target_path))
                for row in read_table(link_target_path):
                    pool_info = json.loads(row["pool_info"])
                    assert pool_info
                    if row["pool_path"] == "/parent_pool":
                        assert pool_info["strong_guarantee_resources"]["cpu"] == 1.0
                        assert pool_info["abc"]["id"] == 1
                    if row["pool_path"] == "/parent_pool/test_pool":
                        assert pool_info["parent"] == "parent_pool"
                        assert pool_info["abc"]["id"] == 1
            else:
                assert get(link_target_path + "/@row_count") == 2
                for row in read_table(link_target_path):
                    assert json.loads(row["tags_json"]) == ["my_key"]

        self._check_operation_info(read_table(output_filepath), op_id)
