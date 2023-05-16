from yt_env_setup import YTEnvSetup, is_asan_build

from yt_commands import authors, create_pool, run_test_vanilla, ls, create, exists, get

from yt_helpers import read_structured_log, write_log_barrier

import yt.wrapper as yt
import yt.yson as yson
from yt.environment import arcadia_interop
from yt.wrapper.schema import yt_dataclass, OtherColumns
from yt.wrapper.schema import YsonBytes, TableSchema

import pytest

import json
import os
import sys
import subprocess
import time
import typing

PREPARE_SCHEDULING_USAGE_BINARY = arcadia_interop.search_binary_path("prepare_scheduling_usage")


@yt_dataclass
class YtStructuredSchedulerLog:
    timestamp: int
    cluster: str
    accumulated_resource_usage_per_tree: typing.Optional[YsonBytes]
    experiment_assignment_names: typing.Optional[YsonBytes]
    runtime_params: typing.Optional[YsonBytes]
    event_type: str
    runtime_parameters: typing.Optional[YsonBytes]
    operation_id: typing.Optional[str]
    pools: typing.Optional[YsonBytes]
    unrecognized_spec: typing.Optional[YsonBytes]
    address: typing.Optional[str]
    instant: str
    operations: typing.Optional[YsonBytes]
    start_time: typing.Optional[str]
    finish_time: typing.Optional[str]
    error: typing.Optional[YsonBytes]
    tree_id: typing.Optional[str]
    iso_eventtime: typing.Optional[str]
    scheduling_info_per_tree: typing.Optional[YsonBytes]
    progress: typing.Optional[YsonBytes]
    spec: typing.Optional[YsonBytes]
    pool: typing.Optional[str]
    alerts: typing.Optional[YsonBytes]
    experiment_assignments: typing.Optional[YsonBytes]
    source_uri: typing.Optional[str]


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

    def _run_script(self, rows):
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
        create_pool("parent_pool", pool_tree="default", attributes={"strong_guarantee_resources": {"cpu": 1.0}})
        create_pool("test_pool", pool_tree="default", parent_name="parent_pool")

        scheduler_address = ls("//sys/scheduler/instances")[0]
        from_barrier = write_log_barrier(scheduler_address)

        op = run_test_vanilla(
            "sleep 5.2",
            pool="test_pool",
            spec={
                "annotations": {"my_key": "my_value"}
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

        rows1, pools1, tags1 = self._run_script(structured_log_part1)
        assert len(rows1) >= 1

        rows2, pools2, tags2 = self._run_script(structured_log_part2)
        assert len(rows2) >= 1

        assert pools1 == pools2
        assert len(pools1) == 3
        parent_pool_info_list = [
            pool_path_info["pool_info"] for pool_path_info in pools1
            if pool_path_info["pool_path"] == "/parent_pool"
        ]
        assert len(parent_pool_info_list) == 1

        def check_tags_content(rows):
            assert rows
            assert all(json.loads(row["tags_json"]) == ["my_key"] for row in rows)
            assert "/parent_pool" in (row["pool_path"] for row in rows)
            assert "/parent_pool/test_pool" in (row["pool_path"] for row in rows)

        check_tags_content(tags1)
        check_tags_content(tags2)

        parent_pool_info = parent_pool_info_list[0]
        assert parent_pool_info["strong_guarantee_resources"]["cpu"] == 1.0

        rows = rows1 + rows2

        accumulated_resource_usage_memory = 0.0
        accumulated_resource_usage_cpu = 0.0
        cumulative_max_memory = 0.0
        cumulative_used_cpu = 0.0

        for index, row in enumerate(rows):
            accumulated_resource_usage_cpu += row["accumulated_resource_usage_cpu"]
            accumulated_resource_usage_memory += row["accumulated_resource_usage_memory"]
            cumulative_max_memory += row["cumulative_max_memory"]
            cumulative_used_cpu += row["cumulative_used_cpu"]
            assert row["operation_id"] == op.id
            assert row["cluster"] == "local_cluster"
            assert row["pool_path"] == "/parent_pool/test_pool"
            assert yson.loads(row["annotations"].encode("ascii")) == {"my_key": "my_value"}
            if index + 1 < len(rows):
                assert row["operation_state"] == "running"
            else:
                assert row["operation_state"] == "completed"

        assert 5.0 <= accumulated_resource_usage_cpu

        assert cumulative_max_memory > 0
        assert cumulative_max_memory <= accumulated_resource_usage_memory

        assert cumulative_used_cpu > 0
        assert cumulative_used_cpu <= accumulated_resource_usage_cpu

    def _run_script_for_yt_table_test(self, target, expiration_timeout):
        tmp_dir = "//tmp/yt_wrapper/file_storage"
        prepare_scheduling_usage_dir = "//prepare_scheduling_usage"

        if not os.path.exists(os.path.join(self.Env.path, "prepare_scheduling_usage")):
            os.mkdir(os.path.join(self.Env.path, "prepare_scheduling_usage"))

        input_dir = yt.ypath_join(prepare_scheduling_usage_dir, "input")
        output_dir = yt.ypath_join(prepare_scheduling_usage_dir, "output")

        create("map_node", prepare_scheduling_usage_dir, force=True)
        create("map_node", input_dir, force=True)
        create("map_node", output_dir, force=True)
        create("map_node", tmp_dir, recursive=True, force=True)

        input_filepath = yt.ypath_join(input_dir, target)
        output_filepath = yt.ypath_join(output_dir, target)

        pool_info_names_types = {
            "cluster": "string",
            "pool_tree": "string",
            "pool_path": "string",
            "pool_info": "string",
        }

        create(
            "table",
            input_filepath,
            attributes={
                "schema": TableSchema.from_row_type(YtStructuredSchedulerLog),
            },
            force=True
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

        pool_info_path = yt.ypath_join(output_dir, "pools", target)
        assert exists(pool_info_path)
        pool_info_schema = get(pool_info_path + "/@schema")
        assert pool_info_names_types == {field["name"]: field["type"] for field in pool_info_schema}

        link_path = yt.ypath_join(output_dir, "tags", "latest")
        link_target_path = yt.ypath_join(output_dir, "tags", target)
        assert exists(link_path)
        assert get(link_path + "&/@target_path") == link_target_path

        if (expiration_timeout or -1) > 0:
            assert get(output_filepath + "/@expiration_timeout") == expiration_timeout
            assert get(link_target_path + "/@expiration_timeout") == expiration_timeout
            assert get(pool_info_path + "/@expiration_timeout") == expiration_timeout
        else:
            assert not exists(output_filepath + "/@expiration_timeout")
            assert not exists(link_target_path + "/@expiration_timeout")
            assert not exists(pool_info_path + "/@expiration_timeout")

    @pytest.mark.timeout(300)
    def test_link(self):
        self._run_script_for_yt_table_test("2020-01-06", 604800000)
        self._run_script_for_yt_table_test("2020-01-07", None)
