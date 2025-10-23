from yt_env_setup import YTEnvSetup, Restarter, CONTROLLER_AGENTS_SERVICE, NODES_SERVICE

from yt_helpers import profiler_factory

from yt_commands import (
    authors, wait, retry, wait_no_assert, wait_breakpoint, release_breakpoint, with_breakpoint, create,
    create_pool, run_sleeping_vanilla, print_debug,
    update_controller_agent_config, get_allocation_id_from_job_id,
    lookup_rows, write_table, map, map_reduce, vanilla, run_test_vanilla, list_jobs, merge, sort,
    abort_job, get_job, set, get, sync_create_cells, raises_yt_error, exists, wait_for_cells)

import yt_error_codes

from yt_operations_archive_helpers import (
    get_allocation_id_from_archive, get_job_from_archive,
    delete_job_from_archive, update_job_in_archive, get_controller_state_from_archive,
    OPERATION_IDS_TABLE)

import yt.environment.init_operations_archive as init_operations_archive

from yt.common import date_string_to_datetime, uuid_to_parts, parts_to_uuid, update, date_string_to_timestamp_mcs

from flaky import flaky

import pytest
import builtins
import time
import datetime
from copy import deepcopy


MB_SEC_TO_BYTES_MS_DIVISOR = 1000 * 1024 * 1024


class _TestGetJobBase(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "gpu_manager": {
                "testing": {
                    "test_resource": True,
                    "test_gpu_count": 8,
                },
            },
        },
        "job_resource_manager": {
            "resource_limits": {
                "cpu": 1,
                "user_slots": 2,
            },
        },
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_reporter": {
                    "reporting_period": 10,
                    "min_repeat_delay": 10,
                    "max_repeat_delay": 10,
                }
            },
        }
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "enable_job_reporter": True,
            "enable_job_spec_reporter": True,
            "enable_job_stderr_reporter": True,
        }
    }

    def setup_method(self, method):
        super(_TestGetJobBase, self).setup_method(method)
        sync_create_cells(1)
        init_operations_archive.create_tables_latest_version(
            self.Env.create_native_client(),
            override_tablet_cell_bundle="default",
        )

    def _check_get_job(
        self,
        op_id,
        job_id,
        before_start_time,
        state=None,
        has_spec=True,
        is_stale=False,
        archive_state=None,
        controller_state=None,
        pool=None,
        pool_tree=None,
    ):
        """None arguments mean do not check corresponding field in job"""

        job_info = retry(lambda: get_job(op_id, job_id))

        assert job_info["job_id"] == job_id
        assert job_info["operation_id"] == op_id
        assert job_info["type"] == "map"
        if state is not None:
            assert job_info["state"] == state
        if archive_state is not None:
            assert job_info["archive_state"] == archive_state
        if controller_state is not None:
            assert job_info["controller_state"] == controller_state
        if pool is not None:
            assert job_info["pool"] == pool
        if pool_tree is not None:
            assert job_info["pool_tree"] == pool_tree
        start_time = date_string_to_datetime(job_info["start_time"])
        assert before_start_time < start_time < datetime.datetime.utcnow()
        assert job_info.get("is_stale") == is_stale

        attributes = ["job_id", "state", "start_time"]
        job_info = retry(lambda: get_job(op_id, job_id, attributes=attributes))
        assert builtins.set(attributes).issubset(builtins.set(job_info.keys()))
        attribute_difference = builtins.set(job_info.keys()) - builtins.set(attributes)
        assert attribute_difference.issubset(builtins.set(["archive_state", "controller_state", "is_stale"]))
        assert job_info.get("is_stale") == is_stale

        def check_has_spec():
            job_info = retry(lambda: get_job(op_id, job_id))
            assert job_info.get("has_spec") == has_spec

        if has_spec is not None:
            wait_no_assert(check_has_spec)


@pytest.mark.enabled_multidaemon
class _TestGetJobCommon(_TestGetJobBase):
    ENABLE_MULTIDAEMON = True

    def _run_consumption_job(
        self,
        *,
        command,
        cpu_limit,
        memory_limit,
        memory_reserve_factor,
        gpu_limit=0,
    ):
        task_patch = {
            "cpu_limit": cpu_limit,
            "memory_limit": memory_limit,
            "memory_reserve_factor": memory_reserve_factor,
        }
        if gpu_limit:
            task_patch["gpu_limit"] = gpu_limit
            task_patch["enable_gpu_layers"] = False

        op = run_test_vanilla(
            command,
            track=True,
            task_patch=task_patch,
        )

        jobs = list_jobs(op.id, state="completed")["jobs"]
        assert len(jobs) == 1
        job_id = jobs[0]["id"]

        stats = get_job(op.id, job_id)["statistics"]
        consumption_stats = stats["resource_usage"]["consumption_running"]
        initial_stats = stats["resource_usage"]["initial"]
        # User job memory reserve + 128mb footprint(for job proxy and other stuff)
        initial_memory_bytes = int(memory_limit * memory_reserve_factor + 128 * 1024 * 1024)
        return consumption_stats, initial_stats, initial_memory_bytes

    @staticmethod
    def _assert_consumption_total(value: int, time_ms: int, units_per_ms: int, units_divisor: int, metric_name: str):
        assert time_ms > 0, f"Running time for {metric_name} consumption should be positive, got {time_ms}"
        assert units_per_ms >= 0, f"{metric_name} usage per millisecond must be non-negative, got {units_per_ms}"
        # Underlying time is in microseconds and floored to milliseconds, so we need to account for that.
        expected_min = (time_ms * units_per_ms) // units_divisor
        expected_max = ((time_ms + 1) * units_per_ms) // units_divisor
        assert expected_min <= value <= expected_max, \
            f"{metric_name} consumption {value} outside [{expected_min}, {expected_max}]. time_ms: {time_ms}, units_per_ms: {units_per_ms}, units_divisor: {units_divisor}"

    @staticmethod
    def _assert_cpu_consumption(running_stats, initial_stats):
        _TestGetJobCommon._assert_consumption_total(
            running_stats["cpu"]["sum"],
            running_stats["time"]["sum"],
            initial_stats["cpu"]["sum"],
            1,
            "cpu",
        )

    @staticmethod
    def _assert_vcpu_consumption(running_stats, initial_stats):
        _TestGetJobCommon._assert_consumption_total(
            running_stats["vcpu"]["sum"],
            running_stats["time"]["sum"],
            initial_stats["vcpu"]["sum"],
            1,
            "vcpu",
        )

    @staticmethod
    def _assert_gpu_consumption(running_stats, initial_stats):
        _TestGetJobCommon._assert_consumption_total(
            running_stats["gpu"]["sum"],
            running_stats["time"]["sum"],
            initial_stats["gpu"]["sum"],
            1,
            "gpu",
        )

    @staticmethod
    def _assert_memory_consumption(running_stats, initial_stats):
        # This is in mb_sec instead of bytes_ms, so we need to divide by 1024 * 1024 for mb -> bytes and by 1000 for ms -> sec.
        _TestGetJobCommon._assert_consumption_total(
            running_stats["user_memory"]["sum"],
            running_stats["time"]["sum"],
            initial_stats["user_memory"]["sum"],
            MB_SEC_TO_BYTES_MS_DIVISOR,
            "user_memory",
        )

    def _assert_completed_jobs_have_consumption(self, op_id):
        jobs = list_jobs(op_id)["jobs"]
        assert jobs, "Operation produced no jobs"
        completed_found = False
        for job_info in jobs:
            if job_info["state"] != "completed":
                continue
            stats = get_job(op_id, job_info["id"])["statistics"]
            resource_usage_stats = stats["resource_usage"]
            running = resource_usage_stats["consumption_running"]
            initial = resource_usage_stats["initial"]
            assert running["cpu"]["sum"] > 0, "CPU consumption should be positive"
            assert running["time"]["sum"] > 0, "Running time should be positive"
            assert running["user_memory"]["sum"] > 0, "Memory consumption should be positive"
            assert initial["cpu"]["sum"] > 0, "Initial CPU usage should be positive"
            completed_found = True
        assert completed_found, "No completed job found for operation"

    @authors("omgronny")
    def test_get_job(self):
        create_pool("my_pool")
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])
        before_start_time = datetime.datetime.utcnow()
        op = map(
            track=False,
            label="get_job",
            in_="//tmp/t1",
            out="//tmp/t2",
            spec={
                "scheduling_options_per_pool_tree": {
                    "default": {"pool": "my_pool"},
                },
                "mapper": {
                    "monitoring": {
                        "enable": True,
                        "sensor_names": ["cpu/user"],
                    },
                },
            },
            command=with_breakpoint(
                """
                echo SOME-STDERR >&2 ;
                cat ;
                if [[ "$YT_JOB_INDEX" == "0" ]]; then
                    BREAKPOINT
                    exit 1
                fi
            """
            ),
            fail_fast=False,
        )
        (job_id,) = wait_breakpoint()

        self._check_get_job(op.id, job_id, before_start_time, state="running", has_spec=None,
                            pool="my_pool", pool_tree="default")

        @wait_no_assert
        def correct_stderr_size():
            job_info = retry(lambda: get_job(op.id, job_id))
            assert job_info.get("stderr_size", 0) == len("SOME-STDERR\n")

        release_breakpoint()
        op.track()

        self._check_get_job(op.id, job_id, before_start_time, state="failed", has_spec=True,
                            pool="my_pool", pool_tree="default")

        job_info = retry(lambda: get_job(op.id, job_id))
        assert job_info["fail_context_size"] > 0
        events = job_info["events"]
        assert len(events) > 0
        assert all(field in events[0] for field in ["phase", "state", "time"])

        delete_job_from_archive(op.id, job_id)

        # Controller agent must be able to respond as it stores
        # zombie operation orchids.
        self._check_get_job(op.id, job_id, before_start_time, state="failed", has_spec=None)

    # Job consumption tracking tests
    # These tests verify that resource consumption (CPU, vCPU, memory, GPU) is tracked during job execution.
    # Metrics are exposed at /resource_usage/consumption_running/{cpu,vcpu,user_memory,gpu}
    # and the initial demand snapshot at /resource_usage/initial/{cpu,vcpu,user_memory,gpu}.

    RESOURCE_SCENARIOS = (
        pytest.param(
            {
                "sleep_s": 0.3,
                "cpu_limit": 1.0,
                "memory_limit": 512 * 1024 * 1024,
                "memory_reserve_factor": 0.5,
                "gpu_limit": 0,
            },
            id="cpu-default",
        ),
        pytest.param(
            {
                "sleep_s": 0.2,
                "cpu_limit": 0.1,
                "memory_limit": 512 * 1024 * 1024,
                "memory_reserve_factor": 0.1,
                "gpu_limit": 0,
            },
            id="minimal-cpu",
        ),
        pytest.param(
            {
                "sleep_s": 0.4,
                "cpu_limit": 1.0,
                "memory_limit": 512 * 1024 * 1024,
                "memory_reserve_factor": 0.8,
                "gpu_limit": 2,
            },
            id="gpu",
        ),
    )

    @authors("aleksandr.gaev")
    @pytest.mark.parametrize("scenario", RESOURCE_SCENARIOS)
    def test_job_consumption_resource_matrix(self, scenario):
        """Verify resource consumption accounting across CPU, vCPU, memory, and GPU."""
        command = f"sleep {scenario['sleep_s']}"
        running, initial, initial_memory_bytes = self._run_consumption_job(
            command=command,
            cpu_limit=scenario["cpu_limit"],
            memory_limit=scenario["memory_limit"],
            memory_reserve_factor=scenario["memory_reserve_factor"],
            gpu_limit=scenario["gpu_limit"],
        )

        time_ms = running["time"]["sum"]
        minimal_expected_time_ms = scenario["sleep_s"] * 1000
        maximum_expected_time_ms = minimal_expected_time_ms + 200
        assert time_ms >= minimal_expected_time_ms, \
            f"Time {time_ms}ms should be at least {minimal_expected_time_ms}ms"
        assert time_ms <= maximum_expected_time_ms, \
            f"Time {time_ms}ms should be at most {maximum_expected_time_ms}ms"

        expected_cpu_millicores = int(round(scenario["cpu_limit"] * 1000))
        expected_gpu_units = int(scenario["gpu_limit"])

        assert initial["cpu"]["sum"] == expected_cpu_millicores, \
            f"CPU usage initially {initial['cpu']['sum']} != expected {expected_cpu_millicores}"
        assert initial["vcpu"]["sum"] == expected_cpu_millicores, \
            f"vCPU usage initially {initial['vcpu']['sum']} != expected {expected_cpu_millicores}"
        assert initial["user_memory"]["sum"] == initial_memory_bytes, \
            f"User memory initially {initial['user_memory']['sum']} != expected {initial_memory_bytes}"
        assert initial["gpu"]["sum"] == expected_gpu_units, \
            f"GPU usage initially {initial['gpu']['sum']} != expected {expected_gpu_units}"

        self._assert_cpu_consumption(running, initial)
        self._assert_vcpu_consumption(running, initial)
        self._assert_memory_consumption(running, initial)
        self._assert_gpu_consumption(running, initial)

    @authors("aleksandr.gaev")
    def test_job_consumption_increases_over_time(self):
        """Verify consumption increases while job is running."""
        cpu_limit = 1.0
        memory_limit_bytes = 512 * 1024 * 1024
        memory_reserve_factor = 0.5

        op = run_test_vanilla(
            with_breakpoint("sleep 0.5; BREAKPOINT; sleep 0.5"),
            track=False,
            task_patch={
                "cpu_limit": cpu_limit,
                "memory_limit": memory_limit_bytes,
                "memory_reserve_factor": memory_reserve_factor,
            },
        )

        (job_id,) = wait_breakpoint()

        # Wait for initial consumption to accumulate
        def has_consumption():
            try:
                job_info = get_job(op.id, job_id)
                stats = job_info.get("statistics", {})
                resource_usage = stats.get("resource_usage", {})
                running = resource_usage.get("consumption_running", {})
                return running.get("cpu", {}).get("sum", 0) > 0
            except Exception:
                return False

        wait(has_consumption, timeout=30)

        # Get consumption while running
        stats_t1 = get_job(op.id, job_id)["statistics"]
        resource_usage_t1 = stats_t1["resource_usage"]
        running_t1 = resource_usage_t1["consumption_running"]
        initial = resource_usage_t1["initial"]

        cpu_t1 = running_t1["cpu"]["sum"]
        memory_t1 = running_t1["user_memory"]["sum"]
        time_t1 = running_t1["time"]["sum"]

        assert cpu_t1 > 0
        assert memory_t1 > 0
        assert time_t1 > 0

        # Wait a bit more
        time.sleep(0.3)

        # Get consumption again while still running
        stats_t2 = get_job(op.id, job_id)["statistics"]
        resource_usage_t2 = stats_t2["resource_usage"]
        running_t2 = resource_usage_t2["consumption_running"]

        assert resource_usage_t2["initial"] == initial, "Initial usage should never change"

        cpu_t2 = running_t2["cpu"]["sum"]
        memory_t2 = running_t2["user_memory"]["sum"]
        time_t2 = running_t2["time"]["sum"]

        # Consumption should increase over time
        assert cpu_t2 > cpu_t1
        assert memory_t2 > memory_t1
        assert time_t2 > time_t1

        # Complete the job
        release_breakpoint()
        op.track()

        # Final consumption should be even higher
        stats_final = get_job(op.id, job_id)["statistics"]
        resource_usage_final = stats_final["resource_usage"]
        running_final = resource_usage_final["consumption_running"]
        initial_final = resource_usage_final["initial"]

        assert initial_final == initial, "Initial usage should not change after job completion"

        cpu_final = running_final["cpu"]["sum"]

        assert cpu_final > cpu_t2
        self._assert_cpu_consumption(running_final, initial_final)
        self._assert_vcpu_consumption(running_final, initial_final)
        self._assert_memory_consumption(running_final, initial_final)
        self._assert_gpu_consumption(running_final, initial_final)

    @authors("krock21")
    def test_job_consumption_tracks_high_memory_usage_with_low_reserve(self):
        """Verify consumption reflects high memory usage even with a small reserve."""
        cpu_limit = 1.0
        memory_limit_bytes = 1024 * 1024 * 1024
        memory_reserve_factor = 0.1

        # Need to give some time to get correct average value for memory. Deleting buf to verify that consumption doesn't go down after that.
        command = """python3 - <<'PY'
import time

buf = bytearray(800 * 1024 * 1024)
time.sleep(10.0)
del buf
time.sleep(10.0)
PY
"""

        running, initial, _ = self._run_consumption_job(
            command=command,
            cpu_limit=cpu_limit,
            memory_limit=memory_limit_bytes,
            memory_reserve_factor=memory_reserve_factor,
            gpu_limit=0,
        )

        self._assert_cpu_consumption(running, initial)
        self._assert_vcpu_consumption(running, initial)
        self._assert_gpu_consumption(running, initial)

        # A job should increase memory usage up to 928mb (800mb user_job + 128mb ahead reserve)

        running_time = running["time"]["sum"]
        memory_consumption = running["user_memory"]["sum"]
        average_memory_consumption = (memory_consumption * 1000) // running_time
        assert 800 <= average_memory_consumption <= 950, (
            "Memory consumption did not reflect high usage. "
            f"Job was expected to consume 800mb and worked for {running_time/1000}s, "
            f"but average consumption is {average_memory_consumption}"
        )

    TERMINAL_SCENARIOS = (
        pytest.param(
            {
                "mode": "failed_breakpoint",
                "use_breakpoint": True,
                "command_template": "BREAKPOINT; sleep 0.3; exit 1",
                "spec": {"fail_on_job_restart": False},
                "expected_state": "failed",
                "sleep_s": 0.3,
                "memory_reserve_factor": 0.5,
                "should_abort": False,
            },
            id="failed",
        ),
        pytest.param(
            {
                "mode": "aborted_breakpoint",
                "use_breakpoint": True,
                "command_template": "BREAKPOINT; sleep 5",
                "spec": {},
                "expected_state": "aborted",
                "sleep_s": 0.0,
                "memory_reserve_factor": 0.5,
                "should_abort": True,
            },
            id="aborted",
        ),
        pytest.param(
            {
                "mode": "failed_no_breakpoint",
                "use_breakpoint": False,
                "command": "sleep 0.3; exit 1",
                "spec": {"fail_on_job_restart": False},
                "expected_state": "failed",
                "sleep_s": 0.3,
                "memory_reserve_factor": 0.5,
                "should_abort": False,
            },
            id="failed-no-breakpoint",
        ),
    )

    @authors("aleksandr.gaev")
    @pytest.mark.parametrize("scenario", TERMINAL_SCENARIOS)
    def test_job_consumption_terminal_states(self, scenario):
        """Verify consumption persists for failed and aborted jobs."""
        cpu_limit = 1.0
        memory_limit_bytes = 512 * 1024 * 1024
        memory_reserve_factor = scenario["memory_reserve_factor"]

        task_patch = {
            "cpu_limit": cpu_limit,
            "memory_limit": memory_limit_bytes,
            "memory_reserve_factor": memory_reserve_factor,
        }

        if scenario["use_breakpoint"]:
            op = run_test_vanilla(
                with_breakpoint(scenario["command_template"]),
                track=False,
                spec=scenario.get("spec"),
                task_patch=task_patch,
            )

            (job_id,) = wait_breakpoint()

            def has_consumption():
                try:
                    stats = get_job(op.id, job_id).get("statistics", {})
                    resource_usage = stats.get("resource_usage", {})
                    running = resource_usage.get("consumption_running", {})
                    return running.get("cpu", {}).get("sum", 0) > 0
                except Exception:
                    return False

            wait(has_consumption, timeout=30)

            resource_usage_before = get_job(op.id, job_id)["statistics"]["resource_usage"]
            running_before = resource_usage_before["consumption_running"]
            initial = resource_usage_before["initial"]
            cpu_before = running_before["cpu"]["sum"]
            time_before = running_before["time"]["sum"]

            if scenario["should_abort"]:
                abort_job(job_id)
            release_breakpoint()
            if scenario["should_abort"]:
                op.track()
            else:
                with raises_yt_error(yt_error_codes.MaxFailedJobsLimitExceeded):
                    op.track()

            job_info = get_job(op.id, job_id)
            assert job_info["state"] == scenario["expected_state"]

            stats = job_info["statistics"]
            resource_usage = stats["resource_usage"]
            running = resource_usage["consumption_running"]
            initial = resource_usage["initial"]

            assert running["cpu"]["sum"] >= cpu_before, \
                f"CPU consumption should not decrease: {cpu_before} -> {running['cpu']['sum']}"
            assert running["time"]["sum"] >= time_before, \
                f"Running time should not decrease: {time_before} -> {running['time']['sum']}"

            minimal_expected_time_ms = scenario["sleep_s"] * 1000
            assert running["time"]["sum"] >= minimal_expected_time_ms, \
                f"Time {running['time']['sum']}ms should be at least {minimal_expected_time_ms}ms"
        else:
            op = run_test_vanilla(
                scenario["command"],
                track=False,
                spec=scenario.get("spec"),
                task_patch=task_patch,
            )
            with raises_yt_error(yt_error_codes.MaxFailedJobsLimitExceeded):
                op.track()

            jobs = list_jobs(op.id)["jobs"]
            assert jobs, "Operation produced no jobs"
            job_id = jobs[0]["id"]
            job_info = get_job(op.id, job_id)
            assert job_info["state"] == scenario["expected_state"]
            stats = job_info["statistics"]
            resource_usage = stats["resource_usage"]
            running = resource_usage["consumption_running"]
            initial = resource_usage["initial"]

            minimal_expected_time_ms = scenario["sleep_s"] * 1000
            maximum_expected_time_ms = minimal_expected_time_ms + 200
            time_ms = running["time"]["sum"]
            assert time_ms >= minimal_expected_time_ms, \
                f"Time {time_ms}ms should be at least {minimal_expected_time_ms}ms"
            assert time_ms <= maximum_expected_time_ms, \
                f"Time {time_ms}ms should be at most {maximum_expected_time_ms}ms"

        self._assert_cpu_consumption(running, initial)
        self._assert_vcpu_consumption(running, initial)
        self._assert_memory_consumption(running, initial)
        self._assert_gpu_consumption(running, initial)

    @authors("aleksandr.gaev")
    def test_merge_job_consumption(self):
        create("table", "//tmp/consumption_merge_in")
        create("table", "//tmp/consumption_merge_out")
        write_table("//tmp/consumption_merge_in", [{"key": i, "value": "x" * 100} for i in range(50)])

        merge_op = merge(
            in_=["//tmp/consumption_merge_in"],
            out="//tmp/consumption_merge_out",
            spec={"force_transform": True},
            track=False,
        )
        merge_op.track()

        self._assert_completed_jobs_have_consumption(merge_op.id)

    @authors("aleksandr.gaev")
    def test_sort_job_consumption(self):
        create("table", "//tmp/consumption_sort_in")
        create("table", "//tmp/consumption_sort_out")
        write_table("//tmp/consumption_sort_in", [{"key": i, "value": "x" * 50} for i in range(40)])

        sort_op = sort(
            in_="//tmp/consumption_sort_in",
            out="//tmp/consumption_sort_out",
            sort_by=["key"],
            track=False,
        )
        sort_op.track()

        self._assert_completed_jobs_have_consumption(sort_op.id)

    @authors("aleksandr.gaev")
    def test_mapreduce_job_consumption(self):
        create("table", "//tmp/consumption_mr_in")
        create("table", "//tmp/consumption_mr_out")
        write_table("//tmp/consumption_mr_in", [{"key": i, "value": i} for i in range(30)])

        mr_op = map_reduce(
            mapper_command="cat",
            reducer_command="cat",
            in_="//tmp/consumption_mr_in",
            out="//tmp/consumption_mr_out",
            sort_by=["key"],
            track=False,
        )
        mr_op.track()

        self._assert_completed_jobs_have_consumption(mr_op.id)

    @authors("omgronny")
    def test_operation_ids_table(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])
        op = map(
            track=False,
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("cat; BREAKPOINT"),
        )
        job_id, = wait_breakpoint()
        release_breakpoint()
        op.track()

        def get_operation_id_from_archive(job_id):
            job_id_hi, job_id_lo = uuid_to_parts(job_id)
            rows = lookup_rows(OPERATION_IDS_TABLE, [{
                "job_id_hi": job_id_hi,
                "job_id_lo": job_id_lo,
            }])
            if not rows:
                return None
            return parts_to_uuid(rows[0]["operation_id_hi"], rows[0]["operation_id_lo"])

        wait(lambda: get_operation_id_from_archive(job_id) is not None)
        assert get_operation_id_from_archive(job_id) == op.id


class TestGetJob(_TestGetJobCommon):
    ENABLE_MULTIDAEMON = False  # There are component restarts.

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "operation_time_limit_check_period": 100,
            "snapshot_period": 500,
            "operations_update_period": 100,
            "job_reporter": {
                "reporting_period": 10,
                "min_repeat_delay": 10,
                "max_repeat_delay": 10,
            },
        }
    }

    DELTA_NODE_CONFIG = update(_TestGetJobBase.DELTA_NODE_CONFIG, {
        "exec_node": {
            "job_proxy": {
                "job_proxy_heartbeat_period":  100,
            },
        },
    })

    def _compare_time_from_archive_with_api(self, op_id, job_id, time_name):
        job_from_api = get_job(op_id, job_id)
        job_from_archive = get_job_from_archive(op_id, job_id)

        assert job_from_api.get(time_name) is not None
        assert job_from_archive.get("controller_" + time_name) is not None
        return date_string_to_timestamp_mcs(job_from_api[time_name]) == job_from_archive["controller_" + time_name]

    @authors("gritukan")
    def test_get_job_task_name_attribute_vanilla(self):
        op = vanilla(
            track=False,
            spec={
                "tasks": {
                    "master": {
                        "job_count": 1,
                        "command": with_breakpoint("BREAKPOINT", breakpoint_name="master"),
                    },
                    "slave": {
                        "job_count": 2,
                        "command": with_breakpoint("BREAKPOINT", breakpoint_name="slave"),
                    },
                },
            },
        )

        master_job_ids = wait_breakpoint(breakpoint_name="master", job_count=1)
        slave_job_ids = wait_breakpoint(breakpoint_name="slave", job_count=2)

        def check_task_names():
            for job_id in master_job_ids:
                job_info = retry(lambda: get_job(op.id, job_id))
                assert job_info["task_name"] == "master"
            for job_id in slave_job_ids:
                job_info = retry(lambda: get_job(op.id, job_id))
                assert job_info["task_name"] == "slave"

        check_task_names()

        release_breakpoint(breakpoint_name="master")
        release_breakpoint(breakpoint_name="slave")
        op.track()

        check_task_names()

    @authors("omgronny")
    def test_get_stubborn_job(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"foo": "bar"}, {"foo": "baz"}, {"foo": "qux"}])
        before_start_time = datetime.datetime.utcnow()
        op = map(
            track=False,
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("echo SOME-STDERR >&2; cat; BREAKPOINT"),
        )
        (job_id,) = wait_breakpoint()

        wait(lambda: get_job_from_archive(op.id, job_id) is not None)
        job_from_archive = get_job_from_archive(op.id, job_id)

        abort_job(job_id)
        release_breakpoint()
        op.track()

        # We emulate the situation when aborted (in CA's opinion) job
        # still reports "running" to archive.
        del job_from_archive["job_id_partition_hash"]
        del job_from_archive["operation_id_hash"]

        @wait_no_assert
        def _check_get_job():
            update_job_in_archive(op.id, job_id, job_from_archive)
            job_info = retry(lambda: get_job(op.id, job_id))
            assert job_info["job_id"] == job_id
            assert job_info["archive_state"] == "running"
            controller_state = job_info.get("controller_state")
            if controller_state is None:
                assert job_info["is_stale"]
            else:
                assert controller_state == "aborted"

        delete_job_from_archive(op.id, job_id)

        self._check_get_job(
            op.id,
            job_id,
            before_start_time,
            state="aborted",
            controller_state="aborted",
            has_spec=None,
        )
        job_info = retry(lambda: get_job(op.id, job_id))
        assert "archive_state" not in job_info

    @authors("omgronny")
    @pytest.mark.parametrize("should_abort", [True, False])
    def test_get_controller_state_from_archive(self, should_abort):
        op = run_test_vanilla(with_breakpoint("BREAKPOINT"))

        (job_id,) = wait_breakpoint()

        wait(lambda: get_job(op.id, job_id).get("controller_state") == "running")

        wait(lambda: get_controller_state_from_archive(op.id, job_id) == "running")

        if should_abort:
            abort_job(job_id)
        release_breakpoint()
        op.track()

        if should_abort:
            wait(lambda: get_controller_state_from_archive(op.id, job_id) == "aborted")
        else:
            wait(lambda: get_controller_state_from_archive(op.id, job_id) == "completed")

    @authors("faucct")
    def test_distributed_operation(self):
        op = vanilla(
            spec={
                "tasks": {
                    "master": {
                        "job_count": 1,
                        "command": ":",
                        "cookie_group_size": 2,
                    },
                },
            },
        )
        wait(lambda: len(op.list_jobs()) == 2)
        main, replica = sorted(
            [{"id": job, **get_job(op.id, job, attributes=["job_cookie_group_index", "main_job_id"])} for job in op.list_jobs()],
            key=lambda job: job["job_cookie_group_index"],
        )
        assert main["main_job_id"] == main["id"]
        assert main["job_cookie_group_index"] == 0
        assert replica["main_job_id"] == main["id"]
        assert replica["job_cookie_group_index"] == 1

    @authors("omgronny")
    def test_abort_vanished_jobs_in_archive(self):
        op = run_test_vanilla(with_breakpoint("BREAKPOINT"))

        (job_id,) = wait_breakpoint()

        wait(lambda: get_job(op.id, job_id).get("controller_state") == "running")

        with Restarter(self.Env, NODES_SERVICE):
            pass

        release_breakpoint()
        op.track()

        update_job_in_archive(op.id, job_id, {"transient_state": "running"})
        wait(lambda: get_controller_state_from_archive(op.id, job_id) == "aborted")

    @authors("arkady-e1ppa")
    @pytest.mark.parametrize("use_get_job", [True, False])
    def test_job_preemption_info_in_archive(self, use_get_job):
        update_controller_agent_config("enable_operation_progress_archivation", True)

        create_pool("research")
        create_pool("prod", attributes={"strong_guarantee_resources": {"cpu": 3}})

        op1 = run_test_vanilla(with_breakpoint("BREAKPOINT"), spec={"pool": "research"})
        job_id = wait_breakpoint(job_count=1)[0]

        op2 = run_sleeping_vanilla(spec={"pool": "prod"}, job_count=3)

        wait(lambda: op1.get_job_count(state="aborted") == 1)

        def get_interruption_info():
            if use_get_job:
                job = get_job(op1.id, job_id)
                print_debug(job)
                return job.get("interruption_info", None)

            return get_job_from_archive(op1.id, job_id).get("interruption_info", None)

        wait(lambda: get_interruption_info().get("interruption_reason", None) is not None, ignore_exceptions=True)
        interruption_info = get_interruption_info()
        print_debug(interruption_info)
        assert interruption_info.get("interruption_reason", None) == "preemption"
        preemption_reason = interruption_info.get("preemption_reason", None)
        assert preemption_reason.startswith("Preempted to start allocation") and \
            "of operation {}".format(op2.id) in preemption_reason

    @authors("omgronny")
    def test_not_found(self):
        with raises_yt_error(yt_error_codes.NoSuchOperation):
            get_job("1-2-3-4", "5-6-7-8")

        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=1,
        )
        (job_id,) = wait_breakpoint()

        with raises_yt_error(yt_error_codes.NoSuchJob):
            get_job(op.id, "5-6-7-8")

        release_breakpoint()
        op.track()

    @authors("omgronny")
    @flaky(max_runs=3)
    def test_get_job_is_stale_during_revival(self):
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=1,
            spec={"testing": {"delay_inside_revive": 5000}},
        )
        (job_id,) = wait_breakpoint()
        op.wait_for_fresh_snapshot()

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass

        with raises_yt_error(yt_error_codes.UncertainOperationControllerState):
            get_job(op.id, job_id)

        job_info = retry(lambda: get_job(op.id, job_id))
        assert job_info.get("controller_state") == "running"
        assert job_info.get("archive_state") == "running"
        assert not job_info.get("is_stale")

    @authors("omgronny")
    def test_get_job_is_stale(self):
        update_controller_agent_config("snapshot_period", 1000000)
        time.sleep(1)

        op = run_test_vanilla(with_breakpoint("BREAKPOINT"))
        (job_id,) = wait_breakpoint()

        with Restarter(self.Env, [CONTROLLER_AGENTS_SERVICE, NODES_SERVICE]):
            pass

        release_breakpoint()
        op.track()

        op.wait_for_state("completed")

        @wait_no_assert
        def check_job_state():
            job_info = retry(lambda: get_job(op.id, job_id))

            assert job_info.get("controller_state") == "running"
            assert job_info.get("archive_state") == "running"
            assert job_info.get("is_stale")

    @authors("omgronny")
    def test_job_archive_ttl(self):
        set("//sys/operations_archive/jobs/@max_data_ttl", 3000)

        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=1,
            task_patch={
                "archive_ttl": 10000000,
            },
        )
        (job_id,) = wait_breakpoint()
        release_breakpoint()

        time.sleep(5)

        get_job(op.id, job_id)

    @authors("aleksandr.gaev")
    def test_job_addresses(self):
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
        )
        (job_id,) = wait_breakpoint()

        def check_addresses(job_info):
            return "address" in job_info and "addresses" in job_info

        wait(lambda: check_addresses(get_job(op.id, job_id)))

        release_breakpoint()

        op.track()

        job_info = get_job(op.id, job_id)
        assert check_addresses(job_info)
        assert len(job_info.get("address")) > 0
        assert len(job_info.get("addresses")) > 0
        assert job_info.get("addresses")["default"] == job_info.get("address")

    @authors("bystrovserg")
    def test_controller_start_finish_time(self):
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
        )
        (job_id,) = wait_breakpoint()

        self._compare_time_from_archive_with_api(op.id, job_id, "start_time")
        release_breakpoint()
        op.track()

        self._compare_time_from_archive_with_api(op.id, job_id, "start_time")
        self._compare_time_from_archive_with_api(op.id, job_id, "finish_time")

    @authors("bystrovserg")
    def test_finish_time_on_broken_node(self):
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
        )
        (job_id,) = wait_breakpoint()

        with Restarter(self.Env, NODES_SERVICE):
            pass

        release_breakpoint()
        op.track()

        orchid_path = op.get_orchid_path()
        wait(lambda: exists(orchid_path))
        wait(lambda: len(get(orchid_path + "/retained_finished_jobs")) == 0)

        wait_for_cells()
        wait(lambda: self._compare_time_from_archive_with_api(op.id, job_id, "finish_time"), ignore_exceptions=True)

    @authors("bystrovserg")
    def test_gang_rank(self):
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=3,
            task_patch={
                "gang_options": {"size": 2},
            },
        )

        wait_breakpoint(job_count=3)

        jobs_with_rank = {}
        jobs_without_rank = []

        @wait_no_assert
        def get_job_ranks():
            wait(lambda: len(op.get_running_jobs()) == 3)
            running_jobs = op.get_running_jobs()
            nonlocal jobs_with_rank, jobs_without_rank
            jobs_with_rank = {job_id: info["gang_rank"] for job_id, info in running_jobs.items() if info.get("gang_rank") is not None}
            jobs_without_rank = [job_id for job_id, info in running_jobs.items() if info.get("gang_rank") is None]
            assert len(jobs_with_rank) == 2
            assert len(jobs_without_rank) == 1

        print_debug("Jobs with rank: {}".format(jobs_with_rank))
        print_debug("Jobs without rank: {}".format(jobs_without_rank))

        def check_job_ranks():
            for job_id, rank in jobs_with_rank.items():
                job = get_job(op.id, job_id, attributes=["gang_rank"])
                assert job["gang_rank"] == rank
            for job_id in jobs_without_rank:
                job = get_job(op.id, job_id)
                assert job.get("gang_rank") is None

        wait_no_assert(lambda: check_job_ranks())

        release_breakpoint()
        op.track()

        wait_no_assert(lambda: check_job_ranks())


@pytest.mark.enabled_multidaemon
class TestGetJobStatisticsLz4(_TestGetJobCommon):
    ENABLE_MULTIDAEMON = True

    DELTA_DYNAMIC_NODE_CONFIG = deepcopy(_TestGetJobBase.DELTA_DYNAMIC_NODE_CONFIG)
    DELTA_DYNAMIC_NODE_CONFIG["%true"]["exec_node"]["job_reporter"]["report_statistics_lz4"] = True


@pytest.mark.enabled_multidaemon
class TestGetJobMonitoring(_TestGetJobBase):
    ENABLE_MULTIDAEMON = True
    USE_PORTO = True

    @authors("omgronny")
    def test_get_job_monitoring(self):
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            job_count=1,
            task_patch={
                "monitoring": {
                    "enable": True,
                    "sensor_names": ["cpu/user"],
                },
            },
        )
        job_id, = wait_breakpoint()

        wait(lambda: "monitoring_descriptor" in get_job(op.id, job_id))

        job = get_job(op.id, job_id)
        descriptor = job["monitoring_descriptor"]

        wait(
            lambda: profiler_factory().at_node(job["address"]).get("user_job/cpu/user", {"job_descriptor": descriptor})
            is not None)


class TestGetJobAllocationBase(_TestGetJobBase):
    NUM_NODES = 1
    ENABLE_MULTIDAEMON = True

    DELTA_NODE_CONFIG = update(_TestGetJobBase.DELTA_NODE_CONFIG, {
        "job_resource_manager": {
            "resource_limits": {
                "user_slots": 1,
            },
        },
    })

    DELTA_DYNAMIC_NODE_CONFIG = update(_TestGetJobBase.DELTA_DYNAMIC_NODE_CONFIG, {
        "%true": {
            "exec_node": {
                "job_controller": {
                    "allocation": {
                        "enable_multiple_jobs": True,
                    },
                },
            },
        },
    })

    def _check_allocation_id(self, op_id, job_id, include_archive=False):
        wait(lambda: "allocation_id" in get_job(op_id, job_id))
        allocation_id_from_get_job = get_job(op_id, job_id)["allocation_id"]
        if include_archive:
            job_allocation_id_from_archive = get_allocation_id_from_archive(op_id, job_id)
            assert allocation_id_from_get_job == job_allocation_id_from_archive
        assert allocation_id_from_get_job == get_allocation_id_from_job_id(job_id)

    def _run_operation_and_check_allocation_id(self):
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
        )

        (job_id,) = wait_breakpoint()
        self._check_allocation_id(op.id, job_id, include_archive=False)
        release_breakpoint(job_id=job_id)
        op.track()
        return op, job_id


class TestGetJobAllocationWithoutArchive(TestGetJobAllocationBase):
    DELTA_CONTROLLER_AGENT_CONFIG = update(_TestGetJobBase.DELTA_CONTROLLER_AGENT_CONFIG, {
        # Disable archive by setting a high reporting_period
        "controller_agent": {
            "job_reporter": {
                "reporting_period": 1000000,
            },
        }
    })

    @authors("bystrovserg")
    def test_allocation_id_from_controller(self):
        self._run_operation_and_check_allocation_id()


class TestGetJobAllocation(TestGetJobAllocationBase):
    @authors("bystrovserg")
    def test_allocation_id(self):
        op, job_id = self._run_operation_and_check_allocation_id()
        self._check_allocation_id(op.id, job_id, include_archive=True)

    @authors("bystrovserg")
    def test_same_allocation(self):
        create("table", "//tmp/t_in", attributes={"replication_factor": 1})
        create("table", "//tmp/t_out", attributes={"replication_factor": 1})

        write_table("//tmp/t_in", [{"foo": "bar"}] * 2)

        op = map(
            wait_for_jobs=True,
            track=False,
            command=with_breakpoint("BREAKPOINT ; cat"),
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"data_size_per_job": 1, "enable_multiple_jobs_in_allocation": True},
        )

        job_ids = wait_breakpoint()
        assert len(job_ids) == 1

        job_id1 = job_ids[0]

        wait(lambda: "allocation_id" in get_job(op.id, job_id1))
        allocation_id_before = get_job(op.id, job_id1)["allocation_id"]

        release_breakpoint(job_id=job_id1)

        job_ids = wait_breakpoint()
        assert len(job_ids) == 1

        job_id2 = job_ids[0]

        assert job_id1 != job_id2

        wait(lambda: "allocation_id" in get_job(op.id, job_id1))
        assert allocation_id_before == get_job(op.id, job_id1)["allocation_id"]

        release_breakpoint()

        op.track()

        wait(lambda: "allocation_id" in get_job(op.id, job_id1))
        assert allocation_id_before == get_job(op.id, job_id1)["allocation_id"]


##################################################################


class TestGetJobRpcProxy(TestGetJob):
    USE_DYNAMIC_TABLES = True
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True


@pytest.mark.enabled_multidaemon
class TestGetJobStatisticsLz4RpcProxy(TestGetJobStatisticsLz4):
    ENABLE_MULTIDAEMON = True
    USE_DYNAMIC_TABLES = True
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True


class TestGetJobAllocationIdRpcProxy(TestGetJobAllocation):
    USE_DYNAMIC_TABLES = True
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    ENABLE_HTTP_PROXY = True
