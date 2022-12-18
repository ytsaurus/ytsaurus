from yt_env_setup import YTEnvSetup, Restarter, CONTROLLER_AGENTS_SERVICE
from yt_commands import (
    authors, clean_operations, wait, wait_breakpoint, release_breakpoint, with_breakpoint, create,
    remove, abort_job, ls,
    vanilla,
    write_table, map, get_operation, sync_create_cells, raises_yt_error, disable_scheduler_jobs_on_node)

import yt.environment.init_operation_archive as init_operation_archive

import time

##################################################################


class Features(object):
    """
    A class representing a single point with its features and tags.
    """
    def __init__(self, entry):
        assert "features" in entry
        self.features = entry["features"]
        assert "tags" in entry
        self.tags = entry["tags"]

    def __getitem__(self, item):
        return self.features[item]


class ControllerFeatures(object):
    """
    A class representing a point corresponding to a whole operation
    and arbitrary number of points representing individual tasks within it.
    """
    def __init__(self, operation_id):
        self.operation_id = operation_id

        # Operation-level features.
        self.operation = None
        # Task-level features per task_name.
        self.task = dict()

        entries = get_operation(operation_id, attributes=["controller_features"], verbose=False)["controller_features"]

        assert len(entries) >= 1

        for entry in entries:
            assert "tags" in entry
            if "task_name" in entry["tags"]:
                task_name = entry["tags"]["task_name"]
                assert task_name not in self.task
                self.task[task_name] = Features(entry)
            else:
                assert self.operation is None
                self.operation = Features(entry)

        assert self.operation is not None


class TestControllerFeatures(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_CONTROLLER_AGENTS = 1
    USE_DYNAMIC_TABLES = True

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "operations_update_period": 10,
            "operations_cleaner": {
                "enable": False,
                "analysis_period": 100,
                # Cleanup all operations
                "hard_retained_operation_count": 0,
                "clean_delay": 0,
            },
            "enable_job_reporter": True,
            "enable_job_spec_reporter": True,
            "enable_job_stderr_reporter": True,
        },
    }

    def setup_method(self, method):
        super(TestControllerFeatures, self).setup_method(method)
        sync_create_cells(1)
        init_operation_archive.create_tables_latest_version(
            self.Env.create_native_client(), override_tablet_cell_bundle="default"
        )

    def teardown_method(self, method):
        remove("//sys/operations_archive", force=True)
        super(TestControllerFeatures, self).teardown_method(method)

    @authors("alexkolodezny")
    def test_controller_features(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"key": 1}])

        op = map(
            in_=["//tmp/t_in"],
            out=[],
            command="sleep 1",
        )

        def check_features(op):
            features = ControllerFeatures(op.id)
            assert features.task["map"]["wall_time"] > 1000
            assert features.operation["wall_time"] > 1000

        check_features(op)

        op = map(
            in_=["//tmp/t_in"],
            out=[],
            command="sleep 1 && exit 1",
            spec={
                "max_failed_job_count": 0,
            },
            track=False,
        )

        with raises_yt_error("Process exited with code 1"):
            op.track()

        check_features(op)

        op = map(
            in_=["//tmp/t_in"],
            out=[],
            command="sleep 1",
        )

        clean_operations()

        check_features(op)

        op = map(
            in_=["//tmp/t_in"],
            out=[],
            command="sleep 1 && exit 1",
            spec={
                "max_failed_job_count": 0,
            },
            track=False,
        )

        with raises_yt_error("Process exited with code 1"):
            op.track()

        clean_operations()

        check_features(op)

    @authors("alexkolodezny")
    def test_various_controller_features(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", [{"key": 1}])

        op = map(
            in_=["//tmp/t_in"],
            out=[],
            command="sleep 1",
            spec={
                "job_count": 1,
            },
        )

        features = ControllerFeatures(op.id)
        assert features.operation["operation_count"] == 1.0
        assert features.operation["job_count.completed.non-interrupted"] == 1.0
        assert features.operation.tags["operation_type"] == "map"
        assert features.operation.tags["total_job_count"] == 1
        assert "total_estimated_input_data_weight" in features.operation.tags
        assert "total_estimated_input_row_count" in features.operation.tags
        assert "authenticated_user" in features.operation.tags
        assert "authenticated_user" in features.task["map"].tags
        assert "total_input_data_weight" in features.task["map"].tags
        assert "total_input_row_count" in features.task["map"].tags
        assert features.task["map"].tags["total_job_count"] == 1

        op = map(
            in_=["//tmp/t_in"],
            out=[],
            command="sleep 1 && exit 1",
            spec={
                "max_failed_job_count": 0,
            },
            track=False,
        )

        with raises_yt_error("Process exited with code 1"):
            op.track()

        features = ControllerFeatures(op.id)
        assert features.operation["operation_count"] == 1.0
        assert features.operation["job_count.failed"] == 1.0
        assert features.operation.tags["total_job_count"] == 1

        op = map(
            in_=["//tmp/t_in"],
            out=[],
            command=with_breakpoint("BREAKPOINT"),
            track=False,
            spec={
                "max_failed_job_count": 0,
            },
        )

        wait_breakpoint()

        for job in op.get_running_jobs():
            abort_job(job)

        release_breakpoint()

        op.track()

        features = ControllerFeatures(op.id)
        assert features.operation["operation_count"] == 1.0
        assert features.operation["job_count.aborted.scheduled.user_request"] == 1.0
        assert features.operation.tags["total_job_count"] == 1


class TestJobStatisticFeatures(YTEnvSetup):
    NUM_SCHEDULERS = 1
    USE_PORTO = True

    @authors("alexkolodezny")
    def test_job_statistic_features(self):
        create("table", "//tmp/t1")
        create("table", "//tmp/t2")
        write_table("//tmp/t1", [{"a": "b"}])
        op = map(
            in_="//tmp/t1",
            out="//tmp/t2",
            command='cat; bash -c "for (( I=0 ; I<=100*1000 ; I++ )) ; do echo $(( I+I*I )); done; sleep 2" >/dev/null && sleep 2',
        )

        def check_statistic():
            features = ControllerFeatures(op.id)
            check = True
            for component in ["user_job", "job_proxy"]:
                component_check = features.task["map"]["job_statistics." + component + ".cpu.user.completed.sum"] > 0 and \
                    features.task["map"]["job_statistics." + component + ".cpu.system.completed.sum"] > 0 and \
                    features.task["map"]["job_statistics." + component + ".cpu.context_switches.completed.sum"] is not None and \
                    features.task["map"]["job_statistics." + component + ".cpu.peak_thread_count.completed.max"] is not None and \
                    features.task["map"]["job_statistics." + component + ".cpu.wait.completed.sum"] is not None and \
                    features.task["map"]["job_statistics." + component + ".cpu.throttled.completed.sum"] is not None and \
                    features.task["map"]["job_statistics." + component + ".block_io.bytes_read.completed.sum"] is not None and \
                    features.task["map"]["job_statistics." + component + ".max_memory.completed.sum"] > 0 and \
                    features.task["map"]["job_statistics." + component + ".cpu.user.completed.avg"] > 0 and \
                    features.task["map"]["job_statistics." + component + ".cpu.system.completed.avg"] > 0 and \
                    features.task["map"]["job_statistics." + component + ".cpu.context_switches.completed.avg"] is not None and \
                    features.task["map"]["job_statistics." + component + ".cpu.peak_thread_count.completed.max"] is not None and \
                    features.task["map"]["job_statistics." + component + ".cpu.wait.completed.avg"] is not None and \
                    features.task["map"]["job_statistics." + component + ".cpu.throttled.completed.avg"] is not None and \
                    features.task["map"]["job_statistics." + component + ".block_io.bytes_read.completed.avg"] is not None and \
                    features.task["map"]["job_statistics." + component + ".max_memory.completed.avg"] > 0
                check = check and component_check

            check = check and features.task["map"]["job_statistics.user_job.cumulative_memory_mb_sec.completed.sum"] > 0
            check = check and features.task["map"]["job_statistics.user_job.cumulative_memory_mb_sec.completed.avg"] > 0
            return check

        wait(check_statistic)


class TestPendingTimeFeatures(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_CONTROLLER_AGENTS = 1
    NUM_NODES = 3

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 500,
        }
    }

    @authors("alexkolodezny")
    def test_pending_time_features(self):
        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) > 2
        for node in nodes[:2]:
            disable_scheduler_jobs_on_node(node, "test pending time features")

        op = vanilla(
            spec={
                "tasks": {
                    "task1": {
                        "job_count": 1,
                        "command": with_breakpoint("sleep 2; BREAKPOINT; sleep 2;"),
                    },
                },
            },
            track=False,
        )
        wait_breakpoint()
        op.wait_for_fresh_snapshot()

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            time.sleep(10)

        op.ensure_running()
        release_breakpoint()

        op.track()

        def get_per_task_times(op):
            features = ControllerFeatures(op.id)
            result = dict()
            for task_name, task in features.task.items():
                result[task_name] = task["ready_time"], task["exhaust_time"], task["wall_time"]
            return result

        ready_time, exhaust_time, wall_time = get_per_task_times(op)["task1"]

        assert ready_time < 500
        assert 4000 < exhaust_time < 17000
        assert wall_time > 14000

        op = vanilla(
            spec={
                "tasks": {
                    "task1": {
                        "job_count": 2,
                        "command": "sleep 3",
                    },
                },
            },
        )

        ready_time, exhaust_time, wall_time = get_per_task_times(op)["task1"]

        assert ready_time > 2000
        assert exhaust_time > 2000
        assert wall_time > 6000

        op = vanilla(
            spec={
                "tasks": {
                    "task1": {
                        "job_count": 1,
                        "command": "sleep 3",
                    },
                },
            },
        )

        ready_time, exhaust_time, wall_time = get_per_task_times(op)["task1"]

        assert ready_time < 500
        assert exhaust_time > 3000
        assert wall_time > 3000

        op = vanilla(
            spec={
                "tasks": {
                    "task1": {
                        "job_count": 1,
                        "command": "sleep 3",
                    },
                    "task2": {
                        "job_count": 1,
                        "command": "sleep 3",
                    }
                },
                "resource_limits": {
                    "user_slots": 1,
                },
            },
        )

        ready_time1, exhaust_time1, wall_time1 = get_per_task_times(op)["task1"]
        ready_time2, exhaust_time2, wall_time2 = get_per_task_times(op)["task2"]

        assert ready_time1 + ready_time2 < 500
        assert exhaust_time1 + exhaust_time2 > 6000
        assert wall_time1 + wall_time2 > 6000
