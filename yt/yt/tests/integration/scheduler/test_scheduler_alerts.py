from yt_env_setup import YTEnvSetup

from yt_commands import (
    alter_table, authors, print_debug, wait, create, ls, get, set, sync_create_cells,
    remove, create_pool,
    read_table, write_table, map, map_reduce, run_test_vanilla, abort_job, get_singular_chunk_id, update_controller_agent_config, set_nodes_banned,
    create_test_tables)

from yt_type_helpers import make_schema

import yt.yson as yson

import pytest
from flaky import flaky

import string
import time

##################################################################


class TestSchedulerAlerts(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "alerts_update_period": 100,
            "watchers_update_period": 100,
            "fair_share_update_period": 100,
            # Unrecognized alert often interferes with the alerts that
            # are tested in this test suite.
            "enable_unrecognized_alert": False,
            "validate_node_tags_period": 100,
        }
    }

    @authors("ignat", "eshcherbin")
    def test_pools(self):
        wait(lambda: len(get("//sys/scheduler/@alerts")) == 0)

        create_pool(
            "good_pool",
            attributes={"min_share_resources": {"cpu": 1}}
        )
        wait(lambda: len(get("//sys/scheduler/@alerts")) == 0)

        # Incorrect pool configuration.
        create_pool(
            "bad_pool",
            attributes={"min_share_resources": {"cpu": 100}},
            wait_for_orchid=False,
        )
        wait(lambda: len(get("//sys/scheduler/@alerts")) == 1)

        set("//sys/pools/bad_pool/@min_share_resources/cpu", 0)
        wait(lambda: len(get("//sys/scheduler/@alerts")) == 0)

    @authors("ignat")
    def test_config(self):
        assert get("//sys/scheduler/@alerts") == []

        set("//sys/scheduler/config", {"fair_share_update_period": -100})
        wait(lambda: len(get("//sys/scheduler/@alerts")) == 1)

        set("//sys/scheduler/config", {})
        wait(lambda: get("//sys/scheduler/@alerts") == [])

    @authors("ignat")
    def test_cluster_directory(self):
        assert get("//sys/scheduler/@alerts") == []

        set("//sys/clusters/banach", {})

        wait(lambda: len(get("//sys/scheduler/@alerts")) == 1)

        set("//sys/clusters", {})

        wait(lambda: get("//sys/scheduler/@alerts") == [])

    @authors("ignat")
    def test_snapshot_loading_alert(self):
        controller_agent = ls("//sys/controller_agents/instances")[0]
        assert len(get("//sys/controller_agents/instances/{0}/@alerts".format(controller_agent))) == 0

        set("//sys/controller_agents/config", {"enable_snapshot_loading": False})

        wait(lambda: len(get("//sys/controller_agents/instances/{0}/@alerts".format(controller_agent))) == 1)

        set("//sys/controller_agents/config/enable_snapshot_loading", True)

        wait(lambda: len(get("//sys/controller_agents/instances/{0}/@alerts".format(controller_agent))) == 0)

    @authors("ignat")
    def test_nodes_without_pool_tree_alert(self):
        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) > 0

        set("//sys/cluster_nodes/{}/@user_tags".format(nodes[0]), ["my_tag"])
        set("//sys/pool_trees/default/@config/nodes_filter", "my_tag")

        wait(lambda: len(get("//sys/scheduler/@alerts")) == 1)

        alerts = get("//sys/scheduler/@alerts")
        attributes = alerts[0]["attributes"]
        assert attributes["alert_type"] == "nodes_without_pool_tree"
        assert len(attributes["node_addresses"]) == 2
        assert attributes["node_count"] == 2

        set("//sys/pool_trees/default/@config/nodes_filter", "")
        wait(lambda: len(get("//sys/scheduler/@alerts")) == 0)


##################################################################


class LowCpuUsageSchedulerAlertBase(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_NODES = 1
    USE_PORTO = True

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "scheduler_connector": {
                    "heartbeat_executor": {
                        "period": 200,  # 200 msec
                    },
                },
                "controller_agent_connector": {
                    "heartbeat_executor": {
                        "period": 200,  # 200 msec
                    }
                }
            }
        }
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "operations_update_period": 100,
        }
    }


class TestLowCpuUsageSchedulerAlertPresence(LowCpuUsageSchedulerAlertBase):
    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "operations_update_period": 100,
            "alert_manager": {
                "low_cpu_usage_alert_min_execution_time": 1,
                "low_cpu_usage_alert_min_average_job_time": 1,
                "low_cpu_usage_alert_cpu_usage_threshold": 0.6,
            },
        }
    }

    @authors("renadeen")
    @flaky(max_runs=3)
    def test_low_cpu_alert_presence(self):
        op = run_test_vanilla("sleep 3")
        op.track()

        assert "low_cpu_usage" in op.get_alerts()


class TestLowCpuUsageSchedulerAlertAbsence(LowCpuUsageSchedulerAlertBase):
    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "operations_update_period": 100,
            "alert_manager": {
                "low_cpu_usage_alert_min_execution_time": 1,
                "low_cpu_usage_alert_min_average_job_time": 1,
                "low_cpu_usage_alert_cpu_usage_threshold": 0.35,
            },
        }
    }

    @authors("renadeen")
    @flaky(max_runs=3)
    def test_low_cpu_alert_absence(self):
        op = run_test_vanilla(
            command='pids=""; for i in 1 2; do while : ; do : ; done & pids="$pids $!"; done; sleep 5; for p in $pids; do kill $p; done'
        )
        op.track()

        assert "low_cpu_usage" not in op.get_alerts()


class TestSchedulerOperationAlerts(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_NODES = 3
    USE_DYNAMIC_TABLES = True
    USE_PORTO = True

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "scheduler_connector": {
                    "heartbeat_executor": {
                        "period": 200,  # 200 msec
                    },
                },
                "controller_agent_connector": {
                    "heartbeat_executor": {
                        "period": 200,  # 200 msec
                    },
                    "settle_jobs_timeout": 30000,  # 30 sec
                },
            },
        },
    }

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "slot_manager": {"job_environment": {"block_io_watchdog_period": 100}},
        }
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "operations_update_period": 100,
            "schedule_allocation_time_limit": 3000,
            "event_log": {"flush_period": 1000},
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "iops_threshold": 50,
            "operations_update_period": 100,
            "alert_manager": {
                "period": 200,
                "tmpfs_alert_min_unused_space_threshold": 200,
                "tmpfs_alert_max_unused_space_ratio": 0.3,
                "aborted_jobs_alert_max_aborted_time": 100,
                "aborted_jobs_alert_max_aborted_time_ratio": 0.05,
                "intermediate_data_skew_alert_min_interquartile_range": 50,
                "intermediate_data_skew_alert_min_partition_size": 50,
                "short_jobs_alert_min_job_count": 3,
                "short_jobs_alert_min_job_duration": 5000,
                "short_jobs_alert_min_allowed_operation_duration_to_max_job_duration_ratio": 1.0,
                "operation_too_long_alert_min_wall_time": 0,
                "operation_too_long_alert_estimate_duration_threshold": 5000,
                "queue_average_wait_time_threshold": 1500,
            },
            "map_reduce_operation_options": {"min_uncompressed_block_size": 1},
            "event_log": {"flush_period": 1000},
        }
    }

    @authors("ignat")
    def test_unused_tmpfs_size_alert(self):
        create_test_tables()

        op = map(
            command="echo abcdef >local_file; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "mapper": {
                    "tmpfs_size": 5 * 1024 * 1024,
                    "tmpfs_path": ".",
                    "memory_reserve_factor": 0.9,
                },
            },
        )

        wait(lambda: "unused_tmpfs_space" in op.get_alerts())

        op = map(
            command="printf '=%.0s' {1..768} >local_file; sleep 3; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "mapper": {
                    "tmpfs_size": 1024,
                    "tmpfs_path": ".",
                    "memory_reserve_factor": 0.9,
                },
            },
        )

        assert "unused_tmpfs_space" not in op.get_alerts()

        update_controller_agent_config("alert_manager/tmpfs_alert_memory_usage_mute_ratio", 0.0)

        op = map(
            command="echo abcdef >local_file; sleep 1.5; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "mapper": {
                    "tmpfs_size": 5 * 1024 * 1024,
                    "tmpfs_path": ".",
                    "memory_reserve_factor": 0.9,
                }
            },
        )

        assert "unused_tmpfs_space" not in op.get_alerts()

    @authors("ignat")
    def test_unused_memory_alert(self):
        update_controller_agent_config("alert_manager/memory_usage_alert_max_unused_size", 1024 * 1024)

        create_test_tables()

        op = map(
            command="sleep 5",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "mapper": {
                    "memory_reserve_factor": 0.9,
                    "memory_limit": 1000 * 1024 * 1024,
                },
            },
        )

        wait(lambda: "unused_memory" in op.get_alerts())

        update_controller_agent_config("alert_manager/memory_usage_alert_max_unused_ratio", 1.0)

        op = map(
            command="echo abcdef >local_file; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "mapper": {
                    "memory_reserve_factor": 0.9,
                    "memory_limit": 1000 * 1024 * 1024,
                },
            },
        )

        assert "unused_memory" not in op.get_alerts()

    @authors("ignat")
    @pytest.mark.parametrize("dynamic", [False, True])
    def test_missing_input_chunks_alert(self, dynamic):
        sync_create_cells(1)

        create_test_tables(attributes={
            "replication_factor": 1,
            "schema": make_schema([{"name": "x", "type": "string", "sort_order": "ascending"}, {"name": "y", "type": "string"}], unique_keys=True),
        })

        if dynamic:
            alter_table("//tmp/t_in", dynamic=True)

        chunk_id = get_singular_chunk_id("//tmp/t_in")
        replicas = get("#{0}/@stored_replicas".format(chunk_id))
        set_nodes_banned(replicas, True)

        op = map(
            command="sleep 1.5; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "unavailable_chunk_strategy": "wait",
                "unavailable_chunk_tactics": "wait",
            },
            track=False,
        )

        expected_operation_state = "materializing" if dynamic else "running"
        wait(lambda: op.get_state() == expected_operation_state)
        wait(lambda: "lost_input_chunks" in op.get_alerts())

        set_nodes_banned(replicas, False)

        wait(lambda: op.get_state() == "running")
        wait(lambda: "lost_input_chunks" not in op.get_alerts())

    @authors("ignat")
    @pytest.mark.skipif("True", reason="YT-6717")
    def test_woodpecker_jobs_alert(self):
        create_test_tables(row_count=7)

        cmd = (
            "set -e; echo aaa >local_file; for i in {1..200}; do "
            "dd if=./local_file of=/dev/null iflag=direct bs=1M count=1; done;"
        )

        op = map(
            command=cmd,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"data_size_per_job": 1, "resource_limits": {"user_slots": 1}},
        )

        assert "excessive_disk_usage" in op.get_alerts()

    @authors("ignat")
    @pytest.mark.parametrize("operation_type", ["map", "vanilla"])
    def test_long_aborted_jobs_alert(self, operation_type):
        if operation_type == "map":
            create_test_tables(row_count=5)

            op = map(
                command="sleep 3; cat",
                in_="//tmp/t_in",
                out="//tmp/t_out",
                spec={"data_size_per_job": 1},
                track=False,
            )
        else:
            op = run_test_vanilla("sleep 3", job_count=5)

        self.wait_for_running_jobs(op)

        # Here we do need time.sleep in order to make aborted jobs long enough
        time.sleep(2)

        for job in op.get_running_jobs():
            abort_job(job)

        op.track()

        # For debug purposes.
        print_debug(
            "Job statistics for",
            op.id,
            ":",
            yson.dumps(get(op.get_path() + "/@progress/job_statistics_v2"), yson_format="text"),
        )

        if operation_type == "map":
            assert "long_aborted_jobs" in op.get_alerts()
        else:
            assert "long_aborted_jobs" not in op.get_alerts()

    @authors("renadeen")
    def test_operation_too_long_alert(self):
        create_test_tables(row_count=100)
        op = map(
            command="sleep 100; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"data_size_per_job": 1},
            track=False,
        )

        self.wait_for_running_jobs(op)
        wait(lambda: "operation_too_long" in op.get_alerts())

    @authors("ignat")
    def test_intermediate_data_skew_alert(self):
        create("table", "//tmp/t_in")

        multiplier = 1
        data = []
        for letter in ["a", "b", "c", "d", "e"]:
            data.extend([{"x": letter} for _ in range(multiplier)])
            multiplier *= 10

        write_table("//tmp/t_in", data)

        create("table", "//tmp/t_out")

        op = map_reduce(
            mapper_command="cat",
            reducer_command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            sort_by=["x"],
            spec={"partition_count": 5},
        )

        assert "intermediate_data_skew" in op.get_alerts()

    @authors("ignat")
    @flaky(max_runs=3)
    def test_short_jobs_alert(self):
        create_test_tables(row_count=4)

        update_controller_agent_config("alert_manager/short_jobs_alert_min_job_duration", 60000)
        op = map(
            command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "data_size_per_job": 1,
            },
        )

        assert "short_jobs_duration" in op.get_alerts()

        update_controller_agent_config("alert_manager/short_jobs_alert_min_job_duration", 5000)
        op = map(
            command="sleep 5; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "data_size_per_job": 1,
            },
        )

        assert "short_jobs_duration" not in op.get_alerts()

    @authors("ignat")
    def test_schedule_job_timed_out_alert(self):
        create_test_tables()

        testing_options = {"schedule_job_delay": {"duration": 3500, "type": "async"}}

        op = map(
            command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "testing": testing_options,
            },
            track=False,
        )

        wait(lambda: "schedule_job_timed_out" in op.get_alerts())

    @authors("ignat")
    def test_event_log(self):
        create_test_tables()

        op = map(
            command="echo abcdef >local_file; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "mapper": {
                    "tmpfs_size": 5 * 1024 * 1024,
                    "tmpfs_path": ".",
                    "memory_reserve_factor": 0.9,
                },
            },
        )

        wait(lambda: "unused_tmpfs_space" in op.get_alerts())

        def check():
            events = read_table("//sys/scheduler/event_log")
            for event in events:
                if event["event_type"] == "operation_completed" and event["operation_id"] == op.id:
                    assert len(event["alerts"]) >= 1
                    assert "unused_tmpfs_space" in event["alerts"]
                    return True
            return False

        wait(check)

    @authors("eshcherbin")
    def test_high_queue_total_time_estimate(self):
        op = run_test_vanilla(
            command="echo 'pass' >/dev/null",
            spec={
                "testing": {
                    "get_job_spec_delay": 3000,
                }
            },
            job_count=30,
        )

        wait(lambda: "high_queue_total_time_estimate" in op.get_alerts())

        op.abort()

    def wait_for_running_jobs(self, operation):
        wait(lambda: operation.get_job_count("running") >= 1)


##################################################################


class TestSchedulerJobSpecThrottlerOperationAlert(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "operations_update_period": 100,
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "operations_update_period": 100,
            "heavy_job_spec_slice_count_threshold": 1,
            "job_spec_slice_throttler": {"limit": 1, "period": 1000},
            "alert_manager": {
                "period": 200,
                "job_spec_throttling_alert_activation_count_threshold": 1,
            },
        }
    }

    @authors("ignat")
    def test_job_spec_throttler_operation_alert(self):
        create("table", "//tmp/t_in")
        for letter in string.ascii_lowercase:
            write_table("<append=%true>//tmp/t_in", [{"x": letter}])

        create("table", "//tmp/t_out")

        op = map(
            command="sleep 100; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"job_count": 3},
            track=False,
        )

        wait(lambda: "excessive_job_spec_throttling" in op.get_alerts())


##################################################################


class TestControllerAgentAlerts(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1

    @authors("ignat")
    def test_unrecognized_options_alert(self):
        agents = ls("//sys/controller_agents/instances")
        assert len(agents) == 1

        agent_path = "//sys/controller_agents/instances/" + agents[0]
        wait(lambda: get(agent_path + "/@alerts", None) == [])

        set("//sys/controller_agents/config", {"unknown_option": 10})
        wait(lambda: len(get(agent_path + "/@alerts")) == 1)

    @authors("ignat")
    def test_incorrect_config(self):
        agents = ls("//sys/controller_agents/instances")
        assert len(agents) == 1

        agent_path = "//sys/controller_agents/instances/" + agents[0]
        wait(lambda: get(agent_path + "/@alerts", default=None) == [])

        remove("//sys/controller_agents/config")
        set("//sys/controller_agents/config", [])
        wait(lambda: len(get(agent_path + "/@alerts")) == 1)
