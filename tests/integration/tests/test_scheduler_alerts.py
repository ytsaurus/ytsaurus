import pytest
from flaky import flaky

from yt_env_setup import YTEnvSetup, unix_only, require_ytserver_root_privileges
from yt_commands import *

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
            # Unrecognized alert often interfers with the alerts that
            # are tested in this test suite.
            "enable_unrecognized_alert": False,
        }
    }

    def test_pools(self):
        assert get("//sys/scheduler/@alerts") == []

        # Incorrect pool configuration.
        create("map_node", "//sys/pools/poolA", attributes={"min_share_ratio": 2.0})
        wait(lambda: len(get("//sys/scheduler/@alerts")) == 1)

        set("//sys/pools/poolA/@min_share_ratio", 0.8)
        wait(lambda: get("//sys/scheduler/@alerts") == [])

        # Total min_share_ratio > 1.
        create("map_node", "//sys/pools/poolB", attributes={"min_share_ratio": 0.8})
        wait(lambda: len(get("//sys/scheduler/@alerts")) == 1)

        set("//sys/pools/poolA/@min_share_ratio", 0.1)
        wait(lambda: get("//sys/scheduler/@alerts") == [])

    def test_config(self):
        assert get("//sys/scheduler/@alerts") == []

        set("//sys/scheduler/config", {"fair_share_update_period": -100})
        wait(lambda: len(get("//sys/scheduler/@alerts")) == 1)

        set("//sys/scheduler/config", {})
        wait(lambda: get("//sys/scheduler/@alerts") == [])

    def test_cluster_directory(self):
        assert get("//sys/scheduler/@alerts") == []

        set("//sys/clusters/banach", {})

        wait(lambda: len(get("//sys/scheduler/@alerts")) == 1)

        set("//sys/clusters", {})

        wait(lambda: get("//sys/scheduler/@alerts") == [])

    def test_snapshot_loading_alert(self):
        controller_agent = ls("//sys/controller_agents/instances")[0]
        assert len(get("//sys/controller_agents/instances/{0}/@alerts".format(controller_agent))) == 0

        set("//sys/controller_agents/config", {"enable_snapshot_loading": False})

        wait(lambda: len(get("//sys/controller_agents/instances/{0}/@alerts".format(controller_agent))) == 1)

        set("//sys/controller_agents/config/enable_snapshot_loading", True)

        wait(lambda: len(get("//sys/controller_agents/instances/{0}/@alerts".format(controller_agent))) == 0)

##################################################################


@require_ytserver_root_privileges
class TestSchedulerOperationAlerts(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_NODES = 3

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "scheduler_connector": {
                "heartbeat_period": 200
            },
            "slot_manager": {
                "job_environment": {
                    "type": "cgroups",
                    "supported_cgroups": ["blkio", "cpu", "cpuacct"],
                    "block_io_watchdog_period": 100
                }
            }
        }
    }

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "operations_update_period": 100,
            "schedule_job_time_limit": 3000,
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "iops_threshold": 50,
            "operations_update_period": 100,
            "operation_progress_analysis_period": 200,
            "operation_alerts": {
                "tmpfs_alert_min_unused_space_threshold": 200,
                "tmpfs_alert_max_unused_space_ratio": 0.3,
                "aborted_jobs_alert_max_aborted_time": 100,
                "aborted_jobs_alert_aborted_time_ratio": 0.05,
                "intermediate_data_skew_alert_min_interquartile_range": 50,
                "intermediate_data_skew_alert_min_partition_size": 50,
                "short_jobs_alert_min_job_count": 3,
                "short_jobs_alert_min_job_duration": 5000,
                "low_cpu_usage_alert_min_execution_time": 1,
                "low_cpu_usage_alert_min_average_job_time": 1,
                "low_cpu_usage_alert_cpu_usage_threshold": 0.3,
                "operation_too_long_alert_min_wall_time": 0,
                "operation_too_long_alert_estimate_duration_threshold": 5000
            },
            "map_reduce_operation_options": {
                "min_uncompressed_block_size": 1
            }
        }
    }

    @unix_only
    def test_unused_tmpfs_size_alert(self):
        create_test_tables()

        op = map(
            command="echo abcdef >local_file; sleep 1.5; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "mapper": {
                    "tmpfs_size": 5 * 1024 * 1024,
                    "tmpfs_path": "."
                }
            })

        assert "unused_tmpfs_space" in op.get_alerts()

        op = map(
            command="printf '=%.0s' {1..768} >local_file; sleep 1.5; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "mapper": {
                    "tmpfs_size": 1024,
                    "tmpfs_path": "."
                }
            })

        assert "unused_tmpfs_space" not in op.get_alerts()

    def test_missing_input_chunks_alert(self):
        create_test_tables(attributes={"replication_factor": 1})

        chunk_ids = get("//tmp/t_in/@chunk_ids")
        assert len(chunk_ids) == 1

        replicas = get("#{0}/@stored_replicas".format(chunk_ids[0]))
        set_banned_flag(True, replicas)

        op = map(
            command="sleep 1.5; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "unavailable_chunk_strategy": "wait",
                "unavailable_chunk_tactics": "wait"
            },
            dont_track=True)

        wait(lambda: op.get_state() == "running")
        wait(lambda: "lost_input_chunks" in op.get_alerts())

        set_banned_flag(False, replicas)

        wait(lambda: "lost_input_chunks" not in op.get_alerts())

    @pytest.mark.skipif("True", reason="YT-6717")
    def test_woodpecker_jobs_alert(self):
        create_test_tables(row_count=7)

        cmd = "set -e; echo aaa >local_file; for i in {1..200}; do " \
              "dd if=./local_file of=/dev/null iflag=direct bs=1M count=1; done;"

        op = map(
            command=cmd,
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "data_size_per_job": 1,
                "resource_limits": {
                    "user_slots": 1
                }
            })

        assert "excessive_disk_usage" in op.get_alerts()

    def test_long_aborted_jobs_alert(self):
        create_test_tables(row_count=5)

        op = map(
            command="sleep 100; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"data_size_per_job": 1},
            dont_track=True)

        self.wait_for_running_jobs(op)

        time.sleep(2)

        for job in op.get_running_jobs():
            abort_job(job)

        wait(lambda: "long_aborted_jobs" in op.get_alerts())

    # if these three tests flap - call renadeen@
    def test_low_cpu_alert_presence(self):
        create_test_tables(attributes={"compression_codec": "none"})
        op = map(
            command="sleep 1; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out")

        assert "low_cpu_usage" in op.get_alerts()

    def test_low_cpu_alert_absence(self):
        create_test_tables()
        op = map(
            command="python -c 'for i in xrange(10000000): x = 1'; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out")

        assert "low_cpu_usage" not in op.get_alerts()

    def test_operation_too_long_alert(self):
        create_test_tables(row_count=100)
        op = map(
            command="sleep 100; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"data_size_per_job": 1},
            dont_track=True)

        self.wait_for_running_jobs(op)
        wait(lambda: "operation_too_long" in op.get_alerts())

    def test_intermediate_data_skew_alert(self):
        create("table", "//tmp/t_in")

        mutliplier = 1
        data = []
        for letter in ["a", "b", "c", "d", "e"]:
            data.extend([{"x": letter} for _ in xrange(mutliplier)])
            mutliplier *= 10

        write_table("//tmp/t_in", data)

        create("table", "//tmp/t_out")

        op = map_reduce(
            mapper_command="cat",
            reducer_command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            sort_by=["x"],
            spec={"partition_count": 5})

        assert "intermediate_data_skew" in op.get_alerts()

    @flaky(max_runs=3)
    def test_short_jobs_alert(self):
        create_test_tables(row_count=4)

        op = map(
            command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "data_size_per_job": 1,
            })

        assert "short_jobs_duration" in op.get_alerts()

        op = map(
            command="sleep 5; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "data_size_per_job": 1,
            })

        assert "short_jobs_duration" not in op.get_alerts()

    def test_schedule_job_timed_out_alert(self):
        create_test_tables()

        testing_options = {"scheduling_delay": 3500, "scheduling_delay_type": "async"}

        op = map(
            command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "testing": testing_options,
            },
            dont_track=True)

        time.sleep(8)

        assert "schedule_job_timed_out" in op.get_alerts()

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
            "operation_progress_analysis_period": 100,
            "heavy_job_spec_slice_count_threshold": 1,
            "job_spec_slice_throttler": {
                "limit": 1,
                "period": 1000
            },
            "operation_alerts": {
                "job_spec_throttling_alert_activation_count_threshold": 1
            }
        }
    }

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
            dont_track=True)

        wait(lambda: "excessive_job_spec_throttling" in op.get_alerts())

@require_ytserver_root_privileges
class TestControllerAgentAlerts(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1

    def teardown(self):
        remove("//sys/controller_agents/config")
        set("//sys/controller_agents/config", {})
        agent = ls("//sys/controller_agents/instances")[0]
        agent_path = "//sys/controller_agents/instances/" + agent
        wait(lambda: len(get(agent_path + "/@alerts")) == 0)

    def test_unrecognized_options_alert(self):
        agents = ls("//sys/controller_agents/instances")
        assert len(agents) == 1

        agent_path = "//sys/controller_agents/instances/" + agents[0]
        get(agent_path + "/@")
        assert len(get(agent_path + "/@alerts")) == 0

        set("//sys/controller_agents/config", {"unknown_option": 10})
        wait(lambda: len(get(agent_path + "/@alerts")) == 1)

    def test_incorrect_config(self):
        agents = ls("//sys/controller_agents/instances")
        assert len(agents) == 1

        agent_path = "//sys/controller_agents/instances/" + agents[0]
        get(agent_path + "/@")
        assert len(get(agent_path + "/@alerts")) == 0

        remove("//sys/controller_agents/config")
        set("//sys/controller_agents/config", [])
        wait(lambda: len(get(agent_path + "/@alerts")) == 1)
