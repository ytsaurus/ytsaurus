from yt_env_setup import (
    YTEnvSetup,
    wait,
    Restarter,
    CONTROLLER_AGENTS_SERVICE,
)
from yt_commands import *  # noqa
from yt_helpers import *

from yt.yson import *

from flaky import flaky

import time

##################################################################


def get_cypress_metrics(operation_id, key, aggr="sum"):
    statistics = get(get_operation_cypress_path(operation_id) + "/@progress/job_statistics")
    return sum(
        filter(
            lambda x: x is not None,
            [
                get_statistics(statistics, "{0}.$.{1}.map.{2}".format(key, job_state, aggr))
                for job_state in ("completed", "failed", "aborted")
            ],
        )
    )


##################################################################


class TestPoolMetrics(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "running_jobs_update_period": 10,
            "fair_share_update_period": 100,
            "profiling_update_period": 100,
            "fair_share_profiling_period": 100,
        },
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "snapshot_period": 2000,
            "job_metrics_report_period": 100,
            "custom_job_metrics": [
                {
                    "statistics_path": "/user_job/block_io/bytes_written",
                    "profiling_name": "my_metric",
                },
                {
                    "statistics_path": "/user_job/block_io/bytes_written",
                    "profiling_name": "my_metric_failed",
                    "job_state_filter": "failed",
                },
                {
                    "statistics_path": "/user_job/block_io/bytes_written",
                    "profiling_name": "my_metric_completed",
                    "job_state_filter": "completed",
                },
                {
                    "statistics_path": "/custom/value",
                    "profiling_name": "my_custom_metric_sum",
                    "aggregate_type": "sum",
                },
                {
                    "statistics_path": "/custom/value",
                    "profiling_name": "my_custom_metric_max",
                    "aggregate_type": "max",
                },
            ],
        }
    }

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "scheduler_connector": {
                "heartbeat_period": 100,  # 100 msec
            },
        }
    }
    USE_PORTO = True

    @authors("ignat")
    def test_map(self):
        pytest.skip("this test is broken")

        create_pool("parent")
        create_pool("child1", parent_name="parent")
        create_pool("child2", parent_name="parent")

        create("table", "//t_input")
        create("table", "//t_output")

        # write table of 2 chunks because we want 2 jobs
        write_table("//t_input", [{"key": i} for i in xrange(0, 100)])
        write_table("<append=%true>//t_input", [{"key": i} for i in xrange(100, 500)])

        # create directory backed by block device and accessible to job
        os.makedirs(self.default_disk_path)
        os.chmod(self.default_disk_path, 0777)

        # our command does the following
        # - writes (and syncs) something to disk
        # - works for some time (to ensure that it sends several heartbeats
        # - writes something to stderr because we want to find our jobs in //sys/operations later
        map_cmd = (
            """for i in $(seq 10); do"""
            """    python -c "import os; os.write(5, '{{value=$i}};')";"""
            """    dd if=/dev/urandom of={}/foo$i bs=1M count=1 oflag=direct;"""
            """    sync; sleep 0.5;"""
            """done;"""
            """cat; sleep 10; echo done > /dev/stderr"""
        )
        map_cmd = map_cmd.format(self.default_disk_path)

        metric_name = "user_job_bytes_written"
        statistics_name = "user_job.block_io.bytes_written"

        profiler = Profiler.at_scheduler(fixed_tags={"tree": "default"})

        pools = ["parent", "child1", "child2"]
        usual_metric_counter = {
            pool: profiler.counter("scheduler/pools/metrics/" + metric_name, tags={"pool": pool})
            for pool in pools
        }
        custom_metric_counter = {
            pool: profiler.counter("scheduler/pools/metrics/my_metric", tags={"pool": pool})
            for pool in pools
        }
        custom_metric_completed_counter = {
            pool: profiler.counter("scheduler/pools/metrics/my_metric_completed", tags={"pool": pool})
            for pool in pools
        }
        custom_metric_failed_counter = {
            pool: profiler.counter("scheduler/pools/metrics/my_metric_failed", tags={"pool": pool})
            for pool in pools
        }
        custom_metric_max_sensor = profiler.gauge("scheduler/pools/metrics/my_custom_metric_max", tags={"pool": "child2"})
        custom_metric_sum_sensor = profiler.gauge("scheduler/pools/metrics/my_custom_metric_sum", tags={"pool": "child2"})

        op11 = map(
            in_="//t_input",
            out="//t_output",
            command=map_cmd,
            spec={"job_count": 2, "pool": "child1"},
        )
        op12 = map(
            in_="//t_input",
            out="//t_output",
            command=map_cmd,
            spec={"job_count": 2, "pool": "child1"},
        )

        op2 = map(
            in_="//t_input",
            out="//t_output",
            command=map_cmd,
            spec={"job_count": 2, "pool": "child2"},
        )

        for counter in (
            usual_metric_counter,
            custom_metric_counter,
            custom_metric_completed_counter,
        ):
            wait(lambda: counter["parent"].get_delta() > 0)

            op11_writes = get_cypress_metrics(op11.id, statistics_name)
            op12_writes = get_cypress_metrics(op12.id, statistics_name)
            op2_writes = get_cypress_metrics(op2.id, statistics_name)

            wait(lambda: counter["child1"].get_delta() == op11_writes + op12_writes > 0)
            wait(lambda: counter["child2"].get_delta() == op2_writes > 0)
            wait(
                lambda: counter["parent"].get_delta() == op11_writes + op12_writes + op2_writes > 0
            )

        assert custom_metric_failed_counter["child2"].get_delta() == 0

        wait(lambda: custom_metric_max_sensor.get() == 20)
        wait(lambda: custom_metric_sum_sensor.get() == 110)

        jobs_11 = ls(op11.get_path() + "/jobs")
        assert len(jobs_11) >= 2

    @authors("ignat")
    def test_time_metrics(self):
        create_pool("parent")
        create_pool("child", parent_name="parent")

        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")

        write_table("<append=%true>//tmp/t_input", [{"key": i} for i in xrange(2)])

        profiler = Profiler.at_scheduler(fixed_tags={"tree": "default"})
        total_time_completed_parent_counter = profiler.counter("scheduler/pools/metrics/total_time_completed", {"pool": "parent"})
        total_time_completed_child_counter = profiler.counter("scheduler/pools/metrics/total_time_completed", {"pool": "child"})
        total_time_aborted_parent_counter = profiler.counter("scheduler/pools/metrics/total_time_aborted", {"pool": "parent"})
        total_time_aborted_child_counter = profiler.counter("scheduler/pools/metrics/total_time_aborted", {"pool": "child"})

        op = map(
            command=with_breakpoint("cat; BREAKPOINT"),
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={"data_size_per_job": 1, "pool": "child"},
            track=False,
        )

        jobs = wait_breakpoint(job_count=2)
        assert len(jobs) == 2

        release_breakpoint(job_id=jobs[0])

        # Wait until short job is completed.
        wait(lambda: len(op.get_running_jobs()) == 1)

        running_jobs = list(op.get_running_jobs())
        assert len(running_jobs) == 1
        abort_job(running_jobs[0])

        def check_metrics(parent_counter, child_counter):
            parent_delta = parent_counter.get_delta()
            child_delta = child_counter.get_delta()
            return parent_delta != 0 and child_delta != 0 and parent_delta == child_delta

        # NB: profiling is built asynchronously in separate thread and can contain non-consistent information.
        wait(lambda: check_metrics(total_time_completed_parent_counter, total_time_completed_child_counter))
        wait(lambda: check_metrics(total_time_aborted_parent_counter, total_time_aborted_child_counter))

    # Temporarily flaky due to YT-12207.
    @flaky(max_runs=3)
    @authors("eshcherbin")
    def test_total_time_operation_by_state(self):
        create_pool("parent")
        for i in xrange(3):
            create_pool("child" + str(i + 1), parent_name="parent")

        create("table", "//tmp/t_input")
        for i in xrange(3):
            create("table", "//tmp/t_output_" + str(i + 1))

        write_table("//tmp/t_input", {"foo": "bar"})

        profiler = Profiler.at_scheduler(fixed_tags={"tree": "default"})

        pools = ["parent", "child1", "child2", "child3"]
        total_time_counter = {
            pool: profiler.counter("scheduler/pools/metrics/total_time", tags={"pool": pool})
            for pool in pools
        }
        total_time_operation_completed_counter = {
            pool: profiler.counter("scheduler/pools/metrics/total_time_operation_completed", tags={"pool": pool})
            for pool in pools
        }
        total_time_operation_failed_counter = {
            pool: profiler.counter("scheduler/pools/metrics/total_time_operation_failed", tags={"pool": pool})
            for pool in pools
        }
        total_time_operation_aborted_counter = {
            pool: profiler.counter("scheduler/pools/metrics/total_time_operation_aborted", tags={"pool": pool})
            for pool in pools
        }

        op1 = map(
            command=("sleep 5; cat"),
            in_="//tmp/t_input",
            out="//tmp/t_output_1",
            spec={"pool": "child1"},
            track=False,
        )
        op2 = map(
            command=("sleep 5; cat; exit 1"),
            in_="//tmp/t_input",
            out="//tmp/t_output_2",
            spec={"pool": "child2", "max_failed_job_count": 1},
            track=False,
        )
        op3 = map(
            command=("sleep 100; cat"),
            in_="//tmp/t_input",
            out="//tmp/t_output_3",
            spec={"pool": "child3"},
            track=False,
        )

        # Wait until at least some metrics are reported for op3
        wait(lambda: total_time_counter["child3"].get_delta() > 0)
        op3.abort(wait_until_completed=True)

        op1.track()
        op2.track(raise_on_failed=False)

        def check_metrics(parent_counter, child_counter):
            parent_delta = parent_counter.get_delta()
            child_delta = child_counter.get_delta()
            return parent_delta != 0 and child_delta != 0 and parent_delta == child_delta

        # TODO(eshcherbin): This is used for flap diagnostics. Remove when the test is fixed.
        time.sleep(2)
        for pool in pools:
            total_time_counter[pool].get()

        # NB: profiling is built asynchronously in separate thread and can contain non-consistent information.
        wait(lambda: check_metrics(total_time_operation_completed_counter["parent"], total_time_operation_completed_counter["child1"]))
        wait(lambda: check_metrics(total_time_operation_failed_counter["parent"], total_time_operation_failed_counter["child2"]))
        wait(lambda: check_metrics(total_time_operation_aborted_counter["parent"], total_time_operation_aborted_counter["child3"]))
        wait(
            lambda: total_time_counter["parent"].get()
            == total_time_operation_completed_counter["parent"].get()
            + total_time_operation_failed_counter["parent"].get()
            + total_time_operation_aborted_counter["parent"].get()
            > 0
        )

    # Temporarily flaky due to YT-12207.
    @flaky(max_runs=5)
    @authors("eshcherbin")
    def test_total_time_operation_completed_several_jobs(self):
        create_pool("unique_pool")

        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")

        write_table("<append=%true>//tmp/t_input", [{"key": i} for i in xrange(2)])

        profiler = Profiler.at_scheduler(fixed_tags={"tree": "default", "pool": "unique_pool"})
        total_time_completed_counter = profiler.counter("scheduler/pools/metrics/total_time_completed")
        total_time_aborted_counter = profiler.counter("scheduler/pools/metrics/total_time_aborted")
        total_time_operation_completed_counter = profiler.counter("scheduler/pools/metrics/total_time_operation_completed")
        total_time_operation_failed_counter = profiler.counter("scheduler/pools/metrics/total_time_operation_failed")
        total_time_operation_aborted_counter = profiler.counter("scheduler/pools/metrics/total_time_operation_aborted")

        op = map(
            command=with_breakpoint("cat; BREAKPOINT; sleep 3"),
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "data_size_per_job": 1,
                "pool": "unique_pool",
                "max_speculative_job_count_per_task": 0,
            },
            track=False,
        )

        jobs = wait_breakpoint(job_count=2)
        assert len(jobs) == 2

        release_breakpoint(job_id=jobs[0])

        # Wait until short job is completed.
        wait(lambda: len(op.get_running_jobs()) == 1)

        running_jobs = list(op.get_running_jobs())
        assert len(running_jobs) == 1

        job_to_abort = running_jobs[0]
        release_breakpoint(job_id=job_to_abort)
        abort_job(job_to_abort)

        # Wait for restarted job to reach breakpoint.
        jobs = wait_breakpoint()
        assert len(jobs) == 1 and jobs[0] != job_to_abort
        release_breakpoint(job_id=jobs[0])

        op.track()

        wait(
            lambda: total_time_operation_completed_counter.get_delta()
            == total_time_completed_counter.get_delta()
            + total_time_aborted_counter.get_delta()
            > 0
        )
        assert total_time_operation_failed_counter.get_delta() == 0
        assert total_time_operation_aborted_counter.get_delta() == 0

    # Temporarily flaky due to YT-12207.
    @flaky(max_runs=3)
    @authors("eshcherbin")
    def test_total_time_operation_failed_several_jobs(self):
        create_pool("unique_pool")

        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")

        write_table(
            "<append=%true>//tmp/t_input",
            [{"sleep": 2, "exit": 0}, {"sleep": 5, "exit": 1}],
            output_format="json",
        )

        map_cmd = """python -c 'import sys; import time; import json; row=json.loads(raw_input()); time.sleep(row["sleep"]); sys.exit(row["exit"])'"""

        profiler = Profiler.at_scheduler(fixed_tags={"tree": "default", "pool": "unique_pool"})
        total_time_counter = profiler.counter("scheduler/pools/metrics/total_time")
        total_time_operation_completed_counter = profiler.counter("scheduler/pools/metrics/total_time_operation_completed")
        total_time_operation_failed_counter = profiler.counter("scheduler/pools/metrics/total_time_operation_failed")
        total_time_operation_aborted_counter = profiler.counter("scheduler/pools/metrics/total_time_operation_aborted")

        op = map(
            command=map_cmd,
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={
                "data_size_per_job": 1,
                "max_failed_job_count": 1,
                "pool": "unique_pool",
                "mapper": {"input_format": "json", "check_input_fully_consumed": True},
            },
            track=False,
        )
        op.track(raise_on_failed=False)

        wait(lambda: total_time_counter.get_delta() == total_time_operation_failed_counter.get_delta() > 0)
        assert total_time_operation_completed_counter.get_delta() == 0
        assert total_time_operation_aborted_counter.get_delta() == 0

    # Temporarily flaky due to YT-12207.
    @flaky(max_runs=3)
    @authors("eshcherbin")
    def test_total_time_operation_completed_per_tree(self):
        create("table", "//tmp/t_in")
        for i in xrange(9):
            write_table("<append=%true>//tmp/t_in", [{"x": i}])
        create("table", "//tmp/t_out")

        # Set up second tree
        node = ls("//sys/cluster_nodes")[0]
        set("//sys/cluster_nodes/" + node + "/@user_tags/end", "other")
        set("//sys/pool_trees/default/@config/nodes_filter", "!other")
        create_pool_tree("other", config={"nodes_filter": "other"})

        time.sleep(1.0)

        profiler = Profiler.at_scheduler(fixed_tags={"pool": "<Root>"})
        total_time_default_counter = profiler.counter("scheduler/pools/metrics/total_time", tags={"tree": "default"})
        total_time_other_counter = profiler.counter("scheduler/pools/metrics/total_time", tags={"tree": "other"})
        total_time_operation_completed_default_counter = profiler.counter("scheduler/pools/metrics/total_time_operation_completed", tags={"tree": "default"})
        total_time_operation_completed_other_counter = profiler.counter("scheduler/pools/metrics/total_time_operation_completed", tags={"tree": "other"})

        map(
            command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"data_size_per_job": 1, "pool_trees": ["default", "other"]},
        )

        wait(lambda: total_time_default_counter.get_delta() == total_time_operation_completed_default_counter.get_delta() > 0)
        wait(lambda: total_time_other_counter.get_delta() == total_time_operation_completed_other_counter.get_delta() > 0)

    @authors("eshcherbin")
    def test_revive(self):
        create_pool("unique_pool")

        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("<append=%true>//tmp/t_input", {"foo": "bar"})

        before_breakpoint = """for i in $(seq 10) ; do python -c "import os; os.write(5, '{value=$i};')" ; sleep 0.5 ; done ; sleep 5 ; """
        after_breakpoint = """for i in $(seq 11 15) ; do python -c "import os; os.write(5, '{value=$i};')" ; sleep 0.5 ; done ; cat ; sleep 5 ; echo done > /dev/stderr ; """

        profiler = Profiler.at_scheduler(fixed_tags={"tree": "default", "pool": "unique_pool"})
        total_time_counter = profiler.counter("scheduler/pools/metrics/total_time")
        custom_counter = profiler.counter("scheduler/pools/metrics/my_custom_metric_sum")

        op = map(
            command=with_breakpoint(before_breakpoint + "BREAKPOINT ; " + after_breakpoint),
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={"pool": "unique_pool"},
            track=False,
        )

        jobs = wait_breakpoint()
        assert len(jobs) == 1

        total_time_before_restart = total_time_counter.get_delta()
        wait(lambda: custom_counter.get_delta() == 55)

        # We need to have the job in the snapshot, so that it is not restarted after operation revival.
        op.wait_for_fresh_snapshot()

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass

        wait(lambda: get("//sys/scheduler/orchid/scheduler/operations/{}/state".format(op.id)) == "running")
        wait(lambda: op.get_state() == "running")

        release_breakpoint()

        op.track()

        wait(lambda: total_time_counter.get_delta() > total_time_before_restart)
        wait(lambda: custom_counter.get_delta() == 120)

    @authors("eshcherbin")
    def test_distributed_resources_profiling(self):
        create_pool("strong", attributes={"strong_guarantee_resources": {"cpu": 1}})
        create_pool("integral", attributes={
            "integral_guarantees": {
                "guarantee_type": "burst",
                "resource_flow": {"cpu": 1},
                "burst_guarantee_resources": {"cpu": 1},
            },
        })

        profiler = Profiler.at_scheduler(fixed_tags={"tree": "default"})
        wait(lambda: profiler.gauge("scheduler/distributed_resources/cpu").get() == 2)
        wait(lambda: profiler.gauge("scheduler/distributed_strong_guarantee_resources/cpu").get() == 1)
        wait(lambda: profiler.gauge("scheduler/distributed_resource_flow/cpu").get() == 1)
        wait(lambda: profiler.gauge("scheduler/distributed_resource_flow/cpu").get() == 1)
        wait(lambda: profiler.gauge("scheduler/distributed_burst_guarantee_resources/cpu").get() == 1)
        wait(lambda: profiler.gauge("scheduler/undistributed_resources/cpu").get() == 1)
        wait(lambda: profiler.gauge("scheduler/undistributed_resource_flow/cpu").get() == 0)
        wait(lambda: profiler.gauge("scheduler/undistributed_burst_guarantee_resources/cpu").get() == 0)


##################################################################
