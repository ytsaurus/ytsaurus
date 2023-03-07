from yt_env_setup import (
    YTEnvSetup, unix_only, patch_porto_env_only, wait, Restarter, CONTROLLER_AGENTS_SERVICE,
    get_porto_delta_node_config, porto_avaliable,
)
from yt_commands import *
from yt_helpers import *

import yt.common
from yt.yson import *

from flaky import flaky

import time

##################################################################

def get_cypress_metrics(operation_id, key, aggr="sum"):
    statistics = get(get_operation_cypress_path(operation_id) + "/@progress/job_statistics")
    return sum(filter(lambda x: x is not None,
                      [get_statistics(statistics, "{0}.$.{1}.map.{2}".format(key, job_state, aggr))
                       for job_state in ("completed", "failed", "aborted")]))

##################################################################

POOL_METRICS_NODE_CONFIG_PATCH = {
    "exec_agent": {
        "scheduler_connector": {
            "heartbeat_period": 100,  # 100 msec
        },
    }
}

@pytest.mark.skip_if('not porto_avaliable()')
class TestPoolMetrics(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    REQUIRE_YTSERVER_ROOT_PRIVILEGES = True

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
            ]
        }
    }

    DELTA_NODE_CONFIG = yt.common.update(
        get_porto_delta_node_config(),
        POOL_METRICS_NODE_CONFIG_PATCH
    )
    USE_PORTO_FOR_SERVERS = True

    @authors("ignat")
    @unix_only
    def test_map(self):
        create_pool("parent")
        create_pool("child1", parent_name="parent")
        create_pool("child2", parent_name="parent")

        create("table", "//t_input")
        create("table", "//t_output")

        # write table of 2 chunks because we want 2 jobs
        write_table("//t_input", [{"key": i} for i in xrange(0, 100)])
        write_table("<append=%true>//t_input", [{"key": i} for i in xrange(100, 500)])

        # our command does the following
        # - writes (and syncs) something to disk
        # - works for some time (to ensure that it sends several heartbeats
        # - writes something to stderr because we want to find our jobs in //sys/operations later
        map_cmd = """for i in $(seq 10) ; do python -c "import os; os.write(5, '{value=$i};')"; echo 5 > /tmp/foo$i ; sync ; sleep 0.5 ; done ; cat ; sleep 10; echo done > /dev/stderr"""

        metric_name = "user_job_bytes_written"
        statistics_name = "user_job.block_io.bytes_written"

        usual_metric_delta = Metric.at_scheduler(
            "scheduler/pools/metrics/" + metric_name,
            grouped_by_tags=["pool"])
        custom_metric_delta = Metric.at_scheduler(
            "scheduler/pools/metrics/my_metric",
            grouped_by_tags=["pool"])
        custom_metric_completed_delta = Metric.at_scheduler(
            "scheduler/pools/metrics/my_metric_completed",
            grouped_by_tags=["pool"])
        custom_metric_failed_delta = Metric.at_scheduler(
            "scheduler/pools/metrics/my_metric_failed",
            grouped_by_tags=["pool"])
        custom_metric_max_last = Metric.at_scheduler(
            "scheduler/pools/metrics/my_custom_metric_max",
            with_tags={"pool": "child2"},
            aggr_method="last")
        custom_metric_sum_last = Metric.at_scheduler(
            "scheduler/pools/metrics/my_custom_metric_sum",
            with_tags={"pool": "child2"},
            aggr_method="last")

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

        for metric_delta in (usual_metric_delta, custom_metric_delta, custom_metric_completed_delta):
            wait(lambda: metric_delta.update().get("parent", verbose=True) > 0)

            op11_writes = get_cypress_metrics(op11.id, statistics_name)
            op12_writes = get_cypress_metrics(op12.id, statistics_name)
            op2_writes = get_cypress_metrics(op2.id, statistics_name)

            wait(lambda: metric_delta.update().get("child1", verbose=True) == op11_writes + op12_writes > 0)
            wait(lambda: metric_delta.update().get("child2", verbose=True) == op2_writes > 0)
            wait(lambda: metric_delta.update().get("parent", verbose=True) == op11_writes + op12_writes + op2_writes > 0)

        assert custom_metric_failed_delta.update().get("child2", verbose=True) == 0

        wait(lambda: custom_metric_max_last.update().get(verbose=True) == 20)
        wait(lambda: custom_metric_sum_last.update().get(verbose=True) == 110)

        jobs_11 = ls(op11.get_path() + "/jobs")
        assert len(jobs_11) >= 2

    @authors("ignat")
    def test_time_metrics(self):
        create_pool("parent")
        create_pool("child", parent_name="parent")

        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")

        write_table("<append=%true>//tmp/t_input", [{"key": i} for i in xrange(2)])

        total_time_completed_delta = Metric.at_scheduler(
            "scheduler/pools/metrics/total_time_completed",
            grouped_by_tags=["pool"])
        total_time_aborted_delta = Metric.at_scheduler(
            "scheduler/pools/metrics/total_time_aborted",
            grouped_by_tags=["pool"])

        op = map(
            command=with_breakpoint("cat; BREAKPOINT"),
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={"data_size_per_job": 1, "pool": "child"},
            track=False)

        jobs = wait_breakpoint(job_count=2)
        assert len(jobs) == 2

        release_breakpoint(job_id=jobs[0])

        # Wait until short job is completed.
        wait(lambda: len(op.get_running_jobs()) == 1)

        running_jobs = list(op.get_running_jobs())
        assert len(running_jobs) == 1
        abort_job(running_jobs[0])

        def check_metrics(metric_delta):
            metric_delta.update()
            for p in ("parent", "child"):
                if metric_delta[p] == 0:
                    return False
            return metric_delta.get("parent", verbose=True) == metric_delta.get("child", verbose=True)

        # NB: profiling is built asynchronously in separate thread and can contain non-consistent information.
        wait(lambda: check_metrics(total_time_completed_delta))
        wait(lambda: check_metrics(total_time_aborted_delta))

    @authors("eshcherbin")
    def test_total_time_operation_by_state(self):
        create_pool("parent")
        for i in xrange(3):
            create_pool("child" + str(i + 1), parent_name="parent")


        create("table", "//tmp/t_input")
        for i in xrange(3):
            create("table", "//tmp/t_output_" + str(i + 1))

        write_table("//tmp/t_input", {"foo": "bar"})

        total_time_delta = Metric.at_scheduler(
            "scheduler/pools/metrics/total_time",
            grouped_by_tags=["pool"])
        total_time_operation_completed_delta = Metric.at_scheduler(
            "scheduler/pools/metrics/total_time_operation_completed",
            grouped_by_tags=["pool"])
        total_time_operation_failed_delta = Metric.at_scheduler(
            "scheduler/pools/metrics/total_time_operation_failed",
            grouped_by_tags=["pool"])
        total_time_operation_aborted_delta = Metric.at_scheduler(
            "scheduler/pools/metrics/total_time_operation_aborted",
            grouped_by_tags=["pool"])

        op1 = map(command=("sleep 5; cat"), in_="//tmp/t_input", out="//tmp/t_output_1", spec={"pool": "child1"}, track=False)
        op2 = map(command=("sleep 5; cat; exit 1"), in_="//tmp/t_input", out="//tmp/t_output_2", spec={"pool": "child2", "max_failed_job_count": 1}, track=False)
        op3 = map(command=("sleep 100; cat"), in_="//tmp/t_input", out="//tmp/t_output_3", spec={"pool": "child3"}, track=False)

        # Wait until at least some metrics are reported for op3
        wait(lambda: total_time_delta.update().get("child3", verbose=True) > 0)
        op3.abort(wait_until_completed=True)

        op1.track()
        op2.track(raise_on_failed=False)

        def check_metrics(metric_delta, child):
            metric_delta.update()
            for p in ("parent", child):
                if metric_delta[p] == 0:
                    return False
            return metric_delta.get("parent", verbose=True) == metric_delta.get(child, verbose=True)

        # TODO(eshcherbin): This is used for flap diagnostics. Remove when the test is fixed.
        time.sleep(2)
        for pool in ["parent", "child1", "child2", "child3"]:
            total_time_delta.update().get(pool, verbose=True)

        # NB: profiling is built asynchronously in separate thread and can contain non-consistent information.
        wait(lambda: check_metrics(total_time_operation_completed_delta, "child1"))
        wait(lambda: check_metrics(total_time_operation_failed_delta, "child2"))
        wait(lambda: check_metrics(total_time_operation_aborted_delta, "child3"))
        wait(lambda: total_time_delta.update().get("parent", verbose=True)
             == total_time_operation_completed_delta.update().get("parent", verbose=True)
             + total_time_operation_failed_delta.update().get("parent", verbose=True)
             + total_time_operation_aborted_delta.update().get("parent", verbose=True)
             > 0)

    # Temporarily flaky due to YT-12207.
    @flaky(max_runs=3)
    @authors("eshcherbin")
    def test_total_time_operation_completed_several_jobs(self):
        create_pool("unique_pool")

        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")

        write_table("<append=%true>//tmp/t_input", [{"key": i} for i in xrange(2)])

        total_time_completed_delta = Metric.at_scheduler(
            "scheduler/pools/metrics/total_time_completed",
            with_tags={"pool": "unique_pool"})
        total_time_aborted_delta = Metric.at_scheduler(
            "scheduler/pools/metrics/total_time_aborted",
            with_tags={"pool": "unique_pool"})
        total_time_operation_completed_delta = Metric.at_scheduler(
            "scheduler/pools/metrics/total_time_operation_completed",
            with_tags={"pool": "unique_pool"})
        total_time_operation_failed_delta = Metric.at_scheduler(
            "scheduler/pools/metrics/total_time_operation_failed",
            with_tags={"pool": "unique_pool"})
        total_time_operation_aborted_delta = Metric.at_scheduler(
            "scheduler/pools/metrics/total_time_operation_aborted",
            with_tags={"pool": "unique_pool"})

        op = map(
            command=with_breakpoint("cat; BREAKPOINT; sleep 3"),
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={"data_size_per_job": 1, "pool": "unique_pool", "max_speculative_job_count_per_task": 0},
            track=False)

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

        wait(lambda: total_time_operation_completed_delta.update().get(verbose=True)
             == total_time_completed_delta.update().get(verbose=True)
             + total_time_aborted_delta.update().get(verbose=True)
             > 0)
        assert total_time_operation_failed_delta.update().get(verbose=True) == 0
        assert total_time_operation_aborted_delta.update().get(verbose=True) == 0

    # Temporarily flaky due to YT-12207.
    @flaky(max_runs=3)
    @authors("eshcherbin")
    def test_total_time_operation_failed_several_jobs(self):
        create_pool("unique_pool")

        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")

        write_table("<append=%true>//tmp/t_input",
                    [{"sleep": 2, "exit": 0},
                     {"sleep": 5, "exit": 1}],
                    output_format="json")

        map_cmd = """python -c 'import sys; import time; import json; row=json.loads(raw_input()); time.sleep(row["sleep"]); sys.exit(row["exit"])'"""

        total_time_delta = Metric.at_scheduler(
            "scheduler/pools/metrics/total_time",
            with_tags={"pool": "unique_pool"})
        total_time_operation_completed_delta = Metric.at_scheduler(
            "scheduler/pools/metrics/total_time_operation_completed",
            with_tags={"pool": "unique_pool"})
        total_time_operation_failed_delta = Metric.at_scheduler(
            "scheduler/pools/metrics/total_time_operation_failed",
            with_tags={"pool": "unique_pool"})
        total_time_operation_aborted_delta = Metric.at_scheduler(
            "scheduler/pools/metrics/total_time_operation_aborted",
            with_tags={"pool": "unique_pool"})

        op = map(
            command=map_cmd,
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={"data_size_per_job": 1,
                  "max_failed_job_count": 1,
                  "pool": "unique_pool",
                  "mapper": {"input_format": "json",
                             "check_input_fully_consumed": True}},
            track=False)
        op.track(raise_on_failed=False)

        wait(lambda: total_time_delta.update().get(verbose=True)
             == total_time_operation_failed_delta.update().get(verbose=True)
             > 0)
        assert total_time_operation_completed_delta.update().get(verbose=True) == 0
        assert total_time_operation_aborted_delta.update().get(verbose=True) == 0

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
        set("//sys/pool_trees/default/@nodes_filter", "!other")
        create_pool_tree("other", attributes={"nodes_filter": "other"})

        time.sleep(1.0)

        total_time_delta = Metric.at_scheduler(
            "scheduler/pools/metrics/total_time",
            with_tags={"pool": "<Root>"},
            grouped_by_tags=["tree"])
        total_time_operation_completed_delta = Metric.at_scheduler(
            "scheduler/pools/metrics/total_time_operation_completed",
            with_tags={"pool": "<Root>"},
            grouped_by_tags=["tree"])

        map(command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"data_size_per_job": 1, "pool_trees": ["default", "other"]})

        wait(lambda: total_time_delta.update().get("default", verbose=True)
             == total_time_operation_completed_delta.update().get("default", verbose=True)
             > 0)
        wait(lambda: total_time_delta.update().get("other", verbose=True)
             == total_time_operation_completed_delta.update().get("other", verbose=True)
             > 0)

    @authors("eshcherbin")
    def test_revive(self):
        create_pool("unique_pool")

        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")
        write_table("<append=%true>//tmp/t_input", {"foo": "bar"})

        before_breakpoint = """for i in $(seq 10) ; do python -c "import os; os.write(5, '{value=$i};')" ; sleep 0.5 ; done ; sleep 5 ; """
        after_breakpoint = """for i in $(seq 11 15) ; do python -c "import os; os.write(5, '{value=$i};')" ; sleep 0.5 ; done ; cat ; sleep 5 ; echo done > /dev/stderr ; """

        total_time_delta = Metric.at_scheduler(
            "scheduler/pools/metrics/total_time",
            with_tags={"pool": "unique_pool"})
        custom_metric_delta = Metric.at_scheduler(
            "scheduler/pools/metrics/my_custom_metric_sum",
            with_tags={"pool": "unique_pool"})

        op = map(
            command=with_breakpoint(before_breakpoint + "BREAKPOINT ; " + after_breakpoint),
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={"pool": "unique_pool"},
            track=False)

        jobs = wait_breakpoint()
        assert len(jobs) == 1

        total_time_before_restart = total_time_delta.update().get(verbose=True)
        wait(lambda: custom_metric_delta.update().get(verbose=True) == 55)

        # We need to have the job in the snapshot, so that it is not restarted after operation revival.
        op.wait_for_fresh_snapshot()

        with Restarter(self.Env, CONTROLLER_AGENTS_SERVICE):
            pass

        wait(lambda: get("//sys/scheduler/orchid/scheduler/operations/{}/state".format(op.id)) == "running")
        wait(lambda: op.get_state() == "running")

        release_breakpoint()

        op.track()

        wait(lambda: total_time_delta.update().get(verbose=True) > total_time_before_restart)
        wait(lambda: custom_metric_delta.update().get(verbose=True) == 120)

##################################################################
