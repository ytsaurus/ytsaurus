from yt_env_setup import (
    YTEnvSetup,
    Restarter,
    SCHEDULERS_SERVICE,
    NODES_SERVICE,
)

from yt_commands import (
    authors, print_debug, wait, wait_no_assert, wait_breakpoint, release_breakpoint, with_breakpoint,
    events_on_fs, reset_events_on_fs,
    create, ls, get, set, remove, exists, create_pool, read_table, write_table,
    map, run_test_vanilla, run_sleeping_vanilla, get_job,
    sync_create_cells, update_controller_agent_config, update_pool_tree_config, update_pool_tree_config_option,
    update_scheduler_config, update_op_parameters, create_test_tables, retry, create_domestic_medium,
    disable_scheduler_jobs_on_node, enable_scheduler_jobs_on_node)

from yt_scheduler_helpers import (
    scheduler_orchid_pool_path, scheduler_orchid_node_path, scheduler_orchid_default_pool_tree_path,
    scheduler_orchid_operation_path, scheduler_orchid_default_pool_tree_config_path,
    scheduler_orchid_pool_tree_config_path)

from yt_helpers import profiler_factory

from yt.test_helpers import are_almost_equal

from yt.common import YtError, YtResponseError

import yt.environment.init_operations_archive as init_operations_archive

import yt.yson as yson

from copy import deepcopy

import pytest

import time
import datetime
import os
import shutil

##################################################################


def get_scheduling_options(user_slots):
    return {"scheduling_options_per_pool_tree": {"default": {"resource_limits": {"user_slots": user_slots}}}}


##################################################################


class TestSchedulerPreemption(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "fair_share_update_period": 100,
            "event_log": {
                "flush_period": 300,
                "retry_backoff_time": 300,
            },
            "enable_job_reporter": True,
            "enable_job_spec_reporter": True,
            "enable_job_stderr_reporter": True,
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "event_log": {
                "flush_period": 300,
                "retry_backoff_time": 300
            },
            "enable_operation_progress_archivation": False,
        }
    }

    DELTA_NODE_CONFIG = {
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
                },
            },
        }
    }

    def setup_method(self, method):
        super(TestSchedulerPreemption, self).setup_method(method)
        sync_create_cells(1)
        init_operations_archive.create_tables_latest_version(
            self.Env.create_native_client(),
            override_tablet_cell_bundle="default",
        )
        update_pool_tree_config("default", {
            "preemption_satisfaction_threshold": 0.99,
            "fair_share_starvation_tolerance": 0.8,
            "fair_share_starvation_timeout": 1000,
            "max_unpreemptible_running_allocation_count": 0,
            "preemptive_scheduling_backoff": 0,
            "allocation_graceful_preemption_timeout": 10000,
        })

    @authors("ignat")
    def test_preemption(self):
        set("//sys/pool_trees/default/@config/max_ephemeral_pools_per_user", 2)
        create("table", "//tmp/t_in")
        for i in range(3):
            write_table("<append=true>//tmp/t_in", {"foo": "bar"})

        create("table", "//tmp/t_out1")
        create("table", "//tmp/t_out2")

        op1 = map(
            track=False,
            command="sleep 1000; cat",
            in_=["//tmp/t_in"],
            out="//tmp/t_out1",
            spec={"pool": "fake_pool", "job_count": 3, "locality_timeout": 0},
        )

        pools_path = scheduler_orchid_default_pool_tree_path() + "/pools"
        wait(lambda: exists(pools_path + "/fake_pool"))
        wait(lambda: get(pools_path + "/fake_pool/fair_share_ratio") >= 0.999)
        wait(lambda: get(pools_path + "/fake_pool/usage_ratio") >= 0.999)

        total_cpu_limit = get("//sys/scheduler/orchid/scheduler/cluster/resource_limits/cpu")
        create_pool("test_pool", attributes={"min_share_resources": {"cpu": total_cpu_limit}})
        op2 = map(
            track=False,
            command="cat",
            in_=["//tmp/t_in"],
            out="//tmp/t_out2",
            spec={"pool": "test_pool"},
        )
        op2.track()

        op1.abort()

    @authors("ignat")
    @pytest.mark.parametrize("interruptible", [False, True])
    def test_interrupt_job_on_preemption(self, interruptible):
        set("//sys/pool_trees/default/@config/fair_share_starvation_timeout", 100)
        set("//sys/pool_trees/default/@config/max_ephemeral_pools_per_user", 2)
        create("table", "//tmp/t_in")
        write_table(
            "//tmp/t_in",
            [{"key": "%08d" % i, "value": "(foo)", "data": "a" * (2 * 1024 * 1024)} for i in range(6)],
            table_writer={"block_size": 1024, "desired_chunk_size": 1024},
        )

        create("table", "//tmp/t_out1")
        create("table", "//tmp/t_out2")

        spec = {
            "pool": "fake_pool",
            "locality_timeout": 0,
            "enable_job_splitting": False,
            "max_failed_job_count": 1,
        }
        if interruptible:
            data_size_per_job = get("//tmp/t_in/@uncompressed_data_size")
            spec["data_size_per_job"] = data_size_per_job // 3 + 1
        else:
            spec["job_count"] = 3

        mapper = " ; ".join([events_on_fs().breakpoint_cmd(), "sleep 7", "cat"])
        op1 = map(
            track=False,
            command=mapper,
            in_=["//tmp/t_in"],
            out="//tmp/t_out1",
            spec=spec,
        )

        time.sleep(3)

        pools_path = scheduler_orchid_default_pool_tree_path() + "/pools"
        assert get(pools_path + "/fake_pool/fair_share_ratio") >= 0.999
        assert get(pools_path + "/fake_pool/usage_ratio") >= 0.999

        total_cpu_limit = get("//sys/scheduler/orchid/scheduler/cluster/resource_limits/cpu")
        create_pool("test_pool", attributes={"min_share_resources": {"cpu": total_cpu_limit}})

        # Ensure that all three jobs have started.
        events_on_fs().wait_breakpoint(timeout=datetime.timedelta(1000), job_count=3)
        events_on_fs().release_breakpoint()

        op2 = map(
            track=False,
            command="cat",
            in_=["//tmp/t_in"],
            out="//tmp/t_out2",
            spec={"pool": "test_pool", "max_failed_job_count": 1},
        )
        op2.track()
        op1.track()
        assert get(op1.get_path() + "/@progress/jobs/completed/total") == (4 if interruptible else 3)

    @authors("dakovalkov")
    def test_graceful_preemption(self):
        create_test_tables(row_count=1)

        command = """(trap "sleep 1; echo '{interrupt=42}'; exit 0" SIGINT; BREAKPOINT)"""

        op = map(
            track=False,
            command=with_breakpoint(command),
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={
                "mapper": {
                    "interruption_signal": "SIGINT",
                },
                "preemption_mode": "graceful",
            },
        )

        wait_breakpoint()

        update_op_parameters(op.id, parameters=get_scheduling_options(user_slots=0))
        wait(lambda: op.get_job_count("running") == 0)
        op.track()
        assert op.get_job_count("completed", from_orchid=False) == 1
        assert op.get_job_count("total", from_orchid=False) == 1
        assert read_table("//tmp/t_out") == [{"interrupt": 42}]

    @authors("dakovalkov")
    def test_graceful_preemption_timeout(self):
        op = run_test_vanilla(
            command=with_breakpoint("BREAKPOINT ; sleep 100"),
            spec={"preemption_mode": "graceful"}
        )

        wait_breakpoint()
        release_breakpoint()
        update_op_parameters(op.id, parameters=get_scheduling_options(user_slots=0))
        wait(lambda: op.get_job_count("aborted") == 1)
        assert op.get_job_count("total") == 1
        assert op.get_job_count("aborted") == 1

    @authors("ignat")
    def test_min_share_ratio(self):
        create_pool("test_min_share_ratio_pool", attributes={"min_share_resources": {"cpu": 3}})

        def get_operation_min_share_ratio(op):
            return op.get_runtime_progress("scheduling_info_per_pool_tree/default/min_share_ratio", 0.0)

        min_share_settings = [{"cpu": 3}, {"cpu": 1, "user_slots": 6}]

        for min_share_spec in min_share_settings:
            reset_events_on_fs()
            op = run_test_vanilla(
                with_breakpoint("BREAKPOINT"),
                spec={
                    "pool": "test_min_share_ratio_pool",
                    "min_share_resources": min_share_spec,
                },
                job_count=3,
            )
            wait_breakpoint()

            wait(lambda: get_operation_min_share_ratio(op) == 1.0)

            release_breakpoint()
            op.track()

    @authors("ignat")
    def test_recursive_preemption_settings(self):
        def get_pool_tolerance(pool):
            return get(scheduler_orchid_pool_path(pool) + "/effective_fair_share_starvation_tolerance")

        def get_operation_tolerance(op):
            return op.get_runtime_progress(
                "scheduling_info_per_pool_tree/default/effective_fair_share_starvation_tolerance", 0.0)

        create_pool("p1", attributes={"fair_share_starvation_tolerance": 0.6})
        create_pool("p2", parent_name="p1")
        create_pool("p3", parent_name="p1", attributes={"fair_share_starvation_tolerance": 0.5})
        create_pool("p4", parent_name="p1", attributes={"fair_share_starvation_tolerance": 0.9})
        create_pool("p5", attributes={"fair_share_starvation_tolerance": 0.8})
        create_pool("p6", parent_name="p5")

        wait(lambda: get_pool_tolerance("p1") == 0.6)
        wait(lambda: get_pool_tolerance("p2") == 0.6)
        wait(lambda: get_pool_tolerance("p3") == 0.5)
        wait(lambda: get_pool_tolerance("p4") == 0.9)
        wait(lambda: get_pool_tolerance("p5") == 0.8)
        wait(lambda: get_pool_tolerance("p6") == 0.8)

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})
        create("table", "//tmp/t_out1")
        create("table", "//tmp/t_out2")
        create("table", "//tmp/t_out3")
        create("table", "//tmp/t_out4")

        op1 = map(
            track=False,
            command="sleep 1000; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out1",
            spec={"pool": "p2"},
        )

        op2 = map(
            track=False,
            command="sleep 1000; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out2",
            spec={"pool": "p6"},
        )

        wait(lambda: get_operation_tolerance(op1) == 0.6)
        wait(lambda: get_operation_tolerance(op2) == 0.8)

    @authors("asaitgalin", "ignat")
    def test_preemption_of_jobs_excessing_resource_limits(self):
        create("table", "//tmp/t_in")
        for i in range(3):
            write_table("<append=%true>//tmp/t_in", {"foo": "bar"})

        create("table", "//tmp/t_out")

        op = map(
            track=False,
            command="sleep 1000; cat",
            in_=["//tmp/t_in"],
            out="//tmp/t_out",
            spec={"data_size_per_job": 1},
        )

        wait(lambda: len(op.get_running_jobs()) == 3)

        update_op_parameters(
            op.id,
            parameters={"scheduling_options_per_pool_tree": {"default": {"resource_limits": {"user_slots": 1}}}},
        )

        wait(lambda: len(op.get_running_jobs()) == 1)

        update_op_parameters(
            op.id,
            parameters={"scheduling_options_per_pool_tree": {"default": {"resource_limits": {"user_slots": 0}}}},
        )

        wait(lambda: len(op.get_running_jobs()) == 0)

    @authors("ignat")
    def test_preemptor_event_log(self):
        set("//sys/pool_trees/default/@config/max_ephemeral_pools_per_user", 2)
        total_cpu_limit = get("//sys/scheduler/orchid/scheduler/cluster/resource_limits/cpu")
        create_pool("pool1", attributes={"min_share_resources": {"cpu": total_cpu_limit}})

        create("table", "//tmp/t_in")
        create("table", "//tmp/t_out0")
        create("table", "//tmp/t_out1")

        for i in range(3):
            write_table("<append=%true>//tmp/t_in", {"foo": "bar"})

        op0 = map(
            track=False,
            command=with_breakpoint("BREAKPOINT; sleep 1000; cat", breakpoint_name="b0"),
            in_="//tmp/t_in",
            out="//tmp/t_out0",
            spec={"pool": "pool0", "job_count": 3},
        )

        wait_breakpoint(breakpoint_name="b0", job_count=3)
        release_breakpoint(breakpoint_name="b0")

        op1 = map(
            track=False,
            command=with_breakpoint("cat; BREAKPOINT", breakpoint_name="b1"),
            in_="//tmp/t_in",
            out="//tmp/t_out1",
            spec={"pool": "pool1", "job_count": 1},
        )

        preemptor_job_id = wait_breakpoint(breakpoint_name="b1")[0]
        release_breakpoint(breakpoint_name="b1")

        def check_events():
            for row in read_table("//sys/scheduler/event_log"):
                event_type = row["event_type"]
                if event_type == "job_aborted" and row["operation_id"] == op0.id:
                    assert row["preempted_for"]["operation_id"] == op1.id
                    assert row["preempted_for"]["job_id"] == preemptor_job_id
                    return True
            return False

        wait(lambda: check_events())

        op0.abort()

    @authors("ignat")
    def test_waiting_job_timeout(self):
        set("//sys/pool_trees/default/@config/waiting_job_timeout", 10000)
        set("//sys/pool_trees/default/@config/allocation_preemption_timeout", 5000)

        total_cpu_limit = get("//sys/scheduler/orchid/scheduler/cluster/resource_limits/cpu")
        create_pool("pool1", attributes={"min_share_resources": {"cpu": total_cpu_limit}})
        create_pool("pool2")

        command = """(trap "sleep 15; exit 0" SIGINT; BREAKPOINT)"""

        run_test_vanilla(
            with_breakpoint(command),
            spec={
                "pool": "pool2",
            },
            task_patch={
                "interruption_signal": "SIGINT",
            },
            job_count=3,
        )
        job_ids = wait_breakpoint(job_count=3)
        assert len(job_ids) == 3

        op1 = run_test_vanilla(
            "sleep 1",
            spec={
                "pool": "pool1",
            },
            job_count=1,
        )

        op1.track()

        assert op1.get_job_count("completed", from_orchid=False) == 1
        assert op1.get_job_count("aborted", from_orchid=False) == 0

    @authors("ignat")
    def test_inconsistent_waiting_job_timeout(self):
        # Pool tree misconfiguration
        set("//sys/pool_trees/default/@config/waiting_job_timeout", 5000)
        set("//sys/pool_trees/default/@config/allocation_preemption_timeout", 15000)

        total_cpu_limit = get("//sys/scheduler/orchid/scheduler/cluster/resource_limits/cpu")
        create_pool("pool1", attributes={"min_share_resources": {"cpu": total_cpu_limit}})
        create_pool("pool2")

        command = """(trap "sleep 10; exit 0" SIGINT; BREAKPOINT)"""

        run_test_vanilla(
            with_breakpoint(command),
            spec={
                "pool": "pool2",
            },
            task_patch={
                "interruption_signal": "SIGINT",
            },
            job_count=3,
        )
        job_ids = wait_breakpoint(job_count=3)
        assert len(job_ids) == 3

        op1 = run_test_vanilla(
            "cat",
            spec={
                "pool": "pool1",
            },
            job_count=1,
        )

        wait(lambda: op1.get_job_count("aborted") == 1)

    @authors("eshcherbin")
    @pytest.mark.xfail(run=False, reason="Fails until YT-14804 is resolved.")
    def test_usage_overcommit_due_to_interruption(self):
        # Pool tree misconfiguration
        set("//sys/pool_trees/default/@config/waiting_job_timeout", 600000)
        set("//sys/pool_trees/default/@config/allocation_preemption_timeout", 600000)

        set("//sys/scheduler/config/running_allocations_update_period", 100)

        total_cpu_limit = int(get("//sys/scheduler/orchid/scheduler/cluster/resource_limits/cpu"))
        create_pool("pool1", attributes={"min_share_resources": {"cpu": total_cpu_limit}})
        create_pool("pool2")

        command = """(trap "sleep 115; exit 0" SIGINT; BREAKPOINT)"""

        run_test_vanilla(
            with_breakpoint(command),
            spec={
                "pool": "pool2",
            },
            task_patch={
                "interruption_signal": "SIGINT",
            },
            job_count=total_cpu_limit,
        )
        job_ids = wait_breakpoint(job_count=total_cpu_limit)
        assert len(job_ids) == total_cpu_limit

        run_test_vanilla(
            "sleep 1",
            spec={
                "pool": "pool1",
            },
            job_count=1,
        )

        for i in range(100):
            time.sleep(0.1)
            assert get(scheduler_orchid_pool_path("<Root>") + "/resource_usage/cpu") <= total_cpu_limit

    @authors("eshcherbin")
    def test_pass_preemption_reason_to_node(self):
        update_controller_agent_config("enable_operation_progress_archivation", True)

        create_pool("research")
        create_pool("prod", attributes={"strong_guarantee_resources": {"cpu": 3}})

        op1 = run_test_vanilla(with_breakpoint("BREAKPOINT"), spec={"pool": "research"})
        job_id = wait_breakpoint(job_count=1)[0]

        op2 = run_sleeping_vanilla(spec={"pool": "prod"}, job_count=3)

        wait(lambda: op1.get_job_count(state="aborted") == 1)

        # NB(eshcherbin): Previous check doesn't guarantee that job's state in the archive is "aborted".
        def get_aborted_job(op_id, job_id):
            j = get_job(op_id, job_id, verbose_error=False)
            if j["state"] != "aborted":
                raise YtError()
            return j

        job = retry(lambda: get_aborted_job(op1.id, job_id))
        print_debug(job["error"])
        assert job["state"] == "aborted"
        assert job["abort_reason"] == "preemption"
        assert job["error"]["attributes"]["abort_reason"] == "preemption"
        preemption_reason = job["error"]["attributes"]["preemption_reason"]
        assert preemption_reason.startswith("Preempted to start allocation") and \
            "of operation {}".format(op2.id) in preemption_reason

    @authors("eshcherbin")
    def test_conditional_preemption(self):
        update_scheduler_config("min_spare_allocation_resources_on_node", {"cpu": 0.5, "user_slots": 1})

        set("//sys/pool_trees/default/@config/max_unpreemptible_running_allocation_count", 1)
        set("//sys/pool_trees/default/@config/enable_conditional_preemption", False)
        wait(lambda: not get(scheduler_orchid_default_pool_tree_config_path() + "/enable_conditional_preemption"))

        create_pool("blocking_pool", attributes={"strong_guarantee_resources": {"cpu": 1}})
        create_pool("guaranteed_pool", attributes={"strong_guarantee_resources": {"cpu": 2}})

        for i in range(3):
            run_sleeping_vanilla(
                spec={"pool": "blocking_pool"},
                task_patch={"cpu_limit": 0.5},
            )
        wait(lambda: get(scheduler_orchid_pool_path("blocking_pool") + "/resource_usage/cpu") == 1.5)

        donor_op = run_sleeping_vanilla(
            job_count=4,
            spec={"pool": "guaranteed_pool"},
            task_patch={"cpu_limit": 0.5},
        )
        wait(lambda: get(scheduler_orchid_operation_path(donor_op.id) + "/resource_usage/cpu", default=0) == 1.5)
        wait(lambda: get(scheduler_orchid_operation_path(donor_op.id) + "/starvation_status", default=None) != "non_starving")
        wait(lambda: get(scheduler_orchid_pool_path("guaranteed_pool") + "/starvation_status") != "non_starving")

        time.sleep(1.5)
        assert get(scheduler_orchid_operation_path(donor_op.id) + "/starvation_status") != "non_starving"
        assert get(scheduler_orchid_pool_path("guaranteed_pool") + "/starvation_status") != "non_starving"

        starving_op = run_sleeping_vanilla(
            job_count=2,
            spec={"pool": "guaranteed_pool"},
            task_patch={"cpu_limit": 0.5},
        )

        wait(lambda: get(scheduler_orchid_operation_path(donor_op.id) + "/starvation_status", default=None) == "non_starving")
        wait(lambda: get(scheduler_orchid_operation_path(starving_op.id) + "/starvation_status", default=None) != "non_starving")
        wait(lambda: get(scheduler_orchid_pool_path("guaranteed_pool") + "/starvation_status") != "non_starving")

        time.sleep(3.0)
        assert get(scheduler_orchid_operation_path(donor_op.id) + "/starvation_status") == "non_starving"
        assert get(scheduler_orchid_operation_path(starving_op.id) + "/starvation_status", default=None) != "non_starving"
        assert get(scheduler_orchid_pool_path("guaranteed_pool") + "/starvation_status") != "non_starving"

        set("//sys/pool_trees/default/@config/enable_conditional_preemption", True)
        wait(lambda: get(scheduler_orchid_operation_path(starving_op.id) + "/resource_usage/cpu") == 0.5)
        wait(lambda: get(scheduler_orchid_operation_path(donor_op.id) + "/resource_usage/cpu") == 1.0)
        wait(lambda: get(scheduler_orchid_pool_path("blocking_pool") + "/resource_usage/cpu") == 1.5)

    @authors("eshcherbin")
    def test_job_preemption_order(self):
        update_scheduler_config("min_spare_allocation_resources_on_node", {"cpu": 0.5, "user_slots": 1}, wait_for_orchid=False)
        update_scheduler_config("operation_hangup_check_period", 1000000000)

        create_pool("production", attributes={"strong_guarantee_resources": {"cpu": 3.0}})
        create_pool(
            "research",
            attributes={
                "allow_aggressive_preemption": False,
                "scheduling_tag_filter": "research",
                "resource_limits": {"cpu": 0.0},
            })

        nodes = ls("//sys/cluster_nodes")

        op1 = run_sleeping_vanilla(
            job_count=6,
            task_patch={"cpu_limit": 0.5},
            spec={"pool": "research"},
        )

        for i, node in enumerate(nodes):
            set("//sys/cluster_nodes/{}/@user_tags/end".format(node), "research")

            set("//sys/pools/research/@resource_limits/cpu", float(i) + 0.5)
            wait(lambda: get(scheduler_orchid_operation_path(op1.id) + "/resource_usage/cpu", default=None) == float(i) + 0.5)
            time.sleep(0.1)

            set("//sys/pools/research/@resource_limits/cpu", float(i) + 1.0)
            wait(lambda: get(scheduler_orchid_operation_path(op1.id) + "/resource_usage/cpu", default=None) == float(i) + 1.0)
            time.sleep(0.1)

        op2 = run_sleeping_vanilla(task_patch={"cpu_limit": 0.5}, spec={"pool": "production", "scheduling_tag_filter": nodes[0]})
        wait(lambda: get(scheduler_orchid_operation_path(op1.id) + "/preemptible_job_count") == 1)
        wait(lambda: get(scheduler_orchid_operation_path(op2.id) + "/resource_usage/cpu", default=None) == 0.0)

        time.sleep(1.5)
        assert get(scheduler_orchid_operation_path(op2.id) + "/resource_usage/cpu") == 0.0

        last_node_jobs = sorted([(job["start_time"], job_id) for job_id, job in op1.get_running_jobs().items() if job["address"] == nodes[2]])
        earlier_job = last_node_jobs[0][1]
        run_sleeping_vanilla(task_patch={"cpu_limit": 0.5}, spec={"pool": "production", "scheduling_tag_filter": nodes[2]})
        wait(lambda: len(op1.get_running_jobs()) == 5)
        wait(lambda: earlier_job in op1.get_running_jobs())

    @authors("eshcherbin")
    def test_retain_preemptible_status_after_revive(self):
        update_scheduler_config("min_spare_allocation_resources_on_node", {"cpu": 0.5, "user_slots": 1}, wait_for_orchid=False)

        create_pool("research", attributes={"resource_limits": {"user_slots": 5}})
        create_pool("production", attributes={"strong_guarantee_resources": {"cpu": 0.5}})

        op = run_sleeping_vanilla(job_count=6, task_patch={"cpu_limit": 0.5}, spec={"pool": "research"})
        wait(lambda: len(op.get_running_jobs()) == 5)

        time.sleep(0.5)
        set("//sys/pool_trees/default/research/@resource_limits", {})
        wait(lambda: len(op.get_running_jobs()) == 6)

        preemptible_job, _ = max(op.get_running_jobs().items(), key=lambda job: job[1]["start_time"])

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            pass

        wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/resource_usage/cpu", default=None) == 3.0)

        run_sleeping_vanilla(task_patch={"cpu_limit": 0.5}, spec={"pool": "production"})
        wait(lambda: len(op.get_running_jobs()) == 5)
        wait(lambda: preemptible_job not in op.get_running_jobs())


class TestNonPreemptibleResourceUsageThreshold(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_update_period": 100,
        },
    }

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "cpu": 10,
                "user_slots": 10,
            },
        },
    }

    def setup_method(self, method):
        super(TestNonPreemptibleResourceUsageThreshold, self).setup_method(method)

        remove("//sys/pool_trees/default/@config/max_unpreemptible_running_allocation_count", force=True),
        update_pool_tree_config_option("default", "max_unpreemptible_running_allocation_count", yson.YsonEntity())
        update_pool_tree_config("default", {
            "aggressive_preemption_satisfaction_threshold": 0.4,
            "preemption_satisfaction_threshold": 0.9,
            "fair_share_starvation_tolerance": 1.0,
            "non_preemptible_resource_usage_threshold": {},
        })

    def _check_preemptible_job_count(self, op, expected_preemptible_count, expected_aggressively_preemptible_count):
        preemptible_count = get(scheduler_orchid_operation_path(op.id) + "/preemptible_job_count")
        aggressively_preemptible_count = get(scheduler_orchid_operation_path(op.id) + "/aggressively_preemptible_job_count")
        assert expected_preemptible_count == preemptible_count and \
            expected_aggressively_preemptible_count == aggressively_preemptible_count

    @authors("eshcherbin")
    def test_non_preemptible_resource_usage_threshold(self):
        op = run_sleeping_vanilla(job_count=15)
        wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/resource_usage/cpu", default=None) == 10.0)

        wait_no_assert(lambda: self._check_preemptible_job_count(op, 1, 5))

        update_pool_tree_config_option(
            "default",
            "non_preemptible_resource_usage_threshold",
            {"cpu": 1.0})
        wait_no_assert(lambda: self._check_preemptible_job_count(op, 1, 5))

        update_pool_tree_config_option(
            "default",
            "non_preemptible_resource_usage_threshold",
            {"cpu": 1.0, "user_slots": 2})
        wait_no_assert(lambda: self._check_preemptible_job_count(op, 1, 5))

        update_pool_tree_config_option(
            "default",
            "non_preemptible_resource_usage_threshold",
            {"cpu": 5.0, "user_slots": 2})
        wait_no_assert(lambda: self._check_preemptible_job_count(op, 1, 5))

        update_pool_tree_config_option(
            "default",
            "non_preemptible_resource_usage_threshold",
            {"cpu": 5.0, "user_slots": 6})
        wait_no_assert(lambda: self._check_preemptible_job_count(op, 1, 4))

        update_pool_tree_config_option(
            "default",
            "non_preemptible_resource_usage_threshold",
            {"user_slots": 7})
        wait_no_assert(lambda: self._check_preemptible_job_count(op, 1, 2))

        update_pool_tree_config_option(
            "default",
            "non_preemptible_resource_usage_threshold",
            {"cpu": 10.0})
        wait_no_assert(lambda: self._check_preemptible_job_count(op, 0, 0))

    @authors("eshcherbin")
    def test_max_unpreemptible_running_allocation_count(self):
        op = run_sleeping_vanilla(job_count=15)
        wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/resource_usage/cpu", default=None) == 10.0)

        wait_no_assert(lambda: self._check_preemptible_job_count(op, 1, 5))

        remove("//sys/pool_trees/default/@config/max_unpreemptible_running_allocation_count", force=True)
        update_pool_tree_config_option(
            "default",
            "max_unpreemptible_running_allocation_count",
            0)
        wait_no_assert(lambda: self._check_preemptible_job_count(op, 1, 5))

        update_pool_tree_config_option(
            "default",
            "max_unpreemptible_running_allocation_count",
            4)
        wait_no_assert(lambda: self._check_preemptible_job_count(op, 1, 5))

        update_pool_tree_config_option(
            "default",
            "max_unpreemptible_running_allocation_count",
            7)
        wait_no_assert(lambda: self._check_preemptible_job_count(op, 1, 2))

        update_pool_tree_config_option(
            "default",
            "max_unpreemptible_running_allocation_count",
            10)
        wait_no_assert(lambda: self._check_preemptible_job_count(op, 0, 0))

    @authors("eshcherbin")
    def test_config_postprocessor(self):
        config_option_orchid_path = scheduler_orchid_pool_tree_config_path("default") + "/non_preemptible_resource_usage_threshold"
        assert get(config_option_orchid_path) == {}

        remove("//sys/pool_trees/default/@config/max_unpreemptible_running_allocation_count", force=True)
        update_pool_tree_config_option(
            "default",
            "max_unpreemptible_running_allocation_count",
            0)
        assert get(config_option_orchid_path) == {"user_slots": 0}

        update_pool_tree_config_option(
            "default",
            "max_unpreemptible_running_allocation_count",
            7)
        assert get(config_option_orchid_path) == {"user_slots": 7}

        update_pool_tree_config_option(
            "default",
            "non_preemptible_resource_usage_threshold",
            {"cpu": 1.0},
            wait_for_orchid=False)
        wait(lambda: get(config_option_orchid_path) == {"cpu": 1.0, "user_slots": 7})

        update_pool_tree_config_option(
            "default",
            "non_preemptible_resource_usage_threshold",
            {"cpu": 1.0, "user_slots": 5})
        assert get(config_option_orchid_path) == {"cpu": 1.0, "user_slots": 5}

        with pytest.raises(YtError):
            remove("//sys/pool_trees/default/@config/non_preemptible_resource_usage_threshold", force=True)
            update_pool_tree_config_option(
                "default",
                "non_preemptible_resource_usage_threshold",
                yson.YsonEntity(),
                wait_for_orchid=False)

    @authors("eshcherbin")
    def test_pool_config_overrides_tree_config(self):
        create_pool("pool")

        op = run_sleeping_vanilla(job_count=15, spec={"pool": "pool"})
        wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/resource_usage/cpu", default=None) == 10.0)

        wait_no_assert(lambda: self._check_preemptible_job_count(op, 1, 5))

        update_pool_tree_config_option(
            "default",
            "non_preemptible_resource_usage_threshold",
            {"user_slots": 1})
        wait_no_assert(lambda: self._check_preemptible_job_count(op, 1, 5))

        set("//sys/pool_trees/default/pool/@non_preemptible_resource_usage_threshold", {"user_slots": 7})
        wait_no_assert(lambda: self._check_preemptible_job_count(op, 1, 2))

        update_pool_tree_config_option(
            "default",
            "non_preemptible_resource_usage_threshold",
            {"user_slots": 10})
        wait_no_assert(lambda: self._check_preemptible_job_count(op, 1, 2))

    @authors("eshcherbin")
    def test_operation_max_unpreemptible_running_allocation_count(self):
        op = run_sleeping_vanilla(job_count=15, spec={"max_unpreemptible_job_count": 7})
        wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/resource_usage/cpu", default=None) == 10.0)

        wait_no_assert(lambda: self._check_preemptible_job_count(op, 1, 2))

        update_pool_tree_config_option(
            "default",
            "non_preemptible_resource_usage_threshold",
            {"user_slots": 1})
        wait_no_assert(lambda: self._check_preemptible_job_count(op, 1, 5))

        update_pool_tree_config_option(
            "default",
            "non_preemptible_resource_usage_threshold",
            {"user_slots": 10})
        wait_no_assert(lambda: self._check_preemptible_job_count(op, 1, 2))


class TestPreemptionPriorityScope(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "running_allocations_update_period": 10,
            "fair_share_update_period": 100,
            "operation_hangup_check_period": 1000000000,
        },
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "safe_scheduler_online_time": 1000000000,
        }
    }

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "cpu": 10,
                "user_slots": 10,
            },
        },
    }

    @authors("eshcherbin")
    def test_preemption_priority_scope(self):
        update_pool_tree_config(
            "default",
            {
                "preemption_satisfaction_threshold": 0.8,
                "fair_share_starvation_timeout": 1000,
                "fair_share_starvation_tolerance": 0.95,
                "max_unpreemptible_running_allocation_count": 0,
                "preemptive_scheduling_backoff": 0,
                "scheduling_preemption_priority_scope": "operation_only",
            })

        create_pool("first", attributes={"strong_guarantee_resources": {"cpu": 4.9, "user_slots": 5}})
        create_pool("second", attributes={"strong_guarantee_resources": {"cpu": 5.1, "user_slots": 5}})

        normal_op = run_sleeping_vanilla(job_count=5, spec={"pool": "first"})
        wait(lambda: get(scheduler_orchid_operation_path(normal_op.id) + "/resource_usage/cpu", default=None) == 5.0)

        starving_op = run_sleeping_vanilla(spec={"pool": "second", "scheduling_tag_filter": "nonexistent_tag"})
        wait(lambda: get(scheduler_orchid_operation_path(starving_op.id) + "/starvation_status", default=None) == "starving")
        wait(lambda: get(scheduler_orchid_pool_path("second") + "/starvation_status", default=None) == "starving")

        greedy_op = run_sleeping_vanilla(job_count=3, spec={"pool": "second"}, task_patch={"cpu_limit": 2.0})
        wait(lambda: are_almost_equal(get(scheduler_orchid_operation_path(greedy_op.id) + "/detailed_fair_share/total/cpu", default=None), 0.41))
        wait(lambda: get(scheduler_orchid_operation_path(greedy_op.id) + "/resource_usage/cpu") == 4.0)

        time.sleep(3.0)
        assert get(scheduler_orchid_operation_path(greedy_op.id) + "/resource_usage/cpu") == 4.0
        assert get(scheduler_orchid_operation_path(greedy_op.id) + "/starvation_status") == "non_starving"
        assert get(scheduler_orchid_pool_path("second") + "/starvation_status") == "starving"

        node = ls("//sys/cluster_nodes")[0]

        def check():
            op_count = get(
                scheduler_orchid_node_path(node) +
                "/last_preemptive_heartbeat_statistics/operation_count_by_preemption_priority"
            )
            return op_count["none"] == 2 and op_count["normal"] == 1
        wait(check)

        update_pool_tree_config_option("default", "scheduling_preemption_priority_scope", "operation_and_ancestors")
        wait(lambda: get(scheduler_orchid_operation_path(greedy_op.id) + "/resource_usage/cpu") == 6.0)


##################################################################


class TestRacyPreemption(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_update_period": 100,
            "schedule_allocation_time_limit": 20000,
        },
    }

    def setup_method(self, method):
        super(TestRacyPreemption, self).setup_method(method)
        update_pool_tree_config("default", {
            "preemption_satisfaction_threshold": 1.0,
            "fair_share_starvation_tolerance": 1.0,
            "max_unpreemptible_running_allocation_count": 0,
            "fair_share_starvation_timeout": 0,
            "preemptive_scheduling_backoff": 0,
        })

    def teardown_method(self, method):
        # Reset not to slow down the teardown method.
        update_scheduler_config("testing_options/finish_operation_transition_delay", None)
        super(TestRacyPreemption, self).teardown_method(method)

    @authors("eshcherbin")
    def test_race_between_preemption_and_user_abort(self):
        # Delay between jobs abortion and operation unregistration.
        update_scheduler_config("testing_options/finish_operation_transition_delay", {"duration": 15000, "type": "async"})

        create_pool("prod", attributes={"strong_guarantee_resources": {"cpu": 1.0}})

        blocking_op = run_sleeping_vanilla()
        wait(lambda: get(scheduler_orchid_operation_path(blocking_op.id) + "/resource_usage/cpu", default=None) == 1.0)

        op = run_sleeping_vanilla(spec={
            "pool": "prod",
            "testing": {
                "schedule_job_delay": {
                    "duration": 5000,
                    "type": "async",
                },
            },
        })

        time.sleep(2.0)
        blocking_op.abort()
        wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/resource_usage/cpu", default=None) == 1.0)


##################################################################


class TestSchedulingBugOfOperationWithGracefulPreemption(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {"job_resource_manager": {"resource_limits": {"cpu": 2, "user_slots": 2}}}

    @authors("renadeen")
    def test_scheduling_bug_of_operation_with_graceful_preemption(self):
        # Scenario:
        # 1. run operation with graceful preemption and with two jobs;
        # 2. first job is successfully scheduled;
        # 3. operation status becomes Normal since usage/fair_share is greater than fair_share_starvation_tolerance;
        # 4. next job will never be scheduled cause scheduler doesn't schedule jobs for operations with graceful preemption and Normal status.

        create_pool("pool", attributes={"fair_share_starvation_tolerance": 0.4})
        # TODO(renadeen): need this placeholder operation to work around some bugs in scheduler (YT-13840).
        run_test_vanilla(with_breakpoint("BREAKPOINT", breakpoint_name="placeholder"))

        op = run_test_vanilla(
            command=with_breakpoint("BREAKPOINT", breakpoint_name="graceful"),
            job_count=2,
            spec={
                "preemption_mode": "graceful",
                "pool": "pool",
            }
        )

        wait_breakpoint(breakpoint_name="graceful", job_count=1)
        release_breakpoint(breakpoint_name="placeholder")

        wait_breakpoint(breakpoint_name="graceful", job_count=2)
        wait(lambda: op.get_job_count("running") == 2)
        release_breakpoint(breakpoint_name="graceful")
        op.track()

##################################################################


class TestResourceLimitsOverdraftPreemption(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 2
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "fair_share_update_period": 100,
            "allowed_node_resources_overcommit_duration": 1000,
        }
    }

    DELTA_NODE_CONFIG = {"job_resource_manager": {"resource_limits": {"cpu": 2, "user_slots": 2}}}

    def setup_method(self, method):
        super(TestResourceLimitsOverdraftPreemption, self).setup_method(method)
        update_pool_tree_config("default", {
            "allocation_graceful_preemption_timeout": 10000,
            "allocation_preemption_timeout": 600000,
        })

    def teardown_method(self, method):
        remove("//sys/scheduler/config", force=True)
        super(TestResourceLimitsOverdraftPreemption, self).teardown_method(method)

    @authors("ignat")
    def test_scheduler_preempt_overdraft_resources(self):
        update_pool_tree_config_option("default", "allocation_preemption_timeout", 1000)

        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) > 0

        set(
            "//sys/cluster_nodes/{}/@resource_limits_overrides".format(nodes[0]),
            {"cpu": 0},
        )
        wait(lambda: get("//sys/cluster_nodes/{}/orchid/exec_node/job_resource_manager/resource_limits/cpu".format(nodes[0])) == 0.0)

        create("table", "//tmp/t_in")
        for i in range(1):
            write_table("<append=%true>//tmp/t_in", {"foo": "bar"})

        create("table", "//tmp/t_out1")
        create("table", "//tmp/t_out2")

        op1 = map(
            track=False,
            command=with_breakpoint("BREAKPOINT; sleep 1000; cat"),
            in_=["//tmp/t_in"],
            out="//tmp/t_out1",
        )
        wait_breakpoint()

        op2 = map(
            track=False,
            command=with_breakpoint("BREAKPOINT; sleep 1000; cat"),
            in_=["//tmp/t_in"],
            out="//tmp/t_out2",
        )
        wait_breakpoint()

        wait(lambda: op1.get_job_count("running") == 1)
        wait(lambda: op2.get_job_count("running") == 1)
        wait(lambda: get("//sys/cluster_nodes/{}/orchid/exec_node/job_resource_manager/resource_usage/cpu".format(nodes[1])) == 2.0)

        # TODO(ignat): add check that jobs are not preemptible.

        set(
            "//sys/cluster_nodes/{}/@resource_limits_overrides".format(nodes[0]),
            {"cpu": 2},
        )
        wait(lambda: get("//sys/cluster_nodes/{}/orchid/exec_node/job_resource_manager/resource_limits/cpu".format(nodes[0])) == 2.0)

        set(
            "//sys/cluster_nodes/{}/@resource_limits_overrides".format(nodes[1]),
            {"cpu": 0},
        )
        wait(lambda: get("//sys/cluster_nodes/{}/orchid/exec_node/job_resource_manager/resource_limits/cpu".format(nodes[1])) == 0.0)

        wait(lambda: op1.get_job_count("aborted") == 1)
        wait(lambda: op2.get_job_count("aborted") == 1)

        wait(lambda: op1.get_job_count("running") == 1)
        wait(lambda: op2.get_job_count("running") == 1)

    @authors("ignat")
    def test_scheduler_force_abort(self):
        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) >= 2

        disable_scheduler_jobs_on_node(nodes[0], "test scheduler force abort")
        wait(
            lambda: get("//sys/cluster_nodes/{}/orchid/exec_node/job_resource_manager/resource_limits/user_slots".format(nodes[0])) == 0
        )

        create("table", "//tmp/t_in")
        for i in range(1):
            write_table("<append=%true>//tmp/t_in", {"foo": "bar"})

        create("table", "//tmp/t_out1")
        create("table", "//tmp/t_out2")

        op1 = map(
            track=False,
            command="sleep 1000; cat",
            in_=["//tmp/t_in"],
            out="//tmp/t_out1",
        )
        op2 = map(
            track=False,
            command="sleep 1000; cat",
            in_=["//tmp/t_in"],
            out="//tmp/t_out2",
        )

        wait(lambda: op1.get_job_count("running") == 1)
        wait(lambda: op2.get_job_count("running") == 1)
        wait(
            lambda: get("//sys/cluster_nodes/{}/orchid/exec_node/job_resource_manager/resource_usage/user_slots".format(nodes[1])) == 2
        )

        # TODO(ignat): add check that jobs are not preemptible.

        enable_scheduler_jobs_on_node(nodes[0])
        wait(
            lambda: get("//sys/cluster_nodes/{}/orchid/exec_node/job_resource_manager/resource_limits/user_slots".format(nodes[0])) == 2
        )

        disable_scheduler_jobs_on_node(nodes[1], "test scheduler force abort")
        wait(
            lambda: get("//sys/cluster_nodes/{}/orchid/exec_node/job_resource_manager/resource_limits/user_slots".format(nodes[1])) == 0
        )

        wait(lambda: op1.get_job_count("aborted") == 1)
        wait(lambda: op2.get_job_count("aborted") == 1)

        wait(lambda: op1.get_job_count("running") == 1)
        wait(lambda: op2.get_job_count("running") == 1)

##################################################################


class TestSchedulerAggressivePreemption(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 4
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {"scheduler": {"fair_share_update_period": 100}}

    def setup_method(self, method):
        super(TestSchedulerAggressivePreemption, self).setup_method(method)
        update_pool_tree_config("default", {
            "aggressive_preemption_satisfaction_threshold": 0.2,
            "preemption_satisfaction_threshold": 1.0,
            "fair_share_starvation_tolerance": 0.9,
            "max_unpreemptible_running_allocation_count": 0,
            "fair_share_starvation_timeout": 100,
            "fair_share_aggressive_starvation_timeout": 200,
            "preemptive_scheduling_backoff": 0,
            "max_ephemeral_pools_per_user": 5,
        })

        nodes = ls("//sys/cluster_nodes")
        for i, node in enumerate(nodes):
            set("//sys/cluster_nodes/{}/@user_tags/end".format(node), self._get_node_group_tag(i % 2))
        for i, node in enumerate(nodes):
            wait(lambda: self._get_node_group_tag(i % 2) in get(scheduler_orchid_node_path(node) + "/tags"))

    def _get_starvation_status(self, op):
        return op.get_runtime_progress("scheduling_info_per_pool_tree/default/starvation_status")

    def _get_fair_share_ratio(self, op):
        return op.get_runtime_progress("scheduling_info_per_pool_tree/default/fair_share_ratio", 0.0)

    def _get_usage_ratio(self, op):
        return op.get_runtime_progress("scheduling_info_per_pool_tree/default/usage_ratio", 0.0)

    def _get_node_group_tag(self, group_index):
        return "group{}".format(group_index)

    @classmethod
    def modify_node_config(cls, config, cluster_index):
        for resource in ["cpu", "user_slots"]:
            config["job_resource_manager"]["resource_limits"][resource] = 2

    @authors("ignat", "eshcherbin")
    def test_aggressive_preemption(self):
        create_pool("special_pool")

        ops = []
        for index in range(2):
            op = run_sleeping_vanilla(
                job_count=4,
                spec={
                    "pool": "fake_pool{}".format(index),
                    "scheduling_tag_filter": self._get_node_group_tag(index),
                },
            )
            ops.append(op)
        time.sleep(3.0)

        for op in ops:
            wait(lambda: are_almost_equal(self._get_fair_share_ratio(op), 1.0 / 2.0))
            wait(lambda: are_almost_equal(self._get_usage_ratio(op), 1.0 / 2.0))
            wait(lambda: len(op.get_running_jobs()) == 4)

        op = run_sleeping_vanilla(
            spec={"pool": "special_pool"},
            task_patch={"cpu_limit": 2},
        )
        wait(lambda: are_almost_equal(self._get_fair_share_ratio(op), 1.0 / 4.0))

        time.sleep(3.0)
        assert are_almost_equal(self._get_usage_ratio(op), 0.0)

        set("//sys/pools/special_pool/@enable_aggressive_starvation", True)
        wait(lambda: are_almost_equal(self._get_usage_ratio(op), 1.0 / 4.0))
        wait(lambda: len(op.get_running_jobs()) == 1)

    @authors("eshcherbin")
    def test_no_aggressive_preemption_for_non_aggressively_starving_operation(self):
        create_pool("fake_pool", attributes={"weight": 10.0})
        create_pool("regular_pool", attributes={"weight": 1.0})
        create_pool("special_pool", attributes={"weight": 2.0})

        bad_op = run_sleeping_vanilla(
            job_count=8,
            spec={
                "pool": "fake_pool",
                "locality_timeout": 0,
                "update_preemptible_jobs_list_logging_period": 1,
            },
        )
        bad_op.wait_presence_in_scheduler()

        wait(lambda: are_almost_equal(self._get_fair_share_ratio(bad_op), 1.0))
        wait(lambda: are_almost_equal(self._get_usage_ratio(bad_op), 1.0))
        wait(lambda: len(bad_op.get_running_jobs()) == 8)

        op1 = run_sleeping_vanilla(job_count=2, spec={"pool": "special_pool", "locality_timeout": 0})
        op1.wait_presence_in_scheduler()

        wait(lambda: are_almost_equal(self._get_fair_share_ratio(op1), 2.0 / 12.0))
        wait(lambda: are_almost_equal(self._get_usage_ratio(op1), 1.0 / 8.0))
        wait(lambda: len(op1.get_running_jobs()) == 1)

        time.sleep(3)
        assert are_almost_equal(self._get_usage_ratio(op1), 1.0 / 8.0)
        assert len(op1.get_running_jobs()) == 1

        op2 = run_sleeping_vanilla(job_count=1, spec={"pool": "regular_pool", "locality_timeout": 0})
        op2.wait_presence_in_scheduler()

        wait(lambda: self._get_starvation_status(op2) == "starving")
        wait(lambda: are_almost_equal(self._get_fair_share_ratio(op2), 1.0 / 13.0))
        wait(lambda: are_almost_equal(self._get_usage_ratio(op2), 0.0))
        wait(lambda: len(op2.get_running_jobs()) == 0)

        time.sleep(3)
        assert are_almost_equal(self._get_usage_ratio(op2), 0.0)
        assert len(op2.get_running_jobs()) == 0

        # (1/8) / (1/6) = 0.75 < 0.9 (which is fair_share_starvation_tolerance).
        assert self._get_starvation_status(op1) == "starving"

        set("//sys/pools/special_pool/@enable_aggressive_starvation", True)
        wait(lambda: are_almost_equal(self._get_usage_ratio(op1), 2.0 / 8.0))
        wait(lambda: len(op1.get_running_jobs()) == 2)
        assert self._get_starvation_status(op1) == "non_starving"

    @authors("eshcherbin")
    def test_aggressive_preemption_for_gang_operations(self):
        set("//sys/pool_trees/default/@config/enable_aggressive_starvation", True)
        update_pool_tree_config_option("default", "allow_aggressive_preemption_for_gang_operations", False)

        gang_ops = []
        for i in range(2):
            op = run_sleeping_vanilla(
                job_count=4,
                spec={"is_gang": True, "scheduling_tag_filter": self._get_node_group_tag(i)}
            )
            gang_ops.append(op)

        for op in gang_ops:
            wait(lambda: exists(scheduler_orchid_operation_path(op.id)))
            wait(lambda: not get(scheduler_orchid_operation_path(op.id) + "/aggressive_preemption_allowed"))
            wait(lambda: not get(scheduler_orchid_operation_path(op.id) + "/effective_aggressive_preemption_allowed"))
            wait(lambda: are_almost_equal(self._get_fair_share_ratio(op), 1.0 / 2.0))
            wait(lambda: are_almost_equal(self._get_usage_ratio(op), 1.0 / 2.0))
            wait(lambda: len(op.get_running_jobs()) == 4)

        starving_op = run_sleeping_vanilla(job_count=1, task_patch={"cpu_limit": 2})
        wait(lambda: are_almost_equal(self._get_fair_share_ratio(starving_op), 1.0 / 4.0))
        wait(lambda: get(scheduler_orchid_operation_path(starving_op.id) + "/aggressive_preemption_allowed") == yson.YsonEntity())
        wait(lambda: get(scheduler_orchid_operation_path(starving_op.id) + "/effective_aggressive_preemption_allowed"))

        time.sleep(3)
        assert are_almost_equal(self._get_usage_ratio(starving_op), 0.0)

        update_pool_tree_config_option("default", "allow_aggressive_preemption_for_gang_operations", True)
        for op in gang_ops:
            wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/aggressive_preemption_allowed") == yson.YsonEntity())
            wait(lambda: get(scheduler_orchid_operation_path(op.id) + "/effective_aggressive_preemption_allowed"))
        wait(lambda: are_almost_equal(self._get_fair_share_ratio(starving_op), 1.0 / 4.0))


# TODO(ignat): merge with class above.
class TestSchedulerAggressivePreemption2(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {"scheduler": {"fair_share_update_period": 100}}

    def setup_method(self, method):
        super(TestSchedulerAggressivePreemption2, self).setup_method(method)
        update_pool_tree_config("default", {
            "aggressive_preemption_satisfaction_threshold": 0.2,
            "preemption_satisfaction_threshold": 0.75,
            "preemption_check_starvation": False,
            "preemption_check_satisfaction": False,
            "fair_share_starvation_timeout": 100,
            "max_unpreemptible_running_allocation_count": 2,
            "preemptive_scheduling_backoff": 0,
        })

    @classmethod
    def modify_node_config(cls, config, cluster_index):
        config["job_resource_manager"]["resource_limits"]["cpu"] = 5
        config["job_resource_manager"]["resource_limits"]["user_slots"] = 5

    @authors("eshcherbin")
    def test_allow_aggressive_starvation_preemption_operation(self):
        def get_fair_share_ratio(op_id):
            return get(scheduler_orchid_operation_path(op_id) + "/fair_share_ratio", default=0.0)

        def get_usage_ratio(op_id):
            return get(scheduler_orchid_operation_path(op_id) + "/usage_ratio", default=0.0)

        create_pool("honest_pool", attributes={"min_share_resources": {"cpu": 15}})
        create_pool(
            "honest_subpool_big",
            parent_name="honest_pool",
            attributes={
                "min_share_resources": {"cpu": 10},
                "allow_aggressive_preemption": False,
            },
        )
        create_pool(
            "honest_subpool_small",
            parent_name="honest_pool",
            attributes={"min_share_resources": {"cpu": 5}},
        )
        create_pool("dishonest_pool")
        create_pool(
            "special_pool",
            attributes={
                "min_share_resources": {"cpu": 10},
                "enable_aggressive_starvation": True,
            },
        )

        op_dishonest = run_sleeping_vanilla(spec={"pool": "dishonest_pool"}, task_patch={"cpu_limit": 5}, job_count=2)
        wait(lambda: are_almost_equal(get_fair_share_ratio(op_dishonest.id), 0.4))
        wait(lambda: are_almost_equal(get_usage_ratio(op_dishonest.id), 0.4))

        op_honest_small = run_sleeping_vanilla(spec={"pool": "honest_subpool_small"}, task_patch={"cpu_limit": 5})
        wait(lambda: are_almost_equal(get_fair_share_ratio(op_honest_small.id), 0.2))
        wait(lambda: are_almost_equal(get_usage_ratio(op_honest_small.id), 0.2))

        op_honest_big = run_sleeping_vanilla(spec={"pool": "honest_subpool_big"}, job_count=20)
        wait(lambda: are_almost_equal(get_fair_share_ratio(op_honest_big.id), 0.6))
        wait(lambda: are_almost_equal(get_usage_ratio(op_honest_big.id), 0.4))

        op_special = run_sleeping_vanilla(spec={"pool": "special_pool"}, job_count=10)

        wait(lambda: are_almost_equal(get_fair_share_ratio(op_special.id), 0.4))
        wait(lambda: are_almost_equal(get_usage_ratio(op_special.id), 0.08))
        wait(lambda: are_almost_equal(get_fair_share_ratio(op_honest_big.id), 0.4))
        wait(lambda: are_almost_equal(get_usage_ratio(op_honest_big.id), 0.32))

    @pytest.mark.skip("This test is almost the same as the previous one. Should we delete it?")
    @authors("eshcherbin")
    def test_allow_aggressive_starvation_preemption_ancestor(self):
        def get_fair_share_ratio(op_id):
            return get(scheduler_orchid_operation_path(op_id) + "/fair_share_ratio", default=0.0)

        def get_usage_ratio(op_id):
            return get(scheduler_orchid_operation_path(op_id) + "/usage_ratio", default=0.0)

        create_pool(
            "honest_pool",
            attributes={
                "min_share_resources": {"cpu": 15},
                "allow_aggressive_preemption": False,
            },
        )
        create_pool(
            "honest_subpool_big",
            parent_name="honest_pool",
            attributes={"min_share_resources": {"cpu": 10}},
        )
        create_pool(
            "honest_subpool_small",
            parent_name="honest_pool",
            attributes={"min_share_resources": {"cpu": 5}},
        )
        create_pool("dishonest_pool")
        create_pool(
            "special_pool",
            attributes={
                "min_share_resources": {"cpu": 10},
                "enable_aggressive_starvation": True,
            },
        )

        op_dishonest = run_sleeping_vanilla(spec={"pool": "dishonest_pool"}, task_patch={"cpu_limit": 5}, job_count=2)
        wait(lambda: are_almost_equal(get_fair_share_ratio(op_dishonest.id), 0.4))
        wait(lambda: are_almost_equal(get_usage_ratio(op_dishonest.id), 0.4))

        op_honest_small = run_sleeping_vanilla(spec={"pool": "honest_subpool_small"}, task_patch={"cpu_limit": 5})
        wait(lambda: are_almost_equal(get_fair_share_ratio(op_honest_small.id), 0.2))
        wait(lambda: are_almost_equal(get_usage_ratio(op_honest_small.id), 0.2))

        op_honest_big = run_sleeping_vanilla(spec={"pool": "honest_subpool_big"}, job_count=20)
        wait(lambda: are_almost_equal(get_fair_share_ratio(op_honest_big.id), 0.6))
        wait(lambda: are_almost_equal(get_usage_ratio(op_honest_big.id), 0.4))

        op_special = run_sleeping_vanilla(spec={"pool": "special_pool"}, job_count=10)

        wait(lambda: are_almost_equal(get_fair_share_ratio(op_special.id), 0.4))
        wait(lambda: are_almost_equal(get_fair_share_ratio(op_honest_big.id), 0.4))
        wait(lambda: are_almost_equal(get_usage_ratio(op_special.id), 0.08))
        wait(lambda: are_almost_equal(get_usage_ratio(op_honest_big.id), 0.32))


##################################################################


class TestIncreasedStarvationToleranceForFullySatisfiedDemand(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {"scheduler": {"fair_share_update_period": 100}}

    DELTA_CONTROLLER_AGENT_CONFIG = {"controller_agent": {"safe_scheduler_online_time": 1000000000}}

    DELTA_NODE_CONFIG = {"job_resource_manager": {"resource_limits": {"cpu": 10, "user_slots": 10}}}

    def setup_method(self, method):
        super(TestIncreasedStarvationToleranceForFullySatisfiedDemand, self).setup_method(method)
        update_pool_tree_config("default", {
            "aggressive_preemption_satisfaction_threshold": 0.2,
            "preemption_satisfaction_threshold": 1.0,
            "fair_share_starvation_tolerance": 0.8,
            "max_unpreemptible_running_allocation_count": 0,
            "fair_share_starvation_timeout": 100,
            "fair_share_aggressive_starvation_timeout": 200,
            "preemptive_scheduling_backoff": 0,
            "max_ephemeral_pools_per_user": 5,
        })

    @authors("eshcherbin")
    def test_increase_for_operation(self):
        update_scheduler_config("operation_hangup_check_period", 1000000000)

        create_pool("regular_pool", attributes={"strong_guarantee_resources": {"cpu": 1.0}})
        create_pool("starving_pool", attributes={"strong_guarantee_resources": {"cpu": 9.0}})

        regular_op = run_sleeping_vanilla(task_patch={"cpu_limit": 2.0}, spec={"pool": "regular_pool"})
        wait(lambda: get(scheduler_orchid_operation_path(regular_op.id) + "/resource_usage/cpu", default=None) == 2.0)

        starving_op = run_sleeping_vanilla(job_count=9, spec={"pool": "starving_pool"})
        wait(lambda: get(scheduler_orchid_operation_path(starving_op.id) + "/resource_usage/cpu", default=None) == 8.0)
        wait(lambda: get(scheduler_orchid_operation_path(starving_op.id) + "/starvation_status") == "starving")

        time.sleep(3.0)
        assert get(scheduler_orchid_operation_path(starving_op.id) + "/starvation_status") == "starving"

    @authors("eshcherbin")
    def test_no_increase_for_pool(self):
        update_scheduler_config("operation_hangup_check_period", 1000000000)

        create_pool("pool")

        regular_op = run_sleeping_vanilla(job_count=9, spec={"pool": "pool"})
        wait(lambda: get(scheduler_orchid_operation_path(regular_op.id) + "/resource_usage/cpu", default=None) == 9.0)

        starving_op = run_sleeping_vanilla(spec={"pool": "pool", "scheduling_tag_filter": "nonexistent_tag"})
        wait(lambda: get(scheduler_orchid_operation_path(starving_op.id) + "/starvation_status", default=None) == "starving")

        time.sleep(3.0)
        assert get(scheduler_orchid_operation_path(starving_op.id) + "/starvation_status") == "starving"
        assert get(scheduler_orchid_pool_path("pool") + "/starvation_status") == "non_starving"


##################################################################

class BaseTestDiskPreemption(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 4
    NUM_SCHEDULERS = 1

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "cluster_connection": {
            "medium_directory_synchronizer": {
                "sync_period": 100,
            },
        },
        "controller_agent": {
            "snapshot_period": 3000,
        }
    }

    DELTA_SCHEDULER_CONFIG = {
        "cluster_connection": {
            "medium_directory_synchronizer": {
                "sync_period": 100,
            },
        },
        "scheduler": {
            "fair_share_update_period": 100,
            "consider_disk_quota_in_preemptive_scheduling_discount": True,
        },
    }

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "slot_manager": {
                "disk_resources_update_period": 100,
            },
        },
        "data_node": {
            "volume_manager": {
                "enable_disk_quota": False,
            }
        },
        "job_resource_manager": {
            "resource_limits": {"user_slots": 2, "cpu": 2.0},
        }
    }

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_controller": {
                    "min_required_disk_space": 0,
                },
            },
        },
    }

    USE_PORTO = True

    SSD_NODE_COUNT = 2
    SSD_MEDIUM = "ssd_slots"
    SSD_NODE_TAG = "ssd_tag"

    DEFAULT_POOL_TREE_CONFIG = {
        "aggressive_preemption_satisfaction_threshold": 0.2,
        "preemption_satisfaction_threshold": 1.0,
        "fair_share_starvation_tolerance": 0.9,
        "max_unpreemptible_running_allocation_count": 0,
        "fair_share_starvation_timeout": 100,
        "fair_share_aggressive_starvation_timeout": 200,
        "preemptive_scheduling_backoff": 0,
        "max_ephemeral_pools_per_user": 5,
        "node_reconnection_timeout": 100,
        "ssd_priority_preemption": {
            "enable": False,
            "node_tag_filter": SSD_NODE_TAG,
            "medium_names": [SSD_MEDIUM],
        },
    }

    @classmethod
    def setup_class(cls):
        super(BaseTestDiskPreemption, cls).setup_class()
        for i in range(cls.NUM_NODES):
            for path in (cls._get_default_location_path(i), cls._get_ssd_location_path(i)):
                if os.path.exists(path):
                    shutil.rmtree(path)
                os.makedirs(path)

        cls._setup_media()

    def setup_method(self, method):
        super(BaseTestDiskPreemption, self).setup_method(method)
        update_pool_tree_config("default", BaseTestDiskPreemption.DEFAULT_POOL_TREE_CONFIG)

    @classmethod
    def _setup_media(cls):
        raise NotImplementedError()

    @classmethod
    def _setup_media_impl(cls, ssd_location_disk_quota, non_ssd_location_disk_quota=None, default_medium_name=None):
        with Restarter(cls.Env, NODES_SERVICE):
            for i, node_config in enumerate(cls.Env.configs["node"]):
                should_add_ssd = i < BaseTestDiskPreemption.SSD_NODE_COUNT

                config = deepcopy(node_config)

                slot_locations = []
                if non_ssd_location_disk_quota is not None:
                    slot_locations.append({
                        "path": cls._get_default_location_path(i),
                        "disk_quota": non_ssd_location_disk_quota,
                        "disk_usage_watermark": 0,
                    })
                if should_add_ssd and ssd_location_disk_quota is not None:
                    slot_locations.append({
                        "path": cls._get_ssd_location_path(i),
                        "disk_quota": ssd_location_disk_quota,
                        "disk_usage_watermark": 0,
                        "medium_name": BaseTestDiskPreemption.SSD_MEDIUM,
                    })

                config["exec_node"]["slot_manager"]["locations"] = slot_locations

                if default_medium_name is not None:
                    config["exec_node"]["slot_manager"]["default_medium_name"] = default_medium_name

                if should_add_ssd:
                    tags = config.pop("tags", list())
                    tags.append(BaseTestDiskPreemption.SSD_NODE_TAG)
                    config["tags"] = tags

                config_path = cls.Env.config_paths["node"][i]
                with open(config_path, "wb") as fout:
                    yson.dump(config, fout)

        actual_ssd_node_count = 0
        for node in ls("//sys/cluster_nodes"):
            has_ssd_tag = BaseTestDiskPreemption.SSD_NODE_TAG in get("//sys/cluster_nodes/{}/@tags".format(node))
            if not has_ssd_tag:
                continue

            wait(lambda: any(medium["medium_name"] == BaseTestDiskPreemption.SSD_MEDIUM
                             for medium in get("//sys/cluster_nodes/{}/@statistics/slot_locations".format(node))))
            actual_ssd_node_count += 1

        assert BaseTestDiskPreemption.SSD_NODE_COUNT == actual_ssd_node_count

    @classmethod
    def on_masters_started(cls):
        create_domestic_medium(BaseTestDiskPreemption.SSD_MEDIUM)

    def _run_sleeping_vanilla_with_ssd(self, disk_space=1, medium_name=SSD_MEDIUM, **kwargs):
        task_patch = kwargs.pop("task_patch", dict())
        task_patch["disk_request"] = {
            "disk_space": disk_space * 1024 * 1024,
        }
        if medium_name is not None:
            task_patch["disk_request"]["medium_name"] = medium_name

        kwargs["task_patch"] = task_patch
        return run_sleeping_vanilla(**kwargs)

    def _get_op_cpu_usage(self, op):
        return get(scheduler_orchid_operation_path(op.id) + "/resource_usage/cpu", default=None)

    @classmethod
    def _get_default_location_path(cls, node_index):
        return "{}/node-{}".format(cls.fake_default_disk_path, node_index)

    @classmethod
    def _get_ssd_location_path(cls, node_index):
        return "{}/node-{}".format(cls.fake_ssd_disk_path, node_index)


class TestSsdPriorityPreemption(BaseTestDiskPreemption):
    @classmethod
    def _setup_media(cls):
        cls._setup_media_impl(ssd_location_disk_quota=4 * 1024 * 1024, non_ssd_location_disk_quota=1 * 1024 * 1024)

    def _run_sleeping_vanilla_with_unpreemptible_jobs_at_ssd_nodes(self):
        # NB: Needed to disable sanity checks.
        update_controller_agent_config("safe_online_node_count", TestSsdPriorityPreemption.NUM_NODES + 1)
        update_scheduler_config("operation_hangup_check_period", 1000000000)

        nodes = ls("//sys/cluster_nodes")
        for node in nodes:
            if TestSsdPriorityPreemption.SSD_NODE_TAG in get("//sys/cluster_nodes/{}/@tags".format(node)):
                set("//sys/cluster_nodes/{}/@user_tags/end".format(node), "tag")

        op = run_sleeping_vanilla(job_count=8, spec={"scheduling_tag_filter": "tag"})
        wait(lambda: self._get_op_cpu_usage(op) == 4.0)

        time.sleep(0.5)
        for node in nodes:
            if TestSsdPriorityPreemption.SSD_NODE_TAG not in get("//sys/cluster_nodes/{}/@tags".format(node)):
                set("//sys/cluster_nodes/{}/@user_tags/end".format(node), "tag")
        wait(lambda: self._get_op_cpu_usage(op) == 8.0)

        return op

    def _get_op_starvation_status(self, op):
        return get(scheduler_orchid_operation_path(op.id) + "/starvation_status", default=None)

    def _get_op_fair_share(self, op):
        return get(scheduler_orchid_operation_path(op.id) + "/detailed_dominant_fair_share/total", default=None)

    def _assert_op_has_no_running_or_aborted_jobs(self, op):
        assert op.get_job_count("running") == 0
        assert op.get_job_count("aborted") == 0

    @authors("eshcherbin")
    def test_regular_ssd_priority_preemption(self):
        self._run_sleeping_vanilla_with_unpreemptible_jobs_at_ssd_nodes()

        op = self._run_sleeping_vanilla_with_ssd(job_count=4)
        wait(lambda: self._get_op_starvation_status(op) == "starving")

        time.sleep(3.0)
        self._assert_op_has_no_running_or_aborted_jobs(op)

        update_pool_tree_config_option("default", "ssd_priority_preemption/enable", True)
        wait(lambda: self._get_op_cpu_usage(op) == 4.0)

        disk_quota_usage = get(scheduler_orchid_operation_path(op.id) + "/disk_quota_usage")
        assert list(disk_quota_usage["disk_space_per_medium"].keys()) == [TestSsdPriorityPreemption.SSD_MEDIUM]

    @authors("eshcherbin")
    def test_regular_ssd_priority_preemption_many_operations(self):
        update_pool_tree_config_option("default", "max_unpreemptible_running_allocation_count", 1)

        create_pool("first", wait_for_orchid=False)
        create_pool("second")

        nodes = ls("//sys/cluster_nodes")
        blocking_ops = []
        for node in nodes:
            blocking_ops.append(run_sleeping_vanilla(
                job_count=2,
                spec={
                    "scheduling_tag_filter": node,
                    "pool": "first",
                },
            ))

        for op in blocking_ops:
            wait(lambda: self._get_op_cpu_usage(op) == 2.0)

        op = self._run_sleeping_vanilla_with_ssd(job_count=2, task_patch={"cpu_limit": 2.0})
        wait(lambda: self._get_op_starvation_status(op) == "starving")

        time.sleep(3.0)
        self._assert_op_has_no_running_or_aborted_jobs(op)

        update_pool_tree_config_option("default", "ssd_priority_preemption/enable", True)
        wait(lambda: self._get_op_cpu_usage(op) == 4.0)

    @authors("eshcherbin")
    def test_regular_preemption_of_ssd_jobs(self):
        update_pool_tree_config_option("default", "ssd_priority_preemption/enable", True)

        create_pool("first", attributes={"strong_guarantee_resources": {"cpu": 2.0}})
        create_pool("second", attributes={"strong_guarantee_resources": {"cpu": 6.0}})

        blocking_op = self._run_sleeping_vanilla_with_ssd(job_count=4, spec={"pool": "first"})
        wait(lambda: self._get_op_cpu_usage(blocking_op) == 4.0)

        run_sleeping_vanilla(job_count=8, spec={"pool": "second"})
        wait(lambda: self._get_op_cpu_usage(blocking_op) == 2.0)

    @authors("eshcherbin")
    @pytest.mark.parametrize("revive", [False, True])
    def test_no_aggressive_preemption_of_ssd_jobs_for_regular_jobs(self, revive):
        update_pool_tree_config_option("default", "ssd_priority_preemption/enable", True)

        create_pool("aggressive", attributes={"enable_aggressive_starvation": True})

        blocking_op = self._run_sleeping_vanilla_with_ssd(job_count=4)
        wait(lambda: self._get_op_cpu_usage(blocking_op) == 4.0)

        if revive:
            blocking_op.wait_for_fresh_snapshot()

            with Restarter(self.Env, SCHEDULERS_SERVICE):
                pass

            wait(lambda: self._get_op_cpu_usage(blocking_op) == 4.0)

        op = run_sleeping_vanilla(spec={
            "scheduling_tag_filter": TestSsdPriorityPreemption.SSD_NODE_TAG,
            "pool": "aggressive",
        })
        wait(lambda: self._get_op_starvation_status(op) == "aggressively_starving")

        time.sleep(3.0)
        self._assert_op_has_no_running_or_aborted_jobs(op)

        update_pool_tree_config_option("default", "ssd_priority_preemption/enable", False)
        wait(lambda: self._get_op_cpu_usage(op) == 1.0)

    @authors("eshcherbin")
    @pytest.mark.parametrize("revive", [False, True])
    def test_aggressive_preemption_of_ssd_jobs_for_ssd_jobs(self, revive):
        update_pool_tree_config_option("default", "ssd_priority_preemption/enable", True)

        create_pool("aggressive", attributes={"enable_aggressive_starvation": True})

        blocking_op = self._run_sleeping_vanilla_with_ssd(job_count=4)
        wait(lambda: self._get_op_cpu_usage(blocking_op) == 4.0)

        if revive:
            blocking_op.wait_for_fresh_snapshot()

            with Restarter(self.Env, SCHEDULERS_SERVICE):
                pass

            wait(lambda: self._get_op_cpu_usage(blocking_op) == 4.0)

        op = self._run_sleeping_vanilla_with_ssd(spec={"pool": "aggressive"})
        wait(lambda: self._get_op_cpu_usage(op) == 1.0)

    @authors("eshcherbin")
    def test_config(self):
        update_pool_tree_config_option("default", "ssd_priority_preemption/medium_names", ["foo", "bar"])
        wait(lambda: get("//sys/scheduler/@alerts"))
        wait(lambda: get("//sys/scheduler/@alerts")[0]["message"] == "Config contains unknown SSD priority preemption media")

        update_pool_tree_config_option("default", "ssd_priority_preemption/medium_names", [])
        wait(lambda: not get("//sys/scheduler/@alerts"))

        update_pool_tree_config_option("default", "ssd_priority_preemption/node_tag_filter", "")
        time.sleep(3.0)
        assert not get("//sys/scheduler/@alerts")

        with pytest.raises(YtResponseError):
            update_pool_tree_config_option("default", "ssd_priority_preemption/enable", True)

    @authors("eshcherbin")
    def test_profiling(self):
        update_pool_tree_config_option("default", "ssd_priority_preemption/enable", True)

        blocking_op = self._run_sleeping_vanilla_with_unpreemptible_jobs_at_ssd_nodes()

        ssd_preemption_aborted_job_counter = profiler_factory().at_scheduler().counter(
            "scheduler/pools/preempted_job_resources/cpu",
            tags={"preemption_reason": "ssd_preemption", "tree": "default", "pool": "root"})

        self._run_sleeping_vanilla_with_ssd()
        wait(lambda: blocking_op.get_job_count("aborted", verbose=True) > 0)

        wait(lambda: ssd_preemption_aborted_job_counter.get_delta() > 0.0)

    @authors("eshcherbin")
    def test_forbid_regular_jobs_on_ssd_nodes(self):
        update_pool_tree_config_option("default", "ssd_priority_preemption/enable", True)

        create_pool("protected", attributes={"allow_regular_jobs_on_ssd_nodes": False})
        create_pool("regular")

        protected_op = run_sleeping_vanilla(job_count=4, spec={"pool": "protected"})
        wait(lambda: get(scheduler_orchid_operation_path(protected_op.id) + "/resource_usage/cpu", default=0.0) == 4.0)
        wait(lambda: not get(scheduler_orchid_operation_path(protected_op.id) + "/are_regular_jobs_on_ssd_nodes_allowed"))

        regular_op = run_sleeping_vanilla(job_count=4, spec={"pool": "regular"})
        wait(lambda: get(scheduler_orchid_operation_path(regular_op.id) + "/resource_usage/cpu", default=0.0) == 4.0)
        wait(lambda: get(scheduler_orchid_operation_path(regular_op.id) + "/are_regular_jobs_on_ssd_nodes_allowed"))

        for _, job in protected_op.get_running_jobs().items():
            assert TestSsdPriorityPreemption.SSD_NODE_TAG not in get("//sys/cluster_nodes/{}/@tags".format(job["address"]))
        for _, job in regular_op.get_running_jobs().items():
            assert TestSsdPriorityPreemption.SSD_NODE_TAG in get("//sys/cluster_nodes/{}/@tags".format(job["address"]))

        regular_op.abort(wait_until_finished=True)

        ssd_op = self._run_sleeping_vanilla_with_ssd(job_count=1, spec={"pool": "protected"})
        wait(lambda: get(scheduler_orchid_operation_path(ssd_op.id) + "/resource_usage/cpu", default=0.0) == 1.0)

    @authors("eshcherbin")
    def test_ignore_preemption_blocking_ancestor_for_ssd_jobs(self):
        update_pool_tree_config("default", {
            "ssd_priority_preemption/enable": True,
            "preemption_check_satisfaction": True,
        })

        create_pool("prod", attributes={"strong_guarantee_resources": {"cpu": 7.0}})
        create_pool("research", attributes={"strong_guarantee_resources": {"cpu": 1.0}})

        blocking_op = run_sleeping_vanilla(job_count=5, task_patch={"cpu_limit": 1.5}, spec={"pool": "prod"})
        wait(lambda: get(scheduler_orchid_operation_path(blocking_op.id) + "/resource_usage/cpu", default=0.0) == 6.0)
        wait(lambda: are_almost_equal(get(scheduler_orchid_operation_path(blocking_op.id) + "/satisfaction_ratio"), 0.8))

        ssd_op = self._run_sleeping_vanilla_with_ssd(job_count=1, spec={"pool": "research"})
        wait(lambda: get(scheduler_orchid_operation_path(ssd_op.id) + "/resource_usage/cpu", default=0.0) == 1.0)
        wait(lambda: are_almost_equal(get(scheduler_orchid_operation_path(blocking_op.id) + "/satisfaction_ratio"), 4.5 / 7.0))


class TestDiskQuotaInRegularPreemption(BaseTestDiskPreemption):
    @classmethod
    def _setup_media(cls):
        cls._setup_media_impl(ssd_location_disk_quota=5 * 1024 * 1024, default_medium_name=cls.SSD_MEDIUM)

    @authors("omgronny")
    @pytest.mark.parametrize("use_default_medium_in_blocking_op", [False, True])
    @pytest.mark.parametrize("use_default_medium_in_starving_op", [False, True])
    def test_consider_disk_quota_discount_in_regular_preemption(self, use_default_medium_in_blocking_op, use_default_medium_in_starving_op):
        create_pool("first", attributes={"strong_guarantee_resources": {"cpu": 2.0}})
        create_pool("second", attributes={"strong_guarantee_resources": {"cpu": 6.0}})

        medium_name = TestDiskQuotaInRegularPreemption.SSD_MEDIUM
        if use_default_medium_in_blocking_op:
            medium_name = None

        blocking_op = self._run_sleeping_vanilla_with_ssd(disk_space=2, medium_name=medium_name, job_count=4, spec={"pool": "first"})
        wait(lambda: self._get_op_cpu_usage(blocking_op) == 4.0)

        medium_name = TestDiskQuotaInRegularPreemption.SSD_MEDIUM
        if use_default_medium_in_starving_op:
            medium_name = None

        starving_op = self._run_sleeping_vanilla_with_ssd(disk_space=2, medium_name=medium_name, job_count=8, spec={"pool": "second"})

        @wait_no_assert
        def wait_for_preemption():
            assert self._get_op_cpu_usage(starving_op) == 2.0
            assert self._get_op_cpu_usage(blocking_op) == 2.0
