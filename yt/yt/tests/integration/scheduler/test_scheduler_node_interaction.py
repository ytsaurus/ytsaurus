from yt_env_setup import YTEnvSetup, Restarter, SCHEDULERS_SERVICE, NODES_SERVICE

from yt_commands import (
    authors, wait, wait_breakpoint, release_breakpoint, with_breakpoint, create, ls,
    get, set, remove, exists, raises_yt_error,
    update_nodes_dynamic_config, update_scheduler_config, update_pool_tree_config_option,
    create_pool, create_pool_tree, read_table, write_table,
    map, run_test_vanilla,
    abort_job, get_job, list_jobs,
    get_operation_cypress_path, set_node_banned)

import yt_error_codes

from yt_helpers import profiler_factory

from yt_scheduler_helpers import (
    scheduler_orchid_node_path, scheduler_orchid_path)

from yt.common import YtError, YtResponseError

import pytest

import random
import time
import builtins

##################################################################


class TestIgnoreJobFailuresAtBannedNodes(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_NODES = 1

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "resource_limits": {
                "user_slots": 10,
                "cpu": 10,
                "memory": 10 * 1024 ** 3,
            }
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {"controller_agent": {"banned_exec_nodes_check_period": 100}}

    @authors("babenko")
    def test_ignore_job_failures_at_banned_nodes(self):
        create("table", "//tmp/t1", attributes={"replication_factor": 1})
        write_table("//tmp/t1", [{"foo": i} for i in range(10)])

        create("table", "//tmp/t2", attributes={"replication_factor": 1})

        op = map(
            track=False,
            in_="//tmp/t1",
            out="//tmp/t2",
            command=with_breakpoint("BREAKPOINT ; exit 1"),
            spec={
                "job_count": 10,
                "max_failed_job_count": 2,
                "ban_nodes_with_failed_jobs": True,
                "ignore_job_failures_at_banned_nodes": True,
                "fail_on_all_nodes_banned": False,
                "mapper": {"memory_limit": 100 * 1024 * 1024},
            },
        )

        jobs = wait_breakpoint(job_count=10)
        for id in jobs:
            release_breakpoint(job_id=id)

        wait(lambda: get(op.get_path() + "/@progress/jobs/failed") == 1)
        wait(lambda: get(op.get_path() + "/@progress/jobs/aborted/scheduled/node_banned") == 9)

    @authors("babenko")
    def test_fail_on_all_nodes_banned(self):
        create("table", "//tmp/t1", attributes={"replication_factor": 1})
        write_table("//tmp/t1", [{"foo": i} for i in range(10)])

        create("table", "//tmp/t2", attributes={"replication_factor": 1})

        op = map(
            track=False,
            in_="//tmp/t1",
            out="//tmp/t2",
            job_count=10,
            command=with_breakpoint("BREAKPOINT ; exit 1"),
            spec={
                "job_count": 10,
                "max_failed_job_count": 2,
                "ban_nodes_with_failed_jobs": True,
                "ignore_job_failures_at_banned_nodes": True,
                "fail_on_all_nodes_banned": True,
                "mapper": {"memory_limit": 100 * 1024 * 1024},
            },
        )

        jobs = wait_breakpoint(job_count=10)
        for id in jobs:
            release_breakpoint(job_id=id)

        with pytest.raises(YtError):
            op.track()

    @authors("ignat")
    def test_non_trivial_error_code(self):
        create("table", "//tmp/t1", attributes={"replication_factor": 1})
        write_table("//tmp/t1", [{"foo": i} for i in range(10)])

        create("table", "//tmp/t2", attributes={"replication_factor": 1})

        with pytest.raises(YtError):
            map(
                in_="//tmp/t1",
                out="//tmp/t2",
                job_count=10,
                command="exit 22",
                spec={
                    "max_failed_job_count": 1,
                },
            )


##################################################################


class TestReplacementCpuToVCpu(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "cpu_model": "AMD EPYC 7702 64-Core Processor",
            "resource_limits": {
                "cpu": 19.3,
            },
        }
    }

    def _get_job_node(self, op):
        wait(lambda: len(op.get_running_jobs()) >= 1)
        jobs = op.get_running_jobs()
        job = jobs[list(jobs)[0]]
        return job["address"]

    def _init_dynamic_config(self):
        update_nodes_dynamic_config({
            "job_resource_manager": {
                "enable_cpu_to_vcpu_factor": True,
                "cpu_model_to_cpu_to_vcpu_factor": {
                    "AMD EPYC 7702 64-Core Processor": 1.21
                }
            },
        })

    @authors("nadya73")
    def test_cpu_to_vcpu_factor_enough_only_with_vcpu(self):
        self._init_dynamic_config()
        op = run_test_vanilla(
            with_breakpoint("BREAKPOINT"),
            task_patch={"cpu_limit": 23},
        )

        node = self._get_job_node(op)

        wait_breakpoint()

        scheduler_resource_limits = get(scheduler_orchid_node_path(node) + "/resource_limits")
        assert scheduler_resource_limits["cpu"] == 23.35

        scheduler_resource_usage = get(scheduler_orchid_node_path(node) + "/resource_usage")
        assert scheduler_resource_usage["cpu"] == 23.0

        node_resource_usage = get(f"//sys/cluster_nodes/{node}/@resource_usage")
        assert node_resource_usage["cpu"] == 19.01
        assert node_resource_usage["vcpu"] == 23.0

        node_resource_limits = get(f"//sys/cluster_nodes/{node}/@resource_limits")
        assert node_resource_limits["cpu"] == 19.3
        assert node_resource_limits["vcpu"] == 23.35

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        for job_id in job_ids:
            base_resource_usage = get(f"//sys/cluster_nodes/{node}/orchid/exec_node/job_controller/active_jobs/{job_id}/base_resource_usage")
            additional_resource_usage = get(f"//sys/cluster_nodes/{node}/orchid/exec_node/job_controller/active_jobs/{job_id}/additional_resource_usage")
            assert base_resource_usage['cpu'] + additional_resource_usage['cpu'] == 19.01
            assert base_resource_usage['vcpu'] + additional_resource_usage['vcpu'] == 23.0

        release_breakpoint()
        op.track()

        assert get(op.get_path() + "/@progress/jobs/aborted/total") == 0
        assert get(op.get_path() + "/@progress/jobs/completed/total") == 1

    @authors("nadya73")
    def test_cpu_to_vcpu_factor_no_enough_cpu(self):
        self._init_dynamic_config()
        with raises_yt_error(yt_error_codes.NoOnlineNodeToScheduleJob):
            run_test_vanilla(
                "sleep 1",
                task_patch={"cpu_limit": 24},
                track=True,
            )

        node = ls("//sys/exec_nodes")[0]
        resource_limits = get(scheduler_orchid_node_path(node) + "/resource_limits")
        assert resource_limits["cpu"] == 23.35

    @authors("nadya73")
    def test_factor_from_dynamic_config(self):
        update_nodes_dynamic_config({
            "job_resource_manager": {
                "enable_cpu_to_vcpu_factor": True,
                "cpu_to_vcpu_factor": 1.21,
                "cpu_model_to_cpu_to_vcpu_factor": {
                    "AMD EPYC 7702 64-Core Processor": 10
                }
            }
        })

        with raises_yt_error(yt_error_codes.NoOnlineNodeToScheduleJob):
            run_test_vanilla(
                "sleep 1",
                task_patch={"cpu_limit": 24},
                track=True,
            )

        node = ls("//sys/exec_nodes")[0]
        resource_limits = get(scheduler_orchid_node_path(node) + "/resource_limits")
        assert resource_limits["cpu"] == 23.35


class TestVCpuDisableByDefault(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "job_resource_manager": {
            "cpu_model": "AMD EPYC 7702 64-Core Processor",
            "resource_limits": {
                "cpu": 19.3,
            }
        }
    }

    @authors("nadya73")
    def test_one_cpu_to_vcpu_factor(self):
        update_nodes_dynamic_config({
            "job_resource_manager": {
                "cpu_model_to_cpu_to_vcpu_factor": {
                    "AMD EPYC 7702 64-Core Processor": 1.21
                }
            },
        })

        with raises_yt_error(yt_error_codes.NoOnlineNodeToScheduleJob):
            run_test_vanilla(
                "sleep 1",
                task_patch={"cpu_limit": 23},
                track=True
            )

        node = ls("//sys/exec_nodes")[0]
        resource_limits = get(scheduler_orchid_node_path(node) + "/resource_limits")
        assert resource_limits["cpu"] == 19.3


##################################################################


class TestResourceLimitsOverrides(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_DYNAMIC_NODE_CONFIG = {
        "%true": {
            "exec_node": {
                "job_controller": {
                    "cpu_overdraft_timeout": 1000,
                    "memory_overdraft_timeout": 1000,
                    "resource_adjustment_period": 100,
                }
            }
        }
    }

    def _wait_for_jobs(self, op_id):
        jobs_path = get_operation_cypress_path(op_id) + "/controller_orchid/running_jobs"
        wait(
            lambda: exists(jobs_path) and len(get(jobs_path)) > 0,
            "Failed waiting for the first job",
        )
        return get(jobs_path)

    @authors("psushin")
    def test_cpu_override_with_preemption(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")

        write_table("<append=%true>//tmp/t_input", [{"key": i} for i in range(2)])

        # first job hangs, second is ok.
        op = map(
            command='if [ "$YT_JOB_INDEX" == "0" ]; then sleep 1000; else cat; fi',
            in_="//tmp/t_input",
            out="//tmp/t_output",
            track=False,
        )

        jobs = self._wait_for_jobs(op.id)
        job_id = next(iter(jobs.keys()))
        address = jobs[job_id]["address"]

        set("//sys/cluster_nodes/{0}/@resource_limits_overrides/cpu".format(address), 0)
        op.track()

        assert get(op.get_path() + "/@progress/jobs/aborted/total") == 1
        assert get(op.get_path() + "/@progress/jobs/completed/total") == 1

    @authors("psushin")
    def test_memory_override_with_preemption(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")

        write_table("<append=%true>//tmp/t_input", [{"key": i} for i in range(2)])

        # first job hangs, second is ok.
        op = map(
            command='if [ "$YT_JOB_INDEX" == "0" ]; then sleep 1000; else cat; fi',
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={"mapper": {"memory_limit": 100 * 1024 * 1024}},
            track=False,
        )

        jobs = self._wait_for_jobs(op.id)
        job_id = next(iter(jobs.keys()))
        address = jobs[job_id]["address"]

        set(
            "//sys/cluster_nodes/{0}/@resource_limits_overrides/user_memory".format(address),
            99 * 1024 * 1024,
        )
        op.track()

        assert get(op.get_path() + "/@progress/jobs/aborted/total") == 1
        assert get(op.get_path() + "/@progress/jobs/completed/total") == 1


##################################################################


class TestSchedulingTags(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 2
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {"scheduler": {"event_log": {"flush_period": 300, "retry_backoff_time": 300}}}

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "event_log": {"flush_period": 300, "retry_backoff_time": 300},
            "available_exec_nodes_check_period": 100,
            "max_available_exec_node_resources_update_period": 100,
            "snapshot_period": 500,
            "safe_scheduler_online_time": 2000,
        }
    }

    def _get_slots_by_filter(self, filter):
        try:
            return get("//sys/scheduler/orchid/scheduler/cluster/resource_limits_by_tags/{0}/user_slots".format(filter))
        except YtResponseError as err:
            if not err.is_resolve_error():
                raise

    def _prepare(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})
        create("table", "//tmp/t_out")

        nodes = list(get("//sys/cluster_nodes"))
        self.node = nodes[0]
        set(
            "//sys/cluster_nodes/{0}/@user_tags".format(self.node),
            ["default", "tagA", "tagB"],
        )
        set("//sys/cluster_nodes/{0}/@user_tags".format(nodes[1]), ["tagC"])

        set("//sys/pool_trees/default/@config/nodes_filter", "default")

        create_pool_tree("other", config={"nodes_filter": "tagC"})

        wait(lambda: self._get_slots_by_filter("default") == 1)
        wait(lambda: self._get_slots_by_filter("tagC") == 1)

    @authors("ignat")
    def test_tag_filters(self):
        self._prepare()

        map(command="cat", in_="//tmp/t_in", out="//tmp/t_out")
        with pytest.raises(YtError):
            map(
                command="cat",
                in_="//tmp/t_in",
                out="//tmp/t_out",
                spec={"scheduling_tag": "tagC"},
            )

        map(
            command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"scheduling_tag": "tagA"},
        )
        assert read_table("//tmp/t_out") == [{"foo": "bar"}]

        map(
            command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"scheduling_tag_filter": "tagA & !tagC"},
        )
        assert read_table("//tmp/t_out") == [{"foo": "bar"}]
        with pytest.raises(YtError):
            map(
                command="cat",
                in_="//tmp/t_in",
                out="//tmp/t_out",
                spec={"scheduling_tag_filter": "tagA & !tagB"},
            )

        set("//sys/cluster_nodes/{0}/@user_tags".format(self.node), ["default"])

        wait(lambda: "default" in get("//sys/scheduler/orchid/scheduler/nodes/{}/tags".format(self.node)))

        with pytest.raises(YtError):
            map(
                command="cat",
                in_="//tmp/t_in",
                out="//tmp/t_out",
                spec={"scheduling_tag": "tagA"},
            )

    @authors("ignat")
    def test_numeric_tag_filter(self):
        self._prepare()

        set("//sys/cluster_nodes/{0}/@user_tags".format(self.node), ["default", "127.0.0.1"])

        wait(lambda: "127.0.0.1" in get("//sys/scheduler/orchid/scheduler/nodes/{}/tags".format(self.node)))

        map(
            command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"scheduling_tag": "127.0.0.1"},
        )
        assert read_table("//tmp/t_out") == [{"foo": "bar"}]

    @authors("ignat")
    def test_pools(self):
        self._prepare()

        create_pool("test_pool", attributes={"scheduling_tag_filter": "tagA"})
        op = map(
            command="cat; echo 'AAA' >&2",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"pool": "test_pool"},
        )
        assert read_table("//tmp/t_out") == [{"foo": "bar"}]

        job_ids = op.list_jobs()
        assert len(job_ids) == 1
        for job_id in job_ids:
            job_addr = get_job(op.id, job_id)["address"]
            assert "tagA" in get("//sys/cluster_nodes/{0}/@user_tags".format(job_addr))

        # We do not support detection of the fact that no node satisfies pool scheduling tag filter.
        # set("//sys/pools/test_pool/@scheduling_tag_filter", "tagC")
        # with pytest.raises(YtError):
        #    map(command="cat", in_="//tmp/t_in", out="//tmp/t_out",
        #        spec={"pool": "test_pool"})

    @authors("ignat")
    def test_tag_correctness(self):
        def get_job_nodes(op):
            nodes = builtins.set()
            for row in read_table("//sys/scheduler/event_log"):
                if row.get("event_type") == "job_started" and row.get("operation_id") == op.id:
                    nodes.add(row["node_address"])
            return nodes

        self._prepare()
        write_table("//tmp/t_in", [{"foo": "bar"} for _ in range(20)])

        set("//sys/cluster_nodes/{0}/@user_tags".format(self.node), ["default", "tagB"])
        time.sleep(1.2)
        op = map(
            command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"scheduling_tag": "tagB", "job_count": 10},
        )
        time.sleep(0.8)
        assert get_job_nodes(op) == builtins.set([self.node])

        op = map(command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec={"job_count": 10})
        time.sleep(0.8)
        assert len(get_job_nodes(op)) <= 2

    @authors("ignat")
    def test_missing_nodes_after_revive(self):
        self._prepare()

        # Set safe_scheduler_online_time to infinity to avoid operations failure by controller.
        set("//sys/controller_agents/config", {"safe_scheduler_online_time": 120000})

        custom_node = None
        for node in ls("//sys/cluster_nodes", attributes=["user_tags"]):
            if "tagC" in node.attributes["user_tags"]:
                custom_node = str(node)

        op = map(
            track=False,
            command="sleep 1000",
            in_=["//tmp/t_in"],
            out="//tmp/t_out",
            spec={
                "pool_trees": ["other"],
                "scheduling_tag_filter": "tagC",
            },
        )

        wait(lambda: len(op.get_running_jobs()) > 0)

        op.wait_for_fresh_snapshot()

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            set("//sys/cluster_nodes/{0}/@user_tags".format(custom_node), [])

        wait(lambda: self._get_slots_by_filter("tagC") == 0)
        time.sleep(2)

        running_jobs = list(op.get_running_jobs())
        if running_jobs:
            assert len(running_jobs) == 1
            job_id = running_jobs[0]
            abort_job(job_id)
            wait(lambda: job_id not in op.get_running_jobs())

        # Just wait some time to be sure that scheduler have not run any other jobs.
        time.sleep(5)
        assert len(op.get_running_jobs()) == 0
        assert op.get_state() == "running"

        set("//sys/cluster_nodes/{0}/@user_tags".format(custom_node), ["tagC"])
        wait(lambda: len(op.get_running_jobs()) > 0)


##################################################################


class TestNodeDoubleRegistration(YTEnvSetup):
    NUM_SCHEDULERS = 1
    NUM_NODES = 1

    DELTA_SCHEDULER_CONFIG = {"scheduler": {"node_heartbeat_timeout": 10000}}

    DELTA_NODE_CONFIG = {
        "data_node": {
            "lease_transaction_timeout": 10000,
            "lease_transaction_ping_period": 10000,
            "register_timeout": 10000,
            "incremental_heartbeat_timeout": 10000,
            "full_heartbeat_timeout": 10000,
            "job_heartbeat_timeout": 10000,
        }
    }

    @authors("ignat")
    def test_remove(self):
        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 1
        node = nodes[0]

        wait(lambda: get("//sys/scheduler/orchid/scheduler/nodes/{}/scheduler_state".format(node)) == "online")

        with Restarter(self.Env, NODES_SERVICE):
            wait(lambda: get("//sys/cluster_nodes/{}/@state".format(node)) == "offline")
            wait(lambda: get("//sys/scheduler/orchid/scheduler/nodes/{}/master_state".format(node)) == "offline")
            wait(lambda: get("//sys/scheduler/orchid/scheduler/nodes/{}/scheduler_state".format(node)) == "offline")
            remove("//sys/cluster_nodes/" + node)

        wait(lambda: exists("//sys/scheduler/orchid/scheduler/nodes/{}".format(node)))
        wait(lambda: get("//sys/scheduler/orchid/scheduler/nodes/{}/master_state".format(node)) == "online")
        wait(lambda: get("//sys/scheduler/orchid/scheduler/nodes/{}/scheduler_state".format(node)) == "online")

    # It is disabled since Restarter await node to become online, this wait fails for banned node.
    @authors("ignat")
    def disabled_test_remove_banned(self):
        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 1
        node = nodes[0]

        set_node_banned(node, True)
        wait(lambda: get("//sys/scheduler/orchid/scheduler/nodes/{}/master_state".format(node)) == "offline")
        wait(lambda: get("//sys/scheduler/orchid/scheduler/nodes/{}/scheduler_state".format(node)) == "online")

        with Restarter(self.Env, NODES_SERVICE):
            wait(lambda: get("//sys/cluster_nodes/{}/@state".format(node)) == "offline")
            wait(lambda: get("//sys/scheduler/orchid/scheduler/nodes/{}/master_state".format(node)) == "offline")
            wait(lambda: get("//sys/scheduler/orchid/scheduler/nodes/{}/scheduler_state".format(node)) == "offline")

        wait(lambda: get("//sys/scheduler/orchid/scheduler/nodes/{}/master_state".format(node)) == "offline")
        wait(lambda: get("//sys/scheduler/orchid/scheduler/nodes/{}/scheduler_state".format(node)) == "online")


class TestNodeMultipleUnregistrations(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_NODES = 2

    DELTA_SCHEDULER_CONFIG = {"scheduler": {"node_heartbeat_timeout": 10000}}

    DELTA_NODE_CONFIG = {
        "data_node": {
            "lease_transaction_timeout": 10000,
            "lease_transaction_ping_period": 10000,
            "register_timeout": 10000,
            "incremental_heartbeat_timeout": 10000,
            "full_heartbeat_timeout": 10000,
            "job_heartbeat_timeout": 10000,
        }
    }

    @authors("ignat")
    @pytest.mark.timeout(150)
    def test_scheduler_node_removal(self):
        nodes = ls("//sys/cluster_nodes")
        assert len(nodes) == 2

        node = "localhost:" + str(self.Env.configs["node"][0]["rpc_port"])
        assert node in nodes

        create("table", "//tmp/t1", attributes={"replication_factor": 2})
        write_table("//tmp/t1", [{"foo": i} for i in range(4)])
        create("table", "//tmp/t2")
        create("table", "//tmp/t3")

        def start_op():
            tag = str(random.randint(0, 1000000))
            op = map(
                track=False,
                command=with_breakpoint("BREAKPOINT", tag),
                in_="//tmp/t1",
                out="//tmp/t2",
                spec={"data_size_per_job": 1},
            )
            jobs = wait_breakpoint(tag, job_count=2)
            release_breakpoint(breakpoint_name=tag, job_id=jobs[0])
            release_breakpoint(breakpoint_name=tag, job_id=jobs[1])
            time.sleep(5)
            wait(lambda: op.get_job_count("running") == 2)
            wait_breakpoint(tag)
            op.tag = tag
            return op

        op = start_op()
        with Restarter(self.Env, NODES_SERVICE, indexes=[0]):
            wait(lambda: get("//sys/cluster_nodes/{}/@state".format(node)) == "offline")
            wait(lambda: get("//sys/scheduler/orchid/scheduler/nodes/{}/master_state".format(node)) == "offline")
            wait(lambda: get("//sys/scheduler/orchid/scheduler/nodes/{}/scheduler_state".format(node)) == "offline")
            release_breakpoint(op.tag)
            wait(lambda: get(op.get_path() + "/@state") == "completed")

        op = start_op()
        with Restarter(self.Env, NODES_SERVICE, indexes=[0]):
            wait(lambda: get("//sys/cluster_nodes/{}/@state".format(node)) == "offline")
            wait(lambda: get("//sys/scheduler/orchid/scheduler/nodes/{}/master_state".format(node)) == "offline")
            wait(lambda: get("//sys/scheduler/orchid/scheduler/nodes/{}/scheduler_state".format(node)) == "offline")
            release_breakpoint(op.tag)
            wait(lambda: get(op.get_path() + "/@state") == "completed")

        op = start_op()
        set("//sys/scheduler/config/max_offline_node_age", 20000)
        with Restarter(self.Env, NODES_SERVICE, indexes=[0]):
            wait(lambda: get("//sys/cluster_nodes/{}/@state".format(node)) == "offline")
            wait(lambda: not exists("//sys/scheduler/orchid/scheduler/nodes/{}".format(node)))
            release_breakpoint(op.tag)
            wait(lambda: get(op.get_path() + "/@state") == "completed")

        op = start_op()
        release_breakpoint(op.tag)
        wait(lambda: get(op.get_path() + "/@state") == "completed")


##################################################################


class TestOperationNodeBan(YTEnvSetup):
    NUM_SCHEDULERS = 1
    NUM_NODES = 5

    @authors("babenko")
    def test_ban_nodes_with_failed_jobs(self):
        create("table", "//tmp/t1")
        write_table("//tmp/t1", [{"foo": i} for i in range(4)])

        create("table", "//tmp/t2")

        op = map(
            track=False,
            in_="//tmp/t1",
            out="//tmp/t2",
            command="exit 1",
            spec={
                "resource_limits": {"cpu": 1},
                "max_failed_job_count": 3,
                "ban_nodes_with_failed_jobs": True,
            },
        )

        with pytest.raises(YtError):
            op.track()

        jobs = list_jobs(op.id)["jobs"]
        assert all(job["state"] == "failed" for job in jobs)
        assert len(builtins.set(job["address"] for job in jobs)) == 3


class TestSchedulingHeartbeatThrottling(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 7
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "node_heartbeat_timeout": 10000000,
            "node_shard_count": 1,
        },
    }

    @authors("omgronny")
    def test_scheduler_throttles_heartbeats_by_complexity(self):
        update_scheduler_config("use_heartbeat_scheduling_complexity_throttling", True)
        update_scheduler_config("scheduling_heartbeat_complexity_limit", 18)

        all_nodes = ls("//sys/cluster_nodes")
        for i, node in enumerate(all_nodes):
            if i == 0:
                set("//sys/cluster_nodes/{}/@user_tags".format(node), ["tagA"])
            elif i < len(all_nodes) - 1:
                set("//sys/cluster_nodes/{}/@user_tags".format(node), ["tagB"])
            else:
                set("//sys/cluster_nodes/{}/@user_tags".format(node), ["tagC"])
        update_pool_tree_config_option(tree="default", option="nodes_filter", value="default")

        create_pool_tree("empty_tree", config={"nodes_filter": "tagA"})
        create_pool_tree("busy_tree", config={"nodes_filter": "tagB"})
        create_pool_tree("small_tree", config={"nodes_filter": "tagC"})

        create_pool("empty_pool", pool_tree="empty_tree")
        create_pool("busy_pool", pool_tree="busy_tree")
        create_pool("small_pool", pool_tree="small_tree")

        concurrent_complexity_limit_reached_count = profiler_factory().at_scheduler().counter("scheduler/node_heartbeat/concurrent_complexity_limit_reached_count")

        def get_total_scheduling_heartbeat_complexity():
            total_scheduling_heartbeat_complexity_orchid_path = scheduler_orchid_path() + "/scheduler/node_shards/total_scheduling_heartbeat_complexity"
            return get(total_scheduling_heartbeat_complexity_orchid_path)

        testing_options = {
            "schedule_job_delay_scheduler": {
                "duration": 10000000,
                "type": "async",
            },
        }

        run_test_vanilla(
            command="sleep 0.5",
            job_count=5,
            spec={
                "pool": "busy_pool",
                "pool_trees": ["busy_tree"],
                "testing": testing_options,
            })

        wait(lambda: get_total_scheduling_heartbeat_complexity() == 15)
        assert concurrent_complexity_limit_reached_count.get_delta() == 0

        run_test_vanilla(
            command="sleep 0.5",
            job_count=1,
            spec={
                "pool": "small_pool",
                "pool_trees": ["small_tree"],
                "testing": testing_options,
            })

        wait(lambda: get_total_scheduling_heartbeat_complexity() == 18)
        wait(lambda: concurrent_complexity_limit_reached_count.get_delta() > 0)
