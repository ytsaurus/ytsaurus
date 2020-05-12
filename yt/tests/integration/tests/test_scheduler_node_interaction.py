from yt_env_setup import YTEnvSetup, wait, Restarter, SCHEDULERS_SERVICE, NODES_SERVICE
from yt_commands import *
from yt_helpers import *

from yt.yson import *

import pytest

import random
import time
import __builtin__

##################################################################

class TestIgnoreJobFailuresAtBannedNodes(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_SCHEDULERS = 1
    NUM_NODES = 1

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "job_controller": {
                "resource_limits": {
                    "user_slots": 10,
                    "cpu": 10,
                    "memory": 10 * 1024 ** 3,
                }
            }
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "banned_exec_nodes_check_period": 100
        }
    }

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
                "mapper": {
                    "memory_limit": 100 * 1024 * 1024
                }
            })

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
                "mapper": {
                    "memory_limit": 100 * 1024 * 1024
                }
            })

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
                })

##################################################################

class TestResourceLimitsOverrides(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_NODE_CONFIG = {
        "exec_agent": {
            "job_controller": {
                "cpu_overdraft_timeout": 1000,
                "memory_overdraft_timeout": 1000,
                "resource_adjustment_period": 100,
            }
        }
    }

    def _wait_for_jobs(self, op_id):
        jobs_path = get_operation_cypress_path(op_id) + "/controller_orchid/running_jobs"
        wait(lambda: exists(jobs_path) and len(get(jobs_path)) > 0,
             "Failed waiting for the first job")
        return get(jobs_path)

    @authors("psushin")
    def test_cpu_override_with_preemption(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")

        write_table("<append=%true>//tmp/t_input", [{"key": i} for i in xrange(2)])

        # first job hangs, second is ok.
        op = map(
            command='if [ "$YT_JOB_INDEX" == "0" ]; then sleep 1000; else cat; fi',
            in_="//tmp/t_input",
            out="//tmp/t_output",
            track=False)

        jobs = self._wait_for_jobs(op.id)
        job_id = jobs.keys()[0]
        address = jobs[job_id]["address"]

        set("//sys/cluster_nodes/{0}/@resource_limits_overrides/cpu".format(address), 0)
        op.track()

        assert get(op.get_path() + "/@progress/jobs/aborted/total") == 1
        assert get(op.get_path() + "/@progress/jobs/completed/total") == 1

    @authors("psushin")
    def test_memory_override_with_preemption(self):
        create("table", "//tmp/t_input")
        create("table", "//tmp/t_output")

        write_table("<append=%true>//tmp/t_input", [{"key": i} for i in xrange(2)])

        # first job hangs, second is ok.
        op = map(
            command='if [ "$YT_JOB_INDEX" == "0" ]; then sleep 1000; else cat; fi',
            in_="//tmp/t_input",
            out="//tmp/t_output",
            spec={"mapper": {"memory_limit": 100 * 1024 * 1024}},
            track=False)

        jobs = self._wait_for_jobs(op.id)
        job_id = jobs.keys()[0]
        address = jobs[job_id]["address"]

        set("//sys/cluster_nodes/{0}/@resource_limits_overrides/user_memory".format(address), 99 * 1024 * 1024)
        op.track()

        assert get(op.get_path() + "/@progress/jobs/aborted/total") == 1
        assert get(op.get_path() + "/@progress/jobs/completed/total") == 1

##################################################################

class TestSchedulingTags(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 2
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "event_log": {
                "flush_period": 300,
                "retry_backoff_time": 300
            }
        }
    }

    DELTA_CONTROLLER_AGENT_CONFIG = {
        "controller_agent": {
            "event_log": {
                "flush_period": 300,
                "retry_backoff_time": 300
            },
            "available_exec_nodes_check_period": 100,
            "max_available_exec_node_resources_update_period": 100,
            "snapshot_period": 500,
            "safe_scheduler_online_time": 2000,
        }
    }

    def _get_slots_by_filter(self, filter):
        try:
            return get("//sys/scheduler/orchid/scheduler/cell/resource_limits_by_tags/{0}/user_slots".format(filter))
        except YtResponseError as err:
            if not err.is_resolve_error():
                raise

    def _prepare(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})
        create("table", "//tmp/t_out")

        nodes = list(get("//sys/cluster_nodes"))
        self.node = nodes[0]
        set("//sys/cluster_nodes/{0}/@user_tags".format(self.node), ["default", "tagA", "tagB"])
        set("//sys/cluster_nodes/{0}/@user_tags".format(nodes[1]), ["tagC"])

        set("//sys/pool_trees/default/@nodes_filter", "default")

        create_pool_tree("other", attributes={"nodes_filter": "tagC"})

        wait(lambda: self._get_slots_by_filter("default") == 1)
        wait(lambda: self._get_slots_by_filter("tagC") == 1)

    @authors("ignat")
    def test_tag_filters(self):
        self._prepare()

        map(command="cat", in_="//tmp/t_in", out="//tmp/t_out")
        with pytest.raises(YtError):
            map(command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec={"scheduling_tag": "tagC"})

        map(command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec={"scheduling_tag": "tagA"})
        assert read_table("//tmp/t_out") == [{"foo": "bar"}]

        map(command="cat", in_="//tmp/t_in", out="//tmp/t_out",
            spec={"scheduling_tag_filter": "tagA & !tagC"})
        assert read_table("//tmp/t_out") == [{"foo": "bar"}]
        with pytest.raises(YtError):
            map(command="cat", in_="//tmp/t_in", out="//tmp/t_out",
                spec={"scheduling_tag_filter": "tagA & !tagB"})

        set("//sys/cluster_nodes/{0}/@user_tags".format(self.node), ["default"])
        time.sleep(1.0)
        with pytest.raises(YtError):
            map(command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec={"scheduling_tag": "tagA"})


    @authors("ignat")
    def test_pools(self):
        self._prepare()

        create_pool("test_pool", attributes={"scheduling_tag_filter": "tagA"})
        op = map(command="cat; echo 'AAA' >&2", in_="//tmp/t_in", out="//tmp/t_out", spec={"pool": "test_pool"})
        assert read_table("//tmp/t_out") == [{"foo": "bar"}]

        job_ids = ls(op.get_path() + "/jobs")
        assert len(job_ids) == 1
        for job_id in job_ids:
            job_addr = get(op.get_path() + "/jobs/{}/@address".format(job_id))
            assert "tagA" in get("//sys/cluster_nodes/{0}/@user_tags".format(job_addr))

        # We do not support detection of the fact that no node satisfies pool scheduling tag filter.
        #set("//sys/pools/test_pool/@scheduling_tag_filter", "tagC")
        #with pytest.raises(YtError):
        #    map(command="cat", in_="//tmp/t_in", out="//tmp/t_out",
        #        spec={"pool": "test_pool"})

    @authors("ignat")
    def test_tag_correctness(self):
        def get_job_nodes(op):
            nodes = __builtin__.set()
            for row in read_table("//sys/scheduler/event_log"):
                if row.get("event_type") == "job_started" and row.get("operation_id") == op.id:
                    nodes.add(row["node_address"])
            return nodes

        self._prepare()
        write_table("//tmp/t_in", [{"foo": "bar"} for _ in xrange(20)])

        set("//sys/cluster_nodes/{0}/@user_tags".format(self.node), ["default", "tagB"])
        time.sleep(1.2)
        op = map(command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec={"scheduling_tag": "tagB", "job_count": 20})
        time.sleep(0.8)
        assert get_job_nodes(op) == __builtin__.set([self.node])

        op = map(command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec={"job_count": 20})
        time.sleep(0.8)
        assert len(get_job_nodes(op)) <= 2

    @authors("ignat")
    def test_missing_nodes_after_revive(self):
        self._prepare()

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
            })

        wait(lambda: len(op.get_running_jobs()) > 0)

        op.wait_for_fresh_snapshot()

        with Restarter(self.Env, SCHEDULERS_SERVICE):
            set("//sys/cluster_nodes/{0}/@user_tags".format(custom_node), [])

        wait(lambda: self._get_slots_by_filter("tagC") == 0)
        time.sleep(2)

        running_jobs = list(op.get_running_jobs())
        if running_jobs:
            assert(len(running_jobs) == 1)
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

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "node_heartbeat_timeout": 10000
        }
    }

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

        set_banned_flag(True, [node])
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

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "node_heartbeat_timeout": 10000
        }
    }

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
                spec={"data_size_per_job": 1})
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
                "resource_limits": {
                    "cpu": 1
                },
                "max_failed_job_count": 3,
                "ban_nodes_with_failed_jobs": True
            })

        with pytest.raises(YtError):
            op.track()

        jobs = ls(op.get_path() + "/jobs", attributes=["state", "address"])
        assert all(job.attributes["state"] == "failed" for job in jobs)
        assert len(__builtin__.set(job.attributes["address"] for job in jobs)) == 3

