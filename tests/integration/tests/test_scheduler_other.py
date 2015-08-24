
import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

import time
import __builtin__

##################################################################

class TestSchedulerOther(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "chunk_scratch_period" : 500
        }
    }

    def _set_banned_flag(self, value):
        if value:
            flag = True
            state = "offline"
        else:
            flag = False
            state = "online"

        nodes = get("//sys/nodes")
        assert len(nodes) == 1
        address = nodes.keys()[0]
        set("//sys/nodes/%s/@banned" % address, flag)

        # Give it enough time to register or unregister the node
        time.sleep(1.0)
        assert get("//sys/nodes/%s/@state" % address) == state
        print >>sys.stderr, "Node is %s" % state

    def _prepare_tables(self):
        create("table", "//tmp/t_in")
        set("//tmp/t_in/@replication_factor", 1)
        write_table("//tmp/t_in", {"foo": "bar"})

        create("table", "//tmp/t_out")
        set("//tmp/t_out/@replication_factor", 1)

    def test_strategies(self):
        self._prepare_tables()
        self._set_banned_flag(True)

        print >>sys.stderr, "Fail strategy"
        with pytest.raises(YtError):
            op_id = map(dont_track=True, in_="//tmp/t_in", out="//tmp/t_out", command="cat", spec={"unavailable_chunk_strategy": "fail"})
            track_op(op_id)

        print >>sys.stderr, "Skip strategy"
        map(in_="//tmp/t_in", out="//tmp/t_out", command="cat", spec={"unavailable_chunk_strategy": "skip"})
        assert read_table("//tmp/t_out") == []

        print >>sys.stderr, "Wait strategy"
        op_id = map(dont_track=True, in_="//tmp/t_in", out="//tmp/t_out", command="cat",  spec={"unavailable_chunk_strategy": "wait"})

        self._set_banned_flag(False)
        track_op(op_id)

        assert read_table("//tmp/t_out") == [ {"foo" : "bar"} ]

    def test_revive(self):
        self._prepare_tables()

        op_id = map(dont_track=True, in_="//tmp/t_in", out="//tmp/t_out", command="cat; sleep 3")

        time.sleep(2)
        self.Env.kill_service("scheduler")
        self.Env.start_schedulers("scheduler")

        track_op(op_id)

        assert read_table("//tmp/t_out") == [ {"foo" : "bar"} ]

    @pytest.mark.skipif("True")
    def test_aborting(self):
        # To run this test you must insert sleep into scheduler.cpp:TerminateOperation.
        # And then you must manually kill scheduler while scheduler handling this sleep after abort command.

        self._prepare_tables()

        op_id = map(dont_track=True, in_='//tmp/t_in', out='//tmp/t_out', command='cat; sleep 3')

        time.sleep(2)
        assert "running" == get("//sys/operations/" + op_id + "/@state")

        try:
            abort_op(op_id)
            # Here you must kill scheduler manually
        except:
            pass

        assert "aborting" == get("//sys/operations/" + op_id + "/@state")

        self.Env.start_schedulers("scheduler")

        time.sleep(1)

        assert "aborted" == get("//sys/operations/" + op_id + "/@state")

    def test_operation_time_limit(self):
        create("table", "//tmp/in")
        set("//tmp/in/@replication_factor", 1)

        create("table", "//tmp/out1")
        set("//tmp/out1/@replication_factor", 1)

        create("table", "//tmp/out2")
        set("//tmp/out2/@replication_factor", 1)

        write_table("//tmp/in", [{"foo": i} for i in xrange(5)])

        # Default infinite time limit.
        op1 = map(dont_track=True,
            command="sleep 1.0; cat >/dev/null",
            in_=["//tmp/in"],
            out="//tmp/out1")

        # Operation specific time limit.
        op2 = map(dont_track=True,
            command="sleep 1.0; cat >/dev/null",
            in_=["//tmp/in"],
            out="//tmp/out2",
            spec={'time_limit': 800})

        time.sleep(0.9)
        assert get("//sys/operations/{0}/@state".format(op1)) not in ["failing", "failed"]
        assert get("//sys/operations/{0}/@state".format(op2)) in ["failing", "failed"]

        track_op(op1)

    def test_pool_resource_limits(self):
        resource_limits = {"cpu": 1, "memory": 100, "network": 10}
        create("map_node", "//sys/pools/test_pool", attributes={"resource_limits": resource_limits})

        while True:
            pools = get("//sys/scheduler/orchid/scheduler/pools")
            if "test_pool" in pools:
                break
            time.sleep(0.1)

        stats = get("//sys/scheduler/orchid/scheduler")
        pool_resource_limits = stats["pools"]["test_pool"]["resource_limits"]
        for resource, limit in resource_limits.iteritems():
            assert pool_resource_limits[resource] == limit


class TestSchedulerMaxChunkPerJob(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "max_chunk_stripes_per_job" : 1,
            "max_chunk_count_per_fetch" : 1
        }
    }

    def test_max_chunk_stripes_per_job(self):
        data = [{"foo": i} for i in xrange(5)]
        create("table", "//tmp/in1")
        create("table", "//tmp/in2")
        create("table", "//tmp/out")
        write_table("//tmp/in1", data, sorted_by="foo")
        write_table("//tmp/in2", data, sorted_by="foo")

        merge(mode="ordered", in_=["//tmp/in1", "//tmp/in2"], out="//tmp/out", spec={"force_transform": True})
        assert data + data == read_table("//tmp/out")

        map(command="cat >/dev/null", in_=["//tmp/in1", "//tmp/in2"], out="//tmp/out")
        with pytest.raises(YtError):
            merge(mode="sorted", in_=["//tmp/in1", "//tmp/in2"], out="//tmp/out")
        with pytest.raises(YtError):
            reduce(command="cat >/dev/null", in_=["//tmp/in1", "//tmp/in2"], out="//tmp/out", reduce_by=["foo"])


class TestSchedulerRunningOperationsLimitJob(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "max_running_operations_per_pool" : 1
        }
    }

    def test_operations_pool_limit(self):
        create("map_node", "//sys/pools/test_pool_1")
        create("map_node", "//sys/pools/test_pool_2")

        create("table", "//tmp/in")
        create("table", "//tmp/out1")
        create("table", "//tmp/out2")
        create("table", "//tmp/out3")
        write_table("//tmp/in", [{"foo": i} for i in xrange(5)])

        op1 = map(
            dont_track=True,
            command="sleep 1.7; cat >/dev/null",
            in_=["//tmp/in"],
            out="//tmp/out1",
            spec={"pool": "test_pool_1"})

        op2 = map(
            dont_track=True,
            command="cat >/dev/null",
            in_=["//tmp/in"],
            out="//tmp/out2",
            spec={"pool": "test_pool_1"})

        op3 = map(
            dont_track=True,
            command="sleep 1.7; cat >/dev/null",
            in_=["//tmp/in"],
            out="//tmp/out3",
            spec={"pool": "test_pool_2"})

        time.sleep(1.5)
        assert get("//sys/operations/{0}/@state".format(op1)) == "running"
        assert get("//sys/operations/{0}/@state".format(op2)) == "pending"
        assert get("//sys/operations/{0}/@state".format(op3)) == "running"

        track_op(op1)
        track_op(op2)
        track_op(op3)

        assert read_table("//tmp/out1") == []
        assert read_table("//tmp/out2") == []
        assert read_table("//tmp/out3") == []

    def test_pending_operations_after_revive(self):
        create("table", "//tmp/in")
        create("table", "//tmp/out1")
        create("table", "//tmp/out2")
        data = [{"foo": i} for i in xrange(5)]
        write_table("//tmp/in", data)

        op1 = map(dont_track=True, command="sleep 5.0; cat", in_=["//tmp/in"], out="//tmp/out1")
        op2 = map(dont_track=True, command="cat", in_=["//tmp/in"], out="//tmp/out2")

        time.sleep(1.5)

        self.Env.kill_service("scheduler")
        self.Env.start_schedulers("scheduler")

        track_op(op1)
        track_op(op2)

        assert sorted(read_table("//tmp/out1")) == sorted(data)
        assert sorted(read_table("//tmp/out2")) == sorted(data)

    def test_abort_of_pending_operation(self):
        create("table", "//tmp/in")
        create("table", "//tmp/out1")
        create("table", "//tmp/out2")
        create("table", "//tmp/out3")
        write_table("//tmp/in", [{"foo": i} for i in xrange(5)])

        op1 = map(dont_track=True, command="sleep 2.0; cat >/dev/null", in_=["//tmp/in"], out="//tmp/out1")
        op2 = map(dont_track=True, command="cat >/dev/null", in_=["//tmp/in"], out="//tmp/out2")
        op3 = map(dont_track=True, command="cat >/dev/null", in_=["//tmp/in"], out="//tmp/out3")

        time.sleep(1.5)
        assert get("//sys/operations/{0}/@state".format(op1)) == "running"
        assert get("//sys/operations/{0}/@state".format(op2)) == "pending"
        assert get("//sys/operations/{0}/@state".format(op3)) == "pending"

        abort_op(op2)
        track_op(op1)
        track_op(op3)

        assert get("//sys/operations/{0}/@state".format(op1)) == "completed"
        assert get("//sys/operations/{0}/@state".format(op2)) == "aborted"
        assert get("//sys/operations/{0}/@state".format(op3)) == "completed"


class TestSchedulingTags(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 2
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler" : {
            "event_log" : {
                "flush_period" : 300,
                "retry_backoff_time": 300
            }
        }
    }

    DELTA_NODE_CONFIG = {
        "exec_agent" : {
            "slot_manager" : {
                "enable_cgroups" : False
            },
        }
    }

    def _prepare(self):
        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})
        create("table", "//tmp/t_out")

        self.node = list(get("//sys/nodes"))[0]
        set("//sys/nodes/{0}/@scheduling_tags".format(self.node), ["tagA", "tagB"])

    def test_failed_cases(self):
        self._prepare()

        map(command="cat", in_="//tmp/t_in", out="//tmp/t_out")
        with pytest.raises(YtError):
            map(command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec={"scheduling_tag": "tagC"})

        map(command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec={"scheduling_tag": "tagA"})
        assert read_table("//tmp/t_out") == [ {"foo" : "bar"} ]

        set("//sys/nodes/{0}/@scheduling_tags".format(self.node), [])
        time.sleep(1.0)
        with pytest.raises(YtError):
            map(command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec={"scheduling_tag": "tagA"})


    def test_pools(self):
        self._prepare()

        create("map_node", "//sys/pools/test_pool", attributes={"scheduling_tag": "tagA"})
        map(command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec={"pool": "test_pool"})
        assert read_table("//tmp/t_out") == [ {"foo" : "bar"} ]

    def test_tag_correctness(self):
        def get_job_nodes(op_id):
            nodes = __builtin__.set()
            for row in read_table("//sys/scheduler/event_log"):
                if row.get("event_type") == "job_started" and row.get("operation_id") == op_id:
                    nodes.add(row["node_address"])
            return nodes

        self._prepare()
        write_table("//tmp/t_in", [{"foo": "bar"} for _ in xrange(20)])

        set("//sys/nodes/{0}/@scheduling_tags".format(self.node), ["tagB"])
        time.sleep(1.2)
        op_id = map(dont_track=True, command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec={"scheduling_tag": "tagB", "job_count": 20})
        track_op(op_id)
        time.sleep(0.8)
        assert get_job_nodes(op_id) == __builtin__.set([self.node])


        op_id = map(dont_track=True, command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec={"job_count": 20})
        track_op(op_id)
        time.sleep(0.8)
        assert len(get_job_nodes(op_id)) <= 2


class TestSchedulerConfig(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler" : {
            "event_log" : {
                "retry_backoff_time" : 7,
                "flush_period" : 5000
            }
        }
    }

    def test_basic(self):
        orchid_scheduler_config = "//sys/scheduler/orchid/scheduler/config"
        assert get("{0}/event_log/flush_period".format(orchid_scheduler_config)) == 5000
        assert get("{0}/event_log/retry_backoff_time".format(orchid_scheduler_config)) == 7

        set("//sys/scheduler/config", { "event_log" : { "flush_period" : 10000 } })
        time.sleep(2)

        assert get("{0}/event_log/flush_period".format(orchid_scheduler_config)) == 10000
        assert get("{0}/event_log/retry_backoff_time".format(orchid_scheduler_config)) == 7

        set("//sys/scheduler/config", {})
        time.sleep(2)

        assert get("{0}/event_log/flush_period".format(orchid_scheduler_config)) == 5000
        assert get("{0}/event_log/retry_backoff_time".format(orchid_scheduler_config)) == 7
