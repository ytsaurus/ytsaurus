
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
        print "Node is %s" % state

    def _prepare_tables(self):
        create("table", "//tmp/t_in")
        set("//tmp/t_in/@replication_factor", 1)
        write("//tmp/t_in", {"foo": "bar"})

        create("table", "//tmp/t_out")
        set("//tmp/t_out/@replication_factor", 1)

    def test_strategies(self):
        self._prepare_tables()
        self._set_banned_flag(True)

        print "Fail strategy"
        with pytest.raises(YtError):
            op_id = map(dont_track=True, in_="//tmp/t_in", out="//tmp/t_out", command="cat", spec={"unavailable_chunk_strategy": "fail"})
            track_op(op_id)

        print "Skip strategy"
        map(in_="//tmp/t_in", out="//tmp/t_out", command="cat", spec={"unavailable_chunk_strategy": "skip"})
        assert read("//tmp/t_out") == []

        print "Wait strategy"
        op_id = map(dont_track=True, in_="//tmp/t_in", out="//tmp/t_out", command="cat",  spec={"unavailable_chunk_strategy": "wait"})

        self._set_banned_flag(False)
        track_op(op_id)

        assert read("//tmp/t_out") == [ {"foo" : "bar"} ]

    def test_revive(self):
        self._prepare_tables()

        op_id = map(dont_track=True, in_="//tmp/t_in", out="//tmp/t_out", command="cat; sleep 3")

        time.sleep(2)
        self.Env._kill_service("scheduler")
        self.Env.start_schedulers("scheduler")

        track_op(op_id)

        assert read("//tmp/t_out") == [ {"foo" : "bar"} ]

class TestSchedulingTags(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 2
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler" : {
            "event_log" : {
                "flush_period" : 300
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
        write("//tmp/t_in", {"foo": "bar"})
        create("table", "//tmp/t_out")

        self.node = list(get("//sys/nodes"))[0]
        set("//sys/nodes/{0}/@scheduling_tags".format(self.node), ["tagA", "tagB"])

    def test_failed_cases(self):
        self._prepare()

        map(command="cat", in_="//tmp/t_in", out="//tmp/t_out")
        with pytest.raises(YtError):
            map(command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec={"scheduling_tag": "tagC"})

        map(command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec={"scheduling_tag": "tagA"})
        assert read("//tmp/t_out") == [ {"foo" : "bar"} ]

        set("//sys/nodes/{0}/@scheduling_tags".format(self.node), [])
        time.sleep(1.0)
        with pytest.raises(YtError):
            map(command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec={"scheduling_tag": "tagA"})


    def test_pools(self):
        self._prepare()

        create("map_node", "//sys/pools/test_pool")
        set("//sys/pools/test_pool/@scheduling_tag", "tagA")
        map(command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec={"pool": "test_pool"})
        assert read("//tmp/t_out") == [ {"foo" : "bar"} ]

    def test_tag_correctness(self):
        def get_job_nodes(op_id):
            nodes = __builtin__.set()
            for row in read("//sys/scheduler/event_log"):
                if row.get("event_type") == "job_started":
                    nodes.add(row["node_address"])
            return nodes

        self._prepare()
        write("//tmp/t_in", [{"foo": "bar"} for _ in xrange(20)])

        set("//sys/nodes/{0}/@scheduling_tags".format(self.node), ["tagB"])
        time.sleep(1.2)
        op_id = map(dont_track=True, command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec={"scheduling_tag": "tagB", "job_count": 20})
        track_op(op_id)
        time.sleep(0.5)
        assert get_job_nodes(op_id) == __builtin__.set([self.node])


        op_id = map(dont_track=True, command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec={"job_count": 20})
        track_op(op_id)
        time.sleep(0.5)
        assert len(get_job_nodes(op_id)) <= 2


