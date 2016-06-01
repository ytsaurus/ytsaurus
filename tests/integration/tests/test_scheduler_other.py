
import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

import time
import __builtin__

##################################################################

def set_banned_flag(value, nodes=None):
    if value:
        flag = True
        state = "offline"
    else:
        flag = False
        state = "online"

    if not nodes:
        nodes = get("//sys/nodes").keys()

    for address in nodes:
        set("//sys/nodes/{0}/@banned".format(address), flag)

    # Give it enough time to register or unregister the node
    time.sleep(1.0)

    for address in nodes:
        assert get("//sys/nodes/{0}/@state".format(address)) == state
        print >>sys.stderr, "Node {0} is {1}".format(address, state)

class TestSchedulerOther(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "operation_time_limit_check_period" : 100,
            "connect_retry_backoff_time": 100,
            "fair_share_update_period": 100,
        }
    }

    def _create_table(self, table):
        create("table", table)
        set(table + "/@replication_factor", 1)

    def _prepare_tables(self):
        self._create_table("//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})

        self._create_table("//tmp/t_out")

    def test_revive(self):
        self._prepare_tables()

        op = map(dont_track=True, in_="//tmp/t_in", out="//tmp/t_out", command="cat; sleep 3")

        time.sleep(2)
        self.Env.kill_service("scheduler")
        self.Env.start_schedulers("scheduler")

        op.track()

        assert read_table("//tmp/t_out") == [ {"foo" : "bar"} ]

    @pytest.mark.skipif("True")
    def test_disconnect_during_revive(self):
        op_count = 20

        self._create_table("//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})
        for i in xrange(1, op_count + 1):
            self._create_table("//tmp/t_out" + str(i))

        ops = []
        for i in xrange(1, op_count):
            ops.append(
                map(dont_track=True,
                    command="cat",
                    in_=["//tmp/t_in"],
                    out="//tmp/t_out" + str(i)))

        for i in range(10):
            while True:
                scheduler_locks = get("//sys/scheduler/lock/@locks", verbose=False)
                if len(scheduler_locks) > 0:
                    scheduler_transaction = scheduler_locks[0]["transaction_id"]
                    abort_transaction(scheduler_transaction)
                    break
                time.sleep(0.01)

        for op in ops:
            op.track()

        for i in xrange(1, op_count):
            assert read_table("//tmp/t_out" + str(i)) == [ {"foo" : "bar"} ]



    @pytest.mark.skipif("True")
    def test_aborting(self):
        # To run this test you must insert sleep into scheduler.cpp:TerminateOperation.
        # And then you must manually kill scheduler while scheduler handling this sleep after abort command.

        self._prepare_tables()

        op = map(dont_track=True, in_='//tmp/t_in', out='//tmp/t_out', command='cat; sleep 3')

        time.sleep(2)
        assert "running" == get("//sys/operations/" + op.id + "/@state")

        try:
            op.abort()
            # Here you must kill scheduler manually
        except:
            pass

        assert "aborting" == get("//sys/operations/" + op.id + "/@state")

        self.Env.start_schedulers("scheduler")

        time.sleep(1)

        assert "aborted" == get("//sys/operations/" + op.id + "/@state")

    def test_operation_time_limit(self):
        self._create_table("//tmp/in")
        self._create_table("//tmp/out1")
        self._create_table("//tmp/out2")

        write_table("//tmp/in", [{"foo": i} for i in xrange(5)])

        # Default infinite time limit.
        op1 = map(dont_track=True,
            command="sleep 1.0; cat >/dev/null",
            in_=["//tmp/in"],
            out="//tmp/out1")

        # Operation specific time limit.
        op2 = map(dont_track=True,
            command="sleep 3.0; cat >/dev/null",
            in_=["//tmp/in"],
            out="//tmp/out2",
            spec={'time_limit': 1000})

        # we should wait as least time_limit + heartbeat_period
        time.sleep(1.2)
        assert get("//sys/operations/{0}/@state".format(op1.id)) not in ["failing", "failed"]
        assert get("//sys/operations/{0}/@state".format(op2.id)) in ["failing", "failed"]

        op1.track()
        with pytest.raises(YtError):
            op2.track()

    def test_fifo_default(self):
        self._create_table("//tmp/in")
        self._create_table("//tmp/out1")
        self._create_table("//tmp/out2")
        self._create_table("//tmp/out3")
        write_table("//tmp/in", [{"foo": i} for i in xrange(5)])

        create("map_node", "//sys/pools/fifo_pool", ignore_existing=True)
        set("//sys/pools/fifo_pool/@mode", "fifo")

        # Waiting for updating pool settings.
        time.sleep(0.6)

        ops = []
        for i in xrange(1, 4):
            ops.append(
                map(dont_track=True,
                    command="sleep 0.3; cat >/dev/null",
                    in_=["//tmp/in"],
                    out="//tmp/out" + str(i),
                    spec={"pool": "fifo_pool"}))

        for op in ops:
            op.track()

        finish_times = [get("//sys/operations/{0}/@finish_time".format(op.id)) for op in ops]
        for cur, next in zip(finish_times, finish_times[1:]):
            assert cur < next

    def test_fifo_by_pending_job_count(self):
        for i in xrange(1, 4):
            self._create_table("//tmp/in" + str(i))
            self._create_table("//tmp/out" + str(i))
            write_table("//tmp/in" + str(i), [{"foo": j} for j in xrange(3 * (4 - i))])

        create("map_node", "//sys/pools/fifo_pool", ignore_existing=True)
        set("//sys/pools/fifo_pool/@mode", "fifo")
        set("//sys/pools/fifo_pool/@fifo_sort_parameters", ["pending_job_count"])

        # Wait until pools tree would be updated
        time.sleep(0.6)

        ops = []
        for i in xrange(1, 4):
            ops.append(
                map(dont_track=True,
                    command="sleep 0.5; cat >/dev/null",
                    in_=["//tmp/in" + str(i)],
                    out="//tmp/out" + str(i),
                    spec={"pool": "fifo_pool", "data_size_per_job": 1}))

        for op in ops:
            op.track()

        finish_times = [get("//sys/operations/{0}/@finish_time".format(op.id)) for op in ops]
        for cur, next in zip(finish_times, finish_times[1:]):
            assert cur > next

    def test_preparing_operation_transactions(self):
        self._prepare_tables()

        set_banned_flag(True)
        op = sort(
            dont_track=True,
            in_="//tmp/t_in",
            out="//tmp/t_in",
            command="cat",
            sort_by=["foo"])
        time.sleep(2)

        for tx in ls("//sys/transactions", attributes=["operation_id"]):
            if tx.attributes.get("operation_id", "") == op.id:
                abort_transaction(tx)

        with pytest.raises(YtError):
            op.track()

        set_banned_flag(False)

    def test_abort_custom_error_message(self):
        self._prepare_tables()

        op = map(dont_track=True, in_="//tmp/t_in", out="//tmp/t_out", command="cat; sleep 3")
        op.abort(abort_message="Test abort")

        assert op.get_state() == "aborted"
        assert get("//sys/operations/{0}/@result/error/inner_errors/0/message".format(op.id)) == "Test abort"


class TestStrategies(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 2
    NUM_SCHEDULERS = 1

    def _prepare_tables(self):
        create("table", "//tmp/t_in")
        set("//tmp/t_in/@replication_factor", 1)
        write_table("//tmp/t_in", {"foo": "bar"})

        create("table", "//tmp/t_out")
        set("//tmp/t_out/@replication_factor", 1)

    def _get_table_chunk_node(self, table):
        chunk_ids = get(table + "/@chunk_ids")
        chunk_id = chunk_ids[0]
        replicas = get("#{0}/@stored_replicas".format(chunk_id))
        assert len(replicas) == 1

        return replicas[0]

    def test_strategies(self):
        self._prepare_tables()

        node = self._get_table_chunk_node("//tmp/t_in")
        set_banned_flag(True, [ node ])

        print >>sys.stderr,  "Fail strategy"
        with pytest.raises(YtError):
            op = map(in_="//tmp/t_in", out="//tmp/t_out", command="cat", spec={"unavailable_chunk_strategy": "fail"})

        print >>sys.stderr,  "Skip strategy"
        map(in_="//tmp/t_in", out="//tmp/t_out", command="cat", spec={"unavailable_chunk_strategy": "skip"})
        assert read_table("//tmp/t_out") == []

        print >>sys.stderr,  "Wait strategy"
        op = map(dont_track=True, in_="//tmp/t_in", out="//tmp/t_out", command="cat",  spec={"unavailable_chunk_strategy": "wait"})

        set_banned_flag(False, [ node ])
        op.track()

        assert read_table("//tmp/t_out") == [ {"foo" : "bar"} ]

    def test_strategies_in_sort(self):
        v1 = {"key" : "aaa"}
        v2 = {"key" : "bb"}
        v3 = {"key" : "bbxx"}
        v4 = {"key" : "zfoo"}
        v5 = {"key" : "zzz"}

        create("table", "//tmp/t_in")
        set("//tmp/t_in/@replication_factor", 1)
        write_table("//tmp/t_in", [v3, v5, v1, v2, v4]) # some random order

        create("table", "//tmp/t_out")
        set("//tmp/t_out/@replication_factor", 1)

        set_banned_flag(True)

        print >>sys.stderr, "Fail strategy"
        with pytest.raises(YtError):
            op = sort(in_="//tmp/t_in", out="//tmp/t_out", sort_by="key", spec={"unavailable_chunk_strategy": "fail"})

        print >>sys.stderr, "Skip strategy"
        sort(in_="//tmp/t_in", out="//tmp/t_out", sort_by="key", spec={"unavailable_chunk_strategy": "skip"})
        assert read_table("//tmp/t_out") == []

        print >>sys.stderr, "Wait strategy"
        op = sort(dont_track=True, in_="//tmp/t_in", out="//tmp/t_out", sort_by="key", spec={"unavailable_chunk_strategy": "wait"})

        # Give a chance to scraper to work
        time.sleep(1.0)
        set_banned_flag(False)
        op.track()

        assert read_table("//tmp/t_out") == [v1, v2, v3, v4, v5]
        assert get("//tmp/t_out/@sorted") == True
        assert get("//tmp/t_out/@sorted_by") == ["key"]

    def test_strategies_in_merge(self):
        create("table", "//tmp/t1")
        set("//tmp/t1/@replication_factor", 1)
        write_table("<append=true>//tmp/t1", [{"a": 0}, {"a": 2}], sorted_by="a")
        write_table("<append=true>//tmp/t1", [{"a": 4}, {"a": 6}], sorted_by="a")

        create("table", "//tmp/t2")
        set("//tmp/t2/@replication_factor", 1)
        write_table("<append=true>//tmp/t2", [{"a": 1}, {"a": 3}], sorted_by="a")
        write_table("<append=true>//tmp/t2", [{"a": 5}, {"a": 7}], sorted_by="a")

        create("table", "//tmp/t_out")
        set("//tmp/t_out/@replication_factor", 1)

        set_banned_flag(True)

        print >>sys.stderr, "Fail strategy"
        with pytest.raises(YtError):
            op = merge(mode="sorted", in_=["//tmp/t1", "//tmp/t2"], out="//tmp/t_out", spec={"unavailable_chunk_strategy": "fail"})

        print >>sys.stderr, "Skip strategy"
        merge(mode="sorted", in_=["//tmp/t1", "//tmp/t2"], out="//tmp/t_out", spec={"unavailable_chunk_strategy": "skip"})
        assert read_table("//tmp/t_out") == []

        print >>sys.stderr, "Wait strategy"
        op = merge(dont_track=True, mode="sorted", in_=["//tmp/t1", "//tmp/t2"], out="//tmp/t_out", spec={"unavailable_chunk_strategy": "wait"})

        # Give a chance for scraper to work
        time.sleep(1.0)
        set_banned_flag(False)
        op.track()

        assert read_table("//tmp/t_out") == [{"a": i} for i in range(8)]
        assert get("//tmp/t_out/@sorted") == True
        assert get("//tmp/t_out/@sorted_by") == ["a"]

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



        op = merge(mode="ordered", in_=["//tmp/in1", "//tmp/in2"], out="//tmp/out", spec={"force_transform": True})
        assert data + data == read_table("//tmp/out")

        # Must be 2 jobs since input has 2 chunks.
        assert get("//sys/operations/{0}/@progress/jobs/total".format(op.id)) == 2

        op = map(command="cat >/dev/null", in_=["//tmp/in1", "//tmp/in2"], out="//tmp/out")
        assert get("//sys/operations/{0}/@progress/jobs/total".format(op.id)) == 2

        op = merge(mode="sorted", in_=["//tmp/in1", "//tmp/in2"], out="//tmp/out")
        assert get("//sys/operations/{0}/@progress/jobs/total".format(op.id)) == 2

        op = reduce(command="cat >/dev/null", in_=["//tmp/in1", "//tmp/in2"], out="//tmp/out", reduce_by=["foo"])
        assert get("//sys/operations/{0}/@progress/jobs/total".format(op.id)) == 2


class TestSchedulerMaxChildrenPerAttachRequest(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "max_children_per_attach_request": 1,
        }
    }

    def test_max_children_per_attach_request(self):
        data = [{"foo": i} for i in xrange(3)]
        create("table", "//tmp/in")
        create("table", "//tmp/out")
        write_table("//tmp/in", data)

        map(command="cat", in_="//tmp/in", out="//tmp/out", spec={"data_size_per_job": 1})

        assert sorted(read_table("//tmp/out")) == sorted(data)
        assert get("//tmp/out/@row_count") == 3

    def test_max_children_per_attach_request_in_live_preview(self):
        data = [{"foo": i} for i in xrange(3)]
        create("table", "//tmp/in")
        create("table", "//tmp/out")
        write_table("//tmp/in", data)

        op = map(
            waiting_jobs=True,
            dont_track=True,
            command="cat",
            in_="//tmp/in",
            out="//tmp/out",
            spec={"data_size_per_job": 1})

        op.resume_job(op.jobs[0])
        op.resume_job(op.jobs[1])
        time.sleep(2)

        operation_path = "//sys/operations/{0}".format(op.id)
        transaction_id = get(operation_path + "/@async_scheduler_transaction_id")
        assert len(read_table(operation_path + "/output_0", tx=transaction_id)) == 2
        assert get(operation_path + "/output_0/@row_count", tx=transaction_id) == 2

        op.resume_jobs()
        op.track()


class TestSchedulerOperationLimits(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "max_running_operations_per_pool" : 1
        }
    }

    def teardown(self):
        set("//sys/pools", {})

    def _run_operations(self):
        create("table", "//tmp/in")
        create("table", "//tmp/out1")
        create("table", "//tmp/out2")
        create("table", "//tmp/out3")
        write_table("//tmp/in", [{"foo": i} for i in xrange(5)])

        op1 = map(
            dont_track=True,
            waiting_jobs=True,
            command="cat >/dev/null",
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
            waiting_jobs=True,
            command="cat >/dev/null",
            in_=["//tmp/in"],
            out="//tmp/out3",
            spec={"pool": "test_pool_2"})

        op1.ensure_running()
        with pytest.raises(TimeoutError):
            op2.ensure_running(timeout=1.0)
        op3.ensure_running()

        op1.resume_jobs()
        op3.resume_jobs()

        op1.track()
        op2.track()
        op3.track()

        assert read_table("//tmp/out1") == []
        assert read_table("//tmp/out2") == []
        assert read_table("//tmp/out3") == []

    def test_operations_pool_limit(self):
        create("map_node", "//sys/pools/test_pool_1")
        create("map_node", "//sys/pools/test_pool_2")
        self._run_operations()

    def test_operations_recursive_pool_limit(self):
        create("map_node", "//sys/pools/research")
        set("//sys/pools/research/@max_running_operations", 2)
        create("map_node", "//sys/pools/research/test_pool_1")
        create("map_node", "//sys/pools/research/test_pool_2")
        self._run_operations()

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

        op1.track()
        op2.track()

        assert sorted(read_table("//tmp/out1")) == sorted(data)
        assert sorted(read_table("//tmp/out2")) == sorted(data)

    def test_abort_of_pending_operation(self):
        create("table", "//tmp/in")
        create("table", "//tmp/out1")
        create("table", "//tmp/out2")
        create("table", "//tmp/out3")
        write_table("//tmp/in", [{"foo": i} for i in xrange(5)])

        op1 = map(dont_track=True, waiting_jobs=True, command="cat >/dev/null", in_=["//tmp/in"], out="//tmp/out1")
        op2 = map(dont_track=True, command="cat >/dev/null", in_=["//tmp/in"], out="//tmp/out2")
        op3 = map(dont_track=True, command="cat >/dev/null", in_=["//tmp/in"], out="//tmp/out3")

        time.sleep(1.5)
        assert op1.get_state() == "running"
        assert op2.get_state() == "pending"
        assert op3.get_state() == "pending"

        op2.abort()
        op1.resume_jobs()
        op1.track()
        op3.track()

        assert op1.get_state() == "completed"
        assert op2.get_state() == "aborted"
        assert op3.get_state() == "completed"

    def test_reconfigured_pools_operations_limit(self):
        create("table", "//tmp/in")
        create("table", "//tmp/out1")
        create("table", "//tmp/out2")
        write_table("//tmp/in", [{"foo": i} for i in xrange(5)])

        create("map_node", "//sys/pools/test_pool_1")
        create("map_node", "//sys/pools/test_pool_2")

        op1 = map(
            dont_track=True,
            waiting_jobs=True,
            command="cat",
            in_=["//tmp/in"],
            out="//tmp/out1",
            spec={"pool": "test_pool_1"})

        remove("//sys/pools/test_pool_1")
        create("map_node", "//sys/pools/test_pool_2/test_pool_1")
        time.sleep(0.5)

        op2 = map(
            dont_track=True,
            command="cat",
            in_=["//tmp/in"],
            out="//tmp/out2",
            spec={"pool": "test_pool_2"})

        op1.ensure_running()
        with pytest.raises(TimeoutError):
            op2.ensure_running(timeout=1.0)

        op1.resume_jobs()
        op1.track()
        op2.track()

    def test_total_operations_limit(self):
        create("map_node", "//sys/pools/research")
        create("map_node", "//sys/pools/research/research_subpool")
        create("map_node", "//sys/pools/production")
        set("//sys/pools/research/@max_operations", 3)

        create("table", "//tmp/in")
        write_table("//tmp/in", [{"foo": "bar"}])
        for i in xrange(5):
            create("table", "//tmp/out" + str(i))


        ops = []
        def run(index, pool, should_raise):
            def execute(dont_track):
                return map(
                    dont_track=dont_track,
                    command="sleep 5; cat",
                    in_=["//tmp/in"],
                    out="//tmp/out" + str(index),
                    spec={"pool": pool})

            if should_raise:
                with pytest.raises(YtError):
                    execute(False)
            else:
                ops.append(execute(True))

        for i in xrange(3):
            run(i, "research", False)

        for i in xrange(3, 5):
            run(i, "research", True)

        for i in xrange(3, 5):
            run(i, "research_subpool", True)

        self.Env.kill_service("scheduler")
        self.Env.start_schedulers("scheduler")

        for i in xrange(3, 5):
            run(i, "research", True)

        for i in xrange(3, 5):
            run(i, "production", False)

        for op in ops:
            op.track()


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
        # Wait applying scheduling tags.
        time.sleep(0.1)

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
        def get_job_nodes(op):
            nodes = __builtin__.set()
            for row in read_table("//sys/scheduler/event_log"):
                if row.get("event_type") == "job_started" and row.get("operation_id") == op.id:
                    nodes.add(row["node_address"])
            return nodes

        self._prepare()
        write_table("//tmp/t_in", [{"foo": "bar"} for _ in xrange(20)])

        set("//sys/nodes/{0}/@scheduling_tags".format(self.node), ["tagB"])
        time.sleep(1.2)
        op = map(command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec={"scheduling_tag": "tagB", "job_count": 20})
        time.sleep(0.8)
        assert get_job_nodes(op) == __builtin__.set([self.node])


        op = map(command="cat", in_="//tmp/t_in", out="//tmp/t_out", spec={"job_count": 20})
        time.sleep(0.8)
        assert len(get_job_nodes(op)) <= 2

##################################################################

class TestSchedulerConfig(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler" : {
            "event_log" : {
                "retry_backoff_time" : 7,
                "flush_period" : 5000
            },
            "operation_options": {
                "spec_template": {
                    "data_size_per_job": 1000
                }
            },
            "map_operation_options": {
                "spec_template": {
                    "data_size_per_job": 2000,
                    "max_failed_job_count": 10
                }
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

    def test_specs(self):
        create("table", "//tmp/t_in")
        write_table("<append=true>//tmp/t_in", {"foo": "bar"})

        create("table", "//tmp/t_out")

        op = map(command="cat", in_=["//tmp/t_in"], out="//tmp/t_out")
        assert get("//sys/operations/{0}/@spec/data_size_per_job".format(op.id)) == 2000

        op = merge(in_=["//tmp/t_in"], out="//tmp/t_out")
        assert get("//sys/operations/{0}/@spec/data_size_per_job".format(op.id)) == 1000

    def test_cypress_config(self):
        create("table", "//tmp/t_in")
        write_table("<append=true>//tmp/t_in", {"foo": "bar"})
        create("table", "//tmp/t_out")

        op = map(command="cat", in_=["//tmp/t_in"], out="//tmp/t_out")
        assert get("//sys/operations/{0}/@spec/data_size_per_job".format(op.id)) == 2000
        assert get("//sys/operations/{0}/@spec/max_failed_job_count".format(op.id)) == 10

        set("//sys/scheduler/config", {"map_operation_options": {"spec_template": {"max_failed_job_count": 50}}})
        time.sleep(0.5)

        op = map(command="cat", in_=["//tmp/t_in"], out="//tmp/t_out")
        assert get("//sys/operations/{0}/@spec/data_size_per_job".format(op.id)) == 2000
        assert get("//sys/operations/{0}/@spec/max_failed_job_count".format(op.id)) == 50


class TestSchedulerPools(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 1
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "watchers_update_period": 100,
            "default_parent_pool": "default_pool"
        }
    }

    def _prepare(self):
        create("table", "//tmp/t_in")
        set("//tmp/t_in/@replication_factor", 1)
        write_table("//tmp/t_in", {"foo": "bar"})

        create("table", "//tmp/t_out")
        set("//tmp/t_out/@replication_factor", 1)

    def test_pools_reconfiguration(self):
        self._prepare();

        testing_options = {"scheduling_delay": 1000}

        create("map_node", "//sys/pools/test_pool_1")
        create("map_node", "//sys/pools/test_pool_2")
        time.sleep(0.2)

        op = map(
            dont_track=True,
            command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out",
            spec={"pool": "test_pool_1", "testing": testing_options})
        time.sleep(1)

        remove("//sys/pools/test_pool_1")
        create("map_node", "//sys/pools/test_pool_2/test_pool_1")

        op.track()

    def test_default_parent_pool(self):
        self._prepare();

        create("map_node", "//sys/pools/default_pool")
        time.sleep(0.2)

        op = map(
            dont_track=True,
            waiting_jobs=True,
            command="cat",
            in_="//tmp/t_in",
            out="//tmp/t_out")

        pool = get("//sys/scheduler/orchid/scheduler/pools/root")
        assert pool["parent"] == "default_pool"

        remove("//sys/pools/default_pool")
        time.sleep(0.2)

        op.resume_jobs()
        op.track()


class TestSchedulerSnapshots(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 1

    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "snapshot_period": 500
        }
    }

    def test_snapshots(self):
        create("table", "//tmp/in")
        write_table("//tmp/in", [{"foo": i} for i in xrange(5)])
        create("table", "//tmp/out")

        testing_options = {"scheduling_delay": 500}

        op = map(
            dont_track=True,
            waiting_jobs=True,
            command="cat",
            in_="//tmp/in",
            out="//tmp/out",
            spec={"data_size_per_job": 1, "testing": testing_options})

        snapshot_path = "//sys/operations/{0}/snapshot".format(op.id)
        track_path(snapshot_path, 10)

        # This is done to avoid read failures due to snapshot file rewriting.
        snapshot_backup_path = snapshot_path + ".backup"
        copy(snapshot_path, snapshot_backup_path)
        assert len(read_file(snapshot_backup_path, verbose=False)) > 0

        op.resume_jobs()
        op.track()

    def test_parallel_snapshots(self):
        create("table", "//tmp/input")

        testing_options = {"scheduling_delay": 100}

        job_count = 1
        original_data = [{"index": i} for i in xrange(job_count)]
        write_table("//tmp/input", original_data)

        operation_count = 5
        ops = []
        for index in range(operation_count):
            output = "//tmp/output" + str(index)
            create("table", output)
            ops.append(
                map(dont_track=True,
                    waiting_jobs=True,
                    command="cat",
                    in_="//tmp/input",
                    out=[output],
                    spec={"data_size_per_job": 1, "testing": testing_options}))

        for op in ops:
            snapshot_path = "//sys/operations/{0}/snapshot".format(op.id)
            track_path(snapshot_path, 10)

            snapshot_backup_path = snapshot_path + ".backup"
            copy(snapshot_path, snapshot_backup_path)
            assert len(read_file(snapshot_backup_path, verbose=False)) > 0
            op.resume_jobs()

class TestSchedulerPreemption(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1


    DELTA_SCHEDULER_CONFIG = {
        "scheduler": {
            "min_share_preemption_timeout": 100,
            "fair_share_starvation_tolerance": 0.7,
            "fair_share_starvation_tolerance_limit": 0.9
        }
    }

    def test_preemption(self):
        create("table", "//tmp/t_in")
        for i in xrange(3):
            write_table("<append=true>//tmp/t_in", {"foo": "bar"})

        create("table", "//tmp/t_out1")
        create("table", "//tmp/t_out2")

        op1 = map(dont_track=True, command="sleep 1000; cat", in_=["//tmp/t_in"], out="//tmp/t_out1",
                  spec={"pool": "fake_pool", "job_count": 3, "locality_timeout": 0})
        time.sleep(3)

        assert get("//sys/scheduler/orchid/scheduler/pools/fake_pool/fair_share_ratio") >= 0.999
        assert get("//sys/scheduler/orchid/scheduler/pools/fake_pool/usage_ratio") >= 0.999

        create("map_node", "//sys/pools/test_pool", attributes={"min_share_ratio": 1.0})
        op2 = map(dont_track=True, command="cat", in_=["//tmp/t_in"], out="//tmp/t_out2", spec={"pool": "test_pool"})
        op2.track()

        op1.abort()

    def test_recursive_preemption_settings(self):
        create("map_node", "//sys/pools/p1", attributes={"fair_share_starvation_tolerance_limit": 0.6})
        create("map_node", "//sys/pools/p1/p2")
        create("map_node", "//sys/pools/p1/p3", attributes={"fair_share_starvation_tolerance": 0.5})
        create("map_node", "//sys/pools/p1/p4", attributes={"fair_share_starvation_tolerance": 0.9})
        create("map_node", "//sys/pools/p5", attributes={"fair_share_starvation_tolerance": 0.8})
        create("map_node", "//sys/pools/p5/p6")
        time.sleep(1)

        get_pool_tolerance = lambda pool: \
            get("//sys/scheduler/orchid/scheduler/pools/{0}/adjusted_fair_share_starvation_tolerance".format(pool))

        assert get_pool_tolerance("p1") == 0.7
        assert get_pool_tolerance("p2") == 0.6
        assert get_pool_tolerance("p3") == 0.5
        assert get_pool_tolerance("p4") == 0.6
        assert get_pool_tolerance("p5") == 0.8
        assert get_pool_tolerance("p6") == 0.8

        create("table", "//tmp/t_in")
        write_table("//tmp/t_in", {"foo": "bar"})
        create("table", "//tmp/t_out1")
        create("table", "//tmp/t_out2")
        create("table", "//tmp/t_out3")
        create("table", "//tmp/t_out4")

        op1 = map(
            dont_track=True,
            command="sleep 1000; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out1",
            spec={"pool": "p2", "fair_share_starvation_tolerance": 0.4})

        op2 = map(
            dont_track=True,
            command="sleep 1000; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out2",
            spec={"pool": "p2", "fair_share_starvation_tolerance": 0.8})

        op3 = map(
            dont_track=True,
            command="sleep 1000; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out3",
            spec={"pool": "p6"})

        op4 = map(
            dont_track=True,
            command="sleep 1000; cat",
            in_="//tmp/t_in",
            out="//tmp/t_out4",
            spec={"pool": "p6", "fair_share_starvation_tolerance": 0.9})

        time.sleep(1)

        get_operation_tolerance = lambda op_id: \
            get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/adjusted_fair_share_starvation_tolerance".format(op_id))

        assert get_operation_tolerance(op1.id) == 0.4
        assert get_operation_tolerance(op2.id) == 0.6
        assert get_operation_tolerance(op3.id) == 0.8
        assert get_operation_tolerance(op4.id) == 0.9

        op1.abort();
        op2.abort();

class TestSchedulerHeterogeneousConfiguration(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    def modify_node_config(self, config):
        if not hasattr(self, "node_counter"):
            self.node_counter = 0
        self.node_counter += 1
        if self.node_counter == 1:
            config["exec_agent"]["job_controller"]["resource_limits"]["user_slots"] = 0

    def test_job_count(self):
        data = [{"foo": i} for i in xrange(3)]
        create("table", "//tmp/in")
        create("table", "//tmp/out")
        write_table("//tmp/in", data)

        assert get("//sys/scheduler/orchid/scheduler/cell/resource_limits/user_slots") == 2
        assert get("//sys/scheduler/orchid/scheduler/cell/resource_usage/user_slots") == 0

        op = map(
            dont_track=True,
            command="sleep 100",
            in_="//tmp/in",
            out="//tmp/out",
            spec={"data_size_per_job": 1, "locality_timeout": 0})

        time.sleep(2)

        assert get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/resource_usage/user_slots".format(op.id)) == 2
        assert get("//sys/scheduler/orchid/scheduler/cell/resource_limits/user_slots") == 2
        assert get("//sys/scheduler/orchid/scheduler/cell/resource_usage/user_slots") == 2

        op.abort()


class TestResourceLimits(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1

    def test_resource_limits(self):
        resource_limits = {"cpu": 1, "memory": 1000 * 1024 * 1024, "network": 10}
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

        data = [{"foo": i} for i in xrange(3)]
        create("table", "//tmp/in")
        create("table", "//tmp/out")
        write_table("//tmp/in", data)

        memory_limit = 30 * 1024 * 1024

        testing_options = {"scheduling_delay": 500}

        op = map(
            dont_track=True,
            command="sleep 5",
            in_="//tmp/in",
            out="//tmp/out",
            spec={"job_count": 3, "pool": "test_pool", "mapper": {"memory_limit": memory_limit}, "testing": testing_options})
        time.sleep(3)
        assert get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/jobs/running".format(op.id)) == 1
        op.abort()

        op = map(
            dont_track=True,
            command="sleep 5",
            in_="//tmp/in",
            out="//tmp/out",
            spec={"job_count": 3, "resource_limits": resource_limits, "mapper": {"memory_limit": memory_limit}, "testing": testing_options})
        time.sleep(3)
        op_limits = get("//sys/scheduler/orchid/scheduler/operations/{0}/progress/resource_limits".format(op.id))
        for resource, limit in resource_limits.iteritems():
            assert op_limits[resource] == limit
        assert get("//sys/operations/{0}/@progress/jobs/running".format(op.id)) == 1
        op.abort()

