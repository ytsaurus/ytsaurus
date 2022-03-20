from yt_env_setup import (YTEnvSetup, Restarter, QUEUE_AGENTS_SERVICE)

from yt_commands import (authors, get, set, ls, wait, assert_yt_error, create, sync_mount_table, sync_create_cells,
                         insert_rows, delete_rows, remove, raises_yt_error, exists, start_transaction, select_rows,
                         sync_unmount_table, sync_reshard_table, trim_rows, print_debug)

from yt.common import YtError

import copy
import datetime
import time
import pytz

from yt.yson import YsonUint64, YsonEntity

import yt_error_codes

from yt.wrapper.ypath import escape_ypath_literal

import yt.environment.init_queue_agent_state as init_queue_agent_state

##################################################################


BIGRT_CONSUMER_TABLE_SCHEMA = [
    {"name": "ShardId", "type": "uint64", "sort_order": "ascending"},
    {"name": "Offset", "type": "uint64"},
]


class QueueAgentOrchid:
    def __init__(self, agent_id=None):
        if agent_id is None:
            agent_ids = ls("//sys/queue_agents/instances", verbose=False)
            assert len(agent_ids) == 1
            agent_id = agent_ids[0]

        self.agent_id = agent_id

    def queue_agent_orchid_path(self):
        return "//sys/queue_agents/instances/" + self.agent_id + "/orchid"

    def get_poll_index(self):
        return get(self.queue_agent_orchid_path() + "/queue_agent/poll_index")

    def get_poll_error(self):
        return YtError.from_dict(get(self.queue_agent_orchid_path() + "/queue_agent/poll_error"))

    def wait_fresh_poll(self):
        poll_index = self.get_poll_index()
        wait(lambda: self.get_poll_index() >= poll_index + 2)

    def validate_no_poll_error(self):
        error = self.get_poll_error()
        if error.code != 0:
            raise error

    def get_queues(self):
        self.wait_fresh_poll()
        return get(self.queue_agent_orchid_path() + "/queue_agent/queues")

    def get_consumers(self):
        self.wait_fresh_poll()
        return get(self.queue_agent_orchid_path() + "/queue_agent/consumers")

    def get_queue_pass_index(self, queue_ref):
        return get(self.queue_agent_orchid_path() + "/queue_agent/queues/" +
                   escape_ypath_literal(queue_ref) + "/pass_index")

    def get_consumer_pass_index(self, consumer_ref):
        return get(self.queue_agent_orchid_path() + "/queue_agent/consumers/" +
                   escape_ypath_literal(consumer_ref) + "/pass_index")

    def wait_fresh_queue_pass(self, queue_ref):
        pass_index = self.get_queue_pass_index(queue_ref)
        wait(lambda: self.get_queue_pass_index(queue_ref) >= pass_index + 2)

    def wait_fresh_consumer_pass(self, consumer_ref):
        pass_index = self.get_consumer_pass_index(consumer_ref)
        wait(lambda: self.get_consumer_pass_index(consumer_ref) >= pass_index + 2)

    def get_queue_status(self, queue_ref):
        return get(self.queue_agent_orchid_path() + "/queue_agent/queues/" +
                   escape_ypath_literal(queue_ref) + "/status")

    def get_queue_partitions(self, queue_ref):
        return get(self.queue_agent_orchid_path() + "/queue_agent/queues/" +
                   escape_ypath_literal(queue_ref) + "/partitions")

    def get_consumer_status(self, consumer_ref):
        return get(self.queue_agent_orchid_path() + "/queue_agent/consumers/" +
                   escape_ypath_literal(consumer_ref) + "/status")

    def get_consumer_partitions(self, consumer_ref):
        return get(self.queue_agent_orchid_path() + "/queue_agent/consumers/" +
                   escape_ypath_literal(consumer_ref) + "/partitions")


class TestQueueAgentBase(YTEnvSetup):
    USE_DYNAMIC_TABLES = True

    def _prepare_tables(self, queue_table_schema=None, consumer_table_schema=None):
        sync_create_cells(1)
        remove("//sys/queue_agents/queues", force=True)
        remove("//sys/queue_agents/consumers", force=True)
        init_queue_agent_state.create_tables(
            self.Env.create_native_client(),
            queue_table_schema=queue_table_schema,
            consumer_table_schema=consumer_table_schema)
        sync_mount_table("//sys/queue_agents/queues")
        sync_mount_table("//sys/queue_agents/consumers")

    def _drop_tables(self):
        remove("//sys/queue_agents/queues", force=True)
        remove("//sys/queue_agents/consumers", force=True)

    def _create_queue(self, path, partition_count=1, enable_timestamp_column=True, **kwargs):
        schema = [{"name": "data", "type": "string"}]
        if enable_timestamp_column:
            schema += [{"name": "$timestamp", "type": "uint64"}]
        attributes = {
            "dynamic": True,
            "schema": schema,
        }
        attributes.update(kwargs)
        create("table", path, attributes=attributes)
        if partition_count != 1:
            sync_reshard_table(path, partition_count)
        sync_mount_table(path)

    def _create_bigrt_consumer(self, path, target_queue, **kwargs):
        attributes = {
            "dynamic": True,
            "schema": BIGRT_CONSUMER_TABLE_SCHEMA,
            "target_queue": target_queue,
        }
        attributes.update(kwargs)
        create("table", path, attributes=attributes)
        sync_mount_table(path)


class TestQueueAgentNoSynchronizer(TestQueueAgentBase):
    NUM_QUEUE_AGENTS = 1

    DELTA_QUEUE_AGENT_CONFIG = {
        "cluster_connection": {
            # Disable cache.
            "table_mount_cache": {
                "expire_after_successful_update_time": 0,
                "expire_after_failed_update_time": 0,
                "expire_after_access_time": 0,
                "refresh_time": 0,
            },
        },
        "queue_agent": {
            "poll_period": 100,
        },
        "cypress_synchronizer": {
            "enable": False,
        },
    }

    @authors("max42")
    def test_polling_loop(self):
        orchid = QueueAgentOrchid()

        self._drop_tables()

        orchid.wait_fresh_poll()
        assert_yt_error(orchid.get_poll_error(), yt_error_codes.ResolveErrorCode)

        wrong_schema = copy.deepcopy(init_queue_agent_state.QUEUE_TABLE_SCHEMA)
        wrong_schema[-1]["type"] = "int64"
        self._prepare_tables(queue_table_schema=wrong_schema)

        orchid.wait_fresh_poll()
        assert_yt_error(orchid.get_poll_error(), "Row range schema is incompatible with queue table row schema")

        self._prepare_tables()
        orchid.wait_fresh_poll()
        orchid.validate_no_poll_error()

    @authors("max42")
    def test_queue_state(self):
        orchid = QueueAgentOrchid()

        self._prepare_tables()

        orchid.wait_fresh_poll()
        queues = orchid.get_queues()
        assert len(queues) == 0

        # Missing row revision.
        insert_rows("//sys/queue_agents/queues",
                    [{"cluster": "primary", "path": "//tmp/q"}])
        orchid.wait_fresh_poll()
        status = orchid.get_queue_status("primary://tmp/q")
        assert_yt_error(YtError.from_dict(status["error"]), "Queue is not in-sync yet")
        assert "type" not in status

        # Missing object type.
        insert_rows("//sys/queue_agents/queues",
                    [{"cluster": "primary", "path": "//tmp/q", "row_revision": YsonUint64(1234)}],
                    update=True)
        orchid.wait_fresh_poll()
        status = orchid.get_queue_status("primary://tmp/q")
        assert_yt_error(YtError.from_dict(status["error"]), "Queue is not in-sync yet")
        assert "type" not in status

        # Wrong object type.
        insert_rows("//sys/queue_agents/queues",
                    [{"cluster": "primary", "path": "//tmp/q", "row_revision": YsonUint64(2345),
                      "object_type": "map_node"}],
                    update=True)
        orchid.wait_fresh_poll()
        status = orchid.get_queue_status("primary://tmp/q")
        assert_yt_error(YtError.from_dict(status["error"]), 'Invalid queue object type "map_node"')

        # Sorted dynamic table.
        insert_rows("//sys/queue_agents/queues",
                    [{"cluster": "primary", "path": "//tmp/q", "row_revision": YsonUint64(3456),
                      "object_type": "table", "dynamic": True, "sorted": True}],
                    update=True)
        orchid.wait_fresh_poll()
        status = orchid.get_queue_status("primary://tmp/q")
        assert_yt_error(YtError.from_dict(status["error"]), "Only ordered dynamic tables are supported as queues")

        # Proper ordered dynamic table.
        insert_rows("//sys/queue_agents/queues",
                    [{"cluster": "primary", "path": "//tmp/q", "row_revision": YsonUint64(4567), "object_type": "table",
                      "dynamic": True, "sorted": False}],
                    update=True)
        orchid.wait_fresh_poll()
        status = orchid.get_queue_status("primary://tmp/q")
        # This error means that controller is instantiated and works properly (note that //tmp/q does not exist yet).
        assert_yt_error(YtError.from_dict(status["error"]), code=yt_error_codes.ResolveErrorCode)

        # Switch back to sorted dynamic table.
        insert_rows("//sys/queue_agents/queues",
                    [{"cluster": "primary", "path": "//tmp/q", "row_revision": YsonUint64(5678), "object_type": "table",
                      "dynamic": False, "sorted": False}],
                    update=True)
        orchid.wait_fresh_poll()
        status = orchid.get_queue_status("primary://tmp/q")
        assert_yt_error(YtError.from_dict(status["error"]), "Only ordered dynamic tables are supported as queues")
        assert "family" not in status

        # Remove row; queue should be unregistered.
        delete_rows("//sys/queue_agents/queues", [{"cluster": "primary", "path": "//tmp/q"}])
        orchid.wait_fresh_poll()

        queues = orchid.get_queues()
        assert len(queues) == 0

    @authors("max42")
    def test_consumer_state(self):
        orchid = QueueAgentOrchid()

        self._prepare_tables()

        orchid.wait_fresh_poll()
        queues = orchid.get_queues()
        consumers = orchid.get_consumers()
        assert len(queues) == 0
        assert len(consumers) == 0

        # Missing row revision.
        insert_rows("//sys/queue_agents/consumers",
                    [{"cluster": "primary", "path": "//tmp/c"}])
        orchid.wait_fresh_poll()
        status = orchid.get_consumer_status("primary://tmp/c")
        assert_yt_error(YtError.from_dict(status["error"]), "Consumer is not in-sync yet")
        assert "target" not in status

        # Missing target.
        insert_rows("//sys/queue_agents/consumers",
                    [{"cluster": "primary", "path": "//tmp/c", "row_revision": YsonUint64(1234)}],
                    update=True)
        orchid.wait_fresh_poll()
        status = orchid.get_consumer_status("primary://tmp/c")
        assert_yt_error(YtError.from_dict(status["error"]), "Consumer is missing target")
        assert "target" not in status

        # Unregistered target queue.
        insert_rows("//sys/queue_agents/consumers",
                    [{"cluster": "primary", "path": "//tmp/c", "row_revision": YsonUint64(2345),
                      "target_cluster": "primary", "target_path": "//tmp/q", "treat_as_consumer": True}],
                    update=True)
        orchid.wait_fresh_poll()
        status = orchid.get_consumer_status("primary://tmp/c")
        assert_yt_error(YtError.from_dict(status["error"]), 'Target queue "primary://tmp/q" is not registered')

        # Register target queue with wrong object type.
        insert_rows("//sys/queue_agents/queues",
                    [{"cluster": "primary", "path": "//tmp/q", "row_revision": YsonUint64(4567),
                      "object_type": "map_node", "dynamic": True, "sorted": False}],
                    update=True)
        orchid.wait_fresh_poll()
        status = orchid.get_queue_status("primary://tmp/q")
        assert_yt_error(YtError.from_dict(status["error"]), 'Invalid queue object type "map_node"')

        # Queue error is propagated as consumer error.
        status = orchid.get_consumer_status("primary://tmp/c")
        assert_yt_error(YtError.from_dict(status["error"]), 'Invalid queue object type "map_node"')


class TestQueueController(TestQueueAgentBase):
    NUM_QUEUE_AGENTS = 1

    DELTA_QUEUE_AGENT_CONFIG = {
        "cluster_connection": {
            # Disable cache.
            "table_mount_cache": {
                "expire_after_successful_update_time": 0,
                "expire_after_failed_update_time": 0,
                "expire_after_access_time": 0,
                "refresh_time": 0,
            },
        },
        "queue_agent": {
            "poll_period": 100,
        },
        "cypress_synchronizer": {
            "sync_period": 100,
        },
    }

    def _timestamp_to_iso_str(self, ts):
        unix_ts = ts >> 30
        dt = datetime.datetime.fromtimestamp(unix_ts, tz=pytz.UTC)
        return dt.isoformat().replace("+00:00", ".000000Z")

    @authors("max42")
    def test_queue_status(self):
        self._prepare_tables()

        orchid = QueueAgentOrchid()

        self._create_queue("//tmp/q", partition_count=2)
        self._create_bigrt_consumer("//tmp/c", "primary://tmp/q")

        insert_rows("//sys/queue_agents/consumers", [{"cluster": "primary", "path": "//tmp/c"}])
        insert_rows("//sys/queue_agents/queues", [{"cluster": "primary", "path": "//tmp/q"}])

        orchid.wait_fresh_poll()
        orchid.wait_fresh_queue_pass("primary://tmp/q")

        queue_status = orchid.get_queue_status("primary://tmp/q")
        assert queue_status["family"] == "ordered_dynamic_table"
        assert queue_status["partition_count"] == 2
        # TODO(max42): support owner attribute.
        assert queue_status["consumers"] == {"primary://tmp/c": {"vital": False, "owner": ""}}

        def assert_partition(partition, lower_row_index, upper_row_index):
            assert partition["lower_row_index"] == lower_row_index
            assert partition["upper_row_index"] == upper_row_index
            assert partition["available_row_count"] == upper_row_index - lower_row_index

        null_time = "1970-01-01T00:00:00.000000Z"

        queue_partitions = orchid.get_queue_partitions("primary://tmp/q")
        for partition in queue_partitions:
            assert_partition(partition, 0, 0)
            assert partition["last_row_commit_time"] == null_time

        insert_rows("//tmp/q", [{"data": "foo", "$tablet_index": 0}])
        orchid.wait_fresh_queue_pass("primary://tmp/q")

        queue_partitions = orchid.get_queue_partitions("primary://tmp/q")
        assert_partition(queue_partitions[0], 0, 1)
        assert queue_partitions[0]["last_row_commit_time"] != null_time
        assert_partition(queue_partitions[1], 0, 0)

        trim_rows("//tmp/q", 0, 1)

        orchid.wait_fresh_queue_pass("primary://tmp/q")

        queue_partitions = orchid.get_queue_partitions("primary://tmp/q")
        assert_partition(queue_partitions[0], 1, 1)
        assert queue_partitions[0]["last_row_commit_time"] != null_time
        assert_partition(queue_partitions[1], 0, 0)

    @authors("max42")
    def test_consumer_status(self):
        self._prepare_tables()

        orchid = QueueAgentOrchid()

        self._create_queue("//tmp/q", partition_count=2)
        self._create_bigrt_consumer("//tmp/c", "primary://tmp/q")

        insert_rows("//sys/queue_agents/consumers", [{"cluster": "primary", "path": "//tmp/c"}])
        insert_rows("//sys/queue_agents/queues", [{"cluster": "primary", "path": "//tmp/q"}])

        orchid.wait_fresh_poll()

        insert_rows("//tmp/q", [{"data": "foo", "$tablet_index": 0}, {"data": "bar", "$tablet_index": 1}])
        time.sleep(1.5)
        insert_rows("//tmp/q", [{"data": "foo", "$tablet_index": 0}, {"data": "bar", "$tablet_index": 1}])
        timestamps = [row["ts"] for row in select_rows("[$timestamp] as ts from [//tmp/q]")]
        timestamps = sorted(timestamps)
        assert timestamps[0] == timestamps[1] and timestamps[2] == timestamps[3]
        timestamps = [timestamps[0], timestamps[2]]
        print_debug(self._timestamp_to_iso_str(timestamps[0]), self._timestamp_to_iso_str(timestamps[1]))

        orchid.wait_fresh_consumer_pass("primary://tmp/c")
        consumer_status = orchid.get_consumer_status("primary://tmp/c")
        assert consumer_status["partition_count"] == 2
        assert not consumer_status["vital"]
        assert consumer_status["target_queue"] == "primary://tmp/q"
        # TODO(max42): support owner attribute.
        assert consumer_status["owner"] == ""

        time.sleep(1.5)
        insert_rows("//tmp/c", [{"ShardId": 0, "Offset": 0}])
        time.sleep(1.5)
        insert_rows("//tmp/c", [{"ShardId": 1, "Offset": 0}])

        def assert_partition(partition, next_row_index):
            assert partition["next_row_index"] == next_row_index
            assert partition["unread_row_count"] == max(0, 2 - next_row_index)
            assert partition["next_row_commit_time"] == (self._timestamp_to_iso_str(timestamps[next_row_index])
                                                         if next_row_index < 2 else YsonEntity())
            assert (partition["processing_lag"] > 0) == (next_row_index < 2)

        orchid.wait_fresh_consumer_pass("primary://tmp/c")
        consumer_partitions = orchid.get_consumer_partitions("primary://tmp/c")
        assert_partition(consumer_partitions[0], 0)
        assert_partition(consumer_partitions[1], 0)

        insert_rows("//tmp/c", [{"ShardId": 0, "Offset": 1}])
        orchid.wait_fresh_consumer_pass("primary://tmp/c")
        consumer_partitions = orchid.get_consumer_partitions("primary://tmp/c")
        assert_partition(consumer_partitions[0], 1)

        insert_rows("//tmp/c", [{"ShardId": 1, "Offset": 2}])
        orchid.wait_fresh_consumer_pass("primary://tmp/c")
        consumer_partitions = orchid.get_consumer_partitions("primary://tmp/c")
        assert_partition(consumer_partitions[1], 2)

    @authors("max42")
    def test_consumer_partition_disposition(self):
        self._prepare_tables()

        orchid = QueueAgentOrchid()

        self._create_queue("//tmp/q")
        self._create_bigrt_consumer("//tmp/c", "primary://tmp/q")

        insert_rows("//sys/queue_agents/consumers", [{"cluster": "primary", "path": "//tmp/c"}])
        insert_rows("//sys/queue_agents/queues", [{"cluster": "primary", "path": "//tmp/q"}])

        orchid.wait_fresh_poll()

        insert_rows("//tmp/q", [{"data": "foo"}] * 3)
        trim_rows("//tmp/q", 0, 1)
        orchid.wait_fresh_queue_pass("primary://tmp/q")

        expected_dispositions = ["expired", "pending_consumption", "pending_consumption", "up_to_date", "ahead"]
        for offset, expected_disposition in enumerate(expected_dispositions):
            insert_rows("//tmp/c", [{"ShardId": 0, "Offset": offset}])
            orchid.wait_fresh_consumer_pass("primary://tmp/c")
            partition = orchid.get_consumer_partitions("primary://tmp/c")[0]
            assert partition["disposition"] == expected_disposition
            assert partition["unread_row_count"] == 3 - offset

    @authors("max42")
    def test_inconsistent_partitions_in_consumer_table(self):
        self._prepare_tables()

        orchid = QueueAgentOrchid()

        self._create_queue("//tmp/q", partition_count=2)
        self._create_bigrt_consumer("//tmp/c", "primary://tmp/q")

        insert_rows("//sys/queue_agents/consumers", [{"cluster": "primary", "path": "//tmp/c"}])
        insert_rows("//sys/queue_agents/queues", [{"cluster": "primary", "path": "//tmp/q"}])

        orchid.wait_fresh_poll()

        insert_rows("//tmp/q", [{"data": "foo"}] * 2)
        orchid.wait_fresh_queue_pass("primary://tmp/q")

        insert_rows("//tmp/c", [{"ShardId": 1, "Offset": 1}, {"ShardId": 2**63 - 1, "Offset": 1},
                                {"ShardId": 2**64 - 1, "Offset": 1}])
        orchid.wait_fresh_consumer_pass("primary://tmp/c")

        partitions = orchid.get_consumer_partitions("primary://tmp/c")
        assert len(partitions) == 2
        assert partitions[0]["next_row_index"] == 0


class TestMultipleAgents(TestQueueAgentBase):
    NUM_QUEUE_AGENTS = 5

    USE_DYNAMIC_TABLES = True

    DELTA_QUEUE_AGENT_CONFIG = {
        "cluster_connection": {
            # Disable cache.
            "table_mount_cache": {
                "expire_after_successful_update_time": 0,
                "expire_after_failed_update_time": 0,
                "expire_after_access_time": 0,
                "refresh_time": 0,
            },
        },
        "queue_agent": {
            "poll_period": 100,
        },
        "election_manager": {
            "transaction_timeout": 5000,
            "transaction_ping_period": 100,
            "lock_acquisition_period": 100,
        },
    }

    @authors("max42")
    def test_leader_election(self):
        instances = ls("//sys/queue_agents/instances")
        assert len(instances) == 5

        def collect_leaders(ignore_instances=[]):
            result = []
            for instance in instances:
                if instance in ignore_instances:
                    continue
                active = get("//sys/queue_agents/instances/" + instance + "/orchid/queue_agent/active")
                if active:
                    result.append(instance)
            return result

        wait(lambda: len(collect_leaders()) == 1)

        leader = collect_leaders()[0]

        # Check that exactly one queue agent instance is performing polling.

        def validate_leader(leader, ignore_instances=[]):
            wait(lambda: get("//sys/queue_agents/instances/" + leader + "/orchid/queue_agent/poll_index") > 10)

            for instance in instances:
                if instance != leader and instance not in ignore_instances:
                    assert get("//sys/queue_agents/instances/" + instance + "/orchid/queue_agent/poll_index") == 0

        validate_leader(leader)

        # Check that leader host is set in lock transaction attributes.

        locks = get("//sys/queue_agents/leader_lock/@locks")
        assert len(locks) == 5
        tx_id = None
        for lock in locks:
            if lock["state"] == "acquired":
                assert not tx_id
                tx_id = lock["transaction_id"]
        leader_from_tx_attrs = get("#" + tx_id + "/@host")

        assert leader == leader_from_tx_attrs

        # Test re-election.

        leader_index = get("//sys/queue_agents/instances/" + leader + "/@annotations/yt_env_index")

        with Restarter(self.Env, QUEUE_AGENTS_SERVICE, indexes=[leader_index]):
            prev_leader = leader

            def reelection():
                leaders = collect_leaders(ignore_instances=[prev_leader])
                return len(leaders) == 1 and leaders[0] != prev_leader
            wait(reelection)

            leader = collect_leaders(ignore_instances=[prev_leader])[0]

            validate_leader(leader, ignore_instances=[prev_leader])


class TestMasterIntegration(TestQueueAgentBase):
    NUM_QUEUE_AGENTS = 1

    USE_DYNAMIC_TABLES = True

    DELTA_QUEUE_AGENT_CONFIG = {
        "cluster_connection": {
            # Disable cache.
            "table_mount_cache": {
                "expire_after_successful_update_time": 0,
                "expire_after_failed_update_time": 0,
                "expire_after_access_time": 0,
                "refresh_time": 0,
            },
        },
        "queue_agent": {
            "poll_period": 100,
        },
        "election_manager": {
            "transaction_timeout": 5000,
            "transaction_ping_period": 100,
            "lock_acquisition_period": 100,
        },
    }

    @authors("max42")
    def test_queue_attributes(self):
        self._prepare_tables()

        create("table", "//tmp/q", attributes={"dynamic": True, "schema": [{"name": "data", "type": "string"}]})
        sync_mount_table("//tmp/q")

        assert get("//tmp/q/@queue_agent_stage") == "production"

        # Before queue is registered, queue agent backed attributes would throw resolution error.
        with raises_yt_error(code=yt_error_codes.ResolveErrorCode):
            get("//tmp/q/@queue_status")

        insert_rows("//sys/queue_agents/queues",
                    [{"cluster": "primary", "path": "//tmp/q", "row_revision": YsonUint64(4567), "object_type": "table",
                      "dynamic": True, "sorted": False}])

        # Wait for queue status to become available.
        wait(lambda: get("//tmp/q/@queue_status/partition_count") == 1, ignore_exceptions=True)

        # Check the zeroth partition.

        def check_partition():
            partitions = get("//tmp/q/@queue_partitions")
            if len(partitions) == 1:
                assert partitions[0]["available_row_count"] == 0
                return True
            return False

        wait(check_partition)

        # Check that queue attributes are opaque.
        full_attributes = get("//tmp/q/@")
        for attribute in ("queue_status", "queue_partitions"):
            assert full_attributes[attribute] == YsonEntity()

    @authors("achulkov2")
    def test_consumer_attributes(self):
        self._prepare_tables()

        create("table", "//tmp/q", attributes={"dynamic": True, "schema": [{"name": "data", "type": "string"}]})
        sync_mount_table("//tmp/q")
        create("table", "//tmp/c", attributes={"dynamic": True, "schema": BIGRT_CONSUMER_TABLE_SCHEMA,
                                               "treat_as_consumer": True, "target_queue": "primary://tmp/q"})
        with raises_yt_error('Builtin attribute "vital_queue_consumer" cannot be set'):
            set("//tmp/c/@vital_queue_consumer", True)
        set("//tmp/c/@treat_as_queue_consumer", True)
        set("//tmp/c/@vital_queue_consumer", True)
        sync_mount_table("//tmp/c")

        assert get("//tmp/c/@queue_agent_stage") == "production"

        # Check that queue_agent_stage is writable.
        set("//tmp/c/@queue_agent_stage", "testing")
        set("//tmp/c/@queue_agent_stage", "production")

        # Before consumer is registered, queue agent backed attributes would throw resolution error.
        with raises_yt_error(code=yt_error_codes.ResolveErrorCode):
            get("//tmp/c/@queue_consumer_status")

        insert_rows("//sys/queue_agents/consumers",
                    [{"cluster": "primary", "path": "//tmp/c", "row_revision": YsonUint64(0)}])
        insert_rows("//sys/queue_agents/queues",
                    [{"cluster": "primary", "path": "//tmp/q", "row_revision": YsonUint64(4567)}])

        # Wait for consumer status to become available.
        wait(lambda: get("//tmp/c/@queue_consumer_status/partition_count") == 1, ignore_exceptions=True)

        # TODO: Check the queue_consumer_partitions attribute once the corresponding queue_controller code is written.

        # Check that consumer attributes are opaque.
        full_attributes = get("//tmp/c/@")
        for attribute in ("queue_consumer_status", "queue_consumer_partitions"):
            assert full_attributes[attribute] == YsonEntity()

    @authors("max42")
    def test_queue_agent_stage(self):
        self._prepare_tables()

        create("table", "//tmp/q", attributes={"dynamic": True, "schema": [{"name": "data", "type": "string"}]})
        sync_mount_table("//tmp/q")

        assert get("//tmp/q/@queue_agent_stage") == "production"

        set("//tmp/q/@queue_agent_stage", "testing")
        assert get("//tmp/q/@queue_agent_stage") == "testing"

        # There is no queue agent with stage "testing", so accessing queue status would result in an error.
        with raises_yt_error('Queue agent stage "testing" is not found'):
            get("//tmp/q/@queue_status")

        tx = start_transaction()
        with raises_yt_error("Operation cannot be performed in transaction"):
            set("//tmp/q/@queue_agent_stage", "value_under_tx", tx=tx)

    @authors("max42")
    def test_non_queues(self):
        create("table", "//tmp/q_static",
               attributes={"schema": [{"name": "data", "type": "string"}]})
        create("table", "//tmp/q_sorted_dynamic",
               attributes={"dynamic": True, "schema": [{"name": "key", "type": "string", "sort_order": "ascending"},
                                                       {"name": "value", "type": "string"}]})
        create("replicated_table", "//tmp/q_ordered_replicated",
               attributes={"dynamic": True, "schema": [{"name": "data", "type": "string"}]})
        attributes = ["queue_agent_stage", "queue_status", "queue_partitions"]
        result = get("//tmp", attributes=attributes)
        for name in ("q_static", "q_sorted_dynamic", "q_ordered_replicated"):
            assert not result[name].attributes
            for attribute in attributes:
                assert not exists("//tmp/" + name + "/@" + attribute)


class CypressSynchronizerOrchid:
    def __init__(self, agent_id=None):
        if agent_id is None:
            agent_ids = ls("//sys/queue_agents/instances", verbose=False)
            assert len(agent_ids) == 1
            agent_id = agent_ids[0]

        self.agent_id = agent_id

    def queue_agent_orchid_path(self):
        return "//sys/queue_agents/instances/" + self.agent_id + "/orchid"

    def get_poll_index(self):
        return get(self.queue_agent_orchid_path() + "/cypress_synchronizer/poll_index")

    def wait_fresh_poll(self):
        poll_index = self.get_poll_index()
        wait(lambda: self.get_poll_index() >= poll_index + 2)

    def get_poll_error(self):
        return YtError.from_dict(get(self.queue_agent_orchid_path() + "/cypress_synchronizer/poll_error"))


class TestCypressSynchronizer(TestQueueAgentBase):
    NUM_QUEUE_AGENTS = 1

    USE_DYNAMIC_TABLES = True

    DELTA_QUEUE_AGENT_CONFIG = {
        "cluster_connection": {
            # Disable cache.
            "table_mount_cache": {
                "expire_after_successful_update_time": 0,
                "expire_after_failed_update_time": 0,
                "expire_after_access_time": 0,
                "refresh_time": 0,
            },
        },
        "queue_agent": {
            "poll_period": 100,
        },
        "cypress_synchronizer": {
            "sync_period": 1000
        }
    }

    def _get_queue_name(self, name):
        return "//tmp/q-{}".format(name)

    def _get_consumer_name(self, name):
        return "//tmp/c-{}".format(name)

    def _set_account_tablet_count_limit(self, account, value):
        set("//sys/accounts/{0}/@resource_limits/tablet_count".format(account), value)

    LAST_REVISIONS = dict()
    QUEUE_REGISTRY = []
    CONSUMER_REGISTRY = []

    def _create_and_register_queue(self, path):
        create("table",
               path,
               attributes={"dynamic": True,
                           "schema": [{"name": "useless", "type": "string"}]},
               force=True)
        self.QUEUE_REGISTRY.append(path)
        insert_rows("//sys/queue_agents/queues",
                    [{"cluster": "primary", "path": path, "row_revision": YsonUint64(1)}])
        self.LAST_REVISIONS[path] = 1

    def _drop_queues(self):
        for queue in self.QUEUE_REGISTRY:
            remove(queue, force=True)
            del self.LAST_REVISIONS[queue]
        self.QUEUE_REGISTRY.clear()

    def _create_and_register_consumer(self, path):
        create("table",
               path,
               attributes={"dynamic": True,
                           "schema": [{"name": "useless", "type": "string", "sort_order": "ascending"},
                                      {"name": "also_useless", "type": "string"}],
                           "treat_as_queue_consumer": True},
               force=True)
        self.CONSUMER_REGISTRY.append(path)
        insert_rows("//sys/queue_agents/consumers",
                    [{"cluster": "primary", "path": path, "row_revision": YsonUint64(1)}])
        assert path not in self.LAST_REVISIONS
        self.LAST_REVISIONS[path] = 1

    def _drop_consumers(self):
        for consumer in self.CONSUMER_REGISTRY:
            remove(consumer, force=True)
            del self.LAST_REVISIONS[consumer]
        self.CONSUMER_REGISTRY.clear()

    def _check_queue_count_and_column_counts(self, queues, size):
        assert len(queues) == size
        column_counts = [len(row) for row in queues]
        assert column_counts == [len(init_queue_agent_state.QUEUE_TABLE_SCHEMA)] * size

    def _check_consumer_count_and_column_counts(self, consumers, size):
        assert len(consumers) == size
        column_counts = [len(row) for row in consumers]
        assert column_counts == [len(init_queue_agent_state.CONSUMER_TABLE_SCHEMA)] * size

    def _get_queues_and_check_invariants(self, expected_count=None):
        queues = select_rows("* from [//sys/queue_agents/queues]")
        if expected_count is not None:
            self._check_queue_count_and_column_counts(queues, expected_count)
        for queue in queues:
            assert queue["revision"] == get(queue["path"] + "/@revision")
        return queues

    def _get_consumers_and_check_invariants(self, expected_count=None):
        consumers = select_rows("* from [//sys/queue_agents/consumers]")
        if expected_count is not None:
            self._check_consumer_count_and_column_counts(consumers, expected_count)
        for consumer in consumers:
            assert consumer["revision"] == get(consumer["path"] + "/@revision")
            assert consumer["treat_as_consumer"] == get(consumer["path"] + "/@treat_as_queue_consumer")
            # Enclosing into a list is a workaround for storing YSON with top-level attributes.
            assert consumer["schema"] == [get(consumer["path"] + "/@schema")]
            assert consumer["vital"] == get(
                consumer["path"] + "/@", attributes=["vital_queue_consumer"]).get("vital_queue_consumer", False)
        return consumers

    def _assert_constant_revision(self, row):
        assert self.LAST_REVISIONS[row["path"]] == row["row_revision"]

    def _assert_increased_revision(self, row):
        assert self.LAST_REVISIONS[row["path"]] < row["row_revision"]
        self.LAST_REVISIONS[row["path"]] = row["row_revision"]

    @authors("achulkov2")
    def test_basic(self):
        orchid = CypressSynchronizerOrchid()

        self._prepare_tables()

        q1 = self._get_queue_name("a")
        q2 = self._get_queue_name("b")
        c1 = self._get_consumer_name("a")
        c2 = self._get_consumer_name("b")

        self._create_and_register_queue(q1)
        self._create_and_register_consumer(c1)
        orchid.wait_fresh_poll()

        queues = self._get_queues_and_check_invariants(expected_count=1)
        consumers = self._get_consumers_and_check_invariants(expected_count=1)
        for queue in queues:
            self._assert_increased_revision(queue)
        for consumer in consumers:
            self._assert_increased_revision(consumer)

        self._create_and_register_queue(q2)
        self._create_and_register_consumer(c2)
        orchid.wait_fresh_poll()

        queues = self._get_queues_and_check_invariants(expected_count=2)
        consumers = self._get_consumers_and_check_invariants(expected_count=2)
        set(c2 + "/@treat_as_queue_consumer", False)
        for queue in queues:
            if queue["path"] == q1:
                self._assert_constant_revision(queue)
            elif queue["path"] == q2:
                self._assert_increased_revision(queue)
        for consumer in consumers:
            if consumer["path"] == c1:
                self._assert_constant_revision(consumer)
            elif consumer["path"] == c2:
                self._assert_increased_revision(consumer)
                assert not consumer["vital"]

        set(c2 + "/@treat_as_queue_consumer", True)
        set(c2 + "/@target_queue", "a_cluster:a_path")
        set(c2 + "/@vital_queue_consumer", False)
        orchid.wait_fresh_poll()

        queues = self._get_queues_and_check_invariants(expected_count=2)
        consumers = self._get_consumers_and_check_invariants(expected_count=2)
        for queue in queues:
            self._assert_constant_revision(queue)
        for consumer in consumers:
            if consumer["path"] == c1:
                self._assert_constant_revision(consumer)
            elif consumer["path"] == c2:
                self._assert_increased_revision(consumer)
                assert consumer["target_cluster"] == "a_cluster"
                assert consumer["target_path"] == "a_path"
                assert not consumer["vital"]

        sync_unmount_table("//sys/queue_agents/queues")
        orchid.wait_fresh_poll()
        assert_yt_error(orchid.get_poll_error(), yt_error_codes.TabletNotMounted)

        sync_mount_table("//sys/queue_agents/queues")
        set(c2 + "/@target_queue", "another_cluster:another_path")
        set(c2 + "/@vital_queue_consumer", True)
        orchid.wait_fresh_poll()

        queues = self._get_queues_and_check_invariants(expected_count=2)
        consumers = self._get_consumers_and_check_invariants(expected_count=2)
        for queue in queues:
            self._assert_constant_revision(queue)
        for consumer in consumers:
            if consumer["path"] == c1:
                self._assert_constant_revision(consumer)
            elif consumer["path"] == c2:
                self._assert_increased_revision(consumer)
                assert consumer["target_cluster"] == "another_cluster"
                assert consumer["target_path"] == "another_path"
                assert consumer["vital"]

        self._drop_queues()
        self._drop_consumers()
        self._drop_tables()
