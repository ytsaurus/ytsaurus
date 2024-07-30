from yt_env_setup import (Restarter, QUEUE_AGENTS_SERVICE)
from yt_queue_agent_test_base import (TestQueueAgentBase, ReplicatedObjectBase, QueueAgentOrchid,
                                      CypressSynchronizerOrchid, AlertManagerOrchid, QueueAgentShardingManagerOrchid)

from yt_commands import (authors, get, get_driver, set, ls, wait, assert_yt_error, create, sync_mount_table, insert_rows,
                         delete_rows, remove, raises_yt_error, exists, start_transaction, select_rows,
                         sync_unmount_table, trim_rows, print_debug, alter_table, register_queue_consumer,
                         unregister_queue_consumer, mount_table, wait_for_tablet_state, sync_freeze_table,
                         sync_unfreeze_table, advance_consumer, sync_flush_table, sync_create_cells, read_table)

from yt.common import YtError, update_inplace

import builtins
import copy
import datetime
import time
import pytz

import pytest

from collections import defaultdict
from operator import itemgetter

from yt.yson import YsonUint64, YsonEntity

import yt_error_codes

import yt.environment.init_queue_agent_state as init_queue_agent_state

##################################################################


class TestQueueAgent(TestQueueAgentBase):
    DELTA_QUEUE_AGENT_DYNAMIC_CONFIG = {
        "cypress_synchronizer": {
            "policy": "watching",
        },
    }

    @authors("achulkov2", "nadya73")
    def test_other_stages_are_ignored(self):
        queue_orchid = QueueAgentOrchid()

        self._create_queue("//tmp/q")

        self._wait_for_component_passes()

        status = queue_orchid.get_queue_orchid("primary://tmp/q").get_status()
        assert status["partition_count"] == 1

        set("//tmp/q/@queue_agent_stage", "testing")

        self._wait_for_component_passes()

        with raises_yt_error(code=yt_error_codes.ResolveErrorCode):
            queue_orchid.get_queue_orchid("primary://tmp/q").get_status()

        set("//tmp/q/@queue_agent_stage", "production")

        self._wait_for_component_passes()

        status = queue_orchid.get_queue_orchid("primary://tmp/q").get_status()
        assert status["partition_count"] == 1

    @authors("cherepashka")
    def test_frozen_tablets_do_not_contain_errors(self):
        queue_orchid = QueueAgentOrchid()
        self._create_queue("//tmp/q")
        self._wait_for_component_passes()

        # Frozen queue doesn't have errors.
        sync_freeze_table("//tmp/q")
        self._wait_for_component_passes()
        partitions = queue_orchid.get_queue_orchid("primary://tmp/q").get_partitions()
        for partition in partitions:
            assert "error" not in partition

        # Consumer of frozen queue doesn't have errors.
        self._create_consumer("//tmp/c", mount=True)
        insert_rows("//sys/queue_agents/consumer_registrations", [
            {
                "queue_cluster": "primary",
                "queue_path": "//tmp/q",
                "consumer_cluster": "primary",
                "consumer_path": "//tmp/c",
                "vital": False,
            }
        ])
        self._wait_for_component_passes()
        consumer_registrations = get("//tmp/c/@queue_consumer_status/registrations")
        consumer_queues = get("//tmp/c/@queue_consumer_status/queues")
        for registration in consumer_registrations:
            assert "error" not in consumer_queues[registration["queue"]]


class TestQueueAgentNoSynchronizer(TestQueueAgentBase):
    DELTA_QUEUE_AGENT_DYNAMIC_CONFIG = {
        "cypress_synchronizer": {
            "enable": False,
        },
    }

    @authors("max42", "nadya73")
    def test_polling_loop(self):
        orchid = QueueAgentOrchid()

        self._drop_tables()

        orchid.wait_fresh_pass()
        assert_yt_error(orchid.get_pass_error(), yt_error_codes.ResolveErrorCode)

        wrong_schema = copy.deepcopy(init_queue_agent_state.QUEUE_TABLE_SCHEMA)
        for i in range(len(wrong_schema)):
            if wrong_schema[i]["name"] == "cluster":
                wrong_schema.pop(i)
                break
        self._prepare_tables()
        create("table", "//sys/queue_agents/queues", force=True, attributes={
            "dynamic": True,
            "schema": wrong_schema,
            **init_queue_agent_state.DEFAULT_TABLE_ATTRIBUTES
        })
        sync_mount_table("//sys/queue_agents/queues")

        insert_rows("//sys/queue_agents/queues", [{"path": "//tmp/q"}])

        orchid.wait_fresh_pass()
        assert_yt_error(orchid.get_pass_error(), "No such column")

        create("table", "//sys/queue_agents/queues", force=True, attributes={
            "dynamic": True,
            "schema": init_queue_agent_state.QUEUE_TABLE_SCHEMA,
            **init_queue_agent_state.DEFAULT_TABLE_ATTRIBUTES
        })
        sync_mount_table("//sys/queue_agents/queues")
        orchid.wait_fresh_pass()
        orchid.validate_no_pass_error()

    @authors("max42", "nadya73")
    def test_queue_state(self):
        orchid = QueueAgentOrchid()

        self._wait_for_component_passes(skip_cypress_synchronizer=True)
        queues = orchid.get_queues()
        assert len(queues) == 0

        # Missing row revision.
        insert_rows("//sys/queue_agents/queues",
                    [{"cluster": "primary", "path": "//tmp/q", "queue_agent_stage": "production"}])
        self._wait_for_component_passes(skip_cypress_synchronizer=True)
        status = orchid.get_queue_orchid("primary://tmp/q").get_status()
        assert_yt_error(YtError.from_dict(status["error"]), "Queue is not in-sync yet")
        assert "type" not in status

        # Wrong object type.
        insert_rows("//sys/queue_agents/queues",
                    [{"cluster": "primary", "path": "//tmp/q", "queue_agent_stage": "production",
                      "row_revision": YsonUint64(2345), "object_type": "map_node"}],
                    update=True)
        orchid.wait_fresh_pass()
        status = orchid.get_queue_orchid("primary://tmp/q").get_status()
        assert_yt_error(YtError.from_dict(status["error"]), 'Invalid queue object type "map_node"')

        # Sorted dynamic table.
        insert_rows("//sys/queue_agents/queues",
                    [{"cluster": "primary", "path": "//tmp/q", "queue_agent_stage": "production",
                      "row_revision": YsonUint64(3456), "object_type": "table", "dynamic": True, "sorted": True}],
                    update=True)
        orchid.wait_fresh_pass()
        status = orchid.get_queue_orchid("primary://tmp/q").get_status()
        assert_yt_error(YtError.from_dict(status["error"]), "Only ordered dynamic tables are supported as queues")

        # Proper ordered dynamic table.
        insert_rows("//sys/queue_agents/queues",
                    [{"cluster": "primary", "path": "//tmp/q", "queue_agent_stage": "production",
                      "row_revision": YsonUint64(4567), "object_type": "table", "dynamic": True, "sorted": False}],
                    update=True)
        orchid.wait_fresh_pass()
        status = orchid.get_queue_orchid("primary://tmp/q").get_status()
        # This error means that controller is instantiated and works properly (note that //tmp/q does not exist yet).
        assert_yt_error(YtError.from_dict(status["error"]), code=yt_error_codes.ResolveErrorCode)

        # Switch back to sorted dynamic table.
        insert_rows("//sys/queue_agents/queues",
                    [{"cluster": "primary", "path": "//tmp/q", "queue_agent_stage": "production",
                      "row_revision": YsonUint64(5678), "object_type": "table", "dynamic": False, "sorted": False}],
                    update=True)
        orchid.wait_fresh_pass()
        status = orchid.get_queue_orchid("primary://tmp/q").get_status()
        assert_yt_error(YtError.from_dict(status["error"]), "Only ordered dynamic tables are supported as queues")
        assert "family" not in status

        # Remove row; queue should be unregistered.
        delete_rows("//sys/queue_agents/queues", [{"cluster": "primary", "path": "//tmp/q"}])
        self._wait_for_component_passes(skip_cypress_synchronizer=True)

        queues = orchid.get_queues()
        assert len(queues) == 0

    @authors("max42", "nadya73")
    def test_consumer_state(self):
        orchid = QueueAgentOrchid()

        self._wait_for_component_passes(skip_cypress_synchronizer=True)
        queues = orchid.get_queues()
        consumers = orchid.get_consumers()
        assert len(queues) == 0
        assert len(consumers) == 0

        # Missing row revision.
        insert_rows("//sys/queue_agents/consumers",
                    [{"cluster": "primary", "path": "//tmp/c", "queue_agent_stage": "production"}])
        self._wait_for_component_passes(skip_cypress_synchronizer=True)
        status = orchid.get_consumer_orchid("primary://tmp/c").get_status()
        assert_yt_error(YtError.from_dict(status["error"]), "Consumer is not in-sync yet")
        assert "target" not in status

    @authors("achulkov2", "nadya73")
    def test_alerts(self):
        orchid = QueueAgentOrchid()
        alert_orchid = AlertManagerOrchid()

        self._drop_tables()

        orchid.wait_fresh_pass()

        wait(lambda: "queue_agent_pass_failed" in alert_orchid.get_alerts())
        assert_yt_error(YtError.from_dict(alert_orchid.get_alerts()["queue_agent_pass_failed"]),
                        "Error while reading dynamic state")

    @authors("achulkov2", "nadya73")
    def test_no_alerts(self):
        alert_orchid = AlertManagerOrchid()

        wait(lambda: not alert_orchid.get_alerts())

    @authors("max42", "nadya73")
    def test_controller_reuse(self):
        orchid = QueueAgentOrchid()

        self._wait_for_component_passes(skip_cypress_synchronizer=True)
        queues = orchid.get_queues()
        consumers = orchid.get_consumers()
        assert len(queues) == 0
        assert len(consumers) == 0

        insert_rows("//sys/queue_agents/consumers",
                    [{"cluster": "primary", "path": "//tmp/c", "queue_agent_stage": "production",
                      "row_revision": YsonUint64(1234), "revision": YsonUint64(100)}])
        self._wait_for_component_passes(skip_cypress_synchronizer=True)
        orchid.get_consumer_orchid("primary://tmp/c").wait_fresh_pass()
        row = orchid.get_consumer_orchid("primary://tmp/c").get_row()
        assert row["revision"] == 100

        # Make sure pass index is large enough.
        time.sleep(3)
        pass_index = orchid.get_consumer_orchid("primary://tmp/c").get_pass_index()

        insert_rows("//sys/queue_agents/consumers",
                    [{"cluster": "primary", "path": "//tmp/c", "queue_agent_stage": "production",
                      "row_revision": YsonUint64(2345), "revision": YsonUint64(200)}])
        orchid.wait_fresh_pass()
        orchid.get_consumer_orchid("primary://tmp/c").wait_fresh_pass()
        row = orchid.get_consumer_orchid("primary://tmp/c").get_row()
        assert row["revision"] == 200

        # Make sure controller was not recreated.
        assert orchid.get_consumer_orchid("primary://tmp/c").get_pass_index() > pass_index


class TestQueueController(TestQueueAgentBase):
    DELTA_QUEUE_AGENT_DYNAMIC_CONFIG = {
        "cypress_synchronizer": {
            "policy": "watching",
        },
    }

    def _timestamp_to_iso_str(self, ts):
        unix_ts = ts >> 30
        dt = datetime.datetime.fromtimestamp(unix_ts, tz=pytz.UTC)
        return dt.isoformat().replace("+00:00", ".000000Z")

    @authors("max42", "nadya73")
    @pytest.mark.parametrize("without_meta", [True, False])
    def test_queue_status(self, without_meta):
        orchid = QueueAgentOrchid()

        schema, _ = self._create_queue("//tmp/q", partition_count=2, enable_cumulative_data_weight_column=False)
        schema_with_cumulative_data_weight = schema + [{"name": "$cumulative_data_weight", "type": "int64"}]
        self._create_registered_consumer("//tmp/c", "//tmp/q", without_meta=without_meta)

        self._wait_for_component_passes()
        orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()

        queue_status = orchid.get_queue_orchid("primary://tmp/q").get_status()
        assert queue_status["family"] == "ordered_dynamic_table"
        assert queue_status["partition_count"] == 2
        assert queue_status["registrations"] == [
            {"queue": "primary://tmp/q", "consumer": "primary://tmp/c", "vital": False}
        ]

        def assert_partition(partition, lower_row_index, upper_row_index):
            assert partition["lower_row_index"] == lower_row_index
            assert partition["upper_row_index"] == upper_row_index
            assert partition["available_row_count"] == upper_row_index - lower_row_index

        null_time = "1970-01-01T00:00:00.000000Z"

        queue_partitions = orchid.get_queue_orchid("primary://tmp/q").get_partitions()
        for partition in queue_partitions:
            assert_partition(partition, 0, 0)
            assert partition["last_row_commit_time"] == null_time

        insert_rows("//tmp/q", [{"data": "foo", "$tablet_index": 0}])
        orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()

        queue_partitions = orchid.get_queue_orchid("primary://tmp/q").get_partitions()
        assert_partition(queue_partitions[0], 0, 1)
        assert queue_partitions[0]["last_row_commit_time"] != null_time
        assert_partition(queue_partitions[1], 0, 0)

        sync_unmount_table("//tmp/q")
        alter_table("//tmp/q", schema=schema_with_cumulative_data_weight)
        sync_mount_table("//tmp/q")

        orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()
        queue_partitions = orchid.get_queue_orchid("primary://tmp/q").get_partitions()
        assert_partition(queue_partitions[0], 0, 1)
        assert_partition(queue_partitions[1], 0, 0)
        assert queue_partitions[0]["cumulative_data_weight"] == YsonEntity()

        trim_rows("//tmp/q", 0, 1)

        self._wait_for_row_count("//tmp/q", 0, 0)

        orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()

        queue_partitions = orchid.get_queue_orchid("primary://tmp/q").get_partitions()
        assert_partition(queue_partitions[0], 1, 1)
        assert queue_partitions[0]["last_row_commit_time"] != null_time
        assert_partition(queue_partitions[1], 0, 0)

        insert_rows("//tmp/q", [{"data": "foo", "$tablet_index": 0}] * 100)

        orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()

        queue_partitions = orchid.get_queue_orchid("primary://tmp/q").get_partitions()

        assert_partition(queue_partitions[0], 1, 101)
        assert queue_partitions[0]["cumulative_data_weight"] == 2012
        assert queue_partitions[0]["trimmed_data_weight"] <= 2 * 20

        trim_rows("//tmp/q", 0, 91)
        self._wait_for_row_count("//tmp/q", 0, 10)

        orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()

        queue_partitions = orchid.get_queue_orchid("primary://tmp/q").get_partitions()

        assert_partition(queue_partitions[0], 91, 101)
        assert queue_partitions[0]["cumulative_data_weight"] == 2012
        assert 89 * 20 <= queue_partitions[0]["trimmed_data_weight"] <= 92 * 20
        assert 9 * 20 <= queue_partitions[0]["available_data_weight"] <= 11 * 20

    @authors("max42", "nadya73")
    @pytest.mark.parametrize("trim", [False, True])
    @pytest.mark.parametrize("without_meta", [True, False])
    def test_consumer_status(self, trim, without_meta):
        orchid = QueueAgentOrchid()

        self._create_queue("//tmp/q", partition_count=2)
        self._create_registered_consumer("//tmp/c", "//tmp/q", without_meta=without_meta)

        self._wait_for_component_passes()

        insert_rows("//tmp/q", [{"data": "foo", "$tablet_index": 0}, {"data": "bar", "$tablet_index": 1}])
        time.sleep(1.5)
        insert_rows("//tmp/q", [{"data": "foo", "$tablet_index": 0}, {"data": "bar", "$tablet_index": 1}])
        timestamps = [row["ts"] for row in select_rows("[$timestamp] as ts from [//tmp/q]")]
        timestamps = sorted(timestamps)
        assert timestamps[0] == timestamps[1] and timestamps[2] == timestamps[3]
        timestamps = [timestamps[0], timestamps[2]]
        print_debug(self._timestamp_to_iso_str(timestamps[0]), self._timestamp_to_iso_str(timestamps[1]))

        orchid.get_consumer_orchid("primary://tmp/c").wait_fresh_pass()
        consumer_status = orchid.get_consumer_orchid("primary://tmp/c").get_status()["queues"]["primary://tmp/q"]
        assert consumer_status["partition_count"] == 2

        time.sleep(1.5)
        self._advance_consumer("//tmp/c", "//tmp/q", 0, 0)
        time.sleep(1.5)
        self._advance_consumer("//tmp/c", "//tmp/q", 1, 0)

        def assert_partition(partition, next_row_index):
            assert partition["next_row_index"] == next_row_index
            assert partition["unread_row_count"] == max(0, 2 - next_row_index)
            assert partition["unread_data_weight"] == (YsonEntity() if next_row_index == 0 else (partition["unread_row_count"] * 20))
            assert partition["next_row_commit_time"] == (self._timestamp_to_iso_str(timestamps[next_row_index])
                                                         if next_row_index < 2 else YsonEntity())
            assert (partition["processing_lag"] > 0) == (next_row_index < 2)

        orchid.get_consumer_orchid("primary://tmp/c").wait_fresh_pass()
        consumer_partitions = orchid.get_consumer_orchid("primary://tmp/c").get_partitions()["primary://tmp/q"]
        assert_partition(consumer_partitions[0], 0)
        assert_partition(consumer_partitions[1], 0)

        self._advance_consumer("//tmp/c", "//tmp/q", 0, 1)

        orchid.get_consumer_orchid("primary://tmp/c").wait_fresh_pass()

        if trim:
            trim_rows("//tmp/q", 0, 1)

        orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()
        orchid.get_consumer_orchid("primary://tmp/c").wait_fresh_pass()
        consumer_partitions = orchid.get_consumer_orchid("primary://tmp/c").get_partitions()["primary://tmp/q"]
        assert_partition(consumer_partitions[0], 1)

        self._advance_consumer("//tmp/c", "//tmp/q", 1, 2)

        if trim:
            trim_rows("//tmp/q", 1, 1)

        orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()
        orchid.get_consumer_orchid("primary://tmp/c").wait_fresh_pass()
        consumer_partitions = orchid.get_consumer_orchid("primary://tmp/c").get_partitions()["primary://tmp/q"]

        assert_partition(consumer_partitions[1], 2)

    @authors("achulkov2", "nadya73")
    @pytest.mark.parametrize("without_meta", [True, False])
    def test_null_columns(self, without_meta):
        orchid = QueueAgentOrchid()

        schema, _ = self._create_queue("//tmp/q", enable_timestamp_column=False, enable_cumulative_data_weight_column=False)
        insert_rows("//tmp/q", [{"data": "foo"}] * 3)

        schema += [{"name": "$timestamp", "type": "uint64"}]
        schema += [{"name": "$cumulative_data_weight", "type": "int64"}]
        sync_unmount_table("//tmp/q")
        alter_table("//tmp/q", schema=schema)
        sync_mount_table("//tmp/q")

        self._create_registered_consumer("//tmp/c", "//tmp/q", without_meta=without_meta)

        self._wait_for_component_passes()

        assert orchid.get_queue_orchid("primary://tmp/q").get_status()["partition_count"] == 1
        assert orchid.get_consumer_orchid("primary://tmp/c").get_status()["queues"]["primary://tmp/q"]["partition_count"] == 1

    @authors("max42", "nadya73")
    @pytest.mark.parametrize("without_meta", [True, False])
    def test_consumer_partition_disposition(self, without_meta):
        orchid = QueueAgentOrchid()

        self._create_queue("//tmp/q")
        self._create_registered_consumer("//tmp/c", "//tmp/q", without_meta=without_meta)

        self._wait_for_component_passes()

        insert_rows("//tmp/q", [{"data": "foo"}] * 3)
        trim_rows("//tmp/q", 0, 1)
        orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()

        expected_dispositions = ["expired", "pending_consumption", "pending_consumption", "up_to_date", "ahead"]
        for offset, expected_disposition in enumerate(expected_dispositions):
            self._advance_consumer("//tmp/c", "//tmp/q", 0, offset)
            orchid.get_consumer_orchid("primary://tmp/c").wait_fresh_pass()
            partition = orchid.get_consumer_orchid("primary://tmp/c").get_partitions()["primary://tmp/q"][0]
            assert partition["disposition"] == expected_disposition
            assert partition["unread_row_count"] == 3 - offset

    @authors("max42", "nadya73")
    @pytest.mark.parametrize("without_meta", [True, False])
    def test_inconsistent_partitions_in_consumer_table(self, without_meta):
        orchid = QueueAgentOrchid()

        self._create_queue("//tmp/q", partition_count=2)
        self._create_registered_consumer("//tmp/c", "//tmp/q", without_meta=without_meta)

        self._wait_for_component_passes()

        insert_rows("//tmp/q", [{"data": "foo"}] * 2)
        orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()

        self._advance_consumer("//tmp/c", "//tmp/q", 1, 1, via_insert=True)
        self._advance_consumer("//tmp/c", "//tmp/q", 2 ** 63 - 1, 1, via_insert=True)
        self._advance_consumer("//tmp/c", "//tmp/q", 2 ** 64 - 1, 1, via_insert=True)

        orchid.get_consumer_orchid("primary://tmp/c").wait_fresh_pass()

        partitions = orchid.get_consumer_orchid("primary://tmp/c").get_partitions()["primary://tmp/q"]
        assert len(partitions) == 2
        assert partitions[0]["next_row_index"] == 0

    @authors("apachee")
    @pytest.mark.timeout(300)
    def test_queue_agent_banned_attribute(self):
        orchid = QueueAgentOrchid()

        self._create_queue("//tmp/q")
        self._wait_for_component_passes()
        queue_orchid = orchid.get_queue_orchid("primary://tmp/q")

        set("//tmp/q/@queue_agent_banned", True)
        wait(lambda: queue_orchid.get_row()["queue_agent_banned"])
        assert "Queue is banned" in queue_orchid.get_status()["error"]["message"]

        set("//tmp/q/@queue_agent_banned", False)
        queue_orchid.wait_fresh_pass()
        assert "error" not in queue_orchid.get_status()

        set("//tmp/q/@queue_agent_banned", True)
        wait(lambda: queue_orchid.get_row()["queue_agent_banned"])
        assert "Queue is banned" in queue_orchid.get_status()["error"]["message"]

        remove("//tmp/q/@queue_agent_banned")
        queue_orchid.wait_fresh_pass()
        assert "error" not in queue_orchid.get_status()


class TestRates(TestQueueAgentBase):
    DELTA_QUEUE_AGENT_DYNAMIC_CONFIG = {
        "cypress_synchronizer": {
            # We manually expose queues and consumers in this test, so we use polling implementation.
            "policy": "polling",
        },
    }

    @authors("max42", "nadya73")
    @pytest.mark.parametrize("without_meta", [True, False])
    def test_rates(self, without_meta):
        eps = 1e-2
        zero = {"current": 0.0, "1m_raw": 0.0, "1m": 0.0, "1h": 0.0, "1d": 0.0}

        orchid = QueueAgentOrchid()

        self._create_queue("//tmp/q", partition_count=2)
        self._create_registered_consumer("//tmp/c", "//tmp/q", without_meta=without_meta)

        # We advance consumer in both partitions by one beforehand in order to workaround
        # the corner-case when consumer stands on the first row in the available row window.
        # In such case we (currently) cannot reliably know cumulative data weight.

        insert_rows("//tmp/q", [{"data": "x", "$tablet_index": 0}, {"data": "x", "$tablet_index": 1}])
        self._advance_consumers("//tmp/c", "//tmp/q", {0: 1, 1: 1})

        # Expose queue and consumer.

        insert_rows("//sys/queue_agents/consumers", [{"cluster": "primary", "path": "//tmp/c"}])
        insert_rows("//sys/queue_agents/queues", [{"cluster": "primary", "path": "//tmp/q"}])

        self._wait_for_component_passes()

        # Test queue write rate. Initially rates are zero.
        orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()
        status, partitions = orchid.get_queue_orchid("primary://tmp/q").get_queue()
        assert len(partitions) == 2
        assert partitions[0]["write_row_count_rate"] == zero
        assert partitions[1]["write_row_count_rate"] == zero
        assert partitions[0]["write_data_weight_rate"] == zero
        assert partitions[1]["write_data_weight_rate"] == zero
        assert status["write_row_count_rate"] == zero
        assert status["write_data_weight_rate"] == zero

        # After inserting (resp.) 2 and 1 rows, write rates should be non-zero and proportional to (resp.) 2 and 1.

        insert_rows("//tmp/q", [{"data": "x", "$tablet_index": 0}] * 2 + [{"data": "x", "$tablet_index": 1}])

        orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()
        status, partitions = orchid.get_queue_orchid("primary://tmp/q").get_queue()
        assert len(partitions) == 2
        assert partitions[1]["write_row_count_rate"]["1m_raw"] > 0
        assert partitions[1]["write_data_weight_rate"]["1m_raw"] > 0
        assert abs(partitions[0]["write_row_count_rate"]["1m_raw"] -
                   2 * partitions[1]["write_row_count_rate"]["1m_raw"]) < eps
        assert abs(partitions[0]["write_data_weight_rate"]["1m_raw"] -
                   2 * partitions[1]["write_data_weight_rate"]["1m_raw"]) < eps

        # Check total write rate.

        status, partitions = orchid.get_queue_orchid("primary://tmp/q").get_queue()
        assert abs(status["write_row_count_rate"]["1m_raw"] -
                   3 * partitions[1]["write_row_count_rate"]["1m_raw"]) < eps
        assert abs(status["write_data_weight_rate"]["1m_raw"] -
                   3 * partitions[1]["write_data_weight_rate"]["1m_raw"]) < eps

        # Test consumer read rate. Again, initially rates are zero.

        orchid.get_consumer_orchid("primary://tmp/c").wait_fresh_pass()
        status, partitions = orchid.get_consumer_orchid("primary://tmp/c").get_subconsumer("primary://tmp/q")
        assert len(partitions) == 2
        assert partitions[0]["read_row_count_rate"] == zero
        assert partitions[1]["read_row_count_rate"] == zero
        assert partitions[0]["read_data_weight_rate"] == zero
        assert partitions[1]["read_data_weight_rate"] == zero
        assert status["read_row_count_rate"] == zero
        assert status["read_data_weight_rate"] == zero

        # Advance consumer by (resp.) 2 and 1. Again, same statements should hold for read rates.

        self._advance_consumers("//tmp/c", "//tmp/q", {0: 3, 1: 2})

        orchid.get_consumer_orchid("primary://tmp/c").wait_fresh_pass()
        status, partitions = orchid.get_consumer_orchid("primary://tmp/c").get_subconsumer("primary://tmp/q")
        assert len(partitions) == 2
        assert partitions[1]["read_row_count_rate"]["1m_raw"] > 0
        assert abs(partitions[0]["read_row_count_rate"]["1m_raw"] -
                   2 * partitions[1]["read_row_count_rate"]["1m_raw"]) < eps
        assert partitions[1]["read_data_weight_rate"]["1m_raw"] > 0
        assert abs(partitions[0]["read_data_weight_rate"]["1m_raw"] -
                   2 * partitions[1]["read_data_weight_rate"]["1m_raw"]) < eps * 10

        # Check total read rate.

        status, partitions = orchid.get_consumer_orchid("primary://tmp/c").get_subconsumer("primary://tmp/q")
        assert abs(status["read_row_count_rate"]["1m_raw"] -
                   3 * partitions[1]["read_row_count_rate"]["1m_raw"]) < eps
        assert abs(status["read_data_weight_rate"]["1m_raw"] -
                   3 * partitions[1]["read_data_weight_rate"]["1m_raw"]) < eps * 10


class TestAutomaticTrimming(TestQueueAgentBase):
    DELTA_QUEUE_AGENT_DYNAMIC_CONFIG = {
        "queue_agent": {
            "controller": {
                "enable_automatic_trimming": True,
            },
        },
        "cypress_synchronizer": {
            "policy": "watching",
        },
    }

    @authors("achulkov2", "nadya73")
    def test_basic(self):
        queue_agent_orchid = QueueAgentOrchid()

        self._create_queue("//tmp/q", partition_count=2)
        self._create_registered_consumer("//tmp/c1", "//tmp/q", vital=True)
        self._create_registered_consumer("//tmp/c2", "//tmp/q", vital=False)
        self._create_registered_consumer("//tmp/c3", "//tmp/q", vital=True)

        set("//tmp/q/@auto_trim_config", {"enable": True})

        self._wait_for_component_passes()

        insert_rows("//tmp/q", [{"$tablet_index": 0, "data": "foo"}] * 5)
        insert_rows("//tmp/q", [{"$tablet_index": 1, "data": "bar"}] * 7)
        queue_agent_orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()

        self._advance_consumer("//tmp/c1", "//tmp/q", 0, 3)
        self._advance_consumer("//tmp/c2", "//tmp/q", 0, 5)
        # Vital consumer c3 was not advanced, so nothing should be trimmed.

        self._advance_consumer("//tmp/c1", "//tmp/q", 1, 3)
        self._advance_consumer("//tmp/c2", "//tmp/q", 1, 1)
        self._advance_consumer("//tmp/c3", "//tmp/q", 1, 2)
        # Consumer c2 is non-vital, only c1 and c3 should be considered.

        queue_agent_orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()

        self._wait_for_row_count("//tmp/q", 1, 5)
        self._wait_for_row_count("//tmp/q", 0, 5)

        # Consumer c3 is the farthest behind.
        self._advance_consumer("//tmp/c3", "//tmp/q", 0, 2)

        queue_agent_orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()
        self._wait_for_row_count("//tmp/q", 0, 3)

        # Now c1 is the farthest behind.
        self._advance_consumer("//tmp/c3", "//tmp/q", 0, 4)

        queue_agent_orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()
        self._wait_for_row_count("//tmp/q", 0, 2)

        # Both vital consumers are at the same offset.
        self._advance_consumer("//tmp/c1", "//tmp/q", 1, 6)
        self._advance_consumer("//tmp/c3", "//tmp/q", 1, 6)

        queue_agent_orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()
        self._wait_for_row_count("//tmp/q", 1, 1)
        # Nothing should have changed here.
        self._wait_for_row_count("//tmp/q", 0, 2)

    @authors("achulkov2", "nadya73")
    def test_retained_rows(self):
        queue_agent_orchid = QueueAgentOrchid()
        cypress_synchronizer_orchid = CypressSynchronizerOrchid()

        self._create_queue("//tmp/q", partition_count=2)
        self._create_registered_consumer("//tmp/c1", "//tmp/q", vital=True)
        self._create_registered_consumer("//tmp/c2", "//tmp/q", vital=False)

        set("//tmp/q/@auto_trim_config", {"enable": True, "retained_rows": 3})

        self._wait_for_component_passes()

        insert_rows("//tmp/q", [{"$tablet_index": 0, "data": "foo"}] * 5)
        insert_rows("//tmp/q", [{"$tablet_index": 1, "data": "bar"}] * 7)
        queue_agent_orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()

        self._advance_consumer("//tmp/c1", "//tmp/q", 0, 3)
        self._advance_consumer("//tmp/c2", "//tmp/q", 0, 5)
        # Consumer c2 is non-vital and is ignored. We should only trim 2 rows, so that at least 3 are left.

        self._advance_consumer("//tmp/c2", "//tmp/q", 1, 1)
        # Nothing should be trimmed since vital consumer c1 was not advanced.

        queue_agent_orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()
        self._wait_for_row_count("//tmp/q", 0, 3)
        self._wait_for_row_count("//tmp/q", 1, 7)

        self._advance_consumer("//tmp/c1", "//tmp/q", 1, 2)
        # Now the first two rows of partition 1 should be trimmed. Consumer c2 is now behind.

        queue_agent_orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()
        self._wait_for_row_count("//tmp/q", 0, 3)
        self._wait_for_row_count("//tmp/q", 1, 5)

        insert_rows("//tmp/q", [{"$tablet_index": 0, "data": "foo"}] * 5)
        # Since there are more rows in the partition now, we can trim up to offset 3 of consumer c1.

        queue_agent_orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()
        self._wait_for_min_row_index("//tmp/q", 0, 3)
        self._wait_for_row_count("//tmp/q", 0, 7)
        self._wait_for_row_count("//tmp/q", 1, 5)

        self._advance_consumer("//tmp/c1", "//tmp/q", 1, 6)
        # We should only trim up to offset 4, so that 3 rows are left
        self._wait_for_row_count("//tmp/q", 1, 3)
        self._wait_for_min_row_index("//tmp/q", 1, 4)
        # This shouldn't change.
        self._wait_for_row_count("//tmp/q", 0, 7)

        remove("//tmp/q/@auto_trim_config/retained_rows")
        cypress_synchronizer_orchid.wait_fresh_pass()
        queue_agent_orchid.wait_fresh_pass()

        # Now we should trim partition 1 up to offset 6 (c1).
        self._wait_for_row_count("//tmp/q", 1, 1)
        self._wait_for_min_row_index("//tmp/q", 1, 6)
        # This shouldn't change, since it was already bigger than 3.
        self._wait_for_row_count("//tmp/q", 0, 7)

    @authors("cherepashka")
    def test_retained_lifetime_duration(self):
        queue_agent_orchid = QueueAgentOrchid()
        cypress_synchronizer_orchid = CypressSynchronizerOrchid()

        self._create_queue("//tmp/q", partition_count=2)
        self._create_registered_consumer("//tmp/c1", "//tmp/q", vital=True)
        self._create_registered_consumer("//tmp/c2", "//tmp/q", vital=False)

        set("//tmp/q/@auto_trim_config", {
            "enable": True,
            "retained_lifetime_duration": 3000})  # 3 seconds

        self._wait_for_component_passes()

        insert_rows("//tmp/q", [{"$tablet_index": 0, "data": "foo"}] * 5)
        queue_agent_orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()

        self._advance_consumer("//tmp/c2", "//tmp/q", 0, 1)
        time.sleep(3)
        # Rows lived more than 3 seconds, but nothing should be trimmed since vital consumer c1 was not advanced.

        queue_agent_orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()
        self._wait_for_row_count("//tmp/q", 0, 5)

        set("//tmp/q/@auto_trim_config", {
            "enable": True,
            "retained_lifetime_duration": 5 * 3600 * 1000})  # 5 hours
        self._wait_for_component_passes()

        self._advance_consumer("//tmp/c1", "//tmp/q", 0, 3)
        self._advance_consumer("//tmp/c2", "//tmp/q", 0, 5)
        # Rows shouldn't be trimmed since they all lived less than 5 hours.

        queue_agent_orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()
        self._wait_for_row_count("//tmp/q", 0, 5)

        remove("//tmp/q/@auto_trim_config/retained_lifetime_duration")
        cypress_synchronizer_orchid.wait_fresh_pass()
        queue_agent_orchid.wait_fresh_pass()

        # Now partition 0 should be trimmed up to offset 3 (c1).
        self._wait_for_row_count("//tmp/q", 0, 2)
        self._wait_for_min_row_index("//tmp/q", 0, 3)

        set("//tmp/q/@auto_trim_config", {
            "enable": True,
            "retained_lifetime_duration": 3000})  # 3 seconds
        self._wait_for_component_passes()

        insert_rows("//tmp/q", [{"$tablet_index": 1, "data": "foo"}] * 3)
        queue_agent_orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()
        self._flush_table("//tmp/q", first_tablet_index=1, last_tablet_index=1)
        # Flush dynamic store with inserted rows into chunk.
        time.sleep(3)

        insert_rows("//tmp/q", [{"$tablet_index": 1, "data": "foo"}] * 5)
        queue_agent_orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()

        self._advance_consumer("//tmp/c1", "//tmp/q", 1, 7)
        # Now we have at least 2 stores in chunk.
        # First store contains 3 rows with expired lifetime duration, so they should be trimmed.
        # Second store contains 0 expired rows, so nothing from it should be trimmed.

        self._wait_for_row_count("//tmp/q", 1, 5)
        self._wait_for_min_row_index("//tmp/q", 1, 3)

    @authors("cherepashka")
    def test_retained_lifetime_duration_and_rows(self):
        queue_agent_orchid = QueueAgentOrchid()
        cypress_synchronizer_orchid = CypressSynchronizerOrchid()

        self._create_queue("//tmp/q", partition_count=2)
        self._create_registered_consumer("//tmp/c1", "//tmp/q", vital=True)
        self._create_registered_consumer("//tmp/c2", "//tmp/q", vital=False)
        set("//tmp/q/@auto_trim_config", {
            "enable": True,
            "retained_lifetime_duration": 3000,  # 3 seconds
            "retained_rows": 6})
        self._wait_for_component_passes()

        insert_rows("//tmp/q", [{"$tablet_index": 0, "data": "foo"}] * 7)
        queue_agent_orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()
        self._advance_consumer("//tmp/c1", "//tmp/q", 0, 3)
        self._advance_consumer("//tmp/c2", "//tmp/q", 0, 5)
        queue_agent_orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()
        time.sleep(3)

        # In partition 0 rows lived more than 3 seconds, but we have to keep 6 rows since retained_rows was set.
        self._wait_for_row_count("//tmp/q", 0, 6)
        self._wait_for_min_row_index("//tmp/q", 0, 1)

        remove("//tmp/q/@auto_trim_config/retained_rows")
        cypress_synchronizer_orchid.wait_fresh_pass()
        queue_agent_orchid.wait_fresh_pass()

        # Now partition 0 should be trimmed up to offset 3 (c1).
        self._wait_for_row_count("//tmp/q", 0, 4)
        self._wait_for_min_row_index("//tmp/q", 0, 3)

        set("//tmp/q/@auto_trim_config", {
            "enable": True,
            "retained_lifetime_duration": 5 * 3600 * 1000,  # 5 hours
            "retained_rows": 6
            })
        self._wait_for_component_passes()

        insert_rows("//tmp/q", [{"$tablet_index": 1, "data": "foo"}] * 8)
        queue_agent_orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()

        self._advance_consumer("//tmp/c1", "//tmp/q", 1, 6)
        self._advance_consumer("//tmp/c2", "//tmp/q", 1, 5)
        queue_agent_orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()

        # We shouldn't trim rows in partition 1 since they all lived less than 5 hours.
        self._wait_for_row_count("//tmp/q", 0, 4)
        self._wait_for_row_count("//tmp/q", 1, 8)

        remove("//tmp/q/@auto_trim_config/retained_lifetime_duration")
        cypress_synchronizer_orchid.wait_fresh_pass()
        queue_agent_orchid.wait_fresh_pass()

        # Now we can trim, so that 6 rows are left in partition 1.
        self._wait_for_row_count("//tmp/q", 0, 4)
        self._wait_for_row_count("//tmp/q", 1, 6)
        self._wait_for_min_row_index("//tmp/q", 1, 2)

        remove("//tmp/q/@auto_trim_config/retained_rows")
        cypress_synchronizer_orchid.wait_fresh_pass()
        queue_agent_orchid.wait_fresh_pass()

        # Now we should trim partition 1 up to offset 6 (c1).
        self._wait_for_row_count("//tmp/q", 0, 4)
        self._wait_for_row_count("//tmp/q", 1, 2)
        self._wait_for_min_row_index("//tmp/q", 1, 6)

    @authors("cherepashka")
    def test_trim_via_object_id(self):
        queue_agent_orchid = QueueAgentOrchid()
        cypress_synchronizer_orchid = CypressSynchronizerOrchid()

        self._create_queue("//tmp/q")
        self._create_registered_consumer("//tmp/c1", "//tmp/q", vital=True)
        set("//tmp/q/@auto_trim_config", {"enable": True})

        self._wait_for_component_passes()

        insert_rows("//tmp/q", [{"$tablet_index": 0, "data": "bar"}] * 7)
        queue_agent_orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()

        self._advance_consumer("//tmp/c1", "//tmp/q", 0, 1)
        self._wait_for_row_count("//tmp/q", 0, 6)

        unregister_queue_consumer("//tmp/q", "//tmp/c1")

        cypress_synchronizer_orchid.wait_fresh_pass()

        self._apply_dynamic_config_patch({
            "cypress_synchronizer": {
                "enable": False
            }
        })
        time.sleep(1)

        remove("//tmp/c1")
        remove("//tmp/q")

        self._create_queue("//tmp/q")
        self._create_registered_consumer("//tmp/c1", "//tmp/q", vital=True)
        set("//tmp/q/@auto_trim_config", {"enable": False})

        insert_rows("//tmp/q", [{"$tablet_index": 0, "data": "bar"}] * 7)
        queue_agent_orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()

        self._advance_consumer("//tmp/c1", "//tmp/q", 0, 4)
        time.sleep(1)
        self._wait_for_row_count("//tmp/q", 0, 7)

        self._apply_dynamic_config_patch({
            "cypress_synchronizer": {
                "enable": True
            }
        })
        time.sleep(1)

        set("//tmp/q/@auto_trim_config", {"enable": True})
        self._wait_for_row_count("//tmp/q", 0, 3)

    @authors("achulkov2", "nadya73")
    def test_vitality_changes(self):
        queue_agent_orchid = QueueAgentOrchid()
        cypress_synchronizer_orchid = CypressSynchronizerOrchid()

        self._create_queue("//tmp/q")
        self._create_registered_consumer("//tmp/c1", "//tmp/q", vital=True)
        self._create_registered_consumer("//tmp/c2", "//tmp/q", vital=False)
        self._create_registered_consumer("//tmp/c3", "//tmp/q", vital=True)

        set("//tmp/q/@auto_trim_config", {"enable": True})

        self._wait_for_component_passes()

        insert_rows("//tmp/q", [{"$tablet_index": 0, "data": "bar"}] * 7)
        queue_agent_orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()

        # Only c1 and c3 are vital, so we should trim up to row 1.
        self._advance_consumer("//tmp/c1", "//tmp/q", 0, 1)
        self._advance_consumer("//tmp/c2", "//tmp/q", 0, 3)
        self._advance_consumer("//tmp/c3", "//tmp/q", 0, 2)

        self._wait_for_row_count("//tmp/q", 0, 6)

        # Now we should only consider c3 and trim more rows.
        unregister_queue_consumer("//tmp/q", "//tmp/c1")
        register_queue_consumer("//tmp/q", "//tmp/c1", vital=False)
        cypress_synchronizer_orchid.wait_fresh_pass()
        queue_agent_orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()

        self._wait_for_row_count("//tmp/q", 0, 5)

        # Now c2 and c3 are vital. Nothing should be trimmed though.
        unregister_queue_consumer("//tmp/q", "//tmp/c2")
        register_queue_consumer("//tmp/q", "//tmp/c2", vital=True)
        cypress_synchronizer_orchid.wait_fresh_pass()
        queue_agent_orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()

        time.sleep(1)
        self._wait_for_row_count("//tmp/q", 0, 5)

        # Now only c2 is vital, so we should trim more rows.
        unregister_queue_consumer("//tmp/q", "//tmp/c3")
        register_queue_consumer("//tmp/q", "//tmp/c3", vital=False)
        cypress_synchronizer_orchid.wait_fresh_pass()
        queue_agent_orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()

        self._wait_for_row_count("//tmp/q", 0, 4)

    @authors("achulkov2", "nadya73")
    def test_erroneous_vital_consumer(self):
        queue_agent_orchid = QueueAgentOrchid()

        self._create_queue("//tmp/q")
        self._create_registered_consumer("//tmp/c1", "//tmp/q", vital=True)
        self._create_registered_consumer("//tmp/c2", "//tmp/q", vital=False)
        self._create_registered_consumer("//tmp/c3", "//tmp/q", vital=True)

        set("//tmp/q/@auto_trim_config", {"enable": True})

        self._wait_for_component_passes()

        insert_rows("//tmp/q", [{"$tablet_index": 0, "data": "foo"}] * 5)
        queue_agent_orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()

        # Consumer c3 is vital, so nothing should be trimmed.
        self._advance_consumer("//tmp/c1", "//tmp/q", 0, 2)
        self._advance_consumer("//tmp/c2", "//tmp/q", 0, 1)
        queue_agent_orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()

        time.sleep(1)
        self._wait_for_row_count("//tmp/q", 0, 5)

        # This should set an error in c1.
        sync_unmount_table("//tmp/c1")

        wait(lambda: "error" in queue_agent_orchid.get_consumer_orchid("primary://tmp/c1").get_status()["queues"]["primary://tmp/q"])

        # Consumers c1 and c3 are vital, but c1 is in an erroneous state, so nothing should be trimmed.
        self._advance_consumer("//tmp/c3", "//tmp/q", 0, 2)
        queue_agent_orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()

        time.sleep(1)
        self._wait_for_row_count("//tmp/q", 0, 5)

        # Now c1 should be back and the first 2 rows should be trimmed.
        sync_mount_table("//tmp/c1")

        self._wait_for_row_count("//tmp/q", 0, 3)

    @authors("achulkov2", "nadya73")
    def test_erroneous_partition(self):
        queue_agent_orchid = QueueAgentOrchid()

        self._create_queue("//tmp/q", partition_count=2)
        self._create_registered_consumer("//tmp/c1", "//tmp/q", vital=True)
        self._create_registered_consumer("//tmp/c2", "//tmp/q", vital=False)

        set("//tmp/q/@auto_trim_config", {"enable": True})

        self._wait_for_component_passes()

        insert_rows("//tmp/q", [{"$tablet_index": 0, "data": "foo"}] * 5)
        insert_rows("//tmp/q", [{"$tablet_index": 1, "data": "bar"}] * 7)
        queue_agent_orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()

        sync_unmount_table("//tmp/q", first_tablet_index=0, last_tablet_index=0)

        # Nothing should be trimmed from the first partition, since it is unmounted.
        self._advance_consumer("//tmp/c1", "//tmp/q", 0, 2, via_insert=True)
        self._advance_consumer("//tmp/c2", "//tmp/q", 0, 1, via_insert=True)

        # Yet the second partition should be trimmed based on c1.
        self._advance_consumer("//tmp/c1", "//tmp/q", 1, 3)
        self._advance_consumer("//tmp/c2", "//tmp/q", 1, 2)

        self._wait_for_row_count("//tmp/q", 1, 4)

        sync_mount_table("//tmp/q", first_tablet_index=0, last_tablet_index=0)
        # Now the first partition should be trimmed based on c1 as well.

        self._wait_for_row_count("//tmp/q", 0, 3)

    @authors("achulkov2", "nadya73")
    def test_erroneous_queue(self):
        queue_agent_orchid = QueueAgentOrchid()

        self._create_queue("//tmp/q1")
        self._create_queue("//tmp/q2")
        self._create_registered_consumer("//tmp/c1", "//tmp/q1", vital=True)
        self._create_registered_consumer("//tmp/c2", "//tmp/q2", vital=True)

        set("//tmp/q1/@auto_trim_config", {"enable": True})
        set("//tmp/q2/@auto_trim_config", {"enable": True})

        self._wait_for_component_passes()

        insert_rows("//tmp/q1", [{"data": "foo"}] * 5)
        queue_agent_orchid.get_queue_orchid("primary://tmp/q1").wait_fresh_pass()

        insert_rows("//tmp/q2", [{"data": "bar"}] * 7)
        queue_agent_orchid.get_queue_orchid("primary://tmp/q2").wait_fresh_pass()

        # Both queues should be trimmed since their sole consumers are vital.
        self._advance_consumer("//tmp/c1", "//tmp/q1", 0, 2)
        self._advance_consumer("//tmp/c2", "//tmp/q2", 0, 3)

        self._wait_for_row_count("//tmp/q1", 0, 3)
        self._wait_for_row_count("//tmp/q2", 0, 4)

        sync_unmount_table("//tmp/q2")

        self._advance_consumer("//tmp/c1", "//tmp/q1", 0, 3)
        self._advance_consumer("//tmp/c2", "//tmp/q2", 0, 4, via_insert=True)

        queue_agent_orchid.get_queue_orchid("primary://tmp/q1").wait_fresh_pass()
        queue_agent_orchid.get_queue_orchid("primary://tmp/q2").wait_fresh_pass()

        self._wait_for_row_count("//tmp/q1", 0, 2)
        # The second queue should not be trimmed yet.

        sync_mount_table("//tmp/q2")

        self._wait_for_row_count("//tmp/q2", 0, 3)

    @authors("achulkov2", "nadya73")
    def test_configuration_changes(self):
        queue_agent_orchid = QueueAgentOrchid()
        cypress_synchronizer_orchid = CypressSynchronizerOrchid()

        self._create_queue("//tmp/q")
        self._create_registered_consumer("//tmp/c", "//tmp/q", vital=True)

        set("//tmp/q/@auto_trim_config", {"enable": True})

        self._wait_for_component_passes()

        insert_rows("//tmp/q", [{"$tablet_index": 0, "data": "bar"}] * 7)

        self._advance_consumer("//tmp/c", "//tmp/q", 0, 1)
        self._wait_for_row_count("//tmp/q", 0, 6)

        set("//tmp/q/@auto_trim_config/enable", False)
        cypress_synchronizer_orchid.wait_fresh_pass()
        queue_agent_orchid.wait_fresh_pass()

        self._advance_consumer("//tmp/c", "//tmp/q", 0, 2)
        time.sleep(1)
        self._wait_for_row_count("//tmp/q", 0, 6)

        set("//tmp/q/@auto_trim_config", {"enable": True, "some_unrecognized_option": "hello"})
        cypress_synchronizer_orchid.wait_fresh_pass()
        queue_agent_orchid.wait_fresh_pass()

        self._wait_for_row_count("//tmp/q", 0, 5)

        remove("//tmp/q/@auto_trim_config")
        cypress_synchronizer_orchid.wait_fresh_pass()
        queue_agent_orchid.wait_fresh_pass()

        self._advance_consumer("//tmp/c", "//tmp/q", 0, 3)
        time.sleep(1)
        self._wait_for_row_count("//tmp/q", 0, 5)

        set("//tmp/q/@auto_trim_config", {"enable": True})
        cypress_synchronizer_orchid.wait_fresh_pass()
        queue_agent_orchid.wait_fresh_pass()

        self._wait_for_row_count("//tmp/q", 0, 4)

        self._apply_dynamic_config_patch({
            "queue_agent": {
                "controller": {
                    "enable_automatic_trimming": False,
                }
            }
        })

        self._advance_consumer("//tmp/c", "//tmp/q", 0, 4)
        time.sleep(1)
        self._wait_for_row_count("//tmp/q", 0, 4)

        self._apply_dynamic_config_patch({
            "queue_agent": {
                "controller": {
                    "enable_automatic_trimming": True,
                    "trimming_period": 999999,
                }
            }
        })

        time.sleep(1)
        self._wait_for_row_count("//tmp/q", 0, 4)

        self._apply_dynamic_config_patch({
            "queue_agent": {
                "controller": {
                    "trimming_period": YsonEntity(),
                }
            }
        })

        self._wait_for_row_count("//tmp/q", 0, 3)

    @authors("cherepashka")
    def test_trim_only_mounted_tablets(self):
        queue_agent_orchid = QueueAgentOrchid()

        self._create_queue("//tmp/q", partition_count=3)
        self._create_registered_consumer("//tmp/c1", "//tmp/q", vital=True)
        self._create_registered_consumer("//tmp/c2", "//tmp/q", vital=False)
        set("//tmp/q/@auto_trim_config", {"enable": True})

        self._wait_for_component_passes()
        vital_consumer_offsets = [0, 0, 0]
        insert_rows("//tmp/q", [{"$tablet_index": 0, "data": "first tablet data"}] * 5)
        insert_rows("//tmp/q", [{"$tablet_index": 1, "data": "second tablet data"}] * 6)
        insert_rows("//tmp/q", [{"$tablet_index": 2, "data": "third tablet data"}] * 7)
        queue_agent_orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()

        # Nothing should be trimmed from the first partition, since it is frozen.
        sync_freeze_table("//tmp/q", first_tablet_index=0, last_tablet_index=0)
        self._advance_consumer("//tmp/c1", "//tmp/q", 0, 2)
        self._advance_consumer("//tmp/c2", "//tmp/q", 0, 1)
        self._wait_for_row_count("//tmp/q", 0, 5)
        vital_consumer_offsets[0] = 2

        # Rows should be trimmed by vital consumer in mounted tablets.
        self._advance_consumer("//tmp/c1", "//tmp/q", 1, 4)
        self._advance_consumer("//tmp/c2", "//tmp/q", 1, 1)
        vital_consumer_offsets[1] = 4
        self._wait_for_row_count("//tmp/q", 1, 6 - vital_consumer_offsets[1])

        self._advance_consumer("//tmp/c1", "//tmp/q", 2, 3)
        self._advance_consumer("//tmp/c2", "//tmp/q", 2, 1)
        vital_consumer_offsets[2] = 3
        self._wait_for_row_count("//tmp/q", 2, 7 - vital_consumer_offsets[2])

        # After unfreezing tablet rows should be trimmed by vital consumer.
        sync_unfreeze_table("//tmp/q", first_tablet_index=0, last_tablet_index=0)
        self._wait_for_row_count("//tmp/q", 0, 5 - vital_consumer_offsets[0])

        # Nothing should be trimmed from first two partitions, since they are frozen.
        sync_freeze_table("//tmp/q", first_tablet_index=0, last_tablet_index=1)
        self._advance_consumer("//tmp/c1", "//tmp/q", 0, 4)
        self._advance_consumer("//tmp/c2", "//tmp/q", 0, 2)
        self._advance_consumer("//tmp/c1", "//tmp/q", 1, 5)
        self._advance_consumer("//tmp/c2", "//tmp/q", 1, 2)
        self._wait_for_row_count("//tmp/q", 0, 5 - vital_consumer_offsets[0])
        self._wait_for_row_count("//tmp/q", 1, 6 - vital_consumer_offsets[1])
        vital_consumer_offsets[0] = 4
        vital_consumer_offsets[1] = 5

        # In mounted tablet trimming should work.
        self._advance_consumer("//tmp/c1", "//tmp/q", 2, 5)
        self._advance_consumer("//tmp/c2", "//tmp/q", 2, 2)
        vital_consumer_offsets[2] = 5
        self._wait_for_row_count("//tmp/q", 2, 7 - vital_consumer_offsets[2])

        # After unfreezing first two tablets they should be trimmed by vital consumer.
        sync_unfreeze_table("//tmp/q", first_tablet_index=0, last_tablet_index=1)
        self._wait_for_row_count("//tmp/q", 0, 5 - vital_consumer_offsets[0])
        self._wait_for_row_count("//tmp/q", 1, 6 - vital_consumer_offsets[1])


class TestMultipleAgents(TestQueueAgentBase):
    NUM_TEST_PARTITIONS = 3

    NUM_QUEUE_AGENTS = 5

    DELTA_QUEUE_AGENT_CONFIG = {
        "election_manager": {
            "transaction_timeout": 5000,
            "transaction_ping_period": 100,
            "lock_acquisition_period": 100,
            "leader_cache_update_period": 100,
        },
    }

    DELTA_QUEUE_AGENT_DYNAMIC_CONFIG = {
        "queue_agent": {
            "pass_period": 75,
            "controller": {
                "pass_period": 75,
                "enable_automatic_trimming": True,
            },
        },
        "cypress_synchronizer": {
            "policy": "watching",
            "pass_period": 75,
        },
    }

    @authors("max42", "nadya73")
    def test_leader_election(self):
        instances = self._wait_for_instances()
        # Will validate that exactly one cypress synchronizer and queue agent manager is leading.
        self._wait_for_elections(instances=instances)

        leader = CypressSynchronizerOrchid.get_leaders(instances=instances)[0]

        # Check that exactly one cypress synchronizer instance is performing passes.
        # Non-leading queue agent managers also increment their pass index.

        def validate_leader(leader, ignore_instances=()):
            wait(lambda: CypressSynchronizerOrchid(leader).get_pass_index() > 10)

            for instance in instances:
                if instance != leader and instance not in ignore_instances:
                    assert CypressSynchronizerOrchid(instance).get_pass_index() == -1

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
            remaining_instances = [instance for instance in instances if instance != prev_leader]

            self._wait_for_elections(instances=remaining_instances)
            leader = CypressSynchronizerOrchid.get_leaders(remaining_instances)[0]

            validate_leader(leader, ignore_instances=(prev_leader,))

    @staticmethod
    def _sync_mount_tables(paths, **kwargs):
        for path in paths:
            mount_table(path, **kwargs)
        for path in paths:
            wait_for_tablet_state(path, "mounted", **kwargs)

    @staticmethod
    def _add_registration(registrations, queue, consumer, vital=False):
        registrations.append({
            "queue_cluster": "primary",
            "queue_path": queue,
            "consumer_cluster": "primary",
            "consumer_path": consumer,
            "vital": vital,
        })

    @authors("achulkov2", "nadya73")
    @pytest.mark.parametrize("restart_victim_policy", ["heavy", "leader"])
    @pytest.mark.timeout(300)
    def test_sharding(self, restart_victim_policy):
        queue_count = 4
        consumer_count = 40

        queues = ["//tmp/q{}".format(i) for i in range(queue_count)]
        for queue in queues:
            self._create_queue(queue, mount=False)
        consumers = ["//tmp/c{}".format(i) for i in range(consumer_count)]

        registrations = []

        for i in range(len(consumers)):
            self._create_consumer(consumers[i], mount=False)
            # Each consumer is reading from 2 queues.
            self._add_registration(registrations, queues[i % len(queues)], consumers[i])
            self._add_registration(registrations, queues[(i + 1) % len(queues)], consumers[i])

        # Save test execution time by bulk performing bulk registrations.
        insert_rows("//sys/queue_agents/consumer_registrations", registrations)

        self._sync_mount_tables(queues + consumers)

        self._wait_for_global_sync()

        instances = ls("//sys/queue_agents/instances")
        mapping = list(select_rows("* from [//sys/queue_agents/queue_agent_object_mapping]"))
        objects_by_host = defaultdict(builtins.set)
        for row in mapping:
            objects_by_host[row["host"]].add(row["object"])
        # Assert that at most one of the queue agents didn't receive any objects to manage.
        assert len(objects_by_host) >= len(instances) - 1

        queue_agent_orchids = {instance: QueueAgentOrchid(instance) for instance in instances}
        for instance, orchid in queue_agent_orchids.items():
            instance_queues = orchid.get_queues()
            instance_consumers = orchid.get_consumers()
            all_instance_objects = instance_queues.keys() | instance_consumers.keys()
            assert all_instance_objects == objects_by_host[instance]

        def perform_checks(ignore_instances=()):
            # Balancing channel should send request to random instances.
            for queue in queues:
                assert len(get(queue + "/@queue_status/registrations")) == 20
            for consumer in consumers:
                consumer_registrations = get(consumer + "/@queue_consumer_status/registrations")
                assert len(consumer_registrations) == 2
                consumer_queues = get(consumer + "/@queue_consumer_status/queues")
                assert len(consumer_queues) == 2
                for registration in consumer_registrations:
                    assert "error" not in consumer_queues[registration["queue"]]
                    assert consumer_queues[registration["queue"]]["partition_count"] == 1

            for instance in instances:
                if instance in ignore_instances:
                    continue
                for queue in queues:
                    queue_orchid = queue_agent_orchids[instance].get_queue_orchid("primary:" + queue)
                    assert len(queue_orchid.get_status()["registrations"]) == 20
                for consumer in consumers:
                    consumer_orchid = queue_agent_orchids[instance].get_consumer_orchid("primary:" + consumer)
                    consumer_status = consumer_orchid.get_status()
                    consumer_queues = consumer_status["queues"]
                    assert len(consumer_queues) == 2
                    for registration in consumer_status["registrations"]:
                        assert "error" not in consumer_queues[registration["queue"]]
                        assert consumer_queues[registration["queue"]]["partition_count"] == 1

        perform_checks()

        victim = None
        if restart_victim_policy == "heavy":
            victim = max(objects_by_host, key=lambda key: len(objects_by_host[key]))
        elif restart_victim_policy == "leader":
            leaders = QueueAgentShardingManagerOrchid.get_leaders()
            assert len(leaders) == 1
            victim = leaders[0]
        else:
            assert False, "Incorrect restart victim policy"

        victim_index = get("//sys/queue_agents/instances/" + victim + "/@annotations/yt_env_index")

        with Restarter(self.Env, QUEUE_AGENTS_SERVICE, indexes=[victim_index]):
            remaining_instances = [instance for instance in instances if instance != victim]
            # Also waits and checks for a leader to be elected among the remaining peers.
            self._wait_for_global_sync(instances=remaining_instances)

            new_mapping = list(select_rows("* from [//sys/queue_agents/queue_agent_object_mapping]"))
            assert {row["object"] for row in new_mapping} == {row["object"] for row in mapping}
            hits = 0
            for row in new_mapping:
                if row["object"] in objects_by_host[row["host"]]:
                    hits += 1
                assert row["host"] in remaining_instances
            assert len({row["host"] for row in new_mapping}) >= len(remaining_instances) - 1
            assert hits >= len(new_mapping) // len(instances)

            perform_checks(ignore_instances=(victim,))

    @authors("achulkov2", "nadya73")
    @pytest.mark.timeout(120)
    def test_trimming_with_sharded_objects(self):
        consumer_count = 10

        queue = "//tmp/q"
        self._create_queue(queue, mount=False)
        consumers = ["//tmp/c{}".format(i) for i in range(consumer_count)]

        registrations = []

        for i in range(len(consumers)):
            self._create_consumer(consumers[i], mount=False)
            self._add_registration(registrations, queue, consumers[i], vital=True)

        # Save test execution time by bulk performing bulk registrations.
        insert_rows("//sys/queue_agents/consumer_registrations", registrations)

        self._sync_mount_tables([queue] + consumers)

        set(queue + "/@auto_trim_config", {"enable": True})

        self._wait_for_global_sync()

        insert_rows("//tmp/q", [{"data": "foo"}] * len(consumers))

        for i, consumer in enumerate(consumers):
            self._advance_consumer(consumer, queue, 0, i)

        # No trimming is performed when none of the consumers are vital, so we don't touch the last one.
        for i in range(len(consumers) - 1):
            register_queue_consumer(queue, consumers[i], vital=False)
            self._wait_for_row_count(queue, 0, len(consumers) - i - 1)

    @authors("achulkov2", "nadya73")
    def test_queue_agent_sharding_manager_alerts(self):
        leading_queue_agent_sharding_manager = QueueAgentShardingManagerOrchid.get_leaders()[0]

        self._drop_tables()

        for instance in sorted(self.INSTANCES, key=lambda host: host != leading_queue_agent_sharding_manager):
            queue_agent_sharding_manager_orchid = QueueAgentShardingManagerOrchid(instance)
            alert_manager_orchid = AlertManagerOrchid(instance)

            queue_agent_sharding_manager_orchid.wait_fresh_pass()

            if instance == leading_queue_agent_sharding_manager:
                wait(lambda: "queue_agent_sharding_manager_pass_failed" in alert_manager_orchid.get_alerts())
            else:
                wait(lambda: "queue_agent_sharding_manager_pass_failed" not in alert_manager_orchid.get_alerts())


class TestMasterIntegration(TestQueueAgentBase):
    DELTA_QUEUE_AGENT_CONFIG = {
        "election_manager": {
            "transaction_timeout": 5000,
            "transaction_ping_period": 100,
            "lock_acquisition_period": 100,
        },
    }

    DELTA_QUEUE_AGENT_DYNAMIC_CONFIG = {
        "cypress_synchronizer": {
            "policy": "polling",
        },
    }

    @authors("max42", "nadya73")
    def test_queue_attributes(self):
        self._create_queue("//tmp/q")
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

    @authors("achulkov2", "nadya73")
    def test_consumer_attributes(self):
        self._create_queue("//tmp/q")
        sync_mount_table("//tmp/q")
        self._create_registered_consumer("//tmp/c", "//tmp/q")
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
        wait(lambda: len(get("//tmp/c/@queue_consumer_status").get("queues", [])) == 1, ignore_exceptions=True)
        wait(lambda: get("//tmp/c/@queue_consumer_status").get("queues").get("primary://tmp/q").get("partition_count") == 1, ignore_exceptions=True)

        wait(lambda: len(get("//tmp/c/@queue_consumer_partitions").get("primary://tmp/q")) == 1, ignore_exceptions=True)

        # Check that consumer attributes are opaque.
        full_attributes = get("//tmp/c/@")
        for attribute in ("queue_consumer_status", "queue_consumer_partitions"):
            assert full_attributes[attribute] == YsonEntity()

    @authors("apachee")
    def test_producer_attributes(self):
        if self._should_skip_queue_producer_attributes_tests():
            return

        create("queue_producer", "//tmp/p")

        # TODO(apachee): Remove the following code after update to create queue_producer.
        assert not get("//tmp/p/@treat_as_queue_producer")
        set("//tmp/p/@treat_as_queue_producer", True)

        assert get("//tmp/p/@queue_agent_stage") == "production"

        set("//tmp/p/@queue_agent_stage", "testing")
        set("//tmp/p/@queue_agent_stage", "production")

        # NB(apachee): Since there are no orchid nodes for producers yet,
        # it should throw resolution error for path //queue_agent/producers.
        with raises_yt_error("Node /queue_agent has no child with key \"producers\""):
            get("//tmp/p/@queue_producer_status")
        with raises_yt_error("Node /queue_agent has no child with key \"producers\""):
            get("//tmp/p/@queue_producer_partitions")

        # Check attributes opaqueness.
        full_attributes = get("//tmp/p/@")
        for attribute in ("queue_producer_status", "queue_producer_partitions"):
            assert full_attributes[attribute] == YsonEntity()

    @authors("max42", "nadya73")
    def test_queue_agent_stage(self):
        create("table", "//tmp/q", attributes={"dynamic": True, "schema": [{"name": "data", "type": "string"}]})
        sync_mount_table("//tmp/q")

        assert get("//tmp/q/@queue_agent_stage") == "production"

        set("//sys/@config/queue_agent_server/default_queue_agent_stage", "another_default")
        assert get("//tmp/q/@queue_agent_stage") == "another_default"

        set("//tmp/q/@queue_agent_stage", "testing")
        assert get("//tmp/q/@queue_agent_stage") == "testing"

        remove("//tmp/q/@queue_agent_stage")
        assert get("//tmp/q/@queue_agent_stage") == "another_default"

        set("//tmp/q/@queue_agent_stage", "testing")
        assert get("//tmp/q/@queue_agent_stage") == "testing"

        # There is no queue agent with stage "testing", so accessing queue status would result in an error.
        with raises_yt_error('Queue agent stage "testing" is not found'):
            get("//tmp/q/@queue_status")

        tx = start_transaction()
        with raises_yt_error("Operation cannot be performed in transaction"):
            set("//tmp/q/@queue_agent_stage", "value_under_tx", tx=tx)

    @authors("max42", "nadya73")
    def test_non_queues(self):
        create("table", "//tmp/q_static",
               attributes={"schema": [{"name": "data", "type": "string"}]})
        create("table", "//tmp/q_sorted_dynamic",
               attributes={"dynamic": True, "schema": [{"name": "key", "type": "string", "sort_order": "ascending"},
                                                       {"name": "value", "type": "string"}]})
        create("replicated_table", "//tmp/q_sorted_replicated",
               attributes={"dynamic": True, "schema": [{"name": "data", "type": "string", "sort_order": "ascending"},
                                                       {"name": "value", "type": "string"}]})
        queue_attributes = ["queue_status", "queue_partitions"]

        result = get("//tmp", attributes=queue_attributes)
        for name in ("q_static", "q_sorted_dynamic", "q_sorted_replicated"):
            assert not result[name].attributes
            for attribute in queue_attributes:
                assert not exists("//tmp/" + name + "/@" + attribute)

        dynamic_table_attributes = ["queue_agent_stage"]
        result = get("//tmp", attributes=dynamic_table_attributes)
        for name in ("q_static",):
            assert not result[name].attributes
            for attribute in dynamic_table_attributes:
                assert not exists("//tmp/" + name + "/@" + attribute)

    def _set_and_assert_revision_change(self, path, attribute, value):
        old_revision = get(path + "/@attribute_revision")
        set(f"{path}/@{attribute}", value)
        assert get(f"{path}/@{attribute}") == value
        assert get(f"{path}/@attribute_revision") > old_revision

    @authors("achulkov2", "nadya73")
    def test_revision_changes_on_queue_attribute_change(self):
        create("table", "//tmp/q", attributes={"dynamic": True, "schema": [{"name": "data", "type": "string"}]})
        sync_mount_table("//tmp/q")

        self._set_and_assert_revision_change("//tmp/q", "queue_agent_stage", "testing")

    @authors("achulkov2", "nadya73")
    def test_revision_changes_on_consumer_attribute_change(self):
        self._create_queue("//tmp/q")
        sync_mount_table("//tmp/q")
        self._create_registered_consumer("//tmp/c", "//tmp/q")

        self._set_and_assert_revision_change("//tmp/c", "treat_as_queue_consumer", True)
        self._set_and_assert_revision_change("//tmp/c", "queue_agent_stage", "testing")
        # TODO(max42): this attribute is deprecated.
        self._set_and_assert_revision_change("//tmp/c", "vital_queue_consumer", True)
        self._set_and_assert_revision_change("//tmp/c", "target_queue", "haha:muahaha")

    @authors("apachee")
    def test_revision_changes_on_producer_attribute_change(self):
        if self._should_skip_queue_producer_attributes_tests():
            return

        create("queue_producer", "//tmp/p")

        # TODO(apachee): Remove the following code after update to create queue_producer.
        assert not get("//tmp/p/@treat_as_queue_producer")
        set("//tmp/p/@treat_as_queue_producer", True)

        self._set_and_assert_revision_change("//tmp/p", "treat_as_queue_producer", True)
        self._set_and_assert_revision_change("//tmp/p", "queue_agent_stage", "testing")


class TestCypressSynchronizerBase(TestQueueAgentBase):
    def _get_queue_name(self, name):
        return "//tmp/q-{}".format(name)

    def _get_consumer_name(self, name):
        return "//tmp/c-{}".format(name)

    LAST_REVISIONS = dict()
    QUEUE_REGISTRY = []
    CONSUMER_REGISTRY = []

    def teardown_method(self, method):
        self._drop_queues()
        self._drop_consumers()

        super(TestCypressSynchronizerBase, self).teardown_method(method)

    def _create_queue_object(self, path, initiate_helpers=True, **queue_attributes):
        update_inplace(queue_attributes, {"dynamic": True, "schema": [{"name": "useless", "type": "string"}]})
        create("table",
               path,
               attributes=queue_attributes,
               force=True)
        sync_mount_table(path)
        if initiate_helpers:
            self.QUEUE_REGISTRY.append(path)
            assert path not in self.LAST_REVISIONS
            self.LAST_REVISIONS[path] = 0

    def _create_and_register_queue(self, path, **queue_attributes):
        insert_rows("//sys/queue_agents/queues",
                    [{"cluster": "primary", "path": path, "row_revision": YsonUint64(0)}])
        self._create_queue_object(path, **queue_attributes)
        assert self.LAST_REVISIONS[path] == 0

    def _drop_queues(self):
        for queue in self.QUEUE_REGISTRY:
            remove(queue, force=True)
            del self.LAST_REVISIONS[queue]
        self.QUEUE_REGISTRY.clear()

    def _create_consumer_object(self, path, initiate_helpers=True):
        create("table",
               path,
               attributes={"dynamic": True,
                           "schema": [{"name": "useless", "type": "string", "sort_order": "ascending"},
                                      {"name": "also_useless", "type": "string"}],
                           "treat_as_queue_consumer": True},
               force=True)
        if initiate_helpers:
            self.CONSUMER_REGISTRY.append(path)
            assert path not in self.LAST_REVISIONS
            self.LAST_REVISIONS[path] = 0

    def _create_and_register_consumer(self, path):
        insert_rows("//sys/queue_agents/consumers",
                    [{"cluster": "primary", "path": path, "row_revision": YsonUint64(0)}])
        self._create_consumer_object(path)
        assert self.LAST_REVISIONS[path] == 0

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

    # Expected_synchronization_errors should contain functions that assert for an expected error YSON.
    def _get_queues_and_check_invariants(self, expected_count=None, expected_synchronization_errors=None):
        queues = select_rows("* from [//sys/queue_agents/queues]")
        if expected_count is not None:
            self._check_queue_count_and_column_counts(queues, expected_count)
        for queue in queues:
            if queue["synchronization_error"] != YsonEntity():
                synchronization_error = YtError.from_dict(queue["synchronization_error"])
                if expected_synchronization_errors is not None and queue["path"] in expected_synchronization_errors:
                    expected_synchronization_errors[queue["path"]](synchronization_error)
                    continue
                assert synchronization_error.code == 0
            assert queue["revision"] == get(queue["path"] + "/@attribute_revision")
            assert queue["object_id"] == get(queue["path"] + "/@id")
        return queues

    # Expected_synchronization_errors should contain functions that assert for an expected error YSON.
    def _get_consumers_and_check_invariants(self, expected_count=None, expected_synchronization_errors=None):
        consumers = select_rows("* from [//sys/queue_agents/consumers]")
        if expected_count is not None:
            self._check_consumer_count_and_column_counts(consumers, expected_count)
        for consumer in consumers:
            if consumer["synchronization_error"] != YsonEntity():
                synchronization_error = YtError.from_dict(consumer["synchronization_error"])
                if expected_synchronization_errors is not None and consumer["path"] in expected_synchronization_errors:
                    expected_synchronization_errors[consumer["path"]](synchronization_error)
                    continue
                assert synchronization_error.code == 0
            assert consumer["revision"] == get(consumer["path"] + "/@attribute_revision")
            assert consumer["treat_as_queue_consumer"] == get(consumer["path"] + "/@treat_as_queue_consumer")
            # Enclosing into a list is a workaround for storing YSON with top-level attributes.
            assert consumer["schema"] == [get(consumer["path"] + "/@schema")]
        return consumers

    def _assert_constant_revision(self, row):
        assert self.LAST_REVISIONS[row["path"]] == row["row_revision"]

    def _assert_increased_revision(self, row):
        assert self.LAST_REVISIONS[row["path"]] < row["row_revision"]
        self.LAST_REVISIONS[row["path"]] = row["row_revision"]


class TestCypressSynchronizerCommon(TestCypressSynchronizerBase):
    @authors("achulkov2", "nadya73")
    @pytest.mark.parametrize("policy", ["polling", "watching"])
    def test_alerts(self, policy):
        self._apply_dynamic_config_patch({
            "cypress_synchronizer": {
                "policy": policy
            }
        })

        orchid = CypressSynchronizerOrchid()
        alert_orchid = AlertManagerOrchid()

        q1 = self._get_queue_name("a")
        c1 = self._get_consumer_name("a")

        self._create_and_register_queue(q1)
        self._create_and_register_consumer(c1)
        orchid.wait_fresh_pass()

        queues = self._get_queues_and_check_invariants(expected_count=1)
        consumers = self._get_consumers_and_check_invariants(expected_count=1)
        for queue in queues:
            self._assert_increased_revision(queue)
        for consumer in consumers:
            self._assert_increased_revision(consumer)

        sync_unmount_table("//sys/queue_agents/queues")

        wait(lambda: "cypress_synchronizer_pass_failed" in alert_orchid.get_alerts())

    @authors("achulkov2", "nadya73")
    @pytest.mark.parametrize("policy", ["polling", "watching"])
    def test_no_alerts(self, policy):
        self._apply_dynamic_config_patch({
            "cypress_synchronizer": {
                "policy": policy
            }
        })

        orchid = CypressSynchronizerOrchid()
        alert_orchid = AlertManagerOrchid()

        q1 = self._get_queue_name("a")
        c1 = self._get_consumer_name("a")

        self._create_and_register_queue(q1)
        self._create_and_register_consumer(c1)
        orchid.wait_fresh_pass()

        queues = self._get_queues_and_check_invariants(expected_count=1)
        consumers = self._get_consumers_and_check_invariants(expected_count=1)
        for queue in queues:
            self._assert_increased_revision(queue)
        for consumer in consumers:
            self._assert_increased_revision(consumer)

        wait(lambda: not alert_orchid.get_alerts())

    @authors("cherepashka")
    @pytest.mark.parametrize("policy", ["polling", "watching"])
    def test_queue_recreation(self, policy):
        self._apply_dynamic_config_patch({
            "cypress_synchronizer": {
                "policy": policy
            }
        })

        orchid = CypressSynchronizerOrchid()

        self._create_and_register_queue("//tmp/q")
        orchid.wait_fresh_pass()

        old_queue = self._get_queues_and_check_invariants(expected_count=1)[0]

        remove("//tmp/q")
        self._wait_for_component_passes()

        self._create_queue_object("//tmp/q", initiate_helpers=False)
        orchid.wait_fresh_pass()

        new_queue = self._get_queues_and_check_invariants(expected_count=1)[0]

        assert new_queue["object_id"] != old_queue["object_id"]


# TODO(achulkov2): eliminate copy & paste between watching and polling versions below.


class TestCypressSynchronizerPolling(TestCypressSynchronizerBase):
    DELTA_QUEUE_AGENT_DYNAMIC_CONFIG = {
        "cypress_synchronizer": {
            "policy": "polling"
        },
    }

    @authors("achulkov2", "nadya73")
    def test_basic(self):
        orchid = CypressSynchronizerOrchid()

        q1 = self._get_queue_name("a")
        q2 = self._get_queue_name("b")
        c1 = self._get_consumer_name("a")
        c2 = self._get_consumer_name("b")

        self._create_and_register_queue(q1)
        self._create_and_register_consumer(c1)
        orchid.wait_fresh_pass()

        queues = self._get_queues_and_check_invariants(expected_count=1)
        consumers = self._get_consumers_and_check_invariants(expected_count=1)
        for queue in queues:
            self._assert_increased_revision(queue)
        for consumer in consumers:
            self._assert_increased_revision(consumer)

        self._create_and_register_queue(q2)
        self._create_and_register_consumer(c2)
        orchid.wait_fresh_pass()

        queues = self._get_queues_and_check_invariants(expected_count=2)
        consumers = self._get_consumers_and_check_invariants(expected_count=2)
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
                assert consumer["queue_agent_stage"] == "production"

        set(c2 + "/@queue_agent_stage", "foo")
        orchid.wait_fresh_pass()

        queues = self._get_queues_and_check_invariants(expected_count=2)
        consumers = self._get_consumers_and_check_invariants(expected_count=2)
        for queue in queues:
            self._assert_constant_revision(queue)
        for consumer in consumers:
            if consumer["path"] == c1:
                self._assert_constant_revision(consumer)
            elif consumer["path"] == c2:
                self._assert_increased_revision(consumer)
                assert consumer["queue_agent_stage"] == "foo"

        sync_unmount_table("//sys/queue_agents/queues")
        orchid.wait_fresh_pass()
        assert_yt_error(orchid.get_pass_error(), yt_error_codes.TabletNotMounted)

        sync_mount_table("//sys/queue_agents/queues")

        set(c2 + "/@queue_agent_stage", "bar")
        orchid.wait_fresh_pass()

        queues = self._get_queues_and_check_invariants(expected_count=2)
        consumers = self._get_consumers_and_check_invariants(expected_count=2)
        for queue in queues:
            self._assert_constant_revision(queue)
        for consumer in consumers:
            if consumer["path"] == c1:
                self._assert_constant_revision(consumer)
            elif consumer["path"] == c2:
                self._assert_increased_revision(consumer)
                assert consumer["queue_agent_stage"] == "bar"

    @authors("achulkov2", "nadya73")
    def test_content_change(self):
        orchid = CypressSynchronizerOrchid()

        q1 = self._get_queue_name("a")
        self._create_and_register_queue(q1, max_dynamic_store_row_count=1)

        orchid.wait_fresh_pass()

        queues = self._get_queues_and_check_invariants(expected_count=1)
        self._assert_increased_revision(queues[0])

        insert_rows(q1, [{"useless": "a"}])

        # Insert can fail while dynamic store is being flushed.
        def try_insert():
            insert_rows(q1, [{"useless": "a"}])
            return True

        wait(try_insert, ignore_exceptions=True)

        wait(lambda: len(get(q1 + "/@chunk_ids")) == 2)
        orchid.wait_fresh_pass()
        queues = self._get_queues_and_check_invariants(expected_count=1)
        # This checks that the revision doesn't change when dynamic stores are flushed.
        self._assert_constant_revision(queues[0])

    @authors("achulkov2", "nadya73")
    def test_synchronization_errors(self):
        orchid = CypressSynchronizerOrchid()

        q1 = self._get_queue_name("a")
        c1 = self._get_consumer_name("a")

        self._create_and_register_queue(q1)
        self._create_and_register_consumer(c1)

        orchid.wait_fresh_pass()

        queues = self._get_queues_and_check_invariants(expected_count=1)
        consumers = self._get_consumers_and_check_invariants(expected_count=1)
        for queue in queues:
            self._assert_increased_revision(queue)
        for consumer in consumers:
            self._assert_increased_revision(consumer)

        remove(q1)
        remove(c1)

        orchid.wait_fresh_pass()

        queues = self._get_queues_and_check_invariants(expected_count=1, expected_synchronization_errors={
            q1: lambda error: assert_yt_error(error, yt_error_codes.ResolveErrorCode),
        })
        consumers = self._get_consumers_and_check_invariants(expected_count=1, expected_synchronization_errors={
            c1: lambda error: assert_yt_error(error, yt_error_codes.ResolveErrorCode),
        })

        for queue in queues:
            self._assert_increased_revision(queue)
        for consumer in consumers:
            self._assert_increased_revision(consumer)

        self._create_queue_object(q1, initiate_helpers=False)
        self._create_consumer_object(c1, initiate_helpers=False)

        orchid.wait_fresh_pass()

        queues = self._get_queues_and_check_invariants(expected_count=1)
        consumers = self._get_consumers_and_check_invariants(expected_count=1)
        for queue in queues:
            self._assert_increased_revision(queue)
        for consumer in consumers:
            self._assert_increased_revision(consumer)


class TestCypressSynchronizerWatching(TestCypressSynchronizerBase):
    DELTA_QUEUE_AGENT_DYNAMIC_CONFIG = {
        "cypress_synchronizer": {
            "policy": "watching",
        },
    }

    @authors("achulkov2", "nadya73")
    def test_basic(self):
        orchid = CypressSynchronizerOrchid()

        q1 = self._get_queue_name("a")
        q2 = self._get_queue_name("b")
        c1 = self._get_consumer_name("a")
        c2 = self._get_consumer_name("b")

        self._create_queue_object(q1)
        self._create_consumer_object(c1)
        orchid.wait_fresh_pass()

        queues = self._get_queues_and_check_invariants(expected_count=1)
        consumers = self._get_consumers_and_check_invariants(expected_count=1)
        for queue in queues:
            self._assert_increased_revision(queue)
        for consumer in consumers:
            self._assert_increased_revision(consumer)

        self._create_queue_object(q2)
        self._create_consumer_object(c2)
        orchid.wait_fresh_pass()

        queues = self._get_queues_and_check_invariants(expected_count=2)
        consumers = self._get_consumers_and_check_invariants(expected_count=2)
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
                assert consumer["queue_agent_stage"] == "production"

        set(c2 + "/@queue_agent_stage", "foo")
        orchid.wait_fresh_pass()

        queues = self._get_queues_and_check_invariants(expected_count=2)
        consumers = self._get_consumers_and_check_invariants(expected_count=2)
        for queue in queues:
            self._assert_constant_revision(queue)
        for consumer in consumers:
            if consumer["path"] == c1:
                self._assert_constant_revision(consumer)
            elif consumer["path"] == c2:
                self._assert_increased_revision(consumer)
                assert consumer["queue_agent_stage"] == "foo"

        sync_unmount_table("//sys/queue_agents/queues")
        orchid.wait_fresh_pass()
        assert_yt_error(orchid.get_pass_error(), yt_error_codes.TabletNotMounted)

        sync_mount_table("//sys/queue_agents/queues")
        set(c2 + "/@queue_agent_stage", "bar")
        orchid.wait_fresh_pass()

        queues = self._get_queues_and_check_invariants(expected_count=2)
        consumers = self._get_consumers_and_check_invariants(expected_count=2)
        for queue in queues:
            self._assert_constant_revision(queue)
        for consumer in consumers:
            if consumer["path"] == c1:
                self._assert_constant_revision(consumer)
            elif consumer["path"] == c2:
                self._assert_increased_revision(consumer)
                assert consumer["queue_agent_stage"] == "bar"

        set(c1 + "/@treat_as_queue_consumer", False)
        orchid.wait_fresh_pass()

        self._get_consumers_and_check_invariants(expected_count=1)

        remove(q2)
        orchid.wait_fresh_pass()

        self._get_queues_and_check_invariants(expected_count=1)

    # TODO(achulkov2): Unify this test with its copy once https://a.yandex-team.ru/review/2527564 is merged.
    @authors("achulkov2", "nadya73")
    def test_content_change(self):
        orchid = CypressSynchronizerOrchid()

        q1 = self._get_queue_name("a")
        self._create_queue_object(q1, max_dynamic_store_row_count=1)

        orchid.wait_fresh_pass()

        queues = self._get_queues_and_check_invariants(expected_count=1)
        self._assert_increased_revision(queues[0])

        insert_rows(q1, [{"useless": "a"}])

        # Insert can fail while dynamic store is being flushed.
        def try_insert():
            insert_rows(q1, [{"useless": "a"}])
            return True

        wait(try_insert, ignore_exceptions=True)

        wait(lambda: len(get(q1 + "/@chunk_ids")) == 2)
        orchid.wait_fresh_pass()
        queues = self._get_queues_and_check_invariants(expected_count=1)
        # This checks that the revision doesn't change when dynamic stores are flushed.
        self._assert_constant_revision(queues[0])

    @authors("achulkov2", "nadya73")
    def test_synchronization_errors(self):
        orchid = CypressSynchronizerOrchid()

        q1 = self._get_queue_name("a")
        c1 = self._get_consumer_name("a")

        self._create_queue_object(q1)
        self._create_consumer_object(c1)

        orchid.wait_fresh_pass()

        queues = self._get_queues_and_check_invariants(expected_count=1)
        consumers = self._get_consumers_and_check_invariants(expected_count=1)
        for queue in queues:
            self._assert_increased_revision(queue)
        for consumer in consumers:
            self._assert_increased_revision(consumer)

        # TODO(max42): come up with some checks here.


class TestMultiClusterReplicatedTableObjects(TestQueueAgentBase, ReplicatedObjectBase):
    DELTA_QUEUE_AGENT_DYNAMIC_CONFIG = {
        "cypress_synchronizer": {
            "policy": "watching",
            "clusters": ["primary", "remote_0", "remote_1"],
            "poll_replicated_objects": True,
            "write_replicated_table_mapping": True,
        },
        "queue_agent": {
            "handle_replicated_objects": True,
            "controller": {
                "enable_automatic_trimming": True,
            }
        }
    }

    QUEUE_SCHEMA = [
        {"name": "$timestamp", "type": "uint64"},
        {"name": "data", "type": "string"},
    ]

    NUM_REMOTE_CLUSTERS = 2
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

    def _create_cells(self):
        for driver in self._get_drivers():
            sync_create_cells(1, driver=driver)

    @staticmethod
    def _wait_for_replicated_queue_row_count(replicas, row_count, partition_index=0):
        def ok():
            for replica in replicas:
                path = replica["replica_path"]
                cluster = replica["cluster_name"]
                replica_row_count = len(select_rows(
                    f"* from [{path}] where [$tablet_index] = {partition_index}",
                    driver=get_driver(cluster=cluster)))
                if replica_row_count != row_count:
                    print_debug(f"Expected {row_count} rows in replica {cluster}:{path}, but found {replica_row_count}")
                    return False
            return True

        wait(ok)

    @staticmethod
    def _flush_replicated_queue(replicas):
        for replica in replicas:
            sync_flush_table(replica["replica_path"], driver=get_driver(cluster=replica["cluster_name"]))

    @staticmethod
    def _assert_queue_partition(partition, lower_row_index, upper_row_index):
        assert partition["lower_row_index"] == lower_row_index
        assert partition["upper_row_index"] == upper_row_index
        assert partition["available_row_count"] == upper_row_index - lower_row_index

    @staticmethod
    def _assert_consumer_partition(partition, next_row_index, unread_row_count):
        assert partition["next_row_index"] == next_row_index
        assert partition["unread_row_count"] == unread_row_count

    def _create_chaos_replicated_queue(self, path):
        queue_queue_replica_path = f"{path}_queue"
        chaos_replicated_queue_replicas = [
            {"cluster_name": "primary", "content_type": "queue", "mode": "async", "enabled": True,
             "replica_path": f"{queue_queue_replica_path}"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True,
             "replica_path": f"{queue_queue_replica_path}"},
            {"cluster_name": "remote_1", "content_type": "queue", "mode": "sync", "enabled": True,
             "replica_path": f"{queue_queue_replica_path}"},
        ]
        self._create_chaos_replicated_table_base(
            path,
            chaos_replicated_queue_replicas,
            self.QUEUE_SCHEMA)
        return chaos_replicated_queue_replicas

    def _create_replicated_queue(self, path):
        queue_replica_path = f"{path}_replica"
        replicated_queue_replicas = [
            {"cluster_name": "primary", "mode": "async", "enabled": True,
             "replica_path": f"{queue_replica_path}"},
            {"cluster_name": "remote_0", "mode": "sync", "enabled": True,
             "replica_path": f"{queue_replica_path}"},
            {"cluster_name": "remote_1", "mode": "sync", "enabled": True,
             "replica_path": f"{queue_replica_path}"},
        ]
        self._create_replicated_table_base(
            path,
            replicated_queue_replicas,
            self.QUEUE_SCHEMA)
        return replicated_queue_replicas

    def _create_chaos_replicated_consumer(self, path):
        consumer_data_replica_path = f"{path}_data"
        consumer_queue_replica_path = f"{path}_queue"
        chaos_replicated_consumer_replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True,
             "replica_path": f"{consumer_data_replica_path}"},
            {"cluster_name": "primary", "content_type": "queue", "mode": "async", "enabled": True,
             "replica_path": f"{consumer_queue_replica_path}"},
            {"cluster_name": "remote_0", "content_type": "data", "mode": "async", "enabled": True,
             "replica_path": f"{consumer_data_replica_path}"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True,
             "replica_path": f"{consumer_queue_replica_path}"},
            {"cluster_name": "remote_1", "content_type": "data", "mode": "sync", "enabled": True,
             "replica_path": f"{consumer_data_replica_path}"},
            {"cluster_name": "remote_1", "content_type": "queue", "mode": "sync", "enabled": True,
             "replica_path": f"{consumer_queue_replica_path}"},
        ]
        self._create_chaos_replicated_table_base(
            path,
            chaos_replicated_consumer_replicas,
            init_queue_agent_state.CONSUMER_OBJECT_TABLE_SCHEMA,
            replicated_table_attributes={"treat_as_queue_consumer": True})
        return chaos_replicated_consumer_replicas

    def _create_replicated_consumer(self, path):
        consumer_replica_path = f"{path}_replica"
        replicated_consumer_replicas = [
            {"cluster_name": "primary", "mode": "sync", "enabled": True,
             "replica_path": f"{consumer_replica_path}"},
            {"cluster_name": "remote_0", "mode": "async", "enabled": True,
             "replica_path": f"{consumer_replica_path}"},
            {"cluster_name": "remote_1", "mode": "sync", "enabled": True,
             "replica_path": f"{consumer_replica_path}"},
        ]
        self._create_replicated_table_base(
            path,
            replicated_consumer_replicas,
            init_queue_agent_state.CONSUMER_OBJECT_TABLE_SCHEMA,
            replicated_table_attributes_patch={"treat_as_queue_consumer": True})
        return replicated_consumer_replicas

    def _create_chaos_queue_consumer_pair(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()
        set("//sys/chaos_cell_bundles/c/@metadata_cell_id", cell_id)

        chaos_replicated_queue = "//tmp/crq"
        chaos_replicated_consumer = "//tmp/crc"

        return (chaos_replicated_queue, self._create_chaos_replicated_queue(chaos_replicated_queue),
                chaos_replicated_consumer, self._create_chaos_replicated_consumer(chaos_replicated_consumer))

    def _create_replicated_queue_consumer_pair(self):
        self._create_cells()

        replicated_queue = "//tmp/rq"
        replicated_consumer = "//tmp/rc"

        return (replicated_queue, self._create_replicated_queue(replicated_queue),
                replicated_consumer, self._create_replicated_consumer(replicated_consumer))

    def _create_chaos_producer(self, path):
        producer_data_replica_path = f"{path}_data"
        producer_queue_replica_path = f"{path}_queue"
        chaos_replicated_producer_replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True,
             "replica_path": f"{producer_data_replica_path}"},
            {"cluster_name": "primary", "content_type": "queue", "mode": "async", "enabled": True,
             "replica_path": f"{producer_queue_replica_path}"},
            {"cluster_name": "remote_0", "content_type": "data", "mode": "async", "enabled": True,
             "replica_path": f"{producer_data_replica_path}"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True,
             "replica_path": f"{producer_queue_replica_path}"},
            {"cluster_name": "remote_1", "content_type": "data", "mode": "sync", "enabled": True,
             "replica_path": f"{producer_data_replica_path}"},
            {"cluster_name": "remote_1", "content_type": "queue", "mode": "sync", "enabled": True,
             "replica_path": f"{producer_queue_replica_path}"},
        ]
        self._create_chaos_replicated_table_base(
            path,
            chaos_replicated_producer_replicas,
            init_queue_agent_state.PRODUCER_OBJECT_TABLE_SCHEMA,
            replicated_table_attributes={"treat_as_queue_producer": True})
        return chaos_replicated_producer_replicas

    @authors("achulkov2", "nadya73")
    @pytest.mark.parametrize("create_queue_consumer_pair", [
        _create_chaos_queue_consumer_pair,
        _create_replicated_queue_consumer_pair,
    ])
    def test_replicated_trim(self, create_queue_consumer_pair):
        queue, queue_replicas, consumer, consumer_replicas = create_queue_consumer_pair(self)

        queue_agent_orchid = QueueAgentOrchid()
        cypress_synchronizer_orchid = CypressSynchronizerOrchid()

        # Register queues and consumers for cypress synchronizer to see them.
        register_queue_consumer(queue, consumer, vital=True)

        self._wait_for_component_passes()

        queue_orchid = queue_agent_orchid.get_queue_orchid(f"primary:{queue}")
        consumer_orchid = queue_agent_orchid.get_consumer_orchid(f"primary:{consumer}")

        queue_orchid.wait_fresh_pass()
        consumer_orchid.wait_fresh_pass()

        queue_orchid.get_status()
        consumer_orchid.get_status()

        queue_partitions = queue_orchid.get_partitions()
        self._assert_queue_partition(queue_partitions[0], 0, 0)
        consumer_partitions = consumer_orchid.get_partitions()
        self._assert_consumer_partition(consumer_partitions[f"primary:{queue}"][0],
                                        next_row_index=0, unread_row_count=0)

        insert_rows(queue, [{"data": "foo", "$tablet_index": 0}] * 3)

        queue_orchid.wait_fresh_pass()
        consumer_orchid.wait_fresh_pass()

        queue_partitions = queue_orchid.get_partitions()
        self._assert_queue_partition(queue_partitions[0], 0, 3)

        consumer_partitions = consumer_orchid.get_partitions()
        self._assert_consumer_partition(consumer_partitions[f"primary:{queue}"][0],
                                        next_row_index=0, unread_row_count=3)

        advance_consumer(consumer, queue, partition_index=0, old_offset=None, new_offset=1)

        consumer_orchid.wait_fresh_pass()

        consumer_partitions = consumer_orchid.get_partitions()
        self._assert_consumer_partition(consumer_partitions[f"primary:{queue}"][0],
                                        next_row_index=1, unread_row_count=2)

        set(f"{queue}/@auto_trim_config", {"enable": True})
        cypress_synchronizer_orchid.wait_fresh_pass()

        self._flush_replicated_queue(queue_replicas)
        insert_rows(queue, [{"data": "bar", "$tablet_index": 0}] * 2)

        self._wait_for_replicated_queue_row_count(queue_replicas, 4)

        queue_orchid.wait_fresh_pass()
        consumer_orchid.wait_fresh_pass()

        queue_partitions = queue_orchid.get_partitions()
        self._assert_queue_partition(queue_partitions[0], 1, 5)

        consumer_partitions = consumer_orchid.get_partitions()
        self._assert_consumer_partition(consumer_partitions[f"primary:{queue}"][0],
                                        next_row_index=1, unread_row_count=4)

        unregister_queue_consumer(queue, consumer)

    @authors("nadya73")
    def test_chaos_queue_agent_stage(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()
        set("//sys/chaos_cell_bundles/c/@metadata_cell_id", cell_id)

        chaos_replicated_queue = "//tmp/crq"
        self._create_chaos_replicated_queue(chaos_replicated_queue)

        assert get("//tmp/crq/@queue_agent_stage") == "production"

        set("//sys/@config/queue_agent_server/default_queue_agent_stage", "another_default")
        assert get("//tmp/crq/@queue_agent_stage") == "another_default"

        set("//tmp/crq/@queue_agent_stage", "testing")
        assert get("//tmp/crq/@queue_agent_stage") == "testing"

        remove("//tmp/crq/@queue_agent_stage")
        assert get("//tmp/crq/@queue_agent_stage") == "another_default"

        set("//tmp/crq/@queue_agent_stage", "testing")
        assert get("//tmp/crq/@queue_agent_stage") == "testing"

        # There is no queue agent with stage "testing", so accessing queue status would result in an error.
        with raises_yt_error('Queue agent stage "testing" is not found'):
            get("//tmp/crq/@queue_status")

        tx = start_transaction()
        with raises_yt_error("Operation cannot be performed in transaction"):
            set("//tmp/crq/@queue_agent_stage", "value_under_tx", tx=tx)

    def _add_chaos_queue_registration(self, queue):
        insert_rows("//sys/queue_agents/consumer_registrations", [{
            "queue_cluster": "primary",
            "queue_path": queue,
            "consumer_cluster": "primary",
            "consumer_path": queue,
            "vital": True,
        }])

    @authors("nadya73")
    def test_chaos_queue_attributes(self):
        cell_id = self._sync_create_chaos_bundle_and_cell()
        set("//sys/chaos_cell_bundles/c/@metadata_cell_id", cell_id)

        chaos_replicated_queue = "//tmp/crq"
        self._create_chaos_replicated_queue(chaos_replicated_queue)
        self._add_chaos_queue_registration(chaos_replicated_queue)

        assert get("//tmp/crq/@queue_agent_stage") == "production"

        # Wait for queue status to become available.
        wait(lambda: get("//tmp/crq/@queue_status/partition_count") == 1, ignore_exceptions=True)

        # Check the zeroth partition.
        def check_partition():
            partitions = get("//tmp/crq/@queue_partitions")
            if len(partitions) == 1:
                assert partitions[0]["available_row_count"] == 0
                return True
            return False

        wait(check_partition)

        # Check that queue attributes are opaque.
        full_attributes = get("//tmp/crq/@")
        for attribute in ("queue_status", "queue_partitions"):
            assert full_attributes[attribute] == YsonEntity()

    @authors("nadya73")
    def test_chaos_consumer_attributes(self):
        chaos_queue, _, chaos_consumer, _ = self._create_chaos_queue_consumer_pair()

        register_queue_consumer(chaos_queue, chaos_consumer, vital=True)

        assert get(f"{chaos_consumer}/@queue_agent_stage") == "production"

        # Check that queue_agent_stage is writable.
        set(f"{chaos_consumer}/@queue_agent_stage", "testing")
        set(f"{chaos_consumer}/@queue_agent_stage", "production")

        # Wait for consumer status to become available.
        wait(lambda: len(get(f"{chaos_consumer}/@queue_consumer_status").get("queues", [])) == 1, ignore_exceptions=True)

        wait(lambda: get(f"{chaos_consumer}/@queue_consumer_partitions").get(f"primary:{chaos_queue}")[0].get("unread_row_count") == 0, ignore_exceptions=True)

        insert_rows(f"{chaos_queue}", [{"$tablet_index": 0, "data": "foo"}] * 5)

        wait(lambda: get(f"{chaos_consumer}/@queue_consumer_partitions").get(f"primary:{chaos_queue}")[0].get("unread_row_count") == 5, ignore_exceptions=True)

        # Check that consumer attributes are opaque.
        full_attributes = get(f"{chaos_consumer}/@")
        for attribute in ("queue_consumer_status", "queue_consumer_partitions"):
            assert full_attributes[attribute] == YsonEntity()

    @authors("apachee")
    def test_chaos_producer_attributes(self):
        if self._should_skip_queue_producer_attributes_tests():
            return

        cell_id = self._sync_create_chaos_bundle_and_cell()
        set("//sys/chaos_cell_bundles/c/@metadata_cell_id", cell_id)

        chaos_producer = "//tmp/crt_producer"
        self._create_chaos_producer(chaos_producer)

        assert get(f"{chaos_producer}/@queue_agent_stage") == "production"

        # Check that queue_agent_stage is writable.
        set(f"{chaos_producer}/@queue_agent_stage", "testing")
        set(f"{chaos_producer}/@queue_agent_stage", "production")

        with raises_yt_error("Node /queue_agent has no child with key \"producers\""):
            get(f"{chaos_producer}/@queue_producer_status")
        with raises_yt_error("Node /queue_agent has no child with key \"producers\""):
            get(f"{chaos_producer}/@queue_producer_partitions")


class TestReplicatedTableObjects(TestQueueAgentBase, ReplicatedObjectBase):
    DELTA_QUEUE_AGENT_DYNAMIC_CONFIG = {
        "cypress_synchronizer": {
            "policy": "watching",
            "poll_replicated_objects": True,
            "write_replicated_table_mapping": True,
        },
        "queue_agent": {
            "handle_replicated_objects": True,
        }
    }

    QUEUE_SCHEMA = [{"name": "data", "type": "string"}]

    @staticmethod
    def _assert_internal_queues_are(expected_queues):
        queues = builtins.set(map(itemgetter("path"), select_rows("[path] from [//sys/queue_agents/queues]")))
        assert queues == builtins.set(expected_queues)

    @staticmethod
    def _assert_internal_consumers_are(expected_consumers):
        consumers = builtins.set(map(itemgetter("path"), select_rows("[path] from [//sys/queue_agents/consumers]")))
        assert consumers == builtins.set(expected_consumers)

    @authors("achulkov2", "nadya73")
    def test_basic(self):
        replicated_queue = "//tmp/replicated_queue"
        replicated_consumer = "//tmp/replicated_consumer"
        chaos_replicated_queue = "//tmp/chaos_replicated_queue"
        chaos_replicated_consumer = "//tmp/chaos_replicated_consumer"

        cell_id = self._sync_create_chaos_bundle_and_cell()
        set("//sys/chaos_cell_bundles/c/@metadata_cell_id", cell_id)

        queue_agent_orchid = QueueAgentOrchid()
        cypress_synchronizer_orchid = CypressSynchronizerOrchid()

        # Create replicated queue.
        replicated_queue_replicas = [
            {"cluster_name": "primary", "replica_path": f"{replicated_queue}_replica_0"},
            {"cluster_name": "primary", "replica_path": f"{replicated_queue}_replica_1"}
        ]
        replicated_queue_replica_ids = self._create_replicated_table_base(
            replicated_queue, replicated_queue_replicas, schema=self.QUEUE_SCHEMA, create_replica_tables=False)

        # Create replicated consumer.
        replicated_consumer_replicas = [
            {"cluster_name": "primary", "replica_path": f"{replicated_consumer}_replica",
             "mode": "sync", "state": "enabled"},
        ]
        replicated_consumer_replica_ids = self._create_replicated_table_base(
            replicated_consumer,
            replicated_consumer_replicas,
            schema=init_queue_agent_state.CONSUMER_OBJECT_TABLE_SCHEMA,
            replicated_table_attributes_patch={"treat_as_queue_consumer": True},
            create_replica_tables=False)

        # Create chaos replicated queue.
        create("chaos_replicated_table", chaos_replicated_queue, attributes={
            "chaos_cell_bundle": "c",
            "schema": self.QUEUE_SCHEMA,
        })
        queue_replication_card_id = get(f"{chaos_replicated_queue}/@replication_card_id")

        # Create chaos replicated consumer.
        consumer_data_replica_path = f"{chaos_replicated_consumer}_data"
        consumer_queue_replica_path = f"{chaos_replicated_consumer}_queue"
        chaos_replicated_consumer_replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True,
             "replica_path": f"{consumer_data_replica_path}_0"},
            {"cluster_name": "primary", "content_type": "queue", "mode": "async", "enabled": True,
             "replica_path": f"{consumer_queue_replica_path}_0"},
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True,
             "replica_path": f"{consumer_data_replica_path}_1"},
            {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "enabled": True,
             "replica_path": f"{consumer_queue_replica_path}_1"},
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True,
             "replica_path": f"{consumer_data_replica_path}_2"},
            {"cluster_name": "primary", "content_type": "queue", "mode": "sync", "enabled": True,
             "replica_path": f"{consumer_queue_replica_path}_2"},
        ]
        chaos_replicated_consumer_replica_ids, consumer_replication_card_id = self._create_chaos_replicated_table_base(
            chaos_replicated_consumer,
            chaos_replicated_consumer_replicas,
            init_queue_agent_state.CONSUMER_OBJECT_TABLE_SCHEMA)

        # Register queues and consumers for cypress synchronizer to see them.
        register_queue_consumer(replicated_queue, replicated_consumer, vital=False)
        register_queue_consumer(chaos_replicated_queue, chaos_replicated_consumer, vital=False)

        self._wait_for_component_passes()

        # TODO(achulkov2): Check these statuses once replicated queue controllers are implemented.
        queue_agent_orchid.get_queue_orchid(f"primary:{replicated_queue}").get_status()
        queue_agent_orchid.get_queue_orchid(f"primary:{chaos_replicated_queue}").get_status()

        # TODO(achulkov2): Check these statuses once replicated consumer controllers are implemented.
        queue_agent_orchid.get_consumer_orchid(f"primary:{replicated_consumer}").get_status()
        queue_agent_orchid.get_consumer_orchid(f"primary:{chaos_replicated_consumer}").get_status()

        self._assert_internal_queues_are({replicated_queue, chaos_replicated_queue})
        self._assert_internal_consumers_are({replicated_consumer, chaos_replicated_consumer})

        def transform_enabled_flag(replicas):
            for replica in replicas:
                enabled = replica.get("enabled", False)
                if "enabled" in replica:
                    del replica["enabled"]

                replica["state"] = "enabled" if enabled else "disabled"

        def build_rt_meta(replica_ids, replicas):
            transform_enabled_flag(replicas)
            return {"replicated_table_meta": {"replicas": dict(zip(replica_ids, replicas))}}

        def build_crt_meta(replication_card_id, replica_ids, replicas):
            transform_enabled_flag(replicas)
            return {"chaos_replicated_table_meta": {
                "replication_card_id": replication_card_id,
                "replicas": dict(zip(replica_ids, replicas))
            }}

        replicated_table_mapping = list(select_rows("* from [//sys/queue_agents/replicated_table_mapping]"))
        assert {r["path"]: r["meta"] for r in replicated_table_mapping} == {
            replicated_queue: build_rt_meta(replicated_queue_replica_ids, replicated_queue_replicas),
            replicated_consumer: build_rt_meta(replicated_consumer_replica_ids, replicated_consumer_replicas),
            chaos_replicated_queue: build_crt_meta(queue_replication_card_id, [], []),
            chaos_replicated_consumer: build_crt_meta(
                consumer_replication_card_id,
                chaos_replicated_consumer_replica_ids,
                chaos_replicated_consumer_replicas),
        }

        unregister_queue_consumer(chaos_replicated_queue, chaos_replicated_consumer)
        remove(chaos_replicated_consumer)
        remove(chaos_replicated_queue)

        cypress_synchronizer_orchid.wait_fresh_pass()

        self._assert_internal_queues_are({replicated_queue})
        self._assert_internal_consumers_are({replicated_consumer})


class TestDynamicConfig(TestQueueAgentBase):
    @authors("achulkov2", "nadya73")
    def test_basic(self):
        orchid = CypressSynchronizerOrchid()
        orchid.wait_fresh_pass()

        self._apply_dynamic_config_patch({
            "cypress_synchronizer": {
                "enable": False
            }
        })

        pass_index = orchid.get_pass_index()
        time.sleep(3)
        assert orchid.get_pass_index() == pass_index

        self._apply_dynamic_config_patch({
            "cypress_synchronizer": {
                "enable": True
            }
        })

        orchid.wait_fresh_pass()


class TestQueueStaticExportBase(TestQueueAgentBase):
    NUM_SECONDARY_MASTER_CELLS = 2
    DELTA_QUEUE_AGENT_DYNAMIC_CONFIG = {
        "queue_agent": {
            "controller": {
                "enable_queue_static_export": True,
            },
        },
        "cypress_synchronizer": {
            "policy": "watching",
            "enable": True,
        },
    }

    @staticmethod
    def _create_export_destination(export_directory, queue_id):
        create("map_node", export_directory, recursive=True, ignore_existing=True)
        set(f"{export_directory}/@queue_static_export_destination", {
            "originating_queue_id": queue_id,
        })

    @staticmethod
    def remove_export_destination(export_directory):
        def try_remove():
            remove(export_directory)
            return True

        wait(try_remove, ignore_exceptions=True)

    @staticmethod
    # NB: The last two options should be used carefully: currently they strictly check that all timestamps are within [ts - period, ts], which might not generally be the case.
    def _check_export(export_directory, expected_data, queue_path=None, use_upper_bound_for_table_names=False, check_lower_bound=False):
        export_fragments = [name for name in sorted(ls(export_directory)) if f"{export_directory}/{name}" != queue_path]
        assert len(export_fragments) == len(expected_data)

        queue_id = get(f"{export_directory}/@queue_static_export_destination/originating_queue_id")

        export_progress = get(f"{export_directory}/@queue_static_export_progress")

        last_fragment_unix_tx, last_fragment_export_period = map(int, export_fragments[-1].split("-"))
        assert export_progress["last_exported_fragment_unix_ts"] == last_fragment_unix_tx + (0 if use_upper_bound_for_table_names else last_fragment_export_period)

        queue_schema_id = get(f"#{queue_id}/@schema_id")
        queue_schema = get(f"#{queue_id}/@schema")
        queue_native_cell_tag = get(f"#{queue_id}/@native_cell_tag")

        max_timestamp = 0
        total_row_count = 0

        for fragment_index, fragment_name in enumerate(export_fragments):
            fragment_table_path = f"{export_directory}/{fragment_name}"

            if fragment_table_path == queue_path:
                continue

            fragment_unix_ts, fragment_export_period = map(int, fragment_name.split("-"))

            fragment_native_cell_tag = get(f"{fragment_table_path}/@native_cell_tag")

            # If both tables are on the same native cell, we use schema ids in upload.
            if fragment_native_cell_tag == queue_native_cell_tag:
                fragment_schema_id = get(f"{fragment_table_path}/@schema_id")
                assert fragment_schema_id == queue_schema_id

            fragment_schema = get(f"{fragment_table_path}/@schema")
            assert fragment_schema == queue_schema

            rows = list(read_table(fragment_table_path))
            assert list(map(itemgetter("data"), rows)) == expected_data[fragment_index]

            ts_lower_bound = fragment_unix_ts - (fragment_export_period if use_upper_bound_for_table_names else 0)
            ts_upper_bound = fragment_unix_ts + (0 if use_upper_bound_for_table_names else fragment_export_period)

            for row in rows:
                ts = row["$timestamp"]
                max_timestamp = max(ts, max_timestamp)

                assert ts >> 30 <= ts_upper_bound

                if check_lower_bound:
                    assert ts_lower_bound <= ts >> 30

            total_row_count += len(rows)

        assert max(map(itemgetter("max_timestamp"), export_progress["tablets"].values())) == max_timestamp
        assert sum(map(itemgetter("row_count"), export_progress["tablets"].values())) == total_row_count

    # NB: We rely on manual flushing in almost all of the static export tests. Override if necessary.
    def _create_queue(self, *args, **kwargs):
        return super()._create_queue(*args, dynamic_store_auto_flush_period=kwargs.pop("dynamic_store_auto_flush_period", YsonEntity()), **kwargs)

    # Sleeps until next instant which is at the specified offset (in seconds) in the specified periodic cycle.
    def _sleep_until_next_export_instant(self, period, offset=0.0):
        now = time.time()
        last_export_time = int(now) // period * period
        next_instant = last_export_time + offset
        if next_instant < now:
            next_instant += period
        assert next_instant >= now
        time.sleep(next_instant - now)
        return next_instant


class TestQueueStaticExport(TestQueueStaticExportBase):
    @authors("cherepashka", "achulkov2", "nadya73")
    @pytest.mark.parametrize("queue_external_cell_tag", [10, 11, 12])
    def test_multicell_export(self, queue_external_cell_tag):
        if getattr(self, "ENABLE_TMP_PORTAL", False) and queue_external_cell_tag == 10:
            pytest.skip()

        queue_agent_orchid = QueueAgentOrchid()
        cypress_orchid = CypressSynchronizerOrchid()

        _, queue_id = self._create_queue("//tmp/q", external_cell_tag=queue_external_cell_tag)

        export_dir = "//tmp/export"
        self._create_export_destination(export_dir, queue_id)

        insert_rows("//tmp/q", [{"$tablet_index": 0, "data": "bar"}] * 7)
        self._flush_table("//tmp/q")

        set("//tmp/q/@static_export_config", {
            "default": {
                "export_directory": export_dir,
                "export_period": 3 * 1000,
            }
        })

        cypress_orchid.wait_fresh_pass()
        queue_agent_orchid.wait_fresh_pass()
        queue_agent_orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()

        wait(lambda: len(ls(export_dir)) == 1)
        self._check_export(export_dir, [["bar"] * 7])

        insert_rows("//tmp/q", [{"$tablet_index": 0, "data": "foo"}] * 5)
        # NB: No flush.
        queue_agent_orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()

        time.sleep(5)
        self._check_export(export_dir, [["bar"] * 7])

        self._flush_table("//tmp/q")
        wait(lambda: len(ls(export_dir)) == 2)
        self._check_export(export_dir, [["bar"] * 7, ["foo"] * 5])

        self.remove_export_destination(export_dir)

    # TODO(achulkov2): Add test that replicated/chaos queues are not exported.

    @authors("cherepashka", "achulkov2", "nadya73")
    def test_export_order(self):
        _, queue_id = self._create_queue("//tmp/q", partition_count=3)

        export_dir = "//tmp/export"
        self._create_export_destination(export_dir, queue_id)

        insert_rows("//tmp/q", [{"$tablet_index": 2, "data": "third chunk"}] * 2)
        self._flush_table("//tmp/q")

        insert_rows("//tmp/q", [{"$tablet_index": 1, "data": "second chunk"}] * 2)
        self._flush_table("//tmp/q")

        insert_rows("//tmp/q", [{"$tablet_index": 0, "data": "first chunk"}] * 2)
        self._flush_table("//tmp/q")

        set("//tmp/q/@static_export_config", {
            "default": {
                "export_directory": export_dir,
                "export_period": 3 * 1000,
            }
        })

        wait(lambda: len(ls(export_dir)) == 1)

        expected_data = [["first chunk"] * 2 + ["second chunk"] * 2 + ["third chunk"] * 2]
        self._check_export(export_dir, expected_data)

        self.remove_export_destination(export_dir)

    @authors("cherepashka", "achulkov2", "nadya73")
    def test_export_to_the_same_folder(self):
        export_dir = "//tmp/export"
        create("map_node", export_dir)

        queue_path = f"{export_dir}/q"
        _, queue_id = self._create_queue(queue_path)

        self._create_export_destination(export_dir, queue_id)

        insert_rows(queue_path, [{"$tablet_index": 0, "data": "foo"}] * 6)
        self._flush_table(queue_path)

        set(f"{queue_path}/@static_export_config", {
            "default": {
                "export_directory": export_dir,
                "export_period": 3 * 1000,
            }
        })

        wait(lambda: len(ls(export_dir)) == 2)

        self._check_export(export_dir, [["foo"] * 6], queue_path=queue_path)

        self.remove_export_destination(export_dir)

    @authors("nadya73")
    def test_several_exports(self):
        queue_agent_orchid = QueueAgentOrchid()
        cypress_orchid = CypressSynchronizerOrchid()

        export_dir_1 = "//tmp/export1"
        export_dir_2 = "//tmp/export2"
        export_dir_3 = "//tmp/export3"
        create("map_node", export_dir_1)
        create("map_node", export_dir_2)
        create("map_node", export_dir_3)

        queue_path = "//tmp/q"
        _, queue_id = self._create_queue(queue_path)

        self._create_export_destination(export_dir_1, queue_id)
        self._create_export_destination(export_dir_2, queue_id)
        self._create_export_destination(export_dir_3, queue_id)

        set(f"{queue_path}/@static_export_config", {
            "first": {
                "export_directory": export_dir_1,
                "export_period": 1 * 1000,
            },
            "second": {
                "export_directory": export_dir_2,
                "export_period": 2 * 1000,
            },
        })

        cypress_orchid.wait_fresh_pass()
        queue_agent_orchid.wait_fresh_pass()

        insert_rows(queue_path, [{"$tablet_index": 0, "data": "foo"}] * 6)
        self._flush_table(queue_path)

        wait(lambda: len(ls(export_dir_1)) == 1)
        wait(lambda: len(ls(export_dir_2)) == 1)

        insert_rows(queue_path, [{"$tablet_index": 0, "data": "bar"}] * 6)
        self._flush_table(queue_path)

        wait(lambda: len(ls(export_dir_1)) == 2)
        wait(lambda: len(ls(export_dir_2)) == 2)

        expected = [["foo"] * 6] + [["bar"] * 6]
        self._check_export(export_dir_1, expected)
        self._check_export(export_dir_2, expected)

        set(f"{queue_path}/@static_export_config", {
            "second": {
                "export_directory": export_dir_2,
                "export_period": 2 * 1000,
            },
        })

        cypress_orchid.wait_fresh_pass()
        queue_agent_orchid.wait_fresh_pass()

        insert_rows(queue_path, [{"$tablet_index": 0, "data": "abc"}] * 6)
        self._flush_table(queue_path)

        wait(lambda: len(ls(export_dir_1)) == 2)
        wait(lambda: len(ls(export_dir_2)) == 3)

        self._check_export(export_dir_1, expected)
        self._check_export(export_dir_2, expected + [["abc"] * 6])

        set(f"{queue_path}/@static_export_config", {
            "second": {
                "export_directory": export_dir_2,
                "export_period": 2 * 1000,
            },
            "third": {
                "export_directory": export_dir_3,
                "export_period": 3 * 1000,
            },
        })

        cypress_orchid.wait_fresh_pass()
        queue_agent_orchid.wait_fresh_pass()

        wait(lambda: len(ls(export_dir_3)) == 1)
        expected_third = [["foo"] * 6 + ["bar"] * 6 + ["abc"] * 6]
        self._check_export(export_dir_3, expected_third)

        insert_rows(queue_path, [{"$tablet_index": 0, "data": "def"}] * 6)
        self._flush_table(queue_path)

        cypress_orchid.wait_fresh_pass()
        queue_agent_orchid.wait_fresh_pass()

        wait(lambda: len(ls(export_dir_2)) == 4)
        wait(lambda: len(ls(export_dir_3)) == 2)

        expected_second = expected + [["abc"] * 6] + [["def"] * 6]
        expected_third = expected_third + [["def"] * 6]

        self._check_export(export_dir_2, expected_second)
        self._check_export(export_dir_3, expected_third)

        self.remove_export_destination(export_dir_1)
        self.remove_export_destination(export_dir_2)
        self.remove_export_destination(export_dir_3)

    @authors("cherepashka", "achulkov2", "nadya73")
    def test_wrong_originating_queue(self):
        queue_agent_orchid = QueueAgentOrchid()
        cypress_orchid = CypressSynchronizerOrchid()

        self._create_queue("//tmp/q")
        _, queue_id = self._create_queue("//tmp/q1")

        export_dir = "//tmp/export"
        self._create_export_destination(export_dir, queue_id)

        insert_rows("//tmp/q", [{"$tablet_index": 0, "data": "some data for export"}] * 2)
        self._flush_table("//tmp/q")

        set("//tmp/q/@static_export_config", {
            "default": {
                "export_directory": export_dir,
                "export_period": 2 * 1000,
            }
        })

        cypress_orchid.wait_fresh_pass()
        queue_agent_orchid.wait_fresh_pass()
        queue_agent_orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()

        #  We perform exports with a period of 2 seconds, so we wait for 4.
        time.sleep(4)

        # The export directory is not configured to accept exports from //tmp/q, so none should have been performed.
        assert len(ls(export_dir)) == 0

        alerts = queue_agent_orchid.get_queue_orchid("primary://tmp/q").get_alerts()
        alerts.assert_matching("queue_agent_queue_controller_static_export_failed", text="does not match queue id", attributes={"export_name": "default"})
        assert alerts.get_alert_count() == 1

        self.remove_export_destination(export_dir)

    @authors("achulkov2", "nadya73")
    @pytest.mark.parametrize("use_upper_bound_for_table_names", [False, True])
    def test_table_name_formatting(self, use_upper_bound_for_table_names):
        export_dir = "//tmp/export"
        _, queue_id = self._create_queue("//tmp/q")

        self._create_export_destination(export_dir, queue_id)

        start = datetime.datetime.utcnow()

        insert_rows("//tmp/q", [{"$tablet_index": 0, "data": "foo"}] * 6)
        self._flush_table("//tmp/q")

        set("//tmp/q/@static_export_config", {
            "default": {
                "export_directory": export_dir,
                "export_period": 3 * 1000,
                "output_table_name_pattern": "%ISO-period-is-%PERIOD-fmt-%Y.%d.%m.%H.%M.%S",
                "use_upper_bound_for_table_names": use_upper_bound_for_table_names,
            }
        })

        wait(lambda: len(ls(export_dir)) == 1)
        output_table_name = ls(export_dir)[0]

        end = datetime.datetime.utcnow()

        def fmt_time(dt):
            return f"{dt.isoformat(timespec='seconds')}Z-period-is-3-fmt-{dt.strftime('%Y.%d.%m.%H.%M.%S')}"

        assert fmt_time(start) <= output_table_name <= fmt_time(end)

        self.remove_export_destination(export_dir)

    @authors("achulkov2", "nadya73")
    def test_lower_bound_naming(self):
        _, queue_id = self._create_queue("//tmp/q")

        export_dir = "//tmp/export"
        self._create_export_destination(export_dir, queue_id)

        set("//tmp/q/@static_export_config", {
            "default": {
                "export_directory": export_dir,
                "export_period": 5 * 1000,
                "use_upper_bound_for_table_names": False,
            }
        })

        # This way we assure that we write the rows at the beginning of the period, so that all rows are physically written and flushed before the next export instant arrives.
        mid_export = self._sleep_until_next_export_instant(period=5, offset=0.5)
        insert_rows("//tmp/q", [{"$tablet_index": 0, "data": "foo"}] * 2)
        self._flush_table("//tmp/q")

        next_export = self._sleep_until_next_export_instant(period=5)
        # Flush should be fast enough. Increase period if this turns out to be flaky.
        assert next_export - mid_export <= 5

        wait(lambda: len(ls(export_dir)) == 1)
        # Given the constraints above, we check that all timestamps lie in [ts, ts + period], where ts is the timestamp in the name of the output table.
        self._check_export(export_dir, [["foo"] * 2], use_upper_bound_for_table_names=False, check_lower_bound=True)

        self.remove_export_destination(export_dir)

    @authors("achulkov2", "nadya73")
    def test_export_ttl(self):
        _, queue_id = self._create_queue("//tmp/q")

        export_dir = "//tmp/export"
        self._create_export_destination(export_dir, queue_id)

        set("//tmp/q/@static_export_config", {
            "default": {
                "export_directory": export_dir,
                "export_period": 3 * 1000,
                "export_ttl":  3 * 1000,
            }
        })

        insert_rows("//tmp/q", [{"$tablet_index": 0, "data": "foo"}] * 2)
        self._flush_table("//tmp/q")

        # Something should be exported.
        wait(lambda: len(ls(export_dir)) == 1)

        # And then deleted after 3 seconds (sleeping for 4 just in case).
        time.sleep(4)
        assert len(ls(export_dir)) == 0

        self.remove_export_destination(export_dir)


class TestAutomaticTrimmingWithExports(TestQueueStaticExportBase):
    DELTA_QUEUE_AGENT_DYNAMIC_CONFIG = {
        "queue_agent": {
            "controller": {
                "pass_period": 75,
                "enable_queue_static_export": True,
                "enable_automatic_trimming": True,
            },
        },
        "cypress_synchronizer": {
            "policy": "watching",
        },
    }

    @authors("apachee")
    @pytest.mark.timeout(200)
    def test_basic(self):
        queue_agent_orchid = QueueAgentOrchid()

        _, queue_id = self._create_queue("//tmp/q", partition_count=2)
        export_dir = "//tmp/export"
        self._create_export_destination(export_dir, queue_id)

        set("//tmp/q/@auto_trim_config", {"enable": True})
        export_period_seconds = 12
        set("//tmp/q/@static_export_config", {
            "default": {
                "export_directory": export_dir,
                "export_period": export_period_seconds * 1000,
            }
        })

        # After this we have 4.5 second window to test that no trimmming
        # is performed until queue is exported.
        self._wait_for_component_passes()
        queue_agent_orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()
        self._sleep_until_next_export_instant(period=export_period_seconds, offset=0.5)
        assert len(ls(export_dir)) == 0

        insert_rows("//tmp/q", [{"$tablet_index": 1, "data": "second"}])
        self._flush_table("//tmp/q")

        insert_rows("//tmp/q", [{"$tablet_index": 0, "data": "first"}])
        self._flush_table("//tmp/q")

        # Wait for trim.
        queue_agent_orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()

        # Nothing should be trimmed, because new rows haven't been exported.
        self._wait_for_row_count("//tmp/q", 0, 1)
        self._wait_for_row_count("//tmp/q", 1, 1)

        # Wait for export of the new rows.
        wait(lambda: len(ls(export_dir)) == 1)

        # Data should now be trimmed.
        self._check_export(export_dir, [["first", "second"]])
        self._wait_for_row_count("//tmp/q", 0, 0)
        self._wait_for_row_count("//tmp/q", 1, 0)

        self.remove_export_destination(export_dir)

    @authors("apachee")
    @pytest.mark.timeout(200)
    def test_vital_consumers_and_exports(self):
        queue_agent_orchid = QueueAgentOrchid()

        _, queue_id = self._create_queue("//tmp/q", partition_count=2)
        self._create_registered_consumer("//tmp/c", "//tmp/q", True)
        export_dir = "//tmp/export"
        self._create_export_destination(export_dir, queue_id)

        set("//tmp/q/@auto_trim_config", {"enable": True})
        export_period_seconds = 12
        set("//tmp/q/@static_export_config", {
            "default": {
                "export_directory": export_dir,
                "export_period": export_period_seconds * 1000,
            }
        })

        # After this we have 4.5 second window to test that no trimmming
        # is performed until queue is exported.
        self._wait_for_component_passes()
        queue_agent_orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()
        queue_agent_orchid.get_consumer_orchid("primary://tmp/c").wait_fresh_pass()
        self._sleep_until_next_export_instant(period=export_period_seconds, offset=0.5)
        assert len(ls(export_dir)) == 0

        insert_rows("//tmp/q", [{"$tablet_index": 1, "data": "second"}])
        self._flush_table("//tmp/q")

        insert_rows("//tmp/q", [{"$tablet_index": 0, "data": "first"}])
        self._flush_table("//tmp/q")

        queue_agent_orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()

        # Nothing should be trimmed at this point.
        self._wait_for_row_count("//tmp/q", 0, 1)
        self._wait_for_row_count("//tmp/q", 1, 1)

        self._advance_consumer("//tmp/c", "//tmp/q", 0, 1)
        self._advance_consumer("//tmp/c", "//tmp/q", 1, 1)

        # Since export is still in progress, no rows should be trimmed.
        self._wait_for_row_count("//tmp/q", 0, 1)
        self._wait_for_row_count("//tmp/q", 1, 1)

        # Wait for export of the new rows.
        wait(lambda: len(ls(export_dir)) == 1)

        # Data should now be trimmed.
        self._check_export(export_dir, [["first", "second"]])
        self._wait_for_row_count("//tmp/q", 0, 0)
        self._wait_for_row_count("//tmp/q", 1, 0)

        # Now check that trim waits for consumers, when exports are ahead.
        insert_rows("//tmp/q", [{"$tablet_index": 1, "data": "second"}])
        self._flush_table("//tmp/q")

        insert_rows("//tmp/q", [{"$tablet_index": 0, "data": "first"}])
        self._flush_table("//tmp/q")

        # Wait for export of the new rows and trim iteration.
        wait(lambda: len(ls(export_dir)) == 2)

        # Since consumers hasn't advanced, no rows should be trimmed.
        self._check_export(export_dir, [["first", "second"]] * 2)
        self._wait_for_row_count("//tmp/q", 0, 1)
        self._wait_for_row_count("//tmp/q", 1, 1)

        # Advance consumers.
        self._advance_consumer("//tmp/c", "//tmp/q", 0, 2)
        self._advance_consumer("//tmp/c", "//tmp/q", 1, 2)

        # Everything should be trimmed.
        self._wait_for_row_count("//tmp/q", 0, 0)
        self._wait_for_row_count("//tmp/q", 1, 0)

        self.remove_export_destination(export_dir)


class TestQueueStaticExportPortals(TestQueueStaticExport):
    ENABLE_TMP_PORTAL = True

    @authors("achulkov2", "nadya73")
    def test_different_native_cells(self):
        _, queue_id = self._create_queue("//portals/q")

        export_dir = "//tmp/export"
        self._create_export_destination(export_dir, queue_id)

        assert get(f"{export_dir}/@native_cell_tag") != get("//portals/q/@native_cell_tag")

        insert_rows("//portals/q", [{"$tablet_index": 0, "data": "foo"}] * 6)
        self._flush_table("//portals/q")

        set("//portals/q/@static_export_config", {
            "default": {
                "export_directory": export_dir,
                "export_period": 3 * 1000,
            }
        })

        wait(lambda: len(ls(export_dir)) == 1)
        self._check_export(export_dir, [["foo"] * 6])

        self.remove_export_destination(export_dir)


class TestObjectAlertCollection(TestQueueStaticExportBase):
    DELTA_QUEUE_AGENT_DYNAMIC_CONFIG = {
        "queue_agent": {
            "controller": {
                "enable_automatic_trimming": True,
                "enable_queue_static_export": True,
            },
        },
        "cypress_synchronizer": {
            "policy": "watching",
        },
    }

    @authors("achulkov2", "nadya73")
    def test_alert_combinations(self):
        queue_agent_orchid = QueueAgentOrchid()

        queue_path = "//tmp/q"

        export_dir = "//tmp/export"

        _, queue_id = self._create_queue(queue_path)

        self._create_export_destination(export_dir, queue_id)

        # Using the same export directory for different exports
        # should trigger static export config misconfiguration alert, and then
        # it would also trigger trim alert.
        set(f"{queue_path}/@static_export_config", {
            "first": {
                "export_directory": export_dir,
                "export_period": 1 * 1000,
            },
            "second": {
                "export_directory": export_dir,
                "export_period": 1 * 1000,
            },
        })
        set(f"{queue_path}/@auto_trim_config", {"enable": True})

        self._wait_for_component_passes()
        queue_agent_orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()

        alerts = queue_agent_orchid.get_queue_orchid("primary://tmp/q").get_alerts()
        alerts.assert_matching("queue_agent_queue_controller_trim_failed", text="Incorrect queue exports")
        alerts.assert_matching("queue_agent_queue_controller_static_export_misconfiguration", text="Static export config check failed")
        assert alerts.get_alert_count() == 2

        self.remove_export_destination(export_dir)
