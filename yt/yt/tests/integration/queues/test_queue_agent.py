from yt_env_setup import (YTEnvSetup, Restarter, QUEUE_AGENTS_SERVICE)
from yt_chaos_test_base import ChaosTestBase

from yt_commands import (authors, get, set, ls, wait, assert_yt_error, create, sync_mount_table, sync_create_cells,
                         insert_rows, delete_rows, remove, raises_yt_error, exists, start_transaction, select_rows,
                         sync_unmount_table, sync_reshard_table, trim_rows, print_debug, alter_table,
                         register_queue_consumer, unregister_queue_consumer, mount_table, wait_for_tablet_state,
                         sync_freeze_table, sync_unfreeze_table, create_table_replica, alter_table_replica)

from yt.common import YtError, update_inplace, update

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

from yt.wrapper.ypath import escape_ypath_literal

import yt.environment.init_queue_agent_state as init_queue_agent_state

##################################################################


class OrchidBase:
    ENTITY_NAME = None

    def __init__(self, agent_id=None):
        if agent_id is None:
            agent_ids = ls("//sys/queue_agents/instances", verbose=False)
            assert len(agent_ids) == 1
            agent_id = agent_ids[0]

        self.agent_id = agent_id

    def queue_agent_orchid_path(self):
        return f"//sys/queue_agents/instances/{self.agent_id}/orchid"

    def orchid_path(self):
        assert self.ENTITY_NAME is not None
        return f"{self.queue_agent_orchid_path()}/{self.ENTITY_NAME}"


class OrchidSingleLeaderMixin:
    ENTITY_NAME = None

    @classmethod
    def get_leaders(cls, instances=None):
        assert cls.ENTITY_NAME is not None

        if instances is None:
            instances = ls("//sys/queue_agents/instances", verbose=False)

        leaders = []
        for instance in instances:
            if get(f"//sys/queue_agents/instances/{instance}/orchid/{cls.ENTITY_NAME}/active"):
                leaders.append(instance)
        return leaders

    @classmethod
    def leader_orchid(cls, instances=None):
        assert cls.ENTITY_NAME is not None

        if instances is None:
            instances = ls("//sys/queue_agents/instances", verbose=False)

        leaders = cls.get_leaders(instances=instances)
        assert len(leaders) <= 1, f"More than one leading {cls.ENTITY_NAME}"
        assert len(leaders) > 0, f"No leading {cls.ENTITY_NAME} found"
        return cls(leaders[0])


class OrchidWithRegularPasses(OrchidBase):
    ENTITY_NAME = None

    def get_pass_index(self):
        return get(f"{self.orchid_path()}/pass_index")

    def get_pass_error(self):
        return YtError.from_dict(get(f"{self.orchid_path()}/pass_error"))

    def wait_fresh_pass(self):
        pass_index = self.get_pass_index()
        wait(lambda: self.get_pass_index() >= pass_index + 2, sleep_backoff=0.15)

    def validate_no_pass_error(self):
        error = self.get_pass_error()
        if error.code != 0:
            raise error


class ObjectOrchid(OrchidWithRegularPasses):
    OBJECT_TYPE = None

    def __init__(self, object_ref, agent_id=None):
        super(ObjectOrchid, self).__init__(agent_id)
        self.object_ref = object_ref

    def orchid_path(self):
        assert self.OBJECT_TYPE is not None
        return f"{self.queue_agent_orchid_path()}/queue_agent/" \
               f"{self.OBJECT_TYPE}s/{escape_ypath_literal(self.object_ref)}"

    def get(self):
        return get(self.orchid_path())

    def get_status(self):
        return get(f"{self.orchid_path()}/status")

    def get_partitions(self):
        return get(f"{self.orchid_path()}/partitions")

    def get_row(self):
        return get(f"{self.orchid_path()}/row")


class QueueOrchid(ObjectOrchid):
    OBJECT_TYPE = "queue"

    def get_queue(self):
        result = self.get()
        return result["status"], result["partitions"]


class ConsumerOrchid(ObjectOrchid):
    OBJECT_TYPE = "consumer"

    def get_consumer(self):
        result = self.get()
        return result["status"], result["partitions"]

    def get_subconsumer(self, queue_ref):
        status, partitions = self.get_consumer()
        return status["queues"][queue_ref], partitions[queue_ref]


class QueueAgentOrchid(OrchidWithRegularPasses):
    ENTITY_NAME = "queue_agent"

    def get_queues(self):
        self.wait_fresh_pass()
        return get(self.orchid_path() + "/queues")

    def get_consumers(self):
        self.wait_fresh_pass()
        return get(self.orchid_path() + "/consumers")

    def get_queue_orchid(self, queue_ref):
        return QueueOrchid(queue_ref, self.agent_id)

    def get_queue_orchids(self):
        return [self.get_queue_orchid(queue) for queue in self.get_queues()]

    def get_consumer_orchid(self, consumer_ref):
        return ConsumerOrchid(consumer_ref, self.agent_id)

    def get_consumer_orchids(self):
        return [self.get_consumer_orchid(consumer) for consumer in self.get_consumers()]


class AlertManagerOrchid(OrchidBase):
    ENTITY_NAME = "alert_manager"

    def get_alerts(self):
        return get("{}/alerts".format(self.queue_agent_orchid_path()))


class QueueAgentShardingManagerOrchid(OrchidWithRegularPasses, OrchidSingleLeaderMixin):
    ENTITY_NAME = "queue_agent_sharding_manager"


class CypressSynchronizerOrchid(OrchidWithRegularPasses, OrchidSingleLeaderMixin):
    ENTITY_NAME = "cypress_synchronizer"


class TestQueueAgentBase(YTEnvSetup):
    NUM_QUEUE_AGENTS = 1
    NUM_DISCOVERY_SERVERS = 3

    USE_DYNAMIC_TABLES = True

    BASE_QUEUE_AGENT_DYNAMIC_CONFIG = {
        "queue_agent_sharding_manager": {
            "pass_period": 100,
        },
        "queue_agent": {
            "pass_period": 100,
            "controller": {
                "pass_period": 100,
            },
        },
        "cypress_synchronizer": {
            # List of clusters for the watching policy.
            "clusters": ["primary"],
            "pass_period": 100,
        },
    }

    INSTANCES = None

    @classmethod
    def modify_queue_agent_config(cls, config):
        update_inplace(config, {
            "cluster_connection": {
                # Disable cache.
                "table_mount_cache": {
                    "expire_after_successful_update_time": 0,
                    "expire_after_failed_update_time": 0,
                    "expire_after_access_time": 0,
                    "refresh_time": 0,
                },
                "transaction_manager": {
                    "retry_attempts": 1,
                },
            },
        })

    @classmethod
    def setup_class(cls):
        super(TestQueueAgentBase, cls).setup_class()

        queue_agent_config = cls.Env._cluster_configuration["queue_agent"][0]
        cls.root_path = queue_agent_config.get("root", "//sys/queue_agents")
        cls.config_path = queue_agent_config.get("dynamic_config_path", cls.root_path + "/config")

        if not exists(cls.config_path):
            create("document", cls.config_path, attributes={"value": cls.BASE_QUEUE_AGENT_DYNAMIC_CONFIG})

        cls._apply_dynamic_config_patch(getattr(cls, "DELTA_QUEUE_AGENT_DYNAMIC_CONFIG", dict()))

        cls.INSTANCES = cls._wait_for_instances()
        cls._wait_for_elections()

    def setup_method(self, method):
        super(TestQueueAgentBase, self).setup_method(method)

        self._prepare_tables()

    def teardown_method(self, method):
        self._drop_tables()

        super(TestQueueAgentBase, self).teardown_method(method)

    @classmethod
    def _apply_dynamic_config_patch(cls, patch):
        config = get(cls.config_path)
        update_inplace(config, patch)
        set(cls.config_path, config)

        instances = ls(cls.root_path + "/instances")

        cypress_config_base = {
            "queue_agent": {
                "controller": {
                    "trimming_period": YsonEntity(),
                }
            }
        }

        def config_updated_on_all_instances():
            for instance in instances:
                effective_config = get(
                    "{}/instances/{}/orchid/dynamic_config_manager/effective_config".format(cls.root_path, instance))
                effective_config = update(cypress_config_base, effective_config)
                if update(effective_config, config) != effective_config:
                    return False
            return True

        wait(config_updated_on_all_instances)

    def _prepare_tables(self, queue_table_schema=None, consumer_table_schema=None, **kwargs):
        sync_create_cells(1)
        self.client = self.Env.create_native_client()
        init_queue_agent_state.create_tables(
            self.client,
            queue_table_schema=queue_table_schema,
            consumer_table_schema=consumer_table_schema,
            create_registration_table=True,
            create_replicated_table_mapping_table=True,
            **kwargs)

    def _drop_tables(self):
        init_queue_agent_state.delete_tables(self.client)

    def _create_queue(self, path, partition_count=1, enable_timestamp_column=True,
                      enable_cumulative_data_weight_column=True, mount=True, **kwargs):
        schema = [{"name": "data", "type": "string"}]
        if enable_timestamp_column:
            schema += [{"name": "$timestamp", "type": "uint64"}]
        if enable_cumulative_data_weight_column:
            schema += [{"name": "$cumulative_data_weight", "type": "int64"}]
        attributes = {
            "dynamic": True,
            "schema": schema,
        }
        attributes.update(kwargs)
        create("table", path, attributes=attributes)
        if partition_count != 1:
            sync_reshard_table(path, partition_count)
        if mount:
            sync_mount_table(path)

        return schema

    def _create_consumer(self, path, mount=True, **kwargs):
        attributes = {
            "dynamic": True,
            "schema": init_queue_agent_state.CONSUMER_OBJECT_TABLE_SCHEMA,
            "treat_as_queue_consumer": True,
        }
        attributes.update(kwargs)
        create("table", path, attributes=attributes)
        if mount:
            sync_mount_table(path)

    def _create_registered_consumer(self, consumer_path, queue_path, vital=False, **kwargs):
        self._create_consumer(consumer_path, **kwargs)
        register_queue_consumer(queue_path, consumer_path, vital=vital)

    def _advance_consumer(self, consumer_path, queue_path, partition_index, offset):
        self._advance_consumers(consumer_path, queue_path, {partition_index: offset})

    def _advance_consumers(self, consumer_path, queue_path, partition_index_to_offset):
        insert_rows(consumer_path, [{
            "queue_cluster": "primary",
            "queue_path": queue_path,
            "partition_index": partition_index,
            "offset": offset,
        } for partition_index, offset in partition_index_to_offset.items()])

    def _flush_table(self, path, first_tablet_index=None, last_tablet_index=None):
        sync_freeze_table(path, first_tablet_index=first_tablet_index, last_tablet_index=last_tablet_index)
        sync_unfreeze_table(path, first_tablet_index=first_tablet_index, last_tablet_index=last_tablet_index)

    @staticmethod
    def _wait_for_row_count(path, tablet_index, count):
        wait(lambda: len(select_rows(f"* from [{path}] where [$tablet_index] = {tablet_index}")) == count)

    @staticmethod
    def _wait_for_min_row_index(path, tablet_index, row_index):
        def check():
            rows = select_rows(f"* from [{path}] where [$tablet_index] = {tablet_index}")
            return rows and rows[0]["$row_index"] >= row_index

        wait(check)

    @staticmethod
    def wait_fresh_pass(orchids):
        pass_indexes = [orchid.get_pass_index() for orchid in orchids]
        ok = [False] * len(pass_indexes)

        def all_passes():
            for i, orchid, pass_index in zip(range(len(pass_indexes)), orchids, pass_indexes):
                if ok[i]:
                    continue
                if orchid.get_pass_index() < pass_index + 2:
                    return False
                ok[i] = True

            return True

        wait(all_passes, sleep_backoff=0.15)

    # Waits for all queue agent instances to register themselves.
    @classmethod
    def _wait_for_instances(cls):
        def all_instances_up():
            return len(ls("//sys/queue_agents/instances", verbose=False)) == getattr(cls, "NUM_QUEUE_AGENTS")

        wait(all_instances_up, sleep_backoff=0.15)

        return ls("//sys/queue_agents/instances", verbose=False)

    # Waits for a single leading cypress synchronizer and queue agent manager to be elected.
    @classmethod
    def _wait_for_elections(cls, instances=None):
        if instances is None:
            instances = cls.INSTANCES
        assert instances is not None

        def cypress_synchronizer_elected():
            return len(CypressSynchronizerOrchid.get_leaders(instances=instances)) == 1

        def queue_agent_sharding_manager_elected():
            return len(QueueAgentShardingManagerOrchid.get_leaders(instances=instances)) == 1

        wait(lambda: cypress_synchronizer_elected(), sleep_backoff=0.15)
        wait(lambda: queue_agent_sharding_manager_elected(), sleep_backoff=0.15)

    @classmethod
    def _wait_for_discovery(cls, instances=None):
        if instances is None:
            instances = cls.INSTANCES
        assert instances is not None

        instance_set = builtins.set(instances)

        discovery_instances = ls("//sys/discovery_servers")

        def discovery_membership_updated():
            for discovery_instance in discovery_instances:
                members = ls(f"//sys/discovery_servers/{discovery_instance}"
                             f"/orchid/discovery_server/queue_agents/@members")
                member_set = builtins.set(members)
                if member_set != instance_set:
                    print_debug(f"Discovery group membership is stale: expected {instance_set}, found {member_set}")
                    return False

            return True

        wait(discovery_membership_updated)

    # Waits for a complete pass by all queue agent components.
    # More specifically, it performs the following (in order):
    #     1. Waits for a complete pass by the leading cypress synchronizer.
    #     2. Waits for a complete pass by the leading queue agent manager.
    #     3. Waits for a complete pass by all queue agents.
    @classmethod
    def _wait_for_component_passes(cls, instances=None, skip_cypress_synchronizer=False):
        if instances is None:
            instances = cls.INSTANCES
        assert instances is not None

        leading_cypress_synchronizer_orchid = CypressSynchronizerOrchid.leader_orchid(instances=instances)
        leading_queue_agent_sharding_manager = QueueAgentShardingManagerOrchid.leader_orchid(instances=instances)
        queue_agent_orchids = [QueueAgentOrchid(instance) for instance in instances]

        if not skip_cypress_synchronizer:
            leading_cypress_synchronizer_orchid.wait_fresh_pass()

        cls._wait_for_discovery(instances=instances)
        leading_queue_agent_sharding_manager.wait_fresh_pass()

        cls.wait_fresh_pass(queue_agent_orchids)

    # Simultaneously waits for a controller pass on all queues and consumers.
    @classmethod
    def _wait_for_object_passes(cls, instances=None):
        if instances is None:
            instances = cls.INSTANCES
        assert instances is not None

        queue_agent_orchids = [QueueAgentOrchid(instance) for instance in instances]
        controller_orchids = sum([queue_agent_orchid.get_queue_orchids() + queue_agent_orchid.get_consumer_orchids()
                                  for queue_agent_orchid in queue_agent_orchids], [])

        cls.wait_fresh_pass(controller_orchids)

    # Waits for a complete pass by all queue agent entities.
    # More specifically, it performs the following (in order):
    #     0. If instances are not specified, waits for all queue agent instances to register themselves.
    #     1. Waits for a single leading cypress synchronizer and queue agent manager to be elected.
    #     2. Waits for a complete pass by the leading cypress synchronizer.
    #     3. Waits for a complete pass by the leading queue agent manager.
    #     4. Waits for a complete pass by all queue agents.
    #     5. Simultaneously waits for a controller pass on all queues and consumers.
    @classmethod
    def _wait_for_global_sync(cls, instances=None):
        start_time = time.time()

        if instances is None:
            instances = cls._wait_for_instances()

        cls._wait_for_elections(instances)
        cls._wait_for_component_passes(instances)
        cls._wait_for_object_passes(instances)

        print_debug("Synced all state; elapsed time:", time.time() - start_time)


class TestQueueAgent(TestQueueAgentBase):
    DELTA_QUEUE_AGENT_DYNAMIC_CONFIG = {
        "cypress_synchronizer": {
            "policy": "watching",
        },
    }

    @authors("achulkov2")
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

    @authors("max42")
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
        self._prepare_tables(queue_table_schema=wrong_schema)

        orchid.wait_fresh_pass()
        assert_yt_error(orchid.get_pass_error(), "No such column")

        self._prepare_tables(force=True)
        orchid.wait_fresh_pass()
        orchid.validate_no_pass_error()

    @authors("max42")
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

    @authors("max42")
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

    @authors("achulkov2")
    def test_alerts(self):
        orchid = QueueAgentOrchid()
        alert_orchid = AlertManagerOrchid()

        self._drop_tables()

        orchid.wait_fresh_pass()

        wait(lambda: "queue_agent_pass_failed" in alert_orchid.get_alerts())
        assert_yt_error(YtError.from_dict(alert_orchid.get_alerts()["queue_agent_pass_failed"]),
                        "Error while reading dynamic state")

    @authors("achulkov2")
    def test_no_alerts(self):
        alert_orchid = AlertManagerOrchid()

        wait(lambda: not alert_orchid.get_alerts())

    @authors("max42")
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

    @authors("max42")
    def test_queue_status(self):
        orchid = QueueAgentOrchid()

        schema = self._create_queue("//tmp/q", partition_count=2, enable_cumulative_data_weight_column=False)
        schema_with_cumulative_data_weight = schema + [{"name": "$cumulative_data_weight", "type": "int64"}]
        self._create_registered_consumer("//tmp/c", "//tmp/q")

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

    @authors("max42")
    def test_consumer_status(self):
        orchid = QueueAgentOrchid()

        self._create_queue("//tmp/q", partition_count=2)
        self._create_registered_consumer("//tmp/c", "//tmp/q")

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
            assert partition["next_row_commit_time"] == (self._timestamp_to_iso_str(timestamps[next_row_index])
                                                         if next_row_index < 2 else YsonEntity())
            assert (partition["processing_lag"] > 0) == (next_row_index < 2)

        orchid.get_consumer_orchid("primary://tmp/c").wait_fresh_pass()
        consumer_partitions = orchid.get_consumer_orchid("primary://tmp/c").get_partitions()["primary://tmp/q"]
        assert_partition(consumer_partitions[0], 0)
        assert_partition(consumer_partitions[1], 0)

        self._advance_consumer("//tmp/c", "//tmp/q", 0, 1)
        orchid.get_consumer_orchid("primary://tmp/c").wait_fresh_pass()
        consumer_partitions = orchid.get_consumer_orchid("primary://tmp/c").get_partitions()["primary://tmp/q"]
        assert_partition(consumer_partitions[0], 1)

        self._advance_consumer("//tmp/c", "//tmp/q", 1, 2)
        orchid.get_consumer_orchid("primary://tmp/c").wait_fresh_pass()
        consumer_partitions = orchid.get_consumer_orchid("primary://tmp/c").get_partitions()["primary://tmp/q"]
        assert_partition(consumer_partitions[1], 2)

    @authors("max42")
    def test_consumer_partition_disposition(self):
        orchid = QueueAgentOrchid()

        self._create_queue("//tmp/q")
        self._create_registered_consumer("//tmp/c", "//tmp/q")

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

    @authors("max42")
    def test_inconsistent_partitions_in_consumer_table(self):
        orchid = QueueAgentOrchid()

        self._create_queue("//tmp/q", partition_count=2)
        self._create_registered_consumer("//tmp/c", "//tmp/q")

        self._wait_for_component_passes()

        insert_rows("//tmp/q", [{"data": "foo"}] * 2)
        orchid.get_queue_orchid("primary://tmp/q").wait_fresh_pass()

        self._advance_consumer("//tmp/c", "//tmp/q", 1, 1)
        self._advance_consumer("//tmp/c", "//tmp/q", 2 ** 63 - 1, 1)
        self._advance_consumer("//tmp/c", "//tmp/q", 2 ** 64 - 1, 1)

        orchid.get_consumer_orchid("primary://tmp/c").wait_fresh_pass()

        partitions = orchid.get_consumer_orchid("primary://tmp/c").get_partitions()["primary://tmp/q"]
        assert len(partitions) == 2
        assert partitions[0]["next_row_index"] == 0


class TestRates(TestQueueAgentBase):
    DELTA_QUEUE_AGENT_DYNAMIC_CONFIG = {
        "cypress_synchronizer": {
            # We manually expose queues and consumers in this test, so we use polling implementation.
            "policy": "polling",
        },
    }

    @authors("max42")
    def test_rates(self):
        eps = 1e-2
        zero = {"current": 0.0, "1m_raw": 0.0, "1m": 0.0, "1h": 0.0, "1d": 0.0}

        orchid = QueueAgentOrchid()

        self._create_queue("//tmp/q", partition_count=2)
        self._create_registered_consumer("//tmp/c", "//tmp/q")

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
                   2 * partitions[1]["read_data_weight_rate"]["1m_raw"]) < eps

        # Check total read rate.

        status, partitions = orchid.get_consumer_orchid("primary://tmp/c").get_subconsumer("primary://tmp/q")
        assert abs(status["read_row_count_rate"]["1m_raw"] -
                   3 * partitions[1]["read_row_count_rate"]["1m_raw"]) < eps
        assert abs(status["read_data_weight_rate"]["1m_raw"] -
                   3 * partitions[1]["read_data_weight_rate"]["1m_raw"]) < eps


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

    @authors("achulkov2")
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

    @authors("achulkov2")
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

    @authors("achulkov2")
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

    @authors("achulkov2")
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

    @authors("achulkov2")
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
        self._advance_consumer("//tmp/c1", "//tmp/q", 0, 2)
        self._advance_consumer("//tmp/c2", "//tmp/q", 0, 1)

        # Yet the second partition should be trimmed based on c1.
        self._advance_consumer("//tmp/c1", "//tmp/q", 1, 3)
        self._advance_consumer("//tmp/c2", "//tmp/q", 1, 2)

        self._wait_for_row_count("//tmp/q", 1, 4)

        sync_mount_table("//tmp/q", first_tablet_index=0, last_tablet_index=0)
        # Now the first partition should be trimmed based on c1 as well.

        self._wait_for_row_count("//tmp/q", 0, 3)

    @authors("achulkov2")
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
        self._advance_consumer("//tmp/c2", "//tmp/q2", 0, 4)

        queue_agent_orchid.get_queue_orchid("primary://tmp/q1").wait_fresh_pass()
        queue_agent_orchid.get_queue_orchid("primary://tmp/q2").wait_fresh_pass()

        self._wait_for_row_count("//tmp/q1", 0, 2)
        # The second queue should not be trimmed yet.

        sync_mount_table("//tmp/q2")

        self._wait_for_row_count("//tmp/q2", 0, 3)

    @authors("achulkov2")
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

    @authors("max42")
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

    @authors("achulkov2")
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

    @authors("achulkov2")
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

    @authors("achulkov2")
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

    @authors("max42")
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

    @authors("achulkov2")
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

        # TODO(achulkov2): Check the queue_consumer_partitions attribute once the corresponding queue_controller
        #  code is written.

        # Check that consumer attributes are opaque.
        full_attributes = get("//tmp/c/@")
        for attribute in ("queue_consumer_status", "queue_consumer_partitions"):
            assert full_attributes[attribute] == YsonEntity()

    @authors("max42")
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

    @authors("max42")
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

    @authors("achulkov2")
    def test_revision_changes_on_queue_attribute_change(self):
        create("table", "//tmp/q", attributes={"dynamic": True, "schema": [{"name": "data", "type": "string"}]})
        sync_mount_table("//tmp/q")

        self._set_and_assert_revision_change("//tmp/q", "queue_agent_stage", "testing")

    @authors("achulkov2")
    def test_revision_changes_on_consumer_attribute_change(self):
        self._create_queue("//tmp/q")
        sync_mount_table("//tmp/q")
        self._create_registered_consumer("//tmp/c", "//tmp/q")

        self._set_and_assert_revision_change("//tmp/c", "treat_as_queue_consumer", True)
        self._set_and_assert_revision_change("//tmp/c", "queue_agent_stage", "testing")
        # TODO(max42): this attribute is deprecated.
        self._set_and_assert_revision_change("//tmp/c", "vital_queue_consumer", True)
        self._set_and_assert_revision_change("//tmp/c", "target_queue", "haha:muahaha")


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
    @authors("achulkov2")
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

    @authors("achulkov2")
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
    def test_queue_recreation(self):
        orchid = CypressSynchronizerOrchid()

        self._create_and_register_queue("//tmp/q")
        orchid.wait_fresh_pass()

        old_queue = self._get_queues_and_check_invariants(expected_count=1)[0]

        remove("//tmp/q")

        self._create_and_register_queue("//tmp/q", initiate_helpers=False)
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

    @authors("achulkov2")
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

    @authors("achulkov2")
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

    @authors("achulkov2")
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

    @authors("achulkov2")
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
    @authors("achulkov2")
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

    @authors("achulkov2")
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


class TestReplicatedTableObjects(TestQueueAgentBase, ChaosTestBase):
    DELTA_QUEUE_AGENT_DYNAMIC_CONFIG = {
        "cypress_synchronizer": {
            "policy": "watching",
            "poll_replicated_objects": True,
            "write_registration_table_mapping": True,
        },
        "queue_agent": {
            "handle_replicated_objects": True,
        }
    }

    QUEUE_SCHEMA = [{"name": "data", "type": "string"}]

    @staticmethod
    def _create_replicas(replicated_table, replicas):
        replica_ids = []
        for replica in replicas:
            replica["mode"] = replica.get("mode", "async")
            replica_id = create_table_replica(replicated_table, replica["cluster_name"], replica["replica_path"],
                                              attributes={"mode": replica["mode"]})
            if replica.get("enabled", False):
                alter_table_replica(replica_id, enabled=True)

            replica_ids.append(replica_id)
        return replica_ids

    @staticmethod
    def _assert_internal_queues_are(expected_queues):
        queues = builtins.set(map(itemgetter("path"), select_rows("[path] from [//sys/queue_agents/queues]")))
        assert queues == builtins.set(expected_queues)

    @staticmethod
    def _assert_internal_consumers_are(expected_consumers):
        consumers = builtins.set(map(itemgetter("path"), select_rows("[path] from [//sys/queue_agents/consumers]")))
        assert consumers == builtins.set(expected_consumers)

    @authors("achulkov2")
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
        create("replicated_table", replicated_queue, attributes={
            "dynamic": True,
            "schema": self.QUEUE_SCHEMA
        })
        replicated_queue_replicas = [
            {"cluster_name": "primary", "replica_path": f"{replicated_queue}_replica_0"},
            {"cluster_name": "primary", "replica_path": f"{replicated_queue}_replica_1"}
        ]
        replicated_queue_replica_ids = self._create_replicas(replicated_queue, replicated_queue_replicas)

        # Create replicated consumer.
        create("replicated_table", replicated_consumer, attributes={
            "dynamic": True,
            "schema": init_queue_agent_state.CONSUMER_OBJECT_TABLE_SCHEMA,
            "treat_as_queue_consumer": True
        })
        replicated_consumer_replicas = [
            {"cluster_name": "primary", "replica_path": f"{replicated_consumer}_replica",
             "mode": "sync", "state": "enabled"},
        ]
        replicated_consumer_replica_ids = self._create_replicas(replicated_consumer, replicated_consumer_replicas)

        # Create chaos replicated queue.
        create("chaos_replicated_table", chaos_replicated_queue, attributes={
            "chaos_cell_bundle": "c",
            "schema": self.QUEUE_SCHEMA,
        })
        queue_replication_card_id = get(f"{chaos_replicated_queue}/@replication_card_id")

        # Create chaos replicated consumer.
        create("chaos_replicated_table", chaos_replicated_consumer, attributes={
            "chaos_cell_bundle": "c",
            "schema": init_queue_agent_state.CONSUMER_OBJECT_TABLE_SCHEMA,
        })
        consumer_replication_card_id = get(f"{chaos_replicated_consumer}/@replication_card_id")
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
        chaos_replicated_consumer_replica_ids = self._create_chaos_table_replicas(
            chaos_replicated_consumer_replicas,
            table_path=chaos_replicated_consumer)
        self._create_replica_tables(chaos_replicated_consumer_replicas, chaos_replicated_consumer_replica_ids,
                                    schema=init_queue_agent_state.CONSUMER_OBJECT_TABLE_SCHEMA)
        self._sync_replication_era(consumer_replication_card_id)

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
    @authors("achulkov2")
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
