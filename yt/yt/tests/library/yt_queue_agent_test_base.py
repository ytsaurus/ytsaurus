from yt_env_setup import YTEnvSetup
from yt_chaos_test_base import ChaosTestBase

from yt_commands import (get, set, ls, wait, create, sync_mount_table, sync_create_cells, exists,
                         select_rows, sync_reshard_table, print_debug, get_driver, register_queue_consumer,
                         sync_freeze_table, sync_unfreeze_table, create_table_replica, sync_enable_table_replica,
                         advance_consumer, insert_rows)

from yt.common import YtError, update_inplace, update

import builtins
import time

from yt.yson import YsonEntity

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


##################################################################


class ReplicatedObjectBase(ChaosTestBase):
    @staticmethod
    def _is_ordered_schema(schema):
        return not any("sort_order" in column for column in schema)

    @staticmethod
    def _create_replicated_table_base(replicated_table_path, replicas, schema, replicated_table_attributes_patch=None,
                                      create_replica_tables=True):
        base_attributes = {
            "dynamic": True,
            "schema": schema,
        }

        create("replicated_table",
               replicated_table_path,
               attributes=update(base_attributes, replicated_table_attributes_patch or {}))
        sync_mount_table(replicated_table_path)

        replica_ids = []
        for replica in replicas:
            replica["mode"] = replica.get("mode", "async")
            replica_id = create_table_replica(replicated_table_path, replica["cluster_name"], replica["replica_path"],
                                              attributes={"mode": replica["mode"]})
            if replica.get("enabled", False):
                sync_enable_table_replica(replica_id)

            replica_ids.append(replica_id)

            if create_replica_tables:
                replica_driver = get_driver(cluster=replica["cluster_name"])
                create("table", replica["replica_path"],
                       attributes=update(base_attributes, {"upstream_replica_id": replica_id}),
                       driver=replica_driver)
                sync_mount_table(replica["replica_path"], driver=replica_driver)

        return replica_ids

    def _create_chaos_replicated_table_base(self, chaos_replicated_table_path, replicas, schema,
                                            replicated_table_options=None, replicated_table_attributes=None,
                                            chaos_cell_bundle="c", sync_replication_era=True):
        replicated_table_options = replicated_table_options or {}
        replicated_table_attributes = replicated_table_attributes or {}
        create("chaos_replicated_table", chaos_replicated_table_path, attributes=update(replicated_table_attributes, {
            "chaos_cell_bundle": chaos_cell_bundle,
            "replicated_table_options": replicated_table_options,
            "schema": schema,
        }))
        card_id = get(f"{chaos_replicated_table_path}/@replication_card_id")

        replica_ids = self._create_chaos_table_replicas(replicas, table_path=chaos_replicated_table_path)
        self._create_replica_tables(replicas, replica_ids, schema=schema, ordered=self._is_ordered_schema(schema))
        if sync_replication_era:
            self._sync_replication_era(card_id)

        return replica_ids, card_id


##################################################################


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

    DO_PREPARE_TABLES_ON_SETUP = True

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

        if self.DO_PREPARE_TABLES_ON_SETUP:
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
        queue_id = create("table", path, attributes=attributes)
        if partition_count != 1:
            sync_reshard_table(path, partition_count)
        if mount:
            sync_mount_table(path)

        return schema, queue_id

    def _create_consumer(self, path, mount=True, without_meta=False, **kwargs):
        attributes = {
            "dynamic": True,
            "schema": init_queue_agent_state.CONSUMER_OBJECT_TABLE_SCHEMA_WITHOUT_META if without_meta else init_queue_agent_state.CONSUMER_OBJECT_TABLE_SCHEMA,
            "treat_as_queue_consumer": True,
        }
        attributes.update(kwargs)
        create("table", path, attributes=attributes)
        if mount:
            sync_mount_table(path)

    def _create_registered_consumer(self, consumer_path, queue_path, vital=False, without_meta=False, **kwargs):
        self._create_consumer(consumer_path, **kwargs)
        register_queue_consumer(queue_path, consumer_path, vital=vital)

    def _advance_consumer(self, consumer_path, queue_path, partition_index, offset, client_side=False, via_insert=False):
        self._advance_consumers(consumer_path, queue_path, {partition_index: offset}, client_side, via_insert)

    def _advance_consumers(self, consumer_path, queue_path, partition_index_to_offset, client_side=False, via_insert=False):
        if via_insert:
            insert_rows(consumer_path, [{
                "queue_cluster": "primary",
                "queue_path": queue_path,
                "partition_index": partition_index,
                "offset": offset,
            } for partition_index, offset in partition_index_to_offset.items()])
        else:
            for partition_index, offset in partition_index_to_offset.items():
                advance_consumer(consumer_path, queue_path, partition_index=partition_index, old_offset=None, new_offset=offset, client_side=client_side)

    @staticmethod
    def _flush_table(path, first_tablet_index=None, last_tablet_index=None):
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

##################################################################
