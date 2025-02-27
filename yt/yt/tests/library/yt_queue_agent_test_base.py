from operator import itemgetter
from yt_env_setup import YTEnvSetup
from yt_chaos_test_base import ChaosTestBase

from yt_commands import (freeze_table, get, read_table, set, ls, unfreeze_table, wait, create, remove, sync_mount_table, sync_create_cells, exists,
                         select_rows, sync_reshard_table, print_debug, get_driver, register_queue_consumer,
                         sync_freeze_table, sync_unfreeze_table, create_table_replica, sync_enable_table_replica,
                         advance_consumer, insert_rows, wait_for_tablet_state)

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


class ObjectAlertHelper:
    def __init__(self, alerts):
        self.alerts = alerts

    # TODO(apachee): Improve code re-usability for check_matching and assert_matching.

    def check_matching(self, category, text=None, attributes=None):
        if category not in self.alerts:
            raise Exception(f"Could not match alert category {category} against collected alerts {list(self.alerts.keys())}")

        errors = self.alerts[category]["inner_errors"]

        text = text or ""
        attributes = attributes or dict()

        def is_matching_error(error):
            print_debug(str(error), text in str(error), update(error["attributes"], attributes), error["attributes"])
            return text in str(error) and update(error["attributes"], attributes) == error["attributes"]

        if not any(map(is_matching_error, errors)):
            raise Exception(f"Could not find matching error in category {category} with substring \"{text}\" and attributes {attributes} in {errors}")

        return True

    def assert_matching(self, category, text=None, attributes=None):
        assert category in self.alerts, f"Could not match alert category {category} against collected alerts {list(self.alerts.keys())}"

        errors = self.alerts[category]["inner_errors"]

        text = text or ""
        attributes = attributes or dict()

        def is_matching_error(error):
            print_debug(str(error), text in str(error), update(error["attributes"], attributes), error["attributes"])
            return text in str(error) and update(error["attributes"], attributes) == error["attributes"]

        assert any(map(is_matching_error, errors)), f"Could not find matching error in category {category} with substring \"{text}\" and attributes {attributes} in {errors}"

    def get_alert_count(self):
        return sum(map(lambda alert: len(alert["inner_errors"]), self.alerts.values()))

    def __len__(self):
        return len(self.alerts)

    def __str__(self):
        return str(self.alerts)

    def __repr__(self):
        return repr(self.alerts)


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

    def get_alerts(self):
        return ObjectAlertHelper(get(f"{self.orchid_path()}/status/alerts"))

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

    def get_controller_info(self):
        return get(f"{self.orchid_path()}/controller_info")


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


class QueueStaticExportHelpers:
    @staticmethod
    def _create_export_destination(export_directory, queue_id, **kwargs):
        create("map_node", export_directory, recursive=True, ignore_existing=True, **kwargs)
        set(f"{export_directory}/@queue_static_export_destination", {
            "originating_queue_id": queue_id,
        }, **kwargs)
        assert exists(export_directory, driver=kwargs.pop("driver", None))

    @staticmethod
    def remove_export_destination(export_directory, **kwargs):
        def try_remove():
            remove(export_directory, **kwargs)
            return True

        wait(try_remove, ignore_exceptions=True)
        assert not exists(export_directory, driver=kwargs.pop("driver", None))

    @staticmethod
    def remove_export_destinations(export_directories, **kwargs):
        is_removed_list = [False] * len(export_directories)

        def try_remove():
            last_exception = None
            for i, is_removed in enumerate(is_removed_list):
                if is_removed:
                    continue
                try:
                    remove(export_directories[i], **kwargs)
                    is_removed_list[i] = True
                except Exception as ex:
                    last_exception = ex

            if all(is_removed_list):
                return True
            raise last_exception

        wait(try_remove, ignore_exceptions=True)
        assert all(not exists(export_directory, driver=kwargs.pop("driver", None)) for export_directory in export_directories)

    @staticmethod
    # NB: The last two options should be used carefully: currently they strictly check that all timestamps are within [ts - period, ts], which might not generally be the case.
    def _check_export(
        export_directory,
        expected_data,
        queue_path=None,
        use_upper_bound_for_table_names=False,
        check_lower_bound=False,
        last_export_unix_ts_field_name="last_export_unix_ts",
        expected_removed_rows=None,
        **kwargs
    ):
        expected_removed_rows = expected_removed_rows or 0

        exported_tables = [name for name in sorted(ls(export_directory, **kwargs)) if f"{export_directory}/{name}" != queue_path]
        assert len(exported_tables) == len(expected_data)

        queue_id = get(f"{export_directory}/@queue_static_export_destination/originating_queue_id", **kwargs)

        export_progress = get(f"{export_directory}/@queue_static_export_progress", **kwargs)

        last_export_unix_ts, last_exported_table_export_period = map(int, exported_tables[-1].split("-"))
        assert export_progress[last_export_unix_ts_field_name] == last_export_unix_ts + (0 if use_upper_bound_for_table_names else last_exported_table_export_period)

        queue_schema_id = get(f"#{queue_id}/@schema_id", **kwargs)
        queue_schema = get(f"#{queue_id}/@schema", **kwargs)
        queue_native_cell_tag = get(f"#{queue_id}/@native_cell_tag", **kwargs)

        max_timestamp = 0
        total_row_count = 0

        for table_index, table_name in enumerate(exported_tables):
            table_path = f"{export_directory}/{table_name}"

            if table_path == queue_path:
                continue

            export_unix_ts, export_period = map(int, table_name.split("-"))

            exported_table_native_cell_tag = get(f"{table_path}/@native_cell_tag", **kwargs)

            # If both tables are on the same native cell, we use schema ids in upload.
            if exported_table_native_cell_tag == queue_native_cell_tag:
                exported_table_schema_id = get(f"{table_path}/@schema_id", **kwargs)
                assert exported_table_schema_id == queue_schema_id

            exported_table_schema = get(f"{table_path}/@schema", **kwargs)
            assert exported_table_schema == queue_schema

            rows = list(read_table(table_path, **kwargs))
            assert list(map(itemgetter("data"), rows)) == expected_data[table_index]

            ts_lower_bound = export_unix_ts - (export_period if use_upper_bound_for_table_names else 0)
            ts_upper_bound = export_unix_ts + (0 if use_upper_bound_for_table_names else export_period)

            for row in rows:
                ts = row["$timestamp"]
                max_timestamp = max(ts, max_timestamp)

                assert ts >> 30 <= ts_upper_bound

                if check_lower_bound:
                    assert ts_lower_bound <= ts >> 30

            total_row_count += len(rows)

        assert max(map(itemgetter("max_timestamp"), export_progress["tablets"].values())) == max_timestamp
        assert sum(map(itemgetter("row_count"), export_progress["tablets"].values())) == total_row_count + expected_removed_rows, \
            f"{list(map(itemgetter('row_count'), export_progress['tablets'].values()))=}, {total_row_count=}"

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


##################################################################


class TestQueueAgentBase(YTEnvSetup):
    # NB(apachee): Create Queue Agent instances only on primary cluster
    NUM_QUEUE_AGENTS_PRIMARY = 1
    NUM_QUEUE_AGENTS = 0

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
                "queue_exporter": {
                    "pass_period": 100,
                    # Fixate greater value for tests in case default decreases in the future.
                    "max_exported_table_count_per_task": 10,
                }
            },
            "queue_export_manager": {
                "export_rate_limit": 100.0,
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
    def _calculate_diff(cls, obj1, obj2):
        if isinstance(obj1, dict) and isinstance(obj2, dict):
            keys = builtins.set(obj1.keys()) | builtins.set(obj2.keys())
            result_l = {}
            result_r = {}
            for key in keys:
                diff_l, diff_r = cls._calculate_diff(obj1.get(key, None), obj2.get(key, None))
                if diff_l != diff_r:
                    result_l[key] = diff_l
                    result_r[key] = diff_r
            return result_l, result_r
        return obj1, obj2

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

        if exists(cls.config_path):
            remove(cls.config_path)

        create("document", cls.config_path, attributes={"value": cls.BASE_QUEUE_AGENT_DYNAMIC_CONFIG})

        cls._apply_dynamic_config_patch(getattr(cls, "DELTA_QUEUE_AGENT_DYNAMIC_CONFIG", dict()))

        cls.INSTANCES = cls._wait_for_instances()
        cls._wait_for_elections()

    def setup_method(self, method):
        super(TestQueueAgentBase, self).setup_method(method)

        primary_cell_tag = get("//sys/@primary_cell_tag")
        for cluster_name in self.get_cluster_names():
            driver = get_driver(cluster=cluster_name)
            set("//sys/tablet_cell_bundles/default/@options/clock_cluster_tag", primary_cell_tag, driver=driver)

        if self.DO_PREPARE_TABLES_ON_SETUP:
            self._prepare_tables()

    def teardown_method(self, method):
        self._drop_tables()

        super(TestQueueAgentBase, self).teardown_method(method)

    @classmethod
    def _apply_dynamic_config_patch(cls, patch):
        config = get(cls.config_path)
        update_inplace(config, patch)
        set(cls.config_path, config, force=True)

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
                    raise Exception(f"diff = {cls._calculate_diff(update(effective_config, config), effective_config)}")
            return True

        wait(config_updated_on_all_instances, ignore_exceptions=True)

    def _prepare_tables(self, queue_table_schema=None, consumer_table_schema=None, **kwargs):
        assert queue_table_schema is None and consumer_table_schema is None, \
            "Current implementation of init_queue_agent_state does not support custom schemas"
        sync_create_cells(1)
        self.client = self.Env.create_native_client()
        init_queue_agent_state.create_tables_latest_version(
            self.client,
            **kwargs)

    def _drop_tables(self):
        init_queue_agent_state.delete_all_tables(self.client)

    @staticmethod
    def _create_queue(path, partition_count=1, enable_timestamp_column=True,
                      enable_cumulative_data_weight_column=True, mount=True, max_inline_hunk_size=None, **kwargs):
        schema = [{"name": "data", "type": "string"}]
        if max_inline_hunk_size is not None:
            schema[0]["max_inline_hunk_size"] = max_inline_hunk_size
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

    @staticmethod
    def _create_consumer(path, mount=True, without_meta=False, driver=None, **kwargs):
        if without_meta or not mount:
            attributes = {
                "dynamic": True,
                "schema": init_queue_agent_state.CONSUMER_OBJECT_TABLE_SCHEMA_WITHOUT_META if without_meta else init_queue_agent_state.CONSUMER_OBJECT_TABLE_SCHEMA,
                "treat_as_queue_consumer": True,
            }
            attributes.update(kwargs)
            create("table", path, attributes=attributes, driver=driver)
            if mount:
                sync_mount_table(path, driver=driver)
            return

        create("queue_consumer", path, driver=driver, attributes=kwargs)

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

    def _create_producer(self, producer_path, wait_for_mount=True, **kwargs):
        create("queue_producer", producer_path, attributes=kwargs)
        wait_for_tablet_state(producer_path, "mounted")

    @staticmethod
    def _flush_table(path, first_tablet_index=None, last_tablet_index=None):
        sync_freeze_table(path, first_tablet_index=first_tablet_index, last_tablet_index=last_tablet_index)
        sync_unfreeze_table(path, first_tablet_index=first_tablet_index, last_tablet_index=last_tablet_index)

    @staticmethod
    def _flush_tables(paths):
        def do_for_all_paths(f):
            for path in paths:
                f(path)

        do_for_all_paths(freeze_table)
        do_for_all_paths(lambda path: wait_for_tablet_state(path, "frozen"))
        do_for_all_paths(unfreeze_table)
        do_for_all_paths(lambda path: wait_for_tablet_state(path, "mounted"))

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
            return len(ls("//sys/queue_agents/instances", verbose=False)) == getattr(cls, "NUM_QUEUE_AGENTS_PRIMARY")

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
