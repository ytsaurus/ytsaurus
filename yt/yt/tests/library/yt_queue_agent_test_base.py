from operator import itemgetter
from yt_env_setup import YTEnvSetup
from yt_chaos_test_base import ChaosTestBase

from yt_commands import (execute_batch, get, get_batch_output, get_connection_orchid_value, make_batch_request, multiset_attributes, read_table, set, ls, wait, create, remove, sync_mount_table,
                         sync_create_cells, exists, select_rows, sync_reshard_table, print_debug, get_driver, register_queue_consumer, sync_freeze_table, sync_unfreeze_table,
                         create_table_replica, sync_enable_table_replica, advance_consumer, insert_rows, wait_for_tablet_state, abort_transactions, raises_yt_error)

from yt_helpers import parse_yt_time, calculate_object_diff

from yt.common import YtError, update_inplace, update

import contextlib
import copy
import builtins
import time

from abc import ABC, abstractmethod
from collections import defaultdict

from yt.yson import YsonEntity

from yt.wrapper.common import generate_uuid
from yt.wrapper.ypath import escape_ypath_literal

import yt.environment.init_queue_agent_state as init_queue_agent_state

##################################################################


class QueueConsumerRegistration:
    def __init__(self, queue_cluster, queue_path, consumer_cluster, consumer_path, vital, partitions=None):
        self.key = (queue_cluster, queue_path, consumer_cluster, consumer_path)
        self.value = (vital, partitions)

    def __eq__(self, other):
        return (self.key, self.value) == (other.key, other.value)

    def __str__(self):
        return str((self.key, self.value))

    def __repr__(self):
        return str(self)

    def __hash__(self):
        return hash(self.key + self.value)

    @staticmethod
    def _normalize_partitions(partitions):
        if partitions is None:
            return partitions
        if partitions == YsonEntity():
            return None
        return tuple(partitions)

    @classmethod
    def from_select(cls, r):
        return cls(r["queue_cluster"], r["queue_path"], r["consumer_cluster"], r["consumer_path"], r["vital"],
                   cls._normalize_partitions(r["partitions"]))

    @classmethod
    def from_orchid(cls, r):
        return cls(*r["queue"].split(":"), *r["consumer"].split(":"), r["vital"],
                   cls._normalize_partitions(r["partitions"]))

    @classmethod
    def from_list_registrations(cls, r):
        return cls(r["queue_path"].attributes["cluster"], str(r["queue_path"]),
                   r["consumer_path"].attributes["cluster"], str(r["consumer_path"]),
                   r["vital"], cls._normalize_partitions(r["partitions"]))


##################################################################


# TODO(apachee): Simplify code for get_xxx method by generating them from
# field names (and maybe post-processing functions).

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


class QueueExporterOrchid(OrchidBase):
    def __init__(self, queue_orchid: "QueueOrchid", export_name: str):
        self.queue_orchid = queue_orchid
        self.export_name = export_name

    def queue_agent_orchid_path(self):
        return self.queue_orchid.queue_agent_orchid_path()

    def orchid_path(self):
        return f"{self.queue_orchid.orchid_path()}/exporters/{self.export_name}"

    def get(self):
        return get(self.orchid_path())

    def get_export_task_invocation_index(self):
        return get(f"{self.orchid_path()}/export_task_invocation_index")

    def get_export_task_invocation_instant(self):
        return parse_yt_time(get(f"{self.orchid_path()}/export_task_invocation_instant"))

    def get_retry_index(self):
        return get(f"{self.orchid_path()}/retry_index")

    def get_retry_backoff_duration_seconds(self):
        return get(f"{self.orchid_path()}/retry_backoff_duration") / 1000

    def get_last_successful_export_unix_ts(self):
        return get(f"{self.orchid_path()}/last_successful_export_unix_ts")

    def wait_fresh_invocation(self):
        invocation_index = self.get_export_task_invocation_index()
        wait(lambda: self.get_export_task_invocation_index() >= invocation_index + 2, sleep_backoff=0.15)


class QueueOrchid(ObjectOrchid):
    OBJECT_TYPE = "queue"

    def get_queue(self):
        result = self.get()
        return result["status"], result["partitions"]

    def get_exporter_orchid(self, export_name: str = "default"):
        return QueueExporterOrchid(self, export_name)


class OwnedQueueOrchid(ObjectOrchid):
    OBJECT_TYPE = "owned_queue"


class OwnedConsumerOrchid(ObjectOrchid):
    OBJECT_TYPE = "owned_consumer"


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

    def list_queues(self):
        """
        List all queues from all instances.
        """
        self.wait_fresh_pass()
        return ls(self.orchid_path() + "/queues")

    def list_consumers(self):
        """
        List all consumers from all instances.
        """
        self.wait_fresh_pass()
        return ls(self.orchid_path() + "/consumers")

    def list_instance_queues(self):
        """
        List all queues from this instance.
        """
        self.wait_fresh_pass()
        return ls(self.orchid_path() + "/owned_queues")

    def list_instance_consumers(self):
        """
        List all consumers from this instance.
        """
        self.wait_fresh_pass()
        return ls(self.orchid_path() + "/owned_consumers")

    def get_instance_queues(self):
        """
        Get all queues from this instance.
        """
        self.wait_fresh_pass()
        return get(self.orchid_path() + "/owned_queues")

    def get_instance_consumers(self):
        """
        Get all consumers from this instance.
        """
        self.wait_fresh_pass()
        return get(self.orchid_path() + "/owned_consumers")

    def get_queue_orchid(self, queue_ref):
        return QueueOrchid(queue_ref, self.agent_id)

    def get_owned_queue_orchid(self, queue_ref):
        return OwnedQueueOrchid(queue_ref, self.agent_id)

    def get_queue_orchids(self):
        return [self.get_queue_orchid(queue) for queue in self.list_queues()]

    def get_owned_queue_orchids(self):
        return [self.get_owned_queue_orchid(queue) for queue in self.list_queues()]

    def get_consumer_orchid(self, consumer_ref):
        return ConsumerOrchid(consumer_ref, self.agent_id)

    def get_owned_consumer_orchid(self, consumer_ref):
        return OwnedConsumerOrchid(consumer_ref, self.agent_id)

    def get_consumer_orchids(self):
        return [self.get_consumer_orchid(consumer) for consumer in self.list_consumers()]

    def get_owned_consumer_orchids(self):
        return [self.get_consumer_orchid(consumer) for consumer in self.list_consumers()]

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
    def _create_table_base(table_path, schema, table_attributes_patch=None, driver=None):
        base_attributes = {
            "dynamic": True,
            "schema": schema,
        }
        create("table", table_path, attributes=update(base_attributes, table_attributes_patch or {}), driver=driver)
        sync_mount_table(table_path)

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


class QueueStaticExportHelpers(ABC):
    def _initialize_active_export_destinations(self):
        if not hasattr(self, "ACTIVE_EXPORT_DESTINATIONS"):
            self.ACTIVE_EXPORT_DESTINATIONS: dict[str, builtins.set[str]] = defaultdict(lambda: builtins.set())

    def _contains_active_export_destination(self, export_dir, cluster_name="primary"):
        return export_dir in self.ACTIVE_EXPORT_DESTINATIONS[cluster_name]

    def _add_active_export_destination(self, export_dir, cluster_name="primary"):
        assert not self._contains_active_export_destination(export_dir, cluster_name)
        self.ACTIVE_EXPORT_DESTINATIONS[cluster_name].add(export_dir)

    def _remove_active_export_destination(self, export_dir, cluster_name="primary"):
        self.ACTIVE_EXPORT_DESTINATIONS[cluster_name].remove(export_dir)

    def _create_export_destination(self, export_dir, queue_id, cluster_name="primary", **kwargs):
        self._initialize_active_export_destinations()

        assert not self._contains_active_export_destination(export_dir, cluster_name)
        assert "driver" not in kwargs

        driver = None
        if cluster_name != "primary":
            driver = get_driver(cluster=cluster_name)

        create("map_node", export_dir, recursive=True, ignore_existing=True, driver=driver, **kwargs)

        attributes = {
            "queue_static_export_destination": {
                "originating_queue_id": queue_id,
            },
        }
        attributes.update(kwargs.pop("attributes", {}))

        multiset_attributes(f"{export_dir}/@", attributes, driver=driver)

        assert exists(export_dir, driver=driver)

        self._add_active_export_destination(export_dir, cluster_name)

    def _create_export_destinations(self, export_dirs, queue_ids, cluster_name="primary", **kwargs):
        self._initialize_active_export_destinations()

        assert all(not self._contains_active_export_destination(export_dir, cluster_name) for export_dir in export_dirs)
        assert len(export_dirs) == len(queue_ids)
        assert "driver" not in kwargs

        driver = None
        if cluster_name != "primary":
            driver = get_driver(cluster=cluster_name)

        create_reqs = []

        for export_dir, queue_id in zip(export_dirs, queue_ids):
            create_reqs.append(make_batch_request("create", type="map_node", path=export_dir, recursive=True, ignore_existing=True, **kwargs))

        create_rsps = execute_batch(create_reqs, driver=driver)
        for rsp in create_rsps:
            # Check that no error has occured
            get_batch_output(rsp)

        kwargs_attributes = kwargs.pop("attributes", {})
        set_attribute_reqs = []

        for export_dir, queue_id in zip(export_dirs, queue_ids):
            attributes = {
                "queue_static_export_destination": {
                    "originating_queue_id": queue_id,
                },
            }
            attributes.update(kwargs_attributes)
            set_attribute_reqs.append(make_batch_request("multiset_attributes", path=f"{export_dir}/@", input=attributes))

        set_attribute_rsps = execute_batch(set_attribute_reqs, driver=driver)

        for rsp in set_attribute_rsps:
            # Check that no error has occured
            get_batch_output(rsp)

        exists_reqs = []
        for export_dir in export_dirs:
            exists_reqs.append(make_batch_request("exists", path=export_dir))

        exists_rsps = execute_batch(exists_reqs, driver=driver)
        for rsp in exists_rsps:
            assert get_batch_output(rsp)

        for export_dir in export_dirs:
            self._add_active_export_destination(export_dir, cluster_name)

    def _abort_transactions_in_subtree(self, path, driver):
        transactions = builtins.set()
        traverse_stack = [get(path, attributes=["locks"], driver=driver)]
        while traverse_stack:
            node = traverse_stack.pop(-1)
            for lock in node.attributes["locks"]:
                if lock["mode"] in ("shared", "exclusive"):
                    transactions.add(lock["transaction_id"])
            if isinstance(node, dict):
                traverse_stack += list(node.values())

        if not transactions:
            return

        print_debug(f"Aborting transactions in subtree {path}")
        abort_transactions(list(transactions), driver=driver, verbose=True)

    # NB(apachee): We only take queue snapshots, so we wouldn't run into any issues with locks here.
    def _remove_queue_export_by_export_destination(self, export_dir, driver=None, **kwargs):
        queue_id = get(f"{export_dir}/@queue_static_export_destination/originating_queue_id", driver=driver, **kwargs)

        static_export_config = get(f"#{queue_id}/@static_export_config", driver=driver, **kwargs)

        # Filtered config.
        static_export_config = {name: config for name, config in static_export_config.items() if export_dir != config["export_directory"]}

        set(f"#{queue_id}/@static_export_config", static_export_config, driver=driver, **kwargs)

        print_debug(f"Successfully removed queue export by export destination ({export_dir})")

    def remove_export_destination(self, export_dir, cluster_name="primary", **kwargs):
        print_debug(f"Removing export destination {export_dir} on cluster {cluster_name}")
        self._initialize_active_export_destinations()

        assert self._contains_active_export_destination(export_dir, cluster_name)
        assert "driver" not in kwargs

        driver = None
        if cluster_name != "primary":
            driver = get_driver(cluster=cluster_name)

        try:
            self._remove_queue_export_by_export_destination(export_dir, driver=driver, **kwargs)
        except Exception as ex:
            print_debug(f"Failed to disable exports to {export_dir} from queue export config: {ex}")

        def try_remove():
            self._abort_transactions_in_subtree(export_dir, driver)
            try:
                remove(export_dir, force=True, driver=driver, **kwargs)
            except YtError as ex:
                print_debug(f"Failed to remove {export_dir}: {ex}; retrying")
                raise
            return True

        wait(try_remove, ignore_exceptions=True)
        assert not exists(export_dir, driver=driver)

        self._remove_active_export_destination(export_dir, cluster_name)

    def remove_export_destinations(self, export_dirs, timeout=None, cluster_name="primary", **kwargs):
        self._initialize_active_export_destinations()

        assert all(self._contains_active_export_destination(export_dir, cluster_name) for export_dir in export_dirs)
        assert "driver" not in kwargs

        driver = None
        if cluster_name != "primary":
            driver = get_driver(cluster=cluster_name)

        print_debug(f"removing the following export destinations: {export_dirs}")

        is_removed_list = [False] * len(export_dirs)

        def try_remove():
            last_exception = None
            for i, is_removed in enumerate(is_removed_list):
                if is_removed:
                    continue
                try:
                    remove(export_dirs[i], force=True, driver=driver, **kwargs)
                except Exception as ex:
                    last_exception = ex
                    continue
                assert not exists(export_dirs[i], driver=driver, **kwargs)
                is_removed_list[i] = True
                print_debug(f"removed export destination {export_dirs[i]}")
                self._remove_active_export_destination(export_dirs[i], cluster_name)

            if all(is_removed_list):
                return True
            raise last_exception

        wait(try_remove, timeout=timeout, ignore_exceptions=True)
        assert all(is_removed_list)

    def remove_all_active_export_destinations(self, **kwargs):
        self._initialize_active_export_destinations()

        items = list(self.ACTIVE_EXPORT_DESTINATIONS.items())
        for cluster_name, export_dirs in items:
            self.remove_export_destinations(list(export_dirs), cluster_name=cluster_name)

        assert sum([len(i) for i in self.ACTIVE_EXPORT_DESTINATIONS.values()]) == 0

    @staticmethod
    def get_export_schedule(use_cron, export_period_seconds):
        # Helper function to generate the correct export schedule entry.
        if use_cron:
            return {"export_cron_schedule": f"0/{export_period_seconds} * * * * *"}
        return {"export_period": export_period_seconds * 1000}

    @classmethod
    @abstractmethod
    def _get_default_last_export_unix_ts_field_name(cls) -> str:
        ...

    @classmethod
    # NB: The last two options should be used carefully: currently they strictly check that all timestamps are within [ts - period, ts], which might not generally be the case.
    def _check_export(
        cls,
        export_directory,
        expected_data,
        queue_path=None,
        use_upper_bound_for_table_names=False,
        check_lower_bound=False,
        last_export_unix_ts_field_name=None,
        expected_removed_rows=None,
        **kwargs
    ):
        last_export_unix_ts_field_name = last_export_unix_ts_field_name or cls._get_default_last_export_unix_ts_field_name()

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


class QueueConsumerRegistrationManagerBase(YTEnvSetup):
    DELTA_QUEUE_CONSUMER_REGISTRATION_MANAGER_CONFIG = {
        "disable_list_all_registrations": True,
        "implementation": "async_expiring_cache",
        "bypass_caching": False,
    }

    QUEUE_CONSUMER_REGISTRATION_MANAGER_LEGACY_IMPLEMENTATION_CHECK = False

    _QUEUE_CONSUMER_REGISTRATION_MANAGER_CONFIG_IGNORED_FIELD_LIST = {
        "local_replica_path",
        "replica_clusters",
        "read_availability_clusters",
        "write_availability_clusters",
    }

    _APPLIED_QUEUE_CONSUMER_REGISTRATION_MANAGER_CONFIG = {}

    # TODO(apachee): Optimize setup by changing registration manager static config. May not help much though, since dynamic config needs to be changed as well anyway,
    # and in the time it is changed config might change to the default one. At the very least configuration refresh period can be reduced.

    @classmethod
    def _check_registration_manager_implementation(cls, implementation: str):
        def get_ctx():
            if cls.QUEUE_CONSUMER_REGISTRATION_MANAGER_LEGACY_IMPLEMENTATION_CHECK:
                return raises_yt_error("Attribute \"queue_consumer_registration_manager_implementation\" is not found")
            else:
                return contextlib.nullcontext()

        for cluster in cls.get_cluster_names():
            driver = get_driver(cluster=cluster)
            for proxy in ls("//sys/rpc_proxies", driver=driver):
                with get_ctx():
                    effective_implementation = get(
                        f"//sys/rpc_proxies/{proxy}/orchid/cluster_connection/queue_consumer_registration_manager/@queue_consumer_registration_manager_implementation",
                        driver=driver
                    )
                    assert effective_implementation == implementation

            if exists("//sys/kafka_proxies", driver=driver):
                for proxy in ls("//sys/kafka_proxies/instances", driver=driver):
                    with get_ctx():
                        effective_implementation = get(
                            f"//sys/kafka_proxies/instances/{proxy}/orchid/cluster_connection/queue_consumer_registration_manager/@queue_consumer_registration_manager_implementation",
                            driver=driver
                        )
                        assert effective_implementation == implementation

            if cls.DRIVER_BACKEND == "native":
                effective_implementation = get_connection_orchid_value("/queue_consumer_registration_manager/@queue_consumer_registration_manager_implementation", driver=driver)
                assert effective_implementation == implementation, \
                    f"Native driver implementation mismatch on cluster {cluster}: expected {implementation}, got {effective_implementation}"

        print_debug(f"Registration manager implementation: {implementation}")

    @classmethod
    def _get_registration_manager_config(cls):
        registration_manager_config = {}
        cls._apply_effective_config_patch(registration_manager_config, "DELTA_QUEUE_CONSUMER_REGISTRATION_MANAGER_CONFIG")
        return registration_manager_config

    @classmethod
    def setup_class(cls):
        super().setup_class()

        # NB(apachee): Stores last applied queue consumer registration manager config.
        cls._APPLIED_QUEUE_CONSUMER_REGISTRATION_MANAGER_CONFIG = {}

        registration_manager_config = cls._get_registration_manager_config()
        cls._apply_registration_manager_config_patch_all(registration_manager_config)
        cls._check_registration_manager_implementation(cls._get_registration_manager_applied_implementation())

    @classmethod
    def _get_registration_manager_applied_implementation(cls):
        config = cls._get_registration_manager_config()

        if cls.QUEUE_CONSUMER_REGISTRATION_MANAGER_LEGACY_IMPLEMENTATION_CHECK:
            assert "implementation" not in cls._APPLIED_QUEUE_CONSUMER_REGISTRATION_MANAGER_CONFIG, \
                f"Invalid applied config value: {cls._APPLIED_QUEUE_CONSUMER_REGISTRATION_MANAGER_CONFIG}"
            assert "implementation" not in config, \
                f"Invalid config value: {config}"
            return "legacy"
        else:
            return cls._APPLIED_QUEUE_CONSUMER_REGISTRATION_MANAGER_CONFIG["implementation"]

    @classmethod
    def _process_patch(cls, patch):
        processed_patch = {}

        for k, v in patch.items():
            if k not in cls._QUEUE_CONSUMER_REGISTRATION_MANAGER_CONFIG_IGNORED_FIELD_LIST:
                processed_patch[k] = v

        # NB(apachee): Improve registration manager responsiveness by reducing refresh periods.

        processed_patch.update({
            "cache_refresh_period": 250,
            "configuration_refresh_period": 500,
        })

        # COMPAT(apachee): After stable release supports this field, we can remove this code.
        implementation = processed_patch.get("implementation", None) or cls._get_registration_manager_applied_implementation()
        if implementation == "async_expiring_cache":
            processed_patch.update({
                "cache": {
                    "base": {
                        "expire_after_access_time": 1000,
                        "expire_after_successful_update_time": 1000,
                        "expire_after_failed_update_time": 0,  # NB(apachee): Do not cache failures.
                        "refresh_time": 1000,
                        "expiration_period": 1000,
                        "batch_update": True,
                    },
                    # TODO(apachee): Add batch lookup config.
                }
            })

        return processed_patch

    @classmethod
    def _apply_registration_manager_config_patch(cls, processed_patch, cluster):
        """Apply processed registration manager config patch to the specified cluster"""
        driver = get_driver(cluster=cluster)
        config_path = f"//sys/clusters/{cluster}/queue_agent/queue_consumer_registration_manager"

        config = get(config_path, driver=driver)
        update_inplace(config, processed_patch)
        print_debug("Setting dynamic config", config)
        set(config_path, config, driver=driver)

        applied_config = {}

        def check_orchid_value(proxy, effective_config, unrecognized_config_options):
            print_debug(f"Checking orchid value for proxy {proxy}")
            if update(effective_config, config) != effective_config or unrecognized_config_options != {}:
                print_debug(f"Diff: {calculate_object_diff(update(effective_config, config), effective_config):expected_config,actual_config}, "
                            f"unrecognized config options: {unrecognized_config_options}")
                return False
            return True

        def config_updated():
            effective_config = None

            assert cls.DRIVER_BACKEND in ["native", "rpc"]

            for proxy in ls("//sys/rpc_proxies", driver=driver):
                orchid_path = f"//sys/rpc_proxies/{proxy}/orchid/cluster_connection/queue_consumer_registration_manager"
                orchid_value = get(orchid_path, driver=driver)
                effective_config = orchid_value["effective_config"]
                unrecognized_config_options = orchid_value.get("unrecognized_config_options", {})
                if not check_orchid_value(proxy, effective_config, unrecognized_config_options):
                    return False

            if exists("//sys/kafka_proxies", driver=driver):
                for proxy in ls("//sys/kafka_proxies/instances", driver=driver):
                    orchid_path = f"//sys/kafka_proxies/instances/{proxy}/orchid/cluster_connection/queue_consumer_registration_manager"
                    orchid_value = get(orchid_path, driver=driver)
                    effective_config = orchid_value["effective_config"]
                    unrecognized_config_options = orchid_value.get("unrecognized_config_options", {})
                    if not check_orchid_value(proxy, effective_config, unrecognized_config_options):
                        return False

            if cls.DRIVER_BACKEND == "native":
                orchid_value = get_connection_orchid_value("/queue_consumer_registration_manager", driver=driver)
                effective_config = orchid_value["effective_config"]
                unrecognized_config_options = orchid_value.get("unrecognized_config_options", {})
                if not check_orchid_value("native_driver", effective_config, unrecognized_config_options):
                    return False

            assert effective_config is not None
            applied_config.update(config)
            return True

        wait(config_updated)
        assert applied_config

        return applied_config

    @classmethod
    def _apply_registration_manager_config_patch_all(cls, patch):
        """Apply registration manager config patch to all clusters"""
        patch = cls._process_patch(patch)

        print_debug(f"Applying config patch {patch} to queue consumer registration manager dynamic config")

        for cluster in cls.get_cluster_names():
            applied_config = cls._apply_registration_manager_config_patch(patch, cluster)
            if cluster == "primary":
                # NB(apachee): This kind of reassignment is fine.
                cls._APPLIED_QUEUE_CONSUMER_REGISTRATION_MANAGER_CONFIG = applied_config

    @staticmethod
    def _list_all_registrations(driver=None):
        if not driver:
            driver = get_driver()
        cluster_name = get("//sys/@cluster_name", driver=driver)
        config = get(f"//sys/clusters/{cluster_name}/queue_agent/queue_consumer_registration_manager", driver=driver)
        registration_table_path = config["state_read_path"]
        return [QueueConsumerRegistration.from_select(r) for r in select_rows(f"* FROM [{registration_table_path}]")]


assert QueueConsumerRegistrationManagerBase._get_registration_manager_config()["implementation"] == "async_expiring_cache"


##################################################################


class TestQueueAgentBase(QueueConsumerRegistrationManagerBase, YTEnvSetup):
    # NB(apachee): Create Queue Agent instances only on primary cluster
    NUM_QUEUE_AGENTS_PRIMARY = 1
    NUM_QUEUE_AGENTS = 0

    NUM_DISCOVERY_SERVERS = 3

    USE_DYNAMIC_TABLES = True

    USE_OLD_QUEUE_EXPORTER_IMPL = False

    BASE_QUEUE_AGENT_DYNAMIC_CONFIG = {
        "queue_agent_sharding_manager": {
            "pass_period": 100,
        },
        "queue_agent": {
            "pass_period": 100,
            "controller": {
                "pass_period": 100,
                "queue_exporter": {
                    "implementation": "new",
                    "pass_period": 100,
                    # Fixate greater value for tests in case default decreases in the future.
                    "max_exported_table_count_per_task": 10,
                    # Retry backoffs should be enabled explicitly in tests
                    "retry_backoff": {
                        "min_backoff": 1,
                        "max_backoff": 1,
                        "backoff_jitter": 0.0,
                    },
                },
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
    QUEUE_AGENT_DO_WAIT_FOR_GLOBAL_SYNC_ON_SETUP = False

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
                    "expiration_period": 0,
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

        # COMPAT(apachee): Temporary hack to ease compatability with old queue exporter implementation for the time being.
        delta_queue_agent_dynamic_config = update(
            getattr(cls, "DELTA_QUEUE_AGENT_DYNAMIC_CONFIG", dict()),
            cls._get_queue_exporter_implementation_patch("old" if getattr(cls, "USE_OLD_QUEUE_EXPORTER_IMPL") else "new")
        )
        cls._apply_dynamic_config_patch(delta_queue_agent_dynamic_config)

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

        if self.QUEUE_AGENT_DO_WAIT_FOR_GLOBAL_SYNC_ON_SETUP:
            self._wait_for_global_sync()

    def teardown_method(self, method):
        self._drop_tables()

        super(TestQueueAgentBase, self).teardown_method(method)

    @classmethod
    def _apply_dynamic_config_patch(cls, patch):
        config = get(cls.config_path)
        update_inplace(config, copy.deepcopy(patch))
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
                    raise Exception(f"diff = {calculate_object_diff(update(effective_config, config), effective_config):expected_config,actual_config}")
            return True

        wait(config_updated_on_all_instances, ignore_exceptions=True)

    def _prepare_tables(self, queue_table_schema=None, consumer_table_schema=None, **kwargs):
        print_debug(f"Preparing queue agent dynamic state tables with kwargs: {kwargs}")

        assert queue_table_schema is None and consumer_table_schema is None, \
            "Current implementation of init_queue_agent_state does not support custom schemas"
        sync_create_cells(1)
        for remote_cluster_index in range(self.NUM_REMOTE_CLUSTERS):
            sync_create_cells(1, driver=get_driver(cluster=f"remote_{remote_cluster_index}"))
        self.client = self.Env.create_native_client()
        init_queue_agent_state.create_tables_latest_version(
            self.client,
            **kwargs)

        print_debug("Prepared queue agent dynamic state tables")

    def _drop_tables(self):
        init_queue_agent_state.delete_all_tables(self.client)

    def create_queue_path(self, ctx=""):
        if ctx:
            ctx = f"-{ctx}"
        return f"//tmp/queue{ctx}-{generate_uuid()}"

    def create_producer_path(self, ctx=""):
        if ctx:
            ctx = f"-{ctx}"
        return f"//tmp/producer{ctx}-{generate_uuid()}"

    def create_consumer_path(self, ctx=""):
        if ctx:
            ctx = f"-{ctx}"
        return f"//tmp/consumer{ctx}-{generate_uuid()}"

    @staticmethod
    def _create_queue(path, partition_count=1, enable_timestamp_column=True,
                      enable_cumulative_data_weight_column=True, mount=True, max_inline_hunk_size=None, driver=None, **kwargs):
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
        queue_id = create("table", path, driver=driver, attributes=attributes)
        if partition_count != 1:
            sync_reshard_table(path, partition_count, driver=driver)
        if mount:
            sync_mount_table(path, driver=driver)

        return schema, queue_id

    # NB: This method must be kept in sync with _create_queue.
    @staticmethod
    def _make_create_queue_batch_request(path, partition_count=1, enable_timestamp_column=True,
                                         enable_cumulative_data_weight_column=True, mount=True, max_inline_hunk_size=None, **kwargs):
        assert partition_count == 1, "partition_count other than 1 is not yet implemented"
        assert mount is False

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
        return make_batch_request("create", type="table", path=path, attributes=attributes)

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
    def _execute_batch_for_all_paths(paths, create_req, process_reqs=None):
        reqs = []
        for path in paths:
            reqs.append(create_req(path))
        rsps = execute_batch(reqs)
        if process_reqs is None:
            return
        return process_reqs(rsps)

    @classmethod
    def _wait_for_table_list_tablet_state(cls, paths, state):
        while True:
            if cls._execute_batch_for_all_paths(paths, lambda path: make_batch_request("get", path=f"{path}/@tablet_state", return_only_value=True),
                                                process_reqs=lambda rsps: all(get_batch_output(rsp) == state for rsp in rsps)):
                break

    @classmethod
    def _mount_tables(cls, paths):
        cls._execute_batch_for_all_paths(paths, lambda path: make_batch_request("mount_table", path=path))
        cls._wait_for_table_list_tablet_state(paths, "mounted")

    @classmethod
    def _unmount_tables(cls, paths):
        cls._execute_batch_for_all_paths(paths, lambda path: make_batch_request("mount_table", path=path))
        cls._wait_for_table_list_tablet_state(paths, "unmounted")

    @classmethod
    def _flush_tables(cls, paths):
        cls._execute_batch_for_all_paths(paths, lambda path: make_batch_request("freeze_table", path=path))
        cls._wait_for_table_list_tablet_state(paths, "frozen")
        cls._execute_batch_for_all_paths(paths, lambda path: make_batch_request("unfreeze_table", path=path))
        cls._wait_for_table_list_tablet_state(paths, "mounted")

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

    @staticmethod
    def _get_queue_exporter_implementation_patch(queue_exporter_implementation):
        return {
            "queue_agent": {
                "controller": {
                    "queue_exporter": {
                        "implementation": queue_exporter_implementation,
                    },
                },
            },
        }

    @classmethod
    def _switch_queue_exporter_implementation(cls, queue_exporter_implementation):
        cls._apply_dynamic_config_patch(cls._get_queue_exporter_implementation_patch(queue_exporter_implementation))

    @classmethod
    def _is_old_queue_exporter_impl(cls):
        return getattr(cls, "USE_OLD_QUEUE_EXPORTER_IMPL")

    @classmethod
    def _get_default_last_export_unix_ts_field_name(cls) -> str:
        return "last_export_unix_ts" if not getattr(cls, "USE_OLD_QUEUE_EXPORTER_IMPL") else "last_exported_fragment_unix_ts"


##################################################################
