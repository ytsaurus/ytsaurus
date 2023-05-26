from yt_chaos_test_base import ChaosTestBase

from yt_commands import (authors, wait, get, set, create, sync_mount_table, create_table_replica, get_driver,
                         sync_enable_table_replica, select_rows, print_debug, check_permission, register_queue_consumer,
                         unregister_queue_consumer, list_queue_consumer_registrations, raises_yt_error, create_user,
                         sync_create_cells, remove)

from yt_env_setup import (
    Restarter,
    RPC_PROXIES_SERVICE,
)

import yt.environment.init_queue_agent_state as init_queue_agent_state

import yt_error_codes

from yt.common import update_inplace, update, YtError
from yt.yson import YsonEntity
from yt.ypath import parse_ypath

import builtins
import pytest


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


class TestConsumerRegistrations(ChaosTestBase):
    NUM_TEST_PARTITIONS = 3

    NUM_REMOTE_CLUSTERS = 2
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True
    DELTA_RPC_DRIVER_CONFIG = {
        "table_mount_cache": {
            "expire_after_successful_update_time": 0,
            "expire_after_failed_update_time": 0,
            "expire_after_access_time": 0,
            "refresh_time": 0,
        },
    }

    def _get_drivers(self, clusters=None):
        if clusters is None:
            clusters = self.get_cluster_names()
        return [get_driver(cluster=cluster) for cluster in clusters]

    def setup_method(self, method):
        super(TestConsumerRegistrations, self).setup_method(method)

        primary_cell_tag = get("//sys/@primary_cell_tag")
        for driver in self._get_drivers():
            set("//sys/tablet_cell_bundles/default/@options/clock_cluster_tag", primary_cell_tag, driver=driver)

    def _create_cells(self):
        for driver in self._get_drivers():
            sync_create_cells(1, driver=driver)

    def _create_chaos_registration_table(self, root="//tmp",
                                         schema=init_queue_agent_state.REGISTRATION_TABLE_SCHEMA):
        self._init_replicated_table_tracker()
        cell_id = self._sync_create_chaos_bundle_and_cell()
        set("//sys/chaos_cell_bundles/c/@metadata_cell_id", cell_id)

        replicated_table_options = {
            "enable_replicated_table_tracker": True,
            "min_sync_replica_count": 1,
            "tablet_cell_bundle_name_ttl": 1000,
            "tablet_cell_bundle_name_failure_interval": 100,
        }
        create("chaos_replicated_table", "//tmp/crt", attributes={
            "chaos_cell_bundle": "c",
            "replicated_table_options": replicated_table_options
        })
        card_id = get("//tmp/crt/@replication_card_id")

        data_replica_path = f"{root}/chaos_consumer_registrations"
        queue_replica_path = f"{root}/chaos_consumer_registrations_queue"

        # Configuration:
        #    - 2 sync queues (do we need more?)
        #    - async data replicas on every cluster
        #    - async queues on every cluster w/o a sync queue?

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True,
             "replica_path": data_replica_path},
            {"cluster_name": "primary", "content_type": "queue", "mode": "async", "enabled": True,
             "replica_path": queue_replica_path},
            {"cluster_name": "remote_0", "content_type": "data", "mode": "async", "enabled": True,
             "replica_path": data_replica_path},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True,
             "replica_path": queue_replica_path},
            {"cluster_name": "remote_1", "content_type": "data", "mode": "async", "enabled": True,
             "replica_path": data_replica_path},
            {"cluster_name": "remote_1", "content_type": "queue", "mode": "sync", "enabled": True,
             "replica_path": queue_replica_path},

        ]
        replica_ids = self._create_chaos_table_replicas(replicas, table_path="//tmp/crt")
        self._create_replica_tables(replicas, replica_ids, schema=schema)
        wait(lambda: all([get("#{0}/@mode".format(replica)) == "sync" for replica in replica_ids]))
        self._sync_replication_era(card_id)

        clusters = builtins.set(self.get_cluster_names())

        return {
            "local_replica_path": data_replica_path,
            "replica_clusters": clusters,
            "state_write_path": data_replica_path,
            "state_read_path": parse_ypath(f'<clusters=[{";".join(clusters)}]>{data_replica_path}'),
            "read_availability_clusters": builtins.set(),
            "write_availability_clusters": builtins.set(),
        }

    def _create_replicated_registration_table(self, root="//tmp",
                                              schema=init_queue_agent_state.REGISTRATION_TABLE_SCHEMA):
        self._create_cells()

        table_path = f"{root}/replicated_consumer_registrations"

        create("replicated_table", table_path, attributes={"dynamic": True, "schema": schema})
        sync_mount_table(table_path)

        clusters = self.get_cluster_names()
        write_cluster = clusters[0]
        read_clusters = builtins.set(clusters[1:])

        for remote_cluster in read_clusters:
            driver = get_driver(cluster=remote_cluster)
            replica_id = create_table_replica(table_path, remote_cluster, table_path)
            create("table", table_path,
                   attributes={"dynamic": True, "schema": schema, "upstream_replica_id": replica_id}, driver=driver)
            sync_mount_table(table_path, driver=driver)
            sync_enable_table_replica(replica_id)

        return {
            "local_replica_path": table_path,
            "replica_clusters": read_clusters,
            "state_write_path": parse_ypath(f"<cluster={write_cluster}>{table_path}"),
            "state_read_path": parse_ypath(f'<clusters=[{";".join(read_clusters)}]>{table_path}'),
            "read_availability_clusters": builtins.set(),
            "write_availability_clusters": {write_cluster},
        }

    def _create_simple_registration_table(self, root="//tmp",
                                          schema=init_queue_agent_state.REGISTRATION_TABLE_SCHEMA):
        self._create_cells()
        table_path = f"{root}/simple_consumer_registrations"

        create("table", table_path, attributes={"dynamic": True, "schema": schema})
        sync_mount_table(table_path)

        cluster = self.get_cluster_names()[0]

        return {
            "local_replica_path": table_path,
            "replica_clusters": {cluster},
            "state_write_path": parse_ypath(f"<cluster={cluster}>{table_path}"),
            "state_read_path": parse_ypath(f"<clusters=[{cluster}]>{table_path}"),
            "read_availability_clusters": {cluster},
            "write_availability_clusters": {cluster},
        }

    @staticmethod
    def _apply_dynamic_config_patch(patch, cluster):
        driver = get_driver(cluster=cluster)
        config_path = f"//sys/clusters/{cluster}/queue_agent/queue_consumer_registration_manager"

        config = get(config_path, driver=driver)
        update_inplace(config, patch)
        print_debug("Setting dynamic config", config)
        set(config_path, config, driver=driver)

        def config_updated():
            for proxy in get("//sys/rpc_proxies", driver=driver).keys():
                orchid_path = f"//sys/rpc_proxies/{proxy}/orchid/cluster_connection/queue_consumer_registration_manager"
                effective_config = get(f"{orchid_path}/effective_config", driver=driver)
                if update(effective_config, config) != effective_config:
                    print_debug(f"Configs differ: {update(effective_config, config)} and {effective_config}")
                    return False
            return True

        wait(config_updated)

    @staticmethod
    def _replica_registrations_are(local_replica_path, expected_registrations, driver):
        registrations = {QueueConsumerRegistration.from_select(r)
                         for r in select_rows(f"* from [{local_replica_path}]", driver=driver)}
        if registrations != expected_registrations:
            print_debug(f"Registrations differ: {registrations} and {expected_registrations}")
        return registrations == expected_registrations

    @staticmethod
    def listed_registrations_are_equal(listed_registrations, expected_registrations):
        expected_registrations = {r if isinstance(r, QueueConsumerRegistration) else QueueConsumerRegistration(*r)
                                  for r in expected_registrations}
        actual_registrations = {QueueConsumerRegistration.from_list_registrations(r) for r in listed_registrations}

        if actual_registrations != expected_registrations:
            print_debug(f"Listed registrations differ: {actual_registrations} and {expected_registrations}")
            return False

        return True

    @staticmethod
    def _cached_registrations_are(expected_registrations, driver):
        TestConsumerRegistrations.listed_registrations_are_equal(
            list_queue_consumer_registrations(driver=driver), expected_registrations)

        for proxy in get("//sys/rpc_proxies", driver=driver).keys():
            orchid_path = f"//sys/rpc_proxies/{proxy}/orchid/cluster_connection/queue_consumer_registration_manager"
            orchid_registrations = {
                QueueConsumerRegistration.from_orchid(r)
                for r in get(f"{orchid_path}/registrations", driver=driver)
            }

            if orchid_registrations != expected_registrations:
                print_debug(f"Registrations differ: {orchid_registrations} and {expected_registrations}")
                return False

        return True

    def _registrations_are(self, local_replica_path, replica_clusters, expected_registrations):
        expected_registrations = {QueueConsumerRegistration(*r) for r in expected_registrations}
        replica_registrations = all(self._replica_registrations_are(local_replica_path, expected_registrations, driver)
                                    for driver in self._get_drivers(replica_clusters))
        cached_registrations = all(self._cached_registrations_are(expected_registrations, driver)
                                   for driver in self._get_drivers())
        return replica_registrations and cached_registrations

    def _apply_registration_table_config(self, config):
        config_patch = {
            "state_write_path": config["state_write_path"],
            "state_read_path": config["state_read_path"],
            "bypass_caching": False,
            "cache_refresh_period": 250,
            "configuration_refresh_period": 500,
        }

        for cluster in self.get_cluster_names():
            self._apply_dynamic_config_patch(config_patch, cluster)

    # The tests below are parametrized with a function that creates a registration table.
    # It returns a dict with the resulting configuration options:
    #     local_replica_path: path to local registration table on clusters where it exists
    #     replica_clusters: set of clusters with a local replica of the registration table
    #     state_write_path: rich path for registration manager config
    #     state_read_path: rich path for registration manager config
    #     read_availability_clusters: set of clusters required to be up for reads to be available
    #     write_availability_clusters: set of clusters required to be up for writes to be available

    @authors("achulkov2")
    @pytest.mark.parametrize("create_registration_table", [
        _create_simple_registration_table,
        _create_replicated_registration_table,
        _create_chaos_registration_table,
    ])
    def test_api_and_permissions(self, create_registration_table):
        config = create_registration_table(self)
        self._apply_registration_table_config(config)

        local_replica_path = config["local_replica_path"]
        replica_clusters = config["replica_clusters"]

        attrs = {"dynamic": True, "schema": [{"name": "a", "type": "string"}]}
        create("table", "//tmp/q", attributes=attrs)
        create("table", "//tmp/c1", attributes=attrs)
        create("table", "//tmp/c2", attributes=attrs)

        set("//tmp/q/@inherit_acl", False)
        set("//tmp/c1/@inherit_acl", False)
        set("//tmp/c2/@inherit_acl", False)

        create_user("egor")
        create_user("bulat")
        create_user("yura")

        # Nobody is allowed to register consumers yet.
        for consumer, user in zip(("//tmp/c1", "//tmp/c2"), ("egor", "bulat", "yura")):
            with raises_yt_error(code=yt_error_codes.AuthorizationErrorCode):
                register_queue_consumer("//tmp/q", consumer, vital=False, authenticated_user=user)

        set("//tmp/q/@acl/end", {"action": "allow", "permissions": ["register_queue_consumer"], "vital": True,
                                 "subjects": ["egor"]})
        assert check_permission("egor", "register_queue_consumer", "//tmp/q", vital=True)["action"] == "allow"

        set("//tmp/q/@acl/end", {"action": "allow", "permissions": ["register_queue_consumer"], "vital": True,
                                 "subjects": ["bulat"]})
        assert check_permission("bulat", "register_queue_consumer", "//tmp/q", vital=True)["action"] == "allow"

        set("//tmp/q/@acl/end", {"action": "allow", "permissions": ["register_queue_consumer"], "vital": False,
                                 "subjects": ["yura"]})
        assert check_permission("yura", "register_queue_consumer", "//tmp/q", vital=False)["action"] == "allow"
        assert check_permission("yura", "register_queue_consumer", "//tmp/q", vital=True)["action"] == "deny"

        # Yura is not allowed to register vital consumers.
        with raises_yt_error(code=yt_error_codes.AuthorizationErrorCode):
            register_queue_consumer("//tmp/q", "//tmp/c2", vital=True, authenticated_user="yura")

        register_queue_consumer("//tmp/q", "//tmp/c1", vital=True, authenticated_user="bulat")
        register_queue_consumer("//tmp/q", "//tmp/c2", vital=False, authenticated_user="yura")
        wait(lambda: self._registrations_are(local_replica_path, replica_clusters, {
            ("primary", "//tmp/q", "primary", "//tmp/c1", True),
            ("primary", "//tmp/q", "primary", "//tmp/c2", False)
        }))

        # Remove permissions on either the queue or the consumer are needed.
        with raises_yt_error(code=yt_error_codes.AuthorizationErrorCode):
            unregister_queue_consumer("//tmp/q", "//tmp/c1", authenticated_user="bulat")

        set("//tmp/c1/@acl/end", {"action": "allow", "permissions": ["remove"], "subjects": ["bulat"]})

        # Bulat can unregister his own consumer.
        unregister_queue_consumer("//tmp/q", "//tmp/c1", authenticated_user="bulat")

        wait(lambda: self._registrations_are(local_replica_path, replica_clusters, {
            ("primary", "//tmp/q", "primary", "//tmp/c2", False)
        }))

        # Bulat cannot unregister Yura's consumer.
        with raises_yt_error(code=yt_error_codes.AuthorizationErrorCode):
            unregister_queue_consumer("//tmp/q", "//tmp/c2", authenticated_user="bulat")

        # Remove permissions on either the queue or the consumer are needed.
        with raises_yt_error(code=yt_error_codes.AuthorizationErrorCode):
            unregister_queue_consumer("//tmp/q", "//tmp/c2", authenticated_user="egor")

        set("//tmp/q/@acl/end", {"action": "allow", "permissions": ["remove"], "subjects": ["egor"]})

        # Now Egor can unregister any consumer to his queue.
        unregister_queue_consumer("//tmp/q", "//tmp/c2", authenticated_user="egor")

        wait(lambda: self._registrations_are(local_replica_path, replica_clusters, builtins.set()))

        register_queue_consumer("//tmp/q", "//tmp/c2", vital=True)
        remove("//tmp/q")
        # Bulat can unregister any consumer to a deleted queue.
        # No neat solution here, we cannot check permissions on deleted objects :(
        self._insistent_call(lambda: unregister_queue_consumer("//tmp/q", "//tmp/c2", authenticated_user="bulat"))
        wait(lambda: self._registrations_are(local_replica_path, replica_clusters, builtins.set()))

    @authors("achulkov2")
    @pytest.mark.parametrize("create_registration_table", [
        _create_simple_registration_table,
    ])
    def test_list_registrations(self, create_registration_table):
        config = create_registration_table(self)
        self._apply_registration_table_config(config)

        attrs = {"dynamic": True, "schema": [{"name": "a", "type": "string"}]}
        create("table", "//tmp/q1", attributes=attrs)
        create("table", "//tmp/q2", attributes=attrs)
        create("table", "//tmp/c1", attributes=attrs)
        create("table", "//tmp/c2", attributes=attrs)

        register_queue_consumer("//tmp/q1", "//tmp/c1", vital=True)
        register_queue_consumer("//tmp/q1", "//tmp/c2", vital=False)
        register_queue_consumer("//tmp/q2", "//tmp/c1", vital=True, partitions=[1, 5, 4, 3])

        wait(lambda: self.listed_registrations_are_equal(
            list_queue_consumer_registrations(queue_path="//tmp/q1", consumer_path="//tmp/c1"),
            [
                ("primary", "//tmp/q1", "primary", "//tmp/c1", True),
            ]
        ))

        wait(lambda: self.listed_registrations_are_equal(
            list_queue_consumer_registrations(queue_path="//tmp/q1"),
            [
                ("primary", "//tmp/q1", "primary", "//tmp/c1", True),
                ("primary", "//tmp/q1", "primary", "//tmp/c2", False),
            ]
        ))

        wait(lambda: self.listed_registrations_are_equal(
            list_queue_consumer_registrations(consumer_path="//tmp/c1"),
            [
                ("primary", "//tmp/q1", "primary", "//tmp/c1", True),
                ("primary", "//tmp/q2", "primary", "//tmp/c1", True, (1, 5, 4, 3)),
            ]
        ))

        wait(lambda: self.listed_registrations_are_equal(
            list_queue_consumer_registrations(consumer_path="<cluster=primary>//tmp/c2"),
            [
                ("primary", "//tmp/q1", "primary", "//tmp/c2", False),
            ]
        ))

        wait(lambda: self.listed_registrations_are_equal(
            list_queue_consumer_registrations(),
            [
                ("primary", "//tmp/q1", "primary", "//tmp/c1", True),
                ("primary", "//tmp/q1", "primary", "//tmp/c2", False),
                ("primary", "//tmp/q2", "primary", "//tmp/c1", True, (1, 5, 4, 3)),
            ]
        ))

        wait(lambda: self.listed_registrations_are_equal(
            list_queue_consumer_registrations(queue_path="<cluster=remote_0>//tmp/q1"),
            []
        ))

        unregister_queue_consumer("//tmp/q1", "//tmp/c1")
        unregister_queue_consumer("//tmp/q1", "//tmp/c2")
        unregister_queue_consumer("//tmp/q2", "//tmp/c1")

        wait(lambda: self.listed_registrations_are_equal(list_queue_consumer_registrations(), []))

    def _restart_service(self, service):
        for env in [self.Env] + self.remote_envs:
            with Restarter(env, service):
                pass

    @staticmethod
    def _insistent_call(functor):
        def do():
            try:
                functor()
                return True
            except YtError as err:
                print_debug("Call failed", err)
                return False

        wait(do)

    @authors("achulkov2")
    @pytest.mark.parametrize("create_registration_table", [
        _create_simple_registration_table,
        _create_replicated_registration_table,
        _create_chaos_registration_table,
    ])
    def test_write_availability(self, create_registration_table):
        config = create_registration_table(self)
        self._apply_registration_table_config(config)

        local_replica_path = config["local_replica_path"]
        replica_clusters = config["replica_clusters"]

        attrs = {"dynamic": True, "schema": [{"name": "a", "type": "string"}]}

        clusters = builtins.set(self.get_cluster_names())

        table_paths = {cluster: {} for cluster in clusters}

        for write_cluster in clusters:
            for dead_cluster in clusters:
                driver = get_driver(cluster=write_cluster)
                queue_path = f"//tmp/q_{write_cluster}_{dead_cluster}"
                consumer_path = f"//tmp/c_{write_cluster}_{dead_cluster}"
                create("table", queue_path, attributes=attrs, driver=driver)
                create("table", consumer_path, attributes=attrs, driver=driver)
                table_paths[write_cluster][dead_cluster] = queue_path, consumer_path

        expected_registrations = builtins.set()

        for cluster in clusters - config["write_availability_clusters"]:
            with self.CellsDisabled(clusters=[cluster], tablet_bundles=["default"]):
                print_debug(f"Disabled cells on {cluster}")

                for write_cluster in clusters:
                    queue_path, consumer_path = table_paths[write_cluster][cluster]
                    self._insistent_call(lambda: register_queue_consumer(
                        queue_path, consumer_path, vital=True, driver=get_driver(cluster=write_cluster)))
                    expected_registrations.add((write_cluster, queue_path, write_cluster, consumer_path, True))

                wait(lambda: self._registrations_are(local_replica_path,
                                                     replica_clusters - {cluster},
                                                     expected_registrations),
                     ignore_exceptions=True)

    @authors("achulkov2")
    @pytest.mark.parametrize("create_registration_table", [
        _create_simple_registration_table,
        _create_replicated_registration_table,
        _create_chaos_registration_table,
    ])
    def test_read_availability(self, create_registration_table):
        config = create_registration_table(self)
        self._apply_registration_table_config(config)

        local_replica_path = config["local_replica_path"]
        replica_clusters = config["replica_clusters"]

        # Currently, register_queue_consumer must be called via the queue's cluster.
        queue_cluster = "primary"
        consumer_cluster = self.get_cluster_names()[1]

        attrs = {"dynamic": True, "schema": [{"name": "a", "type": "string"}]}
        create("table", "//tmp/q", attributes=attrs, driver=get_driver(cluster=queue_cluster))
        create("table", "//tmp/c", attributes=attrs, driver=get_driver(cluster=consumer_cluster))
        register_queue_consumer(f"<cluster={queue_cluster}>//tmp/q", f"<cluster={consumer_cluster}>//tmp/c", vital=True,
                                driver=get_driver(cluster=queue_cluster))

        wait(lambda: self._registrations_are(local_replica_path, replica_clusters, {
            (queue_cluster, "//tmp/q", consumer_cluster, "//tmp/c", True),
        }))

        for cluster in builtins.set(self.get_cluster_names()) - config["read_availability_clusters"]:
            with self.CellsDisabled(clusters=[cluster], tablet_bundles=["default"]):
                print_debug(f"Disabled cells on {cluster}")

                # This will clear caches.
                self._restart_service(RPC_PROXIES_SERVICE)

                wait(lambda: self._registrations_are(local_replica_path, replica_clusters - {cluster}, {
                    (queue_cluster, "//tmp/q", consumer_cluster, "//tmp/c", True),
                }), ignore_exceptions=True)
