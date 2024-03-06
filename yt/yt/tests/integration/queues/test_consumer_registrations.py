from yt_queue_agent_test_base import (TestQueueAgentBase, ReplicatedObjectBase, CypressSynchronizerOrchid)

from yt_commands import (authors, wait, get, set, create, sync_mount_table, get_driver, select_rows, print_debug, link,
                         check_permission, register_queue_consumer, unregister_queue_consumer, commit_transaction,
                         list_queue_consumer_registrations, raises_yt_error, create_user, sync_create_cells, remove,
                         pull_queue, pull_consumer, advance_consumer, insert_rows, start_transaction)

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


class TestQueueConsumerApiBase(ReplicatedObjectBase):
    def _get_drivers(self, clusters=None):
        if clusters is None:
            clusters = self.get_cluster_names()
        return [get_driver(cluster=cluster) for cluster in clusters]

    def setup_method(self, method):
        # NB: Make sure this is evaluated before creating cells.

        super(TestQueueConsumerApiBase, self).setup_method(method)

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

        replica_ids, card_id = self._create_chaos_replicated_table_base(
            "//tmp/crt", replicas, schema, replicated_table_options, sync_replication_era=False)
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

        clusters = self.get_cluster_names()
        write_cluster = clusters[0]
        read_clusters = builtins.set(clusters[1:])

        replicas = [{"replica_path": table_path, "cluster_name": remote_cluster, "enabled": True}
                    for remote_cluster in read_clusters]

        self._create_replicated_table_base(table_path, replicas, schema)

        return {
            "local_replica_path": table_path,
            "replica_clusters": read_clusters,
            "state_write_path": parse_ypath(f"{write_cluster}:{table_path}"),
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
            "state_write_path": parse_ypath(f"{cluster}:{table_path}"),
            "state_read_path": parse_ypath(f"<clusters=[{cluster}]>{table_path}"),
            "read_availability_clusters": {cluster},
            "write_availability_clusters": {cluster},
        }

    @staticmethod
    def _apply_registration_manager_dynamic_config_patch(patch, cluster):
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

    def _apply_registration_table_config(self, config):
        config_patch = {
            "state_write_path": config["state_write_path"],
            "state_read_path": config["state_read_path"],
            "bypass_caching": config.get("bypass_caching", False),
            "cache_refresh_period": 250,
            "configuration_refresh_period": 500,
        }

        for cluster in self.get_cluster_names():
            self._apply_registration_manager_dynamic_config_patch(config_patch, cluster)


class TestConsumerRegistrations(TestQueueConsumerApiBase):
    NUM_TEST_PARTITIONS = 4

    NUM_REMOTE_CLUSTERS = 2
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

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

    def form_queue_name(self, queue, cluster='', root='//tmp'):
        queue_obj = f"{root}/{queue}-{cluster}"
        return self.form_path(queue_obj, cluster)

    def form_path(self, object, cluster=''):
        if len(cluster) > 0:
            return f'{cluster}:{object}'
        return object
    # The tests below are parametrized with a function that creates a registration table.
    # It returns a dict with the resulting configuration options:
    #     local_replica_path: path to local registration table on clusters where it exists
    #     replica_clusters: set of clusters with a local replica of the registration table
    #     state_write_path: rich path for registration manager config
    #     state_read_path: rich path for registration manager config
    #     read_availability_clusters: set of clusters required to be up for reads to be available
    #     write_availability_clusters: set of clusters required to be up for writes to be available

    @authors("nadya73")
    @pytest.mark.parametrize("create_registration_table", [
        TestQueueConsumerApiBase._create_simple_registration_table,
        TestQueueConsumerApiBase._create_replicated_registration_table,
        TestQueueConsumerApiBase._create_chaos_registration_table,
    ])
    @pytest.mark.timeout(150)
    def test_api_and_permissions(self, create_registration_table):
        config = create_registration_table(self)
        self._apply_registration_table_config(config)

        local_replica_path = config["local_replica_path"]
        replica_clusters = config["replica_clusters"]
        clusters = self.get_cluster_names()
        consumer_cluster = "primary"

        attrs = {"dynamic": True, "schema": [{"name": "a", "type": "string"}]}
        create("table", "//tmp/c1", attributes=attrs)
        create("table", "//tmp/c2", attributes=attrs)
        for cluster in clusters:
            create("table", self.form_queue_name("q", cluster), attributes=attrs, driver=get_driver(cluster=cluster))

        set("//tmp/c1/@inherit_acl", False)
        set("//tmp/c2/@inherit_acl", False)
        for cluster in clusters:
            queue_name = self.form_queue_name("q", cluster)
            set(f"{queue_name}/@inherit_acl", False, driver=get_driver(cluster=cluster))

        for cluster in clusters:
            create_user("egor", driver=get_driver(cluster=cluster))
            create_user("bulat", driver=get_driver(cluster=cluster))
            create_user("yura", driver=get_driver(cluster=cluster))

        # Nobody is allowed to register consumers yet.
        for consumer, user in zip(("//tmp/c1", "//tmp/c2"), ("egor", "bulat", "yura")):
            for cluster in clusters:
                with raises_yt_error(code=yt_error_codes.AuthorizationErrorCode):
                    register_queue_consumer(self.form_queue_name("q", cluster), self.form_path(consumer, consumer_cluster),
                                            vital=False, authenticated_user=user, driver=get_driver(cluster=cluster))
        for cluster in clusters:
            queue = self.form_queue_name("q", cluster)
            set(f"{queue}/@acl/end", {"action": "allow", "permissions": ["register_queue_consumer"], "vital": True,
                                      "subjects": ["egor"]}, driver=get_driver(cluster=cluster))
            assert check_permission("egor", "register_queue_consumer", queue, vital=True, driver=get_driver(cluster=cluster))["action"] == "allow"

            set(f"{queue}/@acl/end", {"action": "allow", "permissions": ["register_queue_consumer"], "vital": True,
                                      "subjects": ["bulat"]}, driver=get_driver(cluster=cluster))
            assert check_permission("bulat", "register_queue_consumer", queue, vital=True, driver=get_driver(cluster=cluster))["action"] == "allow"

            set(f"{queue}/@acl/end", {"action": "allow", "permissions": ["register_queue_consumer"], "vital": False,
                                      "subjects": ["yura"]}, driver=get_driver(cluster=cluster))
            assert check_permission("yura", "register_queue_consumer", queue, vital=False, driver=get_driver(cluster=cluster))["action"] == "allow"
            assert check_permission("yura", "register_queue_consumer", queue, vital=True, driver=get_driver(cluster=cluster))["action"] == "deny"

        # Yura is not allowed to register vital consumers.
        for cluster in clusters:
            with raises_yt_error(code=yt_error_codes.AuthorizationErrorCode):
                register_queue_consumer(self.form_queue_name("q", cluster), self.form_path("//tmp/c2", consumer_cluster),
                                        vital=True, authenticated_user="yura", driver=get_driver(cluster=cluster))

        registrations = builtins.set()
        for cluster in clusters:
            queue_name = f"//tmp/q-{cluster}"
            register_queue_consumer(self.form_queue_name("q", cluster), self.form_path("//tmp/c1", consumer_cluster),
                                    vital=True, authenticated_user="bulat", driver=get_driver(cluster=consumer_cluster))
            register_queue_consumer(self.form_queue_name("q", cluster), self.form_path("//tmp/c2", consumer_cluster),
                                    vital=False, authenticated_user="yura", driver=get_driver(cluster=consumer_cluster))
            registrations.add((cluster, queue_name, "primary", "//tmp/c1", True))
            registrations.add((cluster, queue_name, "primary", "//tmp/c2", False))
        wait(lambda: self._registrations_are(local_replica_path, replica_clusters, registrations))

        # Remove permissions on either the queue or the consumer are needed.
        for cluster in clusters:
            with raises_yt_error(code=yt_error_codes.AuthorizationErrorCode):
                unregister_queue_consumer(self.form_queue_name("q", cluster), self.form_path("//tmp/c1", consumer_cluster),
                                          authenticated_user="bulat", driver=get_driver(cluster=cluster))

        set("//tmp/c1/@acl/end", {"action": "allow", "permissions": ["remove"], "subjects": ["bulat"]})
        # Bulat can unregister his own consumer.
        for cluster in clusters:
            queue_name = f"//tmp/q-{cluster}"
            unregister_queue_consumer(self.form_queue_name("q", cluster), self.form_path("//tmp/c1", consumer_cluster),
                                      authenticated_user="bulat", driver=get_driver(cluster=consumer_cluster))
            registrations.remove((cluster, queue_name, "primary", "//tmp/c1", True))
        wait(lambda: self._registrations_are(local_replica_path, replica_clusters, registrations))

        # Bulat cannot unregister Yura's consumer.
        for cluster in clusters:
            with raises_yt_error(code=yt_error_codes.AuthorizationErrorCode):
                unregister_queue_consumer(self.form_queue_name("q", cluster), self.form_path("//tmp/c2", consumer_cluster),
                                          authenticated_user="bulat", driver=get_driver(cluster=cluster))

        # Remove permissions on either the queue or the consumer are needed.
        for cluster in clusters:
            with raises_yt_error(code=yt_error_codes.AuthorizationErrorCode):
                unregister_queue_consumer(self.form_queue_name("q", cluster), self.form_path("//tmp/c2", consumer_cluster),
                                          authenticated_user="egor", driver=get_driver(cluster=cluster))

        for cluster in clusters:
            set(f"{self.form_queue_name('q', cluster)}/@acl/end", {"action": "allow", "permissions": ["remove"], "subjects": ["egor"]}, driver=get_driver(cluster=cluster))

        # Now Egor can unregister any consumer to his queue.
        for cluster in clusters:
            unregister_queue_consumer(self.form_queue_name("q", cluster), self.form_path("//tmp/c2", consumer_cluster),
                                      authenticated_user="egor", driver=get_driver(cluster=consumer_cluster))
        wait(lambda: self._registrations_are(local_replica_path, replica_clusters, builtins.set()))

        # Users can register and unregister queue consumer from any cluster.
        queue_cluster = clusters[1]
        registrations = builtins.set()
        for cluster in clusters:
            queue_name = f"//tmp/q-{queue_cluster}"
            register_queue_consumer(self.form_queue_name("q", queue_cluster), self.form_path("//tmp/c1", consumer_cluster),
                                    vital=True, authenticated_user="bulat", driver=get_driver(cluster=cluster))
            register_queue_consumer(self.form_queue_name("q", queue_cluster), self.form_path("//tmp/c2", consumer_cluster),
                                    vital=False, authenticated_user="egor", driver=get_driver(cluster=cluster))
            wait(lambda: self._registrations_are(local_replica_path, replica_clusters, {
                (queue_cluster, queue_name, consumer_cluster, "//tmp/c1", True),
                (queue_cluster, queue_name, consumer_cluster, "//tmp/c2", False)
            }))
            unregister_queue_consumer(self.form_queue_name("q", queue_cluster), self.form_path("//tmp/c1", consumer_cluster),
                                      authenticated_user="bulat", driver=get_driver(cluster=cluster))
            unregister_queue_consumer(self.form_queue_name("q", queue_cluster), self.form_path("//tmp/c2", consumer_cluster),
                                      authenticated_user="egor", driver=get_driver(cluster=cluster))
            wait(lambda: self._registrations_are(local_replica_path, replica_clusters, builtins.set()))

        for cluster in clusters:
            register_queue_consumer(self.form_queue_name('q', cluster), self.form_path("//tmp/c2", consumer_cluster), vital=True, driver=get_driver(cluster=consumer_cluster))
            remove(self.form_queue_name('q', cluster), driver=get_driver(cluster=cluster))
        # Bulat can unregister any consumer to a deleted queue.
        # No neat solution here, we cannot check permissions on deleted objects :(
        for cluster in clusters:
            self._insistent_call(lambda: unregister_queue_consumer(self.form_queue_name('q', cluster), self.form_path("//tmp/c2", consumer_cluster),
                                                                   authenticated_user="bulat", driver=get_driver(cluster=consumer_cluster)))
        wait(lambda: self._registrations_are(local_replica_path, replica_clusters, builtins.set()))

    @authors("nadya73")
    @pytest.mark.parametrize("create_registration_table", [
        TestQueueConsumerApiBase._create_simple_registration_table,
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
            list_queue_consumer_registrations(consumer_path="primary://tmp/c2"),
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
            list_queue_consumer_registrations(queue_path="remote_0://tmp/q1"),
            []
        ))

        # tests for paths with attributes
        wait(lambda: self.listed_registrations_are_equal(
            list_queue_consumer_registrations(queue_path="<append=true>//tmp/q1[#10:#100]", consumer_path="<append=true>//tmp/c1[#20:#200]"),
            [
                ("primary", "//tmp/q1", "primary", "//tmp/c1", True),
            ]
        ))

        wait(lambda: self.listed_registrations_are_equal(
            list_queue_consumer_registrations(queue_path="<append=true>//tmp/q1[#10:#100]"),
            [
                ("primary", "//tmp/q1", "primary", "//tmp/c1", True),
                ("primary", "//tmp/q1", "primary", "//tmp/c2", False),
            ]
        ))

        wait(lambda: self.listed_registrations_are_equal(
            list_queue_consumer_registrations(consumer_path="<append=true>//tmp/c1[#20:#200]"),
            [
                ("primary", "//tmp/q1", "primary", "//tmp/c1", True),
                ("primary", "//tmp/q2", "primary", "//tmp/c1", True, (1, 5, 4, 3)),
            ]
        ))

        wait(lambda: self.listed_registrations_are_equal(
            list_queue_consumer_registrations(consumer_path="<append=true>primary://tmp/c2[#20:#200]"),
            [
                ("primary", "//tmp/q1", "primary", "//tmp/c2", False),
            ]
        ))

        unregister_queue_consumer("//tmp/q1", "//tmp/c1")
        unregister_queue_consumer("//tmp/q1", "//tmp/c2")
        unregister_queue_consumer("//tmp/q2", "//tmp/c1")

        wait(lambda: self.listed_registrations_are_equal(list_queue_consumer_registrations(), []))

    @authors("cherepashka")
    @pytest.mark.parametrize("create_registration_table", [
        TestQueueConsumerApiBase._create_simple_registration_table,
    ])
    def test_list_registrations_for_symlinks(self, create_registration_table):
        config = create_registration_table(self)
        self._apply_registration_table_config(config)

        attrs = {"dynamic": True, "schema": [{"name": "a", "type": "string"}]}
        create("table", "//tmp/q1", attributes=attrs)
        create("table", "//tmp/q2", attributes=attrs)
        create("table", "//tmp/c1", attributes=attrs)
        create("table", "//tmp/c2", attributes=attrs)

        link("//tmp/q1", "//tmp/q1-link")
        link("//tmp/q2", "//tmp/q2-link")
        link("//tmp/c1", "//tmp/c1-link")
        link("//tmp/c2", "//tmp/c2-link")

        register_queue_consumer("//tmp/q1-link", "//tmp/c1-link", vital=True)
        register_queue_consumer("//tmp/q1-link", "//tmp/c2-link", vital=False)
        register_queue_consumer("//tmp/q2-link", "//tmp/c1-link", vital=True, partitions=[1, 5, 4, 3])

        wait(lambda: self.listed_registrations_are_equal(
            list_queue_consumer_registrations(queue_path="//tmp/q1-link", consumer_path="//tmp/c1-link"),
            [
                ("primary", "//tmp/q1", "primary", "//tmp/c1", True),
            ]
        ))

        wait(lambda: self.listed_registrations_are_equal(
            list_queue_consumer_registrations(queue_path="//tmp/q1-link"),
            [
                ("primary", "//tmp/q1", "primary", "//tmp/c1", True),
                ("primary", "//tmp/q1", "primary", "//tmp/c2", False),
            ]
        ))

        wait(lambda: self.listed_registrations_are_equal(
            list_queue_consumer_registrations(consumer_path="//tmp/c1-link"),
            [
                ("primary", "//tmp/q1", "primary", "//tmp/c1", True),
                ("primary", "//tmp/q2", "primary", "//tmp/c1", True, (1, 5, 4, 3)),
            ]
        ))

        wait(lambda: self.listed_registrations_are_equal(
            list_queue_consumer_registrations(consumer_path="primary://tmp/c2-link"),
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
            list_queue_consumer_registrations(queue_path="remote_0://tmp/q1-link"),
            []
        ))

        unregister_queue_consumer("//tmp/q1-link", "//tmp/c1-link")
        unregister_queue_consumer("//tmp/q1-link", "//tmp/c2-link")
        unregister_queue_consumer("//tmp/q2-link", "//tmp/c1-link")

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

    @authors("nadya73")
    @pytest.mark.parametrize("create_registration_table", [
        TestQueueConsumerApiBase._create_simple_registration_table,
        TestQueueConsumerApiBase._create_replicated_registration_table,
        TestQueueConsumerApiBase._create_chaos_registration_table,
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

    @authors("nadya73")
    @pytest.mark.parametrize("create_registration_table", [
        TestQueueConsumerApiBase._create_simple_registration_table,
        TestQueueConsumerApiBase._create_replicated_registration_table,
        TestQueueConsumerApiBase._create_chaos_registration_table,
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
        register_queue_consumer(f"{queue_cluster}://tmp/q", f"{consumer_cluster}://tmp/c", vital=True,
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

    @authors("cherepashka")
    @pytest.mark.parametrize("create_registration_table", [
        TestQueueConsumerApiBase._create_simple_registration_table,
    ])
    def test_symlink_registrations(self, create_registration_table):
        config = create_registration_table(self)
        self._apply_registration_table_config(config)

        local_replica_path = config["local_replica_path"]
        replica_clusters = config["replica_clusters"]

        cell_id = self._sync_create_chaos_bundle_and_cell(name="cb")
        set("//sys/chaos_cell_bundles/cb/@metadata_cell_id", cell_id)

        create("table", "//tmp/q", attributes={"dynamic": True, "schema": TestDataApi.DEFAULT_QUEUE_SCHEMA})
        create("replicated_table", "//tmp/rep_q", attributes={"dynamic": True, "schema": TestDataApi.DEFAULT_QUEUE_SCHEMA})
        create("chaos_replicated_table",
               "//tmp/chaos_rep_q",
               attributes={"chaos_cell_bundle": "cb",
                           "schema": TestDataApi.DEFAULT_QUEUE_SCHEMA})

        create("table",
               "//tmp/c",
               attributes={"dynamic": True,
                           "schema": init_queue_agent_state.CONSUMER_OBJECT_TABLE_SCHEMA,
                           "treat_as_queue_consumer": True})
        create("replicated_table",
               "//tmp/rep_c",
               attributes={"dynamic": True,
                           "schema": init_queue_agent_state.CONSUMER_OBJECT_TABLE_SCHEMA,
                           "treat_as_queue_consumer": True})
        create("chaos_replicated_table",
               "//tmp/chaos_rep_c",
               attributes={"chaos_cell_bundle": "cb",
                           "schema": init_queue_agent_state.CONSUMER_OBJECT_TABLE_SCHEMA,
                           "treat_as_queue_consumer": True})

        for (queue, consumer) in (("//tmp/q", "//tmp/c"), ("//tmp/rep_q", "//tmp/rep_c"), ("//tmp/chaos_rep_q", "//tmp/chaos_rep_c")):
            link(queue, f"{queue}-link")
            register_queue_consumer(f"{queue}-link", consumer, vital=True)
            wait(lambda: self._registrations_are(local_replica_path, replica_clusters, {
                ("primary", queue, "primary", consumer, True),
            }))
            unregister_queue_consumer(f"{queue}-link", consumer)
            wait(lambda: self._registrations_are(local_replica_path, replica_clusters, builtins.set()))

            link(consumer, f"{consumer}-link")
            register_queue_consumer(queue, f"{consumer}-link", vital=True)
            wait(lambda: self._registrations_are(local_replica_path, replica_clusters, {
                ("primary", queue, "primary", consumer, True),
            }))
            unregister_queue_consumer(queue, f"{consumer}-link")
            wait(lambda: self._registrations_are(local_replica_path, replica_clusters, builtins.set()))

            register_queue_consumer(f"{queue}-link", f"{consumer}-link", vital=True)
            wait(lambda: self._registrations_are(local_replica_path, replica_clusters, {
                ("primary", queue, "primary", consumer, True),
            }))
            unregister_queue_consumer(f"{queue}-link", f"{consumer}-link")
            wait(lambda: self._registrations_are(local_replica_path, replica_clusters, builtins.set()))

            remove(f"{queue}-link")
            remove(f"{consumer}-link")

    @authors("nadya02")
    @pytest.mark.parametrize("create_registration_table", [
        TestQueueConsumerApiBase._create_simple_registration_table,
    ])
    def test_normalize_cluster(self, create_registration_table):
        config = create_registration_table(self)
        self._apply_registration_table_config(config)

        attrs = {"dynamic": True, "schema": [{"name": "a", "type": "string"}]}
        create("table", "//tmp/q1", attributes=attrs)
        create("table", "//tmp/c1", attributes=attrs)

        register_queue_consumer("<cluster=cluster.yt.yandex.net>//tmp/q1",  "<cluster=cluster.yt.yandex.net>//tmp/c1", vital=True)

        wait(lambda: self.listed_registrations_are_equal(
            list_queue_consumer_registrations(),
            [
                ("cluster", "//tmp/q1", "cluster", "//tmp/c1", True),
            ]
        ))

    @authors("cherepashka")
    @pytest.mark.parametrize("create_registration_table", [
        TestQueueConsumerApiBase._create_simple_registration_table,
    ])
    def test_multicluster_symlink_registrations(self, create_registration_table):
        config = create_registration_table(self)
        self._apply_registration_table_config(config)

        local_replica_path = config["local_replica_path"]
        replica_clusters = config["replica_clusters"]

        _, queue_cluster, consumer_cluster = self.get_cluster_names()

        create("table",
               f"{queue_cluster}://tmp/q",
               attributes={"dynamic": True, "schema": [{"name": "a", "type": "string"}]},
               driver=get_driver(cluster=queue_cluster))
        create("table",
               f"{consumer_cluster}://tmp/c",
               attributes={"dynamic": True,
                           "schema": init_queue_agent_state.CONSUMER_OBJECT_TABLE_SCHEMA,
                           "treat_as_queue_consumer": True},
               driver=get_driver(cluster=consumer_cluster))

        link(f"{queue_cluster}://tmp/q", f"{queue_cluster}://tmp/q-link", driver=get_driver(cluster=queue_cluster))
        link(f"{consumer_cluster}://tmp/c", f"{consumer_cluster}://tmp/c-link", driver=get_driver(cluster=consumer_cluster))

        register_queue_consumer(f"{queue_cluster}://tmp/q-link", f"{consumer_cluster}://tmp/c-link", vital=True)
        wait(lambda: self._registrations_are(local_replica_path, replica_clusters, {
            (queue_cluster, "//tmp/q", consumer_cluster, "//tmp/c", True),
        }))
        unregister_queue_consumer(f"{queue_cluster}://tmp/q-link", f"{consumer_cluster}://tmp/c-link")
        wait(lambda: self._registrations_are(local_replica_path, replica_clusters, builtins.set()))


class TestDataApi(TestQueueConsumerApiBase, ReplicatedObjectBase, TestQueueAgentBase):
    NUM_TEST_PARTITIONS = 2

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

    DO_PREPARE_TABLES_ON_SETUP = False

    def setup_method(self, method):
        super(TestDataApi, self).setup_method(method)

        cell_id = self._sync_create_chaos_bundle_and_cell()
        set("//sys/chaos_cell_bundles/c/@metadata_cell_id", cell_id)

        self._prepare_tables()

    # No tear down, since //tmp is cleared automatically.

    DEFAULT_QUEUE_SCHEMA = [
        {"name": "$timestamp", "type": "uint64"},
        {"name": "$cumulative_data_weight", "type": "int64"},
        {"name": "data", "type": "string"},
    ]

    @staticmethod
    def _create_queue(path, schema=None, mount=True, **kwargs):
        attributes = {
            "dynamic": True,
            "schema": schema if schema is not None else TestDataApi.DEFAULT_QUEUE_SCHEMA,
        }
        attributes.update(kwargs)
        create("table", path, attributes=attributes)
        if mount:
            sync_mount_table(path)

    @staticmethod
    def _create_consumer(path, mount=True, without_meta=False, **kwargs):
        attributes = {
            "dynamic": True,
            "schema": init_queue_agent_state.CONSUMER_OBJECT_TABLE_SCHEMA_WITHOUT_META if without_meta else init_queue_agent_state.CONSUMER_OBJECT_TABLE_SCHEMA,
            "treat_as_queue_consumer": True,
        }
        attributes.update(kwargs)
        create("table", path, attributes=attributes)
        if mount:
            sync_mount_table(path)

    @staticmethod
    def _create_symlink_queue(path):
        TestDataApi._create_queue(f"{path}-original")
        link(f"{path}-original", path)

    @staticmethod
    def _create_symlink_consumer(path, without_meta=False):
        TestDataApi._create_consumer(f"{path}-original", without_meta=without_meta)
        link(f"{path}-original", path)

    @staticmethod
    def _create_replicated_table(path, schema, replica_modes, **kwargs):
        replicated_table_path = f"{path}_replicated_table"
        replicas = [
            {
                "cluster_name": "primary",
                "replica_path": f"{path}_replica_{i}",
                "mode": replica_modes[i],
                "enabled": True
            } for i in range(len(replica_modes))
        ]
        replica_ids = TestDataApi._create_replicated_table_base(replicated_table_path, replicas, schema, **kwargs)

        return replicated_table_path, replicas, replica_ids

    def _create_chaos_replicated_table(self, path, schema, replica_modes, disabled_replicas=None, **kwargs):
        replicated_table_path = f"{path}_chaos_replicated_table"
        disabled_replicas = disabled_replicas or {}
        replicas = [
            {
                "cluster_name": "primary",
                "replica_path": f"{path}_chaos_queue_content_type_replica_{i}",
                "mode": replica_modes[i],
                "enabled": i not in disabled_replicas,
                "content_type": "queue",
            } for i in range(len(replica_modes))
        ] + ([
            {
                "cluster_name": "primary",
                "replica_path": f"{path}_chaos_data_content_type_replica_{i}",
                "mode": replica_modes[i],
                "enabled": True,
                "content_type": "data",
            } for i in range(len(replica_modes))
        ] if not self._is_ordered_schema(schema) else [])

        replica_ids, _ = self._create_chaos_replicated_table_base(replicated_table_path, replicas, schema, **kwargs)
        return replicated_table_path, replicas, replica_ids

    @staticmethod
    def _create_replicated_queue(path, replica_modes, **kwargs):
        return TestDataApi._create_replicated_table(path, TestDataApi.DEFAULT_QUEUE_SCHEMA, replica_modes, **kwargs)

    @staticmethod
    def _create_replicated_consumer(path, replica_modes, **kwargs):
        return TestDataApi._create_replicated_table(
            path, init_queue_agent_state.CONSUMER_OBJECT_TABLE_SCHEMA, replica_modes,
            replicated_table_attributes_patch={"treat_as_queue_consumer": True}, **kwargs)

    def _create_chaos_replicated_queue(self, path, replica_modes, **kwargs):
        return self._create_chaos_replicated_table(path, TestDataApi.DEFAULT_QUEUE_SCHEMA, replica_modes, **kwargs)

    def _create_chaos_replicated_consumer(self, path, replica_modes, **kwargs):
        return self._create_chaos_replicated_table(
            path, init_queue_agent_state.CONSUMER_OBJECT_TABLE_SCHEMA, replica_modes, **kwargs)

    @staticmethod
    def _assert_rows_contain(actual_rows, expected_rows):
        assert len(actual_rows) == len(expected_rows)

        for actual_row, expected_row in zip(actual_rows, expected_rows):
            assert update(actual_row, expected_row) == actual_row

    @staticmethod
    def _wait_assert_rows_contain(callback, expected_rows):
        def check():
            return len(callback()) == len(expected_rows)

        wait(check)

        actual_rows = callback()

        TestDataApi._assert_rows_contain(actual_rows, expected_rows)

    @authors("nadya73")
    @pytest.mark.parametrize("create_queue", [
        _create_queue,
        _create_symlink_queue,
    ])
    def test_pull_queue(self, create_queue):
        create_queue("//tmp/q")

        insert_rows("//tmp/q", [{"data": "foo"}])
        insert_rows("//tmp/q", [{"data": "bar"}])

        self._assert_rows_contain(pull_queue("//tmp/q", offset=1, partition_index=0), [
            {"$tablet_index": 0, "$row_index": 1, "data": "bar"},
        ])

        self._assert_rows_contain(pull_queue("//tmp/q", offset=0, partition_index=0, max_row_count=1), [
            {"$tablet_index": 0, "$row_index": 0, "data": "foo"},
        ])

        self._assert_rows_contain(pull_queue("//tmp/q", offset=0, partition_index=0), [
            {"$tablet_index": 0, "$row_index": 0, "data": "foo"},
            {"$tablet_index": 0, "$row_index": 1, "data": "bar"},
        ])

        self._assert_rows_contain(pull_queue("//tmp/q", offset=0, partition_index=0, max_data_weight=5), [
            {"$tablet_index": 0, "$row_index": 0, "data": "foo"},
        ])

    @authors("nadya73")
    def test_pull_replicated_queue(self):
        queue_replicated_table, queue_replicas, queue_replica_ids = self._create_replicated_queue(
            "//tmp/q", replica_modes=["sync", "async", "async"])

        set(f"{queue_replicated_table}/@inherit_acl", False)
        create_user("test")

        with raises_yt_error(code=yt_error_codes.AuthorizationErrorCode):
            pull_queue(queue_replicated_table, offset=1, partition_index=0, authenticated_user="test")

        insert_rows(queue_replicated_table, [{"data": "foo"}])
        insert_rows(queue_replicated_table, [{"data": "bar"}])

        self._assert_rows_contain(pull_queue(queue_replicated_table, offset=1, partition_index=0), [
            {"$tablet_index": 0, "$row_index": 1, "data": "bar"},
        ])

    @authors("nadya73")
    def test_pull_chaos_replicated_queue(self):
        queue_replicated_table, queue_replicas, queue_replica_ids = self._create_chaos_replicated_queue(
            "//tmp/q", replica_modes=["sync", "async", "async"], disabled_replicas={1})

        insert_rows(queue_replicated_table, [{"data": "foo", "$tablet_index": 0}])
        insert_rows(queue_replicated_table, [{"data": "bar", "$tablet_index": 0}])

        # This request should be redirected to a sync replica.
        self._wait_assert_rows_contain(lambda: pull_queue(queue_replicated_table, offset=1, partition_index=0), [
            {"$tablet_index": 0, "$row_index": 1, "data": "bar"},
        ])

        # This replica is async and disabled, so it should not contain any rows.
        self._assert_rows_contain(
            pull_queue(queue_replicas[1]["replica_path"], offset=1, partition_index=0, replica_consistency="none"), [])

        # This request should be redirected to a sync replica.
        self._assert_rows_contain(
            pull_queue(queue_replicas[1]["replica_path"], offset=1, partition_index=0, replica_consistency="sync"), [
                {"$tablet_index": 0, "$row_index": 1, "data": "bar"},
            ])

    @authors("nadya73")
    def test_pull_replicated_consumer(self):
        queue_replicated_table, queue_replicas, queue_replica_ids = self._create_replicated_queue(
            "//tmp/q", replica_modes=["sync", "async", "async"])
        consumer_replicated_table, consumer_replicas, consumer_replica_ids = self._create_replicated_consumer(
            "//tmp/c", replica_modes=["sync", "async", "async"])

        set(f"{queue_replicated_table}/@inherit_acl", False)
        create_user("test")

        insert_rows(queue_replicated_table, [{"data": "foo"}])
        insert_rows(queue_replicated_table, [{"data": "bar"}])

        register_queue_consumer(queue_replicated_table, consumer_replicated_table, vital=False)
        # Wait for registration table mapping to be filled.
        CypressSynchronizerOrchid().wait_fresh_pass()

        # This works, since we perform fallback requests to replicas under root.
        self._assert_rows_contain(
            pull_consumer(consumer_replicas[0]["replica_path"], queue_replicated_table, offset=1,
                          partition_index=0, authenticated_user="test"), [
                {"$tablet_index": 0, "$row_index": 1, "data": "bar"},
            ])

        # Thus, it is important that we check permissions before performing redirection.
        set(f"{consumer_replicated_table}/@inherit_acl", False)
        with raises_yt_error(code=yt_error_codes.AuthorizationErrorCode):
            pull_consumer(consumer_replicated_table, queue_replicated_table, offset=1, partition_index=0,
                          authenticated_user="test")

    @authors("nadya73")
    def test_pull_chaos_replicated_consumer(self):
        queue_replicated_table, queue_replicas, queue_replica_ids = self._create_chaos_replicated_queue(
            "//tmp/q", replica_modes=["sync", "async", "async"], disabled_replicas={1})
        consumer_replicated_table, consumer_replicas, consumer_replica_ids = self._create_chaos_replicated_consumer(
            "//tmp/c", replica_modes=["sync", "async", "async"], disabled_replicas={1})

        insert_rows(queue_replicated_table, [{"data": "foo", "$tablet_index": 0}])
        insert_rows(queue_replicated_table, [{"data": "bar", "$tablet_index": 0}])

        register_queue_consumer(queue_replicated_table, consumer_replicated_table, vital=False)
        # Wait for registration table mapping to be filled.
        CypressSynchronizerOrchid().wait_fresh_pass()

        self._wait_assert_rows_contain(
            lambda: pull_consumer(consumer_replicated_table, queue_replicated_table, offset=1, partition_index=0), [
                {"$tablet_index": 0, "$row_index": 1, "data": "bar"},
            ])

        self._assert_rows_contain(
            pull_consumer(consumer_replicas[0]["replica_path"], queue_replicas[1]["replica_path"], offset=1,
                          partition_index=0, replica_consistency="none"), [])

        self._assert_rows_contain(
            pull_consumer(consumer_replicas[1]["replica_path"], queue_replicas[1]["replica_path"], offset=1,
                          partition_index=0, replica_consistency="sync"), [
                {"$tablet_index": 0, "$row_index": 1, "data": "bar"},
            ])

    @authors("nadya73")
    @pytest.mark.parametrize("create_consumer", [
        _create_consumer,
        _create_symlink_consumer,
    ])
    def test_pull_consumer(self, create_consumer):
        self._create_queue("//tmp/q")
        create_consumer("//tmp/c")

        insert_rows("//tmp/q", [{"data": "foo"}])
        insert_rows("//tmp/q", [{"data": "bar"}])

        with raises_yt_error(code=yt_error_codes.AuthorizationErrorCode):
            pull_consumer("//tmp/c", "//tmp/q", offset=1, partition_index=0)

        register_queue_consumer("//tmp/q", "//tmp/c", vital=False)

        self._assert_rows_contain(pull_consumer("//tmp/c", "//tmp/q", offset=1, partition_index=0), [
            {"$tablet_index": 0, "$row_index": 1, "data": "bar"},
        ])

        self._assert_rows_contain(pull_consumer("//tmp/c", "//tmp/q", offset=0, partition_index=0, max_row_count=1), [
            {"$tablet_index": 0, "$row_index": 0, "data": "foo"},
        ])

        self._assert_rows_contain(pull_consumer("//tmp/c", "//tmp/q", offset=0, partition_index=0), [
            {"$tablet_index": 0, "$row_index": 0, "data": "foo"},
            {"$tablet_index": 0, "$row_index": 1, "data": "bar"},
        ])

        self._assert_rows_contain(pull_consumer("//tmp/c", "//tmp/q", offset=0, partition_index=0, max_data_weight=5), [
            {"$tablet_index": 0, "$row_index": 0, "data": "foo"},
        ])

        self._assert_rows_contain(pull_consumer("//tmp/c", "//tmp/q", partition_index=0, offset=None), [
            {"$tablet_index": 0, "$row_index": 0, "data": "foo"},
            {"$tablet_index": 0, "$row_index": 1, "data": "bar"},
        ])

        advance_consumer("//tmp/c", "//tmp/q", partition_index=0, old_offset=None, new_offset=1)

        self._assert_rows_contain(pull_consumer("//tmp/c", "//tmp/q", partition_index=0, offset=None), [
            {"$tablet_index": 0, "$row_index": 1, "data": "bar"},
        ])

        with raises_yt_error("Invalid tablet index for table"):
            pull_consumer("//tmp/c", "//tmp/q", partition_index=1, offset=None)

        register_queue_consumer("abc://tmp/q", "//tmp/c", vital=False)
        with raises_yt_error("Queue cluster"):
            pull_consumer("//tmp/c", "abc://tmp/q", partition_index=0, offset=None)

        with raises_yt_error("Queue cluster"):
            pull_consumer("//tmp/c", "abc://tmp/q", partition_index=0, offset=0)

    @authors("nadya73")
    @pytest.mark.parametrize("create_consumer", [
        _create_consumer,
        _create_symlink_consumer,
    ])
    @pytest.mark.parametrize("without_meta", [
        False,
        True,
    ])
    def test_advance_consumer(self, create_consumer, without_meta):
        create_consumer("//tmp/c", without_meta=without_meta)

        def select_queue_partition_from_consumer(queue, partition_index=0):
            rows = select_rows(
                f"* from [//tmp/c] where [queue_path] = \"{queue}\" and [partition_index] = {partition_index}")
            assert len(rows) <= 1
            return rows

        def get_offset(queue, partition_index=0):
            rows = select_queue_partition_from_consumer(queue, partition_index)
            if rows:
                return rows[0]["offset"]
            return None

        def get_meta(queue, partition_index=0):
            rows = select_queue_partition_from_consumer(queue, partition_index)
            if rows and len(rows) > 0 and "meta" in rows[0]:
                return rows[0]["meta"]
            return None

        assert get_offset("//tmp/q1") is None

        with raises_yt_error("Failed to resolve queue path"):
            advance_consumer("//tmp/c", "//tmp/q1", partition_index=0, old_offset=None, new_offset=3, client_side=False)

        self._create_queue("//tmp/q1")
        self._create_queue("//tmp/q2")
        self._create_queue("//tmp/q3")
        self._create_queue("//tmp/q4")

        insert_rows("//tmp/q1", [{"data": "foo"}] * 6)

        advance_consumer("//tmp/c", "//tmp/q1", partition_index=0, old_offset=None, new_offset=3, client_side=False)
        assert get_offset("//tmp/q1") == 3

        if without_meta:
            assert get_meta("//tmp/q1") is None
        else:
            meta = get_meta("//tmp/q1")
            # Each row weight is equal to 20.
            assert meta['cumulative_data_weight'] == 60
            assert 'offset_timestamp' in meta

        with raises_yt_error(yt_error_codes.ConsumerOffsetConflict):
            advance_consumer("//tmp/c", "//tmp/q1", partition_index=0, old_offset=4, new_offset=5, client_side=False)

        advance_consumer("//tmp/c", "//tmp/q1", partition_index=0, old_offset=3, new_offset=5, client_side=False)
        assert get_offset("//tmp/q1") == 5

        if without_meta:
            assert get_meta("//tmp/q1") is None
        else:
            meta = get_meta("//tmp/q1")
            assert meta['cumulative_data_weight'] == 100
            assert 'offset_timestamp' in meta

        advance_consumer("//tmp/c", "//tmp/q1", partition_index=0, old_offset=None, new_offset=7, client_side=False)
        assert get_offset("//tmp/q1") == 7

        if without_meta:
            assert get_meta("//tmp/q1") is None
        else:
            assert get_meta("//tmp/q1") == YsonEntity()

        advance_consumer("//tmp/c", "//tmp/q2", partition_index=0, old_offset=0, new_offset=42, client_side=False)
        assert get_offset("//tmp/q2") == 42

        tx = start_transaction(type="tablet")
        advance_consumer("//tmp/c", "//tmp/q3", partition_index=0, old_offset=None, new_offset=1543, transaction_id=tx, client_side=False)
        advance_consumer("//tmp/c", "//tmp/q4", partition_index=0, old_offset=None, new_offset=1543, transaction_id=tx, client_side=False)

        assert get_offset("//tmp/q3") is None
        assert get_offset("//tmp/q4") is None

        commit_transaction(tx)
        assert get_offset("//tmp/q3") == 1543
        assert get_offset("//tmp/q4") == 1543
