from yt_chaos_test_base import ChaosTestBase

from yt_commands import (
    authors, wait, get, set, ls, exists, create, get_driver,
    sync_create_cells, sync_mount_table, sync_flush_table,
    insert_rows, lookup_rows, generate_timestamp, raises_yt_error)

from yt.common import YtError

import yt.yson as yson

from yt_driver_bindings import Driver

import pytest
from copy import deepcopy


##################################################################


class ChaosClockBase(ChaosTestBase):
    SETUP_DEFAULT_BUNDLE_CLOCK_CLUSTER_TAG = False

    NUM_REMOTE_CLUSTERS = 1
    NUM_TIMESTAMP_PROVIDERS = 1
    USE_PRIMARY_CLOCKS = False

    DELTA_MASTER_CACHE_CONFIG = {
        "cluster_connection": {
            "chaos_residency_cache": {
                "use_has_chaos_object": True,
            },
        },
    }

    DELTA_NODE_CONFIG = {
        "tablet_node": {
            "transaction_manager": {
                "reject_incorrect_clock_cluster_tag": True
            }
        },
        "chaos_node": {
            "replication_card_automaton_cache_expiration_time": 100
        },
    }

    @classmethod
    def add_alien_clocks_to_ts_configs(
        cls,
        timestamp_providers_configs,
        self_clock_cluster_tag,
        alien_clock_cluster_tag,
        alien_clock_configs
    ):
        alien_clock_configs = alien_clock_configs[alien_clock_configs["cell_tag"]]
        alien_clock_addresses = [f"localhost:{clock_config['rpc_port']}" for clock_config in alien_clock_configs]

        for timestamp_providers_config in timestamp_providers_configs:
            timestamp_providers_config["alien_timestamp_providers"] = [
                {
                    "clock_cluster_tag": alien_clock_cluster_tag,
                    "timestamp_provider": {
                        "addresses": alien_clock_addresses
                    },
                }
            ]
            timestamp_providers_config["clock_cluster_tag"] = self_clock_cluster_tag

    @classmethod
    def modify_timestamp_providers_configs(cls, timestamp_providers_configs, clock_configs, yt_configs):
        cls.add_alien_clocks_to_ts_configs(
            timestamp_providers_configs[0],
            yt_configs[0].primary_cell_tag,
            yt_configs[1].primary_cell_tag,
            clock_configs[1]
        )

        cls.add_alien_clocks_to_ts_configs(
            timestamp_providers_configs[1],
            yt_configs[1].primary_cell_tag,
            yt_configs[0].primary_cell_tag,
            clock_configs[0]
        )

        return True

    def _create_single_peer_chaos_cell(self, name="c", clock_cluster_tag=None):
        cluster_names = self.get_cluster_names()
        peer_cluster_names = cluster_names[:1]
        meta_cluster_names = cluster_names[1:]
        cell_id = self._sync_create_chaos_bundle_and_cell(
            name=name,
            peer_cluster_names=peer_cluster_names,
            meta_cluster_names=meta_cluster_names,
            clock_cluster_tag=clock_cluster_tag)
        return cell_id


##################################################################


class TestChaosClock(ChaosClockBase):
    ENABLE_MULTIDAEMON = True

    @authors("savrus")
    def test_invalid_clock(self):
        drivers = self._get_drivers()
        for driver in drivers:
            clock_cluster_tag = get("//sys/@primary_cell_tag", driver=driver)
            set("//sys/tablet_cell_bundles/default/@options/clock_cluster_tag", clock_cluster_tag, driver=driver)

        cell_id = self._create_single_peer_chaos_cell()
        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q"}
        ]
        self._create_chaos_tables(cell_id, replicas)

        with pytest.raises(YtError, match="Transaction timestamp is generated from unexpected clock"):
            insert_rows("//tmp/t", [{"key": 0, "value": "0"}])

    @authors("ponasenko-rs")
    @pytest.mark.parametrize("clock_tag_valid", [True, False])
    def test_check_on_mount_with_invalid_clock_tag(self, clock_tag_valid):
        drivers = self._get_drivers()

        for driver in drivers:
            set("//sys/@config/tablet_manager/enable_clock_cell_tag_validation_on_chaos_replica_mount", True, driver=driver)

        def _set_default_bundle_clock_cluster_tag(clock_cluster_tag):
            for driver in drivers:
                set("//sys/tablet_cell_bundles/default/@options/clock_cluster_tag", clock_cluster_tag, driver=driver)

        if clock_tag_valid:
            _set_default_bundle_clock_cluster_tag(get("//sys/@primary_cell_tag"))
        else:
            invalid_cell_tag = 0xf004
            _set_default_bundle_clock_cluster_tag(invalid_cell_tag)

        cell_id = self._create_single_peer_chaos_cell()

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q"}
        ]

        card_id, _ = self._create_chaos_tables(cell_id, replicas, sync_replication_era=False, mount_tables=False)

        if clock_tag_valid:
            for replica in replicas:
                sync_mount_table(replica["replica_path"], driver=get_driver(cluster=replica["cluster_name"]))

            self._sync_replication_era(card_id, replicas)

            values = [{"key": 0, "value": "0"}]
            insert_rows("//tmp/t", values)
            wait(lambda: lookup_rows("//tmp/t", [{"key": 0}]) == values)
        else:
            for replica in replicas:
                with raises_yt_error(
                    "Chaos replicas should be part of tablet cell bundle configured with relevant clock cell tag;"
                    " Please reconfigure bundle or move table to bundle properly configured with respect to clock source"
                ):
                    sync_mount_table(replica["replica_path"], driver=get_driver(cluster=replica["cluster_name"]))

    @authors("savrus")
    @pytest.mark.parametrize("mode", ["sync", "async"])
    @pytest.mark.parametrize("primary", [True, False])
    def test_different_clock(self, primary, mode):
        drivers = self._get_drivers()
        clock_cluster_tag = get("//sys/@primary_cell_tag", driver=drivers[0 if primary else 1])
        for driver in drivers:
            set("//sys/tablet_cell_bundles/default/@options/clock_cluster_tag", clock_cluster_tag, driver=driver)

        cell_id = self._create_single_peer_chaos_cell(clock_cluster_tag=clock_cluster_tag)
        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": mode, "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q"}
        ]
        card_id, replica_ids = self._create_chaos_tables(cell_id, replicas)

        def _run_iterations():
            total_iterations = 10
            for iteration in range(total_iterations):
                rows = [{"key": 1, "value": str(iteration)}]
                keys = [{"key": 1}]
                if primary:
                    insert_rows("//tmp/t", rows)
                else:
                    insert_rows("//tmp/q", rows, driver=drivers[1])
                wait(lambda: lookup_rows("//tmp/t", keys) == rows)

                if iteration < total_iterations - 1:
                    mode = ["sync", "async"][iteration % 2]
                    self._sync_alter_replica(card_id, replicas, replica_ids, 0, mode=mode)
        _run_iterations()

        # Check that master transactions are working.
        sync_flush_table("//tmp/t")
        sync_flush_table("//tmp/q", driver=drivers[1])
        _run_iterations()


##################################################################


class TestChaosClockRpcProxy(ChaosClockBase):
    ENABLE_MULTIDAEMON = True

    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

    def _set_proxies_clock_cluster_tag(self, clock_cluster_tag, driver=None):
        config = {
            "cluster_connection": {
                "clock_manager": {
                    "clock_cluster_tag": clock_cluster_tag,
                },
                # FIXME(savrus) Workaround for YT-16713.
                "table_mount_cache": {
                    "expire_after_successful_update_time": 0,
                    "expire_after_failed_update_time": 0,
                    "expire_after_access_time": 0,
                    "refresh_time": 0,
                    "expiration_period": 0,
                },
            },
        }

        set("//sys/rpc_proxies/@config", config, driver=driver)

        proxies = ls("//sys/rpc_proxies")

        def _check_config():
            orchid_path = "orchid/dynamic_config_manager/effective_config/cluster_connection/clock_manager/clock_cluster_tag"
            for proxy in proxies:
                path = "//sys/rpc_proxies/{0}/{1}".format(proxy, orchid_path)
                if not exists(path) or get(path) != clock_cluster_tag:
                    return False
            return True
        wait(_check_config)

    @authors("savrus")
    def test_invalid_clock_source(self):
        drivers = self._get_drivers()
        remote_clock_tag = get("//sys/@primary_cell_tag", driver=drivers[1])
        self._set_proxies_clock_cluster_tag(remote_clock_tag)

        clock_cluster_tag = get("//sys/@primary_cell_tag")
        set("//sys/tablet_cell_bundles/default/@options/clock_cluster_tag", clock_cluster_tag)

        self._create_sorted_table("//tmp/t")
        sync_create_cells(1)
        sync_mount_table("//tmp/t")

        with pytest.raises(YtError, match="Transaction origin clock source differs from coordinator clock source"):
            insert_rows("//tmp/t", [{"key": 0, "value": "0"}])

        with raises_yt_error("Unable to generate timestamps"):
            generate_timestamp()

    @authors("savrus")
    def test_chaos_write_with_client_clock_tag(self):
        drivers = self._get_drivers()
        remote_clock_tag = get("//sys/@primary_cell_tag", driver=drivers[1])
        self._set_proxies_clock_cluster_tag(remote_clock_tag)

        for driver in drivers:
            clock_cluster_tag = get("//sys/@primary_cell_tag", driver=driver)
            set("//sys/tablet_cell_bundles/default/@options/clock_cluster_tag", clock_cluster_tag, driver=driver)

        cell_id = self._create_single_peer_chaos_cell(clock_cluster_tag=remote_clock_tag)
        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q"}
        ]
        self._create_chaos_tables(cell_id, replicas)

        values = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/t", values)
        wait(lambda: lookup_rows("//tmp/t", [{"key": 0}]) == values)

    @authors("savrus")
    def test_chaos_replicated_table_with_different_clock_tag(self):
        drivers = self._get_drivers()
        remote_clock_tag = get("//sys/@primary_cell_tag", driver=drivers[1])
        self._set_proxies_clock_cluster_tag(remote_clock_tag)

        for driver in drivers:
            clock_cluster_tag = get("//sys/@primary_cell_tag", driver=driver)
            set("//sys/tablet_cell_bundles/default/@options/clock_cluster_tag", clock_cluster_tag, driver=driver)

        cell_id = self._create_single_peer_chaos_cell(name="chaos_bundle", clock_cluster_tag=remote_clock_tag)
        set("//sys/chaos_cell_bundles/chaos_bundle/@metadata_cell_id", cell_id)
        schema = yson.YsonList([
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
        ])
        create("chaos_replicated_table", "//tmp/crt", attributes={"chaos_cell_bundle": "chaos_bundle", "schema": schema})

        replicas = [
            {"cluster_name": "primary", "content_type": "data", "mode": "async", "enabled": True, "replica_path": "//tmp/t"},
            {"cluster_name": "remote_0", "content_type": "queue", "mode": "sync", "enabled": True, "replica_path": "//tmp/q"}
        ]
        replica_ids = self._create_chaos_table_replicas(replicas, table_path="//tmp/crt")
        self._create_replica_tables(replicas, replica_ids)
        card_id = get("//tmp/crt/@replication_card_id")
        self._sync_replication_era(card_id, replicas)

        values = [{"key": 0, "value": "0"}]
        insert_rows("//tmp/crt", values)
        wait(lambda: lookup_rows("//tmp/t", [{"key": 0}]) == values)

    @authors("savrus")
    def test_generate_timestamps(self):
        drivers = self._get_drivers()
        remote_clock_tag = get("//sys/@primary_cell_tag", driver=drivers[1])
        self._set_proxies_clock_cluster_tag(remote_clock_tag)

        with raises_yt_error("Unable to generate timestamps: clock source is configured to non-native clock"):
            generate_timestamp()

        config = deepcopy(self.Env.configs["rpc_driver"])
        config["clock_cluster_tag"] = remote_clock_tag
        config["api_version"] = 3

        rpc_driver = Driver(config=config)
        generate_timestamp(driver=rpc_driver)
