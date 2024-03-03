from yt_dynamic_tables_base import DynamicTablesBase

from yt_commands import (
    print_debug, wait, get_driver, get, set, ls, exists, create, sync_create_cells, sync_mount_table, alter_table, insert_rows,
    create_replication_card, create_chaos_table_replica, alter_table_replica,
    sync_create_chaos_cell, create_chaos_cell_bundle, generate_chaos_cell_id, migrate_replication_cards)

from yt.common import YtError

import builtins


class ChaosTestBase(DynamicTablesBase):
    NUM_CLOCKS = 1
    NUM_MASTER_CACHES = 1
    NUM_NODES = 3
    NUM_CHAOS_NODES = 1

    def _get_drivers(self):
        return [get_driver(cluster=cluster_name) for cluster_name in self.get_cluster_names()]

    def _create_chaos_cell_bundle(self, name="c", peer_cluster_names=None, meta_cluster_names=[], clock_cluster_tag=None):
        if peer_cluster_names is None:
            peer_cluster_names = self.get_cluster_names()
        return create_chaos_cell_bundle(
            name,
            peer_cluster_names,
            meta_cluster_names=meta_cluster_names,
            clock_cluster_tag=clock_cluster_tag)

    def _sync_create_chaos_cell(self, name="c", peer_cluster_names=None, meta_cluster_names=[], area="default"):
        if peer_cluster_names is None:
            peer_cluster_names = self.get_cluster_names()
        cell_id = generate_chaos_cell_id()
        sync_create_chaos_cell(name, cell_id, peer_cluster_names, meta_cluster_names=meta_cluster_names, area=area)
        return cell_id

    def _sync_create_chaos_bundle_and_cell(self, name="c", peer_cluster_names=None, meta_cluster_names=[], clock_cluster_tag=None):
        if peer_cluster_names is None:
            peer_cluster_names = self.get_cluster_names()
        self._create_chaos_cell_bundle(
            name=name,
            peer_cluster_names=peer_cluster_names,
            meta_cluster_names=meta_cluster_names,
            clock_cluster_tag=clock_cluster_tag)
        return self._sync_create_chaos_cell(
            name=name,
            peer_cluster_names=peer_cluster_names,
            meta_cluster_names=meta_cluster_names)

    def _list_chaos_nodes(self, driver=None):
        nodes = ls("//sys/cluster_nodes", attributes=["state", "flavors"], driver=driver)
        return [node for node in nodes if ("chaos" in node.attributes["flavors"])]

    def _get_table_orchids(self, path, driver=None):
        tablets = get("{0}/@tablets".format(path), driver=driver)
        tablet_ids = [tablet["tablet_id"] for tablet in tablets]
        orchids = [get("#{0}/orchid".format(tablet_id), driver=driver) for tablet_id in tablet_ids]
        return orchids

    def _wait_for_era(self, path, era=1, check_write=False, driver=None):
        import logging
        logger = logging.getLogger()

        def _check():
            for orchid in self._get_table_orchids(path, driver=driver):
                if not orchid["replication_card"] or orchid["replication_card"]["era"] != era:
                    logger.debug("Waiting {0} for era {1} got {2}".format(path, era, orchid["replication_card"]["era"] if orchid["replication_card"] else None))
                    return False
                if check_write and orchid["write_mode"] != "direct":
                    logger.debug("Waiting {0} for direct write mode but got {1}".format(path, orchid["write_mode"]))
                    return False
            return True
        wait(_check)

    def _sync_replication_card(self, card_id):
        def _check():
            card = get("#{0}/@".format(card_id))
            replicas = card["replicas"]
            return all(replica["state"] in ["enabled", "disabled"] and replica["mode"] in ["sync", "async"] for replica in replicas.values()) and len(card["coordinator_cell_ids"]) > 0
        wait(_check)
        return get("#{0}/@".format(card_id))

    def _create_chaos_table_replica(self, replica, **kwargs):
        attributes = {
            "enabled": replica.get("enabled", False)
        }
        for key in ["content_type", "mode", "replication_card_id", "table_path", "catchup", "replication_progress", "enable_replicated_table_tracker"]:
            if key in replica:
                attributes[key] = replica[key]
            if kwargs.get(key, None) is not None:
                attributes[key] = kwargs[key]
        return create_chaos_table_replica(replica["cluster_name"], replica["replica_path"], attributes=attributes)

    def _create_chaos_table_replicas(self, replicas, replication_card_id=None, table_path=None):
        return [self._create_chaos_table_replica(replica, replication_card_id=replication_card_id, table_path=table_path) for replica in replicas]

    def _prepare_replica_tables(self, replicas, replica_ids, create_tablet_cells=True, mount_tables=True):
        for replica, replica_id in zip(replicas, replica_ids):
            path = replica["replica_path"]
            driver = get_driver(cluster=replica["cluster_name"])
            alter_table(path, upstream_replica_id=replica_id, driver=driver)
            if create_tablet_cells and len(ls("//sys/tablet_cells", driver=driver)) == 0:
                sync_create_cells(1, driver=driver)
            if mount_tables:
                sync_mount_table(path, driver=driver)

    def _create_replica_tables(self, replicas, replica_ids, create_tablet_cells=True, mount_tables=True, ordered=False, schema=None, pivot_keys=None, replication_progress=None):
        for replica, replica_id in zip(replicas, replica_ids):
            path = replica["replica_path"]
            driver = get_driver(cluster=replica["cluster_name"])
            create_table = self._create_queue_table
            if ordered:
                create_table = self._create_ordered_table
            elif replica["content_type"] == "data":
                create_table = self._create_sorted_table
            kwargs = {"driver": driver, "upstream_replica_id": replica_id}
            if schema:
                kwargs["schema"] = schema
            if pivot_keys:
                kwargs["pivot_keys"] = pivot_keys
            if replication_progress:
                kwargs["replication_progress"] = replication_progress
            create_table(path, **kwargs)
        self._prepare_replica_tables(replicas, replica_ids, create_tablet_cells=create_tablet_cells, mount_tables=mount_tables)

    def _sync_replication_era(self, card_id, replicas=None):
        replication_card = self._sync_replication_card(card_id)
        if not replicas:
            replicas = replication_card["replicas"].values()

        def _enabled(replica):
            return replica["enabled"] if "enabled" in replica else (replica["state"] in ["enabled", "enabling"])
        for replica in replicas:
            path = replica["replica_path"]
            driver = get_driver(cluster=replica["cluster_name"])
            check_write = replica["mode"] == "sync" and _enabled(replica)
            self._wait_for_era(path, era=replication_card["era"], check_write=check_write, driver=driver)

    def _create_chaos_tables(self, cell_id, replicas, sync_replication_era=True, create_replica_tables=True, create_tablet_cells=True, mount_tables=True, ordered=False, schema=None):
        card_id = create_replication_card(chaos_cell_id=cell_id)
        replica_ids = self._create_chaos_table_replicas(replicas, replication_card_id=card_id)
        if create_replica_tables:
            self._create_replica_tables(replicas, replica_ids, create_tablet_cells, mount_tables, ordered, schema=schema)
        if sync_replication_era:
            self._sync_replication_era(card_id, replicas)
        return card_id, replica_ids

    def _sync_migrate_replication_cards(self, cell_id, card_ids, destination_cell_id, origin_driver=None, destination_driver=None):
        if destination_driver is None:
            destination_driver = origin_driver

        migrate_replication_cards(cell_id, card_ids, destination_cell_id=destination_cell_id)

        def _get_orchid_path(cell_id, driver=None):
            address = get(f"#{cell_id}/@peers/0/address", driver=driver)
            return "//sys/cluster_nodes/{0}/orchid/chaos_cells/{1}".format(address, cell_id)

        for card_id in card_ids:
            migration_path = "{0}/chaos_manager/replication_cards/{1}/state".format(
                _get_orchid_path(cell_id, driver=origin_driver),
                card_id
            )
            wait(lambda: get(migration_path, driver=origin_driver) == "migrated")

        for card_id in card_ids:
            migrated_card_path = "{0}/chaos_manager/replication_cards/{1}".format(
                _get_orchid_path(destination_cell_id, driver=destination_driver),
                card_id
            )
            wait(lambda: exists(migrated_card_path, driver=destination_driver))

    def _sync_alter_replica(self, card_id, replicas, replica_ids, replica_index, **kwargs):
        replica_id = replica_ids[replica_index]
        replica = replicas[replica_index]
        replica_driver = get_driver(cluster=replica["cluster_name"])
        alter_table_replica(replica_id, **kwargs)

        enabled = kwargs.get("enabled", None)
        mode = kwargs.get("mode", None)

        if enabled is not None:
            state = "enabled" if enabled else "disabled"
            wait(lambda: get(f"#{card_id}/@replicas/{replica_id}/state") == state)
        if mode is not None:
            wait(lambda: get(f"#{card_id}/@replicas/{replica_id}/mode") == mode)

        def _replica_checker(replica_info):
            if enabled is not None and replica_info["state"] != ("enabled" if enabled else "disabled"):
                return False
            if mode is not None and replica_info["mode"] != mode:
                return False
            return True

        def _check():
            orchids = self._get_table_orchids(replica["replica_path"], driver=replica_driver)
            if not any(orchid["replication_card"] for orchid in orchids):
                return False
            if not all(_replica_checker(orchid["replication_card"]["replicas"][replica_id]) for orchid in orchids):
                return False
            if len(builtins.set(orchid["replication_card"]["era"] for orchid in orchids)) > 1:
                return False
            replica_info = orchids[0]["replication_card"]["replicas"][replica_id]
            if replica_info["mode"] == "sync" and replica_info["state"] == "enabled":
                if not all(orchid["write_mode"] == "direct" for orchid in orchids):
                    return False
            return True

        wait(_check)

        replication_card = get("#{0}/@".format(card_id))
        era = replication_card["era"]

        def _era_in_sync():
            orchids = self._get_table_orchids(replica["replica_path"], driver=replica_driver)
            return all(orchid["replication_card"]["era"] == era for orchid in orchids)

        wait(_era_in_sync)

        def _check_sync():
            for replica in replication_card["replicas"].values():
                if replica["mode"] != "sync" or replica["state"] != "enabled":
                    continue
                orchids = self._get_table_orchids(replica["replica_path"], driver=get_driver(cluster=replica["cluster_name"]))
                if not all(orchid["replication_card"]["era"] == era for orchid in orchids):
                    return False
            return True

        wait(_check_sync)

    def _update_mount_config(self, attributes):
        if "mount_config" not in attributes:
            attributes["mount_config"] = {}
        if "replication_progress_update_tick_period" not in attributes["mount_config"]:
            attributes["mount_config"]["replication_progress_update_tick_period"] = 100

    def _create_sorted_table(self, path, **attributes):
        self._update_mount_config(attributes)
        super(ChaosTestBase, self)._create_sorted_table(path, **attributes)

    def _create_ordered_table(self, path, **attributes):
        self._update_mount_config(attributes)
        if "schema" not in attributes:
            attributes.update(
                {
                    "schema": [
                        {"name": "$timestamp", "type": "uint64"},
                        {"name": "key", "type": "int64"},
                        {"name": "value", "type": "string"},
                    ]
                }
            )
        if "commit_ordering" not in attributes:
            attributes.update({"commit_ordering": "strong"})
        super(ChaosTestBase, self)._create_ordered_table(path, **attributes)

    def _create_queue_table(self, path, **attributes):
        attributes.update({"dynamic": True})
        if "schema" not in attributes:
            attributes.update({
                "schema": [
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"}]
                })
        driver = attributes.pop("driver", None)
        create("replication_log_table", path, attributes=attributes, driver=driver)

    def _insistent_insert_rows(self, table, rows, driver=None):
        def _do():
            try:
                insert_rows(table, rows, driver=driver)
                return True
            except YtError as err:
                print_debug("Insert into {0} failed: ".format(table), err)
                return False
        wait(_do)

    def _init_replicated_table_tracker(self):
        for driver in self._get_drivers():
            set("//sys/@config/tablet_manager/replicated_table_tracker/use_new_replicated_table_tracker", True, driver=driver)
            set("//sys/@config/tablet_manager/replicated_table_tracker/bundle_health_cache", {
                "expire_after_successful_update_time": 100,
                "expire_after_failed_update_time": 100,
                "expire_after_access_time": 100,
                "refresh_time": 50,
            }, driver=driver)

    def setup_method(self, method):
        super(ChaosTestBase, self).setup_method(method)

        # TODO(babenko): consider moving to yt_env_setup.py
        for driver in self._get_drivers():
            synchronizer_config = {
                "enable": True,
                "sync_period": 100,
                "full_sync_period": 200,
            }
            set("//sys/@config/chaos_manager/alien_cell_synchronizer", synchronizer_config, driver=driver)

            discovery_config = {
                "peer_count": 1,
                "update_period": 100,
                "node_tag_filter": "master_cache"
            }
            set("//sys/@config/node_tracker/master_cache_manager", discovery_config, driver=driver)

            chaos_nodes = self._list_chaos_nodes(driver)
            for chaos_node in chaos_nodes:
                set("//sys/cluster_nodes/{0}/@user_tags/end".format(chaos_node), "chaos_cache", driver=driver)

    def _get_schemas_by_name(self, schema_names):
        schemas = {
            "sorted_simple": [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ],
            "sorted_value1": [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value1", "type": "string"},
            ],
            "sorted_value2": [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
                {"name": "value2", "type": "string"},
            ],
            "sorted_key2": [
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "key2", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ],
            "sorted_key2_inverted": [
                {"name": "key2", "type": "int64", "sort_order": "ascending"},
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ],
            "sorted_hash": [
                {"name": "hash", "type": "uint64", "sort_order": "ascending", "expression": "farm_hash(key)"},
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ],
            "sorted_hash_value1": [
                {"name": "hash", "type": "uint64", "sort_order": "ascending", "expression": "farm_hash(key)"},
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value1", "type": "string"},
            ],
            "ordered_simple": [
                {"name": "$timestamp", "type": "uint64"},
                {"name": "key", "type": "int64"},
                {"name": "value", "type": "string"},
            ],
            "ordered_value2": [
                {"name": "$timestamp", "type": "uint64"},
                {"name": "key", "type": "int64"},
                {"name": "value", "type": "string"},
                {"name": "value2", "type": "string"},
            ],
            "ordered_simple_int": [
                {"name": "$timestamp", "type": "uint64"},
                {"name": "key", "type": "int64"},
                {"name": "value", "type": "int64"},
            ],
        }
        return [schemas[name] for name in schema_names]
