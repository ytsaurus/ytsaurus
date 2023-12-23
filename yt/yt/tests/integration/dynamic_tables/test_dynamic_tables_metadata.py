from .test_sorted_dynamic_tables import TestSortedDynamicTablesBase

from yt_env_setup import skip_if_rpc_driver_backend

from yt_commands import (
    authors, print_debug, wait, sync_mount_table, sync_unmount_table,
    ls, remove, insert_rows, select_rows, lookup_rows, alter_table,
    wait_for_tablet_state, sync_create_cells,
    clear_metadata_caches, execute_command)

from yt_helpers import profiler_factory

from yt.common import YtError
from yt.environment.helpers import assert_items_equal

from yt_driver_bindings import Driver

import pytest

from copy import deepcopy

##################################################################


class TestSortedDynamicTablesMetadataCaching(TestSortedDynamicTablesBase):
    USE_MASTER_CACHE = True

    DELTA_DRIVER_CONFIG = {
        "max_rows_per_write_request": 2,
        "table_mount_cache": {
            "expire_after_successful_update_time": 60000,
            "refresh_time": 60000,
            "expire_after_failed_update_time": 1000,
            "expire_after_access_time": 300000,
        },
        "permission_cache": {
            "expire_after_successful_update_time": 60000,
            "refresh_time": 60000,
            "expire_after_failed_update_time": 1000,
            "expire_after_access_time": 300000,
        },
    }

    DELTA_NODE_CONFIG = {"tablet_node": {"tablet_snapshot_eviction_timeout": 0}}

    # Reimplement dynamic table commands without calling clear_metadata_caches()

    def _mount_table(self, path, **kwargs):
        kwargs["path"] = path
        return execute_command("mount_table", kwargs)

    def _unmount_table(self, path, **kwargs):
        kwargs["path"] = path
        return execute_command("unmount_table", kwargs)

    def _reshard_table(self, path, arg, **kwargs):
        kwargs["path"] = path
        kwargs["pivot_keys"] = arg
        return execute_command("reshard_table", kwargs)

    def _sync_mount_table(self, path, **kwargs):
        self._mount_table(path, **kwargs)
        print_debug("Waiting for tablets to become mounted")
        wait_for_tablet_state(path, "mounted", **kwargs)

    def _sync_unmount_table(self, path, **kwargs):
        self._unmount_table(path, **kwargs)
        print_debug("Waiting for tablets to become unmounted")
        wait_for_tablet_state(path, "unmounted", **kwargs)

    @authors("savrus")
    def test_select_with_expired_schema(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        self._reshard_table("//tmp/t", [[], [1]])
        self._sync_mount_table("//tmp/t")
        rows = [{"key": i, "value": str(i)} for i in range(2)]
        insert_rows("//tmp/t", rows)
        assert_items_equal(select_rows("* from [//tmp/t]"), rows)
        self._sync_unmount_table("//tmp/t")
        alter_table(
            "//tmp/t",
            schema=[
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "key2", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ],
        )
        self._sync_mount_table("//tmp/t")
        expected = [{"key": i, "key2": None, "value": str(i)} for i in range(2)]
        assert_items_equal(select_rows("* from [//tmp/t]"), expected)

    @authors("savrus")
    def test_lookup_from_removed_table(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t2")
        self._sync_mount_table("//tmp/t2")
        rows = [{"key": i, "value": str(i)} for i in range(2)]

        # Do lookup to clear metadata and master cache.
        # Unfortunately master cache has old schema and it is retrieved in driver where key is constructed.
        # Client invalidate&retry doesn't rebuild driver's key so this lookup has no chances to be completed.
        try:
            lookup_rows("//tmp/t2", [{"key": 0}])
        except YtError:
            pass

        insert_rows("//tmp/t2", rows)
        assert_items_equal(select_rows("* from [//tmp/t2]"), rows)
        remove("//tmp/t2")
        self._create_simple_table("//tmp/t2")
        self._sync_mount_table("//tmp/t2")
        actual = lookup_rows("//tmp/t2", [{"key": 0}])
        assert actual == []


class TestSortedDynamicTablesMetadataCaching2(TestSortedDynamicTablesMetadataCaching):
    USE_MASTER_CACHE = False

    @authors("savrus")
    @skip_if_rpc_driver_backend
    def test_metadata_cache_invalidation(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t1", enable_compaction_and_partitioning=False)
        self._sync_mount_table("//tmp/t1")

        rows = [{"key": i, "value": str(i)} for i in range(3)]
        keys = [{"key": row["key"]} for row in rows]
        insert_rows("//tmp/t1", rows)
        assert_items_equal(lookup_rows("//tmp/t1", keys), rows)

        self._sync_unmount_table("//tmp/t1")
        with pytest.raises(YtError):
            lookup_rows("//tmp/t1", keys)
        clear_metadata_caches()
        self._sync_mount_table("//tmp/t1")

        assert_items_equal(lookup_rows("//tmp/t1", keys), rows)

        self._sync_unmount_table("//tmp/t1")
        with pytest.raises(YtError):
            select_rows("* from [//tmp/t1]")
        clear_metadata_caches()
        self._sync_mount_table("//tmp/t1")

        assert_items_equal(select_rows("* from [//tmp/t1]"), rows)

        def reshard_mounted_table(path, pivots):
            self._sync_unmount_table(path)
            self._reshard_table(path, pivots)
            self._sync_mount_table(path)

        reshard_mounted_table("//tmp/t1", [[], [1]])
        assert_items_equal(lookup_rows("//tmp/t1", keys), rows)

        reshard_mounted_table("//tmp/t1", [[], [1], [2]])
        assert_items_equal(select_rows("* from [//tmp/t1]"), rows)

        reshard_mounted_table("//tmp/t1", [[]])
        rows = [{"key": i, "value": str(i + 1)} for i in range(3)]
        with pytest.raises(YtError):
            insert_rows("//tmp/t1", rows)
        insert_rows("//tmp/t1", rows)

        insert_rows("//tmp/t1", rows)
        assert_items_equal(lookup_rows("//tmp/t1", keys), rows)


##################################################################


class TestSortedDynamicTablesMetadataCachingMulticell(TestSortedDynamicTablesMetadataCaching):
    NUM_SECONDARY_MASTER_CELLS = 2


class TestSortedDynamicTablesMetadataCachingMulticell2(TestSortedDynamicTablesMetadataCaching2):
    NUM_SECONDARY_MASTER_CELLS = 2


###################################################################


class TestSortedDynamicTablesMetadataCachingRpcProxy(TestSortedDynamicTablesMetadataCaching):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True


class TestSortedDynamicTablesMetadataCachingRpcProxy2(TestSortedDynamicTablesMetadataCaching2):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True


###################################################################


class TestSortedDynamicTablesMetadataCachingOnRpcProxy(TestSortedDynamicTablesBase):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True

    DELTA_RPC_PROXY_CONFIG = {
        "cluster_connection": {
            "table_mount_cache": {
                "expire_after_successful_update_time": 5000,
                "expire_after_access_time": 5000,
            },
        }
    }

    @authors("akozhikhov")
    def test_profile_mount_cache_retries(self):
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", enable_detailed_profiling=True)
        sync_mount_table("//tmp/t")

        rows = [{"key": 1, "value": "one"}]
        insert_rows("//tmp/t", rows)

        rpc_proxy = ls("//sys/rpc_proxies")[0]

        rpc_driver_config = deepcopy(self.Env.configs["rpc_driver"])
        rpc_driver_config["proxy_addresses"] = [rpc_proxy]
        rpc_driver_config["api_version"] = 3
        rpc_driver = Driver(config=rpc_driver_config)

        proxy_lookup_retry_count = profiler_factory().at_rpc_proxy(rpc_proxy).counter(
            name="rpc_proxy/detailed_table_statistics/retry_count",
            tags={"table_path": "//tmp/t"})

        assert lookup_rows("//tmp/t", [{"key": 1}], driver=rpc_driver) == rows

        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")

        assert lookup_rows("//tmp/t", [{"key": 1}], driver=rpc_driver) == rows
        wait(lambda: proxy_lookup_retry_count.get_delta() > 0)
