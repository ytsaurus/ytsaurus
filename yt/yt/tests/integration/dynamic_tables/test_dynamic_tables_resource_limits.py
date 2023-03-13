from yt_tests_common.dynamic_tables_base import DynamicTablesBase

from yt_commands import (
    authors, wait, create, ls, get, set, copy, move,
    remove, exists,
    create_account, create_tablet_cell_bundle, insert_rows, alter_table, write_table, mount_table, reshard_table, remount_table, reshard_table_automatic, sync_create_cells,
    sync_mount_table, sync_unmount_table, sync_reshard_table, sync_flush_table, sync_compact_table,
    build_snapshot,
    multicell_sleep, raises_yt_error, get_driver)

import yt_error_codes

from yt.common import YtError

from yt.test_helpers import assert_items_equal

import pytest

import time

##################################################################


class DynamicTablesResourceLimitsBase(DynamicTablesBase):
    USE_PERMISSION_CACHE = False

    DELTA_NODE_CONFIG = {
        "tablet_node": {
            "security_manager": {
                "resource_limits_cache": {
                    "expire_after_access_time": 0,
                },
            },
            "hydra_manager": {
                "close_changelogs": False,
            }
        },
        "master_cache_service": {
            "capacity": 0
        }
    }

    def _multicell_set(self, path, value):
        set(path, value)
        for i in range(self.NUM_SECONDARY_MASTER_CELLS):
            driver = get_driver(i + 1)
            wait(lambda: exists(path, driver=driver) and get(path, driver=driver) == value)

    def _multicell_wait(self, predicate):
        for i in range(self.NUM_SECONDARY_MASTER_CELLS):
            driver = get_driver(i + 1)
            wait(predicate(driver))

##################################################################


class TestDynamicTablesResourceLimits(DynamicTablesResourceLimitsBase):
    def _verify_resource_usage(self, account, resource, expected):
        def resource_usage_matches(driver):
            return lambda: (
                get(
                    "//sys/accounts/{0}/@resource_usage/{1}".format(account, resource),
                    driver=driver,
                )
                == expected
                and get(
                    "//sys/accounts/{0}/@committed_resource_usage/{1}".format(account, resource),
                    driver=driver,
                )
                == expected
            )

        self._multicell_wait(resource_usage_matches)

    @authors("savrus")
    @pytest.mark.parametrize("resource", ["chunk_count", "disk_space_per_medium/default"])
    def test_resource_limits(self, resource):
        create_account("test_account")
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        set("//tmp/t/@account", "test_account")
        sync_mount_table("//tmp/t")

        set("//sys/accounts/test_account/@resource_limits/" + resource, 0)

        def _wait_func(driver):
            limits = get("//sys/accounts/test_account/@resource_limits", driver=driver)
            if resource == "chunk_count":
                return limits["chunk_count"] == 0
            else:
                return limits["disk_space"] == 0

        self._multicell_wait(lambda driver: lambda: _wait_func(driver))

        insert_rows("//tmp/t", [{"key": 0, "value": "0"}])
        sync_flush_table("//tmp/t")

        with pytest.raises(YtError):
            insert_rows("//tmp/t", [{"key": 0, "value": "0"}])

        self._multicell_set("//sys/accounts/test_account/@resource_limits/" + resource, 10000)
        insert_rows("//tmp/t", [{"key": 0, "value": "0"}])

        set("//sys/accounts/test_account/@resource_limits/" + resource, 0)
        sync_unmount_table("//tmp/t")

    # TODO(ifsmirnov): YT-14310
    @authors("savrus")
    def test_tablet_count_limit_create(self):
        create_account("test_account")
        sync_create_cells(1)

        self._multicell_set("//sys/accounts/test_account/@resource_limits/tablet_count", 0)
        with pytest.raises(YtError):
            self._create_sorted_table("//tmp/t", account="test_account")
        with pytest.raises(YtError):
            self._create_ordered_table("//tmp/t", account="test_account")

        self._multicell_set("//sys/accounts/test_account/@resource_limits/tablet_count", 1)
        with pytest.raises(YtError):
            self._create_ordered_table("//tmp/t", account="test_account", tablet_count=2)
        with pytest.raises(YtError):
            self._create_sorted_table("//tmp/t", account="test_account", pivot_keys=[[], [1]])

        assert get("//sys/accounts/test_account/@ref_counter") == 1

        self._multicell_set("//sys/accounts/test_account/@resource_limits/tablet_count", 4)
        self._create_ordered_table("//tmp/t1", account="test_account", tablet_count=2)
        self._verify_resource_usage("test_account", "tablet_count", 2)
        self._create_sorted_table("//tmp/t2", account="test_account", pivot_keys=[[], [1]])
        self._verify_resource_usage("test_account", "tablet_count", 4)

    # TODO(ifsmirnov): YT-14310
    @authors("lexolordan")
    def test_mount_mounted_table(self):
        create_account("test_account")
        self._multicell_set("//sys/accounts/test_account/@resource_limits/tablet_count", 2)
        sync_create_cells(1)

        def _create_table(table_name):
            self._create_sorted_table(table_name, account="test_account", external_cell_tag=11)

            sync_mount_table(table_name)
            insert_rows(table_name, [{"key": 0, "value": "0"}])
            sync_unmount_table(table_name)

        _create_table("//tmp/t0")
        _create_table("//tmp/t1")

        data_size = get("//tmp/t0/@uncompressed_data_size")
        self._multicell_set(
            "//sys/accounts/test_account/@resource_limits/tablet_static_memory",
            data_size,
        )

        set("//tmp/t0/@in_memory_mode", "uncompressed")
        sync_mount_table("//tmp/t0")

        self._verify_resource_usage("test_account", "tablet_static_memory", data_size)

        set("//tmp/t1/@in_memory_mode", "uncompressed")
        with pytest.raises(YtError):
            sync_mount_table("//tmp/t1")

        remount_table("//tmp/t0")
        sync_unmount_table("//tmp/t0")

    # TODO(ifsmirnov): YT-14310
    @authors("savrus")
    def test_tablet_count_limit_reshard(self):
        create_account("test_account")
        sync_create_cells(1)
        self._multicell_set("//sys/accounts/test_account/@resource_limits/tablet_count", 2)
        self._create_sorted_table("//tmp/t1", account="test_account")
        self._create_ordered_table("//tmp/t2", account="test_account")

        # Wait for resource usage since tabels can be placed to different cells.
        self._multicell_wait(
            lambda driver: lambda: get(
                "//sys/accounts/test_account/@resource_usage/tablet_count",
                driver=driver,
            )
            == 2
        )

        with pytest.raises(YtError):
            reshard_table("//tmp/t1", [[], [1]])
        with pytest.raises(YtError):
            reshard_table("//tmp/t2", 2)

        self._multicell_set("//sys/accounts/test_account/@resource_limits/tablet_count", 4)
        sync_reshard_table("//tmp/t1", [[], [1]])
        sync_reshard_table("//tmp/t2", 2)
        self._verify_resource_usage("test_account", "tablet_count", 4)

    # TODO(ifsmirnov): YT-14310
    @authors("savrus")
    def test_tablet_count_limit_copy(self):
        create_account("test_account")
        self._multicell_set("//sys/accounts/test_account/@resource_limits/tablet_count", 1)

        sync_create_cells(1)
        self._create_sorted_table("//tmp/t", account="test_account")
        wait(lambda: get("//tmp/t/@resource_usage/tablet_count") == 1)

        # Wait for usage propagation to primary.
        wait(lambda: get("//sys/accounts/test_account/@resource_usage/tablet_count") == 1)
        # Wait for usage propagation from primary.
        wait(
            lambda: get(
                "//sys/accounts/test_account/@resource_usage/tablet_count",
                driver=get_driver(get("//tmp/t/@native_cell_tag") - 10),
            )
            == 1
        )

        with pytest.raises(YtError):
            copy("//tmp/t", "//tmp/t_copy", preserve_account=True)

        self._multicell_set("//sys/accounts/test_account/@resource_limits/tablet_count", 2)
        copy("//tmp/t", "//tmp/t_copy", preserve_account=True)
        self._verify_resource_usage("test_account", "tablet_count", 2)

    # TODO(ifsmirnov): YT-14310
    @authors("shakurov")
    def test_tablet_count_copy_across_accounts(self):
        create_account("test_account1")
        create_account("test_account2")
        self._multicell_set("//sys/accounts/test_account1/@resource_limits/tablet_count", 10)
        self._multicell_set("//sys/accounts/test_account2/@resource_limits/tablet_count", 0)

        sync_create_cells(1)
        self._create_sorted_table("//tmp/t", account="test_account1")

        self._verify_resource_usage("test_account1", "tablet_count", 1)
        wait(lambda: get("//tmp/t/@resource_usage/tablet_count") == 1)

        create("map_node", "//tmp/dir", attributes={"account": "test_account2"})

        with pytest.raises(YtError):
            copy("//tmp/t", "//tmp/dir/t_copy", preserve_account=False)

        self._verify_resource_usage("test_account2", "tablet_count", 0)

        self._multicell_set("//sys/accounts/test_account2/@resource_limits/tablet_count", 1)
        copy("//tmp/t", "//tmp/dir/t_copy", preserve_account=False)

        self._verify_resource_usage("test_account1", "tablet_count", 1)
        self._verify_resource_usage("test_account2", "tablet_count", 1)

    # TODO(ifsmirnov): YT-14310
    @authors("savrus")
    def test_tablet_count_remove(self):
        create_account("test_account")
        self._multicell_set("//sys/accounts/test_account/@resource_limits/tablet_count", 1)
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t", account="test_account")
        self._verify_resource_usage("test_account", "tablet_count", 1)
        remove("//tmp/t")
        self._verify_resource_usage("test_account", "tablet_count", 0)

    # TODO(ifsmirnov): YT-14310
    @authors("savrus")
    def test_tablet_count_set_account(self):
        create_account("test_account")
        sync_create_cells(1)
        self._create_ordered_table("//tmp/t", tablet_count=2)

        # Not implemented: YT-7050
        # set("//sys/accounts/test_account/@resource_limits/tablet_count", 1)
        # with pytest.raises(YtError):
        #    set("//tmp/t/@account", "test_account")

        set("//sys/accounts/test_account/@resource_limits/tablet_count", 2)
        set("//tmp/t/@account", "test_account")
        self._verify_resource_usage("test_account", "tablet_count", 2)

    # TODO(ifsmirnov): YT-14310
    @authors("savrus")
    def test_tablet_count_alter_table(self):
        create_account("test_account")
        sync_create_cells(1)
        self._create_ordered_table("//tmp/t")
        set("//tmp/t/@account", "test_account")

        self._verify_resource_usage("test_account", "tablet_count", 1)
        alter_table("//tmp/t", dynamic=False)
        self._verify_resource_usage("test_account", "tablet_count", 0)

        self._multicell_set("//sys/accounts/test_account/@resource_limits/tablet_count", 0)
        with pytest.raises(YtError):
            alter_table("//tmp/t", dynamic=True)

        self._multicell_set("//sys/accounts/test_account/@resource_limits/tablet_count", 1)
        alter_table("//tmp/t", dynamic=True)
        self._verify_resource_usage("test_account", "tablet_count", 1)

    # TODO(ifsmirnov): YT-14310
    @authors("savrus")
    @pytest.mark.parametrize("mode", ["compressed", "uncompressed"])
    def test_in_memory_accounting(self, mode):
        create_account("test_account")
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        set("//tmp/t/@account", "test_account")

        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 0, "value": "0"}])
        sync_unmount_table("//tmp/t")

        set("//tmp/t/@in_memory_mode", mode)
        with pytest.raises(YtError):
            mount_table("//tmp/t")

        def _verify():
            data_size = get("//tmp/t/@{0}_data_size".format(mode))
            resource_usage = get("//sys/accounts/test_account/@resource_usage")
            committed_resource_usage = get("//sys/accounts/test_account/@committed_resource_usage")
            return (
                resource_usage["tablet_static_memory"] == data_size
                and resource_usage == committed_resource_usage
                and get("//tmp/t/@resource_usage/tablet_count") == 1
                and get("//tmp/t/@resource_usage/tablet_static_memory") == data_size
                and get("//tmp/@recursive_resource_usage/tablet_count") == 1
                and get("//tmp/@recursive_resource_usage/tablet_static_memory") == data_size
            )

        self._multicell_set("//sys/accounts/test_account/@resource_limits/tablet_static_memory", 1000)
        sync_mount_table("//tmp/t")
        wait(_verify)

        sync_compact_table("//tmp/t")
        wait(_verify)

        self._multicell_set("//sys/accounts/test_account/@resource_limits/tablet_static_memory", 0)
        with pytest.raises(YtError):
            insert_rows("//tmp/t", [{"key": 1, "value": "1"}])

        self._multicell_set("//sys/accounts/test_account/@resource_limits/tablet_static_memory", 1000)
        insert_rows("//tmp/t", [{"key": 1, "value": "1"}])

        sync_compact_table("//tmp/t")
        wait(_verify)

        sync_unmount_table("//tmp/t")
        self._verify_resource_usage("test_account", "tablet_static_memory", 0)

    # TODO(ifsmirnov): YT-14310
    @authors("savrus")
    def test_remount_in_memory_accounting(self):
        create_account("test_account")
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        set("//tmp/t/@account", "test_account")
        self._multicell_set("//sys/accounts/test_account/@resource_limits/tablet_static_memory", 2048)

        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 0, "value": "A" * 1024}])
        sync_flush_table("//tmp/t")

        def _test(mode):
            data_size = get("//tmp/t/@{0}_data_size".format(mode))
            sync_unmount_table("//tmp/t")
            set("//tmp/t/@in_memory_mode", mode)
            sync_mount_table("//tmp/t")

            def _check():
                resource_usage = get("//sys/accounts/test_account/@resource_usage")
                committed_resource_usage = get("//sys/accounts/test_account/@committed_resource_usage")
                return (
                    resource_usage["tablet_static_memory"] == data_size and resource_usage == committed_resource_usage
                )

            wait(_check)

        _test("compressed")
        _test("uncompressed")

    # TODO(ifsmirnov): clone to bundle accounting after YT-14311.
    @authors("savrus")
    def test_insert_during_tablet_static_memory_limit_violation(self):
        create_account("test_account")
        set("//sys/accounts/test_account/@resource_limits/tablet_count", 10)
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t1", account="test_account", in_memory_mode="compressed")
        sync_mount_table("//tmp/t1")
        insert_rows("//tmp/t1", [{"key": 0, "value": "0"}])
        sync_flush_table("//tmp/t1")
        assert get("//sys/accounts/test_account/@resource_usage/tablet_static_memory") > 0
        assert get("//sys/accounts/test_account/@violated_resource_limits/tablet_static_memory")
        with pytest.raises(YtError):
            insert_rows("//tmp/t1", [{"key": 1, "value": "1"}])

        self._create_sorted_table("//tmp/t2", account="test_account")
        sync_mount_table("//tmp/t2")
        insert_rows("//tmp/t2", [{"key": 2, "value": "2"}])

    @authors("akozhikhov")
    def test_parent_account_violation(self):
        create_account("parent")
        create_account("child", "parent")

        set("//sys/accounts/parent/@resource_limits/tablet_count", 2)
        set("//sys/accounts/parent/@resource_limits/tablet_static_memory", 1)
        set("//sys/accounts/child/@resource_limits/tablet_count", 2)
        set("//sys/accounts/child/@resource_limits/tablet_static_memory", 1)

        sync_create_cells(1)

        self._create_sorted_table("//tmp/t1", account="child", in_memory_mode="compressed")
        sync_mount_table("//tmp/t1")

        self._create_sorted_table("//tmp/t2", account="parent", in_memory_mode="compressed")
        sync_mount_table("//tmp/t2")

        insert_rows("//tmp/t2", [{"key": 1, "value": "1"}])
        sync_flush_table("//tmp/t2")

        with raises_yt_error("tablet static"):
            insert_rows("//tmp/t1", [{"key": 1, "value": "1"}])

    @authors("ifsmirnov")
    def test_snapshot_account_resource_limits_violation(self):
        create_account("test_account")
        create_tablet_cell_bundle("custom", attributes={
            "options": {"snapshot_account": "test_account"},
            "resource_limits": {"tablet_count": 100}})

        sync_create_cells(1, tablet_cell_bundle="custom")
        self._create_sorted_table("//tmp/t", tablet_cell_bundle="custom")
        create("table", "//tmp/junk", attributes={"account": "test_account"})
        write_table("//tmp/junk", [{"key": "value"}])
        set(
            "//sys/accounts/test_account/@resource_limits/disk_space_per_medium/default",
            0,
        )
        self._multicell_wait(
            lambda driver: lambda: get("//sys/accounts/test_account/@resource_limits/disk_space", driver=driver) == 0
        )

        sync_mount_table("//tmp/t")

        with pytest.raises(YtError):
            insert_rows("//tmp/t", [{"key": 1}])

        set(
            "//sys/accounts/test_account/@resource_limits/disk_space_per_medium/default",
            10 ** 9,
        )

        def _wait_func():
            try:
                insert_rows("//tmp/t", [{"key": 1}])
                return True
            except YtError:
                return False

        wait(_wait_func)

    @authors("savrus", "ifsmirnov")
    def test_chunk_view_accounting(self):
        create_account("test_account")
        create_account("other_account")
        self._multicell_set("//sys/accounts/test_account/@resource_limits/tablet_count", 10)
        self._multicell_set("//sys/accounts/other_account/@resource_limits/tablet_count", 10)
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t", account="test_account")

        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 1, "value": "a"}, {"key": 2, "value": "b"}])
        sync_unmount_table("//tmp/t")
        sync_reshard_table("//tmp/t", [[], [2]])
        assert any(
            "//tmp/t" in value.attributes["owning_nodes"]
            for value in list(get("//sys/chunk_views", attributes=["owning_nodes"]).values())
        )

        def _verify(account, disk_space):
            def wait_func():
                usage = get("//sys/accounts/{}/@resource_usage".format(account))
                committed_usage = get("//sys/accounts/{}/@committed_resource_usage".format(account))
                return usage == committed_usage and usage["disk_space"] == disk_space

            wait(wait_func)

        disk_space = get("//sys/accounts/test_account/@resource_usage/disk_space")
        _verify("test_account", disk_space)

        set("//tmp/t/@account", "other_account")
        _verify("test_account", 0)
        _verify("other_account", disk_space)

    @authors("ifsmirnov")
    @pytest.mark.parametrize("in_memory_mode", ["none", "compressed", "uncompressed"])
    @pytest.mark.parametrize("table_type", ["sorted", "ordered"])
    def test_preload_state_attribute(self, in_memory_mode, table_type):
        sync_create_cells(1)
        if table_type == "sorted":
            self._create_sorted_table("//tmp/t")
        else:
            self._create_ordered_table("//tmp/t")
        set("//tmp/t/@in_memory_mode", in_memory_mode)
        sync_mount_table("//tmp/t")
        assert get("//tmp/t/@preload_state") == "complete"
        insert_rows("//tmp/t", [{"key": 1, "value": "a"}])
        sync_unmount_table("//tmp/t")
        sync_mount_table("//tmp/t")
        wait(lambda: get("//tmp/t/@preload_state") == "complete")

    @authors("lukyan")
    def test_changelog_resource_limits(self):
        create_account("test_account")
        create_tablet_cell_bundle("custom", attributes={"options": {
            "changelog_account": "test_account",
            "snapshot_account": "test_account"
        }})

        cell_id = sync_create_cells(1, tablet_cell_bundle="custom")[0]
        self._create_sorted_table("//tmp/t", tablet_cell_bundle="custom")
        sync_mount_table("//tmp/t")

        changelogs = ls("//sys/tablet_cells/{0}/changelogs".format(cell_id))
        assert len(changelogs) > 0

        for changelog in changelogs:
            assert get("//sys/tablet_cells/{0}/changelogs/{1}/@account".format(cell_id, changelog)) == "test_account"

        set("//sys/accounts/test_account/@resource_limits/node_count", 0)

        with pytest.raises(YtError):
            build_snapshot(cell_id=cell_id)

        time.sleep(3.0)
        assert_items_equal(changelogs, ls("//sys/tablet_cells/{0}/changelogs".format(cell_id)))

    @authors("akozhikhov")
    def test_bundle_account_violation_attribute(self):
        create_account("test_account")
        self._multicell_set("//sys/accounts/test_account/@resource_limits/tablet_count", 1)
        get("//sys/accounts/test_account/@resource_limits")

        create_tablet_cell_bundle("b")
        set("//sys/tablet_cell_bundles/b/@options/changelog_account", "test_account")
        set("//sys/tablet_cell_bundles/b/@options/snapshot_account", "test_account")
        sync_create_cells(1, tablet_cell_bundle="b")

        self._create_sorted_table("//tmp/t", tablet_cell_bundle="b", account="test_account")

        # Fill some disk space.
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 1, "value": "foo"}])
        sync_flush_table("//tmp/t")

        assert not get("//sys/tablet_cell_bundles/b/@changelog_account_violated_resource_limits/node_count")
        assert not get("//sys/tablet_cell_bundles/b/@snapshot_account_violated_resource_limits/node_count")
        self._multicell_set("//sys/accounts/test_account/@resource_limits/node_count", 0)
        assert get("//sys/tablet_cell_bundles/b/@changelog_account_violated_resource_limits/node_count")
        assert get("//sys/tablet_cell_bundles/b/@snapshot_account_violated_resource_limits/node_count")

        assert not get("//sys/tablet_cell_bundles/b/@changelog_account_violated_resource_limits/disk_space_per_medium/default")
        self._multicell_set("//sys/accounts/test_account/@resource_limits/disk_space_per_medium/default", 10)
        assert get("//sys/tablet_cell_bundles/b/@changelog_account_violated_resource_limits/disk_space_per_medium/default")


class TestDynamicTablesResourceLimitsMulticell(TestDynamicTablesResourceLimits):
    NUM_SECONDARY_MASTER_CELLS = 2


class TestDynamicTablesResourceLimitsPortal(TestDynamicTablesResourceLimitsMulticell):
    ENABLE_TMP_PORTAL = True


class TestDynamicTablesResourceLimitsShardedTx(TestDynamicTablesResourceLimitsPortal):
    NUM_SECONDARY_MASTER_CELLS = 3
    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["cypress_node_host"]},
        "13": {"roles": ["transaction_coordinator"]},
    }


##################################################################


class TestPerBundleAccounting(DynamicTablesResourceLimitsBase):
    def _verify_resource_usage(self, bundle, resource, expected):
        def resource_usage_matches(driver):
            return lambda: (
                get(
                    "//sys/tablet_cell_bundles/{0}/@resource_usage/{1}".format(bundle, resource),
                    driver=driver,
                )
                == expected
            )

        self._multicell_wait(resource_usage_matches)

    @authors("ifsmirnov")
    def test_tablet_count_limit_create(self):
        create_tablet_cell_bundle("b")
        sync_create_cells(1, tablet_cell_bundle="b")

        self._multicell_set("//sys/tablet_cell_bundles/b/@resource_limits/tablet_count", 0)
        with raises_yt_error(yt_error_codes.BundleResourceLimitExceeded):
            self._create_sorted_table("//tmp/t", tablet_cell_bundle="b")
        with raises_yt_error(yt_error_codes.BundleResourceLimitExceeded):
            self._create_ordered_table("//tmp/t", tablet_cell_bundle="b")

        self._multicell_set("//sys/tablet_cell_bundles/b/@resource_limits/tablet_count", 1)
        with raises_yt_error(yt_error_codes.BundleResourceLimitExceeded):
            self._create_ordered_table("//tmp/t", tablet_cell_bundle="b", tablet_count=2)
        with raises_yt_error(yt_error_codes.BundleResourceLimitExceeded):
            self._create_sorted_table("//tmp/t", tablet_cell_bundle="b", pivot_keys=[[], [1]])

        assert get("//sys/tablet_cell_bundles/b/@ref_counter") == 2

        self._multicell_set("//sys/tablet_cell_bundles/b/@resource_limits/tablet_count", 4)
        self._create_ordered_table("//tmp/t1", tablet_cell_bundle="b", tablet_count=2)
        self._verify_resource_usage("b", "tablet_count", 2)
        self._create_sorted_table("//tmp/t2", tablet_cell_bundle="b", pivot_keys=[[], [1]])
        self._verify_resource_usage("b", "tablet_count", 4)

    @authors("ifsmirnov")
    def test_mount_mounted_table(self):
        create_tablet_cell_bundle("b")
        sync_create_cells(1, tablet_cell_bundle="b")
        self._multicell_set("//sys/tablet_cell_bundles/b/@resource_limits/tablet_count", 2)

        def _create_table(table_name):
            self._create_sorted_table(table_name, tablet_cell_bundle="b", external_cell_tag=11)

            sync_mount_table(table_name)
            insert_rows(table_name, [{"key": 0, "value": "0"}])
            sync_unmount_table(table_name)

        _create_table("//tmp/t0")
        _create_table("//tmp/t1")

        data_size = get("//tmp/t0/@uncompressed_data_size")
        self._multicell_set(
            "//sys/tablet_cell_bundles/b/@resource_limits/tablet_static_memory",
            data_size,
        )

        set("//tmp/t0/@in_memory_mode", "uncompressed")
        sync_mount_table("//tmp/t0")

        self._verify_resource_usage("b", "tablet_static_memory", data_size)

        set("//tmp/t1/@in_memory_mode", "uncompressed")
        with raises_yt_error(yt_error_codes.BundleResourceLimitExceeded):
            sync_mount_table("//tmp/t1")

        remount_table("//tmp/t0")
        sync_unmount_table("//tmp/t0")
        self._verify_resource_usage("b", "tablet_static_memory", 0)
        self._verify_resource_usage("b", "tablet_static_memory", 0)

    @authors("ifsmirnov")
    def test_tablet_count_limit_reshard(self):
        create_tablet_cell_bundle("b")
        sync_create_cells(1, tablet_cell_bundle="b")
        self._multicell_set("//sys/tablet_cell_bundles/b/@resource_limits/tablet_count", 2)
        self._create_sorted_table("//tmp/t1", tablet_cell_bundle="b")
        self._create_ordered_table("//tmp/t2", tablet_cell_bundle="b")

        # Wait for resource usage since tables can be placed to different cells.
        self._multicell_wait(
            lambda driver: lambda: get(
                "//sys/tablet_cell_bundles/b/@resource_usage/tablet_count",
                driver=driver,
            )
            == 2
        )

        with raises_yt_error(yt_error_codes.BundleResourceLimitExceeded):
            reshard_table("//tmp/t1", [[], [1]])
        with raises_yt_error(yt_error_codes.BundleResourceLimitExceeded):
            reshard_table("//tmp/t2", 2)

        self._multicell_set("//sys/tablet_cell_bundles/b/@resource_limits/tablet_count", 4)
        sync_reshard_table("//tmp/t1", [[], [1]])
        sync_reshard_table("//tmp/t2", 2)
        self._verify_resource_usage("b", "tablet_count", 4)

    @authors("ifsmirnov")
    def test_tablet_count_limit_copy(self):
        create_tablet_cell_bundle("b")
        sync_create_cells(1, tablet_cell_bundle="b")
        self._multicell_set("//sys/tablet_cell_bundles/b/@resource_limits/tablet_count", 1)

        self._create_sorted_table("//tmp/t", tablet_cell_bundle="b")
        wait(lambda: get("//tmp/t/@resource_usage/tablet_count") == 1)

        # Wait for usage propagation to primary.
        wait(lambda: get("//sys/tablet_cell_bundles/b/@resource_usage/tablet_count") == 1)
        # Wait for usage propagation from primary.
        wait(
            lambda: get(
                "//sys/tablet_cell_bundles/b/@resource_usage/tablet_count",
                driver=get_driver(get("//tmp/t/@native_cell_tag") - 10),
            )
            == 1
        )

        # Currently the table cannot be moved without 2x temporary quota.
        with raises_yt_error(yt_error_codes.BundleResourceLimitExceeded):
            move("//tmp/t", "//tmp/t_other")

        with raises_yt_error(yt_error_codes.BundleResourceLimitExceeded):
            copy("//tmp/t", "//tmp/t_copy")

        self._multicell_set("//sys/tablet_cell_bundles/b/@resource_limits/tablet_count", 2)
        copy("//tmp/t", "//tmp/t_copy")
        self._verify_resource_usage("b", "tablet_count", 2)

    @authors("ifsmirnov")
    def test_tablet_count_remove(self):
        create_tablet_cell_bundle("b")
        sync_create_cells(1, tablet_cell_bundle="b")
        self._multicell_set("//sys/tablet_cell_bundles/b/@resource_limits/tablet_count", 1)
        self._create_sorted_table("//tmp/t", tablet_cell_bundle="b")
        self._verify_resource_usage("b", "tablet_count", 1)
        remove("//tmp/t")
        self._verify_resource_usage("b", "tablet_count", 0)

    @authors("ifsmirnov")
    def test_tablet_count_set_bundle(self):
        create_tablet_cell_bundle("b")
        sync_create_cells(1, tablet_cell_bundle="b")
        self._multicell_set("//sys/tablet_cell_bundles/b/@resource_limits/tablet_count", 1)
        self._create_ordered_table("//tmp/t", tablet_count=2)
        self._verify_resource_usage("default", "tablet_count", 2)

        multicell_sleep()
        with raises_yt_error(yt_error_codes.BundleResourceLimitExceeded):
            set("//tmp/t/@tablet_cell_bundle", "b")

        self._multicell_set("//sys/tablet_cell_bundles/b/@resource_limits/tablet_count", 2)
        set("//tmp/t/@tablet_cell_bundle", "b")
        self._verify_resource_usage("b", "tablet_count", 2)

    @authors("ifsmirnov")
    def test_tablet_count_alter_table(self):
        create_tablet_cell_bundle("b")
        sync_create_cells(1, tablet_cell_bundle="b")
        self._create_ordered_table("//tmp/t")
        self._multicell_set("//sys/tablet_cell_bundles/b/@resource_limits/tablet_count", 1)
        set("//tmp/t/@tablet_cell_bundle", "b")

        self._verify_resource_usage("b", "tablet_count", 1)
        alter_table("//tmp/t", dynamic=False)
        self._verify_resource_usage("b", "tablet_count", 0)

        self._multicell_set("//sys/tablet_cell_bundles/b/@resource_limits/tablet_count", 0)
        with raises_yt_error(yt_error_codes.BundleResourceLimitExceeded):
            alter_table("//tmp/t", dynamic=True)

        self._multicell_set("//sys/tablet_cell_bundles/b/@resource_limits/tablet_count", 1)
        alter_table("//tmp/t", dynamic=True)
        self._verify_resource_usage("b", "tablet_count", 1)

    @authors("ifsmirnov")
    @pytest.mark.parametrize("mode", ["compressed", "uncompressed"])
    def test_in_memory_accounting(self, mode):
        create_tablet_cell_bundle("b")
        sync_create_cells(1, tablet_cell_bundle="b")
        self._multicell_set("//sys/tablet_cell_bundles/b/@resource_limits/tablet_count", 1)
        self._create_sorted_table("//tmp/t")
        set("//tmp/t/@tablet_cell_bundle", "b")

        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 0, "value": "0"}])
        sync_unmount_table("//tmp/t")

        set("//tmp/t/@in_memory_mode", mode)
        with raises_yt_error(yt_error_codes.BundleResourceLimitExceeded):
            mount_table("//tmp/t")

        def _verify():
            data_size = get("//tmp/t/@{0}_data_size".format(mode))
            resource_usage = get("//sys/tablet_cell_bundles/b/@resource_usage")
            return (
                resource_usage["tablet_static_memory"] == data_size
                and get("//tmp/t/@resource_usage/tablet_count") == 1
                and get("//tmp/t/@resource_usage/tablet_static_memory") == data_size
                and get("//tmp/@recursive_resource_usage/tablet_count") == 1
                and get("//tmp/@recursive_resource_usage/tablet_static_memory") == data_size
            )

        self._multicell_set("//sys/tablet_cell_bundles/b/@resource_limits/tablet_static_memory", 1000)
        sync_mount_table("//tmp/t")
        wait(_verify)

        sync_compact_table("//tmp/t")
        wait(_verify)

        self._multicell_set("//sys/tablet_cell_bundles/b/@resource_limits/tablet_static_memory", 0)
        # TODO(ifsmirnov): YT-14311 - consider new tablet resources at node.
        #  with pytest.raises(YtError):
        #      insert_rows("//tmp/t", [{"key": 1, "value": "1"}])

        self._multicell_set("//sys/tablet_cell_bundles/b/@resource_limits/tablet_static_memory", 1000)
        insert_rows("//tmp/t", [{"key": 1, "value": "1"}])

        sync_compact_table("//tmp/t")
        wait(_verify)

        sync_unmount_table("//tmp/t")
        self._verify_resource_usage("b", "tablet_static_memory", 0)

    @authors("ifsmirnov")
    def test_remount_in_memory_accounting(self):
        create_tablet_cell_bundle("b")
        sync_create_cells(1, tablet_cell_bundle="b")
        self._multicell_set("//sys/tablet_cell_bundles/b/@resource_limits/tablet_count", 1)
        self._multicell_set("//sys/tablet_cell_bundles/b/@resource_limits/tablet_static_memory", 2048)
        self._create_sorted_table("//tmp/t")
        set("//tmp/t/@tablet_cell_bundle", "b")

        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": 0, "value": "A" * 1024}])
        sync_flush_table("//tmp/t")

        def _test(mode):
            data_size = get("//tmp/t/@{0}_data_size".format(mode))
            sync_unmount_table("//tmp/t")
            set("//tmp/t/@in_memory_mode", mode)
            sync_mount_table("//tmp/t")

            def _check():
                resource_usage = get("//sys/tablet_cell_bundles/b/@resource_usage")
                return resource_usage["tablet_static_memory"] == data_size

            wait(_check)

        _test("compressed")
        _test("uncompressed")

        with pytest.raises(YtError):
            set("//tmp/t/@in_memory_mode", "none")

    @authors("ifsmirnov")
    def test_balancer_cannot_exceed_tablet_count_limit(self):
        create_tablet_cell_bundle("b")
        self._multicell_set("//sys/tablet_cell_bundles/b/@resource_limits/tablet_count", 1)
        sync_create_cells(1, tablet_cell_bundle="b")
        self._create_sorted_table("//tmp/t", tablet_cell_bundle="b", dynamic=False)
        for i in range(5):
            write_table("<append=%true>//tmp/t", [{"key": i}])
        alter_table("//tmp/t", dynamic=True)

        compressed_data_size = get("//tmp/t/@compressed_data_size") // 5
        uncompressed_data_size = get("//tmp/t/@uncompressed_data_size") // 5

        # Prepare partitions: balancer will not split tablet with one partition.
        set("//tmp/t/@min_partition_data_size", compressed_data_size - 1)
        set("//tmp/t/@desired_partition_data_size", compressed_data_size)
        set("//tmp/t/@max_partition_data_size", compressed_data_size + 1)

        set("//tmp/t/@tablet_balancer_config", {
            "enable_auto_reshard": False,
            "min_tablet_size": uncompressed_data_size - 1,
            "desired_tablet_size": uncompressed_data_size,
            "max_tablet_size": uncompressed_data_size + 1})

        sync_mount_table("//tmp/t")
        wait(lambda: get("//tmp/t/@tablets/0/statistics/partition_count") == 5)
        action_ids = reshard_table_automatic("//tmp/t", keep_actions=True)
        assert len(action_ids) == 1
        wait(lambda: get("//sys/tablet_actions/{}/@state".format(action_ids[0])) == "failed")
        wait(lambda: get("//tmp/t/@tablet_count") == 1)
        self._verify_resource_usage("b", "tablet_count", 1)

    @authors("ifsmirnov")
    def test_no_in_memory_writes_when_resource_limits_violated(self):
        create_tablet_cell_bundle("b")
        self._multicell_set("//sys/tablet_cell_bundles/b/@resource_limits/tablet_static_memory", 100000)
        create_account("a")
        self._multicell_set("//sys/accounts/a/@resource_limits/tablet_static_memory", 1000000)
        self._multicell_set("//sys/accounts/a/@resource_limits/tablet_count", 1000000)
        sync_create_cells(1, tablet_cell_bundle="b")
        self._create_sorted_table(
            "//tmp/t",
            account="a",
            tablet_cell_bundle="b",
            dynamic=False,
            in_memory_mode="compressed")
        write_table("//tmp/t", [{"key": 1}])
        alter_table("//tmp/t", dynamic=True)
        sync_mount_table("//tmp/t")

        self._multicell_set("//sys/tablet_cell_bundles/b/@resource_limits/tablet_static_memory", 0)
        self._multicell_set("//sys/accounts/a/@resource_limits/tablet_static_memory", 0)
        self._multicell_set("//sys/@config/tablet_manager/enable_tablet_resource_validation", False)
        self._multicell_set("//sys/@config/security_manager/enable_tablet_resource_validation", False)

        insert_rows("//tmp/t", [{"key": 1}])

        self._multicell_set("//sys/@config/security_manager/enable_tablet_resource_validation", True)
        with raises_yt_error(yt_error_codes.AccountLimitExceeded):
            insert_rows("//tmp/t", [{"key": 1}])

        self._multicell_set("//sys/@config/security_manager/enable_tablet_resource_validation", False)
        insert_rows("//tmp/t", [{"key": 1}])

        self._multicell_set("//sys/@config/tablet_manager/enable_tablet_resource_validation", True)
        with raises_yt_error(yt_error_codes.BundleResourceLimitExceeded):
            insert_rows("//tmp/t", [{"key": 1}])

        self._multicell_set("//sys/@config/tablet_manager/enable_tablet_resource_validation", False)
        insert_rows("//tmp/t", [{"key": 1}])


class TestPerBundleAccountingMulticell(TestPerBundleAccounting):
    NUM_SECONDARY_MASTER_CELLS = 2


class TestPerBundleAccountingPortal(TestPerBundleAccountingMulticell):
    ENABLE_TMP_PORTAL = True


class TestPerBundleAccountingShardedTx(TestPerBundleAccountingPortal):
    NUM_SECONDARY_MASTER_CELLS = 3
    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["cypress_node_host"]},
        "13": {"roles": ["transaction_coordinator"]},
    }
