from yt_dynamic_tables_base import DynamicTablesBase

from yt_commands import (
    authors, wait, get, set, remount_table,
    sync_create_cells, sync_mount_table, raises_yt_error,
)

import yt.yson as yson

##################################################################


class TestMountConfig(DynamicTablesBase):
    @staticmethod
    def _validate_dict_subset(full, subset):
        for key, value in subset.items():
            assert full.get(key) == value

    @staticmethod
    def _check_dict_subset(full, subset):
        for key, value in subset.items():
            if full.get(key) != value:
                return False
        return True

    @authors("ifsmirnov")
    def test_mount_config_global_patch(self):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")

        set("//tmp/t/@min_data_ttl", 1)
        set("//tmp/t/@max_data_ttl", 2)

        set("//sys/@config/tablet_manager/mount_config_template_patch", {
            "min_data_ttl": 100,
            "max_dynamic_store_row_count": 200,
        })
        set("//sys/@config/tablet_manager/mount_config_patch", {
            "max_data_ttl": 300,
            "max_dynamic_store_pool_size": 400,
        })

        expected = {
            "min_data_ttl": 1,
            "max_data_ttl": 300,
            "max_dynamic_store_row_count": 200,
            "max_dynamic_store_pool_size": 400,
        }

        sync_mount_table("//tmp/t")
        get(f"//sys/tablets/{tablet_id}/orchid/raw_settings")
        get(f"//sys/tablets/{tablet_id}/orchid/errors")
        self._validate_dict_subset(get(f"//sys/tablets/{tablet_id}/orchid/config"), expected)

        set("//tmp/t/@auto_compaction_period", 12345)
        set("//sys/@config/tablet_manager/mount_config_template_patch", {
            "min_data_ttl": 100,
            "compaction_data_size_base": 500,
        })
        set("//sys/@config/tablet_manager/mount_config_patch", {
            "max_dynamic_store_pool_size": 600,
        })

        expected["compaction_data_size_base"] = 500
        expected["max_data_ttl"] = 2
        expected["max_dynamic_store_pool_size"] = 600
        expected["auto_compaction_period"] = yson.YsonEntity()
        del expected["max_dynamic_store_row_count"]
        wait(lambda: self._check_dict_subset(get(f"//sys/tablets/{tablet_id}/orchid/config"), expected))

        remount_table("//tmp/t")
        expected["auto_compaction_period"] = 12345
        wait(lambda: self._check_dict_subset(get(f"//sys/tablets/{tablet_id}/orchid/config"), expected))

    @authors("ifsmirnov")
    def test_io_config_global_patch(self):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        sync_mount_table("//tmp/t")
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")

        set("//sys/@config/tablet_manager/io_config_patch", {
            "store_reader_config": {
                "retry_timeout": 180000 + 1,
            },
            "hunk_reader_config": {
                "periodic_update_delay": 10000 + 1,
            },
            "store_writer_config": {
                "max_meta_size": 31457280 + 1,
            },
            "hunk_writer_config": {
                "desired_block_size": 16777216 + 1,
            },
        })

        def _get_orchid(suffix):
            return get(f"//sys/tablets/{tablet_id}/orchid{suffix}")

        # Verify that the patch is applied.
        wait(lambda: _get_orchid("/store_reader_config/retry_timeout") == 180000 + 1)
        assert _get_orchid("/hunk_reader_config/periodic_update_delay") == 10000 + 1
        assert _get_orchid("/store_writer_config/max_meta_size") == 31457280 + 1
        assert _get_orchid("/hunk_writer_config/desired_block_size") == 16777216 + 1

        set("//tmp/t/@chunk_reader", {"retry_timeout": 180000 + 2})
        set("//tmp/t/@hunk_chunk_reader", {"periodic_update_delay": 10000 + 2})
        set("//tmp/t/@chunk_writer", {"max_meta_size": 31457280 + 2})
        set("//tmp/t/@hunk_chunk_writer", {"desired_block_size": 16777216 + 2})

        # Verify that the patch overrides explicit per-table settings.
        remount_table("//tmp/t")
        assert _get_orchid("/store_reader_config/retry_timeout") == 180000 + 1
        assert _get_orchid("/hunk_reader_config/periodic_update_delay") == 10000 + 1
        assert _get_orchid("/store_writer_config/max_meta_size") == 31457280 + 1
        assert _get_orchid("/hunk_writer_config/desired_block_size") == 16777216 + 1

        # Verify that when the patch is removed table settings are rolled back.
        set("//sys/@config/tablet_manager/io_config_patch", {})
        wait(lambda: _get_orchid("/store_reader_config/retry_timeout") == 180000 + 2)
        assert _get_orchid("/hunk_reader_config/periodic_update_delay") == 10000 + 2
        assert _get_orchid("/store_writer_config/max_meta_size") == 31457280 + 2
        assert _get_orchid("/hunk_writer_config/desired_block_size") == 16777216 + 2

    @authors("ifsmirnov")
    def test_deep_patch(self):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        set("//tmp/t/@mount_config", {
            "relative_replication_throttler": {
                "ratio": 5.0,
            },
            "replication_throttler": {
                "limit": 1234.0,
            },
            "flush_throttler": {
                "period": 2222,
                "limit": 3333.0,
            },
        })
        set("//tmp/t/@hunk_chunk_writer", {
            "node_channel": {
                "retry_attempts": 101,
            },
        })

        sync_mount_table("//tmp/t")
        tablet_id = get("//tmp/t/@tablets/0/tablet_id")

        def _get_orchid(suffix):
            return get(f"//sys/tablets/{tablet_id}/orchid{suffix}")

        set("//sys/@config/tablet_manager/mount_config_template_patch", {
            "relative_replication_throttler": {
                "enable": True,
            },
            "flush_throttler": {
                "period": 5555,
            },
        })
        set("//sys/@config/tablet_manager/mount_config_patch", {
            "replication_throttler": {
                "period": 5000,
            },
            "flush_throttler": {
                "limit": 6666.0,
            },
        })
        set("//sys/@config/tablet_manager/io_config_patch/hunk_writer_config", {
            "node_channel": {
                "retry_backoff_time": 12345,
            },
        })

        # Check that the last config is applied.
        wait(lambda: _get_orchid("/hunk_writer_config/node_channel/retry_backoff_time") == 12345)

        assert _get_orchid("/config/replication_throttler") == {"limit": 1234.0, "period": 5000}
        assert _get_orchid("/config/relative_replication_throttler/ratio") == 5.0
        assert bool(_get_orchid("/config/relative_replication_throttler/enable")) is True
        assert _get_orchid("/config/flush_throttler/period") == 2222
        assert _get_orchid("/config/flush_throttler/limit") == 6666.0
        assert _get_orchid("/hunk_writer_config/node_channel/retry_attempts") == 101

    @authors("ifsmirnov")
    def test_conflicting_patches(self):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        sync_mount_table("//tmp/t")

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")

        def _get_orchid(suffix):
            return get(f"//sys/tablets/{tablet_id}/orchid{suffix}")

        set("//sys/@config/tablet_manager/mount_config_template_patch", {
            "min_compaction_store_count": 5,
        })
        wait(lambda: _get_orchid("/config/min_compaction_store_count") == 5)

        set("//sys/@config/tablet_manager/mount_config_patch", {
            "max_compaction_store_count": 3,
        })
        wait(lambda: get("//tmp/t/@tablet_error_count") == 1)
        assert _get_orchid("/config/min_compaction_store_count") == 3
        assert _get_orchid("/config/max_compaction_store_count") == 5

        set("//sys/@config/tablet_manager/mount_config_patch", {})
        wait(lambda: _get_orchid("/config/min_compaction_store_count") == 5)
        wait(lambda: get("//tmp/t/@tablet_error_count") == 0)

    @authors("ifsmirnov")
    def test_patch_conflicts_with_table(self):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        sync_mount_table("//tmp/t")

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")

        def _get_orchid(suffix):
            return get(f"//sys/tablets/{tablet_id}/orchid{suffix}")

        set("//sys/@config/tablet_manager/mount_config_template_patch", {
            "min_compaction_store_count": 5,
            "compaction_data_size_base": 12345,
        })
        wait(lambda: _get_orchid("/config/min_compaction_store_count") == 5)

        set("//tmp/t/@max_compaction_store_count", 3)
        remount_table("//tmp/t")
        wait(lambda: get("//tmp/t/@tablet_error_count") == 1)
        assert _get_orchid("/config/min_compaction_store_count") == 3
        assert _get_orchid("/config/max_compaction_store_count") == 3
        assert _get_orchid("/config/compaction_data_size_base") != 12345

    @authors("ifsmirnov")
    def test_patch_validation(self):
        with raises_yt_error():
            set("//sys/@config/tablet_manager/mount_config_patch", {
                "min_compaction_store_count": "foobar",
            })
        with raises_yt_error():
            set("//sys/@config/tablet_manager/io_config_patch/xxx", "yyy")
        with raises_yt_error():
            set("//sys/@config/tablet_manager/io_config_patch/hunk_writer_config", {
                "node_channel": "abc",
            })

    @authors("ifsmirnov")
    def test_garbage_in_patches(self):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        set("//tmp/t/@mount_config/foo", "bar")
        sync_mount_table("//tmp/t")

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")

        def _get_orchid(suffix):
            return get(f"//sys/tablets/{tablet_id}/orchid{suffix}")

        set("//sys/@config/tablet_manager/mount_config_patch/baz", "qux")
        wait(lambda: _get_orchid("/raw_settings/global_patch/mount_config_patch").get("baz") == "qux")
        assert _get_orchid("/raw_settings/provided_extra_config") == {"foo": "bar"}

    @authors("ifsmirnov")
    def test_experiments_simple(self):
        sync_create_cells(1)
        self._create_sorted_table("//tmp/t")
        sync_mount_table("//tmp/t")

        tablet_id = get("//tmp/t/@tablets/0/tablet_id")

        def _get_orchid(suffix):
            return get(f"//sys/tablets/{tablet_id}/orchid{suffix}")

        set("//sys/@config/tablet_manager/table_config_experiments/foo", {
            "fraction": 1.0,
            "patch": {
                "mount_config_patch": {
                    "min_compaction_store_count": 4,
                },
            },
        })
        set("//sys/@config/tablet_manager/mount_config_template_patch/compaction_data_size_base", 1234)

        # Check that the experiment is not auto applied.
        wait(lambda: _get_orchid("/config/compaction_data_size_base") == 1234)
        assert _get_orchid("/config/min_compaction_store_count") == 3

        set("//sys/@config/tablet_manager/table_config_experiments/foo/auto_apply", True)
        wait(lambda: _get_orchid("/config/min_compaction_store_count") == 4)

        set("//sys/@config/tablet_manager/table_config_experiments/foo/sorted", False)
        wait(lambda: _get_orchid("/config/min_compaction_store_count") == 3)


##################################################################


class TestMountConfigMulticell(TestMountConfig):
    NUM_SECONDARY_MASTER_CELLS = 2
