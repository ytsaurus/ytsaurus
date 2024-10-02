from typing import Any

import pytest

from yt.yt_sync.core.settings import Settings


class TestSettingsMake:
    def test_make_defaults(self):
        settings = Settings.make({})
        assert Settings.REPLICATED_DB == settings.db_type
        assert 1 == settings.min_sync_clusters
        assert not settings.always_async
        assert settings.wait_in_sync_replicas is True
        assert settings.ensure_collocation is False
        assert settings.collocation_name is None

    def test_make_db_type(self):
        assert Settings.REPLICATED_DB == Settings.make({}).db_type
        assert Settings.REPLICATED_DB == Settings.make({"use_chaos": False}).db_type
        assert Settings.CHAOS_DB == Settings.make({"use_chaos": True}).db_type

    @pytest.mark.parametrize("rtt_enabled", [True, False])
    def test_make_always_async(self, rtt_enabled: bool):
        cluster_settings = {
            "master": "primary",
            "enable_replicated_table_tracker": rtt_enabled,
            "sync_replicas": ["remote0"],
            "async_replicas": ["remote1", "remote2"],
            "attributes": {
                "remote2": {
                    "enable_replicated_table_tracker": False,
                },
            },
        }
        settings = Settings.make(cluster_settings)
        if rtt_enabled:
            assert set(["remote2"]) == settings.always_async
        else:
            assert not settings.always_async

    @pytest.mark.parametrize("rtt_enabled", [True, False])
    def test_make_no_always_async(self, rtt_enabled: bool):
        cluster_settings = {
            "master": "primary",
            "enable_replicated_table_tracker": rtt_enabled,
            "sync_replicas": ["remote0"],
            "async_replicas": ["remote1", "remote2"],
            "attributes": {},
        }
        settings = Settings.make(cluster_settings)
        assert not settings.always_async


class TestSettingsIsOk:
    @pytest.mark.parametrize("allow_full_downtime", [True, False])
    @pytest.mark.parametrize("is_chaos", [True, False])
    def test_is_add_remove_allowed(self, allow_full_downtime: bool, is_chaos: bool):
        settings = Settings(
            db_type=Settings.CHAOS_DB if is_chaos else Settings.REPLICATED_DB,
            allow_table_full_downtime=allow_full_downtime,
        )
        settings.is_ok()

    def test_is_ok(self):
        Settings.make({}).is_ok()

    def test_no_collocation_name(self):
        settings = Settings.make({})
        settings.ensure_collocation = True
        settings.collocation_name = None
        with pytest.raises(AssertionError):
            settings.is_ok()


class TestSettingsGetOperationSpec:
    def test_unknown_operation(self):
        settings = Settings.make({})
        with pytest.raises(AssertionError):
            settings.get_operation_spec("c", "unknown")

    @pytest.mark.parametrize(
        "operation,spec",
        [
            ("map", Settings.DEFAULT_MAP_SPEC),
            ("sort", Settings.DEFAULT_SORT_SPEC),
            ("remote_copy", Settings.DEFAULT_REMOTE_COPY_SPEC),
        ],
    )
    def test_no_spec(self, operation: str, spec: dict[str, Any]):
        settings = Settings.make({})
        assert settings.get_operation_spec("c", operation) == spec

    def test_get_spec(self):
        settings = Settings.make({})
        settings.operations_spec = {
            "default": {"map": {"from": "default"}, "sort": {"from": "default"}},
            "c1": {"map": {"from": "c1"}},
            "c2": {"sort": {"from": "c2"}},
            "c3": {"map": {"from": "c3"}, "sort": {"from": "c3"}},
            "c4": {"remote_copy": {"from": "c4"}},
        }

        assert settings.get_operation_spec("c4", "map")["from"] == "default"
        assert settings.get_operation_spec("c4", "sort")["from"] == "default"
        assert settings.get_operation_spec("c4", "remote_copy")["from"] == "c4"

        assert settings.get_operation_spec("c3", "map")["from"] == "c3"
        assert settings.get_operation_spec("c3", "sort")["from"] == "c3"
        assert settings.get_operation_spec("c3", "remote_copy") == Settings.DEFAULT_REMOTE_COPY_SPEC

        assert settings.get_operation_spec("c2", "map")["from"] == "default"
        assert settings.get_operation_spec("c2", "sort")["from"] == "c2"
        assert settings.get_operation_spec("c2", "remote_copy") == Settings.DEFAULT_REMOTE_COPY_SPEC

        assert settings.get_operation_spec("c1", "map")["from"] == "c1"
        assert settings.get_operation_spec("c1", "sort")["from"] == "default"
        assert settings.get_operation_spec("c1", "remote_copy") == Settings.DEFAULT_REMOTE_COPY_SPEC

    def test_get_default_spec_remote_copy(self):
        settings = Settings.make({})
        settings.operations_spec = {
            "default": {"remote_copy": {"from": "default"}},
            "c1": {"remote_copy": {"from": "c1"}},
        }

        assert settings.get_operation_spec("c1", "remote_copy")["from"] == "c1"
        assert settings.get_operation_spec("c2", "remote_copy")["from"] == "default"


class TestSettingsGetBatchSizeForParallel:
    def test(self):
        assert Settings(db_type=Settings.REPLICATED_DB).get_batch_size_for_parallel("ensure") == 0
        assert (
            Settings(
                db_type=Settings.REPLICATED_DB, batch_size_for_parallel={"default": 1}
            ).get_batch_size_for_parallel("ensure")
            == 1
        )
        assert (
            Settings(
                db_type=Settings.REPLICATED_DB, batch_size_for_parallel={"default": 1, "ensure": 2}
            ).get_batch_size_for_parallel("ensure")
            == 2
        )
