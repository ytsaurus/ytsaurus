from copy import deepcopy

import pytest

from yt.yt_sync.action import ActionBatch
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtCluster
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.settings import Settings
from yt.yt_sync.core.spec import Table
from yt.yt_sync.core.state_builder import DesiredStateBuilder
from yt.yt_sync.core.test_lib import MockYtClientFactory
from yt.yt_sync.core.test_lib import TableSettingsBuilder
from yt.yt_sync.scenario.helpers import ReplicaSwitcher


class TestSwitcherFactory:
    @staticmethod
    def _build_db(table_spec: Table) -> YtDatabase:
        db = YtDatabase()
        state_builder = DesiredStateBuilder(MockYtClientFactory({}), db, False, True)
        state_builder.add_table(table_spec)
        return db

    @staticmethod
    @pytest.fixture
    def default_table_builder(table_path: str, default_schema: Types.Schema) -> TableSettingsBuilder:
        return (
            TableSettingsBuilder(table_path)
            .with_schema(default_schema)
            .with_main("primary")
            .with_sync_replica("remote0")
            .with_async_replica("remote1")
        )

    @pytest.fixture
    def settings(self) -> Settings:
        return Settings(db_type=Settings.REPLICATED_DB, use_deprecated_spec_format=False)

    def test_switcher_ctor(self, settings: Settings):
        db = YtDatabase()
        db.add_or_get_cluster(YtCluster.make(YtCluster.Type.MAIN, "primary", False))
        switcher = ReplicaSwitcher(settings, db)
        assert isinstance(switcher, ReplicaSwitcher)

    def test_switcher_ctor_without_main_cluster(self, settings: Settings):
        db = YtDatabase()
        with pytest.raises(AssertionError):
            ReplicaSwitcher(settings, db)

    def test_iterate_clusters(self, default_table_builder: TableSettingsBuilder, settings: Settings):
        settings.always_async = set(["remote2"])
        table_spec = default_table_builder.with_async_replica("remote2").build_specification()
        db = self._build_db(table_spec)
        switcher = ReplicaSwitcher(settings, db)
        assert [("remote2", False), ("remote1", False), ("remote0", False), ("primary", True)] == [
            (c.name, is_last) for c, is_last in switcher.clusters()
        ]
        assert [
            ("remote2", False),
            ("remote1", False),
            ("remote0", False),
            ("primary", False),
            ("remote2", False),
            ("remote1", False),
            ("remote0", True),
        ] == [(c.name, is_last) for c, is_last in switcher.clusters(True)]
        assert {"remote0"} == switcher.get_sync_clusters()

    def test_iterate_clusters_with_switch(self, default_table_builder: TableSettingsBuilder, settings: Settings):
        settings.always_async = set(["remote2"])
        table_spec = default_table_builder.with_async_replica("remote2").build_specification()
        db = self._build_db(table_spec)
        switcher = ReplicaSwitcher(settings, db)
        seen_clusters = list()
        for cluster, is_last in switcher.clusters(True):
            seen_clusters.append((cluster.name, is_last))
            switcher.make_async(cluster.name)

        assert [
            ("remote2", False),
            ("remote1", False),
            ("remote0", False),
            ("primary", False),
            ("remote2", False),
            ("remote0", False),
            ("remote1", True),
        ] == seen_clusters
        assert {"remote0"} == switcher.get_sync_clusters()

    def test_iterate_clusters_with_switch_after_main(
        self, default_table_builder: TableSettingsBuilder, settings: Settings
    ):
        settings.always_async = set(["remote2"])
        table_spec = default_table_builder.with_async_replica("remote2").build_specification()
        db = self._build_db(table_spec)
        switcher = ReplicaSwitcher(settings, db)
        seen_clusters = list()
        after_main = False
        for cluster, is_last in switcher.clusters(True):
            seen_clusters.append((cluster.name, is_last))
            if cluster.is_main:
                after_main = True
            if after_main:
                switcher.make_async(cluster.name)

        assert [
            ("remote2", False),
            ("remote1", False),
            ("remote0", False),
            ("primary", False),
            ("remote2", False),
            ("remote1", False),
            ("remote0", True),
        ] == seen_clusters
        assert {"remote1"} == switcher.get_sync_clusters()

    def test_iterate_clusters_chaos(self, default_table_builder: TableSettingsBuilder, settings: Settings):
        settings.db_type = Settings.CHAOS_DB
        settings.always_async = set(["remote2"])
        table_spec = default_table_builder.with_async_replica("remote2").build_specification()
        db = self._build_db(table_spec)
        switcher = ReplicaSwitcher(settings, db)
        assert [("remote1", False), ("remote2", False), ("remote0", False), ("primary", True)] == [
            (c.name, is_last) for c, is_last in switcher.clusters()
        ]
        assert [
            ("remote1", False),
            ("remote2", False),
            ("remote0", False),
            ("primary", False),
            ("remote1", False),
            ("remote2", False),
            ("remote0", True),
        ] == [(c.name, is_last) for c, is_last in switcher.clusters(True)]
        assert {"remote0"} == switcher.get_sync_clusters()

    def test_make_async_all_async(self, default_table_builder: TableSettingsBuilder, settings: Settings):
        settings.always_async = set(["remote2"])
        table_spec = (
            default_table_builder.with_async_replica("remote0").with_async_replica("remote2").build_specification()
        )
        db = self._build_db(table_spec)
        switcher = ReplicaSwitcher(settings, db)
        assert not switcher.get_sync_clusters()
        for cluster in ("primary", "remote0", "remote1", "remote2"):
            # nothing to switch, main can't be switched, replicas already async
            assert 0 == len(switcher.make_async(cluster))
            assert not switcher.get_sync_clusters()

    def test_make_async_zero_min_sync(self, default_table_builder: TableSettingsBuilder, settings: Settings):
        settings.min_sync_clusters = 0
        table_spec = default_table_builder.build_specification()
        db = self._build_db(table_spec)
        switcher = ReplicaSwitcher(settings, db)
        assert {"remote0"} == switcher.get_sync_clusters()
        scripts = switcher.make_async("remote0")
        # expect only one action to make remote0 async
        assert 1 == len(scripts)
        for script in scripts:
            assert "primary" == script.cluster_name
            assert 1 == len(script.actions)
        assert not switcher.get_sync_clusters()

    def test_make_async_with_min_sync(self, default_table_builder: TableSettingsBuilder, settings: Settings):
        settings.min_sync_clusters = 1
        table_spec = default_table_builder.build_specification()
        db = self._build_db(table_spec)
        switcher = ReplicaSwitcher(settings, db)
        assert {"remote0"} == switcher.get_sync_clusters()

        def _check_scripts(scripts: list[ActionBatch]):
            assert 2 == len(scripts)  # async -> sync, wait_sync
            for script in scripts:
                assert "primary" == script.cluster_name
                assert 1 == len(script.actions)

        # expect two actions: make remote1 sync, then remote0 - async
        _check_scripts(switcher.make_async("remote0"))
        assert {"remote1"} == switcher.get_sync_clusters()

        # expect no actions, remote0 already async
        assert 0 == len(switcher.make_async("remote0"))
        assert {"remote1"} == switcher.get_sync_clusters()

        # expect two actions: make remote0 sync, then remote1 - async
        _check_scripts(switcher.make_async("remote1"))
        assert {"remote0"} == switcher.get_sync_clusters()

    def test_make_unavailable_async(self, default_table_builder: TableSettingsBuilder, settings: Settings):
        settings.min_sync_clusters = 1
        settings.always_async = set(["remote1"])
        table_spec = default_table_builder.build_specification()
        db = self._build_db(table_spec)
        switcher = ReplicaSwitcher(settings, db)
        with pytest.raises(AssertionError):
            switcher.make_async("remote0")

    def test_ensure_sync_mode_no_diff(self, default_table_builder: TableSettingsBuilder, settings: Settings):
        settings.min_sync_clusters = 1
        table_spec = default_table_builder.build_specification()
        db = self._build_db(table_spec)
        switcher = ReplicaSwitcher(settings, db)
        # Always emit SwitchReplicaModeAction and WaitReplicasInSyncAction
        batches = switcher.ensure_sync_mode(db)
        assert len(batches) == 2
        for batch in batches:
            assert batch.cluster_name == "primary"
            assert len(batch.actions) == 1

    @pytest.mark.parametrize("with_skip", [True, False])
    def test_ensure_sync_mode(self, default_table_builder: TableSettingsBuilder, settings: Settings, with_skip: bool):
        settings.min_sync_clusters = 1
        table_spec = default_table_builder.build_specification()
        db = self._build_db(table_spec)
        switcher = ReplicaSwitcher(settings, db, skip_wait={default_table_builder.path} if with_skip else None)
        if with_skip:
            assert 1 == len(switcher.make_async("remote0"))  # async -> sync, no wait_sync
        else:
            assert 2 == len(switcher.make_async("remote0"))  # async -> sync, wait_sync
        scripts = switcher.ensure_sync_mode(db)
        # expect actions to switch back remote0 to sync and remote1 to async
        assert 2 == len(scripts)  # async -> sync, wait_sync
        for script in scripts:
            assert "primary" == script.cluster_name
            assert 1 == len(script.actions)

    def test_is_all_switchable(self, default_table_builder: TableSettingsBuilder, settings: Settings):
        settings.min_sync_clusters = 1
        table_spec = default_table_builder.build_specification()
        actual_db = self._build_db(table_spec)
        desired_db = deepcopy(actual_db)
        desired_db.clusters["remote0"].cluster_type = YtCluster.Type.REPLICA
        desired_db.clusters["remote0"].mode = YtCluster.Mode.ASYNC
        desired_db.clusters["remote1"].cluster_type = YtCluster.Type.REPLICA
        desired_db.clusters["remote1"].mode = YtCluster.Mode.SYNC
        assert ReplicaSwitcher.is_all_switchable(settings, actual_db, desired_db)

    def test_is_all_switchable_fail_on_make_async(
        self, default_table_builder: TableSettingsBuilder, settings: Settings
    ):
        settings.min_sync_clusters = 1
        settings.always_async = set(["remote1"])
        table_spec = default_table_builder.build_specification()
        actual_db = self._build_db(table_spec)
        desired_db = deepcopy(actual_db)
        assert not ReplicaSwitcher.is_all_switchable(settings, actual_db, desired_db)
