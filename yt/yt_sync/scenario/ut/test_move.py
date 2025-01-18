from typing import Any

import pytest

from library.python.confmerge import merge_inplace
from yt.yt_sync.action import MountTableAction
from yt.yt_sync.core.client import MockResult
from yt.yt_sync.core.client import YtClientFactory
from yt.yt_sync.core.model import get_folder
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtCluster
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.model import YtNode
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTabletState
from yt.yt_sync.core.settings import Settings
from yt.yt_sync.core.spec import Table
from yt.yt_sync.core.state_builder import ActualStateBuilder
from yt.yt_sync.core.state_builder import DesiredStateBuilder
from yt.yt_sync.core.test_lib import MockYtClientFactory
from yt.yt_sync.core.test_lib import TableSettingsBuilder
from yt.yt_sync.core.test_lib import yt_resolve_error
from yt.yt_sync.scenario import MoveScenario
from yt.yt_sync.scenario.move import patch_path


def make_table_spec(table_path: str, schema: Types.Schema, attrs: Types.Attributes = {"dynamic": True}) -> Table:
    return (
        TableSettingsBuilder(table_path)
        .with_main("primary")
        .with_sync_replica("remote0", f"{table_path}_sync")
        .with_async_replica("remote1", f"{table_path}_async")
        .with_schema(schema)
        .with_attributes(attrs)
        .build_specification()
    )


def folder_to_table_path(folder_path: str) -> str:
    return f"{folder_path}/table"


def sync_table_path(table_path: str) -> str:
    return f"{table_path}_sync"


def async_table_path(table_path: str) -> str:
    return f"{table_path}_async"


def generate_yt_config(
    source_path: str,
    destination_path: str,
    default_schema: Types.Schema,
    pivot_keys: list[Any],
    exists: dict[str, tuple[bool, bool, bool]],
) -> dict[str, dict[str, dict[str, MockResult]]]:
    src_table_path = folder_to_table_path(source_path)
    dst_table_path = folder_to_table_path(destination_path)

    def _make_config(table_path: str, exists: tuple[bool, bool, bool]):
        return {
            "primary": {
                "get": {
                    f"{get_folder(table_path)}&/@": MockResult(result={"type": YtNode.Type.FOLDER}),
                    **(
                        {
                            f"{table_path}&/@": MockResult(
                                result={
                                    "type": "replicated_table",
                                    "dynamic": True,
                                    "schema": default_schema,
                                    "replicas": None,
                                }
                            ),
                            f"{table_path}&/@replicas": MockResult(
                                result={
                                    "0001": {
                                        "cluster_name": "remote0",
                                        "replica_path": sync_table_path(table_path),
                                        "state": "enabled",
                                        "mode": "sync",
                                    },
                                    "0002": {
                                        "cluster_name": "remote1",
                                        "replica_path": async_table_path(table_path),
                                        "state": "enabled",
                                        "mode": "async",
                                    },
                                }
                            ),
                            f"{table_path}&/@pivot_keys": MockResult(result=pivot_keys),
                            f"{table_path}&/@tablet_state": MockResult(result=YtTabletState.MOUNTED),
                        }
                        if exists[0]
                        else {f"{table_path}&/@": MockResult(error=yt_resolve_error())}
                    ),
                },
            },
            "remote0": {
                "get": {
                    f"{get_folder(table_path)}&/@": MockResult(result={"type": YtNode.Type.FOLDER}),
                    **(
                        {
                            f"{sync_table_path(table_path)}&/@": MockResult(
                                result={"type": "table", "dynamic": True, "schema": default_schema}
                            ),
                            f"{sync_table_path(table_path)}&/@pivot_keys": MockResult(result=pivot_keys),
                            f"{sync_table_path(table_path)}&/@tablet_state": MockResult(result=YtTabletState.MOUNTED),
                        }
                        if exists[1]
                        else {f"{sync_table_path(table_path)}&/@": MockResult(error=yt_resolve_error())}
                    ),
                },
            },
            "remote1": {
                "get": {
                    f"{get_folder(table_path)}&/@": MockResult(result={"type": YtNode.Type.FOLDER}),
                    **(
                        {
                            f"{async_table_path(table_path)}&/@": MockResult(
                                result={"type": "table", "dynamic": True, "schema": default_schema}
                            ),
                            f"{async_table_path(table_path)}&/@pivot_keys": MockResult(result=pivot_keys),
                            f"{async_table_path(table_path)}&/@tablet_state": MockResult(result=YtTabletState.MOUNTED),
                        }
                        if exists[2]
                        else {f"{async_table_path(table_path)}&/@": MockResult(error=yt_resolve_error())}
                    ),
                },
            },
        }

    return merge_inplace(_make_config(src_table_path, exists["from"]), _make_config(dst_table_path, exists["to"]))


class TestMove:
    @pytest.fixture()
    @staticmethod
    def settings() -> Settings:
        return Settings(
            db_type=Settings.REPLICATED_DB,
            ensure_folders=True,
            ensure_collocation=True,
            collocation_name="test_collocation",
            use_deprecated_spec_format=False,
        )

    @pytest.fixture
    @staticmethod
    def yt_client_factory() -> MockYtClientFactory:
        return MockYtClientFactory({})

    @pytest.fixture()
    @staticmethod
    def desired_db(
        yt_client_factory: YtClientFactory, settings: Settings, source_path: str, default_schema: Types.Schema
    ) -> YtDatabase:
        db = YtDatabase(is_chaos=settings.is_chaos)
        builder = DesiredStateBuilder(yt_client_factory, db, settings.is_chaos, True)
        builder.add_table(make_table_spec(folder_to_table_path(source_path), default_schema))
        builder.finalize()
        return db

    @pytest.fixture()
    @staticmethod
    def empty_db(
        settings: Settings,
    ) -> YtDatabase:
        db = YtDatabase(is_chaos=settings.is_chaos)
        db.add_or_get_cluster(YtCluster.make(YtCluster.Type.MAIN, "primary", settings.is_chaos))
        db.add_or_get_cluster(YtCluster.make(YtCluster.Type.REPLICA, "remote0", settings.is_chaos, YtCluster.Mode.SYNC))
        db.add_or_get_cluster(
            YtCluster.make(YtCluster.Type.REPLICA, "remote1", settings.is_chaos, YtCluster.Mode.ASYNC)
        )
        db.ensure_db_integrity()
        return db

    @pytest.fixture()
    @staticmethod
    def source_path() -> str:
        return "//source"

    @pytest.fixture()
    @staticmethod
    def destination_path() -> str:
        return "//destination"

    @pytest.fixture
    @staticmethod
    def pivot_keys() -> list[Any]:
        return [[], [1], [1, 123], [1, 456]]

    def test_replicated(
        self,
        desired_db: YtDatabase,
        settings: Settings,
        source_path: str,
        destination_path: str,
        default_schema: Types.Schema,
        pivot_keys: list[Any],
    ):
        yt_client_factory = MockYtClientFactory(
            generate_yt_config(
                source_path,
                destination_path,
                default_schema,
                pivot_keys,
                {
                    "from": (True, True, True),
                    "to": (False, False, False),
                },
            ),
        )
        actual_db = ActualStateBuilder(
            settings,
            yt_client_factory,
        ).build_from(desired_db)

        scenario: MoveScenario = MoveScenario(desired_db, actual_db, settings, yt_client_factory)
        scenario.setup(destination_config={"default": {"from": source_path, "to": destination_path}})
        scenario._setup_destination_database()

        src_table_path = folder_to_table_path(source_path)
        dst_table_path = folder_to_table_path(destination_path)
        for cluster_name in ("primary", "remote0", "remote1"):
            assert src_table_path in scenario.destination_desired.clusters[cluster_name].tables
        replicas = {
            ("remote0", sync_table_path(dst_table_path)): YtReplica.Mode.SYNC,
            ("remote1", async_table_path(dst_table_path)): YtReplica.Mode.ASYNC,
        }
        assert (
            scenario.destination_desired.clusters["primary"].tables[src_table_path].replicas.keys() == replicas.keys()
        )
        for replica_key in replicas.keys():
            assert (
                scenario.destination_desired.clusters["primary"].tables[src_table_path].replicas[replica_key].mode
                == replicas[replica_key]
            )

        for cluster_name in ("primary", "remote0", "remote1"):
            assert src_table_path in scenario.destination_actual.clusters[cluster_name].tables
            assert not scenario.destination_actual.clusters[cluster_name].tables[src_table_path].exists

    def test_already_in_place(
        self,
        desired_db: YtDatabase,
        settings: Settings,
        source_path: str,
        destination_path: str,
        default_schema: Types.Schema,
        pivot_keys: list[Any],
    ):
        yt_client_factory = MockYtClientFactory(
            generate_yt_config(
                source_path,
                destination_path,
                default_schema,
                pivot_keys,
                {
                    "from": (False, False, False),
                    "to": (True, True, True),
                },
            ),
        )
        actual_db = ActualStateBuilder(
            settings,
            yt_client_factory,
        ).build_from(desired_db)

        scenario: MoveScenario = MoveScenario(
            desired_db,
            actual_db,
            settings,
            yt_client_factory,
        )
        scenario.setup(destination_config={"default": {"from": source_path, "to": destination_path}})
        scenario._setup_destination_database()

        for batch in scenario.generate_actions():
            for action in batch.actions:
                assert isinstance(action, MountTableAction), "Only mount actions are expected"

    def _test_bad_config(
        self,
        desired_db: YtDatabase,
        source_path: str,
        destination_path: str,
        settings: Settings,
        yt_config: dict[str, dict[str, dict[str, MockResult]]],
    ):
        yt_client_factory = MockYtClientFactory(yt_config)
        actual_db = ActualStateBuilder(settings, yt_client_factory).build_from(desired_db)

        scenario: MoveScenario = MoveScenario(desired_db, actual_db, settings, yt_client_factory)
        scenario.setup(destination_config={"default": {"from": source_path, "to": destination_path}})
        with pytest.raises(AssertionError):
            scenario._setup_destination_database()

    def test_bad_config_no_tables(
        self,
        desired_db: YtDatabase,
        source_path: str,
        destination_path: str,
        settings: Settings,
        default_schema: Types.Schema,
        pivot_keys: list[Any],
    ):
        self._test_bad_config(
            desired_db,
            source_path,
            destination_path,
            settings,
            generate_yt_config(
                source_path,
                destination_path,
                default_schema,
                pivot_keys,
                {
                    "from": (False, False, False),
                    "to": (False, False, False),
                },
            ),
        )

    @pytest.mark.parametrize(
        "exist_config",
        [
            {
                "from": (True, True, True),
                "to": (True, True, True),
            },
            {
                "from": (True, True, True),
                "to": (False, True, False),
            },
            {
                "from": (True, True, True),
                "to": (False, False, True),
            },
        ],
    )
    def test_bad_config_both_tables(
        self,
        desired_db: YtDatabase,
        source_path: str,
        destination_path: str,
        settings: Settings,
        default_schema: Types.Schema,
        pivot_keys: list[Any],
        exist_config: dict[str, tuple[bool, bool, bool]],
    ):
        self._test_bad_config(
            desired_db,
            source_path,
            destination_path,
            settings,
            generate_yt_config(source_path, destination_path, default_schema, pivot_keys, exist_config),
        )

    @pytest.mark.parametrize(
        "dst_config,path_configs",
        [
            (
                {"default": {"from": "//src", "to": "//dst"}},
                [
                    ("//src", "//dst"),
                    ("//src/table", "//dst/table"),
                    ("//src/table/@attr", "//dst/table/@attr"),
                    ("//dst/path", "//dst/path"),
                ],
            ),
            (
                {"default": {"from": "//src", "to": "//dst"}, "remote0": {"from": "//src", "to": "//other_dst"}},
                {
                    **{
                        cluster_name: [
                            ("//src", "//dst"),
                            ("//src/table", "//dst/table"),
                        ]
                        for cluster_name in ("primary", "remote1")
                    },
                    "remote0": [
                        ("//src", "//other_dst"),
                        ("//src/table", "//other_dst/table"),
                        ("//src/table/@attr", "//other_dst/table/@attr"),
                        ("//other_dst/path", "//other_dst/path"),
                    ],
                },
            ),
            (
                {"default": None, "remote0": {"from": "//src", "to": "//dst"}},
                {
                    **{
                        cluster_name: [
                            ("//src", "//src"),
                            ("//src/table", "//src/table"),
                        ]
                        for cluster_name in ("primary", "remote1")
                    },
                    "remote0": [
                        ("//src", "//dst"),
                        ("//src/table", "//dst/table"),
                        ("//src/table/@attr", "//dst/table/@attr"),
                        ("//dst/path", "//dst/path"),
                    ],
                },
            ),
        ],
    )
    def test_patch_path(
        self,
        dst_config: dict[str, dict[str, str]],
        path_configs: dict[str, list[tuple[str, str]]] | list[tuple[str, str]],
    ):
        if isinstance(path_configs, list):
            path_conversion: dict[str, list[tuple[str, str]]] = dict()
            for cluster_name in ("primary", "remote0", "remote1"):
                path_conversion[cluster_name] = list()
                for config in path_configs:
                    path_conversion[cluster_name].append(config)
            path_configs = path_conversion

        for cluster_name, config_list in path_configs.items():
            for src_path, dst_path in config_list:
                assert patch_path(cluster_name, src_path, dst_config) == dst_path

    @pytest.mark.parametrize(
        "dst_config",
        [
            # Type checks
            list(),
            tuple(),
            str(),
            int(),
            # True configs
            {"remote0": None},  # None can be used only for default
            {"default": None},  # If None is set for default, there should be one more cluster
            {"default": {"from": "//src", "to": "//src"}},  # Path repeated
        ],
    )
    def test_patch_path_config_fail(
        self,
        settings: Settings,
        empty_db: YtDatabase,
        dst_config: dict[str, dict[str, str]],
    ):
        move = MoveScenario(empty_db, empty_db, settings, MockYtClientFactory(dict()))
        with pytest.raises(AssertionError):
            move.setup(destination_config=dst_config)

    @pytest.mark.parametrize("force", [True, False])
    def test_patch_path_fail(self, force: bool):
        if force:
            with pytest.raises(AssertionError):
                patch_path("primary", "//some/path", {"default": {"from": "//src", "to": "//dst"}}, force=force)
        else:
            assert (
                patch_path("primary", "//some/path", {"default": {"from": "//src", "to": "//dst"}}, force=force)
                == "//some/path"
            )

    def test_destination_desired(
        self,
        default_schema: Types.Schema,
        settings: Settings,
        empty_db: YtDatabase,
        yt_client_factory: MockYtClientFactory,
    ):
        builder = DesiredStateBuilder(yt_client_factory, empty_db, settings.is_chaos, True)
        builder.add_table(make_table_spec("//src/table", default_schema))
        builder.add_node(
            {
                "type": YtNode.Type.FILE,
                "clusters": {
                    "primary": {"main": True, "path": "//src/file", "attributes": {}},
                    "remote0": {"main": False, "path": "//src/other_file", "attributes": {}},
                },
            }
        )
        builder.add_node(
            {
                "type": YtNode.Type.DOCUMENT,
                "clusters": {
                    "primary": {"main": True, "path": "//src/doc", "attributes": {}},
                    "remote1": {"main": False, "path": "//src/other_doc", "attributes": {}},
                },
            }
        )
        destination_config = {
            "default": {"from": "//src", "to": "//dst"},
            "remote1": {"from": "//src", "to": "//other_dst"},
        }
        destination_desired: YtDatabase = MoveScenario.build_destination_desired(empty_db, settings, destination_config)

        table_key = "//src/table"
        table = destination_desired.clusters["primary"].tables.get(table_key, None)
        assert table is not None
        assert table.path == "//dst/table"
        assert table.is_replicated
        assert set(table.replicas.keys()) == {
            ("remote0", sync_table_path("//dst/table")),
            ("remote1", async_table_path("//other_dst/table")),
        }
        assert table.replicas[("remote0", sync_table_path("//dst/table"))].replica_path == sync_table_path(
            "//dst/table"
        )
        assert table.replicas[("remote1", async_table_path("//other_dst/table"))].replica_path == async_table_path(
            "//other_dst/table"
        )

        table = destination_desired.clusters["remote0"].tables.get(table_key, None)
        assert table is not None
        assert table.path == sync_table_path("//dst/table")
        assert not table.is_replicated

        table = destination_desired.clusters["remote1"].tables.get(table_key, None)
        assert table is not None
        assert table.path == async_table_path("//other_dst/table")
        assert not table.is_replicated

        node = destination_desired.clusters["primary"].nodes.get("//dst/file", None)
        assert node is not None
        assert node.node_type == YtNode.Type.FILE
        assert node.path == "//dst/file"
        node = destination_desired.clusters["primary"].nodes.get("//dst/doc", None)
        assert node is not None
        assert node.node_type == YtNode.Type.DOCUMENT
        assert node.path == "//dst/doc"

        node = destination_desired.clusters["remote0"].nodes.get("//dst/other_file", None)
        assert node is not None
        assert node.node_type == YtNode.Type.FILE
        assert node.path == "//dst/other_file"

        assert "//dst/doc" not in destination_desired.clusters["remote0"].nodes
        assert "//dst/other_doc" not in destination_desired.clusters["remote0"].nodes

        node = destination_desired.clusters["remote1"].nodes.get("//other_dst/other_doc", None)
        assert node is not None
        assert node.node_type == YtNode.Type.DOCUMENT
        assert node.path == "//other_dst/other_doc"

        assert "//other_dst/file" not in destination_desired.clusters["remote1"].nodes
        assert "//other_dst/other_file" not in destination_desired.clusters["remote1"].nodes
