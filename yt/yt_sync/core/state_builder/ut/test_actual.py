from typing import Any
import uuid

import pytest

import yt.wrapper as yt
from yt.yson.yson_types import YsonList
from yt.yt_sync.core.model import get_folder
from yt.yt_sync.core.model import get_node_name
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtCluster
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.model import YtNode
from yt.yt_sync.core.model import YtSchema
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.model import YtTabletState
from yt.yt_sync.core.settings import Settings
from yt.yt_sync.core.state_builder import ActualStateBuilder
from yt.yt_sync.core.state_builder import DesiredStateBuilderBase
from yt.yt_sync.core.table_filter import TableNameFilter
from yt.yt_sync.core.test_lib import MockResult
from yt.yt_sync.core.test_lib import MockYtClientFactory
from yt.yt_sync.core.test_lib import yt_access_denied
from yt.yt_sync.core.test_lib import yt_generic_error
from yt.yt_sync.core.test_lib import yt_resolve_error

from .base import StateBuilderTestBase


@pytest.mark.parametrize("use_deprecated", [True, False])
class TestActualStateBuilder(StateBuilderTestBase):
    @pytest.fixture()
    @staticmethod
    def settings(use_deprecated: bool) -> Settings:
        assert use_deprecated is not None  # TODO: pass to settings
        return Settings(db_type=Settings.REPLICATED_DB)

    @staticmethod
    def _build_folder_settings_deprecated(db: YtDatabase, table_settings: Types.Attributes) -> Types.Attributes:
        folder_settings = {"type": YtNode.Type.FOLDER, "clusters": {}}
        for cluster_settings in table_settings.values():
            if not isinstance(cluster_settings, dict) or "cluster" not in cluster_settings:
                continue
            if not isinstance(cluster_settings["cluster"], list):
                folder_path = get_folder(table_settings["path"])
                if folder_path in db.clusters[cluster_settings["cluster"]].nodes:
                    continue
                folder_settings["clusters"][cluster_settings["cluster"]] = {
                    "path": folder_path,
                    "attributes": {},
                }
                continue
            for cluster_name in cluster_settings["cluster"]:
                folder_path = get_folder(cluster_settings["replica_path"])
                if folder_path in db.clusters[cluster_name].nodes:
                    continue
                folder_settings["clusters"][cluster_name] = {"path": folder_path, "attributes": {}}
        return folder_settings

    @staticmethod
    def _build_folder_settings(db: YtDatabase, table_settings: Types.Attributes) -> Types.Attributes:
        folder_settings = {"type": YtNode.Type.FOLDER, "clusters": {}}
        for cluster_name, cluster_spec in table_settings.get("clusters", {}).items():
            main: bool = cluster_spec.get("main", False)
            path: str = get_folder(cluster_spec["path"])
            if path in db.clusters[cluster_name].nodes:
                continue
            folder_settings["clusters"][cluster_name] = {"main": main, "path": path}
        return folder_settings

    def _convert_link_attributes(self, cluster_attributes: Types.Attributes, use_deprecated: bool) -> Types.Attributes:
        if use_deprecated:
            cluster_attributes["attributes"]["target_path"] = cluster_attributes.pop("target_path")
        return cluster_attributes

    @pytest.fixture()
    @classmethod
    def filled_db(
        cls, db: YtDatabase, builder: DesiredStateBuilderBase, table_settings: Types.Attributes, use_deprecated: bool
    ) -> YtDatabase:
        builder.add_table(table_settings)
        folder_settings = (
            cls._build_folder_settings_deprecated(db, table_settings)
            if use_deprecated
            else cls._build_folder_settings(db, table_settings)
        )
        builder.add_node(folder_settings)
        return db

    @pytest.fixture()
    @classmethod
    def filled_chaos_db(
        cls,
        db: YtDatabase,
        chaos_builder: DesiredStateBuilderBase,
        chaos_table_settings: Types.Attributes,
        use_deprecated: bool,
    ) -> YtDatabase:
        chaos_builder.add_table(chaos_table_settings)
        folder_settings = (
            cls._build_folder_settings_deprecated(db, chaos_table_settings)
            if use_deprecated
            else cls._build_folder_settings(db, chaos_table_settings)
        )
        chaos_builder.add_node(folder_settings)
        return db

    @pytest.fixture
    @staticmethod
    def pivot_keys() -> list[Any]:
        return [[], [1], [1, 123], [1, 456]]

    @pytest.fixture
    @staticmethod
    def yt_table_schema(default_schema: Types.Schema) -> YsonList:
        return YtSchema.parse(default_schema).yt_schema

    @pytest.fixture
    @staticmethod
    def mock_empty_yt_config(expected_paths: dict[str, str], expected_node_configs: dict[str, Any]) -> Types.Attributes:
        return {
            "primary": {
                "get": {
                    **{f"{path}": MockResult(error=yt_resolve_error()) for path in expected_node_configs},
                    f"{expected_paths['primary']}&/@": MockResult(error=yt_resolve_error()),
                },
            },
            "remote0": {
                "get": {
                    **{f"{path}": MockResult(error=yt_resolve_error()) for path in expected_node_configs},
                    f"{expected_paths['remote0']}&/@": MockResult(error=yt_resolve_error()),
                },
            },
            "remote1": {
                "get": {
                    **{f"{path}": MockResult(error=yt_resolve_error()) for path in expected_node_configs},
                    f"{expected_paths['remote1']}&/@": MockResult(error=yt_resolve_error()),
                },
            },
        }

    @pytest.fixture
    @staticmethod
    def mock_no_tables_yt_config(
        expected_paths: dict[str, str], expected_node_configs: dict[str, Any]
    ) -> Types.Attributes:
        return {
            "primary": {
                "get": {
                    **expected_node_configs,
                    f"{expected_paths['primary']}&/@": MockResult(error=yt_resolve_error()),
                },
            },
            "remote0": {
                "get": {
                    **expected_node_configs,
                    f"{expected_paths['remote0']}&/@": MockResult(error=yt_resolve_error()),
                },
            },
            "remote1": {
                "get": {
                    **expected_node_configs,
                    f"{expected_paths['remote1']}&/@": MockResult(error=yt_resolve_error()),
                },
            },
        }

    @pytest.fixture
    @staticmethod
    def mock_yt_config(
        yt_table_schema: YsonList,
        pivot_keys: list[Any],
        expected_paths: dict[str, str],
        expected_node_configs: dict[str, Any],
    ) -> Types.Attributes:
        return {
            "primary": {
                "get": {
                    **expected_node_configs,
                    f"{expected_paths['primary']}&/@": MockResult(
                        result={
                            "type": "replicated_table",
                            "dynamic": True,
                            "schema": yt_table_schema,
                            "replicas": None,
                            "replication_collocation_id": str(uuid.uuid4()),
                        }
                    ),
                    f"{expected_paths['primary']}&/@replicas": MockResult(
                        result={
                            "0001": {
                                "cluster_name": "remote0",
                                "replica_path": expected_paths["remote0"],
                                "state": "enabled",
                                "mode": "sync",
                            },
                            "0002": {
                                "cluster_name": "remote1",
                                "replica_path": expected_paths["remote1"],
                                "state": "enabled",
                                "mode": "async",
                            },
                        }
                    ),
                    f"{expected_paths['primary']}&/@pivot_keys": MockResult(result=pivot_keys),
                    f"{expected_paths['primary']}&/@tablet_state": MockResult(result=YtTabletState.MOUNTED),
                }
            },
            "remote0": {
                "get": {
                    **expected_node_configs,
                    f"{expected_paths['remote0']}&/@": MockResult(
                        result={"type": "table", "dynamic": True, "schema": yt_table_schema}
                    ),
                    f"{expected_paths['remote0']}&/@pivot_keys": MockResult(result=pivot_keys),
                    f"{expected_paths['remote0']}&/@tablet_state": MockResult(result=YtTabletState.MOUNTED),
                },
            },
            "remote1": {
                "get": {
                    **expected_node_configs,
                    f"{expected_paths['remote1']}&/@": MockResult(
                        result={"type": "table", "dynamic": True, "schema": yt_table_schema}
                    ),
                    f"{expected_paths['remote1']}&/@pivot_keys": MockResult(result=pivot_keys),
                    f"{expected_paths['remote1']}&/@tablet_state": MockResult(result=YtTabletState.MOUNTED),
                },
            },
        }

    def test_yt_error(
        self,
        settings: Settings,
        filled_db: YtDatabase,
        expected_paths: dict[str, str],
        expected_node_configs: dict[str, Any],
    ):
        yt_client_factory = MockYtClientFactory(
            {
                "primary": {
                    "get": {
                        **expected_node_configs,
                        f"{expected_paths['primary']}&/@": MockResult(error=yt_generic_error()),
                    },
                },
                "remote0": {
                    "get": {
                        **expected_node_configs,
                        f"{expected_paths['remote0']}&/@": MockResult(error=yt_generic_error()),
                    },
                },
                "remote1": {
                    "get": {
                        **expected_node_configs,
                        f"{expected_paths['remote1']}&/@": MockResult(error=yt_generic_error()),
                    },
                },
            }
        )
        with pytest.raises(yt.YtResponseError):
            ActualStateBuilder(settings, yt_client_factory).build_from(filled_db)

    def test_yt_attr_error(
        self,
        settings: Settings,
        table_path: str,
        expected_node_configs: dict[str, Any],
        yt_table_schema: YsonList,
        filled_db: YtDatabase,
    ):
        yt_client_factory = MockYtClientFactory(
            {
                "primary": {
                    "get": {
                        **expected_node_configs,
                        f"{table_path}&/@": MockResult(
                            result={
                                "type": "replicated_table",
                                "dynamic": True,
                                "schema": yt_table_schema,
                                "replicas": None,
                            }
                        ),
                        f"{table_path}&/@replicas": MockResult(error=yt_generic_error()),
                        f"{table_path}&/@pivot_keys": MockResult(error=yt_generic_error()),
                        f"{table_path}&/@tablet_state": MockResult(error=yt_generic_error()),
                    }
                },
                "remote0": {
                    "get": {
                        **expected_node_configs,
                        f"{table_path}_sync&/@": MockResult(
                            result={"type": "table", "dynamic": True, "schema": yt_table_schema}
                        ),
                        f"{table_path}_sync&/@pivot_keys": MockResult(error=yt_generic_error()),
                        f"{table_path}_sync&/@tablet_state": MockResult(error=yt_generic_error()),
                    }
                },
                "remote1": {
                    "get": {
                        **expected_node_configs,
                        f"{table_path}_async&/@": MockResult(
                            result={"type": "table", "dynamic": True, "schema": yt_table_schema}
                        ),
                        f"{table_path}_async&/@pivot_keys": MockResult(error=yt_generic_error()),
                        f"{table_path}_async&/@tablet_state": MockResult(error=yt_generic_error()),
                    }
                },
            }
        )
        with pytest.raises(yt.YtResponseError):
            ActualStateBuilder(settings, yt_client_factory).build_from(filled_db)

    def test_yt_empty(
        self, settings: Settings, table_path: str, filled_db: YtDatabase, mock_empty_yt_config: Types.Attributes
    ):
        folder = get_folder(table_path)
        filled_db.clusters["primary"].nodes[folder].attributes.set_value("attribute", "value")
        yt_client_factory = MockYtClientFactory(mock_empty_yt_config)
        actual_db = ActualStateBuilder(settings, yt_client_factory).build_from(filled_db)
        for cluster in ("primary", "remote0", "remote1"):
            assert cluster in actual_db.clusters
            assert table_path in actual_db.clusters[cluster].tables
            table = actual_db.clusters[cluster].tables[table_path]
            assert table.exists is False
            assert not table.schema.columns
            assert not table.attributes
            assert table.table_type == filled_db.clusters[cluster].tables[table.key].table_type

            path = folder
            assert path in actual_db.clusters[cluster].nodes
            node = actual_db.clusters[cluster].nodes[path]
            assert node.exists is False
            assert node.node_type == YtNode.Type.FOLDER
            assert not node.attributes

    def test_no_tables(
        self, settings: Settings, table_path: str, filled_db: YtDatabase, mock_no_tables_yt_config: Types.Attributes
    ):
        yt_client_factory = MockYtClientFactory(mock_no_tables_yt_config)
        actual_db = ActualStateBuilder(settings, yt_client_factory).build_from(filled_db)
        for cluster in ("primary", "remote0", "remote1"):
            assert cluster in actual_db.clusters
            assert table_path in actual_db.clusters[cluster].tables
            table = actual_db.clusters[cluster].tables[table_path]
            assert table.exists is False
            assert not table.schema.columns
            assert not table.attributes
            assert table.table_type == filled_db.clusters[cluster].tables[table.key].table_type

    def test_no_chaos_tables(
        self,
        settings: Settings,
        table_path: str,
        filled_chaos_db: YtDatabase,
        expected_paths: dict[str, str],
        expected_node_configs: dict[str, Any],
    ):
        yt_client_factory = MockYtClientFactory(
            {
                "primary": {
                    "get": {
                        **expected_node_configs,
                        f"{expected_paths['primary']}&/@": MockResult(error=yt_resolve_error()),
                    },
                },
                "remote0": {
                    "get": {
                        **expected_node_configs,
                        f"{expected_paths['remote0']}&/@": MockResult(error=yt_resolve_error()),
                        f"{expected_paths['remote0']}_log&/@": MockResult(error=yt_resolve_error()),
                    }
                },
                "remote1": {
                    "get": {
                        **expected_node_configs,
                        f"{expected_paths['remote1']}&/@": MockResult(error=yt_resolve_error()),
                        f"{expected_paths['remote1']}_log&/@": MockResult(error=yt_resolve_error()),
                    }
                },
            }
        )
        actual_db = ActualStateBuilder(settings, yt_client_factory).build_from(filled_chaos_db)
        for cluster in ("primary", "remote0", "remote1"):
            assert cluster in actual_db.clusters
            assert table_path in actual_db.clusters[cluster].tables
            table = actual_db.clusters[cluster].tables[table_path]
            assert table.exists is False
            assert not table.schema.columns
            assert not table.attributes

            if "primary" == cluster:
                assert not table.replicas
                continue

            log_key = f"{table_path}_log"
            assert log_key in actual_db.clusters[cluster].tables
            log_table = actual_db.clusters[cluster].tables[log_key]
            assert log_table.exists is False
            assert not log_table.schema.columns
            assert not log_table.attributes

    def test_tables(self, settings: Settings, table_path: str, filled_db: YtDatabase, mock_yt_config: Types.Attributes):
        yt_client_factory = MockYtClientFactory(mock_yt_config)
        actual_db = ActualStateBuilder(settings, yt_client_factory).build_from(filled_db)
        for cluster in ("primary", "remote0", "remote1"):
            assert cluster in actual_db.clusters
            assert table_path in actual_db.clusters[cluster].tables
            table = actual_db.clusters[cluster].tables[table_path]
            assert table.exists
            assert table.schema.columns
            assert table.attributes
            if "primary" == cluster:
                assert table.replicas
                assert 2 == len(table.replicas)
                assert table.in_collocation

    def test_table_state_load(
        self,
        settings: Settings,
        table_path: str,
        filled_db: YtDatabase,
        mock_yt_config: Types.Attributes,
        yt_table_schema: YsonList,
    ):
        mock_yt_config["primary"]["get"][f"{table_path}&/@"] = MockResult(
            result={
                "path": table_path,
                "type": YtTable.Type.TABLE,
                "name": get_node_name(table_path),
                "schema": yt_table_schema,
            }
        )
        yt_client_factory = MockYtClientFactory(mock_yt_config)
        actual_db = ActualStateBuilder(settings, yt_client_factory).build_from(filled_db)

        assert "primary" in actual_db.clusters
        table = actual_db.clusters["primary"].tables[table_path]
        assert table.exists
        assert table.table_type == YtTable.Type.TABLE

    def test_node_state_load(self, settings: Settings, folder_path: str):
        desired = YtDatabase()
        desired.add_or_get_cluster(YtCluster.make(YtCluster.Type.MAIN, "primary", False)).add_node(
            YtNode.make(
                cluster_name="primary",
                path=folder_path,
                node_type=YtNode.Type.FOLDER,
                exists=True,
                attributes={},
            )
        )

        responses = {
            "primary": {
                "get": {
                    f"{folder_path}&/@": MockResult(
                        result={"path": folder_path, "type": "special", "name": get_node_name(folder_path)}
                    )
                }
            }
        }
        path = get_folder(folder_path)
        while path != "/":
            responses["primary"]["get"][f"{path}&/@"] = MockResult(
                result={"path": path, "type": YtNode.Type.FOLDER, "name": get_node_name(path)}
            )
            path = get_folder(path)
        yt_client_factory = MockYtClientFactory(responses)

        actual_db = ActualStateBuilder(settings, yt_client_factory).build_from(desired)

        assert "primary" in actual_db.clusters
        assert folder_path in actual_db.clusters["primary"].nodes
        node = actual_db.clusters["primary"].nodes[folder_path]
        assert node.exists
        assert node.node_type == "special"

    def test_partially_denied(
        self, settings: Settings, folder_path: str, db: YtDatabase, builder: DesiredStateBuilderBase
    ):
        builder.add_node(
            {
                "type": YtNode.Type.FILE,
                "clusters": {"primary": {"path": f"{folder_path}/folder1/folder/file", "attributes": {}}},
            }
        )
        builder.add_node(
            {
                "type": YtNode.Type.FOLDER,
                "clusters": {"primary": {"path": f"{folder_path}/folder2/folder", "attributes": {}}},
            }
        )
        builder.finalize(settings)

        mock_yt_config = {
            "primary": {
                "get": {
                    f"{folder_path}&/@": MockResult(error=yt_access_denied()),
                    f"{folder_path}/folder1&/@": MockResult(result={"type": YtNode.Type.FOLDER}),
                    f"{folder_path}/folder1/folder&/@": MockResult(error=yt_resolve_error()),
                    f"{folder_path}/folder1/folder/file&/@": MockResult(error=yt_resolve_error()),
                    f"{folder_path}/folder2&/@": MockResult(error=yt_access_denied()),
                    f"{folder_path}/folder2/folder&/@": MockResult(error=yt_resolve_error()),
                }
            }
        }
        actual_db = ActualStateBuilder(settings, MockYtClientFactory(mock_yt_config)).build_from(db)
        primary = actual_db.clusters["primary"]

        def _assert_state(path: str, exists: bool, is_implicit: bool, node_type: YtNode.Type):
            assert primary.nodes[path].exists == exists
            assert primary.nodes[path].is_implicit == is_implicit
            assert primary.nodes[path].node_type == node_type

        _assert_state(folder_path, True, True, YtNode.Type.FOLDER)
        _assert_state(f"{folder_path}/folder1", True, False, YtNode.Type.FOLDER)
        _assert_state(f"{folder_path}/folder1/folder", False, False, YtNode.Type.FOLDER)
        _assert_state(f"{folder_path}/folder1/folder/file", False, False, YtNode.Type.FILE)
        _assert_state(f"{folder_path}/folder2", True, True, YtNode.Type.FOLDER)
        _assert_state(f"{folder_path}/folder2/folder", False, False, YtNode.Type.FOLDER)

    def test_file_on_propagation_path(
        self, settings: Settings, table_path: str, filled_db: YtDatabase, mock_yt_config: Types.Attributes
    ):
        folder = get_folder(table_path)
        mock_yt_config["primary"]["get"][f"{folder}&/@"] = MockResult(
            result={"path": folder, "type": YtNode.Type.FILE, "name": get_node_name(folder)}
        )
        yt_client_factory = MockYtClientFactory(mock_yt_config)
        with pytest.raises(AssertionError):
            ActualStateBuilder(settings, yt_client_factory).build_from(filled_db)

    def test_chaos_tables(
        self,
        settings: Settings,
        table_path: str,
        yt_table_schema: YsonList,
        pivot_keys: list[Any],
        filled_chaos_db: YtDatabase,
        expected_paths: dict[str, str],
        expected_node_configs: dict[str, Any],
    ):
        yt_client_factory = MockYtClientFactory(
            {
                "primary": {
                    "get": {
                        **expected_node_configs,
                        f"{expected_paths['primary']}&/@": MockResult(
                            result={
                                "type": "chaos_replicated_table",
                                "dynamic": True,
                                "schema": yt_table_schema,
                                "replicas": None,
                            }
                        ),
                        f"{expected_paths['primary']}&/@replicas": MockResult(
                            result={
                                "0001": {
                                    "cluster_name": "remote0",
                                    "replica_path": expected_paths["remote0"],
                                    "state": "enabled",
                                    "mode": "sync",
                                    "content_type": "data",
                                },
                                "0002": {
                                    "cluster_name": "remote0",
                                    "replica_path": f"{expected_paths['remote0']}_log",
                                    "state": "enabled",
                                    "mode": "sync",
                                    "content_type": "queue",
                                },
                                "0003": {
                                    "cluster_name": "remote1",
                                    "replica_path": expected_paths["remote1"],
                                    "state": "enabled",
                                    "mode": "async",
                                    "content_type": "data",
                                },
                                "0004": {
                                    "cluster_name": "remote1",
                                    "replica_path": f"{expected_paths['remote1']}_log",
                                    "state": "enabled",
                                    "mode": "async",
                                    "content_type": "queue",
                                },
                            }
                        ),
                        f"{expected_paths['primary']}&/@pivot_keys": MockResult(result=pivot_keys),
                        f"{expected_paths['primary']}&/@tablet_state": MockResult(result=YtTabletState.MOUNTED),
                    }
                },
                "remote0": {
                    "get": {
                        **expected_node_configs,
                        f"{expected_paths['remote0']}&/@": MockResult(
                            result={"type": "table", "dynamic": True, "schema": yt_table_schema}
                        ),
                        f"{expected_paths['remote0']}&/@pivot_keys": MockResult(result=pivot_keys),
                        f"{expected_paths['remote0']}&/@tablet_state": MockResult(result=YtTabletState.MOUNTED),
                        f"{expected_paths['remote0']}_log&/@": MockResult(
                            result={"type": "replication_log_table", "dynamic": True, "schema": yt_table_schema}
                        ),
                        f"{expected_paths['remote0']}_log&/@pivot_keys": MockResult(result=pivot_keys),
                        f"{expected_paths['remote0']}_log&/@tablet_state": MockResult(result=YtTabletState.MOUNTED),
                    }
                },
                "remote1": {
                    "get": {
                        **expected_node_configs,
                        f"{expected_paths['remote1']}&/@": MockResult(
                            result={"type": "table", "dynamic": True, "schema": yt_table_schema}
                        ),
                        f"{expected_paths['remote1']}&/@pivot_keys": MockResult(error=yt_resolve_error()),
                        f"{expected_paths['remote1']}&/@tablet_state": MockResult(error=yt_resolve_error()),
                        f"{expected_paths['remote1']}_log&/@": MockResult(
                            result={"type": "replication_log_table", "dynamic": True, "schema": yt_table_schema}
                        ),
                        f"{expected_paths['remote1']}_log&/@pivot_keys": MockResult(error=yt_resolve_error()),
                        f"{expected_paths['remote1']}_log&/@tablet_state": MockResult(error=yt_resolve_error()),
                    }
                },
            }
        )
        actual_db = ActualStateBuilder(settings, yt_client_factory).build_from(filled_chaos_db)
        log_key = f"{table_path}_log"
        for cluster in ("primary", "remote0", "remote1"):
            assert cluster in actual_db.clusters
            assert table_path in actual_db.clusters[cluster].tables
            table = actual_db.clusters[cluster].tables[table_path]
            assert table.exists is True
            assert table.schema.columns
            assert table.attributes

            if cluster == "primary":
                assert table.replicas
                assert 4 == len(table.replicas)
                continue

            if cluster == "remote0":
                assert table.pivot_keys
            else:
                assert not table.pivot_keys

            assert table.chaos_replication_log

            assert log_key in actual_db.clusters[cluster].tables
            log_table = actual_db.clusters[cluster].tables[log_key]
            assert log_table.exists is True
            assert log_table.schema.columns
            assert log_table.attributes
            assert log_table.chaos_data_table

    def test_table_filter_pass(
        self, settings: Settings, table_path: str, filled_db: YtDatabase, mock_yt_config: Types.Attributes
    ):
        table_name = get_node_name(table_path)
        yt_client_factory = MockYtClientFactory(mock_yt_config)
        actual_db = ActualStateBuilder(settings, yt_client_factory, TableNameFilter([table_name])).build_from(filled_db)
        for cluster in ("primary", "remote0", "remote1"):
            assert cluster in actual_db.clusters
            assert table_path in actual_db.clusters[cluster].tables

    def test_table_filter_nopass(
        self, settings: Settings, table_path: str, filled_db: YtDatabase, mock_yt_config: Types.Attributes
    ):
        yt_client_factory = MockYtClientFactory(mock_yt_config)
        actual_db = ActualStateBuilder(settings, yt_client_factory, TableNameFilter(["_some_table_"])).build_from(
            filled_db
        )
        for cluster in ("primary", "remote0", "remote1"):
            assert cluster in actual_db.clusters
            assert table_path not in actual_db.clusters[cluster].tables

    @pytest.mark.parametrize("exists", [True, False])
    def test_any(
        self, settings: Settings, folder_path: str, db: YtDatabase, builder: DesiredStateBuilderBase, exists: bool
    ):
        builder.add_node(
            {
                "type": YtNode.Type.ANY,
                "clusters": {"primary": {"path": folder_path, "attributes": {}}},
            }
        )
        builder.finalize(settings)

        actual_db = ActualStateBuilder(
            settings,
            MockYtClientFactory(
                {
                    "primary": {
                        "get": {
                            f"{folder_path}&/@": (
                                MockResult(result={"type": YtNode.Type.FILE})
                                if exists
                                else MockResult(error=yt_resolve_error())
                            )
                        }
                    }
                }
            ),
        ).build_from(db)

        node = actual_db.clusters["primary"].nodes[folder_path]
        if exists:
            assert node.exists
            assert node.node_type == YtNode.Type.FILE
        else:
            assert not node.exists
            assert node.node_type == YtNode.Type.ANY

    def test_any_on_path(self, settings: Settings, db: YtDatabase, builder: DesiredStateBuilderBase):
        folder_path = "//db_folder"
        for path, node_type in (
            (f"{folder_path}/folder", YtNode.Type.ANY),
            (f"{folder_path}/folder/node", YtNode.Type.ANY),
            (f"{folder_path}/folder/file", YtNode.Type.FILE),
        ):
            builder.add_node(
                {
                    "type": node_type,
                    "clusters": {
                        "primary": {
                            "main": True,
                            "path": path,
                            "attributes": {},
                        }
                    },
                }
            )
        builder.finalize(settings)

        yt_client_factory = MockYtClientFactory(
            {
                "primary": {
                    "get": {
                        f"{folder_path}/folder&/@": MockResult(result={"type": YtNode.Type.FILE}),
                        f"{folder_path}/folder/node&/@": MockResult(error=yt_resolve_error()),
                        f"{folder_path}/folder/file&/@": MockResult(error=yt_resolve_error()),
                    }
                }
            }
        )
        with pytest.raises(AssertionError):
            ActualStateBuilder(settings, yt_client_factory).build_from(db)

    def test_links(
        self,
        settings: Settings,
        filled_db: YtDatabase,
        builder: DesiredStateBuilderBase,
        table_path: str,
        mock_yt_config: Types.Attributes,
        use_deprecated: bool,
    ):
        table_folder_path = get_folder(table_path)
        link_path = f"{table_folder_path}/link"
        builder.add_node(
            {
                "type": YtNode.Type.LINK,
                "clusters": {
                    "primary": self._convert_link_attributes(
                        {
                            "main": True,
                            "path": link_path,
                            "attributes": {},
                            "target_path": table_folder_path,
                        },
                        use_deprecated,
                    ),
                    "remote0": self._convert_link_attributes(
                        {
                            "path": link_path,
                            "attributes": {"attribute": "value"},
                            "target_path": table_path + "_sync",
                        },
                        use_deprecated,
                    ),
                },
            }
        )
        builder.finalize()

        mock_yt_config["primary"]["get"][f"{link_path}&/@"] = MockResult(error=yt_resolve_error())
        mock_yt_config["remote0"]["get"][f"{link_path}&/@"] = MockResult(
            result={"type": "link", "name": "link", "target_path": table_path + "_sync", "path": link_path}
        )
        mock_yt_config["remote0"]["get"][f"{link_path}&/@type"] = MockResult(result=YtTable.Type.TABLE)

        yt_client_factory = MockYtClientFactory(mock_yt_config)
        actual_db = ActualStateBuilder(settings, yt_client_factory).build_from(filled_db)

        assert "primary" in actual_db.clusters
        assert link_path in actual_db.clusters["primary"].nodes
        node = actual_db.clusters["primary"].nodes[link_path]
        assert not node.exists
        assert node.node_type == YtNode.Type.LINK

        assert "remote0" in actual_db.clusters
        assert link_path in actual_db.clusters["remote0"].nodes
        node = actual_db.clusters["remote0"].nodes[link_path]
        assert node.exists
        assert node.node_type == YtNode.Type.LINK

    def test_link_no_target(
        self, settings: Settings, db: YtDatabase, builder: DesiredStateBuilderBase, use_deprecated: bool
    ):
        link_path = "//folder/link"
        builder.add_node(
            {
                "type": YtNode.Type.LINK,
                "clusters": {
                    "primary": self._convert_link_attributes(
                        {"path": link_path, "attributes": {}, "target_path": "//no/path"}, use_deprecated
                    )
                },
            }
        )
        builder.finalize()

        mock_yt_config = {
            "primary": {
                "get": {
                    f"{get_folder(link_path)}&/@": MockResult(error=yt_resolve_error()),
                    f"{link_path}&/@": MockResult(error=yt_resolve_error()),
                    "//no/path&/@type": MockResult(error=yt_resolve_error()),
                }
            }
        }
        yt_client_factory = MockYtClientFactory(mock_yt_config)
        with pytest.raises(AssertionError):
            ActualStateBuilder(settings, yt_client_factory).build_from(db)

    def test_link_on_link(
        self, settings: Settings, db: YtDatabase, builder: DesiredStateBuilderBase, use_deprecated: bool
    ):
        folder_path = "//folder"
        link_path = folder_path + "/link"
        builder.add_node(
            {
                "type": YtNode.Type.LINK,
                "clusters": {
                    "primary": self._convert_link_attributes(
                        {"path": link_path, "attributes": {}, "target_path": f"{link_path}_old"}, use_deprecated
                    ),
                },
            }
        )
        builder.finalize()

        mock_yt_config = {
            "primary": {
                "get": {
                    f"{folder_path}&/@": MockResult(result={"type": YtNode.Type.FOLDER}),
                    f"{link_path}_old&/@type": MockResult(result=YtNode.Type.LINK),
                    f"{link_path}&/@": MockResult(error=yt_resolve_error()),
                }
            }
        }
        yt_client_factory = MockYtClientFactory(mock_yt_config)
        with pytest.raises(AssertionError):
            ActualStateBuilder(settings, yt_client_factory).build_from(db)
