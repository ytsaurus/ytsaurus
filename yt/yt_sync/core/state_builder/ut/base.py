from typing import Any

import pytest

from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.model import YtNode
from yt.yt_sync.core.state_builder import DesiredStateBuilder
from yt.yt_sync.core.state_builder import DesiredStateBuilderBase
from yt.yt_sync.core.state_builder import DesiredStateBuilderDeprecated
from yt.yt_sync.core.test_lib import MockResult
from yt.yt_sync.core.test_lib import MockYtClientFactory


def make_table_settings_deprecated(table_path: str, schema: Types.Schema) -> Types.Attributes:
    return {
        "path": table_path,
        "master": {
            "cluster": "primary",
            "attributes": {
                "schema": schema,
            },
        },
        "sync_replicas": {
            "replica_path": f"{table_path}_sync",
            "cluster": ["remote0"],
            "attributes": {
                "remote0": {
                    "dynamic": True,
                    "schema": schema,
                }
            },
        },
        "async_replicas": {
            "replica_path": f"{table_path}_async",
            "cluster": ["remote1"],
            "attributes": {
                "remote1": {
                    "dynamic": True,
                    "schema": schema,
                }
            },
        },
    }


def make_table_settings(table_path: str, schema: Types.Schema, is_chaos: bool) -> Types.Attributes:
    return {
        "chaos": is_chaos,
        "schema": schema,
        "clusters": {
            "primary": {
                "main": True,
                "path": table_path,
                "attributes": {"dynamic": True},
            },
            "remote0": {
                "path": f"{table_path}_sync",
                "preferred_sync": True,
                "attributes": {"dynamic": True},
            },
            "remote1": {
                "path": f"{table_path}_async",
                "attributes": {"dynamic": True},
            },
        },
    }


class StateBuilderTestBase:
    @pytest.fixture()
    @staticmethod
    def db():
        return YtDatabase()

    @pytest.fixture
    @staticmethod
    def yt_client_factory() -> MockYtClientFactory:
        return MockYtClientFactory({})

    @pytest.fixture()
    @staticmethod
    def builder(
        yt_client_factory: MockYtClientFactory, db: YtDatabase, use_deprecated: bool
    ) -> DesiredStateBuilderBase:
        return (
            DesiredStateBuilderDeprecated(
                yt_client_factory, db, is_chaos=False, fix_implicit_replicated_queue_attrs=True
            )
            if use_deprecated
            else DesiredStateBuilder(yt_client_factory, db, is_chaos=False, fix_implicit_replicated_queue_attrs=True)
        )

    @pytest.fixture()
    @staticmethod
    def chaos_builder(
        yt_client_factory: MockYtClientFactory, db: YtDatabase, use_deprecated: bool
    ) -> DesiredStateBuilderBase:
        return (
            DesiredStateBuilderDeprecated(
                yt_client_factory, db, is_chaos=True, fix_implicit_replicated_queue_attrs=True
            )
            if use_deprecated
            else DesiredStateBuilder(yt_client_factory, db, is_chaos=True, fix_implicit_replicated_queue_attrs=True)
        )

    @pytest.fixture()
    @staticmethod
    def table_settings(table_path: str, default_schema: Types.Schema, use_deprecated: bool) -> Types.Attributes:
        return (
            make_table_settings_deprecated(table_path, default_schema)
            if use_deprecated
            else make_table_settings(table_path, default_schema, is_chaos=False)
        )

    @pytest.fixture()
    @staticmethod
    def chaos_table_settings(table_path: str, default_schema: Types.Schema, use_deprecated: bool) -> Types.Attributes:
        return (
            make_table_settings_deprecated(table_path, default_schema)
            if use_deprecated
            else make_table_settings(table_path, default_schema, is_chaos=True)
        )

    @pytest.fixture()
    @staticmethod
    def folder_settings(folder_path: str) -> Types.Attributes:
        return {
            "type": YtNode.Type.FOLDER,
            "clusters": {
                "remote0": {
                    "path": folder_path,
                    "attributes": {"attribute": "value"},
                }
            },
        }

    @pytest.fixture()
    @staticmethod
    def expected_paths(table_path: str) -> dict[str, str]:
        return {"primary": table_path, "remote0": f"{table_path}_sync", "remote1": f"{table_path}_async"}

    @pytest.fixture()
    @staticmethod
    def expected_node_configs(table_path: str) -> dict[str, Any]:
        path_parts = table_path.split("/")
        prefixes = ["/".join(path_parts[: i + 1]) for i in range(1, len(path_parts))]
        return {
            f"{prefixes[i]}&/@": MockResult(result={"type": "map_node", "path": prefixes[i], "name": path_parts[i]})
            for i in range(len(prefixes) - 1)
        }
