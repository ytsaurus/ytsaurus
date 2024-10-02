from copy import deepcopy
from dataclasses import asdict
from enum import StrEnum
from typing import Any
from typing import Union

import pytest

import yt.wrapper as yt
from yt.yson.yson_types import YsonList
from yt.yt_sync.core import Settings
from yt.yt_sync.core.constants import CONSUMER_ATTRS
from yt.yt_sync.core.constants import CONSUMER_SCHEMA
from yt.yt_sync.core.model import get_folder
from yt.yt_sync.core.model import get_node_name
from yt.yt_sync.core.model import make_log_name
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtCluster
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.model import YtNode
from yt.yt_sync.core.model import YtNodeAttributes
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtSchema
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.model import YtTableAttributes
from yt.yt_sync.core.model import YtTabletState
from yt.yt_sync.core.spec import ClusterNode
from yt.yt_sync.core.spec import ClusterTable
from yt.yt_sync.core.spec import Consumer
from yt.yt_sync.core.spec import Node
from yt.yt_sync.core.spec import QueueRegistration
from yt.yt_sync.core.spec import ReplicationLog
from yt.yt_sync.core.spec import Table
from yt.yt_sync.core.spec.details import SchemaSpec
from yt.yt_sync.core.state_builder import ActualStateBuilder
from yt.yt_sync.core.state_builder import DesiredStateBuilder
from yt.yt_sync.core.state_builder import DesiredStateBuilderBase
from yt.yt_sync.core.state_builder import DesiredStateBuilderDeprecated
from yt.yt_sync.core.table_filter import TableNameFilter
from yt.yt_sync.core.test_lib import MockResult
from yt.yt_sync.core.test_lib import MockYtClientFactory
from yt.yt_sync.core.test_lib import yt_access_denied
from yt.yt_sync.core.test_lib import yt_generic_error
from yt.yt_sync.core.test_lib import yt_resolve_error


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


def make_aliased_table_settings_deprecated(table_settings: Types.Attributes) -> Types.Attributes:
    table_settings["master"]["cluster"] = "{}#localhost:8888".format(table_settings["master"]["cluster"])
    for replica in ("sync_replicas", "async_replicas"):
        replica_attrs = table_settings[replica]
        replica_clusters = list()
        for cluster in replica_attrs["cluster"]:
            aliased_cluster = f"{cluster}#localhost:8888"
            replica_attrs["attributes"][aliased_cluster] = replica_attrs["attributes"].pop(cluster)
            replica_clusters.append(aliased_cluster)
        replica_attrs["cluster"] = replica_clusters
    return table_settings


def make_aliased_table_settings(table_settings: Types.Attributes) -> Types.Attributes:
    result: Types.Attributes = dict()
    for key, value in table_settings.items():
        if key in ("primary", "remote0", "remote1"):
            result[f"{key}#localhost:8888"] = value
        else:
            result[key] = value
    return result


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
            DesiredStateBuilderDeprecated(yt_client_factory, db, False)
            if use_deprecated
            else DesiredStateBuilder(yt_client_factory, db, False)
        )

    @pytest.fixture()
    @staticmethod
    def chaos_builder(
        yt_client_factory: MockYtClientFactory, db: YtDatabase, use_deprecated: bool
    ) -> DesiredStateBuilderBase:
        return (
            DesiredStateBuilderDeprecated(yt_client_factory, db, True)
            if use_deprecated
            else DesiredStateBuilder(yt_client_factory, db, True)
        )

    @pytest.fixture()
    @staticmethod
    def table_settings(table_path: str, default_schema: Types.Schema, use_deprecated: bool) -> Types.Attributes:
        return (
            make_table_settings_deprecated(table_path, default_schema)
            if use_deprecated
            else make_table_settings(table_path, default_schema, False)
        )

    @pytest.fixture()
    @staticmethod
    def chaos_table_settings(table_path: str, default_schema: Types.Schema, use_deprecated: bool) -> Types.Attributes:
        return (
            make_table_settings_deprecated(table_path, default_schema)
            if use_deprecated
            else make_table_settings(table_path, default_schema, True)
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


class TestDesiredStateBuilder(StateBuilderTestBase):
    @classmethod
    def _to_dict(cls, data: Union[Table, Consumer, Node]) -> Types.Attributes:
        result = asdict(data)

        def _filter(data: Types.Attributes) -> Types.Attributes:
            filtered = dict()
            for k, v in data.items():
                if isinstance(v, dict):
                    filtered[k] = _filter(v)
                elif isinstance(v, list) and v:
                    new_value = list()
                    for item in v:
                        if isinstance(item, dict):
                            new_value.append(_filter(item))
                        elif item is not None:  # list of lists is not supported
                            new_value.append(deepcopy(item))
                    if new_value:
                        filtered[k] = new_value
                elif isinstance(v, StrEnum):
                    filtered[k] = str(v)
                elif v is not None:
                    filtered[k] = deepcopy(v)
            return filtered

        return _filter(result)

    @classmethod
    def _convert(cls, spec: Union[Table, Consumer, Node], raw: bool) -> Union[Table, Consumer, Node, Types.Attributes]:
        return cls._to_dict(spec) if raw else spec

    @pytest.fixture
    def use_deprecated(self) -> bool:
        return False

    @pytest.mark.parametrize("raw", [True, False], ids=["dict", "spec"])
    def test_empty_specs(self, builder: DesiredStateBuilder, raw: bool):
        table_spec = Table()
        with pytest.raises(AssertionError):
            builder.add_table(self._convert(table_spec, raw))

        node_spec = Node()
        with pytest.raises(AssertionError):
            builder.add_node(self._convert(node_spec, raw))

        consumer_spec = Consumer()
        with pytest.raises(AssertionError):
            builder.add_consumer(self._convert(consumer_spec, raw))

    @pytest.mark.parametrize("raw", [True, False], ids=["dict", "spec"])
    def test_broken_specs(self, builder: DesiredStateBuilder, default_schema: Types.Schema, table_path: str, raw: bool):
        table_spec = Table(schema=SchemaSpec.parse(default_schema))
        with pytest.raises(AssertionError):
            builder.add_table(self._convert(table_spec, raw))

        node_spec = Node(type=YtNode.Type.FOLDER)
        with pytest.raises(AssertionError):
            builder.add_node(self._convert(node_spec, raw))

        consumer_spec = Consumer(queues=[QueueRegistration(cluster="primary", path=table_path)])
        with pytest.raises(AssertionError):
            builder.add_consumer(self._convert(consumer_spec, raw))

    def _assert_single_table(
        self,
        db: YtDatabase,
        table_path: str,
        schema: Types.Schema,
        attrs: Types.Attributes,
        node_attrs: Types.Attributes | None = None,
    ):
        assert db.clusters
        assert len(db.clusters) == 1
        main_cluster = db.main
        assert main_cluster.name == "primary"
        assert main_cluster.is_main

        assert main_cluster.tables
        assert len(main_cluster.tables) == 1
        assert table_path in main_cluster.tables
        table = main_cluster.tables[table_path]
        assert table.exists
        assert table.table_type == YtTable.Type.TABLE
        assert table.key == table_path
        assert table.path == table_path
        assert table.cluster_name == "primary"
        assert table.schema == YtSchema.parse(schema)
        assert table.attributes.attributes == attrs

        assert main_cluster.nodes
        assert len(main_cluster.nodes) == 1
        assert table.folder in main_cluster.nodes
        node = main_cluster.nodes[table.folder]
        assert node.exists
        assert node.path == table.folder
        assert node.node_type == YtNode.Type.FOLDER
        assert node.attributes.attributes == {} if node_attrs is None else node_attrs

    @pytest.mark.parametrize("raw", [True, False], ids=["dict", "spec"])
    def test_add_table_single(
        self, builder: DesiredStateBuilder, db: YtDatabase, table_path: str, default_schema: Types.Schema, raw: bool
    ):
        table_attrs = {"dynamic": True}
        spec = Table(
            schema=SchemaSpec.parse(default_schema),
            clusters={"primary": ClusterTable(path=table_path, attributes=table_attrs)},
        )
        builder.add_table(self._convert(spec, raw))
        builder.finalize()
        self._assert_single_table(db, table_path, default_schema, table_attrs)

    def _assert_single_consumer(self, db: YtDatabase, table_path: str):
        self._assert_single_table(db, table_path, CONSUMER_SCHEMA, CONSUMER_ATTRS)

        cluster = db.main
        assert cluster.consumers
        assert len(cluster.consumers) == 1
        consumer = cluster.consumers[table_path]
        assert consumer.table == cluster.tables[table_path]
        assert consumer.registrations
        assert len(consumer.registrations) == 1

        key = ("primary", table_path)
        assert key in consumer.registrations
        registration = consumer.registrations[key]
        assert registration.cluster_name == "primary"
        assert registration.path == table_path
        assert registration.vital

    @pytest.mark.parametrize("raw", [True, False], ids=["dict", "spec"])
    def test_add_consumer_single(self, builder: DesiredStateBuilder, db: YtDatabase, table_path: str, raw: bool):
        spec = Consumer(
            table=Table(
                schema=SchemaSpec.parse(CONSUMER_SCHEMA),
                clusters={"primary": ClusterTable(path=table_path, attributes=CONSUMER_ATTRS)},
            ),
            queues=[QueueRegistration(cluster="primary", path=table_path, vital=True)],
        )
        builder.add_consumer(self._convert(spec, raw))
        builder.finalize()
        self._assert_single_consumer(db, table_path)

    def _assert_single_node(self, db: YtDatabase, path: str, attrs: Types.Attributes):
        assert db.clusters
        assert len(db.clusters) == 1
        main = db.main
        assert main.is_main
        assert main.nodes
        assert len(main.nodes) == 2
        node = main.nodes[path]
        assert node.node_type == YtNode.Type.DOCUMENT
        assert node.cluster_name == "primary"
        assert node.path == path
        assert node.exists
        assert node.attributes.attributes == attrs
        node = main.nodes[node.folder]
        assert node.node_type == YtNode.Type.FOLDER
        assert node.cluster_name == "primary"
        assert node.path == get_folder(path)
        assert node.exists
        assert node.attributes.attributes == {}

    @pytest.mark.parametrize("raw", [True, False], ids=["dict", "spec"])
    def test_add_node_single(self, builder: DesiredStateBuilder, db: YtDatabase, table_path: str, raw: bool):
        node_attrs = {"my_attr": "my_value"}
        spec = Node(
            type=YtNode.Type.DOCUMENT,
            clusters={"primary": ClusterNode(path=table_path, attributes=node_attrs)},
        )
        builder.add_node(self._convert(spec, raw))
        builder.finalize()
        self._assert_single_node(db, table_path, node_attrs)

    @pytest.mark.parametrize("raw", [True, False], ids=["dict", "spec"])
    @pytest.mark.parametrize("reverse_add", [True, False], ids=["table_node", "node_table"])
    def test_add_node_and_table_single(
        self,
        builder: DesiredStateBuilder,
        db: YtDatabase,
        table_path: str,
        default_schema: Types.Schema,
        raw: bool,
        reverse_add: bool,
    ):
        node_path = get_folder(table_path)
        node_attrs = {"my_attr": "my_value"}
        table_attrs = {"dynamic": True}
        table_spec = Table(
            schema=SchemaSpec.parse(default_schema),
            clusters={"primary": ClusterTable(path=table_path, attributes=table_attrs)},
        )
        node_spec = Node(
            type=YtNode.Type.FOLDER,
            clusters={"primary": ClusterNode(path=node_path, attributes=node_attrs)},
        )
        if reverse_add:
            builder.add_table(self._convert(table_spec, raw))
            builder.add_node(self._convert(node_spec, raw))
        else:
            builder.add_node(self._convert(node_spec, raw))
            builder.add_table(self._convert(table_spec, raw))
        builder.finalize()
        self._assert_single_table(db, table_path, default_schema, node_attrs | table_attrs, node_attrs)

    @pytest.mark.parametrize("raw", [True, False], ids=["dict", "spec"])
    @pytest.mark.parametrize("reverse_add", [True, False], ids=["table_node", "node_table"])
    def test_add_node_and_table_single_main_mismatch(
        self,
        builder: DesiredStateBuilder,
        table_path: str,
        default_schema: Types.Schema,
        raw: bool,
        reverse_add: bool,
    ):
        node_path = get_folder(table_path)
        node_attrs = {"my_attr": "my_value"}
        table_attrs = {"dynamic": True}
        table_spec = Table(
            schema=SchemaSpec.parse(default_schema),
            clusters={"primary": ClusterTable(path=table_path, attributes=table_attrs)},
        )
        node_spec = Node(
            type=YtNode.Type.FOLDER,
            clusters={"primary": ClusterNode(main=False, path=node_path, attributes=node_attrs)},
        )
        if reverse_add:
            builder.add_table(self._convert(table_spec, raw))
            with pytest.raises(AssertionError):
                builder.add_node(self._convert(node_spec, raw))
        else:
            builder.add_node(self._convert(node_spec, raw))
            with pytest.raises(AssertionError):
                builder.add_table(self._convert(table_spec, raw))

    @pytest.mark.parametrize("raw", [True, False], ids=["dict", "spec"])
    def test_add_node_no_main(self, builder: DesiredStateBuilder, table_path: str, raw):
        node_spec = Node(
            type=YtNode.Type.FOLDER,
            clusters={"primary": ClusterNode(main=False, path=table_path, attributes={})},
        )
        builder.add_node(self._convert(node_spec, raw))
        with pytest.raises(AssertionError):
            builder.finalize()

    def _assert_replicated_federation(
        self,
        db: YtDatabase,
        table_path: str,
        schema: Types.Schema,
        replica_path: dict[str, str],
        replica_attrs: dict[str, Types.Attributes],
        replica_rtt: dict[str, tuple[bool, bool]],  # replica -> (rtt_enabled, sync)
        in_collocation: bool,
        rtt_enabled: bool,
        additional_attrs: dict[Types.ReplicaKey, Types.Attributes] | None = None,
    ):
        if not additional_attrs:
            additional_attrs = dict()

        assert replica_path.keys() == replica_attrs.keys() == replica_rtt.keys()
        replicas = replica_path.keys()
        yt_schema = YtSchema.parse(schema)

        assert len(db.clusters) == len(replicas) + 1
        main = db.main
        assert main.name not in replicas

        for cluster in db.clusters.values():
            assert len(cluster.tables) == 1
            table = cluster.tables[table_path]
            assert table.key == table_path
            assert table.exists
            assert table.schema == yt_schema
            assert not table.replication_collocation_id
            assert not table.chaos_replication_card_id
            assert not table.chaos_data_table
            assert not table.chaos_replication_log
            assert not table.is_temporary
            assert not table.total_row_count
            assert not table.chaos_replication_progress
            if table.is_replicated:
                assert table.table_type == YtTable.Type.REPLICATED_TABLE
                assert table.path == table_path
                assert table.in_collocation == in_collocation
                assert table.is_rtt_enabled == rtt_enabled
                assert table.attributes.attributes == additional_attrs.get(table.replica_key, dict()) | {
                    "dynamic": True
                }
                assert len(table.replicas) == len(replicas)
                assert table.sync_replicas == {"remote0"}
                assert table.preferred_sync_replicas == ({"remote0"} if rtt_enabled else set())
                rtt_enabled_replicas = set([r.cluster_name for r in table.rtt_enabled_replicas])
                assert rtt_enabled_replicas == ({"remote0", "remote1"} if rtt_enabled else set())
            else:
                assert table.table_type == YtTable.Type.TABLE
                assert table.path == replica_path[cluster.name]
                assert not table.in_collocation
                assert not table.is_rtt_enabled
                assert (
                    table.attributes.attributes
                    == additional_attrs.get(table.replica_key, dict()) | replica_attrs[cluster.name]
                )
                assert not table.replicas
            assert cluster.nodes
            node = cluster.nodes[table.folder]
            assert node.cluster_name == cluster.name
            assert node.path == table.folder
            assert node.node_type == YtNode.Type.FOLDER

    def _assert_node_federation(
        self, db: YtDatabase, table_path: str, node_path: dict[str, str], folder_attrs: dict[str, Any]
    ):
        folder_path = get_folder(table_path)

        def _assert_folder(cluster: YtCluster):
            assert folder_path in cluster.nodes
            node = cluster.nodes[folder_path]
            assert node.cluster_name == cluster.name
            assert node.path == folder_path
            assert node.node_type == YtNode.Type.FOLDER
            if cluster.is_main:
                assert node.attributes.attributes == folder_attrs
            else:
                assert not node.attributes.attributes

        for cluster in db.clusters.values():
            if cluster.is_main:
                assert len(cluster.nodes) == 1
                _assert_folder(cluster)
                assert cluster.name not in node_path
            else:
                assert len(cluster.nodes) == 2
                _assert_folder(cluster)

                assert cluster.name in node_path
                node = cluster.nodes[node_path[cluster.name]]
                assert node.node_type == YtNode.Type.DOCUMENT
                assert node.cluster_name == cluster.name
                assert node.path == node_path[cluster.name]

    @pytest.mark.parametrize("raw", [True, False], ids=["dict", "spec"])
    @pytest.mark.parametrize("in_collocation", [True, False], ids=["collocation", "none"])
    @pytest.mark.parametrize("rtt_enabled", [True, False], ids=["rtt_on", "rtt_off"])
    def test_add_table_federation(
        self,
        builder: DesiredStateBuilder,
        db: YtDatabase,
        table_path: str,
        default_schema: Types.Schema,
        in_collocation: bool,
        rtt_enabled: bool,
        raw: bool,
    ):
        additional_attrs: dict[Types.ReplicaKey, Types.Attributes] = dict()

        folder_attrs = {"folder_attr": "folder_value"}
        folder_spec = Node(
            type=YtNode.Type.FOLDER,
            clusters={"primary": ClusterNode(main=True, path=get_folder(table_path), attributes=folder_attrs)},
        )

        primary_attrs = {"dynamic": True}
        replicas = ("remote0", "remote1", "remote2")

        table_spec = Table(
            schema=SchemaSpec.parse(default_schema),
            in_collocation=in_collocation,
            clusters={"primary": ClusterTable(main=True, path=table_path, attributes=primary_attrs)},
        )
        additional_attrs[("primary", table_path)] = folder_attrs

        node_spec = Node(type=YtNode.Type.DOCUMENT, clusters={})

        replica_path = {}
        replica_attrs = {}
        node_path = {}

        # replica -> (rtt_enabled, sync)
        replica_rtt = {
            "remote0": (True, True),
            "remote1": (True, False),
            "remote2": (False, False),
        }
        for replica in replicas:
            replica_path[replica] = f"{table_path}_{replica}"
            replica_attrs[replica] = {"dynamic": True, "dbg": replica}
            node_path[replica] = f"{table_path}_{replica}_document"

            table_spec.clusters[replica] = ClusterTable(
                path=replica_path[replica],
                attributes=replica_attrs[replica],
                replicated_table_tracker_enabled=replica_rtt[replica][0] if rtt_enabled else False,
                preferred_sync=replica_rtt[replica][1],
            )

            node_spec.clusters[replica] = ClusterNode(main=False, path=node_path[replica])

        builder.add_node(self._convert(folder_spec, raw))
        builder.add_table(self._convert(table_spec, raw))
        builder.add_node(self._convert(node_spec, raw))
        builder.finalize()
        self._assert_replicated_federation(
            db,
            table_path,
            default_schema,
            replica_path,
            replica_attrs,
            replica_rtt,
            in_collocation,
            rtt_enabled,
            additional_attrs,
        )
        self._assert_node_federation(db, table_path, node_path, folder_attrs)

    def _assert_chaos_federation(
        self,
        db: YtDatabase,
        table_path: str,
        schema: Types.Schema,
        replica_path: dict[str, str],
        replica_log_path: dict[str, str | None],
        replica_attrs: dict[str, Types.Attributes],
        replica_log_attrs: dict[str, Types.Attributes | None],
        replica_rtt: dict[str, tuple[bool, bool]],  # replica -> (rtt_enabled, sync)
        in_collocation: bool,
        rtt_enabled: bool,
        additional_attrs: dict[Types.ReplicaKey, Types.Attributes] | None = None,
    ):
        if not additional_attrs:
            additional_attrs = dict()

        assert (
            replica_path.keys()
            == replica_attrs.keys()
            == replica_rtt.keys()
            == replica_log_path.keys()
            == replica_log_attrs.keys()
        )
        replicas = replica_path.keys()
        yt_schema = YtSchema.parse(schema)

        assert len(db.clusters) == len(replicas) + 1
        main = db.main
        assert main.name not in replicas

        for cluster in db.clusters.values():
            assert len(cluster.tables) == 1 if cluster.is_main else 2
            table = cluster.tables[table_path]
            assert table.key == table_path
            assert table.exists
            assert table.schema == yt_schema
            assert not table.replication_collocation_id
            assert not table.chaos_replication_card_id
            assert not table.is_temporary
            assert not table.total_row_count
            assert not table.chaos_replication_progress
            if table.is_replicated:
                assert table.table_type == YtTable.Type.CHAOS_REPLICATED_TABLE
                assert table.path == table_path
                assert table.in_collocation == in_collocation
                assert table.is_rtt_enabled == rtt_enabled
                assert not table.chaos_data_table
                assert not table.chaos_replication_log
                assert table.attributes.attributes == additional_attrs.get(table.replica_key, dict()) | {
                    "dynamic": True
                }
                assert len(table.replicas) == len(replicas) * 2
                assert table.sync_replicas == {"remote0"}
                assert table.preferred_sync_replicas == ({"remote0"} if rtt_enabled else set())
                rtt_enabled_replicas = set([r.cluster_name for r in table.rtt_enabled_replicas])
                assert rtt_enabled_replicas == ({"remote0", "remote1"} if rtt_enabled else set())
            else:
                assert table.table_type == YtTable.Type.TABLE
                assert table.path == replica_path[cluster.name]
                assert not table.in_collocation
                assert not table.is_rtt_enabled
                assert (
                    table.attributes.attributes
                    == additional_attrs.get(table.replica_key, dict()) | replica_attrs[cluster.name]
                )
                assert not table.replicas

                assert table.chaos_replication_log
                assert table.chaos_replication_log in cluster.tables
                log = cluster.tables[table.chaos_replication_log]
                assert log.table_type == YtTable.Type.REPLICATION_LOG
                assert log.chaos_data_table == table.key
                assert table.chaos_replication_log == log.key
                assert log.exists
                assert log.schema == yt_schema
                expected_path = replica_log_path[cluster.name]
                assert log.path == (expected_path or make_log_name(table.path))
                expected_attrs = replica_log_attrs[cluster.name]
                expected_attrs = expected_attrs or {}
                expected_attrs["dynamic"] = True
                assert log.attributes.attributes == additional_attrs.get(log.replica_key, dict()) | expected_attrs

            assert cluster.nodes
            node = cluster.nodes[table.folder]
            assert node.cluster_name == cluster.name
            assert node.path == table.folder
            assert node.node_type == YtNode.Type.FOLDER

    @pytest.mark.parametrize("raw", [True, False], ids=["dict", "spec"])
    @pytest.mark.parametrize("in_collocation", [True, False], ids=["collocation", "none"])
    @pytest.mark.parametrize("rtt_enabled", [True, False], ids=["rtt_on", "rtt_off"])
    def test_add_table_chaos_federation(
        self,
        builder: DesiredStateBuilder,
        db: YtDatabase,
        table_path: str,
        default_schema: Types.Schema,
        in_collocation: bool,
        rtt_enabled: bool,
        raw: bool,
    ):
        builder.is_chaos = True
        additional_attrs: dict[Types.ReplicaKey, Types.Attributes] = dict()

        folder_attrs = {"folder_attr": "folder_value"}
        folder_spec = Node(
            type=YtNode.Type.FOLDER,
            clusters={"primary": ClusterNode(main=True, path=get_folder(table_path), attributes=folder_attrs)},
        )

        primary_attrs = {"dynamic": True}
        replicas = ("remote0", "remote1", "remote2")

        table_spec = Table(
            chaos=True,
            in_collocation=in_collocation,
            schema=SchemaSpec.parse(default_schema),
            clusters={"primary": ClusterTable(main=True, path=table_path, attributes=primary_attrs)},
        )
        additional_attrs[("primary", table_path)] = folder_attrs

        node_spec = Node(type=YtNode.Type.DOCUMENT, clusters={})

        replica_path = {}
        replica_log_path = {}
        replica_attrs = {}
        replica_log_attrs = {}
        node_path = {}

        # replica -> (rtt_enabled, sync)
        replica_rtt = {
            "remote0": (True, True),
            "remote1": (True, False),
            "remote2": (False, False),
        }
        for replica in replicas:
            replica_path[replica] = f"{table_path}_{replica}"
            replica_attrs[replica] = {"dynamic": True, "dbg": replica}
            node_path[replica] = f"{table_path}_{replica}_document"
            if replica == "remote0":
                replica_log_path[replica] = None
                replica_log_attrs[replica] = None
            else:
                replica_log_path[replica] = f"{replica_path[replica]}_custom_log_{replica}"
                replica_log_attrs[replica] = {"log_dbg": replica}

            table_spec.clusters[replica] = ClusterTable(
                path=replica_path[replica],
                attributes=replica_attrs[replica],
                replicated_table_tracker_enabled=replica_rtt[replica][0] if rtt_enabled else False,
                preferred_sync=replica_rtt[replica][1],
            )

            if replica != "remote0":
                table_spec.clusters[replica].replication_log = ReplicationLog(
                    path=replica_log_path[replica], attributes=replica_log_attrs[replica]
                )
            node_spec.clusters[replica] = ClusterNode(main=False, path=node_path[replica])

        builder.add_node(self._convert(folder_spec, raw))
        builder.add_table(self._convert(table_spec, raw))
        builder.add_node(self._convert(node_spec, raw))
        builder.finalize()
        self._assert_chaos_federation(
            db,
            table_path,
            default_schema,
            replica_path,
            replica_log_path,
            replica_attrs,
            replica_log_attrs,
            replica_rtt,
            in_collocation,
            rtt_enabled,
            additional_attrs,
        )
        self._assert_node_federation(db, table_path, node_path, folder_attrs)

    def test_schema_override(self, db: YtDatabase, builder: DesiredStateBuilder, table_path: str):
        schema = [{"name": "key", "type": "uint64", "sort_order": "ascending"}, {"name": "value", "type": "string"}]
        schema_override = [
            {"name": "key", "type": "uint64", "sort_order": "ascending"},
            {"name": "value", "type": "string", "max_inline_hunk_size": 10},
        ]

        builder.add_table(
            {
                "schema": schema,
                "clusters": {
                    "primary": {"main": True, "path": table_path},
                    "remote0": {"main": False, "path": table_path},
                    "remote1": {"main": False, "path": table_path, "schema_override": schema_override},
                },
            }
        )
        builder.finalize()
        for cluster_name in ("primary", "remote0", "remote1"):
            table = db.clusters[cluster_name].tables[table_path]
            if cluster_name == "remote1":
                assert table.schema == YtSchema.parse(schema_override)
            else:
                assert table.schema == YtSchema.parse(schema)

    def test_cluster_type_mismatch(
        self,
        builder: DesiredStateBuilder,
        table_path: str,
        default_schema: Types.Schema,
    ):
        builder.add_table(
            {
                "schema": default_schema,
                "clusters": {
                    "primary": {"main": True, "path": table_path},
                    "remote0": {"path": table_path},
                    "remote1": {"path": table_path},
                },
            }
        )
        table2 = f"{table_path}_2"
        with pytest.raises(AssertionError):
            builder.add_table(
                {
                    "schema": default_schema,
                    "clusters": {
                        "primary": {"path": table2},
                        "remote0": {"path": table2},
                        "remote1": {"main": True, "path": table2},
                    },
                }
            )

        node_path = get_folder(table_path)
        with pytest.raises(AssertionError):
            builder.add_node(
                Node(
                    type=YtNode.Type.FOLDER,
                    clusters={
                        "primary": ClusterNode(path=node_path),
                        "remote0": ClusterNode(main=True, path=node_path),
                    },
                )
            )

        consumer_path = f"{table_path}_consumer"
        with pytest.raises(AssertionError):
            builder.add_consumer(
                {
                    "table_settings": {
                        "schema": CONSUMER_SCHEMA,
                        "clusters": {
                            "primary": {"path": consumer_path},
                            "remote0": {"main": True, "path": consumer_path, "attributes": CONSUMER_ATTRS},
                        },
                    }
                }
            )

    def test_attribute_propagation(self, default_schema: Types.Schema, db: YtDatabase, builder: DesiredStateBuilder):
        builder.add_node(
            Node(
                type=YtNode.Type.FOLDER,
                clusters={
                    "primary": ClusterNode(main=True, path="//tmp/folder", attributes={"attribute": "value"}),
                    "remote0": ClusterNode(path="//tmp/folder", attributes={"attribute": "value"}),
                    "remote1": ClusterNode(path="//tmp/folder", attributes={"attribute": "value"}),
                },
            )
        )
        builder.add_node(
            Node(
                type=YtNode.Type.FOLDER,
                clusters={
                    "primary": ClusterNode(
                        main=True, path="//tmp/folder/folder", attributes={"attribute": "new_value"}
                    ),
                    "remote0": ClusterNode(path="//tmp/folder/folder", attributes={"other_attribute": "value"}),
                },
            )
        )
        builder.add_node(
            Node(
                type=YtNode.Type.FOLDER,
                clusters={
                    "remote1": ClusterNode(
                        main=False, path="//tmp/folder/folder/folder", attributes={"some_attribute": "do_not_overwrite"}
                    ),
                },
            )
        )

        table_path = "//tmp/folder/folder/folder/table"
        builder.add_table(make_table_settings(table_path, default_schema, False))
        builder.finalize()

        for cluster_name in ("primary", "remote0", "remote1"):
            assert db.clusters[cluster_name].nodes["//tmp/folder"].attributes == YtNodeAttributes(
                attributes={"attribute": "value"}
            )

        assert db.clusters["primary"].nodes["//tmp/folder/folder"].attributes == YtNodeAttributes(
            attributes={"attribute": "new_value"}
        )
        assert db.clusters["primary"].tables[table_path].attributes == YtTableAttributes(
            attributes={"attribute": "new_value"} | {"dynamic": True},
            propagated_attributes={"attribute": "//tmp/folder/folder"},
        )

        assert db.clusters["remote0"].nodes["//tmp/folder/folder"].attributes == YtNodeAttributes(
            attributes={"attribute": "value", "other_attribute": "value"},
            propagated_attributes={"attribute": "//tmp/folder"},
        )
        assert db.clusters["remote0"].tables[table_path].attributes == YtTableAttributes(
            attributes={"attribute": "value", "other_attribute": "value"} | {"dynamic": True},
            propagated_attributes={"attribute": "//tmp/folder", "other_attribute": "//tmp/folder/folder"},
        )

        assert db.clusters["remote1"].nodes["//tmp/folder/folder"].attributes == YtNodeAttributes(
            attributes={"attribute": "value"},
            propagated_attributes={"attribute": "//tmp/folder"},
        )
        assert db.clusters["remote1"].tables[table_path].attributes == YtTableAttributes(
            attributes={"attribute": "value", "some_attribute": "do_not_overwrite"} | {"dynamic": True},
            propagated_attributes={"attribute": "//tmp/folder", "some_attribute": "//tmp/folder/folder/folder"},
        )


class TestDesiredStateBuilderDeprecated(StateBuilderTestBase):
    @pytest.fixture
    def use_deprecated(self) -> bool:
        return True

    def test_no_path(self, builder: DesiredStateBuilderDeprecated):
        with pytest.raises(AssertionError):
            builder.add_table({"master": {"attributes": {}}})

    def test_no_master(self, table_path: str, builder: DesiredStateBuilderDeprecated):
        with pytest.raises(AssertionError):
            builder.add_table({"path": table_path})

    def _test_master_only(
        self, table_path: str, table_settings: Types.Attributes, db: YtDatabase, builder: DesiredStateBuilderDeprecated
    ):
        table_settings.pop("sync_replicas")
        table_settings.pop("async_replicas")
        builder.add_table(table_settings)

        assert "primary" in db.clusters
        for cluster in ("remote0", "remote1"):
            assert cluster not in db.clusters

        primary_cluster = db.clusters["primary"]
        assert primary_cluster.cluster_type == YtCluster.Type.SINGLE
        assert table_path in primary_cluster.tables

        table = primary_cluster.tables[table_path]
        assert YtTable.Type.TABLE == table.table_type
        assert not table.replicas

    def test_master_only(
        self, table_path: str, table_settings: Types.Attributes, db: YtDatabase, builder: DesiredStateBuilderDeprecated
    ):
        self._test_master_only(table_path, table_settings, db, builder)

    def test_master_only_chaos(
        self,
        table_path: str,
        table_settings: Types.Attributes,
        db: YtDatabase,
        chaos_builder: DesiredStateBuilderDeprecated,
    ):
        self._test_master_only(table_path, table_settings, db, chaos_builder)

    def _test_with_replicas_common(
        self,
        table_path: str,
        table_settings: Types.Attributes,
        db: YtDatabase,
        builder: DesiredStateBuilderDeprecated,
        table_type: str,
        expected_paths: dict[str, str],
    ):
        builder.add_table(table_settings)
        table_key = table_path

        for cluster in ("primary", "remote0", "remote1"):
            assert cluster in db.clusters
            assert table_key in db.clusters[cluster].tables
            yt_table = db.clusters[cluster].tables[table_key]
            assert table_key == yt_table.key
            assert expected_paths[cluster] == yt_table.path

        primary_cluster = db.clusters["primary"]
        assert primary_cluster.cluster_type == YtCluster.Type.MAIN
        replicated_table = primary_cluster.tables[table_key]
        assert table_type == replicated_table.table_type
        assert replicated_table.replicas
        cluster2mode = {"remote0": YtReplica.Mode.SYNC, "remote1": YtReplica.Mode.ASYNC}
        for replica in replicated_table.replicas.values():
            assert replica.cluster_name in ("remote0", "remote1")
            assert replica.enabled is True
            assert cluster2mode[replica.cluster_name] == replica.mode

    def test_with_replicas(
        self,
        table_path: str,
        table_settings: Types.Attributes,
        db: YtDatabase,
        builder: DesiredStateBuilderDeprecated,
        expected_paths: dict[str, str],
    ):
        self._test_with_replicas_common(
            table_path, table_settings, db, builder, YtTable.Type.REPLICATED_TABLE, expected_paths
        )

        replicated_table = db.clusters["primary"].tables[table_path]
        for replica in replicated_table.replicas.values():
            assert expected_paths[replica.cluster_name] == replica.replica_path

    def test_with_replicas_aliased(
        self,
        table_path: str,
        table_settings: Types.Attributes,
        db: YtDatabase,
        builder: DesiredStateBuilderDeprecated,
        yt_client_factory: MockYtClientFactory,
        expected_paths: dict[str, str],
    ):
        aliased_table_settings = make_aliased_table_settings_deprecated(table_settings)
        self._test_with_replicas_common(
            table_path, aliased_table_settings, db, builder, YtTable.Type.REPLICATED_TABLE, expected_paths
        )

        replicated_table = db.clusters["primary"].tables[table_path]
        for replica in replicated_table.replicas.values():
            assert expected_paths[replica.cluster_name] == replica.replica_path

        for c in ("primary", "remote0", "remote1"):
            assert c in yt_client_factory.aliases
            assert "localhost:8888" == yt_client_factory.aliases[c]

    def test_with_replicas_chaos(
        self,
        table_path: str,
        table_settings: Types.Attributes,
        db: YtDatabase,
        chaos_builder: DesiredStateBuilderDeprecated,
        expected_paths: dict[str, str],
    ):
        self._test_with_replicas_common(
            table_path, table_settings, db, chaos_builder, YtTable.Type.CHAOS_REPLICATED_TABLE, expected_paths
        )
        log_key = f"{table_path}_log"
        for cluster in ("remote0", "remote1"):
            assert log_key in db.clusters[cluster].tables

        replicated_table = db.clusters["primary"].tables[table_path]
        for replica in replicated_table.replicas.values():
            assert (
                replica.content_type in YtReplica.ContentType.all()
            ), f"Unknown content type for replica {replica.replica_path}"
            if YtReplica.ContentType.DATA == replica.content_type:
                assert expected_paths[replica.cluster_name] == replica.replica_path
            else:
                log_path = f"{expected_paths[replica.cluster_name]}_log"
                assert log_path == replica.replica_path

    def test_table_filter_pass(
        self, yt_client_factory: MockYtClientFactory, db: YtDatabase, table_path: str, table_settings: Types.Attributes
    ):
        table_name = get_node_name(table_path)
        builder = DesiredStateBuilderDeprecated(yt_client_factory, db, False, TableNameFilter([table_name]))
        builder.add_table(table_settings)

        for cluster in ("primary", "remote0", "remote1"):
            assert cluster in db.clusters
            assert table_path in db.clusters[cluster].tables

    def test_table_filter_nopass(
        self, yt_client_factory: MockYtClientFactory, db: YtDatabase, table_settings: Types.Attributes
    ):
        builder = DesiredStateBuilderDeprecated(yt_client_factory, db, False, TableNameFilter(["_some_table_"]))
        builder.add_table(table_settings)

        for cluster in ("primary", "remote0", "remote1"):
            assert cluster not in db.clusters

    @staticmethod
    def _get_replica(db, settings, cluster, chaos_log=False):
        suffix = "" if not chaos_log else "_log"
        return (
            db.clusters["primary"]
            .tables[settings["path"]]
            .replicas[Types.ReplicaKey((cluster, settings["sync_replicas"]["replica_path"] + suffix))]
        )

    def test_add_node(
        self,
        folder_path: str,
        folder_settings: Types.Attributes,
        db: YtDatabase,
        builder: DesiredStateBuilderDeprecated,
    ):
        db.add_or_get_cluster(YtCluster.make(YtCluster.Type.MAIN, "primary", False))
        db.add_or_get_cluster(YtCluster.make(YtCluster.Type.REPLICA, "remote0", False, YtCluster.Mode.SYNC))
        db.add_or_get_cluster(YtCluster.make(YtCluster.Type.REPLICA, "remote1", False, YtCluster.Mode.ASYNC))

        builder.add_node(folder_settings)

        assert not builder._implicit_clusters

        assert db.clusters["primary"].nodes.get(folder_path, None) is None
        node = db.clusters["remote0"].nodes.get(folder_path, None)
        assert node is not None
        assert node.exists
        assert not node.attributes.has_diff_with(YtNodeAttributes({"attribute": "value"}))
        assert db.clusters["remote1"].nodes.get(folder_path, None) is None

        with pytest.raises(AssertionError):
            builder.add_node(folder_settings)

    def test_add_node_without_cluster(
        self,
        folder_path: str,
        folder_settings: Types.Attributes,
        db: YtDatabase,
        builder: DesiredStateBuilderDeprecated,
    ):
        folder_settings["clusters"]["remote1"] = folder_settings["clusters"]["remote0"]
        builder.add_node(folder_settings)

        assert "primary" not in db.clusters
        assert "primary" not in builder._implicit_clusters

        for cluster_name in ("remote0", "remote1"):
            assert cluster_name not in db.clusters
            assert cluster_name in builder._implicit_clusters
            assert builder._implicit_clusters[cluster_name].cluster_type == YtCluster.Type.REPLICA
            assert builder._implicit_clusters[cluster_name].mode == YtCluster.Mode.ASYNC
            assert not builder._implicit_clusters[cluster_name].is_chaos

            node = builder._implicit_clusters[cluster_name].nodes.get(folder_path, None)
            assert node is not None
            assert node.exists
            assert node.node_type == YtNode.Type.FOLDER
            assert not node.attributes.has_diff_with(YtNodeAttributes({"attribute": "value"}))

        with pytest.raises(AssertionError):
            builder.add_node(folder_settings)

        builder._fix_missing_clusters()

        assert not builder._implicit_clusters
        assert "remote0" in db.clusters
        assert "remote1" in db.clusters
        main_cluster = "remote0"
        async_cluster = "remote1"
        if db.clusters["remote0"].cluster_type != YtCluster.Type.MAIN:
            main_cluster, async_cluster = async_cluster, main_cluster

        assert db.clusters[main_cluster].cluster_type == YtCluster.Type.MAIN
        assert not db.clusters[main_cluster].mode
        assert db.clusters[async_cluster].cluster_type == YtCluster.Type.REPLICA
        assert db.clusters[async_cluster].mode == YtCluster.Mode.ASYNC

        assert folder_path in db.clusters[main_cluster].nodes
        assert folder_path in db.clusters[async_cluster].nodes

    def test_add_node_and_table(
        self,
        table_settings: Types.Attributes,
        folder_path: str,
        folder_settings: Types.Attributes,
        db: YtDatabase,
        builder: DesiredStateBuilderDeprecated,
    ):
        folder_settings["clusters"]["special"] = {
            "path": folder_path,
            "attributes": {"attribute": "value"},
        }
        builder.add_node(folder_settings)

        for cluster_name in ("remote0", "special"):
            assert cluster_name not in db.clusters
            assert cluster_name in builder._implicit_clusters
            assert builder._implicit_clusters[cluster_name].cluster_type == YtCluster.Type.REPLICA
            assert builder._implicit_clusters[cluster_name].mode == YtCluster.Mode.ASYNC
            assert not builder._implicit_clusters[cluster_name].is_chaos

            node = builder._implicit_clusters[cluster_name].nodes.get(folder_path, None)
            assert node is not None
            assert node.exists
            assert node.node_type == YtNode.Type.FOLDER
            assert not node.attributes.has_diff_with(YtNodeAttributes({"attribute": "value"}))

        builder.add_table(table_settings)

        assert "remote0" not in builder._implicit_clusters
        assert "remote0" in db.clusters
        assert db.clusters["remote0"].cluster_type == YtCluster.Type.REPLICA
        assert db.clusters["remote0"].mode == YtCluster.Mode.SYNC

        assert "special" in builder._implicit_clusters
        assert "special" not in db.clusters


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

    def test_table_state_load(
        self,
        settings: Settings,
        table_path: str,
        filled_db: YtDatabase,
        mock_yt_config: Types.Attributes,
        yt_table_schema: YsonList,
    ):
        mock_yt_config["primary"]["get"][f"{table_path}&/@"] = MockResult(
            result={"path": table_path, "type": "special", "name": get_node_name(table_path), "schema": yt_table_schema}
        )
        yt_client_factory = MockYtClientFactory(mock_yt_config)
        actual_db = ActualStateBuilder(settings, yt_client_factory).build_from(filled_db)

        assert "primary" in actual_db.clusters
        assert table_path in actual_db.clusters["primary"].tables
        table = actual_db.clusters["primary"].tables[table_path]
        assert table.exists
        assert table.table_type == "special"

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
                    f"{folder_path}&/@type": MockResult(error=yt_access_denied()),
                    f"{folder_path}/folder1&/@type": MockResult(result=YtNode.Type.FOLDER),
                    f"{folder_path}/folder1/folder&/@type": MockResult(error=yt_resolve_error()),
                    f"{folder_path}/folder1/folder/file&/@": MockResult(error=yt_resolve_error()),
                    f"{folder_path}/folder2&/@type": MockResult(error=yt_access_denied()),
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

    def test_links(
        self,
        settings: Settings,
        filled_db: YtDatabase,
        builder: DesiredStateBuilder,
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
        self, settings: Settings, db: YtDatabase, builder: DesiredStateBuilder, use_deprecated: bool
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
                    f"{get_folder(link_path)}&/@type": MockResult(error=yt_resolve_error()),
                    f"{link_path}&/@": MockResult(error=yt_resolve_error()),
                    "//no/path&/@type": MockResult(error=yt_resolve_error()),
                }
            }
        }
        yt_client_factory = MockYtClientFactory(mock_yt_config)
        with pytest.raises(AssertionError):
            ActualStateBuilder(settings, yt_client_factory).build_from(db)

    def test_link_on_link(self, settings: Settings, db: YtDatabase, builder: DesiredStateBuilder, use_deprecated: bool):
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
                    f"{folder_path}&/@type": MockResult(result={"type": YtNode.Type.FOLDER}),
                    f"{link_path}_old&/@type": MockResult(result=YtNode.Type.LINK),
                    f"{link_path}&/@": MockResult(error=yt_resolve_error()),
                }
            }
        }
        yt_client_factory = MockYtClientFactory(mock_yt_config)
        with pytest.raises(AssertionError):
            ActualStateBuilder(settings, yt_client_factory).build_from(db)
