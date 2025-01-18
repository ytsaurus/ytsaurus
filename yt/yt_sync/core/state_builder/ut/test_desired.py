from copy import deepcopy
from dataclasses import asdict
from enum import StrEnum
from typing import Any
from typing import Union

import dacite
import pytest

from yt.yt_sync.core.constants import CONSUMER_ATTRS
from yt.yt_sync.core.constants import CONSUMER_SCHEMA
from yt.yt_sync.core.constants import PIPELINE_FILES
from yt.yt_sync.core.constants import PIPELINE_QUEUES
from yt.yt_sync.core.constants import PIPELINE_TABLES
from yt.yt_sync.core.constants import PRODUCER_ATTRS
from yt.yt_sync.core.constants import PRODUCER_SCHEMA
from yt.yt_sync.core.model import get_folder
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtCluster
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.model import YtNode
from yt.yt_sync.core.model import YtNodeAttributes
from yt.yt_sync.core.model import YtSchema
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.model import YtTableAttributes
from yt.yt_sync.core.spec import ClusterNode
from yt.yt_sync.core.spec import ClusterTable
from yt.yt_sync.core.spec import Consumer
from yt.yt_sync.core.spec import Node
from yt.yt_sync.core.spec import QueueRegistration
from yt.yt_sync.core.spec import ReplicationLog
from yt.yt_sync.core.spec import Table
from yt.yt_sync.core.spec.details import SchemaSpec
from yt.yt_sync.core.spec.details import TableSpec
from yt.yt_sync.core.state_builder import DesiredStateBuilder

from .base import make_table_settings
from .base import StateBuilderTestBase


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
                assert log.path == (expected_path or TableSpec.make_replication_log_name(table.path))
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
        with pytest.raises((AssertionError, ValueError, dacite.exceptions.UnexpectedDataError)):
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

    def test_attribute_propagation_with_filter(self, db: YtDatabase, builder: DesiredStateBuilder):
        builder.add_node(
            Node(
                type=YtNode.Type.FOLDER,
                clusters={
                    "primary": ClusterNode(
                        main=True, path="//tmp/folder", attributes={"attr": "value"}, propagated_attributes=set()
                    ),
                    "remote": ClusterNode(
                        path="//tmp/folder", attributes={"attr": "value"}, propagated_attributes={"attr"}
                    ),
                },
            )
        )
        builder.add_node(
            Node(
                type=YtNode.Type.FOLDER,
                clusters={
                    "remote": ClusterNode(
                        main=False,
                        path="//tmp/folder/folder1",
                        attributes={"new_attr": "value"},
                        propagated_attributes={"new_attr"},
                    ),
                },
            )
        )
        builder.add_node(
            Node(
                type=YtNode.Type.FOLDER,
                clusters={
                    "remote": ClusterNode(
                        main=False,
                        path="//tmp/folder/folder2",
                        attributes={"new_attr": "value"},
                        propagated_attributes={"attr"},
                    ),
                },
            )
        )
        builder.add_node(
            Node(
                type=YtNode.Type.FOLDER,
                clusters={
                    "remote": ClusterNode(
                        main=False,
                        path="//tmp/folder/folder3",
                        attributes={"new_attr": "value"},
                    ),
                },
            )
        )
        builder.add_node(
            Node(
                type=YtNode.Type.FILE,
                clusters={
                    "primary": ClusterNode(main=True, path="//tmp/folder/folder/file", attributes={}),
                },
            )
        )
        for i in range(1, 4):
            builder.add_node(
                Node(
                    type=YtNode.Type.FILE,
                    clusters={
                        "remote": ClusterNode(main=False, path=f"//tmp/folder/folder{i}/file", attributes={}),
                    },
                )
            )
        builder.finalize()

        for cluster_name in ("primary", "remote"):
            assert db.clusters[cluster_name].nodes["//tmp/folder"].attributes == YtNodeAttributes(
                attributes={"attr": "value"}
            )

        assert db.clusters["primary"].nodes["//tmp/folder/folder"].attributes == YtNodeAttributes()
        assert db.clusters["primary"].nodes["//tmp/folder/folder/file"].attributes == YtNodeAttributes()

        for i in range(1, 4):
            assert db.clusters["remote"].nodes[f"//tmp/folder/folder{i}"].attributes == YtNodeAttributes(
                attributes={"attr": "value", "new_attr": "value"},
                propagated_attributes={"attr": "//tmp/folder"},
            )
        assert db.clusters["remote"].nodes["//tmp/folder/folder1/file"].attributes == YtNodeAttributes(
            attributes={"new_attr": "value"},
            propagated_attributes={"new_attr": "//tmp/folder/folder1"},
        )
        assert db.clusters["remote"].nodes["//tmp/folder/folder2/file"].attributes == YtNodeAttributes(
            attributes={"attr": "value"},
            propagated_attributes={"attr": "//tmp/folder"},
        )
        assert db.clusters["remote"].nodes["//tmp/folder/folder3/file"].attributes == YtNodeAttributes(
            attributes={"attr": "value", "new_attr": "value"},
            propagated_attributes={"attr": "//tmp/folder", "new_attr": "//tmp/folder/folder3"},
        )

    @pytest.mark.parametrize("use_chaos", [True, False])
    def test_implicit_replicated_queue_attrs(
        self,
        table_path: str,
        ordered_schema: Types.Schema,
        db: YtDatabase,
        builder: DesiredStateBuilder,
        use_chaos: bool,
    ):
        builder.add_table(
            {
                "chaos": use_chaos,
                "schema": ordered_schema,
                "clusters": {
                    "primary": {"main": True, "path": table_path},
                    "remote0": {"path": table_path},
                    "remote1": {"path": table_path},
                },
            }
        )
        builder.finalize()

        main_table = db.main.tables[table_path]
        assert main_table.attributes["preserve_tablet_index"] is True
        assert main_table.attributes["commit_ordering"] == "strong"

        for replica in db.replicas:
            replica_table = replica.tables[table_path]
            assert replica_table.attributes["commit_ordering"] == "strong"
            if use_chaos:
                assert replica_table.attributes["preserve_tablet_index"] is True
            else:
                assert not replica_table.attributes.has_value("preserve_tablet_index")

    def test_add_pipeline(
        self,
        db: YtDatabase,
        builder: DesiredStateBuilder,
    ):
        pipeline_path = "//tmp/pipeline"
        common_spec = {
            "clusters": {
                "primary": {
                    "attributes": {
                        "compression_codec": "lz4",
                        "tablet_cell_bundle": "test_bundle",
                        "primary_medium": "ssd_blobs",
                    },
                },
            },
        }
        builder.add_pipeline(
            {
                "path": pipeline_path,
                "monitoring_project": "test-project",
                "monitoring_cluster": "test-cluster",
                "table_spec": {name: common_spec for name in PIPELINE_TABLES},
                "queue_spec": {name: common_spec for name in PIPELINE_QUEUES},
                "file_spec": {name: common_spec for name in PIPELINE_FILES},
            }
        )
        builder.finalize()

        root = db.main.nodes[f"{pipeline_path}"]
        assert root.attributes["pipeline_format_version"] == 1
        assert root.attributes["monitoring_project"] == "test-project"
        assert root.attributes["monitoring_cluster"] == "test-cluster"

        first_table = db.main.tables[f"{pipeline_path}/{list(PIPELINE_TABLES)[0]}"]
        assert first_table.attributes["primary_medium"] == "ssd_blobs"

        first_queue = db.main.tables[f"{pipeline_path}/{list(PIPELINE_QUEUES)[0]}"]
        assert first_queue.attributes["primary_medium"] == "ssd_blobs"

        first_node = db.main.nodes[f"{pipeline_path}/{list(PIPELINE_FILES)[0]}"]
        assert first_node.node_type == "file"
        assert first_node.attributes["primary_medium"] == "ssd_blobs"

    def test_add_producer(
        self,
        table_path: str,
        db: YtDatabase,
        builder: DesiredStateBuilder,
    ):
        spec = {
            "table": {
                "schema": PRODUCER_SCHEMA,
                "clusters": {"primary": {"path": table_path, "attributes": PRODUCER_ATTRS}},
            },
        }
        builder.add_producer(spec)
        builder.finalize()
        table = db.main.tables[table_path]
        assert table.attributes["treat_as_queue_producer"] == 1
