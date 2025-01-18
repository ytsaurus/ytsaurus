from __future__ import annotations

from abc import ABC
from dataclasses import asdict
from dataclasses import dataclass
from typing import Any
from typing import ClassVar
from typing import List
from typing import Mapping

from dacite import Config
from dacite import from_dict
from dacite.exceptions import UnionMatchError

from yt.yt_sync.core.constants import PIPELINE_FILES
from yt.yt_sync.core.constants import PIPELINE_QUEUES
from yt.yt_sync.core.constants import PIPELINE_TABLES

from .consumer import Consumer
from .consumer import QueueRegistration
from .node import ClusterNode
from .node import Node
from .pipeline import Pipeline
from .producer import Producer
from .table import ClusterTable
from .table import Column
from .table import ReplicationLog
from .table import Table
from .table import TypeV1
from .table import TypeV3
from .table import TypeV3Union

_SCHEMA_PARSE_CONFIG = Config(
    cast=[
        TypeV1,
        TypeV3.Type,
        Column.SortOrder,
        Column.AggregateFunc,
    ],
    strict=True,
)


@dataclass
class TableSpec:
    spec: Table

    @property
    def chaos(self) -> bool:
        return self.spec.chaos is True

    @property
    def in_collocation(self) -> bool:
        return self.spec.in_collocation is True

    @property
    def schema(self) -> list[Column]:
        assert self.spec.schema
        return self.spec.schema

    @property
    def clusters(self) -> dict[str, ClusterTable]:
        assert self.spec.clusters
        return self.spec.clusters

    @property
    def is_federation(self) -> bool:
        return self.spec.clusters and len(self.spec.clusters) > 1

    @property
    def main_cluster(self) -> str:
        main_clusters: list[str] = (
            [cluster for cluster, spec in self.spec.clusters.items() if spec.main] if self.clusters else []
        )
        assert len(main_clusters) == 1, f"One main cluster required in table specification, got {main_clusters}"
        return main_clusters[0]

    @property
    def main_cluster_spec(self) -> ClusterTable:
        return self.spec.clusters[self.main_cluster]

    @property
    def main_table_type(self) -> Table.Type:
        if self.is_federation:
            return Table.Type.CHAOS_REPLICATED_TABLE if self.spec.chaos else Table.Type.REPLICATED_TABLE
        return Table.Type.TABLE

    @staticmethod
    def make_replication_log_name(path: str) -> str:
        return f"{path}_log"

    def _ensure_replication_log(self, replication_log_spec: ReplicationLog, data_replica_path: str):
        if not replication_log_spec.path:
            replication_log_spec.path = self.make_replication_log_name(data_replica_path)
        if replication_log_spec.attributes is None:
            replication_log_spec.attributes = dict()

    def _ensure_cluster_spec(self, name: str, cluster_spec: ClusterTable):
        assert name, "Cluster name must not be empty"
        assert cluster_spec.path, f"Empty path not allowed for cluster {name}"
        if cluster_spec.main:
            assert (
                cluster_spec.replicated_table_tracker_enabled is None
            ), f"Attribute 'replicated_table_tracker_enabled' is not allowed for main {name}:{cluster_spec.path}"
            assert (
                cluster_spec.preferred_sync is None
            ), f"Attribute 'preferred_sync' is not allowed for main {name}:{cluster_spec.path}"
            assert (
                cluster_spec.replication_log is None
            ), f"Attribute 'replication_log' is not allowed for main {name}:{cluster_spec.path}"
        else:
            if self.chaos:
                if cluster_spec.replication_log is None:
                    cluster_spec.replication_log = ReplicationLog()
                self._ensure_replication_log(cluster_spec.replication_log, cluster_spec.path)
            else:
                assert (
                    cluster_spec.replication_log is None
                ), f"Attribute 'replication_log' is not allowed for replica {name}:{cluster_spec.path}"
        if cluster_spec.schema_override:
            SchemaSpec.ensure(cluster_spec.schema_override)
        if cluster_spec.attributes is None:
            cluster_spec.attributes = dict()

    def ensure(self):
        SchemaSpec.ensure(self.spec.schema)
        assert self.spec.clusters, "Empty clusters not allowed in table specification"

        # check main clusters
        main_clusters: list[str] = list()
        for cluster_name, cluster_spec in self.clusters.items():
            if cluster_spec.main is None:
                cluster_spec.main = not self.is_federation
            if cluster_spec.main:
                main_clusters.append(cluster_name)
        assert (
            len(main_clusters) == 1
        ), f"One main cluster should be defined in table specification, got {main_clusters}"

        main_cluster_name: str = self.main_cluster
        main_cluster_spec: ClusterTable = self.main_cluster_spec
        if not self.is_federation:
            assert self.spec.in_collocation is None, (
                "Attribute 'in_collocation' is not allowed for "
                + f"non-replicated table {main_cluster_name}:{main_cluster_spec.path}"
            )

        for cluster_name, cluster_spec in self.clusters.items():
            self._ensure_cluster_spec(cluster_name, cluster_spec)

    @classmethod
    def parse(cls, raw: dict[str, Any], with_ensure: bool = True) -> TableSpec:
        assert isinstance(raw, Mapping), f"Dict-like object expected as raw data, got: {type(raw)} {raw}"
        result = cls(spec=from_dict(data_class=Table, data=raw, config=_SCHEMA_PARSE_CONFIG))
        if with_ensure:
            result.ensure()
        return result


@dataclass
class ConsumerSpec:
    spec: Consumer

    @property
    def table(self) -> TableSpec:
        assert self.spec.table
        return TableSpec(spec=self.spec.table)

    @property
    def queues(self) -> list[QueueRegistration]:
        assert self.spec.queues
        return self.spec.queues

    def _ensure_queue_spec(self, queue_spec: QueueRegistration):
        assert queue_spec.cluster, "Mandatory attribute 'cluster' is missing or empty in queue specification"
        assert queue_spec.path, "Mandatory attribute 'path' is missing or empty in queue specification"
        queue_spec.vital = bool(queue_spec.vital)

    def ensure(self):
        assert self.spec.table, "Mandatory attribute 'table' is missing in consumer specification"
        assert self.spec.queues, "Mandatory attribute 'queues' is missing or empty in consumer specification"
        table_spec = TableSpec(self.spec.table)
        table_spec.ensure()

        assert table_spec.main_cluster_spec.attributes.get(
            "treat_as_queue_consumer", False
        ), "Mandatory attribute 'treat_as_queue_consumer' in table attributes is absent"
        for queue_spec in self.spec.queues:
            self._ensure_queue_spec(queue_spec)

    @classmethod
    def parse(cls, raw: dict[str, Any], with_ensure: bool = True) -> ConsumerSpec:
        assert isinstance(raw, Mapping), "dict-like object expected as raw data"

        result = cls(
            spec=from_dict(data_class=Consumer, data=raw, config=_SCHEMA_PARSE_CONFIG),
        )
        if with_ensure:
            result.ensure()
        return result


@dataclass
class ProducerSpec:
    spec: Producer

    @property
    def table(self) -> TableSpec:
        assert self.spec.table
        return TableSpec(spec=self.spec.table)

    def ensure(self):
        assert self.spec.table, "Mandatory attribute 'table' is missing in consumer specification"
        table_spec = TableSpec(self.spec.table)
        table_spec.ensure()
        assert table_spec.main_cluster_spec.attributes.get(
            "treat_as_queue_producer", False
        ), "Mandatory attribute 'treat_as_queue_consumer' in table attributes is absent"

    @classmethod
    def parse(cls, raw: dict[str, Any], with_ensure: bool = True) -> ProducerSpec:
        assert isinstance(raw, Mapping), "dict-like object expected as raw data"
        result = cls(spec=from_dict(data_class=Producer, data=raw, config=_SCHEMA_PARSE_CONFIG))
        if with_ensure:
            result.ensure()
        return result


@dataclass
class NodeSpec:
    spec: Node

    @property
    def type(self) -> Node.Type:
        assert self.spec.type
        return self.spec.type

    @property
    def clusters(self) -> dict[str, ClusterNode]:
        assert self.spec.clusters
        return self.spec.clusters

    @property
    def is_federation(self) -> bool:
        return self.spec.clusters and len(self.spec.clusters) > 1

    @property
    def main_cluster(self) -> str | None:
        main_clusters: list[str] = (
            [cluster for cluster, spec in self.spec.clusters.items() if spec.main] if self.spec.clusters else []
        )
        assert (
            len(main_clusters) <= 1
        ), f"No more than one main cluster required in node specification, got {main_clusters}"
        return main_clusters[0] if main_clusters else None

    @property
    def main_cluster_spec(self) -> ClusterNode:
        assert self.main_cluster, "Can't get main cluster spec, no main cluster"
        return self.spec.clusters[self.main_cluster]

    def _ensure_cluster_spec(self, name: str, node_type: Node.Type, cluster_spec: ClusterNode):
        assert cluster_spec.path, f"Empty or absent 'path' is not allowed for cluster '{name}' in node specification"
        if cluster_spec.attributes is None:
            cluster_spec.attributes = dict()
        if node_type == Node.Type.LINK:
            assert (
                cluster_spec.target_path
            ), f"Empty or absent 'target_path' is not allowed for link {name}:{cluster_spec.path}"

    def ensure(self):
        assert self.spec.type, "Mandatory attribute 'type' is missing in node specifications"
        assert self.spec.clusters, "Empty 'clusters' not allowed in node specification"

        # check main clusters
        main_clusters: list[str] = list()
        for cluster_name, cluster_spec in self.clusters.items():
            if cluster_spec.main is None:
                cluster_spec.main = not self.is_federation
            if cluster_spec.main:
                main_clusters.append(cluster_name)
        assert (
            len(main_clusters) <= 1
        ), f"No more than one main cluster allowed in node specification, got {main_clusters}"

        for cluster_name, cluster_spec in self.clusters.items():
            self._ensure_cluster_spec(cluster_name, self.spec.type, cluster_spec)

    @classmethod
    def parse(cls, raw: dict[str, Any], with_ensure: bool = True) -> Node:
        assert isinstance(raw, Mapping), "dict-like object expected as raw data"
        result = cls(spec=from_dict(data_class=Node, data=raw, config=Config(cast=[Node.Type], strict=True)))
        if with_ensure:
            result.ensure()
        return result


@dataclass
class PipelineSpec:
    spec: Pipeline

    @property
    def path(self) -> str:
        assert self.spec.path
        return self.spec.path

    @property
    def monitoring_project(self) -> str:
        assert self.spec.monitoring_project is not None
        return self.spec.monitoring_project

    @property
    def monitoring_cluster(self) -> str:
        assert self.spec.monitoring_cluster is not None
        return self.spec.monitoring_cluster

    @property
    def table_spec(self) -> str:
        return self.spec.table_spec or {}

    @property
    def queue_spec(self) -> str:
        return self.spec.queue_spec or {}

    @property
    def file_spec(self) -> str:
        return self.spec.file_spec or {}

    def main_cluster(self) -> str | None:
        common_main_cluster = None
        for entities in (self.table_spec, self.queue_spec, self.file_spec):
            for name, entity in entities.items():
                for cluster, cluster_spec in entity.clusters.items():
                    if len(entity.clusters) == 1 or entity.clusters[cluster].main:
                        assert (
                            common_main_cluster is None or cluster == common_main_cluster
                        ), "There can be only one main cluster"
                        common_main_cluster = cluster
        return common_main_cluster

    def ensure(self):
        assert self.spec.path, "Mandatory attribute 'path' is missing in pipeline specification"
        assert (
            self.spec.monitoring_project is not None
        ), "Mandatory attribute 'monitoring_project' is missing in pipeline specification"
        assert (
            self.spec.monitoring_cluster is not None
        ), "Mandatory attribute 'monitoring_cluster' is missing in pipeline specification"

        def check_lists(type_name, actual, expected):
            assert set(actual) <= set(expected), f"Unknown {type_name} in pipeline spec: {set(actual) - set(expected)}"
            assert set(actual) >= set(expected), f"Missed {type_name} in pipeline spec: {set(expected) - set(actual)}"

        check_lists("file", self.file_spec.keys(), PIPELINE_FILES)
        check_lists("queue", self.queue_spec.keys(), PIPELINE_QUEUES)
        check_lists("table", self.table_spec.keys(), PIPELINE_TABLES)

        main_cluster = self.main_cluster()

        def check_attributes(spec, type_name, field_name):
            mandatory_attributes = ["primary_medium"]

            if type_name in ("queue", "table"):
                mandatory_attributes.append("tablet_cell_bundle")

            if type_name == "file":
                assert not spec.type, "'type' can not be set for file '{name}'"
                mandatory_attributes.append("compression_codec")

            for cluster, cluster_spec in spec.clusters.items():
                for field in mandatory_attributes:
                    assert (
                        field in cluster_spec.attributes
                    ), f"Mandatory attribute '{field}' is missed for {type_name} '{name}'"
                assert not cluster_spec.path, f"'path' can not be explicitly set for pipeline {type_name} '{name}'"

        for name, spec in self.file_spec.items():
            assert set((main_cluster,)) == set(spec.clusters.keys()), (
                "Pipeline files can be only on main cluster. "
                f"Main cluster: {main_cluster}, clusters: {set(spec.clusters.keys())}"
            )
            check_attributes(spec, "file", name)
        for name, spec in self.queue_spec.items():
            check_attributes(spec, "queue", name)
        for name, spec in self.table_spec.items():
            check_attributes(spec, "table", name)

    @classmethod
    def parse(cls, raw: dict[str, Any], with_ensure: bool = True) -> Pipeline:
        assert isinstance(raw, Mapping), "dict-like object expected as raw data"
        result = cls(spec=from_dict(data_class=Pipeline, data=raw, config=Config(strict=True)))
        if with_ensure:
            result.ensure()
        return result


class TypeSpecBase(ABC):
    @property
    def is_active(self) -> bool:
        raise NotImplementedError()

    @property
    def type_v1(self) -> TypeV1:
        raise NotImplementedError()

    @property
    def type_v3(self) -> TypeV3.Type:
        raise NotImplementedError()


@dataclass
class TypeV1Spec(TypeSpecBase):
    column: Column

    INTEGER: ClassVar[frozenset[TypeV1]] = frozenset(
        {
            TypeV1.INT8,
            TypeV1.INT16,
            TypeV1.INT32,
            TypeV1.INT64,
            TypeV1.UINT8,
            TypeV1.UINT16,
            TypeV1.UINT32,
            TypeV1.UINT64,
        }
    )
    REAL: ClassVar[frozenset[TypeV1]] = frozenset(
        {
            TypeV1.FLOAT,
            TypeV1.DOUBLE,
        }
    )
    NUMERIC: ClassVar[frozenset[TypeV1]] = INTEGER | REAL
    BIG_NUMERIC: ClassVar[frozenset[TypeV1]] = frozenset(
        {
            TypeV1.INT64,
            TypeV1.UINT64,
            TypeV1.DOUBLE,
        }
    )
    BYTES: ClassVar[frozenset[TypeV1]] = frozenset(
        {
            TypeV1.STRING,
            TypeV1.UTF8,
            TypeV1.ANY,
            TypeV1.JSON,
            TypeV1.UUID,
        }
    )
    NON_COMPARABLE: ClassVar[frozenset[TypeV1]] = frozenset(
        {
            TypeV1.JSON,
        }
    )
    BOOL: ClassVar[frozenset[TypeV1]] = frozenset({TypeV1.BOOLEAN})

    @property
    def is_active(self):
        return self.column.type is not None

    @property
    def type_v1(self) -> TypeV1:
        assert self.column.type
        return self.column.type

    @property
    def type_v3(self) -> TypeV3.Type:
        assert self.column.type
        match self.column.type:
            case TypeV1.BOOLEAN:
                return TypeV3.Type.BOOL
            case TypeV1.ANY:
                return TypeV3.Type.YSON
            case _:
                return TypeV3.Type(self.column.type)

    @property
    def is_required(self) -> bool:
        return bool(self.column.required) is True

    def ensure(self, v3_required: bool = False):
        if not self.is_active:
            return
        if self.is_required:
            if not v3_required:
                assert (
                    not self.type_v1 == TypeV1.ANY
                ), f"Column '{self.column.name}' with type '{str(TypeV1.ANY)}' cannot be required"

    def dump_v3(self) -> str | dict[str, Any] | None:
        if not self.is_active:
            return None
        if self.is_required:
            return str(self.type_v3)
        return {"type_name": "optional", "item": str(self.type_v3)}


@dataclass
class TypeV3Spec(TypeSpecBase):
    column: Column

    INTEGER: ClassVar[frozenset[TypeV3.Type]] = {
        TypeV3.Type.INT64,
        TypeV3.Type.INT32,
        TypeV3.Type.INT16,
        TypeV3.Type.INT8,
        TypeV3.Type.UINT64,
        TypeV3.Type.UINT32,
        TypeV3.Type.UINT16,
        TypeV3.Type.UINT8,
    }
    REAL: ClassVar[frozenset[TypeV3.Type]] = {
        TypeV3.Type.DOUBLE,
        TypeV3.Type.FLOAT,
    }
    NUMERIC: ClassVar[frozenset[TypeV3.Type]] = INTEGER | REAL
    BIG_NUMERIC: ClassVar[frozenset[TypeV3.Type]] = frozenset(
        {
            TypeV3.Type.INT64,
            TypeV3.Type.UINT64,
            TypeV3.Type.DOUBLE,
        }
    )
    HASHABLE_BYTES: ClassVar[frozenset[TypeV3.Type]] = frozenset(
        {
            TypeV3.Type.STRING,
            TypeV3.Type.UTF8,
            TypeV3.Type.UUID,
        }
    )
    BYTES: ClassVar[frozenset[TypeV3.Type]] = frozenset(
        {
            TypeV3.Type.STRING,
            TypeV3.Type.UTF8,
            TypeV3.Type.YSON,
            TypeV3.Type.JSON,
            TypeV3.Type.UUID,
        }
    )
    BOOL: ClassVar[frozenset[TypeV3.Type]] = frozenset({TypeV3.Type.BOOL})
    DATETIME: ClassVar[frozenset[TypeV3.Type]] = frozenset(
        {
            TypeV3.Type.DATE,
            TypeV3.Type.DATETIME,
            TypeV3.Type.TIMESTAMP,
            TypeV3.Type.INTERVAL,
        }
    )
    VOID: ClassVar[frozenset[TypeV3.Type]] = frozenset({TypeV3.Type.VOID})
    HASHABLE: ClassVar[frozenset[TypeV3.Type]] = NUMERIC | HASHABLE_BYTES | BOOL | DATETIME
    SIMPLE_TYPES: ClassVar[frozenset[TypeV3.Type]] = NUMERIC | BYTES | BOOL | DATETIME | VOID
    MATCHING_V1_TYPE: ClassVar[dict[TypeV3.Type, TypeV1]] = {
        TypeV3.Type.INT64: TypeV1.INT64,
        TypeV3.Type.INT32: TypeV1.INT32,
        TypeV3.Type.INT16: TypeV1.INT16,
        TypeV3.Type.INT8: TypeV1.INT8,
        TypeV3.Type.UINT64: TypeV1.UINT64,
        TypeV3.Type.UINT32: TypeV1.UINT32,
        TypeV3.Type.UINT16: TypeV1.UINT16,
        TypeV3.Type.UINT8: TypeV1.UINT8,
        TypeV3.Type.DOUBLE: TypeV1.DOUBLE,
        TypeV3.Type.FLOAT: TypeV1.FLOAT,
        TypeV3.Type.BOOL: TypeV1.BOOLEAN,
        TypeV3.Type.STRING: TypeV1.STRING,
        TypeV3.Type.UTF8: TypeV1.UTF8,
        TypeV3.Type.JSON: TypeV1.JSON,
        TypeV3.Type.UUID: TypeV1.UUID,
        TypeV3.Type.DATE: TypeV1.DATE,
        TypeV3.Type.DATETIME: TypeV1.DATETIME,
        TypeV3.Type.TIMESTAMP: TypeV1.TIMESTAMP,
        TypeV3.Type.INTERVAL: TypeV1.INTERVAL,
        TypeV3.Type.YSON: TypeV1.ANY,
        TypeV3.Type.DECIMAL: TypeV1.STRING,
        TypeV3.Type.LIST: TypeV1.ANY,
        TypeV3.Type.STRUCT: TypeV1.ANY,
        TypeV3.Type.TUPLE: TypeV1.ANY,
        TypeV3.Type.VARIANT: TypeV1.ANY,
        TypeV3.Type.DICT: TypeV1.ANY,
        TypeV3.Type.TAGGED: TypeV1.ANY,
        TypeV3.Type.VOID: TypeV1.VOID,
    }

    @property
    def is_active(self) -> bool:
        return self.column.type_v3 is not None

    @property
    def spec(self) -> TypeV3Union:
        assert self.column.type_v3
        return self.column.type_v3

    @property
    def is_optional(self) -> bool:
        return isinstance(self.spec, TypeV3.TypeItem) and self.spec.type_name == TypeV3.Type.OPTIONAL

    @property
    def type_v3(self) -> TypeV3.Type:
        def _extract(type_v3: TypeV3Union) -> TypeV3.Type:
            if isinstance(type_v3, TypeV3.Type):
                return type_v3
            else:
                return type_v3.type_name

        if self.is_optional:
            return _extract(self.spec.item)
        return _extract(self.spec)

    @property
    def type_v1(self) -> TypeV1:
        type_v3: TypeV3.Type = self.type_v3
        return self.MATCHING_V1_TYPE[type_v3]

    @property
    def is_required(self) -> bool:
        return not self.is_optional

    @classmethod
    def _ensure_simple_type(cls, type_v3: TypeV3.Type, name: str, parent_is_optional: bool):
        assert type_v3 in (
            cls.SIMPLE_TYPES
        ), f"Bad type for column '{name}': expected one of simple types, got '{str(type_v3)}'"
        if not parent_is_optional:
            assert type_v3 != TypeV3.Type.YSON, (
                f"Bad type for column '{name}': '{str(TypeV3.Type.YSON)}' outside of "
                + f"'{str(TypeV3.Type.OPTIONAL)}' is forbidden"
            )

    @classmethod
    def _ensure_decimal(cls, type_v3: TypeV3.TypeDecimal, name: str):
        assert type_v3.type_name == TypeV3.Type.DECIMAL, (
            f"Type name mismatch for column '{name}':"
            + f" expected '{str(TypeV3.Type.DECIMAL)}', got '{str(type_v3.type_name)}'"
        )
        assert (
            type_v3.precision >= 1 and type_v3.precision <= 35
        ), f"Bad decimal precision for column '{name}': expected range[1, 35], got {type_v3.precision}"
        max_scale: int = min(10, type_v3.precision)
        assert (
            type_v3.scale >= 0 and type_v3.scale <= max_scale
        ), f"Bad decimal scale for column '{name}': expected range[0, {max_scale}], got {type_v3.scale}"

    @classmethod
    def _ensure_item_type(cls, type_v3: TypeV3.TypeItem, name: str):
        assert type_v3.type_name in (
            TypeV3.Type.OPTIONAL,
            TypeV3.Type.LIST,
        ), (
            f"Bad type for column '{name}': expected oneof ('{str(TypeV3.Type.OPTIONAL)}', '{str(TypeV3.Type.LIST)}'), "
            + f"got '{str(type_v3.type_name)}'"
        )
        assert type_v3.item, f"Empty attribute 'item' for column '{name}', should be some type_v3 definition"
        cls._ensure_type(type_v3.item, name, parent_is_optional=(type_v3.type_name == TypeV3.Type.OPTIONAL))

    @classmethod
    def _ensure_tagged_type(cls, type_v3: TypeV3.TypeTagged, name: str):
        assert (
            type_v3.type_name == TypeV3.Type.TAGGED
        ), f"Bad 'type_name' for column '{name}': expected '{str(TypeV3.Type.TAGGED)}' got '{str(type_v3.type_name)}'"
        assert type_v3.tag, f"Empty attribute 'tag' for column '{name}'"
        assert type_v3.item, f"Empty attribute 'item' for column '{name}'"
        cls._ensure_type(type_v3.item, name)

    @classmethod
    def _ensure_dict_type(cls, type_v3: TypeV3.TypeDict, name: str):
        assert (
            type_v3.type_name == TypeV3.Type.DICT
        ), f"Bad 'type_name' for column '{name}': expected '{str(TypeV3.Type.DICT)}', got '{str(type_v3.type_name)}'"
        assert (
            type_v3.key in TypeV3Spec.HASHABLE
        ), f"Bad 'key' for column '{name}': expected simple hashable type, got '{str(type_v3.key)}'"
        assert type_v3.value, f"Empty 'value' for column '{name}'"
        cls._ensure_type(type_v3.value, name)

    @classmethod
    def _ensure_container_type(cls, type_v3: TypeV3.TypeMembersOrElements, name: str):
        assert type_v3.type_name in (
            TypeV3.Type.TUPLE,
            TypeV3.Type.VARIANT,
            TypeV3.Type.STRUCT,
        ), (
            f"Bad column '{name}' type: expected one of "
            + f"('{str(TypeV3.Type.TUPLE)}', '{str(TypeV3.Type.VARIANT)}', '{str(TypeV3.Type.STRUCT)}'), "
            + f"got '{str(type_v3.type_name)}'"
        )
        name_required: bool = False
        if type_v3.type_name == TypeV3.Type.TUPLE:
            assert type_v3.elements, f"Empty attribute 'elements' for column '{name}'"
            assert not type_v3.members, f"Attribute 'members' for column '{name}' not allowed"
        elif type_v3.type_name == TypeV3.Type.STRUCT:
            assert not type_v3.elements, f"Attribute 'elements' for column '{name}' not allowed"
            assert type_v3.members, f"Empty attribute 'members' for column '{name}"
            name_required = True
        elif type_v3.type_name == TypeV3.Type.VARIANT:
            if type_v3.members and type_v3.elements:
                assert f"Attributes 'elements' and 'members' for column '{name}' can not be set simultaneously"
            if type_v3.members:
                name_required = True
                assert not type_v3.elements, f"Attribute 'elements' for column '{name}' not allowed when 'members' set"
            if type_v3.elements:
                name_required = False
                assert not type_v3.members, f"Attribute 'members' for column '{name}' not allowed when 'elements' set"
        for item in type_v3.elements + type_v3.members:
            if name_required:
                assert item.name, f"Empty 'name' not allowed for column '{name}'"
            else:
                assert not item.name, f"Attribute 'name' not allowed for column '{name}'"
            cls._ensure_type(item.type, name)

    @classmethod
    def _ensure_type(cls, type_v3: TypeV3Union, name: str, parent_is_optional: bool = False):
        if isinstance(type_v3, TypeV3.Type):
            cls._ensure_simple_type(type_v3, name, parent_is_optional)
        elif isinstance(type_v3, TypeV3.TypeDecimal):
            cls._ensure_decimal(type_v3, name)
        elif isinstance(type_v3, TypeV3.TypeItem):
            cls._ensure_item_type(type_v3, name)
        elif isinstance(type_v3, TypeV3.TypeTagged):
            cls._ensure_tagged_type(type_v3, name)
        elif isinstance(type_v3, TypeV3.TypeDict):
            cls._ensure_dict_type(type_v3, name)
        elif isinstance(type_v3, TypeV3.TypeMembersOrElements):
            cls._ensure_container_type(type_v3, name)
        else:
            # Should never happen
            raise ValueError(f"Unknown type {type(type_v3)}")

    def _ensure_v1_type(self):
        if not self.column.type:
            return
        if self.column.required is not None:
            assert self.column.required == self.is_required, (
                f"Attribute 'required' mismatch for type_v3 for column '{self.column.name}': "
                + f"type_v1={self.column.required}, type_v3={self.is_required}"
            )
        type_v3: TypeV3.Type = self.type_v3
        type_v1: TypeV1 = self.type_v1
        assert self.column.type == type_v1, (
            f"Attributes 'type' and 'type_v3' for column '{self.column.name}' mismatch: "
            + f"type='{str(type_v1)}', type_v3='{str(type_v3)}'"
        )

    def ensure(self):
        if not self.is_active:
            return
        self._ensure_type(self.spec, self.column.name)
        self._ensure_v1_type()

    def dump(self) -> str | dict[str, Any] | None:
        def _convert(d: dict[str, Any]):
            result: dict[str, Any] = dict()
            for k, v in d.items():
                if v is None:
                    continue
                elif isinstance(v, list):
                    if len(v) == 0:
                        continue
                    result[k] = [_convert(i) for i in v]
                elif isinstance(v, TypeV3.Type):
                    result[k] = str(v)
                elif isinstance(v, Mapping):
                    tmp = _convert(v)
                    if tmp:
                        result[k] = tmp
                else:
                    result[k] = v
            return result

        if not self.is_active:
            return None

        if isinstance(self.column.type_v3, TypeV3.Type):
            return str(self.column.type_v3)

        return _convert(asdict(self.column.type_v3))


class SchemaSpec:
    MAX_LITERAL_LEN: ClassVar[int] = 256
    AGGREGATE_ALLOWED_TYPES: ClassVar[dict[Column.AggregateFunc, frozenset[TypeV1]]] = {
        Column.AggregateFunc.SUM: TypeV1Spec.BIG_NUMERIC,
        Column.AggregateFunc.MIN: TypeV1Spec.BIG_NUMERIC | TypeV1Spec.BOOL,
        Column.AggregateFunc.MAX: TypeV1Spec.BIG_NUMERIC | TypeV1Spec.BOOL,
        Column.AggregateFunc.FIRST: TypeV1Spec.BIG_NUMERIC | TypeV1Spec.BOOL,
        Column.AggregateFunc.XDELTA: frozenset({TypeV1.STRING}),
        Column.AggregateFunc.DICT_SUM: frozenset({TypeV1.ANY}),
    }
    ALLOWED_NESTED_AGGREGATES: ClassVar[frozenset[Column.AggregateFunc]] = frozenset(
        {
            Column.AggregateFunc.SUM,
            Column.AggregateFunc.MAX,
        }
    )

    @classmethod
    def is_sorted(cls, schema: list[Column]) -> bool:
        return bool(schema and schema[0].sort_order is not None)

    @staticmethod
    def _normalize(data: str) -> str:
        return data.replace(" ", "")

    @classmethod
    def _ensure_name(cls, column: Column, seen_names: set[str]):
        assert column.name, "Column name must be specified"
        assert len(column.name) < cls.MAX_LITERAL_LEN, f"Column name '{column.name}' is too long"
        assert column.name not in seen_names, f"Not unique column name '{column.name}'"
        seen_names.add(column.name)

    @classmethod
    def _ensure_sort_order(cls, column: Column, num: int, schema: list[Column]):
        def _check(type_spec: TypeSpecBase):
            if not type_spec.is_active:
                return
            assert (
                type_spec.type_v1 not in TypeV1Spec.NON_COMPARABLE
            ), f"Key column '{column.name}' can not be of type {TypeV1Spec.NON_COMPARABLE}"

        _check(TypeV1Spec(column))
        _check(TypeV3Spec(column))

        if num > 0:
            previous = schema[num - 1]
            assert previous.sort_order, f"Key column '{column.name}' cannot follow non-key column '{previous.name}'"

    @classmethod
    def _ensure_expression(cls, column: Column):
        def _check_type(type_spec: TypeSpecBase, expected_type: TypeV1):
            if not type_spec.is_active:
                return
            assert (
                type_spec.type_v1 == expected_type
            ), f"Only type={str(expected_type)} allowed for columns with expression"

        assert column.expression, f"Attribute 'expression' for column '{column.name}' cannot be empty"
        assert column.sort_order, f"Computed column '{column.name}' must be key column"
        assert not column.required, f"Computed column '{column.name}' can not be required"

        type_v1 = TypeV1Spec(column)
        type_v3 = TypeV3Spec(column)

        if (type_v1.is_active and type_v1.is_required) or (type_v3.is_active and type_v3.is_required):
            assert False, f"Computed column '{column.name}' can not be required"

        normalized_expression = cls._normalize(column.expression)
        if normalized_expression.startswith("int64(") and normalized_expression.endswith(")"):
            _check_type(type_v1, TypeV1.INT64)
            _check_type(type_v3, TypeV1.INT64)
        else:
            _check_type(type_v1, TypeV1.UINT64)
            _check_type(type_v3, TypeV1.UINT64)

    @classmethod
    def _ensure_lock(cls, column: Column):
        assert column.lock, f"Attribute 'lock' for column '{column.name}' cannot be empty"
        assert len(column.lock) < cls.MAX_LITERAL_LEN, f"Column '{column.name}' lock '{column.lock}' is too long"
        assert not column.sort_order, f"Key column '{column.name}' can't have any lock, got lock '{column.lock}'"

    @classmethod
    def _ensure_group(cls, column: Column):
        assert column.group, f"Attribute 'group' for column '{column.name}' cannot be empty"
        assert len(column.group) < cls.MAX_LITERAL_LEN, f"Column '{column.name}' group '{column.group}' is too long"

    @classmethod
    def _ensure_aggregate(cls, column: Column):
        def _check_nested_type(type_spec: TypeSpecBase):
            if not type_spec.is_active:
                return
            assert (
                type_spec.type_v1 == TypeV1.ANY
            ), f"Column '{column.name}' type must be '{str(TypeV1.ANY)}' for nested aggregate functions"

        def _check_allowed_type(type_spec: TypeSpecBase, aggr_func: str):
            if not type_spec.is_active:
                return
            assert type_spec.type_v1 in cls.AGGREGATE_ALLOWED_TYPES[aggr_func], (
                f"Aggregate column '{column.name}' has inappropriate type '{str(type_spec.type_v1)}' "
                + f"for aggregate function '{aggr_func}'"
            )

        assert column.aggregate, f"Empty aggregate function not allowed for column {column.name}"
        assert not column.sort_order, f"Aggegate column '{column.name}' cannot be key column"

        type_v1 = TypeV1Spec(column)
        type_v3 = TypeV3Spec(column)

        aggr_func: str = column.aggregate
        if aggr_func.startswith("nested_key(") or aggr_func.startswith("nested_value("):
            assert (
                type_v3.is_active
            ), f"Attribute 'type_v3' must be defined for column '{column.name}' with nested aggregate function"
            _check_nested_type(type_v1)
            _check_nested_type(type_v3)

            nested_func: str = cls._normalize(aggr_func)
            assert nested_func.count("(") == 1, f"Bad nested aggregate function '{aggr_func}' for column '{column.name}"
            assert nested_func.count(")") == 1, f"Bad nested aggregate function '{aggr_func}' for column '{column.name}"
            assert nested_func.find(")") > nested_func.find(
                "()"
            ), f"Bad nested aggregate function '{aggr_func}' for column '{column.name}"
            nested_exression: str = nested_func[nested_func.find("(") + 1 : nested_func.find(")")]  # noqa:E203
            parts = nested_exression.split(",")
            assert parts[0], f"Bad nested aggregate function '{aggr_func}' for column '{column.name}'"
            if aggr_func.startswith("nested_key("):
                assert (
                    len(parts) == 1
                ), f"Bad nested_key() expression in function '{aggr_func}' for column '{column.name}'"
            else:
                assert len(parts) in (
                    1,
                    2,
                ), f"Bad nested_value() expression in function '{aggr_func}' for column '{column.name}'"
                if len(parts) == 2:
                    assert (
                        parts[1] in cls.ALLOWED_NESTED_AGGREGATES
                    ), f"Unsupported nested aggregate function in '{aggr_func}' for column '{column.name}'"
        elif aggr_func in Column.AggregateFunc:
            _check_allowed_type(type_v1, aggr_func)
            _check_allowed_type(type_v3, aggr_func)
        else:
            raise ValueError(f"Unkown aggregate function '{aggr_func}' for column '{column.name}'")

    @classmethod
    def _ensure_max_inline_hunk_size(cls, column: Column):
        def _check(type_spec: TypeSpecBase):
            if not type_spec.is_active:
                return
            assert (
                type_spec.type_v1 in TypeV1Spec.BYTES
            ), f"Column '{column.name}' type must be in {TypeV1Spec.BYTES} for 'max_inline_hunk_size' attribute"

        assert column.max_inline_hunk_size >= 0, (
            f"Attribute 'max_inline_hunk_size' must be positive for column '{column.name}',"
            + f" got {column.max_inline_hunk_size}"
        )
        assert not column.sort_order, f"Attribute 'max_inline_hunk_size' cannot be set for key column '{column.name}'"
        assert (
            not column.aggregate
        ), f"Attribute 'max_inline_hunk_size' cannot be set for aggregate column '{column.name}'"

        _check(TypeV1Spec(column))
        _check(TypeV3Spec(column))

    @classmethod
    def ensure(cls, schema: list[Column] | None):
        assert schema, "Empty or absent schema not allowed"
        key_columns: int = 0
        computed_columns: int = 0
        value_columns: int = 0
        seen_names: set[str] = set()
        for i, column in enumerate(schema):
            cls._ensure_name(column, seen_names)

            assert (
                column.type or column.type_v3
            ), f"Column '{column.name}' type must be specified via 'type' or 'type_v3' attribute"

            v3_spec = TypeV3Spec(column)
            v3_spec.ensure()
            v3_required = v3_spec.is_required if v3_spec.is_active else False
            v1_spec = TypeV1Spec(column)
            v1_spec.ensure(v3_required)

            if v1_spec.is_active and v3_spec.is_active:
                assert bool(column.required) == bool(
                    v3_spec.is_required
                ), f"Column '{column.name}' type and type_v3 'required' property mismatch"

            if column.required:
                if not v3_required:
                    assert column.type != TypeV1.ANY, f"Column '{column.name}' with type 'any' cannot be required"

            if column.sort_order:
                key_columns += 1
                cls._ensure_sort_order(column, i, schema)
            else:
                value_columns += 1

            if column.expression is not None:
                computed_columns += 1
                cls._ensure_expression(column)

            if column.lock is not None:
                cls._ensure_lock(column)

            if column.group is not None:
                cls._ensure_group(column)

            if column.aggregate is not None:
                cls._ensure_aggregate(column)

            if column.max_inline_hunk_size is not None:
                cls._ensure_max_inline_hunk_size(column)

        assert value_columns > 0, "There must be at least one non-key column"
        if key_columns > 0:
            assert key_columns > computed_columns, "Key columns count less than computed"

    @classmethod
    def parse(cls, raw: list[dict[str, Any]], with_ensure: bool = True) -> list[Column]:
        @dataclass
        class _dummy:
            schema: list[Column]

        assert isinstance(raw, List), f"list-like object expected as raw data, got {type(raw)} = {raw}"
        try:
            parsed = from_dict(data_class=_dummy, data={"schema": raw}, config=_SCHEMA_PARSE_CONFIG)
            if with_ensure:
                cls.ensure(parsed.schema)
            return parsed.schema
        except UnionMatchError as e:
            raise ValueError(e)
