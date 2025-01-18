from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from typing import Any
from typing import Generator
from typing import Iterable
from typing import Union

from yt.yson.yson_types import YsonEntity
from yt.yt_sync.core.spec import Column
from yt.yt_sync.core.spec import Table
from yt.yt_sync.core.spec.details import SchemaSpec

from .helpers import get_folder
from .helpers import get_node_name
from .helpers import get_rtt_enabled_from_attrs
from .helpers import iter_attributes_recursively
from .helpers import make_log_name
from .replica import YtReplica
from .schema import YtSchema
from .table_attributes import YtTableAttributes
from .tablet_info import YtTabletInfo
from .tablet_state import YtTabletState
from .types import Types


@dataclass
class QueueOptions:
    is_chaos: bool = False
    is_replicated: bool = False
    commit_ordering: str | None = None
    has_commit_ordering: bool = False
    preserve_tablet_index: bool | None = None
    has_preserve_tablet_index: bool = False

    def make_updated(self, attrs: dict[str, Any]) -> QueueOptions:
        result = QueueOptions(
            is_chaos=self.is_chaos,
            is_replicated=self.is_replicated,
            commit_ordering=self.commit_ordering,
            has_commit_ordering=self.has_commit_ordering,
            preserve_tablet_index=self.preserve_tablet_index,
            has_preserve_tablet_index=self.has_preserve_tablet_index,
        )
        if "commit_ordering" in attrs:
            result.commit_ordering = attrs["commit_ordering"]
            result.has_commit_ordering = True
        if "preserve_tablet_index" in attrs:
            result.preserve_tablet_index = attrs["preserve_tablet_index"]
            result.has_preserve_tablet_index = True
        return result


@dataclass
class RttOptions:
    enabled: bool = False
    preferred_sync_replica_clusters: set[str] | None = None
    min_sync_replica_count: int | None = None
    max_sync_replica_count: int | None = None
    attributes: dict[str, Any] | None = None
    explicit_keys: set[str] = dataclass_field(default_factory=set())

    @classmethod
    def make(cls, options: Types.Attributes, explicit_rtt_enabled: bool | None = None) -> RttOptions:
        options_copy = deepcopy(options)
        keys = set(options_copy.keys())
        enabled = bool(get_rtt_enabled_from_attrs(options_copy))
        preferred_sync: Iterable[str] | None = options_copy.pop("preferred_sync_replica_clusters", None)
        min_count: int | None = options_copy.pop("min_sync_replica_count", None)
        max_count: int | None = options_copy.pop("max_sync_replica_count", None)
        sync_count: int | None = options_copy.pop("sync_replica_count", None)
        return cls(
            enabled=explicit_rtt_enabled if explicit_rtt_enabled is not None else enabled,
            preferred_sync_replica_clusters=set(preferred_sync) if preferred_sync else None,
            min_sync_replica_count=min_count,
            max_sync_replica_count=sync_count or max_count,
            attributes=options_copy,
            explicit_keys=keys,
        )

    def clear(self):
        self.enabled = False
        self.preferred_sync_replica_clusters = None
        self.min_sync_replica_count = None
        self.max_sync_replica_count = None
        self.attributes.clear()
        self.explicit_keys.clear()

    def has_diff_with(self, other: RttOptions) -> bool:
        return next(iter(self.changed_attributes(other)), None) is not None

    def changed_attributes(self, other: RttOptions) -> Generator[tuple[str, Any | None, Any | None], None, None]:
        for path, desired, actual in iter_attributes_recursively(
            self.yt_attributes, other.yt_attributes, path="replicated_table_options"
        ):
            if desired != actual:
                yield (path, desired, actual)

    @property
    def yt_attributes(self) -> Types.Attributes:
        result = deepcopy(self.attributes)
        result["enable_replicated_table_tracker"] = self.enabled

        if (
            self.preferred_sync_replica_clusters is None and "preferred_sync_replica_clusters" in self.explicit_keys
        ) or self.preferred_sync_replica_clusters is not None:
            result["preferred_sync_replica_clusters"] = (
                sorted(self.preferred_sync_replica_clusters) if self.preferred_sync_replica_clusters else None
            )

        if (
            self.min_sync_replica_count is None and "min_sync_replica_count" in self.explicit_keys
        ) or self.min_sync_replica_count is not None:
            result["min_sync_replica_count"] = self.min_sync_replica_count

        if (
            self.max_sync_replica_count is None
            and ("max_sync_replica_count" in self.explicit_keys or "sync_replica_count" in self.explicit_keys)
        ) or self.max_sync_replica_count is not None:
            result["max_sync_replica_count"] = self.max_sync_replica_count
        return result


@dataclass
class YtTable:
    Type = Table.Type

    key: str
    cluster_name: str
    path: str
    name: str
    table_type: str
    exists: bool
    schema: YtSchema
    tablet_info: YtTabletInfo
    attributes: YtTableAttributes
    replicas: dict[Types.ReplicaKey, YtReplica] = dataclass_field(default_factory=dict)
    in_collocation: bool = False
    tablet_state: YtTabletState = dataclass_field(default_factory=YtTabletState)
    rtt_options: RttOptions | None = None
    replication_collocation_id: str | None = None
    chaos_replication_card_id: str | None = None
    chaos_data_table: str | None = None
    chaos_replication_log: str | None = None
    is_temporary: bool = False
    # 'total_row_count' property for each tablet in ordered table.
    # Filled on demand in scenario with ReadTotalRowCountAction on freezed state.
    total_row_count: list[int] | None = None
    # replication_progress property of chaos replica table
    # Filled on demand in scenarios with ReadReplicationProgressAction on unmounted state.
    chaos_replication_progress: Any | None = None
    # Explicitly affects RemountTableAction, if it will be executed or not.
    need_remount: bool | None = None

    @property
    def rich_path(self) -> str:
        return f"{self.cluster_name}:{self.path}"

    @property
    def folder(self) -> str:
        folder_path = get_folder(self.path)
        assert folder_path
        return folder_path

    @property
    def chaos_replica_content_type(self):
        if self.Type.TABLE == self.table_type:
            if self.is_ordered:
                return YtReplica.ContentType.QUEUE
            else:
                return YtReplica.ContentType.DATA
        if self.Type.REPLICATION_LOG == self.table_type:
            return YtReplica.ContentType.QUEUE
        raise NotImplementedError(f"Not supported for type '{self.table_type}'")

    @property
    def is_rtt_enabled(self) -> bool:
        if not self.is_replicated:
            return False
        if self.rtt_options is None:
            return False
        return self.rtt_options.enabled

    @is_rtt_enabled.setter
    def is_rtt_enabled(self, value: bool):
        if not self.is_replicated:
            return
        assert self.rtt_options is not None
        self.rtt_options.enabled = value

    def fill_replicas(self, attrs: Union[Types.Attributes, YsonEntity, None]):
        if not self.is_replicated:
            return
        if not attrs or isinstance(attrs, YsonEntity):
            return
        for replica_id, replica_attrs in attrs.items():
            replica = YtReplica.make(replica_id, replica_attrs)
            self.replicas[replica.key] = replica

    def add_replica(self, replica: YtReplica):
        self.replicas[replica.key] = deepcopy(replica)

    def add_replica_for(self, table: YtTable, cluster: "YtCluster", mode: YtReplica.Mode):  # noqa:F821
        assert self.is_replicated, f"Can't add replica {table.rich_path} for not replicated table {self.rich_path}"
        assert not table.is_replicated, f"Can't add replicated table {table.rich_path} as replica to {self.rich_path}"
        assert not self.rich_path == table.rich_path, f"Can't be replica of self {self.rich_path}"
        is_chaos = self.is_chaos_replicated
        data_replica = YtReplica(
            replica_id=None,
            cluster_name=table.cluster_name,
            replica_path=table.path,
            enabled=bool(table.attributes.get("enabled", True)),
            mode=mode,
            enable_replicated_table_tracker=table.rtt_options.enabled,
            content_type=table.chaos_replica_content_type if is_chaos else None,
            is_temporary=table.is_temporary,
        )
        table.rtt_options.enabled = False  # RTT state stored in replicated tables and replicas
        self.add_replica(data_replica)

        if is_chaos and YtTable.Type.TABLE == table.table_type:
            log_key = make_log_name(table.key)
            if log_key in cluster.tables:
                self.add_replica_for(cluster.tables[log_key], cluster, mode)

    def get_replicas_for(self, cluster_name: str) -> list[YtReplica]:
        result: list[YtReplica] = list()
        for replica in self.replicas.values():
            if cluster_name == replica.cluster_name:
                result.append(replica)
        return result

    def sync_replicas_mode(self, sync_clusters: set[str]):
        assert self.is_replicated, f"Can't set replicas mode for non-replicated table {self.rich_path}"
        preferred_sync_replicas: set[str] = set()
        rtt_enabled = False
        for replica in self.replicas.values():
            if replica.enable_replicated_table_tracker:
                rtt_enabled = True
            if replica.cluster_name in sync_clusters:
                replica.mode = YtReplica.Mode.SYNC
                if replica.enable_replicated_table_tracker:
                    preferred_sync_replicas.add(replica.cluster_name)
            else:
                replica.mode = YtReplica.Mode.ASYNC
        if self.Type.REPLICATED_TABLE and rtt_enabled:
            self.rtt_options.preferred_sync_replica_clusters = preferred_sync_replicas

    @property
    def sync_replicas(self) -> set[str]:
        if not self.is_replicated:
            return set()
        sync_replicas: set[str] = set()
        for replica in self.replicas.values():
            if YtReplica.ContentType.QUEUE == replica.content_type and not self.is_ordered:
                continue
            if YtReplica.Mode.SYNC == replica.mode:
                sync_replicas.add(replica.cluster_name)
        return sync_replicas

    @property
    def yt_attributes(self) -> Types.Attributes:
        result = self.attributes.yt_attributes
        self._fill_spec(result)
        return result

    def get_full_spec(self) -> Types.Attributes:
        # Same as self.yt_attributes but with non-filtered attributes and tablet_count/pivot_keys.
        result = deepcopy(self.attributes.attributes)
        self._fill_spec(result)
        if self.is_chaos_replicated:
            return result
        if self.is_ordered:
            if self.tablet_count:
                result["tablet_count"] = self.tablet_count
        elif self.has_tablets:
            if self.has_pivot_keys:
                result["pivot_keys"] = self.pivot_keys
            else:
                result["tablet_count"] = self.tablet_count
            return result

        return result

    def _fill_spec(self, attributes: Types.Attributes):
        attributes["schema"] = self.schema.yt_schema
        if self.is_chaos_replicated:
            attributes.pop("dynamic", None)
        if self.is_replicated:
            attributes["replicated_table_options"] = dict()
            if self.is_rtt_enabled:
                rtt_options: Types.Attributes = self.rtt_options.yt_attributes
                sync_replicas: set[str] = self.preferred_sync_replicas
                # override explicit
                rtt_options["preferred_sync_replica_clusters"] = sorted(sync_replicas) if sync_replicas else None
                attributes["replicated_table_options"] = rtt_options
            if self.replication_collocation_id:
                attributes["replication_collocation_id"] = self.replication_collocation_id

    @property
    def replica_type(self) -> str:
        assert self.is_replicated, f"Not replicated table {self.rich_path}"
        if self.Type.CHAOS_REPLICATED_TABLE == self.table_type:
            return YtReplica.ReplicaType.CHAOS_TABLE_REPLICA
        else:
            return YtReplica.ReplicaType.TABLE_REPLICA

    @property
    def replica_key(self) -> Types.ReplicaKey:
        return YtReplica.make_key(self.cluster_name, self.path)

    @property
    def pivot_keys(self) -> list[Any] | None:
        return self.tablet_info.pivot_keys

    @property
    def has_pivot_keys(self) -> bool:
        return self.tablet_info.has_pivot_keys

    @property
    def tablet_count(self) -> int | None:
        return self.tablet_info.tablet_count

    @property
    def effective_tablet_count(self) -> int:
        if self.is_ordered:
            return self.tablet_count or 1
        if self.has_pivot_keys:
            return len(self.pivot_keys)
        return self.tablet_count or 1

    @property
    def has_tablets(self) -> bool:
        return self.tablet_info.has_tablets

    @property
    def is_in_memory(self) -> bool:
        return self.attributes.get("in_memory_mode") in ("compressed", "uncompressed")

    @property
    def has_hunks(self) -> bool:
        return self.schema.has_hunks

    @property
    def rtt_enabled_replicas(self) -> Generator[YtReplica, None, None]:
        if self.is_replicated:
            for replica in self.replicas.values():
                if replica.enable_replicated_table_tracker:
                    yield replica

    @property
    def preferred_sync_replicas(self) -> set[str]:
        if self.is_ordered:
            return set([r.cluster_name for r in self.rtt_enabled_replicas if YtReplica.Mode.SYNC == r.mode])
        else:
            return set(
                [
                    r.cluster_name
                    for r in self.rtt_enabled_replicas
                    if YtReplica.Mode.SYNC == r.mode and YtReplica.ContentType.QUEUE != r.content_type
                ]
            )

    @property
    def has_rtt_enabled_replicas(self) -> bool:
        if not self.is_replicated:
            return False
        return next(self.rtt_enabled_replicas, None) is not None

    @property
    def effective_max_sync_replica_count(self) -> int:
        assert self.is_replicated, f"Table {self.rich_path} is not replicated"
        # Copy of https://a.yandex-team.ru/arcadia/yt/yt/client/tablet_client/config.h?rev=r14016270#L107
        if self.rtt_options.min_sync_replica_count is None and self.rtt_options.max_sync_replica_count is None:
            return 1
        if self.rtt_options.max_sync_replica_count is not None:
            return self.rtt_options.max_sync_replica_count
        return len(self.replicas)

    @property
    def min_sync_replica_count(self) -> int:
        if not self.is_rtt_enabled:
            return 0
        return self.rtt_options.min_sync_replica_count if self.rtt_options.min_sync_replica_count is not None else 1

    @property
    def max_sync_replica_count(self) -> int:
        if not self.is_rtt_enabled:
            return 0
        return self.rtt_options.max_sync_replica_count if self.rtt_options.max_sync_replica_count is not None else 1

    @property
    def is_ordered(self) -> bool:
        return self.schema.is_ordered

    @property
    def is_consumer(self) -> bool:
        return bool(self.attributes.get("treat_as_queue_consumer", False))

    @property
    def is_data_table(self) -> bool:
        return not self.is_ordered

    @property
    def is_mountable(self) -> bool:
        return self.table_type != self.Type.CHAOS_REPLICATED_TABLE

    def set_rtt_enabled_from_replicas(self):
        if self.has_rtt_enabled_replicas:
            self.is_rtt_enabled = True

    @classmethod
    def make(
        cls,
        key: str,
        cluster_name: str,
        table_type: str,
        path: str,
        exists: bool,
        attributes: Types.Attributes,
        source_attributes: YtTableAttributes | None = None,
        explicit_schema: list[Column] | None = None,
        explicit_in_collocation: bool | None = None,
        ensure_empty_schema: bool = True,
        queue_options: QueueOptions | None = None,
    ) -> YtTable:
        assert table_type in cls.Type.all(), f"Unknown type '{table_type}' for table at {cluster_name}:{path}"

        copied = deepcopy(attributes)
        schema_from_attributes = copied.pop("schema", list())
        schema: list[Column] = explicit_schema or SchemaSpec.parse(
            YtSchema.preprocessed_schema(schema_from_attributes), False
        )
        strict_schema: bool = (
            YtSchema.is_strict(explicit_schema) if explicit_schema else YtSchema.is_strict(schema_from_attributes)
        )
        ensure_schema = exists
        if not ensure_empty_schema and not schema:
            ensure_schema = False
        yt_schema = YtSchema.make(schema, strict_schema, ensure_schema)

        pivot_keys = copied.pop("pivot_keys", None)

        if exists and "dynamic" not in copied:
            copied["dynamic"] = True

        name = get_node_name(path)
        replicas = copied.pop("replicas", None)
        tablet_count = copied.pop("tablet_count", None)

        in_collocation_from_attributes = copied.pop("in_collocation", False)
        in_collocation = (
            explicit_in_collocation if explicit_in_collocation is not None else in_collocation_from_attributes
        )
        in_collocation = in_collocation if cls.is_replicated_type(table_type) else False

        replication_collocation_id = copied.pop("replication_collocation_id", None)
        replication_collocation_id = replication_collocation_id if cls.is_replicated_type(table_type) else None

        chaos_replication_card_id = copied.pop("replication_card_id", None)
        chaos_replication_card_id = chaos_replication_card_id if cls.Type.CHAOS_REPLICATED_TABLE == table_type else None

        is_rtt_enabled = get_rtt_enabled_from_attrs(copied)
        rtt_options = copied.pop("replicated_table_options", {})

        result = cls(
            key=key,
            cluster_name=cluster_name,
            path=path,
            name=name,
            table_type=table_type,
            exists=exists,
            schema=yt_schema,
            tablet_info=YtTabletInfo.make(tablet_count, pivot_keys, copied.get("tablet_balancer_config", dict())),
            attributes=YtTableAttributes.make(copied, source_attributes),
            in_collocation=in_collocation,
            tablet_state=YtTabletState(YtTabletState.MOUNTED),
            rtt_options=RttOptions.make(rtt_options, is_rtt_enabled),
            replication_collocation_id=replication_collocation_id,
            chaos_replication_card_id=chaos_replication_card_id,
        )
        if exists:
            result.fill_replicas(replicas)

        # implicitly set replicated queue attributes if needed
        if queue_options is not None and queue_options.is_replicated and result.is_ordered:
            if not result.attributes.has_value("commit_ordering"):
                if queue_options.has_commit_ordering:
                    result.attributes.set_value("commit_ordering", queue_options.commit_ordering)
                else:
                    result.attributes.set_value("commit_ordering", "strong")
            if queue_options.is_chaos or result.is_replicated:
                if not result.attributes.has_value("preserve_tablet_index"):
                    if queue_options.has_preserve_tablet_index:
                        result.attributes.set_value("preserve_tablet_index", queue_options.preserve_tablet_index)
                    else:
                        result.attributes.set_value("preserve_tablet_index", True)
        return result

    @classmethod
    def resolve_table_type(cls, table_type: YtTable.Type, is_chaos: bool) -> YtTable.Type:
        if cls.Type.REPLICATED_TABLE == table_type and is_chaos:
            return cls.Type.CHAOS_REPLICATED_TABLE
        return table_type

    @staticmethod
    def resolve_replication_log_attributes(attributes: Types.Attributes) -> Types.Attributes:
        log_attributes: dict[str, Any] = deepcopy(attributes.pop("replication_log", {}))
        for k in ("tablet_cell_bundle", "primary_medium", "schema", "dynamic", "enable_replicated_table_tracker"):
            v = attributes.get(k)
            if v is not None:
                log_attributes.setdefault(k, deepcopy(v))
        return log_attributes

    @classmethod
    def is_replicated_type(cls, table_type: YtTable.Type) -> bool:
        return table_type in (cls.Type.REPLICATED_TABLE, cls.Type.CHAOS_REPLICATED_TABLE)

    @property
    def is_replicated(self) -> bool:
        return self.is_replicated_type(self.table_type)

    @property
    def is_chaos_replicated(self) -> bool:
        return self.Type.CHAOS_REPLICATED_TABLE == self.table_type

    @property
    def is_chaos_replication_log_required(self) -> bool:
        return self.Type.TABLE == self.table_type and not self.is_ordered

    def request_opaque_attrs(self) -> list[str]:
        attrs: list[str] = list()
        if self.is_replicated and not self.replicas:
            attrs.append("replicas")
        if not self.is_chaos_replicated:
            attrs.append("pivot_keys")
            attrs.append("tablet_state")
        return attrs

    def apply_opaque_attribute(self, attr_name: str, attr_value: Any):
        if "replicas" == attr_name:
            self.fill_replicas(dict(attr_value))
        elif "pivot_keys" == attr_name:
            self.tablet_info.pivot_keys = list(attr_value)
        elif "tablet_state" == attr_name:
            self.tablet_state.set(attr_value)

    def to_data_table(self):
        if not self.is_replicated:
            return
        self.table_type = YtTable.Type.TABLE
        self.replicas.clear()
        self.rtt_options.clear()

    def sync_user_attributes(self, other: YtTable):
        added_attrs: set[str] = set()
        for key in other.attributes.user_attribute_keys - self.attributes.user_attribute_keys:
            value = other.attributes.get_filtered(key)
            if value is not None:
                added_attrs.add(key)
                self.attributes.set_value(key, value)
        self.attributes.user_attribute_keys = frozenset(self.attributes.user_attribute_keys | added_attrs)

    def link_replication_log(self, replication_log: YtTable):
        assert replication_log.table_type == self.Type.REPLICATION_LOG
        assert self.table_type == YtTable.Type.TABLE
        assert replication_log.key == make_log_name(self.key)

        replication_log.chaos_data_table = self.key
        self.chaos_replication_log = replication_log.key
