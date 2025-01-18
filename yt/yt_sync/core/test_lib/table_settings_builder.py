from __future__ import annotations

from copy import deepcopy
from typing import ClassVar

from library.python.confmerge import merge_copy
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtCluster
from yt.yt_sync.core.spec import ClusterTable
from yt.yt_sync.core.spec import ReplicationLog
from yt.yt_sync.core.spec import Table
from yt.yt_sync.core.spec.details import SchemaSpec


class TableSettingsBuilder:
    _ALL: ClassVar[str] = "_all_"

    def __init__(self, table_path: str, use_chaos: bool = False):
        self.path: str = table_path
        self.use_chaos: bool = use_chaos
        self._schema: Types.Schema | None = None
        self._clusters: dict[str, tuple[YtCluster.Type, YtCluster.Mode | None]] = dict()
        self._attributes: dict[str, list[Types.Attributes]] = dict()
        self._replica_paths: dict[str, str] = dict()

    def with_schema(self, schema: Types.Schema) -> TableSettingsBuilder:
        self._schema = schema
        return self

    def with_main(self, cluster: str) -> TableSettingsBuilder:
        self._clusters[cluster] = (YtCluster.Type.MAIN, None)
        return self

    def with_sync_replica(self, cluster: str, replica_path: str | None = None) -> TableSettingsBuilder:
        self._clusters[cluster] = (YtCluster.Type.REPLICA, YtCluster.Mode.SYNC)
        if replica_path:
            self._replica_paths[cluster] = replica_path
        return self

    def with_async_replica(self, cluster: str, replica_path: str | None = None) -> TableSettingsBuilder:
        self._clusters[cluster] = (YtCluster.Type.REPLICA, YtCluster.Mode.ASYNC)
        if replica_path:
            self._replica_paths[cluster] = replica_path
        return self

    def with_attributes(self, attrs: Types.Attributes, cluster: str | None = None) -> TableSettingsBuilder:
        if not cluster:
            self._attributes.setdefault(self._ALL, list()).append(attrs)
        else:
            self._attributes.setdefault(cluster, list()).append(attrs)
        return self

    def build_specification(self) -> Table:
        assert self._schema
        in_collocation: bool = self._is_in_collocation()
        result = Table(
            chaos=self.use_chaos, in_collocation=in_collocation, schema=SchemaSpec.parse(self._schema), clusters={}
        )
        for cluster_name, (cluster_type, cluster_mode) in self._clusters.items():
            attrs = deepcopy(self._get_attrs_for_cluster(cluster_name))
            is_main = cluster_type in (YtCluster.Type.MAIN, YtCluster.Type.SINGLE)
            rtt_enabled = self._is_rtt_enabled(attrs)
            attrs.pop("in_collocation", None)  # just cleanup
            replication_log_attrs: Types.Attributes | None = attrs.pop("replication_log", None)
            cluster_spec = ClusterTable(
                main=is_main,
                path=self._replica_paths.get(cluster_name) or self.path,
                replicated_table_tracker_enabled=None if is_main else rtt_enabled,
                preferred_sync=None if is_main else (cluster_mode == YtCluster.Mode.SYNC),
                attributes=attrs,
            )
            if self.use_chaos and replication_log_attrs:
                cluster_spec.replication_log = ReplicationLog(attributes=replication_log_attrs)
            result.clusters[cluster_name] = cluster_spec
        return result

    def _is_in_collocation(self) -> bool | None:
        for cluster_name, (cluster_type, _) in self._clusters.items():
            is_main = cluster_type in (YtCluster.Type.MAIN, YtCluster.Type.SINGLE)
            if not is_main:
                continue
            attrs = self._get_attrs_for_cluster(cluster_name)
            return attrs.get("in_collocation", None)
        return None

    @staticmethod
    def _is_rtt_enabled(attrs: Types.Attributes) -> bool:
        v1 = bool(attrs.pop("enable_replicated_table_tracker", False))
        v2 = bool(attrs.pop("replicated_table_tracker_enabled", False))
        return v1 or v2

    @staticmethod
    def _fill_cluster_attrs(cluster_attrs: Types.Attributes, schema: Types.Schema, attrs: Types.Attributes | None):
        cluster_attrs["schema"] = schema
        if attrs:
            cluster_attrs = merge_copy(cluster_attrs, attrs)

    def _get_attrs_for_cluster(self, cluster: str) -> Types.Attributes | None:
        result = dict()
        for patch in self._attributes.get(self._ALL, list()):
            result = merge_copy(result, patch)

        for patch in self._attributes.get(cluster, list()):
            result = merge_copy(result, patch)

        return result
