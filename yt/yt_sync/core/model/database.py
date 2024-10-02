from dataclasses import dataclass
from dataclasses import field as dataclass_field
import logging
from typing import Callable

from .cluster import YtCluster
from .table import YtTable

LOG = logging.getLogger("yt_sync")


@dataclass
class YtDatabase:
    clusters: dict[str, YtCluster] = dataclass_field(default_factory=dict)
    is_chaos: bool = False

    def add_or_get_cluster(self, another: YtCluster) -> YtCluster:
        if another.name in self.clusters:
            return self.clusters[another.name]
        if another.cluster_type in (YtCluster.Type.MAIN, YtCluster.Type.SINGLE):
            assert not self.has_main, (
                f"Already has main cluster ({another.dump_stat()}, "
                + f"CurrentClusters: {[c.dump_stat() for c in self.clusters.values()]})"
            )
        cluster = YtCluster.make(
            name=another.name, cluster_type=another.cluster_type, is_chaos=another.is_chaos, mode=another.mode
        )
        self.clusters[another.name] = cluster
        self.is_chaos = cluster.is_chaos  # TODO: get this param from settings only
        return cluster

    def get_replicated_table_for(self, table: YtTable) -> YtTable:
        main_cluster = self.main
        if YtTable.Type.REPLICATION_LOG == table.table_type:
            assert table.chaos_data_table, f"No link to chaos data table from replication log {table.rich_path}"
            assert (
                table.chaos_data_table in main_cluster.tables
            ), f"Missing chaos data table {table.chaos_data_table} for replication log {table.rich_path}"
            return main_cluster.tables[table.chaos_data_table]
        else:
            assert table.key in main_cluster.tables, f"Missing table {table.rich_path}"
            return main_cluster.tables[table.key]

    @property
    def all_clusters(self) -> list[YtCluster]:
        return self._get_clusters_sorted(
            [
                lambda c: c.is_main,
                lambda c: c.is_async_replica,
                lambda c: c.is_mixed_replica,
                lambda c: c.is_sync_replica,
            ]
        )

    @property
    def data_clusters(self) -> list[YtCluster]:
        replicas = self.replicas
        if replicas:
            return replicas
        return [self.main]

    @property
    def main(self) -> YtCluster:
        main_clusters = self._get_clusters_sorted([lambda c: c.is_main])
        assert 1 == len(
            main_clusters
        ), f"Only one main cluster allowed. Candidates: {[c.dump_stat() for c in main_clusters]}"
        return main_clusters[0]

    @property
    def has_main(self) -> bool:
        main_clusters = self._get_clusters_sorted([lambda c: c.is_main])
        return len(main_clusters) > 0

    @property
    def has_replicas(self) -> bool:
        return len(self.replicas) > 0

    @property
    def replicas(self) -> list[YtCluster]:
        return self._get_clusters_sorted(
            [lambda c: c.is_sync_replica, lambda c: c.is_mixed_replica, lambda c: c.is_async_replica]
        )

    @property
    def async_replicas_relaxed(self) -> list[YtCluster]:
        replica_stat: dict[str, dict[YtCluster.Mode, int]] = self._replicas_per_cluster_stat()
        result: list[YtCluster] = list()

        for replica_cluster, stat in replica_stat.items():
            sync_count: int = stat.get(YtCluster.Mode.SYNC, 0)
            async_count: int = stat.get(YtCluster.Mode.ASYNC, 0)
            if async_count >= sync_count:
                result.append(self.clusters[replica_cluster])
        return sorted(result, key=lambda r: r.name)

    @property
    def sync_replicas_relaxed(self) -> list[YtCluster]:
        async_replicas = set([r.name for r in self.async_replicas_relaxed])
        return sorted([r for r in self.replicas if r.name not in async_replicas], key=lambda r: r.name)

    @property
    def mountable_clusters(self) -> list[YtCluster]:
        return list([c for c in self.all_clusters if c.is_mountable])

    def _get_clusters_sorted(self, filters: list[Callable[[YtCluster], bool]]) -> list[YtCluster]:
        result: list[YtCluster] = list()
        for cluster_filter in filters:
            for cluster_name in sorted(self.clusters):
                cluster: YtCluster = self.clusters[cluster_name]
                if cluster_filter(cluster):
                    result.append(cluster)
        return result

    def _replicas_per_cluster_stat(self) -> dict[str, dict[YtCluster.Mode, int]]:
        main: YtCluster = self.main
        all_replicas: frozenset[str] = frozenset([r.name for r in self.clusters.values() if r.is_replica])
        result: dict[str, dict[YtCluster.Mode, int]] = dict()

        def _update_count(cluster: str, mode: YtCluster.Mode):
            current: int = result.setdefault(cluster, dict()).setdefault(mode, 0)
            result[cluster][mode] = current + 1

        for table in main.tables.values():
            if not table.exists:
                continue
            if not table.is_replicated:
                continue
            table_sync_replicas = table.sync_replicas
            for sync_replica in table_sync_replicas:
                _update_count(sync_replica, YtCluster.Mode.SYNC)
            for async_replica in all_replicas - table_sync_replicas:
                _update_count(async_replica, YtCluster.Mode.ASYNC)

        return result

    def _ensure_clusters_type(self):
        main: YtCluster = self.main
        main.cluster_type = YtCluster.Type.MAIN if self.has_replicas else YtCluster.Type.SINGLE

    def _ensure_clusters_mode(self, always_async: set[str] | None):
        main: YtCluster = self.main
        if main.cluster_type == YtCluster.Type.SINGLE:
            # No replica clusters to ensure.
            return
        replica_stat: dict[str, dict[YtCluster.Mode, int]] = self._replicas_per_cluster_stat()
        for replica_cluster, stat in replica_stat.items():
            sync_count: int = stat.get(YtCluster.Mode.SYNC, 0)
            async_count: int = stat.get(YtCluster.Mode.ASYNC, 0)
            if sync_count > 0 and async_count == 0:
                self.clusters[replica_cluster].mode = YtCluster.Mode.SYNC
            elif sync_count == 0 and async_count > 0:
                self.clusters[replica_cluster].mode = YtCluster.Mode.ASYNC
            elif sync_count > 0 and async_count > 0:
                self.clusters[replica_cluster].mode = YtCluster.Mode.MIXED
            else:
                self.clusters[replica_cluster].mode = YtCluster.Mode.ASYNC
        if always_async:
            for replica in self.replicas:
                if replica.name in always_async:
                    assert replica.mode == YtCluster.Mode.ASYNC, (
                        f"Replica cluster {replica.name} with mode {replica.mode} "
                        + f"can't be in always async clusters list {always_async}"
                    )

    def _ensure_rtt_settings(self):
        main: YtCluster = self.main
        if main.cluster_type == YtCluster.Type.SINGLE:
            # nothing to ensure
            return
        seen_collocation_sync_replicas: set[str] | None = None
        for table in main.tables_sorted:
            if not table.is_replicated:
                # nothing to ensure
                continue

            if table.in_collocation:
                if seen_collocation_sync_replicas is None:
                    seen_collocation_sync_replicas = table.sync_replicas
                else:
                    table_sync_replicas = table.sync_replicas
                    assert seen_collocation_sync_replicas == table_sync_replicas, (
                        f"Sync replicas mismatch for table {table.rich_path} in collocation:"
                        + f" expected {seen_collocation_sync_replicas}, got {table_sync_replicas}"
                    )

            if table.is_rtt_enabled:
                rtt_enabled_replica_count: int = len(list(table.rtt_enabled_replicas))
                assert (
                    rtt_enabled_replica_count > 0
                ), f"Bad RTT configuration for table {table.key}: no RTT enabled replicas for RTT enabled table"

                assert table.min_sync_replica_count <= rtt_enabled_replica_count, (
                    f"Bad RTT configuration for table {table.key}: "
                    + f"min_sync_replica_count={table.min_sync_replica_count}, "
                    + f"RTT enabled replicas count={rtt_enabled_replica_count}"
                )

                assert table.max_sync_replica_count <= rtt_enabled_replica_count, (
                    f"Bad RTT configuration for table {table.key}: "
                    + f"max_sync_replica_count={table.max_sync_replica_count}, "
                    + f"RTT enabled replicas count={rtt_enabled_replica_count}"
                )

                assert table.min_sync_replica_count <= table.max_sync_replica_count, (
                    f"Bad RTT configuration for table {table.key}: "
                    + f"min_sync_replica_count={table.min_sync_replica_count}, "
                    + f"max_sync_replica_count={table.max_sync_replica_count}"
                )

    def ensure_db_integrity(self, always_async: set[str] | None = None, ensure_rtt_settings: bool = True):
        if not self.clusters:
            return
        self._ensure_clusters_type()
        self._ensure_clusters_mode(always_async)
        if ensure_rtt_settings:
            self._ensure_rtt_settings()
