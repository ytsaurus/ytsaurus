from copy import deepcopy
import logging
from typing import Callable
from typing import Generator

from yt.yt_sync.action import ActionBatch
from yt.yt_sync.action import AlterRttOptionsAction
from yt.yt_sync.action import RemountTableAction
from yt.yt_sync.action import SwitchReplicaModeAction
from yt.yt_sync.action import TableActionCollector
from yt.yt_sync.action import WaitReplicasInSyncAction
from yt.yt_sync.core.helpers import get_dummy_logger
from yt.yt_sync.core.model import YtCluster
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.settings import Settings

LOG = logging.getLogger("yt_sync")


class ReplicaSwitcher:
    def __init__(
        self,
        settings: Settings,
        actual_db: YtDatabase,
        logger: logging.Logger | None = None,
        skip_wait: set[str] | None = None,
    ):
        self._settings = settings
        self._db = actual_db
        self._logger: logging.Logger = logger or LOG
        self._skip_wait: set[str] = skip_wait or set()

        self._main: set[str] = set()
        self._always_async_pool: set[str] = set()
        self._async_pool: set[str] = set()
        self._sync_pool: set[str] = set()
        self._fill_pools()

    def _fill_pools(self):
        self._main.add(self._db.main.name)
        async_replicas: list[YtCluster] = self._db.async_replicas_relaxed
        self._always_async_pool = set([c.name for c in async_replicas if c.name in self._settings.always_async])
        self._async_pool = set([c.name for c in async_replicas if c.name not in self._settings.always_async])
        self._sync_pool = set([c.name for c in self._db.sync_replicas_relaxed])

    def clusters(self, add_reversed: bool = False) -> Generator[tuple[YtCluster, bool], None, None]:
        # cluster_name, is_last
        clusters: list[(str, bool)] = list()

        always_async: list[str] = sorted(self._always_async_pool)
        async_: list[str] = sorted(self._async_pool)
        sync: list[str] = sorted(self._sync_pool)

        if self._settings.is_chaos:
            clusters.extend([(name, False) for name in async_])
            clusters.extend([(name, False) for name in always_async])
        else:
            clusters.extend([(name, False) for name in always_async])
            clusters.extend([(name, False) for name in async_])
        clusters.extend([(name, False) for name in sync])
        clusters.extend([(name, not add_reversed) for name in self._main])

        for cluster_name, is_last in clusters:
            yield self._db.clusters[cluster_name], is_last

        if add_reversed:
            # Pools changed in previous yield cycle
            clusters.clear()

            if self._settings.is_chaos:
                clusters.extend([(name, False) for name in reversed(sorted(self._async_pool))])
                clusters.extend([(name, False) for name in reversed(sorted(self._always_async_pool))])
            else:
                clusters.extend([(name, False) for name in reversed(sorted(self._always_async_pool))])
                clusters.extend([(name, False) for name in reversed(sorted(self._async_pool))])
            clusters.extend([(name, False) for name in reversed(sorted(self._sync_pool))])
            if clusters:
                clusters[-1] = (clusters[-1][0], True)
            for cluster_name, is_last in clusters:
                yield self._db.clusters[cluster_name], is_last

    def get_sync_clusters(self) -> set[str]:
        return deepcopy(self._sync_pool)

    def make_async(self, cluster_name: str) -> list[ActionBatch]:
        self._logger.debug(
            "Requested %s as async replica, current state is: main=%s, sync=%s, async=%s, min_sync_cluster=%s",
            cluster_name,
            self._main,
            self._sync_pool,
            (self._async_pool | self._always_async_pool),
            self._settings.min_sync_clusters,
        )
        non_sync_pool: set[str] = self._main | self._always_async_pool | self._async_pool
        assert cluster_name in non_sync_pool | self._sync_pool, f"Unknown cluster {cluster_name}"
        if cluster_name in non_sync_pool:
            if cluster_name in self._main:
                self._logger.debug("Main cluster %s can't be async, nothing to switch", cluster_name)
            else:
                self._logger.debug("Replica %s is already async, nothing to switch", cluster_name)
            return []  # no need to prepare
        self._sync_pool.remove(cluster_name)
        next_sync: str | None = None
        if self._settings.min_sync_clusters and len(self._sync_pool) < self._settings.min_sync_clusters:
            try:
                next_sync = self._async_pool.pop()
            except KeyError:
                raise AssertionError("No available async clusters to make them sync")
        if next_sync:
            self._sync_pool.add(next_sync)
        self._async_pool.add(cluster_name)
        return self._generate_switch_actions(True)

    def ensure_sync_mode(self, desired_db: YtDatabase, update_rtt: bool = False) -> list[ActionBatch]:
        main_cluster: YtCluster = self._db.main
        desired_main: YtCluster = desired_db.main
        action_collector = TableActionCollector(main_cluster.name)

        for table_key in sorted(main_cluster.tables):
            actual_table: YtTable = main_cluster.tables[table_key]
            if not actual_table.is_replicated:
                continue
            if actual_table.is_temporary:
                continue

            desired_table: YtTable = desired_main.tables[table_key]
            if desired_table.is_rtt_enabled and update_rtt:
                action_collector.add(desired_table.key, AlterRttOptionsAction(desired_table, actual_table))
                if not desired_table.is_chaos_replicated:
                    action_collector.add(desired_table.key, RemountTableAction(actual_table))
                desired_sync_replicas: set[str] = desired_table.sync_replicas
                if self._settings.wait_in_sync_replicas and desired_sync_replicas:
                    action_collector.add(
                        desired_table.key, WaitReplicasInSyncAction(actual_table, desired_sync_replicas)
                    )
            else:
                self._setup_sync_replicas(
                    action_collector,
                    actual_table,
                    desired_table.sync_replicas,
                    False,
                    min_sync_replica_count=desired_table.min_sync_replica_count,
                    max_sync_replica_count=desired_table.max_sync_replica_count,
                )

        return action_collector.dump()

    @classmethod
    def is_all_switchable(cls, settings: Settings, actual_db: YtDatabase, desired_db: YtDatabase) -> bool:
        def _test(action: Callable) -> bool:
            try:
                action()
                return True
            except AssertionError:
                return False

        desired_sync_replicas = set([r.name for r in desired_db.sync_replicas_relaxed])
        if desired_sync_replicas & settings.always_async:
            return False

        switcher = cls(settings, actual_db, get_dummy_logger())
        result = True
        for cluster, _ in switcher.clusters(True):
            result = result and _test(lambda: switcher.make_async(cluster.name))
        result = result and _test(lambda: switcher.ensure_sync_mode(desired_db))
        return result

    def _generate_switch_actions(self, use_skip: bool) -> list[ActionBatch]:
        self._logger.warning(
            "Schedule switch replicas to: sync=%s, async=%s",
            sorted(self._sync_pool),
            sorted(self._async_pool | self._always_async_pool),
        )

        main_cluster: YtCluster = self._db.main
        action_collector = TableActionCollector(main_cluster.name)
        sync_replica_count: int = self._settings.min_sync_clusters
        for table_key in sorted(main_cluster.tables):
            table: YtTable = main_cluster.tables[table_key]
            self._setup_sync_replicas(
                action_collector,
                table,
                self._sync_pool,
                use_skip,
                min_sync_replica_count=sync_replica_count,
                max_sync_replica_count=sync_replica_count,
            )

        return action_collector.dump()

    def _setup_sync_replicas(
        self,
        action_collector: TableActionCollector,
        table: YtTable,
        sync_replicas: set[str],
        use_skip: bool,
        min_sync_replica_count: int | None,
        max_sync_replica_count: int | None,
    ):
        if not table.is_replicated:
            return
        if table.is_temporary:
            return

        if self._settings.is_chaos:
            if table.is_ordered:
                action_collector.add(
                    table.key,
                    SwitchReplicaModeAction(
                        table,
                        sync_replicas,
                        min_sync_replica_count=min_sync_replica_count,
                        max_sync_replica_count=max_sync_replica_count,
                    ),
                )
            else:
                action_collector.add(
                    table.key,
                    SwitchReplicaModeAction(
                        table,
                        sync_replicas,
                        set([YtReplica.ContentType.DATA]),
                        min_sync_replica_count=min_sync_replica_count,
                        max_sync_replica_count=max_sync_replica_count,
                    ),
                )
        else:
            action_collector.add(
                table.key,
                SwitchReplicaModeAction(
                    table,
                    sync_replicas,
                    min_sync_replica_count=min_sync_replica_count,
                    max_sync_replica_count=max_sync_replica_count,
                ),
            )

        should_skip: bool = use_skip and table.key in self._skip_wait
        if not should_skip and self._settings.wait_in_sync_replicas and sync_replicas:
            action_collector.add(table.key, WaitReplicasInSyncAction(table, sync_replicas))
