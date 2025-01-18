from typing import ClassVar

import pytest

from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtCluster
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.model import YtNode
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.settings import Settings


class DiffDbTestBase:
    _CLUSTER2TYPE: ClassVar[dict[str, tuple[YtCluster.Type, YtCluster.Mode | None]]] = {
        "primary": (YtCluster.Type.MAIN, None),
        "remote0": (YtCluster.Type.REPLICA, YtCluster.Mode.SYNC),
        "remote1": (YtCluster.Type.REPLICA, YtCluster.Mode.ASYNC),
    }

    @pytest.fixture
    def main_cluster_name(self) -> str:
        return "primary"

    @pytest.fixture
    def sync_cluster_name(self) -> str:
        return "remote0"

    @pytest.fixture
    def async_cluster_name(self) -> str:
        return "remote1"

    @pytest.fixture
    def remote_clusters(self, sync_cluster_name: str, async_cluster_name: str) -> list[str]:
        return [sync_cluster_name, async_cluster_name]

    def _add_clusters(self, db: YtDatabase, is_chaos: bool):
        for cluster, (cluster_type, cluster_mode) in self._CLUSTER2TYPE.items():
            db.add_or_get_cluster(YtCluster.make(cluster_type, cluster, is_chaos, cluster_mode))

    def _add_table(
        self,
        db: YtDatabase,
        table_path: str,
        default_schema: Types.Schema,
        exists: bool,
        is_chaos: bool,
        skip_clusters: set[str] | None = None,
    ):
        for cluster in db.clusters.values():
            if skip_clusters and cluster.name in skip_clusters:
                continue
            table_type = (
                YtTable.resolve_table_type(YtTable.Type.REPLICATED_TABLE, is_chaos)
                if YtCluster.Type.MAIN == cluster.cluster_type
                else YtTable.Type.TABLE
            )
            table = cluster.add_table_from_attributes(
                table_path, table_type, table_path, exists, {"dynamic": True, "schema": default_schema}
            )
            if table.folder not in cluster.nodes:
                cluster.add_node(
                    YtNode.make(
                        cluster_name=cluster.name,
                        path=table.folder,
                        node_type=YtNode.Type.FOLDER,
                        exists=table.exists,
                        attributes={},
                    )
                )
            if db.is_chaos and YtTable.Type.TABLE == table_type:
                cluster.add_replication_log_for(table)
        if exists:
            main_table = db.main.tables[table_path]
            for cluster in db.replicas:
                if skip_clusters and cluster.name in skip_clusters:
                    continue
                table = cluster.tables[table_path]
                main_table.add_replica_for(table, cluster, YtReplica.Mode.ASYNC)

    @pytest.fixture
    def is_chaos(self) -> bool:
        return False

    @pytest.fixture
    def settings(self, is_chaos: bool) -> Settings:
        return Settings(db_type=Settings.CHAOS_DB if is_chaos else Settings.REPLICATED_DB)

    @pytest.fixture
    def desired_db(
        self,
        table_path: str,
        folder_path: str,
        default_schema: Types.Schema,
        is_chaos: bool,
    ) -> YtDatabase:
        desired = YtDatabase()
        self._add_clusters(desired, is_chaos)
        self._add_table(desired, table_path, default_schema, exists=True, is_chaos=is_chaos)

        desired.clusters["remote0"].nodes[folder_path] = YtNode.make(
            cluster_name="remote0",
            path=folder_path,
            node_type=YtNode.Type.FOLDER,
            exists=True,
            attributes={"attribute": "value"},
        )
        return desired

    @pytest.fixture
    def actual_db(
        self,
        table_path: str,
        folder_path: str,
        default_schema: Types.Schema,
        is_chaos: bool,
    ) -> YtDatabase:
        db = YtDatabase()
        self._add_clusters(db, is_chaos)
        self._add_table(db, table_path, default_schema, exists=False, is_chaos=is_chaos)
        db.clusters["remote0"].nodes[folder_path] = YtNode.make(
            cluster_name="remote0",
            path=folder_path,
            node_type=YtNode.Type.FOLDER,
            exists=False,
            attributes={},
        )
        return db
