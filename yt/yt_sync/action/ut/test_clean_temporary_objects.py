from yt.yt_sync.action.clean_temporary_objects import CleanTemporaryObjectsAction
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtCluster
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTable

from .helpers import empty_mock_client

_TABLE_COUNT: int = 4


def _get_path(table_path: str, num: int):
    return f"{table_path}_{num}"


def test_action(table_path: str, default_schema: Types.Schema):
    cluster = YtCluster.make(YtCluster.Type.MAIN, "p", False)
    deleted_replica: Types.ReplicaKey | None = None
    for i in range(_TABLE_COUNT):
        path = _get_path(table_path, i)
        table = cluster.add_table(
            YtTable.make(path, "p", YtTable.Type.TABLE, path, True, {"dynamic": True, "schema": default_schema})
        )
        if 0 == i:
            table.table_type = YtTable.Type.REPLICATED_TABLE
            table.add_replica(
                YtReplica(
                    replica_id=None,
                    cluster_name="r0",
                    replica_path=path,
                    enabled=True,
                    mode=YtReplica.Mode.SYNC,
                )
            )
            table.add_replica(
                YtReplica(
                    replica_id=None,
                    cluster_name="r0",
                    replica_path=f"{path}.tmp.1",
                    enabled=True,
                    mode=YtReplica.Mode.SYNC,
                    exists=True,
                    is_temporary=True,
                )
            )
            deleted_replica_path = f"{path}.tmp.2"
            table.add_replica(
                YtReplica(
                    replica_id=None,
                    cluster_name="r0",
                    replica_path=f"{path}.tmp.2",
                    enabled=True,
                    mode=YtReplica.Mode.SYNC,
                    exists=False,
                    is_temporary=True,
                )
            )
            deleted_replica = YtReplica.make_key("r0", deleted_replica_path)
        elif 1 == i:
            pass  # usual table
        elif 2 == i:
            table.is_temporary = True
        elif 3 == i:
            table.is_temporary = True
            table.exists = False

    action = CleanTemporaryObjectsAction(cluster)
    assert action.schedule_next(empty_mock_client()) is False
    action.process()

    for i in range(_TABLE_COUNT):
        path = _get_path(table_path, i)
        if 0 == i:
            table = cluster.tables[path]
            assert 2 == len(table.replicas)
            assert deleted_replica
            assert deleted_replica not in table.replicas
        elif 1 == i:
            assert path in cluster.tables
        elif 2 == i:
            assert path in cluster.tables
        elif 3 == i:
            assert path not in cluster.tables
