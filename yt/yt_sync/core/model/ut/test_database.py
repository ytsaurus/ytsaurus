import pytest

from yt.yt_sync.core.model.cluster import YtCluster
from yt.yt_sync.core.model.database import YtDatabase
from yt.yt_sync.core.model.table import YtReplica
from yt.yt_sync.core.model.table import YtTable
from yt.yt_sync.core.model.types import Types

from .helpers import make_replica


class TestYtDatabase:
    @pytest.mark.parametrize("is_chaos", [True, False])
    def test_add_or_get_cluster(self, is_chaos: bool):
        src_cluster = YtCluster.make(YtCluster.Type.MAIN, "primary", is_chaos)
        db = YtDatabase()
        dst_cluster = db.add_or_get_cluster(src_cluster)
        assert db.is_chaos == is_chaos
        assert src_cluster.name == dst_cluster.name
        assert src_cluster.cluster_type == dst_cluster.cluster_type
        assert src_cluster.is_chaos == dst_cluster.is_chaos
        assert src_cluster.name in db.clusters

    def test_add_or_get_cluster_double(self, table_path: str, default_schema: Types.Schema):
        src_cluster = YtCluster.make(YtCluster.Type.MAIN, "primary", False)
        db = YtDatabase()
        dst_cluster = db.add_or_get_cluster(src_cluster)
        table = dst_cluster.add_table_from_attributes(
            "k", YtTable.Type.TABLE, table_path, True, {"schema": default_schema}
        )
        dst_cluster2 = db.add_or_get_cluster(src_cluster)
        assert dst_cluster == dst_cluster2
        assert table.key in dst_cluster2.tables

    def test_add_or_get_cluster_double_main(self):
        db = YtDatabase()
        db.add_or_get_cluster(YtCluster.make(YtCluster.Type.MAIN, "primary", False))
        with pytest.raises(AssertionError):
            db.add_or_get_cluster(YtCluster.make(YtCluster.Type.MAIN, "remote0", False))

    def test_clusters_properties(self):
        db = YtDatabase()
        db.add_or_get_cluster(YtCluster.make(YtCluster.Type.MAIN, "primary", True))
        db.add_or_get_cluster(YtCluster.make(YtCluster.Type.REPLICA, "remote0", True, YtCluster.Mode.SYNC))
        db.add_or_get_cluster(YtCluster.make(YtCluster.Type.REPLICA, "remote1", True, YtCluster.Mode.SYNC))

        assert [] == list([c.name for c in db.async_replicas_relaxed])
        assert ["remote0", "remote1"] == list([c.name for c in db.sync_replicas_relaxed])
        assert ["primary", "remote0", "remote1"] == list([c.name for c in db.all_clusters])

    def test_main_empty(self):
        db = YtDatabase()
        with pytest.raises(AssertionError):
            db.main
        assert not db.has_main

    @pytest.mark.parametrize("cluster_type", [YtCluster.Type.MAIN, YtCluster.Type.SINGLE])
    def test_main(self, cluster_type: YtCluster.Type):
        db = YtDatabase()
        src_main = db.add_or_get_cluster(YtCluster.make(cluster_type, "primary", False))
        assert src_main == db.main
        assert db.has_main

    def test_replicas_empty(self):
        assert not YtDatabase().replicas

    def test_replicas(self):
        db = YtDatabase()
        db.add_or_get_cluster(YtCluster.make(YtCluster.Type.REPLICA, "remote0", False, YtCluster.Mode.SYNC))
        db.add_or_get_cluster(YtCluster.make(YtCluster.Type.REPLICA, "remote1", False, YtCluster.Mode.ASYNC))

        assert ["remote0", "remote1"] == [r.name for r in db.replicas]

    def test_db_integrity_for_empty(self):
        db = YtDatabase()
        # Not fails on empty db.
        db.ensure_db_integrity()

    def test_db_integrity_for_single(self):
        db = YtDatabase()
        main_cluster = db.add_or_get_cluster(YtCluster.make(YtCluster.Type.MAIN, "primary", False))
        db.ensure_db_integrity()
        assert main_cluster.cluster_type == YtCluster.Type.SINGLE

    def test_db_integrity_for_replicated(self):
        db = YtDatabase()
        main_cluster = db.add_or_get_cluster(YtCluster.make(YtCluster.Type.SINGLE, "primary", False))
        db.add_or_get_cluster(YtCluster.make(YtCluster.Type.REPLICA, "remote0", False, YtCluster.Mode.MIXED))
        db.ensure_db_integrity()
        assert main_cluster.cluster_type == YtCluster.Type.MAIN

    def _make_filled_db(self, table_path: str, schema: Types.Schema) -> YtDatabase:
        db = YtDatabase()
        main_cluster = db.add_or_get_cluster(YtCluster.make(YtCluster.Type.MAIN, "primary", False))
        db.add_or_get_cluster(YtCluster.make(YtCluster.Type.REPLICA, "remote0", False, YtCluster.Mode.MIXED))
        db.add_or_get_cluster(YtCluster.make(YtCluster.Type.REPLICA, "remote1", False, YtCluster.Mode.MIXED))
        db.add_or_get_cluster(YtCluster.make(YtCluster.Type.REPLICA, "remote2", False, YtCluster.Mode.MIXED))

        REPLICA_MODE = {
            0: {"remote0": YtReplica.Mode.SYNC, "remote1": YtReplica.Mode.SYNC, "remote2": YtReplica.Mode.ASYNC},
            1: {"remote0": YtReplica.Mode.SYNC, "remote1": YtReplica.Mode.SYNC, "remote2": YtReplica.Mode.ASYNC},
            2: {"remote0": YtReplica.Mode.SYNC, "remote1": YtReplica.Mode.ASYNC, "remote2": YtReplica.Mode.ASYNC},
            3: {"remote0": YtReplica.Mode.SYNC, "remote1": YtReplica.Mode.ASYNC, "remote2": YtReplica.Mode.ASYNC},
        }

        for num in range(len(REPLICA_MODE)):
            path: str = f"{table_path}_{num}"
            table: YtTable = YtTable.make(
                path, "primary", YtTable.Type.REPLICATED_TABLE, path, True, {"dynamic": True, "schema": schema}
            )
            for replica_cluster in db.replicas:
                table.add_replica(make_replica(replica_cluster.name, path, REPLICA_MODE[num][replica_cluster.name]))

            main_cluster.add_table(table)

        return db

    def test_db_integrity_replica_clusters(self, table_path: str, default_schema: Types.Schema):
        db: YtDatabase = self._make_filled_db(table_path, default_schema)
        db.ensure_db_integrity()

        assert db.clusters["remote0"].mode == YtCluster.Mode.SYNC
        assert db.clusters["remote1"].mode == YtCluster.Mode.MIXED
        assert db.clusters["remote2"].mode == YtCluster.Mode.ASYNC

        assert ["remote0"] == [c.name for c in db.sync_replicas_relaxed]
        assert ["remote1", "remote2"] == [c.name for c in db.async_replicas_relaxed]
        assert ["remote0", "remote1", "remote2"] == [c.name for c in db.replicas]

        with pytest.raises(AssertionError):
            db.ensure_db_integrity({"remote1"})

    def test_db_integrity_for_rtt(self, table_path: str, default_schema: Types.Schema):
        db: YtDatabase = self._make_filled_db(table_path, default_schema)
        for table in db.main.tables.values():
            table.in_collocation = True

        # Check fails on different sync replicas for tables in collocation
        with pytest.raises(AssertionError):
            db.ensure_db_integrity()

        # Make all replicas mode same
        for table in db.main.tables.values():
            for replica in table.replicas.values():
                if replica.cluster_name == "remote0":
                    replica.mode = YtReplica.Mode.SYNC
                else:
                    replica.mode = YtReplica.Mode.ASYNC

        # remote1 is async now
        db.ensure_db_integrity({"remote1"})

        assert db.clusters["remote0"].mode == YtCluster.Mode.SYNC
        assert db.clusters["remote1"].mode == YtCluster.Mode.ASYNC
        assert db.clusters["remote2"].mode == YtCluster.Mode.ASYNC

        assert ["remote0"] == [c.name for c in db.sync_replicas_relaxed]
        assert ["remote1", "remote2"] == [c.name for c in db.async_replicas_relaxed]
        assert ["remote0", "remote1", "remote2"] == [c.name for c in db.replicas]

    def test_db_integrity_for_rtt_sync_replicas_count(self, table_path: str, default_schema: Types.Schema):
        db: YtDatabase = self._make_filled_db(table_path, default_schema)
        for table in db.main.tables.values():
            table.is_rtt_enabled = True
            for replica in table.replicas.values():
                if replica.cluster_name in ("remote0", "remote1"):
                    replica.enable_replicated_table_tracker = True
                else:
                    replica.enable_replicated_table_tracker = False
        db.ensure_db_integrity()

        # min_sync_replica_count > RTT enabled replicas
        for table in db.main.tables.values():
            table.rtt_options.min_sync_replica_count = 3
        with pytest.raises(AssertionError):
            db.ensure_db_integrity()

        # max_sync_replica_count > RTT enabled replicas
        for table in db.main.tables.values():
            table.rtt_options.min_sync_replica_count = 2
            table.rtt_options.max_sync_replica_count = 3
        with pytest.raises(AssertionError):
            db.ensure_db_integrity()

        # min_sync_replica_count > max_sync_replica_count
        for table in db.main.tables.values():
            table.rtt_options.min_sync_replica_count = 2
            table.rtt_options.max_sync_replica_count = 1
        with pytest.raises(AssertionError):
            db.ensure_db_integrity()

        # min_sync_replica_count = max_sync_replica_count = RTT enabled replicas count
        for table in db.main.tables.values():
            table.rtt_options.min_sync_replica_count = 2
            table.rtt_options.max_sync_replica_count = 2
        db.ensure_db_integrity()

    def test_mountable_clusters_empty(self):
        assert not YtDatabase().mountable_clusters

    @pytest.mark.parametrize("is_chaos", [True, False])
    def test_mountable_clusters_replicated(self, is_chaos: bool):
        db = YtDatabase()
        db.add_or_get_cluster(YtCluster.make(YtCluster.Type.MAIN, "primary", is_chaos))
        db.add_or_get_cluster(YtCluster.make(YtCluster.Type.REPLICA, "remote0", is_chaos, YtCluster.Mode.SYNC))
        mountable = db.mountable_clusters
        if is_chaos:
            assert 1 == len(mountable)
            assert "remote0" == mountable[0].name
        else:
            assert 2 == len(mountable)
            for c in mountable:
                assert c.name in ("primary", "remote0")

    @pytest.mark.parametrize("is_chaos", [True, False])
    def test_get_replicated_table_for(self, is_chaos: bool, table_path: str, default_schema: Types.Schema):
        db = YtDatabase()

        table_key = table_path
        table_attrs = {"dynamic": True, "schema": default_schema}
        replicated_table_type = YtTable.resolve_table_type(YtTable.Type.REPLICATED_TABLE, is_chaos)

        for cluster_name, cluster_type, cluster_mode in (
            ("primary", YtCluster.Type.MAIN, None),
            ("remote0", YtCluster.Type.REPLICA, YtCluster.Mode.SYNC),
        ):
            cluster = db.add_or_get_cluster(YtCluster.make(cluster_type, cluster_name, is_chaos, cluster_mode))
            table_type = replicated_table_type if cluster.is_main else YtTable.Type.TABLE
            table = cluster.add_table_from_attributes(table_key, table_type, table_path, True, table_attrs)
            if is_chaos and not cluster.is_main:
                cluster.add_replication_log_for(table)

        # no table with same key
        with pytest.raises(AssertionError):
            table = YtTable.make("_bad_key_", "p", YtTable.Type.TABLE, table_path, True, table_attrs)
            db.get_replicated_table_for(table)

        def _get_table(table_type: str) -> YtTable:
            return next(iter(t for t in db.replicas[0].tables.values() if table_type == t.table_type))

        def _check_replicated(table: YtTable):
            replicated = db.get_replicated_table_for(table)
            assert replicated_table_type == replicated.table_type
            assert "primary" == replicated.cluster_name
            assert table_key == replicated.key

        _check_replicated(_get_table(YtTable.Type.TABLE))
        if is_chaos:
            _check_replicated(_get_table(YtTable.Type.REPLICATION_LOG))

    def test_get_data_clusters_replicated(self):
        db = YtDatabase()
        db.add_or_get_cluster(YtCluster.make(YtCluster.Type.MAIN, "primary", False))
        db.add_or_get_cluster(YtCluster.make(YtCluster.Type.REPLICA, "remote0", False, YtCluster.Mode.SYNC))
        db.add_or_get_cluster(YtCluster.make(YtCluster.Type.REPLICA, "remote1", False, YtCluster.Mode.ASYNC))
        db.add_or_get_cluster(YtCluster.make(YtCluster.Type.REPLICA, "remote2", False, YtCluster.Mode.ASYNC))
        data_clusters = set(cluster.name for cluster in db.data_clusters)
        assert set(("remote0", "remote1", "remote2")) == data_clusters

    def test_get_data_clusters_standalone(self):
        db = YtDatabase()
        db.add_or_get_cluster(YtCluster.make(YtCluster.Type.SINGLE, "primary", False))
        data_clusters = set(cluster.name for cluster in db.data_clusters)
        assert set(("primary",)) == data_clusters
