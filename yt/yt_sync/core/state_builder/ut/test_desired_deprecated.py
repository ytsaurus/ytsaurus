import pytest

from yt.yt_sync.core.model import get_node_name
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtCluster
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.model import YtNode
from yt.yt_sync.core.model import YtNodeAttributes
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.model import YtTableAttributes
from yt.yt_sync.core.state_builder import DesiredStateBuilderDeprecated
from yt.yt_sync.core.table_filter import TableNameFilter
from yt.yt_sync.core.test_lib import MockYtClientFactory

from .base import make_table_settings_deprecated
from .base import StateBuilderTestBase


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
        builder = DesiredStateBuilderDeprecated(yt_client_factory, db, False, True, TableNameFilter(["_some_table_"]))
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

    @pytest.mark.parametrize("use_chaos", [True, False])
    def test_implicit_replicated_queue_attrs(
        self,
        table_path: str,
        ordered_schema: Types.Schema,
        db: YtDatabase,
        builder: DesiredStateBuilderDeprecated,
        use_chaos: bool,
    ):
        table_settings = make_table_settings_deprecated(table_path, ordered_schema)
        table_settings["use_chaos"] = use_chaos
        builder.add_table(table_settings)
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

    def test_attribute_propagation(
        self, default_schema: Types.Schema, db: YtDatabase, builder: DesiredStateBuilderDeprecated
    ):
        builder.add_node(
            {
                "type": YtNode.Type.FOLDER,
                "clusters": {
                    "primary": {"main": True, "path": "//tmp/folder", "attributes": {"attribute": "value"}},
                    "remote0": {"path": "//tmp/folder", "attributes": {"attribute": "value"}},
                    "remote1": {"path": "//tmp/folder", "attributes": {"attribute": "value"}},
                },
            }
        )
        builder.add_node(
            {
                "type": YtNode.Type.FOLDER,
                "clusters": {
                    "primary": {"main": True, "path": "//tmp/folder/folder", "attributes": {"attribute": "new_value"}},
                    "remote0": {"path": "//tmp/folder/folder", "attributes": {"other_attribute": "value"}},
                },
            }
        )
        builder.add_node(
            {
                "type": YtNode.Type.FOLDER,
                "clusters": {
                    "remote1": {
                        "main": False,
                        "path": "//tmp/folder/folder/folder",
                        "attributes": {"some_attribute": "do_not_overwrite"},
                    },
                },
            }
        )

        table_path = "//tmp/folder/folder/folder/table"
        builder.add_table(make_table_settings_deprecated(table_path, default_schema))
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

    def test_attribute_propagation_with_filter(self, db: YtDatabase, builder: DesiredStateBuilderDeprecated):
        builder.add_node(
            {
                "type": YtNode.Type.FOLDER,
                "clusters": {
                    "primary": {
                        "main": True,
                        "path": "//tmp/folder",
                        "attributes": {"attr": "value"},
                        "propagated_attributes": set(),
                    },
                    "remote": {
                        "path": "//tmp/folder",
                        "attributes": {"attr": "value"},
                        "propagated_attributes": {"attr"},
                    },
                },
            }
        )
        builder.add_node(
            {
                "type": YtNode.Type.FOLDER,
                "clusters": {
                    "remote": {
                        "main": False,
                        "path": "//tmp/folder/folder1",
                        "attributes": {"new_attr": "value"},
                        "propagated_attributes": {"new_attr"},
                    },
                },
            }
        )
        builder.add_node(
            {
                "type": YtNode.Type.FOLDER,
                "clusters": {
                    "remote": {
                        "main": False,
                        "path": "//tmp/folder/folder2",
                        "attributes": {"new_attr": "value"},
                        "propagated_attributes": {"attr"},
                    },
                },
            }
        )
        builder.add_node(
            {
                "type": YtNode.Type.FOLDER,
                "clusters": {
                    "remote": {
                        "main": False,
                        "path": "//tmp/folder/folder3",
                        "attributes": {"new_attr": "value"},
                    },
                },
            }
        )
        builder.add_node(
            {
                "type": YtNode.Type.FILE,
                "clusters": {
                    "primary": {"main": True, "path": "//tmp/folder/folder/file", "attributes": {}},
                },
            }
        )
        for i in range(1, 4):
            builder.add_node(
                {
                    "type": YtNode.Type.FILE,
                    "clusters": {
                        "remote": {"main": False, "path": f"//tmp/folder/folder{i}/file", "attributes": {}},
                    },
                }
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
