import pytest

from yt.yt_sync.core.model.cluster import YtCluster
from yt.yt_sync.core.model.helpers import get_node_name
from yt.yt_sync.core.model.helpers import make_tmp_name
from yt.yt_sync.core.model.node import YtNode
from yt.yt_sync.core.model.node_attributes import YtNodeAttributes
from yt.yt_sync.core.model.table import YtTable
from yt.yt_sync.core.model.table import YtTabletState
from yt.yt_sync.core.model.table_attributes import YtTableAttributes
from yt.yt_sync.core.model.types import Types


class TestYtCluster:
    @pytest.fixture
    @staticmethod
    def table_attrs(default_schema: Types.Schema) -> Types.Attributes:
        return {"dynamic": True, "schema": default_schema}

    @pytest.fixture
    @staticmethod
    def folder_attrs() -> Types.Attributes:
        return {"attribute": "value"}

    @pytest.fixture
    @staticmethod
    def yt_cluster() -> YtCluster:
        return YtCluster.make(YtCluster.Type.MAIN, "primary", True)

    @pytest.mark.parametrize("cluster_type", [YtCluster.Type.SINGLE, YtCluster.Type.MAIN, YtCluster.Type.REPLICA])
    @pytest.mark.parametrize("cluster_mode", [None, YtCluster.Mode.MIXED, YtCluster.Mode.SYNC, YtCluster.Mode.ASYNC])
    @pytest.mark.parametrize("is_chaos", [True, False])
    def test_make(self, cluster_type: YtCluster.Type, cluster_mode: YtCluster.Mode | None, is_chaos: bool):
        cluster = YtCluster.make(cluster_type, "primary", is_chaos, cluster_mode)
        assert "primary" == cluster.name
        assert cluster_type == cluster.cluster_type
        if cluster_type in (YtCluster.Type.MAIN, YtCluster.Type.SINGLE):
            assert cluster.is_main is True
            assert cluster.is_replica is False
        else:
            assert cluster.is_main is False
            assert cluster.is_replica is True

        if cluster_mode is None:
            assert cluster.mode is None
            if cluster.is_replica:
                assert not cluster.is_async_replica
                assert not cluster.is_sync_replica
                assert cluster.is_mixed_replica
        else:
            assert cluster.mode == cluster_mode
            if cluster.is_replica:
                assert cluster.is_sync_replica == (cluster_mode == YtCluster.Mode.SYNC)
                assert cluster.is_async_replica == (cluster_mode == YtCluster.Mode.ASYNC)
                assert cluster.is_mixed_replica == (cluster_mode == YtCluster.Mode.MIXED)

        assert is_chaos == cluster.is_chaos
        assert len(cluster.tables) == 0
        assert len(cluster.consumers) == 0

    def test_add_table(self, table_path: str, yt_cluster: YtCluster, table_attrs: Types.Attributes):
        table = YtTable.make(table_path, yt_cluster.name, YtTable.Type.TABLE, table_path, True, table_attrs)
        yt_cluster.add_table(table)

        assert table.key in yt_cluster.tables
        with pytest.raises(AssertionError):
            yt_cluster.add_table(table)

    def test_add_table_from_attributes_double(
        self, table_path: str, yt_cluster: YtCluster, table_attrs: Types.Attributes
    ):
        yt_cluster.add_table_from_attributes("k", YtTable.Type.TABLE, table_path, True, table_attrs)
        with pytest.raises(AssertionError):
            yt_cluster.add_table_from_attributes("k", YtTable.Type.TABLE, table_path, True, table_attrs)

    @pytest.mark.parametrize("explicit_in_collocation", [True, False, None])
    def test_add_table_from_attributes(
        self,
        table_path: str,
        yt_cluster: YtCluster,
        table_attrs: Types.Attributes,
        explicit_in_collocation: bool | None,
    ):
        name = get_node_name(table_path)

        yt_cluster.add_table_from_attributes(
            "k",
            YtTable.Type.REPLICATED_TABLE,
            table_path,
            True,
            table_attrs,
            explicit_in_collocation=explicit_in_collocation,
        )
        assert "k" in yt_cluster.tables

        table = yt_cluster.tables["k"]
        assert "k" == table.key
        assert table_path == table.path
        assert name == table.name
        assert YtTable.Type.REPLICATED_TABLE == table.table_type
        assert table.exists is True
        assert 3 == len(table.schema.columns)
        assert table.attributes["dynamic"] is True
        assert table.in_collocation == bool(explicit_in_collocation)

    def test_add_node(self, folder_path: str, yt_cluster: YtCluster):
        node = YtNode.make(
            cluster_name=yt_cluster.name,
            path=folder_path,
            node_type=YtNode.Type.FOLDER,
            exists=True,
            attributes={},
        )
        yt_cluster.add_node(node)
        assert folder_path in yt_cluster.nodes
        with pytest.raises(AssertionError):
            yt_cluster.add_node(node)

    def test_folders_construction_necessity(
        self,
        yt_cluster: YtCluster,
        folder_path: str,
        table_attrs: Types.Attributes,
        folder_attrs: Types.Attributes,
    ):
        node = YtNode.make(
            cluster_name=yt_cluster.name,
            path=folder_path,
            node_type=YtNode.Type.FOLDER,
            exists=True,
            attributes=folder_attrs,
        )
        yt_cluster.add_node(node)

        other_folder_path = folder_path + "/other"
        yt_cluster.add_node(
            YtNode.make(
                cluster_name=yt_cluster.name,
                path=other_folder_path,
                node_type=YtNode.Type.FOLDER,
                exists=True,
                attributes={},
            )
        )
        assert not node.attributes.has_diff_with(YtNodeAttributes.make(folder_attrs))

        table_path = folder_path + "/table"
        yt_cluster.add_table(
            YtTable.make(table_path, yt_cluster.name, YtTable.Type.TABLE, table_path, True, table_attrs)
        )
        assert not node.attributes.has_diff_with(YtNodeAttributes.make(folder_attrs))

    def test_double_node_update(
        self, yt_cluster: YtCluster, folder_path: str, table_attrs: Types.Attributes, folder_attrs: Types.Attributes
    ):
        table_path = folder_path + "/table"
        yt_cluster.add_table(
            YtTable.make(table_path, yt_cluster.name, YtTable.Type.TABLE, table_path, True, table_attrs)
        )

        with pytest.raises(AssertionError):
            yt_cluster.add_node(
                YtNode.make(
                    cluster_name=yt_cluster.name,
                    path=folder_path,
                    node_type=YtNode.Type.FILE,
                    exists=True,
                    attributes=folder_attrs,
                )
            )

        yt_cluster.add_node(
            YtNode.make(
                cluster_name=yt_cluster.name,
                path=folder_path,
                node_type=YtNode.Type.FOLDER,
                exists=True,
                attributes=folder_attrs,
            )
        )
        assert yt_cluster.nodes[folder_path].node_type == YtNode.Type.FOLDER
        assert not yt_cluster.nodes[folder_path].attributes.has_diff_with(YtNodeAttributes.make(folder_attrs))

        with pytest.raises(AssertionError):
            yt_cluster.add_node(
                YtNode.make(
                    cluster_name=yt_cluster.name,
                    path=folder_path,
                    node_type=YtNode.Type.FOLDER,
                    exists=True,
                    attributes=folder_attrs,
                )
            )

    def test_path_conflict(self, yt_cluster: YtCluster, folder_path: str):
        yt_cluster.add_node(
            YtNode.make(
                cluster_name=yt_cluster.name,
                path=folder_path,
                node_type=YtNode.Type.FILE,
                exists=True,
                attributes={},
            )
        )
        assert folder_path in yt_cluster.nodes

        child_path = folder_path + "/child"

        with pytest.raises(AssertionError):
            yt_cluster.add_node(
                YtNode.make(
                    cluster_name=yt_cluster.name,
                    path=child_path,
                    node_type=YtNode.Type.FOLDER,
                    exists=True,
                    attributes={},
                )
            )
        assert child_path not in yt_cluster.nodes

    @pytest.mark.parametrize("exists", [True, False])
    @pytest.mark.parametrize("rtt_enabled", [True, False])
    def test_add_replication_log_for(
        self, table_path: str, yt_cluster: YtCluster, table_attrs: Types.Attributes, exists: bool, rtt_enabled: bool
    ):
        table = yt_cluster.add_table_from_attributes("k", YtTable.Type.TABLE, table_path, exists, table_attrs)
        table.rtt_options.enabled = rtt_enabled
        replication_log = yt_cluster.add_replication_log_for(table)
        assert replication_log
        assert replication_log.cluster_name == yt_cluster.name
        assert table.schema == replication_log.schema
        assert table.exists == replication_log.exists
        assert YtTable.Type.REPLICATION_LOG == replication_log.table_type
        assert table.chaos_replication_log == "k_log"
        assert replication_log.chaos_data_table == table.key
        assert rtt_enabled == replication_log.rtt_options.enabled

    def test_add_replication_log_for_ordered_table(
        self, table_path: str, yt_cluster: YtCluster, ordered_schema: Types.Schema
    ):
        table_attrs = {"dynamic": True, "schema": ordered_schema}
        table = yt_cluster.add_table_from_attributes("k", YtTable.Type.TABLE, table_path, True, table_attrs)
        assert not yt_cluster.add_replication_log_for(table)

    def test_add_replication_log_for_not_chaos(
        self, table_path: str, yt_cluster: YtCluster, table_attrs: Types.Attributes
    ):
        yt_cluster.is_chaos = False
        table = yt_cluster.add_table_from_attributes("k", YtTable.Type.TABLE, table_path, True, table_attrs)
        assert yt_cluster.add_replication_log_for(table) is None

    def test_add_replication_log_for_not_data_table(
        self, table_path: str, yt_cluster: YtCluster, table_attrs: Types.Attributes
    ):
        table = yt_cluster.add_table_from_attributes("k", YtTable.Type.REPLICATION_LOG, table_path, True, table_attrs)
        assert yt_cluster.add_replication_log_for(table) is None

    def test_get_replication_log_for(self, table_path: str, yt_cluster: YtCluster, table_attrs: Types.Attributes):
        table = yt_cluster.add_table_from_attributes("k", YtTable.Type.TABLE, table_path, True, table_attrs)
        replication_log = yt_cluster.add_replication_log_for(table)
        assert replication_log == yt_cluster.get_replication_log_for(table)

    def test_get_replication_log_for_absent1(
        self, table_path: str, yt_cluster: YtCluster, table_attrs: Types.Attributes
    ):
        table = yt_cluster.add_table_from_attributes("k", YtTable.Type.TABLE, table_path, True, table_attrs)
        assert yt_cluster.get_replication_log_for(table) is None

    def test_get_replication_log_for_absent2(
        self, table_path: str, yt_cluster: YtCluster, table_attrs: Types.Attributes
    ):
        table = yt_cluster.add_table_from_attributes("k", YtTable.Type.TABLE, table_path, True, table_attrs)
        table.chaos_replication_log = "_unknown_"
        assert yt_cluster.get_replication_log_for(table) is None

    @pytest.mark.parametrize("exists", [True, False, None])
    def test_get_or_add_temporary_table_for(
        self, table_path: str, yt_cluster: YtCluster, table_attrs: Types.Attributes, exists: bool | None
    ):
        table = yt_cluster.add_table_from_attributes(
            "k", YtTable.Type.TABLE, table_path, True if exists is None else exists, table_attrs
        )
        table.tablet_state.set(YtTabletState.MOUNTED)
        assert table.key in yt_cluster.tables

        tmp_table = yt_cluster.get_or_add_temporary_table_for(table, exists)
        assert tmp_table.key in yt_cluster.tables
        assert tmp_table.is_temporary
        assert tmp_table.tablet_state.is_unmounted
        assert make_tmp_name(table.key) == tmp_table.key
        assert make_tmp_name(table.name) == tmp_table.name
        assert make_tmp_name(table.path) == tmp_table.path
        if exists is None:
            assert table.exists == tmp_table.exists
        else:
            assert exists == tmp_table.exists

        tmp_table.attributes["my_attr"] = "my_value"

        tmp_table2 = yt_cluster.get_or_add_temporary_table_for(table, exists)
        assert tmp_table2.key in yt_cluster.tables
        assert tmp_table2.is_temporary
        assert tmp_table.key == tmp_table2.key
        assert tmp_table.exists == tmp_table2.exists
        assert "my_value" == tmp_table2.attributes["my_attr"]  # check same instance as tmp_table, not new one

    def test_find_table_by_path(self, table_path: str, yt_cluster: YtCluster, table_attrs: Types.Attributes):
        assert yt_cluster.find_table_by_path(table_path) is None
        table = yt_cluster.add_table_from_attributes("k", YtTable.Type.TABLE, table_path, True, table_attrs)
        assert table == yt_cluster.find_table_by_path(table_path)

    def test_is_mountable(self):
        assert YtCluster.make(YtCluster.Type.MAIN, "c", False).is_mountable
        assert not YtCluster.make(YtCluster.Type.MAIN, "c", True).is_mountable
        assert YtCluster.make(YtCluster.Type.SINGLE, "c", False).is_mountable
        for cluster_mode in (YtCluster.Mode.SYNC, YtCluster.Mode.ASYNC):
            assert YtCluster.make(YtCluster.Type.REPLICA, "c", False, cluster_mode).is_mountable
            assert YtCluster.make(YtCluster.Type.REPLICA, "c", True, cluster_mode).is_mountable

    @pytest.mark.parametrize(
        "first_link_path, second_link_path",
        (
            ("//folder", "//folder/link1"),
            ("//folder/link2", "//folder"),
        ),
    )
    def test_links(self, yt_cluster: YtCluster, first_link_path: str, second_link_path: str):
        yt_cluster.add_node(
            YtNode.make(
                cluster_name=yt_cluster.name,
                path="//folder/link1",
                node_type=YtNode.Type.LINK,
                exists=True,
                attributes={},
                explicit_target_path=first_link_path,
            )
        )
        with pytest.raises(AssertionError):
            yt_cluster.add_node(
                YtNode.make(
                    cluster_name=yt_cluster.name,
                    path="//folder/link2",
                    node_type=YtNode.Type.LINK,
                    exists=True,
                    attributes={},
                    explicit_target_path=second_link_path,
                )
            )

    def _test_any_as_folder_with_children(self, folder_path: str, yt_cluster: YtCluster):
        yt_cluster.add_node(
            YtNode.make(
                cluster_name=yt_cluster.name,
                path=folder_path + "/folder/folder/file",
                node_type=YtNode.Type.FILE,
                exists=True,
                attributes={},
            )
        )
        yt_cluster.add_node(
            YtNode.make(
                cluster_name=yt_cluster.name,
                path=folder_path + "/folder",
                node_type=YtNode.Type.ANY,
                exists=True,
                attributes={},
            )
        )
        yt_cluster._fix_folders()

        if folder_path != "/":
            assert yt_cluster.nodes[folder_path].node_type == YtNode.Type.FOLDER
            assert yt_cluster.nodes[folder_path].is_implicit
        else:
            assert folder_path not in yt_cluster.nodes
        assert yt_cluster.nodes[folder_path + "/folder/folder"].node_type == YtNode.Type.FOLDER
        assert yt_cluster.nodes[folder_path + "/folder/folder"].is_implicit
        assert yt_cluster.nodes[folder_path + "/folder"].node_type == YtNode.Type.ANY

    def test_any_as_folder_with_children(self, folder_path: str, yt_cluster: YtCluster):
        self._test_any_as_folder_with_children(folder_path, yt_cluster)

    def test_any_with_children_under_root(self, yt_cluster: YtCluster):
        self._test_any_as_folder_with_children("/", yt_cluster)

    def test_fix_folders(self, table_path: str, yt_cluster: YtCluster, table_attrs: Types.Attributes):
        table = YtTable.make(table_path, yt_cluster.name, YtTable.Type.TABLE, table_path, True, table_attrs)
        yt_cluster.add_table(table)
        assert table.folder not in yt_cluster.nodes

        node1 = YtNode.make(yt_cluster.name, table.folder + "/folder1/file", YtNode.Type.FILE, True, {})
        yt_cluster.add_node(node1)
        assert node1.folder not in yt_cluster.nodes

        node2 = YtNode.make(yt_cluster.name, table.folder + "/folder2/folder", YtNode.Type.FOLDER, True, {})
        yt_cluster.add_node(node2)
        assert node2.folder not in yt_cluster.nodes

        yt_cluster._fix_folders()

        for path in (table.folder, node1.folder, node2.folder):
            assert path in yt_cluster.nodes
            assert yt_cluster.nodes[path].node_type == YtNode.Type.FOLDER
            assert yt_cluster.nodes[path].is_implicit

    def test_no_useless_folders(self, folder_path: str, yt_cluster: YtCluster):
        yt_cluster.add_node(
            YtNode.make(
                cluster_name=yt_cluster.name,
                path=folder_path + "/folder1",
                node_type=YtNode.Type.ANY,
                exists=True,
                attributes={},
            )
        )
        yt_cluster.add_node(
            YtNode.make(
                cluster_name=yt_cluster.name,
                path=folder_path + "/folder2/folder/folder/file",
                node_type=YtNode.Type.FILE,
                exists=True,
                attributes={},
            )
        )
        yt_cluster.add_node(
            YtNode.make(
                cluster_name=yt_cluster.name,
                path=folder_path + "/folder2/file",
                node_type=YtNode.Type.ANY,
                exists=True,
                attributes={},
            )
        )
        yt_cluster.add_node(
            YtNode.make(
                cluster_name=yt_cluster.name,
                path=folder_path + "/folder3/folder/document",
                node_type=YtNode.Type.DOCUMENT,
                exists=True,
                attributes={},
            )
        )
        yt_cluster._fix_folders()

        for suffix in ("", "/folder2", "/folder2/folder", "/folder2/folder/folder", "/folder3", "/folder3/folder"):
            assert yt_cluster.nodes[folder_path + suffix].node_type == YtNode.Type.FOLDER
            assert yt_cluster.nodes[folder_path + suffix].is_implicit

    def test_no_useless_folders_on_path(self, folder_path: str, yt_cluster: YtCluster):
        for suffix, node_type in (
            ("/folder1", YtNode.Type.FOLDER),
            ("/folder1/folder/folder", YtNode.Type.ANY),
            ("/folder1/folder/folder/file", YtNode.Type.FILE),
            ("/folder1/folder/folder/folder/file", YtNode.Type.FILE),
            ("/folder2/file", YtNode.Type.FILE),
        ):
            yt_cluster.add_node(
                YtNode.make(
                    cluster_name=yt_cluster.name,
                    path=folder_path + suffix,
                    node_type=node_type,
                    exists=True,
                    attributes={},
                )
            )
        yt_cluster._fix_folders()

        assert yt_cluster.nodes[folder_path + "/folder1"].node_type == YtNode.Type.FOLDER
        assert not yt_cluster.nodes[folder_path + "/folder1"].is_implicit
        assert yt_cluster.nodes[folder_path + "/folder1/folder/folder"].node_type == YtNode.Type.ANY
        for suffix in ("", "/folder1/folder", "/folder1/folder/folder/folder", "/folder2"):
            assert yt_cluster.nodes[folder_path + suffix].node_type == YtNode.Type.FOLDER
            assert yt_cluster.nodes[folder_path + suffix].is_implicit

    def test_fix_folders_with_any(self, yt_cluster: YtCluster, folder_path: str):
        yt_cluster.add_node(YtNode.make(yt_cluster.name, folder_path + "/folder1/file", YtNode.Type.FILE, True, {}))
        yt_cluster.add_node(YtNode.make(yt_cluster.name, folder_path + "/folder2/file", YtNode.Type.ANY, True, {}))
        yt_cluster._fix_folders()

        path = folder_path + "/folder1/file"
        assert yt_cluster.nodes[path].node_type == YtNode.Type.FILE
        assert not yt_cluster.nodes[path].is_implicit
        path = folder_path + "/folder2/file"
        assert yt_cluster.nodes[path].node_type == YtNode.Type.ANY
        assert not yt_cluster.nodes[path].is_implicit

        for path in (folder_path, folder_path + "/folder1", folder_path + "/folder2"):
            assert yt_cluster.nodes[path].node_type == YtNode.Type.FOLDER
            assert yt_cluster.nodes[path].is_implicit

    def test_fix_folders_lca(self, yt_cluster: YtCluster, table_attrs: Types.Attributes):
        lca_path = "//folder/folder"
        yt_cluster.add_table(
            YtTable.make("k1", yt_cluster.name, YtTable.Type.TABLE, lca_path + "/folder1/table", True, table_attrs)
        )
        yt_cluster.add_table(
            YtTable.make(
                "k2", yt_cluster.name, YtTable.Type.TABLE, lca_path + "/folder1/folder/folder/table", True, table_attrs
            )
        )
        yt_cluster.add_node(YtNode.make(yt_cluster.name, lca_path + "/folder1/doc", YtNode.Type.DOCUMENT, True, {}))
        yt_cluster.add_node(
            YtNode.make(
                yt_cluster.name, lca_path + "/folder2/link", YtNode.Type.LINK, True, {}, explicit_target_path="//folder"
            )
        )
        yt_cluster._fix_folders()

        for path in (
            lca_path,
            lca_path + "/folder1",
            lca_path + "/folder1/folder",
            lca_path + "/folder1/folder/folder",
            lca_path + "/folder2",
        ):
            assert path in yt_cluster.nodes
            assert yt_cluster.nodes[path].node_type == YtNode.Type.FOLDER

        for node_path in yt_cluster.nodes:
            assert node_path.startswith(lca_path)

    @pytest.mark.parametrize(
        "is_table",
        [
            (True, True),
            (False, True),
            (False, False),
        ],
    )
    def test_forbid_root_lca(self, yt_cluster: YtCluster, table_attrs: Types.Attributes, is_table: tuple[bool, bool]):
        for i in range(2):
            if is_table[i]:
                yt_cluster.add_table(
                    YtTable.make(
                        f"//folder{i}/table",
                        yt_cluster.name,
                        YtTable.Type.TABLE,
                        f"//folder{i}/table",
                        True,
                        table_attrs,
                    )
                )
            else:
                yt_cluster.add_node(YtNode.make(yt_cluster.name, f"//folder{i}/folder", YtNode.Type.FOLDER, True, {}))
        with pytest.raises(AssertionError):
            yt_cluster._fix_folders()

    @pytest.mark.parametrize("node_type", YtNode.Type.all())
    def test_fix_folders_under_root(self, yt_cluster: YtCluster, node_type: YtNode.Type):
        yt_cluster.add_node(
            YtNode.make(yt_cluster.name, f"//{node_type}", node_type, True, {}, explicit_target_path="/")
        )
        if node_type == YtNode.Type.FOLDER:
            yt_cluster.add_node(YtNode.make(yt_cluster.name, f"//{node_type}/file", YtNode.Type.FILE, True, {}))
        yt_cluster._fix_folders()

    def test_attributes_propagation_override(self, table_attrs: Types.Attributes):
        cluster = YtCluster.make(YtCluster.Type.MAIN, "c", False)
        cluster.add_node(
            YtNode.make(cluster.name, "//tmp/folder", YtNode.Type.FOLDER, True, {"attr": "value", "removed": None})
        )
        cluster.add_node(
            YtNode.make(
                cluster.name, "//tmp/folder/folder", YtNode.Type.FOLDER, True, {"attr": "new_value", "removed": "value"}
            )
        )
        cluster.add_node(
            YtNode.make(cluster.name, "//tmp/folder/removed", YtNode.Type.FOLDER, True, {"new_attr": "value"})
        )
        for folder in ("folder", "removed"):
            for node_type in YtNode.Type.all():
                cluster.add_node(
                    YtNode.make(
                        cluster.name,
                        f"//tmp/folder/{folder}/{node_type}",
                        node_type,
                        True,
                        {},
                        explicit_target_path="//tmp/folder" if node_type == YtNode.Type.LINK else None,
                    )
                )
            table_path = f"//tmp/folder/{folder}/table"
            cluster.add_table_from_attributes(table_path, YtTable.Type.TABLE, table_path, True, table_attrs)
        cluster._propagate_attributes()

        assert cluster.nodes["//tmp/folder"].attributes == YtNodeAttributes(
            attributes={"attr": "value", "removed": None}
        )
        assert cluster.nodes["//tmp/folder/folder"].attributes == YtNodeAttributes(
            attributes={"attr": "new_value", "removed": "value"},
        )
        assert cluster.nodes["//tmp/folder/removed"].attributes == YtNodeAttributes(
            attributes={"attr": "value", "new_attr": "value", "removed": None},
            propagated_attributes={"attr": "//tmp/folder", "removed": "//tmp/folder"},
        )
        for node_type in YtNode.Type.all():
            assert cluster.nodes[f"//tmp/folder/folder/{node_type}"].attributes == YtNodeAttributes(
                attributes={"attr": "new_value", "removed": "value"},
                propagated_attributes={"attr": "//tmp/folder/folder", "removed": "//tmp/folder/folder"},
            )
            assert cluster.nodes[f"//tmp/folder/removed/{node_type}"].attributes == YtNodeAttributes(
                attributes={"attr": "value", "new_attr": "value", "removed": None},
                propagated_attributes={
                    "attr": "//tmp/folder",
                    "new_attr": "//tmp/folder/removed",
                    "removed": "//tmp/folder",
                },
            )
        assert cluster.tables["//tmp/folder/folder/table"].attributes == YtTableAttributes(
            attributes={"attr": "new_value", "removed": "value"} | {"dynamic": True},
            propagated_attributes={"attr": "//tmp/folder/folder", "removed": "//tmp/folder/folder"},
        )
        assert cluster.tables["//tmp/folder/removed/table"].attributes == YtTableAttributes(
            attributes={"attr": "value", "new_attr": "value", "removed": None} | {"dynamic": True},
            propagated_attributes={
                "attr": "//tmp/folder",
                "new_attr": "//tmp/folder/removed",
                "removed": "//tmp/folder",
            },
        )

    def test_attributes_propagation_new_key(self, table_attrs: Types.Attributes):
        cluster = YtCluster.make(YtCluster.Type.MAIN, "c", False)
        cluster.add_node(YtNode.make(cluster.name, "//tmp/folder", YtNode.Type.FOLDER, True, {"attribute": "value"}))
        cluster.add_node(
            YtNode.make(cluster.name, "//tmp/folder/folder", YtNode.Type.FOLDER, True, {"other_attribute": "value"})
        )
        cluster.add_table_from_attributes(
            "//tmp/folder/folder/table", YtTable.Type.TABLE, "//tmp/folder/folder/table", True, table_attrs
        )

        cluster._propagate_attributes()

        assert cluster.nodes["//tmp/folder"].attributes == YtNodeAttributes(attributes={"attribute": "value"})
        assert cluster.nodes["//tmp/folder/folder"].attributes == YtNodeAttributes(
            attributes={"attribute": "value", "other_attribute": "value"},
            propagated_attributes={"attribute": "//tmp/folder"},
        )
        assert cluster.tables["//tmp/folder/folder/table"].attributes == YtTableAttributes(
            attributes={"attribute": "value", "other_attribute": "value"} | {"dynamic": True},
            propagated_attributes={"attribute": "//tmp/folder", "other_attribute": "//tmp/folder/folder"},
        )

    def test_attributes_propagation_with_gaps(self, table_attrs: Types.Attributes):
        cluster = YtCluster.make(YtCluster.Type.MAIN, "c", False)
        cluster.add_node(YtNode.make(cluster.name, "//tmp/folder", YtNode.Type.FOLDER, True, {"attribute": "value"}))
        cluster.add_table_from_attributes(
            "//tmp/folder/folder/table", YtTable.Type.TABLE, "//tmp/folder/folder/table", True, table_attrs
        )

        cluster._propagate_attributes()

        assert cluster.nodes["//tmp/folder"].attributes == YtNodeAttributes(attributes={"attribute": "value"})
        assert not cluster.nodes.get("//tmp/folder/folder", None)
        assert cluster.tables["//tmp/folder/folder/table"].attributes == YtTableAttributes(
            attributes={"attribute": "value"} | {"dynamic": True},
            propagated_attributes={"attribute": "//tmp/folder"},
        )

    @pytest.mark.parametrize("use_any", [True, False])
    def test_attributes_propagation_with_filter(self, table_attrs: Types.Attributes, use_any: bool):
        cluster = YtCluster.make(YtCluster.Type.MAIN, "c", False)

        def _add_folder(path: str, attrs: Types.Attributes, propagated_attrs: set[str] | None = None):
            cluster.add_node(
                YtNode.make(
                    cluster.name,
                    path,
                    YtNode.Type.FOLDER if not use_any else YtNode.Type.ANY,
                    True,
                    attrs,
                    propagated_attributes=propagated_attrs,
                )
            )

        _add_folder(
            "//tmp/folder",
            {
                "attr_update": "value",
                "attr_remove": "value",
                "attr_stop": "value",
                "attr_propagate": "value",
            },
            {"attr_update", "attr_remove", "attr_propagate"},
        )
        _add_folder("//tmp/folder/folder1", {"attr_update": "new_value", "attr_remove": None})
        _add_folder("//tmp/folder/folder1/folder", {}, {"attr_update", "attr_propagate"})
        cluster.add_node(
            YtNode.make(
                cluster.name,
                "//tmp/folder/folder1/folder/file",
                YtNode.Type.FILE if not use_any else YtNode.Type.ANY,
                True,
                {},
            )
        )
        _add_folder("//tmp/folder/folder2", {}, {"attr_propagate", "unknown"})
        cluster.add_table_from_attributes("table", YtTable.Type.TABLE, "//tmp/folder/folder2/table", True, table_attrs)
        cluster.finalize()

        assert cluster.nodes["//tmp/folder"].attributes == YtNodeAttributes(
            attributes={
                "attr_update": "value",
                "attr_remove": "value",
                "attr_stop": "value",
                "attr_propagate": "value",
            },
        )
        assert cluster.nodes["//tmp/folder/folder1"].attributes == YtNodeAttributes(
            attributes={
                "attr_update": "new_value",
                "attr_remove": None,
                "attr_propagate": "value",
            },
            propagated_attributes={
                "attr_propagate": "//tmp/folder",
            },
        )
        assert cluster.nodes["//tmp/folder/folder1/folder"].attributes == YtNodeAttributes(
            attributes={
                "attr_update": "new_value",
                "attr_remove": None,
                "attr_propagate": "value",
            },
            propagated_attributes={
                "attr_update": "//tmp/folder/folder1",
                "attr_remove": "//tmp/folder/folder1",
                "attr_propagate": "//tmp/folder",
            },
        )
        assert cluster.nodes["//tmp/folder/folder1/folder/file"].attributes == YtNodeAttributes(
            attributes={
                "attr_update": "new_value",
                "attr_propagate": "value",
            },
            propagated_attributes={
                "attr_update": "//tmp/folder/folder1",
                "attr_propagate": "//tmp/folder",
            },
        )
        assert cluster.nodes["//tmp/folder/folder2"].attributes == YtNodeAttributes(
            attributes={
                "attr_update": "value",
                "attr_remove": "value",
                "attr_propagate": "value",
            },
            propagated_attributes={
                "attr_update": "//tmp/folder",
                "attr_remove": "//tmp/folder",
                "attr_propagate": "//tmp/folder",
            },
        )
        assert cluster.tables["table"].attributes == YtTableAttributes(
            attributes={
                "attr_propagate": "value",
                "dynamic": True,
            },
            propagated_attributes={"attr_propagate": "//tmp/folder"},
        )

    def test_attributes_no_propagation(self):
        cluster = YtCluster.make(YtCluster.Type.MAIN, "c", False)

        def _add_folder(path: str, attrs: Types.Attributes, propagated_attrs: set[str] | None = None):
            cluster.add_node(
                YtNode.make(
                    cluster.name,
                    path,
                    YtNode.Type.FOLDER,
                    True,
                    attrs,
                    propagated_attributes=propagated_attrs,
                )
            )

        _add_folder("//tmp/folder", {"attr": "value"})
        _add_folder("//tmp/folder/folder1", {}, propagated_attrs=None)
        cluster.add_node(
            YtNode.make(
                cluster.name,
                "//tmp/folder/folder1/file",
                YtNode.Type.FILE,
                True,
                {},
            )
        )
        _add_folder("//tmp/folder/folder2", {}, propagated_attrs=set())
        cluster.add_node(
            YtNode.make(
                cluster.name,
                "//tmp/folder/folder2/file",
                YtNode.Type.FILE,
                True,
                {},
            )
        )
        cluster.finalize()

        assert cluster.nodes["//tmp/folder/folder1/file"].attributes == YtNodeAttributes(
            attributes={"attr": "value"}, propagated_attributes={"attr": "//tmp/folder"}
        )
        assert cluster.nodes["//tmp/folder/folder2/file"].attributes == YtNodeAttributes()

    def test_remove_ancestors_of_any(self, yt_cluster: YtCluster):
        """
        folder
        -- 1
        -- -- any
        -- -- 2
        -- -- -- file
        -- -- -- 3 [attr = value]
        -- -- -- -- 41
        -- -- -- -- -- any
        -- -- -- -- 42
        -- -- -- -- -- file
        """
        yt_cluster.add_node(YtNode.make(yt_cluster.name, "//tmp/folder/1/any", YtNode.Type.ANY, True, {}))
        yt_cluster.add_node(YtNode.make(yt_cluster.name, "//tmp/folder/1/2/file", YtNode.Type.FILE, True, {}))
        yt_cluster.add_node(
            YtNode.make(yt_cluster.name, "//tmp/folder/1/2/3", YtNode.Type.FOLDER, True, {"attr": "value"})
        )
        yt_cluster.add_node(YtNode.make(yt_cluster.name, "//tmp/folder/1/2/3/41/any", YtNode.Type.ANY, True, {}))
        yt_cluster.add_node(YtNode.make(yt_cluster.name, "//tmp/folder/1/2/3/42/file", YtNode.Type.FILE, True, {}))

        yt_cluster.finalize()

        for path in (
            "//tmp/folder/1",
            "//tmp/folder/1/2",
        ):
            assert path not in yt_cluster.nodes

        assert yt_cluster.nodes["//tmp/folder/1/2/file"].node_type == YtNode.Type.FILE
        for path in (
            "//tmp/folder/1/2/3",
            "//tmp/folder/1/2/3/41",
            "//tmp/folder/1/2/3/42",
        ):
            assert yt_cluster.nodes[path].node_type == YtNode.Type.FOLDER
            assert yt_cluster.nodes[path].is_implicit == (path != "//tmp/folder/1/2/3")

        for path in (
            "//tmp/folder/1/2/3",
            "//tmp/folder/1/2/3/41",
            "//tmp/folder/1/2/3/41/any",
            "//tmp/folder/1/2/3/42",
            "//tmp/folder/1/2/3/42/file",
        ):
            assert not yt_cluster.nodes[path].attributes.has_diff_with(YtNodeAttributes.make({"attr": "value"}))
