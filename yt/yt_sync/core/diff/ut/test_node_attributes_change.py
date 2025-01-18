from copy import deepcopy

import pytest

from yt.yt_sync.core.diff import NodeAttributesChange
from yt.yt_sync.core.helpers import get_dummy_logger
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.model import YtNode

from .helpers import DiffDbTestBase


class TestNodeAttributesDiff(DiffDbTestBase):
    @pytest.fixture
    def affected_cluster(self) -> str:
        return "remote0"

    def test_empty_diff(self, desired_db: YtDatabase, affected_cluster: str, folder_path: str):
        actual_db = deepcopy(desired_db)

        diff = NodeAttributesChange.make(
            desired_db.clusters[affected_cluster].nodes[folder_path],
            actual_db.clusters[affected_cluster].nodes[folder_path],
        )
        assert diff.is_empty()

    def test_empty_diff_on_missing_node(self, desired_db: YtDatabase, affected_cluster: str, folder_path: str):
        actual_db = deepcopy(desired_db)

        actual_db.clusters[affected_cluster].nodes[folder_path].exists = False

        diff = NodeAttributesChange.make(
            desired_db.clusters[affected_cluster].nodes[folder_path],
            actual_db.clusters[affected_cluster].nodes[folder_path],
        )
        assert diff.is_empty()

    def test_creation_has_no_diff(self, desired_db: YtDatabase, affected_cluster: str, folder_path: str):
        actual_db = deepcopy(desired_db)

        del desired_db.clusters[affected_cluster].nodes[folder_path].attributes.attributes["attribute"]
        actual_db.clusters[affected_cluster].nodes[folder_path].exists = False

        diff = NodeAttributesChange.make(
            desired_db.clusters[affected_cluster].nodes[folder_path],
            actual_db.clusters[affected_cluster].nodes[folder_path],
        )
        assert diff.is_empty()

    def test_added_attribute(self, desired_db: YtDatabase, affected_cluster: str, folder_path: str):
        actual_db = deepcopy(desired_db)
        actual_db.clusters[affected_cluster].nodes[folder_path].attributes["attribute"] = None

        diff = NodeAttributesChange.make(
            desired_db.clusters[affected_cluster].nodes[folder_path],
            actual_db.clusters[affected_cluster].nodes[folder_path],
        )
        assert not diff.is_empty()
        assert diff.check_and_log(get_dummy_logger())
        assert diff.get_changes_as_dict() == {"attribute": ("value", None)}

    def test_removed_attribute(self, desired_db: YtDatabase, affected_cluster: str, folder_path: str):
        actual_db = deepcopy(desired_db)
        desired_db.clusters[affected_cluster].nodes[folder_path].attributes["attribute"] = None

        diff = NodeAttributesChange.make(
            desired_db.clusters[affected_cluster].nodes[folder_path],
            actual_db.clusters[affected_cluster].nodes[folder_path],
        )
        assert not diff.is_empty()
        assert diff.check_and_log(get_dummy_logger())
        assert diff.get_changes_as_dict() == {"attribute": (None, "value")}

    def test_different_types(self, desired_db: YtDatabase, affected_cluster: str, folder_path: str):
        actual_db = deepcopy(desired_db)
        desired_db.clusters[affected_cluster].nodes[folder_path].node_type = "special"
        desired_db.clusters[affected_cluster].nodes[folder_path].attributes["attribute"] = None

        diff = NodeAttributesChange.make(
            desired_db.clusters[affected_cluster].nodes[folder_path],
            actual_db.clusters[affected_cluster].nodes[folder_path],
        )
        assert not diff.is_empty()
        assert not diff.check_and_log(get_dummy_logger())

    @pytest.mark.parametrize("node_type", YtNode.Type.all() + ["int64_type"])
    def test_any_types(self, desired_db: YtDatabase, affected_cluster: str, folder_path: str, node_type: str):
        actual_db = deepcopy(desired_db)
        desired_db.clusters[affected_cluster].nodes[folder_path].node_type = YtNode.Type.ANY
        actual_db.clusters[affected_cluster].nodes[folder_path].attributes["attribute"] = None
        actual_db.clusters[affected_cluster].nodes[folder_path].node_type = node_type

        diff = NodeAttributesChange.make(
            desired_db.clusters[affected_cluster].nodes[folder_path],
            actual_db.clusters[affected_cluster].nodes[folder_path],
        )
        assert not diff.is_empty()
        assert diff.check_and_log(get_dummy_logger())
        assert diff.get_changes_as_dict() == {"attribute": ("value", None)}

    @pytest.mark.parametrize("node_type", [YtNode.Type.LINK, YtNode.Type.ANY])
    def test_link_change_target(self, desired_db: YtDatabase, affected_cluster: str, folder_path: str, node_type: str):
        actual_db = deepcopy(desired_db)
        link_path = f"{folder_path}/link"
        desired_db.clusters[affected_cluster].add_node(
            YtNode.make(
                cluster_name=affected_cluster,
                path=link_path,
                node_type=node_type,
                exists=True,
                attributes={},
                explicit_target_path=folder_path + "_other",
            )
        )
        actual_db.clusters[affected_cluster].add_node(
            YtNode.make(
                cluster_name=affected_cluster,
                path=link_path,
                node_type=YtNode.Type.LINK,
                exists=True,
                attributes={},
                explicit_target_path=folder_path,
            )
        )

        diff = NodeAttributesChange.make(
            desired_db.clusters[affected_cluster].nodes[link_path],
            actual_db.clusters[affected_cluster].nodes[link_path],
        )
        assert not diff.is_empty()
        if node_type == YtNode.Type.LINK:
            assert diff.check_and_log(get_dummy_logger())
            assert diff.get_changes_as_dict() == {"target_path": (folder_path + "_other", folder_path)}
        else:
            assert not diff.check_and_log(get_dummy_logger())

    def test_change_attribute_on_any(self, desired_db: YtDatabase, affected_cluster: str, folder_path: str):
        actual_db = deepcopy(desired_db)
        desired_node = desired_db.clusters[affected_cluster].nodes[folder_path]
        desired_node.node_type = YtNode.Type.ANY
        desired_node.attributes["attribute"] = "new_value"

        diff = NodeAttributesChange.make(
            desired_db.clusters[affected_cluster].nodes[folder_path],
            actual_db.clusters[affected_cluster].nodes[folder_path],
        )
        assert not diff.is_empty()
        assert diff.check_and_log(get_dummy_logger())
        assert diff.get_changes_as_dict() == {"attribute": ("new_value", "value")}
