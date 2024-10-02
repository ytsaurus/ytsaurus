import pytest

from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtNode
from yt.yt_sync.core.model import YtNodeAttributes


class TestYtNode:
    @pytest.mark.parametrize("exists", [True, False])
    @pytest.mark.parametrize("node_type", set(YtNode.Type.all()).difference({YtNode.Type.LINK}))
    @pytest.mark.parametrize("attributes", [{}, {"attribute": "value"}])
    def test_make(
        self,
        folder_path: str,
        exists: bool,
        node_type: str,
        attributes: Types.Attributes,
    ):
        yt_node = YtNode.make(
            cluster_name="primary",
            path=folder_path,
            node_type=node_type,
            exists=exists,
            attributes=attributes,
        )
        assert yt_node.cluster_name == "primary"
        assert yt_node.path == folder_path
        assert yt_node.exists == exists
        assert yt_node.node_type == node_type
        assert not yt_node.attributes.has_diff_with(YtNodeAttributes.make(attributes))

    @pytest.mark.parametrize("exists", [True, False])
    def test_make_link(self, folder_path: str, exists: bool):
        yt_node = YtNode.make(
            cluster_name="primary",
            path=folder_path,
            node_type=YtNode.Type.LINK,
            exists=exists,
            attributes={},
            explicit_target_path="//some/path",
        )
        assert yt_node.cluster_name == "primary"
        assert yt_node.path == folder_path
        assert yt_node.exists == exists
        assert yt_node.node_type == YtNode.Type.LINK
        assert yt_node.yt_attributes == {"target_path": "//some/path"}
        assert yt_node.link_target_path == "//some/path"

    @pytest.mark.parametrize("exists", [True, False])
    def test_make_link_no_target(self, folder_path: str, exists: bool):
        with pytest.raises(AssertionError):
            YtNode.make(
                cluster_name="primary",
                path=folder_path,
                node_type=YtNode.Type.LINK,
                exists=exists,
                attributes={"attribute": "value"},
            )
