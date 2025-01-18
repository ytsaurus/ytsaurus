from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from yt.yt_sync.core.spec import Node

from .helpers import get_folder
from .node_attributes import YtNodeAttributes
from .table import YtTable
from .types import Types


@dataclass
class YtNode:
    Type = Node.Type

    cluster_name: str
    path: str
    node_type: str
    exists: bool
    attributes: YtNodeAttributes
    propagated_attributes: set[str] | None
    link_target_path: str | None

    is_implicit: bool
    """
    - Desired: marks unmanaged folders from nodes to LCA
    - Actual: marks unmanaged and unaccessible folders from nodes to LCA
    """

    @property
    def readable_node_type(self) -> str:
        if self.node_type == YtNode.Type.FOLDER:
            return "folder"
        return str(self.node_type)

    @property
    def yt_attributes(self) -> Types.Attributes:
        attributes = self.attributes.yt_attributes
        if self.node_type == YtNode.Type.LINK:
            attributes["target_path"] = self.link_target_path
        return attributes

    @property
    def folder(self) -> str:
        parent = get_folder(self.path)
        assert parent, "Node cannot be root"
        return parent

    @property
    def rich_path(self) -> str:
        return f"{self.cluster_name}:{self.path}"

    @property
    def replica_key(self) -> Types.ReplicaKey:
        return (self.cluster_name, self.path)

    @classmethod
    def node_types(cls) -> list[str]:
        return cls.Type.all()

    @classmethod
    def make(
        cls,
        cluster_name: str,
        path: str,
        node_type: str,
        exists: bool,
        attributes: Types.Attributes,
        filter_: YtNodeAttributes | None = None,
        propagated_attributes: set[str] | None = None,
        explicit_target_path: str | None = None,
        is_implicit: bool = False,
    ) -> YtNode:
        assert path != "/", "Root folder must be configured manually"
        assert node_type not in YtTable.Type.all(), f"Use YtTable class for table at '{cluster_name}:{path}'"

        if node_type == YtNode.Type.LINK:
            assert explicit_target_path, f"Empty explicit target path for link at '{cluster_name}:{path}'"
        if is_implicit:
            assert (
                node_type == YtNode.Type.FOLDER
            ), f"Only folder may be marked as implicit (node at '{cluster_name}:{path}')"

        assert (
            attributes.pop("target_path", None) is None
        ), "Target path attribute must be passed by 'explicit_target_path' parameter"
        return cls(
            cluster_name=cluster_name,
            path=path,
            node_type=node_type,
            exists=exists,
            attributes=YtNodeAttributes.make(deepcopy(attributes), filter_),
            propagated_attributes=propagated_attributes,
            link_target_path=explicit_target_path,
            is_implicit=is_implicit,
        )

    @classmethod
    def request_opaque_attrs(cls) -> list[str]:
        return []

    def apply_opaque_attribute(self, attr_name: str, attr_value: Any):
        self.attributes.set_value(attr_name, attr_value)
