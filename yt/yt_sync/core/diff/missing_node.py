from __future__ import annotations

from dataclasses import dataclass
from logging import Logger

from yt.yt_sync.core import YtNode

from .base import NodeDiffBase
from .base import NodeDiffType


@dataclass
class MissingNode(NodeDiffBase):
    desired_node: YtNode
    actual_node: YtNode

    def is_empty(self) -> bool:
        return self.actual_node.exists

    def check_and_log(self, log: Logger) -> bool:
        log.warning("  missing node at %s", self.desired_node.rich_path)
        return True

    @classmethod
    def make(cls, desired_node: YtNode, actual_node: YtNode) -> MissingNode:
        assert desired_node.cluster_name == actual_node.cluster_name, "Nodes must be from one cluster"
        assert desired_node.path == actual_node.path, "Nodes must have same path"

        return cls(
            diff_type=NodeDiffType.MISSING_NODE,
            desired_node=desired_node,
            actual_node=actual_node,
        )
