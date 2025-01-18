from __future__ import annotations

from dataclasses import dataclass
from logging import Logger

from yt.yt_sync.core.model import YtNode

from .base import NodeDiffBase
from .base import NodeDiffType


@dataclass
class MissingNode(NodeDiffBase):
    desired_node: YtNode
    actual_node: YtNode

    def is_empty(self) -> bool:
        return self.actual_node.exists

    def check_and_log(self, log: Logger) -> bool:
        if self.desired_node.node_type == YtNode.Type.ANY:
            log.error("  cannot create node at %s with implicit type", self.desired_node.rich_path)
            return False
        if self.desired_node.node_type != self.actual_node.node_type:
            log.error(
                "  type mismatch for missing node at %s (desired: '%s', actual: '%s')",
                self.desired_node.rich_path,
                self.desired_node.node_type,
                self.actual_node.node_type,
            )
        log.warning("  missing node at %s", self.desired_node.rich_path)
        return True

    @classmethod
    def make(cls, desired_node: YtNode, actual_node: YtNode) -> MissingNode:
        assert (
            desired_node.cluster_name == actual_node.cluster_name
        ), f"Nodes at '{desired_node.rich_path}' and '{actual_node.rich_path}' are from different clusters"
        assert (
            desired_node.path == actual_node.path
        ), f"Nodes at '{desired_node.rich_path}' and '{actual_node.rich_path}' have different paths"

        return cls(
            diff_type=NodeDiffType.MISSING_NODE,
            desired_node=desired_node,
            actual_node=actual_node,
        )
