from __future__ import annotations

from dataclasses import dataclass
from logging import Logger
from typing import Any
from typing import Generator

from yt.yt_sync.core.model import YtNode

from .base import NodeDiffBase
from .base import NodeDiffType


@dataclass
class NodeAttributesChange(NodeDiffBase):
    desired_node: YtNode
    actual_node: YtNode

    def is_empty(self) -> bool:
        return not self.actual_node.exists or (
            (
                self.desired_node.node_type == YtNode.Type.ANY
                or self.desired_node.node_type == self.actual_node.node_type
            )
            and self.desired_node.link_target_path == self.actual_node.link_target_path
            and not self.desired_node.attributes.has_diff_with(self.actual_node.attributes)
        )

    def check_and_log(self, log: Logger) -> bool:
        if self.desired_node.node_type != YtNode.Type.ANY and self.desired_node.node_type != self.actual_node.node_type:
            log.error(
                "  type mismatch for existing node at '%s' (desired: %s, actual: %s)",
                self.desired_node.rich_path,
                self.desired_node.node_type,
                self.actual_node.node_type,
            )
            return False
        if (
            self.desired_node.node_type == YtNode.Type.ANY
            and self.actual_node.node_type == YtNode.Type.LINK
            and self.desired_node.link_target_path != self.actual_node.link_target_path
        ):
            log.error(
                "  link target path must be changed only via link node type for link at '%s'",
                self.desired_node.rich_path,
            )
            return False

        for attribute_path, desired_value, actual_value in self.desired_node.attributes.changed_attributes(
            self.actual_node.attributes
        ):
            if desired_value is None:
                log.warning(
                    "  remove attribute %s/@%s",
                    self.desired_node.rich_path,
                    attribute_path,
                )
            elif actual_value is None:
                log.warning(
                    "  add %s %s/@%s: value=%s",
                    (
                        "propagated attribute"
                        if attribute_path in self.desired_node.attributes.propagated_attributes
                        else "attribute"
                    ),
                    self.desired_node.rich_path,
                    attribute_path,
                    desired_value,
                )
            else:
                log.warning(
                    "  update attribute %s/@%s: actual=%s, desired=%s",
                    self.desired_node.rich_path,
                    attribute_path,
                    actual_value,
                    desired_value,
                )
        return True

    def is_link_path_changed(self) -> bool:
        return (
            self.desired_node.node_type == YtNode.Type.LINK
            and self.desired_node.link_target_path != self.actual_node.link_target_path
        )

    def get_changes(self) -> Generator[tuple[str, Any | None, Any | None], None, None]:
        if self.is_empty():
            return

        if (
            self.desired_node.node_type == YtNode.Type.LINK
            and self.desired_node.link_target_path != self.actual_node.link_target_path
        ):
            yield "target_path", self.desired_node.link_target_path, self.actual_node.link_target_path
        for path, desired, actual in self.desired_node.attributes.changed_attributes(self.actual_node.attributes):
            yield path, desired, actual

    def get_changes_as_dict(self) -> dict[str, tuple[Any | None, Any | None]]:
        diff_dict = dict()
        for path, desired, actual in self.get_changes():
            diff_dict[path] = (desired, actual)
        return diff_dict

    @classmethod
    def make(cls, desired_node: YtNode, actual_node: YtNode) -> NodeAttributesChange:
        assert (
            desired_node.cluster_name == actual_node.cluster_name
        ), f"Nodes at '{desired_node.rich_path}' and '{actual_node.rich_path}' are in different clusters"
        assert (
            desired_node.path == actual_node.path
        ), f"Nodes at '{desired_node.rich_path}' and '{actual_node.rich_path}' have different paths"

        return cls(
            diff_type=NodeDiffType.ATTRIBUTES_CHANGE,
            desired_node=desired_node,
            actual_node=actual_node,
        )
