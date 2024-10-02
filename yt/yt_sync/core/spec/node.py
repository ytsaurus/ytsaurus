from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any
from typing import Optional


@dataclass
class Node:
    """
    Specification for YT cypress nodes (like folders, links, etc) over one or several clusters.

    See https://ytsaurus.tech/docs/ru/user-guide/storage/cypress for details.
    """

    class Type(StrEnum):
        FOLDER = "map_node"
        FILE = "file"
        LINK = "link"
        DOCUMENT = "document"

        @classmethod
        def all(cls) -> list[str]:
            return [v.value for v in cls]

    type: Optional[Type] = None
    """
    Type of node.

    Mandatory attribute.

    See https://ytsaurus.tech/docs/ru/user-guide/storage/objects#primitive_types for details.
    """

    clusters: Optional[dict[str, ClusterNode]] = None
    """
    List of clusters where given node is present, at least one cluster is required.

    See :py:class:`ClusterNode` for details.
    """


@dataclass
class ClusterNode:
    """
    Node specification on cluster.
    """

    main: Optional[bool] = None
    """
    Marks main cluster. Must be consistent for all nodes and tables.

    May be omitted for single cluster.
    """

    path: Optional[str] = None
    """Path to node on cluster. Mandatory attribute."""

    attributes: Optional[dict[str, Any]] = None
    """Node attributes on cluster. Optional."""

    target_path: Optional[str] = None
    """Link target path on cluster. Mandatory for links."""
