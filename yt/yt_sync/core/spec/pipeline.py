from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .node import Node
from .table import Table


@dataclass
class Pipeline:
    """
    Specification for YT Flow pipeline with its files and tables.
    Never use it in a direct way. Use easy mode to create pipelines.
    """

    path: Optional[str] = None
    """Root of pipeline. Mandatory attribute."""

    monitoring_project: Optional[str] = None
    """For YT UI to show right monitoring metrics."""

    monitoring_cluster: Optional[str] = None
    """For YT UI to show right monitoring metrics."""

    table_spec: Optional[dict[str, Table]] = None
    """Specs for pipeline tables."""

    queue_spec: Optional[dict[str, Table]] = None
    """Specs for pipeline queues."""

    file_spec: Optional[dict[str, Node]] = None
    """Specs for pipeline files."""
