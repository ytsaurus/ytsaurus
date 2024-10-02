from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .table import Table


@dataclass
class Consumer:
    """
    Specification for YT consumer.

    See https://ytsaurus.tech/docs/ru/user-guide/dynamic-tables/queues#data_model for details.
    """

    table: Optional[Table] = None
    """
    Specification for consumer table. Mandatory attribute.

    See :py:class:`Table` for details.
    """

    queues: Optional[list[QueueRegistration]] = None
    """
    List of queue registrations for consumer.

    At least one queue registration must be present.

    Queues will be registered for consumer table on main cluster.

    See :py:class:`QueueRegistration` for details.
    """


@dataclass
class QueueRegistration:
    """
    Queue registration for consumer.

    See https://ytsaurus.tech/docs/ru/user-guide/dynamic-tables/queues#registraciya-konsyumera-k-ocheredi for details.
    """

    cluster: Optional[str] = None
    """Cluster where queue is located. Mandatory attribute."""

    path: Optional[str] = None
    """Path of queue table. Mandatory attribute."""

    vital: Optional[bool] = None
    """If consumer is vital.

    See https://ytsaurus.tech/docs/ru/user-guide/dynamic-tables/queues#automatic_trimming for details.
    """

    partitions: Optional[list[int]] = None
    """List of partitions to track by consumer."""
