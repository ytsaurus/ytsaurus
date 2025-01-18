from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .table import Table


@dataclass
class Producer:
    """
    Specification for YT producer.

    See https://ytsaurus.tech/docs/ru/user-guide/dynamic-tables/queues#data_model for details.
    """

    table: Optional[Table] = None
    """
    Specification for producer table. Mandatory attribute.

    See :py:class:`Table` for details.
    """
