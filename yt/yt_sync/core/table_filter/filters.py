import fnmatch
from typing import Iterable

from yt.yt_sync.core.model import get_node_name

from .base import TableFilterBase


class DefaultTableFilter(TableFilterBase):
    def is_ok(self, _: str) -> bool:
        return True


class TableNameFilter(TableFilterBase):
    def __init__(self, filtered_tables: Iterable[str] = list()):
        super().__init__()
        self._filter = frozenset(filtered_tables)

    def is_ok(self, table_key: str) -> bool:
        if not self._filter:
            return True
        table_name = get_node_name(table_key)
        for pattern in self._filter:
            if fnmatch.fnmatch(table_name, pattern):
                return True
        return False
