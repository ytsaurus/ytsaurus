from abc import ABC
from abc import abstractmethod
from typing import Generator
from typing import Iterable

from .model import get_node_name
from .model import YtCluster
from .model import YtTable


class TableFilterBase(ABC):
    def __call__(self, cluster: YtCluster) -> Generator[YtTable, None, None]:
        for table_key in sorted(cluster.tables):
            table = cluster.tables[table_key]
            if self.is_ok(table.key):
                yield table

    @abstractmethod
    def is_ok(self, table_key: str) -> bool:
        raise NotImplementedError()


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
        return table_name in self._filter
