from abc import ABC
from abc import abstractmethod
from typing import Generator

from yt.yt_sync.core.model import YtCluster
from yt.yt_sync.core.model import YtTable


class TableFilterBase(ABC):
    def __call__(self, cluster: YtCluster) -> Generator[YtTable, None, None]:
        for table_key in sorted(cluster.tables):
            table = cluster.tables[table_key]
            if self.is_ok(table.key):
                yield table

    @abstractmethod
    def is_ok(self, table_key: str) -> bool:
        raise NotImplementedError()
