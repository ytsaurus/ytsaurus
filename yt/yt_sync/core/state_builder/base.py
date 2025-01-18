from abc import ABC
from abc import abstractmethod

from yt.yt_sync.core.client import YtClientFactory
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.model import YtNativeConsumer
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.settings import Settings
from yt.yt_sync.core.table_filter import DefaultTableFilter
from yt.yt_sync.core.table_filter import TableFilterBase


class DesiredStateBuilderBase(ABC):
    def __init__(
        self,
        yt_client_factory: YtClientFactory,
        db: YtDatabase,
        is_chaos: bool,
        fix_implicit_replicated_queue_attrs: bool,
        table_filter: TableFilterBase | None = None,
    ):
        self.yt_client_factory: YtClientFactory = yt_client_factory
        self.db = db
        self.is_chaos = is_chaos
        self.fix_implicit_replicated_queue_attrs: bool = fix_implicit_replicated_queue_attrs
        self.table_filter: TableFilterBase = table_filter or DefaultTableFilter()

    @abstractmethod
    def add_table(self, table_settings: Types.Attributes) -> YtTable:
        raise NotImplementedError()

    @abstractmethod
    def add_node(self, node_settings: Types.Attributes):
        raise NotImplementedError()

    @abstractmethod
    def add_consumer(self, consumer_settings: Types.Attributes) -> YtNativeConsumer:
        raise NotImplementedError()

    @abstractmethod
    def add_producer(self, producer_settings: Types.Attributes):
        raise NotImplementedError()

    @abstractmethod
    def add_pipeline(self, pipeline_specification: Types.Attributes):
        raise NotImplementedError()

    @abstractmethod
    def finalize(self, settings: Settings | None = None):
        raise NotImplementedError()
