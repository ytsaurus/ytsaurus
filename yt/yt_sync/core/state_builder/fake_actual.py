from copy import deepcopy

from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.model import YtNode
from yt.yt_sync.core.table_filter import DefaultTableFilter
from yt.yt_sync.core.table_filter import TableFilterBase

from .helpers import link_chaos_tables


class FakeActualStateBuilder:
    def __init__(self, table_filter: TableFilterBase | None = None):
        self._table_filter: TableFilterBase = table_filter or DefaultTableFilter()

    def build_from(self, desired: YtDatabase) -> YtDatabase:
        actual = YtDatabase(is_chaos=desired.is_chaos)
        for desired_cluster in desired.clusters.values():
            actual_cluster = actual.add_or_get_cluster(desired_cluster)
            for node_path in sorted(desired_cluster.nodes):
                node = desired_cluster.nodes[node_path]
                actual_cluster.add_node(
                    YtNode.make(
                        cluster_name=node.cluster_name,
                        path=node.path,
                        node_type=node.node_type,
                        exists=False,
                        attributes={},
                        explicit_target_path=node.link_target_path,
                    )
                )

            for table in self._table_filter(desired_cluster):
                actual_cluster.add_table_from_attributes(
                    table_key=table.key,
                    table_type=table.table_type,
                    table_path=table.path,
                    exists=False,
                    table_attributes={},
                )
            for consumer in desired_cluster.consumers.values():
                fake_consumer = deepcopy(consumer)
                fake_consumer.registrations = dict()
                actual_cluster.add_consumer(fake_consumer)

            if desired.is_chaos and desired_cluster.is_replica:
                link_chaos_tables(desired_cluster, actual_cluster)
        return actual
