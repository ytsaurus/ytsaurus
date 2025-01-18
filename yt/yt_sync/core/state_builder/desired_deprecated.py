from copy import deepcopy
import logging
from typing import Union

from yt.yt_sync.core.client import YtClientFactory
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtCluster
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.model import YtNativeConsumer
from yt.yt_sync.core.model import YtNode
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.model import YtTableAttributes
from yt.yt_sync.core.model.table import QueueOptions
from yt.yt_sync.core.settings import Settings
from yt.yt_sync.core.table_filter import TableFilterBase

from .base import DesiredStateBuilderBase
from .helpers import get_cluster_name

LOG = logging.getLogger("yt_sync")


class DesiredStateBuilderDeprecated(DesiredStateBuilderBase):
    def __init__(
        self,
        yt_client_factory: YtClientFactory,
        db: YtDatabase,
        is_chaos: bool,
        fix_implicit_replicated_queue_attrs: bool,
        table_filter: TableFilterBase | None = None,
    ):
        super().__init__(yt_client_factory, db, is_chaos, fix_implicit_replicated_queue_attrs, table_filter)
        self._implicit_clusters: dict[str, YtCluster] = dict()

    def _add_table(self, table_settings: Types.Attributes) -> YtTable | None:
        assert "path" in table_settings, "Mandatory attribute 'path' is absent"
        table_path = table_settings["path"]
        if not self.table_filter.is_ok(table_path):
            return
        assert "master" in table_settings, f"Mandatory section 'master' is absent for '{table_path}' settings"

        is_chaos = table_settings.get("use_chaos", False)
        replicated = bool(table_settings.get("async_replicas") or table_settings.get("sync_replicas"))
        queue_options = QueueOptions(is_chaos=is_chaos, is_replicated=replicated).make_updated(
            table_settings["master"].get("attributes", {})
        )

        async_replicas = self._add_replicas(
            table_path,
            YtCluster.Type.REPLICA,
            YtCluster.Mode.ASYNC,
            table_settings.get("async_replicas"),
            queue_options,
        )
        sync_replicas = self._add_replicas(
            table_path, YtCluster.Type.REPLICA, YtCluster.Mode.SYNC, table_settings.get("sync_replicas"), queue_options
        )
        main_table = self._add_main(table_settings["master"], table_path, sync_replicas, async_replicas, queue_options)

        return main_table

    def _add_table_at_cluster(
        self,
        default_cluster: YtCluster,
        table_key: str,
        table_type: str,
        table_path: str,
        table_attributes: Types.Attributes,
        source_attributes: YtTableAttributes | None = None,
        queue_options: QueueOptions | None = None,
    ) -> YtTable:
        yt_cluster = self.db.add_or_get_cluster(default_cluster)
        if yt_cluster.name in self._implicit_clusters:
            # TODO: Maybe can be moved by pure assignment?
            for node in self._implicit_clusters[yt_cluster.name].nodes.values():
                yt_cluster.add_node(node)
            del self._implicit_clusters[yt_cluster.name]

        return yt_cluster.add_table_from_attributes(
            table_key,
            table_type,
            table_path,
            True,
            table_attributes,
            source_attributes,
            queue_options=queue_options if self.fix_implicit_replicated_queue_attrs else None,
        )

    def add_table(self, table_settings: Types.Attributes) -> YtTable:
        return self._add_table(table_settings)

    def _add_node_at_cluster(self, node: YtNode) -> YtNode:
        if node.cluster_name not in self.db.clusters:
            cluster = self._implicit_clusters.setdefault(
                node.cluster_name,
                YtCluster.make(
                    cluster_type=YtCluster.Type.REPLICA,
                    name=node.cluster_name,
                    is_chaos=False,
                    mode=YtCluster.Mode.ASYNC,
                ),
            )
        else:
            cluster = self.db.clusters[node.cluster_name]

        return cluster.add_node(node)

    def add_node(self, node_settings: Types.Attributes):
        assert "type" in node_settings, "Mandatory attribute 'type' is absent"
        node_type = node_settings["type"]
        assert node_type in YtNode.node_types(), f"Unknown node type {node_type}"
        assert "clusters" in node_settings, "Mandatory section 'clusters' is absent"

        for raw_cluster_name, cluster_settings in node_settings["clusters"].items():
            cluster_name = get_cluster_name(self.yt_client_factory, raw_cluster_name)
            assert "path" in cluster_settings, "Mandatory attribute 'path' is absent"
            path = cluster_settings["path"]
            assert "attributes" in cluster_settings, "Mandatory section 'attributes' is absent"
            attributes = cluster_settings["attributes"]

            self._add_node_at_cluster(
                YtNode.make(
                    cluster_name=cluster_name,
                    path=path,
                    node_type=node_type,
                    exists=True,
                    attributes=attributes,
                    propagated_attributes=cluster_settings.get("propagated_attributes", None),
                    explicit_target_path=attributes.pop("target_path", None),
                )
            )

    def add_consumer(self, consumer_settings: Types.Attributes) -> YtNativeConsumer:
        assert "table_settings" in consumer_settings, "Mandatory attribute 'table_settings' is absent"
        assert "queues" in consumer_settings, "Mandatory attribute 'queues' is absent"
        assert consumer_settings["table_settings"]["master"]["attributes"].get(
            "treat_as_queue_consumer", False
        ), "Mandatory attribute 'treat_as_queue_consumer' in table attributes is absent"
        consumer_table = self._add_table(consumer_settings["table_settings"])
        if consumer_table is None:
            return
        consumer: YtNativeConsumer = YtNativeConsumer(consumer_table)
        for queue in consumer_settings["queues"]:
            consumer.add_registration(queue["cluster"], queue["path"], queue["vital"], queue.get("partitions"))
        cluster_name = get_cluster_name(self.yt_client_factory, consumer_table.cluster_name)
        yt_cluster = self.db.add_or_get_cluster(YtCluster.make(YtCluster.Type.MAIN, cluster_name, self.is_chaos))
        yt_cluster.add_consumer(consumer)
        return consumer

    def add_producer(self, producer_settings: Types.Attributes):
        raise NotImplementedError()

    def add_cluster(self, cluster_type: str, cluster_name: str, is_chaos: bool):
        self.db.add_or_get_cluster(YtCluster.make(cluster_type, cluster_name, is_chaos))

    def add_pipeline(self, pipeline_specification: Types.Attributes):
        raise NotImplementedError()

    def _add_main(
        self,
        attributes: Types.Attributes,
        path: str,
        sync_replicas: set[str],
        async_replicas: set[str],
        queue_options: QueueOptions,
    ) -> YtTable:
        cluster_name = get_cluster_name(self.yt_client_factory, attributes["cluster"])
        # if no replicas, treat tables in main as ordinary tables
        has_replicas: bool = bool(sync_replicas or async_replicas)
        table_type = YtTable.Type.REPLICATED_TABLE if has_replicas else YtTable.Type.TABLE

        main_table = self._add_table_at_cluster(
            YtCluster.make(YtCluster.Type.MAIN if has_replicas else YtCluster.Type.SINGLE, cluster_name, self.is_chaos),
            path,
            YtTable.resolve_table_type(table_type, self.is_chaos),
            path,
            attributes.get("attributes", dict()),
            queue_options=queue_options,
        )
        if has_replicas:
            # fill 'replicas' attribute
            self._patch_main_table_replicas(main_table, sync_replicas, async_replicas)
        return main_table

    def _add_replicas(
        self,
        main_path: str,
        replica_type: YtCluster.Type,
        cluster_mode: YtCluster.Mode,
        attributes: Union[Types.Attributes, None],
        main_queue_options: QueueOptions,
    ) -> set[str]:
        result: set[str] = set()
        if attributes is None:
            return result
        path = attributes.get("replica_path")
        assert path, "Attribute 'replica_path' is mandatory"
        assert "cluster" in attributes, "Attribute 'cluster' is mandatory for replicas"
        for cluster in attributes["cluster"]:
            cluster_name = get_cluster_name(self.yt_client_factory, cluster)

            table_attributes = attributes["attributes"][cluster]
            replica_queue_options = main_queue_options.make_updated(table_attributes)

            table = self._add_table_at_cluster(
                YtCluster.make(replica_type, cluster_name, self.is_chaos, cluster_mode),
                main_path,
                YtTable.Type.TABLE,
                path,
                deepcopy(table_attributes),
                queue_options=replica_queue_options,
            )
            result.add(cluster_name)
            if self.is_chaos:
                self.db.clusters[cluster_name].add_replication_log_for(table)
        return result

    def _patch_main_table_replicas(self, main_table: YtTable, sync_replicas: set[str], async_replicas: set[str]):
        table_key = main_table.key
        for cluster in self.db.clusters.values():
            if cluster.name == main_table.cluster_name:
                continue
            if table_key not in cluster.tables:
                continue
            if cluster.name not in sync_replicas | async_replicas:
                # No replica on cluster for given table
                continue

            main_table.add_replica_for(
                cluster.tables[table_key],
                cluster,
                YtReplica.Mode.SYNC if cluster.name in sync_replicas else YtReplica.Mode.ASYNC,
            )
        main_table.set_rtt_enabled_from_replicas()  # TODO: tests!!

    def _fix_missing_clusters(self):
        for cluster in self._implicit_clusters.values():
            if not self.db.has_main:
                cluster.cluster_type = YtCluster.Type.MAIN
                cluster.mode = None
            LOG.info(f"Cluster '{cluster.name}' is considered implicit with type '{cluster.cluster_type}'")
            real_cluster = self.db.add_or_get_cluster(cluster)
            for node in cluster.nodes.values():
                real_cluster.add_node(node)
        self._implicit_clusters.clear()

    def finalize(self, settings: Settings | None = None):
        self._fix_missing_clusters()
        for cluster in self.db.clusters.values():
            cluster.finalize()

        self.db.ensure_db_integrity(
            settings.always_async if settings else None,
            settings.ensure_rtt_settings if settings else True,
        )
