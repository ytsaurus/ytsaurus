from copy import deepcopy
from typing import Union

from library.python.confmerge import merge_inplace
from yt.yt_sync.core.client import YtClientFactory
from yt.yt_sync.core.constants import PIPELINE_FILES
from yt.yt_sync.core.constants import PIPELINE_QUEUES
from yt.yt_sync.core.constants import PIPELINE_TABLES
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtCluster
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.model import YtNativeConsumer
from yt.yt_sync.core.model import YtNode
from yt.yt_sync.core.model import YtReplica
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.model.table import QueueOptions
from yt.yt_sync.core.settings import Settings
from yt.yt_sync.core.spec import ClusterNode
from yt.yt_sync.core.spec import Consumer
from yt.yt_sync.core.spec import Node
from yt.yt_sync.core.spec import Pipeline
from yt.yt_sync.core.spec import Producer
from yt.yt_sync.core.spec import Table
from yt.yt_sync.core.spec.details import ConsumerSpec
from yt.yt_sync.core.spec.details import NodeSpec
from yt.yt_sync.core.spec.details import PipelineSpec
from yt.yt_sync.core.spec.details import ProducerSpec
from yt.yt_sync.core.spec.details import TableSpec
from yt.yt_sync.core.table_filter import TableFilterBase

from .base import DesiredStateBuilderBase
from .helpers import get_cluster_name


class DesiredStateBuilder(DesiredStateBuilderBase):
    def __init__(
        self,
        yt_client_factory: YtClientFactory,
        db: YtDatabase,
        is_chaos: bool,
        fix_implicit_replicated_queue_attrs: bool,
        table_filter: TableFilterBase | None = None,
    ):
        super().__init__(yt_client_factory, db, is_chaos, fix_implicit_replicated_queue_attrs, table_filter)

    def _assert_main(self, cluster: YtCluster, node_type: str, path: str):
        assert cluster.is_main, f"Expected cluster {cluster.name} to be main, got replica ({node_type}={path})"

    def _assert_replica(self, cluster: YtCluster, node_type: str, path: str):
        assert cluster.is_replica, f"Expected cluster {cluster.name} to be replica, got main ({node_type}={path})"

    def _add_single_table(self, parsed_spec: TableSpec) -> YtTable:
        assert not parsed_spec.is_federation
        cluster_name: str = get_cluster_name(self.yt_client_factory, parsed_spec.main_cluster)
        cluster: YtCluster = self.db.add_or_get_cluster(
            YtCluster.make(YtCluster.Type.MAIN, cluster_name, parsed_spec.chaos)
        )
        main_spec = parsed_spec.main_cluster_spec
        self._assert_main(cluster, "table", main_spec.path)
        table = YtTable.make(
            main_spec.path,
            cluster_name,
            parsed_spec.main_table_type,
            main_spec.path,
            True,
            main_spec.attributes,
            explicit_schema=parsed_spec.schema,
        )
        return cluster.add_table(table)

    def _add_table_federation(self, parsed_spec: TableSpec) -> YtTable:
        assert parsed_spec.is_federation
        main_cluster_name: str = get_cluster_name(self.yt_client_factory, parsed_spec.main_cluster)
        main_cluster: YtCluster = self.db.add_or_get_cluster(
            YtCluster.make(YtCluster.Type.MAIN, main_cluster_name, self.is_chaos)
        )
        main_spec = parsed_spec.main_cluster_spec
        self._assert_main(main_cluster, "table", main_spec.path)
        queue_options = QueueOptions(is_chaos=parsed_spec.chaos, is_replicated=True).make_updated(main_spec.attributes)
        main_table: YtTable = main_cluster.add_table(
            YtTable.make(
                key=main_spec.path,
                cluster_name=main_cluster_name,
                table_type=parsed_spec.main_table_type,
                path=main_spec.path,
                exists=True,
                attributes=main_spec.attributes,
                explicit_schema=parsed_spec.schema,
                explicit_in_collocation=bool(parsed_spec.in_collocation),
                queue_options=queue_options if self.fix_implicit_replicated_queue_attrs else None,
            )
        )
        for raw_cluster_name, cluster_spec in parsed_spec.clusters.items():
            cluster_name = get_cluster_name(self.yt_client_factory, raw_cluster_name)
            if cluster_name == main_cluster_name:
                continue
            replica_cluster: YtCluster = self.db.add_or_get_cluster(
                YtCluster.make(YtCluster.Type.REPLICA, cluster_name, self.is_chaos, YtCluster.Mode.ASYNC)
            )
            self._assert_replica(replica_cluster, "table", cluster_spec.path)
            replica_queue_options = queue_options.make_updated(cluster_spec.attributes)
            replica_table: YtTable = YtTable.make(
                key=main_table.key,
                cluster_name=cluster_name,
                table_type=YtTable.Type.TABLE,
                path=cluster_spec.path,
                exists=True,
                attributes=cluster_spec.attributes,
                explicit_schema=cluster_spec.schema_override or parsed_spec.schema,
                queue_options=replica_queue_options if self.fix_implicit_replicated_queue_attrs else None,
            )
            if cluster_spec.replicated_table_tracker_enabled is not None:
                replica_table.rtt_options.enabled = cluster_spec.replicated_table_tracker_enabled
            replica_cluster.add_table(replica_table)
            if parsed_spec.chaos:
                replication_spec = cluster_spec.replication_log
                log_attributes = YtTable.resolve_replication_log_attributes(replica_table.attributes)
                if replication_spec.attributes:
                    merge_inplace(log_attributes, replication_spec.attributes)
                replication_log: YtTable = YtTable.make(
                    key=TableSpec.make_replication_log_name(main_table.key),
                    cluster_name=cluster_name,
                    table_type=YtTable.Type.REPLICATION_LOG,
                    path=replication_spec.path,
                    exists=True,
                    attributes=log_attributes,
                    explicit_schema=cluster_spec.schema_override or parsed_spec.schema,
                )
                if cluster_spec.replicated_table_tracker_enabled is not None:
                    replication_log.rtt_options.enabled = cluster_spec.replicated_table_tracker_enabled
                replica_cluster.add_replication_log_for(replica_table, replication_log)

            main_table.add_replica_for(
                replica_table,
                replica_cluster,
                YtReplica.Mode.SYNC if cluster_spec.preferred_sync else YtReplica.Mode.ASYNC,
            )
        main_table.set_rtt_enabled_from_replicas()
        return main_table

    def _add_table(self, table_spec: TableSpec) -> YtTable:
        if table_spec.is_federation:
            return self._add_table_federation(table_spec)
        else:
            return self._add_single_table(table_spec)

    def add_table(self, table_specification: Union[Types.Attributes, Table]) -> YtTable:
        parsed_spec: TableSpec = (
            TableSpec(spec=table_specification)
            if isinstance(table_specification, Table)
            else TableSpec.parse(table_specification, with_ensure=False)
        )
        parsed_spec.ensure()
        return self._add_table(parsed_spec)

    def add_consumer(self, consumer_specification: Union[Types.Attributes, Consumer]) -> YtNativeConsumer:
        parsed_spec: ConsumerSpec = (
            ConsumerSpec(spec=consumer_specification)
            if isinstance(consumer_specification, Consumer)
            else ConsumerSpec.parse(consumer_specification, with_ensure=False)
        )
        parsed_spec.ensure()
        consumer_table: YtTable = self._add_table(parsed_spec.table)

        consumer: YtNativeConsumer = YtNativeConsumer(consumer_table)
        for queue in parsed_spec.queues:
            consumer.add_registration(
                get_cluster_name(self.yt_client_factory, queue.cluster),
                queue.path,
                bool(queue.vital),
                queue.partitions,
            )

        yt_cluster = self.db.add_or_get_cluster(
            YtCluster.make(YtCluster.Type.MAIN, consumer_table.cluster_name, self.is_chaos)
        )
        self._assert_main(yt_cluster, "consumer", consumer_table.key)
        yt_cluster.add_consumer(consumer)
        return consumer

    def add_producer(self, producer_specification: Union[Types.Attributes, Producer]):
        parsed_spec: ProducerSpec = (
            ProducerSpec(spec=producer_specification)
            if isinstance(producer_specification, Producer)
            else ProducerSpec.parse(producer_specification, with_ensure=False)
        )
        parsed_spec.ensure()
        self._add_table(parsed_spec.table)

    def add_node(self, node_specification: Union[Types.Attributes, Node]):
        parsed_spec: NodeSpec = (
            NodeSpec(spec=node_specification)
            if isinstance(node_specification, Node)
            else NodeSpec.parse(node_specification, with_ensure=False)
        )
        parsed_spec.ensure()

        for raw_cluster_name, cluster_spec in parsed_spec.clusters.items():
            cluster_name = get_cluster_name(self.yt_client_factory, raw_cluster_name)
            cluster_type = YtCluster.Type.MAIN if cluster_spec.main else YtCluster.Type.REPLICA
            cluster: YtCluster = self.db.add_or_get_cluster(YtCluster.make(cluster_type, cluster_name, self.is_chaos))
            if cluster_spec.main:
                self._assert_main(cluster, "node", cluster_spec.path)
            else:
                self._assert_replica(cluster, "node", cluster_spec.path)

            cluster.add_node(
                YtNode.make(
                    cluster_name=cluster_name,
                    path=cluster_spec.path,
                    node_type=parsed_spec.type,
                    exists=True,
                    attributes=cluster_spec.attributes,
                    propagated_attributes=cluster_spec.propagated_attributes,
                    explicit_target_path=cluster_spec.target_path,
                )
            )

    def add_pipeline(self, pipeline_specification: Union[Types.Attributes, Pipeline]):
        parsed_spec: PipelineSpec = (
            PipelineSpec(spec=pipeline_specification)
            if isinstance(pipeline_specification, Pipeline)
            else PipelineSpec.parse(pipeline_specification, with_ensure=False)
        )
        parsed_spec.ensure()

        main_cluster = parsed_spec.main_cluster()

        # Root of pipeline.
        self.add_node(
            Node(
                type=Node.Type.FOLDER,
                clusters={
                    main_cluster: ClusterNode(
                        path=parsed_spec.path,
                        attributes={
                            "pipeline_format_version": 1,
                            "monitoring_project": parsed_spec.monitoring_project,
                            "monitoring_cluster": parsed_spec.monitoring_cluster,
                        },
                        propagated_attributes=set(),
                    ),
                },
            )
        )

        for name in PIPELINE_FILES:
            spec = deepcopy(parsed_spec.file_spec[name])
            spec.type = Node.Type.FILE
            spec.clusters[main_cluster].path = f"{parsed_spec.path}/{name}"
            self.add_node(spec)

        table_specs = []
        for name, base_spec in PIPELINE_QUEUES.items():
            spec = deepcopy(parsed_spec.queue_spec[name])
            spec.schema = TableSpec.parse(base_spec, with_ensure=False).spec.schema
            table_specs.append((name, spec))
        for name, base_spec in PIPELINE_TABLES.items():
            spec = deepcopy(parsed_spec.table_spec[name])
            spec.schema = TableSpec.parse(base_spec, with_ensure=False).spec.schema
            table_specs.append((name, spec))
        for name, spec in table_specs:
            for cluster_spec in spec.clusters.values():
                cluster_spec.path = f"{parsed_spec.path}/{name}"
            self.add_table(spec)

    def finalize(self, settings: Settings | None = None):
        for cluster in self.db.clusters.values():
            cluster.finalize()

        self.db.ensure_db_integrity(
            settings.always_async if settings else None,
            settings.ensure_rtt_settings if settings else True,
        )
