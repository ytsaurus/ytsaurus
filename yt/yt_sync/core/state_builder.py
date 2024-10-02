from __future__ import annotations

from abc import ABC
from abc import abstractmethod
from copy import deepcopy
import logging
from typing import Any
from typing import Callable
from typing import Generator
from typing import Iterable
from typing import Union

from library.python.confmerge import merge_inplace
import yt.wrapper as yt

from .client import YtClientFactory
from .client import YtClientProxy
from .model import Types
from .model import YtCluster
from .model import YtDatabase
from .model import YtNativeConsumer
from .model import YtNode
from .model import YtReplica
from .model import YtTable
from .model import YtTableAttributes
from .settings import Settings
from .spec import Consumer
from .spec import Node
from .spec import Table
from .spec.details import ConsumerSpec
from .spec.details import NodeSpec
from .spec.details import TableSpec
from .table_filter import DefaultTableFilter
from .table_filter import TableFilterBase

LOG = logging.getLogger("yt_sync")


def _link_chaos_tables(desired_cluster: YtCluster, yt_cluster: YtCluster):
    for table_key, table in yt_cluster.tables.items():
        src_table = desired_cluster.tables[table_key]
        if src_table.is_chaos_replication_log_required:
            assert src_table.chaos_replication_log, f"No chaos replication log for {src_table.rich_path}"
            table.chaos_replication_log = src_table.chaos_replication_log
            replication_log = yt_cluster.tables[src_table.chaos_replication_log]
            replication_log.chaos_data_table = table_key


def _get_cluster_name(yt_client_factory: YtClientFactory, cluster_name: str) -> str:
    name, address = YtClientFactory.split_name(cluster_name)
    if address:
        yt_client_factory.add_alias(name, address)
    return name


class DesiredStateBuilderBase(ABC):
    def __init__(
        self,
        yt_client_factory: YtClientFactory,
        db: YtDatabase,
        is_chaos: bool,
        table_filter: TableFilterBase = DefaultTableFilter(),
    ):
        self.yt_client_factory: YtClientFactory = yt_client_factory
        self.db = db
        self.is_chaos = is_chaos
        self.table_filter: TableFilterBase = table_filter

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
    def finalize(self, settings: Settings | None = None):
        raise NotImplementedError()


class DesiredStateBuilder(DesiredStateBuilderBase):
    def __init__(
        self,
        yt_client_factory: YtClientFactory,
        db: YtDatabase,
        is_chaos: bool,
        table_filter: TableFilterBase = DefaultTableFilter(),
    ):
        super().__init__(yt_client_factory, db, is_chaos, table_filter)

    def _assert_main(self, cluster: YtCluster, node_type: str, path: str):
        assert cluster.is_main, f"Expected cluster {cluster.name} to be main, got replica ({node_type}={path})"

    def _assert_replica(self, cluster: YtCluster, node_type: str, path: str):
        assert cluster.is_replica, f"Expected cluster {cluster.name} to be replica, got main ({node_type}={path})"

    def _add_single_table(self, parsed_spec: TableSpec) -> YtTable:
        assert not parsed_spec.is_federation
        cluster_name: str = _get_cluster_name(self.yt_client_factory, parsed_spec.main_cluster)
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
        main_cluster_name: str = _get_cluster_name(self.yt_client_factory, parsed_spec.main_cluster)
        main_cluster: YtCluster = self.db.add_or_get_cluster(
            YtCluster.make(YtCluster.Type.MAIN, main_cluster_name, self.is_chaos)
        )
        main_spec = parsed_spec.main_cluster_spec
        self._assert_main(main_cluster, "table", main_spec.path)
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
            )
        )
        for raw_cluster_name, cluster_spec in parsed_spec.clusters.items():
            cluster_name = _get_cluster_name(self.yt_client_factory, raw_cluster_name)
            if cluster_name == main_cluster_name:
                continue
            replica_cluster: YtCluster = self.db.add_or_get_cluster(
                YtCluster.make(YtCluster.Type.REPLICA, cluster_name, self.is_chaos, YtCluster.Mode.ASYNC)
            )
            self._assert_replica(replica_cluster, "table", cluster_spec.path)
            replica_table: YtTable = YtTable.make(
                key=main_table.key,
                cluster_name=cluster_name,
                table_type=YtTable.Type.TABLE,
                path=cluster_spec.path,
                exists=True,
                attributes=cluster_spec.attributes,
                explicit_schema=cluster_spec.schema_override or parsed_spec.schema,
            )
            replica_table.rtt_options.enabled = bool(cluster_spec.replicated_table_tracker_enabled)
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
                replication_log.rtt_options.enabled = bool(cluster_spec.replicated_table_tracker_enabled)
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
            else TableSpec.parse(table_specification, False)
        )
        parsed_spec.ensure()
        return self._add_table(parsed_spec)

    def add_consumer(self, consumer_specification: Union[Types.Attributes, Consumer]) -> YtNativeConsumer:
        parsed_spec: ConsumerSpec = (
            ConsumerSpec(spec=consumer_specification)
            if isinstance(consumer_specification, Consumer)
            else ConsumerSpec.parse(consumer_specification, False)
        )
        parsed_spec.ensure()
        consumer_table: YtTable = self._add_table(parsed_spec.table)

        consumer: YtNativeConsumer = YtNativeConsumer(consumer_table)
        for queue in parsed_spec.queues:
            consumer.add_registration(
                _get_cluster_name(self.yt_client_factory, queue.cluster),
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

    def add_node(self, node_specification: Union[Types.Attributes, Node]):
        parsed_spec: NodeSpec = (
            NodeSpec(spec=node_specification)
            if isinstance(node_specification, Node)
            else NodeSpec.parse(node_specification, False)
        )
        parsed_spec.ensure()

        for raw_cluster_name, cluster_spec in parsed_spec.clusters.items():
            cluster_name = _get_cluster_name(self.yt_client_factory, raw_cluster_name)
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
                    explicit_target_path=cluster_spec.target_path,
                )
            )

    def finalize(self, settings: Settings | None = None):
        for cluster in self.db.clusters.values():
            cluster.fix_folders()
            cluster.propagate_attributes()

        self.db.ensure_db_integrity(
            settings.always_async if settings else None,
            settings.ensure_rtt_settings if settings else True,
        )


class DesiredStateBuilderDeprecated(DesiredStateBuilderBase):
    def __init__(
        self,
        yt_client_factory: YtClientFactory,
        db: YtDatabase,
        is_chaos: bool,
        table_filter: TableFilterBase = DefaultTableFilter(),
    ):
        super().__init__(yt_client_factory, db, is_chaos, table_filter)
        self._implicit_clusters: dict[str, YtCluster] = dict()

    def _add_table(self, table_settings: Types.Attributes) -> YtTable | None:
        assert "path" in table_settings, "Mandatory attribute 'path' is absent"
        table_path = table_settings["path"]
        if not self.table_filter.is_ok(table_path):
            return
        assert "master" in table_settings, f"Mandatory section 'master' is absent for '{table_path}' settings"

        async_replicas = self._add_replicas(
            table_path, YtCluster.Type.REPLICA, YtCluster.Mode.ASYNC, table_settings.get("async_replicas")
        )
        sync_replicas = self._add_replicas(
            table_path, YtCluster.Type.REPLICA, YtCluster.Mode.SYNC, table_settings.get("sync_replicas")
        )
        main_table = self._add_main(table_settings["master"], table_path, sync_replicas, async_replicas)

        return main_table

    def _add_table_at_cluster(
        self,
        default_cluster: YtCluster,
        table_key: str,
        table_type: str,
        table_path: str,
        table_attributes: Types.Attributes,
        source_attributes: YtTableAttributes | None = None,
    ) -> YtTable:
        yt_cluster = self.db.add_or_get_cluster(default_cluster)
        if yt_cluster.name in self._implicit_clusters:
            # TODO: Maybe can be moved by pure assignment?
            for node in self._implicit_clusters[yt_cluster.name].nodes.values():
                yt_cluster.add_node(node)
            del self._implicit_clusters[yt_cluster.name]

        return yt_cluster.add_table_from_attributes(
            table_key, table_type, table_path, True, table_attributes, source_attributes
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
            cluster_name = _get_cluster_name(self.yt_client_factory, raw_cluster_name)
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
        cluster_name = _get_cluster_name(self.yt_client_factory, consumer_table.cluster_name)
        yt_cluster = self.db.add_or_get_cluster(YtCluster.make(YtCluster.Type.MAIN, cluster_name, self.is_chaos))
        yt_cluster.add_consumer(consumer)
        return consumer

    def add_cluster(self, cluster_type: str, cluster_name: str, is_chaos: bool):
        self.db.add_or_get_cluster(YtCluster.make(cluster_type, cluster_name, is_chaos))

    def _add_main(
        self, attributes: Types.Attributes, path: str, sync_replicas: set[str], async_replicas: set[str]
    ) -> YtTable:
        cluster_name = _get_cluster_name(self.yt_client_factory, attributes["cluster"])
        # if no replicas, treat tables in main as ordinary tables
        has_replicas: bool = bool(sync_replicas or async_replicas)
        table_type = YtTable.Type.REPLICATED_TABLE if has_replicas else YtTable.Type.TABLE

        main_table = self._add_table_at_cluster(
            YtCluster.make(YtCluster.Type.MAIN if has_replicas else YtCluster.Type.SINGLE, cluster_name, self.is_chaos),
            path,
            YtTable.resolve_table_type(table_type, self.is_chaos),
            path,
            attributes.get("attributes", dict()),
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
    ) -> set[str]:
        result: set[str] = set()
        if attributes is None:
            return result
        path = attributes.get("replica_path")
        assert path, "Attribute 'replica_path' is mandatory"
        assert "cluster" in attributes, "Attribute 'cluster' is mandatory for replicas"
        for cluster in attributes["cluster"]:
            cluster_name = _get_cluster_name(self.yt_client_factory, cluster)

            table_attributes = attributes["attributes"][cluster]

            table = self._add_table_at_cluster(
                YtCluster.make(replica_type, cluster_name, self.is_chaos, cluster_mode),
                main_path,
                YtTable.Type.TABLE,
                path,
                deepcopy(table_attributes),
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
            cluster.fix_folders()
            cluster.propagate_attributes()

        self.db.ensure_db_integrity(
            settings.always_async if settings else None,
            settings.ensure_rtt_settings if settings else True,
        )


class FakeActualStateBuilder:
    def __init__(self, table_filter: TableFilterBase = DefaultTableFilter()):
        self._table_filter: TableFilterBase = table_filter

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
                _link_chaos_tables(desired_cluster, actual_cluster)
        return actual


class ActualStateBuilder:
    def __init__(
        self,
        settings: Settings,
        yt_client_factory: YtClientFactory,
        table_filter: TableFilterBase = DefaultTableFilter(),
    ):
        self._settings: Settings = settings
        self._yt_client_factory: YtClientFactory = yt_client_factory
        self._table_filter: TableFilterBase = table_filter

    def build_from(self, desired: YtDatabase) -> YtDatabase:
        actual = YtDatabase(is_chaos=desired.is_chaos)
        for cluster_name, desired_cluster in desired.clusters.items():
            actual_cluster = actual.add_or_get_cluster(desired_cluster)
            yt_client = self._yt_client_factory(cluster_name)
            batch_client = yt_client.create_batch_client()

            # fetch all nodes and tables attributes via get("path&/@")
            node_responses: dict[str, Any] = self._fetch_all_attributes(
                batch_client, desired_cluster, self._get_explicit_nodes, lambda item: item.path
            )
            table_responses: dict[str, Any] = self._fetch_all_attributes(
                batch_client, desired_cluster, lambda cluster: self._table_filter(cluster), lambda item: item.key
            )

            link_target_responses: dict[str, Any] = self._fetch_link_targets(
                batch_client,
                self._get_link_targets(
                    desired_cluster,
                    {node.path for node in desired_cluster.nodes.values()}
                    | {desired_cluster.tables[key].path for key in table_responses},
                ),
            )
            batch_client.commit_batch()

            self._fetch_implicit_nodes(batch_client, desired_cluster, actual_cluster)

            # assert all link targets are correct (exist and are not links themselves)
            self._assert_link_targets(desired_cluster, link_target_responses)

            # parse requested attributes and fill cluster with nodes and tables
            for responses in (node_responses, table_responses):
                additional_responses: dict[str, dict[str, Any]] = self._process_responses(
                    responses, batch_client, desired_cluster, actual_cluster
                )
                # some attributes can not be fetched via get("path&/@") request and should be requested individually
                self._process_opaque_attrs_responses(additional_responses, actual_cluster)

            if desired.is_chaos and desired_cluster.is_replica:
                # link data tables and replication logs together
                _link_chaos_tables(desired_cluster, actual_cluster)
            # Create consumers
            self._process_consumers(yt_client, actual_cluster, desired_cluster)

        actual.ensure_db_integrity(None, False)
        return actual

    @classmethod
    def _get_explicit_nodes(cls, desired_cluster: YtCluster) -> Generator[YtNode, None, None]:
        for node in desired_cluster.nodes_sorted:
            if node.is_implicit:
                continue
            yield node

    @classmethod
    def _get_link_targets(cls, desired_cluster: YtCluster, requested_paths: set[str]) -> set[str]:
        targets: set[str] = set()
        for node in desired_cluster.nodes_sorted:
            if node.node_type != YtNode.Type.LINK:
                continue
            target_path = node.link_target_path
            if target_path not in requested_paths:
                targets.add(target_path)
        return targets

    @classmethod
    def _fetch_link_targets(
        cls,
        batch_client: YtClientProxy,
        link_targets: Iterable[str],
    ) -> dict[str, Any]:
        responses: dict[str, Any] = dict()
        for path in link_targets:
            responses[path] = batch_client.get(f"{path}&/@type")
        return responses

    @classmethod
    def _assert_link_targets(cls, desired_cluster: YtCluster, link_target_responses: dict[str, Any]) -> None:
        for link_target, result in link_target_responses.items():
            if not result.is_ok():
                error = yt.YtResponseError(result.get_error())
                if error.is_resolve_error():
                    assert False, f"Link target '{desired_cluster.name}:{link_target}' does not exist"
                else:
                    raise error
            assert (
                result.get_result() != YtNode.Type.LINK
            ), f"Link target '{desired_cluster.name}:{link_target}' is another link"

    @classmethod
    def _fetch_all_attributes(
        cls,
        batch_client: YtClientProxy,
        desired_cluster: YtCluster,
        item_source: Callable[[YtCluster], Iterable[YtNode | YtTable]],
        item_key: Callable[[Any], str],
    ) -> dict[str, Any]:
        responses: dict[str, Any] = dict()
        for item in item_source(desired_cluster):
            responses[item_key(item)] = batch_client.get(f"{item.path}&/@")
        return responses

    @classmethod
    def _fetch_implicit_nodes(
        cls,
        batch_client: YtClientProxy,
        desired_cluster: YtCluster,
        actual_cluster: YtCluster,
    ):
        responses: dict[str, Any] = dict()

        for item in desired_cluster.nodes_sorted:
            if item.is_implicit:
                responses[item.path] = batch_client.get(f"{item.path}&/@type")
        batch_client.commit_batch()

        for path, response in responses.items():
            source_node = desired_cluster.nodes[path]
            if response.is_ok():
                cls._add_item_from_attributes(
                    desired_cluster, actual_cluster, True, {"type": response.get_result()}, source_node
                )
            else:
                error = yt.YtResponseError(response.get_error())
                if error.is_resolve_error():
                    cls._add_item_from_attributes(desired_cluster, actual_cluster, False, {}, source_node)
                elif source_node.is_implicit and error.is_access_denied():
                    item = cls._add_item_from_attributes(
                        desired_cluster, actual_cluster, True, {"type": YtNode.Type.FOLDER}, source_node
                    )
                    item.is_implicit = True
                else:
                    raise error

    @classmethod
    def _process_responses(
        cls,
        responses: dict[str, Any],
        batch_client: YtClientProxy,
        desired_cluster: YtCluster,
        actual_cluster: YtCluster,
    ) -> dict[str, dict[str, Any]]:
        opaque_attrs_responses: dict[str, dict[str, Any]] = dict()
        for item_key, response in responses.items():
            req = cls._process_response(batch_client, desired_cluster, actual_cluster, item_key, response)
            if req:
                opaque_attrs_responses.setdefault(item_key, dict()).update(req)
        if opaque_attrs_responses:
            batch_client.commit_batch()
        return opaque_attrs_responses

    @classmethod
    def _add_item_from_attributes(
        cls,
        desired_cluster: YtCluster,
        actual_cluster: YtCluster,
        exists: bool,
        attrs: Types.Attributes,
        source_item: YtTable | YtNode,
    ) -> YtTable | YtNode:
        if isinstance(source_item, YtTable):
            item = actual_cluster.add_table_from_attributes(
                table_key=source_item.key,
                table_type=attrs.pop("type") if exists else source_item.table_type,
                table_path=source_item.path,
                exists=exists,
                table_attributes=attrs,
                source_attributes=source_item.attributes,
                ensure_empty_schema=False,
            )
        else:
            assert isinstance(source_item, YtNode)
            item = actual_cluster.add_node(
                YtNode.make(
                    cluster_name=desired_cluster.name,
                    path=source_item.path,
                    node_type=attrs.pop("type") if exists else source_item.node_type,
                    exists=exists,
                    attributes=attrs,
                    filter_=source_item.attributes,
                    explicit_target_path=attrs.pop("target_path", None) if exists else source_item.link_target_path,
                )
            )
        return item

    @classmethod
    def _process_response(
        cls,
        batch_client: YtClientProxy,
        desired_cluster: YtCluster,
        actual_cluster: YtCluster,
        item_key: str,
        response: Any,
    ) -> dict[str, Any]:
        opaque_attrs_responses: dict[str, Any] = dict()
        source_item: YtTable | YtNode | None = desired_cluster.tables.get(item_key) or desired_cluster.nodes.get(
            item_key
        )
        assert source_item

        if response.is_ok():
            attrs = dict(response.get_result())
            item = cls._add_item_from_attributes(desired_cluster, actual_cluster, True, attrs, source_item)
            for attr_name in item.request_opaque_attrs():
                opaque_attrs_responses[attr_name] = batch_client.get(f"{item.path}&/@{attr_name}")
        else:
            error = yt.YtResponseError(response.get_error())
            if error.is_resolve_error():
                item = cls._add_item_from_attributes(desired_cluster, actual_cluster, False, {}, source_item)
            else:
                raise error
        return opaque_attrs_responses

    @classmethod
    def _process_opaque_attrs_responses(cls, responses: dict[str, dict[str, Any]], yt_cluster: YtCluster):
        for item_key, item_responses in responses.items():
            yt_item: YtTable | YtNode | None = yt_cluster.tables.get(item_key) or yt_cluster.nodes.get(item_key)
            assert yt_item is not None, f"{item_key} is neither a node nor a table"
            for attr, response in item_responses.items():
                if response.is_ok():
                    yt_item.apply_opaque_attribute(attr, response.get_result())
                else:
                    error = yt.YtResponseError(response.get_error())
                    if not error.is_resolve_error():
                        raise error

    @classmethod
    def _process_consumers(cls, yt_client: YtClientProxy, actual_cluster: YtCluster, desired_cluster: YtCluster):
        for key, consumer in desired_cluster.consumers.items():
            actual_table = actual_cluster.tables[key]
            consumer = YtNativeConsumer(actual_table)
            if actual_table.attributes.get("treat_as_queue_consumer", False):
                for registration in yt_client.list_queue_consumer_registrations(consumer_path=consumer.table.path):
                    queue_cluster = registration["queue_path"].attributes["cluster"]
                    queue_path = str(registration["queue_path"])
                    consumer.add_registration(
                        queue_cluster, queue_path, registration["vital"], registration.get("partitions")
                    )
            actual_cluster.add_consumer(consumer)
