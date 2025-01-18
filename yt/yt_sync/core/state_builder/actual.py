from __future__ import annotations

from concurrent.futures import as_completed
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
import logging
import time
from typing import Any
from typing import Callable
from typing import Iterable

import yt.wrapper as yt
from yt.yt_sync.core.client import YtClientFactory
from yt.yt_sync.core.client import YtClientProxy
from yt.yt_sync.core.helpers import is_valid_collocation_id
from yt.yt_sync.core.model import is_in_subtree
from yt.yt_sync.core.model import Types
from yt.yt_sync.core.model import YtCluster
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.model import YtNativeConsumer
from yt.yt_sync.core.model import YtNode
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.settings import Settings
from yt.yt_sync.core.table_filter import DefaultTableFilter
from yt.yt_sync.core.table_filter import TableFilterBase

from .fake_actual import FakeActualStateBuilder
from .helpers import link_chaos_tables

LOG = logging.getLogger("yt_sync")


def _submit(
    builder: ActualStateBuilder,
    desired: YtDatabase,
    desired_cluster: YtCluster,
    actual_cluster: YtCluster,
):
    builder._process_cluster(desired, desired_cluster, actual_cluster)


class ActualStateBuilder:
    def __init__(
        self,
        settings: Settings,
        yt_client_factory: YtClientFactory,
        table_filter: TableFilterBase | None = None,
    ):
        self._settings: Settings = settings
        self._yt_client_factory: YtClientFactory = yt_client_factory
        self._table_filter: TableFilterBase = table_filter or DefaultTableFilter()

    def build_from(self, desired: YtDatabase) -> YtDatabase:
        actual = YtDatabase(is_chaos=desired.is_chaos)

        worker_count = min(self._settings.max_actual_state_builder_workers, len(desired.clusters))
        if worker_count <= 1:
            for desired_cluster in desired.clusters.values():
                actual_cluster = actual.add_or_get_cluster(desired_cluster)
                self._process_cluster(desired, desired_cluster, actual_cluster)
        else:
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                futures: list[Future] = list()
                for desired_cluster in desired.clusters.values():
                    actual_cluster = actual.add_or_get_cluster(desired_cluster)
                    futures.append(executor.submit(_submit, self, desired, desired_cluster, actual_cluster))
                for future in as_completed(futures):
                    future.result()

        actual.ensure_db_integrity(always_async=None, ensure_rtt_settings=False)
        return actual

    def _process_cluster(self, desired: YtDatabase, desired_cluster: YtCluster, actual_cluster: YtCluster):
        cluster_name = desired_cluster.name
        yt_client = self._yt_client_factory(cluster_name)
        batch_client = yt_client.create_batch_client()

        # fetch all nodes and tables attributes via get("path&/@")
        node_responses: dict[str, Any] = self._fetch_all_attributes(
            batch_client, desired_cluster, lambda cluster: cluster.nodes.values(), lambda item: item.path
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

        # assert all link targets are correct (exist and are not links themselves)
        self._assert_link_targets(desired_cluster, link_target_responses)

        # parse requested attributes and fill cluster with nodes and tables
        for responses in (node_responses, table_responses):
            additional_responses: dict[str, dict[str, Any]] = self._process_responses(
                responses, batch_client, desired_cluster, actual_cluster
            )
            # some attributes can not be fetched via get("path&/@") request and should be requested individually
            self._process_opaque_attrs_responses(additional_responses, actual_cluster)

        self._assert_any_nodes_as_folders(desired_cluster, actual_cluster)

        if desired.is_chaos and desired_cluster.is_replica:
            # link data tables and replication logs together
            link_chaos_tables(desired_cluster, actual_cluster)
        # Create consumers
        self._process_consumers(yt_client, actual_cluster, desired_cluster)

    @classmethod
    def _assert_any_nodes_as_folders(cls, desired_cluster: YtCluster, actual_cluster: YtCluster):
        last_any_node: YtNode | None = None
        for node in desired_cluster.nodes_sorted:
            if node.node_type == YtNode.Type.ANY:
                last_any_node = node
            elif last_any_node:
                if is_in_subtree(node.path, last_any_node.path):
                    assert (
                        actual_cluster.nodes[last_any_node.path].node_type == YtNode.Type.FOLDER
                    ), f"Node at '{last_any_node.rich_path}' is marked as ANY, has children, and is not a folder"
                last_any_node = None

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
    def _is_in_collocation(cls, attrs: Types.Attributes) -> bool:
        collocation_id = attrs.get("replication_collocation_id", None)
        return is_valid_collocation_id(collocation_id)

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
                explicit_in_collocation=cls._is_in_collocation(attrs) if exists else None,
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
            item = cls._add_item_from_attributes(
                desired_cluster, actual_cluster, exists=True, attrs=attrs, source_item=source_item
            )
            for attr_name in item.request_opaque_attrs():
                opaque_attrs_responses[attr_name] = batch_client.get(f"{item.path}&/@{attr_name}")
        else:
            error = yt.YtResponseError(response.get_error())
            if error.is_resolve_error():
                cls._add_item_from_attributes(
                    desired_cluster, actual_cluster, exists=False, attrs={}, source_item=source_item
                )
            elif isinstance(source_item, YtNode) and source_item.is_implicit and error.is_access_denied():
                item = cls._add_item_from_attributes(
                    desired_cluster,
                    actual_cluster,
                    exists=True,
                    attrs={"type": YtNode.Type.FOLDER},
                    source_item=source_item,
                )
                item.is_implicit = True
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


def load_actual_db(
    settings: Settings,
    yt_client_factory: YtClientFactory,
    desired: YtDatabase,
    table_filter: TableFilterBase | None = None,
    use_fake_builder: bool = False,
) -> YtDatabase:
    builder = ActualStateBuilder(settings, yt_client_factory, table_filter or DefaultTableFilter())
    if settings.use_fake_actual_state_builder or use_fake_builder:
        LOG.info("Use fake actual state builder")
        builder = FakeActualStateBuilder(table_filter or DefaultTableFilter())

    retries = 5
    for i in range(retries):
        try_num = i + 1
        LOG.debug("Build actual state, try #%s", try_num)
        try:
            actual = builder.build_from(desired)
            LOG.debug("Build actual state is successful")
            return actual
        except (AssertionError, yt.YtResponseError) as e:
            if try_num < retries:
                wait_time = try_num * 2
                LOG.debug("Build actual state failed with error, next try in %s seconds...", wait_time, exc_info=e)
                time.sleep(wait_time)
            else:
                raise e
