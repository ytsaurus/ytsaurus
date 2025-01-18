from copy import deepcopy
import logging

import yt.wrapper as yt
from yt.yt_sync.core.client import YtClientFactory
from yt.yt_sync.core.model import is_in_subtree
from yt.yt_sync.core.model import YtCluster
from yt.yt_sync.core.model import YtDatabase
from yt.yt_sync.core.model import YtNode
from yt.yt_sync.core.model import YtTable
from yt.yt_sync.core.settings import Settings

LOG = logging.getLogger("yt_sync")


class UnmanagedDatabaseBuilder:
    def __init__(
        self, desired_db: YtDatabase, actual_db: YtDatabase, yt_client_factory: YtClientFactory, settings: Settings
    ):
        self.desired_db: YtDatabase = desired_db
        self.actual_db: YtDatabase = actual_db
        self.yt_client_factory: YtClientFactory = yt_client_factory
        self.settings: Settings = settings

    def _check_managed_roots(self):
        LOG.debug("Roots config: %s", self.settings.managed_roots)

        if not isinstance(self.settings.managed_roots, dict):
            LOG.error(
                "Wrong type for 'managed_roots' settings parameter (expected: 'dict', got: '%s')",
                type(self.settings.managed_roots),
            )
            return False

        success = True
        for cluster_name, roots in self.settings.managed_roots.items():
            if not (isinstance(roots, list) and roots):
                LOG.error(
                    "Wrong type for cluster '%s' roots list (expected: 'list', got: '%s')", cluster_name, type(roots)
                )
                success = False
                continue

            if not roots:
                LOG.error("Empty roots list for cluster '%s'", cluster_name)
                success = False
                continue
            for root in roots:
                if isinstance(root, str) and root:
                    continue
                LOG.error(
                    "Wrong type for root '%s:%s' roots list (expected: 'str', got: '%s')",
                    cluster_name,
                    root,
                    type(root),
                )
                success = False

            for index, root in enumerate(roots):
                for other_index, other_root in enumerate(roots):
                    if index == other_index:
                        continue
                    if not is_in_subtree(root, other_root):
                        continue
                    LOG.error("Roots '%s' and '%s' for cluster %s are equal or nested", root, other_root, cluster_name)
                    success = False
        return success

    def _check_containing_managed_cluster_roots(self, desired_managed_roots: dict[str, set[str]]) -> bool:
        success = True
        for cluster_name, managed_roots in desired_managed_roots.items():
            if not managed_roots:
                LOG.warning("Cluster '%s' has no managed roots", cluster_name)
                success = False
        return success

    def _check_existing_managed_cluster_roots(self, desired_managed_roots: dict[str, set[str]]) -> bool:
        success = True
        for cluster_name, managed_roots in desired_managed_roots.items():
            yt_client = self.yt_client_factory(cluster_name)
            batch_client = yt_client.create_batch_client()
            root_types: dict[str, str | None] = dict()
            for root in sorted(managed_roots):
                root_types[root] = batch_client.get(f"{root}&/@type")
            batch_client.commit_batch()

            for root, response in sorted(root_types.items()):
                if response.is_ok():
                    root_type = str(response.get_result())
                    if root_type != YtNode.Type.FOLDER:
                        LOG.error("Managed root '%s:%s' is not a folder", cluster_name, root)
                        success = False
                else:
                    error = yt.YtResponseError(response.get_error())
                    if error.is_resolve_error():
                        LOG.error("Managed root '%s:%s' does not exist", cluster_name, root)
                        success = False
                    else:
                        raise error
        return success

    def _get_managed_cluster_roots(self) -> dict[str, set[str]]:
        result = dict()
        for cluster in self.desired_db.all_clusters:
            cluster_roots = set(self.settings.get_managed_roots_for(cluster.name))
            all_paths = [t.path for t in cluster.tables.values()] + [n.path for n in cluster.nodes.values()]
            desired_managed_roots = {r for r in cluster_roots if any([is_in_subtree(p, r) for p in all_paths])}
            result[cluster.name] = desired_managed_roots
        LOG.debug("Clusters for unmanaged database: %s", sorted(result.keys()))
        return result

    def _process_cluster(
        self,
        desired_cluster: YtCluster,
        actual_cluster: YtCluster,
        unmanaged_db: YtDatabase,
        managed_roots: set[str],
    ):
        REQUESTED_ATTRIBUTES = [
            "type",
            "target_path",
            "schema",
            "replicas",
            "tablet_state",
            "upstream_replica_id",
            "treat_as_queue_consumer",
        ]

        unmanaged_cluster = unmanaged_db.add_or_get_cluster(desired_cluster)

        yt_client = self.yt_client_factory(unmanaged_cluster.name)
        managed_paths = [n.path for n in desired_cluster.nodes.values()] + [
            t.path for t in desired_cluster.tables.values()
        ]

        for root_path in managed_roots:
            for yson_path in yt_client.search(
                root=root_path,
                node_type=None,
                map_node_order=lambda path, object: sorted(object),
                attributes=REQUESTED_ATTRIBUTES,
            ):
                if yson_path.attributes["type"] in YtTable.Type.all():
                    table: YtTable | None = actual_cluster.find_table_by_path(str(yson_path))
                    if not table:
                        attributes = deepcopy(yson_path.attributes)
                        table_type = attributes.pop("type")
                        table = unmanaged_cluster.add_table(
                            YtTable.make(
                                str(yson_path),
                                unmanaged_cluster.name,
                                table_type,
                                str(yson_path),
                                True,
                                attributes,
                            )
                        )
                    table.attributes["upstream_replica_id"] = yson_path.attributes.get("upstream_replica_id")
                elif any([is_in_subtree(p, str(yson_path)) for p in managed_paths]):
                    continue
                else:
                    attributes = deepcopy(yson_path.attributes)
                    node_type = attributes.pop("type")
                    target_path = attributes.pop("target_path", None)
                    unmanaged_cluster.add_node(
                        YtNode.make(
                            unmanaged_cluster.name,
                            str(yson_path),
                            node_type,
                            True,
                            attributes,
                            explicit_target_path=target_path,
                        )
                    )

    def build(self) -> YtDatabase:
        LOG.debug("Building unmanaged database.")

        assert self._check_managed_roots(), "Managed roots setting is invalid."
        unmanaged_db: YtDatabase = YtDatabase(is_chaos=self.settings.is_chaos)

        managed_cluster_roots = self._get_managed_cluster_roots()
        if not self._check_containing_managed_cluster_roots(managed_cluster_roots):
            return unmanaged_db

        assert self._check_existing_managed_cluster_roots(
            managed_cluster_roots
        ), "Desired state misconfiguration for managed roots."

        for cluster_name, managed_paths in managed_cluster_roots.items():
            self._process_cluster(
                self.desired_db.clusters[cluster_name],
                self.actual_db.clusters[cluster_name],
                unmanaged_db,
                managed_paths,
            )
        return unmanaged_db
