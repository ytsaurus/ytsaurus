from .db_manager import (
    DbManagerBase,
    get_yt_cluster_url,
    get_yt_cluster_name,
    set_db_finalization_timestamp,
)

from yt.orm.library.common import wait

from yt.common import update

from yt.wrapper import ypath_join


class ReplicatedDbManager(DbManagerBase):
    def __init__(
        self,
        yt_client,
        path,
        replica_clients,
        data_model_traits,
        version=None,
        mount_wait_time=None,
    ):
        super(ReplicatedDbManager, self).__init__(data_model_traits, version, mount_wait_time)
        self._yt_client = yt_client
        self._path = path
        self._replica_clients = replica_clients
        self._validate_replicas_independence()

    ################################################################################
    # Private interface.

    def _validate_replicas_independence(self):
        paths_per_cluster = dict()
        for yt_client, path in self.iterate_replicas():
            cluster = get_yt_cluster_url(yt_client)
            if cluster not in paths_per_cluster:
                paths_per_cluster[cluster] = []
            paths_per_cluster[cluster].append(path)
        for cluster in paths_per_cluster:
            paths = paths_per_cluster[cluster]
            for i in range(len(paths)):
                for j in range(len(paths)):
                    if i == j:
                        continue
                    if paths[i].startswith(paths[j]):
                        raise ValueError(
                            "Expected independent replicas, but path {} is a prefix of path {} within cluster {}".format(
                                paths[j],
                                paths[i],
                                cluster,
                            )
                        )

    def _wait_for_replicas_enabled(self, yt_client, table_path, iter=10, sleep_backoff=5):
        def are_enabled():
            return all(
                [
                    replica["state"] == "enabled"
                    for replica in yt_client.get(ypath_join(table_path, "@replicas")).values()
                ]
            )

        wait(are_enabled, iter=iter, sleep_backoff=sleep_backoff)

    ################################################################################
    # Protected interface for base class.

    def _create_tables_impl(self, tables, recursive):
        for table in tables:
            self._create_table_impl(table, table.attributes, recursive)

    def _create_table_impl(self, table, attributes, recursive):
        primary_table_path = self._get_table_path(self._path, table)
        primary_attributes = update(
            dict(
                replicated_table_options=dict(
                    enable_replicated_table_tracker=True,
                    preserve_tablet_index=True,  # Required for non-trivial watch log distribution policy.
                )
            ),
            attributes,
        )
        # In memory mode is not supported for replicated tables, because it does not make any sense.
        primary_attributes.pop("in_memory_mode", None)
        self._yt_client.create(
            "replicated_table",
            primary_table_path,
            attributes=primary_attributes,
            recursive=recursive,
        )
        for replica_client in self._replica_clients:
            replica_table_path = self._get_table_path(self._path, table)
            replica_id = self._yt_client.create(
                "table_replica",
                attributes=dict(
                    table_path=primary_table_path,
                    cluster_name=get_yt_cluster_name(replica_client),
                    replica_path=replica_table_path,
                ),
            )
            if attributes and "upstream_replica_id" in attributes:
                raise ValueError(
                    'Attribute "upstream_replica_id" of replicated table cannot be set'
                )
            replica_attributes = update(attributes, dict(upstream_replica_id=replica_id))
            replica_client.create(
                "table",
                replica_table_path,
                attributes=replica_attributes,
                recursive=recursive,
            )
            self._yt_client.alter_table_replica(replica_id, enabled=True, mode="sync")

        self._wait_for_replicas_enabled(self._yt_client, primary_table_path)

    def _update_finalization_timestamp(self):
        timestamp = self._yt_client.generate_timestamp()
        for yt_client, path in self.iterate_replicas():
            set_db_finalization_timestamp(yt_client, path, timestamp)

    ################################################################################
    # Public interface.

    def iterate_replicas(self):
        yield (self._yt_client, self._path)
        for replica_client in self._replica_clients:
            yield (replica_client, self._path)

    def mount_tables(self, tables, freeze=False):
        self._mount_tables(self._yt_client, self._path, tables, freeze)
        for replica_client in self._replica_clients:
            self._mount_tables(replica_client, self._path, tables, freeze)

    def insert_rows(self, table, rows, object_table=True):
        if object_table:
            rows = self._make_object_rows(rows)
        self._yt_client.insert_rows(
            self._get_table_path(self._path, table),
            rows,
        )
