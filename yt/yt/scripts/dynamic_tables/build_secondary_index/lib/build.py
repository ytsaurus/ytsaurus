import yt.wrapper as yt
from yt.wrapper.default_config import get_config_from_env

from yt.common import wait

import logging
from itertools import zip_longest

SYSTEM_EMPTY_COLUMN_NAME = "$empty"
TRANSIENT_PREDICATE_COLUMN_NAME = "transient_predicate"
EXPIRATION_TIMEOUT_MS = 1000 * 60 * 60 * 24
FULL_SYNC = "full_sync"
UNFOLDING = "unfolding"
UNIQUE = "unique"


class UnfoldingMapper:
    def __init__(self, unfolded_column):
        self.unfolded_column = unfolded_column

    def __call__(self, row):
        if self.unfolded_column not in row or row[self.unfolded_column] is None:
            return

        unfolded_unique_list = set(row[self.unfolded_column])

        for unfolded in unfolded_unique_list:
            row[self.unfolded_column] = unfolded
            yield row


def unfold_type(column_type):
    if column_type["type_name"] == "optional":
        column_type = column_type["item"]
    return column_type["item"]


def run_operations(
    table: str,
    index_table: str,
    predicate: str,
    barrier_timestamp: int,
    table_schema,
    index_table_schema,
    unfolded_column: str,
    client: yt.YtClient,
    pool=None,
):
    output_columns = [col["name"] for col in index_table_schema if "expression" not in col]
    shared_columns = [col["name"] for col in table_schema if col["name"] in output_columns]
    logging.debug(f"\n\t\tFound shared columns {shared_columns}")
    necessary_columns = [x for x in shared_columns]
    if predicate:
        for col in table_schema:
            if col["name"] in predicate:
                necessary_columns.append(col["name"])
        necessary_columns = list(set(necessary_columns))

    logging.debug(f"\n\t\tFound necessary columns {necessary_columns}")

    current_input = yt.TablePath(table, columns=necessary_columns)
    current_schema = [
        {"name": col["name"], "type_v3": col["type_v3"]}
        for col in table_schema if col["name"] in necessary_columns]
    spec = {"pool": pool} if pool else None

    if unfolded_column is not None:
        logging.info("\n\t\tRunning unfolding mapper")

        output_schema = [
            {
                "name": col["name"],
                "type_v3": unfold_type(col["type_v3"]) if col["name"] == unfolded_column else col["type_v3"],
            }
            for col in current_schema]

        output_table = client.create_temp_table(
            attributes={"schema": output_schema, "optimize_for": "scan"},
            expiration_timeout=EXPIRATION_TIMEOUT_MS)

        op = client.run_map(
            binary=UnfoldingMapper(unfolded_column),
            source_table=current_input,
            destination_table=output_table,
            spec=spec,
            sync=False)

        wait(lambda: not op.get_state().is_starting())

        yield

        current_input = output_table
        current_schema = output_schema

        op.wait()

    if predicate is not None:
        logging.info("\n\t\tRunning predicate calculating sorter")

        output_schema = [{
            "name": TRANSIENT_PREDICATE_COLUMN_NAME,
            "type": "boolean",
            "expression": predicate,
            "sort_order": "ascending"}] + current_schema
        for col in output_schema:
            if "sort_order" in col:
                del col["sort_order"]

        output_table = client.create_temp_table(
            attributes={"schema": output_schema, "optimize_for": "scan"},
            expiration_timeout=EXPIRATION_TIMEOUT_MS)

        op = client.run_sort(
            source_table=current_input,
            destination_table=output_table,
            sort_by=[TRANSIENT_PREDICATE_COLUMN_NAME],
            spec=spec,
            sync=False)

        wait(lambda: not op.get_state().is_starting())

        yield

        current_input = yt.TablePath(output_table, lower_key=[True], columns=shared_columns)
        current_schema = output_schema

        op.wait()

    client.mount_table(index_table, sync=True)

    logging.info(f"\n\t\tRunning sort with barrier timestamp {barrier_timestamp} into index table {index_table}")

    op = client.run_sort(
        source_table=current_input,
        destination_table=f"<append=%true;output_timestamp={barrier_timestamp}>{index_table}",
        sort_by=[col["name"] for col in index_table_schema if "sort_order" in col],
        spec=spec,
        table_writer={"block_size": 256 * 2**10, "desired_chunk_size": 100 * 2**20},
        sync=False)

    wait(lambda: not op.get_state().is_starting())

    yield

    op.wait()

    yield


def build_secondary_index(
    proxy,
    table,
    index_table,
    kind,
    predicate,
    unfolded_column,
    dry_run,
    online,
    evaluated_columns_schema=None,
    pool=None,
    pools={},
):
    assert not ((kind == UNIQUE) and online), "A correct unique index can only be built strictly"

    client = yt.YtClient(proxy=proxy, config=get_config_from_env())

    attr_list = ["type", "dynamic", "sorted", "schema", "replicas", "upstream_replica_id"]

    table_attrs = client.get(table, attributes=attr_list).attributes
    index_table_attrs = client.get(index_table, attributes=attr_list).attributes

    assert table_attrs["type"] in ("table", "replicated_table")
    assert table_attrs["type"] == index_table_attrs["type"]

    assert table_attrs["dynamic"]
    assert table_attrs["sorted"]
    assert index_table_attrs["dynamic"]
    assert index_table_attrs["sorted"]

    assert table_attrs["upstream_replica_id"] == "0-0-0-0", \
        "Incorrect table type: expected table or replicated table"
    assert index_table_attrs["upstream_replica_id"] == "0-0-0-0", \
        "Incorrect index table type: expected table or replicated table"

    table_schema = table_attrs["schema"]
    index_table_schema = index_table_attrs["schema"]

    if table_attrs["type"] == "replicated_table":
        cluster_infos = {
            replica["cluster_name"]: {"replica_id": id, "replica_path": replica["replica_path"]}
            for id, replica in table_attrs["replicas"].items()
        }
        assert len(cluster_infos) == len(table_attrs["replicas"])

        index_cluster_infos = {
            replica["cluster_name"]: {"index_replica_id": id, "index_replica_path": replica["replica_path"]}
            for id, replica in index_table_attrs["replicas"].items()
        }
        assert len(index_cluster_infos) == len(index_table_attrs["replicas"])

        common_clusters = set(cluster_infos) & set(index_cluster_infos)
        assert len(common_clusters)

        cluster_infos = {
            cluster: {**cluster_infos[cluster], **index_cluster_infos[cluster]}
            for cluster in common_clusters
        }

    logging.info("\n\t\tValidation complete")

    if dry_run:
        return

    client.unmount_table(table, sync=True)
    logging.info(f"\n\t\t{table} unmounted")
    client.unmount_table(index_table, sync=True)
    logging.info(f"\n\t\t{index_table} unmounted")

    barrier_timestamp = client.generate_timestamp()
    secondary_index_id = None

    try:
        attributes = {
            "table_path": table,
            "index_table_path": index_table,
            "kind": kind,
        }

        attributes["table_to_index_correspondence"] = "injective" if online else "bijective"
        if predicate:
            attributes["predicate"] = predicate
        if kind == UNFOLDING:
            assert unfolded_column
            attributes["unfolded_column"] = unfolded_column
        if evaluated_columns_schema:
            attributes["evaluated_columns_schema"] = evaluated_columns_schema

        logging.info("\n\t\tCreating secondary index link")
        secondary_index_id = client.create("secondary_index", attributes=attributes)

        logging.info(f"\n\t\tLink created: {secondary_index_id}. Mounting {index_table} and freezing {table}")
        client.mount_table(table, sync=True)
        client.freeze_table(table, sync=True)
        client.mount_table(index_table, sync=True)

        operation_iterators = []

        if table_attrs["type"] == "table":
            operation_iterators.append(run_operations(
                table,
                index_table,
                predicate,
                barrier_timestamp,
                table_schema,
                index_table_schema,
                unfolded_column,
                client,
                pool))
        else:
            for cluster, info in cluster_infos.items():
                replica = info["replica_path"]

                logging.info(f"\n\t\tWaiting for replica {replica} to catch up")
                wait(lambda: all([
                    tablet["flushed_row_count"] == tablet["current_replication_row_index"]
                    for tablet in client.get(f'#{info["replica_id"]}/@tablets')
                ]), sleep_backoff=3, timeout=600)

                # Flush so that all data becomes visible to map reduce.
                replica_client = yt.YtClient(proxy=cluster, config=get_config_from_env())
                replica_client.freeze_table(replica, sync=True)
                replica_client.unfreeze_table(replica, sync=True)

            logging.info("\n\t\tAll replicas caught up")

            for cluster, info in cluster_infos.items():
                operation_iterators.append(run_operations(
                    info["replica_path"],
                    info["index_replica_path"],
                    predicate,
                    barrier_timestamp,
                    table_schema,
                    index_table_schema,
                    unfolded_column,
                    yt.YtClient(proxy=cluster, config=get_config_from_env()),
                    pools.get(cluster)))

        # Launch the first operations, taking snapshot locks in process.
        for it in operation_iterators:
            next(it)

        if online:
            logging.info(f"\n\t\tUnfreezing {table}")
            client.unfreeze_table(table, sync=True)
            logging.info(f"\n\t\t Unfrozen {table}, you may now write to it and read from it")

        # Operation chains have equal length, so zip_longest isn't strictly necessary, but merely a precaution.
        for _ in zip_longest(*operation_iterators):
            pass

        if not online:
            logging.info(f"\n\t\tUnfreezing {table}")
            client.unfreeze_table(table, sync=True)

        logging.info(f"\n\t\tBuild finished. You may now read from {table} using index")

    except Exception:
        logging.exception("Unexpected error when building secondary index")
        if secondary_index_id:
            logging.info(f"\n\t\tRemoving secondary index link {secondary_index_id}")
            client.remove(f"#{secondary_index_id}")
        raise
