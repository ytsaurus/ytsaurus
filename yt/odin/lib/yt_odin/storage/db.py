from .storage import Storage, StorageForCluster, OdinDBRecord

from yt_odin.logging import TaskLoggerAdapter
from yt.wrapper import YtClient

from six import text_type, binary_type

import logging

odin_logger = logging.getLogger("Odin")


class OdinYtTableClient(Storage):
    def __init__(self, table, client):
        self._table = table
        self._client = client

    def _get_task_logger(self, check_id):
        return TaskLoggerAdapter(odin_logger, check_id)

    def add_record(self, check_id, cluster, service, timestamp, **kwargs):
        row = dict(
            cluster=cluster,
            service=service,
            timestamp=timestamp,
            **kwargs
        )
        logger = self._get_task_logger(check_id)
        logger.info("Inserting row %s to %s", str(row), self._table)
        self._client.insert_rows(self._table, [row])
        logger.info("Successfully inserted row to %s", self._table)

    def add_records_bulk_impl(self, records):
        for i, row in enumerate(records, start=1):
            check_id = row.pop("check_id")
            logger = self._get_task_logger(check_id)
            logger.info("Bulk-inserting row %s to %s (%s/%s)", str(row), self._table, i, len(records))
        self._client.insert_rows(self._table, records)
        odin_logger.info("Successfully bulk-inserted %s rows to %s", len(records), self._table)

    def get_records(self, clusters, services, start_timestamp, stop_timestamp):
        clusters_tuple_string = self._make_tuple_string(clusters)
        services_tuple_string = self._make_tuple_string(services)
        query = 'cluster, service, timestamp, state, messages, duration ' \
                'FROM [{0}] WHERE timestamp >= {1} AND timestamp <= {2} ' \
                'AND service IN {3} AND cluster IN {4}' \
                .format(self._table, start_timestamp, stop_timestamp, services_tuple_string,
                        clusters_tuple_string)
        result = []
        for row in self._client.select_rows(query):
            record = OdinDBRecord(cluster=row["cluster"],
                                  service=row["service"],
                                  timestamp=row["timestamp"],
                                  state=row["state"],
                                  duration=row["duration"],
                                  messages=row["messages"])
            result.append(record)
        return result

    def is_alive(self):
        try:
            return self._client.exists(self._table)
        except Exception:
            odin_logger.exception("Failed to check odin liveness")
            return False

    @staticmethod
    def _make_tuple_string(items):
        if isinstance(items, (text_type, binary_type)):
            return '("{0}")'.format(items)
        else:
            assert len(items) > 0
            return '("' + '","'.join(items) + '")'


class OdinTableClientForCluster(StorageForCluster):
    def __init__(self, storage, cluster):
        self._storage = storage
        self._cluster = cluster

    def add_record(self, check_id, service, timestamp, **kwargs):
        self._storage.add_record(check_id, self._cluster, service, timestamp, **kwargs)

    def add_records_bulk_impl(self, records):
        for record in records:
            record["cluster"] = self._cluster
        self._storage.add_records_bulk(records)

    def get_records(self, services, start_timestamp, stop_timestamp):
        return self._storage.get_records(self._cluster, services, start_timestamp, stop_timestamp)


def create_yt_table_client(table, proxy, token, config=None):
    client = YtClient(proxy=proxy, token=token, config=config)
    return OdinYtTableClient(table, client)


def create_yt_table_client_for_cluster(table, cluster, proxy, token, config=None):
    table_client = create_yt_table_client(table, proxy, token, config)
    return OdinTableClientForCluster(table_client, cluster)


def create_yt_table_client_from_config(db_config):
    db_options = db_config["options"]
    return create_yt_table_client(db_options["table"], db_options["proxy"], db_options["token"],
                                  db_options.get("config"))


def get_cluster_client_factory_from_db_config(db_config):
    storage_type = db_config["type"]
    if storage_type == "yt":
        allowed_options = ("table", "cluster", "proxy", "token", "config")
        options = db_config["options"]
        options = {key: options[key] for key in allowed_options if key in options}
        return lambda: create_yt_table_client_for_cluster(**options)
    else:
        raise RuntimeError("Unsupported storage type: {}".format(repr(storage_type)))


def init_yt_table(table, client, bundle):
    schema = [
        {"name": "cluster", "type": "string", "sort_order": "ascending"},
        {"name": "service", "type": "string", "sort_order": "ascending"},
        {"name": "timestamp", "type": "int64", "sort_order": "ascending"},
        {"name": "state", "type": "double"},
        {"name": "duration", "type": "int64"},
        {"name": "messages", "type": "string"}
    ]

    client.create("table", table, recursive=True,
                  attributes={"dynamic": True, "schema": schema, "tablet_cell_bundle": bundle})
    client.mount_table(table, sync=True)
