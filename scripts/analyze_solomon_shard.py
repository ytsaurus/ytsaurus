#!/usr/bin/env python

import json
import requests
import argparse
import logging
from tabulate import tabulate
from pprint import pprint
import yt.wrapper as yt
from yt.wrapper.http_helpers import get_token


DESCRIPTION = """
Detect heavy sensors in solomon shards.

When run at the first time, the script collects sensors statistics from
solomon backups and stores it in the provided table. Subsequent runs only show
aggregated data and do not recompute anything.

Feel free to add other statistics to show_top_sensors.

Services for yt are listed at https://solomon.yandex-team.ru/admin/projects/yt/services.

Solomon export data is stored at
https://yt.yandex-team.ru/hahn/navigation?path=//home/solomon/SAS_PROD/Metrics.
"""


yt.config['pickling']['module_filter'] = lambda module: 'hashlib' not in getattr(module, '__name__', '') and \
    getattr(module, "__name__", "") != "hmac"


SOLOMON_API_URL = "http://solomon.yandex.net/api/v2"
SOLOMON_BACKUP_PATH= "//home/solomon/{}_PROD/Metrics"


DC_TO_CLUSTER = {
    "SAS": "hahn",
    "VLA": "arnold",
}


def get_json(url, token):
    headers = {
        "Authorization": "OAuth {}".format(token),
        "Accept": "application/json",
    }
    rsp = requests.get(url, headers=headers)
    rsp.raise_for_status()
    return rsp.json()


def get_shard_num_id(project_id, shard_id, token):
    rsp = get_json(SOLOMON_API_URL + "/projects/{}/shards/{}".format(project_id, shard_id), token)
    num_id = rsp["numId"]
    if num_id < 0:
        num_id += 2**32
    return num_id


def get_shard_ids(project_id, service_id, token):
    logging.info("Retrieving shards of %s/%s from solomon", project_id, service_id)
    rsp = get_json(SOLOMON_API_URL + "/projects/{}/services/{}/clusters".format(project_id, service_id), token)
    return [shard["shardId"] for shard in rsp]
    logging.info("Collected %s shards", len(shards))


def list_clusters():
    locke_client = yt.YtClient("locke")
    return locke_client.list("//sys/clusters")


def map_clusters_to_shard_ids(shard_ids, clusters, project_id, token):
    logging.info("Collecting shard ids for given clusters")

    result = {}
    for cluster in sorted(clusters, key=lambda x: -len(x)):
        matching = [shard for shard in shard_ids if cluster.replace('-', '_') in shard]
        if len(matching) > 1:
            raise Exception("Too many shards found for cluster {}: {}".format(cluster, ", ".join(matching)))
        elif len(matching) == 0:
            logging.warn("No shards found for cluster {}, skipping it".format(cluster))
        else:
            shard_id = matching[0]
            shard_ids.remove(shard_id)
            num_id = get_shard_num_id(project_id, shard_id, token)
            result[cluster] = num_id
    return result


@yt.aggregator
class Mapper:
    def __init__(self, cluster_by_shard_id):
        self.cluster_by_shard_id = cluster_by_shard_id


    def __call__(self, rows):
        yield yt.create_table_switch(0)

        schema = {}

        for row in rows:
            cluster = self.cluster_by_shard_id.get(row.get("shardId"))
            if cluster is None:
                continue
                continue
            if "labels" not in row or not row["labels"]:
                continue
            fields = filter(lambda x: x, row["labels"].split("&"))
            res = {}
            for f in fields:
                x, y = f.split("=")
                res[x] = y
                schema[x] = ""
            res["cluster"] = cluster
            yield res

        yield yt.create_table_switch(1)
        yield schema


def collect_schemaless_data(clusters, backup_path, intermediate_table, schema_table, pool):
    spec = {}
    if pool:
        spec["pool"] = pool
    cluster_by_shard_id = {shard_id: cluster for cluster, shard_id in clusters.iteritems()}
    yt.run_map(
        Mapper(cluster_by_shard_id),
        backup_path,
        [intermediate_table, schema_table],
        format=yt.YsonFormat(control_attributes_mode="iterator"),
        spec=spec)


def make_schema(schema_table):
    logging.info("Creating schema from schema table")
    schema = set()
    for row in yt.read_table(schema_table):
        for column in row:
            schema.add(column)
    schema = sorted(schema)
    schema.remove("sensor")
    schema[:0] = ["cluster", "sensor"]
    yson_schema = yt.yson.YsonList([{"name": name, "type": "string"} for name in schema])
    yson_schema.attributes["strict"] = True
    return yson_schema


def execute_chyt_query(query, cluster, alias, token, timeout=600):
    logging.debug("Executing query: %s", query)
    proxy = "http://{}.yt.yandex.net".format(cluster)
    s = requests.Session()
    url = "{proxy}/query?database={alias}&password={token}".format(proxy=proxy, alias=alias, token=token)
    resp = s.post(url, data=query, timeout=timeout)
    if resp.status_code != 200:
        logging.error("Response status: %s", resp.status_code)
        logging.error("Response headers: %s", resp.headers)
        logging.error("Response content: %s", resp.content)
    logging.debug("Trace id: %s", resp.headers["X-Yt-Trace-Id"])
    logging.debug("Query id: %s", resp.headers["X-ClickHouse-Query-Id"])
    resp.raise_for_status()
    rows = resp.content.strip().split('\n')
    logging.debug("Time spent: %s seconds, rows returned: %s", resp.elapsed.total_seconds(), len(rows))
    return [row.split('\t') for row in rows]


def prepare_sensors_data(args):
    shard_ids = get_shard_ids(args.project, args.service, get_token())
    shard_id_by_cluster = map_clusters_to_shard_ids(shard_ids, args.clusters, args.project, get_token())

    logging.info("Running operations to collect schemaless data")
    try:
        intermediate_table = yt.create_temp_table()
        schema_table = yt.create_temp_table()
        logging.info("Intermediate table: %s", intermediate_table)
        logging.info("Schema table: %s", schema_table)
        collect_schemaless_data(
            shard_id_by_cluster,
            SOLOMON_BACKUP_PATH.format(args.dc),
            intermediate_table,
            schema_table,
            args.pool)

        spec = {}
        if args.pool:
            spec["pool"] = args.pool

        chunk_count = yt.get(schema_table + "/@chunk_count")
        if chunk_count > 200:
            logging.info("Combining chunks of a schema table")
            merge_spec = dict(spec)
            merge_spec["job_count"] = chunk_count / 200
            merge_spec["combine_chunks"] = True
            yt.run_merge(schema_table, schema_table, mode="unordered", spec=merge_spec)

        schema = make_schema(schema_table)
        logging.info("Preparing final data into %s", args.table)
        yt.create("table", args.table, attributes={"schema": schema, "optimize_for": "scan"}, force=args.force)
        yt.run_sort(intermediate_table, args.table, sort_by=["cluster", "sensor"], spec=spec)
    except:
        yt.remove(schema_table, force=True)
        yt.remove(intermediate_table, force=True)
        raise


def show_top_sensors(dc, table, limit, cluster, token):
    def _execute(query):
        return execute_chyt_query(query, DC_TO_CLUSTER[dc], "*ch_public", token)

    query = '''
        select count(1) as cnt, "sensor"
        from "{table}"
        where "cluster" = '{cluster}'
        group by "sensor"
        order by cnt desc
        limit {limit};
    '''.format(table=table, cluster=cluster, limit=limit)
    result = _execute(query)
    print tabulate(result)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)-8s %(message)s')


    parser = argparse.ArgumentParser(description=DESCRIPTION, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--project", type=str, default="yt", help="Solomon project (e.g. `yt') [default=yt]")
    parser.add_argument(
        "--service",
        type=str,
        required=True,
        help="Solomon service (e.g. `yt_bridge_node_tablet_profiling')")
    parser.add_argument("--dc", type=str, default="SAS", choices=list(DC_TO_CLUSTER), help="[default=SAS]")
    parser.add_argument(
        "--clusters",
        nargs='*',
        type=str,
        help="Collect statistics only for these YT clusters [default=all]")
    parser.add_argument("--table", type=str, required=True, help="Table to store sensors data")
    parser.add_argument("--force", action="store_true", default=False, help="Remove table if exists")
    parser.add_argument("--pool", type=str, help="YT pool")
    parser.add_argument("--top-k", type=int, default=20, help="Count of top sensors to show")
    args = parser.parse_args()

    yt.config.set_proxy(DC_TO_CLUSTER[args.dc])
    if args.clusters is None:
        args.clusters = list_clusters()

    if yt.exists(args.table):
        logging.info("Table %s exists, will not run preparation phase; use --force to override", args.table)
    else:
        prepare_sensors_data(args)

    for cluster in args.clusters:
        logging.info("Top sensors for %s", cluster)
        show_top_sensors(args.dc, args.table, args.top_k, cluster, get_token())
