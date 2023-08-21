# -*- coding: utf-8 -*-
import yt.wrapper as yt
import argparse


def create_production_database(path, meta_cluster, replica_clusters, force):
    def create_replica(table_path, replica_path, replica_cluster, schema):
        meta_client = yt.YtClient(meta_cluster, config={"backend": "rpc"})
        replica_client = yt.YtClient(replica_cluster, config={"backend": "rpc"})
        if force:
            replica_client.remove(replica_path, force=True)

        replica_id = meta_client.create("table_replica", attributes={
            "table_path": table_path,
            "cluster_name": replica_cluster,
            "replica_path": replica_path,
        })

        replica_client.create("table", replica_path, ignore_existing=True, attributes={
            "schema": schema, "dynamic": True,
            "upstream_replica_id": replica_id,
            "primary_medium": "ssd_blobs",
        })
        replica_client.mount_table(replica_path, sync=True)
        meta_client.alter_table_replica(replica_id, True, mode="async")

    def create_replicated_table(name, schema):
        table_path = "{}/{}".format(path, name)
        replica_path = "{}/{}_replica".format(path, name)

        client = yt.YtClient(meta_cluster, config={"backend": "rpc"})
        if client.exists(table_path):
            if not force:
                print("Replicated table {} at cluster {} already exists; use option --force to recreate it".format(
                    table_path, meta_cluster
                ))
                return None
            client.remove(table_path, force=True)

        client.create("replicated_table", table_path, attributes={
            "schema": schema, "dynamic": True,
            "primary_medium": "ssd_blobs",
            "replicated_table_options": {"enable_replicated_table_tracker": True},
        })
        client.mount_table(table_path, sync=True)

        for replica_cluster in replica_clusters:
            create_replica(table_path, replica_path, replica_cluster, schema)

    create_database(create_replicated_table)


def create_database(create_table):
    create_table(
        name="topic_comments",
        schema=[
            {"name": "topic_id", "type": "string", "sort_order": "ascending"},
            {"name": "parent_path", "type": "string", "sort_order": "ascending"},
            {"name": "comment_id", "type": "uint64"},
            {"name": "parent_id", "type": "uint64"},
            {"name": "user", "type": "string"},
            {"name": "creation_time", "type": "uint64"},
            {"name": "update_time", "type": "uint64"},
            {"name": "content", "type": "string"},
            {"name": "views_count", "type": "int64", "aggregate": "sum"},
            {"name": "deleted", "type": "boolean"},
        ],
    )

    create_table(
        name="user_comments",
        schema=[
            {"name": "hash", "type": "uint64", "sort_order": "ascending", "expression": "farm_hash(user)"},
            {"name": "user", "type": "string", "sort_order": "ascending"},
            {"name": "topic_id", "type": "string", "sort_order": "ascending"},
            {"name": "parent_path", "type": "string", "sort_order": "ascending"},
            {"name": "update_time", "type": "uint64"},
        ],
    )

    create_table(
        name="topics",
        schema=[
            {"name": "topic_id", "type": "string", "sort_order": "ascending"},
            {"name": "comment_count", "type": "uint64", "aggregate": "sum"},
            {"name": "update_time", "type": "uint64"},
        ],
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--path", type=str)
    parser.add_argument("--meta_cluster", type=str)
    parser.add_argument("--replica_clusters", nargs='*')
    parser.add_argument("--force", action="store_true")
    params = parser.parse_args()

    create_production_database(params.path, params.meta_cluster, params.replica_clusters, params.force)


if __name__ == "__main__":
    main()
