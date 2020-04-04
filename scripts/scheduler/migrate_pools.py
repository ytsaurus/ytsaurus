#!/usr/bin/env python
# -*- encoding: utf8 -*-

import argparse
import logging

import yt.wrapper as yt
from transform_pools_helper import transform
from recode_idm_pools_helper import recode_idm

SCHEDULER_POOL_OBJECT_ID = "3e9"
SCHEDULER_POOL_TREE_OBJECT_ID = "3ea"
IDM_POOLS_TABLE_PATH = "//sys/idm/pools"


def get_schema_id(root_id, object_id):
    schema_id = root_id.split("-")
    schema_id[2] = schema_id[2][:-4] + "8" + object_id
    return "-".join(schema_id)


def migrate(cluster, current_pool_trees_path, backup_path, tmp_path, migrate_idm):
    client = yt.YtClient(cluster)

    if not client.exists(current_pool_trees_path):
        raise Exception("Path {} does not exist".format(current_pool_trees_path))

    node_type = client.get(current_pool_trees_path + "/@type")
    if node_type != "map_node":
        raise Exception("Wrong type of node. Expected 'map_node', actual '{}'".format(node_type))

    if client.exists(backup_path):
        raise Exception("Path {} already exists".format(backup_path))

    if client.exists(tmp_path):
        raise Exception("Path {} already exists".format(tmp_path))

    if migrate_idm:
        logging.info("Disabling IDM")
        client.set("//sys/idm/@disabled", True)

        if client.exists("//sys/idm/lock/@locks/0/transaction_id"):
            idm_tx = client.get("//sys/idm/lock/@locks/0/transaction_id")
            client.abort_transaction(idm_tx)
    else:
        logging.info("Skipping IDM pool table migration")

    logging.info("Transforming from %s to %s", current_pool_trees_path, tmp_path)
    transform(cluster, current_pool_trees_path, cluster, tmp_path)
    logging.info("Transformation successful!")

    if migrate_idm:
        logging.info("Migrating IDM pools table.")
        recode_idm(cluster, current_pool_trees_path, tmp_path, IDM_POOLS_TABLE_PATH)

    logging.info("Creating links to pool and pool tree schema")
    client.link("#" + get_schema_id(client.get("//@id"), SCHEDULER_POOL_OBJECT_ID), "//sys/schemas/scheduler_pool", force=True)
    client.link("#" + get_schema_id(client.get("//@id"), SCHEDULER_POOL_TREE_OBJECT_ID), "//sys/schemas/scheduler_pool_tree", force=True)

    logging.info("Initializing acl of schemas")
    acl = [{
        "action": "allow",
        "subjects": ["users"],
        "permissions": ["create", "read", "use"],
        "inheritance_mode": "object_and_descendants"
    }, {
        "action": "allow",
        "subjects": ["admins"],
        "permissions": ["create", "read", "write", "remove", "administer", "use"],
        "inheritance_mode": "object_and_descendants"
    }]
    client.set("//sys/schemas/scheduler_pool/@acl", acl)
    client.set("//sys/schemas/scheduler_pool_tree/@acl", acl)

    logging.info("Backing up old config: %s to %s", current_pool_trees_path, backup_path)
    client.move(current_pool_trees_path, backup_path)

    logging.info("Moving forward transformed config: %s to %s", tmp_path, current_pool_trees_path)
    client.move(tmp_path, current_pool_trees_path)

    if migrate_idm:
        logging.info("Enabling IDM")
        client.set("//sys/idm/@disabled", False)

    logging.info("Migration successful!")


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)

    parser = argparse.ArgumentParser(description="Migrate cluster to new pool layout")
    parser.add_argument("--proxy", type=str, required=True)
    parser.add_argument("--migrate-idm", type=str, required=True)
    parser.add_argument("--current-pool-trees-path", type=str, default="//sys/pool_trees")
    parser.add_argument("--backup-path", type=str, default="//sys/pool_trees_bak")
    parser.add_argument("--tmp-path", type=str, default="//sys/pool_trees_tmp")
    args = parser.parse_args()
    migrate(args.proxy, args.current_pool_trees_path, args.backup_path, args.tmp_path, args.migrate_idm == "true")
