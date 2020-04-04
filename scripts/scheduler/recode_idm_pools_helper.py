import argparse
import logging

import yt.wrapper as yt


def create_id_map(client, old_pools_path, new_pools_path):
    logging.info("Creating pool id map from {} to {}".format(old_pools_path, new_pools_path))
    old_pools = client.get(old_pools_path, attributes=["id", "name", "type"])
    new_pools = client.get(new_pools_path, attributes=["id", "name", "type"])
    id_map = {}
    correlate(old_pools, new_pools, [], id_map)
    logging.info("Pool id map successfully created")
    return id_map


def correlate(old_node, new_node, pools_chain_from_root, result_map):
    if len(old_node) != len(new_node):
        raise Exception("On path {} old node has {} children, but new node has {} children"
                        .format("/".join(pools_chain_from_root), str(len(old_node)), str(len(new_node))))
    for child_name in old_node:
        if child_name not in new_node:
            raise Exception("On path {} old node contains child {}, but new node doesn't",
                            "/".join(pools_chain_from_root), child_name)

        old_child = old_node[child_name]
        new_child = new_node[child_name]
        if new_child.attributes["type"] == "scheduler_pool":
            result_map[old_child.attributes["id"]] = new_child.attributes["id"]

        pools_chain_from_root.append(child_name)
        correlate(old_child, new_child, pools_chain_from_root, result_map)
        pools_chain_from_root.pop()


def recode_pool_ids_table(client, pool_ids_table_path, pool_ids_backup_path, id_map):
    logging.info("Updating pool ids IDM table...")

    if client.exists(pool_ids_backup_path):
        raise Exception("Backup path %s for pool ids table already exists", pool_ids_backup_path)
    temp_table_path = pool_ids_table_path + "_tmp"
    if client.exists(temp_table_path):
        logging.info("Found table on temp path %s. Removing...", temp_table_path)
        yt.remove(temp_table_path)

    rows = list(client.select_rows("* from [{}]".format(pool_ids_table_path)))
    logging.info("Row count: %s, ids map size: %s", str(len(rows)), str(len(id_map)))
    for row in rows:
        old_id = row["id"]
        if old_id not in id_map:
            raise Exception("Pool id {} not found in ids_map".format(old_id))
        row["id"] = id_map[old_id]

    logging.info("Creating recoded IDM table %s", temp_table_path)
    schema = client.get(pool_ids_table_path + "/@schema")
    client.create("table", temp_table_path, attributes={"schema": schema, "dynamic": True})
    client.mount_table(temp_table_path, sync=True)
    client.insert_rows(temp_table_path, rows)
    client.unmount_table(temp_table_path, sync=True)

    logging.info("Backing up old IDM table: %s to %s", pool_ids_table_path, pool_ids_backup_path)
    client.unmount_table(pool_ids_table_path, sync=True)
    client.move(pool_ids_table_path, pool_ids_backup_path)

    logging.info("Moving forward recoded IDM table: %s to %s", temp_table_path, pool_ids_table_path)
    client.move(temp_table_path, pool_ids_table_path)
    client.mount_table(pool_ids_table_path, sync=True)


def recode_idm(cluster, old_pools_path, new_pools_path, idm_pools_table):
    client = yt.YtClient(cluster)
    id_map = create_id_map(client, old_pools_path, new_pools_path)
    recode_pool_ids_table(client, idm_pools_table, idm_pools_table + "_bak", id_map)


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)

    parser = argparse.ArgumentParser(description='Recode IDM pools table.')
    parser.add_argument('--cluster', type=str, required=True)
    parser.add_argument('--old-pools-path', type=str, required=True)
    parser.add_argument('--new-pools-path', type=str, required=True)
    parser.add_argument('--idm-pools-table', type=str, default="//sys/idm/pools")
    args = parser.parse_args()

    recode_idm(args.cluster, args.old_pools_path, args.new_pools_path, args.idm_pools_table)
