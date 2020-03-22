import argparse
import logging

import yt.wrapper as yt


def transform(src_cluster, src_path, dst_cluster, dst_path):
    src_yt = yt.YtClient(src_cluster)
    dst_yt = yt.YtClient(dst_cluster)

    node = src_yt.get(src_path, attributes=["user_attribute_keys"])
    attribute_keys = {"acl", "inherit_acl"}
    collect(node, attribute_keys)

    full_node = src_yt.get(src_path, attributes=list(attribute_keys))
    dst_yt.create("scheduler_pool_tree_map", dst_path, attributes=full_node.attributes)

    i = 0
    current_wave = []
    for key in full_node:
        current_wave.append((key, None, None, full_node[key]))
    while len(current_wave) > 0:
        i += 1
        logging.info("Wave # %d. About to create %d pools.", i, len(current_wave))

        batch_client = dst_yt.create_batch_client(raise_errors=True)
        next_wave = []
        for (name, pool_tree, parent_name, node) in current_wave:
            attributes = node.attributes
            attributes["name"] = name
            if pool_tree is not None:
                attributes["pool_tree"] = pool_tree
            if parent_name is not None:
                attributes["parent_name"] = parent_name
            object_type = "scheduler_pool_tree" if pool_tree is None else "scheduler_pool"
            batch_client.create(object_type, attributes=attributes)

            for key in node:
                if pool_tree is None:
                    next_wave.append((key, name, None, node[key]))
                else:
                    next_wave.append((key, pool_tree, name, node[key]))

        batch_client.commit_batch()
        current_wave = next_wave


def collect(yson_map, result):
    if "user_attribute_keys" in yson_map.attributes:
        for k in yson_map.attributes["user_attribute_keys"]:
            result.add(k)

    for key in yson_map:
        collect(yson_map[key], result)


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)

    parser = argparse.ArgumentParser(description='Clone operations root node to new location.')
    parser.add_argument('--src-cluster', type=str, required=True)
    parser.add_argument('--src-path', type=str, default="//sys/pool_trees")
    parser.add_argument('--dst-cluster', type=str, required=True)
    parser.add_argument('--dst-path', type=str, required=True)
    args = parser.parse_args()

    transform(args.src_cluster, args.src_path, args.dst_cluster, args.dst_path)
