""" Note: the code if this script was vibe-coded using Claude Sonnet 3.5 """

from yt.wrapper import YtClient

import yt.yson as yson

import argparse
import sys


ROOT_NAME = "<Root>"


def get_scheduler_data(cluster, pool_tree):
    """Fetch scheduler pools and operations data"""
    client = YtClient(cluster)

    pools = client.get(f"//sys/scheduler/orchid/scheduler/pool_trees/{pool_tree}/pools")
    operations = client.get(f"//sys/scheduler/orchid/scheduler/pool_trees/{pool_tree}/operations")

    return pools, operations


class TreeNode:
    def __init__(self, name, data, is_pool=True):
        self.name = name
        self.data = data
        self.is_pool = is_pool
        self.children = []
        self.parent = None


def build_tree(pools, operations):
    """Build tree structure from pools and operations data"""
    # Create nodes dictionary.
    nodes = {}

    # Create pool nodes.
    for pool_name, pool_data in pools.items():
        nodes[pool_name] = TreeNode(pool_name, pool_data, is_pool=True)

    # Set pool parents.
    for pool_name, pool_data in pools.items():
        parent_name = pool_data.get("parent")
        if parent_name and parent_name in nodes:
            nodes[pool_name].parent = nodes[parent_name]
            nodes[parent_name].children.append(nodes[pool_name])

    # Create operation nodes and link to pools
    for op_name, op_data in operations.items():
        pool_name = op_data.get("pool")
        op_node = TreeNode(op_name, op_data, is_pool=False)
        nodes[op_name] = op_node
        if pool_name and pool_name in nodes:
            op_node.parent = nodes[pool_name]
            nodes[pool_name].children.append(op_node)

    return nodes[ROOT_NAME]


def obfuscate_names(root):
    operation_index = 0
    pool_index = 0

    def dfs(node):
        nonlocal pool_index, operation_index
        if node.is_pool:
            if node.name != ROOT_NAME:
                node.name = "pool" + str(pool_index)
                pool_index += 1
        else:
            node.name = "operation" + str(operation_index)
            operation_index += 1

        for child in node.children:
            dfs(child)

    dfs(root)


def format_node(node):
    """Format node data as YSON string"""
    if node.is_pool:
        return {
            "type": "pool",
            "parent": None if node.name == ROOT_NAME else node.parent.name,
            "name": node.name,
            "mode": "fair_share" if node.name == ROOT_NAME else node.data["mode"],
            "strong_guarantee_resources": node.data.get("strong_guarantee_resources"),
            "total_fair_share": node.data["detailed_fair_share"]["total"],
            "weight": node.data["weight"],
            "resource_limits": node.data["resource_limits"] if node.name == ROOT_NAME else None,
        }
    else:
        return {
            "type": "operation",
            "name": node.name,
            "parent": node.parent.name,
            "resource_usage": node.data["resource_usage"],
            "resource_demand": node.data["resource_demand"],
            "total_fair_share": node.data["detailed_fair_share"]["total"],
            "operation_type": node.data["type"],
            "weight": node.data["weight"],
        }


def topological_sort(root):
    """Perform topological sort and return formatted YSON list"""
    result = []
    visited = set()

    def dfs(node):
        visited.add(node)
        for child in node.children:
            if child not in visited:
                dfs(child)
        result.append(node)

    dfs(root)
    return reversed(result)


def main():
    parser = argparse.ArgumentParser(description="Print GPU modules info")
    parser.add_argument("-c", "--cluster", type=str, required=True)
    parser.add_argument("-t", "--pool-tree", type=str, required=True)
    args = parser.parse_args()

    pools, operations = get_scheduler_data(args.cluster, args.pool_tree)
    if pools is None or operations is None:
        return 1

    root = build_tree(pools, operations)
    obfuscate_names(root)
    yson.dump(list(map(format_node, topological_sort(root))), sys.stdout.buffer, yson_format="pretty")


if __name__ == "__main__":
    main()
