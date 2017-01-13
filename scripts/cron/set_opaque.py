#!/usr/bin/env python

from yt.wrapper.common import parse_bool

import yt.logger as logger
import yt.yson as yson
import yt.wrapper as yt

import argparse
#from cStringIO import StringIO

def is_map_node(object):
    return object.attributes["type"] == "map_node"

def is_opaque(object):
    return parse_bool(object.attributes.get("opaque", False))

def get_path(object):
    return object.attributes["path"]

def get_size(object):
    return object.attributes["size"]

# Make recursive get on cypress. Add attrbiute path.
def get(path, trimmed_nodes=None):
    if trimmed_nodes is None:
        trimmed_nodes = ["//sys"]

    def walk(path, object):
        object.attributes["path"] = path
        if is_map_node(object):
            for key, value in object.items():
                new_path = yt.ypath_join(path, key)
                if new_path in trimmed_nodes:
                    object[key] = yson.to_yson_type({}, object[key].attributes)
                    object[key].attributes["path"] = new_path
                elif is_opaque(value):
                    result = get(new_path)
                    if result is not None:
                        object[key] = result
                    else:
                        del object[key]
                else:
                    walk(new_path, value)

    try:
        # NOTE: Each node in Cypress has "type" attribute and after yt.get this
        # attribute will be represented with YsonString. To avoid this and reduce
        # memory consumption using yson.loads with always_create_attributes=False here.
        result = yson.loads(yt.get(path, attributes=["type", "opaque"], format="yson"),
                            always_create_attributes=False)
    except yt.YtResponseError as err:
        if err.is_access_denied():
            # TODO(ignat): remove this code since set_opaque performed by superuser.
            result = yson.YsonMap()
            result.attributes["type"] = "map_node"
            logger.warning("Have no access to %s", path)
        elif err.is_resolve_error():
            return None
        else:
            raise

    walk(path, result)
    return result

# Convert to normal tree representation. Node is a list of children with attributes.
def convert_to_tree(obj):
    result = []
    for key, value in obj.iteritems() if is_map_node(obj) else []:
        subtree = convert_to_tree(value)
        result.append(subtree)

    return yson.to_yson_type(result, obj.attributes)

# Extract subtree with nodes that satisfy pred.
# For each node calculates number of intermediate nodes in terms of initial tree.
def extract_subtree(root, pred):
    sum_size = 1
    result = []
    for child in root:
        node = extract_subtree(child, pred)
        if pred(node):
            result.append(node)
            sum_size += 1
        else:
            result += node
            sum_size += get_size(node)

    root.attributes["size"] = sum_size
    return yson.to_yson_type(result, root.attributes)

def apply(root, functor):
    for child in root:
        apply(child, functor)
    functor(root)

def get_paths(root):
    result = []
    def extract_path(node):
        result.append(get_path(node))
    apply(root, extract_path)
    return result

def add_opaques(root, min_threshold, max_threshold):
    result = []
    def dfs(root):
        nodes = map(dfs, root)
        nodes.sort(key=get_size, reverse=True)
        size = sum(map(get_size, nodes))
        for child in nodes:
            if size > max_threshold and get_size(child) > min_threshold:
                result.append(get_path(child))
                size -= get_size(child) - 1

        if is_opaque(root):
            size = 1
        else:
            size += 1
        root.attributes["size"] = size
        return yson.to_yson_type(nodes, root.attributes)

    dfs(root)
    return result

def remove_opaques(root, min_threshold, max_threshold):
    result = []

    def remove(node):
        size = get_size(node)
        node.sort(key=get_size)
        for child in node:
            if size + get_size(child) - 1 <= max_threshold or get_size(child) < min_threshold:
                result.append(get_path(child))
                size += get_size(child) - 1
        node.attributes["size"] = size

    apply(root, remove)

    return result

def print_pretty(fout, obj, indent=0):
    def write(value):
        fout.write(" " * indent)
        fout.write(value)
        fout.write("\n")
    if isinstance(obj, yson.YsonList):
        write(get_path(obj))
        write(str(get_size(obj)))
    for value in obj:
        print_pretty(fout, value, indent + 4)

def main():
    parser = argparse.ArgumentParser(description="Set opaques to avoid heavy get requests and minimize number of requests to traverse all tree")
    parser.add_argument("--path", default="/")
    parser.add_argument("--min-threshold", type=int, metavar="N", default=50,
                       help="Do not set opaque to nodes with less that N descendants")
    parser.add_argument("--max-threshold", type=int, metavar="N", default=5000,
                       help="Maximum number of nodes under one opaque node")
    parser.add_argument("--save", action="append",  help="Opaques to not remove")


    args = parser.parse_args()

    if args.save is None:
        if args.path == "/":
            args.save = yt.list("/", absolute=True)
        else:
            args.save = []

    obj = get(args.path)

    tree = convert_to_tree(obj)

    new_opaques = set(add_opaques(extract_subtree(tree, lambda node: True), args.min_threshold, args.max_threshold))

    opaque_tree = extract_subtree(tree, lambda node: is_opaque(node) or get_path(node) in new_opaques)

    removed_opaques = set(remove_opaques(opaque_tree, args.min_threshold, args.max_threshold)) - set(args.save)

    #result_opaque_tree = extract_subtree(tree, lambda node: (is_opaque(node) or get_path(node) in new_opaques) and get_path(node) not in removed_opaques)

    #sout = StringIO()
    #print_pretty(sout, result_opaque_tree)
    #logger.info("Dump opaque tree:\n%s", sout.getvalue())

    for elem in new_opaques - removed_opaques:
        logger.info("Setting opaque to %s", elem)
        try:
            yt.set(elem + "/@opaque", True)
        except yt.YtResponseError as err:
            if err.is_resolve_error():
                logger.info("Path %s is missing", elem)
            elif err.is_concurrent_transaction_lock_conflict():
                logger.info("Path %s is locked, failed to set opaque", elem)
            else:
                raise

    for elem in removed_opaques - new_opaques:
        logger.info("Removing opaque from %s", elem)
        try:
            yt.remove(elem + "/@opaque")
        except yt.YtResponseError as err:
            if not err.is_resolve_error():
                raise

if __name__ == "__main__":
    main()

