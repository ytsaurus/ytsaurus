#!/usr/bin/python

import yt.yson as yson
import yt.wrapper as yt

import sys
import copy
import argparse

def is_map_node(object):
    return object.attributes["type"] == "map_node"

def is_opaque(object):
    return object.attributes.get("opaque", False)

def get_path(object):
    return object.attributes["path"]

def get_size(object):
    return object.attributes["size"]

def join(path, dir):
    return "{0}/{1}".format(path, dir)

#!!!! Remove copy-paste from set_opaque.py

# Make recursive get on cypress. Add attrbiute path.
def get(path, trimmed_nodes = None):
    if trimmed_nodes is None: trimmed_nodes = ["//sys"]

    def walk(path, object):
        object.attributes["path"] = path
        if is_map_node(object):
            for key, value in object.iteritems():
                new_path = join(path, key)
                if new_path in trimmed_nodes:
                    object[key] = yson.to_yson_type({}, object[key].attributes)
                    object[key].attributes["path"] = new_path
                elif is_opaque(value):
                    object[key] = get(new_path)
                else:
                    walk(new_path, value)
    
    result = yt.get(path, attributes=["type", "opaque", "uncompressed_data_size", "compressed_data_size", "compression_codec", "erasure_codec", "replication_factpr", "account", "resource_usage"])
    walk(path, result)
    return result

# Convert to normal tree representation. Node is a list of children with attributes.
def convert_to_tree(obj):
    result = []
    for key, value in obj.iteritems() if is_map_node(obj) else []:
        subtree = convert_to_tree(value)
        result.append(subtree)

    return yson.to_yson_type(result, obj.attributes)

def sizeof_fmt(num):
    for x in ['bytes','KB','MB','GB','TB', "PB"]:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0

def dfs(root):
    def update(d, u):
        for k, v in u.iteritems():
            if k not in d:
                d[k] = copy.deepcopy(v)
            else:
                for i in xrange(len(v)):
                    d[k][i] += v[i]
    
    for child in root:
        dfs(child)

    attr = root.attributes
    if attr["type"] in ["table", "file"]:
        disk_space = attr["resource_usage"]["disk_space"]
        uncompressed_size = attr["uncompressed_data_size"]
        compressed_size = attr["compressed_data_size"]
        info = [disk_space, compressed_size, uncompressed_size]
        attr["compression_stat"] = {attr["compression_codec"]: info}
        attr["erasure_stat"] = {attr["erasure_codec"]: info}
    if attr["type"] == "map_node":
        for stat in ["compression_stat", "erasure_stat"]:
            attr[stat] = {}
            for child in root:
                update(attr[stat], child.attributes.get(stat, {}))

def print_pretty(fout, obj, depth, indent=0):
    def format(stat):
        res = {}
        for k, v in stat.iteritems():
            res[k] = map(sizeof_fmt, v)
        return res
    def write(value):
        for line in value.split("\n"):
            fout.write(" " * indent)
            fout.write(line)
            fout.write("\n")

    if depth == 0:
        return
    if isinstance(obj, yson.YsonList):
        write(get_path(obj))
        write(yson.dumps(format(obj.attributes.get("erasure_stat", {})), indent=2))
        write(yson.dumps(format(obj.attributes.get("compression_stat", {})), indent=2))
        fout.write("\n")
    for value in obj:
        print_pretty(fout, value, depth - 1, indent + 8)


def main():
    parser = argparse.ArgumentParser(description='Calculate stats over tree.')
    parser.add_argument('--root', default="/")
    parser.add_argument('--depth', type=int, default=2)
    args = parser.parse_args()

    tree = convert_to_tree(get(args.root))
    dfs(tree)
    print_pretty(sys.stdout, tree, args.depth)


if __name__ == "__main__":
    main()
