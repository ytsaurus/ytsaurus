from path_tools import escape_path, split_path, dirs, split_table_ranges
from http import make_request
import config

import os
import string
import random

def get(path, check_errors=True, attributes=None):
    if attributes is None:
        attributes = []
    return make_request("GET", "get",
                        # Hacky way to pass attributes into url
                        dict(
                            [("transaction_id", config.TRANSACTION)] +
                            [("path", escape_path(path))] +
                            [("attributes[%d]" % i, attributes[i]) for i in xrange(len(attributes))]
                        ),
                        #{"path": escape_path(path),
                        # "attributes": attributes},
                        check_errors=check_errors)

def set(path, value):
    return make_request("PUT", "set",
                        {"path": escape_path(path),
                         "transaction_id": config.TRANSACTION}, value)

def copy(source_path, destination_path):
    return make_request("POST", "copy",
                        {"source_path": escape_path(source_path),
                         "destination_path": escape_path(destination_path),
                         "transaction_id": config.TRANSACTION})

def list(path):
    if not exists(path):
        # TODO(ignat):think about throwing exception here
        return []
    return make_request("GET", "list",
            {"path": escape_path(path),
             "transaction_id": config.TRANSACTION})

def exists(path):
    # TODO(ignat): this function is very hacky because of
    # path can contain table ranges and attribute delimiter
    # The right way is to add this functionality to driver
    def find_attributes_switch(str):
        index = 0
        while index < len(str):
            pos = path.find("/@", index)
            if pos > 0 and str[pos - 1] == "\\":
                index = pos + 1
            else:
                return pos
        return -1

    def check_tree_existance(path, objects_tree):
        cur_path = "/"
        for elem in split_path(path):
            if objects_tree is None:
                objects_tree = get(cur_path)
            if not isinstance(objects_tree, dict) or elem not in objects_tree:
                return False
            else:
                objects_tree = objects_tree[elem]
                cur_path = os.path.join(cur_path, elem)
        return True

    if path == "/":
        return True

    attr_switch = find_attributes_switch(path)
    if attr_switch != -1:
        main_part = path[:attr_switch]
        attr_part = path[attr_switch + 2:]
        return check_tree_existance(main_part, get("/")) and \
               (attr_part == "" or check_tree_existance(attr_part, get(main_part + "/@")))
    else:
        return check_tree_existance(split_table_ranges(path)[0], get("/"))

def remove(path):
    if exists(path):
        return make_request("POST", "remove",
                {"path": escape_path(path),
                 "transaction_id": config.TRANSACTION})
    # TODO(ignat):think about throwing exception here
    return None

def mkdir(path):
    create = False
    for dir in dirs(path):
        if not create and not exists(dir):
            create = True
        if create:
            set(dir, "{}")

def get_attribute(path, attribute, check_errors=True, default=None):
    if default is not None and attribute not in list_attributes(path):
        return default
    return get("%s/@%s" % (path, attribute), check_errors=check_errors)

def set_attribute(path, attribute, value):
    return set("%s/@%s" % (path, attribute), value)

def list_attributes(path, attribute_path=""):
    # TODO(ignat): it doesn't work now. We need support attributes in exists
    return list("%s/@%s" % (path, attribute_path))

def find_free_subpath(path):
    if not path.endswith("/") and not exists(path):
        return path
    LENGTH = 10
    char_set = string.ascii_lowercase + string.ascii_uppercase + string.digits
    while True:
        name = "%s%s" % (path, "".join(random.sample(char_set, LENGTH)))
        if not exists(name):
            return name

def search(root="/", node_type=None, path_filter=None, attributes=None):
    result = []
    def walk(path, object):
        object_type = object["$attributes"]["type"]
        if node_type is None or object_type == node_type:
            if path_filter(object):
                result.append(path)
        if object_type == "map_node" and object["$value"] is not None:
            for key, value in object["$value"].iteritems():
                walk('%s/%s' % (path, key), value)
    if attributes is None:
        attributes = []
    attributes.append("type")
    walk(root, get(root, attributes=attributes))
    return result

