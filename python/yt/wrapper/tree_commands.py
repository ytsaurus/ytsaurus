from common import require, YtError, parse_bool, flatten
from path_tools import dirs, split_table_ranges
from format import JsonFormat
from http import make_request
from transaction_commands import add_transaction_params

from yt.yson.yson_types import YSONString

import os
import string
import random
from copy import deepcopy
import simplejson as json


def get(path, attributes=None, format=None):
    if attributes is None:
        attributes = []
    return make_request("get",
                        add_transaction_params({
                            "path": path,
                            "attributes": attributes
                        }),
                        format=format)

def set(path, value):
    return make_request("set",
                        add_transaction_params({
                            "path": path,
                        }),
                        json.dumps(value),
                        format=JsonFormat())

def copy(source_path, destination_path):
    return make_request("copy",
                        add_transaction_params({
                            "source_path": source_path,
                            "destination_path": destination_path
                        }))

def list(path):
    return make_request("list",
                        add_transaction_params({
                            "path": path
                        }))

def exists(path):
    return parse_bool(
        make_request("exists",
                     add_transaction_params({
                        "path": split_table_ranges(path)[0]
                     })))

def remove(path):
    require(exists(path),
            YtError("You try to delete non-existing path " + path))
    return make_request("remove",
                        add_transaction_params({
                            "path": path
                        }))

def remove_with_empty_dirs(path):
    while True:
        remove(path)
        path = os.path.dirname(path)
        if path == "//" or list(path):
            break

def mkdir(path):
    create = False
    for dir in dirs(path):
        if not create and not exists(dir):
            create = True
        if create:
            set(dir, {})

def get_attribute(path, attribute, default=None):
    if default is not None and attribute not in list_attributes(path):
        return default
    return get("%s/@%s" % (path, attribute))

def set_attribute(path, attribute, value):
    return set("%s/@%s" % (path, attribute), value)

def list_attributes(path, attribute_path=""):
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

def get_type(path):
    return get_attribute(path, "type")

def search(root="/", node_type=None, path_filter=None, object_filter=None, attributes=None):
    result = []
    def walk(path, object):
        object_type = object["$attributes"]["type"]
        if (node_type is None or object_type == node_type) and \
           (object_filter is None or object_filter(object)) and \
           (path_filter is None or path_filter(path)):
            # TODO(ignat): bad solution, because of embedded attributes
            # have wrong represantation
            rich_path = YSONString(path)
            rich_path.attributes = object["$attributes"]
            result.append(rich_path)
        if object_type == "map_node" and object["$value"] is not None:
            for key, value in object["$value"].iteritems():
                walk('%s/%s' % (path, key), value)
    if attributes is None: attributes = []
    copy_attributes = deepcopy(flatten(attributes))
    copy_attributes.append("type")

    walk(root, get(root, attributes=copy_attributes))
    return result

