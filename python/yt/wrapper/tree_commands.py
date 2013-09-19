import config
import logger
from common import parse_bool, flatten, get_value, bool_to_string
from transaction_commands import _make_transactional_request, \
                                 _make_formatted_transactional_request
from table import prepare_path, to_name
from errors import YtResponseError

import yt.yson as yson

import os
import string
import random
from copy import deepcopy

def get(path, attributes=None, format=None, spec=None):
    """
    Gets the tree growning from path.
    attributes -- attributes to provide for each node in the response.
    format -- output format (by default it is yson that automatically parsed to python structure).

    Be carefull: attributes have weird representation in json format.
    """
    return _make_formatted_transactional_request(
        "get",
        {
            "path": prepare_path(path),
            "attributes": get_value(attributes, []),
            "spec": {} if spec is None else spec
        },
        format=format)

def set(path, value):
    """
    Sets the value by path. Value should json-able object.
    """
    return _make_transactional_request(
        "set",
        {
            "path": prepare_path(path),
            "input_format": "yson"
        },
        data=yson.dumps(value))

def copy(source_path, destination_path, preserve_account=None):
    params = {"source_path": prepare_path(source_path),
              "destination_path": prepare_path(destination_path)}
    if preserve_account is not None:
        params["preserve_account"] = bool_to_string(preserve_account)
    return _make_transactional_request("copy", params)

def move(source_path, destination_path):
    _make_transactional_request(
        "move",
        {
            "source_path": prepare_path(source_path),
            "destination_path": prepare_path(destination_path)
        })

def link(target_path, link_path, recursive=False, ignore_existing=False):
    return _make_transactional_request(
        "link",
        {
            "target_path": prepare_path(target_path),
            "link_path": prepare_path(link_path),
            "recursive": bool_to_string(recursive),
            "ignore_existing": bool_to_string(ignore_existing),
        })


def list(path, max_size=1000, format=None, absolute=False, attributes=None):
    """
    Lists all items in the path. Paht should be map_node or list_node.
    In case of map_node it returns keys of the node.
    """
    def join(elem):
        return yson.convert_to_yson_type(
            yson.YsonString("{0}/{1}".format(path, elem)),
            elem.attributes)

    result = _make_formatted_transactional_request(
        "list",
        {
            "path": prepare_path(path),
            "max_size": max_size,
            "attributes": get_value(attributes, [])
        },
        format=format)
    if absolute and format is None:
        result = map(join, result)
    return result

def exists(path):
    return parse_bool(
        _make_formatted_transactional_request(
            "exists",
             {"path": prepare_path(path)},
             format=None))

def remove(path, recursive=False, force=False):
    _make_transactional_request(
        "remove",
        {
            "path": prepare_path(path),
            "recursive": bool_to_string(recursive),
            "force": bool_to_string(force)
        })

def create(type, path=None, recursive=False, ignore_existing=False, attributes=None):
    params = {
        "type": type,
        "recursive": bool_to_string(recursive),
        "ignore_existing": bool_to_string(ignore_existing),
        "attributes": get_value(attributes, {})
    }
    if path is not None:
        params["path"] = prepare_path(path)
    return _make_transactional_request("create", params)

def mkdir(path, recursive=None):
    recursive = get_value(recursive, config.CREATE_RECURSIVE)
    return create("map_node", path, recursive=recursive, ignore_existing=recursive)


# TODO: maybe remove this methods
def get_attribute(path, attribute, default=None):
    if default is not None and attribute not in list_attributes(path):
        return default
    return get("%s/@%s" % (path, attribute))

def has_attribute(path, attribute):
    return exists("%s/@%s" % (path, attribute))

def set_attribute(path, attribute, value):
    return set("%s/@%s" % (path, attribute), value)

def list_attributes(path, attribute_path=""):
    return list("%s/@%s" % (path, attribute_path))

def get_type(path):
    return get_attribute(path, "type")


def find_free_subpath(path):
    """
    Searches free node started with path.
    Path can have form {dir}/{prefix}.
    """
    # Temporary comment it because of race condirtion while uploading file
    # TODO(ignat): Uncomment it with apperance of proper locking
    #if not path.endswith("/") and not exists(path):
    #    return path
    LENGTH = 10
    char_set = string.ascii_lowercase + string.ascii_uppercase + string.digits
    while True:
        name = "%s%s" % (path, "".join(random.sample(char_set, LENGTH)))
        if not exists(name):
            return name

def search(root="/", node_type=None, path_filter=None, object_filter=None, attributes=None, exclude=None, depth_bound=None):
    """
    Searches all objects in root that have specified node_type,
    satisfy path and object filters. Returns list of the objects.
    Adds given attributes to objects.
    """
    attributes = get_value(attributes, [])

    request_attributes = deepcopy(flatten(get_value(attributes, [])))
    request_attributes.append("type")
    request_attributes.append("opaque")

    exclude = deepcopy(flatten(get_value(exclude, [])))
    exclude.append("//sys/operations")

    def safe_get(path):
        try:
            return get(path, attributes=request_attributes)
        except YtResponseError as rsp:
            if rsp.is_access_denied():
                logger.warning("Cannot traverse %s, access denied" % path)
            elif rsp.is_resolve_error():
                logger.warning("Path %s is absent" % path)
            else:
                raise
        return None

    result = []
    def walk(path, object, depth, ignore_opaque=False):
        if object is None:
            return
        if path in exclude or (depth_bound is not None and depth > depth_bound):
            return
        if object.attributes.get("opaque", False) and not ignore_opaque:
            walk(path, safe_get(path), depth, True)
            return
        object_type = object.attributes["type"]
        if (node_type is None or object_type in flatten(node_type)) and \
           (object_filter is None or object_filter(object)) and \
           (path_filter is None or path_filter(path)):
            yson_path = yson.YsonString(path)
            yson_path.attributes = dict(filter(lambda item: item[0] in attributes, object.attributes.iteritems()))
            result.append(yson_path)

        if object_type == "map_node":
            for key, value in object.iteritems():
                walk("{0}/{1}".format(path, key), value, depth + 1)

    walk(root, safe_get(root), 0, True)
    return result

def remove_with_empty_dirs(path):
    """ Removes path and all empty dirs that appear after deletion.  """
    path = to_name(path)
    while True:
        remove(path, recursive=True)
        path = os.path.dirname(path)
        if path == "//" or list(path):
            break

