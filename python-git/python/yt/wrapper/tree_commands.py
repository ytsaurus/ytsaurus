import config
import yt.logger as logger
from common import parse_bool, flatten, get_value, bool_to_string
from errors import YtResponseError
from transaction_commands import _make_transactional_request, \
                                 _make_formatted_transactional_request
from table import prepare_path, to_name

import yt.yson as yson

import os
import string
import random
from copy import deepcopy

import __builtin__

def get(path, attributes=None, format=None, ignore_opaque=False, spec=None, client=None):
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
            "ignore_opaque": bool_to_string(ignore_opaque)
        },
        format=format,
        client=client)

def set(path, value, client=None):
    """
    Sets the value by path. Value should json-able object.
    """
    return _make_transactional_request(
        "set",
        {
            "path": prepare_path(path),
            "input_format": "yson"
        },
        data=yson.dumps(value),
        client=client)

def copy(source_path, destination_path, preserve_account=None, client=None):
    params = {"source_path": prepare_path(source_path),
              "destination_path": prepare_path(destination_path)}
    if preserve_account is not None:
        params["preserve_account"] = bool_to_string(preserve_account)
    return _make_transactional_request("copy", params, client=client)

def move(source_path, destination_path, client=None):
    _make_transactional_request(
        "move",
        {
            "source_path": prepare_path(source_path),
            "destination_path": prepare_path(destination_path)
        },
        client=client)

def link(target_path, link_path, recursive=False, ignore_existing=False, client=None):
    return _make_transactional_request(
        "link",
        {
            "target_path": prepare_path(target_path),
            "link_path": prepare_path(link_path),
            "recursive": bool_to_string(recursive),
            "ignore_existing": bool_to_string(ignore_existing),
        },
        client=client)


def list(path, max_size=1000, format=None, absolute=False, attributes=None, client=None):
    """
    Lists all items in the path. Paht should be map_node or list_node.
    In case of map_node it returns keys of the node.
    """
    def join(elem):
        return yson.to_yson_type(
            yson.YsonString("{0}/{1}".format(path, elem)),
            elem.attributes)

    result = _make_formatted_transactional_request(
        "list",
        {
            "path": prepare_path(path),
            "max_size": max_size,
            "attributes": get_value(attributes, [])
        },
        format=format,
        client=client)
    if absolute and format is None:
        result = map(join, result)
    return result

def exists(path, client=None):
    return parse_bool(
        _make_formatted_transactional_request(
            "exists",
            {"path": prepare_path(path)},
            format=None,
            client=client))

def remove(path, recursive=False, force=False, client=None):
    _make_transactional_request(
        "remove",
        {
            "path": prepare_path(path),
            "recursive": bool_to_string(recursive),
            "force": bool_to_string(force)
        },
        client=client)

def create(type, path=None, recursive=False, ignore_existing=False, attributes=None, client=None):
    params = {
        "type": type,
        "recursive": bool_to_string(recursive),
        "ignore_existing": bool_to_string(ignore_existing),
        "attributes": get_value(attributes, {})
    }
    if path is not None:
        params["path"] = prepare_path(path)
    return _make_formatted_transactional_request("create", params, format=None, client=client)

def mkdir(path, recursive=None, client=None):
    recursive = get_value(recursive, config.CREATE_RECURSIVE)
    return create("map_node", path, recursive=recursive, ignore_existing=recursive, client=client)


# TODO: maybe remove this methods
def get_attribute(path, attribute, default=None, client=None):
    if default is not None and attribute not in list_attributes(path):
        return default
    return get("%s/@%s" % (path, attribute), client=client)

def has_attribute(path, attribute, client=None):
    return exists("%s/@%s" % (path, attribute), client=client)

def set_attribute(path, attribute, value, client=None):
    return set("%s/@%s" % (path, attribute), value, client=client)

def list_attributes(path, attribute_path="", client=None):
    return list("%s/@%s" % (path, attribute_path), client=client)

def get_type(path, client=None):
    return get_attribute(path, "type", client=client)


def find_free_subpath(path, client=None):
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
        if not exists(name, client=client):
            return name

def search(root="", node_type=None, path_filter=None, object_filter=None, attributes=None, exclude=None, depth_bound=None, client=None):
    """
    Searches all objects in root that have specified node_type,
    satisfy path and object filters. Returns list of the objects.
    Adds given attributes to objects.
    """
    # Deprecated. Default value "/" should be removed. 
    if not root and not config.PREFIX:
        root = "/"
    root = to_name(root, client=client)
    attributes = get_value(attributes, [])

    request_attributes = deepcopy(flatten(get_value(attributes, [])))
    request_attributes.append("type")
    request_attributes.append("opaque")

    exclude = deepcopy(flatten(get_value(exclude, [])))
    exclude.append("//sys/operations")

    def safe_get(path):
        try:
            return get(path, attributes=request_attributes, client=client)
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

        if object_type in ["account_map", "tablet_cell"]:
            object = safe_get(path)

        if isinstance(object, dict):
            for key, value in object.iteritems():
                walk("{0}/{1}".format(path, key), value, depth + 1)

        if isinstance(object, __builtin__.list):
            for index, value in enumerate(object):
                walk("{0}/{1}".format(path, index), value, depth + 1)


    walk(root, safe_get(root), 0, True)
    return result

def remove_with_empty_dirs(path, force=True, client=None):
    """ Removes path and all empty dirs that appear after deletion.  """
    path = to_name(path, client=client)
    while True:
        try:
            remove(path, recursive=True, force=True, client=client)
        except YtResponseError as error:
            if error.is_access_denied():
                logger.warning("Cannot remove %s, access denied", path)
                break
            else:
                raise
        path = os.path.dirname(path)
        if path == "//" or not exists(path) or list(path) or get(path + "/@acl"):
            break

