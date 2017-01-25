from . import yson
from .config import get_config, get_option
from .common import parse_bool, flatten, get_value, bool_to_string, YtError, set_param
from .errors import YtResponseError
from .transaction_commands import _make_transactional_request, \
                                  _make_formatted_transactional_request
from .ypath import YPath, escape_ypath_literal
from .format import create_format

import yt.logger as logger

from yt.packages.six import iteritems
from yt.packages.six.moves import builtins, map as imap, filter as ifilter

import os
import string
from copy import deepcopy

# XXX(asaitgalin): Used in get_attribute function for `default` argument
# instead of None value to distinguish case when default argument
# is passed and is None from case when default is not passed.
class _KwargSentinelClass(object):
    __instance = None
    def __new__(cls):
        if cls.__instance == None:
            cls.__instance = object.__new__(cls)
            cls.__instance.name = "_KwargSentinelClassInstance"
        return cls.__instance
_KWARG_SENTINEL = _KwargSentinelClass()

def get(path, attributes=None, format=None, ignore_opaque=False, read_from=None, client=None):
    """Gets Cypress node content (attribute tree).

    :param path: path to tree, it must exist!
    :type path: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    :param list attributes: desired node attributes in the response.
    :param format: output format (by default python dict automatically parsed from YSON).
    :type format: str or descendant of :class:`Format <yt.wrapper.format.Format>`
    :param bool ignore_opaque: ignore opaque.
    :return: node tree content in `format`

    Be careful: attributes have specific representation in JSON format.

    .. seealso:: `get on wiki <https://wiki.yandex-team.ru/yt/userdoc/api#get>`_
    """
    params = {
        "path": YPath(path, client=client),
        "ignore_opaque": bool_to_string(ignore_opaque)}
    set_param(params, "attributes", attributes)
    set_param(params, "read_from", read_from)
    return _make_formatted_transactional_request(
        "get",
        params=params,
        format=format,
        client=client)

def set(path, value, format=None, client=None):
    """Sets new value to Cypress node.

    :param path: path.
    :type path: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    :param value: json-able object.
    :param format: format of the value. If format is None than value should be \
    object that can be dumped to JSON of YSON. Otherwise it should be string.

    .. seealso:: `set on wiki <https://wiki.yandex-team.ru/yt/userdoc/api#set>`_
    """
    if format is None:
        value = yson.dumps(value)
        format = "yson"

    if isinstance(format, str):
        format = create_format(format)

    return _make_transactional_request(
        "set",
        {
            "path": YPath(path, client=client),
            "input_format": format.to_yson_type()
        },
        data=value,
        client=client)

def copy(source_path, destination_path, recursive=None, preserve_account=None, force=None, client=None):
    """Copies Cypress node.

    :param source_path: source path.
    :type source_path: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    :param destination_path: destination path.
    :type destination_path: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    :param bool recursive: ``yt.wrapper.config["yamr_mode"]["create_recursive"]`` by default.
    :param bool preserve_account: preserve account.
    :param bool force: force.

    .. seealso:: `copy on wiki <https://wiki.yandex-team.ru/yt/userdoc/api#copy>`_
    """
    params = {"source_path": YPath(source_path, client=client),
              "destination_path": YPath(destination_path, client=client)}

    recursive = get_value(recursive, get_config(client)["yamr_mode"]["create_recursive"])
    set_param(params, "recursive", recursive, bool_to_string)
    set_param(params, "force", force, bool_to_string)
    set_param(params, "preserve_account", preserve_account, bool_to_string)
    return _make_formatted_transactional_request("copy", params, format=None, client=client)

def move(source_path, destination_path, recursive=None, preserve_account=None, force=None, client=None):
    """Moves (renames) Cypress node.

    :param source_path: source path.
    :type source_path: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    :param destination_path: destination path.
    :type destination_path: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    :param bool recursive: ``yt.wrapper.config["yamr_mode"]["create_recursive"]`` by default.
    :param bool preserve_account: preserve account.
    :param bool force: force.

    .. seealso:: `move on wiki <https://wiki.yandex-team.ru/yt/userdoc/api#move>`_
    """
    params = {"source_path": YPath(source_path, client=client),
              "destination_path": YPath(destination_path, client=client)}

    recursive = get_value(recursive, get_config(client)["yamr_mode"]["create_recursive"])
    set_param(params, "recursive", recursive, bool_to_string)
    set_param(params, "force", force, bool_to_string)
    set_param(params, "preserve_account", preserve_account, bool_to_string)
    return _make_formatted_transactional_request("move", params, format=None, client=client)

def concatenate(source_paths, destination_path, client=None):
    """Concatenates cypress nodes. This command applicable only to files and tables.

    :param source_paths: source paths.
    :type source_paths: list[str or :class:`YPath <yt.wrapper.ypath.YPath>`]
    :param destination_path: destination path.
    :type destination_path: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    """
    source_paths = builtins.list(imap(lambda path: YPath(path, client=client), source_paths))
    destination_path = YPath(destination_path, client=client)
    if not source_paths:
        raise YtError("Source paths must be non-empty")
    type = get(source_paths[0] + "/@type", client=client)
    if type not in ["file", "table"]:
        raise YtError("Type of '{0}' is not table or file".format(source_paths[0]))
    create(type, destination_path, ignore_existing=True, client=client)
    params = {"source_paths": source_paths,
              "destination_path": destination_path}
    _make_transactional_request("concatenate", params, client=client)

def link(target_path, link_path, recursive=False, ignore_existing=False, force=False, attributes=None, client=None):
    """Makes link to Cypress node.

    :param target_path: target path.
    :type target_path: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    :param link_path: link path.
    :type link_path: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    :param bool recursive: recursive.
    :param bool ignore_existing: ignore existing.

    .. seealso:: `link on wiki <https://wiki.yandex-team.ru/yt/userdoc/api#link>`_
    """
    params = {
        "target_path": YPath(target_path, client=client),
        "link_path": YPath(link_path, client=client),
        "recursive": bool_to_string(recursive),
        "ignore_existing": bool_to_string(ignore_existing),
        "force": bool_to_string(force)}
    set_param(params, "attributes", attributes)
    return _make_formatted_transactional_request(
        "link",
        params,
        format=None,
        client=client)


def list(path, max_size=None, format=None, absolute=None, attributes=None, sort=True, read_from=None, client=None):
    """Lists directory (map_node) content. Node type must be "map_node".

    :param path: path.
    :type path: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    :param int max_size: max output size.
    :param list attributes: desired node attributes in the response.
    :param format: command response format, by default - `None`.
    :type format: descendant of :class:`Format <yt.wrapper.format.Format>`
    :param bool absolute: convert relative paths to absolute. Works only if format isn't specified.
    :param bool sort: if set to `True` output will be sorted.

    .. note:: Output is never sorted if format is specified or result is incomplete, \
    i.e. path children count exceeds max_size.

    :return: raw YSON (string) by default, parsed YSON or JSON if format is not specified (= `None`).

    .. seealso:: `list on wiki <https://wiki.yandex-team.ru/yt/userdoc/api#list>`_
    """
    if format is not None and absolute:
        raise YtError("Option 'absolute' is supported only for non-specified format")

    def join(elem):
        return yson.to_yson_type("{0}/{1}".format(path, elem), elem.attributes)

    if max_size is None:
        max_size = 65535

    params = {
        "path": YPath(path, client=client),
        "max_size": max_size}
    set_param(params, "attributes", attributes)
    set_param(params, "read_from", read_from)
    result = _make_formatted_transactional_request(
        "list",
        params=params,
        format=format,
        client=client)
    if format is None and not result.attributes.get("incomplete", False) and sort:
        result.sort()
    if absolute and format is None:
        attributes = result.attributes
        result = yson.YsonList(imap(join, result))
        result.attributes = attributes
    return result

def exists(path, read_from=None, client=None):
    """Checks if Cypress node exists.

    :param path: path.
    :type path: str or :class:`YPath <yt.wrapper.ypath.YPath>`

    .. seealso:: `exists on wiki <https://wiki.yandex-team.ru/yt/userdoc/api#exists>`_
    """
    params = {"path": YPath(path, client=client)}
    set_param(params, "read_from", read_from)
    return parse_bool(
        _make_formatted_transactional_request(
            "exists",
            params,
            format=None,
            client=client))

def remove(path, recursive=False, force=False, client=None):
    """Removes Cypress node.

    :param path: path.
    :type path: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    :param bool recursive: recursive.
    :param bool force: force.

    .. seealso:: `remove on wiki <https://wiki.yandex-team.ru/yt/userdoc/api#remove>`_
    """
    _make_transactional_request(
        "remove",
        {
            "path": YPath(path, client=client),
            "recursive": bool_to_string(recursive),
            "force": bool_to_string(force)
        },
        client=client)

def create(type, path=None, recursive=False, ignore_existing=False, attributes=None, client=None):
    """Creates Cypress node.

    :param str type: one of ["table", "file", "map_node", "list_node", ...].
    :param path: path.
    :type path: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    :param bool recursive: ``yt.wrapper.config["yamr_mode"]["create_recursive"]`` by default.
    :param dict attributes: attributes.

    .. seealso:: `create on wiki <https://wiki.yandex-team.ru/yt/userdoc/api#create>`_
    """
    recursive = get_value(recursive, get_config(client)["yamr_mode"]["create_recursive"])
    params = {
        "type": type,
        "recursive": bool_to_string(recursive),
        "ignore_existing": bool_to_string(ignore_existing),
        "attributes": get_value(attributes, {})
    }
    if path is not None:
        params["path"] = YPath(path, client=client)
    return _make_formatted_transactional_request("create", params, format=None, client=client)

def mkdir(path, recursive=None, client=None):
    """Makes directory (Cypress node of map_node type).

    :param path: path.
    :type path: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    :param bool recursive: ``yt.wrapper.config["yamr_mode"]["create_recursive"]`` by default.
    """
    recursive = get_value(recursive, get_config(client)["yamr_mode"]["create_recursive"])
    return create("map_node", path, recursive=recursive, ignore_existing=recursive, client=client)

# TODO: maybe remove this methods
def get_attribute(path, attribute, default=_KWARG_SENTINEL, client=None):
    """Gets attribute of Cypress node.

    :param str path: path.
    :param str attribute: attribute.
    :param default: if node hasn't attribute `attribute` this value will be returned.
    """
    if default is not _KWARG_SENTINEL and attribute not in list_attributes(path, client=client):
        return default
    return get("%s/@%s" % (path, attribute), client=client)

def has_attribute(path, attribute, client=None):
    """Checks if Cypress node has attribute.

    :param str path: path.
    :param str attribute: attribute.
    """
    return exists("%s/@%s" % (path, attribute), client=client)

def set_attribute(path, attribute, value, client=None):
    """Sets Cypress node `attribute` to `value`.

    :param str path: path.
    :param str attribute: attribute.
    :param value: value.
    """
    return set("%s/@%s" % (path, attribute), value, client=client)

def list_attributes(path, attribute_path="", client=None):
    """Lists all attributes of Cypress node.

    :param str path: path.
    :param str attribute_path: attribute path.
    """
    return list("%s/@%s" % (path, attribute_path), client=client)

def get_type(path, client=None):
    """Gets Cypress node attribute type.

    :param str path: path.
    """
    return get_attribute(path, "type", client=client)

def find_free_subpath(path, client=None):
    """Generates some free random subpath.

    :param str path: path.
    :rtype: str
    """
    LENGTH = 10
    char_set = string.ascii_letters + string.digits
    while True:
        name = "".join([path] + get_option("_random_generator", client).sample(char_set, LENGTH))
        if not exists(name, client=client):
            return name

def search(root="", node_type=None,
           path_filter=None, object_filter=None, subtree_filter=None,
           map_node_order=lambda path, obj: sorted(obj),
           list_node_order=None,
           attributes=None, exclude=None, depth_bound=None,
           follow_links=False, read_from=None, client=None):
    """Searches for some nodes in Cypress subtree.

    :param root: path to search.
    :type root: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    :param node_type: node types.
    :type node_type: list[str]
    :param object_filter: filtering predicate.
    :param map_node_order: function that specifies order of traversing map_node children;
        that function should take two arguments (path, object)
        and should return iterable over object children;
        default map_node_order sorts children lexicographically;
        set it to None in order to switch off sorting.
    :param attributes: these attributes will be added to result objects.
    :type attributes: list[str]
    :param exclude: excluded paths.
    :type exclude: list[str]
    :param int depth_bound: recursion depth.
    :param bool follow_links: follow links.
    :return: result paths as iterable over :class:`YsonString <yt.yson.yson_types.YsonString>`.
    """
    # Deprecated. Default value "/" should be removed.
    if not root and not get_config(client)["prefix"]:
        root = "/"
    # Normalize path.
    root = str(YPath(root, client=client))

    attributes = get_value(attributes, [])

    request_attributes = deepcopy(flatten(attributes))
    request_attributes.append("type")
    request_attributes.append("opaque")

    exclude = deepcopy(flatten(get_value(exclude, ["//sys/operations"])))

    def safe_get(path, ignore_resolve_error=True):
        try:
            return get(path, attributes=request_attributes, read_from=read_from, client=client)
        except YtResponseError as rsp:
            if rsp.is_access_denied():
                logger.warning("Cannot traverse %s, access denied" % path)
            elif rsp.is_resolve_error() and ignore_resolve_error:
                logger.warning("Path %s is missing" % path)
            else:
                raise
        return None

    def is_opaque(object):
        # We have bug that get to document don't return attributes.
        return object.attributes.get("opaque", False) and object.attributes["type"] != "document"

    def walk(path, object, depth, ignore_opaque=False):
        if object is None:
            return
        if path in exclude or (depth_bound is not None and depth > depth_bound):
            return
        if subtree_filter is not None and not subtree_filter(path, object):
            return
        if is_opaque(object) and not ignore_opaque:
            for obj in walk(path, safe_get(path), depth, True):
                yield obj
            return

        object_type = object.attributes["type"]
        if object_type == "link" and follow_links:
            for obj in walk(path, safe_get(path), depth):
                yield obj
            return

        if (node_type is None or object_type in flatten(node_type)) and \
           (object_filter is None or object_filter(object)) and \
           (path_filter is None or path_filter(path)):
            yson_path_attributes = dict(ifilter(lambda item: item[0] in attributes, iteritems(object.attributes)))
            yson_path = yson.to_yson_type(path, attributes=yson_path_attributes)
            yield yson_path

        if object_type in ["account_map", "tablet_cell"]:
            object = safe_get(path)

        if isinstance(object, dict):
            if map_node_order is not None:
                items_iter = ((key, object[key]) for key in map_node_order(path, object))
            else:
                items_iter = iteritems(object)
            for key, value in items_iter:
                for obj in walk("{0}/{1}".format(path, escape_ypath_literal(key)), value, depth + 1):
                    yield obj

        if isinstance(object, builtins.list):
            if list_node_order is not None:
                enumeration = ((index, object[index]) for index in list_node_order(path, object))
            else:
                enumeration = enumerate(object)
            for index, value in enumeration:
                for obj in walk("{0}/{1}".format(path, index), value, depth + 1):
                    yield obj

    ignore_root_path_resolve_error = get_config(client)["ignore_root_path_resolve_error_in_search"]
    return walk(root, safe_get(root, ignore_resolve_error=ignore_root_path_resolve_error), 0, True)

def remove_with_empty_dirs(path, force=True, client=None):
    """Removes path and all empty dirs that appear after deletion.

    :param path: path.
    :type path: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    :param bool force: force.
    """
    path = YPath(path, client=client)
    while True:
        try:
            remove(path, recursive=True, force=True, client=client)
        except YtResponseError as error:
            if error.is_access_denied():
                logger.warning("Cannot remove %s, access denied", path)
                break
            else:
                raise
        # TODO(ignat): introduce ypath_dirname and use it here.
        path = YPath(os.path.dirname(str(path)), simplify=False)
        try:
            if str(path) == "//" or not exists(path, client=client) or list(path, client=client) or get(path + "/@acl", client=client):
                break
        except YtResponseError as err:
            if err.is_resolve_error():
                break
            else:
                raise

def create_revision_parameter(path, transaction_id=None, revision=None, client=None):
    """Creates revision parameter of the path.

    :param str path: path.
    :rtype: dict
    """
    if revision is None:
        revision = get_attribute(path, "revision")
    if transaction_id is None:
        transaction_id = get_option("TRANSACTION", client)
    return {"path": path, "transaction_id": transaction_id, "revision": revision}
