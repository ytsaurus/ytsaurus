from . import yson
from .config import get_config, get_option, get_command_param
from .common import flatten, get_value, YtError, set_param
from .errors import YtResponseError
from .driver import set_read_from_params, get_api_version
from .transaction_commands import (_make_transactional_request,
                                   _make_formatted_transactional_request)
from .transaction import Transaction
from .ypath import YPath, escape_ypath_literal, ypath_join, ypath_dirname
from .format import create_format
from .batch_response import apply_function_to_result
from .retries import Retrier, default_chaos_monkey
from .http_helpers import get_retriable_errors

import yt.logger as logger

from yt.packages.six import iteritems, string_types
from yt.packages.six.moves import builtins, map as imap, filter as ifilter

import string
from copy import deepcopy, copy as shallowcopy

# XXX(asaitgalin): Used in get_attribute function for `default` argument
# instead of None value to distinguish case when default argument
# is passed and is None from case when default is not passed.
class _KwargSentinelClass(object):
    __instance = None
    def __new__(cls):
        if cls.__instance is None:
            cls.__instance = object.__new__(cls)
            cls.__instance.name = "_KwargSentinelClassInstance"
        return cls.__instance
_KWARG_SENTINEL = _KwargSentinelClass()

def get(path, max_size=None, attributes=None, format=None, read_from=None, cache_sticky_group_size=None, client=None):
    """Gets Cypress node content (attribute tree).

    :param path: path to tree, it must exist!
    :type path: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    :param list attributes: desired node attributes in the response.
    :param format: output format (by default python dict automatically parsed from YSON).
    :type format: str or descendant of :class:`Format <yt.wrapper.format.Format>`
    :return: node tree content in `format`

    Be careful: attributes have specific representation in JSON format.

    .. seealso:: `get on wiki <https://wiki.yandex-team.ru/yt/userdoc/api#get>`_
    """
    if max_size is None:
        max_size = 65535

    params = {
        "path": YPath(path, client=client),
        "max_size": max_size}
    set_param(params, "attributes", attributes)
    set_read_from_params(params, read_from, cache_sticky_group_size)
    if get_api_version(client) == "v4":
        set_param(params, "return_only_value", True)
    result = _make_formatted_transactional_request(
        "get",
        params=params,
        format=format,
        client=client)
    return result

def set(path, value, format=None, recursive=False, force=None, client=None):
    """Sets new value to Cypress node.

    :param path: path.
    :type path: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    :param value: json-able object.
    :param format: format of the value. If format is None than value should be \
    object that can be dumped to JSON of YSON. Otherwise it should be string.
    :param bool recursive: recursive.

    .. seealso:: `set on wiki <https://wiki.yandex-team.ru/yt/userdoc/api#set>`_
    """
    if format is None:
        value = yson.dumps(value)
        format = "yson"

    if isinstance(format, string_types):
        format = create_format(format)

    params = {
        "path": YPath(path, client=client),
        "input_format": format.to_yson_type(),
    }
    set_param(params, "recursive", recursive)
    set_param(params, "force", force)

    return _make_transactional_request(
        "set",
        params,
        data=value,
        client=client)

def copy(source_path, destination_path,
         recursive=None, ignore_existing=None, preserve_account=None,
         preserve_expiration_time=None, preserve_creation_time=None,
         force=None, pessimistic_quota_check=None, client=None):
    """Copies Cypress node.

    :param source_path: source path.
    :type source_path: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    :param destination_path: destination path.
    :type destination_path: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    :param bool recursive: ``yt.wrapper.config["yamr_mode"]["create_recursive"]`` by default.
    :param bool ignore_existing: ignore existing.
    :param bool preserve_account: preserve account.
    :param bool preserve_expiration_time: preserve expiration time.
    :param bool preserve_creation_time: preserve creation time.
    :param bool force: force.
    :param bool pessimistic_quota_check: pessimistic quota check.

    .. seealso:: `copy on wiki <https://wiki.yandex-team.ru/yt/userdoc/api#copy>`_
    """
    params = {"source_path": YPath(source_path, client=client),
              "destination_path": YPath(destination_path, client=client)}

    recursive = get_value(recursive, get_config(client)["yamr_mode"]["create_recursive"])
    set_param(params, "recursive", recursive)
    set_param(params, "ignore_existing", ignore_existing)
    set_param(params, "force", force)
    set_param(params, "preserve_account", preserve_account)
    set_param(params, "preserve_expiration_time", preserve_expiration_time)
    set_param(params, "preserve_creation_time", preserve_creation_time)
    set_param(params, "pessimistic_quota_check", pessimistic_quota_check)
    return _make_formatted_transactional_request("copy", params, format=None, client=client)

def move(source_path, destination_path,
         recursive=None, preserve_account=None, preserve_expiration_time=False, force=None,
         pessimistic_quota_check=None, client=None):
    """Moves (renames) Cypress node.

    :param source_path: source path.
    :type source_path: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    :param destination_path: destination path.
    :type destination_path: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    :param bool recursive: ``yt.wrapper.config["yamr_mode"]["create_recursive"]`` by default.
    :param bool preserve_account: preserve account.
    :param bool preserve_expiration_time: preserve expiration time.
    :param bool force: force.
    :param bool pessimistic_quota_check: pessimistic quota check.

    .. seealso:: `move on wiki <https://wiki.yandex-team.ru/yt/userdoc/api#move>`_
    """
    params = {"source_path": YPath(source_path, client=client),
              "destination_path": YPath(destination_path, client=client)}

    recursive = get_value(recursive, get_config(client)["yamr_mode"]["create_recursive"])
    set_param(params, "recursive", recursive)
    set_param(params, "force", force)
    set_param(params, "preserve_account", preserve_account)
    set_param(params, "preserve_expiration_time", preserve_expiration_time)
    set_param(params, "pessimistic_quota_check", pessimistic_quota_check)
    return _make_formatted_transactional_request("move", params, format=None, client=client)

class _ConcatenateRetrier(Retrier):
    def __init__(self, type, source_paths, destination_path, client):
        self.type = type
        self.source_paths = source_paths
        self.destination_path = destination_path
        self.client = client

        retry_config = get_config(client)["concatenate_retries"]
        chaos_monkey_enable = get_option("_ENABLE_HEAVY_REQUEST_CHAOS_MONKEY", client)
        super(_ConcatenateRetrier, self).__init__(retry_config=retry_config,
                                                  exceptions=get_retriable_errors(),
                                                  chaos_monkey=default_chaos_monkey(chaos_monkey_enable))

    def action(self):
        title = "Python wrapper: concatenate"
        with Transaction(attributes={"title": title},
                         client=self.client):
            create(self.type, self.destination_path, ignore_existing=True, client=self.client)
            params = {"source_paths": self.source_paths,
                      "destination_path": self.destination_path}
            _make_transactional_request("concatenate", params, client=self.client)

    def except_action(self, error, attempt):
        logger.warning("Concatenate failed with error %s", repr(error))


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

    retrier = _ConcatenateRetrier(type, source_paths, destination_path, client=client)
    retrier.run()


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
    }
    set_param(params, "recursive", recursive)
    set_param(params, "ignore_existing", ignore_existing)
    set_param(params, "force", force)
    set_param(params, "attributes", attributes)
    return _make_formatted_transactional_request(
        "link",
        params,
        format=None,
        client=client)


def list(path, max_size=None, format=None, absolute=None, attributes=None, sort=True, read_from=None, cache_sticky_group_size=None, client=None):
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

    def _process_result(request_result):
        if format is None and not request_result.attributes.get("incomplete", False) and sort:
            request_result.sort()
        if absolute and format is None:
            attributes = request_result.attributes
            request_result = yson.YsonList(imap(join, request_result))
            request_result.attributes = attributes

        return request_result

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
    set_read_from_params(params, read_from, cache_sticky_group_size)
    if get_api_version(client) == "v4":
        set_param(params, "return_only_value", True)
    result = _make_formatted_transactional_request(
        "list",
        params=params,
        format=format,
        client=client)
    return apply_function_to_result(_process_result, result)

def exists(path, read_from=None, cache_sticky_group_size=None, client=None):
    """Checks if Cypress node exists.

    :param path: path.
    :type path: str or :class:`YPath <yt.wrapper.ypath.YPath>`

    .. seealso:: `exists on wiki <https://wiki.yandex-team.ru/yt/userdoc/api#exists>`_
    """
    def _process_result(result):
        return result["value"] if get_api_version(client) == "v4" else result
    params = {"path": YPath(path, client=client)}
    set_read_from_params(params, read_from, cache_sticky_group_size)
    result = _make_formatted_transactional_request(
        "exists",
        params,
        format=None,
        client=client)
    return apply_function_to_result(_process_result, result)

def remove(path, recursive=False, force=False, client=None):
    """Removes Cypress node.

    :param path: path.
    :type path: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    :param bool recursive: recursive.
    :param bool force: force.

    .. seealso:: `remove on wiki <https://wiki.yandex-team.ru/yt/userdoc/api#remove>`_
    """
    params = {
        "path": YPath(path, client=client),
    }
    set_param(params, "recursive", recursive)
    set_param(params, "force", force)
    return _make_transactional_request("remove", params, client=client)

def create(type, path=None, recursive=False, ignore_existing=False, force=None, attributes=None, client=None):
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
    }
    set_param(params, "recursive", recursive)
    set_param(params, "ignore_existing", ignore_existing)
    set_param(params, "attributes", attributes)
    set_param(params, "force", force)
    if path is not None:
        params["path"] = YPath(path, client=client)

    def _process_result(result):
        if get_api_version(client) == "v4":
            if "node_id" in result:
                return result["node_id"]
            if "object_id" in result:
                return result["object_id"]
        else:
            return result

    result = _make_formatted_transactional_request("create", params, format=None, client=client)
    return apply_function_to_result(_process_result, result)

def externalize(path, cell_tag, client=None):
    """Externalize cypress node

    :param path: path.
    :type path: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    :param int: cell_tag.
    """
    params = {
        "path": YPath(path, client=client),
        "cell_tag": cell_tag,
    }
    return _make_transactional_request("externalize", params, client=client)

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
    attribute_path = "{0}/@{1}".format(YPath(path, client=client), attribute)
    if default is not _KWARG_SENTINEL and not exists(attribute_path, client=client):
        return default
    return get(attribute_path, client=client)

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

def search(root="", node_type=None, path_filter=None, object_filter=None, subtree_filter=None,
           map_node_order=lambda path, obj: sorted(obj), list_node_order=None, attributes=None,
           exclude=None, depth_bound=None, follow_links=False, read_from=None, cache_sticky_group_size=None,
           enable_batch_mode=None, client=None):
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

    # TODO(ostyakov): Remove local import
    from .batch_helpers import create_batch_client

    if not root and not get_config(client)["prefix"]:
        root = "/"
    root = str(YPath(root, client=client))
    attributes = get_value(attributes, [])

    request_attributes = deepcopy(flatten(attributes))
    request_attributes.append("type")
    request_attributes.append("opaque")

    exclude = flatten(get_value(exclude, []))
    if root != "//sys/operations":
        exclude = exclude + ["//sys/operations"]

    if enable_batch_mode is None:
        enable_batch_mode = get_config(client)["enable_batch_mode_for_search"]

    class CompositeNode(object):
        def __init__(self, path, depth, content=None, ignore_opaque=False,
                     ignore_resolve_error=True, force_search=False):
            self.path = path
            self.depth = depth
            self.ignore_opaque = ignore_opaque
            self.ignore_resolve_error = ignore_resolve_error
            self.content = content
            self.force_search = force_search
            self.yield_path = True
            # It is necessary to avoid infinite recursion.
            self.followed_by_link = False

    def process_response_error(error, node):
        node.content = None
        if error.is_access_denied():
            logger.warning("Cannot traverse %s, access denied" % node.path)
        elif error.is_resolve_error() and node.ignore_resolve_error:
            logger.warning("Path %s is missing" % node.path)
        else:
            raise

    def is_opaque(content):
        # We have bug that get to document don't return attributes.
        return \
            content.attributes.get("opaque", False) and content.attributes["type"] != "document" or \
            content.attributes["type"] in ("account_map", "tablet_cell")

    def safe_batch_get(nodes, batch_client):
        get_result = []
        for node in nodes:
            get_result.append(batch_client.get(node.path, attributes=request_attributes, read_from=read_from, cache_sticky_group_size=cache_sticky_group_size))
        batch_client.commit_batch()

        for content, node in zip(get_result, nodes):
            try:
                if content.get_error():
                    raise YtResponseError(content.get_error())
                node.content = content.get_result()
            except YtResponseError as rsp:
                process_response_error(rsp, node)
            yield node

    def safe_get(nodes, client):
        for node in nodes:
            try:
                node.content = get(node.path, attributes=request_attributes, client=client, read_from=read_from, cache_sticky_group_size=cache_sticky_group_size)
            except YtResponseError as rsp:
                process_response_error(rsp, node)
            yield node

    if enable_batch_mode:
        batch_client = create_batch_client(client=client)

    ignore_root_path_resolve_error = get_config(client)["ignore_root_path_resolve_error_in_search"]

    def process_node(node, nodes_to_request):
        if node.content is None:
            return
        if node.path in exclude or (depth_bound is not None and node.depth > depth_bound):
            return
        if subtree_filter is not None and not subtree_filter(node.path, node.content):
            return

        # If content is YsonEntity and does not have attributes at all
        # (even "type") then user does not have permission to access it.
        if isinstance(node.content, yson.YsonEntity) and not node.content.attributes:
            logger.warning("Access to %s is denied", node.path)
            return

        object_type = node.content.attributes["type"]

        if node.followed_by_link and object_type == "link":
            return

        if is_opaque(node.content) and not node.ignore_opaque and object_type != "link":
            new_node = shallowcopy(node)
            new_node.ignore_opaque = True
            new_node.yield_path = False
            nodes_to_request.append(new_node)

        if object_type == "link" and follow_links:
            assert not node.content
            new_node = shallowcopy(node)
            new_node.followed_by_link = True
            new_node.ignore_opaque = True
            nodes_to_request.append(new_node)

        if node.yield_path and \
                (node_type is None or object_type in flatten(node_type)) and \
                (object_filter is None or object_filter(node.content)) and \
                (path_filter is None or path_filter(node.path)):
            yson_path_attributes = dict(ifilter(lambda item: item[0] in attributes,
                                                iteritems(node.content.attributes)))
            yson_path = yson.to_yson_type(node.path, attributes=yson_path_attributes)
            yield yson_path

        if isinstance(node.content, dict):
            if map_node_order is not None:
                items_iter = ((key, node.content[key]) for key in map_node_order(node.path, node.content))
            else:
                items_iter = iteritems(node.content)
            for key, value in items_iter:
                path = "{0}/{1}".format(node.path, escape_ypath_literal(key))
                for yson_path in process_node(CompositeNode(path, node.depth + 1, value), nodes_to_request):
                    yield yson_path

        if isinstance(node.content, builtins.list):
            if list_node_order is not None:
                enumeration = ((index, node.content[index]) for index in list_node_order(
                    node.path, node.content))
            else:
                enumeration = enumerate(node.content)
            for index, value in enumeration:
                path = ypath_join(node.path, str(index))
                for yson_path in process_node(CompositeNode(path, node.depth + 1, value), nodes_to_request):
                    yield yson_path

    nodes_to_request = []
    nodes_to_request.append(CompositeNode(root, 0, ignore_opaque=True, ignore_resolve_error=ignore_root_path_resolve_error))

    while nodes_to_request:
        if enable_batch_mode:
            nodes_to_process = safe_batch_get(nodes_to_request, batch_client)
        else:
            nodes_to_process = safe_get(nodes_to_request, client)
        nodes_to_request = []

        for node in nodes_to_process:
            for yson_path in process_node(node, nodes_to_request):
                yield yson_path

def remove_with_empty_dirs(path, force=True, client=None):
    """Removes path and all empty dirs that appear after deletion.

    :param path: path.
    :type path: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    :param bool force: force.
    """
    path = YPath(path, client=client)
    while True:
        try:
            remove(path, recursive=True, force=force, client=client)
        except YtResponseError as error:
            if error.is_access_denied():
                logger.warning("Cannot remove %s, access denied", path)
                break
            else:
                raise

        path = ypath_dirname(path)
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
        transaction_id = get_command_param("transaction_id", client)
    return {"path": path, "transaction_id": transaction_id, "revision": revision}
