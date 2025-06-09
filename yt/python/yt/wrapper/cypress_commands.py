from . import yson
from .config import get_config, get_option, get_command_param
from .common import flatten, get_value, YtError, set_param, deprecated
from .errors import YtResponseError
from .driver import make_request, make_formatted_request, set_master_read_params, get_api_version, get_structured_format
from .format import Format
from .transaction import Transaction
from .ypath import YPath, escape_ypath_literal, ypath_join, ypath_dirname
from .batch_response import apply_function_to_result
from .retries import Retrier, default_chaos_monkey
from .http_helpers import get_retriable_errors
from .schema import TableSchema

import yt.logger as logger

from yt.yson import is_unicode, get_bytes

import builtins
import string
from copy import deepcopy, copy as shallowcopy
from typing import Union, Optional, Literal, Callable, Any, List, Dict


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


class _MapOrderSorted(object):
    __instance = None

    def __new__(cls):
        if cls.__instance is None:
            cls.__instance = object.__new__(cls)
            cls.__instance.name = "_MapOrderSorted"
        return cls.__instance


MAP_ORDER_SORTED = _MapOrderSorted()


def _is_batch_client(client):
    # XXX: Import inside function to avoid import loop.
    # batch_client imports all API, including current module
    from .batch_client import BatchClient

    return isinstance(client, BatchClient)


def get(
    path: Union[str, YPath],
    max_size: Optional[int] = None,
    attributes: Optional[List[str]] = None,
    format: Optional[Union[str, Format]] = None,
    read_from: Optional[Literal["cache"]] = None,
    cache_sticky_group_size: Optional[bool] = None,
    suppress_transaction_coordinator_sync: Optional[bool] = None,
    suppress_upstream_sync: Optional[bool] = None,
    client=None,
):
    """Gets Cypress node content (attribute tree).

    :param path: path to tree, it must exist!
    :type path: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    :param list attributes: desired node attributes in the response.
    :param format: output format (by default python dict automatically parsed from YSON).
    :type format: str or descendant of :class:`Format <yt.wrapper.format.Format>`
    :return: node tree content in `format`

    Be careful: attributes have specific representation in JSON format.

    .. seealso:: `get in the docs <https://ytsaurus.tech/docs/en/api/commands#get>`_
    """
    if max_size is None:
        max_size = 65535
    params = {
        "path": YPath(path, client=client),
        "max_size": max_size}
    set_param(params, "attributes", attributes)
    set_param(params, "suppress_transaction_coordinator_sync", suppress_transaction_coordinator_sync)
    set_param(params, "suppress_upstream_sync", suppress_upstream_sync)
    set_master_read_params(params, read_from, cache_sticky_group_size)
    if get_api_version(client) == "v4":
        set_param(params, "return_only_value", True)
    result = make_formatted_request(
        "get",
        params=params,
        format=format,
        client=client)
    return result


def set(
    path: Union[str, YPath],
    value,
    format=None,
    recursive: bool = False,
    force: bool = None,
    suppress_transaction_coordinator_sync: bool = None,
    suppress_upstream_sync: bool = None,
    client=None,
):
    """Sets new value to Cypress node.

    :param path: path.
    :type path: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    :param value: json-able object.
    :param format: format of the value. If format is None than value should be \
    object that can be dumped to JSON of YSON. Otherwise it should be string.
    :param bool recursive: recursive.

    .. seealso:: `set in the docs <https://ytsaurus.tech/docs/en/api/commands#set>`_
    """
    is_format_specified = format is not None
    format = get_structured_format(format, client=client)
    if not is_format_specified:
        value = format.dumps_node(value)

    params = {
        "path": YPath(path, client=client),
        "input_format": format.to_yson_type(),
    }
    set_param(params, "recursive", recursive)
    set_param(params, "force", force)
    set_param(params, "suppress_transaction_coordinator_sync", suppress_transaction_coordinator_sync)
    set_param(params, "suppress_upstream_sync", suppress_upstream_sync)

    return make_request(
        "set",
        params,
        data=value,
        client=client)


class _CrosscellCopyMoveRetrier(Retrier):
    def __init__(self, method, params, client):
        self.method = method
        self.params = params
        self.client = client

        retry_config = get_config(client)["proxy"]["retries"]
        chaos_monkey_enable = get_option("_ENABLE_HEAVY_REQUEST_CHAOS_MONKEY", client)
        super(_CrosscellCopyMoveRetrier, self).__init__(
            retry_config=retry_config,
            exceptions=get_retriable_errors(),
            chaos_monkey=default_chaos_monkey(chaos_monkey_enable)
        )

    def action(self):
        title = "Python wrapper: {}".format(self.method)
        with Transaction(attributes={"title": title}, client=self.client):
            set_param(self.params, "enable_cross_cell_copying", True)
            return make_formatted_request(self.method, self.params, format=None, allow_retries=False, client=self.client)

    def except_action(self, error, attempt):
        logger.warning("Copy/move failed with error %s", repr(error))

    @classmethod
    def copy(cls, params, client):
        return cls._method("copy", params, client)

    @classmethod
    def move(cls, params, client):
        return cls._method("move", params, client)

    @classmethod
    def _method(cls, method, params, client):
        try:
            # try copy/move without any extra protection
            set_param(params, "enable_cross_cell_copying", False)
            return make_formatted_request(method, params, format=None, client=client)
        except YtResponseError as ex:
            if ex.is_prohibited_cross_cell_copy():
                # it's copy/move from portal, make it retryable
                return cls(method, params, client).run()
            else:
                raise


def copy(
    source_path: Union[str, YPath],
    destination_path: Union[str, YPath],
    recursive: bool = None,
    force: bool = None,
    ignore_existing: bool = None,
    lock_existing: bool = None,
    preserve_account: bool = None,
    preserve_owner: bool = None,
    preserve_acl: bool = None,
    preserve_expiration_time: bool = None,
    preserve_expiration_timeout: bool = None,
    preserve_creation_time: bool = None,
    preserve_modification_time: bool = None,
    pessimistic_quota_check: bool = None,
    enable_cross_cell_copying: bool = None,
    client=None
):
    """Copies Cypress node.

    :param source_path: source path.
    :type source_path: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    :param destination_path: destination path.
    :type destination_path: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    :param bool recursive: ``yt.wrapper.config["yamr_mode"]["create_recursive"]`` by default.
    :param bool ignore_existing: ignore existing.
    :param bool lock_existing: lock existing node.
    :param bool preserve_account: preserve account.
    :param bool preserve_owner: preserve owner.
    :param bool preserve_acl: preserve acl.
    :param bool preserve_expiration_time: preserve expiration time.
    :param bool preserve_expiration_timeout: preserve expiration timeout.
    :param bool preserve_creation_time: preserve creation time.
    :param bool preserve_modification_time: preserve modification time.
    :param bool force: force.
    :param bool pessimistic_quota_check: pessimistic quota check.
    :param bool enable_cross_cell_copying: enable cross cell copying.

    .. seealso:: `copy in the docs <https://ytsaurus.tech/docs/en/api/commands#copy>`_
    """
    params = {"source_path": YPath(source_path, client=client),
              "destination_path": YPath(destination_path, client=client)}

    recursive = get_value(recursive, get_config(client)["yamr_mode"]["create_recursive"])
    set_param(params, "recursive", recursive)
    set_param(params, "ignore_existing", ignore_existing)
    set_param(params, "lock_existing", lock_existing)
    set_param(params, "force", force)
    set_param(params, "preserve_account", preserve_account)
    set_param(params, "preserve_owner", preserve_owner)
    set_param(params, "preserve_acl", preserve_acl)
    set_param(params, "preserve_expiration_time", preserve_expiration_time)
    set_param(params, "preserve_expiration_timeout", preserve_expiration_timeout)
    set_param(params, "preserve_creation_time", preserve_creation_time)
    set_param(params, "preserve_modification_time", preserve_modification_time)
    set_param(params, "pessimistic_quota_check", pessimistic_quota_check)

    if _is_batch_client(client) or enable_cross_cell_copying is not None:
        set_param(params, "enable_cross_cell_copying", enable_cross_cell_copying)
        return make_formatted_request("copy", params, format=None, client=client)
    else:
        return _CrosscellCopyMoveRetrier.copy(params, client)


def move(
    source_path: Union[str, YPath],
    destination_path: Union[str, YPath],
    recursive: bool = None,
    force: bool = None,
    preserve_account: bool = None,
    preserve_owner: bool = None,
    preserve_acl: bool = None,
    preserve_expiration_time: bool = None,
    preserve_expiration_timeout: bool = None,
    preserve_creation_time: bool = None,
    preserve_modification_time: bool = None,
    pessimistic_quota_check: bool = None,
    enable_cross_cell_copying: bool = None,
    client=None,
):
    """Moves (renames) Cypress node.

    :param source_path: source path.
    :type source_path: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    :param destination_path: destination path.
    :type destination_path: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    :param bool recursive: ``yt.wrapper.config["yamr_mode"]["create_recursive"]`` by default.
    :param bool preserve_account: preserve account.
    :param bool preserve_owner: preserve owner.
    :param bool preserve_acl: preserve acl.
    :param bool preserve_expiration_time: preserve expiration time.
    :param bool preserve_expiration_timeout: preserve expiration timeout.
    :param bool preserve_creation_time: preserve creation time.
    :param bool preserve_modification_time: preserve modification time.
    :param bool force: force.
    :param bool pessimistic_quota_check: pessimistic quota check.
    :param bool enable_cross_cell_copying: enable cross cell copying.

    .. seealso:: `move in the docs <https://ytsaurus.tech/docs/en/api/commands#move>`_
    """
    params = {"source_path": YPath(source_path, client=client),
              "destination_path": YPath(destination_path, client=client)}

    recursive = get_value(recursive, get_config(client)["yamr_mode"]["create_recursive"])
    set_param(params, "recursive", recursive)
    set_param(params, "force", force)
    set_param(params, "preserve_account", preserve_account)
    set_param(params, "preserve_owner", preserve_owner)
    set_param(params, "preserve_acl", preserve_acl)
    set_param(params, "preserve_expiration_time", preserve_expiration_time)
    set_param(params, "preserve_expiration_timeout", preserve_expiration_timeout)
    set_param(params, "preserve_creation_time", preserve_creation_time)
    set_param(params, "preserve_modification_time", preserve_modification_time)
    set_param(params, "pessimistic_quota_check", pessimistic_quota_check)

    if _is_batch_client(client) or enable_cross_cell_copying is not None:
        set_param(params, "enable_cross_cell_copying", enable_cross_cell_copying)
        return make_formatted_request("move", params, format=None, client=client)
    else:
        return _CrosscellCopyMoveRetrier.move(params, client)


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
            make_request("concatenate", params, client=self.client)

    def except_action(self, error, attempt):
        logger.warning("Concatenate failed with error %s", repr(error))


def concatenate(
    source_paths: Union[str, YPath],
    destination_path: Union[str, YPath],
    client=None,
):
    """Concatenates cypress nodes. This command applicable only to files and tables.

    :param source_paths: source paths.
    :type source_paths: list[str or :class:`YPath <yt.wrapper.ypath.YPath>`]
    :param destination_path: destination path.
    :type destination_path: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    """
    source_paths = builtins.list(map(lambda path: YPath(path, client=client), source_paths))
    destination_path = YPath(destination_path, client=client)
    if not source_paths:
        raise YtError("Source paths must be non-empty")
    type = get(source_paths[0] + "/@type", client=client)
    if type not in ["file", "table"]:
        raise YtError("Type of '{0}' is not table or file".format(source_paths[0]))

    retrier = _ConcatenateRetrier(type, source_paths, destination_path, client=client)
    retrier.run()


def link(
    target_path: Union[str, YPath],
    link_path: Union[str, YPath],
    recursive: bool = False,
    ignore_existing: bool = False,
    lock_existing: bool = None,
    force: bool = False,
    attributes: Optional[Dict[str, Any]] = None,
    client=None,
):
    """Makes link to Cypress node.

    :param target_path: target path.
    :type target_path: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    :param link_path: link path.
    :type link_path: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    :param bool recursive: recursive.

    :param bool ignore_existing: ignore existing.
    :param bool lock_existing: lock existing node.

    .. seealso:: `link in the docs <https://ytsaurus.tech/docs/en/api/commands#link>`_
    """
    params = {
        "target_path": YPath(target_path, client=client),
        "link_path": YPath(link_path, client=client),
    }
    set_param(params, "recursive", recursive)
    set_param(params, "ignore_existing", ignore_existing)
    set_param(params, "lock_existing", lock_existing)
    set_param(params, "force", force)
    set_param(params, "attributes", attributes)
    return make_formatted_request(
        "link",
        params,
        format=None,
        client=client)


def list(
    path: Union[str, YPath],
    max_size: Optional[int] = None,
    format: Optional[Format] = None,
    absolute: bool = None,
    attributes: Optional[List[str]] = None,
    sort: bool = True,
    read_from=None,
    cache_sticky_group_size: int = None,
    suppress_transaction_coordinator_sync: bool = None,
    suppress_upstream_sync: bool = None,
    client=None,
):
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

    .. seealso:: `list in the docs <https://ytsaurus.tech/docs/en/api/commands#list>`_
    """

    def _process_result(request_result):
        if format is None and not request_result.attributes.get("incomplete", False) and sort:
            request_result.sort()
        if absolute and format is None:
            attributes = request_result.attributes
            request_result = yson.YsonList(map(join, request_result))
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
    set_param(params, "suppress_transaction_coordinator_sync", suppress_transaction_coordinator_sync)
    set_param(params, "suppress_upstream_sync", suppress_upstream_sync)
    set_master_read_params(params, read_from, cache_sticky_group_size)
    if get_api_version(client) == "v4":
        set_param(params, "return_only_value", True)
    result = make_formatted_request(
        "list",
        params=params,
        format=format,
        client=client)
    return apply_function_to_result(_process_result, result)


def exists(
    path: Union[str, YPath],
    read_from: str = None,
    cache_sticky_group_size: int = None,
    suppress_transaction_coordinator_sync: bool = None,
    client=None,
):
    """Checks if Cypress node exists.

    :param path: path.
    :type path: str or :class:`YPath <yt.wrapper.ypath.YPath>`

    .. seealso:: `exists in the docs <https://ytsaurus.tech/docs/en/api/commands#exists>`_
    """
    def _process_result(result):
        return result["value"] if get_api_version(client) == "v4" else result
    params = {"path": YPath(path, client=client)}
    set_master_read_params(params, read_from, cache_sticky_group_size)
    set_param(params, "suppress_transaction_coordinator_sync", suppress_transaction_coordinator_sync)
    result = make_formatted_request(
        "exists",
        params,
        format=None,
        client=client)
    return apply_function_to_result(_process_result, result)


def remove(
    path: Union[str, YPath],
    recursive: bool = False,
    force: bool = False,
    client=None,
):
    """Removes Cypress node.

    :param path: path.
    :type path: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    :param bool recursive: recursive.
    :param bool force: force.

    .. seealso:: `remove in the docs <https://ytsaurus.tech/docs/en/api/commands#remove>`_
    """
    params = {
        "path": YPath(path, client=client),
    }
    set_param(params, "recursive", recursive)
    set_param(params, "force", force)
    return make_request("remove", params, client=client)


def create(
    type: Literal["table", "file", "map_node", "list_node"],
    path: Union[str, YPath, None] = None,
    recursive: bool = False,
    ignore_existing: bool = False,
    lock_existing: bool = None,
    force: bool = None,
    attributes: Optional[Dict[str, Any]] = None,
    ignore_type_mismatch: bool = False,
    client=None,
):
    """Creates Cypress node.

    :param str type: one of ["table", "file", "map_node", "list_node", ...].
    :param path: path.
    :type path: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    :param bool recursive: ``yt.wrapper.config["yamr_mode"]["create_recursive"]`` by default.
    :param bool ignore_existing: ignore existing node.
    :param bool lock_existing: lock existing node.
    :param bool force: force.
    :param dict attributes: attributes.
    :param bool ignore_type_mismatch: ignore type mismatch with existing node.

    .. seealso:: `create in the docs <https://ytsaurus.tech/docs/en/api/commands#create>`_
    """
    recursive = get_value(recursive, get_config(client)["yamr_mode"]["create_recursive"])
    params = {
        "type": type,
    }
    set_param(params, "recursive", recursive)
    set_param(params, "ignore_existing", ignore_existing)
    set_param(params, "lock_existing", lock_existing)
    set_param(params, "attributes", attributes)
    set_param(params, "force", force)
    set_param(params, "ignore_type_mismatch", ignore_type_mismatch)
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

    result = make_formatted_request("create", params, format=None, client=client)
    return apply_function_to_result(_process_result, result)


def externalize(
    path: Union[str, YPath],
    cell_tag: int,
    client=None,
):
    """Externalize cypress node

    :param path: path.
    :type path: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    :param int: cell_tag.
    """
    params = {
        "path": YPath(path, client=client),
        "cell_tag": cell_tag,
    }
    return make_request("externalize", params, client=client)


def internalize(path: Union[str, YPath], client=None):
    """Internalize cypress node

    :param path: path.
    :type path: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    """
    params = {
        "path": YPath(path, client=client)
    }
    return make_request("internalize", params, client=client)


def mkdir(
    path: Union[str, YPath],
    recursive: bool = None,
    client=None,
):
    """Makes directory (Cypress node of map_node type).

    :param path: path.
    :type path: str or :class:`YPath <yt.wrapper.ypath.YPath>`
    :param bool recursive: ``yt.wrapper.config["yamr_mode"]["create_recursive"]`` by default.
    """
    recursive = get_value(recursive, get_config(client)["yamr_mode"]["create_recursive"])
    return create("map_node", path, recursive=recursive, ignore_existing=recursive, client=client)


def _check_attribute_name(attribute_name):
    if "/" in attribute_name:
        raise YtError("Attribute commands forbid to use '/' in attribute names")
    if "@" in attribute_name:
        raise YtError("Attribute commands forbid to use '@' in attribute names")


def get_attribute(path: Union[str, YPath], attribute: str, default=_KWARG_SENTINEL, client=None):
    """Gets attribute of Cypress node.

    :param str path: path.
    :param str attribute: attribute.
    :param default: if node hasn't attribute `attribute` this value will be returned.
    """
    def process_default_value(result, raw_error):
        if raw_error is not None:
            error = YtResponseError(raw_error)
            if default is not _KWARG_SENTINEL and error.is_resolve_error():
                return default, None
        return result, raw_error

    _check_attribute_name(attribute)

    attribute_path = "{0}/@{1}".format(YPath(path, client=client), attribute)

    if _is_batch_client(client):
        return apply_function_to_result(
            process_default_value,
            get(attribute_path, client=client),
            include_error=True)
    else:
        try:
            return get(attribute_path, client=client)
        except YtResponseError as err:
            if default is not _KWARG_SENTINEL and err.is_resolve_error():
                return default
            raise


def has_attribute(path: Union[str, YPath], attribute: str, client=None):
    """Checks if Cypress node has attribute.

    :param str path: path.
    :param str attribute: attribute.
    """
    _check_attribute_name(attribute)
    return exists("%s/@%s" % (path, attribute), client=client)


def set_attribute(path: Union[str, YPath], attribute: str, value, client=None):
    """Sets Cypress node `attribute` to `value`.

    :param str path: path.
    :param str attribute: attribute.
    :param value: value.
    """
    _check_attribute_name(attribute)
    return set("%s/@%s" % (path, attribute), value, client=client)


def remove_attribute(path: Union[str, YPath], attribute: str, client=None):
    """Removes Cypress node `attribute`

    :param str path: path.
    :param str attribute: attribute.
    """
    _check_attribute_name(attribute)
    return remove("%s/@%s" % (path, attribute), client=client)


@deprecated(alternative="get 'type' attribute")
def get_type(path: Union[str, YPath], client=None):
    """Gets Cypress node attribute type.

    :param str path: path.
    """
    return get_attribute(path, "type", client=client)


def find_free_subpath(path: Union[str, YPath], client=None):
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


def search(
    root: Union[str, YPath, Literal[""]] = "",
    node_type: List[str] = None,
    path_filter: Callable[[str], bool] = None,
    object_filter: Callable[[Any], bool] = None,
    subtree_filter: Callable[[str, Any], bool] = None,
    map_node_order: Callable[[str, List[Any]], List[int]] = MAP_ORDER_SORTED,
    list_node_order: Callable[[str, List[Any]], List[int]] = None,
    attributes: List[str] = None,
    exclude: List[str] = None,
    depth_bound: int = None,
    follow_links: bool = False,
    read_from: Literal["cache"] = None,
    cache_sticky_group_size: int = None,
    enable_batch_mode: bool = None,
    client=None,
):
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
    :param lambda action: apply given method to each found path.
    :return: result paths as iterable over :class:`YsonString <yt.yson.yson_types.YsonString>`.
    """

    # TODO(ostyakov): Remove local import
    from .batch_helpers import create_batch_client

    if map_node_order is MAP_ORDER_SORTED:
        def _convert_to_unicode(str_obj):
            if is_unicode(str_obj):
                return str_obj
            return get_bytes(str_obj).decode("utf-8", "replace")
        map_node_order = lambda path, obj: sorted(obj, key=_convert_to_unicode)  # noqa

    encoding = get_structured_format(format=None, client=client)._encoding

    def to_response_key_type(string):
        if isinstance(string, bytes):
            if encoding is None:
                return string
            else:
                return string.decode("ascii")
        elif isinstance(string, str):
            if encoding is None:
                return string.encode("ascii")
            else:
                return string
        else:
            assert False, "Unexpected input <{}>{!r}".format(type(string), string)

    def to_native_string(string):
        if isinstance(string, bytes):
            return string.decode("ascii")
        else:
            return string

    if not root and not get_config(client)["prefix"]:
        root = "/"
    root = str(YPath(root, client=client))
    attributes = get_value(attributes, [])

    request_attributes = deepcopy(flatten(attributes)) + ["type", "opaque"]

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
            return True
        elif error.is_resolve_error() and node.ignore_resolve_error:
            logger.warning("Path %s is missing" % node.path)
            return True
        else:
            return False

    def is_opaque(content):
        # We have bug that get to document don't return attributes.
        return \
            content.attributes.get(to_response_key_type("opaque"), False) and \
            to_native_string(content.attributes[to_response_key_type("type")]) != "document" or \
            to_native_string(content.attributes[to_response_key_type("type")]) in ("account_map", "tablet_cell", "portal_entrance")

    def safe_batch_get(nodes, batch_client):
        get_result = []
        for node in nodes:
            get_result.append(
                batch_client.get(
                    node.path,
                    attributes=request_attributes,
                    read_from=read_from,
                    cache_sticky_group_size=cache_sticky_group_size))
        batch_client.commit_batch()

        for content, node in zip(get_result, nodes):
            try:
                if content.get_error():
                    raise YtResponseError(content.get_error())
                node.content = content.get_result()
            except YtResponseError as rsp:
                if not process_response_error(rsp, node):
                    raise
            yield node

    def safe_get(nodes, client):
        for node in nodes:
            try:
                node.content = get(
                    node.path,
                    attributes=request_attributes,
                    client=client,
                    read_from=read_from,
                    cache_sticky_group_size=cache_sticky_group_size)
            except YtResponseError as rsp:
                if not process_response_error(rsp, node):
                    raise
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

        try:
            object_type = to_native_string(node.content.attributes[to_response_key_type("type")])
        except KeyError:
            logger.warning("No attribute 'type' at %s", node.path)
            object_type = "document"

        if node.followed_by_link and object_type == "link":
            return

        depth_limit_reached = (depth_bound is not None and node.depth == depth_bound)
        if is_opaque(node.content) and not node.ignore_opaque and object_type != "link" and not depth_limit_reached:
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
            yson_path_attributes = dict(filter(lambda item: item[0] in attributes,
                                               node.content.attributes.items()))
            yson_path = yson.to_yson_type(node.path, attributes=yson_path_attributes)
            yield yson_path

        if isinstance(node.content, dict):
            if map_node_order is not None:
                items_iter = ((key, node.content[key]) for key in map_node_order(node.path, node.content))
            else:
                items_iter = node.content.items()
            for key, value in items_iter:
                if isinstance(key, yson.yson_types.YsonStringProxy):
                    actual_key = to_response_key_type(escape_ypath_literal(yson.get_bytes(key)))
                else:
                    actual_key = escape_ypath_literal(key, encoding=encoding)
                path = to_response_key_type("/").join([node.path, actual_key])
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
    nodes_to_request.append(
        CompositeNode(
            to_response_key_type(root),
            0,
            ignore_opaque=True,
            ignore_resolve_error=ignore_root_path_resolve_error
        ))

    while nodes_to_request:
        if enable_batch_mode:
            nodes_to_process = safe_batch_get(nodes_to_request, batch_client)
        else:
            nodes_to_process = safe_get(nodes_to_request, client)
        nodes_to_request = []

        for node in nodes_to_process:
            for yson_path in process_node(node, nodes_to_request):
                yield yson_path


def remove_with_empty_dirs(path: Union[str, YPath], force: bool = True, client=None):
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
            if str(path) == "//" or not exists(path, client=client) or \
                    list(path, client=client) or get(path + "/@acl", client=client):
                break
        except YtResponseError as err:
            if err.is_resolve_error():
                break
            else:
                raise


def create_revision_parameter(path: Union[str, YPath], transaction_id=None, revision=None, client=None):
    """Creates revision parameter of the path.

    :param str path: path.
    :rtype: dict
    """
    if revision is None:
        revision = get_attribute(path, "revision", client=client)
    # TODO(shakurov): remove transaction_id when proxies are up to
    # date and no longer deem this parameter required.
    if transaction_id is None:
        transaction_id = get_command_param("transaction_id", client)
    return {"path": path, "transaction_id": transaction_id, "revision": revision}


def get_table_schema(table_path: Union[str, YPath], client=None):
    """Gets schema of table.

    :param table_path: path to table.
    :type table_path: str or :class:`TablePath <yt.wrapper.ypath.TablePath>`
    """
    attributes_tree = get(table_path, attributes=["type", "schema"], client=client)
    attributes = attributes_tree.attributes

    node_type = attributes["type"]
    if node_type != "table":
        raise YtError("Can't get schema for node '{path}' with type '{node_type}' ('table' is expected)".format(
            path=table_path,
            node_type=node_type,
        ))

    return TableSchema.from_yson_type(attributes["schema"])
