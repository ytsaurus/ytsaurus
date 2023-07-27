from .config import get_config
from .http_helpers import get_token

from yt.common import YtError
import yt.logger as logger

import yt.packages.requests as requests

try:
    from yt.packages.six import iteritems, string_types
except ImportError:
    from six import iteritems, string_types


DEFAULT_BASE_ACL_SERVICE_URL = "https://idm.yt.yandex-team.ru"


def decode_latin1(obj):
    if isinstance(obj, dict):
        return dict([(key, decode_latin1(value)) for key, value in iteritems(obj)])
    if isinstance(obj, list):
        return [decode_latin1(value) for value in obj]
    if isinstance(obj, string_types):
        return obj.encode("latin1").decode("utf-8")
    return obj


def make_idm_client(address=None, client=None):
    """Creates IdmClient from YtClient.
    """
    cluster = get_config(client)["proxy"]["url"].split(".")[0]
    token = get_token(client=client)
    return YtIdmClient(cluster, token, address)


def _flatten_dict(dict_):
    result = {}
    for key, value in iteritems(dict_):
        if isinstance(value, dict):
            for subkey, subvalue in iteritems(_flatten_dict(value)):
                result["{}.{}".format(key, subkey)] = subvalue
        elif isinstance(value, bool):
            result[key] = "true" if value else "false"
        else:
            result[key] = value
    return result


def _get_object_id(path=None, account=None, pool=None, group=None, tablet_cell_bundle=None,
                   pool_tree=None, network_project=None, rpc_proxy_role=None, http_proxy_role=None,
                   ql_pool=None, access_control_object=None, access_control_object_namespace=None):
    no_additional_data = dict(path=path, account=account, group=group,
                              tablet_cell_bundle=tablet_cell_bundle,
                              network_project=network_project,
                              rpc_proxy_role=rpc_proxy_role,
                              http_proxy_role=http_proxy_role,
                              ql_pool=ql_pool)
    exclusive = [
        dict(pool=pool, **no_additional_data),
        dict(pool_tree=pool_tree, **no_additional_data),
        dict(access_control_object=access_control_object, **no_additional_data),
        dict(access_control_object_namespace=access_control_object_namespace, **no_additional_data),
        dict(access_control_object=access_control_object, pool=pool),
        dict(access_control_object_namespace=access_control_object_namespace, pool=pool),
        dict(access_control_object=access_control_object, pool_tree=pool_tree),
        dict(access_control_object_namespace=access_control_object_namespace, pool_tree=pool_tree),
    ]
    for exclusive_group in exclusive:
        keys = [key for key, value in iteritems(exclusive_group) if value is not None]
        if len(keys) > 1:
            raise TypeError("mutually exclusive arguments: '{}'"
                            .format("', '".join(keys)))

    for key, value in iteritems(no_additional_data):
        if value is not None:
            return dict(kind=key, name=value)

    if pool is not None or pool_tree is not None:
        if pool is None or pool_tree is None:
            raise TypeError("expected both 'pool' and 'pool_tree'")
        return dict(kind="pool", name=pool, pool_tree=pool_tree)

    if access_control_object is not None or access_control_object_namespace is not None:
        if access_control_object is None or access_control_object_namespace is None:
            raise TypeError("expected both 'access_control_object' and 'access_control_object_namespace'")
        return dict(kind="access_control_object", name=access_control_object, namespace=access_control_object_namespace)

    args = sorted(set(key for group in exclusive for key in group))
    raise TypeError("expected one of: '{}'".format("', '".join(args)))


def _with_object_id(func):
    def wrapper(client, path=None, account=None, pool=None, group=None, tablet_cell_bundle=None,
                pool_tree=None, network_project=None,
                rpc_proxy_role=None, http_proxy_role=None, ql_pool=None,
                access_control_object=None, access_control_object_namespace=None,
                *args, **kwargs):
        object_id = _get_object_id(path, account, pool, group, tablet_cell_bundle, pool_tree,
                                   network_project, rpc_proxy_role, http_proxy_role, ql_pool,
                                   access_control_object, access_control_object_namespace)
        return func(client, *args, object_id=object_id, **kwargs)
    wrapper.__doc__ = func.__doc__
    return wrapper


def _with_optional_object_id(func):
    def wrapper(client, path=None, account=None, pool=None, group=None, tablet_cell_bundle=None,
                pool_tree=None, network_project=None, rpc_proxy_role=None, http_proxy_role=None,
                ql_pool=None, access_control_object=None, access_control_object_namespace=None, *args, **kwargs):
        if any((path, account, pool, group, tablet_cell_bundle, pool_tree, network_project,
               rpc_proxy_role, http_proxy_role, ql_pool, access_control_object,
               access_control_object_namespace)):
            object_id = _get_object_id(path, account, pool, group, tablet_cell_bundle, pool_tree,
                                       network_project, rpc_proxy_role, http_proxy_role, ql_pool,
                                       access_control_object, access_control_object_namespace)
        else:
            object_id = None
        return func(client, *args, object_id=object_id, **kwargs)
    wrapper.__doc__ = func.__doc__
    return wrapper


class YtIdmClient(object):
    """Implements YT IDM client."""
    def __init__(self, cluster, token, base_url=None, certificate_path=None):
        self._cluster = cluster
        self._token = token
        if base_url is None:
            self._base_url = DEFAULT_BASE_ACL_SERVICE_URL
        else:
            self._base_url = base_url

    def _make_request(self, method, name, params=None, body=None, extra_headers=None, v2=False):
        # NB: `extra_headers` is only used to supply additional headers in integration tests.
        url = "{}/{}/api/{}".format(self._base_url, self._cluster, name)
        if v2:
            url = "{}/{}/api/v2/{}".format(self._base_url, self._cluster, name)
        headers = {
            "Authorization": "OAuth {}".format(self._token),
            "Content-Type": "application/json"
        }
        if extra_headers:
            headers.update(extra_headers)

        params = params or {}
        body = body or {}

        logger.debug("Sending %s %s (params: %s, body: %s)", method.upper(), url, params, body)
        response = requests.request(method, url, headers=headers, params=_flatten_dict(params), json=body)
        logger.debug("Got response %s (body: %s)", response.status_code, response.text)

        if response.status_code == 400:
            raise YtError.from_dict(decode_latin1(response.json()))
        else:
            response.raise_for_status()

        return response.json()

    @_with_object_id
    def get_acl(self, object_id, include_managed_ace=False):
        """Gets ACL info."""
        params = dict(id=object_id, include_managed_ace=include_managed_ace)
        return self._make_request("get", "acl", params)

    @_with_object_id
    def set_inherit_acl(self, object_id, inherit_acl):
        """Sets or removes inherit_acl flag for object."""
        return self._make_request("post", "acl", dict(id=object_id, inherit_acl=inherit_acl))

    @_with_optional_object_id
    def remove_role(self, object_id=None, role=None, role_key=None, comment=""):
        """Removes role."""
        xor = lambda a, b: bool(a) ^ bool(b)
        if not xor((object_id and role and not role_key), (not object_id and not role and role_key)):
            raise TypeError("expected either 'role_key' or both 'object_id' and 'role'")
        params = dict(comment=comment)
        if role_key:
            params["role_key"] = role_key
        else:
            params["id"] = object_id
            params["role"] = role
        return self._make_request("delete", "role", params)

    @_with_object_id
    def add_role(self, object_id, comment="", **kwargs):
        """Adds role."""
        if "role" in kwargs and "roles" in kwargs:
            raise TypeError("expected either 'role' or 'roles', not both")
        if "role" in kwargs:
            roles = [kwargs["role"]]
        else:
            roles = kwargs["roles"]
        params = dict(id=object_id, comment=comment)
        body = dict(roles=roles)
        return self._make_request("put", "role", params, body)

    @_with_object_id
    def get_responsible(self, object_id):
        """Gets subject responsible for object."""
        return self._make_request("get", "responsible", dict(id=object_id), v2=True)

    @_with_object_id
    def set_responsible(self, object_id, version, responsible, inherit_acl=None, comment=""):
        """Sets subject responsible for object."""
        params = dict(id=object_id, version=version, comment=comment)
        if inherit_acl is not None:
            params["inherit_acl"] = inherit_acl
        return self._make_request("post", "responsible", params, responsible)

    def get_group(self, group_name):
        """Gets legacy YT group info by group name."""
        return self._make_request("get", "group", dict(group_name=group_name), v2=True)

    def update_group(self, name, version, group, comment=""):
        """Updates legacy YT group."""
        params = dict(group_name=name, version=version, comment=comment)
        return self._make_request("put", "group", params, group)

    @_with_object_id
    def remove_all_roles(self, object_id, recursive=False):
        """Removes all roles from object and from it's descendants if recursive is set to True"""
        return self._make_request("delete", "roles", dict(id=object_id, recursive=recursive))

    def add_column_group(self, name, columns, path):
        """Adds column group."""
        column_group = dict(columns=columns, name=name)
        params = dict(path=path)
        return self._make_request("put", "column_group", params, column_group)

    def update_column_group(self, guid, name, columns, enabled):
        """Updates column group."""
        column_group = dict(id=guid, columns=columns, name=name, enabled=enabled)
        params = dict()
        return self._make_request("post", "column_group", params, column_group)

    def remove_column_group(self, guid):
        """Removes column group."""
        params = dict(id=guid)
        return self._make_request("delete", "column_group", params)
