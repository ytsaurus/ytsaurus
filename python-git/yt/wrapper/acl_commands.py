"""Permissions commands"""

from .common import set_param
from .driver import make_request
from .transaction_commands import _make_formatted_transactional_request

def check_permission(user, permission, path, format=None, read_from=None, client=None):
    """Check permission for Cypress node

    :param user: (string) user login
    :param permission: (string) ["read", "write", "administer", "create", "use"]
    :return: permission in specified format (YSON by default)
    .. seealso:: `permissions on wiki <https://wiki.yandex-team.ru/yt/userdoc/accesscontrol#polzovateligruppyisubekty>`_
    """
    params ={
        "user": user,
        "permission": permission,
        "path": path
    }
    set_param(params, "read_from", read_from)
    return _make_formatted_transactional_request(
        "check_permission",
        params,
        format=format,
        client=client)

def add_member(member, group, client=None):
    """Add member to Cypress node group

    :param member: (string)
    :param group: (string)
    .. seealso:: `permissions on wiki <https://wiki.yandex-team.ru/yt/userdoc/accesscontrol#polzovateligruppyisubekty>`_
    """
    return make_request(
        "add_member",
        {
            "member": member,
            "group": group
        },
        client=client)

def remove_member(member, group, client=None):
    """Remove member from Cypress node group

    :param member: (string)
    :param group: (string)
    .. seealso:: `permissions on wiki <https://wiki.yandex-team.ru/yt/userdoc/accesscontrol#polzovateligruppyisubekty>`_
    """
    return make_request(
        "remove_member",
        {
            "member": member,
            "group": group
        },
        client=client)
