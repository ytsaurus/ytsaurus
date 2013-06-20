from driver import make_request
from transaction_commands import _make_formatted_transactional_request

def check_permission(user, permission, path, format=None):
    return _make_formatted_transactional_request(
        "check_permission",
        {
            "user": user,
            "permission": permission,
            "path": path
        },
        format=format)

def add_member(member, group):
    return make_request(
        "add_member",
        {
            "member": member,
            "group": group
        })

def remove_member(member, group):
    return make_request(
        "remove_member",
        {
            "member": member,
            "group": group
        })
