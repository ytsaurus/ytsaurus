import yt.logger as logger
import yt.wrapper as yt

from copy import deepcopy
import sys
from six import iteritems

class Namespace(object):
    def __init__(self, **kwargs):
        for key, value in iteritems(kwargs):
            setattr(self, key, value)

def add_responsibles(first, second):
    res = deepcopy(first)

    for item in second:
        record = {"user": item}
        if item.startswith("idm-group:"):
            try:
                grp_id = int(item.split(":")[1])
                record = {"group": grp_id}
            except:
                logger.error("Invalid format of group in subject [%s]" % item)
                sys.exit(1)

        if record not in res:
            res.append(record)
    return res

def acl_request_group(acl_client, name, acls, responsibles, args):
    roles_current = {
        "responsible": responsibles["responsible"],
        "members": acls["roles"]["members"]
    }

    roles_new = deepcopy(roles_current)

    changes_dict = {
        "responsible": {
            "active": responsibles["responsible"],
            "requested": args.responsibles,
        },
        "members": {
            "active": acls["roles"]["members"],
            "requested": args.members
        }
    }

    for role_type in changes_dict:
        if changes_dict[role_type]["requested"]:
            diff_list = changes_dict[role_type]["requested"]
            new_list = add_responsibles(changes_dict[role_type]["active"], diff_list)

            roles_new[role_type] = new_list

    if args.dry_run:
        print("Old struct:{}\nNew struct:{}".format(roles_current, roles_new))

    if not args.dry_run:
        acl_client.update_group(name=name, version=responsibles["version"], group=roles_new)

def acl_request_modify_responsible(acl_client, type_, name, responsibles, args):
    responsibles_new = deepcopy(responsibles["responsible"])
    resps = responsibles["responsible"]

    resps_dict = {
        "responsible": {
            "active": resps.get("responsible", []),
            "requested": args.responsibles
        },
        "read_approvers": {
            "active": resps.get("read_approvers", []),
            "requested": args.read_approvers
        },
        "auditors": {
            "active": resps.get("auditors", []),
            "requested": args.auditors
        }
    }

    for resp_class in resps_dict:
        if resps_dict[resp_class]["requested"]:
            diff_resps = resps_dict[resp_class]["requested"]
            new_resp_list = add_responsibles(resps_dict[resp_class]["active"], diff_resps)
            responsibles_new[resp_class] = new_resp_list

    if isinstance(args.inherit_resps, bool):
        responsibles_new["disable_inheritance"] = not args.inherit_acl

    if isinstance(args.boss_approval, bool):
        responsibles_new["require_boss_approval"] = args.boss_approval

    if isinstance(args.inherit_acl, bool):
        inherit_acl = args.inherit_acl
    else:
        inherit_acl = None

    if args.dry_run:
        del responsibles["version"]
        new = dict(responsible=responsibles_new, inherit_acl=inherit_acl)
        print("Old struct:{}\nNew struct:{}".format(responsibles, new))
    else:
        object_id = {type_: name}
        version = responsibles["version"]
        acl_client.set_responsible(
            version=version,
            responsible=responsibles_new,
            inherit_acl=inherit_acl,
            **object_id
        )

def acl_request_modify_role(acl_client, type_, name, acls, args):
    roles = []
    permissions = []

    if args.permissions == "R":
        permissions = ["read"]
    elif args.permissions == "RW":
        permissions = ["read", "write", "remove"]
    elif args.permissions == "M":
        permissions = ["mount"]
    elif args.permissions == "U":
        permissions = ["use"]

    for item in args.subjects:
        if item.startswith("idm-group:"):
            try:
                grp_id = int(item.split(":")[1])
            except IndexError:
                logger.error("Invalid format of group in subject [%s]" % item)
                sys.exit(1)
            else:
                roles.append({
                    "permissions": permissions,
                    "subject": {
                        "group": grp_id
                    }
                })
        else:
            roles.append({
                "permissions": permissions,
                "subject": {
                    "user": item
                }
            })

    if not args.dry_run:
        object_id = {type_: name}
        acl_client.add_role(object_id, roles=roles, **object_id)

def pretty_print_acls(acl_data, responsibles_data, object_type, show_inherited=False):
    if object_type in ["path", "account"]:
        acl = []
        for ace in acl_data.get("roles", []):
            if ace.get("state") != "granted":
                logger.warn("Found role not in granted state [%s]" % ace)
                continue
            if ace.get("inherited") == True and not show_inherited:
                continue
            permissions = ace.get("permissions")
            if "user" in ace.get("subject").keys():
                subjects = ace.get("subject").values()[0]
            elif "group" in ace.get("subject").keys():
                subjects = "%s (idm-group:%s)" % (
                        ace.get("subject").get("url"),
                        ace.get("subject").get("group")
                )

            acl.append({"permissions": permissions, "subjects": subjects})

        resps = get_role_members(responsibles_data["responsible"].get("responsible", []))
        read_appr = get_role_members(responsibles_data["responsible"].get("read_approvers", []))
        auditors = get_role_members(responsibles_data["responsible"].get("auditors", []))

        disable_inheritance_resps = responsibles_data["responsible"].get("disable_inheritance", False)
        boss_approval = responsibles_data["responsible"].get("require_boss_approval", False)
        inherit_acl = responsibles_data.get("inherit_acl", False)

        print_aligned("Responsibles:", " ".join(resps))
        print_aligned("Read approvers:", " ".join(read_appr))
        print_aligned("Auditors:", " ".join(auditors))
        print_aligned("Inherit responsibles:", (not disable_inheritance_resps))
        print_aligned("Boss approval required:", boss_approval)
        print_aligned("Inherit ACL:", inherit_acl)

        print("ACL roles:")
        for ace in acl:
            print("  {:<20} - {}".format(ace["subjects"], "/".join(ace["permissions"])))

    elif object_type == "group":
        resps = get_role_members(acl_data["resps"]["responsibles"]["responsible"])

        print_aligned("Responsibles:", " ".join(resps))
        print("Members:")
        for member in acl_data["roles"]["members"]:
            m = ""
            if "user" in member.keys():
                m = member["user"]
            elif "group" in member.keys():
                m = "{} (idm-group:{})".format(
                    member.get("url"),
                    member.get("group")
                )
            print("  {}".format(m))
    else:
        raise NotImplementedError("showing acls of '{}' objects is not implemented".format(object_type))

def get_role_members(node):
    members = []
    for n in node:
        members.append(n.values()[0])
    return members

def print_aligned(left, right):
    print("{:<30}{}".format(left, right))

def acl_show(type_, name):
    acl_client = yt.make_acl_client()
    object_id = {type_: name}
    acls = acl_client.get_acl(**object_id)
    responsibles = acl_client.get_responsible(**object_id)
    pretty_print_acls(acls, responsibles, type_)

def acl_request(type_, name, **kwargs):
    args = Namespace(**kwargs)
    acl_client = yt.make_acl_client()
    object_id = {type_: name}
    acls = acl_client.get_acl(**object_id)
    responsibles = acl_client.get_responsible(**object_id)
    if type_ == "group":
        acl_request_group(acl_client, name, acls, responsibles, args)
    else:
        modify_arguments = ["responsibles", "read_approvers", "auditors", "inherit_acl",
                            "inherit_resps", "boss_approval", "revoke_all_roles"]
        if any(kwargs[argument] is not None for argument in modify_arguments):
            acl_request_modify_responsible(acl_client, type_, name, responsibles, args)

        if kwargs["permissions"]:
            acl_request_modify_role(type_, name, acls, args)
