import yt.logger as logger
import yt.wrapper as yt

try:
    from yt.packages.six import iteritems
except ImportError:
    from six import iteritems

from argparse import ArgumentTypeError
from contextlib import contextmanager
import sys

from copy import deepcopy


class Namespace(object):
    def __init__(self, **kwargs):
        for key, value in iteritems(kwargs):
            setattr(self, key, value)


def decode_permissions(string):
    permissions = set()
    for char in string:
        lower = char.lower()
        try:
            permissions.add(dict(
                w="write",
                r="read",
                m="mount",
                u="use",
            )[lower])
        except KeyError:
            raise ArgumentTypeError("Unknown permission '{}' in string '{}'".format(lower, string))
    if "write" in permissions:
        permissions.add("remove")
    order = ("read", "write", "remove", "mount", "use")
    return sorted(permissions, key=lambda s: order.index(s))


class Subject(object):
    def __init__(self, subject, inherited=False):
        if "user" in subject:
            self.kind = "user"
            self.user = subject["user"]
        elif "tvm_id" in subject:
            self.kind = "tvm_app"
            self.tvm_id = int(subject["tvm_id"])
        else:
            self.kind = "group"
            self.group = int(subject["group"])
            self.human_readable_name = subject.get("group_name", "")
        self.inherited = inherited
        self.url = subject.get("url", "")

    def _signature(self):
        if self.kind == "user":
            return ("user", self.user)
        elif self.kind == "tvm_app":
            return ("tvm_app", self.tvm_id)
        else:
            return ("group", self.group)

    def __hash__(self):
        return hash(self._signature())

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self._signature() == other._signature()

    @classmethod
    def from_string(cls, string):
        if string.startswith("idm-group:"):
            try:
                group_id = int(string[len("idm-group:"):])
            except ValueError:
                logger.error("Invalid IDM group format: %s", string)
                sys.exit(1)
            return cls(dict(group=group_id))
        if string.startswith("tvm-app:"):
            try:
                tvm_id = int(string[len("tmv-app:"):])
            except ValueError:
                logger.error("Invalid TVM app format: %s", string)
                sys.exit(1)
            return cls(dict(tvm_id=tvm_id))
        else:
            return cls(dict(user=string))

    def to_pretty_string(self):
        postfix = "*" if self.inherited else ""
        if self.kind == "user":
            return self.user + postfix
        elif self.kind == "tvm_app":
            return "{} (tvm-app:{}){}".format(self.url, self.tvm_id, postfix)
        else:
            return "{} (idm-group:{}){}".format(self.url, self.group, postfix)

    def to_json_type(self):
        if self.kind == "user":
            return dict(user=self.user, url=self.url)
        elif self.kind == "tvm_app":
            return dict(tvm_id=self.tvm_id, url=self.url)
        else:
            return dict(group=self.group, group_name=self.human_readable_name, url=self.url)


class Role(object):
    def __init__(self, role, comment=None):
        self.key = role["role_key"]
        self.state = role["state"]
        self.permissions = role.get("permissions", [])
        self.columns = role.get("columns", [])
        self.idm_link = role["idm_link"]

        if isinstance(role["subject"], Subject):
            self.subject = role["subject"]
        else:
            self.subject = Subject(role["subject"])

        bool_attributes = ("inherited", "member", "is_depriving", "is_requested", "is_approved",
                           "is_unrecognized")
        for attr in bool_attributes:
            setattr(self, attr, role.get(attr, False))

        self.comment = comment

    def _signature(self):
        return (self.subject, tuple(sorted(self.permissions)), tuple(sorted(self.columns)))

    def __hash__(self):
        return hash(self._signature())

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self._signature() == other._signature()

    def to_pretty_string(self):
        permissions = "/".join(self.permissions)
        postfix = "*" if self.inherited else ""
        return "{:<20} - {}".format(self.subject.to_pretty_string() + postfix, permissions)

    def to_json_type(self):
        copy_attrs = ("state", "permissions", "columns", "idm_link", "inherited", "member",
                      "is_depriving", "is_requested", "is_approved", "is_unrecognized")
        result = {attr: getattr(self, attr) for attr in copy_attrs}
        result["role_key"] = self.key
        result["subject"] = self.subject.to_json_type()
        return result


class ObjectIdmSnapshot(object):
    def __init__(self, object_id, idm_client):
        self.idm_client = idm_client
        self.object_id = object_id

        responsibles_reply = idm_client.get_responsible(**object_id)
        self.version = responsibles_reply["version"]

        extract_roles = lambda roles: [Subject(role["subject"], role.get("inherited", False)) for role in roles]

        responsibles = responsibles_reply["responsible"]
        self.responsibles = extract_roles(responsibles.get("responsible", []))
        self.read_approvers = extract_roles(responsibles.get("read_approvers", []))
        self.auditors = extract_roles(responsibles.get("auditors", []))
        self.require_boss_approval = bool(responsibles.get("require_boss_approval", False))

        roles = idm_client.get_acl(**object_id)

        if "path" in object_id or "pool" in object_id or "account" in object_id:
            self.disable_responsible_inheritance = bool(responsibles.get("disable_inheritance", False))
            self.inherit_acl = roles.get("acl", {}).get("inherit_acl", False)
        else:
            self.disable_responsible_inheritance = None
            self.inherit_acl = None

        self.roles = list(map(Role, roles.get("roles", [])))
        self._new_roles = []
        self._roles_to_remove = []

    def add_responsibles(self, subjects):
        self.responsibles = self._add_subjects(self.responsibles, subjects)

    def add_read_approvers(self, subjects):
        self.read_approvers = self._add_subjects(self.read_approvers, subjects)

    def add_auditors(self, subjects):
        self.auditors = self._add_subjects(self.auditors, subjects)

    @staticmethod
    def _add_subjects(old_subjects, new_subjects):
        old_subjects = set(old_subjects)
        result = list(old_subjects)
        for subject in new_subjects:
            if subject not in old_subjects:
                result.append(subject)
                old_subjects.add(subject)
        return result

    def remove_responsibles(self, subjects):
        self.responsibles = self._remove_subjects(self.responsibles, subjects)

    def remove_read_approvers(self, subjects):
        self.read_approvers = self._remove_subjects(self.read_approvers, subjects)

    def remove_auditors(self, subjects):
        self.auditors = self._remove_subjects(self.auditors, subjects)

    @staticmethod
    def _remove_subjects(old_subjects, subjects_to_remove):
        subjects_to_remove = set(subjects_to_remove)
        return [subj for subj in old_subjects if subj not in subjects_to_remove]

    def add_roles(self, subjects, permissions, comment):
        for subject in subjects:
            role = Role(dict(
                role_key=None,
                state=None,
                permissions=permissions,
                subject=subject,
                idm_link=None,
            ), comment=comment)
            self._new_roles.append(role)
            self.roles.append(role)

    def remove_roles(self, subjects, permissions, comment=""):
        subjects = set(subjects)
        permissions = set(permissions)

        for role in self.roles:
            can_remove = not permissions or set(role.permissions) == permissions
            if role.subject in subjects and can_remove:
                role.comment = comment
                self._roles_to_remove.append(role)

        removed = set(self._roles_to_remove)
        self.roles = [role for role in self.roles if role not in removed]

    def remove_all_immediate_roles(self, comment=""):
        for role in self.roles:
            if not role.inherited:
                role.comment = comment
                self._roles_to_remove.append(role)

        removed = set(self._roles_to_remove)
        self.roles = [role for role in self.roles if role not in removed]

    def clear_immediate(self):
        clear = lambda subjects: [sub for sub in subjects if sub.inherited]
        self.responsibles = clear(self.responsibles)
        self.read_approvers = clear(self.read_approvers)
        self.auditors = clear(self.auditors)
        self.remove_all_immediate_roles()

    def copy_flags_from(self, other):
        for flag in ("inherit_acl", "disable_responsible_inheritance", "require_boss_approval"):
            setattr(self, flag, getattr(other, flag))

    def copy_permissions_from(self, other, immediate=False):
        for attr in ("responsibles", "read_approvers", "auditors"):
            for subject in getattr(other, attr):
                my_list = getattr(self, attr)
                if subject not in my_list and (not immediate or not subject.inherited):
                    subject = deepcopy(subject)
                    subject.inherited = False
                    my_list.append(subject)

        for role in other.roles:
            if role not in self.roles and (not immediate or not role.inherited):
                role = deepcopy(role)
                role.inherited = False
                self._new_roles.append(role)

    def effective_roles(self):
        role_set = set(r for r in self.roles)
        return self.roles + [r for r in self._new_roles if r not in role_set]

    def commit(self):
        subjects_to_json = lambda subjects: [subj.to_json_type() for subj in subjects]

        responsible_params = dict(
            version=self.version,
            responsible=dict(
                responsible=subjects_to_json([r for r in self.responsibles if not r.inherited]),
                read_approvers=subjects_to_json([r for r in self.read_approvers if not r.inherited]),
                auditors=subjects_to_json([r for r in self.auditors if not r.inherited]),
                require_boss_approval=self.require_boss_approval,
            ),
            **self.object_id
        )
        if self.disable_responsible_inheritance is not None:
            responsible_params["responsible"]["disable_inheritance"] = self.disable_responsible_inheritance
        if self.inherit_acl is not None:
            responsible_params["inherit_acl"] = self.inherit_acl

        self.idm_client.set_responsible(**responsible_params)

        if self._new_roles:
            comment_to_roles = {}
            for role in self._new_roles:
                if role.comment not in comment_to_roles:
                    comment_to_roles[role.comment] = []
                comment_to_roles[role.comment].append(role)
            for comment, roles in iteritems(comment_to_roles):
                roles = [role.to_json_type() for role in self._new_roles]
                self.idm_client.add_role(roles=roles, comment=comment, **self.object_id)
        if self._roles_to_remove:
            for role in self._roles_to_remove:
                assert role.key
                self.idm_client.remove_role(role_key=role.key)


def print_aligned(left, right, indent=0):
    print("{}{:<30}{}".format(" " * indent, left, right))


def print_indented(string, indent=2):
    print(" " * indent + string)


def pretty_print_idm_info(object_idm_snapshot, indent=0, immediate=False):

    def preprocess_subjects(subjects):
        if immediate:
            subjects = (sub for sub in subjects if not sub.inherited)
        return " ".join(sub.to_pretty_string() for sub in subjects)

    print_aligned("Responsibles:", preprocess_subjects(object_idm_snapshot.responsibles), indent)
    print_aligned("Read approvers:", preprocess_subjects(object_idm_snapshot.read_approvers), indent)
    print_aligned("Auditors:", preprocess_subjects(object_idm_snapshot.auditors), indent)
    print_aligned("Inherit responsibles:", (not object_idm_snapshot.disable_responsible_inheritance), indent)
    print_aligned("Boss approval required:", object_idm_snapshot.require_boss_approval, indent)
    if object_idm_snapshot.inherit_acl is not None:
        print_aligned("Inherit ACL:", object_idm_snapshot.inherit_acl, indent)

    print_indented("ACL roles:", indent)
    for role in object_idm_snapshot.effective_roles():
        if not immediate or not role.inherited:
            print_indented(role.to_pretty_string(), indent + 2)


def subjects_from_string(subjects):
    return [Subject.from_string(subj) for subj in subjects]


def apply_flags(object_idm_snapshot, args):
    if (
        object_idm_snapshot.inherit_acl is not None
        and getattr(args, "inherit_acl", None) is not None
    ):
        object_idm_snapshot.inherit_acl = args.inherit_acl
    if (
        object_idm_snapshot.disable_responsible_inheritance is not None
        and getattr(args, "inherit_responsibles", None) is not None
    ):
        object_idm_snapshot.disable_responsible_inheritance = not args.inherit_responsibles
    if getattr(args, "boss_approval", None) is not None:
        object_idm_snapshot.require_boss_approval = args.boss_approval


@contextmanager
def modify_idm_snapshot(snapshot, dry_run):
    if dry_run:
        print("BEFORE:")
        pretty_print_idm_info(snapshot, indent=2)
    yield
    if dry_run:
        print("AFTER:")
        pretty_print_idm_info(snapshot, indent=2)
    else:
        snapshot.commit()


def extract_object_id(**kwargs):
    object_types = ["path", "account", "pool", "tablet_cell_bundle", "group"]
    given_types = [type_ for type_ in object_types if kwargs[type_] is not None]
    assert len(given_types) == 1

    object_type = given_types[0]

    # NB: this object id is not a string because it corresponds to ObjectID proto message in
    # yt/idm-integration/aclapi/aclapi.proto.
    object_id = {object_type: kwargs[object_type]}
    del kwargs[object_type]

    if object_type == "pool":
        if not kwargs.get("pool_tree"):
            raise ArgumentTypeError("Specify --pool-tree")

        object_id["pool_tree"] = kwargs["pool_tree"]
        del kwargs["pool_tree"]

    return object_id, Namespace(**kwargs)


def with_idm_info(func):
    def wrapper(**kwargs):
        object_id, args = extract_object_id(**kwargs)
        idm_client = yt.make_idm_client(args.address)
        snapshot = ObjectIdmSnapshot(object_id, idm_client)
        return func(snapshot, args)
    return wrapper


@with_idm_info
def show(object_idm_snapshot, args):
    pretty_print_idm_info(object_idm_snapshot, immediate=args.immediate)


@with_idm_info
def request(object_idm_snapshot, args):
    with modify_idm_snapshot(object_idm_snapshot, args.dry_run):
        object_idm_snapshot.add_responsibles(subjects_from_string(args.responsibles))
        object_idm_snapshot.add_read_approvers(subjects_from_string(args.read_approvers))
        object_idm_snapshot.add_auditors(subjects_from_string(args.auditors))

        apply_flags(object_idm_snapshot, args)

        object_idm_snapshot.add_roles(
            subjects_from_string(args.subjects),
            args.permissions,
            args.comment,
        )


@with_idm_info
def revoke(object_idm_snapshot, args):
    with modify_idm_snapshot(object_idm_snapshot, args.dry_run):
        object_idm_snapshot.remove_responsibles(subjects_from_string(args.responsibles))
        object_idm_snapshot.remove_read_approvers(subjects_from_string(args.read_approvers))
        object_idm_snapshot.remove_auditors(subjects_from_string(args.auditors))

        apply_flags(object_idm_snapshot, args)

        if args.revoke_all_roles:
            object_idm_snapshot.remove_all_immediate_roles()
        else:
            object_idm_snapshot.remove_roles(
                subjects_from_string(args.subjects),
                args.permissions,
                args.comment,
            )


@with_idm_info
def copy(source_idm_snapshot, args):
    dest_object_id = source_idm_snapshot.object_id.copy()
    assert len(dest_object_id) == 1
    dest_object_id[tuple(dest_object_id)[0]] = args.destination

    dest_idm_snapshot = ObjectIdmSnapshot(dest_object_id, source_idm_snapshot.idm_client)

    with modify_idm_snapshot(dest_idm_snapshot, args.dry_run):
        if args.erase:
            dest_idm_snapshot.clear_immediate()
            dest_idm_snapshot.copy_flags_from(source_idm_snapshot)
        dest_idm_snapshot.copy_permissions_from(source_idm_snapshot, immediate=args.immediate)
