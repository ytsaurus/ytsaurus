import yt.logger as logger
import yt.wrapper as yt

from yt.packages.six import iteritems

from argparse import ArgumentTypeError
from contextlib import contextmanager
import sys

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
    def __init__(self, subject):
        if "user" in subject:
            self.kind = "user"
            self.user = subject["user"]
        else:
            self.kind = "group"
            self.group = subject["group"]
            self.human_readable_name = subject.get("group_name", "")
        self.url = subject.get("url", "")

    def _signature(self):
        if self.kind == "user":
            return ("user", self.user)
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
                group_id = int(string[:len("idm-group")])
            except ValueError:
                logger.error("Invalid IDM group format: %s", string)
                sys.exit(1)
            return cls(dict(group=group_id))
        else:
            return cls(dict(user=string))

    def to_pretty_string(self):
        if self.kind == "user":
            return self.user
        else:
            return "{} (idm-group:{})".format(self.url, self.group)

    def to_json_type(self):
        if self.kind == "user":
            return dict(user=self.user, url=self.url)
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

    def to_pretty_string(self):
        permissions = "/".join(self.permissions)
        return "{:<20} - {}".format(self.subject.to_pretty_string(), permissions)

    def to_json_type(self):
        copy_attrs = ("state", "permissions", "columns", "idm_link", "inherited", "member",
                      "is_depriving", "is_requested", "is_approved", "is_unrecognized")
        result = {attr: getattr(self, attr) for attr in copy_attrs}
        result["role_key"] = self.key
        result["subject"] = self.subject.to_json_type()
        return result

class ObjectAclSnapshot(object):
    def __init__(self, object_id, idm_client):
        self.idm_client = idm_client
        self.object_id = object_id

        responsibles_reply = idm_client.get_responsible(**object_id)
        self.inherit_acl = responsibles_reply.get("inherit_acl", False)
        self.version = responsibles_reply["version"]

        responsibles = responsibles_reply["responsible"]
        self.responsibles = list(map(Subject, responsibles.get("responsible", [])))
        self.read_approvers = list(map(Subject, responsibles.get("read_approvers", [])))
        self.auditors = list(map(Subject, responsibles.get("auditors", [])))
        self.disable_responsible_inheritance = responsibles.get("disable_inheritance", False)
        self.require_boss_approval = responsibles.get("require_boss_approval", False)

        roles = idm_client.get_acl(**object_id)
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

    def remove_roles(self, subjects, comment):
        subjects = set(subjects)
        for role in self.roles:
            if role.subject in subjects:
                role.comment = comment
                self._roles_to_remove.append(role)
        self.roles = [role for role in self.roles if role.subject not in subjects]

    def remove_all_roles(self, comment):
        for role in self.roles:
            role.comment = comment
            self._roles_to_remove.append(role)
        self.roles = []

    def commit(self):
        subjects_to_json = lambda subjects: [subj.to_json_type() for subj in subjects]
        self.idm_client.set_responsible(
            version=self.version,
            responsible=dict(
                responsible=subjects_to_json(self.responsibles),
                read_approvers=subjects_to_json(self.read_approvers),
                auditors=subjects_to_json(self.auditors),
                require_boss_approval=self.require_boss_approval,
                disable_inheritance=self.disable_responsible_inheritance,
            ),
            inherit_acl=self.inherit_acl,
            **self.object_id
        )
        if self._new_roles:
            comment_to_roles = {}
            for role in self._new_roles:
                if role.comment not in comment_to_roles:
                    comment_to_roles[role.comment] = []
                comment_to_roles[role.comment].append(role)
            for comment, roles in iteritems(comment_to_roles):
                roles = [role.to_json() for role in self._new_roles]
                self.idm_client.add_role(roles=roles, comment=comment, **self.object_id)
        if self._roles_to_remove:
            for role in self._roles_to_remove:
                assert role.key
                self.idm_client.remove_role(role_key=role.key)

def print_aligned(left, right, indent=0):
    print("{}{:<30}{}".format(" " * indent, left, right))

def print_indented(string, indent=2):
    print(" " * indent + string)

def pretty_print_idm_info(object_idm_snapshot, indent=0):
    prettify_list = lambda list_: " ".join(entry.to_pretty_string() for entry in list_)
    print_aligned("Responsibles:", prettify_list(object_idm_snapshot.responsibles), indent)
    print_aligned("Read approvers:", prettify_list(object_idm_snapshot.read_approvers), indent)
    print_aligned("Auditors:", prettify_list(object_idm_snapshot.auditors), indent)
    print_aligned("Inherit responsibles:", (not object_idm_snapshot.disable_responsible_inheritance), indent)
    print_aligned("Boss approval required:", object_idm_snapshot.require_boss_approval, indent)
    print_aligned("Inherit ACL:", object_idm_snapshot.inherit_acl, indent)

    print_indented("ACL roles:", indent)
    for role in object_idm_snapshot.roles:
        print_indented(role.to_pretty_string(), indent + 2)

def subjects_from_string(subjects):
    return [Subject.from_string(subj) for subj in subjects]

def apply_flags(object_idm_snapshot, args):
    if isinstance(getattr(args, "inherit_acl", None), bool):
        object_idm_snapshot.inherit_acl = args.inherit_acl
    if isinstance(getattr(args, "inherit_responsibles", None), bool):
        object_idm_snapshot.disable_responsible_inheritance = not args.inherit_responsibles
    if isinstance(getattr(args, "boss_approval", None), bool):
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
    object_types = ["path", "account", "pool", "bundle", "group"]
    given_types = [type_ for type_ in object_types if kwargs[type_] is not None]
    assert len(given_types) == 1

    object_type = given_types[0]

    # NB: this object id is not a string because it corresponds to ObjectID proto message in
    # yt/idm-integration/aclapi/aclapi.proto.
    object_id = {object_type: kwargs[object_type]}

    del kwargs[object_type]
    return object_id, Namespace(**kwargs)

def with_idm_info(func):
    def wrapper(**kwargs):
        object_id, args = extract_object_id(**kwargs)
        idm_client = yt.make_idm_client(args.address)
        snapshot = ObjectAclSnapshot(object_id, idm_client)
        return func(snapshot, args)
    return wrapper

@with_idm_info
def show(object_idm_snapshot, args):
    pretty_print_idm_info(object_idm_snapshot)

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
            object_idm_snapshot.remove_all_roles()
        else:
            object_idm_snapshot.remove_roles(
                subjects_from_string(args.subjects),
                args.comment,
            )
