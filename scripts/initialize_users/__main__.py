from yp.client import (
    YpClient,
    find_token,
)

from yp.common import YpNoSuchObjectError

import argparse
import copy
import logging
import sys


logger = None


class Subject(object):
    def __init__(self, id, type):
        self.id = id
        self.type = type


class User(Subject):
    def __init__(self, id):
        super(User, self).__init__(id, "user")


class Group(Subject):
    def __init__(self, id):
        super(Group, self).__init__(id, "group")


def create(client, subject):
    global_subject_ids = ("everyone",)

    if subject.id in global_subject_ids:
        return

    try:
        client.get_object(subject.type, subject.id, ["/meta"])
    except YpNoSuchObjectError:
        client.create_object(
            object_type=subject.type, attributes=dict(meta=dict(id=subject.id)),
        )


def configure_logger():
    global logger

    logging.basicConfig(
        format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO, stream=sys.stdout,
    )
    logger = logging


def format_args(args, kwargs):
    tokens = []
    if len(args) > 0:
        tokens.extend(list(map(repr, args)))
    if len(kwargs) > 0:
        tokens.extend(list(map(lambda key: "{} = {}".format(key, kwargs[key]), kwargs)))
    return ", ".join(tokens)


class ClientWrapper(object):
    def __init__(self, client, dry_run):
        self._client = client
        self._dry_run = dry_run

    def create_object(self, *args, **kwargs):
        logger.info("Creating object: " + format_args(args, kwargs))
        if not self._dry_run:
            self._client.create_object(*args, **kwargs)

    def update_object(self, *args, **kwargs):
        logger.info("Updating object: " + format_args(args, kwargs))
        if not self._dry_run:
            self._client.update_object(*args, **kwargs)

    def select_objects(self, *args, **kwargs):
        return self._client.select_objects(*args, **kwargs)

    def get_object(self, *args, **kwargs):
        return self._client.get_object(*args, **kwargs)

    def check_object_permissions(self, *args, **kwargs):
        return self._client.check_object_permissions(*args, **kwargs)

    def start_transaction(self, *args, **kwargs):
        return self._client.start_transaction(*args, **kwargs)

    def commit_transaction(self, *args, **kwargs):
        return self._client.commit_transaction(*args, **kwargs)


def add_group_members(client, group, subjects):
    create(client, group)

    transaction = client.start_transaction(enable_structured_response=True)
    transaction_id = transaction["transaction_id"]
    start_timestamp = transaction["start_timestamp"]

    spec = client.get_object("group", group.id, ["/spec"], timestamp=start_timestamp)[0]
    members = spec.get("members", [])

    old_members = set(members)
    new_members = set(members + list(map(lambda subject: subject.id, subjects)))

    if old_members != new_members:
        new_members = list(new_members)

        client.update_object(
            "group",
            group.id,
            set_updates=[dict(path="/spec/members", value=new_members)],
            transaction_id=transaction_id,
        )

        client.commit_transaction(transaction_id)


def add_permission(client, object_type, object_id, subject, permission, attribute):
    create(client, subject)

    logger.debug(
        "Adding permission: object_type = {}, object_id = {}, subject = {}, permission = {}, attribute = {}".format(
            object_type, object_id, subject.id, permission, attribute,
        )
    )

    matching_ace = None

    acl = client.get_object(object_type, object_id, ["/meta/acl"])[0]
    for ace in acl:
        if ace["action"] != "allow":
            continue
        if subject.id not in ace["subjects"]:
            continue
        if permission not in ace["permissions"]:
            continue
        if "attributes" not in ace or len(ace["attributes"]) == 0:
            matching_ace = ace
            break
        for ace_attribute in ace["attributes"]:
            if attribute.startswith(ace_attribute):
                matching_ace = ace
                break
        if matching_ace is not None:
            break

    if matching_ace is not None:
        logger.debug("Found matching ace = {}".format(matching_ace))
        return

    attributes = []
    if attribute != "":
        attributes.append(attribute)

    set_updates = [
        dict(
            path="/meta/acl/end",
            value=dict(
                action="allow",
                subjects=[subject.id],
                permissions=[permission],
                attributes=attributes,
            ),
        )
    ]

    client.update_object(object_type, object_id, set_updates=set_updates)


def add_schema_permissions(client, type, subject, permissions):
    for permission in permissions:
        add_permission(client, "schema", type, subject, permission, attribute="")


def set_account(client, account_name, segment_name, cpu, memory, hdd, ssd, ipv4):
    resource_limits = client.get_object(
        "account", account_name, selectors=["/spec/resource_limits/per_segment"],
    )[0]
    resource_limits = resource_limits.get(segment_name, {})

    def get_capacity(path_tokens):
        node = resource_limits
        for token in path_tokens:
            node = node.get(token, {})
        return node.get("capacity", None)

    updates_set = list()
    if get_capacity(["cpu"]) != long(cpu):
        updates_set.append(
            dict(
                path="/spec/resource_limits/per_segment/{}/cpu/capacity".format(segment_name),
                value=long(cpu),
                recursive=True,
            )
        )

    if get_capacity(["memory"]) != long(memory):
        updates_set.append(
            dict(
                path="/spec/resource_limits/per_segment/{}/memory/capacity".format(segment_name),
                value=long(memory),
                recursive=True,
            )
        )

    if get_capacity(["internet_address"]) != long(ipv4):
        updates_set.append(
            dict(
                path="/spec/resource_limits/per_segment/{}/internet_address/capacity".format(
                    segment_name
                ),
                value=long(ipv4),
                recursive=True,
            )
        )

    if get_capacity(["disk_per_storage_class", "hdd"]) != long(hdd):
        updates_set.append(
            dict(
                path="/spec/resource_limits/per_segment/{}/disk_per_storage_class/hdd/capacity".format(
                    segment_name
                ),
                value=long(hdd),
                recursive=True,
            )
        )

    if get_capacity(["disk_per_storage_class", "ssd"]) != long(ssd):
        updates_set.append(
            dict(
                path="/spec/resource_limits/per_segment/{}/disk_per_storage_class/ssd/capacity".format(
                    segment_name
                ),
                value=long(ssd),
                recursive=True,
            )
        )

    if updates_set:
        client.update_object("account", account_name, updates_set)


def create_account(client, account_name, allow_use_for_all):
    try:
        client.get_object("account", account_name, ["/meta"])
    except YpNoSuchObjectError:
        attributes = {"meta": {"id": account_name, "inherit_acl": True}}
        if allow_use_for_all:
            attributes["meta"]["acl"] = [
                {"action": "allow", "permissions": ["use"], "subjects": ["everyone"]}
            ]

        client.create_object(object_type="account", attributes=attributes)


def resolve_all_segments(client):
    return [segment[0] for segment in client.select_objects("node_segment", selectors=["/meta/id"])]


def create_accounts(client, cluster, accounts):
    for account in accounts:
        create_account(client, account.name, account.allow_use_for_all)
        segments = None

        if len(account.quotas_per_segment) == 1 and account.quotas_per_segment.keys()[0] == "*":
            segments = resolve_all_segments(client)

            limits = account.quotas_per_segment["*"]

            for segment in segments:
                set_account(
                    client,
                    account.name,
                    segment,
                    cpu=limits["cpu"],
                    memory=limits["memory"],
                    hdd=limits["hdd"],
                    ssd=limits["ssd"],
                    ipv4=limits.get("ipv4", 0),
                )

        else:
            for segment in account.quotas_per_segment:
                limits = account.quotas_per_segment[segment]
                set_account(
                    client,
                    account.name,
                    segment,
                    cpu=limits["cpu"],
                    memory=limits["memory"],
                    hdd=limits["hdd"],
                    ssd=limits["ssd"],
                    ipv4=limits.get("ipv4", 0),
                )


def setup_tentacles_podset(client, cluster):
    tentacles_podset_name = "yp-rtc-sla-tentacles-production-{}".format(cluster)
    try:
        account_id = client.get_object("pod_set", tentacles_podset_name, ["/spec/account_id"])[0]
    except YpNoSuchObjectError:
        return

    updates_set = []
    if account_id != "tentacles":
        updates_set.append(dict(path="/spec/account_id", value="tentacles",))

    if updates_set:
        client.update_object("pod_set", tentacles_podset_name, updates_set)


def assign_podsets_to_accounts(client, cluster):
    setup_tentacles_podset(client, cluster)


def allow_account_usage(client, account, subject):
    can_use = client.check_object_permissions(
        [
            {
                "object_type": "account",
                "object_id": account,
                "subject_id": subject.id,
                "permission": "use",
            }
        ]
    )

    if len(can_use) == 0 or can_use[0]["action"] != "allow":
        updates_set = list()
        updates_set.append(
            {
                "path": "/meta/acl/end",
                "value": {"action": "allow", "permissions": ["use"], "subjects": [subject.id]},
            }
        )

        client.update_object("account", account, updates_set)


class Account(object):
    def __init__(self, name, quotas_per_segment, allow_use_for_all=False):
        for segment in quotas_per_segment:
            if segment == "*":
                assert len(quotas_per_segment) == 1

        self.name = name
        self.quotas_per_segment = quotas_per_segment
        self.allow_use_for_all = allow_use_for_all


KB = 1024
MB = 1024 * KB
GB = 1024 * MB
TB = 1024 * GB


ACCOUNTS = [
    Account(
        "replication-common-account",
        {"*": {"cpu": 1000000, "memory": 10 * TB, "hdd": 100 * TB, "ssd": 100 * TB, "ipv4": 10}},
    ),
    Account(
        "tmp",
        {
            "default": {
                "cpu": 1700000,
                "memory": 10000000000000,
                "hdd": 150000000000000,
                "ssd": 20000000000000,
            },
        },
        allow_use_for_all=True,
    ),
    Account(
        "odin",
        {
            "*": {
                "cpu": 100000000,
                "memory": 1000000000000,
                "hdd": 1000000000000000,
                "ssd": 1000000000000000,
            }
        },
    ),
    Account(
        "tentacles",
        {
            "default": {
                "cpu": 100000000,
                "memory": 1000000000000,
                "hdd": 1000000000000000,
                "ssd": 1000000000000000,
            }
        },
    ),
]


def accounts_override_xdc(cluster, accounts, client):
    # XDC accounts not presented in ABC, so order monitoring resources inplace
    assert cluster == "xdc"
    accounts.append(
        Account(
            "abc:service:1979",
            {
                "default": {
                    "cpu": 100000,
                    "memory": 1099511627776,
                    "hdd": 1099511627776000,
                    "ssd": 1099511627776000,
                    "ipv4": 0,
                }
            },
        )
    )


def is_cluster_with_qyp_dev_segment(cluster, client):
    try:
        client.get_object("node_segment", "dev", ["/meta/id"])
    except YpNoSuchObjectError:
        return False

    return True


def setup_dev_segment(cluster, accounts, client):
    cluster_to_accounts = {
        "sas": [
            Account(
                "tmp",
                {
                    "dev": {
                        "cpu": 159500,
                        "memory": 1389370749747,
                        "hdd": 15750772503347,
                        "ssd": 1192927166464,
                        "ipv4": 0,
                    }
                },
            )
        ],
        "man": [
            Account(
                "tmp",
                {
                    "dev": {
                        "cpu": 156202,
                        "memory": 1571816262861,
                        "hdd": 18485541004902,
                        "ssd": 992137445376,
                        "ipv4": 0,
                    }
                },
            )
        ],
        "vla": [
            Account(
                "tmp",
                {
                    "dev": {
                        "cpu": 283000,
                        "memory": 3653874640486,
                        "hdd": 15993491842662,
                        "ssd": 17324609581875,
                        "ipv4": 0,
                    }
                },
            )
        ],
        "sas-test": [
            Account(
                "tmp",
                {
                    "dev": {
                        "cpu": 283000,
                        "memory": 3653874640486,
                        "hdd": 15993491842662,
                        "ssd": 17324609581875,
                        "ipv4": 100,
                    }
                },
            )
        ],
    }
    accounts.extend(cluster_to_accounts.get(cluster, []))


####################################################################################################


# YPADMIN-287
def configure_pod_eviction_requesters_group(client):
    pod_eviction_requesters = Group("pod-eviction-requesters")
    members = (
        User("robot-yp-heavy-sched"),
        Group("abc:service:1171"),
    )
    add_group_members(client, pod_eviction_requesters, members)
    for attribute in ("/control/request_eviction", "/control/abort_eviction"):
        add_permission(
            client,
            object_type="schema",
            object_id="pod",
            subject=pod_eviction_requesters,
            permission="write",
            attribute=attribute,
        )


# Public objects common to all deploy systems (see YP-1769).
def configure_common_public_object_creators(client):
    common_public_object_types = (
        "endpoint",
        "endpoint_set",
    )
    common_public_object_creators = Group("common-public-object-creators")
    members = (Group("staff:department:1"),)
    add_group_members(client, common_public_object_creators, members)
    for object_type in common_public_object_types:
        add_permission(
            client,
            object_type="schema",
            object_id=object_type,
            subject=common_public_object_creators,
            permission="create",
            attribute="",
        )


# Public objects of Y.Deploy (see YP-1769).
def configure_deploy_public_object_creators(client):
    deploy_public_object_types = ("stage",)
    deploy_public_object_creators = Group("deploy-public-object-creators")
    members = (Group("staff:department:1"), User("robot-metrika-test"))
    add_group_members(client, deploy_public_object_creators, members)
    create(client, deploy_public_object_creators)
    for object_type in deploy_public_object_types:
        add_permission(
            client,
            object_type="schema",
            object_id=object_type,
            subject=deploy_public_object_creators,
            permission="create",
            attribute="",
        )


# YPADMIN-286
def configure_admins_group(client):
    admins = Group("admins")
    members = (
        User("babenko"),
        User("bidzilya"),
        User("deep"),
        User("ignat"),
        User("se4min"),
        User("slonnn"),
    )
    add_group_members(client, admins, members)
    for group_name in [
        "common-public-object-creators",
        "deploy-public-object-creators",
        "pod-eviction-requesters",
    ]:
        add_permission(
            client,
            object_type="group",
            object_id=group_name,
            subject=admins,
            permission="write",
            attribute="/spec/members",
        )


####################################################################################################


def initialize_users(cluster, dry_run):
    right_c = ["create"]
    right_crw = ["create", "read", "write"]
    right_ro = ["read"]
    right_rw = ["read", "write"]
    right_crwu = ["create", "read", "write", "use"]
    right_u = ["read", "use"]

    token = find_token()
    with YpClient(cluster, config=dict(token=token)) as raw_client:
        client = ClientWrapper(raw_client, dry_run)

        configurators = (
            configure_pod_eviction_requesters_group,
            configure_common_public_object_creators,
            configure_deploy_public_object_creators,
            configure_admins_group,
        )

        for configurator in configurators:
            try:
                configurator(client)
            except Exception:
                logger.exception("Error running %s", configurator.__name__)

        accounts = copy.deepcopy(ACCOUNTS)
        if cluster == "xdc":
            accounts_override_xdc(cluster, accounts, client)
        if is_cluster_with_qyp_dev_segment(cluster, client):
            setup_dev_segment(cluster, accounts, client)

        for subject_id in ("odin", "nanny-robot", "robot-yp-export"):
            create(client, User(subject_id))

        add_schema_permissions(client, "pod_set", User("robot-yp-export"), right_crw)
        add_schema_permissions(client, "pod_set", User("robot-yp-hfsm"), right_rw)

        add_schema_permissions(client, "replica_set", User("robot-rsc"), right_crw)

        add_schema_permissions(client, "node", User("robot-yp-export"), right_crw)
        add_schema_permissions(client, "node", User("robot-yp-hfsm"), right_crw)
        add_schema_permissions(client, "node", User("robot-yp-inet-mngr"), right_rw)
        add_schema_permissions(client, "node", User("robot-yp-eviction-st"), right_rw)

        add_schema_permissions(client, "node_segment", User("robot-yp-export"), right_crw)
        add_schema_permissions(client, "resource", User("robot-yp-export"), right_crw)
        add_schema_permissions(client, "user", User("robot-yp-export"), right_crw)
        add_schema_permissions(client, "group", User("robot-yp-export"), right_crw)
        add_schema_permissions(client, "group", User("robot-yp-idm"), right_rw)
        add_schema_permissions(client, "virtual_service", User("robot-yp-export"), right_crw)

        add_schema_permissions(client, "pod", User("robot-yp-hfsm"), right_rw)
        add_schema_permissions(client, "pod", User("robot-yp-pdns"), right_ro)
        add_schema_permissions(client, "pod", User("robot-yp-cauth"), right_ro)

        add_schema_permissions(client, "network_project", User("nanny-robot"), right_u)
        add_schema_permissions(client, "network_project", User("robot-yp-export"), right_crw)
        add_schema_permissions(client, "network_project", User("odin"), right_u)
        add_schema_permissions(client, "network_project", User("robot-rsc"), right_u)
        add_schema_permissions(client, "network_project", User("robot-mcrsc"), right_u)
        add_schema_permissions(client, "network_project", User("robot-vmagent-rtc"), right_u)

        add_schema_permissions(client, "account", User("robot-yp-export"), right_crw)
        add_schema_permissions(client, "account", User("nanny-robot"), right_u)
        add_schema_permissions(client, "account", User("robot-drug-deploy"), right_u)
        add_schema_permissions(client, "account", User("robot-mcrsc"), right_u)
        add_schema_permissions(client, "account", User("robot-rsc"), right_u)
        add_schema_permissions(client, "account", User("robot-vmagent-rtc"), right_u)

        add_schema_permissions(client, "internet_address", User("robot-yp-inet-mngr"), right_crwu)
        add_schema_permissions(client, "ip4_address_pool", User("robot-yp-inet-mngr"), right_crwu)

        add_schema_permissions(client, "endpoint_set", User("robot-srv-ctl"), right_rw)

        add_schema_permissions(client, "replica_set", User("robot-rsc"), right_rw)
        add_schema_permissions(client, "replica_set", User("robot-drug-deploy"), right_crwu)

        add_schema_permissions(client, "multi_cluster_replica_set", User("robot-mcrsc"), right_rw)
        add_schema_permissions(
            client, "multi_cluster_replica_set", User("robot-drug-deploy"), right_crw
        )

        add_schema_permissions(client, "stage", User("robot-drug-deploy"), right_rw)

        add_schema_permissions(client, "dynamic_resource", User("robot-yp-dynresource"), right_crwu)

        add_schema_permissions(client, "pod_disruption_budget", User("robot-yt-odin"), right_c)

        # DEPLOY-1117
        add_schema_permissions(client, "dynamic_resource", Group("everyone"), right_crwu)

        if cluster == "xdc":
            add_schema_permissions(client, "dns_record_set", User("robot-gencfg"), right_crw)

        # YPADMIN-233
        if cluster in ("sas-test", "man-pre"):
            add_schema_permissions(client, "stage", User("robot-deploy-test"), right_rw)
            add_schema_permissions(client, "account", User("robot-deploy-test"), right_u)

        add_schema_permissions(client, "dns_record_set", User("robot-ydnxdns-export"), right_crwu)

        # YPADMIN-257
        add_schema_permissions(client, "pod_disruption_budget", User("nanny-robot"), right_crw)
        if cluster in ("man-pre", "sas-test"):
            add_schema_permissions(
                client, "pod_disruption_budget", Group("abc:service-scope:730:5"), right_crw,
            )

        # YPADMIN-266
        if cluster in ("sas-test", "man-pre"):
            add_schema_permissions(client, "network_project", User("robot-deploy-test"), right_u)
        add_schema_permissions(client, "network_project", User("robot-drug-deploy"), right_u)

        # YPADMIN-282
        if cluster == "sas-test":
            add_schema_permissions(client, "group", User("robot-deploy-auth-t"), right_c)

        # YPSUPPORT-49
        if cluster in ("sas-test", "man-pre"):
            add_schema_permissions(client, "stage", User("robot-deploy-auth-t"), right_rw)
            add_schema_permissions(client, "account", User("robot-deploy-auth-t"), right_u)
        if cluster == "xdc":
            add_schema_permissions(client, "stage", User("robot-deploy-auth"), right_rw)
            add_schema_permissions(client, "account", User("robot-deploy-auth"), right_u)

        # YPSUPPORT-48
        if cluster in ("sas-test", "man-pre"):
            add_schema_permissions(
                client, "resource_cache", Group("abc:service-scope:3494:5"), right_crw,
            )

        create_accounts(client, cluster, accounts)

        allow_account_usage(client, account="odin", subject=User("odin"))
        allow_account_usage(client, account="odin", subject=User("robot-yt-odin"))

        assign_podsets_to_accounts(client, cluster)


def main(args):
    configure_logger()
    initialize_users(args.cluster, args.dry_run)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cluster", required=True)
    parser.add_argument("--dry-run", action="store_true", default=False)
    return parser.parse_args()


if __name__ == "__main__":
    main(parse_args())
