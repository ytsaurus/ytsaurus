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
        self._dry_objects = {}

    def create_object(self, *args, **kwargs):
        logger.info("Creating object: " + format_args(args, kwargs))
        if self._dry_run:
            self._dry_objects.setdefault(kwargs["object_type"], []).append(
                kwargs["attributes"]["meta"]["id"]
            )
        else:
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

    def has_dry_object(self, object_type, object_id):
        ids = self._dry_objects.get(object_type)
        return ids is not None and object_id in ids


def try_get_group_members(client, group, timestamp=None):
    try:
        spec = client.get_object("group", group, ["/spec"], timestamp=timestamp)[0]
        return spec.get("members", [])
    except YpNoSuchObjectError:
        if client.has_dry_object("group", group):
            logger.warning(
                "Group [%s] created in dry-run mode, assuming empty list of members", group
            )
            return []
        else:
            raise


def try_get_acl(client, object_type, object_id):
    try:
        return client.get_object(object_type, object_id, ["/meta/acl"])[0]
    except YpNoSuchObjectError:
        if client.has_dry_object(object_type, object_id):
            logger.warning(
                "Object [%s][%s] created in dry-run mode, assuming empty acl",
                object_type,
                object_id,
            )
            return []
        else:
            raise


def add_group_members(client, group, subjects):
    create(client, group)

    transaction = client.start_transaction(enable_structured_response=True)
    transaction_id = transaction["transaction_id"]
    start_timestamp = transaction["start_timestamp"]

    members = try_get_group_members(client, group.id, timestamp=start_timestamp)

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

    acl = try_get_acl(client, object_type, object_id)
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


def set_account(
    client,
    account_name,
    segment_name,
    cpu,
    memory,
    ipv4,
    disk_capacity_per_storage_class,
    disk_bandwidth_per_storage_class,
    gpu_capacity_per_model,
):
    resource_limits = client.get_object(
        "account", account_name, selectors=["/spec/resource_limits/per_segment"],
    )[0]
    resource_limits = resource_limits.get(segment_name, {})

    def get_resource(path_tokens, resource):
        node = resource_limits
        for token in path_tokens:
            node = node.get(token, {})
        return node.get(resource, None)

    def get_capacity(path_tokens):
        return get_resource(path_tokens, "capacity")

    def get_bandwidth(path_tokens):
        return get_resource(path_tokens, "bandwidth")

    updates_set = list()
    if cpu is not None and get_capacity(["cpu"]) != long(cpu):
        updates_set.append(
            dict(
                path="/spec/resource_limits/per_segment/{}/cpu/capacity".format(segment_name),
                value=long(cpu),
                recursive=True,
            )
        )

    if memory is not None and get_capacity(["memory"]) != long(memory):
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

    for storage_class in disk_capacity_per_storage_class:
        value = long(disk_capacity_per_storage_class[storage_class])

        if get_capacity(["disk_per_storage_class", storage_class]) != value:
            updates_set.append(
                dict(
                    path="/spec/resource_limits/per_segment/{}/disk_per_storage_class/{}/capacity".format(
                        segment_name, storage_class,
                    ),
                    value=value,
                    recursive=True,
                )
            )

    for storage_class in disk_bandwidth_per_storage_class:
        value = long(disk_bandwidth_per_storage_class[storage_class])

        if get_bandwidth(["disk_per_storage_class", storage_class]) != value:
            updates_set.append(
                dict(
                    path="/spec/resource_limits/per_segment/{}/disk_per_storage_class/{}/bandwidth".format(
                        segment_name, storage_class,
                    ),
                    value=value,
                    recursive=True,
                )
            )

    for model, capacity in gpu_capacity_per_model.items():
        value = long(capacity)

        if get_capacity(["gpu_per_model", model]) != value:
            updates_set.append(
                dict(
                    path="/spec/resource_limits/per_segment/{}/gpu_per_model/{}/capacity".format(
                        segment_name, model,
                    ),
                    value=value,
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

        def set_account_for_segment(segment, limits):
            set_account(
                client,
                account.name,
                segment,
                cpu=limits.get("cpu"),
                memory=limits.get("memory"),
                ipv4=limits.get("ipv4", 0),
                disk_capacity_per_storage_class=limits.get("disk_capacity_per_storage_class", {}),
                disk_bandwidth_per_storage_class=limits.get("disk_bandwidth_per_storage_class", {}),
                gpu_capacity_per_model=limits.get("gpu_capacity_per_model", {}),
            )

        if len(account.quotas_per_segment) == 1 and account.quotas_per_segment.keys()[0] == "*":
            limits = account.quotas_per_segment["*"]
            for segment in resolve_all_segments(client):
                set_account_for_segment(segment, limits)
        else:
            for segment in account.quotas_per_segment:
                limits = account.quotas_per_segment[segment]
                set_account_for_segment(segment, limits)


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
        {
            "*": {
                "cpu": 1000000,
                "memory": 10 * TB,
                "ipv4": 10,
                "disk_capacity_per_storage_class": {"hdd": 100 * TB, "ssd": 100 * TB},
            },
        },
    ),
    Account(
        "tmp",
        {
            "default": {
                "cpu": 1700000,
                "memory": 10000000000000,
                "disk_capacity_per_storage_class": {"hdd": 150000000000000, "ssd": 20000000000000},
                "disk_bandwidth_per_storage_class": {"hdd": 10 * GB, "ssd": 10 * GB},
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
                "disk_capacity_per_storage_class": {
                    "hdd": 1000000000000000,
                    "ssd": 1000000000000000,
                },
            }
        },
    ),
    Account(
        "tentacles",
        {
            "default": {
                "cpu": 100000000,
                "memory": 1000000000000,
                "disk_capacity_per_storage_class": {
                    "hdd": 1000000000000000,
                    "ssd": 1000000000000000,
                },
            },
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
                    "ipv4": 0,
                    "disk_capacity_per_storage_class": {
                        "hdd": 1099511627776000,
                        "ssd": 1099511627776000,
                    },
                },
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
                        "ipv4": 0,
                        "disk_capacity_per_storage_class": {
                            "hdd": 15750772503347,
                            "ssd": 1192927166464,
                        },
                        "disk_bandwidth_per_storage_class": {"hdd": 1 * GB, "ssd": 1 * GB},
                    },
                },
            ),
            Account(
                "abc:service:7758", {"gpu-dev": {"gpu_capacity_per_model": {"gpu_tesla_k40": 3}}},
            ),
        ],
        "man": [
            Account(
                "tmp",
                {
                    "dev": {
                        "cpu": 156202,
                        "memory": 1571816262861,
                        "ipv4": 0,
                        "disk_capacity_per_storage_class": {
                            "hdd": 18485541004902,
                            "ssd": 992137445376,
                        },
                        "disk_bandwidth_per_storage_class": {"hdd": 1 * GB, "ssd": 1 * GB},
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
                        "ipv4": 0,
                        "disk_capacity_per_storage_class": {
                            "hdd": 15993491842662,
                            "ssd": 17324609581875,
                        },
                        "disk_bandwidth_per_storage_class": {"hdd": 1 * GB, "ssd": 1 * GB},
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
                        "ipv4": 100,
                        "disk_capacity_per_storage_class": {
                            "hdd": 15993491842662,
                            "ssd": 17324609581875,
                        },
                    }
                },
            )
        ],
    }
    accounts.extend(cluster_to_accounts.get(cluster, []))


####################################################################################################


# YPADMIN-287
def configure_pod_eviction_requesters(client, **kwargs):
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


# YPADMIN-324
def configure_pod_resource_reallocators(client, **kwargs):
    pod_resource_reallocators = Group("pod-resource-reallocators")
    for attribute in ("/control/reallocate_resources",):
        add_permission(
            client,
            object_type="schema",
            object_id="pod",
            subject=pod_resource_reallocators,
            permission="write",
            attribute=attribute,
        )


# Public objects common to all deploy systems (see YP-1769).
def configure_common_public_object_creators(client, **kwargs):
    common_public_object_creators = Group("common-public-object-creators")
    members = (Group("staff:department:1"),)
    add_group_members(client, common_public_object_creators, members)


# Public objects of Y.Deploy (see YP-1769).
def configure_deploy_public_object_creators(client, cluster, **kwargs):
    deploy_public_object_creators = Group("deploy-public-object-creators")
    members = [Group("staff:department:1"), User("robot-infracloudui")]
    if cluster in ("xdc", "sas", "man", "vla", "iva", "myt"):
        members += [
            User("robot-metrika-test"),
            User("robot-market-infra"),
            User("robot-yappy"),
            User("robot-ci"),
            User("robot-tap"),
            User("robot-bobr"),
            User("robot-logbroker"),
            User("robot-trendbot"),
            User("robot-srch-releaser"),
            User("robot-vertis-shiva"),
            User("robot-partner"),
            User("zomb-podrick"),
            User("robot-tt-front"),
        ]
    add_group_members(client, deploy_public_object_creators, members)


# YPADMIN-316
def configure_yt_controllers(client, cluster, **kwargs):
    yt_controllers = Group("yt-controllers")
    members = (Group("abc:service:470"),)  # YT service.
    add_group_members(client, yt_controllers, members)
    segments_per_cluster = {
        "man-pre": ("yt_hume",),
        "sas": ("yt_ada", "yt_hahn", "yt_kelvin", "yt_locke", "yt_ofd_xdc", "yt_vanga",),
        "man": ("yt_freud", "yt_kelvin", "yt_locke", "yt_vanga",),
        "vla": ("yt_arnold", "yt_kelvin", "yt_locke", "yt_ofd_xdc", "yt_vanga",),
        "iva": ("yt_locke",),
        "myt": ("yt_locke", "yt_ofd_xdc", "yt_vanga",),
    }
    for segment in segments_per_cluster.get(cluster, tuple()):
        add_permission(
            client,
            "node_segment",
            segment,
            yt_controllers,
            "use",
            "/access/scheduling/assign_pod_to_node",
        )
        pod_sets_response = client.select_objects(
            "pod_set",
            filter='[/spec/node_segment_id] = "{}"'.format(segment),
            selectors=["/meta/id"],
        )
        pod_sets = map(lambda response: response[0], pod_sets_response)
        for pod_set in pod_sets:
            for permission in ("write", "read_secrets"):
                add_permission(
                    client, "pod_set", pod_set, yt_controllers, permission, attribute="",
                )


# YPADMIN-286
def configure_admins(client, **kwargs):
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
    for group_name in (
        "pod-eviction-requesters",
        "pod-resource-reallocators",
        "common-public-object-creators",
        "deploy-public-object-creators",
        "yt-controllers",
    ):
        add_permission(
            client,
            object_type="group",
            object_id=group_name,
            subject=admins,
            permission="write",
            attribute="/spec/members",
        )


####################################################################################################

PERMISSIONS_ALL = {
    "account": {
        User("robot-yp-export"): "crw",
        User("nanny-robot"): "u",
        User("robot-drug-deploy"): "u",
        User("robot-mcrsc"): "u",
        User("robot-rsc"): "u",
        User("robot-vmagent-rtc"): "u",
    },
    "dns_record_set": {User("robot-ydnxdns-export"): "crwu"},
    "dynamic_resource": {
        User("robot-yp-dynresource"): "crwu",
        Group("everyone"): "crwu",  # DEPLOY-1117
    },
    "endpoint": {Group("common-public-object-creators"): "c"},
    "endpoint_set": {Group("common-public-object-creators"): "c", User("robot-srv-ctl"): "rw"},
    "group": {User("robot-yp-export"): "crw", User("robot-yp-idm"): "rw"},
    "horizontal_pod_autoscaler": {User("robot-yd-hpa-ctl"): "rw"},  # YPSUPPORT-71
    "internet_address": {User("robot-yp-inet-mngr"): "crwu"},
    "ip4_address_pool": {User("robot-yp-inet-mngr"): "crwu"},
    "multi_cluster_replica_set": {User("robot-mcrsc"): "rw", User("robot-drug-deploy"): "rw"},
    "network_project": {
        User("nanny-robot"): "u",
        User("robot-yp-export"): "crw",
        User("odin"): "u",
        User("robot-rsc"): "u",
        User("robot-mcrsc"): "u",
        User("robot-vmagent-rtc"): "u",
        User("robot-drug-deploy"): "u",  # YPADMIN-266
    },
    "node": {
        User("robot-yp-export"): "crw",
        User("robot-yp-hfsm"): "crw",
        User("robot-yp-inet-mngr"): "rw",
        User("robot-yp-eviction-st"): "rw",
    },
    "node_segment": {User("robot-yp-export"): "crw"},
    "pod": {User("robot-yp-hfsm"): "rw", User("robot-yp-pdns"): "r", User("robot-yp-cauth"): "r"},
    "pod_disruption_budget": {User("nanny-robot"): "rw"},  # YPADMIN-257
    "pod_set": {User("robot-yp-export"): "crw", User("robot-yp-hfsm"): "rw"},
    "replica_set": {User("robot-rsc"): "rw", User("robot-drug-deploy"): "rwu"},
    "resource": {User("robot-yp-export"): "crw"},
    "resource_cache": {User("robot-rcc"): "rw"},  # YPSUPPORT-70
    "stage": {User("robot-drug-deploy"): "rw"},
    "user": {User("robot-yp-export"): "crw"},
    "virtual_service": {User("robot-yp-export"): "crw"},
}

PERMISSIONS_SAS_TEST = {
    "account": {
        User("robot-deploy-test"): "u",  # YPADMIN-233
        User("robot-deploy-auth-t"): "u",  # YPSUPPORT-49
    },
    "deploy_ticket": {Group("everyone"): "c"},  # YP-1769
    "dns_record_set": {User("robot-dns"): "c"},  # YPSUPPORT-69
    "endpoint_set": {Group("everyone"): "c"},  # YP-1769
    "group": {User("robot-deploy-auth-t"): "c"},  # YPADMIN-282
    "horizontal_pod_autoscaler": {Group("everyone"): "c"},  # YP-1769
    "multi_cluster_replica_set": {Group("everyone"): "c"},  # YP-1769
    "network_project": {User("robot-deploy-test"): "u"},  # YPADMIN-266
    "persistent_volume": {Group("everyone"): "c"},
    "persistent_disk": {Group("everyone"): "c"},
    "persistent_volume_claim": {Group("everyone"): "c"},
    "pod": {Group("everyone"): "c"},  # YP-1769
    "pod_disruption_budget": {
        Group("everyone"): "c",  # YP-1769
        Group("abc:service-scope:730:5"): "rw",  # YPADMIN-257
    },
    "pod_set": {Group("everyone"): "c"},  # YP-1769
    "project": {
        Group("everyone"): "c",  # YP-1769
        User("robot-deploy-auth-t"): "rw",  # YPSUPPORT-74
    },
    "release": {Group("everyone"): "c"},  # YP-1769
    "release_rule": {Group("everyone"): "c"},  # YP-1769
    "replica_set": {Group("everyone"): "c"},  # YP-1769
    "resource_cache": {Group("abc:service-scope:3494:5"): "crw"},  # YPSUPPORT-48
    "stage": {
        Group("everyone"): "c",  # YP-1769
        User("robot-deploy-test"): "rw",  # YPADMIN-233
        User("robot-deploy-auth-t"): "rw",  # YPSUPPORT-49
    },
}

PERMISSIONS_MAN_PRE = {
    "account": {
        User("robot-deploy-test"): "u",  # YPADMIN-233
        User("robot-deploy-auth-t"): "u",  # YPSUPPORT-49
    },
    "endpoint_set": {
        User("nanny-robot"): "c",  # YP-1769
        User("robot-deploy-test"): "c",  # YP-1769
        User("robot-drug-deploy"): "c",  # YP-1769
    },
    "group": {User("robot-deploy-auth-t"): "c"},  # YPSUPPORT-82
    "horizontal_pod_autoscaler": {
        User("robot-drug-deploy"): "c",  # YPSUPPORT-75, # YP-1769
        Group("deploy-public-object-creators"): "c",  # YPSUPPORT-75, # YP-1769
    },
    "multi_cluster_replica_set": {
        User("robot-deploy-test"): "c",  # YP-1769
        User("robot-drug-deploy"): "c",  # YP-1769
    },
    "network_project": {User("robot-deploy-test"): "u"},  # YPADMIN-266
    "pod": {
        User("robot-vmagent-rtc"): "c",  # YP-1769
        User("nanny-robot"): "c",  # YP-1769
        User("robot-mcrsc"): "c",  # YP-1769
        User("robot-rsc"): "c",  # YP-1769
    },
    "pod_disruption_budget": {
        User("nanny-robot"): "c",  # YP-1769
        User("robot-drug-deploy"): "c",  # YP-1769
        Group("abc:service-scope:730:5"): "rw",  # YPADMIN-257
    },
    "pod_set": {
        User("robot-vmagent-rtc"): "c",  # YP-1769
        User("nanny-robot"): "c",  # YP-1769
        User("robot-mcrsc"): "c",  # YP-1769
        User("robot-rsc"): "c",  # YP-1769
    },
    "project": {
        Group("deploy-public-object-creators"): "c",  # YP-1769
        User("robot-deploy-auth-t"): "crw",  # YPSUPPORT-74, YP-1769
    },
    "replica_set": {
        User("robot-deploy-test"): "c",  # YP-1769
        User("robot-drug-deploy"): "c",  # YP-1769
    },
    "resource_cache": {Group("abc:service-scope:3494:5"): "crw"},  # YPSUPPORT-48
    "stage": {
        Group("deploy-public-object-creators"): "c",  # YP-1769
        User("robot-deploy-test"): "rw",  # YPADMIN-233
        User("robot-deploy-auth-t"): "rw",  # YPSUPPORT-49
    },
}

PERMISSIONS_PROD = {
    "endpoint_set": {
        User("nanny-robot"): "c",  # YP-1769
        User("robot-deploy-test"): "c",  # YP-1769
        User("robot-drug-deploy"): "c",  # YP-1769
    },
    "group": {User("robot-deploy-auth"): "c"},  # YPSUPPORT-82
    "horizontal_pod_autoscaler": {
        User("robot-drug-deploy"): "c",  # YPSUPPORT-75, # YP-1769
        Group("deploy-public-object-creators"): "c",  # YPSUPPORT-75, # YP-1769
    },
    "pod": {
        User("robot-vmagent-rtc"): "c",  # YP-1769
        User("nanny-robot"): "c",  # YP-1769
        User("robot-mcrsc"): "c",  # YP-1769
        User("robot-rsc"): "c",  # YP-1769
    },
    "pod_disruption_budget": {
        User("nanny-robot"): "c",  # YP-1769
        User("robot-drug-deploy"): "c",  # YP-1769
    },
    "pod_set": {
        User("robot-vmagent-rtc"): "c",  # YP-1769
        User("nanny-robot"): "c",  # YP-1769
        User("robot-mcrsc"): "c",  # YP-1769
        User("robot-rsc"): "c",  # YP-1769
    },
    "replica_set": {
        User("robot-deploy-test"): "c",  # YP-1769
        User("robot-drug-deploy"): "c",  # YP-1769
    },
}

PERMISSIONS_XDC = {
    "account": {User("robot-deploy-auth"): "u"},  # YPSUPPORT-49
    "deploy_ticket": {
        User("robot-srch-releaser"): "c",  # YP-1769
        User("robot-yd-releaser"): "c",  # YP-1769
    },
    "dns_record_set": {User("robot-gencfg"): "crw", User("mcden"): "crw"},  # YPSUPPORT-72
    "endpoint": {User("robot-gencfg"): "c"},  # YP-1769
    "endpoint_set": {User("robot-gencfg"): "c"},  # YP-1769
    "group": {User("robot-deploy-auth"): "c"},  # YPSUPPORT-81
    "multi_cluster_replica_set": {
        User("robot-deploy-test"): "c",  # YP-1769
        User("robot-drug-deploy"): "c",  # YP-1769
    },
    "project": {
        Group("deploy-public-object-creators"): "c",  # YP-1769
        User("robot-deploy-auth"): "crw",  # YPSUPPORT-74, YP-1769
    },
    "release": {Group("everyone"): "c"},  # YP-1769
    "release_rule": {Group("deploy-public-object-creators"): "c"},  # YP-1769
    "stage": {
        Group("deploy-public-object-creators"): "c",  # YP-1769
        User("robot-deploy-auth"): "rw",  # YPSUPPORT-49
    },
}


def _convert_permission(p):
    mapping = {
        "c": "create",
        "r": "read",
        "w": "write",
        "u": "use",
    }

    permission = mapping.get(p)
    if permission is None:
        raise Exception("Bad permission: " + p)
    return permission


def _add_schema_permissions_impl(client, permissions_bundle):
    for object_id, subjects in permissions_bundle.items():
        for subject, ps in subjects.items():
            for p in ps:
                add_permission(client, "schema", object_id, subject, _convert_permission(p), "")


def add_schema_permissions(client, cluster):
    _add_schema_permissions_impl(client, PERMISSIONS_ALL)

    if cluster == "sas-test":
        _add_schema_permissions_impl(client, PERMISSIONS_SAS_TEST)
    elif cluster == "man-pre":
        _add_schema_permissions_impl(client, PERMISSIONS_MAN_PRE)
    elif cluster == "xdc":
        _add_schema_permissions_impl(client, PERMISSIONS_XDC)
    elif cluster in ("sas", "man", "vla", "iva", "myt"):
        _add_schema_permissions_impl(client, PERMISSIONS_PROD)
    else:
        raise Exception("Cluster not found")


def initialize_users(cluster, dry_run):
    token = find_token()
    with YpClient(cluster, config=dict(token=token)) as raw_client:
        client = ClientWrapper(raw_client, dry_run)

        configurators = (
            configure_pod_eviction_requesters,
            configure_pod_resource_reallocators,
            configure_common_public_object_creators,
            configure_deploy_public_object_creators,
            configure_yt_controllers,
            configure_admins,
        )

        for configurator in configurators:
            try:
                configurator(client, cluster=cluster)
            except Exception:
                logger.exception("Error running %s", configurator.__name__)

        accounts = copy.deepcopy(ACCOUNTS)
        if cluster == "xdc":
            accounts_override_xdc(cluster, accounts, client)
        if is_cluster_with_qyp_dev_segment(cluster, client):
            setup_dev_segment(cluster, accounts, client)

        for subject_id in ("odin", "nanny-robot", "robot-yp-export"):
            create(client, User(subject_id))

        add_schema_permissions(client, cluster)

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
