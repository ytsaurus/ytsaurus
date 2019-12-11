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


def configure_logger():
    global logger

    logging.basicConfig(
        format="%(asctime)-15s %(levelname)s %(message)s",
        level=logging.INFO,
        stream=sys.stdout,
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


def infer_subject_type(subject):
    if subject.startswith("abc:"):
        return "group"
    return "user"


def create_subject(client, subject):
    if subject == "everyone":
        return

    type_name = infer_subject_type(subject)

    try:
        client.get_object(type_name, subject, ["/meta"])
    except YpNoSuchObjectError:
        client.create_object(
            object_type=type_name,
            attributes=dict(meta=dict(id=subject)),
        )


def set_schema_permissions(client, type, subject, rights):
    # type: (YpClient, basestring, basestring, list, basestring) -> None

    create_subject(client, subject)

    logger.debug("Set schema permission subject={}, type={}, rights={}".format(subject, type, rights))

    rights_to_grant = set(rights)
    schema_rights = client.get_object("schema", type, ["/meta/acl"])

    logger.debug("Current schema permissions for subject={}, type={}, rights={}".format(subject, type, schema_rights))

    if schema_rights:
        actual_subject_permissions = set()
        for record in schema_rights[0]:
            action, subjects, permissions = record["action"], record["subjects"], record["permissions"]
            assert action == "allow"
            if subject in subjects or "everyone" in subjects:
                for permission in permissions:
                    actual_subject_permissions.add(permission)

        rights_to_add = rights_to_grant.difference(actual_subject_permissions)
        # rights_to_revoke = actual_subject_permissions.difference(rights_to_grant)

        updates_set = []
        for right in rights_to_add:
            updates_set.append(
                {"path": "/meta/acl/end",
                 "value": {
                     "action": "allow",
                     "subjects": [subject],
                     "permissions": [right]
                 }})

        # if len(rights_to_revoke):
        #     for right in rights_to_revoke:
        #         updates_remove.append(
        #             {"path": "/meta/acl/end",
        #              "value": {
        #                  "action": "allow",
        #                  "subjects": [subject],
        #                  "permissions": [right]
        #              }})

        if updates_set:
            client.update_object("schema", type, updates_set)


def set_schema_permission_for_attribute(client, type, subject, permission, attribute):
    create_subject(client, subject)

    logger.debug("Setting schema permission for attribute: type = {}, subject = {}, permission = {}, attribute = {}".format(
        type,
        subject,
        permission,
        attribute,
    ))

    matching_ace = None

    acl = client.get_object("schema", type, ["/meta/acl"])[0]
    for ace in acl:
        if ace["action"] != "allow":
            continue
        if subject not in ace["subjects"]:
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

    set_updates = [
        dict(
            path="/meta/acl/end",
            value=dict(
                action="allow",
                subjects=[subject],
                permissions=[permission],
                attributes=[attribute],
            )
        )
    ]

    client.update_object("schema", type, set_updates=set_updates)


def set_account(client, account_name, segment_name, cpu, memory, hdd, ssd, ipv4):
    resource_limits = client.get_object(
        "account",
        account_name,
        selectors=["/spec/resource_limits/per_segment"],
    )[0]
    resource_limits = resource_limits.get(segment_name, {})

    def get_capacity(path_tokens):
        node = resource_limits
        for token in path_tokens:
            node = node.get(token, {})
        return node.get("capacity", None)

    updates_set = list()
    if get_capacity(["cpu"]) != long(cpu):
        updates_set.append(dict(
            path="/spec/resource_limits/per_segment/{}/cpu/capacity".format(segment_name),
            value=long(cpu),
            recursive=True,
        ))

    if get_capacity(["memory"]) != long(memory):
        updates_set.append(dict(
            path="/spec/resource_limits/per_segment/{}/memory/capacity".format(segment_name),
            value=long(memory),
            recursive=True,
        ))

    if get_capacity(["internet_address"]) != long(ipv4):
        updates_set.append(dict(
            path="/spec/resource_limits/per_segment/{}/internet_address/capacity".format(segment_name),
            value=long(ipv4),
            recursive=True,
        ))

    if get_capacity(["disk_per_storage_class", "hdd"]) != long(hdd):
        updates_set.append(dict(
            path="/spec/resource_limits/per_segment/{}/disk_per_storage_class/hdd/capacity".format(segment_name),
            value=long(hdd),
            recursive=True,
        ))

    if get_capacity(["disk_per_storage_class", "ssd"]) != long(ssd):
        updates_set.append(dict(
            path="/spec/resource_limits/per_segment/{}/disk_per_storage_class/ssd/capacity".format(segment_name),
            value=long(ssd),
            recursive=True,
        ))

    if updates_set:
        client.update_object("account", account_name, updates_set)


def create_account(client, account_name, allow_use_for_all):
    # type: (YpClient, basestring, basestring) -> None
    try:
        client.get_object("account", account_name, ["/meta"])
    except YpNoSuchObjectError:
        attributes = {"meta": {"id": account_name, "inherit_acl": True}}
        if allow_use_for_all:
            attributes["meta"]["acl"] = [
                {
                    "action": "allow",
                    "permissions": ["use"],
                    "subjects": ["everyone"]
                }
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
                set_account(client, account.name, segment,
                            cpu=limits["cpu"], memory=limits["memory"],
                            hdd=limits["hdd"], ssd=limits["ssd"],
                            ipv4=limits.get("ipv4", 0))

        else:
            for segment in account.quotas_per_segment:
                limits = account.quotas_per_segment[segment]
                set_account(client, account.name, segment,
                            cpu=limits["cpu"], memory=limits["memory"],
                            hdd=limits["hdd"], ssd=limits["ssd"],
                            ipv4=limits.get("ipv4", 0))


# setup tentacles
def setup_tentacles_podset(client, cluster):
    tentacles_podset_name = "yp-rtc-sla-tentacles-production-{}".format(cluster)
    try:
        account_id = client.get_object("pod_set", tentacles_podset_name, ["/spec/account_id"])[0]
    except YpNoSuchObjectError:
        return

    updates_set = []
    if account_id != "tentacles":
        updates_set.append(dict(
            path="/spec/account_id",
            value="tentacles",
        ))

    if updates_set:
        client.update_object("pod_set", tentacles_podset_name, updates_set)


def assign_podsets_to_accounts(client, cluster):
    setup_tentacles_podset(client, cluster)


def allow_account_usage(client, account, subject):
    can_use = client.check_object_permissions(
        [
            {"object_type": "account",
             "object_id": account,
             "subject_id": subject,
             "permission": "use"}])

    if len(can_use) == 0 or can_use[0]["action"] != "allow":
        updates_set = list()
        updates_set.append(
            {"path": "/meta/acl/end",
             "value": {
                 "action": "allow",
                 "permissions": ["use"],
                 "subjects": [subject]
             }
             })

        client.update_object("account", account, updates_set)


class Account(object):
    def __init__(self, name, quotas_per_segment, allow_use_for_all=False, use_allowed_to=None):
        for segment in quotas_per_segment:
            if segment == "*":
                assert len(quotas_per_segment) == 1

        self.name = name
        self.quotas_per_segment = quotas_per_segment
        self.allow_use_for_all = allow_use_for_all
        self.use_allowed_to = use_allowed_to

KB = 1024
MB = 1024 * KB
GB = 1024 * MB
TB = 1024 * GB


ACCOUNTS = [
    Account("replication-common-account", {
        "*": {
            "cpu": 1000000,
            "memory": 10*TB,
            "hdd": 100*TB,
            "ssd": 100*TB,
            "ipv4": 10
        }
    }),

    Account("tmp", {
        "default": {
            "cpu": 1700000,
            "memory": 10000000000000,
            "hdd": 150000000000000,
            "ssd": 20000000000000
        },
    }, allow_use_for_all=True),

    Account("odin",
            {
                "*": {
                    "cpu": 100000000,
                    "memory": 1000000000000,
                    "hdd": 1000000000000000,
                    "ssd": 1000000000000000
                }
            },
            ),

    Account("tentacles", {
        "default": {
            "cpu": 100000000,
            "memory": 1000000000000,
            "hdd": 1000000000000000,
            "ssd": 1000000000000000
        }
    }),
]


def accounts_override_xdc(cluster, accounts, client):
    # XDC accounts not presented in ABC, so order monitoring resources inplace
    accounts.append(
        Account("abc:service:1979",
                {
                    "default": {
                        "cpu": 100000,
                        "memory": 1099511627776,
                        "hdd": 1099511627776000,
                        "ssd": 1099511627776000,
                        "ipv4": 0
                    }
                })
    )


def accounts_override_man_pre(cluster, accounts, client):
    pass


def accounts_override_sas(cluster, accounts, client):
    pass


def accounts_override_man(cluster, accounts, client):
    pass


def is_cluster_with_qyp_dev_segment(cluster, client):
    try:
        client.get_object("node_segment", "dev", ["/meta/id"])
    except YpNoSuchObjectError:
        return False

    return True


def setup_dev_segment(cluster, accounts, client):
    if cluster == "sas":
        accounts.append(
            Account("tmp", {
                    "dev": {
                        "cpu": 159500,
                        "memory": 1389370749747,
                        "hdd": 15750772503347,
                        "ssd": 1192927166464,
                        "ipv4": 0
                        }
                    })
        )
    elif cluster == "man":
        accounts.append(
            Account("tmp", {
                    "dev": {
                        "cpu": 156202,
                        "memory": 1571816262861,
                        "hdd": 18485541004902,
                        "ssd": 992137445376,
                        "ipv4": 0
                        }
                    })
        )
    elif cluster == "vla":
        accounts.append(
            Account("tmp", {
                    "dev": {
                        "cpu": 283000,
                        "memory": 3653874640486,
                        "hdd": 15993491842662,
                        "ssd": 17324609581875,
                        "ipv4": 0
                        }
                    })
        )
    elif cluster == "sas-test":
        accounts.append(
            Account("tmp", {
                    "dev": {
                        "cpu": 283000,
                        "memory": 3653874640486,
                        "hdd": 15993491842662,
                        "ssd": 17324609581875,
                        "ipv4": 100
                        }
                    })
        )

    else:
        assert not "Should not be here"


def accounts_override(cluster, accounts, client):
    if cluster == "xdc":
        accounts_override_xdc(cluster, accounts, client)

    if cluster == "man-pre":
        accounts_override_man_pre(cluster, accounts, client)

    if cluster == "sas":
        accounts_override_sas(cluster, accounts, client)

    if cluster == "man":
        accounts_override_man(cluster, accounts, client)

    if is_cluster_with_qyp_dev_segment(cluster, client):
        setup_dev_segment(cluster, accounts, client)


def initialize_users(cluster, dry_run):
    right_crw = ["create", "read", "write"]
    right_ro = ["read"]
    right_rw = ["read", "write"]
    right_crwu = ["create", "read", "write", "use"]
    right_u = ["read", "use"]

    token = find_token()
    with YpClient(cluster, config=dict(token=token)) as raw_client:
        client = ClientWrapper(raw_client, dry_run)

        accounts = copy.deepcopy(ACCOUNTS)
        accounts_override(cluster, accounts, client)

        for subject in ("odin", "nanny-robot", "robot-yp-export"):
            create_subject(client, subject)

        set_schema_permissions(client, "pod_set", "robot-yp-export", right_crw)
        set_schema_permissions(client, "pod_set", "robot-yp-hfsm", right_rw)

        set_schema_permissions(client, "replica_set", "robot-rsc", right_crw)

        set_schema_permissions(client, "node", "robot-yp-export", right_crw)
        set_schema_permissions(client, "node", "robot-yp-hfsm", right_crw)
        set_schema_permissions(client, "node", "robot-yp-inet-mngr", right_rw)
        set_schema_permissions(client, "node", "robot-yp-eviction-st", right_rw)

        set_schema_permissions(client, "node_segment", "robot-yp-export", right_crw)
        set_schema_permissions(client, "resource", "robot-yp-export", right_crw)
        set_schema_permissions(client, "user", "robot-yp-export", right_crw)
        set_schema_permissions(client, "group", "robot-yp-export", right_crw)
        set_schema_permissions(client, "group", "robot-yp-idm", right_rw)
        set_schema_permissions(client, "virtual_service", "robot-yp-export", right_crw)

        set_schema_permissions(client, "pod", "robot-yp-hfsm", right_rw)
        set_schema_permissions(client, "pod", "robot-yp-pdns", right_ro)
        set_schema_permissions(client, "pod", "robot-yp-cauth", right_ro)

        set_schema_permissions(client, "network_project", "nanny-robot", right_u)
        set_schema_permissions(client, "network_project", "robot-yp-export", right_crw)
        set_schema_permissions(client, "network_project", "odin", right_u)
        set_schema_permissions(client, "network_project", "robot-rsc", right_u)
        set_schema_permissions(client, "network_project", "robot-mcrsc", right_u)
        set_schema_permissions(client, "network_project", "robot-vmagent-rtc", right_u)

        set_schema_permissions(client, "account", "robot-yp-export", right_crw)
        set_schema_permissions(client, "account", "nanny-robot", right_u)
        set_schema_permissions(client, "account", "robot-drug-deploy", right_u)
        set_schema_permissions(client, "account", "robot-mcrsc", right_u)
        set_schema_permissions(client, "account", "robot-rsc", right_u)
        set_schema_permissions(client, "account", "robot-vmagent-rtc", right_u)

        set_schema_permissions(client, "internet_address", "robot-yp-inet-mngr", right_crwu)
        set_schema_permissions(client, "ip4_address_pool", "robot-yp-inet-mngr", right_crwu)

        set_schema_permissions(client, "endpoint_set", "robot-srv-ctl", right_rw)

        set_schema_permissions(client, "replica_set", "robot-rsc", right_rw)
        set_schema_permissions(client, "replica_set", "robot-drug-deploy", right_crwu)

        set_schema_permissions(client, "multi_cluster_replica_set", "robot-mcrsc", right_rw)
        set_schema_permissions(client, "multi_cluster_replica_set", "robot-drug-deploy", right_crw)

        set_schema_permissions(client, "stage", "robot-drug-deploy", right_rw)

        set_schema_permissions(client, "dynamic_resource", "robot-yp-dynresource", right_crwu)

        # DEPLOY-1117
        set_schema_permissions(client, "dynamic_resource", "everyone", right_crwu)

        if cluster == "xdc":
            set_schema_permissions(client, "dns_record_set", "robot-gencfg", right_crw)

        # YPADMIN-233
        if cluster in ("sas-test", "man-pre"):
            set_schema_permissions(client, "stage", "robot-deploy-test", right_rw)
            set_schema_permissions(client, "account", "robot-deploy-test", right_u)

        set_schema_permissions(client, "dns_record_set", "robot-ydnxdns-export", right_crwu)

        # YPADMIN-257
        set_schema_permissions(client, "pod_disruption_budget", "nanny-robot", right_crw)
        if cluster in ("man-pre", "sas-test"):
            set_schema_permissions(client, "pod_disruption_budget", "abc:service-scope:730:5", right_crw)

        set_schema_permission_for_attribute(client, "pod", "robot-yp-heavy-sched", "write", "/control/request_eviction")

        # YPADMIN-266
        if cluster in ("sas-test", "man-pre"):
            set_schema_permissions(client, "network_project", "robot-deploy-test", right_u)
        set_schema_permissions(client, "network_project", "robot-drug-deploy", right_u)

        create_accounts(client, cluster, accounts)

        allow_account_usage(client, account="odin", subject="odin")
        allow_account_usage(client, account="odin", subject="robot-yt-odin")

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
