#!/usr/bin/env python

import yt.wrapper as yt
import yt.logger as logger
from yt.common import get_value

TB = 1024 ** 4

def create(type_, name, client):
    try:
        client.create(type_, attributes={"name": name})
    except yt.YtResponseError as err:
        if err.contains_code(501):
            logger.warning("'%s' already exists", name)
        else:
            raise

def add_member(subject, group, client):
    try:
        client.add_member(subject, group)
    except yt.YtResponseError as err:
        if "is already present in group" in err.message:
            logger.warning(err.message)
        else:
            raise

def add_acl(path, new_acl, client):
    current_acls = client.get(path + "/@acl")
    if new_acl not in current_acls:
        client.set(path + "/@acl/end", new_acl)

 # Backwards compatibility.
def get_default_resource_limits(client):
    result = {"node_count": 200000, "chunk_count": 1000000}
    if client.exists("//sys/media"):
        result["disk_space_per_medium"] = {"default": 10 * TB}
    else:
        result["disk_space"] = 10 * TB

    return result

def initialize_world(client=None):
    client = get_value(client, yt)

    for user in ["odin", "cron", "nightly_tester"]:
        create("user", user, client)
    for group in ["devs", "admins"]:
        create("group", group, client)
    add_member("cron", "superusers", client)
    add_member("devs", "admins", client)

    for dir in ["//sys", "//tmp", "//sys/tokens"]:
        client.set(dir + "/@opaque", "true")

    add_acl("/", {"action": "allow", "subjects": ["admins"], "permissions": ["write", "remove", "administer"]}, client)
    add_acl("//sys", {"action": "allow", "subjects": ["everyone"], "permissions": ["read"]}, client)
    add_acl("//sys", {"action": "allow", "subjects": ["admins"], "permissions": ["write", "remove", "administer"]}, client)
    client.set("//sys/@inherit_acl", "false")

    add_acl("//sys/accounts/sys", {"action": "allow", "subjects": ["root", "admins"], "permissions": ["use"]}, client)

    add_acl("//sys/tokens", {"action": "allow", "subjects": ["admins"], "permissions": ["read", "write", "remove"]}, client)
    client.set("//sys/tokens/@inherit_acl", "false")

    if not client.exists("//home"):
        client.create("map_node", "//home",
                  attributes={
                      "opaque": "true",
                      "account": "tmp"})

    for schema in ["user", "group", "tablet_cell"]:
        client.set("//sys/schemas/%s/@acl" % schema,
            [
                {"action": "allow", "subjects": ["everyone"], "permissions": ["read"]},
                {"action": "allow", "subjects": ["admins"], "permissions": ["write", "remove", "create"]}
            ])

    client.set("//sys/schemas/account/@acl",
        [
            {"action": "allow", "subjects": ["everyone"], "permissions": ["read"]},
            {"action": "allow", "subjects": ["admins"], "permissions": ["write", "remove", "create", "administer", "use"]}
        ])

    client.set("//sys/schemas/rack/@acl",
        [
            {"action": "allow", "subjects": ["everyone"], "permissions": ["read"]},
            {"action": "allow", "subjects": ["admins"], "permissions": ["write", "remove", "create", "administer"]}
        ])

    client.set("//sys/schemas/lock/@acl",
        [
            {"action": "allow", "subjects": ["everyone"], "permissions": ["read"]},
        ])

    client.set("//sys/schemas/transaction/@acl",
        [
            {"action": "allow", "subjects": ["everyone"], "permissions": ["read"]},
            {"action": "allow", "subjects": ["users"], "permissions": ["write", "create"]}
        ])

    if not client.exists("//sys/empty_yamr_table"):
        yamr_table_schema = [{"name": name, "type": "any", "sort_order": "ascending"}
                             for name in ["key", "subkey"]] + [{"name": "value", "type": "any"}]
        client.create("table", "//sys/empty_yamr_table", attributes={"schema": yamr_table_schema})

    client.create("account", attributes={"name": "tmp_files",
                                         "acl": [{"action": "allow", "subjects": ["users"], "permissions": ["use"]}],
                                         "resource_limits": get_default_resource_limits(client)})

def main():
    initialize_world()

if __name__ == "__main__":
    main()
