#!/usr/bin/env python

from .init_queue_agent_state import create_tables as create_queue_agent_state_tables

from yt.wrapper.constants import UI_ADDRESS_PATTERN

from yt.common import get_fqdn

import yt.wrapper as yt
import yt.logger as logger
from yt.common import get_value, wait

import string
import time
import argparse


def create(type_, name, client):
    try:
        client.create(type_, attributes={"name": name})
    except yt.YtResponseError as err:
        if err.is_already_exists():
            logger.warning("'%s' already exists", name)
        else:
            raise


def is_member_of(subject, group, client):
    members = client.get("//sys/groups/{0}/@members".format(group))
    return subject in members


def add_member(subject, group, client):
    try:
        client.add_member(subject, group)
    except:
        if is_member_of(subject, group, client):
            logger.warning("'{0}' is already present in group '{1}'".format(subject, group))
            return True
        else:
            raise


def check_acl(acl, required_keys, optional_keys):
    for k in required_keys:
        if k not in acl:
            logger.warning("Can't find required key '%s' in ACL: %s", k, acl)
            return False
    for k in acl:
        if k not in optional_keys and k not in required_keys:
            logger.warning("Found unknown key '%s' in ACL: %s", k, acl)
            return False
    return True


def need_to_add_new_acl(new_acl, current_acls):
    required_keys = ["subjects", "permissions", "action"]
    optional_keys = ["inheritance_mode"]

    if not check_acl(new_acl, required_keys, optional_keys):
        return False

    for cur_acl in current_acls:
        found_equal_acl = True
        for k in new_acl:
            if k in cur_acl:
                if sorted(new_acl[k]) != sorted(cur_acl[k]):
                    found_equal_acl = False

        if found_equal_acl:
            return False

    return True


def add_acl(path, new_acl, client):
    current_acls = client.get(path + "/@acl")

    if need_to_add_new_acl(new_acl, current_acls):
        client.set(path + "/@acl/end", new_acl)
    else:
        logger.warning("ACL '%s' is already present in %s/@acl", new_acl, path)


def get_default_resource_limits(client):
    """By default, accounts have empty resource limits upon creation."""
    GB = 1024 ** 3
    TB = 1024 ** 4

    result = {
        "node_count": 500000,
        "chunk_count": 1000000,
        "tablet_count": 1000,
        "tablet_static_memory": 1 * GB,
        "disk_space_per_medium": {"default": 10 * TB},
    }

    return result


def create_account(client, attributes):
    client.create("account", attributes=attributes)

    GB = 1024 ** 3
    account_name = attributes["name"]
    client.set(
        "//sys/accounts/{0}/@resource_limits/master_memory/total".format(account_name),
        100 * GB
    )
    client.set(
        "//sys/accounts/{0}/@resource_limits/master_memory/chunk_host".format(account_name),
        100 * GB
    )


def initialize_world(client=None, idm=None, proxy_address=None, ui_address=None, configure_pool_trees=True, is_multicell=False):
    client = get_value(client, yt)
    users = ["robot-yt-mon", "robot-yt-idm", "robot-yt-hermes"]
    groups = ["devs", "admins", "admin_snapshots"]
    if idm:
        groups.append("yandex")
    everyone_group = "users" if not idm else "yandex"

    for user in users:
        create("user", user, client)
    for group in groups:
        create("group", group, client)

    client.create("map_node", "//sys/cron", ignore_existing=True)

    add_member("devs", "admins", client)
    add_member("robot-yt-mon", "admin_snapshots", client)
    add_member("robot-yt-idm", "superusers", client)

    for dir in ["//sys", "//tmp", "//sys/tokens"]:
        client.set(dir + "/@opaque", "true")

    add_acl("/", {"action": "allow", "subjects": [everyone_group], "permissions": ["read"]}, client)
    add_acl("/", {"action": "allow", "subjects": ["admins"], "permissions": ["write", "remove", "administer", "mount"]}, client)

    add_acl("//sys", {"action": "allow", "subjects": ["users"], "permissions": ["read"]}, client)
    add_acl("//sys", {"action": "allow", "subjects": ["admins"], "permissions": ["write", "remove", "administer", "mount"]},
            client)
    client.set("//sys/@inherit_acl", "false")

    add_acl("//sys/accounts/sys", {"action": "allow", "subjects": ["root", "admins"], "permissions": ["use"]}, client)

    add_acl("//sys/tokens", {"action": "allow", "subjects": ["admins"], "permissions": ["read", "write", "remove"]},
            client)
    add_acl("//sys/tablet_cells", {"action": "allow", "subjects": ["admins"], "permissions": ["read", "write", "remove", "administer"]}, client)
    client.set("//sys/tokens/@inherit_acl", "false")
    client.set("//sys/tablet_cells/@inherit_acl", "false")

    if not client.exists("//sys/accounts/tmp_files"):
        create_account(client, attributes={
            "name": "tmp_files",
            "acl": [{
                "action": "allow",
                "subjects": ["users"],
                "permissions": ["use"]
            }],
            "resource_limits": get_default_resource_limits(client)})
        if is_multicell:
            wait(lambda: client.get("//sys/accounts/tmp_files/@life_stage") == 'creation_committed')
    else:
        logger.warning("Account 'tmp_files' already exists")

    if not client.exists("//sys/accounts/default"):
        create_account(client, attributes={
            "name": "default",
            "acl": [{
                "action": "allow",
                "subjects": ["users"],
                "permissions": ["use"]
            }],
            "resource_limits": get_default_resource_limits(client)})

        if is_multicell:
            wait(lambda: client.get("//sys/accounts/default/@life_stage") == 'creation_committed')
    else:
        logger.warning("Account 'default' already exists")

    if not client.exists("//sys/accounts/tmp_jobs"):
        create_account(client, attributes={
            "name": "tmp_jobs",
            "resource_limits": get_default_resource_limits(client)})
        if is_multicell:
            wait(lambda: client.get("//sys/accounts/tmp_jobs/@life_stage") == 'creation_committed')
    else:
        logger.warning("Account 'tmp_jobs' already exists")

    if not client.exists("//home"):
        client.create("map_node", "//home",
                      attributes={
                          "opaque": "true",
                          "account": "default"})

    client.create("map_node", "//sys/admin", ignore_existing=True)
    client.create("map_node", "//sys/admin/snapshots", ignore_existing=True)

    if client.exists("//sys/admin"):
        client.set("//sys/admin/@acl",
                   [
                       {"action": "allow", "subjects": ["admins"], "permissions": ["write", "remove", "read", "mount", "administer"]}
                   ])

    if client.exists("//sys/admin/snapshots"):
        client.set("//sys/admin/snapshots/@acl",
                   [
                       {"action": "allow", "subjects": ["admin_snapshots"], "permissions": ["read"]}
                   ])

    client.create("map_node", "//sys/admin/odin", ignore_existing=True)
    client.create("map_node", "//sys/admin/lock/autorestart/nodes/disabled", recursive=True, ignore_existing=True)

    for medium in ["default", "ssd_journals"]:
        if not client.exists("//sys/media/%s" % medium):
            # COMPAT(babenko)
            try:
                client.create("domestic_medium", attributes={"name": medium})
            except yt.YtResponseError as err:
                if err.contains_text("Error parsing"):
                    client.create("medium", attributes={"name": medium})

    # add_acl to schemas
    for schema in ["user", "group", "tablet_cell", "tablet_cell_bundle"]:
        if client.exists("//sys/schemas/%s" % schema):
            client.set("//sys/schemas/%s/@acl" % schema,
                       [
                           {"action": "allow", "subjects": ["users"], "permissions": ["read"]},
                           {"action": "allow", "subjects": ["admins"], "permissions": ["write", "remove", "create"]}
                       ])

    if client.exists("//sys/schemas/account"):
        client.set("//sys/schemas/account/@acl",
                   [
                       {"action": "allow", "subjects": ["users"], "permissions": ["read"]},
                       {"action": "allow", "subjects": ["admins"],
                        "permissions": ["write", "remove", "create", "administer", "use"]}
                   ])

    for schema in ["rack", "cluster_node", "data_center"]:
        if client.exists("//sys/schemas/%s" % schema):
            client.set("//sys/schemas/%s/@acl" % schema,
                       [
                           {"action": "allow", "subjects": ["users"], "permissions": ["read"]},
                           {"action": "allow", "subjects": ["admins"],
                            "permissions": ["write", "remove", "create", "administer"]}
                       ])
    if client.exists("//sys/schemas/lock"):
        client.set("//sys/schemas/lock/@acl",
                   [
                       {"action": "allow", "subjects": ["users"], "permissions": ["read"]},
                   ])

    if client.exists("//sys/schemas/transaction"):
        client.set(
            "//sys/schemas/transaction/@acl",
            [
                {"action": "allow", "subjects": ["users"], "permissions": ["read"]},
                {"action": "allow", "subjects": ["users"], "permissions": ["write", "create"]}
            ])

    if not client.exists("//sys/empty_yamr_table"):
        yamr_table_schema = [{"name": name, "type": "any", "sort_order": "ascending"}
                             for name in ["key", "subkey"]] + [{"name": "value", "type": "any"}]
        client.create("table", "//sys/empty_yamr_table", attributes={"schema": yamr_table_schema})

    if client.exists("//sys/schemas/tablet_cell_bundle"):
        client.set(
            "//sys/schemas/tablet_cell_bundle/@options",
            [{
                "snapshot_replication_factor": 5,
                "snapshot_primary_medium": "default",
                "changelog_write_quorum": 3,
                "changelog_replication_factor": 5,
                "changelog_read_quorum": 3,
                "changelog_primary_medium": "ssd_journals"
            }])
        client.set("//sys/schemas/tablet_cell_bundle/@enable_bundle_balancer", False)

    add_acl("//tmp", {"action": "allow", "subjects": [everyone_group], "permissions": ["write", "remove", "read"]}, client)

    client.create("map_node",
                  "//tmp/yt_wrapper/file_storage",
                  attributes={"account": "tmp_files"},
                  recursive=True,
                  ignore_existing=True)
    client.create("map_node",
                  "//tmp/yt_wrapper/table_storage",
                  recursive=True,
                  ignore_existing=True)
    client.create("map_node",
                  "//tmp/yt_regular/table_storage",
                  recursive=True,
                  ignore_existing=True)
    client.set("//tmp/yt_regular/@acl", [
        {
            "action": "allow",
            "subjects": ["admins"],
            "permissions": ["read", "write", "remove", "administer"]
        }
    ])
    client.set("//tmp/yt_regular/@inherit_acl", False)

    if not client.exists("//sys/tablet_cell_bundles/sys"):
        client.create("tablet_cell_bundle", attributes={
            "name": "sys",
            "options": {"changelog_account": "sys", "snapshot_account": "sys"}})
    else:
        logger.warning('Tablet cell bundle "sys" already exists')

    if proxy_address is not None:
        client.set("//sys/@cluster_proxy_address", proxy_address)
    if ui_address is not None:
        client.set("//sys/@cluster_ui_address", ui_address)
    client.set("//sys/@ui_config", {"web_json_value_format": "yql"})

    client.create("map_node", "//tmp/trash", ignore_existing=True)

    client.set(
        "//tmp/trash/@acl",
        [
            {"action": "deny", "subjects": ["everyone"], "permissions": ["remove"], "inheritance_mode": "object_only"}
        ])

    client.link("//tmp/trash", "//trash", ignore_existing=True)

    if configure_pool_trees:
        client.create("scheduler_pool_tree", ignore_existing=True, attributes={
            "name": "physical",
            "config": {"nodes_filter": "internal"}
        })
        client.create("scheduler_pool", ignore_existing=True, attributes={
            "name": "research",
            "pool_tree": "physical",
            "forbid_immediate_operations": True
        })
        client.create("scheduler_pool", ignore_existing=True, attributes={
            "name": "transfer_manager",
            "pool_tree": "physical"
        })
        client.set("//sys/pool_trees/@default_tree", "physical")
        client.link("//sys/pool_trees/physical", "//sys/pools", force=True)
        client.set("//sys/pool_trees/physical/@config/default_parent_pool", "research")


def _initialize_world(client, environment, yt_config):
    cluster_connection = environment.configs["driver"]

    initialize_world(
        client,
        proxy_address=None,
        configure_pool_trees=False,
        is_multicell=yt_config.secondary_cell_count > 0)

    tablet_cell_attributes = {
        "changelog_replication_factor": 1,
        "changelog_read_quorum": 1,
        "changelog_write_quorum": 1,
        "changelog_account": "sys",
        "snapshot_account": "sys"
    }

    if not client.get("//sys/tablet_cell_bundles/default/@tablet_cell_ids"):
        client.set("//sys/tablet_cell_bundles/default/@options", tablet_cell_attributes)

    tablet_cells = client.get("//sys/tablet_cells")
    if not tablet_cells:
        tablet_cell_id = client.create("tablet_cell")
    else:
        tablet_cell_id = tablet_cells.keys()[0]

    if yt_config.wait_tablet_cell_initialization or yt_config.init_operations_archive:
        logger.info("Waiting for tablet cells to become ready...")
        while client.get("//sys/tablet_cells/{0}/@health".format(tablet_cell_id)) != "good":
            time.sleep(0.1)
        logger.info("Tablet cells are ready")

    if yt_config.init_operations_archive:
        import yt.environment.init_operation_archive as yt_env_init_operation_archive
        yt_env_init_operation_archive.create_tables_latest_version(client)

    # Used to automatically determine local mode from python wrapper.
    client.set("//sys/@local_mode_fqdn", get_fqdn())

    # Cluster connection and clusters.
    client.set("//sys/@cluster_connection", cluster_connection)
    client.set("//sys/@cluster_name", environment.id)
    client.set("//sys/clusters", {environment.id: cluster_connection})

    # Tablet limits for tmp account.
    client.set("//sys/accounts/tmp/@resource_limits/tablet_count", 1000)
    client.set("//sys/accounts/tmp/@resource_limits/tablet_static_memory", 5 * 1024 ** 3)

    if yt_config.wait_tablet_cell_initialization:
        client.create("map_node", "//sys/queue_agents", ignore_existing=True)
        create_queue_agent_state_tables(client, create_registration_table=True)


def main():
    parser = argparse.ArgumentParser(description="new YT cluster init script")
    parser.add_argument("--idm", action="store_true", dest="idm", default=False,
                        help="Use IDM system with this cluster")
    args = parser.parse_args()

    proxy_address = None
    ui_address = None
    if yt.config["proxy"]["url"]:
        suffix = yt.config["proxy"]["default_suffix"]
        proxy_short_address = yt.config["proxy"]["url"]
        if proxy_short_address.endswith(suffix):
            proxy_short_address = proxy_short_address[:-len(suffix)]
        if all(ch in string.ascii_letters + string.digits for ch in proxy_short_address):
            proxy_address = proxy_short_address + suffix
            ui_address = UI_ADDRESS_PATTERN.format(cluster_name=proxy_short_address)

    initialize_world(idm=args.idm, proxy_address=proxy_address, ui_address=ui_address)

if __name__ == "__main__":
    main()
