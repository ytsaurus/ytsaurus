#!/usr/bin/env python

from .init_queue_agent_state import create_tables as create_queue_agent_state_tables

from yt.wrapper.constants import UI_ADDRESS_PATTERN

from yt.test_helpers import wait

from yt.common import get_fqdn, get_value

import yt.wrapper as yt
import yt.logger as logger

import argparse
import copy
import string


class RequestType(object):
    CreateSystemObject = 1
    AddMember = 2
    AddAclCheck = 3
    Other = 4


class BatchProcessor(object):
    def __init__(self, client):
        self._batch_client = client.create_batch_client()
        self._requests = []

    def has_requests(self):
        return len(self._requests) > 0

    def execute(self):
        if not self._requests:
            return

        logger.debug("Waiting batched requests (Count: %d)", len(self._requests))

        add_acl_requests = []

        self._batch_client.commit_batch()

        for request_type, params, rsp in self._requests:
            if rsp.is_ok():
                if request_type == RequestType.AddAclCheck:
                    new_acl = params["new_acl"]
                    path = params["path"]
                    current_acls = rsp.get_result()
                    if self.need_to_add_new_acl(new_acl, current_acls):
                        add_acl_requests.append((path, new_acl))
                    else:
                        logger.warning("ACL '{new_acl}' is already present in {path}/@acl".format(new_acl=new_acl, path=path))
                continue

            error = yt.YtResponseError(rsp.get_error())

            if request_type == RequestType.CreateSystemObject and error.is_already_exists():
                logger.warning("'%s' already exists", params["name"])
            elif request_type == RequestType.CreateSystemObject and error.contains_text("Error parsing EObjectType") and error.contains_text("medium"):
                pass
            elif request_type == RequestType.AddMember and error.is_already_present_in_group():
                logger.warning("'{subject}' is already present in group '{group}'".format(**params))
            else:
                raise error

        self._requests = []

        logger.debug("Batch successfully executed")

        for path, new_acl in add_acl_requests:
            self.set(path + "/@acl/end", new_acl)

    def create_system_object(self, type_, name, attributes=None, ignore_existing=None):
        request_attributes = copy.deepcopy(get_value(attributes, {}))
        request_attributes["name"] = name
        create_rsp = self._batch_client.create(type_, attributes=request_attributes, ignore_existing=ignore_existing)
        self._requests.append((RequestType.CreateSystemObject, dict(name=name), create_rsp))

    def create(self, *args, **kwargs):
        create_rsp = self._batch_client.create(*args, **kwargs)
        self._requests.append((RequestType.Other, kwargs, create_rsp))
        return create_rsp

    def set(self, *args, **kwargs):
        set_rsp = self._batch_client.set(*args, **kwargs)
        self._requests.append((RequestType.Other, kwargs, set_rsp))

    def link(self, *args, **kwargs):
        link_rsp = self._batch_client.link(*args, **kwargs)
        self._requests.append((RequestType.Other, kwargs, link_rsp))

    def get(self, *args, **kwargs):
        get_rsp = self._batch_client.get(*args, **kwargs)
        self._requests.append((RequestType.Other, kwargs, get_rsp))
        return get_rsp

    def add_member(self, subject, group):
        add_member_rsp = self._batch_client.add_member(subject, group)
        self._requests.append((RequestType.AddMember, dict(subject=subject, group=group), add_member_rsp))

    def check_acl(self, acl, required_keys, optional_keys):
        for k in required_keys:
            if k not in acl:
                logger.warning("Can't find required key '%s' in ACL: %s", k, acl)
                return False
        for k in acl:
            if k not in optional_keys and k not in required_keys:
                logger.warning("Found unknown key '%s' in ACL: %s", k, acl)
                return False
        return True

    def need_to_add_new_acl(self, new_acl, current_acls):
        required_keys = ["subjects", "permissions", "action"]
        optional_keys = ["inheritance_mode"]

        if not self.check_acl(new_acl, required_keys, optional_keys):
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

    def add_acl(self, path, new_acl):
        get_rsp = self._batch_client.get(path + "/@acl")
        self._requests.append((RequestType.AddAclCheck, dict(path=path, new_acl=new_acl), get_rsp))

    def create_account(self, name, attributes, ignore_existing=None):
        GB = 1024 ** 3
        if "resource_limits" not in attributes:
            attributes["resource_limits"] = {}

        if "master_memory" not in attributes["resource_limits"]:
            attributes["resource_limits"]["master_memory"] = {"total": 100 * GB, "chunk_host": 100 * GB}

        self.create_system_object("account", name, attributes, ignore_existing=ignore_existing)

    def __del__(self):
        assert len(self._requests) == 0


def get_default_resource_limits():
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


def initialize_world(client=None, idm=None, proxy_address=None, ui_address=None, configure_pool_trees=True, is_multicell=False):
    logger.info("World initialization started")

    client = get_value(client, yt)
    batch_processor = BatchProcessor(client)

    account_to_wait_life_stage = []

    users = ["robot-yt-mon", "robot-yt-idm", "robot-yt-hermes"]
    groups = ["devs", "admins", "admin_snapshots"]
    if idm:
        groups.append("yandex")
    everyone_group = "users" if not idm else "yandex"

    for user in users:
        batch_processor.create_system_object("user", user)
    for group in groups:
        batch_processor.create_system_object("group", group)

    users_use_acl = [{
        "action": "allow",
        "subjects": ["users"],
        "permissions": ["use"]
    }]
    for account, acl in (("default", users_use_acl), ("tmp_files", users_use_acl), ("tmp_jobs", [])):
        batch_processor.create_account(
            account,
            ignore_existing=True,
            attributes={
                "acl": acl,
                "resource_limits": get_default_resource_limits(),
            })
        if is_multicell:
            account_to_wait_life_stage.append("tmp_files")

    for medium in ("default", "ssd_journals"):
        batch_processor.create_system_object("domestic_medium", medium)
        # COMPAT(babenko)
        batch_processor.create_system_object("medium", medium)

    if configure_pool_trees:
        batch_processor.create(
            "scheduler_pool_tree",
            ignore_existing=True,
            attributes={
                "name": "physical",
                "config": {"nodes_filter": "internal", "default_parent_pool": "research"}
            })

    batch_processor.create("map_node", "//tmp/trash", ignore_existing=True)

    batch_processor.create("map_node", "//sys/cron", ignore_existing=True)
    for dir in ["//sys", "//tmp", "//sys/tokens"]:
        batch_processor.set(dir + "/@opaque", "true")

    batch_processor.create("map_node", "//sys/admin/snapshots", recursive=True, ignore_existing=True)
    batch_processor.create("map_node", "//sys/admin/odin", recursive=True, ignore_existing=True)
    batch_processor.create("map_node", "//sys/admin/lock/autorestart/nodes/disabled", recursive=True, ignore_existing=True)

    batch_processor.create(
        "map_node",
        "//tmp/yt_wrapper/table_storage",
        recursive=True,
        ignore_existing=True)
    batch_processor.create(
        "map_node",
        "//tmp/yt_regular/table_storage",
        recursive=True,
        ignore_existing=True)

    batch_processor.execute()

    batch_processor.add_member("devs", "admins")
    batch_processor.add_member("robot-yt-mon", "admin_snapshots")
    batch_processor.add_member("robot-yt-idm", "superusers")

    batch_processor.add_acl("/", {"action": "allow", "subjects": [everyone_group], "permissions": ["read"]})
    batch_processor.add_acl("/", {"action": "allow", "subjects": ["admins"], "permissions": ["write", "remove", "administer", "mount"]})

    batch_processor.add_acl("//sys", {"action": "allow", "subjects": ["users"], "permissions": ["read"]})
    batch_processor.add_acl("//sys", {"action": "allow", "subjects": ["admins"], "permissions": ["write", "remove", "administer", "mount"]})
    batch_processor.set("//sys/@inherit_acl", "false")

    batch_processor.add_acl("//sys/accounts/sys", {"action": "allow", "subjects": ["root", "admins"], "permissions": ["use"]})

    batch_processor.add_acl("//sys/tokens", {"action": "allow", "subjects": ["admins"], "permissions": ["read", "write", "remove"]})
    batch_processor.add_acl("//sys/tablet_cells", {"action": "allow", "subjects": ["admins"], "permissions": ["read", "write", "remove", "administer"]})
    batch_processor.set("//sys/tokens/@inherit_acl", "false")
    batch_processor.set("//sys/tablet_cells/@inherit_acl", "false")

    batch_processor.create(
        "map_node",
        "//home",
        attributes={"opaque": True, "account": "default"},
        ignore_existing=True)

    batch_processor.create(
        "map_node",
        "//tmp/yt_wrapper/file_storage",
        attributes={"account": "tmp_files"},
        recursive=True,
        ignore_existing=True)

    batch_processor.set("//sys/admin/@inherit_acl", "false")
    batch_processor.set(
        "//sys/admin/@acl",
        [
            {"action": "allow", "subjects": ["admins"], "permissions": ["write", "remove", "read", "mount", "administer"]}
        ])

    batch_processor.set(
        "//sys/admin/snapshots/@acl",
        [
            {"action": "allow", "subjects": ["admin_snapshots"], "permissions": ["read"]}
        ])

    # add_acl to schemas
    for schema in ["user", "group", "tablet_cell", "tablet_cell_bundle"]:
        batch_processor.set(
            "//sys/schemas/{}/@acl".format(schema),
            [
                {"action": "allow", "subjects": ["users"], "permissions": ["read"]},
                {"action": "allow", "subjects": ["admins"], "permissions": ["write", "remove", "create"]}
            ])

    batch_processor.set(
        "//sys/schemas/account/@acl",
        [
            {"action": "allow", "subjects": ["users"], "permissions": ["read"]},
            {
                "action": "allow",
                "subjects": ["admins"],
                "permissions": ["write", "remove", "create", "administer", "use"],
            }
        ])

    for schema in ["rack", "cluster_node", "data_center"]:
        batch_processor.set(
            "//sys/schemas/%s/@acl" % schema,
            [
                {"action": "allow", "subjects": ["users"], "permissions": ["read"]},
                {
                    "action": "allow",
                    "subjects": ["admins"],
                    "permissions": ["write", "remove", "create", "administer"],
                }
            ])
    batch_processor.set(
        "//sys/schemas/lock/@acl",
        [
            {"action": "allow", "subjects": ["users"], "permissions": ["read"]},
        ])

    batch_processor.set(
        "//sys/schemas/transaction/@acl",
        [
            {"action": "allow", "subjects": ["users"], "permissions": ["read"]},
            {"action": "allow", "subjects": ["users"], "permissions": ["write", "create"]}
        ])

    yamr_table_schema = [{"name": name, "type": "any", "sort_order": "ascending"}
                         for name in ["key", "subkey"]] + [{"name": "value", "type": "any"}]
    batch_processor.create("table", "//sys/empty_yamr_table", attributes={"schema": yamr_table_schema}, ignore_existing=True)

    batch_processor.set(
        "//sys/schemas/tablet_cell_bundle/@options",
        [{
            "snapshot_replication_factor": 5,
            "snapshot_primary_medium": "default",
            "changelog_write_quorum": 3,
            "changelog_replication_factor": 5,
            "changelog_read_quorum": 3,
            "changelog_primary_medium": "ssd_journals"
        }])
    batch_processor.set("//sys/schemas/tablet_cell_bundle/@enable_bundle_balancer", False)

    batch_processor.add_acl("//tmp", {"action": "allow", "subjects": [everyone_group], "permissions": ["write", "remove", "read"]})

    batch_processor.set(
        "//tmp/yt_regular/@acl",
        [{
            "action": "allow",
            "subjects": ["admins"],
            "permissions": ["read", "write", "remove", "administer"]
        }])
    batch_processor.set("//tmp/yt_regular/@inherit_acl", False)

    batch_processor.create_system_object(
        "tablet_cell_bundle",
        "sys",
        {"options": {"changelog_account": "sys", "snapshot_account": "sys"}})

    if proxy_address is not None:
        batch_processor.set("//sys/@cluster_proxy_address", proxy_address)
    if ui_address is not None:
        batch_processor.set("//sys/@cluster_ui_address", ui_address)
    batch_processor.set("//sys/@ui_config", {"web_json_value_format": "yql"})

    batch_processor.set(
        "//tmp/trash/@acl",
        [
            {"action": "deny", "subjects": ["everyone"], "permissions": ["remove"], "inheritance_mode": "object_only"}
        ])

    batch_processor.link("//tmp/trash", "//trash", ignore_existing=True)

    if configure_pool_trees:
        batch_processor.create(
            "scheduler_pool",
            ignore_existing=True,
            attributes={
                "name": "research",
                "pool_tree": "physical",
                "forbid_immediate_operations": True
            })
        batch_processor.create(
            "scheduler_pool",
            ignore_existing=True,
            attributes={
                "name": "transfer_manager",
                "pool_tree": "physical"
            })
        batch_processor.set("//sys/pool_trees/@default_tree", "physical")
        batch_processor.link("//sys/pool_trees/physical", "//sys/pools", force=True)

    if account_to_wait_life_stage:
        while True:
            rsps = []
            for account in account_to_wait_life_stage:
                rsps.append(batch_processor.get("//sys/accounts/tmp_files/@life_stage"))
            batch_processor.execute()

            get_values = []
            for rsp in rsps:
                assert rsp.is_ok()
                get_values.append(rsp.get_value())
            if all(value == "creation_committed" for value in get_values):
                break

    while batch_processor.has_requests():
        batch_processor.execute()

    logger.info("World initialization completed")


def _initialize_world_for_local_cluster(client, environment, yt_config):
    logger.info("World initialization for local cluster started")

    cluster_connection = environment.configs["driver"]

    initialize_world(
        client,
        proxy_address=None,
        configure_pool_trees=False,
        is_multicell=yt_config.secondary_cell_count > 0)

    batch_processor = BatchProcessor(client)

    # Used to automatically determine local mode from python wrapper.
    batch_processor.set("//sys/@local_mode_fqdn", get_fqdn())

    # Cluster connection and clusters.
    batch_processor.set("//sys/@cluster_connection", cluster_connection)
    batch_processor.set("//sys/@cluster_name", environment.id)
    batch_processor.set("//sys/clusters", {environment.id: cluster_connection})

    # Tablet limits for tmp account.
    batch_processor.set("//sys/accounts/tmp/@resource_limits/tablet_count", 1000)
    batch_processor.set("//sys/accounts/tmp/@resource_limits/tablet_static_memory", 5 * 1024 ** 3)

    get_tablet_cell_ids_rsp = batch_processor.get("//sys/tablet_cell_bundles/default/@tablet_cell_ids")

    batch_processor.execute()

    tablet_cell_attributes = {
        "changelog_replication_factor": 1,
        "changelog_read_quorum": 1,
        "changelog_write_quorum": 1,
        "changelog_account": "sys",
        "snapshot_account": "sys"
    }

    tablet_cell_ids = get_tablet_cell_ids_rsp.get_result()
    if not tablet_cell_ids:
        batch_processor.set("//sys/tablet_cell_bundles/default/@options", tablet_cell_attributes)
        create_tablet_cell_id_rsp = batch_processor.create("tablet_cell")

        batch_processor.execute()

        tablet_cell_id = create_tablet_cell_id_rsp.get_result()
    else:
        tablet_cell_id = tablet_cell_ids.keys()[0]

    if yt_config.wait_tablet_cell_initialization or yt_config.init_operations_archive:
        logger.info("Waiting for tablet cells to become ready...")
        wait(lambda: client.get("//sys/tablet_cells/{0}/@health".format(tablet_cell_id)) == "good")
        logger.info("Tablet cells are ready")

    if yt_config.init_operations_archive:
        import yt.environment.init_operation_archive as yt_env_init_operation_archive
        yt_env_init_operation_archive.create_tables_latest_version(client)

    if yt_config.wait_tablet_cell_initialization:
        client.create("map_node", "//sys/queue_agents", ignore_existing=True)
        create_queue_agent_state_tables(client, create_registration_table=True)

    logger.info("World initialization for local cluster completed")


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
