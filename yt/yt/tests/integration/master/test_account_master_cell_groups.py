from yt_env_setup import YTEnvSetup, Restarter, MASTERS_SERVICE

from yt_commands import (
    authors, build_snapshot, create, create_account, create_master_cell_group, exists, get,
    get_driver, ls, master_memory_sleep, raises_yt_error, remove, remove_account, set, transfer_account_resources,
    wait, write_table,
)

import pytest

################################################################################


class AccountMasterCellGroupsBase(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SECONDARY_MASTER_CELLS = 2

    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["cypress_node_host"]},
        "11": {"roles": ["chunk_host"]},
        "12": {"roles": ["chunk_host"]},
    }

    @staticmethod
    def _limits(account):
        return "//sys/accounts/{}/@resource_limits".format(account)

    @staticmethod
    def _usage(group, driver=None):
        return get("//sys/accounts/a/@resource_usage/per_group/{}".format(group), driver=driver)

    def _check_node_count_limit(self, path, group, cell_tag, overall_limit, group_limit):
        create_master_cell_group(group, [cell_tag])
        create_account("a")
        set(self._limits("a") + "/node_count", overall_limit)
        set(self._limits("a") + "/per_group", {group: {"node_count": group_limit}})

        create("map_node", path + "/used", attributes={"account": "a"})
        assert get(path + "/used/@native_cell_tag") == cell_tag
        wait(lambda: self._usage(group)["node_count"] == 1)

        with raises_yt_error("over Cypress node count limit"):
            create("map_node", path + "/blocked", attributes={"account": "a"})
        assert not exists(path + "/blocked")


class TestAccountMasterCellGroups(AccountMasterCellGroupsBase):
    ENABLE_MULTIDAEMON = False

    @authors("evanevannnn")
    def test_partial_limits_and_group_identity(self):
        """Validate defaults for omitted limits and group identity across renames."""
        create_master_cell_group("primary", [10])
        create_account("a")
        set(self._limits("a") + "/per_group", {
            "primary": {"node_count": 5, "disk_space_per_medium": {"default": 0}},
        })

        limits = get(self._limits("a") + "/per_group/primary")
        assert limits["node_count"] == 5
        assert "chunk_count" not in limits
        assert "tablet_count" not in limits
        assert limits["disk_space_per_medium"] == {"default": 0}

        group_id = get("//sys/master_cell_groups/primary/@id")
        set("//sys/master_cell_groups/primary/@name", "renamed")
        assert get("//sys/master_cell_groups/renamed/@id") == group_id
        assert get(self._limits("a") + "/per_group/renamed") == limits

        with raises_yt_error("used by account"):
            remove("//sys/master_cell_groups/renamed")

        remove(self._limits("a") + "/per_group")
        remove("//sys/master_cell_groups/renamed")
        assert ls("//sys/master_cell_groups") == ["universum"]

    @authors("evanevannnn")
    def test_unknown_group_and_duplicate_universum_limit(self):
        """Validate that unknown groups and a per-group entry for the universum group are rejected."""
        create_account("a")

        for name, message in [("missing", "No such master cell group"), ("universum", "top level")]:
            with raises_yt_error(message):
                set(self._limits("a") + "/per_group", {name: {"node_count": 1}})
            with raises_yt_error(message):
                create_account("invalid", attributes={
                    "resource_limits": {"per_group": {name: {"node_count": 1}}},
                })
            assert not exists("//sys/accounts/invalid")

    @authors("evanevannnn")
    @pytest.mark.parametrize("overall_limit, group_limit", [(1, 100), (100, 1)])
    def test_primary_node_count(self, overall_limit, group_limit):
        """Validate overall and group node-count limits on the primary cell."""
        self._check_node_count_limit("//tmp", "primary", 10, overall_limit, group_limit)

    @authors("evanevannnn")
    @pytest.mark.parametrize("cell_tag, resource, error", [
        (10, "node_count", "over Cypress node count limit"),
        (11, "chunk_count", "over chunk count limit"),
    ])
    def test_group_creation_and_removal_with_usage(self, cell_tag, resource, error):
        """Validate accounting and limit enforcement across group lifecycle and config changes."""
        create_account("a")
        driver = None if cell_tag == 10 else get_driver(1)
        if cell_tag != 10:
            create("table", "//tmp/t", attributes={"account": "a", "external_cell_tag": cell_tag})

        def consume_resource(index):
            if cell_tag == 10:
                create("map_node", "//tmp/n{}".format(index), attributes={"account": "a"})
            else:
                write_table("<append=true>//tmp/t", [{"x": index}])

        consume_resource(0)
        wait(lambda: get("//sys/accounts/a/@resource_usage/" + resource) == 1)

        create_master_cell_group("group", [cell_tag])
        set(self._limits("a") + "/per_group", {"group": {resource: 2}})
        wait(lambda: self._usage("group", driver)[resource] == 1)

        consume_resource(1)
        wait(lambda: self._usage("group")[resource] == 2)
        wait(lambda: self._usage("group", driver)[resource] == 2)

        set("//sys/@config/multicell_manager/max_master_cell_group_count", 2)
        wait(lambda: (
            get("//sys/@config/multicell_manager/max_master_cell_group_count", driver=driver) == 2
        ))
        assert self._usage("group", driver)[resource] == 2

        with raises_yt_error(error):
            consume_resource(2)

        remove(self._limits("a") + "/per_group")
        remove("//sys/master_cell_groups/group")
        wait(lambda: not exists("//sys/master_cell_groups/group", driver=driver))

        consume_resource(2)
        wait(lambda: get("//sys/accounts/a/@resource_usage/" + resource) == 3)
        assert not exists("//sys/accounts/a/@resource_usage/per_group")

        create_master_cell_group("group", [cell_tag])
        set(self._limits("a") + "/per_group", {"group": {resource: 3}})
        wait(lambda: self._usage("group")[resource] == 3)
        wait(lambda: self._usage("group", driver)[resource] == 3)

        with raises_yt_error(error):
            consume_resource(3)

    @authors("evanevannnn")
    @pytest.mark.parametrize("only_updates", [False, True])
    def test_group_gossip_and_membership_change(self, only_updates):
        """Validate group usage gossip and accounting after cell membership changes."""
        set("//sys/@config/security_manager/send_only_updates_in_account_gossip", only_updates)
        create_master_cell_group("data", [11, 12])
        create_master_cell_group("first", [11])
        create_account("a")
        set(self._limits("a") + "/per_group", {"data": {"chunk_count": 1}})

        for tag in [11, 12]:
            create("table", "//tmp/t{}".format(tag), attributes={
                "account": "a", "external_cell_tag": tag,
            })
        write_table("//tmp/t11", [{"x": 1}])
        wait(lambda: self._usage("data", get_driver(2))["chunk_count"] == 1)
        assert self._usage("data")["chunk_count"] == 1
        assert self._usage("first")["chunk_count"] == 1

        with raises_yt_error("over chunk count limit"):
            write_table("//tmp/t12", [{"x": 2}])

        set("//sys/master_cell_groups/data/@cell_tags", [12])
        wait(lambda: self._usage("data", get_driver(2))["chunk_count"] == 0)
        assert self._usage("data")["chunk_count"] == 0
        write_table("//tmp/t12", [{"x": 2}])
        wait(lambda: self._usage("data")["chunk_count"] == 1)
        wait(lambda: get("//sys/accounts/a/@resource_usage/chunk_count") == 2)

        remove("//tmp/t11")
        wait(lambda: self._usage("first")["chunk_count"] == 0)
        assert self._usage("data")["chunk_count"] == 1
        remove("//tmp/t12")
        wait(lambda: self._usage("data")["chunk_count"] == 0)
        wait(lambda: get("//sys/accounts/a/@resource_usage/chunk_count") == 0)

    @authors("evanevannnn")
    def test_violated_limits(self):
        """Validate group violations in direct and recursive resource-limit attributes."""
        create_master_cell_group("primary", [10])
        create_account("a")
        create("map_node", "//tmp/used", attributes={"account": "a"})
        set(self._limits("a") + "/per_group", {"primary": {"node_count": 0}})

        violated = get("//sys/accounts/a/@violated_resource_limits")
        assert not violated["node_count"]
        assert violated["per_group"]["primary"]["node_count"]
        assert get(
            "//sys/accounts/root/@recursive_violated_resource_limits/per_group/primary/node_count"
        ) == 1

    @authors("evanevannnn")
    def test_group_limit_transfer(self):
        """Validate group limit transfers and rollback when a transfer fails."""
        create_master_cell_group("primary", [10])
        for name, count in [("a", 10), ("b", 0)]:
            create_account(name)
            set(self._limits(name) + "/per_group", {"primary": {"node_count": count}})

        transfer_account_resources("a", "b", {"per_group": {"primary": {"node_count": 4}}})
        assert get(self._limits("a") + "/per_group/primary/node_count") == 6
        assert get(self._limits("b") + "/per_group/primary/node_count") == 4

        with raises_yt_error("Failed to transfer resources"):
            transfer_account_resources("a", "b", {"per_group": {"primary": {"node_count": 7}}})
        assert get(self._limits("a") + "/per_group/primary/node_count") == 6
        assert get(self._limits("b") + "/per_group/primary/node_count") == 4

    @authors("evanevannnn")
    def test_group_limits_in_account_hierarchy(self):
        """Validate that child group limits cannot exceed their parent's limits."""
        create_master_cell_group("primary", [10])
        create_account("a")
        create_account("child", parent_name="a", empty=True)
        set(self._limits("child") + "/per_group", {"primary": {"node_count": 3}})
        set(self._limits("a") + "/per_group", {"primary": {"node_count": 5}})

        with raises_yt_error("cannot be above that of its parent"):
            set(self._limits("child") + "/per_group/primary/node_count", 6)

        with raises_yt_error("cannot be below that of its child"):
            set(self._limits("a") + "/per_group/primary/node_count", 2)

        assert get(self._limits("child") + "/per_group/primary/node_count") == 3
        assert get(self._limits("a") + "/per_group/primary/node_count") == 5

    @authors("evanevannnn")
    @pytest.mark.parametrize("group", [None, "group"])
    def test_total_children_per_cell_master_memory_limits(self, group):
        """Validate that child per-cell limits sum only explicitly configured entries."""
        suffix = "/master_memory/per_cell"
        if group is not None:
            create_master_cell_group(group, [10, 11, 12])
            suffix = "/per_group/{}".format(group) + suffix

        create_account("parent")
        set("//sys/accounts/parent/@allow_children_limit_overcommit", True)
        limits = {"master_memory": {"per_cell": {"10": 100, "11": 0}}}
        create_account("first", parent_name="parent", attributes={
            "resource_limits": limits if group is None else {"per_group": {group: limits}},
        })

        total_limits = "//sys/accounts/parent/@total_children_resource_limits" + suffix
        assert get(total_limits) == {"10": 100}

        create_account("second", parent_name="parent", empty=True)
        assert get(total_limits) == {"10": 100}

        limits = {"master_memory": {"per_cell": {"10": 200, "12": 50}}}
        set(self._limits("second"), limits if group is None else {"per_group": {group: limits}})
        assert get(total_limits) == {"10": 300, "12": 50}
        assert get(self._limits("first") + suffix) == {"10": 100, "11": 0}

    @authors("evanevannnn")
    @pytest.mark.parametrize("allow_overcommit", [False, True])
    def test_attach_child_to_parent_with_group_limit(self, allow_overcommit):
        """Validate explicit group limits during child creation and attachment."""
        create_master_cell_group("primary", [10])
        create_account("a")
        set("//sys/accounts/a/@allow_children_limit_overcommit", allow_overcommit)
        set(self._limits("a") + "/per_group", {"primary": {"node_count": 5}})

        create_account("child", parent_name="a", empty=True)
        assert get("//sys/accounts/child/@parent_name") == "a"
        assert not exists(self._limits("child") + "/per_group")
        remove_account("child")

        with raises_yt_error("cannot be above that of its parent"):
            create_account("child", parent_name="a", empty=True, attributes={
                "resource_limits": {"per_group": {"primary": {"node_count": 6}}},
            })
        assert not exists("//sys/accounts/child")

        attributes = {"resource_limits": {"per_group": {"primary": {"node_count": 3}}}}
        with raises_yt_error("cannot be negative"):
            create_account("child", parent_name="a", attributes={
                **attributes, "merge_job_rate_limit": -1,
            })
        assert not exists("//sys/accounts/child")
        assert ls("//sys/account_tree/a") == []

        create_account("child", parent_name="a", attributes=attributes)
        assert get("//sys/accounts/child/@parent_name") == "a"
        assert get(self._limits("child") + "/per_group/primary/node_count") == 3
        for driver in [get_driver(1), get_driver(2)]:
            wait(lambda: (
                exists("//sys/accounts/child", driver=driver)
                and get(self._limits("child") + "/per_group/primary/node_count", driver=driver) == 3
            ))

        if allow_overcommit:
            create_account("sibling", parent_name="a", attributes=attributes)
        else:
            with raises_yt_error("the change would overcommit its parent"):
                create_account("sibling", parent_name="a", attributes=attributes)
            assert not exists("//sys/accounts/sibling")

        set("//sys/accounts/child/@parent_name", "root")
        remove(self._limits("child") + "/per_group")
        assert not exists(self._limits("child") + "/per_group")

        set("//sys/accounts/child/@parent_name", "a")
        assert get("//sys/accounts/child/@parent_name") == "a"

        set("//sys/accounts/child/@parent_name", "root")
        set(self._limits("child") + "/per_group", {"primary": {"node_count": 3}})
        set("//sys/accounts/child/@parent_name", "a")
        assert get("//sys/accounts/child/@parent_name") == "a"
        assert get(self._limits("child") + "/per_group/primary/node_count") == 3
        assert get(self._limits("a") + "/per_group/primary/node_count") == 5

    def _create_child_with_remote_memory(self):
        create_account("parent")
        create_account("child")

        create("table", "//tmp/t", attributes={"account": "child", "external_cell_tag": 11})
        write_table("//tmp/t", [{"x": 1}])

        master_memory_sleep()
        wait(lambda: get("//sys/accounts/child/@resource_usage/master_memory/chunk_host") > 0)

    def _set_attachment_memory_limits(self, group, memory_limits):
        for account in ["child", "parent"]:
            limits = get(self._limits(account))
            limits.setdefault("per_group", {})[group] = {"master_memory": memory_limits}
            set(self._limits(account), limits)

    @authors("evanevannnn")
    @pytest.mark.parametrize("memory_limits, error", [
        ({"chunk_host": 0}, "over chunk host master memory limit"),
        ({"per_cell": {"11": 0}}, "over cell 11 master memory limit"),
        ({"total": 0}, "over master memory limit"),
    ])
    def test_attach_checks_remote_master_memory(self, memory_limits, error):
        """Validate remote master-memory usage against parent group limits during attachment."""
        create_master_cell_group("data", [11, 12])
        self._create_child_with_remote_memory()
        self._set_attachment_memory_limits("data", memory_limits)
        set("//sys/@config/security_manager/enable_master_memory_usage_validation", True)

        with raises_yt_error(error):
            set("//sys/accounts/child/@parent_name", "parent")
        assert get("//sys/accounts/child/@parent_name") == "root"

    @authors("evanevannnn")
    def test_attach_ignores_master_memory_outside_group(self):
        """Validate that attachment ignores memory usage outside the relevant cell group."""
        create_master_cell_group("data", [11, 12])
        create_master_cell_group("primary", [10])
        self._create_child_with_remote_memory()
        assert get("//sys/accounts/child/@resource_usage/master_memory/per_cell/10") > 0

        self._set_attachment_memory_limits("data", {"per_cell": {"10": 0}})
        self._set_attachment_memory_limits("primary", {"chunk_host": 0})
        set("//sys/@config/security_manager/enable_master_memory_usage_validation", True)

        set("//sys/accounts/child/@parent_name", "parent")
        assert get("//sys/accounts/child/@parent_name") == "parent"

    @authors("evanevannnn")
    def test_snapshot(self):
        """Validate group identity, limits, usage, and enforcement after snapshot recovery."""
        create_master_cell_group("primary", [10])
        create_account("a")
        set(self._limits("a") + "/per_group", {"primary": {"node_count": 1}})
        create("map_node", "//tmp/used", attributes={"account": "a"})
        limits = get(self._limits("a"))
        group_id = get("//sys/master_cell_groups/primary/@id")

        build_snapshot(cell_id=get("//sys/@cell_id"))
        with Restarter(self.Env, MASTERS_SERVICE):
            pass

        assert get(self._limits("a")) == limits
        assert get("//sys/master_cell_groups/primary/@id") == group_id
        assert self._usage("primary")["node_count"] == 1

        with raises_yt_error("over Cypress node count limit"):
            create("map_node", "//tmp/blocked", attributes={"account": "a"})


class TestAccountMasterCellGroupsPortals(AccountMasterCellGroupsBase):
    ENABLE_MULTIDAEMON = True

    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["cypress_node_host"]},
        "11": {"roles": ["cypress_node_host"]},
        "12": {"roles": ["chunk_host"]},
    }

    @authors("evanevannnn")
    @pytest.mark.parametrize("overall_limit, group_limit", [(1, 100), (100, 1)])
    def test_portal_node_count(self, overall_limit, group_limit):
        """Validate overall and group node-count limits behind a portal."""
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})
        self._check_node_count_limit("//tmp/p", "portal", 11, overall_limit, group_limit)

    @authors("evanevannnn")
    @pytest.mark.parametrize("same_group", [False, True])
    def test_external_table_is_charged_once(self, same_group):
        """Validate external table usage is charged once with shared or separate cell groups."""
        create("portal_entrance", "//tmp/p", attributes={"exit_cell_tag": 11})
        groups = {"combined": [11, 12]} if same_group else {"native": [11], "external": [12]}
        for name, tags in groups.items():
            create_master_cell_group(name, tags)

        create_account("a")
        set(self._limits("a") + "/per_group", {name: {"chunk_count": 1} for name in groups})

        create("table", "//tmp/p/t", attributes={"account": "a", "external_cell_tag": 12})
        assert get("//tmp/p/t/@native_cell_tag") == 11
        assert get("//tmp/p/t/@external_cell_tag") == 12

        write_table("//tmp/p/t", [{"x": 1}])

        charged_group = "combined" if same_group else "external"
        wait(lambda: self._usage(charged_group)["chunk_count"] == 1)
        # The external cell accounts for both the table node and its chunks.
        # Wait for the upload transaction's branched node to be removed as well.
        wait(lambda: self._usage(charged_group)["node_count"] == 1)
        if not same_group:
            assert self._usage("native")["node_count"] == 0
            assert self._usage("native")["chunk_count"] == 0

        usage = get("//sys/accounts/a/@resource_usage")
        assert usage["node_count"] == 1
        assert usage["chunk_count"] == 1

        with raises_yt_error("over chunk count limit"):
            write_table("<append=true>//tmp/p/t", [{"x": 2}])


class TestAccountMasterCellGroupsSequoia(AccountMasterCellGroupsBase):
    ENABLE_MULTIDAEMON = True
    USE_SEQUOIA = True
    ENABLE_CYPRESS_TRANSACTIONS_IN_SEQUOIA = True
    ENABLE_TMP_ROOTSTOCK = True

    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["sequoia_node_host"]},
        "11": {"roles": ["chunk_host"]},
        "12": {"roles": ["chunk_host"]},
    }

    DELTA_CYPRESS_PROXY_CONFIG = {
        "testing": {
            "enable_ground_update_queues_sync": True,
            "enable_user_directory_per_request_sync": True,
        },
    }

    @authors("evanevannnn")
    @pytest.mark.parametrize("overall_limit, group_limit", [(1, 100), (100, 1)])
    def test_sequoia_node_count(self, overall_limit, group_limit):
        """Validate overall and group node-count limits for Sequoia nodes."""
        self._check_node_count_limit("//tmp", "sequoia", 10, overall_limit, group_limit)
