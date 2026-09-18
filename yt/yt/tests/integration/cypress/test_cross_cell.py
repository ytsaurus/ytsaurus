from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, raises_yt_error, wait, create, ls, get, set, copy, move, remove, link, exists, create_account,
    create_user, make_ace, check_permission, start_transaction, abort_transaction, commit_transaction,
    read_file, write_file, read_table, write_table, sync_create_cells, create_dynamic_table, insert_rows,
    lookup_rows, sync_mount_table, sync_unmount_table, sync_freeze_table, abort_all_transactions, gc_collect)

import yt_error_codes

from yt_helpers import get_current_time, account_usage_all_zero

from yt.common import YtError
from yt.test_helpers import assert_items_equal

from datetime import timedelta
import pytest
import random

import time


################################################################################


class TestCrossCellCopy(YTEnvSetup):
    NUM_TEST_PARTITIONS = 3

    NUM_MASTERS = 3
    NUM_NODES = 3
    NUM_SECONDARY_MASTER_CELLS = 3
    USE_DYNAMIC_TABLES = True

    MASTER_CELL_DESCRIPTORS = {
        "11": {"roles": ["cypress_node_host", "chunk_host"]},
        "12": {"roles": ["cypress_node_host", "chunk_host"]},
        "13": {"roles": ["chunk_host"]},
    }

    FILE_PAYLOAD = b"FILE PAYLOAD SOME BYTES AND STUFF"
    TABLE_PAYLOAD = [{"key": 42, "value": "the answer"}]

    COMMAND = "copy"
    SRC = "//tmp/source"
    DST = "//tmp/destination"
    COPY_TO_SEQUOIA = False

    AVAILABLE_ACCOUNTS = [
        "george",
        "geoff",
        "geronimo",
        "golem",
    ]

    AVAILABLE_USERS = [
        "john",
        "jim",
        "jason",
        "jolem",
    ]

    PRESERVABLE_ATTRIBUTES = {
        "account": False,
        "creation_time": False,
        "modification_time": False,
        "expiration_time": False,
        "expiration_timeout": False,
        "owner": False,
        "acl": False,
    }

    # These attributes can change or be the same depending on the context.
    CONTEXT_DEPENDENT_ATTRIBUTES = [
        "access_counter",
        "account_id",
        "actual_tablet_state",
        "expected_tablet_state",
        "tablet_state",
        "unflushed_timestamp",

        # Revision is calculated independently for each cell.
        "attribute_revision",
        "content_revision",
        "native_content_revision",
        "revision",
    ]

    # These attributes have to change when cross-cell copying.
    EXPECTED_ATTRIBUTE_CHANGES = [
        "access_time",

        # Changed due to cell change:
        "cow_cookie",
        "id",
        "native_cell_tag",
        "parent_id",
        "schema_id",
        "shard_id",
    ]

    def setup_method(self, method):
        super(TestCrossCellCopy, self).setup_method(method)

        # Setting defaults for meta state.
        self.PRESERVABLE_ATTRIBUTES = {
            "account": False,
            "creation_time": False,
            "modification_time": False,
            "expiration_time": False,
            "expiration_timeout": False,
            "owner": False,
            "acl": False,
        }
        self.SRC_ATTRIBUTES = {}

        for account in self.AVAILABLE_ACCOUNTS:
            create_account(account, ignore_existing=True)
        for user in self.AVAILABLE_USERS:
            create_user(user, ignore_existing=True)

        if not self.USE_SEQUOIA:
            # Cypress -> Portal.
            create("map_node", self.SRC)
            create("portal_entrance", self.DST, attributes={"exit_cell_tag": 11})
        elif self.COPY_TO_SEQUOIA:
            # Cypress -> Sequoia.
            create("map_node", self.SRC)
            create("rootstock", self.DST)
        else:
            # Sequoia -> Cypress.
            create("rootstock", self.SRC)
            create("map_node", self.DST)

        # NB: this attribute is not supported in Sequoia yet.
        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("recursive_resource_usage")

    def teardown_method(self, method):
        abort_all_transactions()
        # XXX(babenko): cleanup is weird
        remove(self.DST)
        super(TestCrossCellCopy, self).teardown_method(method)

    def _fetch_extended_attributes(self, path, tx):
        attributes = get(f"{path}/@", tx=tx)
        if attributes["type"] == "table":
            attributes["schema"] = get(f"{path}/@schema", tx=tx)
        attributes.pop("sequoia_acl", None)
        return attributes

    def _validate_attribute_consistency_for_node(self, src_path, dst_path, tx):
        src_attributes = self._fetch_extended_attributes(src_path, tx=tx) if self.COMMAND == "copy" else self.SRC_ATTRIBUTES[src_path]
        dst_attributes = self._fetch_extended_attributes(dst_path, tx=tx)

        for attribute_key in src_attributes.keys():
            # Preservable attributes should be tested directly.
            if attribute_key in self.CONTEXT_DEPENDENT_ATTRIBUTES or attribute_key in self.PRESERVABLE_ATTRIBUTES.keys():
                # This if was added for move command, because pre-move we get data from under a tx, but post-move we look at it without a tx.
                # Maybe it can be avoided. Think about it.
                if attribute_key in dst_attributes:
                    del dst_attributes[attribute_key]
                continue

            src_attribute_value = src_attributes[attribute_key]
            dst_attribute_value = dst_attributes[attribute_key]

            assert (src_attribute_value != dst_attribute_value) == (attribute_key in self.EXPECTED_ATTRIBUTE_CHANGES)
            del dst_attributes[attribute_key]

        for attribute_key in dst_attributes.keys():
            assert attribute_key in self.CONTEXT_DEPENDENT_ATTRIBUTES

    def _validate_preservable_attributes(self, src_path, dst_path, tx):
        src_attributes = get(f"{src_path}/@", tx=tx) if self.COMMAND == "copy" else self.SRC_ATTRIBUTES[src_path]
        dst_attributes = get(f"{dst_path}/@", tx=tx)

        for attribute_key, equality_expected in self.PRESERVABLE_ATTRIBUTES.items():
            if equality_expected:
                if attribute_key not in src_attributes.keys():
                    assert attribute_key not in dst_attributes.keys()
                else:
                    assert src_attributes[attribute_key] == dst_attributes[attribute_key]
            elif attribute_key in dst_attributes.keys():
                assert src_attributes[attribute_key] != dst_attributes[attribute_key]

    def _populate_preservable_attributes(self, path, tx):
        set(f"{path}/@account", random.choice(self.AVAILABLE_ACCOUNTS))
        user = random.choice(self.AVAILABLE_USERS)
        set(f"{path}/@owner", user)
        set(f"{path}/@acl", [
            make_ace("allow", user, "read"),
            make_ace("allow", user, "write"),
            ])
        set(f"{path}/@expiration_time", f"{random.randint(2050, 2150)}-01-01T00:00:00.000000Z", tx=tx)
        set(f"{path}/@expiration_timeout", random.randint(2800000, 3200000), tx=tx)
        # Creation and modification difference is easy to spot without setting anything.

    def create_map_node(self, path, tx="0-0-0-0"):
        create(
            "map_node",
            path,
            attributes={"account": random.choice(self.AVAILABLE_ACCOUNTS)},
            tx=tx)

    def create_file(self, path, tx="0-0-0-0"):
        create(
            "file",
            path,
            attributes={
                "account": random.choice(self.AVAILABLE_ACCOUNTS),
                "external_cell_tag": 13
            },
            tx=tx)
        write_file(path, self.FILE_PAYLOAD, tx=tx)

    def create_table(self, path, tx="0-0-0-0"):
        create(
            "table",
            path,
            attributes={
                "external_cell_tag": 13,
                "optimize_for": "scan",
                "account": random.choice(self.AVAILABLE_ACCOUNTS),
                "schema": [
                    {"name": "key", "type": "int64"},
                    {"name": "value", "type": "string"}
                ],
            },
            tx=tx)
        write_table(path, self.TABLE_PAYLOAD, tx=tx)

    def create_document(self, path, tx="0-0-0-0"):
        create(
            "document",
            path,
            attributes={
                "account": random.choice(self.AVAILABLE_ACCOUNTS),
                "value": {"hello": "world", "greetings": "sun"}
            },
            tx=tx)

    def create_unmounted_table(self, path, tx="0-0-0-0"):
        create_dynamic_table(
            path,
            external_cell_tag=13,
            optimize_for="scan",
            account="tmp",  # This is the account with enough quota.
            schema=[
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ])
        sync_create_cells(1)
        sync_mount_table(path)
        insert_rows(path, self.TABLE_PAYLOAD)
        sync_unmount_table(path)

    def create_frozen_table(self, path, tx="0-0-0-0"):
        create_dynamic_table(
            path,
            external_cell_tag=13,
            optimize_for="scan",
            account="tmp",  # This is the account with enough quota.
            schema=[
                {"name": "key", "type": "int64", "sort_order": "ascending"},
                {"name": "value", "type": "string"},
            ])
        sync_create_cells(1)
        sync_mount_table(path)
        insert_rows(path, self.TABLE_PAYLOAD)
        sync_freeze_table(path)

    def create_subtree(self, path, tx="0-0-0-0"):
        # starting_path
        # |-- map_node
        # |   |-- table
        # |   |-- file
        # |   |-- document
        # |   `-- nested_map_node
        # |       `-- other_table
        # `-- top_level_table

        create("map_node", path, force=True, tx=tx)  # Ensure starting path is created and empty.

        self.create_map_node(f"{path}/map_node", tx=tx)
        self.create_table(f"{path}/map_node/table", tx=tx)
        self.create_file(f"{path}/map_node/file", tx=tx)
        self.create_document(f"{path}/map_node/document", tx=tx)
        self.create_map_node(f"{path}/map_node/nested_map_node", tx=tx)
        self.create_table(f"{path}/map_node/nested_map_node/other_table", tx=tx)
        self.create_table(f"{path}/top_level_table", tx=tx)

    def populate_preservable_attributes_subtree(self, path, tx):
        self._populate_preservable_attributes(path, tx=tx)
        self._populate_preservable_attributes(f"{path}/map_node", tx=tx)
        for node in ls(f"{path}/map_node", tx=tx):
            self._populate_preservable_attributes(f"{path}/map_node/{node}", tx=tx)
        self._populate_preservable_attributes(f"{path}/map_node/nested_map_node/other_table", tx=tx)
        self._populate_preservable_attributes(f"{path}/top_level_table", tx=tx)

    def _iterate_over_subtree_and_validate_attributes(
            self,
            src_path,
            dst_path,
            validation_function,
            tx="0-0-0-0"):
        paths_to_check = [[src_path, dst_path]]

        # This is copy + paste, maybe improve this later? But it requires a lot of python magic.
        while len(paths_to_check) > 0:
            current_src_path, current_dst_path = paths_to_check.pop(0)
            validation_function(current_src_path, current_dst_path, tx=tx)

            # Document supports "ls" command, but we don't actually want to check it's contents.
            if get(f"{current_dst_path}/@type", tx=tx) == "document":
                continue

            # Some other nodes don't support "ls" command.
            try:
                for child_node in ls(current_dst_path, tx=tx, verbose_error=False):
                    next_src_path = f"{current_src_path}/{child_node}"
                    next_dst_path = f"{current_dst_path}/{child_node}"
                    paths_to_check.append([next_src_path, next_dst_path])
            except YtError as err:
                if err.contains_code(yt_error_codes.NoSuchYPathMethod):
                    continue
                else:
                    raise err

    def validate_subtree_attribute_consistency(self, src_path, dst_path, tx="0-0-0-0"):
        self._iterate_over_subtree_and_validate_attributes(
            src_path,
            dst_path,
            self._validate_attribute_consistency_for_node,
            tx=tx)

    def validate_subtree_preservable_attribute_consistency(self, src_path, dst_path, tx):
        self._iterate_over_subtree_and_validate_attributes(
            src_path,
            dst_path,
            self._validate_preservable_attributes,
            tx=tx)

    def validate_copy_base(self, src_path, dst_path, tx="0-0-0-0"):
        if self.COMMAND == "copy":
            assert get(src_path, tx=tx) == get(dst_path, tx=tx)
        else:
            assert self.SRC_GET_RESULT == get(dst_path, tx=tx)

    def validate_map_node_copy(self, path, tx="0-0-0-0"):
        return

    def validate_file_copy(self, path, tx="0-0-0-0"):
        assert read_file(path, tx=tx) == self.FILE_PAYLOAD

    def validate_table_copy(self, path, tx="0-0-0-0"):
        assert read_table(path, tx=tx) == self.TABLE_PAYLOAD

    def populate_preservable_attributes_table(self, path, tx):
        self._populate_preservable_attributes(path, tx=tx)

    def validate_document_copy(self, path, tx="0-0-0-0"):
        assert get(path, tx=tx) == {"hello": "world", "greetings": "sun"}

    def validate_unmounted_table_copy(self, path, tx="0-0-0-0"):
        sync_mount_table(path)
        assert lookup_rows(path, [{"key": 42}]) == self.TABLE_PAYLOAD

    def validate_frozen_table_copy(self, path, tx="0-0-0-0"):
        sync_mount_table(path)
        assert lookup_rows(path, [{"key": 42}]) == self.TABLE_PAYLOAD

    def write_user_attribute(self, path, tx="0-0-0-0"):
        set(f"{path}/@my_personal_attribute", "is_here", tx=tx)

    def _preserve_src_state(self, src_path, tx):
        self.SRC_GET_RESULT = get(src_path, tx=tx)

        # This is needed to properly test move with symlink.
        try:
            resolved_path = get(f"{src_path}/@path")
        except YtError as err:
            if not err.is_resolve_error():
                raise
            resolved_path = src_path

        paths_to_check = [resolved_path]
        while len(paths_to_check) > 0:
            next_path = paths_to_check.pop(0)
            attributes = self._fetch_extended_attributes(next_path, tx=tx)
            self.SRC_ATTRIBUTES[next_path] = attributes

            # Document supports "ls" command, but we don't actually want to check it's contents with it.
            if attributes["type"] == "document":
                continue

            # Some other nodes don't support "ls" command.
            try:
                for child_node in ls(next_path, tx=tx, verbose_error=False):
                    # Done to ensure that test with special YPath symbols works.
                    if child_node.startswith("["):
                        child_node = f"\\{child_node}"

                    paths_to_check.append(f"{next_path}/{child_node}")
            except YtError as err:
                if err.contains_code(yt_error_codes.NoSuchYPathMethod):
                    continue
                else:
                    raise err

    def execute_command(self, src_path, dst_path, tx="0-0-0-0", **kwargs):
        if self.COMMAND == "move":
            self._preserve_src_state(src_path, tx=tx)
            move(src_path, dst_path, tx=tx, **kwargs)
        else:
            copy(src_path, dst_path, tx=tx, **kwargs)

    @authors("h0pless")
    def test_subtree_size_flag(self):
        if not self.COMMAND == "copy":
            pytest.skip()

        if self.USE_SEQUOIA and not self.COPY_TO_SEQUOIA:
            pytest.skip()

        src_path = f"{self.SRC}/dir"
        dst_path = f"{self.DST}/dir"
        set("//sys/@config/cypress_manager/cross_cell_copy_max_subtree_size", 1)
        self.create_subtree(src_path)

        with raises_yt_error("Subtree is too large for cross-cell copy"):
            copy(src_path, dst_path)

    @authors("h0pless")
    @pytest.mark.parametrize("node_type", ["map_node", "file", "table", "document", "unmounted_table", "frozen_table"])
    def test_single_node(self, node_type):
        if node_type == "frozen_table" and self.COMMAND == "move":
            pytest.skip()

        if self.USE_SEQUOIA:
            # TODO(h0pless): Sequoia doesn't quite work with dynamic tables just yet.
            if node_type == "frozen_table" or node_type == "unmounted_table":
                pytest.skip()

        src_path = f"{self.SRC}/{node_type}"
        dst_path = f"{self.DST}/{node_type}"

        create_node = getattr(self, f"create_{node_type}")
        create_node(src_path)
        self.write_user_attribute(src_path)

        self.execute_command(src_path, dst_path)

        self.validate_copy_base(src_path, dst_path)
        validate_node_copy = getattr(self, f"validate_{node_type}_copy")
        validate_node_copy(dst_path)

    @authors("h0pless")
    @pytest.mark.parametrize("use_redundant_flags", [True, False])
    @pytest.mark.parametrize("use_tx", [True, False])
    def test_subtree(self, use_redundant_flags, use_tx):
        # It would've been fine to use SRC/dir and DST/dir as paths, but this test is the best place
        # to verify that cross-cell copy works between two portals, between a portal and a rootstock and
        # vice versa. Because of this added complexity I am forced to write the following if statement.
        if not self.USE_SEQUOIA or self.COPY_TO_SEQUOIA:
            portal_path = f"{self.SRC}/portal"
            src_path = f"{portal_path}/dir"
            dst_path = f"{self.DST}/dir"
        else:
            # Copy from Sequoia to Cypress.
            portal_path = f"{self.DST}/portal"
            src_path = f"{self.SRC}/dir"
            dst_path = f"{portal_path}/dir"

        create("portal_entrance", f"{portal_path}", attributes={"exit_cell_tag": 12})

        tx = "0-0-0-0"
        if use_tx:
            tx = start_transaction()
            self.CONTEXT_DEPENDENT_ATTRIBUTES.append("ref_counter")
            self.CONTEXT_DEPENDENT_ATTRIBUTES.append("update_mode")
            self.CONTEXT_DEPENDENT_ATTRIBUTES.append("security_tags_update_mode")

        self.create_subtree(src_path, tx=tx)
        self.execute_command(
            src_path,
            dst_path,
            ignore_existing=use_redundant_flags,
            recursive=use_redundant_flags,
            tx=tx)

        self.validate_copy_base(src_path, dst_path, tx=tx)
        self.validate_subtree_attribute_consistency(src_path, dst_path, tx=tx)

        if use_tx:
            if self.COMMAND == "copy":
                assert self.CONTEXT_DEPENDENT_ATTRIBUTES.pop() == "security_tags_update_mode"
                assert self.CONTEXT_DEPENDENT_ATTRIBUTES.pop() == "update_mode"
                assert self.CONTEXT_DEPENDENT_ATTRIBUTES.pop() == "ref_counter"
            else:
                self.CONTEXT_DEPENDENT_ATTRIBUTES.append("versioned_resource_usage")

            commit_transaction(tx)

            self.validate_copy_base(src_path, dst_path)
            self.validate_subtree_attribute_consistency(src_path, dst_path)

            if self.COMMAND != "copy":
                assert self.CONTEXT_DEPENDENT_ATTRIBUTES.pop() == "versioned_resource_usage"

        # XXX(babenko): cleanup is weird
        remove(portal_path)

    @authors("h0pless")
    @pytest.mark.parametrize("subtree_type", ["subtree", "table"])
    @pytest.mark.parametrize("use_tx", [True, False])
    @pytest.mark.parametrize("should_preserve", [True, False])
    def test_preservable_attributes(self, subtree_type, use_tx, should_preserve):
        src_path = f"{self.SRC}/parent"
        dst_path = f"{self.DST}/parent"

        create_subtree = getattr(self, f"create_{subtree_type}")
        create_subtree(src_path)

        tx = start_transaction() if use_tx else "0-0-0-0"
        populate_attributes = getattr(self, f"populate_preservable_attributes_{subtree_type}")
        populate_attributes(src_path, tx=tx)

        create_user("not_john")
        user = "not_john"
        # Preserving is trickier than not preserving.
        if should_preserve:
            # In order to preserve ACL user has to have "administer" permission for the parent node.
            administer_permission = make_ace("allow", user, "administer")
            set("//sys/@config/cypress_manager/graft_synchronization_period", 100)
            set(f"{self.DST}&/@acl", [
                administer_permission,
            ])

            # The scion's @acl can be updated on master before the ACL used by
            # the Cypress proxy is replicated to the ground.
            wait(lambda: check_permission(user, "administer", self.DST)["action"] == "allow")

            # In order to preserve account user has to have "use" permission for the account.
            for account in self.AVAILABLE_ACCOUNTS:
                set(f"//sys/accounts/{account}/@acl/end", make_ace("allow", user, "use"))

        self.execute_command(
            src_path,
            dst_path,
            tx=tx,
            authenticated_user=user,
            preserve_account=should_preserve,
            preserve_creation_time=should_preserve,
            preserve_modification_time=should_preserve,
            preserve_expiration_time=should_preserve,
            preserve_expiration_timeout=should_preserve,
            preserve_owner=should_preserve,
            preserve_acl=should_preserve)

        self.PRESERVABLE_ATTRIBUTES = {
            "account": should_preserve,
            "creation_time": should_preserve,
            "modification_time": should_preserve,
            "expiration_time": should_preserve,
            "expiration_timeout": should_preserve,
            "owner": should_preserve,
            "acl": should_preserve,
        }
        self.validate_subtree_preservable_attribute_consistency(src_path, dst_path, tx=tx)

        if use_tx:
            commit_transaction(tx)
            self.validate_subtree_preservable_attribute_consistency(src_path, dst_path, tx="0-0-0-0")

    # Maybe move set to the test below to save up on time?
    @authors("h0pless")
    def test_opaque_subtree(self):
        src_path = f"{self.SRC}/subtree"
        dst_path = f"{self.DST}/subtree"

        self.create_subtree(src_path)
        set(f"{src_path}/map_node/@opaque", True)
        self.execute_command(src_path, dst_path)

        # Opaqueness is ignored in Sequoia.
        if not self.USE_SEQUOIA:
            self.validate_copy_base(src_path, dst_path)

        self.validate_subtree_attribute_consistency(src_path, dst_path)

    @authors("h0pless")
    @pytest.mark.parametrize("is_redundant", [True, False])
    def test_copy_recursive(self, is_redundant):
        src_path = f"{self.SRC}/subtree"
        if is_redundant:
            dst_path = f"{self.DST}/subtree"
        else:
            dst_path = f"{self.DST}/some/arbitrary/long/path/that/has/to/be/created/during/copy/of/the/aforementioned/subtree"

        self.create_subtree(src_path)
        self.execute_command(src_path, dst_path, recursive=True)

        self.validate_copy_base(src_path, dst_path)
        self.validate_subtree_attribute_consistency(src_path, dst_path)

    @authors("h0pless")
    @pytest.mark.parametrize("is_redundant", [True, False])
    def test_copy_force(self, is_redundant):
        src_path = f"{self.SRC}/subtree"
        dst_path = f"{self.DST}/subtree"

        if not is_redundant:
            self.create_table(dst_path)

        self.create_subtree(src_path)
        self.execute_command(src_path, dst_path, force=True)

        self.validate_copy_base(src_path, dst_path)
        self.validate_subtree_attribute_consistency(src_path, dst_path)

    @authors("h0pless")
    def test_ignore_existing(self):
        src_path = f"{self.SRC}/file"
        self.create_file(src_path)

        dst_path = f"{self.DST}/table"
        self.create_table(dst_path)

        if self.COMMAND == "move":
            with raises_yt_error(f"Node {self.DST}/table already exists"):
                self.execute_command(src_path, dst_path, ignore_existing=True)
            return
        else:
            self.execute_command(src_path, dst_path, ignore_existing=True)

        # This actually checks that table wasn't overwritten by a copy of the file.
        self.validate_table_copy(dst_path)

    @authors("h0pless")
    def test_lock_existing(self):
        src_path = f"{self.SRC}/file"
        self.create_file(src_path)

        dst_path = f"{self.DST}/table"
        self.create_table(dst_path)

        # Lock existing doesn't make sense without a transaction.
        tx = start_transaction()
        if self.COMMAND == "move":
            with raises_yt_error(f"Node {self.DST}/table already exists"):
                self.execute_command(src_path, dst_path, ignore_existing=True, lock_existing=True, tx=tx)
            return
        else:
            self.execute_command(src_path, dst_path, ignore_existing=True, lock_existing=True, tx=tx)

        # This actually checks that table wasn't overwritten by a copy of the file.
        self.validate_table_copy(dst_path, tx=tx)
        assert get(f"{dst_path}/@lock_count", tx=tx) == 1

    @authors("h0pless")
    def test_accounting(self):
        src_path = f"{self.SRC}/table"
        dst_path = f"{self.DST}/table"
        self.create_table(src_path)
        account = get(f"{src_path}/@account")
        wait(lambda: get(f"//sys/accounts/{account}/@resource_usage/chunk_count") == 1)

        self.execute_command(src_path, dst_path, preserve_account=True)
        assert get(f"//sys/accounts/{account}/@resource_usage/chunk_count") == 1

        create_account("jack")
        set(f"{dst_path}/@account", "jack")
        original_account_expected_usage = 1 if self.COMMAND == "copy" else 0
        wait(
            lambda: get(f"//sys/accounts/{account}/@resource_usage/chunk_count") == original_account_expected_usage and
            get("//sys/accounts/jack/@resource_usage/chunk_count") == 1
        )

        chunk_ids = get(f"{self.DST}/table/@chunk_ids")
        assert len(chunk_ids) == 1
        chunk_id = chunk_ids[0]
        expected_owning_nodes = [dst_path]
        if self.COMMAND == "copy":
            expected_owning_nodes.append(src_path)
        assert_items_equal(get(f"#{chunk_id}/@owning_nodes"), expected_owning_nodes)

        remove(src_path, force=True)
        remove(dst_path)
        wait(lambda: not exists("#" + chunk_id))

    @authors("kvk1920")
    def test_prerequisite_transaction_ids(self):
        src_path = f"{self.SRC}/table"
        dst_path = f"{self.DST}/table"
        self.create_table(src_path)

        tx = start_transaction()
        abort_transaction(tx)
        with raises_yt_error("Prerequisite check failed"):
            self.execute_command(src_path, dst_path, prerequisite_transaction_ids=[tx])

        tx = start_transaction()
        self.execute_command(src_path, dst_path, prerequisite_transaction_ids=[tx])
        assert exists(dst_path)
        self.validate_table_copy(dst_path)
        remove(dst_path)

    @authors("h0pless")
    @pytest.mark.parametrize("use_tx", [False, True])
    def test_inheritable_attributes(self, use_tx):
        #                                                 portal_exit               attribute = A
        # starting_node         attribute = NONE          `-- starting_node         attribute = NONE
        # |-- map_node          attribute = B                 |-- map_node          attribute = B
        # |   |-- table_keep_b  attribute = B      COPY       |   |-- table_keep_b  attribute = B
        # |   `-- table_c_to_b  attribute = C       ->        |   `-- table_c_to_b  attribute = B
        # |-- table_c_to_a      attribute = C                 |-- table_c_to_a      attribute = A
        # `-- table_none_to_a   attribute = NONE              `-- table_none_to_a   attribute = A

        # For the sake of simplicity I used "chunk_merger_mode". Later this test can be expanded.
        attribute_not_found = "Attribute \"chunk_merger_mode\" is not found"

        src_path = f"{self.SRC}/starting_node"
        dst_parent = self.DST
        dst_path = f"{self.DST}/starting_node"

        A = "auto"
        B = "deep"
        C = "shallow"

        tx = start_transaction() if use_tx else "0-0-0-0"

        set(f"{dst_parent}/@chunk_merger_mode", A)

        self.create_map_node(src_path)
        with raises_yt_error(attribute_not_found):
            get(f"{src_path}/@chunk_merger_mode")

        self.create_map_node(f"{src_path}/map_node")
        set(f"{src_path}/map_node/@chunk_merger_mode", B, tx=tx)

        self.create_table(f"{src_path}/map_node/table_keep_b", tx=tx)
        assert get(f"{src_path}/map_node/table_keep_b/@chunk_merger_mode", tx=tx) == B

        self.create_table(f"{src_path}/map_node/table_c_to_b", tx=tx)
        set(f"{src_path}/map_node/table_c_to_b/@chunk_merger_mode", C, tx=tx)

        self.create_table(f"{src_path}/table_c_to_a")
        set(f"{src_path}/table_c_to_a/@chunk_merger_mode", C, tx=tx)

        self.create_table(f"{src_path}/table_none_to_a")
        assert get(f"{src_path}/table_none_to_a/@chunk_merger_mode") == "none"

        self.execute_command(src_path, dst_path, tx=tx)

        if self.COMMAND == "copy":
            # Validate source hasn't changed.
            with raises_yt_error(attribute_not_found):
                get(f"{src_path}/@chunk_merger_mode", tx=tx)
            assert get(f"{src_path}/map_node/@chunk_merger_mode", tx=tx) == B
            assert get(f"{src_path}/map_node/table_keep_b/@chunk_merger_mode", tx=tx) == B
            assert get(f"{src_path}/map_node/table_c_to_b/@chunk_merger_mode", tx=tx) == C
            assert get(f"{src_path}/table_c_to_a/@chunk_merger_mode", tx=tx) == C
            assert get(f"{src_path}/table_none_to_a/@chunk_merger_mode", tx=tx) == "none"

        # Validate destination is correct.
        # These should have the same attribute value.
        assert get(f"{dst_parent}/@chunk_merger_mode", tx=tx) == A
        with raises_yt_error(attribute_not_found):
            get(f"{dst_path}/@chunk_merger_mode", tx=tx)
        assert get(f"{dst_path}/map_node/@chunk_merger_mode", tx=tx) == B
        assert get(f"{dst_path}/map_node/table_keep_b/@chunk_merger_mode", tx=tx) == B
        # These might change, depending on config.
        expected_value = B
        assert get(f"{dst_path}/map_node/table_c_to_b/@chunk_merger_mode", tx=tx) == expected_value
        expected_value = A
        assert get(f"{dst_path}/table_c_to_a/@chunk_merger_mode", tx=tx) == expected_value
        expected_value = A
        assert get(f"{dst_path}/table_none_to_a/@chunk_merger_mode", tx=tx) == expected_value

    @authors("h0pless")
    def test_many_transactions_in_subtree(self):
        # Transaction hierarchy:
        # topmost_tx
        # |-- grandparent_tx
        # |   `-- parent_tx < -- intentionally unused
        # |       `-- child_tx
        # |           `-- grandchild_tx
        # `-- great_uncle_tx

        topmost_tx = start_transaction()
        grandparent_tx = start_transaction(tx=topmost_tx)
        parent_tx = start_transaction(tx=grandparent_tx)
        child_tx = start_transaction(tx=parent_tx)
        grandchild_tx = start_transaction(tx=child_tx)
        great_uncle_tx = start_transaction(tx=topmost_tx)

        # Tree that should be copied / moved.
        # trunk_map_node
        # |-- grandparent_tx_map_node
        # |   |-- grandparent_tx_table
        # |   `-- child_tx_table
        # |-- other_trunk_map_node
        # |   `-- topmost_tx_table
        # `-- great_uncle_table

        src_path = f"{self.SRC}/trunk_map_node"
        dst_path = f"{self.DST}/trunk_map_node"

        create("map_node", src_path)
        create("map_node", f"{src_path}/grandparent_tx_map_node", tx=grandparent_tx)
        create("map_node", f"{src_path}/grandparent_tx_map_node/grandparent_tx_table", tx=grandparent_tx)
        create("map_node", f"{src_path}/grandparent_tx_map_node/child_tx_table", tx=child_tx)
        create("map_node", f"{src_path}/other_trunk_map_node")
        create("map_node", f"{src_path}/other_trunk_map_node/topmost_tx_table", tx=topmost_tx)
        create("map_node", f"{src_path}/great_uncle_table", tx=great_uncle_tx)

        if self.COMMAND != "copy":
            abort_transaction(great_uncle_tx)  # This leads to a lock conflict, which is reasonable.

        self.execute_command(src_path, dst_path, tx=grandchild_tx)

        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("ref_counter")
        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("update_mode")
        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("security_tags_update_mode")
        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("lock_count")
        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("lock_mode")
        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("resource_usage")

        self.validate_copy_base(src_path, dst_path, tx=grandchild_tx)
        self.validate_subtree_attribute_consistency(src_path, dst_path, tx=grandchild_tx)

        assert self.CONTEXT_DEPENDENT_ATTRIBUTES.pop() == "resource_usage"
        assert self.CONTEXT_DEPENDENT_ATTRIBUTES.pop() == "lock_mode"
        assert self.CONTEXT_DEPENDENT_ATTRIBUTES.pop() == "lock_count"
        assert self.CONTEXT_DEPENDENT_ATTRIBUTES.pop() == "security_tags_update_mode"
        assert self.CONTEXT_DEPENDENT_ATTRIBUTES.pop() == "update_mode"
        assert self.CONTEXT_DEPENDENT_ATTRIBUTES.pop() == "ref_counter"

    # Maybe these two tests are redundant, considering the preservable attributes test.
    @authors("shakurov")
    def test_expiration_time(self):
        expiration_time = str(get_current_time() + timedelta(seconds=3))
        src_path = f"{self.SRC}/table"
        dst_path = f"{self.DST}/table"
        self.create_table(src_path)
        set(f"{src_path}/@expiration_time", expiration_time)
        self.execute_command(src_path, dst_path, preserve_expiration_time=True)
        wait(
            lambda: not exists(dst_path),
            sleep_backoff=0.5,
            timeout=5)

    @authors("shakurov")
    def test_expiration_timeout(self):
        src_path = f"{self.SRC}/table"
        dst_path = f"{self.DST}/table"
        self.create_table(src_path)
        set(f"{src_path}/@expiration_timeout", 3000)
        self.execute_command(src_path, dst_path, preserve_expiration_timeout=True)

        wait(
            lambda: not exists(dst_path, suppress_expiration_timeout_renewal=True),
            sleep_backoff=0.5,
            timeout=5)

    @authors("babenko", "theevilbird")
    def test_removed_account(self):
        src_path = f"{self.SRC}/file"
        dst_path = f"{self.DST}/file"

        self.create_file(src_path)
        account = get(f"{src_path}/@account")
        wait(lambda: get(f"//sys/accounts/{account}/@resource_usage/master_memory/total") > 0)
        with raises_yt_error(f"Cannot remove account \"{account}\" because its usage is not zero"):
            remove(f"//sys/accounts/{account}")
        assert get(f"//sys/accounts/{account}/@life_stage") == "creation_committed"

        self.execute_command(src_path, dst_path, preserve_account=True)
        assert get(f"{dst_path}/@account") == account

        remove(src_path, force=True)
        remove(dst_path, force=True)
        gc_collect()
        wait(lambda: not exists(src_path))
        wait(lambda: not exists(dst_path))
        wait(lambda: account_usage_all_zero(get(f"//sys/accounts/{account}/@recursive_resource_usage")))
        get(f"//sys/accounts/{account}/@resource_usage")
        remove(f"//sys/accounts/{account}")
        wait(lambda: not exists(f"//sys/accounts/{account}"))

    # SYMLINK SHENANIGANS
    @authors("h0pless")
    def test_destination_symlink_safety(self):
        set("//sys/@config/cypress_manager/enable_cross_cell_links", True)

        src_path = f"{self.SRC}/subtree"
        self.create_subtree(src_path)

        underlying_dst_path = f"{self.SRC}/some_node"
        self.create_map_node(underlying_dst_path)

        # Weird naming here due to the test setup.
        dst_path = f"{self.DST}/subtree"
        link(underlying_dst_path, dst_path)

        if self.USE_SEQUOIA:
            time.sleep(.5)  # To ensure that proxy has synced with master.

        self.execute_command(src_path, dst_path, force=True)

        if self.USE_SEQUOIA and not self.COPY_TO_SEQUOIA:
            time.sleep(.5)  # To ensure that proxy has synced with master.

        self.validate_copy_base(src_path, dst_path)
        self.validate_subtree_attribute_consistency(src_path, dst_path)

    @authors("h0pless")
    def test_destination_symlink_resolution(self):
        src_path = f"{self.SRC}/subtree"
        self.create_subtree(src_path)

        underlying_dst_path = f"{self.DST}/some_node"
        self.create_map_node(underlying_dst_path)

        link_path = f"{self.SRC}/link"
        link(underlying_dst_path, link_path)

        if self.USE_SEQUOIA:
            time.sleep(.5)  # To ensure that proxy has synced with master.

        self.execute_command(src_path, f"{link_path}/subtree")

        if self.USE_SEQUOIA and not self.COPY_TO_SEQUOIA:
            time.sleep(.5)  # To ensure that proxy has synced with master.

        self.validate_copy_base(src_path, f"{underlying_dst_path}/subtree")
        self.validate_subtree_attribute_consistency(src_path, f"{underlying_dst_path}/subtree")

    @authors("h0pless")
    @pytest.mark.parametrize("link_beyond_entrance", [True, False])
    def test_source_symlink(self, link_beyond_entrance):
        set("//sys/@config/cypress_manager/enable_cross_cell_links", link_beyond_entrance)
        underlying_src_path = f"{self.SRC}/subtree"
        self.create_subtree(underlying_src_path)

        src_path = f"{self.DST}/link" if link_beyond_entrance else f"{self.SRC}/link"
        link(underlying_src_path, src_path)

        if self.USE_SEQUOIA:
            time.sleep(.5)  # To ensure that proxy has synced with master.

        dst_path = f"{self.DST}/subtree"
        self.execute_command(src_path, dst_path)

        if self.USE_SEQUOIA and not self.COPY_TO_SEQUOIA:
            time.sleep(.5)  # To ensure that proxy has synced with master.

        self.validate_copy_base(underlying_src_path, dst_path)
        self.validate_subtree_attribute_consistency(underlying_src_path, dst_path)

    # IMPROPER USES
    @authors("h0pless")
    def test_force_not_set(self):
        src_path = f"{self.SRC}/table"
        dst_path = f"{self.DST}/existing_node"

        tx = start_transaction()

        self.create_table(src_path)
        self.create_map_node(dst_path, tx=tx)

        # Conflict because of locks.
        with raises_yt_error(f"Cannot take lock for child \"existing_node\" of node {self.DST} since this child is locked by concurrent transaction"):
            self.execute_command(src_path, dst_path)

        # Force was not used.
        with raises_yt_error(f"Node {dst_path} already exists"):
            self.execute_command(src_path, dst_path, tx=tx)

        commit_transaction(tx)
        # Force was not used.
        with raises_yt_error(f"Node {dst_path} already exists"):
            self.execute_command(src_path, dst_path)

    @authors("h0pless")
    def test_recursive_not_set(self):
        src_path = f"{self.SRC}/table"
        dst_path = f"{self.DST}/listen/its/hard/to/come/up/with/funny/paths/every/time/table"

        self.create_table(src_path)

        with raises_yt_error(f"Node {self.DST} has no child with key \"listen\""):
            self.execute_command(src_path, dst_path)

    @authors("h0pless")
    def test_non_external_table(self):
        if self.USE_SEQUOIA:
            # It's really hard to ensure that a node is created on a specific cell.
            pytest.skip("Not implemented in Sequoia")

        src_path = f"{self.SRC}/table"
        dst_path = f"{self.DST}/table"

        tabl_id = create("table", src_path, attributes={"external_cell_tag": 11})

        with raises_yt_error(f"Cannot copy node {tabl_id} to cell 11 since the latter is its external cell"):
            self.execute_command(src_path, dst_path)

    @authors("h0pless")
    def test_cant_copy_to_root(self):
        src_path = f"{self.DST}/table"
        self.create_table(src_path)
        with raises_yt_error("Node / cannot be replaced"):
            self.execute_command(src_path, "/", force=True)

    @authors("h0pless")
    def test_cant_copy_subtree_with_portal(self):
        with raises_yt_error("Cannot clone a (portal|rootstock)"):
            self.execute_command("//tmp", "//home/other")

    @authors("h0pless")
    def test_ignore_existing_error(self):
        src_path = f"{self.SRC}/table"
        dst_path = f"{self.DST}/table"

        self.create_table(src_path)

        if self.COMMAND == "copy":
            with raises_yt_error("Cannot specify"):
                self.execute_command(src_path, dst_path, ignore_existing=True, force=True)
        else:
            self.execute_command(src_path, dst_path, lock_existing=True)

    @authors("h0pless")
    def test_lock_existing_error(self):
        src_path = f"{self.SRC}/table"
        dst_path = f"{self.DST}/table"

        self.create_table(src_path)

        if self.COMMAND == "copy":
            with raises_yt_error("Cannot specify"):
                self.execute_command(src_path, dst_path, lock_existing=True)
        else:
            self.execute_command(src_path, dst_path, lock_existing=True)

    @authors("h0pless")
    def test_ypath_special_symbols(self):
        src_path = f"{self.SRC}/map_node"
        self.create_map_node(src_path)

        bad_path = src_path + r"/\[b16:0:b00b:5:0:15:0:c001]:1337"
        self.create_map_node(bad_path)

        dst_path = f"{self.DST}/map_node"

        self.execute_command(src_path, dst_path)

    @authors("cherepashka")
    def test_cross_cell_copy_with_prerequisite_revision(self):
        source_path = f"{self.SRC}/revision_node"
        create("table", source_path)
        revision = get(f"{source_path}/@revision")

        with raises_yt_error("Cross-cell \"copy\"/\"move\" command does not support prerequisite revisions"):
            self.execute_command(
                source_path,
                f"{self.DST}/revision_node",
                prerequisite_revisions=[
                    {
                        "path": source_path,
                        "revision": revision,
                    }
                ])

################################################################################


class TestCrossCellMove(TestCrossCellCopy):
    COMMAND = "move"

    def setup_method(self, method):
        super(TestCrossCellMove, self).setup_method(method)
        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("access_counter")
        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("lock_count")
        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("lock_mode")
        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("schema_duplicate_count")


################################################################################


class TestCypressToSequoiaCopy(TestCrossCellCopy):
    USE_SEQUOIA = True
    ENABLE_CYPRESS_TRANSACTIONS_IN_SEQUOIA = True
    NUM_CYPRESS_PROXIES = 1
    NUM_SECONDARY_MASTER_CELLS = 6
    NUM_MASTERS = 1

    COPY_TO_SEQUOIA = True

    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["cypress_node_host"]},
        "11": {"roles": ["cypress_node_host"]},
        "12": {"roles": ["cypress_node_host"]},
        "13": {"roles": ["chunk_host"]},
        "14": {"roles": ["chunk_host"]},
        "15": {"roles": ["sequoia_node_host"]},
        "16": {"roles": ["sequoia_node_host"]},
    }

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "sequoia_manager": {
            "enable_ground_update_queues": True
        },
    }

    DELTA_CYPRESS_PROXY_CONFIG = {
        "testing": {
            "enable_ground_update_queues_sync": True,
            "enable_user_directory_per_request_sync": True,
        }
    }

    DELTA_CYPRESS_PROXY_DYNAMIC_CONFIG = {
        "object_service": {
            "allow_bypass_master_resolve": True,
        },
    }

    def setup_method(self, method):
        super(TestCypressToSequoiaCopy, self).setup_method(method)

        self.EXPECTED_ATTRIBUTE_CHANGES.append("sequoia")

        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("key")
        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("schema_duplicate_count")

        # Sequoia refs differ from Cyperss refs around creation of new nodes under transactions.
        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("ref_counter")

        # Unsupported in Sequoia.
        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("cow_cookie")
        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("children")

        # Remove once inherited attributes are implemented in Sequoia.
        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("chunk_merger_mode")

        # Remove once dynamic tables work in Sequoia.
        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("remount_needed_tablet_count")

        # TODO(h0pless): FIX this.
        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("shard_id")
        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("native_content_revision")


################################################################################


class TestCypressToSequoiaMove(TestCypressToSequoiaCopy):
    COMMAND = "move"

    def setup_method(self, method):
        super(TestCypressToSequoiaMove, self).setup_method(method)
        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("access_counter")
        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("lock_count")
        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("lock_mode")


################################################################################

class TestSequoiaToCypressCopy(TestCrossCellCopy):
    USE_SEQUOIA = True
    ENABLE_CYPRESS_TRANSACTIONS_IN_SEQUOIA = True
    NUM_CYPRESS_PROXIES = 1
    NUM_SECONDARY_MASTER_CELLS = 6
    NUM_MASTERS = 1

    MASTER_CELL_DESCRIPTORS = {
        "10": {"roles": ["cypress_node_host"]},
        "11": {"roles": ["cypress_node_host"]},
        "12": {"roles": ["cypress_node_host"]},
        "13": {"roles": ["chunk_host"]},
        "14": {"roles": ["chunk_host"]},
        "15": {"roles": ["sequoia_node_host"]},
        "16": {"roles": ["sequoia_node_host"]},
    }

    DELTA_DYNAMIC_MASTER_CONFIG = {
        "sequoia_manager": {
            "enable_ground_update_queues": True
        },
    }

    DELTA_CYPRESS_PROXY_CONFIG = {
        "testing": {
            "enable_ground_update_queues_sync": True,
            "enable_user_directory_per_request_sync": True,
        }
    }

    DELTA_CYPRESS_PROXY_DYNAMIC_CONFIG = {
        "object_service": {
            "allow_bypass_master_resolve": True,
        },
    }

    def setup_method(self, method):
        super(TestSequoiaToCypressCopy, self).setup_method(method)

        self.EXPECTED_ATTRIBUTE_CHANGES.append("sequoia")

        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("key")
        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("schema_duplicate_count")

        # Unsupported in Sequoia.
        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("reachable")
        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("cow_cookie")
        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("children")

        # Remove once inherited attributes are implemented in Sequoia.
        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("chunk_merger_mode")

        # Remove once dynamic tables work in Sequoia.
        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("remount_needed_tablet_count")

        # TODO(h0pless): FIX this.
        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("shard_id")
        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("native_content_revision")


################################################################################


class TestSequoiaToCypressMove(TestSequoiaToCypressCopy):
    COMMAND = "move"

    def setup_method(self, method):
        super(TestSequoiaToCypressMove, self).setup_method(method)
        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("access_counter")
        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("lock_count")
        self.CONTEXT_DEPENDENT_ATTRIBUTES.append("lock_mode")
