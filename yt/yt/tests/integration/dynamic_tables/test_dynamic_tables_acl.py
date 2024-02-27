from .test_sorted_dynamic_tables import TestSortedDynamicTablesBase
from .test_ordered_dynamic_tables import TestOrderedDynamicTablesBase

from yt_commands import (
    authors, get, set, exists, create_user, make_ace, insert_rows,
    select_rows, lookup_rows,
    delete_rows, trim_rows, sync_create_cells, sync_mount_table, create, read_table, sync_flush_table)

from yt.environment.helpers import assert_items_equal
from yt.common import YtError

import pytest

################################################################################


class TestSortedDynamicTablesAcl(TestSortedDynamicTablesBase):
    USE_PERMISSION_CACHE = False

    SIMPLE_SCHEMA = [
        {"name": "key", "type": "int64", "sort_order": "ascending"},
        {"name": "value", "type": "string"},
    ]
    COLUMNAR_SCHEMA = [
        {"name": "key", "type": "int64", "sort_order": "ascending"},
        {"name": "value1", "type": "string"},
        {"name": "value2", "type": "string"},
        {"name": "value3", "type": "string"},
    ]

    def _prepare_env(self):
        if not exists("//sys/users/u"):
            create_user("u")
        if get("//sys/tablet_cells/@count") == 0:
            sync_create_cells(1)

    def _prepare_allowed(self, permission, table="//tmp/t"):
        self._prepare_env()
        self._create_simple_table(table, schema=self.SIMPLE_SCHEMA)
        sync_mount_table(table)
        set(table + "/@inherit_acl", False)
        set(table + "/@acl", [make_ace("allow", "u", permission)])

    def _prepare_denied(self, permission, table="//tmp/t"):
        self._prepare_env()
        self._create_simple_table(table, schema=self.SIMPLE_SCHEMA)
        sync_mount_table(table)
        set(table + "/@acl", [make_ace("deny", "u", permission)])

    def _prepare_columnar(self):
        create_user("u1")
        create_user("u2")
        sync_create_cells(1)
        self._create_simple_table("//tmp/t", schema=self.COLUMNAR_SCHEMA)
        sync_mount_table("//tmp/t")
        set(
            "//tmp/t/@acl",
            [
                make_ace("allow", ["u1", "u2"], "read"),
                make_ace("deny", "u1", "read", columns=["value1"]),
                make_ace("allow", "u2", "read", columns=["value1"]),
                make_ace("deny", "u2", "read", columns=["value2"]),
            ],
        )

    @authors("babenko")
    def test_select_allowed(self):
        self._prepare_allowed("read")
        insert_rows("//tmp/t", [{"key": 1, "value": "test"}])
        expected = [{"key": 1, "value": "test"}]
        actual = select_rows("* from [//tmp/t]", authenticated_user="u")
        assert_items_equal(actual, expected)

    @authors("babenko")
    def test_select_with_join_allowed(self):
        self._prepare_allowed("read", "//tmp/t1")
        self._prepare_allowed("read", "//tmp/t2")
        insert_rows("//tmp/t1", [{"key": 1, "value": "test1"}])
        insert_rows("//tmp/t2", [{"key": 1, "value": "test2"}])
        expected = [{"key": 1, "value1": "test1", "value2": "test2"}]
        actual = select_rows(
            "t1.key as key, t1.value as value1, t2.value as value2 "
            "from [//tmp/t1] as t1 join [//tmp/t2] as t2 on t1.key = t2.key",
            authenticated_user="u",
        )
        assert_items_equal(actual, expected)

    @authors("babenko")
    def test_select_with_join_denied(self):
        self._prepare_allowed("read", "//tmp/t1")
        self._prepare_denied("read", "//tmp/t2")
        insert_rows("//tmp/t1", [{"key": 1, "value": "test1"}])
        insert_rows("//tmp/t2", [{"key": 1, "value": "test2"}])
        with pytest.raises(YtError):
            select_rows(
                "t1.key as key, t1.value as value1, t2.value as value2 "
                "from [//tmp/t1] as t1 join [//tmp/t2] as t2 on t1.key = t2.key",
                authenticated_user="u",
            )

    @authors("babenko")
    def test_select_denied(self):
        self._prepare_denied("read")
        with pytest.raises(YtError):
            select_rows("* from [//tmp/t]", authenticated_user="u")

    @authors("babenko")
    def test_lookup_allowed(self):
        self._prepare_allowed("read")
        insert_rows("//tmp/t", [{"key": 1, "value": "test"}])
        expected = [{"key": 1, "value": "test"}]
        actual = lookup_rows("//tmp/t", [{"key": 1}], authenticated_user="u")
        assert_items_equal(actual, expected)

    @authors("babenko")
    def test_lookup_denied(self):
        self._prepare_denied("read")
        insert_rows("//tmp/t", [{"key": 1, "value": "test"}])
        with pytest.raises(YtError):
            lookup_rows("//tmp/t", [{"key": 1}], authenticated_user="u")

    @authors("babenko")
    def test_columnar_lookup_denied(self):
        self._prepare_columnar()
        insert_rows("//tmp/t", [{"key": 1, "value1": "a", "value2": "b", "value3": "c"}])
        with pytest.raises(YtError):
            lookup_rows("//tmp/t", [{"key": 1}], authenticated_user="u1")
        with pytest.raises(YtError):
            lookup_rows("//tmp/t", [{"key": 2}], authenticated_user="u1")
        with pytest.raises(YtError):
            lookup_rows(
                "//tmp/t",
                [{"key": 1}],
                column_names=["value1"],
                authenticated_user="u1",
            )
        with pytest.raises(YtError):
            lookup_rows(
                "//tmp/t",
                [{"key": 1}],
                column_names=["value2"],
                authenticated_user="u2",
            )

    @authors("babenko")
    def test_columnar_lookup_allowed(self):
        self._prepare_columnar()
        insert_rows("//tmp/t", [{"key": 1, "value1": "a", "value2": "b", "value3": "c"}])
        assert (
            lookup_rows(
                "//tmp/t",
                [{"key": 1}],
                column_names=["key", "value3"],
                authenticated_user="u1",
            )
            == [{"key": 1, "value3": "c"}]
        )
        assert (
            lookup_rows(
                "//tmp/t",
                [{"key": 1}],
                column_names=["key", "value1"],
                authenticated_user="u2",
            )
            == [{"key": 1, "value1": "a"}]
        )

    @authors("babenko")
    def test_columnar_select_denied(self):
        self._prepare_columnar()
        insert_rows("//tmp/t", [{"key": 1, "value1": "a", "value2": "b", "value3": "c"}])
        with pytest.raises(YtError):
            select_rows("* from [//tmp/t]", authenticated_user="u1")
        with pytest.raises(YtError):
            select_rows("value1 from [//tmp/t]", authenticated_user="u1")
        with pytest.raises(YtError):
            select_rows("value2 from [//tmp/t]", authenticated_user="u2")

    @authors("babenko")
    def test_columnar_select_allowed(self):
        self._prepare_columnar()
        insert_rows("//tmp/t", [{"key": 1, "value1": "a", "value2": "b", "value3": "c"}])
        assert select_rows("key, value3 from [//tmp/t] where key = 1", authenticated_user="u1") == [
            {"key": 1, "value3": "c"}
        ]
        assert select_rows("key, value1 from [//tmp/t] where key = 1", authenticated_user="u2") == [
            {"key": 1, "value1": "a"}
        ]

    @authors("babenko")
    def test_insert_allowed(self):
        self._prepare_allowed("write")
        insert_rows("//tmp/t", [{"key": 1, "value": "test"}], authenticated_user="u")
        expected = [{"key": 1, "value": "test"}]
        actual = lookup_rows("//tmp/t", [{"key": 1}])
        assert_items_equal(actual, expected)

    @authors("babenko")
    def test_insert_denied(self):
        self._prepare_denied("write")
        with pytest.raises(YtError):
            insert_rows("//tmp/t", [{"key": 1, "value": "test"}], authenticated_user="u")

    @authors("babenko")
    def test_delete_allowed(self):
        self._prepare_allowed("write")
        insert_rows("//tmp/t", [{"key": 1, "value": "test"}])
        delete_rows("//tmp/t", [{"key": 1}], authenticated_user="u")
        expected = []
        actual = lookup_rows("//tmp/t", [{"key": 1}])
        assert_items_equal(actual, expected)

    @authors("babenko")
    def test_delete_denied(self):
        self._prepare_denied("write")
        with pytest.raises(YtError):
            delete_rows("//tmp/t", [{"key": 1}], authenticated_user="u")

    @authors("ponasenko-rs")
    @pytest.mark.parametrize("read_from_dynamic_store", [False, True])
    @pytest.mark.parametrize("omit_inaccessible_columns", [False, True])
    def test_inaccessible_columns(self, read_from_dynamic_store, omit_inaccessible_columns):
        self._prepare_env()

        attributes = {
            "schema": [
                {"name": "key", "type": "string", "sort_order": "ascending"},
                {"name": "public", "type": "string"},
                {"name": "private", "type": "string"},
            ],
            "dynamic": True,
            "acl": [
                make_ace("deny", "u", "read", columns=["private"]),
            ],
        }

        if read_from_dynamic_store:
            attributes["enable_dynamic_store_read"] = True

        create(
            "table",
            "//tmp/t",
            attributes=attributes,
        )
        sync_mount_table("//tmp/t")

        insert_rows("//tmp/t", [{"key": "1", "public": "public1", "private": "private1"}])
        if not read_from_dynamic_store:
            sync_flush_table("//tmp/t")

        try:
            rows = read_table(
                "//tmp/t",
                omit_inaccessible_columns=omit_inaccessible_columns,
                authenticated_user="u",
            )
            assert rows == [{"key": "1", "public": "public1"}]
        except YtError as err:
            assert err.contains_text(
                'Access denied for user "u": "read" permission for column "private" of node //tmp/t is denied for "u" by ACE at node //tmp/t'
            )
            assert not omit_inaccessible_columns


class TestSortedDynamicTablesAclMulticell(TestSortedDynamicTablesAcl):
    NUM_SECONDARY_MASTER_CELLS = 2


class TestSortedDynamicTablesAclRpcProxy(TestSortedDynamicTablesAcl):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True


class TestSortedDynamicTablesAclPortal(TestSortedDynamicTablesAclMulticell):
    ENABLE_TMP_PORTAL = True


################################################################################


class TestOrderedDynamicTablesAcl(TestOrderedDynamicTablesBase):
    USE_PERMISSION_CACHE = False

    def _prepare_allowed(self, permission):
        create_user("u")
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")
        set("//tmp/t/@inherit_acl", False)
        set("//tmp/t/@acl", [make_ace("allow", "u", permission)])

    def _prepare_denied(self, permission):
        create_user("u")
        sync_create_cells(1)
        self._create_simple_table("//tmp/t")
        sync_mount_table("//tmp/t")
        set("//tmp/t/@acl", [make_ace("deny", "u", permission)])

    @authors("babenko")
    def test_select_allowed(self):
        self._prepare_allowed("read")
        rows = [{"a": 1}]
        insert_rows("//tmp/t", rows)
        assert select_rows("a from [//tmp/t]", authenticated_user="u") == rows

    @authors("babenko")
    def test_select_denied(self):
        self._prepare_denied("read")
        with pytest.raises(YtError):
            select_rows("* from [//tmp/t]", authenticated_user="u")

    @authors("babenko")
    def test_insert_allowed(self):
        self._prepare_allowed("write")
        rows = [{"a": 1}]
        insert_rows("//tmp/t", rows, authenticated_user="u")
        assert select_rows("a from [//tmp/t]") == rows

    @authors("babenko")
    def test_insert_denied(self):
        self._prepare_denied("write")
        with pytest.raises(YtError):
            insert_rows("//tmp/t", [{"a": 1}], authenticated_user="u")

    @authors("babenko")
    def test_trim_allowed(self):
        self._prepare_allowed("write")
        rows = [{"a": 1}, {"a": 2}]
        insert_rows("//tmp/t", rows)
        trim_rows("//tmp/t", 0, 1, authenticated_user="u")
        assert select_rows("a from [//tmp/t]") == rows[1:]

    @authors("babenko")
    def test_trim_denied(self):
        self._prepare_denied("write")
        insert_rows("//tmp/t", [{"a": 1}, {"a": 2}])
        with pytest.raises(YtError):
            trim_rows("//tmp/t", 0, 1, authenticated_user="u")


class TestOrderedDynamicTablesAclMulticell(TestOrderedDynamicTablesAcl):
    NUM_SECONDARY_MASTER_CELLS = 2


class TestOrderedDynamicTablesAclRpcProxy(TestOrderedDynamicTablesAcl):
    DRIVER_BACKEND = "rpc"
    ENABLE_RPC_PROXY = True


class TestOrderedDynamicTablesAclPortal(TestOrderedDynamicTablesAclMulticell):
    ENABLE_TMP_PORTAL = True
