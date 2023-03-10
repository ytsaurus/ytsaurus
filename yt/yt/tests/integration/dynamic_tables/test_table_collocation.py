from yt_tests_common.dynamic_tables_base import DynamicTablesBase

from yt_commands import (
    authors, wait, get_driver,
    get, set, exists, remove, create, create_table_collocation, create_dynamic_table,
    sync_mount_table, sync_create_cells,
    raises_yt_error,
)

import yt_error_codes

from yt.test_helpers import assert_items_equal

import pytest


class TestReplicatedTablesCollocationBase(DynamicTablesBase):
    def _create_replicated_table(self, path, **attributes):
        attributes["schema"] = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value1", "type": "string"},
            {"name": "value2", "type": "int64"},
        ]
        attributes["dynamic"] = True
        attributes["enable_replication_logging"] = True
        id = create("replicated_table", path, attributes=attributes)
        sync_mount_table(path)
        return id

    def _create_sorted_table(self, path, **attributes):
        attributes["schema"] = [
            {"name": "key", "type": "int64", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
        ]
        return create_dynamic_table(path, **attributes)


class TestReplicatedTablesCollocation(TestReplicatedTablesCollocationBase):
    @authors("akozhikhov")
    def test_table_collocation_creation_errors(self):
        sync_create_cells(1)
        id0 = self._create_replicated_table("//tmp/t0", external_cell_tag=11)

        with raises_yt_error("non-empty"):
            create_table_collocation(table_ids=[])
        with raises_yt_error("Cannot specify both"):
            create_table_collocation(table_ids=[], table_paths=[])
        with raises_yt_error(yt_error_codes.ResolveErrorCode):
            create_table_collocation(table_paths=["//tmp/no_such_table"])
        with raises_yt_error("Duplicate table"):
            create_table_collocation(table_ids=[id0, id0])
        with raises_yt_error(yt_error_codes.ResolveErrorCode):
            corrupted_id = list(id0)
            corrupted_id[0] = '2'
            corrupted_id = "".join(corrupted_id)
            create_table_collocation(table_ids=[id0, corrupted_id])

        doc_id = create("document", "//tmp/d")
        with raises_yt_error("is expected to be a table"):
            create_table_collocation(table_ids=[id0, doc_id])
        with raises_yt_error("is expected to be a table"):
            create_table_collocation(table_paths=["//tmp/d"])

        table_id = self._create_sorted_table("//tmp/simple")
        with raises_yt_error("Unexpected type of table"):
            create_table_collocation(table_ids=[id0, table_id])

        id1 = self._create_replicated_table("//tmp/t1", external_cell_tag=11)

        create_table_collocation(table_ids=[id0])
        with raises_yt_error("already belongs to replication collocation"):
            create_table_collocation(table_ids=[id0, id1])
        other_collocation_id = create_table_collocation(table_ids=[id1])
        with raises_yt_error("already belongs to replication collocation"):
            set("#{}/@replication_collocation_id".format(id0), other_collocation_id)
        with raises_yt_error("cannot be set"):
            set("#{}/@replication_collocation_id".format(table_id), other_collocation_id)
        remove("#{}/@replication_collocation_id".format(id0))

    @authors("akozhikhov")
    def test_table_collocation_attributes(self):
        sync_create_cells(1)
        table0 = "//tmp/t0"
        table1 = "//tmp/t1"
        collocated_tables = {
            table0: self._create_replicated_table(table0, external_cell_tag=11),
        }

        assert not exists("{}/@replication_collocation_table_paths".format(table0))

        collocation_id = create_table_collocation(table_ids=list(collocated_tables.values()))
        collocated_tables[table1] = self._create_replicated_table(table1, external_cell_tag=11)
        set("{}/@replication_collocation_id".format(table1), collocation_id)

        assert get("#{}/@type".format(collocation_id)) == "table_collocation"
        assert get("#{}/@collocation_type".format(collocation_id)) == "replication"
        assert_items_equal(
            get("#{}/@table_ids".format(collocation_id)),
            list(collocated_tables.values()))
        assert_items_equal(
            get("#{}/@table_paths".format(collocation_id)),
            list(collocated_tables.keys()))

        for table in list(collocated_tables.keys()):
            assert get("{}/@replication_collocation_id".format(table)) == collocation_id
            assert_items_equal(
                get("{}/@replication_collocation_table_paths".format(table)),
                list(collocated_tables.keys()))
            if self.NUM_SECONDARY_MASTER_CELLS:
                assert get("{}/@external_cell_tag".format(table)) == get("#{}/@external_cell_tag".format(collocation_id))
            else:
                assert not exists("{}/@external_cell_tag".format(table))

    @authors("akozhikhov")
    def test_table_collocation_removal_1(self):
        sync_create_cells(1)
        table0 = "//tmp/t0"
        table1 = "//tmp/t1"
        table2 = "//tmp/t2"
        collocated_tables = {
            table0: self._create_replicated_table(table0, external_cell_tag=11),
            table1: self._create_replicated_table(table1, external_cell_tag=11),
        }

        collocation_id = create_table_collocation(table_paths=list(collocated_tables.keys()))

        remove(table0)
        del collocated_tables[table0]
        assert exists("#{}".format(collocation_id))
        assert get("{}/@replication_collocation_table_paths".format(table1)) == list(collocated_tables.keys())
        if self.NUM_SECONDARY_MASTER_CELLS:
            assert exists("#{}".format(collocation_id), driver=get_driver(1))
            assert get("#{}/@replication_collocation_id".format(collocated_tables[table1]), driver=get_driver(1)) == \
                collocation_id

        collocated_tables[table2] = self._create_replicated_table(table2, external_cell_tag=11)
        set("{}/@replication_collocation_id".format(table2), collocation_id)
        if self.NUM_SECONDARY_MASTER_CELLS:
            assert get("#{}/@replication_collocation_id".format(collocated_tables[table2]), driver=get_driver(1)) == \
                collocation_id

        remove("#{}".format(collocation_id))
        assert not exists("#{}".format(collocation_id))
        assert not exists("{}/@replication_collocation_id".format(table1))
        if self.NUM_SECONDARY_MASTER_CELLS:
            assert not exists("#{}".format(collocation_id), driver=get_driver(1))
            assert not exists("#{}/@replication_collocation_id".format(collocated_tables[table1]), driver=get_driver(1))
            assert not exists("#{}/@replication_collocation_id".format(collocated_tables[table2]), driver=get_driver(1))

    @authors("akozhikhov")
    def test_table_collocation_removal_2(self):
        sync_create_cells(1)
        table0 = "//tmp/t0"
        table1 = "//tmp/t1"
        collocated_tables = {
            table0: self._create_replicated_table(table0, external_cell_tag=11),
            table1: self._create_replicated_table(table1, external_cell_tag=11),
        }

        collocation_id = create_table_collocation(table_ids=list(collocated_tables.values()))
        assert exists("#{}".format(collocation_id))

        remove("{}/@replication_collocation_id".format(table0))
        remove(table1)
        wait(lambda: not exists("#{}".format(collocation_id)))
        if self.NUM_SECONDARY_MASTER_CELLS:
            wait(lambda: not exists("#{}".format(collocation_id), driver=get_driver(1)))

    @authors("akozhikhov")
    def test_table_collocation_removal_3(self):
        sync_create_cells(1)
        table0 = "//tmp/t0"
        collocated_tables = {
            table0: self._create_replicated_table(table0, external_cell_tag=11),
        }

        collocation_id = create_table_collocation(table_ids=list(collocated_tables.values()))
        assert exists("#{}".format(collocation_id))

        remove("{}/@replication_collocation_id".format(table0))
        wait(lambda: not exists("#{}".format(collocation_id)))
        if self.NUM_SECONDARY_MASTER_CELLS:
            wait(lambda: not exists("#{}".format(collocation_id), driver=get_driver(1)))


##################################################################


class TestReplicatedTablesCollocationMulticell(TestReplicatedTablesCollocation):
    NUM_SECONDARY_MASTER_CELLS = 2

    @authors("akozhikhov")
    def test_table_collocation_multicell(self):
        sync_create_cells(1)
        table0 = "//tmp/t0"
        table1 = "//tmp/t1"
        collocated_tables = {
            table0: self._create_replicated_table(table0, external_cell_tag=11),
            table1: self._create_replicated_table(table1, external_cell_tag=12),
        }

        with raises_yt_error("same external cell tag"):
            create_table_collocation(table_ids=list(collocated_tables.values()))

        collocation_id = create_table_collocation(table_ids=[collocated_tables[table0]])
        assert get("#{}/@table_ids".format(collocation_id), driver=get_driver(1)) == \
            [collocated_tables[table0]]
        assert get("#{}/@replication_collocation_table_paths".format(collocated_tables[table0]), driver=get_driver(1)) == \
            ["#{}".format(collocated_tables[table0])]

        with raises_yt_error("same external cell tag"):
            set("{}/@replication_collocation_id".format(table1), collocation_id)


##################################################################


class TestReplicatedTablesCollocationPortal(TestReplicatedTablesCollocationBase):
    NUM_SECONDARY_MASTER_CELLS = 2
    ENABLE_TMP_PORTAL = True

    @authors("akozhikhov")
    @pytest.mark.parametrize("external_cell_tag", [10, 11, 12])
    def test_portal_error(self, external_cell_tag):
        sync_create_cells(1)

        if external_cell_tag == 10:
            id0 = self._create_replicated_table("//sys/t", external=False)
            id1 = self._create_replicated_table("//tmp/t", external=False)
        else:
            id0 = self._create_replicated_table("//sys/t", external_cell_tag=external_cell_tag)
            id1 = self._create_replicated_table("//tmp/t", external_cell_tag=external_cell_tag)

        with raises_yt_error("No such object"):
            create_table_collocation(table_paths=["//sys/t", "//tmp/t"])
        with raises_yt_error("No such object"):
            create_table_collocation(table_ids=[id0, id1])
        with raises_yt_error("No such object"):
            create_table_collocation(table_paths=["//tmp/t"])
        with raises_yt_error("No such object"):
            create_table_collocation(table_ids=[id1])

        collocation_id = create_table_collocation(table_paths=["//sys/t"])
        if external_cell_tag == 11:
            with raises_yt_error("Unexpected native cell tag"):
                set("//tmp/t/@replication_collocation_id", collocation_id)
        else:
            with raises_yt_error("No such table collocation"):
                set("//tmp/t/@replication_collocation_id", collocation_id)

        remove("//sys/t")
