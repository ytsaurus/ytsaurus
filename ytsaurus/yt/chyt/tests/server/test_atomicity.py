from yt_commands import (authors, raises_yt_error, create, write_table, remove, read_table,
                         get, link, insert_rows, sync_mount_table, sync_unmount_table)

from yt.test_helpers import assert_items_equal

from base import ClickHouseTestBase, Clique, QueryFailedError

import yt.yson as yson

import time
import threading
import pytest


class TestClickHouseAtomicity(ClickHouseTestBase):
    def get_config_for_dynamic_table_tests(self):
        return {
            "yt": {
                "subquery": {
                    "min_data_weight_per_thread": 0,
                },
                "settings": {
                    "dynamic_table": {
                        "max_rows_per_write": 5000,
                    },
                },
            },
        }

    @authors("gudqeit")
    @pytest.mark.parametrize("table_read_lock_mode", ["none", "sync"])
    def test_read_for_static_table(self, table_read_lock_mode):
        create("table", "//tmp/t_in", attributes={"schema": [{"name": "a", "type": "int64"}]})
        rows = [{"a": i} for i in range(10)]
        write_table("//tmp/t_in", rows, verbose=False)
        with Clique(1) as clique:
            def remove_table():
                time.sleep(1)
                remove("//tmp/t_in")

            thread = threading.Thread(target=remove_table)
            thread.start()

            settings = {
                "chyt.execution.table_read_lock_mode": table_read_lock_mode,
                "chyt.testing.chunk_spec_fetcher_sleep_duration": 1500,
            }

            query = "select * from `//tmp/t_in`"

            if table_read_lock_mode == "sync":
                result = clique.make_query(query, settings=settings)
                assert_items_equal(result, rows)

            elif table_read_lock_mode == "none":
                with raises_yt_error(QueryFailedError):
                    clique.make_query(query, settings=settings)

            thread.join()

    @authors("gudqeit")
    def test_read_and_write_transactions(self):
        create("table", "//tmp/t_in", attributes={"schema": [{"name": "a", "type": "int64"}]})
        rows = [{"a": i} for i in range(100)]
        write_table("//tmp/t_in", rows, verbose=False)
        create("table", "//tmp/t_out", attributes={"schema": [{"name": "a", "type": "int64"}]})

        with Clique(3, config_patch={"clickhouse": {"settings": {"max_threads": 1}},
                                     "yt": {"subquery": {"min_data_weight_per_subquery": 1}}}) as clique:
            settings = {
                "parallel_distributed_insert_select": 1,
                "chyt.execution.table_read_lock_mode": "sync",
            }

            with raises_yt_error(QueryFailedError):
                query = "insert into `//tmp/t_out` select throwIf(a = 50, 'Generate error') from `//tmp/t_in`"
                clique.make_query(query, settings=settings)
            read_table("//tmp/t_out", verbose=False) == []

            clique.make_query("insert into `//tmp/t_out` select * from `//tmp/t_in`", settings=settings)
            assert_items_equal(read_table("//tmp/t_out", verbose=False), rows)
            assert get("//tmp/t_out/@chunk_count") == 3

            clique.make_query("insert into `<append=%false>//tmp/t_in` select * from `//tmp/t_in`",
                              settings={"parallel_distributed_insert_select": 1})
            assert_items_equal(read_table("//tmp/t_in", verbose=False), rows)

            query = 'create table "//tmp/s_out" engine YtTable() as select * from "//tmp/t_in"'
            clique.make_query(query, settings=settings)
            assert_items_equal(read_table("//tmp/s_out", verbose=False), rows)

    @authors("gudqeit")
    def test_join_with_locks(self):
        create(
            "table",
            "//tmp/t1",
            attributes={
                "schema": [
                    {"name": "key", "type": "int64", "required": True, "sort_order": "ascending"},
                    {"name": "lhs", "type": "string", "required": True},
                ]
            },
        )
        create(
            "table",
            "//tmp/t2",
            attributes={
                "schema": [
                    {"name": "key", "type": "int64", "required": True, "sort_order": "ascending"},
                    {"name": "rhs", "type": "string", "required": True},
                ]
            },
        )
        lhs_rows = [
            [{"key": 1, "lhs": "foo1"}],
            [{"key": 2, "lhs": "foo2"}, {"key": 3, "lhs": "foo3"}],
            [{"key": 4, "lhs": "foo4"}],
        ]
        rhs_rows = [
            [{"key": 1, "rhs": "bar1"}, {"key": 2, "rhs": "bar2"}],
            [{"key": 3, "rhs": "bar3"}, {"key": 4, "rhs": "bar4"}],
        ]

        for rows in lhs_rows:
            write_table("<append=%true>//tmp/t1", rows)
        for rows in rhs_rows:
            write_table("<append=%true>//tmp/t2", rows)

        with Clique(1) as clique:
            expected = [
                {"key": 1, "lhs": "foo1", "rhs": "bar1"},
                {"key": 2, "lhs": "foo2", "rhs": "bar2"},
                {"key": 3, "lhs": "foo3", "rhs": "bar3"},
                {"key": 4, "lhs": "foo4", "rhs": "bar4"},
            ]

            # Test without any interruption
            query = ("select key, lhs, rhs from `//tmp/t1` t1 join `//tmp/t2` t2 "
                     "on t1.key = t2.key order by key")
            settings = {"chyt.execution.table_read_lock_mode": "sync"}
            assert clique.make_query(query, settings=settings) == expected

            # Test with table removing during the query
            def remove_tables():
                time.sleep(1)
                remove("//tmp/t1")
                remove("//tmp/t2")

            thread = threading.Thread(target=remove_tables)
            thread.start()

            settings = {
                "chyt.execution.table_read_lock_mode": "sync",
                "chyt.testing.chunk_spec_fetcher_sleep_duration": 1500,
            }

            query = ("select key, lhs, rhs from `//tmp/t1` t1 join (select * from `//tmp/t2`) t2 "
                     "on t1.key = t2.key order by key")
            assert clique.make_query(query, settings=settings) == expected

            thread.join()

    @authors("gudqeit")
    def test_read_for_link(self):
        create("table", "//tmp/t_in1", attributes={"schema": [{"name": "a", "type": "int64"}]})
        rows = [{"a": i} for i in range(10)]
        write_table("//tmp/t_in1", rows, verbose=False)

        create("table", "//tmp/t_in2", attributes={"schema": [{"name": "a", "type": "int64"}]})

        link("//tmp/t_in1", "//tmp/link")

        with Clique(1) as clique:
            def change_link():
                time.sleep(1)
                link("//tmp/t_in2", "//tmp/link", force=True)
                assert read_table("//tmp/link") == []
                remove("//tmp/t_in1")

            thread = threading.Thread(target=change_link)
            thread.start()

            settings = {
                "chyt.execution.table_read_lock_mode": "sync",
                "chyt.testing.chunk_spec_fetcher_sleep_duration": 1500,
            }

            query = "select * from `//tmp/link`"
            result = clique.make_query(query, settings=settings)
            assert_items_equal(result, rows)

            thread.join()

    @authors("gudqeit")
    def test_yt_tables_function(self):
        create("map_node", "//tmp/dir")

        def create_test_table(path, table_num):
            create(
                "table",
                path,
                attributes={
                    "schema": [
                        {"name": "a", "type": "int64"},
                    ],
                },
            )
            write_table(path, [{"a": table_num}])

        create_test_table("//tmp/dir/t0", 0)
        create_test_table("//tmp/dir/t1", 1)

        with Clique(1) as clique:
            def remove_tables():
                time.sleep(1)
                remove("//tmp/dir/t0")
                remove("//tmp/dir/t1")

            thread = threading.Thread(target=remove_tables)
            thread.start()

            settings = {
                "chyt.execution.table_read_lock_mode": "sync",
                "chyt.testing.chunk_spec_fetcher_sleep_duration": 1500,
            }

            query = "select * from ytTables('//tmp/dir/t0', '//tmp/dir/t1') order by a"
            assert clique.make_query(query, settings=settings) == [{"a": 0}, {"a": 1}]

            thread.join()

    @authors("gudqeit")
    @pytest.mark.parametrize("table_read_lock_mode", ["none", "sync"])
    def test_read_for_dynamic_table(self, table_read_lock_mode):
        with Clique(1, config_patch=self.get_config_for_dynamic_table_tests()) as clique:
            create(
                "table",
                "//tmp/dt",
                attributes={
                    "dynamic": True,
                    "schema": [
                        {"name": "key", "type": "int64", "sort_order": "ascending"},
                        {"name": "value", "type": "string"},
                    ],
                    "enable_dynamic_store_read": True,
                    "dynamic_store_auto_flush_period": yson.YsonEntity(),
                },
            )
            sync_mount_table("//tmp/dt")

            data = [{"key": i, "value": "foo" + str(i)} for i in range(10)]

            for i in range(10):
                insert_rows("//tmp/dt", [data[i]])

            extra_row = {"key": 10, "value": "foo10"}

            def edit_table():
                time.sleep(1)
                insert_rows("//tmp/dt", [extra_row])

            thread = threading.Thread(target=edit_table)
            thread.start()

            settings = {
                "chyt.execution.table_read_lock_mode": table_read_lock_mode,
                "chyt.testing.chunk_spec_fetcher_sleep_duration": 1500,
            }

            query = "select * from `//tmp/dt` order by key"
            result = clique.make_query(query, settings=settings)

            if table_read_lock_mode == "sync":
                for row in result:
                    assert row in data

            elif table_read_lock_mode == "none":
                assert result == data + [extra_row]

            thread.join()

    @authors("gudqeit")
    @pytest.mark.parametrize("table_read_lock_mode", ["none", "best_effort", "sync"])
    def test_locks_and_chunk_removing(self, table_read_lock_mode):
        create(
            "table",
            "//tmp/dt",
            attributes={
                "dynamic": True,
                "schema": [
                    {"name": "key", "type": "int64", "sort_order": "ascending"},
                    {"name": "value", "type": "string"},
                ],
                "enable_dynamic_store_read": True,
                "dynamic_store_auto_flush_period": yson.YsonEntity(),
            },
        )
        sync_mount_table("//tmp/dt")

        data = [{"key": i, "value": "foo" + str(i)} for i in range(10)]
        insert_rows("//tmp/dt", data)

        sync_unmount_table("//tmp/dt")
        sync_mount_table("//tmp/dt")

        with Clique(1, config_patch=self.get_config_for_dynamic_table_tests()) as clique:
            def remove_table():
                time.sleep(1)
                remove("//tmp/dt")

            thread = threading.Thread(target=remove_table)
            thread.start()

            settings = {
                "chyt.execution.table_read_lock_mode": table_read_lock_mode,
                "chyt.testing.input_stream_factory_sleep_duration": 2500,
            }

            query = "select * from `//tmp/dt` order by key"

            if table_read_lock_mode == "none":
                with raises_yt_error(QueryFailedError):
                    clique.make_query(query, settings=settings)

            elif table_read_lock_mode in ["best_effort", "sync"]:
                assert clique.make_query(query, settings=settings) == data

            thread.join()

    @authors("gudqeit")
    @pytest.mark.parametrize("table_read_lock_mode", ["none", "sync"])
    def test_concat_tables_range_function(self, table_read_lock_mode):
        create("map_node", "//tmp/test_dir")
        for table_index in range(1, 3):
            create(
                "table",
                "//tmp/test_dir/table_" + str(table_index),
                attributes={"schema": [{"name": "i", "type": "int64"}]},
            )
            write_table("//tmp/test_dir/table_" + str(table_index), [{"i": table_index}])

        with Clique(1) as clique:
            def add_table():
                time.sleep(1)
                extra_table = "//tmp/test_dir/table_3"
                create(
                    "table",
                    extra_table,
                    attributes={"schema": [{"name": "i", "type": "int64"}]},
                )
                write_table(extra_table, [{"i": 3}])

            thread = threading.Thread(target=add_table)
            thread.start()

            settings = {
                "chyt.execution.table_read_lock_mode": table_read_lock_mode,
                "chyt.testing.concat_tables_range_sleep_duration": 1500,
            }

            query = "select * from concatYtTablesRange('//tmp/test_dir', 'table_1') order by i"

            if table_read_lock_mode == "none":
                assert clique.make_query(query, settings=settings) == [{"i": 1}, {"i": 2}, {"i": 3}]

            elif table_read_lock_mode == "sync":
                assert clique.make_query(query, settings=settings) == [{"i": 1}, {"i": 2}]

            thread.join()

    @authors("gudqeit")
    @pytest.mark.parametrize("table_read_lock_mode", ["none", "sync"])
    def test_yt_list_nodes(self, table_read_lock_mode):
        create("map_node", "//tmp/test_dir")
        for table_index in range(1, 3):
            create(
                "table",
                "//tmp/test_dir/table_" + str(table_index),
                attributes={"schema": [{"name": "i", "type": "int64"}]},
            )

        with Clique(1) as clique:
            def edit_test_dir():
                time.sleep(1)
                create(
                    "table",
                    "//tmp/test_dir/table_3",
                    attributes={"schema": [{"name": "i", "type": "int64"}]},
                )
                remove("//tmp/test_dir/table_2")

            thread = threading.Thread(target=edit_test_dir)
            thread.start()

            settings = {
                "chyt.execution.table_read_lock_mode": table_read_lock_mode,
                "chyt.testing.list_dirs_sleep_duration": 1500,
            }

            query = "select $path from ytListNodes('//tmp/test_dir') order by $path"

            if table_read_lock_mode == "none":
                expected = [
                    {"$path": "//tmp/test_dir/table_1"},
                    {"$path": "//tmp/test_dir/table_3"},
                ]
                assert clique.make_query(query, settings=settings) == expected

            elif table_read_lock_mode == "sync":
                expected = [
                    {"$path": "//tmp/test_dir/table_1"},
                    {"$path": "//tmp/test_dir/table_2"},
                ]
                assert clique.make_query(query, settings=settings) == expected

            thread.join()
