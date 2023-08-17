from yt_commands import (authors, create, write_table, insert_rows, get, print_debug, sync_mount_table,
                         sync_unmount_table)
from yt.wrapper import yson

from base import ClickHouseTestBase, Clique

import pytest
import itertools
import random


class TestClickHousePrewhere(ClickHouseTestBase):
    def get_config_patch(self, value, enable_min_max_filtering=False):
        return {"clickhouse": {"settings": {"optimize_move_to_prewhere": int(value)}},
                "yt": {"settings": {"execution": {"enable_min_max_filtering": enable_min_max_filtering}}}}

    @authors("evgenstf")
    @pytest.mark.parametrize("optimize_for, required", itertools.product(["lookup", "scan"], [False, True]))
    def test_prewhere_actions(self, optimize_for, required):
        with Clique(1, config_patch=self.get_config_patch(False)) as clique:
            create(
                "table",
                "//tmp/t1",
                attributes={
                    "schema": [{"name": "value", "type": "int64", "required": required}],
                    "optimize_for": optimize_for,
                },
            )
            write_table("//tmp/t1", [{"value": 0}, {"value": 1}, {"value": 2}, {"value": 3}])

            assert clique.make_query('select count() from "//tmp/t1"') == [{"count()": 4}]
            assert clique.make_query('select count() from "//tmp/t1" prewhere (value < 3)') == [{"count()": 3}]
            assert clique.make_query('select count(*) from "//tmp/t1" prewhere (value < 3)') == [{"count()": 3}]
            assert clique.make_query('select count(value) from "//tmp/t1" prewhere (value < 3)') == [
                {"count(value)": 3}
            ]
            assert clique.make_query('select count() from "//tmp/t1" prewhere (value < 3)') == [{"count()": 3}]
            assert clique.make_query('select any(0) from "//tmp/t1" prewhere (value < 3)') == [{"any(0)": 0}]

            create(
                "table",
                "//tmp/t2",
                attributes={
                    "schema": [
                        {"name": "key", "type": "int64", "required": required},
                        {"name": "value", "type": "string", "required": required},
                    ],
                    "optimize_for": optimize_for,
                },
            )
            write_table(
                "//tmp/t2",
                [
                    {"key": 0, "value": "aaa"},
                    {"key": 1, "value": "bbb"},
                    {"key": 2, "value": "bbb"},
                    {"key": 3, "value": "ddd"},
                ],
            )
            assert clique.make_query(
                'select value from "//tmp/t2" prewhere key in (select key from "//tmp/t2" where value = \'bbb\')'
            ) == [{"value": "bbb"}, {"value": "bbb"}]

    @authors("evgenstf")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_prewhere_one_chunk(self, optimize_for):
        with Clique(1, config_patch=self.get_config_patch(False)) as clique:
            create(
                "table",
                "//tmp/table_1",
                attributes={
                    "schema": [
                        {"name": "i", "type": "int64"},
                        {"name": "j", "type": "int64"},
                        {"name": "k", "type": "int64"},
                    ],
                    "optimize_for": optimize_for,
                },
            )
            write_table(
                "//tmp/table_1",
                [
                    {"i": 1, "j": 11, "k": 101},
                    {"i": 2, "j": 12, "k": 102},
                    {"i": 3, "j": 13, "k": 103},
                    {"i": 4, "j": 14, "k": 104},
                    {"i": 5, "j": 15, "k": 105},
                    {"i": 6, "j": 16, "k": 106},
                    {"i": 7, "j": 17, "k": 107},
                    {"i": 8, "j": 18, "k": 108},
                    {"i": 9, "j": 19, "k": 109},
                    {"i": 10, "j": 110, "k": 110},
                ],
            )
            assert clique.make_query('select i from "//tmp/table_1" prewhere j > 13 and j < 18 order by i') == [
                {"i": 4},
                {"i": 5},
                {"i": 6},
                {"i": 7},
            ]

    @authors("evgenstf")
    @pytest.mark.parametrize("optimize_for", ["lookup", "scan"])
    def test_prewhere_several_chunks(self, optimize_for):
        with Clique(1, config_patch=self.get_config_patch(False)) as clique:
            create(
                "table",
                "//tmp/test_table",
                attributes={
                    "schema": [
                        {"name": "key", "type": "string"},
                        {"name": "index", "type": "int64"},
                        {"name": "data", "type": "string"},
                    ],
                    "optimize_for": optimize_for,
                },
            )
            rows = [
                {"key": "b_key", "index": i, "data": "b" * 50}
                if i == 1234
                else {"key": "a_key", "data": "a" * 50 + str(random.randint(0, 1000000))}
                for i in range(10 * 10 * 1024)
            ]
            for i in range(10):
                write_table(
                    "<append=%true>//tmp/test_table",
                    rows[(len(rows) * i) // 10:(len(rows) * (i + 1)) // 10],
                    table_writer={"block_size": 1024, "desired_chunk_size": 10 * 1024},
                )

            assert get("//tmp/test_table/@chunk_count") == 10
            assert clique.make_query("select index from \"//tmp/test_table\" prewhere key = 'b_key'") == [
                {"index": 1234}
            ]
            clique.make_query_and_validate_row_count(
                "select index from \"//tmp/test_table\" where key = 'b_key'", exact=102400
            )
            clique.make_query_and_validate_row_count(
                "select index from \"//tmp/test_table\" prewhere key = 'b_key'", exact=1
            )

    @authors("max42")
    def test_prewhere_optimizer_static(self):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "heavy", "type": "string"}, {"name": "light", "type": "int64"}]
        })

        from random import randint as rnd

        def random_str():
            return hex(rnd(16**1023, 16**1024 - 1))[2:]

        rows = [{"heavy": random_str(), "light": i} for i in range(1024)]
        write_table("//tmp/t", rows, table_writer={"block_size": 1024, "desired_chunk_size": 10 * 1024})

        with Clique(1, config_patch=self.get_config_patch(True)) as clique:
            query = "select light from `//tmp/t` where heavy == '{}'".format(rows[42]["heavy"])
            explain_result = clique.make_query("explain syntax " + query)
            print_debug(explain_result)
            assert any("PREWHERE" in row["explain"] for row in explain_result)

            result = clique.make_query_and_validate_row_count(
                query,
                exact=1,
                verbose=False)
            assert len(result) == 1
            assert result[0]["light"] == 42

    @authors("max42")
    def test_prewhere_optimizer_dynamic(self):
        # TODO(max42): CHYT-462.
        # PREWHERE for dynamic tables is not supported yet.
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "heavy", "type": "string", "sort_order": "ascending"},
                       {"name": "light", "type": "int64"}],
            "dynamic": True,
            "enable_dynamic_store_read": True,
            "dynamic_store_auto_flush_period": yson.YsonEntity(),
        })
        sync_mount_table("//tmp/t")

        from random import randint as rnd

        def random_str():
            return hex(rnd(16**1023, 16**1024 - 1))[2:]

        rows = [{"heavy": random_str(), "light": i} for i in range(1024)]
        insert_rows("//tmp/t", rows)

        # Without unmount we would simply get zeroes as columnar statistics as all rows will reside in dynamic stores.
        sync_unmount_table("//tmp/t")

        with Clique(1, config_patch={"yt": {"settings": {"execution": {"enable_min_max_filtering": False}}}}) as clique:
            query = "select light from `//tmp/t` where heavy == '{}'".format(rows[42]["heavy"])
            explain_result = clique.make_query("explain syntax " + query)
            print_debug(explain_result)
            assert not any("PREWHERE" in row["explain"] for row in explain_result)

            result = clique.make_query_and_validate_row_count(
                query,
                exact=1024,
                verbose=False)
            assert len(result) == 1
            assert result[0]["light"] == 42
