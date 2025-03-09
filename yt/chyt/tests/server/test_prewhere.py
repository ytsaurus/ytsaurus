from yt_commands import (authors, create, write_table, insert_rows, get, print_debug, sync_mount_table,
                         sync_unmount_table, raises_yt_error)
from yt.wrapper import yson

from base import ClickHouseTestBase, Clique, QueryFailedError

import pytest
import itertools
import random


class TestClickHousePrewhere(ClickHouseTestBase):
    NUM_TEST_PARTITIONS = 2

    def get_config_patch(self, optimize_move_to_prewhere, prefilter_data_slices=False):
        return {
            "clickhouse": {
                "settings": {
                    "optimize_move_to_prewhere": int(optimize_move_to_prewhere),
                },
            },
            "yt": {
                "settings": {
                    "execution": {
                        "enable_min_max_filtering": False,
                    },
                    "prewhere": {
                        "prefilter_data_slices": prefilter_data_slices,
                    },
                },
            },
        }

    @staticmethod
    def _extract_step_summary(query_log_rows, step_index, metric_name):
        result = 0
        for row in query_log_rows:
            statistics = row["chyt_query_statistics"]
            if "secondary_query_source" not in statistics:
                continue
            if str(step_index) not in statistics["secondary_query_source"]["steps"]:
                continue
            result += statistics["secondary_query_source"]["steps"][str(step_index)][metric_name]["sum"]
        return result

    @authors("evgenstf")
    @pytest.mark.parametrize("optimize_for, required, prefilter_data_slices", itertools.product(["lookup", "scan"], [False, True], [False, True]))
    def test_prewhere_actions(self, optimize_for, required, prefilter_data_slices):
        with Clique(1, config_patch=self.get_config_patch(False, prefilter_data_slices)) as clique:
            create(
                "table",
                "//tmp/t1",
                attributes={
                    "schema": [
                        {"name": "value1", "type": "int64", "required": required},
                        {"name": "value2", "type": "int64", "required": required},
                        {"name": "value3", "type": "int64", "required": required},
                    ],
                    "optimize_for": optimize_for,
                },
            )
            write_table("//tmp/t1", [{"value1": i, "value2": i, "value3": i} for i in range(4)])

            assert clique.make_query('select count() from "//tmp/t1"') == [{"count()": 4}]
            assert clique.make_query('select count() from "//tmp/t1" prewhere (value1 < 3)') == [{"count()": 3}]
            assert clique.make_query('select count(*) from "//tmp/t1" prewhere (value1 < 3)') == [{"count()": 3}]
            assert clique.make_query('select count(value1) from "//tmp/t1" prewhere (value1 < 3)') == [
                {"count(value1)": 3}
            ]
            assert clique.make_query('select count() from "//tmp/t1" prewhere (value1 < 3)') == [{"count()": 3}]
            assert clique.make_query('select any(0) from "//tmp/t1" prewhere (value1 < 3)') == [{"any(0)": 0}]

            # CHYT-1222
            settings = {"optimize_move_to_prewhere": 1}
            query = 'select count(*) from "//tmp/t1" where value1 != 1 and value2 != 2 and (value1 != 2 or value2 != 3)'
            assert clique.make_query(query, settings=settings) == [{"count()": 2}]
            query = 'select count(*) from "//tmp/t1" where (value1 != 1 and value2 != 2) and 1=1'
            assert clique.make_query(query, settings=settings) == [{"count()": 2}]
            query = 'select count(value3) from "//tmp/t1" where value1 != 1 and value2 != 2 and true'
            assert clique.make_query(query, settings=settings) == [{"count(value3)": 2}]

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
        with Clique(1, export_query_log=True, config_patch=self.get_config_patch(False)) as clique:
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
            clique.make_query_and_validate_prewhered_row_count(
                "select index from \"//tmp/test_table\" where key = 'b_key'", exact=102400
            )
            clique.make_query_and_validate_prewhered_row_count(
                "select index from \"//tmp/test_table\" prewhere key = 'b_key'", exact=1
            )

    @pytest.mark.skipif(True, reason="CHYT-1263")
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

        with Clique(1, export_query_log=True, config_patch=self.get_config_patch(True)) as clique:
            query = "select light from `//tmp/t` where heavy == '{}'".format(rows[42]["heavy"])
            explain_result = clique.make_query("explain syntax " + query)
            print_debug(explain_result)
            assert any("PREWHERE" in row["explain"] for row in explain_result)

            result = clique.make_query_and_validate_prewhered_row_count(
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

        with Clique(1, config_patch=self.get_config_patch(False)) as clique:
            query = "select light from `//tmp/t` where heavy == '{}'".format(rows[42]["heavy"])
            explain_result = clique.make_query("explain syntax " + query)
            print_debug(explain_result)
            assert not any("PREWHERE" in row["explain"] for row in explain_result)

            result = clique.make_query_and_validate_read_row_count(
                query,
                exact=1024,
                verbose=False)
            assert len(result) == 1
            assert result[0]["light"] == 42

    @authors("dakovalkov")
    def test_prefilter_data_slices(self):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "heavy", "type": "string"},
                       {"name": "light", "type": "int64"}],
        })
        write_table("<append=%true>//tmp/t", [{"heavy": "abc", "light": 1}])
        write_table("<append=%true>//tmp/t", [{"heavy": "bcd", "light": 2}])

        config_patch = self.get_config_patch(optimize_move_to_prewhere=False, prefilter_data_slices=True)
        with Clique(1, export_query_log=True, config_patch=config_patch) as clique:
            result = clique.make_query("select * from `//tmp/t` prewhere light = 1", full_response=True)
            assert result.json()["data"] == [{"heavy": "abc", "light": 1}]

            query_log_rows = clique.wait_and_get_query_log_rows(result.headers["X-ClickHouse-Query-Id"])

            assert self._extract_step_summary(query_log_rows, 0, "block_rows") == 3
            assert self._extract_step_summary(query_log_rows, 1, "block_rows") == 1

    @authors("dakovalkov")
    def test_filter_heavy_columns(self):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "heavy", "type": "string"},
                       {"name": "light", "type": "int64"}],
        })

        heavy_value_size = 1000

        write_table("//tmp/t", [{"heavy": "abc", "light": 1}, {"heavy": "b" * heavy_value_size, "light": 2}])

        config_patch = self.get_config_patch(optimize_move_to_prewhere=False)
        with Clique(1, export_query_log=True, config_patch=config_patch) as clique:
            result = clique.make_query("select * from `//tmp/t` prewhere light = 1", full_response=True)

            assert result.json()["data"] == [{"heavy": "abc", "light": 1}]

            query_log_rows = clique.wait_and_get_query_log_rows(result.headers["X-ClickHouse-Query-Id"])

            assert self._extract_step_summary(query_log_rows, 0, "block_rows") == 2
            assert self._extract_step_summary(query_log_rows, 1, "block_rows") == 2
            assert self._extract_step_summary(query_log_rows, 1, "block_bytes") < heavy_value_size

    @authors("dakovalkov")
    def test_filter_by_const_and_low_cardinality_column(self):
        create("table", "//tmp/t", attributes={
            "schema": [
                {"name": "a", "type": "int64", "required": True},
                {"name": "b", "type": "int64", "required": True}
            ],
        })
        write_table("//tmp/t", [{"a": 1, "b": 1}, {"a": 1, "b": 2}])

        config_patch = self.get_config_patch(optimize_move_to_prewhere=False)
        with Clique(1, config_patch=config_patch) as clique:
            # NB: a is not NULL / a is NULL are constant expressions, because column a is required.
            # b = 2 is needed because if the whole expression is constant, CH eliminates it.
            query = "select * from `//tmp/t` prewhere b = 2 and 1 = 1 and 1 and (a is not NULL) and (not (a is NULL))"
            assert clique.make_query(query) == [{"a": 1, "b": 2}]

            query = "select * from `//tmp/t` prewhere cast(a != b, 'LowCardinality(UInt8)')"
            settings = {
                "allow_suspicious_low_cardinality_types": 1,
            }
            assert clique.make_query(query, settings=settings) == [{"a": 1, "b": 2}]

            query = "select * from `//tmp/t` prewhere 1=1 and 1 and b = 2"
            assert clique.make_query(query) == [{"a": 1, "b": 2}]

    @authors("dakovalkov")
    def test_illegal_column_type(self):
        create("table", "//tmp/t", attributes={
            "schema": [{"name": "str", "type": "string"},
                       {"name": "int", "type": "int64"}],
        })

        write_table("//tmp/t", [{"str": "abc", "int": 1}])

        config_patch = self.get_config_patch(optimize_move_to_prewhere=False)
        with Clique(1, config_patch=config_patch) as clique:
            with raises_yt_error(QueryFailedError):
                clique.make_query("select * from `//tmp/t` prewhere int")
            with raises_yt_error(QueryFailedError):
                clique.make_query("select * from `//tmp/t` prewhere str")

    @authors("dakovalkov")
    def test_short_circuit(self):
        create("table", "//tmp/t", attributes={
            "schema": [
                {"name": "a", "type": "int64", "required": True},
                {"name": "b", "type": "string", "required": True},
                {"name": "c", "type": "int64", "required": True},
            ],
        })
        write_table("//tmp/t", [{"a": 1, "b": "abcd", "c": 1}, {"a": 2, "b": "1999-10-01", "c": 2}])

        config_patch = self.get_config_patch(optimize_move_to_prewhere=False)
        with Clique(1, config_patch=config_patch) as clique:
            query = "select * from `//tmp/t` prewhere a = 2 and toDate(b) = '1999-10-01'"
            assert clique.make_query(query) == [{"a": 2, "b": "1999-10-01", "c": 2}]
            # NB: CHYT-1220
            query = """
                select * from `//tmp/t`
                prewhere
                    (a in [2, 3]) and
                    (toDateTime('1999-11-01 00:00:00') >= (toDate(b) as dt)) and
                    (dt >= toDateTime('1999-09-01 00:00:00'))
            """
            assert clique.make_query(query) == [{"a": 2, "b": "1999-10-01", "c": 2}]
