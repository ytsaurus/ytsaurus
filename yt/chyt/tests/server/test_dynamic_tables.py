from base import ClickHouseTestBase, Clique

from yt_commands import (create_dynamic_table, set, get, read_table, write_table, authors, sync_mount_table,
                         insert_rows, create, sync_unmount_table, raises_yt_error, generate_timestamp,
                         sync_flush_table, sync_compact_table, sync_freeze_table)

from yt.common import YtError

from yt.test_helpers import assert_items_equal

import yt.yson as yson

import pytest
import time


class TestClickHouseDynamicTables(ClickHouseTestBase):
    NUM_TEST_PARTITIONS = 2

    def _get_config_patch(self):
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

    @staticmethod
    def _create_simple_dynamic_table(path, sort_order="ascending", **attributes):
        attributes["dynamic_store_auto_flush_period"] = yson.YsonEntity()
        if "schema" not in attributes:
            attributes.update(
                {
                    "schema": [
                        {"name": "key", "type": "int64", "sort_order": sort_order},
                        {"name": "value", "type": "string"},
                    ]
                }
            )
        create_dynamic_table(path, **attributes)

    @authors("max42")
    def test_simple(self):
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

        with Clique(1, config_patch=self._get_config_patch()) as clique:
            assert clique.make_query("select * from `//tmp/dt` order by key") == data
            assert clique.make_query("select value from `//tmp/dt` where key == 5 order by key") == [{"value": "foo5"}]
            assert clique.make_query("select key from `//tmp/dt` where value == 'foo7' order by key") == [{"key": 7}]

            sync_unmount_table("//tmp/dt")

            assert clique.make_query("select * from `//tmp/dt` order by key") == data
            assert clique.make_query("select value from `//tmp/dt` where key == 5 order by key") == [{"value": "foo5"}]
            assert clique.make_query("select key from `//tmp/dt` where value == 'foo7' order by key") == [{"key": 7}]

            sync_mount_table("//tmp/dt")

            assert clique.make_query("select * from `//tmp/dt` order by key") == data
            assert clique.make_query("select value from `//tmp/dt` where key == 5 order by key") == [{"value": "foo5"}]
            assert clique.make_query("select key from `//tmp/dt` where value == 'foo7' order by key") == [{"key": 7}]

            with raises_yt_error("CHYT-462"):
                settings = {
                    "chyt.prewhere.prefilter_data_slices": 1,
                }
                assert clique.make_query("select value from `//tmp/dt` prewhere key == 5 order by key", settings=settings) == [
                    {"value": "foo5"}
                ]

    @authors("achulkov2")
    def test_simple_ordered_table(self):
        create(
            "table",
            "//tmp/dt",
            attributes={
                "dynamic": True,
                "schema": [
                    {"name": "data", "type": "string"},
                ],
                "enable_dynamic_store_read": True,
                "dynamic_store_auto_flush_period": yson.YsonEntity(),
            },
        )
        sync_mount_table("//tmp/dt")

        data = [{"data": f"foo{i}"} for i in range(10)]

        for i in range(0, 2):
            insert_rows("//tmp/dt", [data[i]])

        sync_flush_table("//tmp/dt")

        for i in range(2, 5):
            insert_rows("//tmp/dt", [data[i]])

        sync_flush_table("//tmp/dt")

        for i in range(5, 10):
            insert_rows("//tmp/dt", [data[i]])

        # Two chunks + dynamic stores.

        with Clique(1, config_patch=self._get_config_patch()) as clique:
            range_specifier = "{lower_limit={tablet_index=0; row_index=3};upper_limit={tablet_index=0; row_index=8}}"

            assert clique.make_query("select * from `//tmp/dt` order by data") == data
            assert clique.make_query("select * from `//tmp/dt` where data == 'foo7'") == [data[7]]
            assert clique.make_query(f"select * from `<ranges=[{range_specifier}]>//tmp/dt` order by data") == data[3:8]

            sync_unmount_table("//tmp/dt")
            # Now everything is flushed. There is still an empty dynamic store though.

            assert clique.make_query("select * from `//tmp/dt` order by data") == data
            assert clique.make_query("select * from `//tmp/dt` where data == 'foo7'") == [data[7]]
            assert clique.make_query(f"select * from `<ranges=[{range_specifier}]>//tmp/dt` order by data") == data[3:8]

            sync_mount_table("//tmp/dt")

            assert clique.make_query("select * from `//tmp/dt` order by data") == data
            assert clique.make_query("select * from `//tmp/dt` where data == 'foo7'") == [data[7]]
            assert clique.make_query(f"select * from `<ranges=[{range_specifier}]>//tmp/dt` order by data") == data[3:8]

    @authors("achulkov2")
    def test_ordered_table_partial_select(self):
        schema = [
            {"name": "data1", "type": "string"},
            {"name": "data2", "type": "string"},
            {"name": "data3", "type": "string"},
        ]

        create(
            "table",
            "//tmp/dt",
            attributes={
                "dynamic": True,
                "schema": schema,
                "enable_dynamic_store_read": True,
                "dynamic_store_auto_flush_period": yson.YsonEntity(),
            },
        )
        sync_mount_table("//tmp/dt")

        data = [{"data1": f"foo{i}", "data2": f"foo2-{i}", "data3": f"foo3-{i}"} for i in range(10)]

        for i in range(0, 2):
            insert_rows("//tmp/dt", [data[i]])

        sync_flush_table("//tmp/dt")

        for i in range(2, 5):
            insert_rows("//tmp/dt", [data[i]])

        sync_flush_table("//tmp/dt")

        for i in range(5, 10):
            insert_rows("//tmp/dt", [data[i]])

        # Two chunks + dynamic stores.

        with Clique(1, config_patch=self._get_config_patch()) as clique:
            assert clique.make_query("select `data1`, `data3` from `//tmp/dt` order by data1") == [{"data1": row["data1"], "data3": row["data3"]} for row in data]

    @authors("achulkov2")
    def test_ordered_table_concat(self):
        schema = [
            {"name": "data", "type": "string"},
        ]

        create(
            "table",
            "//tmp/dt",
            attributes={
                "dynamic": True,
                "schema": schema,
                "enable_dynamic_store_read": True,
                "dynamic_store_auto_flush_period": yson.YsonEntity(),
            },
        )
        sync_mount_table("//tmp/dt")

        data = [{"data": f"foo{i}"} for i in range(10)]

        for i in range(0, 2):
            insert_rows("//tmp/dt", [data[i]])

        sync_flush_table("//tmp/dt")

        for i in range(2, 5):
            insert_rows("//tmp/dt", [data[i]])

        sync_flush_table("//tmp/dt")

        for i in range(5, 10):
            insert_rows("//tmp/dt", [data[i]])

        # Two chunks + dynamic stores.

        create("table", "//tmp/st", attributes={"schema": schema}, force=True)
        write_table("//tmp/st", data)

        with Clique(1, config_patch=self._get_config_patch()) as clique:
            assert clique.make_query("select * from concatYtTables(`//tmp/dt`) order by data") == data
            assert clique.make_query("select * from concatYtTables(`//tmp/dt`, `//tmp/dt`) order by data") == sorted(data * 2, key=lambda row: row["data"])
            assert clique.make_query("select * from concatYtTables(`//tmp/dt`, `//tmp/st`) order by data") == sorted(data * 2, key=lambda row: row["data"])
            assert clique.make_query("select * from concatYtTables(`//tmp/st`, `//tmp/dt`) order by data") == sorted(data * 2, key=lambda row: row["data"])

    @authors("dakovalkov")
    def test_ordered_table_insert(self):
        create(
            "table",
            "//tmp/dt",
            attributes={
                "dynamic": True,
                "schema": [{"name": "data", "type": "string"}],
                "enable_dynamic_store_read": True,
                "dynamic_store_auto_flush_period": yson.YsonEntity(),
            },
        )
        sync_mount_table("//tmp/dt")

        with Clique(1, config_patch=self._get_config_patch()) as clique:
            clique.make_query("insert into `//tmp/dt` select 'abcd' as data")
            assert clique.make_query("select data from `//tmp/dt`") == [{"data": "abcd"}]

    @authors("dakovalkov")
    @pytest.mark.skipif(True, reason="CHYT-506")
    def test_ordered_table_virtual_columns(self):
        schema = [
            {"name": "data", "type": "string"},
        ]

        create(
            "table",
            "//tmp/dt",
            attributes={
                "dynamic": True,
                "schema": schema,
                "enable_dynamic_store_read": True,
                "dynamic_store_auto_flush_period": yson.YsonEntity(),
            },
        )
        sync_mount_table("//tmp/dt")

        insert_rows("//tmp/dt", [{"data": "abcd_1"}])
        sync_flush_table("//tmp/dt")

        insert_rows("//tmp/dt", [{"data": "abcd_2"}])

        # One chunks + dynamic stores.

        create("table", "//tmp/st", attributes={"schema": schema}, force=True)
        write_table("//tmp/st", [{"data": "abcd_3"}])

        with Clique(1, config_patch=self._get_config_patch()) as clique:
            query = '''
                select data, $table_name, $table_path, $table_index
                from ytTables('//tmp/dt', '//tmp/st')
                order by data
            '''
            assert clique.make_query(query) == [
                {"data": "abcd_1", "$table_name": "dt", "$table_path": "//tmp/dt", "$table_index": 0},
                {"data": "abcd_2", "$table_name": "dt", "$table_path": "//tmp/dt", "$table_index": 0},
                {"data": "abcd_3", "$table_name": "st", "$table_path": "//tmp/st", "$table_index": 1},
            ]

    # Tests below are obtained from similar already existing tests on dynamic tables.

    @authors("max42")
    @pytest.mark.parametrize("instance_count", [1, 2])
    @pytest.mark.timeout(250)
    def test_map_on_dynamic_table(self, instance_count):
        self._create_simple_dynamic_table("//tmp/t", sort_order="ascending")
        set("//tmp/t/@min_compaction_store_count", 5)

        rows = [{"key": i, "value": str(i)} for i in range(10)]
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", rows)
        sync_unmount_table("//tmp/t")

        with Clique(instance_count, config_patch=self._get_config_patch()) as clique:
            assert_items_equal(clique.make_query("select * from `//tmp/t`"), rows)

            rows1 = [{"key": i, "value": str(i + 1)} for i in range(3)]
            sync_mount_table("//tmp/t")
            insert_rows("//tmp/t", rows1)
            sync_unmount_table("//tmp/t")

            rows2 = [{"key": i, "value": str(i + 2)} for i in range(2, 6)]
            sync_mount_table("//tmp/t")
            insert_rows("//tmp/t", rows2)
            sync_unmount_table("//tmp/t")

            rows3 = [{"key": i, "value": str(i + 3)} for i in range(7, 8)]
            sync_mount_table("//tmp/t")
            insert_rows("//tmp/t", rows3)
            sync_unmount_table("//tmp/t")

            assert len(get("//tmp/t/@chunk_ids")) == 4

            def update(new):
                def update_row(row):
                    for r in rows:
                        if r["key"] == row["key"]:
                            r["value"] = row["value"]
                            return
                    rows.append(row)

                for row in new:
                    update_row(row)

            update(rows1)
            update(rows2)
            update(rows3)

            assert_items_equal(clique.make_query("select * from `//tmp/t`"), rows)

    @authors("max42")
    @pytest.mark.parametrize("instance_count", [1, 2])
    def test_dynamic_table_timestamp(self, instance_count):
        self._create_simple_dynamic_table("//tmp/t", enable_dynamic_store_read=False)

        rows = [{"key": i, "value": str(i)} for i in range(2)]
        sync_mount_table("//tmp/t")
        insert_rows("//tmp/t", rows)

        time.sleep(1)
        ts = generate_timestamp()

        sync_flush_table("//tmp/t")
        insert_rows("//tmp/t", [{"key": i, "value": str(i + 1)} for i in range(2)])
        sync_flush_table("//tmp/t")
        sync_compact_table("//tmp/t")

        settings = {"chyt.dynamic_table.enable_dynamic_store_read": 0}

        with Clique(instance_count, config_patch=self._get_config_patch()) as clique:
            assert_items_equal(clique.make_query("select * from `<timestamp=%s>//tmp/t`" % ts, settings=settings), rows)

            # TODO(max42): these checks are not working for now but TBH I can't imagine
            # anybody using dynamic table timestamps in CHYT.
            # To make them work, invoke ValidateDynamicTableTimestamp helper somewhere
            # in table preparation pipeline. Note that required table attributes are
            # taken from cache, thus are stale.
            #
            # with pytest.raises(YtError):
            #     clique.make_query("select * from `<timestamp=%s>//tmp/t`" % MinTimestamp)
            # insert_rows("//tmp/t", rows)
            # with pytest.raises(YtError):
            #     clique.make_query("select * from `<timestamp=%s>//tmp/t`" % generate_timestamp())

    @authors("max42")
    @pytest.mark.parametrize("instance_count", [1, 2])
    def test_basic_read1(self, instance_count):
        self._create_simple_dynamic_table("//tmp/t")
        set("//tmp/t/@enable_dynamic_store_read", True)
        sync_mount_table("//tmp/t")

        rows = [{"key": i, "value": str(i)} for i in range(300)]

        insert_rows("//tmp/t", rows[:150])

        with Clique(instance_count, config_patch=self._get_config_patch()) as clique:
            assert clique.make_query("select * from `//tmp/t` order by key", verbose=False) == rows[:150]
            assert clique.make_query("select * from `//tmp/t[10:50]` order by key", verbose=False) == rows[10:50]

            ts = generate_timestamp()
            ypath_with_ts = "<timestamp={}>//tmp/t".format(ts)

            insert_rows("//tmp/t", rows[150:])

            assert clique.make_query("select * from `//tmp/t` order by key", verbose=False) == rows
            assert clique.make_query("select * from `{}` order by key".format(ypath_with_ts),
                                     verbose=False) == rows[:150]

            sync_freeze_table("//tmp/t")
            assert clique.make_query("select * from `//tmp/t` order by key", verbose=False) == rows

    @authors("dakovalkov")
    @pytest.mark.parametrize("sorted", [True, False])
    def test_enable_dynamic_store_read(self, sorted):
        schema = [
            {"name": "key", "type": "int64"},
            {"name": "value", "type": "string"},
        ]
        if sorted:
            schema[0]["sort_order"] = "ascending"

        rows = [{"key": i, "value": str(i)} for i in range(30)]

        create("table", "//tmp/t", attributes={"schema": schema}, force=True)
        write_table("//tmp/t", rows)

        create_dynamic_table(
            "//tmp/dyn_on",
            schema=schema,
            enable_dynamic_store_read=True,
            dynamic_store_auto_flush_period=yson.YsonEntity(),
        )
        sync_mount_table("//tmp/dyn_on")
        insert_rows("//tmp/dyn_on", rows)

        create_dynamic_table(
            "//tmp/dyn_off",
            schema=schema,
            enable_dynamic_store_read=False,
            dynamic_store_auto_flush_period=yson.YsonEntity(),
        )
        sync_mount_table("//tmp/dyn_off")
        insert_rows("//tmp/dyn_off", rows)

        settings = {"chyt.dynamic_table.enable_dynamic_store_read": 0}

        with Clique(2, config_patch=self._get_config_patch()) as clique:
            assert clique.make_query("select * from `//tmp/t` order by key") == rows
            assert clique.make_query("select * from `//tmp/t` order by key", settings=settings) == rows

            assert clique.make_query("select * from `//tmp/dyn_on` order by key") == rows
            assert clique.make_query("select * from `//tmp/dyn_on` order by key", settings=settings) == []

            with pytest.raises(YtError):
                clique.make_query("select * from `//tmp/dyn_off` order by key")
            assert clique.make_query("select * from `//tmp/dyn_off` order by key", settings=settings) == []

    @authors("dakovalkov")
    @pytest.mark.timeout(150)
    def test_write_to_dynamic_table(self):
        self._create_simple_dynamic_table("//tmp/t", enable_dynamic_store_read=True)
        sync_mount_table("//tmp/t")

        with Clique(1, config_patch=self._get_config_patch()) as clique:
            clique.make_query("insert into `//tmp/t` select number as key, toString(number) as value from numbers(10) "
                              "order by key")
            assert read_table("//tmp/t") == [{"key": i, "value": str(i)} for i in range(10)]

            clique.make_query(
                "insert into `//tmp/t` select number as key, toString(number) as value from numbers(25000)"
            )
            rows = [{"key": i, "value": str(i)} for i in range(25000)]
            # Somehow select * from ... works faster than read_table.
            written_rows = clique.make_query("select * from `//tmp/t` order by key", verbose=False)
            # "assert rows == written_rows" is a bad idea. In case of error printing diff will take too long.
            # These checks can detect simple failures and avoid printing 500'000 rows.
            assert len(rows) == len(written_rows)
            assert rows[0] == written_rows[0]
            assert rows[-1] == written_rows[-1]
            rows_are_equal = rows == written_rows
            assert rows_are_equal

            with raises_yt_error("Overriding dynamic tables"):
                clique.make_query(
                    "insert into `<append=%false>//tmp/t` select number as key, "
                    "toString(number) as value from numbers(10)")

    @authors("dakovalkov")
    def test_write_to_unmounted_dynamic_table(self):
        self._create_simple_dynamic_table("//tmp/t", enable_dynamic_store_read=True)

        with Clique(1, config_patch=self._get_config_patch()) as clique:
            # To speed up the test we disable the backoff timeout.
            fast_error_settings = {"chyt.dynamic_table.write_retry_backoff": 0}
            with pytest.raises(YtError):
                clique.make_query(
                    "insert into `//tmp/t` select number as key, toString(number) as value from numbers(10)",
                    settings=fast_error_settings,
                )

            t = clique.make_async_query(
                "insert into `//tmp/t` select number as key, toString(number) as value from numbers(20) "
                "order by key"
            )
            time.sleep(2)
            sync_mount_table("//tmp/t")
            t.join()
            assert read_table("//tmp/t") == [{"key": i, "value": str(i)} for i in range(20)]


class TestClickHouseDynamicTablesFetchFromTablets(TestClickHouseDynamicTables):
    def _get_config_patch(self):
        config_patch = super(TestClickHouseDynamicTablesFetchFromTablets, self)._get_config_patch()
        config_patch["yt"]["settings"]["dynamic_table"]["fetch_from_tablets"] = True
        return config_patch
