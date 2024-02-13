from yt_env_setup import YTEnvSetup

from yt_commands import (
    alter_table, authors, create, get, insert_rows, partition_tables, raises_yt_error, read_table,
    sorted_dicts, sync_create_cells, sync_flush_table, sync_mount_table, sync_reshard_table, write_table)

from yt.yson import dumps, to_yson_type

from collections import defaultdict

import pytest


class TestPartitionTablesBase(YTEnvSetup):
    def setup_method(self, method):
        super(TestPartitionTablesBase, self).setup_method(method)
        sync_create_cells(1)

    @staticmethod
    def _create_table(table, chunk_count, rows_per_chunk, row_weight, dynamic=False, sorted=True, columnar=False, shard_count=None):
        schema = [
            {"name": "key_0", "type": "string"},
            {"name": "key_1", "type": "string"},
            {"name": "value_0", "type": "string"},
            {"name": "value_1", "type": "string"},
        ]
        if sorted:
            for i in range(len(schema)):
                if schema[i]["name"].startswith("key"):
                    schema[i]["sort_order"] = "ascending"
        create("table", table, attributes={
            "schema": schema,
            "replication_factor": 1,
            "dynamic": dynamic,
            "optimize_for": "scan" if columnar else "lookup",
        })
        if shard_count is not None:
            sync_reshard_table(table, shard_count)

        if dynamic:
            sync_mount_table(table)

        for chunk in range(chunk_count):
            rows = []
            for i in range(rows_per_chunk):
                row = {"key_0": "{:010d}".format(chunk), "key_1": "{:010d}".format(i), "value_0": "", "value_1": ""}
                value_weight = row_weight - len(dumps(row, yson_format="binary"))
                row["value_0"] = "x" * (value_weight // 2)
                row["value_1"] = "x" * (value_weight - len(row["value_0"]))
                rows.append(row)
            if dynamic:
                insert_rows(table, rows)
                sync_flush_table(table)
            else:
                write_table(f"<append=%true>{table}", rows)

        return get(f"{table}/@data_weight")


class TestPartitionTablesCommand(TestPartitionTablesBase):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    @staticmethod
    def check_partitions(table, partitions):
        rows = read_table(table)
        partitioned_rows = []
        for partition in partitions:
            for table_range in partition["table_ranges"]:
                partitioned_rows.extend(read_table(table_range))
        assert sorted_dicts(rows) == sorted_dicts(partitioned_rows)

    @staticmethod
    def check_aggregate_statistics(partitions, chunk_count, row_count, data_weight=None):
        aggregate_statistics = defaultdict(int)
        for partition in partitions:
            assert partition["aggregate_statistics"].keys() == set(["chunk_count", "data_weight", "row_count"])
            for statistics, value in partition["aggregate_statistics"].items():
                aggregate_statistics[statistics] += value
                assert value > 0
            del partition["aggregate_statistics"]
        if data_weight is None:
            data_weight = aggregate_statistics["data_weight"]
        assert aggregate_statistics == {
            "chunk_count": chunk_count,
            "data_weight": data_weight,
            "row_count": row_count,
        }

    @authors("galtsev")
    def test_empty_input(self):
        partitions = partition_tables([], data_weight_per_partition=1)
        assert partitions == []

    @authors("galtsev")
    @pytest.mark.parametrize("dynamic,sorted", [(False, False), (False, True), (True, False)])
    def test_unordered_one_table(self, dynamic, sorted):
        table = "//tmp/sorted-static"
        chunk_count = 6
        rows_per_chunk = 1000
        row_weight = 1000
        data_weight = self._create_table(table, chunk_count, rows_per_chunk, row_weight, dynamic=dynamic, sorted=sorted)

        # TODO(galtsev): yields unequal partitions for data_weight_per_partition=int(2 * row_weight * rows_per_chunk)
        partitions = partition_tables([table], data_weight_per_partition=data_weight // 3)
        self.check_partitions(table, partitions)
        self.check_aggregate_statistics(partitions, chunk_count, chunk_count * rows_per_chunk, data_weight)

        expected_partitions = []
        for lower_limit in range(0, 6000, 2000):
            limits = {"lower_limit": {"row_index": lower_limit}, "upper_limit": {"row_index": lower_limit + 2000}}
            if dynamic and not sorted:
                limits["lower_limit"]["tablet_index"] = 0
                limits["upper_limit"]["tablet_index"] = 0
            partition = {"table_ranges": [to_yson_type(table, attributes={"ranges": [limits]})]}
            expected_partitions.append(partition)

        assert partitions == expected_partitions

    @authors("galtsev")
    def test_multishard_ordered_dynamic_table(self):
        table = "//tmp/multishard_ordered_dynamic_table"
        chunk_count = 6
        rows_per_chunk = 1000
        row_weight = 1000
        data_weight = self._create_table(table, chunk_count, rows_per_chunk, row_weight, dynamic=True, sorted=False, shard_count=4)

        partitions = partition_tables([table], data_weight_per_partition=data_weight // 3)
        self.check_partitions(table, partitions)
        self.check_aggregate_statistics(partitions, chunk_count, chunk_count * rows_per_chunk, data_weight)

        assert len(partitions) >= 3

    @authors("galtsev")
    def test_unordered_one_table_with_columnar_statistics(self):
        table = "//tmp/sorted-static"
        chunk_count = 6
        rows_per_chunk = 1000
        row_weight = 1000
        data_weight = self._create_table(table, chunk_count, rows_per_chunk, row_weight, columnar=True)

        partitions_full = partition_tables([table], data_weight_per_partition=data_weight // 3)
        partitions_half = partition_tables([table + "{key_0,value_1}"],  data_weight_per_partition=data_weight // 3)

        self.check_partitions(table, partitions_full)
        self.check_partitions(table + "{key_0,value_1}", partitions_half)

        assert len(partitions_half) > 0
        assert len(partitions_half) < len(partitions_full)

    @authors("galtsev")
    def test_unordered_one_sorted_dynamic_table(self):
        table = "//tmp/sorted-dynamic"
        chunk_count = 6
        rows_per_chunk = 1000
        row_weight = 1000
        self._create_table(table, chunk_count, rows_per_chunk, row_weight, dynamic=True)

        partitions = partition_tables([table], data_weight_per_partition=2 * row_weight * rows_per_chunk)

        self.check_partitions(table, partitions)

        assert len(partitions) > 1

        def key_into_linear(key, rows_per_chunk):
            return int(key[0]) * rows_per_chunk + int(key[1])

        def key_from_linear(linear_key, rows_per_chunk):
            return ("{:010d}".format(linear_key // rows_per_chunk), "{:010d}".format(linear_key % rows_per_chunk))

        def occurs(key, key_range):
            for limit in ("lower_limit", "upper_limit"):
                operation, bound = key_range[limit]["key_bound"]
                bound = tuple(bound)
                if (
                    (operation == ">=" and key < bound) or
                    (operation == "<=" and key > bound) or
                    (operation == ">" and key <= bound) or
                    (operation == "<" and key >= bound)
                ):
                    return False
            return True

        def test_keys_from_chunks(chunk_count, rows_per_chunk):
            test_keys = set()
            for chunk in range(chunk_count):
                for row in (0, 1, rows_per_chunk - 2, rows_per_chunk - 1):
                    test_keys.add(("{:010d}".format(chunk), "{:010d}".format(row)))
            return test_keys

        def test_keys_from_partitions(partitions):
            test_keys = set()
            for partition in partitions:
                for table_range in partition["table_ranges"]:
                    for key_range in table_range.attributes["ranges"]:
                        for limit in ("lower_limit", "upper_limit"):
                            key = key_range[limit]["key_bound"][1]
                            for delta in (-1, 0, 1):
                                linear_key = key_into_linear(key, rows_per_chunk) + delta
                                if 0 <= linear_key and linear_key < chunk_count * rows_per_chunk:
                                    test_keys.add(key_from_linear(linear_key, rows_per_chunk))
            return test_keys

        def count_occurrences(key, partitions):
            occurrences = 0
            for partition in partitions:
                for table_range in partition["table_ranges"]:
                    for key_range in table_range.attributes["ranges"]:
                        if occurs(key, key_range):
                            occurrences += 1
            return occurrences

        for key in test_keys_from_chunks(chunk_count, rows_per_chunk) | test_keys_from_partitions(partitions):
            assert count_occurrences(key, partitions) == 1

    @authors("galtsev")
    def test_slice_chunk_into_rows(self):
        table = "//tmp/sorted-static"
        chunk_count = 6
        rows_per_chunk = 1000
        row_weight = 1000
        self._create_table(table, chunk_count, rows_per_chunk, row_weight)

        requested_rows = 10

        in_table = '<"ranges"=[{"lower_limit"={"row_index"=0}; "upper_limit"={"row_index"=' + str(requested_rows) + '}}]>' + table
        partitions = partition_tables([in_table], data_weight_per_partition=1)

        self.check_partitions(in_table, partitions)

        assert len(partitions) == requested_rows

    @authors("galtsev")
    @pytest.mark.parametrize("partition_mode", ["ordered", "unordered"])
    @pytest.mark.parametrize(
        "tables_parameters", [
            [("//tmp/sorted-static", 6, 1000, 1000)],
            [("//tmp/sorted-static-1", 6, 1000, 1000), ("//tmp/sorted-static-2", 6, 1000, 100)],
        ],
    )
    def test_max_partition_count_exceeded_strict(self, partition_mode, tables_parameters):
        data_weight = 0
        tables = []
        for (table, chunk_count, rows_per_chunk, row_weight) in tables_parameters:
            data_weight += self._create_table(table, chunk_count, rows_per_chunk, row_weight)
            tables.append(table)

        max_partition_count = 2
        with raises_yt_error("Maximum partition count exceeded"):
            partition_tables(
                tables,
                partition_mode=partition_mode,
                data_weight_per_partition=data_weight // 6,
                max_partition_count=max_partition_count,
                adjust_data_weight_per_partition=False,
            )

    @authors("galtsev")
    @pytest.mark.parametrize("partition_mode", ["ordered", "unordered"])
    @pytest.mark.parametrize(
        "tables_parameters", [
            [("//tmp/sorted-static", 6, 1000, 1000)],
            [("//tmp/sorted-static-1", 6, 1000, 1000), ("//tmp/sorted-static-2", 6, 1000, 100)],
        ],
    )
    def test_adjust_data_weight_per_partition(self, partition_mode, tables_parameters):
        data_weight = 0
        tables = []
        for (table, chunk_count, rows_per_chunk, row_weight) in tables_parameters:
            data_weight += self._create_table(table, chunk_count, rows_per_chunk, row_weight)
            tables.append(table)

        max_partition_count = 2
        partitions = partition_tables(
            tables,
            partition_mode=partition_mode,
            data_weight_per_partition=data_weight // 6,
            max_partition_count=max_partition_count,
        )
        assert len(partitions) == max_partition_count

    @authors("galtsev")
    def test_unordered_two_equal_tables(self):
        table = "//tmp/sorted-static"
        table1 = table + "-1"
        table2 = table + "-2"
        chunk_count = 6
        rows_per_chunk = 1000
        row_weight = 1000
        data_weight1 = self._create_table(table1, chunk_count, rows_per_chunk, row_weight)
        data_weight2 = self._create_table(table2, chunk_count, rows_per_chunk, row_weight)
        data_weight = data_weight1 + data_weight2

        partitions = partition_tables([table1, table2], data_weight_per_partition=data_weight // 3)
        self.check_aggregate_statistics(partitions, 2 * chunk_count, 2 * chunk_count * rows_per_chunk, data_weight)
        assert partitions == [
            {
                "table_ranges": [
                    to_yson_type(table1, attributes={"ranges": [{"lower_limit": {"row_index": 0}, "upper_limit": {"row_index": 4000}}]}),
                    to_yson_type(table2, attributes={"ranges": []}),
                ],
            },
            {
                "table_ranges": [
                    to_yson_type(table1, attributes={"ranges": [{"lower_limit": {"row_index": 4000}, "upper_limit": {"row_index": 6000}}]}),
                    to_yson_type(table2, attributes={"ranges": [{"lower_limit": {"row_index": 0}, "upper_limit": {"row_index": 2000}}]}),
                ],
            },
            {
                "table_ranges": [
                    to_yson_type(table1, attributes={"ranges": []}),
                    to_yson_type(table2, attributes={"ranges": [{"lower_limit": {"row_index": 2000}, "upper_limit": {"row_index": 6000}}]}),
                ],
            },
        ]

    @authors("galtsev")
    def test_unordered_two_unequal_tables(self):
        table = "//tmp/sorted-static"
        table1 = table + "-1"
        table2 = table + "-2"
        chunk_count = 6
        rows_per_chunk = 1000
        row_weight1 = 1000
        row_weight2 = 100
        data_weight1 = self._create_table(table1, chunk_count, rows_per_chunk, row_weight1)
        data_weight2 = self._create_table(table2, chunk_count, rows_per_chunk, row_weight2)
        data_weight = data_weight1 + data_weight2

        partitions = partition_tables([table1, table2], data_weight_per_partition=data_weight // 3)
        self.check_aggregate_statistics(partitions, 2 * chunk_count, 2 * chunk_count * rows_per_chunk, data_weight)
        assert partitions == [
            {
                "table_ranges": [
                    to_yson_type(table1, attributes={"ranges": [{"lower_limit": {"row_index": 0}, "upper_limit": {"row_index": 3000}}]}),
                    to_yson_type(table2, attributes={"ranges": []}),
                ],
            },
            {
                "table_ranges": [
                    to_yson_type(table1, attributes={"ranges": [{"lower_limit": {"row_index": 3000}, "upper_limit": {"row_index": 5000}}]}),
                    to_yson_type(table2, attributes={"ranges": []}),
                ],
            },
            {
                "table_ranges": [
                    to_yson_type(table1, attributes={"ranges": [{"lower_limit": {"row_index": 5000}, "upper_limit": {"row_index": 6000}}]}),
                    to_yson_type(table2, attributes={"ranges": [{"lower_limit": {"row_index": 0}, "upper_limit": {"row_index": 6000}}]}),
                ],
            },
        ]

    @authors("galtsev")
    def test_empty_range_does_not_break_output_order(self):
        table = "//tmp/sorted-static"
        table1 = table + "-1"
        table2 = table + "-2"
        chunk_count = 6
        rows_per_chunk = 1000
        row_weight1 = 1000
        row_weight2 = 100
        data_weight1 = self._create_table(table1, chunk_count, rows_per_chunk, row_weight1)
        data_weight2 = self._create_table(table2, chunk_count, rows_per_chunk, row_weight2)
        data_weight = data_weight1 + data_weight2

        partitions = partition_tables([table1, f"{table2}[#3141:#3141]", table2], data_weight_per_partition=data_weight // 3)
        self.check_aggregate_statistics(partitions, 2 * chunk_count, 2 * chunk_count * rows_per_chunk, data_weight1 + data_weight2)
        assert partitions == [
            {
                "table_ranges": [
                    to_yson_type(table1, attributes={"ranges": [{"lower_limit": {"row_index": 0}, "upper_limit": {"row_index": 3000}}]}),
                    to_yson_type(table2, attributes={"ranges": []}),
                    to_yson_type(table2, attributes={"ranges": []}),
                ],
            },
            {
                "table_ranges": [
                    to_yson_type(table1, attributes={"ranges": [{"lower_limit": {"row_index": 3000}, "upper_limit": {"row_index": 5000}}]}),
                    to_yson_type(table2, attributes={"ranges": []}),
                    to_yson_type(table2, attributes={"ranges": []}),
                ],
            },
            {
                "table_ranges": [
                    to_yson_type(table1, attributes={"ranges": [{"lower_limit": {"row_index": 5000}, "upper_limit": {"row_index": 6000}}]}),
                    to_yson_type(table2, attributes={"ranges": []}),
                    to_yson_type(table2, attributes={"ranges": [{"lower_limit": {"row_index": 0}, "upper_limit": {"row_index": 6000}}]}),
                ],
            },
        ]

    @authors("galtsev")
    def test_empty_range(self):
        table = "//tmp/sorted-static"
        chunk_count = 6
        rows_per_chunk = 1000
        row_weight = 1000
        data_weight = self._create_table(table, chunk_count, rows_per_chunk, row_weight)

        partitions = partition_tables([f"{table}[#3141:#3141]"], data_weight_per_partition=data_weight // 3)
        assert partitions == []

    @authors("galtsev")
    def test_unordered_one_table_row_range(self):
        table = "//tmp/sorted-static"
        chunk_count = 6
        rows_per_chunk = 1000
        row_weight = 1000
        data_weight = self._create_table(table, chunk_count, rows_per_chunk, row_weight)
        row_lower_limit = 111
        row_upper_limit = 5888

        in_table = f"{table}[#{row_lower_limit}:#{row_upper_limit}]"
        partitions = partition_tables([in_table], data_weight_per_partition=data_weight // 3)
        self.check_partitions(in_table, partitions)
        self.check_aggregate_statistics(partitions, chunk_count, row_upper_limit - row_lower_limit)
        assert partitions == [
            {
                "table_ranges": [to_yson_type(table, attributes={"ranges": [{"lower_limit": {"row_index": 111}, "upper_limit": {"row_index": 3000}}]})],
            },
            {
                "table_ranges": [to_yson_type(table, attributes={"ranges": [{"lower_limit": {"row_index": 3000}, "upper_limit": {"row_index": 5000}}]})],
            },
            {
                "table_ranges": [to_yson_type(table, attributes={"ranges": [{"lower_limit": {"row_index": 5000}, "upper_limit": {"row_index": 5888}}]})],
            },
        ]

    @authors("galtsev")
    def test_unordered_one_table_with_columns(self):
        table = "//tmp/sorted-static"
        chunk_count = 6
        rows_per_chunk = 1000
        row_weight = 1000
        data_weight = self._create_table(table, chunk_count, rows_per_chunk, row_weight)

        in_table = table + "{key_1, value_1}"
        partitions = partition_tables([in_table], data_weight_per_partition=data_weight // 3)
        self.check_partitions(in_table, partitions)
        self.check_aggregate_statistics(partitions, chunk_count, chunk_count * rows_per_chunk)
        assert partitions == [
            {
                "table_ranges": [
                    to_yson_type(
                        table,
                        attributes={
                            "columns": ["key_1", "value_1"],
                            "ranges": [
                                {
                                    "lower_limit": {"row_index": lower_limit},
                                    "upper_limit": {"row_index": lower_limit + 3000},
                                },
                            ],
                        },
                    ),
                ],
            }
            for lower_limit in range(0, 6000, 3000)
        ]

    @authors("galtsev")
    def test_attributes_and_columns(self):
        table = "//tmp/sorted-static"
        chunk_count = 6
        rows_per_chunk = 1000
        row_weight = 1000
        data_weight = self._create_table(table, chunk_count, rows_per_chunk, row_weight)

        attributes = "<int=0; list=[1; [2; [3]]]; my_attribute=my_value; three_eighths=0.375; yes=%true>"
        in_table = attributes + table + "{key_1, value_1}"
        partitions = partition_tables([in_table], data_weight_per_partition=data_weight // 3)
        self.check_partitions(in_table, partitions)
        self.check_aggregate_statistics(partitions, chunk_count, chunk_count * rows_per_chunk)
        assert partitions == [
            {
                "table_ranges": [
                    to_yson_type(
                        table,
                        attributes={
                            "columns": ["key_1", "value_1"],
                            "list": [1, [2, [3]]],
                            "my_attribute": "my_value",
                            "three_eighths": 0.375,
                            "int": 0,
                            "yes": True,
                            "ranges": [
                                {
                                    "lower_limit": {"row_index": lower_limit},
                                    "upper_limit": {"row_index": lower_limit + 3000},
                                },
                            ],
                        },
                    ),
                ],
            }
            for lower_limit in range(0, 6000, 3000)
        ]

    @authors("galtsev")
    def test_chunk_keys_wider_than_table_keys(self):
        table = "//tmp/different-key-width"
        wide_key_schema = [
            {"name": name, "type": "int64", "sort_order": "ascending"}
            for name in ["a", "b", "c"]
        ]
        create("table", table, attributes={
            "replication_factor": 1,
            "schema": wide_key_schema,
        })

        write_table(f"<append=%true>{table}", [{"a": i, "b": i, "c": i} for i in range(10)])

        narrow_key_schema = wide_key_schema.copy()
        del narrow_key_schema[-1]["sort_order"]
        alter_table(table, schema=narrow_key_schema)

        chunk_id = get(f"{table}/@chunk_ids")[0]
        assert len(get(f"{table}/@key_columns")) < len(get(f"#{chunk_id}/@min_key"))

        in_table = f"{table}[3:4]"
        partition_tables([in_table], data_weight_per_partition=1)
