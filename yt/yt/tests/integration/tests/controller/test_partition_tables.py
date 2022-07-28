from yt_env_setup import YTEnvSetup

from yt_commands import (
    authors, create, get, insert_rows, partition_tables, raises_yt_error, sync_create_cells, sync_flush_table, sync_mount_table, write_table)

from yt.yson import to_yson_type


class TestPartitionTablesBase:
    @staticmethod
    def _create_table(table, chunk_count, rows_per_chunk, row_weight, dynamic=False):
        schema = [
            {"name": "key_0", "type": "string", "sort_order": "ascending"},
            {"name": "key_1", "type": "string", "sort_order": "ascending"},
            {"name": "value", "type": "string"},
        ]
        if dynamic:
            sync_create_cells(1)
        create("table", table, attributes={"schema": schema, "replication_factor": 1, "dynamic": dynamic})
        if dynamic:
            sync_mount_table(table)

        for chunk in range(chunk_count):
            rows = []
            for i in range(rows_per_chunk):
                row = {"key_0": "{:010d}".format(chunk), "key_1": "{:010d}".format(i), "value": ""}
                value_weight = row_weight - len(to_yson_type(row))
                row["value"] = "x" * value_weight
                rows.append(row)
            if dynamic:
                insert_rows(table, rows)
                sync_flush_table(table)
            else:
                write_table(f"<append=%true>{table}", rows)

        return get(f"{table}/@data_weight")


class TestPartitionTablesCommand(YTEnvSetup, TestPartitionTablesBase):
    NUM_MASTERS = 1
    NUM_NODES = 1
    NUM_SCHEDULERS = 1
    USE_DYNAMIC_TABLES = True

    @authors("galtsev")
    def test_empty_input(self):
        partitions = partition_tables([], data_weight_per_partition=1)
        assert partitions == []

    @authors("galtsev")
    def test_unordered_one_table(self):
        table = "//tmp/sorted-static"
        chunk_count = 6
        rows_per_chunk = 1000
        row_weight = 1000
        data_weight = self._create_table(table, chunk_count, rows_per_chunk, row_weight)

        # TODO(galtsev): yields unequal partitions for data_weight_per_partition=int(2 * row_weight * rows_per_chunk)
        partitions = partition_tables([table], data_weight_per_partition=data_weight // 3)
        assert partitions == [
            {
                "table_ranges": [to_yson_type(table, attributes={"ranges": [{"lower_limit": {"row_index": lower_limit}, "upper_limit": {"row_index": lower_limit + 2000}}]})],
            }
            for lower_limit in range(0, 6000, 2000)
        ]

    @authors("galtsev")
    def test_unordered_one_dynamic_table(self):
        table = "//tmp/sorted-dynamic"
        chunk_count = 6
        rows_per_chunk = 1000
        row_weight = 1000
        self._create_table(table, chunk_count, rows_per_chunk, row_weight, dynamic=True)

        partitions = partition_tables([table], data_weight_per_partition=2 * row_weight * rows_per_chunk)
        assert partitions == [
            {
                "table_ranges": [to_yson_type(table, attributes={"ranges": [{"lower_limit": {}, "upper_limit": {}}]})],
            }
        ]

    @authors("galtsev")
    def test_max_partition_count_exceeded(self):
        table = "//tmp/sorted-static"
        chunk_count = 6
        rows_per_chunk = 1000
        row_weight = 1000
        data_weight = self._create_table(table, chunk_count, rows_per_chunk, row_weight)

        max_partition_count = 1
        with raises_yt_error(f'Maximum partition count exceeded: {max_partition_count}'):
            partition_tables([table], data_weight_per_partition=data_weight // 6, max_partition_count=max_partition_count)

    @authors("galtsev")
    def test_unordered_two_equal_tables(self):
        table = "//tmp/sorted-static"
        table1 = table + '-1'
        table2 = table + '-2'
        chunk_count = 6
        rows_per_chunk = 1000
        row_weight = 1000
        data_weight1 = self._create_table(table1, chunk_count, rows_per_chunk, row_weight)
        data_weight2 = self._create_table(table2, chunk_count, rows_per_chunk, row_weight)
        data_weight = data_weight1 + data_weight2

        partitions = partition_tables([table1, table2], data_weight_per_partition=data_weight // 3)
        assert partitions == [
            {
                "table_ranges": [
                    to_yson_type(table1, attributes={"ranges": [{"lower_limit": {"row_index": 0}, "upper_limit": {"row_index": 4000}}]}),
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
                    to_yson_type(table2, attributes={"ranges": [{"lower_limit": {"row_index": 2000}, "upper_limit": {"row_index": 6000}}]}),
                ],
            },
        ]

    @authors("galtsev")
    def test_unordered_two_unequal_tables(self):
        table = "//tmp/sorted-static"
        table1 = table + '-1'
        table2 = table + '-2'
        chunk_count = 6
        rows_per_chunk = 1000
        row_weight1 = 1000
        row_weight2 = 100
        data_weight1 = self._create_table(table1, chunk_count, rows_per_chunk, row_weight1)
        data_weight2 = self._create_table(table2, chunk_count, rows_per_chunk, row_weight2)
        data_weight = data_weight1 + data_weight2

        partitions = partition_tables([table1, table2], data_weight_per_partition=data_weight // 3)
        assert partitions == [
            {
                "table_ranges": [
                    to_yson_type(table1, attributes={"ranges": [{"lower_limit": {"row_index": 0}, "upper_limit": {"row_index": 3000}}]}),
                ],
            },
            {
                "table_ranges": [
                    to_yson_type(table1, attributes={"ranges": [{"lower_limit": {"row_index": 3000}, "upper_limit": {"row_index": 5000}}]}),
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
    def test_empty_range(self):
        table = "//tmp/sorted-static"
        chunk_count = 6
        rows_per_chunk = 1000
        row_weight = 1000
        data_weight = self._create_table(table, chunk_count, rows_per_chunk, row_weight)

        partitions = partition_tables([f'{table}[#3141:#3141]'], data_weight_per_partition=data_weight // 3)
        assert partitions == []

    @authors("galtsev")
    def test_unordered_one_table_row_range(self):
        table = "//tmp/sorted-static"
        chunk_count = 6
        rows_per_chunk = 1000
        row_weight = 1000
        data_weight = self._create_table(table, chunk_count, rows_per_chunk, row_weight)

        partitions = partition_tables([f'{table}[#111:#5888]'], data_weight_per_partition=data_weight // 3)
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
