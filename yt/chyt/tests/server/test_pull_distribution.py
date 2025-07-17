from yt_commands import (authors, create, write_table)

from base import ClickHouseTestBase, Clique

from yt.test_helpers import assert_items_equal

import pytest


class TestPullDistributionMode(ClickHouseTestBase):
    NUM_TEST_PARTITIONS = 4

    CHUNKS_COUNT = 20

    STREAMS_PER_SECONDARY_QUERY = 2
    TASK_COUNT_INCREASE_FACTOR = 2.

    @classmethod
    def _get_config_patch(cls):
        return {
            "yt": {
                "settings": {
                    "execution": {
                        "enable_input_specs_pulling": True,
                        "task_count_increase_factor": cls.TASK_COUNT_INCREASE_FACTOR,
                        "input_streams_per_secondary_query": cls.STREAMS_PER_SECONDARY_QUERY,
                    }
                }
            }
        }

    @staticmethod
    def extract_pulling_stats(clique, query_id):
        query_log_rows = clique.wait_and_get_query_log_rows(query_id)

        secondary_queries_count = 0
        pull_mode_queries_count = 0
        processed_reader_count = 0
        pull_request_count = 0

        for row in query_log_rows:
            statistics = row["chyt_query_statistics"]
            if "secondary_query_source" in statistics:
                secondary_queries_count += 1
                processed_reader_count += statistics["secondary_query_source"]["processed_reader_count"]["sum"]

            if "secondary_query_read_task_puller" in statistics:
                pull_mode_queries_count += 1
                pull_request_count += statistics["secondary_query_read_task_puller"]["do_pull_task_us"]["count"]

        return {
            "secondary_queries_count": secondary_queries_count,
            "pull_mode_queries_count": pull_mode_queries_count,
            "processed_reader_count": processed_reader_count,
            "pull_request_count": pull_request_count,
        }

    @authors("buyval01")
    @pytest.mark.parametrize("instance_count", [1, 2])
    def test_simple_select(self, instance_count):
        create("table", "//tmp/t", attributes={"schema": [{"name": "a", "type": "int64"}]})
        for i in range(self.CHUNKS_COUNT):
            write_table("<append=%true>//tmp/t", [{"a": 2 * i}, {"a": 2 * i + 1}])

        with Clique(instance_count, export_query_log=True, config_patch=self._get_config_patch()) as clique:
            query = 'select * from "//tmp/t"'
            result = clique.make_query(query, full_response=True)
            assert_items_equal(result.json()["data"], [{"a": i} for i in range(2 * self.CHUNKS_COUNT)])

            pulling_stats = self.extract_pulling_stats(
                clique,
                result.headers["X-ClickHouse-Query-Id"]
            )
            tasks_count = instance_count * self.STREAMS_PER_SECONDARY_QUERY * self.TASK_COUNT_INCREASE_FACTOR
            assert pulling_stats['processed_reader_count'] == tasks_count
            assert pulling_stats['pull_request_count'] >= tasks_count

    @authors("buyval01")
    @pytest.mark.parametrize("instance_count", [1, 2])
    def test_sorted_join(self, instance_count):
        create("table", "//tmp/t1", attributes={"schema": [{"name": "a", "type": "int64", "sort_order": "ascending"}]})
        for chunk_index in range(self.CHUNKS_COUNT):
            row = [{"a": i} for i in range(20 * chunk_index, 20 * (chunk_index + 1))]
            write_table("<append=%true>//tmp/t1", row)

        create(
            "table",
            "//tmp/t2",
            attributes={
                "schema": [
                    {"name": "a", "type": "int64", "sort_order": "ascending"},
                    {"name": "b", "type": "string"}
                ]
            }
        )

        str_size = 1000
        for chunk_index in range(self.CHUNKS_COUNT):
            row = [{"a": i, "b": "a" * str_size} for i in range(20 * chunk_index, 20 * (chunk_index + 1), 2)]
            write_table("<append=%true>//tmp/t2", row)

        with Clique(instance_count, export_query_log=True, config_patch=self._get_config_patch()) as clique:
            query = 'select * from "//tmp/t1" t1 inner join "//tmp/t2" t2 using a'
            result = clique.make_query(query, full_response=True)
            assert_items_equal(result.json()["data"], [{"a": 2 * i, "b": "a" * str_size} for i in range(10 * self.CHUNKS_COUNT)])

            pulling_stats = self.extract_pulling_stats(
                clique,
                result.headers["X-ClickHouse-Query-Id"])

            tasks_count = instance_count * self.STREAMS_PER_SECONDARY_QUERY * self.TASK_COUNT_INCREASE_FACTOR
            # It is necessary to multiply by two,
            # because in one task there will be two readers for each of the tables
            assert pulling_stats['processed_reader_count'] == 2 * tasks_count
            assert pulling_stats['pull_request_count'] >= tasks_count

    @authors("buyval01")
    @pytest.mark.parametrize("instance_count", [1, 2])
    def test_concat_tables(self, instance_count):
        create("map_node", "//tmp/test_dir")
        for table_index in range(1, 7):
            create(
                "table",
                "//tmp/test_dir/table_" + str(table_index),
                attributes={"schema": [{"name": "i", "type": "int64"}]},
                )
            write_table("//tmp/test_dir/table_" + str(table_index), [{"i": table_index}])
        with Clique(instance_count, export_query_log=True, config_patch=self._get_config_patch()) as clique:
            result = clique.make_query(
                "select * from concatYtTablesRange('//tmp/test_dir', 'table_2', 'table_5') order by i",
                full_response=True
            )
            assert result.json()['data'] == [{"i": 2}, {"i": 3}, {"i": 4}, {"i": 5}]

            pulling_stats = self.extract_pulling_stats(
                clique,
                result.headers["X-ClickHouse-Query-Id"])

            assert pulling_stats["pull_mode_queries_count"] == instance_count

    @authors("buyval01")
    @pytest.mark.parametrize("instance_count", [1, 2])
    def test_global_join(self, instance_count):
        create("table", "//tmp/t1", attributes={"schema": [{"name": "a", "type": "int64"}]})
        for i in range(50):
            write_table("<append=%true>//tmp/t1", [{"a": 2 * i}, {"a": 2 * i + 1}])
        create("table", "//tmp/t2", attributes={"schema": [{"name": "a", "type": "int64"}]})
        for i in range(50):
            write_table("<append=%true>//tmp/t2", [{"a": 2 * i}])

        with Clique(instance_count, export_query_log=True, config_patch=self._get_config_patch()) as clique:
            query = 'select * from "//tmp/t1" t1 global join "//tmp/t2" t2 using a'
            result = clique.make_query(query, full_response=True)
            assert_items_equal(result.json()["data"], [{"a": 2 * i} for i in range(50)])

            pulling_stats = self.extract_pulling_stats(
                clique,
                result.headers["X-ClickHouse-Query-Id"])

            # Right table will be read by separate secondary queries using pull mode
            assert pulling_stats["secondary_queries_count"] == 2 * instance_count
            assert pulling_stats["pull_mode_queries_count"] == 2 * instance_count

    @authors("buyval01")
    @pytest.mark.parametrize("instance_count", [1, 2])
    def test_filter_joined_subquery(self, instance_count):
        create("table", "//tmp/t1", attributes={"schema": [{"name": "a", "type": "int64", "sort_order": "ascending"}]})
        write_table("//tmp/t1", [
            {"a": 1},
            {"a": 3},
        ])
        write_table("<append=%true>//tmp/t1", [
            {"a": 13},
            {"a": 17},
        ])

        create("table", "//tmp/t2", attributes={"schema": [{"name": "b", "type": "int64"}]})
        write_table("//tmp/t2", [
            {"b": 1},
            {"b": 2},
            {"b": 3},
        ])
        write_table("<append=%true>//tmp/t2", [
            {"b": 13},
            {"b": 15},
            {"b": 17},
        ])

        with Clique(instance_count, export_query_log=True, config_patch=self._get_config_patch()) as clique:
            query = '''
                select * from "//tmp/t1" t1 right join (select * from "//tmp/t2") t2 on t1.a = t2.b order by b
            '''
            result = clique.make_query(query, full_response=True)
            assert result.json()["data"] == [
                {"a": 1, "b": 1},
                {"a": None, "b": 2},
                {"a": 3, "b": 3},
                {"a": 13, "b": 13},
                {"a": None, "b": 15},
                {"a": 17, "b": 17},
            ]

            pulling_stats = self.extract_pulling_stats(
                clique,
                result.headers["X-ClickHouse-Query-Id"])

            # Joined subquery filtering is disabled for pull distribution mode.
            # That is why join will be executed on coordinator.
            # Separate tables will be read using pull mode.
            assert pulling_stats["secondary_queries_count"] == 2 * instance_count
            assert pulling_stats["pull_mode_queries_count"] == 2 * instance_count
