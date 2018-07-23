from .helpers import set_config_option

import yt.wrapper as yt

import pytest


@pytest.mark.usefixtures("yt_env")
class TestRandomSample(object):
    def test_random_sample_locally(self):
        yt.write_table("//tmp/test_table", [{"x": i} for i in range(1000)])

        with set_config_option("max_row_count_for_local_sampling", 100):
            yt.sample_rows_from_table("//tmp/test_table", "//tmp/test_output_first", 30)
            yt.sample_rows_from_table("//tmp/test_table", "//tmp/test_output_second", 30)

            assert yt.get("//tmp/test_output_first/@row_count") == 30

            first_table_iterator = yt.read_table("//tmp/test_output_first")
            second_table_iterator = yt.read_table("//tmp/test_output_second")

            assert sorted([row["x"] for row in first_table_iterator]) != sorted([row["x"] for row in second_table_iterator])

    def test_random_sample_in_operation(self):
        yt.write_table("//tmp/test_table", [{"x": i} for i in range(1000)])

        with set_config_option("max_row_count_for_local_sampling", 100):
            yt.sample_rows_from_table("//tmp/test_table", "//tmp/test_output_first", 300)
            yt.sample_rows_from_table("//tmp/test_table", "//tmp/test_output_second", 300)

            assert yt.get("//tmp/test_output_first/@row_count") == 300

            first_table_iterator = yt.read_table("//tmp/test_output_first")
            second_table_iterator = yt.read_table("//tmp/test_output_second")

            assert sorted([row["x"] for row in first_table_iterator]) != sorted([row["x"] for row in second_table_iterator])

            yt.sample_rows_from_table("//tmp/test_table", "//tmp/test_output_first", 1000)
            yt.sample_rows_from_table("//tmp/test_table", "//tmp/test_output_second", 1000)

            assert yt.get("//tmp/test_output_first/@row_count") == 1000

            first_table_iterator = yt.read_table("//tmp/test_output_first")
            second_table_iterator = yt.read_table("//tmp/test_output_second")

            assert sorted([row["x"] for row in first_table_iterator]) == sorted([row["x"] for row in second_table_iterator]) == list(range(1000))
