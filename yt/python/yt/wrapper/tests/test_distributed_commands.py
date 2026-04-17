from yt.testlib import authors
from .helpers import TEST_DIR, set_config_option, check_rows_equality, inject_http_read_error

import yt.wrapper as yt

import pytest


@pytest.mark.usefixtures("yt_env_with_rpc")
class TestCypressCommands(object):

    @authors("denvr")
    def test_distributed_write_command(self):
        client = yt.YtClient(config=yt.config.config)
        client.config["write_retries"]["rows_chunk_size"] = 2
        sorted_table_path = "//tmp/target_table"
        client.create("table", sorted_table_path, attributes={"schema": [{"name": "f_str", "type": "string", "sort_order": "ascending"}]})

        if yt.config["backend"] == "http" and yt.driver.get_api_version() == "v3":
            with pytest.raises(yt.YtError) as pytest_err:
                write_session, write_cookies = client.start_distributed_write_session(path=sorted_table_path, fragments_count=1)
            assert pytest_err.value.is_command_not_supported()
        else:
            write_session, write_cookies = client.start_distributed_write_session(path=sorted_table_path, fragments_count=2, timeout=1000)

            with pytest.raises(yt.YtError) as pytest_err:
                write_result_1 = client.write_table_fragment(cookie=write_cookies[0], input_stream=b"{\"f_str\": \"bar_2\"}\n{\"f_str\": \"bar_1\"}\n", format="json", raw=True)
            assert pytest_err.value.is_sorting_order_violated()

            write_result_1 = client.write_table_fragment(cookie=write_cookies[0], input_stream=b"{f_str=_foo_;}", format="yson", raw=True)
            write_result_1 = client.write_table_fragment(cookie=write_cookies[0], input_stream=b"{\"f_str\": \"bar_9\"}", format="json", raw=True)
            write_result_2 = client.write_table_fragment(cookie=write_cookies[1], input_stream=[{"f_str": "bar_1"}, {"f_str": "bar_2"}, {"f_str": "bar_3"}])
            client.ping_distributed_write_session(session=write_session)
            client.finish_distributed_write_session(session=write_session, results=[write_result_2, write_result_1])
            assert list(client.read_table(sorted_table_path)) == [{"f_str": "bar_1"}, {"f_str": "bar_2"}, {"f_str": "bar_3"}, {"f_str": "bar_9"}]
            assert client.get(f"{sorted_table_path}/@sorted")

    @authors("alex-shishkin", "denvr")
    def test_partition_tables(self):
        if yt.driver.get_api_version() == "v3":
            pytest.skip()

        client = yt.YtClient(config=yt.config.config)

        table1_path = TEST_DIR + "/table1"
        client.create("table", table1_path)
        rows1 = [{"value": i} for i in range(5)]
        client.write_table(table1_path, rows1)

        # read partitions by ranges
        partitions = client.partition_tables([table1_path], partition_mode="ordered", data_weight_per_partition=10)
        assert len(partitions) <= 3 and len(partitions) > 1
        table_data = []
        for partition in partitions:
            table_ranges = partition["table_ranges"]
            assert len(table_ranges) == 1
            range_data = list(client.read_table(table_ranges[0]))
            assert len(range_data) <= 2
            table_data.extend(range_data)
        check_rows_equality(rows1, table_data)

        # read partitions by cookie
        partitions = client.partition_tables([table1_path], partition_mode="ordered", data_weight_per_partition=10, enable_cookies=True)
        table_data = []
        for cookie in [partition["cookie"] for partition in partitions]:
            range_data = list(client.read_table_partition(cookie))
            table_data.extend(range_data)
        check_rows_equality(rows1, table_data)

        # partition muliple tables
        table2_path = TEST_DIR + "/table2"
        client.create("table", table2_path)
        rows2 = [{"value": 10 * i} for i in range(20)]
        client.write_table(table2_path, rows2)
        partitions = client.partition_tables([table1_path, table2_path], partition_mode="ordered", data_weight_per_partition=10)
        assert len(partitions) <= 15 and len(partitions) > 10
        assert len(partitions[0]["table_ranges"]) == 2
        assert str(partitions[0]["table_ranges"][0]) == table1_path
        assert str(partitions[0]["table_ranges"][1]) == table2_path
        table_data = []
        for partition in partitions:
            for table_range in partition["table_ranges"]:
                table_data.extend(list(client.read_table(table_range)))
        check_rows_equality(rows1 + rows2, table_data)

        # partition range tables
        table2_path_with_ranges = "<ranges=[{lower_limit={row_index=5;};upper_limit={row_index=15;};};];>" + table2_path
        partitions = client.partition_tables(table2_path_with_ranges, partition_mode="ordered", data_weight_per_partition=10)
        assert len(partitions) <= 6 and len(partitions) > 4
        assert partitions[0]["table_ranges"][0].attributes["ranges"][0]["lower_limit"]["row_index"] == 5
        assert partitions[-1]["table_ranges"][0].attributes["ranges"][0]["upper_limit"]["row_index"] == 15

    @authors("denvr")
    def test_distributed_read_command(self):
        if (yt.driver.get_api_version(), yt.config["backend"]) not in (("v4", "http"), ):
            pytest.skip()

        table_path = TEST_DIR + "/table"
        rows = [{"value": i} for i in range(20)]
        client = yt.YtClient(config=yt.config.config)
        client.write_table(table_path, rows)

        # check parallel read content retries
        table_data = []
        with set_config_option("read_parallel/enable", True), \
                set_config_option("read_parallel/max_thread_count", 2), \
                set_config_option("read_parallel/use_distributed_read", True), \
                set_config_option("read_parallel/data_size_per_thread", 50), \
                set_config_option("read_buffer_size", 10), \
                set_config_option("read_retries/backoff/policy", "constant_time"), \
                set_config_option("read_retries/backoff/constant_time", 0), \
                inject_http_read_error(
                    filter_url="read_table_partition",
                    interrupt_from=0,
                    interrupt_till=2,
                    interrupt_every=1,
                    interrupt_read_iteration=3,
                ) as cnt:
            client = yt.YtClient(config=yt.config.config)
            table_data = list(client.read_table(table_path))
        check_rows_equality(rows, table_data)
        if yt.config["backend"] == "http":
            assert cnt.filtered_raises == 1
