from yt.testlib import authors

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

        if yt.config["backend"] == "http" and yt.driver.get_api_version() == "v3" or yt.config["backend"] == "rpc":
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
            client.finish_distributed_write_session(session=write_session, results=[write_result_1, write_result_2])
            assert list(client.read_table(sorted_table_path)) == [{"f_str": "bar_1"}, {"f_str": "bar_2"}, {"f_str": "bar_3"}, {"f_str": "bar_9"}]
            assert client.get(f"{sorted_table_path}/@sorted")
