from __future__ import with_statement, print_function

from yt.testlib import authors
from yt.wrapper.testlib.helpers import TEST_DIR, inject_http_error, set_config_option

import yt.wrapper as yt

from yt.wrapper.schema import TableSchema

import yt.type_info as type_info

import pytest
import tempfile

import random
import string


def generate_random_string(length):
    letters = string.ascii_lowercase
    rand_string = "".join(random.choice(letters) for i in range(length))
    return rand_string


@pytest.mark.usefixtures("yt_env_v4")
class TestOrc(object):
    @authors("nadya02")
    @pytest.mark.parametrize("enable_parallel", [True, False])
    def test_simple_orc(self, enable_parallel):
        with tempfile.NamedTemporaryFile() as temp_file:
            filename = temp_file.name
            table = TEST_DIR + "/table"
            schema = TableSchema() \
                .add_column("key", type_info.Optional[type_info.Int64]) \
                .add_column("value", type_info.Optional[type_info.String]) \
                .add_column("bool", type_info.Optional[type_info.Bool])

            yt.create("table", table, attributes={"schema": schema})
            row = {"key": 1, "value": "one", "bool" : True}
            yt.write_table(table, [row])

            with set_config_option("read_parallel/enable", enable_parallel):
                yt.dump_orc(table, filename)

            output_table = TEST_DIR + "/output_table"

            yt.upload_orc(output_table, filename)

            schema_from_attr = TableSchema.from_yson_type(yt.get(output_table + "/@schema"))
            assert schema == schema_from_attr

            assert list(yt.read_table(output_table)) == list(yt.read_table(table))

    @authors("nadya02")
    @pytest.mark.parametrize("enable_parallel", [True, False])
    def test_multi_chunks_upload_orc(self, enable_parallel):
        with tempfile.NamedTemporaryFile() as temp_file:
            filename = temp_file.name
            input_table = TEST_DIR + "/table"

            schema = TableSchema() \
                .add_column("key", type_info.Optional[type_info.Int64]) \
                .add_column("value", type_info.Optional[type_info.String])

            yt.create("table", input_table, attributes={"schema": schema})
            rows_in_chunk = 10

            for i in range(5):
                rows = []
                for j in range(rows_in_chunk):
                    rows.append({"key": i * rows_in_chunk + j, "value": generate_random_string(5)})
                yt.write_table(yt.TablePath(input_table, append=True), rows)

            with set_config_option("read_parallel/enable", enable_parallel):
                yt.dump_orc(input_table, filename)

            output_table = TEST_DIR + "/output_table"

            yt.upload_orc(output_table, filename)

            schema_from_attr = TableSchema.from_yson_type(yt.get(output_table + "/@schema"))
            assert schema == schema_from_attr

            assert list(yt.read_table(output_table)) == list(yt.read_table(input_table))

    @authors("nadya02")
    def test_upload_orc_retry(self):
        with tempfile.NamedTemporaryFile() as temp_file:
            client = yt.YtClient(config=yt.config.config)

            client.config["write_retries"]["chunk_size"] = 500
            client.config["upload_table_options"]["write_arrow_batch_size"] = 10

            filename = temp_file.name
            input_table = "//tmp/dump_orc"

            schema = TableSchema() \
                .add_column("key", type_info.Optional[type_info.Int64]) \
                .add_column("value", type_info.Optional[type_info.String])

            client.create("table", input_table, attributes={"schema": schema})
            rows_in_chunk = 20

            for i in range(5):
                rows = []
                for j in range(rows_in_chunk):
                    rows.append({"key": i * rows_in_chunk + j, "value": generate_random_string(5)})
                client.write_table(yt.TablePath(input_table, append=True), rows)

            client.dump_orc(input_table, filename)

            output_table = "//tmp/upload_orc"

            with inject_http_error(client, filter_url='/write_table', interrupt_from=0, interrupt_till=999, interrupt_every=2, raise_connection_reset=True) as cnt:
                client.upload_orc(output_table, filename)

            assert list(yt.read_table(output_table)) == list(yt.read_table(input_table))
            assert client.get(output_table + "/@row_count") == 100
            assert client.get(output_table + "/@chunk_count") == 10
            assert cnt.filtered_total_calls == 20
            assert cnt.filtered_raises == 10
