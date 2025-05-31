from __future__ import with_statement, print_function

from .conftest import authors
from .helpers import (TEST_DIR, check_rows_equality, set_config_option, get_tests_sandbox, set_config_options,
                      wait, failing_heavy_request, random_string)

import yt.wrapper.format as yt_format

import yt.wrapper.py_wrapper as py_wrapper
from yt.wrapper import heavy_commands, parallel_writer
from yt.wrapper.common import MB
from yt.wrapper.py_wrapper import OperationParameters
from yt.wrapper.schema import TableSchema
from yt.wrapper.table import TempTable

from yt.yson import YsonMap

from yt.local import start, stop

import yt.wrapper as yt

import yt.type_info as typing

import os
import pytest
import tempfile
import time
import uuid
import gzip
from io import BytesIO
from copy import deepcopy
from contextlib import contextmanager
from datetime import datetime, timedelta


class FakeFileManager(object):
    def __init__(self, client):
        self.client = client
        self.disk_size = 0
        self.local_size = 0
        self.files = []
        self.uploaded_files = []

    def add_files(self, files):
        assert files != []
        assert os.listdir(yt.config["local_temp_directory"]) != []

    def upload_files(self):
        return []


@pytest.mark.usefixtures("yt_env_with_rpc")
class TestTableCommands(object):
    def _test_read_write_small(self):
        # NB: this method should not operate with large number of rows, since in some test configurations
        # we perform write using very small chunks.
        table = TEST_DIR + "/table"
        yt.create("table", table)
        check_rows_equality([], yt.read_table(table))

        yt.write_table(table, [{"x": 1}])
        check_rows_equality([{"x": 1}], yt.read_table(table))

        yt.write_table(table, [{"x": 1}])
        check_rows_equality([{"x": 1}], yt.read_table(table))

        yt.write_table(table, [{"x": 1}], raw=False)
        check_rows_equality([{"x": 1}], yt.read_table(table))

        yt.write_table(table, iter([{"x": 1}]))
        check_rows_equality([{"x": 1}], yt.read_table(table))

        yt.write_table(yt.TablePath(table, append=True), [{"y": 1}])
        check_rows_equality([{"x": 1}, {"y": 1}], yt.read_table(table))

        yt.write_table(yt.TablePath(table), [{"x": 1}, {"y": 1}])
        check_rows_equality([{"x": 1}, {"y": 1}], yt.read_table(table))

        yt.write_table(table, [{"y": 1}])
        check_rows_equality([{"y": 1}], yt.read_table(table))

        yt.write_table(table, BytesIO(b'{"y": 1}\n'), raw=True, format=yt.JsonFormat())
        check_rows_equality([{"y": 1}], yt.read_table(table))

        yt.write_table(table, [{"y": 1}])
        response_parameters = {}
        list(yt.read_table(table, response_parameters=response_parameters))
        assert response_parameters["start_row_index"] == 0
        assert response_parameters["approximate_row_count"] == 1

        yt.write_table(table, [{"y": "1"}], raw=False)
        assert [{"y": "1"}] == list(yt.read_table(table, raw=False))

        with set_config_option("tabular_data_format", yt.DsvFormat()):
            yt.write_table(table, [b"x=1\n"], raw=True)

        with set_config_option("read_retries/change_proxy_period", 1):
            yt.write_table(table, [{"y": i} for i in range(10)])
            check_rows_equality([{"y": i} for i in range(10)], yt.read_table(table))

        file = TEST_DIR + "/test_file"
        yt.create("file", file)
        with pytest.raises(yt.YtError):
            yt.read_table(file)

        # Pull parser reads 1MB on creation, so table should be >1MB. Otherwise parser reads the whole table and no error occurs.
        yt.write_table(table, [{"x": "a" * (2 * 10**5)}] * 10)
        client = yt.YtClient(config=deepcopy(yt.config.config))
        with set_config_option("read_parallel/max_thread_count", 2), \
                set_config_option("transaction_timeout", 3000), \
                set_config_option("proxy/request_timeout", 1000), \
                set_config_option("proxy/retries/total_timeout", 3000), \
                set_config_option("read_buffer_size", 10):
            iterator = yt.read_table(table)

            for transaction in client.search("//sys/transactions", attributes=["title", "id"]):
                if transaction.attributes.get("title", "").startswith("Python wrapper: read"):
                    client.abort_transaction(transaction.attributes["id"])

            time.sleep(5)
            with pytest.raises(yt.YtError):
                for _ in iterator:
                    pass

    def _test_abandoned_read(self):
        table = TEST_DIR + "/table"
        yt.write_table(yt.TablePath(table), [{"x": i} for i in range(100000)])
        iterator = yt.read_table(table)
        row = next(iterator)
        assert row == {"x": 0}
        del iterator

    @authors("levysotsky")
    def test_read_write_with_schema(self, yt_env_with_rpc):
        if yt.config["backend"] == "rpc":
            return

        table = TEST_DIR + "/table"
        schema = TableSchema() \
            .add_column("x", typing.Int64) \
            .add_column("y", typing.Optional[typing.Double]) \
            .add_column("z", typing.Struct[
                "foo": typing.Variant["a": typing.Uint8, "b": typing.String],
                "bar": typing.String,
            ])
        yt.create("table", table, attributes={"schema": schema})
        check_rows_equality([], yt.read_table(table))

        row = {"x": 1, "y": 0.125, "z": {"foo": ["a", yt.yson.YsonUint64(255)], "bar": "BAR"}}
        yt.write_table(table, [row])
        check_rows_equality([row], yt.read_table(table))

        yt.write_table(table, [row])
        check_rows_equality([row], yt.read_table(table))

        yt.write_table(table, [row], raw=False)
        check_rows_equality([row], yt.read_table(table))

        yt.write_table(table, iter([row]))
        check_rows_equality([row], yt.read_table(table))

        other_row = {"x": 2, "z": {"bar": "", "foo": ["b", "xxx"]}}
        other_row_with_y = {"x": 2, "y": None, "z": {"bar": "", "foo": ["b", "xxx"]}}
        yt.write_table(yt.TablePath(table, append=True), [other_row])
        check_rows_equality([row, other_row_with_y], yt.read_table(table))

        yt.write_table(yt.TablePath(table), [row, other_row])
        check_rows_equality([row, other_row_with_y], yt.read_table(table))

        yt.write_table(table, [other_row])
        check_rows_equality([other_row_with_y], yt.read_table(table))

        yt.write_table(
            table,
            BytesIO(b'{"x": 3, "y": -3.25, "z": {"foo": ["b", "bbb"], "bar": "fff"}}\n'),
            raw=True,
            format=yt.JsonFormat())
        check_rows_equality([{"x": 3, "y": -3.25, "z": {"foo": ["b", "bbb"], "bar": "fff"}}], yt.read_table(table))

    @authors("ignat")
    def test_table_path(self, yt_env_with_rpc):
        path = yt.TablePath("//path/to/table", attributes={"my_attr": 10})
        assert path.attributes["my_attr"] == 10
        assert str(path) == "//path/to/table"

        with set_config_option("prefix", "//path/"):
            path = yt.TablePath("to/table", attributes={"my_attr": 10})
            assert path.attributes["my_attr"] == 10
            assert str(path) == "//path/to/table"

        path = yt.TablePath("//path/to/table", exact_key="test_key")
        path.canonize_exact_ranges()
        assert "exact" not in path.ranges[0]
        assert path.ranges[0]["lower_limit"]["key"] == ["test_key"]
        sentinel = yt.yson.YsonEntity()
        sentinel.attributes["type"] = "max"
        assert path.ranges[0]["upper_limit"]["key"] == ["test_key", sentinel]

        path = yt.TablePath("//path/to/table", exact_index=5)
        path.canonize_exact_ranges()
        assert "exact" not in path.ranges[0]
        assert path.ranges[0]["lower_limit"]["row_index"] == 5
        assert path.ranges[0]["upper_limit"]["row_index"] == 6

        path = yt.TablePath("//path/to/table", attributes={"ranges": [{"exact": {"chunk_index": 1}}]})
        path.canonize_exact_ranges()
        assert "exact" not in path.ranges[0]
        assert path.ranges[0]["lower_limit"]["chunk_index"] == 1
        assert path.ranges[0]["upper_limit"]["chunk_index"] == 2

    @authors("asaitgalin")
    def test_read_write_with_retries(self):
        with set_config_option("write_retries/enable", True):
            self._test_read_write_small()

    @authors("asaitgalin", "ignat")
    def test_read_write_without_retries(self):
        with set_config_option("write_retries/enable", False):
            self._test_read_write_small()
            self._test_abandoned_read()

    @authors("ignat")
    def test_read_parallel(self, yt_env_with_rpc):
        with set_config_option("read_parallel/enable", True):
            self._test_read_write_small()

    @authors("asaitgalin", "ignat")
    def test_parallel_write(self):
        with set_config_option("write_parallel/enable", True):
            with set_config_option("write_retries/chunk_size", 1):
                with set_config_option("write_parallel/concatenate_size", 3):
                    self._test_read_write_small()

    @authors("asaitgalin", "babenko")
    def test_schemaful_parallel_write(self):
        yt.create(
            "table",
            "//tmp/table",
            recursive=True,
            attributes={
                "schema": [
                    {"name": "id", "type": "int64"},
                    {"name": "value", "type": "double"},
                ],
                "optimize_for": "scan",
            })

        data = [{"id": i, "value": 0.9 * i} for i in range(64)]
        with set_config_option("write_parallel/enable", True):
            with set_config_option("write_retries/chunk_size", 256):
                yt.write_table("//tmp/table", data)

        assert yt.get("//tmp/table/@optimize_for_statistics/scan/chunk_count") > 0

    @authors("levysotsky")
    def test_schemaful_parallel_write_typed_schema(self):
        schema = TableSchema().add_column("id", typing.Int64).add_column("value", typing.Double)
        yt.create("table", "//tmp/table", recursive=True, attributes={"schema": schema})

        data = [{"id": i, "value": 0.9 * i} for i in range(64)]
        with set_config_option("write_parallel/enable", True):
            with set_config_option("write_retries/chunk_size", 256):
                yt.write_table("//tmp/table", data)

    @authors("ostyakov", "ignat")
    def test_empty_table(self):
        dir = TEST_DIR + "/dir"
        table = dir + "/table"

        with pytest.raises(yt.YtError):
            yt.create("table", table)
        with pytest.raises(yt.YtError):
            yt.row_count(table)

        yt.create("table", table, recursive=True)
        assert yt.row_count(table) == 0
        check_rows_equality([], yt.read_table(table, format=yt.DsvFormat()))

        yt.run_erase(table)
        assert yt.row_count(table) == 0

        yt.remove(dir, recursive=True)
        with pytest.raises(yt.YtError):
            yt.create("table", table)

    @authors("asaitgalin", "ostyakov")
    def test_create_temp_table(self):
        table = yt.create_temp_table(path=TEST_DIR)
        assert table.startswith(TEST_DIR)

        table = yt.create_temp_table(path=TEST_DIR, prefix="prefix")
        assert table.startswith(TEST_DIR + "/prefix")

        with TempTable() as table:
            assert yt.exists(table)
        assert not yt.exists(table)

        config = deepcopy(yt.config.config)
        config["temp_expiration_timeout"] = 4 * 1000
        client = yt.YtClient(config=config)
        with client.TempTable() as table:
            assert client.exists(table)
            wait(lambda: not client.exists(table))

    @authors("asaitgalin", "ignat")
    def test_write_many_chunks(self):
        with set_config_option("write_retries/chunk_size", 1):
            table = TEST_DIR + "/table"
            for i in range(3):
                yt.write_table("<append=%true>" + table, [{"x": 1}, {"y": 2}, {"z": 3}])
            assert yt.get(table + "/@chunk_count") == 9

    @authors("ostyakov", "ignat")
    def test_binary_data_with_dsv(self):
        with set_config_option("tabular_data_format", yt.DsvFormat()):
            record = {"\tke\n\\\\y=": "\\x\\y\tz\n"}
            table = TEST_DIR + "/table"
            yt.write_table(table, [yt.dumps_row(record)], raw=True)
            assert [record] == list(yt.read_table(table))

    @authors("ostyakov")
    def test_remove_locks(self):
        from yt.wrapper.table_helpers import _remove_locks
        table = TEST_DIR + "/table"
        yt.create("table", table)
        try:
            for _ in range(5):
                tx = yt.start_transaction(timeout=10000)
                yt.config.COMMAND_PARAMS["transaction_id"] = tx
                yt.lock(table, mode="shared")
            yt.config.COMMAND_PARAMS["transaction_id"] = "0-0-0-0"
            assert len(yt.get_attribute(table, "locks")) == 5
            _remove_locks(table)
            assert yt.get_attribute(table, "locks") == []

            tx = yt.start_transaction(timeout=10000)
            yt.config.COMMAND_PARAMS["transaction_id"] = tx
            yt.lock(table)
            yt.config.COMMAND_PARAMS["transaction_id"] = "0-0-0-0"
            _remove_locks(table)
            assert yt.get_attribute(table, "locks") == []
        finally:
            yt.config.COMMAND_PARAMS["transaction_id"] = "0-0-0-0"

    def _set_banned(self, value):
        # NB: we cannot unban proxy using proxy, so we must use client for that.
        client = yt.YtClient(config={"backend": "native", "driver_config": yt.config["driver_config"]})
        proxy = "//sys/http_proxies/" + client.list("//sys/http_proxies")[0]
        client.set(proxy + "/@banned", value)

        def check():
            try:
                yt.get("/")
                return not value
            except yt.YtError:
                return value

        time.sleep(0.5)
        wait(check)

    @authors("ostyakov")
    def test_banned_proxy(self):
        if yt.config["backend"] in ("native", "rpc"):
            pytest.skip()

        table = TEST_DIR + "/table"
        yt.create("table", table)

        self._set_banned(True)
        with set_config_option("proxy/retries/count", 1,
                               final_action=lambda: self._set_banned(False)):
            try:
                yt.get(table)
            except yt.YtProxyUnavailable as err:
                assert "banned" in str(err)

    @authors("ostyakov")
    def test_error_occurred_after_starting_to_write_chunked_requests(self):
        if yt.config["api_version"] != "v3":
            pytest.skip()

        with set_config_option("proxy/content_encoding", "identity"):
            table = TEST_DIR + "/table"

            def gen_table():
                yield b'{"abc": "123"}\n' * 100000
                yield b'{a:b}\n'
                yield b'{"dfg": "456"}\n' * 10000000

            try:
                yt.write_table(table, gen_table(), raw=True, format=yt.JsonFormat())
            except yt.YtResponseError as err:
                assert "JSON" in str(err), "Incorrect error message: " + str(err)
            else:
                assert False, "Failed to catch response error"

    @authors("ostyakov")
    def test_reliable_remove_tempfiles_in_py_wrap(self):
        def foo(rec):
            yield rec

        with set_config_option("is_local_mode", False):
            client = yt.YtClient(config=yt.config.config)

            new_temp_dir = tempfile.mkdtemp(dir=yt.config["local_temp_directory"])
            client.config["local_temp_directory"] = new_temp_dir

            assert os.listdir(client.config["local_temp_directory"]) == []

            params = OperationParameters(
                input_format=None,
                output_format=None,
                operation_type="map",
                job_type="mapper",
                group_by=None)
            with pytest.raises(AttributeError):
                with py_wrapper.TempfilesManager(
                        remove_temp_files=True,
                        directory=py_wrapper.get_local_temp_directory(client)) as tempfiles_manager:
                    py_wrapper.wrap(function=foo,
                                    file_manager=None,
                                    tempfiles_manager=tempfiles_manager,
                                    params=params,
                                    local_mode=False,
                                    client=client)

            with py_wrapper.TempfilesManager(
                    remove_temp_files=True,
                    directory=py_wrapper.get_local_temp_directory(client)) as tempfiles_manager:
                py_wrapper.wrap(function=foo,
                                file_manager=FakeFileManager(client),
                                tempfiles_manager=tempfiles_manager,
                                params=params,
                                local_mode=False,
                                client=client)
            assert os.listdir(client.config["local_temp_directory"]) == []

    @authors("ignat", "ilpauzner")
    def test_write_compressed_table_data(self):
        fd, filename = tempfile.mkstemp()
        os.close(fd)

        with gzip.GzipFile(filename, "w", 5) as fout:
            fout.write(b"x=1\nx=2\nx=3\n")

        with open(filename, "rb") as f:
            with set_config_option("proxy/content_encoding", "gzip"):
                if yt.config["backend"] in ("native", "rpc"):
                    with pytest.raises(yt.YtError):  # not supported for native backend
                        yt.write_table(TEST_DIR + "/table", f, format="dsv", is_stream_compressed=True, raw=True)
                else:
                    yt.write_table(TEST_DIR + "/table", f, format="dsv", is_stream_compressed=True, raw=True)
                    check_rows_equality([{"x": "1"}, {"x": "2"}, {"x": "3"}], yt.read_table(TEST_DIR + "/table"))

    @authors("ignat")
    def test_read_lost_chunk(self):
        mode = yt.config["backend"]
        if mode not in ("native", "rpc"):
            mode = yt.config["api_version"]

        test_name = "TestYtWrapper" + mode.capitalize()
        dir = os.path.join(get_tests_sandbox(), test_name)
        id = "run_" + uuid.uuid4().hex[:8]
        instance = None
        try:
            instance = start(
                path=dir,
                id=id,
                node_count=10,
                http_proxy_count=1 if (yt.config["backend"] != "native") else 0,
                rpc_proxy_count=1 if (yt.config["backend"] == "rpc") else 0,
                fqdn="localhost",
                enable_debug_logging=True,
                cell_tag=2)
            client = instance.create_client()
            client.config["driver_config"] = instance.configs["rpc_driver"] if yt.config["backend"] == "rpc" else instance.configs["driver"]
            client.config["backend"] = yt.config["backend"]

            client.config["read_retries"]["backoff"]["constant_time"] = 5000
            client.config["read_retries"]["backoff"]["policy"] = "constant_time"

            table = TEST_DIR + "/test_lost_chunk_table"
            client.create("table", table, recursive=True)
            client.set(table + "/@erasure_codec", "reed_solomon_6_3")
            client.write_table(table, [{"x": 1}])
            client.write_table("<append=%true>" + table, [{"x": 2}])

            chunk_ids = client.get(table + "/@chunk_ids")
            chunk_id = chunk_ids[1]
            replicas = client.get("#%s/@stored_replicas" % chunk_id)
            address = str(replicas[0])

            # COMPAT(kvk1920)
            if client.exists("//sys/cluster_nodes/{0}/@maintenance_requests".format(address)):
                client.add_maintenance("cluster_node", address, "ban", "test read lost chunk")
            else:
                client.set("//sys/cluster_nodes/{0}/@banned".format(address), True)
            wait(lambda: client.get("//sys/cluster_nodes/{0}/@state".format(address)) == "offline")

            assert list(client.read_table(table)) == [{"x": 1}, {"x": 2}]
        finally:
            if instance is not None:
                stop(instance.id, path=dir, remove_runtime_data=True)

    @authors("ignat")
    def test_lazy_read(self):
        table = TEST_DIR + "/test_lazy_read_table"
        yt.write_table(table, [{"x": "abacaba", "y": 1}, {"z": 2}])
        stream = yt.read_table(table, format="<lazy=%true>yson")
        result = list(stream)

        assert not isinstance(result[0], (YsonMap, dict))
        assert result[0]["x"] == "abacaba"
        assert result[1]["z"] == 2

    @authors("se4min", "denvid")
    def test_get_table_columnar_statistics(self):
        table = TEST_DIR + "/test_table"
        yt.write_table(table, [{"x": "abacaba", "y": 1}, {"x": 2}])

        res = yt.get_table_columnar_statistics(paths=[table + "{x,y}"])
        assert res
        assert res[0].get("column_data_weights", {}) == {"x": 15, "y": 8}
        assert res[0].get("column_min_values", {}) == {"x": 2, "y": 1}
        assert res[0].get("column_max_values", {}) == {"x": "abacaba", "y": 1}
        assert res[0].get("column_non_null_value_counts", {}) == {"x": 2, "y": 1}
        assert res[0].get("chunk_row_count", 0) == 2
        assert res[0].get("legacy_chunk_row_count") == 0

    @authors("ignat")
    def test_unaligned_write(self):
        with set_config_option("write_retries/chunk_size", 3 * MB):
            with set_config_option("proxy/content_encoding", "identity"):
                string_length = 4 * MB
                rows = [
                    {"x": "1" * string_length},
                    {"x": "2" * string_length},
                    {"x": "3" * string_length},
                    {"x": "4" * string_length},
                    {"x": "5" * string_length},
                ]

                table_path = TEST_DIR + "/table"
                yt.create("table", table_path)
                yt.write_table(table_path, rows)

                check_rows_equality(rows, yt.read_table(table_path))

    @authors("ignat")
    def test_write_table_retries(self):
        rows = [
            {"x": "1"},
            {"x": "2"},
            {"x": "4"},
            {"x": "8"},
        ]
        rows_generator = (row for row in rows)

        table_path = TEST_DIR + "/table"
        yt.create("table", table_path)

        with failing_heavy_request(heavy_commands, n_fails=2, assert_exhausted=True):
            yt.write_table(table_path, rows_generator)

        check_rows_equality(rows, yt.read_table(table_path))

    @authors("ignat")
    def test_read_with_columns_with_slashes(self):
        table_path = TEST_DIR + "/table"
        yt.create("table", table_path)

        yt.write_table(table_path, [{"a/b": 1}])
        check_rows_equality([{"a/b": 1}], yt.read_table(yt.TablePath(table_path, columns=["a/b"])))

    @authors("alex-shishkin")
    def test_partition_tables(self):
        table_path = TEST_DIR + "/table"
        yt.create("table", table_path)
        row_count = 5
        rows = [{"value": i} for i in range(row_count)]
        yt.write_table(table_path, rows)
        partitions = yt.partition_tables(table_path, partition_mode="ordered", data_weight_per_partition=10)
        assert len(partitions) <= 3
        table_data = []
        for partition in partitions:
            table_ranges = partition["table_ranges"]
            assert len(table_ranges) == 1
            range_data = list(yt.read_table(table_ranges[0]))
            assert len(range_data) <= 2
            table_data.extend(range_data)
        check_rows_equality(rows, table_data)


@pytest.mark.usefixtures("yt_env_with_authentication")
class TestTableCommandsWithAuthorization(object):

    @authors("denvr")
    def test_omit_columns(self):
        table = TEST_DIR + "/table_columns"
        yt.create("user", attributes={"name": "user1"})
        wait(lambda: yt.get("//sys/users/user1/@life_stage") == "creation_committed")
        yt.set("//sys/tokens/sometoken", "user1")
        yt.create(
            "table",
            table,
            attributes={
                "schema": [
                    {"name": "a", "type": "string"},
                    {"name": "b", "type": "string"},
                    {"name": "c", "type": "string"},
                ],
                "acl": [
                    {
                        "action": "deny",
                        "subjects": ["user1"],
                        "permissions": ["read"],
                        "inheritance_mode": "object_and_descendants",
                        "columns": ["a"],
                    },
                    {
                        "action": "deny",
                        "subjects": ["user1"],
                        "permissions": ["read"],
                        "inheritance_mode": "object_and_descendants",
                        "columns": ["b"],
                    },
                ],
            },
        )

        yt.write_table(
            table,
            [
                {"a": "a", "b": "b", "c": "c"},
                {"a": "a", "b": "b_new", "c": "c_new"},
            ]
        )

        assert list(yt.read_table(table)) == [{"a": "a", "b": "b", "c": "c"}, {"a": "a", "b": "b_new", "c": "c_new"},]
        assert list(yt.read_table(table + "{b,c}")) == [{"b": "b", "c": "c"}, {"b": "b_new", "c": "c_new"},]

        client_config = deepcopy(yt.config.config)
        client_config.update({"token": "sometoken", "enable_token": True})
        client = yt.YtClient(config=client_config)
        assert client.get_user_name() == "user1"
        with pytest.raises(yt.YtResponseError) as ex_info:
            result = client.read_table(table)
        assert "Error getting basic attributes of user objects" in ex_info.value.error["message"]
        with pytest.raises(yt.YtResponseError) as ex_info:
            result = client.read_table(table+"{b,c}")
        assert "Error getting basic attributes of user objects" in ex_info.value.error["message"]

        result = client.read_table(table, omit_inaccessible_columns=True)
        assert list(result) == [{"c": "c"}, {"c": "c_new"},]

        result = client.read_table(table+"{b,c}", omit_inaccessible_columns=True)
        assert list(result) == [{"c": "c"}, {"c": "c_new"},]

        client.config["read_omit_inaccessible_columns"] = True
        result = client.read_table(table+"{b,c}")
        assert list(result) == [{"c": "c"}, {"c": "c_new"},]


@pytest.mark.usefixtures("yt_env_with_rpc")
class TestTableCommandsControlAttributes(object):
    @authors("ignat", "asaitgalin")
    def test_start_row_index(self):
        table = TEST_DIR + "/table"

        yt.write_table(yt.TablePath(table, sorted_by=["a"]), [{"a": "b"}, {"a": "c"}, {"a": "d"}])

        with set_config_option("tabular_data_format", yt.JsonFormat()):
            rsp = yt.read_table(table, raw=True)
            assert rsp.response_parameters["start_row_index"] == 0
            assert rsp.response_parameters["approximate_row_count"] == 3

            rsp = yt.read_table(yt.TablePath(table, start_index=1), raw=True)
            assert rsp.response_parameters["start_row_index"] == 1
            assert rsp.response_parameters["approximate_row_count"] == 2

            rsp = yt.read_table(yt.TablePath(table, lower_key=["d"]), raw=True)
            assert rsp.response_parameters["start_row_index"] == 2
            # When reading with key limits row count is estimated rounded up to the chunk row count.
            assert rsp.response_parameters["approximate_row_count"] == 3

            rsp = yt.read_table(yt.TablePath(table, lower_key=["x"]), raw=True)
            assert rsp.response_parameters["approximate_row_count"] == 0

    @authors("ignat")
    def test_start_row_index_parallel(self, yt_env_with_rpc):
        with set_config_option("read_parallel/enable", True):
            self.test_start_row_index()

    @authors("asaitgalin")
    def test_table_index(self):
        dsv = yt.format.DsvFormat(enable_table_index=True, table_index_column="TableIndex")
        schemaful_dsv = yt.format.SchemafulDsvFormat(columns=["1", "2", "3"],
                                                     enable_table_index=True,
                                                     table_index_column="_table_index_")

        src_table_a = TEST_DIR + "/in_table_a"
        src_table_b = TEST_DIR + "/in_table_b"
        dst_table_a = TEST_DIR + "/out_table_a"
        dst_table_b = TEST_DIR + "/out_table_b"
        dst_table_ab = TEST_DIR + "/out_table_ab"

        len_a = 5
        len_b = 3

        yt.create("table", src_table_a, recursive=True, ignore_existing=True)
        yt.create("table", src_table_b, recursive=True, ignore_existing=True)
        yt.write_table(src_table_a, b"1=a\t2=a\t3=a\n" * len_a, format=dsv, raw=True)
        yt.write_table(src_table_b, b"1=b\t2=b\t3=b\n" * len_b, format=dsv, raw=True)

        assert yt.row_count(src_table_a) == len_a
        assert yt.row_count(src_table_b) == len_b

        def mix_table_indexes(row):
            row["_table_index_"] = row["TableIndex"]
            yield row
            row["_table_index_"] = 2
            yield row

        yt.run_operation_commands.run_map(binary=mix_table_indexes,
                                          source_table=[src_table_a, src_table_b],
                                          destination_table=[dst_table_a, dst_table_b, dst_table_ab],
                                          input_format=dsv,
                                          output_format=schemaful_dsv)
        assert yt.row_count(dst_table_b) == len_b
        assert yt.row_count(dst_table_a) == len_a
        assert yt.row_count(dst_table_ab) == len_a + len_b
        for table in (dst_table_a, dst_table_b, dst_table_ab):
            rsp = yt.read_table(table, raw=False)
            row = next(rsp)
            for field in ("@table_index", "TableIndex", "_table_index_"):
                assert field not in row

    @authors("ignat")
    def test_read_with_table_path(self, yt_env_with_rpc):
        table = TEST_DIR + "/table"
        yt.write_table(table, [{"y": "w3"}, {"x": "b", "y": "w1"}, {"x": "a", "y": "w2"}])
        yt.run_sort(table, sort_by=["x", "y"])

        def read_table(**kwargs):
            kwargs["name"] = kwargs.get("name", table)
            return list(yt.read_table(yt.TablePath(**kwargs), raw=False))

        assert read_table(lower_key="a", upper_key="d") == [{"x": "a", "y": "w2"},
                                                            {"x": "b", "y": "w1"}]
        assert read_table(columns=["y"]) == [{"y": "w" + str(i)} for i in [3, 2, 1]]
        assert read_table(lower_key="a", end_index=2, columns=["x"]) == [{"x": "a"}]
        assert read_table(start_index=0, upper_key="b") == [{"x": None, "y": "w3"}, {"x": "a", "y": "w2"}]
        assert read_table(start_index=1, columns=["x"]) == [{"x": "a"}, {"x": "b"}]
        assert read_table(ranges=[{"lower_limit": {"row_index": 1}}], columns=["x"]) == [{"x": "a"}, {"x": "b"}]

        assert read_table(name=table + "{y}[:#2]") == [{"y": "w3"}, {"y": "w2"}]
        assert read_table(name=table + "[#1:]") == [{"x": "a", "y": "w2"}, {"x": "b", "y": "w1"}]

        assert read_table(name="<ranges=[{lower_limit={key=[b]}}]>" + table) == \
            [{"x": "b", "y": "w1"}]
        assert read_table(name="<ranges=[{upper_limit={row_index=2}}]>" + table) == \
            [{"x": None, "y": "w3"}, {"x": "a", "y": "w2"}]

        with pytest.raises(yt.YtError):
            assert read_table(ranges=[{"lower_limit": {"index": 1}}], end_index=1)
        with pytest.raises(yt.YtError):
            read_table(name=yt.TablePath(table, lower_key="a", start_index=1))
        with pytest.raises(yt.YtError):
            read_table(name=yt.TablePath(table, upper_key="c", end_index=1))

        table_path = yt.TablePath(table, exact_index=1)
        assert list(yt.read_table(table_path.to_yson_string(), format=yt.DsvFormat())) == [{"x": "a", "y": "w2"}]

        yt.write_table(table, [{"x": "b"}, {"x": "a"}, {"x": "c"}])
        with pytest.raises(yt.YtError):
            yt.read_table(yt.TablePath(table, lower_key="a"))
        # No prefix
        with pytest.raises(yt.YtError):
            yt.TablePath("abc")
        # Prefix should start with //
        yt.config["prefix"] = "abc/"
        with pytest.raises(yt.YtError):
            yt.TablePath("abc")
        # Prefix should end with /
        yt.config["prefix"] = "//abc"
        with pytest.raises(yt.YtError):
            yt.TablePath("abc")

    @authors("ostyakov")
    def test_read_parallel_with_table_path(self, yt_env_with_rpc):
        with set_config_option("read_parallel/enable", True):
            self.test_read_with_table_path(yt_env_with_rpc)


@pytest.mark.usefixtures("yt_env_with_rpc")
class TestBlobTables(object):
    def _test_read_blob_table(self):
        table = TEST_DIR + "/test_blob_table"

        yt.create("table", table, attributes={"schema": [{"name": "key", "type": "string", "sort_order": "ascending"},
                                                         {"name": "part_index", "type": "int64",
                                                          "sort_order": "ascending"},
                                                         {"name": "data", "type": "string"}]})
        yt.write_table(table, [{"key": "test" + str(i),
                                "part_index": j,
                                "data": "data" + str(j) + str(i)} for i in range(3) for j in range(3)])

        stream = yt.read_blob_table(table + "[test0]", part_size=6)
        assert stream.read() == b"data00data10data20"

        stream = yt.read_blob_table(table + "[test0:test1]", part_size=6)
        assert stream.read() == b"data00data10data20"

        stream = yt.read_blob_table(table + "[test1]", part_size=6)
        assert stream.read() == b"data01data11data21"

        with pytest.raises(yt.YtError):
            yt.read_blob_table(table, part_size=6)

        with pytest.raises(yt.YtError):
            stream = yt.read_blob_table(table + "[test0]", part_size=7)
            stream.read()

        with pytest.raises(yt.YtError):
            yt.read_blob_table(table + "[test0]")

        with pytest.raises(yt.YtError):
            yt.read_blob_table(table + "[test0, test1]", part_size=6)

        yt.set(table + "/@part_size", 6)
        stream = yt.read_blob_table(table + "[test0:#1]")
        assert stream.read() == b"data00"

        with pytest.raises(yt.YtError):
            yt.read_blob_table(table + "[#4:]")

        with pytest.raises(yt.YtError):
            yt.read_blob_table(table + "[#4]")

        with pytest.raises(yt.YtError):
            yt.read_blob_table(table + "[(test0,1)]")

        yt.write_table(table, [{"key": "test",
                                "part_index": j,
                                "data": "data" + str(j)} for j in range(3)])

        yt.set(table + "/@part_size", 5)
        stream = yt.read_blob_table(table + "[test]")
        assert stream.read() == b"data0data1data2"

        stream = yt.read_blob_table(table + "[test5]")
        assert stream.read() == b""

        yt.write_table("<append=%true>" + table, [{"key": "test", "part_index": 3, "data": "ab"}])
        stream = yt.read_blob_table(table + "[test]")
        assert stream.read() == b"data0data1data2ab"

        yt.write_table("<append=%true>" + table, [{"key": "test", "part_index": 4, "data": "data4"}])
        with pytest.raises(yt.YtError):
            stream = yt.read_blob_table(table + "[test]")
            stream.read()

        yt.write_table(table, [{"key": "test",
                                "part_index": j,
                                "data": "data" + str(j)} for j in range(3)])
        yt.write_table("<append=%true>" + table, [{"key": "test", "part_index": 3, "data": "data03"}])
        with pytest.raises(yt.YtError):
            stream = yt.read_blob_table(table + "[test]")
            stream.read()

        yt.remove(table)
        yt.create("table", table, attributes={"schema": [{"name": "part_index", "type": "int64",
                                                          "sort_order": "ascending"},
                                                         {"name": "data", "type": "string"}]})

        yt.write_table(table, [{"part_index": i, "data": "data" + str(i)}
                               for i in range(3)])

        stream = yt.read_blob_table(table, part_size=5)
        assert stream.read() == b"data0data1data2"

    @authors("se4min")
    def test_read_blob_table_with_retries(self):
        with set_config_option("read_retries/enable", True):
            with set_config_option("read_buffer_size", 10):
                yt.config._ENABLE_READ_TABLE_CHAOS_MONKEY = True
                yt.config._ENABLE_HTTP_CHAOS_MONKEY = True
                try:
                    self._test_read_blob_table()
                finally:
                    yt.config._ENABLE_READ_TABLE_CHAOS_MONKEY = False
                    yt.config._ENABLE_HTTP_CHAOS_MONKEY = False

    @authors("se4min")
    def test_read_blob_table_without_retries(self):
        with set_config_option("read_retries/enable", False):
            self._test_read_blob_table()


@pytest.mark.usefixtures("yt_env_with_rpc")
class TestTableCommandsOperations(object):
    @authors("ignat")
    def test_transform(self):
        table = TEST_DIR + "/test_transform_table"
        other_table = TEST_DIR + "/test_transform_table2"

        assert not yt.transform(table)

        yt.create("table", table)
        assert not yt.transform(table)

        yt.write_table(table, [{"x": 1}, {"x": 2}])

        yt.transform(table)
        check_rows_equality([{"x": 1}, {"x": 2}], yt.read_table(table))

        yt.transform(table, other_table)
        check_rows_equality([{"x": 1}, {"x": 2}], yt.read_table(other_table))

        yt.remove(other_table)
        assert yt.transform(table, other_table, compression_codec="zlib_6")
        assert yt.get(other_table + "/@compression_codec") == "zlib_6"
        assert not yt.transform(other_table, other_table, compression_codec="zlib_6", check_codecs=True)

        assert yt.transform(table, other_table, optimize_for="scan")
        assert yt.get(other_table + "/@optimize_for") == "scan"

        assert not yt.transform(other_table, other_table, erasure_codec="none", check_codecs=True)

    @authors("ignat")
    def test_erase(self):
        table = TEST_DIR + "/table"
        yt.write_table(table, [{"a": i} for i in range(10)])
        assert yt.row_count(table) == 10
        yt.run_erase(yt.TablePath(table, start_index=0, end_index=5))
        assert yt.row_count(table) == 5
        yt.run_erase(yt.TablePath(table, start_index=0, end_index=5))
        assert yt.row_count(table) == 0


@pytest.mark.usefixtures("yt_env_with_rpc")
class TestTableCommandsHuge(object):
    @pytest.mark.parametrize("parallel,progress_bar", [(False, False), (True, False), (False, True),
                                                       (True, True)])
    @authors("ignat")
    def test_write_big_table_retries(self, parallel, progress_bar):
        with set_config_option("write_parallel/enable", parallel):
            with set_config_option("write_progress_bar/enable", progress_bar):
                with set_config_option("write_retries/chunk_size", 3 * MB):
                    string_length = 4 * MB
                    rows = [
                        {"x": "1" * string_length},
                        {"x": "2" * string_length},
                        {"x": "3" * string_length},
                        {"x": "4" * string_length},
                        {"x": "5" * string_length},
                    ]

                    rows_generator = (row for row in rows)

                    table_path = TEST_DIR + "/table"
                    yt.create("table", table_path)

                    if parallel:
                        module = parallel_writer
                    else:
                        module = heavy_commands

                    with failing_heavy_request(module, n_fails=2, assert_exhausted=True):
                        yt.write_table(table_path, rows_generator)

                    check_rows_equality(rows, yt.read_table(table_path))

    @authors("ignat")
    def test_huge_table(self):
        table = TEST_DIR + "/table"
        power = 3
        format = yt.JsonFormat()
        records = map(format.dumps_row, ({"k": i, "s": i * i, "v": "long long string with strange symbols"
                                                                   " #*@*&^$#%@(#!@:L|L|KL..,,.~`"}
                      for i in range(10 ** power)))
        yt.write_table(table, yt.StringIterIO(records), format=format, raw=True)

        assert yt.row_count(table) == 10 ** power

        row_count = 0
        for _ in yt.read_table(table):
            row_count += 1
        assert row_count == 10 ** power

    @authors("ignat")
    def test_read_parallel_huge_table(self, yt_env_with_rpc):
        with set_config_option("read_parallel/enable", True):
            self.test_huge_table()

    @pytest.mark.parametrize("use_tmp_dir_for_intermediate_data", [True, False])
    @authors("ignat")
    def test_write_parallel_huge_table(self, use_tmp_dir_for_intermediate_data):
        override_options = {
            "write_parallel/concatenate_size": 7,
            "write_parallel/enable": True,
            "write_parallel/use_tmp_dir_for_intermediate_data": use_tmp_dir_for_intermediate_data,
            "write_retries/chunk_size": 1
        }
        with set_config_options(override_options):
            table = TEST_DIR + "/table"
            for i in range(3):
                yt.write_table("<append=%true>" + table, [{"x": i, "y": j} for j in range(100)])
            assert yt.get(table + "/@chunk_count") == 300
            assert list(yt.read_table(table)) == [{"x": i, "y": j} for i in range(3) for j in range(100)]


@pytest.mark.usefixtures("yt_env")
class TestTableCommandsJsonFormat(object):
    @classmethod
    def setup_class(cls):
        cls._enable_ujson = False

    def _create_format(self, **kwargs):
        return yt.JsonFormat(enable_ujson=self._enable_ujson, **kwargs)

    def _legace_mode_checks(self):
        table = TEST_DIR + "/table"
        yt.create("table", table)

        # utf-8 correct data
        row = {b"\xc3\xbf": b"\xc3\xae"}
        yt.write_table(table, [row])

        # Legacy mode
        assert list(yt.read_table(table, format=self._create_format()))[0] == {u"\xc3\xbf": u"\xc3\xae"}
        assert list(yt.read_table(table, format=self._create_format(encode_utf8=True)))[0] == {u"\xc3\xbf": u"\xc3\xae"}
        # Specified encoding mode
        assert list(yt.read_table(table, format=self._create_format(encoding=None)))[0] == {b"\xc3\xbf": b"\xc3\xae"}
        assert list(yt.read_table(table, format=self._create_format(encoding=None, encode_utf8=True)))[0] == {b"\xc3\xbf": b"\xc3\xae"}
        assert list(yt.read_table(table, format=self._create_format(encoding=None, encode_utf8=False)))[0] == {b"\xc3\xbf": b"\xc3\xae"}
        assert list(yt.read_table(table, format=self._create_format(encoding="utf-8")))[0] == {u"\xff": u"\xee"}
        assert list(yt.read_table(table, format=self._create_format(encoding="utf-8", encode_utf8=True)))[0] == {u"\xff": u"\xee"}
        assert list(yt.read_table(table, format=self._create_format(encoding="utf-8", encode_utf8=False)))[0] == {u"\xff": u"\xee"}

        # non-utf-8 data
        row = {b"\xFF": b"\xEE"}
        yt.write_table(table, [row])

        # Legacy mode
        assert list(yt.read_table(table, format=self._create_format()))[0] == {u"\xff": u"\xee"}
        with pytest.raises(yt.YtError):
            yt.read_table(table, format=self._create_format(encode_utf8=False))
        assert list(yt.read_table(table, format=self._create_format()))[0] == {u"\xff": u"\xee"}

        # Specified encoding mode
        assert list(yt.read_table(table, format=self._create_format(encoding=None)))[0] == {b"\xff": b"\xee"}
        assert list(yt.read_table(table, format=self._create_format(encoding=None, encode_utf8=True)))[0] == {b"\xff": b"\xee"}
        with pytest.raises(yt.YtFormatError):  # UnicodeDecodeError
            assert list(yt.read_table(table, format=self._create_format(encoding="utf-8")))[0] == {u"\xff": u"\xee"}
        with pytest.raises(yt.YtFormatError):  # UnicodeDecodeError
            list(yt.read_table(table, format=self._create_format(encoding="utf-8", encode_utf8=True)))
        with pytest.raises(yt.YtError):
            yt.read_table(table, format=self._create_format(encoding="utf-8", encode_utf8=False))

    @authors("ignat")
    def test_json_encoding_read(self):
        try:
            yt_format.JSON_ENCODING_LEGACY_MODE = True
            self._legace_mode_checks()
        finally:
            yt_format.JSON_ENCODING_LEGACY_MODE = False

    @authors("ignat")
    def test_json_encoding_write(self):
        table = TEST_DIR + "/table"
        yt.create("table", table)

        # TODO(ignat): test ujson

        # utf-8 correct data
        row = {b"\xc3\xbf": b"\xc3\xae"}
        row_unicode = {b"\xc3\xbf".decode("utf-8"): b"\xc3\xae".decode("utf-8")}

        for encode_utf8 in (True, None):
            format = self._create_format(encoding=None, encode_utf8=encode_utf8)
            yt.write_table(table, [row], format=format)
            assert list(yt.read_table(table, format=yt.YsonFormat(encoding=None)))[0] == {b"\xc3\xbf": b"\xc3\xae"}
            assert list(yt.read_table(table, format=format))[0] == row

        with pytest.raises(yt.YtFormatError):
            yt.write_table(table, [row], format=self._create_format(encoding=None, encode_utf8=False))

        for encode_utf8 in (False, True, None):
            format = self._create_format(encoding="utf-8", encode_utf8=encode_utf8)
            yt.write_table(table, [row_unicode], format=format)
            assert list(yt.read_table(table, format=yt.YsonFormat(encoding=None)))[0] == {b"\xc3\xbf": b"\xc3\xae"}
            assert list(yt.read_table(table, format=format))[0] == row_unicode

        # ascii row
        row_ascii = {u"\x77": u"\x66"}
        for encode_utf8 in (True, None):
            format = self._create_format(encoding="utf-16", encode_utf8=encode_utf8)
            yt.write_table(table, [row_ascii], format=format)
            assert list(yt.read_table(table, format=yt.YsonFormat(encoding=None)))[0] == {u"\x77".encode("utf-16"): u"\x66".encode("utf-16")}
            assert list(yt.read_table(table, format=format))[0] == row_ascii

        with pytest.raises(yt.YtFormatError):
            yt.write_table(table, [row_ascii], format=self._create_format(encoding="utf-16", encode_utf8=False))


class TestTableCommandsJsonFormatUjson(TestTableCommandsJsonFormat):
    @classmethod
    def setup_class(cls):
        super(TestTableCommandsJsonFormatUjson, cls).setup_class()
        cls._enable_ujson = True


# NB: Use _check_delay to validate that framing actually works.
# heavy_request_timeout and delay_before_command are tweaked so that
# without framing we get read timeout.
@contextmanager
def _check_delay(yt_env_with_framing):
    delay_before_command = yt_env_with_framing.framing_options["delay_before_command"]
    start = datetime.now()
    yield
    assert datetime.now() - start > timedelta(milliseconds=delay_before_command)


@pytest.mark.usefixtures("yt_env_with_framing")
class TestTableCommandsFraming:
    OVERRIDE_OPTIONS = {
        "read_retries/use_locked_node_id": False,
        "proxy/heavy_request_timeout": 2 * 1000,
        "proxy/commands_with_framing": ["read_table", "get_table_columnar_statistics"],
    }

    @authors("ermolovd")
    def test_bad_write_YIML_128(self):
        path = TEST_DIR + "/test-bad-framing-write"
        yt.create("table", path, attributes={
            "schema": [{"name": "a", "type": "string"}]
        })

        def gen_data():
            data = b"{a=42};" * (1024 * 1024)
            while True:
                yield data

        options = {
            "proxy/content_encoding": "gzip",
            "proxy/accept_encoding": "gzip",
            "proxy/commands_with_framing": ["write_table"],
        }
        with set_config_options(options):
            with pytest.raises(yt.YtError, match="Invalid type: expected \"string\", actual \"int64\""):
                yt.write_table(path, gen_data(), format="yson", raw=True)

    @authors("levysotsky")
    @pytest.mark.parametrize("use_compression", [True, False])
    def test_read(self, use_compression, yt_env_with_framing):
        suspending_path = yt_env_with_framing.framing_options["suspending_path"]
        yt.create("table", suspending_path)
        chunk_size = 100
        rows_chunk = [{"column_1": 1, "column_2": "foo"}] * chunk_size
        chunk_count = 100
        for i in range(chunk_count):
            yt.write_table(yt.TablePath(suspending_path, append=True), rows_chunk)
        override_options = deepcopy(self.OVERRIDE_OPTIONS)
        if not use_compression:
            override_options["proxy/accept_encoding"] = "identity"
        with _check_delay(yt_env_with_framing), set_config_options(override_options):
            read_rows = yt.read_table(suspending_path)
        check_rows_equality(read_rows, rows_chunk * chunk_count)

    @authors("levysotsky")
    def test_error_in_read(self, yt_env_with_framing):
        suspending_path = yt_env_with_framing.framing_options["suspending_path"]
        row_count = 10000
        rows = [{"a": random_string(100)} for _ in range(row_count)]
        rows.append({"b": 12})
        yt.write_table(suspending_path, rows)
        with _check_delay(yt_env_with_framing), \
                set_config_options(self.OVERRIDE_OPTIONS), \
                pytest.raises(yt.YtResponseError):
            for _ in yt.read_table(suspending_path, format=yt.SchemafulDsvFormat(columns=["a"])):
                pass

    @authors("levysotsky")
    def test_get_table_columnar_statistics(self, yt_env_with_framing):
        suspending_path = yt_env_with_framing.framing_options["suspending_path"]
        yt.create("table", suspending_path)
        yt.write_table(suspending_path, [{"column_1": 1, "column_2": "foo"}])
        with _check_delay(yt_env_with_framing), set_config_options(self.OVERRIDE_OPTIONS):
            statistics = yt.get_table_columnar_statistics(suspending_path + "{column_1}")
        assert len(statistics) == 1
        assert "column_data_weights" in statistics[0]


@pytest.mark.usefixtures("yt_env")
class TestTableCommandsMultiChunk(object):
    @authors("abodrov")
    @pytest.mark.parametrize("write_parallel", [True, False])
    def test_storage_attributes_preserved_on_multi_chunk(self, write_parallel):
        storage_attributes = {
            'compression_codec': 'zlib_3',
            'schema': yt.schema.TableSchema().add_column("a", typing.Int8),
            'optimize_for': 'scan'
        }
        with set_config_option("write_parallel/enable", write_parallel):
            with set_config_option("write_retries/chunk_size", 1):
                yt.write_table(yt.YPath("//tmp/table", attributes=storage_attributes), [{"a": 1}] * 100)

        assert yt.get("//tmp/table/@compression_codec") == "zlib_3"
        assert yt.get("//tmp/table/@compression_statistics").keys() == {"zlib_3"}
        assert yt.get_table_schema("//tmp/table") == storage_attributes["schema"]
        assert yt.get("//tmp/table/@optimize_for") == "scan"

    @authors("abodrov")
    @pytest.mark.parametrize("write_parallel", [True, False])
    def test_storage_attributes_from_parent_preserved_on_multi_chunk(self, write_parallel):
        zlib3_compressed_path = TEST_DIR + '/zlib3_compressed'
        yt.create("map_node", zlib3_compressed_path, attributes={"compression_codec": "zlib_3", "optimize_for": "scan"}, recursive=True)
        table_path = zlib3_compressed_path + "/table"
        with set_config_option("write_parallel/enable", write_parallel):
            with set_config_option("write_retries/chunk_size", 1):
                yt.write_table(yt.YPath(table_path), [{"a": 1}] * 100)

        assert yt.get(table_path + "/@compression_codec") == "zlib_3"
        assert yt.get(table_path + "/@compression_statistics").keys() == {"zlib_3"}
        assert yt.get(table_path + "/@optimize_for") == "scan"

    @authors("abodrov")
    @pytest.mark.parametrize("write_parallel", [True, False])
    def test_table_specific_storage_attributes_from_directory_are_ignored_on_multi_chunk(self, write_parallel):
        specials_set = TEST_DIR + '/specials_set'
        dir_attributes = {"schema": yt.schema.TableSchema().add_column("a", typing.Int8)}
        yt.create("map_node", specials_set, attributes=dir_attributes, recursive=True)
        table_path = specials_set + "/table"
        with set_config_option("write_parallel/enable", write_parallel):
            with set_config_option("write_retries/chunk_size", 1):
                yt.write_table(yt.YPath(table_path), [{"a": 1}] * 100)

        assert not yt.get(table_path + "/@schema")
