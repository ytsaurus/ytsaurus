from __future__ import with_statement

from .helpers import (TEST_DIR, check, set_config_option, get_tests_sandbox, set_config_options,
                      wait, failing_heavy_request)

import yt.wrapper.py_wrapper as py_wrapper
from yt.wrapper.py_wrapper import OperationParameters
from yt.wrapper.table import TablePath, TempTable
from yt.wrapper.common import MB
from yt.wrapper import heavy_commands, parallel_writer

from yt.yson import YsonMap

from yt.packages.six.moves import xrange, map as imap

from yt.local import start, stop

import yt.wrapper as yt

import os
import pytest
import tempfile
import time
import uuid
import gzip
from io import BytesIO
from copy import deepcopy

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
    def _test_read_write(self):
        table = TEST_DIR + "/table"
        yt.create("table", table)
        check([], yt.read_table(table))

        yt.write_table(table, [{"x": 1}])
        check([{"x": 1}], yt.read_table(table))

        yt.write_table(table, [{"x": 1}])
        check([{"x": 1}], yt.read_table(table))

        yt.write_table(table, [{"x": 1}], raw=False)
        check([{"x": 1}], yt.read_table(table))

        yt.write_table(table, iter([{"x": 1}]))
        check([{"x": 1}], yt.read_table(table))

        yt.write_table(yt.TablePath(table, append=True), [{"y": 1}])
        check([{"x": 1}, {"y": 1}], yt.read_table(table))

        yt.write_table(yt.TablePath(table), [{"x": 1}, {"y": 1}])
        check([{"x": 1}, {"y": 1}], yt.read_table(table))

        yt.write_table(table, [{"y": 1}])
        check([{"y": 1}], yt.read_table(table))

        yt.write_table(table, BytesIO(b'{"y": 1}\n'), raw=True, format=yt.JsonFormat())
        check([{"y": 1}], yt.read_table(table))

        response_parameters = {}
        list(yt.read_table(table, response_parameters=response_parameters))
        assert response_parameters["start_row_index"] == 0
        assert response_parameters["approximate_row_count"] == 1

        yt.write_table(table, [{"y": "1"}], raw=False)
        assert [{"y": "1"}] == list(yt.read_table(table, raw=False))

        with set_config_option("tabular_data_format", yt.DsvFormat()):
            yt.write_table(table, [b"x=1\n"], raw=True)

        with set_config_option("read_retries/change_proxy_period", 1):
            yt.write_table(table, [{"y": i} for i in xrange(100)])
            check([{"y": i} for i in xrange(100)], yt.read_table(table))

        file = TEST_DIR + "/test_file"
        yt.create("file", file)
        with pytest.raises(yt.YtError):
            yt.read_table(file)

        yt.write_table(table, [{"x": i} for i in xrange(10)])
        client = yt.YtClient(config=deepcopy(yt.config.config))
        with set_config_option("read_parallel/max_thread_count", 2):
            with set_config_option("proxy/request_timeout", 1000):
                with set_config_option("proxy/retries/count", 3):
                    iterator = yt.read_table(table)

                    for transaction in client.search("//sys/transactions", attributes=["title", "id"]):
                        if transaction.attributes.get("title", "").startswith("Python wrapper: read"):
                            client.abort_transaction(transaction.attributes["id"])

                    time.sleep(5)
                    with pytest.raises(yt.YtError):
                        for _ in iterator:
                            pass

    def test_table_path(self, yt_env):
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

    def test_read_write_with_retries(self):
        with set_config_option("write_retries/enable", True):
            self._test_read_write()

    def test_read_write_without_retries(self):
        with set_config_option("write_retries/enable", False):
            self._test_read_write()

    def test_read_parallel(self, yt_env_with_rpc):
        if yt_env_with_rpc.version <= "19.6" and yt.config["backend"] == "rpc":
            pytest.skip()

        with set_config_option("read_parallel/enable", True):
            self._test_read_write()

    def test_parallel_write(self):
        with set_config_option("write_parallel/enable", True):
            with set_config_option("write_retries/chunk_size", 1):
                with set_config_option("write_parallel/concatenate_size", 3):
                    self._test_read_write()

    def test_schemaful_parallel_write(self):
        yt.create("table", "//tmp/table", recursive=True,
            attributes={
                "schema":
                    [
                        {"name": "id", "type": "int64"},
                        {"name": "value", "type": "double"},
                    ]
            })

        data = [{"id": i, "value": 0.9 * i} for i in xrange(64)]
        with set_config_option("write_parallel/enable", True):
            with set_config_option("write_retries/chunk_size", 256):
                yt.write_table("//tmp/table", data)

    def test_empty_table(self):
        dir = TEST_DIR + "/dir"
        table = dir + "/table"

        with pytest.raises(yt.YtError):
            yt.create("table", table)
        with pytest.raises(yt.YtError):
            yt.row_count(table)

        yt.create("table", table, recursive=True)
        assert yt.row_count(table) == 0
        check([], yt.read_table(table, format=yt.DsvFormat()))

        yt.run_erase(table)
        assert yt.row_count(table) == 0

        yt.remove(dir, recursive=True)
        with pytest.raises(yt.YtError):
            yt.create("table", table)

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

    def test_write_many_chunks(self):
        with set_config_option("write_retries/chunk_size", 1):
            table = TEST_DIR + "/table"
            for i in xrange(3):
                yt.write_table("<append=%true>" + table, [{"x": 1}, {"y": 2}, {"z": 3}])
            assert yt.get(table + "/@chunk_count") == 9

    @pytest.mark.parametrize("use_tmp_dir_for_intermediate_data", [True, False])
    def test_write_parallel_huge_table(self, use_tmp_dir_for_intermediate_data):
        override_options = {
            "write_parallel/concatenate_size": 7,
            "write_parallel/enable": True,
            "write_parallel/use_tmp_dir_for_intermediate_data": use_tmp_dir_for_intermediate_data,
            "write_retries/chunk_size": 1
        }
        with set_config_options(override_options):
            table = TEST_DIR + "/table"
            for i in xrange(3):
                yt.write_table("<append=%true>" + table, [{"x": i, "y": j} for j in xrange(100)])
            assert yt.get(table + "/@chunk_count") == 300
            assert list(yt.read_table(table)) == [{"x": i, "y": j} for i in xrange(3) for j in xrange(100)]

    def test_binary_data_with_dsv(self):
        with set_config_option("tabular_data_format", yt.DsvFormat()):
            record = {"\tke\n\\\\y=": "\\x\\y\tz\n"}
            table = TEST_DIR + "/table"
            yt.write_table(table, [yt.dumps_row(record)], raw=True)
            assert [record] == list(yt.read_table(table))

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

    def test_start_row_index_parallel(self, yt_env_with_rpc):
        if yt_env_with_rpc.version <= "19.6" and yt.config["backend"] == "rpc":
            pytest.skip()

        with set_config_option("read_parallel/enable", True):
            self.test_start_row_index()

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

    def test_erase(self):
        table = TEST_DIR + "/table"
        yt.write_table(table, [{"a": i} for i in xrange(10)])
        assert yt.row_count(table) == 10
        yt.run_erase(TablePath(table, start_index=0, end_index=5))
        assert yt.row_count(table) == 5
        yt.run_erase(TablePath(table, start_index=0, end_index=5))
        assert yt.row_count(table) == 0

    def test_read_with_table_path(self, yt_env):
        table = TEST_DIR + "/table"
        yt.write_table(table, [{"y": "w3"}, {"x": "b", "y": "w1"}, {"x": "a", "y": "w2"}])
        yt.run_sort(table, sort_by=["x", "y"])

        def read_table(**kwargs):
            kwargs["name"] = kwargs.get("name", table)
            return list(yt.read_table(TablePath(**kwargs), raw=False))

        assert read_table(lower_key="a", upper_key="d") == [{"x": "a", "y": "w2"},
                                                            {"x": "b", "y": "w1"}]
        assert read_table(columns=["y"]) == [{"y": "w" + str(i)} for i in [3, 2, 1]]
        assert read_table(lower_key="a", end_index=2, columns=["x"]) == [{"x": "a"}]
        assert read_table(start_index=0, upper_key="b") == [{"x": None, "y": "w3"}, {"x": "a", "y": "w2"}]
        assert read_table(start_index=1, columns=["x"]) == [{"x": "a"}, {"x": "b"}]
        assert read_table(ranges=[{"lower_limit": {"row_index": 1}}], columns=["x"]) == [{"x": "a"}, {"x": "b"}]

        assert read_table(name=table + "{y}[:#2]") == [{"y": "w3"}, {"y": "w2"}]
        assert read_table(name=table + "[#1:]") == [{"x": "a", "y": "w2"}, {"x": "b", "y": "w1"}]

        assert read_table(name=
                          "<ranges=[{"
                          "lower_limit={key=[b]}"
                          "}]>" + table) == [{"x": "b", "y": "w1"}]
        assert read_table(name=
                          "<ranges=[{"
                          "upper_limit={row_index=2}"
                          "}]>" + table) == [{"x": None, "y": "w3"}, {"x": "a", "y": "w2"}]

        with pytest.raises(yt.YtError):
            assert read_table(ranges=[{"lower_limit": {"index": 1}}], end_index=1)
        with pytest.raises(yt.YtError):
            read_table(name=TablePath(table, lower_key="a", start_index=1))
        with pytest.raises(yt.YtError):
            read_table(name=TablePath(table, upper_key="c", end_index=1))

        table_path = TablePath(table, exact_index=1)
        assert list(yt.read_table(table_path.to_yson_string(), format=yt.DsvFormat())) == [{"x": "a", "y": "w2"}]

        yt.write_table(table, [{"x": "b"}, {"x": "a"}, {"x": "c"}])
        with pytest.raises(yt.YtError):
            yt.read_table(TablePath(table, lower_key="a"))
        # No prefix
        with pytest.raises(yt.YtError):
            TablePath("abc")
        # Prefix should start with //
        yt.config["prefix"] = "abc/"
        with pytest.raises(yt.YtError):
            TablePath("abc")
        # Prefix should end with /
        yt.config["prefix"] = "//abc"
        with pytest.raises(yt.YtError):
            TablePath("abc")

    def test_read_parallel_with_table_path(self, yt_env):
        with set_config_option("read_parallel/enable", True):
            self.test_read_with_table_path(yt_env)

    def test_huge_table(self):
        table = TEST_DIR + "/table"
        power = 3
        format = yt.JsonFormat()
        records = imap(format.dumps_row, ({"k": i, "s": i * i, "v": "long long string with strange symbols"
                                                                    " #*@*&^$#%@(#!@:L|L|KL..,,.~`"}
                       for i in xrange(10 ** power)))
        yt.write_table(table, yt.StringIterIO(records), format=format, raw=True)

        assert yt.row_count(table) == 10 ** power

        row_count = 0
        for _ in yt.read_table(table):
            row_count += 1
        assert row_count == 10 ** power

    def test_read_parallel_huge_table(self, yt_env_with_rpc):
        if yt_env_with_rpc.version <= "19.6" and yt.config["backend"] == "rpc":
            pytest.skip()

        with set_config_option("read_parallel/enable", True):
            self.test_huge_table()

    def test_remove_locks(self):
        from yt.wrapper.table_helpers import _remove_locks
        table = TEST_DIR + "/table"
        yt.create("table", table)
        try:
            for _ in xrange(5):
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
        proxy = "//sys/proxies/" + client.list("//sys/proxies")[0]
        client.set(proxy + "/@banned".format(proxy), value)

        def check():
            try:
                yt.get("/")
                return not value
            except:
                return value

        time.sleep(0.5)
        wait(check)

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

    def test_error_occured_after_starting_to_write_chunked_requests(self):
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

    def test_reliable_remove_tempfiles_in_py_wrap(self):
        def foo(rec):
            yield rec

        client = yt.YtClient(config=yt.config.config)

        new_temp_dir = tempfile.mkdtemp(dir=yt.config["local_temp_directory"])
        client.config["local_temp_directory"] = new_temp_dir

        assert os.listdir(client.config["local_temp_directory"]) == []

        params = OperationParameters(input_format=None, output_format=None, operation_type="map", job_type="mapper", group_by=None)
        with pytest.raises(AttributeError):
            with py_wrapper.TempfilesManager(remove_temp_files=True, directory=py_wrapper.get_local_temp_directory(client)) as tempfiles_manager:
                py_wrapper.wrap(function=foo,
                                file_manager=None,
                                tempfiles_manager=tempfiles_manager,
                                params=params,
                                local_mode=False,
                                client=client)

        with py_wrapper.TempfilesManager(remove_temp_files=True, directory=py_wrapper.get_local_temp_directory(client)) as tempfiles_manager:
            py_wrapper.wrap(function=foo,
                            file_manager=FakeFileManager(client),
                            tempfiles_manager=tempfiles_manager,
                            params=params,
                            local_mode=False,
                            client=client)
        assert os.listdir(client.config["local_temp_directory"]) == []

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
                    check([{"x": "1"}, {"x": "2"}, {"x": "3"}], yt.read_table(TEST_DIR + "/table"))

    def _test_read_blob_table(self):
        table = TEST_DIR + "/test_blob_table"

        yt.create("table", table, attributes={"schema": [{"name": "key", "type": "string", "sort_order": "ascending"},
                                                         {"name": "part_index", "type": "int64",
                                                          "sort_order": "ascending"},
                                                         {"name": "data", "type": "string"}]})
        yt.write_table(table, [{"key": "test" + str(i),
                                "part_index": j,
                                "data": "data" + str(j) + str(i)} for i in xrange(3) for j in xrange(3)])

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
                                "data": "data" + str(j)} for j in xrange(3)])

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
                                "data": "data" + str(j)} for j in xrange(3)])
        yt.write_table("<append=%true>" + table, [{"key": "test", "part_index": 3, "data": "data03"}])
        with pytest.raises(yt.YtError):
            stream = yt.read_blob_table(table + "[test]")
            stream.read()

        yt.remove(table)
        yt.create("table", table, attributes={"schema": [{"name": "part_index", "type": "int64",
                                                          "sort_order": "ascending"},
                                                         {"name": "data", "type": "string"}]})

        yt.write_table(table, [{"part_index": i, "data": "data" + str(i)}
                               for i in xrange(3)])

        stream = yt.read_blob_table(table, part_size=5)
        assert stream.read() == b"data0data1data2"

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

    def test_read_blob_table_without_retries(self):
        with set_config_option("read_retries/enable", False):
            self._test_read_blob_table()

    def test_transform(self):
        table = TEST_DIR + "/test_transform_table"
        other_table = TEST_DIR + "/test_transform_table2"

        assert not yt.transform(table)

        yt.create("table", table)
        assert not yt.transform(table)

        yt.write_table(table, [{"x": 1}, {"x": 2}])

        yt.transform(table)
        check([{"x": 1}, {"x": 2}], yt.read_table(table))

        yt.transform(table, other_table)
        check([{"x": 1}, {"x": 2}], yt.read_table(other_table))

        yt.remove(other_table)
        assert yt.transform(table, other_table, compression_codec="zlib_6")
        assert yt.get(other_table + "/@compression_codec") == "zlib_6"
        assert not yt.transform(other_table, other_table, compression_codec="zlib_6", check_codecs=True)

        assert yt.transform(table, other_table, optimize_for="scan")
        assert yt.get(other_table + "/@optimize_for") == "scan"

        assert not yt.transform(other_table, other_table, erasure_codec="none", check_codecs=True)

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
                start_proxy=(yt.config["backend"] != "native"),
                start_rpc_proxy=(yt.config["backend"] == "rpc"),
                enable_debug_logging=True)
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

            client.set("//sys/cluster_nodes/{0}/@banned".format(address), True)
            wait(lambda: client.get("//sys/cluster_nodes/{0}/@state".format(address)) == "offline")

            assert list(client.read_table(table)) == [{"x": 1}, {"x": 2}]
        finally:
            if instance is not None:
                stop(instance.id, path=dir)

    def test_lazy_read(self):
        table = TEST_DIR + "/test_lazy_read_table"
        yt.write_table(table, [{"x": "abacaba", "y": 1}, {"z": 2}])
        stream = yt.read_table(table, format="<lazy=%true>yson")
        result = list(stream)

        assert not isinstance(result[0], (YsonMap, dict))
        assert result[0]["x"] == "abacaba"
        assert result[1]["z"] == 2

    def test_get_table_columnar_statistics(self):
        table = TEST_DIR + "/test_table"
        yt.write_table(table, [{"x": "abacaba", "y": 1}, {"x": 2}])

        res = yt.get_table_columnar_statistics(paths=[table + "{x}"])
        assert res
        assert res[0].get("column_data_weights", {}).get("x", 0) > 0

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

                check(rows, yt.read_table(table_path))

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

        check(rows, yt.read_table(table_path))

    @pytest.mark.parametrize("parallel,progress_bar", [(False, False), (True, False), (False, True),
                                                       (True, True)])
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

                    check(rows, yt.read_table(table_path))
