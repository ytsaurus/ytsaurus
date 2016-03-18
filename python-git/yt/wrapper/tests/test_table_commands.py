import yt.wrapper.py_wrapper as py_wrapper
from yt.wrapper.table import TablePath, TempTable
from yt.wrapper.client import Yt
import yt.wrapper as yt

from helpers import TEST_DIR, check, get_temp_dsv_records

import os
import pytest
import tempfile
import shutil
import time
from StringIO import StringIO
from itertools import imap


@pytest.mark.usefixtures("yt_env")
class TestTableCommands(object):
    def setup(self):
        yt.config["tabular_data_format"] = yt.format.DsvFormat()

    def _test_read_write(self):
        table = TEST_DIR + "/table"
        yt.create_table(table)
        check([], yt.read_table(table))

        yt.write_table(table, "x=1\n")
        check(["x=1\n"], yt.read_table(table))

        yt.write_table(table, ["x=1\n"])
        check(["x=1\n"], yt.read_table(table))

        yt.write_table(table, [{"x": 1}], raw=False)
        check(["x=1\n"], yt.read_table(table))

        yt.write_table(table, iter(["x=1\n"]))
        check(["x=1\n"], yt.read_table(table))

        yt.write_table(yt.TablePath(table, append=True), ["y=1\n"])
        check(["x=1\n", "y=1\n"], yt.read_table(table))

        yt.write_table(yt.TablePath(table), ["x=1\n", "y=1\n"])
        check(["x=1\n", "y=1\n"], yt.read_table(table))

        yt.write_table(table, ["y=1\n"])
        check(["y=1\n"], yt.read_table(table))

        yt.write_table(table, StringIO("y=1\n"), raw=True, format=yt.DsvFormat())
        check(["y=1\n"], yt.read_table(table))

        response_parameters = {}
        yt.read_table(table, response_parameters=response_parameters)
        assert {"start_row_index": 0, "approximate_row_count": 1} == response_parameters

        yt.write_table(table, [{"y": "1"}], raw=False)
        assert [{"y": "1"}] == list(yt.read_table(table, raw=False))

        yt.config["tabular_data_format"] = None
        try:
            yt.write_table(table, ["x=1\n"], format="dsv")
        finally:
            yt.config["tabular_data_format"] = yt.format.DsvFormat()

    def test_read_write_with_retries(self):
        old_value = yt.config["write_retries"]["enable"]
        yt.config["write_retries"]["enable"] = True
        try:
            self._test_read_write()
        finally:
            yt.config["write_retries"]["enable"] = old_value

    def test_read_write_without_retries(self):
        old_value = yt.config["write_retries"]["enable"]
        yt.config["write_retries"]["enable"] = False
        try:
            self._test_read_write()
        finally:
            yt.config["write_retries"]["enable"] = old_value

    def test_empty_table(self):
        dir = TEST_DIR + "/dir"
        table = dir + "/table"

        with pytest.raises(yt.YtError):
            yt.create_table(table)
        with pytest.raises(yt.YtError):
            yt.row_count(table)

        yt.create_table(table, recursive=True, replication_factor=3)
        assert yt.row_count(table) == 0
        check([], yt.read_table(table, format=yt.DsvFormat()))

        yt.create_table(TEST_DIR + "/compressed", compression_codec="zlib9")
        assert yt.row_count(TEST_DIR + "/compressed") == 0

        yt.run_erase(table)
        assert yt.row_count(table) == 0

        yt.remove(dir, recursive=True)
        with pytest.raises(yt.YtError):
            yt.create_table(table)

    def test_create_temp_table(self):
        table = yt.create_temp_table(path=TEST_DIR)
        assert table.startswith(TEST_DIR)

        table = yt.create_temp_table(path=TEST_DIR, prefix="prefix")
        assert table.startswith(TEST_DIR + "/prefix")

        with TempTable() as table:
            assert yt.exists(table)
        assert not yt.exists(table)

    def test_write_many_chunks(self):
        yt.config.WRITE_BUFFER_SIZE = 1
        table = TEST_DIR + "/table"
        yt.write_table(table, ["x=1\n", "y=2\n", "z=3\n"])
        yt.write_table(table, ["x=1\n", "y=2\n", "z=3\n"])
        yt.write_table(table, ["x=1\n", "y=2\n", "z=3\n"])

    def test_binary_data_with_dsv(self):
        record = {"\tke\n\\\\y=": "\\x\\y\tz\n"}

        table = TEST_DIR + "/table"
        yt.write_table(table, map(yt.dumps_row, [record]))
        assert [record] == map(yt.loads_row, yt.read_table(table))

    def test_mount_unmount(self, yt_env):
        if yt.config["api_version"] == "v2":
            pytest.skip()

        table = TEST_DIR + "/table"
        yt.create_table(table)
        if yt_env.version < "0.18":
            yt.set(table + "/@schema", [{"name": name, "type": "string"} for name in ["x", "y"]])
            yt.set(table + "/@key_columns", ["x"])
        else:
            yt.set(table + "/@schema", [
                {"name": "x", "type": "string", "sort_order": "ascending"},
                {"name": "y", "type": "string"}])

        tablet_id = yt.create("tablet_cell", attributes={"size": 1})
        while yt.get("//sys/tablet_cells/{0}/@health".format(tablet_id)) != 'good':
            time.sleep(0.1)

        yt.mount_table(table)
        while yt.get("{0}/@tablets/0/state".format(table)) != 'mounted':
            time.sleep(0.1)

        yt.unmount_table(table)
        while yt.get("{0}/@tablets/0/state".format(table)) != 'unmounted':
            time.sleep(0.1)

    @pytest.mark.xfail(run = False, reason = "In progress")
    def test_select(self):
        if yt.config["api_version"] == "v2":
            pytest.skip()

        table = TEST_DIR + "/table"

        def select():
            schema = '<schema=[{"name"="x";"type"="int64"}; {"name"="y";"type"="int64"}; {"name"="z";"type"="int64"}]>'
            return list(yt.select_rows(
                '* from [{0}{1}]'.format(schema, table),
                format=yt.YsonFormat(format="text", process_table_index=False),
                raw=False))

        yt.remove(table, force=True)
        yt.create_table(table)
        yt.run_sort(table, sort_by=["x"])

        yt.set(table + "/@schema", [
            {"name": "x", "type": "int64", "sort_order": "ascending"},
            {"name": "y", "type": "int64"},
            {"name": "z", "type": "int64"}])

        assert [] == select()

        yt.write_table(yt.TablePath(table, append=True, sorted_by=["x"]),
                       ["{x=1;y=2;z=3}"], format=yt.YsonFormat())

        assert [{"x": 1, "y": 2, "z": 3}] == select()

    def test_insert_lookup_delete(self, yt_env):
        if yt.config["api_version"] == "v2":
            pytest.skip()

        yt.config["tabular_data_format"] = None
        try:
            # Name must differ with name of table in select test because of metadata caches
            table = TEST_DIR + "/table2"
            yt.remove(table, force=True)
            yt.create_table(table)
            if yt_env.version < "0.18":
                yt.set(table + "/@schema", [{"name": name, "type": "string"} for name in ["x", "y"]])
                yt.set(table + "/@key_columns", ["x"])
            else:
                yt.set(table + "/@schema", [
                    {"name": "x", "type": "string", "sort_order": "ascending"},
                    {"name": "y", "type": "string"}])

            tablet_id = yt.create("tablet_cell", attributes={"size": 1})
            while yt.get("//sys/tablet_cells/{0}/@health".format(tablet_id)) != 'good':
                time.sleep(0.1)

            yt.mount_table(table)
            while yt.get("{0}/@tablets/0/state".format(table)) != 'mounted':
                time.sleep(0.1)

            yt.insert_rows(table, [{"x": "a", "y": "b"}], raw=False)
            assert [{"x": "a", "y": "b"}] == list(yt.select_rows("* from [{0}]".format(table), raw=False))

            yt.insert_rows(table, [{"x": "c", "y": "d"}], raw=False)
            assert [{"x": "c", "y": "d"}] == list(yt.lookup_rows(table, [{"x": "c"}], raw=False))

            yt.delete_rows(table, [{"x": "a"}], raw=False)
            assert [{"x": "c", "y": "d"}] == list(yt.select_rows("* from [{0}]".format(table), raw=False))
        finally:
            yt.config["tabular_data_format"] = yt.format.DsvFormat()

    def test_insert_lookup_delete_with_transaction(self, yt_env):
        if yt.config["backend"] != "native":
            pytest.skip()
        if not yt_env.version.startswith("0.17.4"):
            pytest.skip()

        yt.config["tabular_data_format"] = None
        try:
            # Name must differ with name of table in select test because of metadata caches
            table = TEST_DIR + "/table2"
            yt.remove(table, force=True)
            yt.create_table(table)
            if yt_env.version < "0.18":
                yt.set(table + "/@schema", [{"name": name, "type": "string"} for name in ["x", "y"]])
                yt.set(table + "/@key_columns", ["x"])
            else:
                yt.set(table + "/@schema", [
                    {"name": "x", "type": "string", "sort_order": "ascending"},
                    {"name": "y", "type": "string"}])

            tablet_id = yt.create("tablet_cell", attributes={"size": 1})
            while yt.get("//sys/tablet_cells/{0}/@health".format(tablet_id)) != 'good':
                time.sleep(0.1)

            yt.mount_table(table)
            while yt.get("{0}/@tablets/0/state".format(table)) != 'mounted':
                time.sleep(0.1)

            vanilla_client = Yt(config=yt.config)

            assert list(vanilla_client.select_rows("* from [{0}]".format(table), raw=False)) == []
            assert list(vanilla_client.lookup_rows(table, [{"x": "a"}], raw=False)) == []

            with yt.Transaction(type="tablet", sticky=True):
                yt.insert_rows(table, [{"x": "a", "y": "a"}], raw=False)
                assert list(vanilla_client.select_rows("* from [{0}]".format(table), raw=False)) == []
                assert list(vanilla_client.lookup_rows(table, [{"x": "a"}], raw=False)) == []

            assert list(vanilla_client.select_rows("* from [{0}]".format(table), raw=False)) == [{"x": "a", "y": "a"}]
            assert list(vanilla_client.lookup_rows(table, [{"x": "a"}], raw=False)) == [{"x": "a", "y": "a"}]

            class FakeError(RuntimeError):
                pass

            with pytest.raises(FakeError):
                with yt.Transaction(type="tablet", sticky=True):
                    yt.insert_rows(table, [{"x": "b", "y": "b"}], raw=False)
                    raise FakeError()

            assert list(vanilla_client.select_rows("* from [{0}]".format(table), raw=False)) == [{"x": "a", "y": "a"}]
            assert list(vanilla_client.lookup_rows(table, [{"x": "a"}], raw=False)) == [{"x": "a", "y": "a"}]
        finally:
            yt.config["tabular_data_format"] = yt.format.DsvFormat()


    def test_start_row_index(self):
        table = TEST_DIR + "/table"

        yt.write_table(yt.TablePath(table, sorted_by=["a"]), ["a=b\n", "a=c\n", "a=d\n"])

        rsp = yt.read_table(table)
        assert rsp.response_parameters == {"start_row_index": 0L,
                                           "approximate_row_count": 3L}

        rsp = yt.read_table(yt.TablePath(table, start_index=1))
        assert rsp.response_parameters == {"start_row_index": 1L,
                                           "approximate_row_count": 2L}

        rsp = yt.read_table(yt.TablePath(table, lower_key=["d"]))
        assert rsp.response_parameters == \
            {"start_row_index": 2L,
             # When reading with key limits row count is estimated rounded up to the chunk row count.
             "approximate_row_count": 3L}

        rsp = yt.read_table(yt.TablePath(table, lower_key=["x"]))
        assert rsp.response_parameters == {"approximate_row_count": 0L}

    def test_table_index(self):
        dsv = yt.format.DsvFormat(enable_table_index=True, table_index_column="TableIndex")
        schemaful_dsv = yt.format.SchemafulDsvFormat(columns=['1', '2', '3'],
                                                     enable_table_index=True,
                                                     table_index_column="_table_index_")

        src_table_a = TEST_DIR + '/in_table_a'
        src_table_b = TEST_DIR + '/in_table_b'
        dst_table_a = TEST_DIR + '/out_table_a'
        dst_table_b = TEST_DIR + '/out_table_b'
        dst_table_ab = TEST_DIR + '/out_table_ab'

        len_a = 5
        len_b = 3

        yt.create_table(src_table_a, recursive=True, ignore_existing=True)
        yt.create_table(src_table_b, recursive=True, ignore_existing=True)
        yt.write_table(src_table_a, "1=a\t2=a\t3=a\n" * len_a, format=dsv)
        yt.write_table(src_table_b, "1=b\t2=b\t3=b\n" * len_b, format=dsv)

        assert yt.row_count(src_table_a) == len_a
        assert yt.row_count(src_table_b) == len_b

        def mix_table_indexes(row):
            row["_table_index_"] = row["TableIndex"]
            yield row
            row["_table_index_"] = 2
            yield row

        yt.table_commands.run_map(binary=mix_table_indexes,
                                  source_table=[src_table_a, src_table_b],
                                  destination_table=[dst_table_a, dst_table_b, dst_table_ab],
                                  input_format=dsv,
                                  output_format=schemaful_dsv)
        assert yt.row_count(dst_table_b) == len_b
        assert yt.row_count(dst_table_a) == len_a
        assert yt.row_count(dst_table_ab) == len_a + len_b
        for table in (dst_table_a, dst_table_b, dst_table_ab):
            row = yt.read_table(table, raw=False).next()
            for field in ("@table_index", "TableIndex", "_table_index_"):
                assert field not in row

    def test_erase(self):
        table = TEST_DIR + "/table"
        yt.write_table(table, get_temp_dsv_records())
        assert yt.row_count(table) == 10
        yt.run_erase(TablePath(table, start_index=0, end_index=5))
        assert yt.row_count(table) == 5
        yt.run_erase(TablePath(table, start_index=0, end_index=5))
        assert yt.row_count(table) == 0

    def test_read_with_table_path(self, yt_env):
        table = TEST_DIR + "/table"
        yt.write_table(table, ["y=w3\n", "x=b\ty=w1\n", "x=a\ty=w2\n"])
        yt.run_sort(table, sort_by=["x", "y"])

        def read_table(**kwargs):
            return list(yt.read_table(TablePath(table, **kwargs), raw=False))

        assert read_table(lower_key="a", upper_key="d") == [{"x": "a", "y": "w2"},
                                                            {"x": "b", "y": "w1"}]
        assert read_table(columns=["y"]) == [{"y": "w" + str(i)} for i in [3, 2, 1]]
        assert read_table(lower_key="a", end_index=2, columns=["x"]) == [{"x": "a"}]
        assert read_table(start_index=0, upper_key="b") == [{"y": "w3"}, {"x": "a", "y": "w2"}]
        assert read_table(start_index=1, columns=["x"]) == [{"x": "a"}, {"x": "b"}]
        assert read_table(ranges=[{"lower_limit": {"row_index": 1}}], columns=["x"]) == [{"x": "a"}, {"x": "b"}]

        assert list(yt.read_table(table + "{y}[:#2]")) == ["y=w3\n", "y=w2\n"]
        assert list(yt.read_table(table + "[#1:]")) == ["x=a\ty=w2\n", "x=b\ty=w1\n"]

        assert list(yt.read_table("<ranges=[{"
                                  "lower_limit={key=[b]}"
                                  "}]>" + table)) == ["x=b\ty=w1\n"]
        assert list(yt.read_table("<ranges=[{"
                                  "upper_limit={row_index=2}"
                                  "}]>" + table)) == ["y=w3\n", "x=a\ty=w2\n"]

        with pytest.raises(yt.YtError):
            assert read_table(ranges=[{"lower_limit": {"index": 1}}], end_index=1)
        with pytest.raises(yt.YtError):
            yt.read_table(TablePath(table, lower_key="a", start_index=1))
        with pytest.raises(yt.YtError):
            yt.read_table(TablePath(table, upper_key="c", end_index=1))

        if yt_env.version >= "0.18":
            table_path = TablePath(table, exact_index=1)
            assert list(yt.read_table(table_path.to_yson_string())) == ["x=a\ty=w2\n"]

        yt.write_table(table, ["x=b\n", "x=a\n", "x=c\n"])
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

    def test_huge_table(self):
        table = TEST_DIR + "/table"
        power = 3
        records = imap(yt.dumps_row, ({"k": i, "s": i * i, "v": "long long string with strange symbols"
                                                                " #*@*&^$#%@(#!@:L|L|KL..,,.~`"}
                                      for i in xrange(10 ** power)))
        yt.write_table(table, yt.StringIterIO(records))

        assert yt.row_count(table) == 10 ** power

        row_count = 0
        for _ in yt.read_table(table):
            row_count += 1
        assert row_count == 10 ** power

    def test_remove_locks(self):
        from yt.wrapper.table_commands import _remove_locks
        table = TEST_DIR + "/table"
        yt.create_table(table)
        try:
            for _ in xrange(5):
                tx = yt.start_transaction(timeout=10000)
                yt.config.TRANSACTION = tx
                yt.lock(table, mode="shared")
            yt.config.TRANSACTION = "0-0-0-0"
            assert len(yt.get_attribute(table, "locks")) == 5
            _remove_locks(table)
            assert yt.get_attribute(table, "locks") == []

            tx = yt.start_transaction(timeout=10000)
            yt.config.TRANSACTION = tx
            yt.lock(table)
            yt.config.TRANSACTION = "0-0-0-0"
            _remove_locks(table)
            assert yt.get_attribute(table, "locks") == []
        finally:
            yt.config.TRANSACTION = "0-0-0-0"

    def _set_banned(self, value):
        # NB: we cannot unban proxy using proxy, so we must using client for that.
        client = Yt(config={"backend": "native", "driver_config": yt.config["driver_config"]})
        proxy = "//sys/proxies/" + client.list("//sys/proxies")[0]
        client.set(proxy + "/@banned".format(proxy), value)
        time.sleep(1)

    def test_banned_proxy(self):
        if yt.config["backend"] == "native":
            pytest.skip()

        table = TEST_DIR + "/table"
        yt.create_table(table)

        self._set_banned("true")

        old_request_retry_count = yt.config["proxy"]["request_retry_count"]
        yt.config["proxy"]["request_retry_count"] = 1
        try:
            with pytest.raises(yt.YtProxyUnavailable):
                yt.get(table)

            try:
                yt.get(table)
            except yt.YtProxyUnavailable as err:
                assert "banned" in str(err)

        finally:
            yt.config["proxy"]["request_retry_count"] = old_request_retry_count
            self._set_banned("false")

    def test_error_occured_after_starting_to_write_chunked_requests(self):
        if yt.config["api_version"] != "v3":
            pytest.skip()

        yt.config["proxy"]["content_encoding"] = "identity"
        table = TEST_DIR + "/table"
        try:
            yt.write_table(table, iter(['{"abc": "123"}\n'] * 100000 + ["{a:b}"] + ['{"abc": "123"}\n'] * 100000), format=yt.JsonFormat())
        except yt.YtResponseError as err:
            assert "JSON" in str(err), "Incorrect error messager: " + str(err)
        else:
            assert False, "Failed to catch response error"

    def test_reliable_remove_tempfiles_in_py_wrap(self):
        old_tmp_dir = yt.config["local_temp_directory"]
        yt.config["local_temp_directory"] = tempfile.mkdtemp(dir=old_tmp_dir)

        def foo(rec):
            yield rec

        def uploader(files):
            assert files != []
            assert os.listdir(yt.config["local_temp_directory"]) != []

        try:
            assert os.listdir(yt.config["local_temp_directory"]) == []
            with pytest.raises(Exception):
                py_wrapper.wrap(function=foo, operation_type="mapper", uploader=None,
                                tempfiles_manager=None, client=None, input_format=None, output_format=None, group_by=None)
            assert os.listdir(yt.config["local_temp_directory"]) == []
            py_wrapper.wrap(function=foo, operation_type="mapper", uploader=uploader,
                            tempfiles_manager=None, client=None, input_format=None, output_format=None, group_by=None)
            assert os.listdir(yt.config["local_temp_directory"]) == []
        finally:
            shutil.rmtree(yt.config["local_temp_directory"])
            yt.config["local_temp_directory"] = old_tmp_dir

