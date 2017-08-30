from __future__ import print_function

from .helpers import (TEST_DIR, TESTS_SANDBOX, TESTS_LOCATION,
                      get_environment_for_binary_test, check, set_config_options, set_config_option)

from yt.wrapper.exceptions_catcher import KeyboardInterruptsCatcher
from yt.wrapper.response_stream import ResponseStream, EmptyResponseStream
from yt.wrapper.retries import run_with_retries
from yt.wrapper.mappings import VerifiedDict, FrozenDict
from yt.wrapper.ypath import ypath_join
from yt.common import makedirp
from yt.yson import to_yson_type
import yt.yson as yson
import yt.json as json
import yt.subprocess_wrapper as subprocess

from yt.packages.six import iterkeys, itervalues, iteritems, PY3, Iterator, b
from yt.packages.six.moves import xrange, filter as ifilter

import yt.wrapper as yt

import inspect
import random
import string
import os
import sys
import uuid
import tempfile
import time
import pytest
import shutil
import collections
from copy import deepcopy

def test_docs_exist():
    functions = inspect.getmembers(
        yt, lambda o: inspect.isfunction(o) and not o.__name__.startswith("_"))

    functions_without_doc = list(ifilter(lambda pair: not inspect.getdoc(pair[1]), functions))
    assert not functions_without_doc

    classes = inspect.getmembers(yt, lambda o: inspect.isclass(o))
    for name, cl in classes:
        assert inspect.getdoc(cl)
        if name == "PingTransaction":
            continue # Python Thread is not documented O_o
        public_methods = inspect.getmembers(cl, lambda o: inspect.ismethod(o) and \
                                                          not o.__name__.startswith("_"))
        ignore_methods = set()
        if issubclass(cl, collections.Iterator) and not PY3:
            ignore_methods.add("next")

        methods_without_doc = [method for name, method in public_methods
                               if name not in ignore_methods and not inspect.getdoc(method)]
        assert not methods_without_doc

def test_ypath_join():
    assert ypath_join("a", "b") == "a/b"
    assert ypath_join("a/", "b") == "a/b"
    assert ypath_join("/", "b") == "//b"
    assert ypath_join("//", "b") == "//b"
    assert ypath_join("a", "b", "c") == "a/b/c"
    assert ypath_join("a", "//b", "c") == "//b/c"
    assert ypath_join("a", "//b", "/") == "/"
    assert ypath_join("/", "a", "/") == "/"
    assert ypath_join("/a", "/b", "/c") == "/a/b/c"
    assert ypath_join("/a", "/b", "c") == "/a/b/c"
    assert ypath_join("/a", "//b", "c") == "//b/c"

@pytest.mark.timeout(1200)
@pytest.mark.usefixtures("yt_env")
class TestYtBinary(object):
    def test_yt_binary(self):
        env = get_environment_for_binary_test()
        env["FALSE"] = "%false"
        env["TRUE"] = "%true"

        sandbox_dir = os.path.join(TESTS_SANDBOX, "TestYtBinary_" + uuid.uuid4().hex[:8])
        binaries_dir = os.path.join(os.path.dirname(TESTS_LOCATION), "bin")
        makedirp(sandbox_dir)
        try:
            test_binary = os.path.join(TESTS_LOCATION, "test_yt.sh")
            proc = subprocess.Popen([test_binary, sandbox_dir], env=env, cwd=binaries_dir)
            proc.communicate()
            assert proc.returncode == 0
        finally:
            shutil.rmtree(sandbox_dir, ignore_errors=True)

@pytest.mark.usefixtures("yt_env")
class TestMutations(object):
    def check_command(self, command, post_action=None, check_action=None, final_action=None):
        mutation_id = yt.common.generate_uuid()
        def run_command():
            yt.config.COMMAND_PARAMS["mutation_id"] = mutation_id
            try:
                result = command()
            finally:
                del yt.config.COMMAND_PARAMS["mutation_id"]
            return result

        result = run_command()
        if post_action is not None:
            post_action()
        for _ in xrange(5):
            yt.config.COMMAND_PARAMS["retry"] = True
            assert result == run_command()
            yt.config.COMMAND_PARAMS["retry"] = False
            if check_action is not None:
                assert check_action()

        if final_action is not None:
            final_action(result)

    def test_master_mutation_id(self):
        test_dir = ypath_join(TEST_DIR, "test")
        test_dir2 = ypath_join(TEST_DIR, "test2")
        test_dir3 = ypath_join(TEST_DIR, "test3")

        self.check_command(
            lambda: yt.set(test_dir, {"a": "b"}),
            lambda: yt.set(test_dir, {}),
            lambda: yt.get(test_dir) == {})

        self.check_command(
            lambda: yt.remove(test_dir3, force=True),
            lambda: yt.mkdir(test_dir3),
            lambda: yt.get(test_dir3) == {})

        parent_tx = yt.start_transaction()
        self.check_command(
            lambda: yt.start_transaction(parent_tx),
            None,
            lambda: len(yt.get("//sys/transactions/{0}/@nested_transaction_ids".format(parent_tx))) == 1)

        id = yt.start_transaction()
        self.check_command(lambda: yt.abort_transaction(id))

        id = yt.start_transaction()
        self.check_command(lambda: yt.commit_transaction(id))

        self.check_command(lambda: yt.move(test_dir, test_dir2))

    def test_scheduler_mutation_id(self):
        def abort(operation_id):
            yt.abort_operation(operation_id)
            time.sleep(1.0) # Wait for aborting transactions

        table = TEST_DIR + "/table"
        other_table = TEST_DIR + "/other_table"
        yt.write_table(table, [{"x": 1}, {"x": 2}])
        yt.create_table(other_table)

        params = {
            "spec": {
                "mapper": {
                    "command": "sleep 2; cat"
                },
                "input_table_paths": [table],
                "output_table_paths": [other_table]
            },
            "output_format": "json"
        }

        operations_count = yt.get("//sys/operations/@count")

        self.check_command(
            lambda: yson.loads(yt.driver.make_request("map", params, decode_content=False)),
            None,
            lambda: yt.get("//sys/operations/@count") == operations_count + 1,
            abort)


@pytest.mark.usefixtures("yt_env")
class TestRetries(object):
    def test_run_with_retries(self):
        def action():
            if random.randint(0, 3) == 1:
                raise yt.YtError()
            return 1

        assert 1 == run_with_retries(action, backoff=0.1)

    def test_read_with_retries(self):
        old_value = yt.config["read_retries"]["enable"]
        yt.config["read_retries"]["enable"] = True
        yt.config._ENABLE_READ_TABLE_CHAOS_MONKEY = True
        yt.config._ENABLE_HTTP_CHAOS_MONKEY = True
        try:
            table = TEST_DIR + "/table"

            with pytest.raises(yt.YtError):
                yt.read_table(table)

            yt.create_table(table)
            check([], yt.read_table(table))
            assert b"" == yt.read_table(table, format=yt.JsonFormat(), raw=True).read()

            yt.write_table(table, [{"x": 1}, {"y": 2}])
            check([{"x": 1}, {"y": 2}], yt.read_table(table))

            rsp = yt.read_table(table, format=yt.JsonFormat(), raw=True)
            assert json.loads(next(rsp)) == {"x": 1}
            yt.write_table(table, [{"x": 1}, {"y": 3}])
            assert json.loads(next(rsp)) == {"y": 2}
            rsp.close()

            rsp = yt.read_table(table, raw=False)
            assert [("x", 1), ("y", 3)] == sorted([list(iteritems(x))[0] for x in rsp])

            response_parameters = {}
            rsp = yt.read_table(table, response_parameters=response_parameters, format=yt.JsonFormat(), raw=True)
            assert {"start_row_index": 0, "approximate_row_count": 2} == response_parameters
            rsp.close()
        finally:
            yt.config._ENABLE_READ_TABLE_CHAOS_MONKEY = False
            yt.config._ENABLE_HTTP_CHAOS_MONKEY = False
            yt.config["read_retries"]["enable"] = old_value

    def test_read_ranges_with_retries(self, yt_env):
        old_value = yt.config["read_retries"]["enable"], yt.config["read_retries"]["allow_multiple_ranges"]
        yt.config["read_retries"]["enable"] = True
        yt.config["read_retries"]["allow_multiple_ranges"] = True
        yt.config._ENABLE_READ_TABLE_CHAOS_MONKEY = True
        try:
            table = TEST_DIR + "/table"

            yt.create_table(table)
            assert b"" == yt.read_table(table, format=yt.JsonFormat(), raw=True).read()

            yt.write_table("<sorted_by=[x]>" + table, [{"x": 1}, {"x": 2}, {"x": 3}])
            assert b'{"x":1}\n{"x":2}\n{"x":3}\n' == yt.read_table(table, format=yt.JsonFormat(), raw=True).read()
            assert [{"x":1}] == list(yt.read_table(table + "[#0]", format=yt.JsonFormat()))

            table_path = yt.TablePath(table, exact_key=[2])
            for i in xrange(10):
                assert [{"x": 2}] == list(yt.read_table(table_path))

            assert [{"x": 1}, {"x": 3}, {"x": 2}, {"x": 1}, {"x": 2}] == \
                [{"x": elem["x"]} for elem in yt.read_table(table + '[#0,"2":"1",#2,#1,1:3]', format=yt.YsonFormat())]

            assert [{"x": 1}, {"x": 3}, {"x": 2}, {"x": 1}, {"x": 2}] == \
                list(yt.read_table(table + '[#0,"2":"1",#2,#1,1:3]', format=yt.YsonFormat(process_table_index=False)))

            assert [to_yson_type(None, attributes={"range_index": 0}),
                    to_yson_type(None, attributes={"row_index": 0}),
                    {"x": 1},
                    to_yson_type(None, attributes={"range_index": 1}),
                    to_yson_type(None, attributes={"row_index": 2}),
                    {"x": 3}] == \
                list(yt.read_table(table + "[#0,#2]", format=yt.YsonFormat(process_table_index=False),
                                   control_attributes={"enable_row_index": True, "enable_range_index": True}))

            assert [{"$attributes": {"range_index": 0}, "$value": None},
                    {"$attributes": {"row_index": 0}, "$value": None},
                    {"x": 1},
                    {"$attributes": {"range_index": 1}, "$value": None},
                    {"$attributes": {"row_index": 2}, "$value": None},
                    {"x": 3}] == \
                list(yt.read_table(table + "[#0,#2]", format=yt.JsonFormat(process_table_index=False),
                                   control_attributes={"enable_row_index": True, "enable_range_index": True}))

            assert [{"x": 1, "@row_index": 0, "@range_index": 0, "@table_index": None},
                    {"x": 3, "@row_index": 2, "@range_index": 1, "@table_index": None}] == \
                list(yt.read_table(table + "[#0,#2]", format=yt.JsonFormat(process_table_index=True),
                                   control_attributes={"enable_row_index": True, "enable_range_index": True}))

            with pytest.raises(yt.YtError):
                list(yt.read_table(table + "[#0,2]", raw=False, format=yt.YsonFormat(process_table_index=False), unordered=True))

            assert [b"x=2\n", b"x=3\n"] == list(yt.read_table(table + "[2:]", raw=True, format=yt.DsvFormat()))

            with pytest.raises(yt.YtError):
                list(yt.read_table(table + "[#0,2]", raw=False, format=yt.DsvFormat()))

        finally:
            yt.config._ENABLE_READ_TABLE_CHAOS_MONKEY = False
            yt.config["read_retries"]["enable"] = old_value[0]
            yt.config["read_retries"]["allow_multiple_ranges"] = old_value[1]

    def test_read_parallel_with_retries(self):
        with set_config_option("read_parallel/enable", True):
            self.test_read_with_retries()

    def test_read_ranges_parallel_with_retries(self, yt_env):
        with set_config_option("read_parallel/enable", True):
            self.test_read_ranges_with_retries(yt_env)

    def test_heavy_requests_with_retries(self):
        table = TEST_DIR + "/table"

        old_request_timeout = yt.config["proxy"]["heavy_request_timeout"]
        old_enable_write_retries = yt.config["write_retries"]["enable"]

        yt.config["write_retries"]["enable"] = True
        yt.config["proxy"]["heavy_request_timeout"] = 1000
        yt.config._ENABLE_HEAVY_REQUEST_CHAOS_MONKEY = True

        _, filename = tempfile.mkstemp()
        with open(filename, "w") as fout:
            fout.write("retries test")

        try:
            yt.write_table(table, [{"x": 1}])
            yt.smart_upload_file(filename, placement_strategy="random")
            yt.write_table(table, [{"x": 1}])
            yt.write_table(table, [{"x": 1}])
            yt.smart_upload_file(filename, placement_strategy="random")
            yt.smart_upload_file(filename, placement_strategy="random")
            yt.write_table(table, [{"x": 1}])
        finally:
            yt.config._ENABLE_HEAVY_REQUEST_CHAOS_MONKEY = False
            yt.config["proxy"]["heavy_request_timeout"] = old_request_timeout
            yt.config["write_retries"]["enable"] = old_enable_write_retries

    def test_http_retries(self):
        old_request_timeout = yt.config["proxy"]["request_timeout"]
        yt.config._ENABLE_HTTP_CHAOS_MONKEY = True
        yt.config["proxy"]["request_timeout"] = 1000
        try:
            for backoff_time in [3000, None]:
                yt.config["proxy"]["request_backoff_time"] = backoff_time
                yt.get("/")
                yt.list("/")
                yt.exists("/")
                yt.exists(TEST_DIR)
                yt.exists(TEST_DIR + "/some_node")
                yt.set(TEST_DIR + "/some_node", {})
                yt.exists(TEST_DIR + "/some_node")
                yt.list(TEST_DIR)
        finally:
            yt.config._ENABLE_HTTP_CHAOS_MONKEY = False
            yt.config["proxy"]["request_timeout"] = old_request_timeout
            yt.config["proxy"]["request_backoff_time"] = None

    def test_download_with_retries(self):
        text = b"some long text repeated twice " * 2
        file_path = TEST_DIR + "/file"
        yt.write_file(file_path, text)
        assert text == yt.read_file(file_path).read()

        old_value = yt.config["read_retries"]["enable"]
        old_buffer_size = yt.config["read_buffer_size"]
        yt.config["read_retries"]["enable"] = True
        # NB: with lower values requests library works incorrectly :(
        yt.config["read_buffer_size"] = 16
        yt.config._ENABLE_READ_TABLE_CHAOS_MONKEY = True
        try:
            assert text == yt.read_file(file_path).read()
            assert b"twice " == yt.read_file(file_path, offset=len(text) - 6).read()
            assert b"some" == yt.read_file(file_path, length=4).read()
        finally:
            yt.config._ENABLE_READ_TABLE_CHAOS_MONKEY = False
            yt.config["read_retries"]["enable"] = old_value
            yt.config["read_buffer_size"] = old_buffer_size

    def test_write_retries_and_schema(self):
        table = TEST_DIR + "/table"

        old_request_retry_timeout = yt.config["proxy"]["request_retry_timeout"]
        old_enable_write_retries = yt.config["write_retries"]["enable"]
        old_chunk_size = yt.config["write_retries"]["chunk_size"]

        yt.config["write_retries"]["enable"] = True
        yt.config["write_retries"]["chunk_size"] = 100
        yt.config["proxy"]["request_retry_timeout"] = 1000
        yt.config._ENABLE_HEAVY_REQUEST_CHAOS_MONKEY = True

        data = [{"x": "y"} for _ in xrange(100)]
        try:
            yt.write_table("<schema=[{name=x;type=string}]>" + table, data)
            yt.write_table("<schema=[{name=x;type=string}]>" + table, data)
        finally:
            yt.config._ENABLE_HEAVY_REQUEST_CHAOS_MONKEY = False
            yt.config["proxy"]["request_retry_timeout"] = old_request_retry_timeout
            yt.config["write_retries"]["enable"] = old_enable_write_retries
            yt.config["write_retries"]["chunk_size"] = old_chunk_size

    def test_dynamic_tables_requests_retries(self):
        table = TEST_DIR + "/dyn_table"
        yt.create("table", table, attributes={
            "schema": [
                {"name": "x", "type": "string", "sort_order": "ascending"},
                {"name": "y", "type": "string"}
             ],
             "dynamic": True})

        tablet_id = yt.create("tablet_cell", attributes={"size": 1})
        while yt.get("//sys/tablet_cells/{0}/@health".format(tablet_id)) != "good":
            time.sleep(0.1)

        yt.mount_table(table, sync=True)
        yt.config._ENABLE_HEAVY_REQUEST_CHAOS_MONKEY = True
        override_options = {
            "write_retries/enable": True,
            "write_retries/count": 10,
            "read_retries/enable": True,
            "proxy/heavy_request_timeout": 1000,
            "dynamic_table_retries/count": 10,
        }

        try:
            with set_config_options(override_options):
                yt.insert_rows(table, [{"x": "a", "y": "b"}], raw=False)
                yt.insert_rows(table, [{"x": "c", "y": "d"}], raw=False)
                yt.delete_rows(table, [{"x": "a"}], raw=False)
                yt.lookup_rows(table, [{"x": "c"}], raw=False)
                yt.select_rows("* from [{0}]".format(table), raw=False)
                yt.insert_rows(table, [{"x": "a", "y": "b"}], raw=False)
                yt.insert_rows(table, [{"x": "c", "y": "d"}], raw=False)
                yt.delete_rows(table, [{"x": "a"}], raw=False)
                yt.lookup_rows(table, [{"x": "c"}], raw=False)
                yt.select_rows("* from [{0}]".format(table), raw=False)
        finally:
            yt.config._ENABLE_HEAVY_REQUEST_CHAOS_MONKEY = False

def test_wrapped_streams():
    import yt.wrapper.py_runner_helpers as runner_helpers
    with pytest.raises(yt.YtError):
        with runner_helpers.WrappedStreams():
            print("test")
    with pytest.raises(yt.YtError):
        with runner_helpers.WrappedStreams():
            sys.stdout.write("test")
    with pytest.raises(yt.YtError):
        with runner_helpers.WrappedStreams():
            input()
    with pytest.raises(yt.YtError):
        with runner_helpers.WrappedStreams():
            sys.stdin.read()
    with runner_helpers.WrappedStreams(wrap_stdout=False):
        print("")
        sys.stdout.write("")

def test_keyboard_interrupts_catcher():
    result = []

    def action():

        raise KeyboardInterrupt()
        result.append(True)
        if len(result) in [1, 3, 4, 5]:
            raise KeyboardInterrupt()

    with pytest.raises(KeyboardInterrupt):
        with KeyboardInterruptsCatcher(lambda: None):
            raise KeyboardInterrupt()

    list = []
    def append_and_raise():
        list.append(None)
        raise KeyboardInterrupt()

    with pytest.raises(KeyboardInterrupt):
        with KeyboardInterruptsCatcher(append_and_raise, limit=2):
            raise KeyboardInterrupt()
    assert len(list) == 3

    list = []
    with pytest.raises(KeyboardInterrupt):
        with KeyboardInterruptsCatcher(append_and_raise, enable=False):
            raise KeyboardInterrupt()
    assert len(list) == 0

def test_verified_dict():
    vdict = VerifiedDict({"b": 1, "c": True, "a": {"k": "v"}, "d": {"x": "y"}}, keys_to_ignore=["a"])
    assert len(vdict) == 4
    assert vdict["b"] == 1
    assert vdict["c"]
    vdict["b"] = 2
    assert vdict["b"] == 2
    del vdict["b"]
    assert "b" not in vdict
    with pytest.raises(RuntimeError):
        vdict["other_key"] = "value"
    with pytest.raises(RuntimeError):
        vdict["b"] = 2
    with pytest.raises(RuntimeError):
        vdict["d"]["other"] = "value"
    vdict["a"]["other"] = "value"

    def transform(value, origin_value):
        if isinstance(value, str):
            return value.lower()
        return value
    vdict = VerifiedDict({"a": "bCd"}, [], transform)
    assert len(vdict) == 1
    assert vdict["a"] == "bcd"

    vdict["a"] = "E"
    assert vdict["a"] == "e"

def test_frozen_dict():
    fdict = FrozenDict(a=1, b=2)
    assert len(fdict) == 2
    for k in ["a", "b"]:
        assert k in fdict
    assert fdict["a"] == 1
    assert fdict["b"] == 2
    assert fdict.get("c", 100) == 100

    s = 0
    for k, v in iteritems(fdict):
        s += v
        assert k in ["a", "b"]
    assert s == 3

    assert sorted(fdict.values()) == [1, 2]
    assert sorted(fdict.keys()) == ["a", "b"]
    if not PY3:
        assert sorted(itervalues(fdict)) == [1, 2]
        assert sorted(iterkeys(fdict)) == ["a", "b"]

    assert fdict == {"a": 1, "b": 2}
    assert fdict.as_dict() == {"a": 1, "b": 2}

    with pytest.raises(NotImplementedError):
        fdict.pop("a")

    fdict.pop("c", default=1) == 1

    assert fdict == FrozenDict({"a": 1, "b": 2})
    assert fdict != {"c": 1}

    assert hash(fdict) == hash(FrozenDict({"b": 2, "a": 1}))

    repr(fdict), str(fdict)

    with pytest.raises(TypeError):
        fdict["abcde"] = 123

    with pytest.raises(TypeError):
        del fdict["a"]

    with pytest.raises(TypeError):
        fdict["a"] = 3

class TestResponseStream(object):
    def test_chunk_iterator(self):
        random_line = lambda: b("".join(random.choice(string.ascii_lowercase) for _ in xrange(100)))
        s = b"\n".join(random_line() for _ in xrange(3))

        class StringIterator(Iterator):
            def __init__(self, string, chunk_size=10):
                self.string = string
                self.pos = 0
                self.chunk_size = chunk_size

                self.empty_string_before_finish_yielded = False
                self.stop_iteration_raised = False
                self.process_error_called = False

            def __iter__(self):
                return self

            def __next__(self):
                str_part = self.string[self.pos:self.pos + self.chunk_size]
                if not str_part:
                    if not self.empty_string_before_finish_yielded:
                        # Check that ResponseStream will call next one more time after empty response.
                        self.empty_string_before_finish_yielded = True
                        return b""
                    else:
                        self.stop_iteration_raised = True
                        raise StopIteration()
                self.pos += self.chunk_size
                return str_part

            def process_error(self, response):
                assert response == s
                self.process_error_called = True
                assert self.stop_iteration_raised

        close_list = []
        def close():
            close_list.append(True)

        string_iterator = StringIterator(s, 10)
        stream = ResponseStream(lambda: s, string_iterator, close, string_iterator.process_error, lambda: None)

        assert stream.read(20) == s[:20]
        assert stream.read(2) == s[20:22]

        iterator = stream.chunk_iter()
        assert next(iterator) == s[22:30]
        assert next(iterator) == s[30:40]

        assert stream.readline() == s[40:101]
        assert stream.readline() == s[101:202]

        assert stream.read(8) == s[202:210]

        chunks = []
        for chunk in stream.chunk_iter():
            chunks.append(chunk)

        assert string_iterator.process_error_called

        assert len(chunks) == 10
        assert b"".join(chunks) == s[210:]
        assert stream.read() == b""

        stream.close()
        assert len(close_list) > 0

    def test_empty_response_stream(self):
        stream = EmptyResponseStream()
        assert stream.read() == b""
        assert len([x for x in stream.chunk_iter()]) == 0
        assert stream.readline() == b""

        values = []
        for value in stream:
            values.append(value)
        assert len(values) == 0

        with pytest.raises(StopIteration):
            next(stream)

@pytest.mark.usefixtures("yt_env")
class TestExecuteBatch(object):
    @pytest.mark.parametrize("concurrency", [None, 1])
    def test_simple(self, concurrency):
        yt.create("map_node", "//tmp/test_dir", ignore_existing=True)
        rsp = yt.execute_batch(
            requests=[
                {"command": "list", "parameters": {"path": "//tmp"}},
                {"command": "list", "parameters": {"path": "//tmp/test_dir"}},
                {"command": "list", "parameters": {"path": "//tmp/missing"}},
            ],
            concurrency=concurrency)

        assert "test_dir" in rsp[0]["output"]

        assert rsp[1]["output"] == []

        err = yt.YtResponseError(rsp[2]["error"])
        assert err.is_resolve_error()

@pytest.mark.usefixtures("yt_env_multicell")
class TestCellId(object):
    def test_simple(self, yt_env_multicell):
        yt.mkdir("//tmp/test_dir")
        yt.write_table("//tmp/test_dir/table", [{"a": "b"}])
        chunk_id = yt.get("//tmp/test_dir/table/@chunk_ids/0")

        config = deepcopy(yt.config.config)
        config["backend"] = "native"
        client = yt.YtClient(config=config)
        assert client.get("#{0}/@owning_nodes".format(chunk_id)) == ["//tmp/test_dir/table"]

        for secondary_master in config["driver_config"]["secondary_masters"]:
            cell_id = secondary_master["cell_id"]
            client.COMMAND_PARAMS["master_cell_id"] = cell_id
            assert client.get("//sys/@cell_id") == cell_id

