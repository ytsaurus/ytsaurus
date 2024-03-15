from __future__ import print_function

from .conftest import authors
from .helpers import (TEST_DIR, get_tests_sandbox, get_test_file_path, wait, get_default_resource_limits,
                      check_rows_equality, set_config_options, set_config_option,
                      get_python, get_binary_path, get_environment_for_binary_test)

from yt.subprocess_wrapper import Popen, PIPE
from yt.wrapper.errors import YtRetriableError
from yt.wrapper.exceptions_catcher import KeyboardInterruptsCatcher
from yt.wrapper.mappings import VerifiedDict, FrozenDict
from yt.wrapper.response_stream import ResponseStream, EmptyResponseStream
from yt.wrapper.driver import get_api_version
from yt.wrapper.retries import run_with_retries, Retrier
from yt.wrapper.ypath import ypath_join, ypath_dirname, ypath_split, YPath
from yt.wrapper.stream import _ChunkStream
from yt.wrapper.default_config import retries_config as get_default_retries_config, get_default_config
from yt.wrapper.format import SkiffFormat

import yt.environment.arcadia_interop as arcadia_interop

from yt.common import makedirp
from yt.yson import to_yson_type, dumps
import yt.yson as yson
import yt.json_wrapper as json

try:
    from yt.packages.six import iterkeys, itervalues, iteritems, PY3, Iterator, b
    from yt.packages.six.moves import xrange, filter as ifilter
except ImportError:
    from six import iterkeys, itervalues, iteritems, PY3, Iterator, b
    from six.moves import xrange, filter as ifilter

import yt.wrapper as yt

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

from flaky import flaky

from copy import deepcopy
try:
    import collections.abc as collections_abc
except ImportError:
    import collections as collections_abc
import gc
import inspect
import itertools
import mock
import os
import pytest
import pickle
import random
import signal
import string
import subprocess
import sys
import tempfile
import time
import uuid


def is_process_alive(pid):
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True


@authors("asaitgalin")
def test_docs_exist():
    functions = inspect.getmembers(
        yt, lambda o: inspect.isfunction(o) and not o.__name__.startswith("_"))

    functions_without_doc = list(ifilter(lambda pair: not inspect.getdoc(pair[1]), functions))
    assert not functions_without_doc

    classes = inspect.getmembers(yt, lambda o: inspect.isclass(o))
    for name, cl in classes:
        assert inspect.getdoc(cl)
        if name == "PingTransaction":
            continue  # Python Thread is not documented O_o
        public_methods = inspect.getmembers(
            cl,
            lambda o: inspect.ismethod(o) and not o.__name__.startswith("_"))
        ignore_methods = set()
        if issubclass(cl, collections_abc.Iterator) and not PY3:
            ignore_methods.add("next")

        methods_without_doc = [method for name, method in public_methods
                               if name not in ignore_methods and not inspect.getdoc(method)]
        assert not methods_without_doc


@authors("ignat")
def test_client_impl():
    if yatest_common is None:
        pytest.skip()

    generate_client_impl = arcadia_interop.search_binary_path("generate_client_impl")

    _, client_impl_yandex_generated_path = tempfile.mkstemp()

    client_impl_yandex_data = arcadia_interop.resource.find("/modules/client_impl_yandex.py")

    subprocess.check_call([generate_client_impl, client_impl_yandex_generated_path], stderr=sys.stderr)

    with open(client_impl_yandex_generated_path, "rb") as fin:
        assert client_impl_yandex_data == fin.read()


@authors("ignat")
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
    assert ypath_join(YPath("//a"), "b") == "//a/b"
    assert ypath_join("a", YPath("//b")) == "//b"


@authors("ostyakov")
def test_ypath_split():
    assert ypath_split("/") == ("/", "")
    assert ypath_split("//home") == ("/", "home")

    with pytest.raises(yt.YtError):
        ypath_split("//")

    assert ypath_split("#a-b-c-d/a/b") == ("#a-b-c-d/a", "b")
    assert ypath_split("//a/b\\\\\\/c") == ("//a", "b\\\\\\/c")


@authors("asaitgalin")
def test_ypath_dirname():
    assert ypath_dirname("/") == "/"
    assert ypath_dirname("//a") == "/"
    assert ypath_dirname("//a/b") == "//a"
    assert ypath_dirname("//a/b/c/d/e/f") == "//a/b/c/d/e"
    assert ypath_dirname("//a/b\\/c") == "//a"
    with pytest.raises(yt.YtError):
        ypath_dirname("//a/")
    assert ypath_dirname("//a\\/") == "/"
    with pytest.raises(yt.YtError):
        ypath_dirname("//a//b")
    with pytest.raises(yt.YtError):
        ypath_dirname("//a///b")
    with pytest.raises(yt.YtError):
        ypath_dirname("//////b")
    with pytest.raises(yt.YtError):
        ypath_dirname("//")
    assert ypath_dirname("//a\\\\b/c\\/") == "//a\\\\b"
    assert ypath_dirname("//a/b\\\\/c") == "//a/b\\\\"
    assert ypath_dirname("//a/b\\\\\\/c") == "//a"
    assert ypath_dirname("//a/b\\\\\\\\/c") == "//a/b\\\\\\\\"
    assert ypath_dirname("//\\\\a\\/b") == "/"
    assert ypath_dirname("//a/b\\\\\\/c/d/\\\\e") == "//a/b\\\\\\/c/d"
    assert ypath_dirname("//a/b\\\\\\/c/d/\\\\\\[") == "//a/b\\\\\\/c/d"
    assert ypath_dirname("//a/b\\/\\/c") == "//a"
    assert ypath_dirname("#a-b-c-d") == "#a-b-c-d"
    assert ypath_dirname("#a-b-c-d/a") == "#a-b-c-d"
    assert ypath_dirname("#a-b-c-d/a/b") == "#a-b-c-d/a"
    with pytest.raises(yt.YtError):
        assert ypath_dirname("#a-b-c-d/a/")
    with pytest.raises(yt.YtError):
        assert ypath_dirname("abc/def")


@pytest.mark.timeout(1200)
@flaky(max_runs=3)
@pytest.mark.usefixtures("yt_env_job_archive")
class TestYtBinary(object):
    @authors("asaitgalin")
    def test_yt_binary(self, yt_env_job_archive):
        env = get_environment_for_binary_test(yt_env_job_archive)
        env["FALSE"] = "%false"
        env["TRUE"] = "%true"

        sandbox_dir = os.path.join(get_tests_sandbox(), "TestYtBinary_" + uuid.uuid4().hex[:8])
        makedirp(sandbox_dir)

        test_binary = get_test_file_path("test_yt.sh", use_files=False)

        output_file = os.path.join(sandbox_dir, "stderr")
        output = open(output_file, "w")
        proc = Popen([test_binary], env=env, stdout=output, stderr=output, cwd=sandbox_dir)
        proc.communicate()

        if arcadia_interop is not None:
            sys.stderr.write(open(output_file).read())

        assert proc.returncode == 0

    @authors("ignat")
    def test_secure_vault_in_logging(self, yt_env_job_archive):
        if yatest_common is None:
            pytest.skip()

        env = get_environment_for_binary_test(yt_env_job_archive)
        env.update({
            "YT_LOG_LEVEL": "DEBUG",
            "YT_SPEC": '{"secure_vault": {"ENV": "MY_TOKEN"}}',
        })

        yt.write_table(TEST_DIR + "/input", [{"x": 10}])
        # TODO(ignat): use compiled yt
        proc = subprocess.Popen(
            [
                get_python(), get_binary_path("yt"),
                "map", "cat",
                "--format", "yson",
                "--src", TEST_DIR + "/input",
                "--dst", TEST_DIR + "/output",
            ],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)

        stdout, stderr = proc.communicate()

        if proc.returncode != 0:
            print("Process stdout", stdout, file=sys.stderr)
            print("Process stderr", stderr, file=sys.stderr)
        assert proc.returncode == 0

        start_op_line = None
        for line in stderr.split(b"\n"):
            if b"Executing " not in line:
                assert b"MY_TOKEN" not in line
            if b"start_op" in line:
                start_op_line = line
        assert start_op_line is not None
        assert b"secure_vault" in start_op_line
        assert b"hidden" in start_op_line


@pytest.mark.usefixtures("yt_env_with_rpc")
class TestDriverLogging(object):
    @authors("ignat")
    def test_driver_logging(self, yt_env_with_rpc):
        def get_stderr_from_cli(log_level=None):
            env = get_environment_for_binary_test(yt_env_with_rpc, enable_request_logging=False)
            if log_level:
                env["YT_LOG_LEVEL"] = log_level
            proc = Popen([env["PYTHON_BINARY"], env["YT_CLI_PATH"], "get", TEST_DIR], stderr=PIPE, stdout=PIPE, env=env)
            _, p_stderr = proc.communicate()
            return p_stderr

        if yt.config["backend"] != "rpc":
            pytest.skip()

        driver_log_default = get_stderr_from_cli()
        driver_log_info = get_stderr_from_cli(log_level="INFO")
        driver_log_warning = get_stderr_from_cli(log_level="WARNING")

        print("Default log output", driver_log_default, file=sys.stderr)
        print("Info log output", driver_log_info, file=sys.stderr)
        print("Warning log output", driver_log_warning, file=sys.stderr)

        assert len(driver_log_default) < len(driver_log_info)
        assert len(driver_log_warning) == 0

    @authors("denvr")
    def test_yp_discovery_enabled(self, yt_env_with_rpc):
        if yt.config["backend"] != "rpc":
            pytest.skip()
        assert yt.native_driver.yp_service_discovery_configured
        assert yt.config["yp_service_discovery_config"] is None and yt.native_driver.ENABLE_YP_SERVICE_DISCOVERY


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

    @authors("asaitgalin")
    def test_master_mutation_id(self):
        test_dir = ypath_join(TEST_DIR, "test")
        test_dir2 = ypath_join(TEST_DIR, "test2")
        test_dir3 = ypath_join(TEST_DIR, "test3")

        self.check_command(
            lambda: yt.set(test_dir, {"a": "b"}, force=True),
            lambda: yt.set(test_dir, {}, force=True),
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

    @authors("ignat")
    def test_scheduler_mutation_id(self):
        def abort(operation_response):
            operation_id = operation_response["operation_id"] if get_api_version() == "v4" else operation_response
            yt.abort_operation(operation_id)
            time.sleep(1.0)  # Wait for aborting transactions

        def get_operation_count():
            operations = yt.get("//sys/operations")
            count = 0
            for key in operations:
                if len(key) != 2:
                    continue
                count += len(operations[key])
            return count

        table = TEST_DIR + "/table"
        other_table = TEST_DIR + "/other_table"
        yt.write_table(table, [{"x": 1}, {"x": 2}])
        yt.create("table", other_table)

        params = {
            "spec": {
                "mapper": {
                    "command": "sleep 2; cat"
                },
                "input_table_paths": [table],
                "output_table_paths": [other_table]
            },
            "output_format": "yson",
            "operation_type": "map"
        }

        operations_count = get_operation_count()

        command_name = "start_operation" if get_api_version() == "v4" else "start_op"
        self.check_command(
            lambda: yson.loads(yt.driver.make_request(command_name, params)),
            None,
            lambda: get_operation_count() == operations_count + 1,
            abort)


@pytest.mark.usefixtures("yt_env")
class TestRetries(object):
    @authors("bidzilya")
    def test_custom_chaos_monkey(self):
        class _Retrier(Retrier):
            def __init__(self, chaos_monkey_values, retries_count=1):
                retry_config = get_default_retries_config()
                retry_config.update({
                    "enable": True,
                    "count": retries_count,
                    "backoff": {
                        "policy": "rounded_up_to_request_timeout"
                    }
                })
                chaos_monkey_state = itertools.cycle(chaos_monkey_values)
                chaos_monkey = lambda: next(chaos_monkey_state) # noqa
                super(_Retrier, self).__init__(
                    retry_config,
                    timeout=10,
                    exceptions=(YtRetriableError,),
                    chaos_monkey=chaos_monkey)

            def action(self):
                return 42

        for retries_count in range(1, 5):
            chaos_monkey_values = [True] * retries_count + [False]
            with pytest.raises(YtRetriableError):
                _Retrier(chaos_monkey_values, retries_count).run()
            assert 42 == _Retrier(chaos_monkey_values, retries_count + 1).run()

    @authors("ostyakov")
    def test_run_with_retries(self):
        def action():
            if random.randint(0, 3) == 1:
                raise yt.YtError()
            return 1

        assert 1 == run_with_retries(action, retry_count=10, backoff=0.01)

    @authors("asaitgalin")
    def test_read_with_retries(self):
        old_value = yt.config["read_retries"]["enable"]
        yt.config["read_retries"]["enable"] = True
        yt.config._ENABLE_READ_TABLE_CHAOS_MONKEY = True
        yt.config._ENABLE_HTTP_CHAOS_MONKEY = True
        try:
            table = TEST_DIR + "/table"

            with pytest.raises(yt.YtError):
                yt.read_table(table)

            yt.create("table", table)
            check_rows_equality([], yt.read_table(table))
            assert b"" == yt.read_table(table, format=yt.JsonFormat(), raw=True).read()

            yt.write_table(table, [{"x": 1}, {"y": 2}])
            check_rows_equality([{"x": 1}, {"y": 2}], yt.read_table(table))

            rsp = yt.read_table(table, format=yt.JsonFormat(), raw=True)
            assert json.loads(next(rsp)) == {"x": 1}
            yt.write_table(table, [{"x": 1}, {"y": 3}])
            assert json.loads(next(rsp)) == {"y": 2}
            rsp.close()

            rsp = yt.read_table(table, raw=False)
            assert [("x", 1), ("y", 3)] == sorted([list(iteritems(x))[0] for x in rsp])

            response_parameters = {}
            rsp = yt.read_table(table, response_parameters=response_parameters, format=yt.JsonFormat(), raw=True)
            assert response_parameters["start_row_index"] == 0
            assert response_parameters["approximate_row_count"] == 2
            rsp.close()
        finally:
            yt.config._ENABLE_READ_TABLE_CHAOS_MONKEY = False
            yt.config._ENABLE_HTTP_CHAOS_MONKEY = False
            yt.config["read_retries"]["enable"] = old_value

    @authors("ignat")
    def test_read_ranges_with_retries(self, yt_env):
        yt.config._ENABLE_READ_TABLE_CHAOS_MONKEY = True
        try:
            table = TEST_DIR + "/table"

            yt.create("table", table)
            assert b"" == yt.read_table(table, format=yt.JsonFormat(), raw=True).read()

            yt.write_table("<sorted_by=[x]>" + table, [{"x": 1}, {"x": 2}, {"x": 3}])
            assert b'{"x":1}\n{"x":2}\n{"x":3}\n' == yt.read_table(table, format=yt.JsonFormat(), raw=True).read()
            assert [{"x": 1}] == list(yt.read_table(table + "[#0]", format=yt.JsonFormat()))

            table_path = yt.TablePath(table, exact_key=[2])
            for i in xrange(10):
                assert [{"x": 2}] == list(yt.read_table(table_path))

            assert [{"x": 1}, {"x": 3}, {"x": 2}, {"x": 1}, {"x": 2}] == \
                [{"x": elem["x"]} for elem in yt.read_table(table + '[#0,"2":"1",#2,#1,1:3]', format=yt.YsonFormat())]

            assert [{"x": 1}, {"x": 3}, {"x": 2}, {"x": 1}, {"x": 2}] == \
                list(yt.read_table(table + '[#0,"2":"1",#2,#1,1:3]', format=yt.YsonFormat()))

            assert [to_yson_type(None, attributes={"range_index": 0}),
                    to_yson_type(None, attributes={"row_index": 0}),
                    {"x": 1},
                    to_yson_type(None, attributes={"range_index": 1}),
                    to_yson_type(None, attributes={"row_index": 2}),
                    {"x": 3}] == \
                list(yt.read_table(table + "[#0,#2]", format=yt.YsonFormat(control_attributes_mode="none"),
                                   control_attributes={"enable_row_index": True, "enable_range_index": True}))

            assert [{"$attributes": {"range_index": 0}, "$value": None},
                    {"$attributes": {"row_index": 0}, "$value": None},
                    {"x": 1},
                    {"$attributes": {"range_index": 1}, "$value": None},
                    {"$attributes": {"row_index": 2}, "$value": None},
                    {"x": 3}] == \
                list(yt.read_table(table + "[#0,#2]", format=yt.JsonFormat(control_attributes_mode="none"),
                                   control_attributes={"enable_row_index": True, "enable_range_index": True}))

            assert [{"x": 1, "@row_index": 0, "@range_index": 0, "@table_index": None},
                    {"x": 3, "@row_index": 2, "@range_index": 1, "@table_index": None}] == \
                list(yt.read_table(table + "[#0,#2]", format=yt.JsonFormat(control_attributes_mode="row_fields"),
                                   control_attributes={"enable_row_index": True, "enable_range_index": True}))

            with pytest.raises(yt.YtError):
                list(yt.read_table(table + "[#0,2]", raw=False, format=yt.YsonFormat(), unordered=True))

            assert [b"x=2\n", b"x=3\n"] == list(yt.read_table(table + "[2:]", raw=True, format=yt.DsvFormat()))

            with pytest.raises(yt.YtError):
                list(yt.read_table(table + "[#0,2]", raw=False, format=yt.DsvFormat()))

            if not yt.config["read_parallel"]["enable"]:
                two_exact_ranges = yt.TablePath(table, attributes={"ranges": [{"exact": {"key": [1]}}, {"exact": {"key": [3]}}]})
                assert [{"x": 1}, {"x": 3}] == list(yt.read_table(two_exact_ranges))

                yt.write_table("<sorted_by=[x]>" + table, [{"x": 1}, {"x": 1}, {"x": 2}, {"x": 3}, {"x": 3}, {"x": 3}])
                two_exact_ranges = yt.TablePath(table, attributes={"ranges": [{"exact": {"key": [1]}}, {"exact": {"key": [3]}}]})
                assert [{"x": 1}, {"x": 1}, {"x": 3}, {"x": 3}, {"x": 3}] == list(yt.read_table(two_exact_ranges))

        finally:
            yt.config._ENABLE_READ_TABLE_CHAOS_MONKEY = False

    @authors("ostyakov")
    def test_read_parallel_with_retries(self):
        with set_config_option("read_parallel/enable", True):
            self.test_read_with_retries()

    @authors("ostyakov")
    def test_read_ranges_parallel_with_retries(self, yt_env):
        with set_config_option("read_parallel/enable", True):
            self.test_read_ranges_with_retries(yt_env)

    @authors("ignat")
    def test_read_has_no_leaks(self):
        table = TEST_DIR + "/table"
        yt.write_table(table, [{"a": "b"}])

        def test_func():
            config = deepcopy(yt.config.config)
            config["apply_remote_patch_at_start"] = None

            client = yt.YtClient(config=config)
            for row in client.read_table(table):
                pass
            return id(client)

        obj_id = test_func()
        for obj in gc.get_objects():
            assert id(obj) != obj_id

    @authors("asaitgalin")
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

    @authors("asaitgalin")
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
                yt.set(TEST_DIR + "/some_node", {}, force=True)
                yt.exists(TEST_DIR + "/some_node")
                yt.list(TEST_DIR)
        finally:
            yt.config._ENABLE_HTTP_CHAOS_MONKEY = False
            yt.config["proxy"]["request_timeout"] = old_request_timeout
            yt.config["proxy"]["request_backoff_time"] = None

    @authors("asaitgalin")
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

    @authors("asaitgalin")
    def test_write_retries_and_schema(self):
        table = TEST_DIR + "/table"

        old_request_retry_timeout = yt.config["proxy"]["request_timeout"]
        old_enable_write_retries = yt.config["write_retries"]["enable"]
        old_chunk_size = yt.config["write_retries"]["chunk_size"]

        yt.config["write_retries"]["enable"] = True
        yt.config["write_retries"]["chunk_size"] = 100
        yt.config["proxy"]["request_timeout"] = 1000
        yt.config._ENABLE_HEAVY_REQUEST_CHAOS_MONKEY = True

        data = [{"x": "y"} for _ in xrange(100)]
        try:
            yt.write_table("<schema=[{name=x;type=string}]>" + table, data)
            yt.write_table("<schema=[{name=x;type=string}]>" + table, data)
        finally:
            yt.config._ENABLE_HEAVY_REQUEST_CHAOS_MONKEY = False
            yt.config["proxy"]["request_timeout"] = old_request_retry_timeout
            yt.config["write_retries"]["enable"] = old_enable_write_retries
            yt.config["write_retries"]["chunk_size"] = old_chunk_size

    @authors("asaitgalin")
    def test_dynamic_tables_requests_retries(self):
        table = TEST_DIR + "/dyn_table"
        yt.create("table", table, attributes={
            "schema": [
                {"name": "x", "type": "string", "sort_order": "ascending"},
                {"name": "y", "type": "string"},
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

    @authors("ignat")
    def test_concatenate(self):
        yt.config._ENABLE_HTTP_CHAOS_MONKEY = True
        override_options = {
            "write_retries/enable": True,
            "write_retries/count": 10,
            "concatenate_retries/count": 10,
        }
        try:
            with set_config_options(override_options):
                tableA = TEST_DIR + "/tableA"
                tableB = TEST_DIR + "/tableB"
                output_table = TEST_DIR + "/outputTable"

                yt.write_table(tableA, [{"x": 1, "y": 2}])
                yt.write_table(tableB, [{"x": 10, "y": 20}])
                yt.concatenate([tableA, tableB], output_table)

                assert [{"x": 1, "y": 2}, {"x": 10, "y": 20}] == list(yt.read_table(output_table))
        finally:
            yt.config._ENABLE_HTTP_CHAOS_MONKEY = False

    @authors("se4min")
    @flaky(max_runs=5)
    @pytest.mark.parametrize("total_timeout", [3000, 20000])
    def test_retries_total_timeout(self, total_timeout):
        class FailingRetrier(Retrier):
            def action(self):
                raise yt.YtError

        retry_config = {
            "count": 6,
            "enable": True,
            "total_timeout": total_timeout,
            "backoff": {
                "policy": "exponential",
                "exponential_policy": {
                    "start_timeout": 1000,
                    "base": 2,
                    "max_timeout": 20000,
                    "decay_factor_bound": 0.3
                }
            }
        }
        retrier = FailingRetrier(retry_config)

        start = time.time()
        with pytest.raises(yt.YtError):
            retrier.run()
        assert 0.5 * total_timeout / 1000. < time.time() - start < total_timeout / 1000. + 1


@authors("asaitgalin")
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


@authors("asaitgalin", "ignat")
def test_keyboard_interrupts_catcher():
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


@authors("asaitgalin", "ignat")
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


@authors("asaitgalin")
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

    assert fdict.pop("c", default=1) == 1

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
    @authors("asaitgalin")
    def test_chunk_iterator(self):
        random_line = lambda: b("".join(random.choice(string.ascii_lowercase) for _ in xrange(100))) # noqa
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

        def close(from_delete):
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

    @authors("asaitgalin")
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
    @authors("ignat")
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
        if get_api_version() == "v4":
            rsp = rsp["results"]
            assert "test_dir" in rsp[0]["output"]["value"]
            assert rsp[1]["output"]["value"] == []
        else:
            assert "test_dir" in rsp[0]["output"]
            assert rsp[1]["output"] == []

        err = yt.YtResponseError(rsp[2]["error"])
        assert err.is_resolve_error()


@pytest.mark.usefixtures("yt_env_multicell")
class TestCellId(object):
    @authors("ignat")
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


@pytest.mark.usefixtures("yt_env_multicell")
class TestExternalize(object):
    @authors("ignat")
    def test_externalize(self):
        yt.create("account", attributes={"name": "a", "resource_limits": get_default_resource_limits()})
        yt.create("account", attributes={"name": "b", "resource_limits": get_default_resource_limits()})
        yt.create("user", attributes={"name": "u"})
        wait(lambda: yt.get("//sys/users/u/@life_stage") == "creation_committed")
        wait(lambda: yt.get("//sys/accounts/a/@life_stage") == "creation_committed")
        wait(lambda: yt.get("//sys/accounts/b/@life_stage") == "creation_committed")

        yt.create("map_node", "//tmp/m", attributes={"attr": "value", "acl": [
            {
                "action": "allow",
                "subjects": ["u"],
                "permissions": ["write"],
            }
        ]})

        TABLE_PAYLOAD = [{"key": "value"}]
        yt.create("table", "//tmp/m/t", attributes={"external": True, "external_cell_tag": 3, "account": "a", "attr": "t"})
        yt.write_table("//tmp/m/t", TABLE_PAYLOAD)

        FILE_PAYLOAD = b"PAYLOAD"
        yt.create("file", "//tmp/m/f", attributes={"external": True, "external_cell_tag": 3, "account": "b", "attr": "f"})
        yt.write_file("//tmp/m/f", FILE_PAYLOAD)

        yt.create("document", "//tmp/m/d", attributes={"value": {"hello": "world"}})
        ct = yt.get("//tmp/m/d/@creation_time")

        yt.create("map_node", "//tmp/m/m", attributes={"account": "a", "compression_codec": "brotli_8"})

        yt.create("table", "//tmp/m/et", attributes={"external_cell_tag": 3, "expiration_time": "2100-01-01T00:00:00.000000Z"})

        yt.create("map_node", "//tmp/m/acl1", attributes={"inherit_acl": True,  "acl": [
            {
                "action": "deny",
                "subjects": ["u"],
                "permissions": ["read"],
            }
        ]})
        yt.create("map_node", "//tmp/m/acl2", attributes={"inherit_acl": False, "acl": [
            {
                "action": "deny",
                "subjects": ["u"],
                "permissions": ["read"],
            }
        ]})

        effective_acl_m = yt.get("//tmp/m/@effective_acl")
        real_acl_m = yt.get("//tmp/m/@acl")
        acl1 = yt.get("//tmp/m/acl1/@acl")
        acl2 = yt.get("//tmp/m/acl2/@acl")

        yt.externalize("//tmp/m", 2)

        assert yt.get("//tmp/m/@inherit_acl")
        assert yt.get("//tmp/m/@effective_acl") == effective_acl_m
        assert yt.get("//tmp/m/@acl") == real_acl_m

        assert yt.get("//tmp/m/acl1/@inherit_acl")
        assert yt.get("//tmp/m/acl1/@acl") == acl1

        assert not yt.get("//tmp/m/acl2/@inherit_acl")
        assert yt.get("//tmp/m/acl2/@acl") == acl2

        assert yt.get("//tmp/m/@type") == "portal_exit"
        assert yt.get("//tmp/m/@attr") == "value"

        assert list(yt.read_table("//tmp/m/t")) == TABLE_PAYLOAD
        assert yt.get("//tmp/m/t/@account") == "a"
        assert yt.get("//tmp/m/t/@attr") == "t"

        assert yt.read_file("//tmp/m/f").read() == FILE_PAYLOAD
        assert yt.get("//tmp/m/f/@account") == "b"
        assert yt.get("//tmp/m/f/@attr") == "f"

        assert yt.get("//tmp/m/d") == {"hello": "world"}
        assert yt.get("//tmp/m/d/@creation_time") == ct
        # XXX(babenko): modification time is not preserved yet
        # assert get("//tmp/m/d/@modification_time") == mt

        assert yt.get("//tmp/m/m/@account") == "a"
        assert yt.get("//tmp/m/m/@compression_codec") == "brotli_8"

        assert yt.get("//tmp/m/et/@expiration_time") == "2100-01-01T00:00:00.000000Z"


@pytest.mark.usefixtures("yt_env")
class TestGenerateTimestamp(object):
    @authors("asaitgalin", "se4min")
    def test_generate_timestamp(self):
        ts = yt.generate_timestamp()
        assert ts >= 0


class TestStream(object):
    @authors("se4min")
    def test_empty_stream(self):
        for allow_resplit in (True, False):
            stream = _ChunkStream([], chunk_size=512, allow_resplit=allow_resplit)
            assert list(stream) == [b""]

    @authors("se4min")
    def test_no_resplit(self):
        lines = ["".join(random.choice(string.digits) for _ in xrange(random.randrange(1, 10))) + "\n"
                 for _ in xrange(100)]
        if PY3:
            lines = [line.encode("ascii") for line in lines]
        make_stream = lambda: _ChunkStream(lines, chunk_size=5, allow_resplit=False) # noqa
        assert [line + b"\n" for chunk in make_stream() for line in chunk.split(b"\n") if line] == lines
        for pieces in make_stream().split_chunks(2):
            chunk = b"".join(pieces)
            assert chunk[-1:] == b"\n"


@pytest.mark.usefixtures("yt_env_with_rpc")
class TestTransferAccountResources(object):
    @authors("kiselyovp")
    def test_transfer_account_resources_simple(self):
        if yt.config["api_version"] != "v4":
            pytest.skip()

        yt.create("account", attributes={"name": "a1", "resource_limits": {"node_count": 5}})
        yt.create("account", attributes={"name": "a2", "resource_limits": {"node_count": 5}})

        with pytest.raises(yt.YtError):
            yt.transfer_account_resources("a1", "a2", 123)
        with pytest.raises(yt.YtError):
            yt.transfer_account_resources("a1", "a2", {"node_count": -1})
        with pytest.raises(yt.YtError):
            yt.transfer_account_resources("a1", "a2", {"chunk_count": 1})

        yt.transfer_account_resources("a1", "a2", {"node_count": 2})
        assert yt.get("//sys/accounts/a1/@resource_limits/node_count") == 3
        assert yt.get("//sys/accounts/a2/@resource_limits/node_count") == 7

        yt.transfer_account_resources("a2", "a1", {"node_count": 3, "chunk_count": 0})
        assert yt.get("//sys/accounts/a1/@resource_limits/node_count") == 6
        assert yt.get("//sys/accounts/a2/@resource_limits/node_count") == 4

    @authors("kiselyovp")
    def test_transfer_account_resources(self):
        if yt.config["api_version"] != "v4":
            pytest.skip()

        def get_limits(x=1):
            return {
                "node_count": 100 * x,
                "chunk_count": 1000 * x,
                "tablet_count": 5 * x,
                "tablet_static_memory": 1000 * x,
                "master_memory":
                {
                    "total": 25000 * x,
                    "chunk_host": 25000 * x,
                    "per_cell": {}
                },
                "disk_space": 50000 * x,
                "disk_space_per_medium": {"default": 50000 * x} if x != 0 else {}}

        yt.create("account", attributes={"name": "a", "resource_limits": get_limits(6)})
        yt.create("account", attributes={"name": "a1", "parent_name": "a", "resource_limits": get_limits(3)})
        yt.create("account", attributes={"name": "a2", "parent_name": "a", "resource_limits": get_limits(3)})

        with pytest.raises(yt.YtError):
            yt.transfer_account_resources("a1", "a2", get_limits(4))
        yt.transfer_account_resources("a1", "a2", get_limits())
        assert yt.get("//sys/accounts/a1/@resource_limits") == get_limits(2)
        assert yt.get("//sys/accounts/a2/@resource_limits") == get_limits(4)
        assert yt.get("//sys/accounts/a/@resource_limits") == get_limits(6)

        yt.create("account", attributes={"name": "a11", "parent_name": "a1", "resource_limits": get_limits()})
        yt.create("account", attributes={"name": "a21", "parent_name": "a2", "resource_limits": get_limits()})
        yt.transfer_account_resources("a21", "a11", get_limits())
        assert yt.get("//sys/accounts/a21/@resource_limits") == get_limits(0)
        assert yt.get("//sys/accounts/a2/@resource_limits") == get_limits(3)
        assert yt.get("//sys/accounts/a1/@resource_limits") == get_limits(3)
        assert yt.get("//sys/accounts/a11/@resource_limits") == get_limits(2)
        assert yt.get("//sys/accounts/a/@resource_limits") == get_limits(6)


@pytest.mark.usefixtures("yt_env_with_rpc")
class TestTransferPoolResources(object):
    @authors("renadeen")
    def test_transfer_pool_resources_simple(self):
        if yt.config["api_version"] != "v4":
            pytest.skip()

        yt.create("scheduler_pool", attributes={
            "name": "from",
            "pool_tree": "default",
            "strong_guarantee_resources": {"cpu": 10},
            "integral_guarantees": {"resource_flow": {"cpu": 20}, "burst_guarantee_resources": {"cpu": 30}},
            "max_running_operation_count": 40,
            "max_operation_count": 50,
        })
        yt.create("scheduler_pool", attributes={
            "name": "to",
            "pool_tree": "default",
            "strong_guarantee_resources": {"cpu": 10},
            "integral_guarantees": {"resource_flow": {"cpu": 20}, "burst_guarantee_resources": {"cpu": 30}},
            "max_running_operation_count": 40,
            "max_operation_count": 50,
        })

        with pytest.raises(yt.YtError):
            yt.transfer_pool_resources("from", "to", "default", 123)
        with pytest.raises(yt.YtError):
            yt.transfer_pool_resources("from", "to", "default", {"max_operation_count": -1})
        with pytest.raises(yt.YtError):
            yt.transfer_pool_resources("from", "inexistent", "default", {"max_operation_count": 1})

        yt.transfer_pool_resources("from", "to", "default", {
            "strong_guarantee_resources": {"cpu": 4},
            "resource_flow": {"cpu": 8},
            "burst_guarantee_resources": {"cpu": 12},
            "max_running_operation_count": 16,
            "max_operation_count": 20,
        })

        assert yt.get("//sys/pool_trees/default/from/@strong_guarantee_resources/cpu") == 6.0
        assert yt.get("//sys/pool_trees/default/from/@integral_guarantees/resource_flow/cpu") == 12.0
        assert yt.get("//sys/pool_trees/default/from/@integral_guarantees/burst_guarantee_resources/cpu") == 18.0
        assert yt.get("//sys/pool_trees/default/from/@max_running_operation_count") == 24
        assert yt.get("//sys/pool_trees/default/from/@max_operation_count") == 30

        assert yt.get("//sys/pool_trees/default/to/@strong_guarantee_resources/cpu") == 14.0
        assert yt.get("//sys/pool_trees/default/to/@integral_guarantees/resource_flow/cpu") == 28.0
        assert yt.get("//sys/pool_trees/default/to/@integral_guarantees/burst_guarantee_resources/cpu") == 42.0
        assert yt.get("//sys/pool_trees/default/to/@max_running_operation_count") == 56
        assert yt.get("//sys/pool_trees/default/to/@max_operation_count") == 70


@pytest.mark.usefixtures("yt_env")
class TestGetSupportedFeatures(object):
    @authors("levysotsky")
    def test_get_supported_features(self):
        features = yt.get_supported_features()

        assert "primitive_types" in features
        expected_types = {
            "int8", "int16", "int32", "int64",
            "uint8", "uint16", "uint32", "uint64",
            "string", "yson", "utf8", "json",
            "float", "double",
            "void", "null",
            "date", "datetime", "timestamp", "interval",
            "date32", "datetime64", "timestamp64", "interval64",
        }
        assert expected_types == expected_types.intersection(set(features["primitive_types"]))

        assert "compression_codecs" in features
        expected_compression_codecs = {
            "none", "snappy", "lz4", "lz4_high_compression",
            "brotli_1", "brotli_11",
            "zlib_1", "zlib_9",
            "zstd_1", "zstd_21",
            "lzma_0", "lzma_9",
            "bzip2_1", "bzip2_9",
        }
        assert expected_compression_codecs == \
            expected_compression_codecs.intersection(set(features["compression_codecs"]))
        deprecated_compression_codecs = {"zlib6", "gzip_best_compression", "brotli8"}
        assert not deprecated_compression_codecs.intersection(set(features["compression_codecs"]))

        assert "erasure_codecs" in features
        expected_erasure_codecs = {
            "none",
            "reed_solomon_6_3",
            "reed_solomon_3_3",
            "lrc_12_2_2",
            "isa_lrc_12_2_2",
        }
        assert expected_erasure_codecs == \
            expected_erasure_codecs.intersection(set(features["erasure_codecs"]))


@pytest.mark.usefixtures("yt_env")
class TestRunCommandWithLock(object):
    @authors("ignat")
    def test_conflict(self, yt_env):
        if yt.config["backend"] == "native":
            pytest.skip()

        def exited_with_code(process, expected_code):
            returncode = process.poll()
            print("Process returncode:", returncode, file=sys.stderr)
            if returncode is None:
                return False
            if returncode == expected_code:
                return True
            assert True, "Exited with unexpected code"

        procA = None
        procB = None

        try:
            env = get_environment_for_binary_test(yt_env)
            procA = subprocess.Popen(
                [
                    get_python(), get_binary_path("yt"),
                    "run-command-with-lock", "//tmp/lock_node", "sleep", "1000"
                ],
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)

            time.sleep(1)
            assert procA.poll() is None

            wait(lambda: yt.exists("//tmp/lock_node"))
            wait(lambda: len(yt.get("//tmp/lock_node/@locks")) == 1, sleep_backoff=5)

            procB = subprocess.Popen(
                [
                    get_python(), get_binary_path("yt"),
                    "run-command-with-lock", "//tmp/lock_node", "sleep", "1000",
                    "--conflict-exit-code", "7",
                ],
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)

            wait(lambda: exited_with_code(procB, 7))
        finally:
            if procA is not None:
                procA.kill()
            if procB is not None:
                procB.kill()

    @authors("ignat")
    def test_signal(self, yt_env):
        if yt.config["backend"] == "native":
            pytest.skip()

        procA = None

        try:
            _, filename = tempfile.mkstemp()

            env = get_environment_for_binary_test(yt_env)
            procA = subprocess.Popen(
                [
                    get_python(), get_binary_path("yt"),
                    "run-command-with-lock", "//tmp/lock_node", "echo $$ >{}; sleep 1000".format(filename),
                    "--shell",
                ],
                env=env,
                stdout=sys.stderr,
                stderr=sys.stderr)

            time.sleep(1)
            assert procA.poll() is None

            wait(lambda: yt.exists("//tmp/lock_node"))
            wait(lambda: len(yt.get("//tmp/lock_node/@locks")) == 1, sleep_backoff=5)
            wait(lambda: open(filename).read().strip())

            subprocess_pid = int(open(filename).read().strip())
            wait(lambda: is_process_alive(subprocess_pid))

            procA.send_signal(signal.SIGTERM)
            wait(lambda: procA.poll() is not None)

            wait(lambda: not is_process_alive(subprocess_pid))

        finally:
            if procA is not None:
                procA.kill()


@pytest.mark.usefixtures("yt_env")
class TestSkiffFormat(object):
    @authors("ermolovd")
    def test_bad_arguments(self):
        # YT-14559
        with pytest.raises(yt.YtError, match="Cannot resolve type reference"):
            SkiffFormat(schema_registry={}, schemas=['$0'])


@authors("denvr")
def test_show_default_config():
    assert dumps(get_default_config())


@authors("denvr")
def test_remote_config_value():
    config = VerifiedDict({
        "k1": "v1",
        "k2": yt.default_config.RemotePatchableString("v2", "path2"),
        "k3": yt.default_config.RemotePatchableBoolean(False, "path3"),
    })

    def _callback():
        _callback.counter += 1

    _callback.counter = 0
    yt.default_config.RemotePatchableValueBase.set_read_access_callback(config, _callback)
    assert isinstance(config["k2"], yt.default_config.RemotePatchableValueBase)
    assert _callback.counter == 0
    assert str(config["k2"]) == "v2"
    assert _callback.counter == 1
    assert isinstance(config["k3"], yt.default_config.RemotePatchableValueBase)
    assert _callback.counter == 1
    assert bool(config["k3"]) == False  # noqa
    assert _callback.counter == 2

    yt.default_config.RemotePatchableValueBase._materialize_all_leafs(config)

    assert isinstance(config["k2"], str)
    assert config["k2"] == "v2"
    assert _callback.counter == 2
    assert config["k3"] == False  # noqa
    assert _callback.counter == 2


@pytest.mark.usefixtures("yt_env_v4")
class TestClientConfigFromCluster(object):
    def prepare_client_with_cluster_config(self, cluster_path, cluster_config, client_config=None, req_mock=None):
        yt.default_config.RemotePatchableValueBase._REMOTE_CACHE = {}
        yt.remove(cluster_path, recursive=True, force=True)
        yt.create("map_node", cluster_path)
        for k, v in cluster_config.items():
            yt.create("document", cluster_path + "/" + k)
            yt.set(
                cluster_path + "/" + k,
                v
            )
        if not client_config:
            client_config = {}
        client_config.update({"proxy": {"url": yt.config.config["proxy"]["url"]}})
        client_config["config_remote_patch_path"] = cluster_path
        if req_mock:
            req_mock.reset_mock()
        client = yt.YtClient(config=client_config)
        return client

    @authors("denvr")
    def test_remote_config_load(self):
        yt.default_config.default_config["apply_remote_patch_at_start"] = False
        yt.default_config.default_config["proxy"]["enable_proxy_discovery"] = yt.default_config.RemotePatchableBoolean(True, "enable_proxy_discovery")
        yt.default_config.default_config["proxy"]["operation_link_pattern"] = yt.default_config.RemotePatchableString(
            "{proxy}/{cluster_path}?page=operation&mode=detail&id={id}&tab=details",
            "operation_link_template",
            lambda local_value, remote_value: remote_value
        )

        config_remote_patch_path = "//test_client_config"

        req_mock_patch = mock.patch('yt.wrapper.http_driver.make_request', wraps=yt.http_driver.make_request)
        req_mock = req_mock_patch.start()

        def is_changed(config):
            changed = str(config["proxy"]["operation_link_pattern"]) == "zzz" \
                and bool(config["proxy"]["enable_proxy_discovery"]) == False  # noqa
            default = config["proxy"]["connect_timeout"] == 5000 \
                and config["proxy"]["request_timeout"] == 20000
            return changed and default

        def is_unchanged(config):
            return str(config["proxy"]["operation_link_pattern"]) == "{proxy}/{cluster_path}?page=operation&mode=detail&id={id}&tab=details" \
                and bool(config["proxy"]["enable_proxy_discovery"]) \
                and config["proxy"]["connect_timeout"] == 5000 \
                and config["proxy"]["request_timeout"] == 20000

        # start
        assert is_unchanged(yt.config)

        # absent path - unchanged
        client_config = {"proxy": {"url": yt.config.config["proxy"]["url"]}}
        client_config["config_remote_patch_path"] = "//some_wrong_path"
        client = yt.YtClient(config=client_config)

        req_mock.reset_mock()
        _ = client.config["proxy"]["url"]
        assert req_mock.call_count == 0, "No interact with cluster"

        _ = bool(client.config["proxy"]["enable_proxy_discovery"])
        assert req_mock.call_count > 0, "Interacted with cluster"
        assert is_unchanged(client.config)

        del client

        # all ok - load config
        client = self.prepare_client_with_cluster_config(config_remote_patch_path, {
            "default": {
                "operation_link_template": "zzz",
                "enable_proxy_discovery": False,
                "other": 999
            }
        })

        req_mock.reset_mock()
        _ = client.config["proxy"]["url"]
        assert req_mock.call_count == 0, "no interact with cluster"

        assert str(client.config["proxy"]["operation_link_pattern"]) == "zzz"
        assert req_mock.call_count > 0, "interacted with cluster"
        assert is_changed(client.config), "config changed"

        req_mock.reset_mock()
        client2 = yt.YtClient(config=client.config)
        assert is_changed(client2.config), "config2 changed"
        assert req_mock.call_count == 0, "cached"

        del client
        del client2

        # all ok (initialize all fields) (depends on prev step)
        client_config = yt.default_config.get_default_config()
        client_config["config_remote_patch_path"] = config_remote_patch_path
        client_config["proxy"]["url"] = yt.config.config["proxy"]["url"]
        client = yt.YtClient(config=client_config)
        assert is_changed(client.config), "config (all fields) changed"

        # do not override user config 1 (depends on prev step)
        client_config = {"proxy": {"url": yt.config.config["proxy"]["url"], "operation_link_pattern": "custom"}}
        client_config["config_remote_patch_path"] = config_remote_patch_path
        client = yt.YtClient(config=client_config)
        assert bool(client.config["proxy"]["enable_proxy_discovery"]) == False, "changed"  # noqa
        assert str(client.config["proxy"]["operation_link_pattern"]) == "custom", "do not change user config"
        assert client.config["proxy"]["connect_timeout"] == 5000, "default"

        # do not override user config 2 (depends on prev step)
        client_config = {"proxy": {"url": yt.config.config["proxy"]["url"]}}
        client_config["config_remote_patch_path"] = config_remote_patch_path
        client = yt.YtClient(config=client_config)
        client.config["proxy"]["operation_link_pattern"] = "custom"
        assert bool(client.config["proxy"]["enable_proxy_discovery"]) == False, "changed"  # noqa
        assert str(client.config["proxy"]["operation_link_pattern"]) == "custom", "do not change user config"
        assert client.config["proxy"]["connect_timeout"] == 5000, "default"

        # preload
        client = self.prepare_client_with_cluster_config(
            config_remote_patch_path,
            {
                "default": {
                    "operation_link_template": "zzz",
                    "enable_proxy_discovery": False,
                },
            },
            {
                "apply_remote_patch_at_start": True,
            },
            req_mock=req_mock,
        )
        assert req_mock.call_count > 0, "interacted with cluster at start"
        del client

        # do not load at all
        client = self.prepare_client_with_cluster_config(
            config_remote_patch_path,
            {
                "default": {
                    "operation_link_template": "zzz",
                    "enable_proxy_discovery": False,
                },
            },
            {
                "apply_remote_patch_at_start": None,
            },
            req_mock=req_mock,
        )
        assert req_mock.call_count == 0, "do not interacted with cluster at start"
        assert is_unchanged(client.config)
        assert req_mock.call_count == 0, "do not interacted with cluster at all"
        del client

        # lazy load
        client = self.prepare_client_with_cluster_config(
            config_remote_patch_path,
            {
                "default": {
                    "operation_link_template": "zzz",
                    "enable_proxy_discovery": False,
                },
            },
            {
                "apply_remote_patch_at_start": False,
            },
            req_mock=req_mock,
        )
        assert req_mock.call_count == 0, "do not interacted with cluster at start"
        assert bool(client.config["proxy"]["enable_proxy_discovery"]) == False, "changed"  # noqa
        assert is_changed(client.config)
        assert req_mock.call_count > 0, "interacted with cluster"
        del client

        # bad config 1 (str instead doc)
        client = self.prepare_client_with_cluster_config(config_remote_patch_path, {
            "default": "some_wrong_string",
        })
        req_mock.reset_mock()
        assert is_unchanged(client.config)
        assert req_mock.call_count > 0, "interacted with cluster"
        req_mock.reset_mock()
        assert is_unchanged(client.config)
        assert req_mock.call_count == 0, "cache bad config"
        assert len(pickle.dumps(client.config["proxy"])) > 0
        assert deepcopy(client.config) is not None
        del client

        # bad config 2 (table instead root map_node)
        client = self.prepare_client_with_cluster_config(config_remote_patch_path, {})
        yt.remove(config_remote_patch_path, recursive=True)
        yt.create("table", config_remote_patch_path)
        yt.write_table(config_remote_patch_path, [{"foo": 1, "bar": 1}, {"foo": 2, "bar": 2}])
        req_mock.reset_mock()
        assert is_unchanged(client.config)
        assert req_mock.call_count > 0, "interacted with cluster"
        req_mock.reset_mock()
        assert is_unchanged(client.config)
        assert req_mock.call_count == 0, "cache bad config"
        del client

        # bad config 3 (table instead document)
        client = self.prepare_client_with_cluster_config(config_remote_patch_path, {})
        yt.create("table", config_remote_patch_path + "/default")
        yt.write_table(config_remote_patch_path + "/default", [{"foo": 1, "bar": 1}, {"foo": 2, "bar": 2}])
        req_mock.reset_mock()
        assert is_unchanged(client.config)
        assert req_mock.call_count > 0, "interacted with cluster"
        req_mock.reset_mock()
        assert is_unchanged(client.config)
        assert req_mock.call_count == 0, "cache bad config"
        del client

        yt.default_config.default_config["proxy"]["operation_link_pattern"] = yt.default_config.RemotePatchableString(
            "{proxy}/{cluster_path}?page=operation&mode=detail&id={id}&tab=details",
            "operation_link_template",
            yt.default_config._validate_operation_link_pattern
        )

        # bad config template (wrong placeholder in cluster)
        client = self.prepare_client_with_cluster_config(config_remote_patch_path, {
            "default": {
                "operation_link_template": "zzz{wrong_place_holder}",
            }
        })
        assert is_unchanged(client.config)
        del client

        # good config template
        client = self.prepare_client_with_cluster_config(config_remote_patch_path, {
            "default": {
                "operation_link_template": "-{cluster_ui_host}-{cluster_path}-{operation_id}-zzz",
            }
        })
        assert str(client.config["proxy"]["operation_link_pattern"]) == "-{proxy}-{cluster_path}-{id}-zzz"
        del client

    @authors("denvr")
    def test_remote_config_load_experement(self):
        config_remote_patch_path = "//test_client_config_exp"

        yt.default_config.default_config["proxy"]["operation_link_pattern"] = yt.default_config.RemotePatchableString(
            "{proxy}/{cluster_path}?page=operation&mode=detail&id={id}&tab=details",
            "operation_link_template",
            lambda local_value, remote_value: remote_value
        )

        client = self.prepare_client_with_cluster_config(config_remote_patch_path, {
            "default": {
                "operation_link_template": "zzz",
            },
            "experiment_20": {
                "operation_link_template": "mmm",
            },
        })

        ret_default = 0
        ret_experiment_20 = 0
        randoms = [19, 19, 21, 21, 21, 50]

        while len(randoms):
            with mock.patch('random.randrange', lambda size: randoms.pop()):
                yt.default_config.RemotePatchableValueBase._REMOTE_CACHE = {}
                client = yt.YtClient(config={"proxy": {"url": client.config["proxy"]["url"]}, "config_remote_patch_path": client.config["config_remote_patch_path"]})
                if str(client.config["proxy"]["operation_link_pattern"]) == "zzz":
                    ret_default += 1
                elif str(client.config["proxy"]["operation_link_pattern"]) == "mmm":
                    ret_experiment_20 += 1
        assert ret_default + ret_experiment_20 == 6
        assert ret_experiment_20 == 2
        del client

    @authors("denvr")
    def test_remote_config_load_global(self):
        config_remote_patch_path = "//test_client_config_1"
        yt.default_config.default_config["proxy"]["enable_proxy_discovery"] = yt.default_config.RemotePatchableBoolean(True, "enable_proxy_discovery")
        yt.default_config.default_config["proxy"]["operation_link_pattern"] = yt.default_config.RemotePatchableString(
            "{proxy}/{cluster_path}?page=operation&mode=detail&id={id}&tab=details",
            "operation_link_template",
            lambda local_value, remote_value: remote_value
        )

        yt.config["config_remote_patch_path"] = config_remote_patch_path
        yt.config._init_from_cluster()

        yt.create("map_node", config_remote_patch_path)
        yt.create("document", config_remote_patch_path + "/default")
        yt.set(
            config_remote_patch_path + "/default",
            {
                "operation_link_template": "zzz",
                "enable_proxy_discovery": False,
                "other": 999
            },
        )

        assert str(yt.config["proxy"]["operation_link_pattern"]) == "zzz", "changed"
        assert bool(yt.config["proxy"]["enable_proxy_discovery"]) == False, "changed"  # noqa
        assert yt.config["proxy"]["request_timeout"] == 20000, "default"
        assert len(pickle.dumps(yt.config.config["proxy"])) > 0
        assert deepcopy(yt.config.config) is not None
        yt.remove(config_remote_patch_path, recursive=True, force=True)

    @authors("denvr")
    def test_remote_config_and_env(self):
        config_remote_patch_path = "//test_client_config_1"
        yt.create("map_node", config_remote_patch_path)
        yt.create("document", config_remote_patch_path + "/default")
        yt.set(
            config_remote_patch_path + "/default",
            {
                "enable_proxy_discovery": True,
            },
        )
        yt.config["config_remote_patch_path"] = config_remote_patch_path

        yt.default_config.default_config["proxy"]["enable_proxy_discovery"] = yt.default_config.RemotePatchableBoolean(False, "enable_proxy_discovery")
        env_patched = os.environ
        env_patched.update({"YT_USE_HOSTS": "1"})
        with mock.patch.dict(os.environ, env_patched):
            config = yt.default_config.get_config_from_env()

        assert type(yt.default_config.default_config["proxy"]["enable_proxy_discovery"]) == yt.default_config.RemotePatchableBoolean
        assert type(config["proxy"]["enable_proxy_discovery"]) == bool
        assert config["proxy"]["enable_proxy_discovery"]
        yt.remove(config_remote_patch_path, recursive=True, force=True)

    @authors("pechatnov")
    def test_proxy_aliases_config(self):
        node_path = "//test_proxy_aliases_config_node"

        original_client = yt.YtClient(config={"proxy": {"url": yt.config.config["proxy"]["url"]}})
        original_client.set(node_path, "value")

        manual_alias_client = yt.YtClient(config={"proxy": {"url": "cluster-name", "aliases": {"cluster-name": yt.config.config["proxy"]["url"]}}})
        assert manual_alias_client.get(node_path) == "value"

        # Env was set by YT recipe.
        config = deepcopy(yt.default_config.get_config_from_env())
        config["proxy"]["url"] = str(original_client.get("//sys/@cluster_name"))
        env_alias_client = yt.YtClient(config=config)
        assert env_alias_client.get(node_path) == "value"
