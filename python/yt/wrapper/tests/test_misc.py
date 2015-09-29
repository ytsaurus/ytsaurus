from yt.wrapper.keyboard_interrupts_catcher import KeyboardInterruptsCatcher
from yt.wrapper.response_stream import ResponseStream, EmptyResponseStream
from yt.wrapper.verified_dict import VerifiedDict
import yt.yson as yson
import yt.wrapper as yt

from helpers import TEST_DIR, get_environment_for_binary_test, check

import inspect
import random
import string
import os
import sys
import subprocess
import tempfile
import time
import pytest

def test_docs_exist():
    functions = inspect.getmembers(yt, lambda o: inspect.isfunction(o) and \
                                                 not o.__name__.startswith('_'))
    functions_without_doc = filter(lambda (name, func): not inspect.getdoc(func), functions)
    assert not functions_without_doc

    classes = inspect.getmembers(yt, lambda o: inspect.isclass(o))
    for name, cl in classes:
        assert inspect.getdoc(cl)
        if name == "PingTransaction":
            continue # Python Thread is not documented O_o
        public_methods = inspect.getmembers(cl, lambda o: inspect.ismethod(o) and \
                                                          not o.__name__.startswith('_'))
        methods_without_doc = [method for name, method in public_methods
                                                            if (not inspect.getdoc(method))]
        assert not methods_without_doc

@pytest.mark.usefixtures("yt_env")
def test_yt_binary():
    env = get_environment_for_binary_test()
    if yt.config["api_version"] == "v2":
        env["FALSE"] = '"false"'
        env["TRUE"] = '"true"'
    else:
        env["FALSE"] = '%false'
        env["TRUE"] = '%true'

    current_dir = os.path.dirname(os.path.abspath(__file__))
    proc = subprocess.Popen(
        os.path.join(current_dir, "../test_yt.sh"),
        shell=True,
        env=env)
    proc.communicate()
    assert proc.returncode == 0


@pytest.mark.usefixtures("yt_env")
class TestMutations(object):
    def check_command(self, command, post_action=None, check_action=None, final_action=None):
        mutation_id = yt.common.generate_uuid()
        def run_command():
            yt.config.MUTATION_ID = mutation_id
            result = command()
            yt.config.MUTATION_ID = None
            return result

        result = run_command()
        if post_action is not None:
            post_action()
        for _ in xrange(5):
            yt.config.RETRY = True
            assert result == run_command()
            yt.config.RETRY = None
            if check_action is not None:
                assert check_action()

        if final_action is not None:
            final_action(result)

    def test_master_mutation_id(self):
        test_dir = os.path.join(TEST_DIR, "test")
        test_dir2 = os.path.join(TEST_DIR, "test2")
        test_dir3 = os.path.join(TEST_DIR, "test3")

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
        yt.write_table(table, ["x=1\n", "x=2\n"], format="dsv")
        yt.create_table(other_table)

        for command, params in \
            [(
                "map",
                {"spec":
                    {"mapper":
                        {"command": "sleep 1; cat"},
                     "input_table_paths": [table],
                     "output_table_paths": [other_table]}})]:

            operations_count = yt.get("//sys/operations/@count")

            self.check_command(
                lambda: yson.loads(yt.driver.make_request(command, params)),
                None,
                lambda: yt.get("//sys/operations/@count") == operations_count + 1,
                abort)


@pytest.mark.usefixtures("yt_env")
class TestRetries(object):
    def setup(self):
        yt.config["tabular_data_format"] = yt.format.DsvFormat()

    def test_read_with_retries(self):
        old_value = yt.config["read_retries"]["enable"]
        yt.config["read_retries"]["enable"] = True
        yt.config._ENABLE_READ_TABLE_CHAOS_MONKEY = True
        try:
            table = TEST_DIR + "/table"

            with pytest.raises(yt.YtError):
                yt.read_table(table)

            yt.create_table(table)
            check([], yt.read_table(table, raw=False))
            assert "" == yt.read_table(table).read()

            yt.write_table(table, ["x=1\n", "y=2\n"])
            check(["x=1\n", "y=2\n"], yt.read_table(table))

            rsp = yt.read_table(table)
            assert rsp.next() == "x=1\n"
            yt.write_table(table, ["x=1\n", "y=3\n"])
            assert rsp.next() == "y=2\n"
            rsp.close()

            rsp = yt.read_table(table, raw=False)
            assert [("x", "1"), ("y", "3")] == sorted([x.items()[0] for x in rsp])

            response_parameters = {}
            rsp = yt.read_table(table, response_parameters=response_parameters)
            assert {"start_row_index": 0, "approximate_row_count": 2} == response_parameters
            rsp.close()

        finally:
            yt.config._ENABLE_READ_TABLE_CHAOS_MONKEY = False
            yt.config["read_retries"]["enable"] = old_value

    def test_heavy_requests_with_retries(self):
        table = TEST_DIR + "/table"

        old_request_retry_timeout = yt.config["proxy"]["request_retry_timeout"]
        old_enable_write_retries = yt.config["write_retries"]["enable"]

        yt.config["write_retries"]["enable"] = True
        yt.config["proxy"]["request_retry_timeout"] = 1000
        yt.config._ENABLE_HEAVY_REQUEST_CHAOS_MONKEY = True

        _, filename = tempfile.mkstemp()
        with open(filename, "w") as fout:
            fout.write("retries test")

        try:
            yt.write_table(table, ["x=1\n"])
            yt.smart_upload_file(filename, placement_strategy="random")
            yt.write_table(table, ["x=1\n"])
            yt.write_table(table, ["x=1\n"])
            yt.smart_upload_file(filename, placement_strategy="random")
            yt.smart_upload_file(filename, placement_strategy="random")
            yt.write_table(table, ["x=1\n"])
        finally:
            yt.config._ENABLE_HEAVY_REQUEST_CHAOS_MONKEY = False
            yt.config["proxy"]["request_retry_timeout"] = old_request_retry_timeout
            yt.config["write_retries"]["enable"] = old_enable_write_retries

    def test_http_retries(self):
        old_request_retry_timeout = yt.config["proxy"]["request_retry_timeout"]
        yt.config._ENABLE_HTTP_CHAOS_MONKEY = True
        yt.config["proxy"]["request_retry_timeout"] = 1000
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
            yt.config["proxy"]["request_retry_timeout"] = old_request_retry_timeout
            yt.config["proxy"]["request_backoff_time"] = None

    def test_download_with_retries(self):
        text = "some long text repeated twice " * 2
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
            assert "twice " == yt.read_file(file_path, offset=len(text) - 6).read()
            assert "some" == yt.read_file(file_path, length=4).read()
        finally:
            yt.config._ENABLE_READ_TABLE_CHAOS_MONKEY = False
            yt.config["read_retries"]["enable"] = old_value
            yt.config["read_buffer_size"] = old_buffer_size


def test_wrapped_streams():
    import yt.wrapper._py_runner_helpers as runner_helpers
    with pytest.raises(yt.YtError):
        with runner_helpers.WrappedStreams():
            print "test"
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
        print "",
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
    vdict = VerifiedDict(["a"], {"b": 1, "c": True, "a": {"k": "v"}, "d": {"x": "y"}})
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

class TestResponseStream(object):
    def test_chunk_iterator(self):
        random_line = lambda: ''.join(random.choice(string.ascii_lowercase) for _ in xrange(100))
        s = '\n'.join(random_line() for _ in xrange(3))

        class StringIterator(object):
            def __init__(self, string, chunk_size=10):
                self.string = string
                self.pos = 0
                self.chunk_size = chunk_size

            def __iter__(self):
                return self

            def next(self):
                str_part = self.string[self.pos:self.pos + self.chunk_size]
                if not str_part:
                    raise StopIteration()
                self.pos += self.chunk_size
                return str_part

        close_list = []
        def close():
            close_list.append(True)

        string_iterator = StringIterator(s, 10)
        stream = ResponseStream(lambda: s, string_iterator, close, lambda x: None, lambda: None)

        assert stream.read(20) == s[:20]
        assert stream.read(2) == s[20:22]

        iterator = stream.chunk_iter()
        assert iterator.next() == s[22:30]
        assert iterator.next() == s[30:40]

        assert stream.readline() == s[40:101]
        assert stream.readline() == s[101:202]

        assert stream.read(8) == s[202:210]

        chunks = []
        for chunk in stream.chunk_iter():
            chunks.append(chunk)

        assert len(chunks) == 10
        assert "".join(chunks) == s[210:]
        assert stream.read() == ""

        stream.close()
        assert len(close_list) > 0

    def test_empty_response_stream(self):
        stream = EmptyResponseStream()
        assert stream.read() == ""
        assert len([x for x in stream.chunk_iter()]) == 0
        assert stream.readline() == ""

        values = []
        for value in stream:
            values.append(value)
        assert len(values) == 0

        with pytest.raises(StopIteration):
            stream.next()
