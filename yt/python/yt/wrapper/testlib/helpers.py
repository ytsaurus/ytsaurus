from yt.packages import requests

from yt.test_helpers import wait, get_tests_sandbox
from yt.test_helpers.job_events import JobEvents

from yt.testlib import (yatest_common, authors, check_rows_equality, set_config_option, set_config_options, set_cypress_attribute)  # noqa

import yt.logger as logger
import yt.yson as yson
import yt.subprocess_wrapper as subprocess
import yt.environment.arcadia_interop as arcadia_interop

from yt.wrapper.errors import YtRetriableError
from yt.wrapper.http_driver import TokenAuth
import yt.wrapper as yt

import datetime
import glob
import os
import random
import shutil
import stat
import string
import sys
import tempfile
import threading
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass
from typing import Optional, List
from unittest.mock import patch
try:
    import collections.abc as collections_abc
except ImportError:
    import collections as collections_abc

TEST_DIR = "//home/wrapper_tests"


def get_tests_location():
    if yatest_common is None:
        return os.path.abspath(os.path.join(os.path.dirname(__file__), "../tests"))
    else:
        return yatest_common.source_path("yt/python/yt/wrapper/tests")


def get_test_files_dir_path():
    return os.path.join(get_tests_location(), "files")


def get_test_file_path(name, use_files=True):
    if yatest_common is not None:
        import library.python.resource
        file_path = os.path.join(yatest_common.work_path(), "tmp_files", name)
        dir_path = os.path.dirname(file_path)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        with open(file_path, "wb") as fout:
            files_dir = "files/" if use_files else ""
            resource_path = "/yt_python_test/" + files_dir + name
            fout.write(library.python.resource.find(resource_path))
        os.chmod(file_path, 0o744)
        return file_path
    else:
        if use_files:
            return os.path.join(get_tests_location(), "files", name)
        else:
            return os.path.join(get_tests_location(), name)


def get_binary_path(name):
    if yatest_common is not None:
        import library.python.resource
        file_path = os.path.join(yatest_common.work_path(), "tmp_files", name)
        dir_path = os.path.dirname(file_path)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        with open(file_path, "wb") as fout:
            resource_path = "/binaries/" + name
            fout.write(library.python.resource.find(resource_path))
        os.chmod(file_path, 0o744)
        return file_path
    else:
        return os.path.join(get_tests_location(), "../bin", name)


def get_python():
    if yatest_common is None:
        return sys.executable
    else:
        return arcadia_interop.search_binary_path("yt-python3")


def _filter_simple_types(obj):
    if isinstance(obj, int) or \
            isinstance(obj, float) or \
            obj is None or \
            isinstance(obj, yson.YsonType) or \
            isinstance(obj, (bytes, str)):
        return obj
    elif isinstance(obj, datetime.timedelta):
        return obj.total_seconds() * 1000.0
    elif isinstance(obj, yt.default_config.RemotePatchableValueBase):
        return obj.value
    elif isinstance(obj, list):
        return [_filter_simple_types(item) for item in obj]
    elif isinstance(obj, collections_abc.Mapping):
        return dict([(key, _filter_simple_types(value)) for key, value in obj.items()])
    return None


def get_environment_for_binary_test(yt_env, enable_request_logging=True):
    binaries_dir = os.path.join(os.path.dirname(get_tests_location()), "bin")

    if yatest_common is None:
        python_binary = sys.executable
        yt_binary = os.path.join(binaries_dir, "yt")
        mapreduce_binary = os.path.join(binaries_dir, "mapreduce-yt")
    else:
        python_binary = get_python()
        yt_binary = get_binary_path("yt")
        mapreduce_binary = get_binary_path("mapreduce-yt")

    env = {
        "PYTHON_BINARY": python_binary,
        "YT_ENABLE_TOKEN": "0",
        "YT_VERSION": yt.config["api_version"],
        "YT_PRINT_BACKTRACE": "1",
        "YT_CLI_PATH": yt_binary,
        "MAPREDUCE_YT_CLI_PATH": mapreduce_binary,
    }
    if yatest_common is None:
        env["PYTHONPATH"] = os.environ["PYTHONPATH"]

    config = deepcopy(_filter_simple_types(yt.config.config))

    if config["backend"] == "native":
        _, filename = tempfile.mkstemp(dir=get_tests_sandbox(), prefix="binary_test_driver_config")
        with open(filename, "wb") as f:
            yson.dump({"driver": config["driver_config"], "logging": yt_env.env.configs["driver_logging"]}, f)

        config["driver_config"] = None
        config["driver_config_path"] = filename

    config["enable_request_logging"] = enable_request_logging

    env["YT_CONFIG_PATCHES"] = yson._dumps_to_native_str(config)
    return env


def build_python_egg(egg_contents_dir, temp_dir=None):
    dir_ = tempfile.mkdtemp(dir=temp_dir)

    for obj in os.listdir(egg_contents_dir):
        src = os.path.join(egg_contents_dir, obj)
        dst = os.path.join(dir_, obj)
        if os.path.isdir(src):
            shutil.copytree(src, dst)
        else:  # file
            shutil.copy2(src, dst)

    _, egg_filename = tempfile.mkstemp(dir=temp_dir, suffix=".egg")
    try:
        subprocess.check_call([get_python(), "setup.py", "bdist_egg"], cwd=dir_)

        eggs = glob.glob(os.path.join(dir_, "dist", "*.egg"))
        assert len(eggs) == 1

        shutil.copy2(eggs[0], egg_filename)
        return egg_filename
    finally:
        shutil.rmtree(dir_, ignore_errors=True)


def dumps_yt_config():
    config = _filter_simple_types(yt.config.config)
    return yson._dumps_to_native_str(config)


def run_python_script_with_check(yt_env, script):
    dir_ = yt_env.env.path

    with tempfile.NamedTemporaryFile(mode="w", dir=dir_, suffix=".py", delete=False) as f:
        f.write(script)
        f.close()

        proc = subprocess.Popen(
            [sys.executable, f.name],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)

        out, err = proc.communicate(dumps_yt_config().encode("latin-1"))
        assert proc.returncode == 0, err

        return out, err


# By default, accounts have empty resource limits upon creation.
def get_default_resource_limits():
    GB = 1024 ** 3
    TB = 1024 ** 4

    result = {
        "node_count": 500000,
        "chunk_count": 1000000,
        "master_memory": {
            "total": 100 * GB,
            "chunk_host": 100 * GB,
            "per_cell": {}
        },
        "disk_space_per_medium": {
            "default": 10 * TB,
        },
    }

    return result


def sync_create_cell():
    tablet_id = yt.create("tablet_cell", attributes={"size": 1})
    wait(lambda: yt.get("//sys/tablet_cells/{0}/@health".format(tablet_id)) == "good")


def wait_record_in_job_archive(operation_id, job_id):
    operation_id_hash_pair = yt.common.uuid_hash_pair(operation_id)
    job_id_hash_pair = yt.common.uuid_hash_pair(job_id)
    # Jobs
    key = {}
    key["operation_id_hi"], key["operation_id_lo"] = operation_id_hash_pair.hi, operation_id_hash_pair.lo
    key["job_id_hi"], key["job_id_lo"] = job_id_hash_pair.hi, job_id_hash_pair.lo
    wait(lambda: any(yt.lookup_rows("//sys/operations_archive/jobs", [key], column_names=["operation_id_hi"])))
    # Job specs
    key = {}
    key["job_id_hi"], key["job_id_lo"] = job_id_hash_pair.hi, job_id_hash_pair.lo
    wait(lambda: any(yt.lookup_rows("//sys/operations_archive/job_specs", [key], column_names=["spec_version"])))


def get_operation_path(operation_id):
    return "//sys/operations/{:02x}/{}".format(int(operation_id.split("-")[-1], 16) % 256, operation_id)


def create_job_events():
    tmpdir = tempfile.mkdtemp(prefix="job_events", dir=get_tests_sandbox())
    os.chmod(tmpdir, stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
    return JobEvents(tmpdir)


@contextmanager
def failing_heavy_request(module, n_fails, assert_exhausted=True):
    make_request = module.make_request
    fail_state = dict(fails_left=n_fails, exhausted=False)
    lock = threading.Lock()

    def failing_make_request(*args, **kwargs):
        with lock:
            if fail_state["fails_left"] > 0:
                if "data" in kwargs:
                    list(kwargs["data"])  # exhaust data generator
                fail_state["fails_left"] -= 1
                raise YtRetriableError
            else:
                fail_state["exhausted"] = True
        return make_request(*args, **kwargs)

    module.make_request = failing_make_request
    try:
        yield
    finally:
        module.make_request = make_request

    if assert_exhausted:
        assert fail_state["exhausted"]


def random_string(length):
    char_set = string.ascii_lowercase + string.digits + string.ascii_uppercase
    return "".join(random.choice(char_set) for _ in range(length))


@contextmanager
def log_http_request(
    commands: Optional[List[str]] = None,
):
    """Log commands called via `make_request` (http only)
    """
    retrier_action_orig = yt.http_helpers.HTTPRequestRetrier.action
    stat = []

    def retrier_action_wrapper(this, *args, **kwargs):
        if this.url and "/api/" in this.url:
            command_name = this.url.split("/")[-1]
            if not commands or command_name in commands:
                stat.append(
                    {
                        "command_name": command_name,
                        "params": this.params,
                        "headers": this.headers,
                    }
                )
        return retrier_action_orig(this, *args, **kwargs)

    with patch.object(yt.http_helpers.HTTPRequestRetrier, "action", retrier_action_wrapper):
        yield stat


@contextmanager
def inject_http_error(
    client,
    filter_url: Optional[str] = None,
    interrupt_from: int = 0,
    interrupt_till: int = 3,
    interrupt_every: int = 2,
    raise_connection_reset: bool = False,
    raise_custom_exception: Optional[requests.exceptions.RequestException] = None,
    response: Optional[requests.Response] = None,
):
    """Raises RuntimeError or ConnectionError("Connection aborted.") every N http request.
       NB: Modifies client.config retries, patches client's requests.Session object
         filter_url - which urls will intercepted
         interrupt_from/interrupt_till - "window" in filtered requests
         interrupt_every - raise every N filtered request
       Returns Counters with stat
         total_calls - total http reqeusts
         filtered_total_calls - http requests matched by "filter_url"
         filtered_raises - how many times exception rised
    """
    class Counters(object):
        total_calls = 0  # type: int
        filtered_total_calls = 0  # type: int
        filtered_raises = 0  # type: int
        filtered_bypasses = 0  # type: int

        def __str__(self):
            return f"Counters: {self.filtered_raises=} {self.total_calls=} {self.filtered_total_calls=} {self.filtered_bypasses=}"

    if not client._requests_session:
        yt.http_helpers._get_session(client)

    cnt = Counters()
    reqeust_session_send_orig = client._requests_session.send

    client.config["write_retries"]["backoff"]["policy"] = "constant_time"
    client.config["write_retries"]["backoff"]["constant_time"] = 0

    def send_wrapper(*args, **kwargs):
        cnt.total_calls += 1
        if (filter_url is not None and args[0].url and filter_url in args[0].url):
            cnt.filtered_total_calls += 1
            if cnt.filtered_total_calls > interrupt_from \
                    and interrupt_every and not (cnt.filtered_total_calls - interrupt_from - 1) % interrupt_every \
                    and cnt.filtered_total_calls < interrupt_till:
                cnt.filtered_raises += 1
                logger.debug("Simulate network error for url \"{}\".".format(args[0].url))
                if raise_custom_exception:
                    raise raise_custom_exception
                elif raise_connection_reset:
                    # PY3: raise_custom_exception=requests.ConnectionError("Connection aborted.", ConnectionResetError(104, "Connection reset by peer"))
                    # PY2: raise_custom_exception=requests.ConnectionError("Connection aborted.", socket.error(errno.ECONNRESET))
                    raise requests.ConnectionError("Connection aborted.")
                elif response is not None:
                    return response
                else:
                    raise RuntimeError()
            else:
                cnt.filtered_bypasses += 1

        return reqeust_session_send_orig(*args, **kwargs)

    with patch.object(client._requests_session, "send", send_wrapper):
        yield cnt


@contextmanager
def inject_http_read_error(
    filter_url: Optional[str] = None,
    interrupt_from: int = 0,
    interrupt_till: int = 2,
    interrupt_every: int = 1,
    interrupt_read_iteration: int = 5,
):
    """Raises ReadTimeout while reading response stream.
       NB: patches response class
    """
    @dataclass
    class Counters:
        total_calls: int = 0
        filtered_total_calls: int = 0
        filtered_raises: int = 0
        filtered_bypasses: int = 0
        filtered_read_calls: int = 0

        def __str__(self):
            return f"Counters: {self.filtered_raises=} {self.total_calls=} {self.filtered_total_calls=} {self.filtered_bypasses=}"

    cnt = Counters()

    reqeust_session_send_orig = requests.Session.send

    def session_send_wrapper(self, request, *args, **kwargs):
        cnt.total_calls += 1
        if (filter_url is not None and request.url and filter_url in request.url):
            cnt.filtered_total_calls += 1
            if cnt.filtered_total_calls > interrupt_from \
                    and interrupt_every and not (cnt.filtered_total_calls - interrupt_from - 1) % interrupt_every \
                    and cnt.filtered_total_calls < interrupt_till:
                response = reqeust_session_send_orig(self, request, *args, **kwargs)
                all_data = response.content

                def iter_content_wrapper(chunk_size=None, decode_unicode=None):
                    if not chunk_size:
                        chunk_size = 1
                    cnt.filtered_read_calls = 0
                    while True:
                        if cnt.filtered_read_calls * chunk_size > len(all_data):
                            return
                        if cnt.filtered_read_calls == interrupt_read_iteration:
                            cnt.filtered_raises += 1
                            raise requests.ReadTimeout(Exception(f"Read timeout {cnt.filtered_read_calls=}"), request=request)
                        yield all_data[cnt.filtered_read_calls * chunk_size:cnt.filtered_read_calls * chunk_size + chunk_size]
                        cnt.filtered_read_calls += 1

                response.iter_content = iter_content_wrapper
                return response
            else:
                cnt.filtered_bypasses += 1

        return reqeust_session_send_orig(self, request, *args, **kwargs)

    with patch.object(requests.Session, "send", session_send_wrapper):
        yield cnt


class CustomAuthTest(TokenAuth):
    def __init__(self, config):
        super().__init__(None)
        self.config = config

    def __call__(self, request):
        self.token = self.config["token"]
        return super().__call__(request)
