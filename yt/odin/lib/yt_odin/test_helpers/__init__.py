from __future__ import print_function

import yt_odin.odinserver.odin as odin_module

from yt_odin.odinserver.odin import Odin
from yt_odin.storage.storage import StorageForCluster, OdinDBRecord

from yt.environment import arcadia_interop
import yt.local as yt_local
from yt.test_helpers import wait, get_tests_sandbox, get_build_root, prepare_yt_environment

from six import text_type, binary_type
from six.moves import xrange

import pytest

import contextlib
import json
import os
import shutil
import sys
import tempfile
import time
import uuid


def generate_unique_id(prefix):
    return prefix + '_' + uuid.uuid4().hex[:8]


class FileStorage(StorageForCluster):
    def __init__(self, filename):
        self.file = filename

    def add_record(self, check_id, service, timestamp, **kwargs):
        row = dict(service=service, timestamp=timestamp, **kwargs)
        print("Adding record", repr(row), "from check", check_id, "to file", self.file, file=sys.stderr)
        assert sorted(kwargs.keys()) == sorted(["duration", "state", "messages"])
        with open(self.file, "a") as output:
            json.dump(row, output)
            output.write("\n")

    def get_records(self, services, start_timestamp, stop_timestamp):
        if isinstance(services, (text_type, binary_type)):
            services = [services]
        result = []
        with open(self.file, "r") as f:
            for line in f:
                if not line:
                    continue
                record = json.loads(line)
                if record["service"] in services and start_timestamp <= record["timestamp"] <= stop_timestamp:
                    result.append(OdinDBRecord(cluster=None, **record))
        return result

    def get_all_records(self, service):
        return self.get_records(service, 0, float("+inf"))

    def get_service_states(self, service):
        records = self.get_all_records(service)
        return [record.state for record in records]


def run_checks(odin):
    odin.start_tasks(time.time())
    odin.wait_for_tasks()


def make_check_dir(check_name, check_options=None, check_secrets=None):
    if check_options is None:
        check_options = {}
    if check_secrets is None:
        check_secrets = {}
    check_path = arcadia_interop.search_binary_path(
        check_name,
        binary_root=os.path.join(get_build_root(), "yt/odin/checks/bin"))
    config = {
        "checks": {
            check_name: {
                "options": check_options,
                "secrets": check_secrets,
            }
        },
    }
    result = tempfile.mkdtemp()
    os.mkdir(os.path.join(result, check_name))
    shutil.copy(check_path, os.path.join(result, check_name))
    with open(os.path.join(result, "config.json"), "w") as fout:
        json.dump(config, fout)
    return result


class CheckWatcher(object):
    def __init__(self, db_client, check_name):
        self.db_client = db_client
        self.check_name = check_name
        self._check_count = 0

    def wait_new_result(self):
        wait(lambda: len(self.db_client.get_service_states(self.check_name)) == self._check_count + 1)
        self._check_count += 1
        return self.db_client.get_service_states(self.check_name)[-1]


@contextlib.contextmanager
def configure_odin(yt_cluster_url, checks_path):
    storage_file = tempfile.mkstemp()[1]
    storage_factory = lambda: FileStorage(storage_file)  # noqa

    socket_path = os.path.join(
        tempfile.gettempdir(),
        "{}.sock".format(generate_unique_id("configure_checks"))
    )

    odin_module.RELOAD_CHECKS = True
    odin = Odin(
        storage_factory,
        "test_cluster",
        yt_cluster_url,
        token=None,
        checks_path=checks_path,
        log_server_socket_path=socket_path,
        log_server_max_write_batch_size=256,
    )

    try:
        yield odin
    finally:
        odin.terminate()


def configure_and_run_checks(yt_cluster_url, checks_path):
    with configure_odin(yt_cluster_url, checks_path) as odin:
        run_checks(odin)
        time.sleep(5)
        return odin.create_db_client()


def _get_odin_tests_sandbox():
    return os.path.join(get_tests_sandbox(), "TestOdin")


class YtTestEnvironment(object):
    def __init__(self, yt_instance_params=None):
        if yt_instance_params is not None and not isinstance(yt_instance_params, dict):
            raise RuntimeError("'yt_instance_params' should be dict not '{}'".format(type(yt_instance_params)))
        if yt_instance_params is None:
            yt_instance_params = {}

        path = _get_odin_tests_sandbox()
        self.yt_instance_id = generate_unique_id("yt_env")
        self.yt_instance_path = os.path.join(path, self.yt_instance_id)

        self.yt_instance = yt_local.start(
            wait_tablet_cell_initialization=True,
            rpc_proxy_count=yt_instance_params.get("rpc_proxy_count", 1),
            http_proxy_count=yt_instance_params.get("http_proxy_count", 1),
            master_cache_count=yt_instance_params.get("master_cache_count", 1),
            path=path,
            id=self.yt_instance_id,
            enable_debug_logging=True,
            enable_master_cache=True)
        self.yt_client = self.yt_instance.create_client()

    def cleanup(self):
        self.yt_instance.stop()


class YtRandomNameTestEnvironment(object):
    def __init__(self, cell_tag):
        path = _get_odin_tests_sandbox()
        self.yt_instance_id = generate_unique_id("yt_env")
        self.yt_instance_path = os.path.join(path, self.yt_instance_id)

        self.yt_instance = yt_local.start(
            node_count=3,
            path=path,
            id=self.yt_instance_id,
            cell_tag=cell_tag,
            enable_debug_logging=True)
        self.yt_client = self.yt_instance.create_client()

    def cleanup(self):
        self.yt_instance.stop()


class YtMultipleInstancesTestEnvironment(object):
    def __init__(self, instances=2):
        self.environments = [YtRandomNameTestEnvironment(i + 2000) for i in xrange(instances)]

    def cleanup(self):
        for environment in self.environments:
            environment.cleanup()


@pytest.fixture(scope="session", autouse=True)
def prepare_yt():
    prepare_yt_environment()


@pytest.fixture(scope="function")
def yt_env(request):
    environment = YtTestEnvironment()
    request.addfinalizer(lambda: environment.cleanup())
    return environment


@pytest.fixture(scope="function")
def yt_env_two_http_proxies(request):
    environment = YtTestEnvironment({"http_proxy_count": 2})
    request.addfinalizer(lambda: environment.cleanup())
    return environment


@pytest.fixture(scope="function")
def yt_env_two_clusters(request):
    environments = YtMultipleInstancesTestEnvironment(2)
    request.addfinalizer(lambda: environments.cleanup())
    return environments
