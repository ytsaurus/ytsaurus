from .test_tm_config import config as tm_config

from yt.transfer_manager.client import TransferManager
from yt.common import YtError
from yt.local import start, stop
from yt.environment.helpers import OpenPortIterator

import yt.subprocess_wrapper as subprocess
import yt.json as json
import yt.wrapper as yt
import yt.logger as logger

from yt.packages.six import PY3
from yt.packages.requests.exceptions import ConnectionError

import yt.packages.requests as requests

import os
import pytest
import tarfile
import time
import uuid
import signal
import platform

TM_CONFIG_PATH = "config.json"
TESTS_LOCATION = os.path.dirname(os.path.abspath(__file__))
TESTS_SANDBOX = os.environ.get("TESTS_SANDBOX", TESTS_LOCATION + ".sandbox")
TEST_RUN_PATH = os.path.join(TESTS_SANDBOX, "test_transfer_manager", "run_" + uuid.uuid4().hex[:8])

MAX_WAIT_TIME = 60
SLEEP_QUANTUM = 0.1

def get_yt_client():
    # If we have teamcity yt token use it otherwise use default token.
    token = os.environ.get("TEAMCITY_YT_TOKEN", None)
    yt_client = yt.YtClient(proxy="locke", token=token)
    try:
        yt_client.exists("/")
        return yt_client
    except ConnectionError:
        # If we have network errors we don't return client and tests will be skipped.
        return None

def _read_node_content(path, node_type, client):
    if node_type == "file":
        return client.read_file(path).read()
    return list(client.read_table(path))

def _write_node_content(path, node_type, client):
    if node_type == "file":
        client.write_file(path, b"Test file content")
    else:
        client.write_table(path, [{"a": 1}, {"b": 2}, {"c": 3}])

def _prepare_activate_file(env_path):
    activate_file_path = os.path.join(env_path, "bin", "activate")

    with open(activate_file_path) as activate_file:
        activate_file_content = activate_file.read().split("\n")

    for row_index, row in enumerate(activate_file_content):
        if row.startswith("VIRTUAL_ENV="):
            activate_file_content[row_index] = "VIRTUAL_ENV={0}".format(env_path)

    with open(activate_file_path, "w") as activate_file:
        activate_file.write("\n".join(activate_file_content))

def _start_transfer_manager(yt_client, config):
    config_path = os.path.join(TEST_RUN_PATH, TM_CONFIG_PATH)
    json.dump(config, open(config_path, "w"))

    tests_path = os.path.split(__file__)[0]
    tm_server_path = os.path.split(tests_path)[0]

    tm_binary_path = os.path.join(tm_server_path, "bin", "transfer-manager-server")
    script_binary_path = os.path.join(tests_path, "start_tm.sh")
    venv_path = os.path.join(TESTS_SANDBOX, "tmvenv")

    _, _, distribution_codename = platform.linux_distribution()

    start_tm_command = [script_binary_path, tm_binary_path, config_path, venv_path]
    logger.info("Preparing TM environment...")

    if not os.path.exists(venv_path):
        logger.info("Path with TM environment doesn't exists")
        os.mkdir(venv_path)
        cache_path = None
        with open(os.path.join(tests_path, "cache_path")) as fin:
            cache_path = fin.read()

        cache_file_path = "{0}/{1}-tmvenv.tar".format(cache_path, distribution_codename)
        if not yt_client.exists(cache_file_path):
            logger.info("Cached TM environment doesn't exists for '{0}'.".format(distribution_codename))
            prepare_tm_script_path = os.path.join(tests_path, "prepare_tm_environment.sh")
            tm_path = os.path.split(tm_server_path)[0]
            yt_path = os.path.split(tm_path)[0]
            python_path = os.path.split(yt_path)[0]

            tm_requirements_path = os.path.join(python_path, "yandex-yt-transfer-manager", "requirements.txt")
            prepare_tm_command = [prepare_tm_script_path, tm_requirements_path, venv_path]
            prepare_and_start_tm_command = "{0} && {1}".format(" ".join(prepare_tm_command), " ".join(start_tm_command))
            return subprocess.Popen(prepare_and_start_tm_command, shell=True, preexec_fn=os.setsid)

        logger.info("TM environment will be used from cache")
        logger.info("Starting to download TM environment...")
        content = yt_client.read_file(cache_file_path)
        archive_path = os.path.join(venv_path, "tmvenv.tar")
        with open(archive_path, "wb") as fin:
            fin.write(content.read())

        logger.info("Extracting files from an archive...")

        tar = tarfile.open(archive_path)
        tar.extractall(venv_path)
        tar.close()

        _prepare_activate_file(venv_path)
        logger.info("TM environment is ready")
    else:
        logger.info("Path with TM environment already exists")

    return subprocess.Popen(start_tm_command, preexec_fn=os.setsid)

def _abort_operations_and_transactions(client):
    for operation in client.list("//sys/operations"):
        client.abort_operation(operation)

    for transaction in client.list("//sys/transactions", attributes=["owner", "title"]):
        transaction_title = transaction.attributes.get("title", "")
        allowed_titles = ["Scheduler lock", "Lease for", "Prerequisite for", "Transfer manager lock"]

        if any(map(lambda allowed_title: allowed_title in transaction_title, allowed_titles)):
            continue

        client.abort_transaction(transaction)

@pytest.mark.skipif(PY3, reason="Transfer manager is available only for Python 2")
class TestTransferManager(object):
    @classmethod
    def _stop_processes(self):
        try:
            stop(self._first_cluster_yt_instance.id, path=TEST_RUN_PATH)
            stop(self._second_cluster_yt_instance.id, path=TEST_RUN_PATH)

            pgid = os.getpgid(self._tm_process.pid)
            os.killpg(pgid, signal.SIGKILL)
        except:
            pass

    def setup_class(self):
        yt_client = get_yt_client()
        if yt_client is None:
            pytest.skip("Cluster locke is unavailable")

        port_locks_path = os.path.join(TESTS_SANDBOX, "ports")
        os.environ["YT_LOCAL_PORT_LOCKS_PATH"] = port_locks_path

        try:
            self._first_cluster_yt_instance = start(node_count=3, path=TEST_RUN_PATH, enable_debug_logging=True, id="first", cell_tag=0)
            self._second_cluster_yt_instance = start(node_count=3, path=TEST_RUN_PATH, enable_debug_logging=True, id="second", cell_tag=1)

            self.first_cluster_client = self._first_cluster_yt_instance.create_client()
            self.second_cluster_client = self._second_cluster_yt_instance.create_client()

            self.first_cluster_client.create("map_node", "//tm_token_storage")
            self.first_cluster_client.create("map_node", "//transfer_manager")

            first_cluster_url = self.first_cluster_client.config["proxy"]["url"]
            second_cluster_url = self.second_cluster_client.config["proxy"]["url"]

            tm_config["clusters"]["clusterA"]["options"]["proxy"] = first_cluster_url
            tm_config["clusters"]["clusterB"]["options"]["proxy"] = second_cluster_url

            port_iterator = OpenPortIterator(port_locks_path)
            tm_config["port"] = next(port_iterator)
            tm_config["logging"]["port"] = next(port_iterator)
            tm_config["task_executor"]["port"] = next(port_iterator)
            tm_config["yt_backend_options"]["proxy"] = first_cluster_url
            tm_config["logging"]["filename"] = os.path.join(TEST_RUN_PATH, "transfer_manager.log")

            self._tm_process = _start_transfer_manager(yt_client, tm_config)

            current_wait_time = 0
            while current_wait_time < MAX_WAIT_TIME:
                try:
                    result = requests.get("http://localhost:{0}/ping".format(tm_config["port"]))
                    if result.status_code == 200:
                        break
                except:
                    pass

                time.sleep(SLEEP_QUANTUM)
                current_wait_time += SLEEP_QUANTUM
            else:
                raise YtError("Transfer manager is still not ready after {0} seconds".format(current_wait_time))

            self.tm_client = TransferManager("http://localhost:{0}".format(tm_config["port"]), token="test_token")
        except:
            self._stop_processes()
            raise

    def teardown_class(self):
        self._stop_processes()

    def setup(self):
        for dir in ("//tm", "//tmp/yt_wrapper/file_storage"):
            for client in (self.first_cluster_client, self.second_cluster_client):
                client.create("map_node", dir, recursive=True, ignore_existing=True)

    def teardown(self):
        for task in self.tm_client.get_tasks():
            self.tm_client.abort_task(task["id"])

        _abort_operations_and_transactions(self.first_cluster_client)
        _abort_operations_and_transactions(self.second_cluster_client)

        self.first_cluster_client.remove("//tm", force=True, recursive=True)
        self.second_cluster_client.remove("//tm", force=True, recursive=True)

    @pytest.mark.parametrize("node_type,copy_method", [("table", "proxy"), ("table", "native"),
                                                       ("file", "proxy")])
    def test_copy_empty_object(self, node_type, copy_method):
        self.first_cluster_client.create(node_type, "//tm/test_object")

        self.tm_client.add_task("clusterA", "//tm/test_object", "clusterB", "//tm/test_object", sync=True,
                                params={"copy_method": copy_method})
        assert self.second_cluster_client.exists("//tm/test_object")

    @pytest.mark.parametrize("node_type,copy_method,force_copy_with_operation",
                             [("file", "proxy", False), ("table", "proxy", False), ("table", "native", False),
                              ("file", "proxy", True), ("table", "proxy", True)])
    def test_copy_object(self, node_type, copy_method, force_copy_with_operation):
        _write_node_content("//tm/test_object", node_type, self.first_cluster_client)

        self.tm_client.add_task("clusterA", "//tm/test_object", "clusterB", "//tm/test_object", sync=True,
                                params={"force_copy_with_operation": force_copy_with_operation,
                                        "copy_method": copy_method})
        assert self.second_cluster_client.exists("//tm/test_object")

        assert _read_node_content("//tm/test_object", node_type, self.first_cluster_client) == \
               _read_node_content("//tm/test_object", node_type, self.second_cluster_client)

    @pytest.mark.parametrize("node_type,copy_method,force_copy_with_operation",
                             [("file", "proxy", False), ("table", "proxy", False), ("table", "native", False),
                              ("file", "proxy", True), ("table", "proxy", True)])
    def test_copy_attributes(self, node_type, copy_method, force_copy_with_operation):
        self.first_cluster_client.create(node_type, "//tm/test")
        self.first_cluster_client.set("//tm/test/@expiration_time", 2100000000000)
        self.first_cluster_client.set("//tm/test/@compression_codec", "zlib_9")
        self.first_cluster_client.set("//tm/test/@test_attr", "attr_value")
        self.tm_client.add_task("clusterA", "//tm/test", "clusterB", "//tm/test", sync=True,
                                params={"additional_attributes": ["expiration_time"],
                                        "force_copy_with_operation": force_copy_with_operation,
                                        "copy_method": copy_method})

        for attr_name in ["expiration_time", "compression_codec", "test_attr"]:
            assert self.first_cluster_client.get("//tm/test/@" + attr_name) == \
                   self.second_cluster_client.get("//tm/test/@" + attr_name)

    @pytest.mark.parametrize("node_type,copy_method,force_copy_with_operation",
                             [("file", "proxy", False), ("table", "proxy", False), ("table", "native", False),
                              ("file", "proxy", True), ("table", "proxy", True)])
    def test_destination_codecs(self, node_type, copy_method, force_copy_with_operation):
        _write_node_content("//tm/test_object", node_type, self.first_cluster_client)

        self.tm_client.add_task("clusterA", "//tm/test_object", "clusterB", "//tm/test_object", sync=True,
                                params={"destination_compression_codec": "zlib_6",
                                        "force_copy_with_operation": force_copy_with_operation,
                                        "copy_method": copy_method})
        assert "zlib_6" == self.second_cluster_client.get("//tm/test_object/@compression_codec")
        assert _read_node_content("//tm/test_object", node_type, self.first_cluster_client) == \
               _read_node_content("//tm/test_object", node_type, self.second_cluster_client)

    @pytest.mark.parametrize("node_type,copy_method,force_copy_with_operation",
                             [("file", "proxy", False), ("table", "proxy", False), ("table", "native", False),
                              ("file", "proxy", True), ("table", "proxy", True)])
    def test_source_codecs(self, node_type, copy_method, force_copy_with_operation):
        self.first_cluster_client.create(node_type, "//tm/test_object", attributes={"compression_codec": "zlib_6"})
        _write_node_content("//tm/test_object", node_type, self.first_cluster_client)

        self.tm_client.add_task("clusterA", "//tm/test_object", "clusterB", "//tm/test_object", sync=True,
                                params={"force_copy_with_operation": force_copy_with_operation,
                                        "copy_method": copy_method})
        assert "zlib_6" == self.second_cluster_client.get("//tm/test_object/@compression_codec")

        assert _read_node_content("//tm/test_object", node_type, self.first_cluster_client) == \
               _read_node_content("//tm/test_object", node_type, self.second_cluster_client)

    @pytest.mark.parametrize("force_copy_with_operation", [True, False])
    def test_copy_table_range(self, force_copy_with_operation):
        self.first_cluster_client.write_table("//tm/test_table", [{"a": 1}, {"b": 2}, {"c": 3}])
        self.tm_client.add_task("clusterA", "//tm/test_table[#1:#2]", "clusterB", "//tm/test_table", sync=True,
                                params={"force_copy_with_operation": force_copy_with_operation,
                                        "copy_method": "proxy"})

        assert list(self.second_cluster_client.read_table("//tm/test_table")) == [{"b": 2}]

    @pytest.mark.parametrize("force_copy_with_operation", [True, False])
    def test_copy_table_range_with_codec(self, force_copy_with_operation):
        self.first_cluster_client.write_table("//tm/test_table", [{"a": 1}, {"b": 2}, {"c": 3}])
        self.tm_client.add_task("clusterA", "//tm/test_table[#1:#2]", "clusterB", "//tm/test_table", sync=True,
                                params={"copy_method": "proxy", "destination_compression_codec": "zlib_9",
                                        "force_copy_with_operation": force_copy_with_operation})

        assert list(self.second_cluster_client.read_table("//tm/test_table")) == [{"b": 2}]
        assert "zlib_9" == self.second_cluster_client.get("//tm/test_table/@compression_codec")

    @pytest.mark.parametrize("force_copy_with_operation", [True, False])
    def test_schema_copy(self, force_copy_with_operation):
        self.first_cluster_client.create("table", "//tm/test_table",
                                         attributes={"schema": [{"type": "string", "name": "a"}]})
        self.first_cluster_client.write_table("//tm/test_table", [{"a": "test"}])

        self.tm_client.add_task("clusterA", "//tm/test_table", "clusterB", "//tm/test_table", sync=True,
                                params={"force_copy_with_operation": force_copy_with_operation,
                                        "copy_method": "proxy"})

        assert list(self.second_cluster_client.read_table("//tm/test_table")) == [{"a": "test"}]
        assert self.second_cluster_client.get("//tm/test_table/@schema/0/type") == "string"
        assert self.second_cluster_client.get("//tm/test_table/@schema/@strict")
        assert self.second_cluster_client.get("//tm/test_table/@schema_mode") == "strong"

    def test_lease(self):
        self.first_cluster_client.write_table("//tm/test_table", [{"a": 1}, {"b": 2}, {"c": 3}])
        task_id = self.tm_client.add_task("clusterA", "//tm/test_table", "clusterB", "//tm/test_table",
                                          params={"copy_method": "proxy", "lease_timeout": 1})
        time.sleep(5)
        task_info = self.tm_client.get_task_info(task_id)
        assert task_info["state"] == "aborted"

    @pytest.mark.parametrize("copy_method,force_copy_with_operation",
                             [("proxy", False), ("native", False), ("proxy", True)])
    def test_types_preserving_during_copy(self, copy_method, force_copy_with_operation):
        self.first_cluster_client.write_table("//tm/test_table", [{"a": True}, {"b": 0}, {"c": "test"}])
        self.tm_client.add_task("clusterA", "//tm/test_table", "clusterB", "//tm/test_table", sync=True,
                                params={"force_copy_with_operation": force_copy_with_operation,
                                        "copy_method": copy_method})
        assert list(self.second_cluster_client.read_table("//tm/test_table")) == \
               list(self.first_cluster_client.read_table("//tm/test_table"))

    @pytest.mark.parametrize("copy_method,force_copy_with_operation",
                             [("proxy", False), ("native", False), ("proxy", True)])
    def test_destination_directory(self, copy_method, force_copy_with_operation):
        params = {"copy_method": copy_method, "force_copy_with_operation": force_copy_with_operation}

        self.first_cluster_client.write_table("//tm/test_table", [{"a": 1}])

        # missing dir case
        self.tm_client.add_task("clusterA", "//tm/test_table", "clusterB", "//tm/test_dir/test_table",
                                sync=True, params=params)
        assert list(self.second_cluster_client.read_table("//tm/test_dir/test_table")) == [{"a": 1}]

        self.second_cluster_client.remove("//tm/test_dir", force=True, recursive=True)

        # link case
        self.second_cluster_client.create("map_node", "//tm/map_node")
        self.second_cluster_client.link("//tm/map_node", "//tm/test_dir")
        self.tm_client.add_task("clusterA", "//tm/test_table", "clusterB", "//tm/test_dir/test_table",
                                sync=True, params=params)
        assert list(self.second_cluster_client.read_table("//tm/test_dir/test_table")) == [{"a": 1}]
        assert list(self.second_cluster_client.read_table("//tm/map_node/test_table")) == [{"a": 1}]

        self.second_cluster_client.remove("//tm/test_dir", force=True, recursive=True)

        # document case
        self.second_cluster_client.create("document", "//tm/test_dir")

        task_id = self.tm_client.add_task("clusterA", "//tm/test_table", "clusterB", "//tm/test_dir/test_table",
                                          params=params)

        task_info = None
        for _ in xrange(5):
            task_info = self.tm_client.get_task_info(task_id)
            if task_info["state"] in ["failed", "aborted", "completed"]:
                break
            time.sleep(1)

        assert task_info["state"] == "failed"

    def test_skip_if_destination_exists(self):
        self.first_cluster_client.write_table("//tm/test_table", [{"a": 1}])
        self.second_cluster_client.write_table("//tm/test_table", [{"b": 2}])

        self.tm_client.add_task("clusterA", "//tm/test_table", "clusterB", "//tm/test_table",
                                sync=True, params={"copy_method": "proxy", "skip_if_destination_exists": True})
        assert list(self.second_cluster_client.read_table("//tm/test_table")) == [{"b": 2}]
