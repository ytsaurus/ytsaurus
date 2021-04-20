from yt.common import update
from yt.environment.arcadia_interop import (prepare_yt_binaries, collect_cores, search_binary_path,
                                            remove_runtime_data, copy_misc_binaries)

import yt.yson as yson

from functools import wraps
import logging
import os
import sys
import shutil
import tempfile
import time
import uuid
import signal
import fcntl
import copy

import yatest.common
import yatest.common.network

import devtools.swag.daemon
import devtools.swag.ports

_ADMISSIBLE_ENV_VARS = [
    # Required for proper coverage.
    "LLVM_PROFILE_FILE",
    "PYTHON_COVERAGE_PREFIX",
]

_YT_PREFIX = "//"
_YT_MAX_START_RETRIES = 3

# Default size of listen port pool allocated to local YT
_YT_LISTEN_PORT_POOL_SIZE = 50

# If user specified node count we add this additional ports for each node to our port pool.
_YT_LISTEN_PORT_PER_NODE = 3

MB = 1024 * 1024
GB = MB * 1024


def get_value(value, default):
    if value is None:
        return default
    return value


class YtConfig(object):
    def __init__(self,
                 fqdn=None,
                 yt_id=None,
                 yt_binaries_dir=None,
                 yt_work_dir=None,
                 proxy_port=None,
                 master_config=None,
                 node_config=None,
                 proxy_config=None,
                 scheduler_config=None,
                 controller_agent_config=None,
                 node_count=None,
                 save_runtime_data=None,
                 ram_drive_path=None,
                 local_cypress_dir=None,
                 wait_tablet_cell_initialization=None,
                 init_operations_archive=None,
                 job_controller_resource_limits=None,
                 forbid_chunk_storage_in_tmpfs=None,
                 node_chunk_store_quota=None,
                 cell_tag=None,
                 python_binary=None,
                 enable_debug_logging=None,
                 start_timeout=None):

        self.fqdn = get_value(fqdn, "localhost")
        self.yt_id = yt_id

        self.proxy_port = proxy_port
        self.node_count = node_count

        self.node_config = node_config
        self.scheduler_config = scheduler_config
        self.master_config = master_config
        self.proxy_config = proxy_config
        self.controller_agent_config = controller_agent_config

        self.yt_binaries_dir = yt_binaries_dir

        self.save_runtime_data = save_runtime_data
        self.yt_work_dir = yt_work_dir
        self.ram_drive_path = ram_drive_path
        self.local_cypress_dir = local_cypress_dir

        self.wait_tablet_cell_initialization = wait_tablet_cell_initialization
        self.init_operations_archive = init_operations_archive
        self.job_controller_resource_limits = job_controller_resource_limits
        self.forbid_chunk_storage_in_tmpfs = forbid_chunk_storage_in_tmpfs
        self.node_chunk_store_quota = node_chunk_store_quota

        self.enable_debug_logging = enable_debug_logging

        self.cell_tag = cell_tag
        self.python_binary = python_binary
        self.start_timeout = start_timeout

    def build_master_config(self):
        if self.master_config is not None:
            return copy.deepcopy(self.master_config)
        else:
            return {}

    def build_node_config(self):
        if self.node_config is not None:
            return copy.deepcopy(self.node_config)
        else:
            return {}

    def build_proxy_config(self):
        if self.proxy_config is not None:
            return copy.deepcopy(self.proxy_config)
        else:
            return {}

    def build_scheduler_config(self):
        if self.scheduler_config is not None:
            return copy.deepcopy(self.scheduler_config)
        else:
            return {}

    def build_controller_agent_config(self):
        if self.controller_agent_config is not None:
            conrtoller_agent_config = copy.deepcopy(self.controller_agent_config)
        else:
            conrtoller_agent_config = {}

        return update(
            conrtoller_agent_config,
            {
                "controller_agent": {
                    "environment": {x: os.environ[x] for x in _ADMISSIBLE_ENV_VARS if x in os.environ}
                }
            }
        )

    def build_job_controller_resource_limits(self):
        if self.job_controller_resource_limits is None:
            job_controller_resource_limits = {}
        else:
            job_controller_resource_limits = copy.copy(self.job_controller_resource_limits)

        if "memory" not in job_controller_resource_limits:
            job_controller_resource_limits["memory"] = 25 * GB

        return job_controller_resource_limits


class YtStuff(object):
    def __init__(self, config=None):
        self.config = config or YtConfig()

        self.python_binary = self.config.python_binary or yatest.common.python_path()

        self.yt_id = self.config.yt_id or str(uuid.uuid4())
        self.yt_proxy_port = None

        self._prepare_logger()
        self._prepare_files()
        self._prepare_env()
        self._import_wrapper()
        self._port_manager = yatest.common.network.PortManager()

    def _prepare_logger(self):
        self.logger = logging.getLogger()

    def _log(self, *args, **kwargs):
        self.logger.debug(*args, **kwargs)

    def _timing(method):
        @wraps(method)
        def wrap(self, *args, **kwargs):
            start_time = time.time()
            ret = method(self, *args, **kwargs)
            finish_time = time.time()
            self._log("%s time: %f", method.__name__, finish_time - start_time)
            return ret
        return wrap

    @_timing
    def _extract_tar(self, tgz, where):
        try:
            import subprocess
            subprocess.check_output(['tar', '-xf', tgz], cwd=where, stderr=subprocess.STDOUT)
        except OSError:
            import tarfile
            tarfile.open(tgz).extractall(path=where)

    def _prepare_files(self):
        work_path = yatest.common.runtime.work_path()

        # YT directory.
        self.binaries_yt_path = get_value(
            self.config.yt_binaries_dir,
            os.path.join(tempfile.mkdtemp(dir=work_path, prefix="yt_"), "bin"))

        # YT binaries.
        os.makedirs(self.binaries_yt_path)

        source_prefix = ""
        prepare_yt_binaries(self.binaries_yt_path, source_prefix, use_from_package=True)
        copy_misc_binaries(self.binaries_yt_path)

        self.yt_local_exec = [search_binary_path("yt_local")]

        user_yt_work_dir_base = self.config.yt_work_dir or yatest.common.get_param("yt_work_dir")
        if user_yt_work_dir_base:
            self.yt_work_dir = os.path.join(user_yt_work_dir_base, "yt_wd")
        else:
            ram_drive_path = os.environ.get("YA_TEST_OUTPUT_RAM_DRIVE_PATH")
            if ram_drive_path is None or self.config.ram_drive_path:
                self.yt_work_dir = yatest.common.output_path("yt_wd")
            else:
                self.yt_work_dir = os.path.join(yatest.common.output_ram_drive_path(), "yt_wd")

        self.tmpfs_path = self.config.ram_drive_path or yatest.common.get_param("ram_drive_path")
        if self.tmpfs_path:
            self.tmpfs_path = tempfile.mkdtemp(prefix="yt_", dir=self.tmpfs_path)

        if not os.path.isdir(self.yt_work_dir):
            os.mkdir(self.yt_work_dir)

        self.yt_local_dir = os.path.join(self.yt_work_dir, self.yt_id)

        self.yt_wrapper_log_path = os.path.join(self.yt_work_dir, "yt_wrapper_%s.log" % self.yt_id)

        # Create files for yt_local stdout/stderr. We can't just open them in 'w' mode, because
        # devtools.swag.daemon.run_daemon reads from them. So we create files and open them in 'r+' mode.
        yt_local_out_path = os.path.join(self.yt_work_dir, "yt_local_%s.out" % self.yt_id)
        yt_local_err_path = os.path.join(self.yt_work_dir, "yt_local_%s.err" % self.yt_id)
        open(yt_local_out_path, 'a').close()
        open(yt_local_err_path, 'a').close()
        self.yt_local_out = open(yt_local_out_path, "r+b")
        self.yt_local_err = open(yt_local_err_path, "r+b")
        self.is_running = False

    def _prepare_env(self):
        self.env = {}
        self.env["PATH"] = ":".join([
            self.binaries_yt_path,
        ])
        self.env["YT_ENABLE_VERBOSE_LOGGING"] = "1"
        self.env["YT_LOG_LEVEL"] = "DEBUG"
        self.env["RECEIVE_TOKEN_FROM_SSH_SESSION"] = "0"
        self.env.update({x: os.environ[x] for x in _ADMISSIBLE_ENV_VARS if x in os.environ})

    def _import_wrapper(self):
        import yt.wrapper
        import yt.logger

        yt.logger.LOGGER.setLevel(logging.DEBUG)
        handler = logging.FileHandler(self.yt_wrapper_log_path)
        handler.setFormatter(yt.logger.BASIC_FORMATTER)
        handler.setLevel(logging.DEBUG)
        yt.logger.LOGGER.handlers = [handler]

        self.yt_wrapper = yt.wrapper
        self.yt_wrapper.config["prefix"] = _YT_PREFIX
        self.yt_wrapper.config["pickling"]["python_binary"] = self.python_binary

        self.yt_client = yt.wrapper.YtClient()
        self.yt_client.config["prefix"] = _YT_PREFIX
        self.yt_client.config["pickling"]["python_binary"] = self.python_binary

    def _start_local_yt(self):
        self._log("Try to start local YT with id=%s", self.yt_id)
        try:
            # Prepare arguments.
            args = [
                "start",
                "--sync",
                "--id", self.yt_id,
                "--path", os.path.abspath(self.yt_work_dir),
                "--fqdn", self.config.fqdn,
                "--rpc-proxy-count", "1"
            ]

            if get_value(self.config.enable_debug_logging, True):
                args += ["--enable-debug-logging"]

            if self.config.wait_tablet_cell_initialization:
                args += ["--wait-tablet-cell-initialization"]
            if self.config.init_operations_archive:
                args += ["--init-operations-archive"]
            if self.config.forbid_chunk_storage_in_tmpfs:
                args += ["--forbid-chunk-storage-in-tmpfs"]
            if self.config.node_chunk_store_quota:
                args += ["--node-chunk-store-quota", str(self.config.node_chunk_store_quota)]

            if self.config.proxy_port is not None:
                self.yt_proxy_port = self.config.proxy_port
                args += ["--proxy-port", str(self.config.proxy_port)]

            if self.tmpfs_path is not None:
                args += ["--tmpfs-path", self.tmpfs_path]
            if self.config.node_count:
                args += ["--node-count", str(self.config.node_count)]
            if self.config.cell_tag is not None:
                args += ["--cell-tag", str(self.config.cell_tag)]

            if sys.version_info.major > 2:
                _dumps = lambda *a, **kw: yson.dumps(*a, **kw).decode("utf-8")
            else:
                _dumps = yson.dumps

            args += ["--master-config", _dumps(self.config.build_master_config(), yson_format="text")]
            args += ["--node-config", _dumps(self.config.build_node_config(), yson_format="text")]
            args += ["--scheduler-config", _dumps(self.config.build_scheduler_config(), yson_format="text")]
            args += ["--proxy-config", _dumps(self.config.build_proxy_config(), yson_format="text")]
            args += ["--controller-agent-config",
                     _dumps(self.config.build_controller_agent_config(), yson_format="text")]
            args += ["--jobs-resource-limits",
                     _dumps(self.config.build_job_controller_resource_limits(), yson_format="text")]

            local_cypress_dir = self.config.local_cypress_dir or yatest.common.get_param("yt_local_cypress_dir")
            if local_cypress_dir:
                args += ["--local-cypress-dir", local_cypress_dir]

            port_pool_size = _YT_LISTEN_PORT_POOL_SIZE
            if self.config.node_count:
                port_pool_size += _YT_LISTEN_PORT_PER_NODE * self.config.node_count
            args += ["--listen-port-pool"]
            for _ in range(port_pool_size):
                args += [str(self._port_manager.get_port())]

            cmd = [str(s) for s in self.yt_local_exec + list(args)]
            self._log(" ".join([os.path.basename(cmd[0])] + cmd[1:]))

            special_file = os.path.join(self.yt_local_dir, "started")

            if os.path.lexists(special_file):
                # It may be start after suspend
                os.remove(special_file)

            yt_daemon = devtools.swag.daemon.run_daemon(
                cmd,
                env=self.env,
                cwd=self.yt_work_dir,
                stdout=self.yt_local_out,
                stderr=self.yt_local_err,
            )
            # Wait until special file will appear. It means that yt_local had been started. See YT-4425 for details.
            # time amounts in seconds
            MAX_WAIT_TIME = self.config.start_timeout if self.config.start_timeout else 600
            SLEEP_TIME = 1
            if yatest.common.context.sanitize is not None:
                MAX_WAIT_TIME = MAX_WAIT_TIME * 3

            NUM_TRIES = int(MAX_WAIT_TIME / SLEEP_TIME)
            for i in range(NUM_TRIES):
                if os.path.lexists(special_file):
                    break
                if not yt_daemon.is_alive():
                    raise Exception("yt_local process failed.")
                time.sleep(SLEEP_TIME)
            else:
                self._log("Cannot find 'started' file for %d seconds.", MAX_WAIT_TIME)
                yt_daemon.stop()
                return False
            if self.config.proxy_port is None:
                info_yson_file = os.path.join(self.yt_local_dir, "info.yson")
                with open(info_yson_file, "rb") as f:
                    info = yson.load(f)
                self.yt_proxy_port = int(info["http_proxies"][0]["address"].split(":")[1])

            self.cluster_config = dict()
            with open(os.path.join(self.yt_local_dir, "configs", "master-0-0.yson"), "rb") as f:
                v = yson.load(f)
                for field in ["primary_master", "secondary_masters", "timestamp_provider", "transaction_manager"]:
                    if field in v:
                        self.cluster_config[field] = v[field]
            with open(os.path.join(self.yt_local_dir, "configs", "driver-0.yson"), "rb") as f:
                v = yson.load(f)
                for field in ["table_mount_cache", "cell_directory_synchronizer", "cluster_directory_synchronizer"]:
                    if field in v:
                        self.cluster_config[field] = v[field]
        except Exception:
            self.logger.exception("Failed to start local YT")
            for pid in self._get_pids():
                try:
                    os.kill(pid, signal.SIGKILL)
                except OSError:
                    pass
            return False
        self.yt_wrapper.config["proxy"]["url"] = self.get_server()
        self.yt_client.config["proxy"]["url"] = self.get_server()
        self.env["YT_PROXY"] = self.get_server()

        tmpdir = os.environ.get("TMPDIR")
        if tmpdir:
            self.yt_client.config["local_temp_directory"] = tmpdir
            self.yt_wrapper.config["local_temp_directory"] = tmpdir

        self._log("Local YT was started with id=%s", self.yt_id)
        return True

    def get_yt_client(self):
        return self.yt_client

    def get_server(self):
        return "localhost:%d" % self.yt_proxy_port

    def get_cluster_config(self):
        return self.cluster_config

    def get_env(self):
        return self.env

    @_timing
    def start_local_yt(self):
        max_retries = int(yatest.common.get_param(
            "yt_start_max_tries",
            default=os.environ.get("YT_STUFF_MAX_START_RETRIES", _YT_MAX_START_RETRIES)
        ))
        for i in range(max_retries):
            self._log("Start local YT, attempt %d.", i)
            if self._start_local_yt():
                self.is_running = True
                break
            else:
                if os.path.exists(self.yt_local_dir):
                    dir_i = i
                    while True:
                        failed_dirname = "%s_FAILED_try_%d" % (self.yt_local_dir, dir_i)
                        if os.path.exists(failed_dirname):
                            dir_i += 1
                        else:
                            os.rename(self.yt_local_dir, failed_dirname)
                            break
                if self.tmpfs_path:
                    for dir_ in os.listdir(self.tmpfs_path):
                        shutil.rmtree(os.path.join(self.tmpfs_path, dir_))
                MAX_WAIT_TIME = 60
                FAIL_PENALTY = 5
                time_to_sleep = min(i * FAIL_PENALTY, MAX_WAIT_TIME)
                time.sleep(time_to_sleep)
        else:
            self._cleanup_working_directory(save_runtime_data=True)
            raise Exception("Cannot start local YT with id %s for %d attempts." % (self.yt_id, max_retries))

    def suspend_local_yt(self):
        try:
            cmd = self.yt_local_exec + ["stop", self.yt_local_dir]
            self._log(" ".join([os.path.basename(cmd[0])] + cmd[1:]))
            yatest.common.process.execute(
                cmd,
                env=self.env,
                cwd=self.yt_work_dir,
                stdout=self.yt_local_out,
                stderr=self.yt_local_err,
            )
            self.is_running = False
            self._port_manager.release()
        except Exception as e:
            self._log("Errors while stopping local YT:\n%s", str(e))
            self._cleanup_working_directory(save_runtime_data=True)
            raise

    @_timing
    def stop_local_yt(self):
        if self.is_running:
            self.suspend_local_yt()
            with open(os.path.join(self.yt_local_dir, "lock_file")) as lock_file:
                fcntl.flock(lock_file, fcntl.LOCK_EX)
                fcntl.flock(lock_file, fcntl.LOCK_UN)

        self._cleanup_working_directory()

    @_timing
    def _cleanup_working_directory(self, save_runtime_data=False):
        self._log("Cleaning working directory %s", self.yt_work_dir)

        os.system("chmod -R 0775 " + self.yt_work_dir)

        save_runtime_data = save_runtime_data or \
            self.config.save_runtime_data or \
            yatest.common.get_param("yt_save_runtime_data")
        if not save_runtime_data:
            remove_runtime_data(self.yt_local_dir)

        collect_cores(
            self._get_pids(),
            self.yt_work_dir,
            [os.path.join(self.binaries_yt_path, binary) for binary in os.listdir(self.binaries_yt_path)],
            logger=self.logger)

    def _get_pids(self):
        pids_file = os.path.join(self.yt_local_dir, "pids.txt")
        pids = []
        if os.path.exists(pids_file):
            with open(pids_file) as f:
                for line in f.readlines():
                    try:
                        pids.append(int(line))
                    except ValueError:
                        pass
        return pids


def yt_stuff():
    raise Exception("This should not be called (or imported). Just remove this import?")
