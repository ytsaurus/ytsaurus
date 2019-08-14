from yt.common import update
from yt.environment.arcadia_interop import prepare_yt_binaries, collect_cores

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
import random
import string
import gzip
import contextlib
import copy

import yatest.common

import devtools.swag.daemon
import devtools.swag.ports

_ADMISSIBLE_ENV_VARS = [
    # Required for proper coverage.
    "LLVM_PROFILE_FILE",
    "PYTHON_COVERAGE_PREFIX",
]

_YT_PREFIX = "//"
_YT_MAX_START_RETRIES = 3

MB = 1024 * 1024
GB = MB * 1024

# See https://a.yandex-team.ru/arc/trunk/arcadia/devtools/ya/test/node/run_test.py?rev=2316309#L30
FILE_SIZE_LIMIT = 2 * MB


def get_value(value, default):
    if value is None:
        return default
    return value


@contextlib.contextmanager
def disable_gzip_crc_check():
    """
    Context manager that replaces gzip.GzipFile._read_eof with a no-op.

    This is useful when decompressing partial files, something that won't
    work if GzipFile does it's checksum comparison.

    """
    _read_eof = gzip.GzipFile._read_eof
    gzip.GzipFile._read_eof = lambda *args, **kwargs: None
    yield
    gzip.GzipFile._read_eof = _read_eof


class YtConfig(object):
    def __init__(self,
                 fqdn=None,
                 yt_id=None,
                 yt_path=None,
                 yt_work_dir=None,
                 keep_yt_work_dir=None,
                 proxy_port=None,
                 master_config=None,
                 node_config=None,
                 proxy_config=None,
                 scheduler_config=None,
                 controller_agent_config=None,
                 node_count=None,
                 save_all_logs=None,
                 ram_drive_path=None,
                 local_cypress_dir=None,
                 wait_tablet_cell_initialization=None,
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

        self.yt_path = yt_path

        self.save_all_logs = save_all_logs
        self.yt_work_dir = yt_work_dir
        self.keep_yt_work_dir = keep_yt_work_dir
        self.ram_drive_path = ram_drive_path
        self.local_cypress_dir = local_cypress_dir

        self.wait_tablet_cell_initialization = wait_tablet_cell_initialization
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

    @_timing
    def _split_file(self, file_path):
        def split_on_chunks(stream):
            size = 0
            chunk = []
            for obj in stream:
                if size + len(obj) > FILE_SIZE_LIMIT and size > 0:
                    yield chunk
                    chunk = []
                    size = 0
                chunk.append(obj)
                size += len(obj)
            if size > 0:
                yield chunk

        with disable_gzip_crc_check():
            if file_path.endswith(".gz"):
                file_obj = gzip.open(file_path)
                gzipped = True
            else:
                file_obj = open(file_path)
                gzipped = False

            for index, chunk in enumerate(split_on_chunks(file_obj)):
                new_file_path = file_path
                if gzipped:
                    new_file_path = new_file_path[:-3]
                new_file_path = new_file_path + ".split." + str(index + 1)
                if os.path.exists(new_file_path):
                    random_suffix = "".join(random.choice(string.ascii_uppercase + string.digits) for _ in range(5))
                    new_file_path = new_file_path + "." + random_suffix
                if gzipped:
                    new_file_path = new_file_path + ".gz"

                if gzipped:
                    fout = gzip.open(new_file_path, "w")
                else:
                    fout = open(new_file_path, "w")

                for line in chunk:
                    fout.write(line)

            fout.close()
            file_obj.close()

    def _prepare_files(self):
        # build_path = yatest.common.runtime.build_path()
        work_path = yatest.common.runtime.work_path()

        self.tmpfs_path = self.config.ram_drive_path or yatest.common.get_param("ram_drive_path")
        if self.tmpfs_path:
            self.tmpfs_path = tempfile.mkdtemp(prefix="yt_", dir=self.tmpfs_path)

        # YT directory.
        self.yt_path = tempfile.mkdtemp(dir=work_path, prefix="yt_") if self.config.yt_path is None else self.config.yt_path

        # YT binaries.
        self.yt_bins_path = os.path.join(self.yt_path, "bin")
        os.makedirs(self.yt_bins_path)

        source_prefix = ""
        yt_package_versions = os.listdir(yatest.common.build_path("yt/packages"))
        if len(yt_package_versions) > 1:
            raise RuntimeError("Test should specify not more than one version of YT package in DEPENDS")
        if len(yt_package_versions) == 1:
            source_prefix = "yt/packages/" + yt_package_versions[0] + "/"

        prepare_yt_binaries(self.yt_bins_path, source_prefix, use_ytserver_all=True)

        self.yt_local_exec = [yatest.common.binary_path(source_prefix + "yt/python/yt/local/bin/yt_local_make/yt_local")]

        user_yt_work_dir_base = self.config.yt_work_dir or yatest.common.get_param("yt_work_dir")
        if user_yt_work_dir_base:
            self.yt_work_dir = os.path.join(user_yt_work_dir_base, "yt_wd")
        else:
            self.yt_work_dir = yatest.common.output_path("yt_wd")

        if not os.path.isdir(self.yt_work_dir):
            os.mkdir(self.yt_work_dir)

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
            self.yt_bins_path,
        ])
        self.env["YT_ENABLE_VERBOSE_LOGGING"] = "1"
        self.env["YT_LOG_LEVEL"] = "DEBUG"
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
                "--rpc-proxy",
            ]

            if get_value(self.config.enable_debug_logging, True):
                args += ["--enable-debug-logging"]

            if self.config.wait_tablet_cell_initialization:
                args += ["--wait-tablet-cell-initialization"]
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
            args += ["--controller-agent-config", _dumps(self.config.build_controller_agent_config(), yson_format="text")]
            args += ["--jobs-resource-limits", _dumps(self.config.build_job_controller_resource_limits(), yson_format="text")]

            local_cypress_dir = self.config.local_cypress_dir or yatest.common.get_param("yt_local_cypress_dir")
            if local_cypress_dir:
                args += ["--local-cypress-dir", local_cypress_dir]

            cmd = [str(s) for s in self.yt_local_exec + list(args)]
            self._log(" ".join([os.path.basename(cmd[0])] + cmd[1:]))

            special_file = os.path.join(self.yt_work_dir, self.yt_id, "started")

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
                info_yson_file = os.path.join(self.yt_work_dir, self.yt_id, "info.yson")
                with open(info_yson_file, "rb") as f:
                    info = yson.load(f)
                self.yt_proxy_port = int(info["http_proxies"][0]["address"].split(":")[1])

            self.cluster_config = dict()
            with open(os.path.join(self.yt_work_dir, self.yt_id, "configs", "master-0-0.yson"), "rb") as f:
                v = yson.load(f)
                for field in ["primary_master", "secondary_masters", "timestamp_provider", "transaction_manager"]:
                    if field in v:
                        self.cluster_config[field] = v[field]
            with open(os.path.join(self.yt_work_dir, self.yt_id, "configs", "driver-0.yson"), "rb") as f:
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
                dirname = os.path.join(self.yt_work_dir, self.yt_id)
                if os.path.exists(dirname):
                    dir_i = i
                    while True:
                        failed_dirname = "%s_FAILED_try_%d" % (dirname, dir_i)
                        if os.path.exists(failed_dirname):
                            dir_i += 1
                        else:
                            os.rename(dirname, failed_dirname)
                            break
                if self.tmpfs_path:
                    for dir_ in os.listdir(self.tmpfs_path):
                        shutil.rmtree(os.path.join(self.tmpfs_path, dir_))
                MAX_WAIT_TIME = 60
                FAIL_PENALTY = 5
                time_to_sleep = min(i * FAIL_PENALTY, MAX_WAIT_TIME)
                time.sleep(time_to_sleep)
        else:
            self._save_logs(save_yt_all=True)
            raise Exception("Cannot start local YT with id %s for %d attempts." % (self.yt_id, max_retries))

    def suspend_local_yt(self):
        try:
            cmd = self.yt_local_exec + [
                "stop", os.path.join(self.yt_work_dir, self.yt_id),
            ]
            self._log(" ".join([os.path.basename(cmd[0])] + cmd[1:]))
            yatest.common.process.execute(
                cmd,
                env=self.env,
                cwd=self.yt_work_dir,
                stdout=self.yt_local_out,
                stderr=self.yt_local_err,
            )
            self.is_running = False
        except Exception as e:
            self._log("Errors while stopping local YT:\n%s", str(e))
            self._save_logs(save_yt_all=True)
            raise

    @_timing
    def stop_local_yt(self):
        if self.is_running:
            self.suspend_local_yt()
            with open(os.path.join(self.yt_work_dir, self.yt_id, "lock_file")) as lock_file:
                fcntl.flock(lock_file, fcntl.LOCK_EX)
                fcntl.flock(lock_file, fcntl.LOCK_UN)

        self._save_logs(save_yt_all=self.config.save_all_logs or yatest.common.get_param("yt_save_all_data"))

        if not self.config.keep_yt_work_dir:
            shutil.rmtree(self.yt_work_dir, ignore_errors=True)

    @_timing
    def _save_logs(self, save_yt_all=None):
        output_path = yatest.common.output_path()

        self._log("Logs saved in %s", output_path)

        common_interface_log = yatest.common.work_path("mr-client.log")
        if os.path.exists(common_interface_log):
            p = os.path.join(output_path, "mr-client.log")
            shutil.copyfile(common_interface_log, p)

        def _ignore(path, names):
            IGNORE_DIRS_ALWAYS = ["pipes"]
            IGNORE_DIRS = ["chunk_store", "chunk_cache", "changelogs", "snapshots"]
            ignored = set()
            for name in names:
                full_path = os.path.join(path, name)
                if os.path.islink(full_path):
                    ignored.add(name)
                elif os.path.isdir(full_path):
                    should_ignore = False
                    should_ignore |= name in IGNORE_DIRS_ALWAYS
                    should_ignore |= not save_yt_all and name in IGNORE_DIRS
                    if should_ignore:
                        ignored.add(name)
            return ignored

        yt_output_dir = os.path.join(output_path, "yt_logs_%s" % self.yt_id)
        shutil.copytree(src=self.yt_work_dir, dst=yt_output_dir, ignore=_ignore)
        os.system("chmod -R 0775 " + yt_output_dir)

        # Split huge files, because ya.test cuts them.
        for root, dirs, files in os.walk(yt_output_dir):
            for file in files:
                file_path = os.path.join(root, file)
                if os.path.getsize(file_path) >= FILE_SIZE_LIMIT:
                    if sys.version_info.major < 3:  # XXX
                        self._split_file(file_path)
                        os.remove(file_path)

        collect_cores(
            self._get_pids(),
            yt_output_dir,
            [os.path.join(self.yt_bins_path, binary) for binary in os.listdir(self.yt_bins_path)],
            logger=self.logger)

    def _get_pids(self):
        pids_file = os.path.join(self.yt_work_dir, self.yt_id, "pids.txt")
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
