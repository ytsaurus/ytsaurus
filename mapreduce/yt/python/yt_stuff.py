from functools import wraps
import logging
import os
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


import yatest.common

import yt.yson as yson

import devtools.swag.daemon
import devtools.swag.ports

_ADMISSIBLE_ENV_VARS = [
    # Required for proper coverage.
    "LLVM_PROFILE_FILE",
    "PYTHON_COVERAGE_PREFIX",
]

_YT_PREFIX = "//"
_YT_MAX_START_RETRIES = 3

# See https://a.yandex-team.ru/arc/trunk/arcadia/devtools/ya/test/node/run_test.py?rev=2316309#L30
FILE_SIZE_LIMIT = 2 * 1024 * 1024


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
                 jobs_memory_limit=None,
                 jobs_cpu_limit=None,
                 jobs_user_slot_count=None,
                 forbid_chunk_storage_in_tmpfs=None,
                 node_chunk_store_quota=None,
                 yt_version=None,
                 cell_tag=None,
                 python_binary=None,
                 enable_debug_logging=None,
                 enable_rpc_proxy=None):

        self.fqdn = get_value(fqdn, "localhost")
        self.yt_id = yt_id

        self.proxy_port = proxy_port
        self.node_count = node_count

        self.node_config = node_config
        self.scheduler_config = scheduler_config
        self.master_config = master_config
        self.proxy_config = proxy_config

        controller_config = {
            "controller_agent": {
                "environment": {x: os.environ[x] for x in _ADMISSIBLE_ENV_VARS if x in os.environ}
            }
        }

        with tempfile.NamedTemporaryFile(delete=False) as controller_agent_config_with_default_env:
            yson.dump(controller_config, controller_agent_config_with_default_env)

        self.controller_agent_config = get_value(controller_agent_config, controller_agent_config_with_default_env.name)

        self.yt_path = yt_path

        self.save_all_logs = save_all_logs
        self.yt_work_dir = yt_work_dir
        self.keep_yt_work_dir = keep_yt_work_dir
        self.ram_drive_path = ram_drive_path
        self.local_cypress_dir = local_cypress_dir

        self.wait_tablet_cell_initialization = wait_tablet_cell_initialization
        self.jobs_memory_limit = get_value(jobs_memory_limit, 25 * 1024 ** 3)
        self.jobs_cpu_limit = jobs_cpu_limit
        self.jobs_user_slot_count = jobs_user_slot_count
        self.forbid_chunk_storage_in_tmpfs = forbid_chunk_storage_in_tmpfs
        self.node_chunk_store_quota = node_chunk_store_quota

        self.enable_debug_logging = enable_debug_logging
        self.enable_rpc_proxy = enable_rpc_proxy

        yt_package_versions = os.listdir(yatest.common.build_path("yt/packages"))
        if len(yt_package_versions) != 1:
            raise RuntimeError("Test should specify exactly one version of YT package in DEPENDS")

        if yt_version is None:
            self.yt_version = yt_package_versions[-1]
        else:
            if yt_version not in yt_package_versions:
                raise RuntimeError("YT package version mismatch (version in DEPENDS: {0}, version in YtConfig: {1}) "
                                   .format(yt_package_versions[-1], yt_version))
            self.yt_version = yt_version

        self.cell_tag = cell_tag
        self.python_binary = python_binary


class YtStuff(object):
    def __init__(self, config=None):
        self.config = config or YtConfig()
        self.version = self.config.yt_version

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
                new_file_path = new_file_path + "." + str(index + 1)
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

        # Folders
        self.yt_path = tempfile.mkdtemp(dir=work_path, prefix="yt_") if self.config.yt_path is None else self.config.yt_path
        self.yt_bins_path = os.path.join(self.yt_path, "bin")
        if self.version != "19_4":
            self.yt_node_path = os.path.join(self.yt_path, "node")
            self.yt_node_bin_path = os.path.join(self.yt_node_path, "bin")
            self.yt_node_modules_path = os.path.join(self.yt_path, "node_modules")
        self.yt_thor_path = os.path.join(self.yt_path, "yt-thor")
        # Binaries
        self.yt_local_path = [self.python_binary, os.path.join(self.yt_bins_path, "yt_local")]

        yt_archive_path = yatest.common.binary_path('yt/packages/{0}/yt/packages/{0}/yt_thor.tar'.format(self.version))
        self._extract_tar(yt_archive_path, self.yt_path)

        self._replace_binaries()

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
        self.yt_local_out = open(yt_local_out_path, "r+")
        self.yt_local_err = open(yt_local_err_path, "r+")
        self.is_running = False

    def _replace_binaries(self):
        paths = [self.yt_bins_path]
        if self.version != "19_4":
            paths.append(self.yt_node_bin_path)
        for path in paths:
            if not os.path.exists(path):
                os.makedirs(path)

        self.yt_local_path = [yatest.common.binary_path('yt/packages/{}/contrib/python/yt_local/bin/local/yt_local'.format(self.version))]
        self.yt_env_watcher_dir_path = yatest.common.binary_path('yt/packages/{}/contrib/python/yt_local/bin/watcher'.format(self.version))

        programs = [('master', 'cell_master_program'),
                    ('node', 'cell_node_program'),
                    ('job-proxy', 'job_proxy_program'),
                    ('exec', 'exec_program'),
                    ('proxy', 'cell_proxy_program'),
                    ('tools', 'tools_program'),
                    ('scheduler', 'programs/scheduler'),
                    ('controller-agent', 'programs/controller_agent')]

        if self.version != "19_4":
            for binary, server_dir in programs:
                binary_path = yatest.common.binary_path('yt/packages/{0}/yt/{0}/yt/server/{1}/ytserver-{2}'
                                                        .format(self.version, server_dir, binary))
                os.symlink(binary_path, os.path.join(self.yt_bins_path, 'ytserver-' + binary))
        else:
            programs = [('master', 'cell_master/bin'),
                        ('node', 'cell_node/bin'),
                        ('job-proxy', 'job_proxy/bin'),
                        ('exec', 'bin/exec'),
                        ('proxy', 'rpc_proxy/bin'),
                        ('http-proxy', 'http_proxy/bin'),
                        ('tools', 'bin/tools'),
                        ('scheduler', 'scheduler/bin'),
                        ('controller-agent', 'controller_agent/bin')]
            for binary, server_dir in programs:
                binary_path = yatest.common.binary_path('yt/packages/{0}/yt/{0}/yt/server/{1}/ytserver-{2}'
                                                        .format(self.version, server_dir, binary))
                os.symlink(binary_path, os.path.join(self.yt_bins_path, 'ytserver-' + binary))

        if self.version != "19_4":
            yt_node_arcadia_path = yatest.common.binary_path('yt/packages/{0}/yt/{0}/yt/nodejs/targets/bin/ytnode'.format(self.version))
            os.symlink(yt_node_arcadia_path, os.path.join(self.yt_node_bin_path, 'nodejs'))

            node_modules_archive_path = yatest.common.binary_path('yt/packages/{0}/yt/{0}/yt/node_modules/resource.tar.gz'.format(self.version))
            self._extract_tar(node_modules_archive_path, self.yt_path)

            yt_node_path = yatest.common.binary_path('yt/packages/{0}/yt/{0}/yt/nodejs/targets/package'.format(self.version))
            os.symlink(yt_node_path, os.path.join(self.yt_node_modules_path, 'yt'))

    def _prepare_env(self):
        self.env = {}
        self.env["PATH"] = ":".join([
            "/usr/sbin",  # It is required to locate logrotate in watcher process
            self.yt_bins_path,
            self.yt_env_watcher_dir_path,
        ])
        if self.version != "19_4":
            self.env["NODE_MODULES"] = self.yt_node_modules_path
            self.env["NODE_PATH"] = ":".join([
                self.yt_node_path,
                self.yt_node_modules_path,
            ])
            self.env["PATH"] += ":" + ":".join([
                self.yt_node_path,
                self.yt_node_bin_path
            ])
        self.env["YT_LOCAL_THOR_PATH"] = self.yt_thor_path
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
                "--path", self.yt_work_dir,
                "--fqdn", self.config.fqdn,
                "--jobs-memory-limit", str(self.config.jobs_memory_limit),
            ]

            if get_value(self.config.enable_debug_logging, True):
                args += ["--enable-debug-logging"]

            if get_value(self.config.enable_rpc_proxy, True):
                args += ["--rpc-proxy"]

            if self.config.jobs_cpu_limit:
                args += ["--jobs-cpu-limit", str(self.config.jobs_cpu_limit)]
            if self.config.jobs_user_slot_count:
                args += ["--jobs-user-slot-count", str(self.config.jobs_user_slot_count)]

            if self.config.wait_tablet_cell_initialization:
                args += ["--wait-tablet-cell-initialization"]
            if self.config.forbid_chunk_storage_in_tmpfs:
                args += ["--forbid-chunk-storage-in-tmpfs"]
            if self.config.node_chunk_store_quota:
                args += ["--node-chunk-store-quota", str(self.config.node_chunk_store_quota)]

            if self.config.proxy_port is not None:
                self.yt_proxy_port = self.config.proxy_port
                args += ["--proxy-port", str(self.config.proxy_port)]

            if self.version == '19_4':
                args += ["--use-new-proxy"]

            if self.tmpfs_path:
                args += ["--tmpfs-path", self.tmpfs_path]
            if self.config.node_config:
                args += ["--node-config", self.config.node_config]
            if self.config.node_count:
                args += ["--node-count", str(self.config.node_count)]
            if self.config.scheduler_config:
                args += ["--scheduler-config", self.config.scheduler_config]
            if self.config.proxy_config:
                args += ["--proxy-config", self.config.proxy_config]
            if self.config.cell_tag is not None:
                args += ["--cell-tag", str(self.config.cell_tag)]
            if self.config.controller_agent_config:
                args += ["--controller-agent-config", self.config.controller_agent_config]

            local_cypress_dir = self.config.local_cypress_dir or yatest.common.get_param("yt_local_cypress_dir")
            if local_cypress_dir:
                args += ["--local-cypress-dir", local_cypress_dir]

            cmd = self.yt_local_path + list(args)
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
            MAX_WAIT_TIME, SLEEP_TIME = 600, 0.1  # in seconds
            if yatest.common.context.sanitize is not None:
                MAX_WAIT_TIME = MAX_WAIT_TIME * 3

            NUM_TRIES = int(MAX_WAIT_TIME / SLEEP_TIME)
            for i in xrange(NUM_TRIES):
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
                with open(info_yson_file) as f:
                    info = yson.load(f)
                self.yt_proxy_port = int(info["proxy"]["address"].split(":")[1])

            self.cluster_config = dict()
            with open(os.path.join(self.yt_work_dir, self.yt_id, "configs", "master-0-0.yson")) as f:
                v = yson.load(f)
                for field in ["primary_master", "secondary_masters", "timestamp_provider", "transaction_manager"]:
                    if field in v:
                        self.cluster_config[field] = v[field]
            with open(os.path.join(self.yt_work_dir, self.yt_id, "configs", "driver-0.yson")) as f:
                v = yson.load(f)
                for field in ["table_mount_cache", "cell_directory_synchronizer", "cluster_directory_synchronizer"]:
                    if field in v:
                        self.cluster_config[field] = v[field]
        except Exception, e:
            self._log("Failed to start local YT:\n%s", str(e))
            for pid in self._get_pids():
                try:
                    os.kill(pid, signal.SIGKILL)
                except OSError:
                    pass
            return False
        self.yt_wrapper.config["proxy"]["url"] = self.get_server()
        self.yt_wrapper.config["proxy"]["enable_proxy_discovery"] = False
        self.yt_client.config["proxy"]["url"] = self.get_server()
        self.yt_client.config["proxy"]["enable_proxy_discovery"] = False
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
        for i in xrange(max_retries):
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
            cmd = self.yt_local_path + [
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
        except Exception, e:
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
            IGNORE_DIRS_ALWAYS = ["ui", "pipes", "node_modules"]
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
                    self._split_file(file_path)
                    os.remove(file_path)

        cores_dir = os.path.join(yt_output_dir, "cores")
        if not os.path.isdir(cores_dir):
            os.mkdir(cores_dir)

        for pid in self._get_pids():
            core_file = yatest.common.cores.recover_core_dump_file(
                os.path.join(self.yt_bins_path, "ytserver"),
                self.yt_work_dir,
                pid
            )
            if core_file:
                shutil.copy(core_file, cores_dir)

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
