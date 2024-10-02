from __future__ import print_function
from . import local_ssl_keys

from yt.orm.admin.db_operations import Migrator

from yt.orm.yt_manager.local import LocalYTManager

from yt.orm.library.retries import get_disabled_retries_config
from yt.orm.library.monitoring_client import OrmMonitoringClient

from yt.orm.library.common import (
    ClientError,
    YtResponseError,
    get_retriable_errors,
    try_read_from_file,
)

from yt.environment.configs_provider import init_logging
from yt.environment.helpers import OpenPortIterator
from yt.environment.watcher import ProcessWatcher
from yt.wrapper.common import generate_uuid

import yt.yson as yson

import yt.logger as logger

from yt.common import (
    get_value,
    makedirp,
    update,
    which,
)

try:
    from yt.packages.six import (
        iteritems,
        reraise,
    )
    from yt.packages.six.moves import xrange
except ImportError:
    from six import iteritems, reraise
    from six.moves import xrange

import yt.packages.requests as requests

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

from abc import ABCMeta, abstractmethod

from copy import deepcopy
import json
import os
import signal
import subprocess
import sys
import time

if yatest_common is not None:
    from yt.environment.arcadia_interop import collect_cores


DEFAULT_YT_OPTIONS = {
    "fqdn": "localhost",
    "enable_debug_logging": True,
    "node_config": {
        "tablet_node": {
            "changelogs": {
                "writer": {
                    # Usually we work with one node and do not want to ban it for too long.
                    "node_ban_timeout": 1000,
                },
            },
            "in_memory_manager": {
                "max_concurrent_preloads": 128,
            },
            "store_flusher": {
                "max_concurrent_flushes": 128,
            },
            "resource_limits": {
                "tablet_static_memory": 100 * 1024 * 1024,
                "master_memory": 1000000,
            },
            "tablet_manager": {
                "compaction_backoff_time": 1000,
                "preload_backoff_time": 1000,
                "flush_backoff_time": 1000,
            },
        },
    },
    "driver_config": {
        # Expiration duration after failed update is equal to refresh time consciously, because
        # for now async expiring cache disables refresh for erroneous entries,
        # so an erroneous entry will be held in a cache for the whole expiration duration.
        "table_mount_cache": {
            "expire_after_successful_update_time": 10000,
            "expire_after_failed_update_time": 100,
            "expire_after_access_time": 10000,
            "refresh_time": 100,
        },
        "permission_cache": {
            "expire_after_successful_update_time": 10000,
            "expire_after_failed_update_time": 1000,
            "expire_after_access_time": 10000,
            "refresh_time": 1000,
        },
    },
}

CONFIG_TEMPLATE = {
    "address_resolver": {"localhost_fqdn": "localhost"},
    "client_grpc_server": {"addresses": []},
    "monitoring_server": {},
    "solomon_exporter": {"enable_core_profiling_compatibility": True},
    "internal_bus_server": {},
    "internal_rpc_server": {},
    "client_http_server": {},
    "authentication_manager": {"require_authentication": False},
    "access_control_manager": {
        "cluster_state_update_period": 100,
        "preload_acl_for_superusers": True,
    },
    "object_manager": {"attributes_extensibility_mode": "none"},
    "logging": {},
    "dynamic_config_manager": {
        "update_period": 1000,
        "ignore_config_absence": True,
    },
    "object_service": {
        # TODO: remove enable_request_cancelation after it becomes on by default.
        # Note that it will remain off in old master binaries used in migration tests.
        "enable_request_cancelation": True,
        "enable_mutating_request_cancelation": True,
    },
}


def _create_medium(client, name):
    try:
        client.create("domestic_medium", attributes=dict(name=name))
    except YtResponseError as err:
        if not err.contains_text("Error parsing"):
            raise
        # COMPAT(dgolear): Drop after 23.1
        client.create("medium", attributes=dict(name=name))


def _try_set_environ_without_override(name, value):
    if os.environ.get(name, value) != value:
        return False
    os.environ[name] = value
    return True


def are_sanitizers_enabled():
    return yatest_common is not None and bool(yatest_common.context.sanitize)


def _check_process_alive(proc):
    return proc is not None and proc.poll() is not None


def _validate_process_alive(proc, stderr_file_name=None):
    if _check_process_alive(proc):
        error_message = None
        if stderr_file_name and os.path.exists(stderr_file_name):
            with open(stderr_file_name, "r") as stderr_file:
                error_message = [Exception(stderr_file.read())]
        raise ClientError(
            "Process unexpectedly terminated with error code {0}".format(proc.returncode),
            inner_errors=error_message,
        )


def _announce_addresses(instance, config):
    instance.orm_client_grpc_address = config["client_grpc_server"]["addresses"][0]["address"]
    instance.orm_client_grpc_port = int(instance.orm_client_grpc_address.split(":")[-1])
    if "client_http_server" in config:
        instance.orm_client_http_port = config["client_http_server"]["port"]
        instance.orm_client_http_address = "localhost:" + str(instance.orm_client_http_port)

    if instance.is_ssl_enabled():
        addresses = config["secure_client_grpc_server"]["addresses"]
        instance.orm_client_secure_grpc_address = addresses[0]["address"]
        instance.orm_client_secure_grpc_port = int(instance.orm_client_secure_grpc_address.split(":")[1])
        if "secure_client_http_server" in config:
            instance.orm_client_secure_http_port = config["secure_client_http_server"]["port"]
            instance.orm_client_secure_http_address = "localhost:" + str(instance.orm_client_secure_http_port)


class OrmMaster(object):
    def __init__(
        self,
        tag,
        path,
        make_client,
        enable_ssl,
        snake_case_name,
        master_config=None,
    ):
        self._path = path
        self._tag = tag
        self._config = master_config
        self._debug_wrapper = os.environ.get("ORM_MASTER_DEBUG_WRAPPER", "").split()
        self._debug_extend_timeouts = self._debug_wrapper != []
        self._snake_case_name = snake_case_name
        self._make_client = make_client
        self._is_ssl_enabled = enable_ssl
        self._binary_path = None
        self._directory = None
        self._process = None
        self._watcher = None
        self.config = None
        self.config_path = None
        self._master_log_file = None
        self._master_stderr_file = None

    def get_tag(self):
        return self._tag

    def get_directory(self):
        return self._directory

    def get_process(self):
        return self._process

    def get_binary_path(self):
        return self._binary_path

    def is_alive(self):
        return self.get_process() is not None

    def is_ssl_enabled(self):
        return self._is_ssl_enabled

    def create_monitoring_client(self):
        return OrmMonitoringClient("localhost:{}".format(self.config["monitoring_server"]["port"]))

    def create_client(self, config=None, transport="grpc", **kwargs):
        config = update({"request_timeout": 10000, "enable_ssl": False}, get_value(config, {}))
        if are_sanitizers_enabled():
            config["request_timeout"] = config["request_timeout"] * 5

        address = None
        if transport == "grpc":
            if config.get("enable_ssl", False):
                address = self.orm_client_secure_grpc_address
            else:
                address = self.orm_client_grpc_address
        elif transport == "http":
            if config.get("enable_ssl", False):
                address = self.orm_client_secure_http_address
            else:
                address = self.orm_client_http_address
        else:
            raise ClientError(
                "Cannot create {} client: unknown transport '{}'".format(
                    self._data_model_traits.get_human_readable_name(), transport
                )
            )

        if address is None:
            raise ClientError(
                "Cannot determine {} master address".format(self._data_model_traits.get_human_readable_name())
            )

        return self._make_client(address, config, transport, **kwargs)

    def _make_master_directory(self):
        def get_path(index):
            return os.path.abspath(
                os.path.join(
                    self._path,
                    "{}_master_tag{}_{}".format(self._snake_case_name, self._tag, index),
                )
            )

        index = 0
        while os.path.exists(get_path(index)):
            index += 1
        self._directory = get_path(index)
        makedirp(self._directory)

    # orm-local CLI uses disk to pass meta from command to command.
    def _dump_watcher_process_meta(self):
        assert self._watcher is not None
        meta = dict(pid=self._watcher.get_pid())
        with open(os.path.join(self._path, "watcher_tag_{}.meta".format(self._tag)), "w") as fout:
            json.dump(meta, fout, indent=4)

    def _try_load_watcher_process_meta(self):
        path = os.path.join(self._path, "watcher_tag_{}.meta".format(self._tag))
        if not os.path.exists(path):
            return None
        with open(path, "r") as fin:
            return json.load(fin)

    def _create_master_process_meta(self):
        assert self.get_process() is not None
        assert self.get_binary_path() is not None
        assert self.get_directory() is not None
        return dict(
            pid=self.get_process().pid,
            binary_path=os.path.abspath(self.get_binary_path()),
            directory=os.path.abspath(self.get_directory()),
        )

    def _get_master_process_meta_path(self):
        return os.path.join(self._path, "{}_master_tag_{}.meta".format(self._snake_case_name, self._tag))

    # orm-local CLI uses disk to pass meta from command to command.
    def _dump_master_process_meta(self):
        meta = self._create_master_process_meta()
        with open(self._get_master_process_meta_path(), "w") as fout:
            json.dump(meta, fout, indent=4)

    def _try_load_master_process_meta(self):
        path = self._get_master_process_meta_path()
        if not os.path.exists(path):
            return None
        with open(path, "r") as fin:
            return json.load(fin)

    def _wait_for_alive_master(self, data_model_traits):
        alive = False
        last_error = None
        monitoring_client = self.create_monitoring_client()
        wait_iterations = 10 if not self._debug_extend_timeouts else 1000
        if are_sanitizers_enabled():
            wait_iterations = wait_iterations + 5
        with self.create_client(config=dict(retries=get_disabled_retries_config())) as client:
            for i in xrange(wait_iterations):
                try:
                    monitoring_client.get("/health_check", service="")
                    client.get_masters()
                    alive = True
                    break
                except (
                    YtResponseError,
                    requests.exceptions.HTTPError,
                ) + get_retriable_errors() as error:
                    last_error = error
                    _validate_process_alive(self.get_process(), self._master_stderr_file)
                time.sleep(0.1 + i)
        if not alive:
            assert last_error is not None
            raise ClientError(
                "Local {} master with tag={} has failed to start, see {} for details".format(
                    data_model_traits.get_human_readable_name(),
                    self._tag,
                    self._master_log_file or "master log file",
                ),
                inner_errors=[last_error],
            )

    def start_watcher(self):
        # Watcher is used to rotate master logs. It is necessary to save disk space and
        # keep log files small for distbuild.
        log_paths = sorted(x["file_name"] for x in self.config["logging"]["writers"].values())

        self._watcher = ProcessWatcher(
            # Binary should be available in PATH
            watcher_binary=None,
            process_pids=[self._process.pid],
            process_log_paths=log_paths,
            lock_path=os.path.join(self._path, "lock_file"),
            config_dir=self._directory,
            logs_dir=self._directory,
            runtime_dir=os.path.join(self._directory, "logs_rotator_data"),
        )

        self._watcher.start()

        self._dump_watcher_process_meta()

    def stop_watcher(self):
        if self._watcher is not None:
            self._watcher.stop()
            self._watcher = None
        else:
            meta = self._try_load_watcher_process_meta()
            if meta is not None:
                self._try_kill("watcher", meta["pid"])
            else:
                logger.warning("Could not stop watcher instance: unknown meta")

    def prepare_master_start(self, generate_config):
        self._make_master_directory()

        self.config = generate_config(self._tag)
        if self._config is not None:
            self.config = update(self.config, self._config)

        self.config_path = os.path.join(self._directory, "config.yson")
        with open(self.config_path, "wb") as fout:
            yson.dump(self.config, fout, yson_format="pretty")

        self._master_log_file = self.config.get("logging", {}).get("writers", {}).get("info", {}).get("file_name", None)

        _announce_addresses(self, self.config)

    def start(self, master_binary_path, data_model_traits):
        self._binary_path = master_binary_path

        logger.info("Starting %s master", data_model_traits.get_human_readable_name())

        self._master_stderr_file = os.path.join(
            self._directory,
            "stderr.{}_master".format(data_model_traits.get_snake_case_name()),
        )
        stderr = open(self._master_stderr_file, "w")

        status = os.stat(self._binary_path)
        logger.info("Binary %s has permissions %s", self._binary_path, oct(status.st_mode & 0o777))

        self._process = subprocess.Popen(
            self._debug_wrapper + [self._binary_path, "--config", self.config_path],
            cwd=self._directory,
            stdout=open(os.devnull, "w"),
            stderr=stderr,
        )

        self._dump_master_process_meta()

        time.sleep(0.5)
        _validate_process_alive(self.get_process(), self._master_stderr_file)

        self.start_watcher()

        self._wait_for_alive_master(data_model_traits)

        logger.info("%s master started successfully", data_model_traits.get_human_readable_name())

    def _try_kill(self, name, pid):
        try:
            logger.info("Killing {} instance with pid {}".format(name, pid))
            os.kill(pid, 9)
        except OSError:
            logger.exception(
                "Failed to kill {} instance with pid {}".format(
                    name,
                    pid,
                )
            )

    def stop(self, data_model_traits):
        """Stops master."""

        self.stop_watcher()

        meta = None
        if self._process is not None:
            meta = self._create_master_process_meta()
        else:
            meta = self._try_load_master_process_meta()

        if meta is None:
            logger.warning(
                "Could not stop %s master instance: unknown meta",
                data_model_traits.get_human_readable_name(),
            )
        else:
            self._try_kill("{} master".format(data_model_traits.get_human_readable_name()), meta["pid"])
            if yatest_common is not None:
                found_cores = collect_cores(
                    [meta["pid"]],
                    meta["directory"],
                    [os.path.abspath(meta["binary_path"])],
                    logger=logger,
                )
                assert not found_cores

        self._process = None
        self._directory = None
        self._binary_path = None

        self.config = None
        self.config_path = None


class OrmInstance(object):
    __metaclass__ = ABCMeta

    def __init__(
        self,
        path,
        data_model_traits,
        db_version,
        init_yt_cluster,
        master_config=None,
        local_yt_options=None,
        enable_ssl=None,
        port_locks_path=None,
        enable_debug_logging=True,
        require_authentication=True,
        use_rpc_proxy_for_discovery=True,
        replica_instance_count=0,
        yt_manager=None,
        avoid_migrations=True,
        master_count=1,
    ):
        self.path = path
        self._data_model_traits = data_model_traits
        self._local_yt_options = update(DEFAULT_YT_OPTIONS, get_value(local_yt_options, {}))
        self._masters = [OrmMaster(
            tag,
            path,
            self.make_client,
            get_value(enable_ssl, False),
            data_model_traits.get_snake_case_name(),
            master_config=master_config) for tag in range(master_count)]
        self._used_media = set(self._data_model_traits.get_used_media()).union({"default"})
        self._local_yt_options["store_location_count"] = len(self._used_media)
        if use_rpc_proxy_for_discovery or require_authentication:
            if not self._local_yt_options.get("rpc_proxy_count"):
                self._local_yt_options["rpc_proxy_count"] = 1

            # Http proxy required in get_proxy_address().
            if not self._local_yt_options.get("http_proxy_count"):
                self._local_yt_options["http_proxy_count"] = 1

        self._replica_instance_count = replica_instance_count

        self._yt_manager = yt_manager
        self._yt_manager_owned = False
        if self._yt_manager is None:
            self._yt_manager = LocalYTManager(self.path)
            self._yt_manager_owned = True
        self._yt_package_dir = None

        self._init_yt_cluster = init_yt_cluster
        self._avoid_migrations = avoid_migrations
        self._migration_yt_client = None

        self._prepared = False
        self._enable_ssl = enable_ssl
        self._enable_debug_logging = enable_debug_logging
        assert db_version is not None
        self._set_db_version(db_version)

        self._port_locks_path = port_locks_path
        if self._port_locks_path is not None:
            if not _try_set_environ_without_override("YT_LOCAL_PORT_LOCKS_PATH", self._port_locks_path):
                raise ClientError(
                    "Could not prepare environment for YT local start: "
                    "YT_LOCAL_PORT_LOCKS_PATH environment variable is already set "
                    "to a different value"
                )

        self._require_authentication = require_authentication
        self._use_rpc_proxy_for_discovery = use_rpc_proxy_for_discovery

    def get_working_directory(self, index=0):
        return self._master[index].get_directory()

    def is_ssl_enabled(self):
        return get_value(self._enable_ssl, False)

    def allocate_port(self):
        return next(self._port_generator)

    @abstractmethod
    def get_arcadia_master_binary_path(self):
        raise NotImplementedError

    @abstractmethod
    def make_client(self, address, config, transport, **kwargs):
        raise NotImplementedError

    def get_config_template(self):
        config = deepcopy(CONFIG_TEMPLATE)
        config = update(
            config,
            dict(
                yt_connector=dict(
                    root_path=self.get_db_path(),
                    user=self._data_model_traits.get_user_name(),
                )
            ),
        )
        return config

    def generate_config(self, master_index=0):
        default_orm_client_grpc_port = self.allocate_port()
        default_orm_client_grpc_address = "localhost:" + str(default_orm_client_grpc_port)

        config = self.get_config_template()
        config["monitoring_server"]["port"] = self.allocate_port()
        config["internal_bus_server"]["port"] = self.allocate_port()

        config["client_http_server"]["port"] = self.allocate_port()
        config["client_grpc_server"]["addresses"].append({"address": default_orm_client_grpc_address})

        if len(self._masters) > 1:
            config["fqdn_override"] = "localhost_tag_{}".format(master_index)

        if self.is_ssl_enabled():
            default_orm_client_secure_grpc_port = self.allocate_port()
            default_orm_client_secure_grpc_address = "localhost:" + str(default_orm_client_secure_grpc_port)
            config["secure_client_grpc_server"] = {
                "addresses": [
                    {
                        "address": default_orm_client_secure_grpc_address,
                        "credentials": {
                            "client_certificate_request": "dont_request_client_certificate",
                            "pem_key_cert_pairs": [
                                {
                                    "cert_chain": {"value": local_ssl_keys.server_cert},
                                    "private_key": {"value": local_ssl_keys.server_key},
                                },
                            ],
                            "pem_root_certs": {"value": local_ssl_keys.root_cert},
                        },
                    }
                ]
            }

            config["secure_client_http_server"] = {
                "credentials": {
                    "cert_chain": {"value": local_ssl_keys.server_cert},
                    "private_key": {"value": local_ssl_keys.server_key},
                },
                "port": self.allocate_port(),
            }
        if self._require_authentication:
            config["yt_connector"]["connection_cluster_url"] = self.yt_instance.get_proxy_address()
        if not self._use_rpc_proxy_for_discovery:
            config["yt_connector"]["connection"] = update(
                config["yt_connector"].setdefault("connection", {}), self.yt_instance.configs["driver"]
            )
        config["yt_connector"]["cluster_name"] = "{}-local".format(self._data_model_traits.get_kebab_case_name())
        config["yt_connector"]["cluster_tag"] = 0
        config["yt_connector"]["instance_tag"] = 0

        config["logging"] = init_logging(
            path=self._masters[master_index].get_directory(),
            name="{}server_master".format(self._data_model_traits.get_snake_case_name()),
            log_errors_to_stderr=False,
            enable_debug_logging=self._enable_debug_logging,
            enable_log_compression=True,
            log_compression_method="zstd",
            abort_on_alert=False,
        )

        return config

    # Dynamic config patching via #local_yt_options is not supported yet,
    # so we perform it explicitly.
    def _patch_yt_master_dynamic_config(self):
        yt_client = self.create_yt_client()
        # Process requests to master every 1 ms instead of default 10 ms not to pose
        # too big artificial delays in tests execution.
        yt_client.set("//sys/@config/object_service/process_sessions_period", 1, recursive=True)

    def _patch_queue_agent_dynamic_config(self):
        yt_client = self.create_yt_client()
        cluster_name = yt_client.get("//sys/@cluster_connection/cluster_name")
        yt_client.set(
            "//sys/queue_agents/config",
            {
                "queue_agent_sharding_manager": {
                    "pass_period": 100,
                },
                "queue_agent": {
                    "pass_period": 100,
                    "controller": {"enable_automatic_trimming": True, "pass_period": 100},
                },
                "cypress_synchronizer": {
                    "policy": "watching",
                    "clusters": [cluster_name],
                    "pass_period": 100,
                },
            },
        )

    def _configure_media(self, yt_client):
        media = set(yt_client.list("//sys/media"))
        locations = yt_client.list("//sys/chunk_locations", absolute=True)
        assert len(locations) >= len(self._used_media)

        for location, medium in zip(locations, self._used_media):
            if medium not in media:
                _create_medium(yt_client, medium)
            if medium != "default":
                yt_client.set(location + "/@medium_override", medium)

    def prepare(self):
        self._yts = []
        for index in range(self._replica_instance_count + 1):
            local_yt = self._yt_manager.start_or_find_started_yt(
                self._local_yt_options,
                package_dir=self._yt_package_dir,
                is_replica=index != 0,
                used_instances=self._yts)
            self._yts.append(local_yt)

        self._patch_yt_master_dynamic_config()
        if self._local_yt_options.get("queue_agent_count", 0) > 0:
            self._patch_queue_agent_dynamic_config()
        for yt in self._yts:
            self._configure_media(yt.create_client())

        self._init_yt_cluster(
            self._get_or_create_migration_yt_client(),
            self.get_db_path(),
            replica_clients=self.create_replica_clients(),
            to_actual_version=self._avoid_migrations,
            initialize_account=True,
        )

        self._prepared = True

        db_version = self._db_version
        self._set_db_version(self._data_model_traits.get_initial_db_version())
        self.migrate(db_version)

    def init_yt_cluster_for_tests(self, path_suffix, to_actual_version):
        orm_path = self.get_db_path() + path_suffix
        self._init_yt_cluster(
            self._get_or_create_migration_yt_client(),
            orm_path,
            replica_clients=self.create_replica_clients(),
            to_actual_version=to_actual_version,
            initialize_account=True,
        )
        return orm_path

    def _log_addresses(self):
        address_variables = {
            "Grpc": "orm_client_grpc_address",
            "Http": "orm_client_http_address",
            "Grpc secure": "orm_client_secure_grpc_address",
            "Http secure": "orm_client_secure_http_address",
        }
        for title, address_variable_name in iteritems(address_variables):
            if hasattr(self, address_variable_name):
                logger.info("%s address: %s", title, getattr(self, address_variable_name))

    def announce_addresses(self, config):
        _announce_addresses(self, config)

    def prepare_master_start(self):
        self._port_generator = OpenPortIterator(port_locks_path=self._port_locks_path)
        for master in self._masters:
            master.prepare_master_start(self.generate_config)
        self.announce_addresses(self._masters[0].config)

    def try_pause_master(self):
        for master in self._masters:
            try:
                os.kill(master.get_process().pid, signal.SIGSTOP)
            except OSError:
                logger.exception(
                    "Failed to pause {} instance with pid {}".format(
                        self._name,
                        self.rex_instance.pid,
                    )
                )

    def try_wake_up_master(self):
        for master in self._masters:
            try:
                os.kill(master.get_process().pid, signal.SIGCONT)
            except OSError:
                logger.exception(
                    "Failed to wake up {} instance with pid {}".format(
                        self._name,
                        self.rex_instance.pid,
                    )
                )

    def _start(self, master_binary_path):
        for master in self._masters:
            assert not _check_process_alive(
                master.get_process()), "{} master has been started already".format(
                    self._data_model_traits.get_human_readable_name()
                )

        if not self._prepared:
            self.prepare()

        if master_binary_path is None:
            master_binary_arcadia_path = self.get_arcadia_master_binary_path()
            if yatest_common is None:
                binary_name = os.path.basename(master_binary_arcadia_path)
                candidate_paths = which(binary_name)
                assert candidate_paths, "Error locating binary {}".format(binary_name)
                logger.info(
                    "Located binary (name: {}, candidates: {}, result: {})".format(
                        binary_name,
                        candidate_paths,
                        candidate_paths[0],
                    )
                )
                master_binary_path = candidate_paths[0]
            else:
                # Try to find binary within Arcadia testing environment best-effortly because
                # some users do not add binary path to environ PATH variable.
                master_binary_path = yatest_common.binary_path(
                    master_binary_arcadia_path,
                )

        self.prepare_master_start()

        for master in self._masters:
            master.start(master_binary_path, self._data_model_traits)

        self._log_addresses()

    def _set_db_version(self, db_version):
        assert db_version >= self._data_model_traits.get_initial_db_version()
        assert db_version <= self._data_model_traits.get_actual_db_version()
        self._db_version = db_version

    def get_db_version(self):
        return self._db_version

    def get_db_path(self):
        return self._data_model_traits.get_default_path()

    def _get_or_create_migration_yt_client(self):
        # Speeds up migration operations.
        if self._migration_yt_client:
            return self._migration_yt_client

        yt_client = self.create_yt_client()
        local_temp_directory = os.path.join(self.path, "migrations_tmp_" + generate_uuid()[:4])
        if not os.path.exists(local_temp_directory):
            os.mkdir(local_temp_directory)
        yt_client.config["local_temp_directory"] = local_temp_directory
        yt_client.config["pickling"]["modules_archive_compression_codec"] = "none"

        self._migration_yt_client = yt_client
        return self._migration_yt_client

    def _get_migrator(self):
        return Migrator(
            self._get_or_create_migration_yt_client(),
            self.get_db_path(),
            self._db_version,
            self._data_model_traits,
            backup_path=None,
            forced_compaction="skip",
        )

    def migrate(self, db_version, verify_schemas=False):
        """Migrates DB to the given version."""

        assert self._prepared, "Instance must be prepared"
        assert db_version >= self._db_version, "Migration is possible only in forward direction"
        if db_version == self._db_version:
            return
        for master in self._masters:
            assert (
                not master.is_alive()
            ), "Migration cannot be performed safely while {} master-{} is alive".format(
                master.get_tag(),
                self._data_model_traits.get_human_readable_name()
            )
        self._set_db_version(db_version)

        migrator = self._get_migrator()
        migrator.migrate_in_place(verify_schemas=verify_schemas)

    def migrate_without_downtime(self):
        assert self._db_version == self._data_model_traits.get_actual_db_version()
        self._get_migrator().migrate_without_downtime(sleep_between_phases=5)

    def prepare_yt(self, package_dir):
        if package_dir is None:
            package_dir = LocalYTManager.get_default_package_dir()
        self._yt_package_dir = package_dir

    def start(self, master_binary_path=None, stop_yt_on_failure=True):
        """Prepares underlying YT environment if not prepared yet and starts master."""

        try:
            self._start(master_binary_path=master_binary_path)
        except Exception:
            # Save sys.exc_info() because subsequent calls may overwrite it.
            start_exception_info = sys.exc_info()
            logger.exception(
                "Failed to start %s instance",
                self._data_model_traits.get_human_readable_name(),
            )
            try:
                self.stop(ignore_lock=True, stop_yt=stop_yt_on_failure)
            except Exception:
                logger.exception(
                    "Failed to stop %s instance after start failure",
                    self._data_model_traits.get_human_readable_name(),
                )
            reraise(*start_exception_info)

    def _try_read_from_file(self, file_name):
        def _on_error():
            logger.exception(
                "Failed to read from file %s during %s instance stop",
                file_name,
                self._data_model_traits.get_human_readable_name(),
            )

        return try_read_from_file(file_name, _on_error)

    def stop_master(self):
        for master in self._masters:
            master.stop(self._data_model_traits)

    def stop(self, ignore_lock=None, stop_yt=True):
        """Stops master and the underlying YT environment."""

        self.stop_master()
        if stop_yt and self._prepared and self._yt_manager_owned:
            self._prepared = False
            self._yt_manager.stop_managed_yts(ignore_lock)

    def create_client(self, config=None, transport="grpc", master_index=0, **kwargs):
        return self._masters[master_index].create_client(config=config, transport=transport, **kwargs)

    def create_replica_clients(self):
        return [yt.create_client() for yt in self._yts[1:]]

    def create_yt_client(self):
        return self._yts[0].create_client()

    def create_monitoring_client(self, master_index=0):
        return self._masters[master_index].create_monitoring_client()

    def get_certificate(self, master_index=0):
        credentials = self._masters[master_index].config["secure_client_grpc_server"]["addresses"][0]["credentials"]
        return credentials["pem_root_certs"]["value"]

    # Backward compatibility.
    @property
    def yt_instance(self):
        return self._yts[0].yt_instance

    @property
    def config(self):
        return self._masters[0].config

    @property
    def config_path(self):
        return self._masters[0].config_path
