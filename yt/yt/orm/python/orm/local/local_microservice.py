from .local import OrmMonitoringClient

from yt.orm.library.common import wait

from yt.wrapper import (
    YtClient,
    ypath_join,
)

from yt.environment.configs_provider import init_logging
from yt.environment.helpers import OpenPortIterator

from yt.wrapper.retries import run_with_retries

import yt.yson as yson

from yt.common import (
    update,
    which,
)

try:
    import yatest.common as yatest_common
except ImportError:
    yatest_common = None

if yatest_common is not None:
    from yt.environment.arcadia_interop import collect_cores

from abc import ABCMeta, abstractmethod
from copy import deepcopy

import contextlib
import os
import subprocess

MICROSERVICE_CONFIG_TEMPLATE = dict(
    logging=dict(),
    monitoring_server=dict(),
    internal_bus_server=dict(),
    internal_rpc_server=dict(),
    environment=dict(
        yt_connector=dict(
            connection=dict(),
            user="root",
            leader_lock_manager=dict(
                iteration_period=500,
                lock_holder=dict(
                    lock_transaction_timeout=3000,
                    lock_transaction_ping_period=500,
                ),
            ),
            instance_node_manager=dict(
                iteration_period=500,
                node_expiration_timeout=3000,
            ),
        ),
    ),
    address_resolver=dict(localhost_fqdn="localhost"),
    configuration_update_period=500,
)

SIMPLE_MICROSERVICE_CONFIG_TEMPLATE = dict(
    environment=dict(
        client=dict(
            connection=dict(
                authentication=dict(user="root"),
                secure=False,
                use_legacy_connection=False,
            )
        ),
    ),
)


class OrmMicroserviceInstance(object):
    __metaclass__ = ABCMeta

    def __init__(
        self,
        working_directory_path,
        config_patch,
        dynamic_config_patch,
        instance_tag=0,
        enable_debug_logging=True,
    ):
        self._working_directory_path = working_directory_path
        self._config_patch = deepcopy(config_patch)
        self._dynamic_config_patch = deepcopy(dynamic_config_patch)
        self._enable_debug_logging = enable_debug_logging
        self._instance_tag = instance_tag
        self._dynamic_config_path = ypath_join(self.get_yt_root_path(), "config")
        self._dynamic_config_revision_path = ypath_join(self._dynamic_config_path, "@revision")

        self._config = None
        self._dynamic_config = None
        self._process = None

    @abstractmethod
    def get_logger(self):
        raise NotImplementedError

    @abstractmethod
    def get_service_name(self):
        raise NotImplementedError

    @abstractmethod
    def get_binary_name(self):
        raise NotImplementedError

    @abstractmethod
    def get_log_file_name(self):
        raise NotImplementedError

    @abstractmethod
    def get_iteration_config_revision_path(self):
        raise NotImplementedError

    @abstractmethod
    def get_yt_root_path(self):
        raise NotImplementedError

    @abstractmethod
    def get_dynamic_config_template(self):
        raise NotImplementedError

    def get_working_directory_path(self):
        return self._working_directory_path

    def get_config_template(self):
        """
        Returns microservice config populated with parameters known before underlying
        environment has been started.
        """
        config = deepcopy(MICROSERVICE_CONFIG_TEMPLATE)

        config["instance_fqdn"] = self._get_instance_fqdn()
        config["environment"]["yt_connector"]["root_path"] = self.get_yt_root_path()
        config["logging"] = init_logging(
            path=self._working_directory_path,
            name=self.get_log_file_name(),
            log_errors_to_stderr=False,
            enable_debug_logging=self._enable_debug_logging,
            enable_log_compression=True,
            log_compression_method="zstd",
            abort_on_alert=False,
        )

        return config

    def populate_config_transient_parameters(
        self,
        config,
        yt_http_proxy_address,
        open_port_iterator,
        **kwargs
    ):
        """
        Populates config with parameters that are known only after underlying environment
        has been started.
        """
        config["environment"]["yt_connector"]["connection"]["cluster_url"] = yt_http_proxy_address
        config["monitoring_server"]["port"] = next(open_port_iterator)
        config["internal_bus_server"]["port"] = next(open_port_iterator)

    def populate_dynamic_config_transient_parameters(self, config, **kwargs):
        """
        Similar to populate_config_transient_parameters, but populates
        dynamic config instead of static one.
        """
        pass

    def _get_instances_path(self):
        return ypath_join(self.get_yt_root_path(), "instances")

    def _prepare_yt_environment(self, yt_client):
        yt_client.create(
            "document",
            self._dynamic_config_path,
            recursive=True,
            ignore_existing=True,
        )
        yt_client.set(self._dynamic_config_path, self._dynamic_config)

    def _prepare_local_environment(self):
        os.makedirs(self._working_directory_path, exist_ok=True)

    def _generate_config(self, yt_http_proxy_address, open_port_iterator, aux_transient_parameters):
        config = self.get_config_template()
        self.populate_config_transient_parameters(
            config,
            yt_http_proxy_address=yt_http_proxy_address,
            open_port_iterator=open_port_iterator,
            **aux_transient_parameters
        )

        if self._config_patch is not None:
            config = update(config, self._config_patch)

        return config

    def _generate_dynamic_config(self, yt_http_proxy_address, open_port_iterator, aux_transient_parameters):
        config = self.get_dynamic_config_template()
        self.populate_dynamic_config_transient_parameters(
            config,
            yt_http_proxy_address=yt_http_proxy_address,
            open_port_iterator=open_port_iterator,
            **aux_transient_parameters
        )

        if self._dynamic_config_patch is not None:
            config = update(config, self._dynamic_config_patch)

        return config

    def _dump_config(self, config):
        config_file_path = os.path.join(self._working_directory_path, "config.yson")
        with open(config_file_path, "wb") as config_file:
            yson.dump(config, config_file, yson_format="pretty")
        return config_file_path

    def _get_monitoring_port(self):
        return self._config["monitoring_server"]["port"]

    def create_monitoring_client(self):
        return OrmMonitoringClient("localhost:{}".format(self._get_monitoring_port()))

    def _wait_for_instance_liveness(self, yt_client):
        monitoring_client = self.create_monitoring_client()

        def check_liveness():
            logger = self.get_logger()
            logger.debug(
                "Checking %s monitoring interface (tag: %d)",
                self.get_service_name(),
                self._instance_tag,
            )

            monitoring_client.get("/yt_connector/is_leading")

            logger.debug(
                "Checking %s instances node (tag: %d)",
                self.get_service_name(),
                self._instance_tag,
            )
            instances = set(yt_client.list(self._get_instances_path()))
            instance_fqdn = self._get_instance_fqdn()
            assert instance_fqdn in instances

            logger.debug(
                "Checking %s instance \"%s\" Orchid",
                self.get_service_name(),
                instance_fqdn,
            )
            assert "orchid" in yt_client.list(
                ypath_join(self._get_instances_path(), instance_fqdn),
            )

        run_with_retries(
            check_liveness,
            retry_count=100,
            backoff=0.5,
            exceptions=(Exception, AssertionError),
        )

    def _set_dynamic_config_impl(self, config):
        current_config = self.get_dynamic_config()
        if config == current_config:
            return

        monitoring_client = self.create_monitoring_client()

        def get_actual_config_revision():
            return monitoring_client.get(self.get_iteration_config_revision_path())

        self._yt_client.set(self._dynamic_config_path, config)

        cypress_config_revision = self._yt_client.get(self._dynamic_config_revision_path)
        wait(lambda: get_actual_config_revision() >= cypress_config_revision)

    # Passed parameters are transient: they are not known at the instance construction time
    # and determined only after underlying components (YT cluster, ORM master) are started.
    def start(self, yt_http_proxy_address, port_locks_path, **kwargs):
        self._prepare_local_environment()

        open_port_iterator = OpenPortIterator(port_locks_path)
        self._config = self._generate_config(
            yt_http_proxy_address,
            open_port_iterator,
            aux_transient_parameters=kwargs,
        )
        self._dynamic_config = self._generate_dynamic_config(
            yt_http_proxy_address,
            open_port_iterator,
            aux_transient_parameters=kwargs,
        )
        config_file_path = self._dump_config(self._config)

        self._yt_client = YtClient(proxy=yt_http_proxy_address)
        self._prepare_yt_environment(self._yt_client)

        stdout = open(os.path.join(self._working_directory_path, "stdout"), "a")
        stderr = open(os.path.join(self._working_directory_path, "stderr"), "a")

        self._process = subprocess.Popen(
            [self.get_binary_name(), "--config", config_file_path],
            cwd=self._working_directory_path,
            stdout=stdout,
            stderr=stderr,
        )

        self._wait_for_instance_liveness(self._yt_client)
        self.get_logger().info(
            "%s instance started successfully (tag: %d, pid: %d)",
            self.get_service_name(),
            self._instance_tag,
            self._process.pid,
        )

    def stop(self):
        pid = self._process.pid
        logger = self.get_logger()
        try:
            os.kill(pid, 9)
            logger.info(
                "Killed %s instance (tag: %d, pid: %d)",
                self.get_service_name(),
                self._instance_tag,
                pid,
            )
        except OSError:
            logger.exception(
                "Failed to kill %s instance (tag: %d, pid: %d)",
                self.get_service_name(),
                self._instance_tag,
                pid,
            )
        self._collect_cores([pid])

    def get_pid(self):
        return self._process.pid

    def get_dynamic_config(self):
        return self._yt_client.get(self._dynamic_config_path)

    def set_dynamic_config(self, config):
        self._set_dynamic_config_impl(config)

    def patch_dynamic_config(self, patch):
        old = self.get_dynamic_config()
        self._set_dynamic_config_impl(update(old, patch))

    @contextlib.contextmanager
    def dynamic_config_patched(self, patch):
        old = self.get_dynamic_config()
        new = update(old, patch)

        try:
            self._set_dynamic_config_impl(new)
            yield
        finally:
            self._set_dynamic_config_impl(old)

    def reset_dynamic_config_to_initial(self):
        self._set_dynamic_config_impl(self._dynamic_config)

    def _locate_binary(self):
        candidate_paths = which(self.get_binary_name())
        assert candidate_paths, "Error locating binary {}".format(self.get_binary_name())
        self.get_logger().info("Located binary (name: {}, candidates: {}, result: {})".format(
            self.get_binary_name(),
            candidate_paths,
            candidate_paths[0],
        ))
        return candidate_paths[0]

    def _collect_cores(self, pids):
        if yatest_common is None:
            return

        collect_cores(
            pids,
            self._working_directory_path,
            [self._locate_binary()],
            logger=self.get_logger(),
        )

    def _get_instance_fqdn(self):
        return "localhost-{}".format(self._instance_tag)


class OrmSimpleMicroserviceInstance(OrmMicroserviceInstance):
    def get_config_template(self):
        config = super(OrmSimpleMicroserviceInstance, self).get_config_template()
        return update(config, SIMPLE_MICROSERVICE_CONFIG_TEMPLATE)

    def populate_config_transient_parameters(
        self,
        config,
        yt_http_proxy_address,
        open_port_iterator,
        orm_master_grpc_address,
        **kwargs
    ):
        super(OrmSimpleMicroserviceInstance, self).populate_config_transient_parameters(
            config,
            yt_http_proxy_address,
            open_port_iterator,
        )
        config["environment"]["client"]["connection"]["addresses"] = [orm_master_grpc_address]
